# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Main charm module for the microcloud charm.

The charm operates in one of two auto-detected modes per node:

Deployment mode (MicroCloud not yet initialized)
------------------------------------------------
install / config-changed / peer-relation-changed
    • Install the microcloud, lxd, and (optionally) microceph/microovn snaps
      at the configured channels with a shared cohort, then hold refreshes.
    • Once every unit is ready, the leader opens a join session: it starts
      "microcloud preseed" in the background, outside the hook, and
      publishes the session (its address and the systems it lists) on the
      peer relation. Joining is unicast (not multicast), so every unit
      renders the same document and each daemon determines its own role by
      matching its address against the initiator address.
    • Publishing the session triggers a hook on every other unit, from
      which each listed joiner starts dialling in, in the background too,
      retrying until the session has opened.
    • Once the session ends, it fires update-status on the leader, which
      clears it and opens the next session if any unit is still waiting.
      Units added to a formed cluster join it the same way.

Observe-only mode (MicroCloud already initialized out-of-band)
--------------------------------------------------------------
    • No snaps are installed and no bootstrap is attempted.
    • The leader validates that every MicroCloud member has a corresponding
      Juju unit (matched on hostname) and blocks on any mismatch.

Observability (both modes, optional)
------------------------------------
cos-agent-relation-joined / changed
    • Ensure metrics endpoints exist (LXD metrics config, Ceph mgr module,
      ovn-exporter snap) and publish scrape jobs + dashboards. MicroCeph's
      own logs are forwarded over this same relation via its "ceph-logs"
      snap slot.

cos-agent-relation-broken
    • Tear the metrics setup back down.

logging-relation-joined / changed
    • Point LXD's native Loki client (loki.api.url) at the related
      Loki-compatible endpoint (e.g. opentelemetry-collector's
      receive-loki-logs) so LXD streams its own logs there directly.

logging-relation-broken
    • Disable LXD's Loki streaming again.
"""

import json
import logging
import re
import subprocess
import time
import uuid
from typing import Any

import ops
from charms.grafana_agent.v0.cos_agent import COSAgentProvider
from charms.loki_k8s.v1.loki_push_api import LokiPushApiConsumer

import microcloud
import session
import snap
from ceph_mgr import CephMgrError, CephMgrPrometheus
from cluster import ClusterCoordinator, JoinSession, PeerSystem, validate_membership
from network import UnitNetwork
from ovn_exporter import OVNExporter, OVNExporterError
from preseed import PreseedInputs, SystemEntry, render

logger = logging.getLogger(__name__)

# Dashboard JSON directories — one per service.
_DASHBOARD_DIRS = [
    "./src/dashboards/lxd",
    "./src/dashboards/microceph",
    "./src/dashboards/microovn",
]
_ALERT_RULES_DIR = "./src/prometheus_alert_rules"

# LXD metrics address — dedicated loopback listener, always TLS.
_LXD_METRICS_ADDRESS = "127.0.0.1:8444"

# How long past its session timeout a join session is assumed to have ended,
# for a new leader deciding whether it may open another.
_SESSION_GRACE = 120

# How many join sessions in a row may fail before the leader stops opening
# the next one straight away and blocks, retrying only on later hooks.
_SESSION_RETRIES = 3


class LXDConfigError(Exception):
    """Raised when an LXD configuration operation fails."""


class JoinSessionEndedEvent(ops.EventBase):
    """A join session worker on this unit has finished."""


class MicroCloudCharmEvents(ops.CharmEvents):
    """Charm events, including the one join session workers dispatch."""

    join_session_ended = ops.EventSource(JoinSessionEndedEvent)


class MicroCloudCharm(ops.CharmBase):
    """Deploys and operates a MicroCloud cluster, optionally wired to COS."""

    on = MicroCloudCharmEvents()  # pyright: ignore[reportAssignmentType]
    _stored = ops.StoredState()

    def __init__(self, *args: Any) -> None:
        super().__init__(*args)
        self._stored.set_default(session_failures=0, worker_pid=0, worker_session="")

        # Lazy-initialised observability helpers.
        self._ceph: CephMgrPrometheus | None = None
        self._ovn: OVNExporter | None = None

        self._coordinator = ClusterCoordinator(self)
        self._network = UnitNetwork(self.model, self.config)

        # Set once the deploy path reports a transitional status of its own,
        # so the generic status at the end of reconciliation does not
        # overwrite it.
        self._status_held = False

        self._cos_agent = COSAgentProvider(
            self,
            scrape_configs=self._build_scrape_configs,
            dashboard_dirs=_DASHBOARD_DIRS,
            metrics_rules_dir=_ALERT_RULES_DIR,
            log_slots=self._log_slots(),
            refresh_events=[
                self.on.config_changed,
                self.on.update_status,
            ],
        )

        self._loki_consumer = LokiPushApiConsumer(self, relation_name="logging")

        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.upgrade_charm, self._on_install)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.update_status, self._on_update_status)
        self.framework.observe(self.on.join_session_ended, self._on_join_session_ended)
        self.framework.observe(self.on.cluster_relation_changed, self._on_cluster_relation_changed)
        self.framework.observe(
            self.on.cluster_relation_departed, self._on_cluster_relation_changed
        )
        self.framework.observe(self.on.stop, self._on_stop)
        self.framework.observe(self.on.remove, self._on_remove)

        self.framework.observe(
            self.on.cos_agent_relation_joined, self._on_cos_agent_relation_joined
        )
        self.framework.observe(
            self.on.cos_agent_relation_changed, self._on_cos_agent_relation_joined
        )
        self.framework.observe(
            self.on.cos_agent_relation_broken, self._on_cos_agent_relation_broken
        )

        self.framework.observe(
            self.on.logging_relation_joined, self._on_loki_push_api_endpoint_joined
        )
        self.framework.observe(
            self.on.logging_relation_changed, self._on_loki_push_api_endpoint_joined
        )
        self.framework.observe(
            self.on.logging_relation_departed, self._on_loki_push_api_endpoint_departed
        )
        self.framework.observe(
            self.on.logging_relation_broken, self._on_loki_push_api_endpoint_departed
        )

        # Actions
        self.framework.observe(self.on.status_action, self._on_status_action)
        self.framework.observe(self.on.dump_metrics_config_action, self._on_dump_metrics_config)

    # ------------------------------------------------------------------
    # Helpers factory
    # ------------------------------------------------------------------

    def _make_helpers(self) -> None:
        self._ceph = CephMgrPrometheus(
            port=int(self.config.get("ceph-mgr-prometheus-port", 9283)),
            rbd_stats_pools=self.config.get("ceph-rbd-stats-pools", ""),
            enable_perf_metrics=self.config.get("ceph-enable-perf-metrics", False),
        )
        self._ovn = OVNExporter(
            channel=self.config.get("ovn-exporter-channel", "latest/edge"),
        )

    # ------------------------------------------------------------------
    # Mode detection
    # ------------------------------------------------------------------

    def _cos_related(self) -> bool:
        return bool(self.model.relations.get("cos-agent"))

    def _log_slots(self) -> list[str]:
        """Snap log slots to advertise over cos-agent.

        The microceph snap exposes its logs via a "ceph-logs" content-interface
        slot, so we can forward them without any custom log-shipping logic.
        LXD and microovn do not (yet) expose an equivalent slot.
        """
        slots: list[str] = []
        if snap.is_installed("microceph"):
            slots.append("microceph:ceph-logs")
        return slots

    # ------------------------------------------------------------------
    # Core hook handlers
    # ------------------------------------------------------------------

    def _on_install(self, event: ops.InstallEvent | ops.UpgradeCharmEvent) -> None:
        self._reconcile()

    def _on_config_changed(self, event: ops.ConfigChangedEvent) -> None:
        self._reconcile()

    def _on_cluster_relation_changed(self, event: ops.RelationEvent) -> None:
        self._reconcile()

    def _on_update_status(self, event: ops.UpdateStatusEvent) -> None:
        self._reconcile()

    def _on_join_session_ended(self, event: JoinSessionEndedEvent) -> None:
        self._reconcile()

    def _on_stop(self, event: ops.StopEvent) -> None:
        if self._cos_related():
            self._teardown_observability(full_cleanup=False)

    def _on_remove(self, event: ops.RemoveEvent) -> None:
        # Never destroy the MicroCloud cluster itself; only clean up
        # observability artifacts this charm created.
        self._teardown_observability(full_cleanup=True)

    # ------------------------------------------------------------------
    # cos-agent handlers
    # ------------------------------------------------------------------

    def _on_cos_agent_relation_joined(self, event: ops.RelationEvent) -> None:
        self._setup_observability()
        self._reconcile()

    def _on_cos_agent_relation_broken(self, event: ops.RelationBrokenEvent) -> None:
        self._teardown_observability(full_cleanup=True)
        self._reconcile()

    # ------------------------------------------------------------------
    # logging (Loki) handlers
    # ------------------------------------------------------------------

    def _on_loki_push_api_endpoint_joined(self, event: ops.EventBase) -> None:
        self._ensure_lxd_loki_config()

    def _on_loki_push_api_endpoint_departed(self, event: ops.RelationEvent) -> None:
        self._teardown_lxd_loki_config()

    # ------------------------------------------------------------------
    # Reconciliation
    # ------------------------------------------------------------------

    def _reconcile(self) -> None:
        """Drive deployment or observe-only mode, then set status."""
        self._make_helpers()

        ovn_uplink_interface, problem = self._network.ovn_uplink_interface()
        if problem:
            self.unit.status = ops.BlockedStatus(problem)
            return

        initialized = microcloud.is_initialized()

        # Publish our identity for peers as early as possible.
        self._coordinator.publish_identity(
            microcloud.hostname(),
            self._network.bind_address(),
            ovn_uplink_interface=ovn_uplink_interface,
            ovn_underlay_ip=self._network.space_bind_address("ovn-underlay"),
            storage_local_path=self._storage_local_path(),
            storage_ceph_paths=self._storage_ceph_paths(),
            initialized=initialized,
        )

        # A unit that is not yet clustered runs the deploy path, whether that
        # forms the cluster or joins an existing one.
        #
        # A clustered unit normally just observes. The one exception is the
        # leader while peers are still joining: it is the initiator of that
        # join and has to open the session. Other clustered units must stay
        # out of it entirely - running "microcloud preseed" on an already
        # clustered non-initiator is rejected outright ("MicroCloud is already
        # initialized and can only be the initiator"), which would leave them
        # retrying instead of sitting active.
        try:
            pending = self._pending_systems(initialized)
        except microcloud.MicroCloudError as exc:
            self.unit.status = ops.BlockedStatus(f"Cannot read MicroCloud members: {exc}")
            return

        # The leader also stays on the deploy path while its join session is
        # published, to pick up the result and clear it.
        initiating = self.unit.is_leader() and (
            bool(pending) or self._coordinator.session() is not None
        )

        if initialized and not initiating:
            problem = self._reconcile_observe_only()
        else:
            problem = self._reconcile_deploy(initialized, pending)

        if problem:
            self.unit.status = ops.BlockedStatus(problem)
            return

        if self._cos_related():
            obs_problem = self._reconcile_observability()
            if obs_problem:
                self.unit.status = ops.BlockedStatus(obs_problem)
                return

        if not self._status_held:
            self._set_status(initialized=microcloud.is_initialized())

    def _hold_status(self, status: ops.StatusBase) -> None:
        """Report a transitional status that reconciliation must not overwrite."""
        self.unit.status = status
        self._status_held = True

    # ---- Deployment mode ----

    def _reconcile_deploy(self, initialized: bool, pending: list[PeerSystem]) -> str | None:
        """Install snaps, then form or grow the cluster through a join session.

        Juju only propagates a unit's relation-data writes to its peers
        once the writing hook exits, and a unit's own write never triggers a
        hook on that unit. A session opened inside a hook could therefore not
        be announced until it was already over. Instead:

        1. The leader starts "microcloud preseed" in the background (see
           ``session``), publishes the session it opened and exits. The
           commit triggers relation-changed on every other unit.
        2. Each joiner listed in the session starts dialling in from that
           hook, also in the background, retrying until the session opens.
           A later session replaces an attempt still waiting on this one.
        3. When the session ends, the leader picks up its result from a hook
           the background process fires, clears the session and opens the
           next one if units are still waiting to join.

        ``initialized`` is whether this unit is already clustered, which
        only holds for the leader when it is growing the cluster.
        ``pending`` is the systems not yet clustered.

        Returns a problem string to block on, or None.
        """
        # A clustered leader only gets here to run a join session. Its snaps
        # are already in place, and refreshing them now would upgrade this
        # one member ahead of the rest of the cluster.
        if not initialized:
            channels = {
                "lxd": self.config.get("snap-channel-lxd", "6/stable"),
                "microceph": self.config.get("snap-channel-microceph", ""),
                "microovn": self.config.get("snap-channel-microovn", ""),
                "microcloud": self.config.get("snap-channel-microcloud", "3/stable"),
            }
            try:
                snap.ensure_snaps(channels)
            except snap.SnapError as exc:
                return f"Snap install: {exc}"

        if not microcloud.waitready(timeout=60):
            self._hold_status(ops.WaitingStatus("Waiting for microcloud daemon"))
            return None

        if not self._coordinator.all_identities_published():
            self._hold_status(ops.WaitingStatus("Waiting for all peers to report identity"))
            return None

        passphrase = self._coordinator.ensure_passphrase()
        if len(self._coordinator.all_members()) > 1 and not passphrase:
            self._hold_status(ops.WaitingStatus("Waiting for session passphrase"))
            return None

        # Every unit has now cleared all its own prerequisites: signal that
        # it is ready to be listed in a join session.
        self._coordinator.publish_ready()

        if self.unit.is_leader():
            return self._lead_session(initialized, pending, passphrase or "")
        return self._join_session(passphrase or "")

    def _lead_session(
        self, initialized: bool, pending: list[PeerSystem], passphrase: str
    ) -> str | None:
        """Leader side: follow the published join session, or open the next one."""
        address = self._network.bind_address()
        current = self._coordinator.session()
        failure = ""

        if current is not None and current.address != address:
            # Opened by a previous leader. This unit's own state for that
            # session is from dialling in to it, not the session's result.
            if time.time() < current.deadline:
                # It may still be running on that unit, and opening another
                # now would compete with it.
                self._hold_status(
                    ops.WaitingStatus("Waiting for the previous leader's join session to end")
                )
                return None
            self._coordinator.publish_session(None)

        elif current is not None:
            # Check for a result before whether the worker is running: the
            # hook the worker fires once it is done runs while the worker is
            # still waiting on that hook.
            outcome = session.result(current.id)
            if outcome is None and self._worker_running(current.id):
                self._hold_status(
                    ops.MaintenanceStatus(_session_message(current, microcloud.hostname()))
                )
                return None

            self._coordinator.publish_session(None)
            session.clear()
            if outcome is not None and outcome[0] != 0:
                logger.error("Join session %s failed:\n%s", current.id, outcome[1])
                failure = _last_line(outcome[1])
                self._stored.session_failures += 1
                # Nothing else wakes the leader once its session is cleared,
                # so open the next one from this hook, unless sessions keep
                # failing: then block and leave retrying to later hooks.
                if self._stored.session_failures >= _SESSION_RETRIES:
                    return f"Join session failed: {failure}"
            else:
                self._stored.session_failures = 0

        if current is not None:
            # The membership read at the start of this hook can predate the
            # session that just ended: its result may have landed while this
            # hook was already running. Read it again, or the next session
            # would list units that have just joined, and that never dial in.
            initialized = microcloud.is_initialized()
            try:
                pending = self._pending_systems(initialized)
            except microcloud.MicroCloudError as exc:
                return f"Cannot read MicroCloud members: {exc}"

        if not pending:
            return None

        # Units already in the cluster have nothing left to get ready for,
        # and in a cluster formed outside Juju they never report ready.
        if not self._coordinator.all_ready(pending if initialized else None):
            self._hold_status(ops.WaitingStatus("Waiting for all peers to be ready"))
            return None

        # "microcloud preseed" cannot add systems to a cluster without
        # MicroCeph: it panics looking up the cluster's Ceph networks. Say so,
        # rather than open sessions that are bound to fail.
        if initialized and not snap.is_installed("microceph"):
            return (
                f"Cannot add {len(pending)} unit(s): MicroCloud cannot add systems "
                "to a cluster without MicroCeph"
            )

        # Only a clustered unit can add others to the cluster, so an
        # unclustered leader must not take over from a cluster that already
        # exists: it would bootstrap a second one instead.
        if not initialized and self._coordinator.any_initialized():
            return (
                f"Leader {microcloud.hostname()} is not a MicroCloud member "
                "but other units are; cannot initiate"
            )

        # MicroCloud infers whether to form a cluster or add to one from
        # whether the initiator is itself listed under "systems". Listing
        # every unit forms a cluster; listing only the units not yet
        # clustered adds them to the one the initiator belongs to.
        listed = pending if initialized else self._coordinator.all_systems()
        timeout = int(self.config.get("session-timeout", 300))
        opened = JoinSession(
            id=uuid.uuid4().hex,
            address=address,
            systems=[system.name for system in listed],
            deadline=time.time() + timeout + _SESSION_GRACE,
        )

        document = render(self._preseed_inputs(address, passphrase, _system_entries(listed)))
        try:
            self._start_worker(opened.id, document)
        except session.SessionError as exc:
            return str(exc)

        self._coordinator.publish_session(opened)
        message = _session_message(opened, microcloud.hostname())
        if failure:
            message = f"{message}; retrying after: {failure}"
        self._hold_status(ops.MaintenanceStatus(message))
        return None

    def _pending_systems(self, initialized: bool) -> list[PeerSystem]:
        """Return the systems not yet clustered.

        ``initialized`` is whether this unit is clustered. The published flags
        lag behind, so a clustered leader reads the membership back from
        MicroCloud instead.

        Raises microcloud.MicroCloudError if the membership cannot be read.
        """
        if initialized and self.unit.is_leader():
            members = {member.name for member in microcloud.list_members()}
            return self._coordinator.pending_systems(members)
        return self._coordinator.pending_systems()

    def _join_session(self, passphrase: str) -> str | None:
        """Joiner side: dial in to the published join session if listed in it.

        The attempt runs in the background, like the initiator's session, so
        that a later session can replace it: a joiner still waiting on a
        session that failed would otherwise miss the next one. Once it
        succeeds, the hook it fires finds this unit clustered and publishes
        that, which is what tells the leader.
        """
        current = self._coordinator.session()
        if current is None:
            self._hold_status(ops.WaitingStatus("Waiting for the leader to open a join session"))
            return None

        if microcloud.hostname() not in current.systems:
            self._hold_status(ops.WaitingStatus("Waiting for the next join session"))
            return None

        # A leader that lost leadership mid-session is still its initiator.
        if current.address == self._network.bind_address():
            self._hold_status(
                ops.WaitingStatus("Waiting for the join session this unit opened to end")
            )
            return None

        outcome = session.result(current.id)
        if outcome is not None and outcome[0] == 0:
            # MicroCloud reports this unit as clustered only once the other
            # services have joined too, a little after the attempt succeeds.
            self._hold_status(ops.MaintenanceStatus("Joined the MicroCloud cluster"))
            return None
        if outcome is not None:
            # The leader opens the next session; this unit just waits for it.
            logger.error("Joining session %s failed:\n%s", current.id, outcome[1])
            self._hold_status(
                ops.WaitingStatus(
                    f"Waiting for the next join session; joining failed: {_last_line(outcome[1])}"
                )
            )
            return None

        if not self._worker_running(current.id):
            # Render exactly the systems the initiator listed, so this
            # document matches the one the session was opened with.
            by_name = {system.name: system for system in self._coordinator.all_systems()}
            listed = [by_name[name] for name in current.systems if name in by_name]
            document = render(
                self._preseed_inputs(current.address, passphrase, _system_entries(listed))
            )
            try:
                self._start_worker(current.id, document, retry_until=current.deadline)
            except session.SessionError as exc:
                return str(exc)

        self._hold_status(ops.MaintenanceStatus("Joining the MicroCloud cluster"))
        return None

    def _worker_running(self, session_id: str) -> bool:
        """Return True while this unit's worker for ``session_id`` is running."""
        return self._stored.worker_session == session_id and session.is_running(
            self._stored.worker_pid
        )

    def _start_worker(self, session_id: str, document: str, retry_until: float = 0) -> None:
        """Replace this unit's join session worker with one for ``session_id``."""
        self._stored.worker_pid = session.start(
            session_id,
            document,
            self.unit.name,
            self.charm_dir,
            retry_until=retry_until,
            replacing=self._stored.worker_pid,
        )
        self._stored.worker_session = session_id

    def _preseed_inputs(
        self, initiator_address: str, passphrase: str, systems: list[SystemEntry]
    ) -> PreseedInputs:
        """Build the PreseedInputs for this unit's preseed document."""
        with_ceph_storage = any(s.storage_ceph_paths for s in systems)

        return PreseedInputs(
            initiator_address=initiator_address,
            session_passphrase=passphrase,
            systems=systems,
            session_timeout=int(self.config.get("session-timeout", 300)),
            # In preseed mode this is how long the initiator waits for every
            # listed system to reach out, not just a multicast setting. Its
            # 60s default would expire long before a joiner held up in
            # another hook dials in, so wait for the whole session.
            lookup_timeout=int(self.config.get("session-timeout", 300)),
            with_ceph=bool(self.config.get("snap-channel-microceph", "")),
            ceph_cephfs=bool(self.config.get("ceph-cephfs", False)),
            # "microcloud preseed" rejects a public/internal network without
            # Ceph storage disks ("Cannot specify a Ceph public network
            # without Ceph storage disks"), so only derive these from the
            # ceph-public/ceph-internal bindings when at least one system
            # has a "ceph" Juju storage volume attached. Otherwise Juju's
            # default-space fallback for an unbound extra-binding would
            # still resolve to *some* subnet and produce an invalid
            # preseed.
            ceph_public_network=self._network.space_network_cidr("ceph-public")
            if with_ceph_storage
            else "",
            ceph_internal_network=self._network.space_network_cidr("ceph-internal")
            if with_ceph_storage
            else "",
            with_ovn=bool(self.config.get("snap-channel-microovn", "")),
            ovn_ipv4_gateway=self.config.get("ovn-ipv4-gateway", ""),
            ovn_ipv4_range=self.config.get("ovn-ipv4-range", ""),
            ovn_ipv6_gateway=self.config.get("ovn-ipv6-gateway", ""),
            ovn_dns_servers=self.config.get("ovn-dns-servers", ""),
            storage_wipe=bool(self.config.get("storage-wipe", False)),
            storage_encrypt=bool(self.config.get("storage-encrypt", False)),
        )

    def _storage_local_path(self) -> str:
        """Return the device path of this unit's attached "local" Juju storage.

        The "local" storage volume is declared with ``multiple: range: 0-1``
        in charmcraft.yaml, so at most one instance is ever attached.
        Returns "" if none is attached.
        """
        storages = self.model.storages["local"]
        if not storages:
            return ""
        return str(storages[0].location)

    def _storage_ceph_paths(self) -> list[str]:
        """Return device paths of this unit's attached "ceph" Juju storage.

        The "ceph" storage volume is declared with ``multiple: range: 0-``
        in charmcraft.yaml, so any number of instances may be attached -
        each becomes a separate Ceph OSD on this system.
        """
        return [str(storage.location) for storage in self.model.storages["ceph"]]

    # ---- Observe-only mode ----

    def _reconcile_observe_only(self) -> str | None:
        """Validate Juju units against existing MicroCloud members.

        Only the leader performs cross-unit validation (it can see the full
        peer relation). Returns a problem string to block on, or None.
        """
        if not self.unit.is_leader():
            return None

        try:
            members = microcloud.list_members()
        except microcloud.MicroCloudError as exc:
            return f"Cannot read MicroCloud members: {exc}"

        member_names = {m.name for m in members}
        juju_hostnames = {name for name, _ in self._coordinator.all_members()}

        problems = validate_membership(juju_hostnames, member_names)
        if problems:
            return "; ".join(problems)
        return None

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    def _setup_observability(self) -> None:
        self._make_helpers()

    def _reconcile_observability(self) -> str | None:
        """Idempotently ensure metrics endpoints exist. Returns problem or None."""
        errors: list[str] = []

        try:
            self._ensure_lxd_metrics_config()
        except LXDConfigError as exc:
            errors.append(f"LXD metrics config: {exc}")

        if snap.is_installed("microceph"):
            try:
                self._ceph.ensure_enabled()
            except CephMgrError as exc:
                errors.append(f"Ceph mgr: {exc}")

        if snap.is_installed("microovn"):
            try:
                self._ovn.ensure_installed()
            except OVNExporterError as exc:
                errors.append(f"OVN exporter: {exc}")

        if errors:
            return "; ".join(errors)

        self._cos_agent_refresh()
        return None

    def _teardown_observability(self, *, full_cleanup: bool) -> None:
        self._make_helpers()
        try:
            _lxc_config_set("core.metrics_address", "")
            _lxc_config_set("core.metrics_authentication", "true")
        except LXDConfigError as exc:
            logger.warning("Cannot reset LXD metrics config during teardown: %s", exc)

        if full_cleanup and snap.is_installed("microovn"):
            try:
                self._ovn.remove()
            except OVNExporterError as exc:
                logger.warning("Cannot remove ovn-exporter during teardown: %s", exc)

    def _cos_agent_refresh(self) -> None:
        self._cos_agent._on_refresh(None)

    def _ensure_lxd_metrics_config(self) -> None:
        """Set core.metrics_address and core.metrics_authentication on LXD."""
        _lxc_config_set("core.metrics_address", _LXD_METRICS_ADDRESS)
        _lxc_config_set("core.metrics_authentication", "false")

    def _ensure_lxd_loki_config(self) -> None:
        """Point LXD's native Loki client at the related Loki push endpoint."""
        endpoints = self._loki_consumer.loki_endpoints
        if not endpoints:
            logger.debug("logging relation joined but no Loki endpoint published yet")
            return

        url = endpoints[0].get("url", "")
        if not url:
            logger.warning("Loki endpoint data is missing a url")
            return

        # LXD expects only the base API URL (protocol + host + optional port),
        # not the full push path.
        if url.endswith("/loki/api/v1/push"):
            url = url[: -len("/loki/api/v1/push")]

        if not _lxd_has_api_extension("loki"):
            logger.error("LXD is missing the loki API extension; cannot stream logs to %s", url)
            return

        try:
            _lxc_config_set("loki.api.url", url)
        except LXDConfigError as exc:
            logger.warning("Cannot set LXD loki.api.url: %s", exc)
            return

        logger.info("LXD is now streaming logs to Loki at %s", url)

    def _teardown_lxd_loki_config(self) -> None:
        """Stop LXD from streaming logs to Loki."""
        try:
            _lxc_config_set("loki.api.url", "")
        except LXDConfigError as exc:
            logger.warning("Cannot reset LXD loki.api.url during teardown: %s", exc)
            return

        logger.info("LXD is no longer streaming logs to Loki")

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def _set_status(self, *, initialized: bool) -> None:
        if not initialized:
            self.unit.status = ops.WaitingStatus("Waiting for cluster to form")
            return

        if self._cos_related():
            problems: list[str] = []
            if snap.is_installed("microovn"):
                healthy, reason = self._ovn.is_healthy()
                if not healthy:
                    problems.append(reason)
            if problems:
                self.unit.status = ops.BlockedStatus("; ".join(problems))
                return
            self.unit.status = ops.ActiveStatus("Cluster ready; observability active")
            return

        self.unit.status = ops.ActiveStatus("Cluster ready")

    # ------------------------------------------------------------------
    # Action handlers
    # ------------------------------------------------------------------

    def _on_status_action(self, event: ops.ActionEvent) -> None:
        initialized = microcloud.is_initialized()
        result: dict[str, Any] = {
            "mode": "observe-only" if initialized else "deploy",
            "initialized": initialized,
        }
        if initialized:
            try:
                members = microcloud.list_members()
                result["members"] = json.dumps(
                    [{"name": m.name, "address": m.address, "status": m.status} for m in members]
                )
            except microcloud.MicroCloudError as exc:
                result["members-error"] = str(exc)
        event.set_results(result)

    def _on_dump_metrics_config(self, event: ops.ActionEvent) -> None:
        self._make_helpers()
        configs = self._build_scrape_configs()
        event.set_results({"scrape-configs": json.dumps(configs, indent=2)})

    # ------------------------------------------------------------------
    # Scrape config builder (called by COSAgentProvider)
    # ------------------------------------------------------------------

    def _build_scrape_configs(self) -> list[dict]:
        if self._ceph is None or self._ovn is None:
            self._make_helpers()

        configs: list[dict] = []
        cluster = self._cluster_label()
        member = self._member_label()
        interval = self.config.get("scrape-interval", "30s")

        # ---- LXD ----
        configs.append(
            {
                "job_name": "microcloud-lxd",
                "scrape_interval": interval,
                "metrics_path": "/1.0/metrics",
                "scheme": "https",
                "tls_config": {
                    "insecure_skip_verify": True,
                },
                "static_configs": [
                    {
                        "targets": [_LXD_METRICS_ADDRESS],
                        "labels": {
                            "microcloud_service": "lxd",
                            "microcloud_member": member,
                            "microcloud_cluster": cluster,
                        },
                    }
                ],
            }
        )

        # ---- MicroCeph ----
        if snap.is_installed("microceph") and self._ceph.is_mgr_active():
            ceph_port = self.config.get("ceph-mgr-prometheus-port", 9283)
            ceph_target = f"127.0.0.1:{ceph_port}"
            configs.append(
                {
                    "job_name": "microcloud-microceph",
                    "scrape_interval": interval,
                    "metrics_path": "/metrics",
                    # The Ceph mgr exporter tags per-host metrics (e.g.
                    # ceph_disk_occupation) with their own "instance" label
                    # identifying the owning host. honor_labels keeps that
                    # label as-is instead of renaming it to
                    # "exported_instance" and overwriting "instance" with the
                    # scrape target address, which is identical
                    # (127.0.0.1:<port>) on every unit and would collapse
                    # per-host panels/variables in the bundled dashboards.
                    "honor_labels": True,
                    "static_configs": [
                        {
                            "targets": [ceph_target],
                            "labels": {
                                "microcloud_service": "microceph",
                                "microcloud_member": member,
                                "microcloud_cluster": cluster,
                            },
                        }
                    ],
                    "metric_relabel_configs": [
                        {
                            # Metrics without their own per-host "instance"
                            # label (e.g. cluster-wide summaries) still fall
                            # back to the scrape target address, which is
                            # meaningless and identical across units.
                            # Replace it with this unit's member name so it
                            # stays unique and matches microcloud_member.
                            "source_labels": ["instance"],
                            "regex": re.escape(ceph_target),
                            "target_label": "instance",
                            "action": "replace",
                            "replacement": member,
                        },
                    ],
                }
            )

        # ---- MicroOVN ----
        if snap.is_installed("microovn"):
            ovn_port = self.config.get("ovn-exporter-listen-port", 9310)
            configs.append(
                {
                    "job_name": "microcloud-microovn",
                    "scrape_interval": interval,
                    "metrics_path": "/metrics",
                    "static_configs": [
                        {
                            "targets": [f"127.0.0.1:{ovn_port}"],
                            "labels": {
                                "microcloud_service": "microovn",
                                "microcloud_member": member,
                                "microcloud_cluster": cluster,
                            },
                        }
                    ],
                }
            )

        return configs

    # ------------------------------------------------------------------
    # Label / address helpers
    # ------------------------------------------------------------------

    def _cluster_label(self) -> str:
        return self.app.name

    def _member_label(self) -> str:
        try:
            result = subprocess.run(
                ["lxc", "query", "/1.0/cluster/members"],
                capture_output=True,
                text=True,
                check=True,
                timeout=5,
            )
            members = json.loads(result.stdout)
            host = microcloud.hostname()
            for member_url in members:
                name = member_url.rstrip("/").split("/")[-1]
                if host.startswith(name) or name.startswith(host):
                    return name
        except Exception:  # noqa: BLE001
            pass
        return microcloud.hostname()


def _system_entries(systems: list[PeerSystem]) -> list[SystemEntry]:
    """Convert published peer identities into preseed system entries."""
    return [
        SystemEntry(
            name=system.name,
            address=system.address,
            ovn_uplink_interface=system.ovn_uplink_interface,
            ovn_underlay_ip=system.ovn_underlay_ip,
            storage_local_path=system.storage_local_path,
            storage_ceph_paths=system.storage_ceph_paths,
        )
        for system in systems
    ]


def _session_message(opened: JoinSession, initiator: str) -> str:
    """Describe a join session in progress for the leader's status."""
    # A session that does not list its initiator adds to an existing cluster.
    if initiator not in opened.systems:
        return f"Joining {len(opened.systems)} unit(s) to the MicroCloud cluster"
    return f"Forming the MicroCloud cluster with {len(opened.systems)} units"


def _last_line(output: str) -> str:
    """Return the last non-empty line of command output, for a status message."""
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    return lines[-1] if lines else "no output"


def _lxc_config_set(key: str, value: str) -> None:
    """Run `lxc config set <key> <value>`.  Raise LXDConfigError on failure."""
    try:
        subprocess.run(
            ["lxc", "config", "set", key, value],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        raise LXDConfigError(
            f"Cannot set LXD config {key}={value!r} (rc={exc.returncode}): {exc.stderr.strip()}"
        ) from exc


def _lxd_has_api_extension(name: str) -> bool:
    """Return True if the running LXD advertises the given API extension."""
    try:
        result = subprocess.run(
            ["lxc", "query", "/1.0"],
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        )
        info = json.loads(result.stdout)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, json.JSONDecodeError) as exc:
        logger.warning("Cannot query LXD API extensions: %s", exc)
        return False

    return name in info.get("api_extensions", [])


if __name__ == "__main__":
    ops.main(MicroCloudCharm)
