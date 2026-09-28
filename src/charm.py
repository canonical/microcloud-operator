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
import time
import uuid
from typing import Any

import ops
from charms.grafana_agent.v0.cos_agent import COSAgentProvider
from charms.loki_k8s.v1.loki_push_api import LokiPushApiConsumer

import lxd_cluster
import microcloud
import network
import session
import snap
from cluster import ClusterCoordinator, JoinSession, PeerSystem, validate_membership
from failure_domains import FailureDomains
from network import UnitNetwork
from observability import ALERT_RULES_DIR, DASHBOARD_DIRS, Observability
from preseed import render

logger = logging.getLogger(__name__)

# How long past its session timeout a join session is assumed to have ended,
# for a new leader deciding whether it may open another.
_SESSION_GRACE = 120

# How many join sessions in a row may fail before the leader stops opening
# the next one straight away and blocks, retrying only on later hooks.
_SESSION_RETRIES = 3


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
        self._stored.set_default(
            session_failures=0, worker_pid=0, worker_session="", lxd_join_since=0.0
        )

        self._coordinator = ClusterCoordinator(self)
        self._failure_domains = FailureDomains()
        self._network = UnitNetwork(self.model, self.config)
        self._sessions = session.JoinSessions(
            self.unit,
            self._coordinator,
            self._network,
            self.config,
            self.charm_dir,
            self._stored,
            self._hold_status,
        )
        self._observability = Observability(self.config, self.app.name)

        # Set once the deploy path reports a transitional status of its own,
        # so the generic status at the end of reconciliation does not
        # overwrite it.
        self._status_held = False

        self._cos_agent = COSAgentProvider(
            self,
            scrape_configs=self._observability.scrape_configs,
            dashboard_dirs=DASHBOARD_DIRS,
            metrics_rules_dir=ALERT_RULES_DIR,
            log_slots=self._observability.log_slots(),
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
    # Mode detection
    # ------------------------------------------------------------------

    def _cos_related(self) -> bool:
        return bool(self.model.relations.get("cos-agent"))

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
            self._observability.teardown(full_cleanup=False)

    def _on_remove(self, event: ops.RemoveEvent) -> None:
        # Never destroy the MicroCloud cluster itself; only clean up
        # observability artifacts this charm created.
        self._observability.teardown(full_cleanup=True)

    # ------------------------------------------------------------------
    # cos-agent handlers
    # ------------------------------------------------------------------

    def _on_cos_agent_relation_joined(self, event: ops.RelationEvent) -> None:
        self._reconcile()

    def _on_cos_agent_relation_broken(self, event: ops.RelationBrokenEvent) -> None:
        self._observability.teardown(full_cleanup=True)
        self._reconcile()

    # ------------------------------------------------------------------
    # logging (Loki) handlers
    # ------------------------------------------------------------------

    def _on_loki_push_api_endpoint_joined(self, event: ops.EventBase) -> None:
        self._observability.ensure_loki(self._loki_consumer.loki_endpoints)

    def _on_loki_push_api_endpoint_departed(self, event: ops.RelationEvent) -> None:
        self._observability.teardown_loki()

    # ------------------------------------------------------------------
    # Reconciliation
    # ------------------------------------------------------------------

    def _reconcile(self) -> None:
        """Drive deployment or observe-only mode, then set status."""
        ovn_uplink_interface, problem = self._network.ovn_uplink_interface()
        if problem:
            self.unit.status = ops.BlockedStatus(problem)
            return

        storage_local_path, problem = self._storage_local_path()
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
            storage_local_path=storage_local_path,
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
            pending = self._sessions.pending(initialized)
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

        # Only a unit that was clustered when this hook started, and only once
        # its LXD has joined too. MicroCloud reports a joining unit as clustered
        # as soon as it has joined MicroCloud itself, before the initiator adds
        # its LXD. The leader clearing its join session wakes it up again.
        if not problem and initialized:
            try:
                lxd_clustered = lxd_cluster.is_clustered()
            except lxd_cluster.LXDClusterError as exc:
                problem = f"Cannot read the LXD cluster: {exc}"
            else:
                if lxd_clustered:
                    self._stored.lxd_join_since = 0.0
                    problem = self._failure_domains.reconcile()
                else:
                    problem = self._await_lxd_join()

        if problem:
            self.unit.status = ops.BlockedStatus(problem)
            return

        if self._cos_related():
            obs_problem = self._observability.reconcile()
            if obs_problem:
                self.unit.status = ops.BlockedStatus(obs_problem)
                return
            self._cos_agent_refresh()

        if not self._status_held:
            self._set_status(initialized=microcloud.is_initialized())

    def _await_lxd_join(self) -> str | None:
        """Wait for this unit's LXD to join, or report a bootstrap that stopped half way.

        MicroCloud reports a unit clustered once it has joined MicroCloud
        itself, a little before the initiator has added its LXD, so a short
        wait here is normal.

        A bootstrap that fails between those two points never recovers,
        though: the charm sees an initialized MicroCloud, so it treats every
        later session as growth, and growth needs the LXD that is missing.
        MicroCloud then rejects each session with "LXD is not initialized",
        which names neither the service that failed nor the real state. Say
        what actually happened instead of waiting forever.

        Returns a problem string to block on, or None while still waiting.
        """
        now = time.time()

        # A published session is the initiator still working, and LXD joins at
        # the end of it. Keep restarting the clock while one is open so the
        # grace below is measured from the session ending, not from the first
        # hook that noticed LXD missing.
        if self._coordinator.session() is not None:
            self._stored.lxd_join_since = now
            self._hold_status(ops.WaitingStatus("Waiting for LXD to join the cluster"))
            return None

        if not self._stored.lxd_join_since:
            self._stored.lxd_join_since = now
        waiting_for = now - float(self._stored.lxd_join_since)

        grace = int(self.config.get("session-timeout", 300)) + _SESSION_GRACE
        if waiting_for < grace:
            self._hold_status(ops.WaitingStatus("Waiting for LXD to join the cluster"))
            return None

        return (
            f"MicroCloud is initialized but LXD has not joined after {int(waiting_for)}s: "
            "the bootstrap stopped part way and the charm cannot finish it. The last error is "
            f"in {session.STATE_DIR / 'join-session.log'} on this unit."
        )

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

        # Only where the uplink is actually used: with MicroOVN disabled the
        # interface name never reaches the preseed, and a stale value left in
        # the config must not stop the cluster forming.
        if self.config.get("snap-channel-microovn", ""):
            interface, _ = self._network.ovn_uplink_interface()
            problem = self._network.missing_uplink_interface(interface)
            if problem:
                return problem

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
        return self._sessions.join(passphrase or "")

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
            if outcome is None and self._sessions._worker_running(current.id):
                self._hold_status(
                    ops.MaintenanceStatus(_session_message(current, microcloud.hostname()))
                )
                return None

            self._coordinator.publish_session(None)
            session.clear()
            if outcome is not None and outcome[0] != 0:
                logger.error("Join session %s failed:\n%s", current.id, outcome[1])
                failure = session._last_line(outcome[1])
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
                pending = self._sessions.pending(initialized)
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

        document = render(
            self._sessions._preseed_inputs(address, passphrase, session._system_entries(listed))
        )
        try:
            self._sessions._start_worker(opened.id, document)
        except session.SessionError as exc:
            return str(exc)

        self._coordinator.publish_session(opened)
        message = _session_message(opened, microcloud.hostname())
        if failure:
            message = f"{message}; retrying after: {failure}"
        self._hold_status(ops.MaintenanceStatus(message))
        return None

    def _storage_local_path(self) -> tuple[str, str | None]:
        """Return this unit's local storage device path.

        An attached "local" Juju storage volume wins. Otherwise the
        "local-device" config names the device, which is how local storage
        Juju cannot attach is reached. A MAAS storage pool selects by tag,
        and MAAS matches that tag against whole block devices when it
        allocates a machine, so a tag on a partition, a RAID or an LVM
        volume matches no machine at all: the unit never gets placed and
        the storage stays pending forever.

        The "local" storage volume is declared with ``multiple: range: 0-1``
        in charmcraft.yaml, so at most one instance is ever attached.

        Returns (path, problem). "path" is "" when this unit has no local
        storage at all, which is allowed.
        """
        storages = self.model.storages["local"]
        if storages:
            attached = str(storages[0].location)
            if str(self.config.get("local-device", "")).strip():
                logger.info("Ignoring local-device: %s is attached as Juju storage", attached)
            return attached, None

        return network.per_host_value(str(self.config.get("local-device", "")), "local-device")

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

    def _cos_agent_refresh(self) -> None:
        self._cos_agent._on_refresh(None)

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def _set_status(self, *, initialized: bool) -> None:
        if not initialized:
            self.unit.status = ops.WaitingStatus("Waiting for cluster to form")
            return

        if self._cos_related():
            problems = self._observability.health_problems()
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
                result["members"] = lxd_cluster.render_table(lxd_cluster.members())
            except lxd_cluster.LXDClusterError as exc:
                result["members-error"] = str(exc)
            try:
                members = microcloud.list_members()
                result["microcloud-members"] = json.dumps(
                    [{"name": m.name, "address": m.address, "status": m.status} for m in members]
                )
            except microcloud.MicroCloudError as exc:
                result["microcloud-members-error"] = str(exc)
        event.set_results(result)

    def _on_dump_metrics_config(self, event: ops.ActionEvent) -> None:
        configs = self._observability.scrape_configs()
        event.set_results({"scrape-configs": json.dumps(configs, indent=2)})


def _session_message(opened: JoinSession, initiator: str) -> str:
    """Describe a join session in progress for the leader's status."""
    # A session that does not list its initiator adds to an existing cluster.
    if initiator not in opened.systems:
        return f"Joining {len(opened.systems)} unit(s) to the MicroCloud cluster"
    return f"Forming the MicroCloud cluster with {len(opened.systems)} units"


if __name__ == "__main__":
    ops.main(MicroCloudCharm)
