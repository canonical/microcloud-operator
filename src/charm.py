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

logging-relation-departed
    • Re-point LXD at a remaining endpoint, or disable streaming when none
      is left.

logging-relation-broken
    • Disable LXD's Loki streaming again.
"""

import json
import logging
import time
from typing import Any

import ops
from charms.grafana_agent.v0.cos_agent import COSAgentProvider
from charms.loki_k8s.v1.loki_push_api import LokiPushApiConsumer

import lxd_cluster
import microcloud
import network
import session
import snap
from cluster import ClusterCoordinator, PeerSystem, validate_membership
from control_plane import ControlPlane
from failure_domains import FailureDomains
from network import UnitNetwork
from observability import ALERT_RULES_DIR, DASHBOARD_DIRS, Observability

logger = logging.getLogger(__name__)


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
        self._control_plane = ControlPlane()
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
            self.on.logging_relation_broken, self._on_loki_push_api_endpoint_broken
        )

        # Actions
        self.framework.observe(self.on.status_action, self._on_status_action)
        self.framework.observe(self.on.dump_metrics_config_action, self._on_dump_metrics_config)
        self.framework.observe(self.on.add_control_plane_role_action, self._on_add_control_plane)
        self.framework.observe(
            self.on.remove_control_plane_role_action, self._on_remove_control_plane
        )

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

    def _on_loki_push_api_endpoint_departed(self, event: ops.RelationDepartedEvent) -> None:
        # The relation is global, so a subordinate collector on every machine
        # is a remote unit of every MicroCloud unit, and one leaving with its
        # machine must not stop streaming for the whole cluster. Juju already
        # leaves the departing unit out of the relation here.
        endpoints = self._loki_consumer.loki_endpoints
        if endpoints:
            self._observability.ensure_loki(endpoints)
        else:
            self._observability.teardown_loki()

    def _on_loki_push_api_endpoint_broken(self, event: ops.RelationBrokenEvent) -> None:
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

        grace = int(self.config.get("session-timeout", 300)) + session.SESSION_GRACE
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

        See :class:`session.JoinSessions` for the session lifecycle.

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
            return self._sessions.lead(initialized, pending, passphrase or "")
        return self._sessions.join(passphrase or "")

    def _storage_local_path(self) -> tuple[str, str | None]:
        """Return this unit's local storage device path.

        An attached "local" Juju storage volume wins, and on MAAS this can be a
        tagged partition from a ``partition,<tag>`` storage pool. When
        "local-device" is also set, it is ignored and an INFO record is logged.
        Otherwise, "local-device" names the device for clouds where Juju cannot
        attach the device (clouds without partition pools, pre-placed machines,
        or when using ``--to``).

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

    def _on_add_control_plane(self, event: ops.ActionEvent) -> None:
        self._control_plane.on_add_action(event)

    def _on_remove_control_plane(self, event: ops.ActionEvent) -> None:
        self._control_plane.on_remove_action(event)


if __name__ == "__main__":
    ops.main(MicroCloudCharm)
