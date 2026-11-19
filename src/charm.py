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

Orchestrator and member applications
------------------------------------
    • Each hardware profile, such as an architecture, is its own application.
      The one with ``orchestrator=true`` forms the cluster as above; every
      other application relates its "orchestrator" endpoint to the
      orchestrator's "members" endpoint (see ``cluster_relation``).
    • A unit that joins the orchestrator application after the cluster has
      formed installs the channels the cluster runs, not its config.
    • A member unit installs the channels the cluster runs, waits for the
      cluster to exist, and is joined to it in a grow session the
      orchestrator leader opens for its application's units.

Observe-only mode (MicroCloud already initialized out-of-band)
--------------------------------------------------------------
    • This exists to integrate an existing cluster with COS. It is not a way
      to deploy one or to bring one under the charm's management.
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
import platform
import time
from typing import Any

import ops
from charms.grafana_agent.v0.cos_agent import COSAgentProvider
from charms.loki_k8s.v1.loki_push_api import LokiPushApiConsumer

import bgp
import loopback
import lxd_cluster
import microcloud
import session
import snap
import steering
import storage_health
from cluster import (
    MODE_MEMBER,
    MODE_ORCHESTRATOR,
    ClusterCoordinator,
    JoinSession,
    PeerSystem,
    identity_fields,
    system_from_data,
    validate_membership,
)
from cluster_relation import ClusterRelation, OrchestratorFacts
from control_plane import ControlPlane
from failure_domains import FailureDomains
from network import UnitNetwork
from observability import ALERT_RULES_DIR, DASHBOARD_DIRS, Observability

logger = logging.getLogger(__name__)

# Kernel machine names (uname -m) mapped to Debian/snap architecture names.
_MACHINE_TO_ARCH = {
    "x86_64": "amd64",
    "aarch64": "arm64",
    "armv7l": "armhf",
    "ppc64le": "ppc64el",
}


def host_architecture() -> str:
    """Return this machine's architecture as a Debian name, e.g. "amd64"."""
    machine = platform.machine()
    return _MACHINE_TO_ARCH.get(machine, machine)


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
        self._relations = ClusterRelation(self)
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
            self._relations,
        )
        self._observability = Observability(self.config, self.app.name)

        # Set once the deploy path reports a transitional status of its own,
        # so the generic status at the end of reconciliation does not
        # overwrite it.
        self._status_held = False
        # Added to the active status when this host deploys without NUMA steering.
        self._steering_note = ""

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
        self.framework.observe(self.on.leader_elected, self._on_cluster_relation_changed)
        for endpoint in ("members", "orchestrator"):
            relation_events = self.on[endpoint]
            for event in (
                relation_events.relation_created,
                relation_events.relation_joined,
                relation_events.relation_changed,
                relation_events.relation_departed,
                relation_events.relation_broken,
            ):
                self.framework.observe(event, self._on_cluster_relation_changed)
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

    def _orchestrator_option(self) -> bool:
        """Return the "orchestrator" option as set, which may not be in effect."""
        return bool(self.config.get("orchestrator", False))

    def _is_orchestrator(self) -> bool:
        """Return whether this application is the orchestrator.

        Once the leader has recorded a mode, the record decides it, not the
        option. The record follows the option only until the cluster forms
        (see _check_cluster_mode), so changing the option afterwards has no
        effect beyond blocking.
        """
        recorded = self._coordinator.cluster_mode()
        if recorded:
            return recorded == MODE_ORCHESTRATOR
        return self._orchestrator_option()

    # ------------------------------------------------------------------
    # Core hook handlers
    # ------------------------------------------------------------------

    def _on_install(self, event: ops.InstallEvent | ops.UpgradeCharmEvent) -> None:
        self._reconcile()

    def _on_config_changed(self, event: ops.ConfigChangedEvent) -> None:
        self._reconcile()

    def _on_cluster_relation_changed(self, event: ops.EventBase) -> None:
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
        initialized = microcloud.is_initialized()

        # Before anything that can block this unit: the mode belongs to the
        # application, and its other units wait for the leader to record it.
        mode_problem = self._check_cluster_mode(initialized)

        ovn_uplink_interface, problem = self._network.ovn_uplink_interface()
        if problem:
            self.unit.status = ops.BlockedStatus(problem)
            return

        problem = self._network.network_options_problem()
        if problem:
            self.unit.status = ops.BlockedStatus(problem)
            return

        _, problem = steering.numa_networks(self.config)
        if problem:
            self.unit.status = ops.BlockedStatus(problem)
            return

        # Before anything names these addresses: MicroCloud looks for them on
        # the host when it reads the preseed.
        derived = self._network.derived_addresses()
        problem = loopback.ensure(derived)
        if problem:
            self.unit.status = ops.BlockedStatus(problem)
            return

        # On every hook once the snap is in: keeps BIRD alone and up to date.
        if bgp.installed():
            problem = self._ensure_bgp()
            if problem:
                self.unit.status = ops.BlockedStatus(problem)
                return

        storage_ceph_paths, problem = self._storage_ceph_paths()
        if problem:
            self.unit.status = ops.BlockedStatus(problem)
            return

        # Publish our identity for peers as early as possible.
        system = {
            "name": microcloud.hostname(),
            "address": self._network.bind_address(),
            "ovn_uplink_interface": ovn_uplink_interface,
            "ovn_underlay_ip": self._network.space_address("ovn-underlay"),
            "storage_local_path": self._storage_local_path(),
            "storage_ceph_paths": storage_ceph_paths,
            "storage_wipe": bool(self.config.get("storage-wipe", True)),
            "storage_encrypt": bool(self.config.get("storage-encrypt", False)),
            "loopback_addresses": {
                str(network): str(address) for network, address in derived.items()
            },
            "initialized": initialized,
        }
        self._coordinator.publish_identity(**system)

        if mode_problem:
            self.unit.status = ops.BlockedStatus(mode_problem)
            return

        # Set by the orchestrator's leader while systems wait to join.
        self._join_pending = False
        if self._is_orchestrator():
            if not self._reconcile_orchestrator(initialized):
                return
        else:
            problem = self._reconcile_member(initialized, system)
            if problem:
                self.unit.status = ops.BlockedStatus(problem)
                return

        self._reconcile_clustered(initialized)

    def _reconcile_orchestrator(self, initialized: bool) -> bool:
        """Form or grow the cluster, or observe it.

        Returns False once it has set a blocked status of its own.
        """
        session_open = self._coordinator.session() is not None
        if self.unit.is_leader():
            if initialized:
                self._record_cluster_channels()
            self._publish_to_members(initialized)

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
            self._join_pending = bool(pending)
            # Member units are grown into the cluster by its clustered leader.
            members_waiting = (
                self.unit.is_leader() and initialized and self._sessions.has_member_candidates()
            )
        except microcloud.MicroCloudError as exc:
            self.unit.status = ops.BlockedStatus(f"Cannot read MicroCloud members: {exc}")
            return False

        # The leader also stays on the deploy path while its join session is
        # published, to pick up the result and clear it.
        initiating = self.unit.is_leader() and (
            bool(pending) or members_waiting or self._coordinator.session() is not None
        )

        if initialized and not initiating:
            problem = self._reconcile_observe_only()
        else:
            problem = self._reconcile_deploy(initialized, pending)

        # Nothing else wakes a leader with no peers once its session has ended,
        # and member units are waiting for the storage pools it held back.
        if self.unit.is_leader() and session_open and self._coordinator.session() is None:
            # Unless that session formed the cluster, everything else was
            # published at the start of this hook.
            self._publish_to_members(microcloud.is_initialized(), held_back=initialized)

        if problem:
            self.unit.status = ops.BlockedStatus(problem)
            return False
        return True

    # ---- Orchestrator and member applications ----

    def _snap_channels(self) -> dict[str, str]:
        return snap.channels(self.config)

    def _check_cluster_mode(self, initialized: bool) -> str | None:
        """Pin which side of "microcloud_cluster" this application is on.

        The leader records the mode until the cluster forms, after which it
        cannot change: a formed cluster cannot move between applications.
        Also rejects relations on the endpoint that does not match the
        option. Returns a problem string to block on, or None.
        """
        orchestrator = self._orchestrator_option()
        mode = MODE_ORCHESTRATOR if orchestrator else MODE_MEMBER

        recorded = self._coordinator.cluster_mode()
        formed = initialized or self._coordinator.any_initialized()
        if formed and recorded and recorded != mode:
            # The other side keeps seeing the role this application formed as.
            self._relations.publish_role(recorded == MODE_ORCHESTRATOR)
            return f"orchestrator cannot change after the cluster formed (formed as {recorded})"
        # A cluster with no mode recorded formed before this charm recorded
        # one, or outside Juju. It did not join as a member, which takes the
        # relation and a recorded mode, so the option's default must not pin
        # it as one: that could never be changed to orchestrator=true.
        unpinned = (
            formed
            and not recorded
            and not orchestrator
            and self._relations.orchestrator_relation() is None
        )
        if self.unit.is_leader() and recorded != mode and not unpinned:
            self._coordinator.record_cluster_mode(mode)

        # From the record when there is one: on a unit that is not the leader
        # it can lag behind the option until the leader records it.
        orchestrator = self._is_orchestrator()
        self._relations.publish_role(orchestrator)
        if orchestrator:
            if self._relations.orchestrator_relation() is not None:
                return "orchestrator endpoint needs orchestrator=false"
            members = self._relations.members_relations()
            for relation in members:
                if self._relations.remote_role(relation) == "true" and relation.app is not None:
                    return f"{relation.app.name} on members has orchestrator=true"
            if members and not snap.channel(self.config, "microceph"):
                return "members relation needs MicroCeph (snap-channel-microceph)"
        else:
            if self._relations.members_relations():
                return "members endpoint needs orchestrator=true"
            relation = self._relations.orchestrator_relation()
            if (
                relation is not None
                and relation.app is not None
                and self._relations.remote_role(relation) == "false"
            ):
                return f"{relation.app.name} on orchestrator has orchestrator=false"
        return None

    def _publish_to_members(self, initialized: bool, *, held_back: bool = False) -> None:
        """Leader publishes cluster facts on every "members" relation.

        With ``held_back``, only what an open session kept from being
        published: this hook has already published the rest.
        """
        relations = self._relations.members_relations()
        if relations:
            self._publish_facts(relations, initialized, held_back)
        # Also once the last relation is gone: a detached member's entry has to
        # go when its MicroCloud member is removed.
        if initialized and (relations or self._coordinator.joined_units()):
            self._sync_joined_units(relations)

    def _publish_facts(
        self, relations: list[ops.Relation], initialized: bool, held_back: bool = False
    ) -> None:
        """Publish what member units need to join on each of ``relations``.

        With ``held_back``, the channels, versions and secret stay as this
        hook already published them, which saves reading them twice.
        """
        # A leader that has not joined an existing cluster yet only knows what
        # it would run itself, so what a clustered leader published stands.
        cluster_exists = initialized or self._coordinator.any_initialized()
        channels = None
        versions = None
        if not held_back and (initialized or not cluster_exists):
            channels = self._cluster_channels(initialized)
        if channels is not None:
            names = [name for name, channel in channels.items() if channel]
            own_versions = snap.service_versions(names)
            # Publish only a full set, so members never match a partial one.
            if set(own_versions) == set(names):
                versions = own_versions

        fingerprint: str | None = None
        storage: list[str] | None = None
        if initialized:
            try:
                if lxd_cluster.is_clustered():
                    fingerprint = lxd_cluster.certificate_fingerprint()
                    # LXD clusters before the bootstrap session creates the
                    # pools, and member units take what is published as final.
                    if self._coordinator.session() is None:
                        storage = lxd_cluster.storage_pool_kinds()
            except lxd_cluster.LXDClusterError as exc:
                logger.warning("Cannot read the cluster facts for member units: %s", exc)

        secret_ids = {} if held_back else self._coordinator.grant_passphrase(relations)
        for relation in relations:
            self._relations.publish_facts(
                relation,
                snap_channels=channels,
                service_versions=versions,
                initialized=cluster_exists,
                fingerprint=fingerprint,
                storage=storage,
                secret_id=secret_ids.get(relation.id),
                session_timeout=int(self.config.get("session-timeout", 300)),
            )

    def _record_cluster_channels(self) -> None:
        """Record the channels the cluster runs, for this application's units that join later."""
        channels = self._cluster_channels(True)
        if channels is not None:
            self._coordinator.record_snap_channels(channels)

    def _install_channels(self) -> dict[str, str] | None:
        """Return the snap channels this unit installs before it joins, or None to wait.

        These are the configured channels until the cluster forms, then the
        ones its leader recorded. A clustered unit does not follow config
        changes, so a unit that joins later installs what the cluster runs,
        as member units do.
        """
        if not self._coordinator.any_initialized():
            return self._snap_channels()
        return self._coordinator.snap_channels()

    def _cluster_channels(self, initialized: bool) -> dict[str, str] | None:
        """Return the snap channels member units are to install, or None if unreadable.

        These are the configured channels until the cluster forms, then the
        ones this unit's snaps track: a clustered unit does not follow config
        changes, so the config can differ from what the cluster runs. A snap
        that tracks no channel keeps the configured one, and a snap that is not
        installed is not part of the cluster.
        """
        configured = self._snap_channels()
        if not initialized:
            return configured
        try:
            tracked = snap.tracked_channels()
        except snap.SnapError as exc:
            logger.warning("Cannot read the tracked snap channels: %s", exc)
            return None
        return {
            name: (tracked[name] or channel) if name in tracked else ""
            for name, channel in configured.items()
        }

    def _sync_joined_units(self, relations: list[ops.Relation]) -> None:
        """Record the member units that have joined the cluster.

        A unit is dropped once its application lists it no longer, but kept
        when the whole relation is gone, so its MicroCloud member stays
        known as that of a detached member application.
        """
        try:
            members = self._sessions.member_names()
        except microcloud.MicroCloudError as exc:
            logger.warning("Cannot read MicroCloud members: %s", exc)
            return

        joined = {
            name: unit
            for name, unit in self._coordinator.joined_units().items()
            if name in members
        }
        for relation in relations:
            for member in self._relations.member_systems(relation):
                if member.system.name in members:
                    joined[member.system.name] = member.unit
            units = self._relations.member_units(relation)
            if units is None or relation.app is None:
                continue
            prefix = f"{relation.app.name}/"
            joined = {
                name: unit
                for name, unit in joined.items()
                if not unit.startswith(prefix) or unit in units
            }
        if joined != self._coordinator.joined_units():
            self._coordinator.record_joined_units(joined)

    def _reconcile_member(self, initialized: bool, system: dict[str, Any]) -> str | None:
        """Join this member unit to the orchestrator's cluster, or observe it.

        Returns a problem string to block on, or None.
        """
        identity = identity_fields(**system)
        relation = self._relations.orchestrator_relation()
        self._relations.publish_member(identity)
        if self.unit.is_leader() and relation is not None:
            peer = self._coordinator.relation
            units = [self.unit.name] + ([unit.name for unit in peer.units] if peer else [])
            self._relations.publish_units(units)

        if initialized:
            return self._reconcile_member_clustered()

        # A worker only runs for the session the orchestrator has published,
        # whatever keeps this unit from joining below: no relation, a failed
        # prerequisite, an exclusion, or a session that does not list it.
        facts = self._relations.facts()
        self._sessions.stand_down(facts.session if facts is not None else None)

        if relation is None:
            self._hold_status(
                ops.WaitingStatus(
                    "Waiting for orchestrator relation (set orchestrator=true to start a cluster)"
                )
            )
            return None

        # The orchestrator cannot list a unit that has no address.
        own = system_from_data(identity)
        if own is None:
            self._relations.publish_member({}, ready=False)
            self._hold_status(ops.WaitingStatus("Waiting for an address on the cluster binding"))
            return None

        problem, passphrase = self._member_prerequisites(facts, system)
        # Withdrawn whenever a prerequisite fails, so the orchestrator never
        # lists a unit that would not dial in.
        self._relations.publish_member({}, ready=passphrase is not None)
        if facts is None or passphrase is None:
            return problem

        # Checked after publishing ready: the orchestrator's failure count
        # resets once a unit is added or changed, and it needs ready to see it.
        if facts.failures.count >= session.SESSION_RETRIES:
            return (
                f"excluded after {facts.failures.count} failed join sessions: "
                f"{facts.failures.error}"
            )

        return self._sessions.join_member(passphrase, facts, own)

    def _member_prerequisites(
        self, facts: OrchestratorFacts | None, system: dict[str, Any]
    ) -> tuple[str | None, str | None]:
        """Check what an unclustered member unit needs before it can join.

        Returns ``(problem, passphrase)``. The passphrase is set only when
        the unit is ready to be listed in a session; otherwise the problem is
        the block message, or None when a waiting status was held.
        """
        if facts is None or facts.snap_channels is None:
            self._hold_status(ops.WaitingStatus("Waiting for the orchestrator"))
            return None, None
        channels = facts.snap_channels

        # The channels the cluster runs, not this application's own: MicroCloud
        # only joins systems whose services report the same versions.
        try:
            snap.ensure_installed(channels)
        except snap.SnapError as exc:
            return f"Snap install: {exc}", None

        if not microcloud.waitready(timeout=60):
            self._hold_status(ops.WaitingStatus("Waiting for microcloud daemon"))
            return None, None

        problem, waiting = self._match_service_versions(channels, facts.service_versions)
        if problem or waiting:
            return problem, None

        problem = self._member_storage_problem(system, facts.storage)
        if problem:
            return problem, None

        if channels.get("microovn"):
            interface = str(system.get("ovn_uplink_interface", ""))
            problem = self._network.missing_uplink_interface(interface)
            if problem:
                return problem, None

        if not facts.initialized:
            self._hold_status(ops.WaitingStatus("Waiting for the orchestrator to bootstrap"))
            return None, None

        # LXD clusters shortly after MicroCloud initializes; the storage
        # guards above need its pools.
        if facts.storage is None:
            self._hold_status(ops.WaitingStatus("Waiting for the cluster's storage pools"))
            return None, None

        passphrase = self._relations.passphrase(facts)
        if not passphrase:
            self._hold_status(ops.WaitingStatus("Waiting for session passphrase"))
            return None, None

        # Joining pins this application as a member, so the leader must have
        # recorded that first.
        if self._coordinator.cluster_mode() != MODE_MEMBER:
            self._hold_status(
                ops.WaitingStatus("Waiting for the leader to record the cluster mode")
            )
            return None, None

        return None, passphrase

    def _match_service_versions(
        self, channels: dict[str, str], wanted: dict[str, str]
    ) -> tuple[str | None, bool]:
        """Publish this unit's service versions, refreshing snaps that differ.

        Returns ``(problem, waiting)``: a problem string while a version still
        differs, or waiting True, with a waiting status held, while a daemon
        has not reported its version yet.
        """
        names = [name for name, channel in channels.items() if channel]
        own = snap.service_versions(names)
        mismatches = snap.version_mismatches(own, wanted) if wanted else {}
        # Only a version that was read and differs calls for a refresh. A
        # daemon that has not answered yet, as happens right after its snap
        # is installed, is waited for below instead.
        refresh = [name for name in mismatches if name in own and channels.get(name)]
        if refresh:
            try:
                for name in refresh:
                    snap.install(name, channels[name])
                    snap.hold(name)
            except snap.SnapError as exc:
                return f"Snap refresh: {exc}", False
            own = snap.service_versions(names)
            mismatches = snap.version_mismatches(own, wanted)
        self._relations.publish_member({}, service_versions=own)

        missing = [name for name in names if name not in own]
        if missing:
            self._hold_status(
                ops.WaitingStatus(
                    f"Waiting for the {', '.join(missing)} daemon(s) to report a version"
                )
            )
            return None, True
        if mismatches:
            differing = ", ".join(
                f"{name} {version} (orchestrator {wanted[name]})"
                for name, version in mismatches.items()
            )
            return f"service versions differ from the orchestrator: {differing}", False
        return None, False

    @staticmethod
    def _member_storage_problem(system: dict[str, Any], storage: list[str] | None) -> str | None:
        """Check this member unit's storage against the cluster's pools."""
        if storage is None:
            return None
        if system.get("storage_ceph_paths") and not {"ceph", "cephfs"} & set(storage):
            return "ceph storage attached but the cluster has no Ceph pools"
        local_path = system.get("storage_local_path")
        if "local" in storage and not local_path:
            return "the cluster has a local pool; attach local storage"
        if local_path and "local" not in storage:
            return "local storage attached but the cluster has no local pool"
        return None

    def _reconcile_member_clustered(self) -> str | None:
        """Check a clustered member unit against the orchestrator's cluster."""
        facts = self._relations.facts()
        if facts is not None:
            fingerprint = ""
            try:
                if lxd_cluster.is_clustered():
                    fingerprint = lxd_cluster.certificate_fingerprint()
            except lxd_cluster.LXDClusterError as exc:
                logger.warning("Cannot read the cluster certificate: %s", exc)
            if fingerprint:
                self._relations.publish_member({}, fingerprint=fingerprint)
                if facts.fingerprint and fingerprint != facts.fingerprint:
                    return "already a member of another MicroCloud"

        # The orchestrator leader validates the whole cluster. This
        # application's leader only checks that its own clustered units are
        # members.
        if not self.unit.is_leader():
            return None
        try:
            members = {member.name for member in microcloud.list_members()}
        except microcloud.MicroCloudError as exc:
            return f"Cannot read MicroCloud members: {exc}"
        clustered = {
            system.name for system in self._coordinator.peer_systems() if system.initialized
        }
        problems = validate_membership(clustered, members, known=members)
        return "; ".join(problems) or None

    def _open_session(self) -> JoinSession | None:
        """Return the join session this unit is joining through, if any."""
        if self._is_orchestrator():
            return self._coordinator.session()
        facts = self._relations.facts()
        return facts.session if facts is not None else None

    def _reconcile_clustered(self, initialized: bool) -> None:
        """Finish a clustered unit's setup, then set status."""
        problem = None
        # Only a unit that was clustered when this hook started, and only once
        # its LXD has joined too. MicroCloud reports a joining unit as clustered
        # as soon as it has joined MicroCloud itself, before the initiator adds
        # its LXD, and LXD reads as clustered before its own join has ended. The
        # leader clearing its join session wakes it up again.
        if initialized:
            try:
                lxd_clustered = lxd_cluster.is_clustered()
            except lxd_cluster.LXDClusterError as exc:
                problem = f"Cannot read the LXD cluster: {exc}"
            else:
                if lxd_clustered:
                    self._stored.lxd_join_since = 0.0
                    current = self._open_session()
                    if current is None or microcloud.hostname() not in current.systems:
                        problem = self._failure_domains.reconcile()
                    # Not while a join is in flight: MicroCloud adds the disks
                    # and creates the pools after the systems have joined.
                    if not problem and not self._join_pending and current is None:
                        problem = self._storage_problem() or self._osd_steering_problem()
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
        if self._open_session() is not None:
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
        # A worker only runs for the published session, whatever keeps this
        # unit from leading or joining one below.
        self._sessions.stand_down(self._coordinator.session())

        # A clustered leader only gets here to run a join session. Its snaps
        # are already in place, and refreshing them now would upgrade this
        # one member ahead of the rest of the cluster.
        if not initialized:
            channels = self._install_channels()
            if channels is None:
                self._hold_status(ops.WaitingStatus("Waiting for the cluster's snap channels"))
                return None
            try:
                snap.ensure_snaps(channels)
            except snap.SnapError as exc:
                return f"Snap install: {exc}"

            # Before MicroCloud can see a running MicroOVN daemon.
            problem = self._ensure_bgp()
            if problem:
                return problem

        if not microcloud.waitready(timeout=60):
            self._hold_status(ops.WaitingStatus("Waiting for microcloud daemon"))
            return None

        # Only where the uplink is actually used: with MicroOVN disabled the
        # interface name never reaches the preseed, and a stale value left in
        # the config must not stop the cluster forming.
        if snap.channel(self.config, "microovn") and not self._bgp_uplinks():
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

        # A unit that is not clustered yet waits until its peers answer on
        # their derived addresses: until those are advertised both ways, a
        # join session would start Ceph or OVN on addresses nothing can reach.
        if not initialized and not self._coordinator.published_ready():
            waiting = self._unreachable_peer()
            if waiting:
                self._hold_status(ops.WaitingStatus(waiting))
                return None

        # Every unit has now cleared all its own prerequisites: signal that
        # it is ready to be listed in a join session.
        self._coordinator.publish_ready()

        if self.unit.is_leader():
            return self._sessions.lead(initialized, pending, passphrase or "")
        return self._sessions.join(passphrase or "")

    def _bgp_uplinks(self) -> list[str]:
        return bgp.uplinks(str(self.config.get("bgp-uplinks", "")))

    def _ensure_bgp(self) -> str | None:
        """Advertise this unit's derived addresses over BGP, if asked to.

        Returns a problem to block on, or None.
        """
        interfaces = self._bgp_uplinks()
        if not interfaces:
            return None
        if not snap.channel(self.config, "microovn"):
            return "bgp-uplinks needs snap-channel-microovn for BIRD"
        derived = self._network.derived_addresses()
        if not derived:
            return "bgp-uplinks needs a network option and an IPv6 cluster address"
        try:
            router_id, asn = bgp.identity(next(iter(derived.values())))
        except ValueError as exc:
            return str(exc)
        return bgp.ensure(bgp.render(router_id, asn, interfaces, derived.keys()))

    def _unreachable_peer(self) -> str | None:
        """Return which peer does not answer on a derived address yet, or None.

        Each peer is pinged on its address in every network this unit has a
        derived address in, from that address.
        """
        own = {
            str(network): str(address)
            for network, address in self._network.derived_addresses().items()
        }
        if not own:
            return None

        hostname = microcloud.hostname()
        # In name order, so that the peer named in the status does not change
        # from one hook to the next.
        for system in sorted(self._coordinator.peer_systems(), key=lambda peer: peer.name):
            if system.name == hostname:
                continue
            for cidr, target in sorted(system.loopback_addresses.items()):
                source = own.get(cidr)
                if source and not loopback.reachable(source, target):
                    return f"Waiting for {system.name} to answer on {target}"
        return None

    def _storage_local_path(self) -> str:
        """Return the device path of this unit's "local" Juju storage, or "".

        On MAAS this is a tagged partition or disk from a storage pool, for
        example a ``partition,<tag>`` pool. The "local" storage is declared
        with ``multiple: range: 0-1`` in charmcraft.yaml, so at most one
        instance is ever attached; with none, this system has no local
        storage, which is allowed.
        """
        storages = self.model.storages["local"]
        return str(storages[0].location) if storages else ""

    def _storage_ceph_paths(self) -> tuple[list[str], str | None]:
        """Return device paths of this unit's attached "ceph" Juju storage.

        The "ceph" storage volume is declared with ``multiple: range: 0-``
        in charmcraft.yaml, so any number of instances may be attached -
        each becomes a separate Ceph OSD on this system.

        OSDs may only run on the architectures listed in
        "ceph-osd-architectures". Units on other architectures still run
        MicroCeph without OSDs, which gives LXD the Ceph keyring.

        Returns (paths, problem).
        """
        paths = [str(storage.location) for storage in self.model.storages["ceph"]]
        if not paths:
            return [], None

        allowed = [
            arch.strip()
            for arch in str(self.config.get("ceph-osd-architectures", "amd64")).split(",")
            if arch.strip()
        ]
        arch = host_architecture()
        if arch not in allowed:
            return [], (
                f"Ceph storage attached but {arch} is not in ceph-osd-architectures "
                f"({','.join(allowed) or 'empty'}); detach it"
            )

        return paths, None

    def _osd_steering_problem(self) -> str | None:
        """Bind this unit's OSDs to their NUMA node's addresses, if asked to.

        The NUMA network maps name each node's /64; this host's steered
        address for a node is the one on "lo" inside it. Restarts each OSD
        whose options change, one at a time, and only while every other OSD
        is up. Returns a problem to block on, or None.
        """
        maps, problem = steering.numa_networks(self.config)
        if problem or not maps or not self.config.get("snap-channel-microceph", ""):
            return problem

        hostname = microcloud.hostname()
        unsteered = steering.Addresses(
            self._network.space_address("ceph-public"),
            self._network.space_address("ceph-internal"),
        )
        if not unsteered.public or not unsteered.cluster:
            # Not known yet, or neither an option nor a binding gives one.
            return (
                "NUMA steering needs the unsteered Ceph addresses: set ceph-public-network "
                "and ceph-internal-network, or bind ceph-public and ceph-internal"
            )

        try:
            steered, problem = steering.steered_addresses(
                maps, loopback.host_addresses().get("lo", set())
            )
            if problem:
                return problem
            if not steered:
                # The machine's deployment found no card per NUMA node.
                self._steering_note = f"NUMA steering disabled on {hostname}"

            disks = steering.local_disks(hostname)
            if not disks:
                return None
            wanted = steering.bindings(steered, unsteered, disks)

            dump = steering.config_dump()
            steering.ensure_bind_before_connect(dump)
            pending = steering.changes(
                wanted,
                steering.osd_options(dump, sorted(disks)),
                {osd: steering.runs_bind_before_connect(osd) for osd in disks},
            )
            for osd, change in pending.items():
                down = [other for other in steering.down_osds() if other != osd]
                if down:
                    self._hold_status(
                        ops.WaitingStatus(
                            f"Waiting for osd.{down[0]} to be up to restart osd.{osd}"
                        )
                    )
                    return None
                # Another unit may be restarting one of its OSDs at the same time.
                if not steering.ok_to_stop(osd):
                    self._hold_status(
                        ops.WaitingStatus(f"Waiting for Ceph to allow restarting osd.{osd}")
                    )
                    return None
                steering.apply(osd, change)
                steering.restart(osd)
                logger.info(
                    "Restarted osd.%d with %s%s",
                    osd,
                    change.set,
                    f", without {', '.join(change.remove)}" if change.remove else "",
                )
        except (steering.SteeringError, loopback.LoopbackError) as exc:
            return f"Cannot steer the Ceph OSDs: {exc}"
        return None

    def _storage_problem(self) -> str | None:
        """Return what storage attached to this unit is missing from the cluster, or None.

        MicroCloud reports a cluster as formed even when adding its disks
        failed, so a unit that was given storage checks that it is in use.
        """
        local_path = self._storage_local_path()
        ceph_paths, _ = self._storage_ceph_paths()
        if not snap.channel(self.config, "microceph"):
            ceph_paths = []
        if not local_path and not ceph_paths:
            return None

        hostname = microcloud.hostname()
        try:
            found = storage_health.problems(
                hostname,
                lxd_cluster.own_name(),
                local=bool(local_path),
                ceph_disks=len(ceph_paths),
                cephfs=bool(self.config.get("ceph-cephfs", False)),
            )
        except (storage_health.StorageHealthError, lxd_cluster.LXDClusterError) as exc:
            return f"Cannot check the storage of {hostname}: {exc}"

        if found:
            return "Cluster formed without storage: " + "; ".join(found)
        return None

    # ---- Observe-only mode ----

    def _reconcile_observe_only(self) -> str | None:
        """Validate Juju units against existing MicroCloud members.

        Only the leader performs cross-unit validation (it can see the full
        peer relation). Returns a problem string to block on, or None.
        """
        if not self.unit.is_leader():
            return None

        try:
            member_names = self._sessions.member_names()
        except microcloud.MicroCloudError as exc:
            return f"Cannot read MicroCloud members: {exc}"

        juju_hostnames = {name for name, _ in self._coordinator.all_members()}
        # Member units are expected once they report being in this cluster,
        # and allowed while joining or after their relation is gone.
        known = set(self._coordinator.joined_units())
        for relation in self._relations.members_relations():
            cluster = self._relations.published_fingerprint(relation)
            for member in self._relations.member_systems(relation):
                known.add(member.system.name)
                # A unit that is clustered elsewhere, or whose LXD has not
                # joined or cannot be read, publishes another fingerprint or
                # none, and blocks itself. It says nothing about this
                # cluster, so it must not stop this leader's work.
                here = bool(cluster) and member.fingerprint == cluster
                if member.system.initialized and here:
                    juju_hostnames.add(member.system.name)

        problems = validate_membership(juju_hostnames, member_names, known)
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

        message = "Cluster ready"
        if not self._is_orchestrator() and self._relations.orchestrator_relation() is None:
            if self._coordinator.cluster_mode() == MODE_MEMBER:
                message += "; orchestrator relation removed"
            else:
                # Formed with no mode recorded. See _check_cluster_mode.
                message += "; no orchestrator relation, set orchestrator=true"
        if self._cos_related():
            problems = self._observability.health_problems()
            if problems:
                self.unit.status = ops.BlockedStatus("; ".join(problems))
                return
            message += "; observability active"

        # The leader alone reports control-plane coverage, so it shows once.
        if self.unit.is_leader():
            warning = self._control_plane.coverage_warning()
            if warning:
                message += f"; {warning}"

        if self._steering_note:
            message += f"; {self._steering_note}"

        self.unit.status = ops.ActiveStatus(message)

    # ------------------------------------------------------------------
    # Action handlers
    # ------------------------------------------------------------------

    def _on_status_action(self, event: ops.ActionEvent) -> None:
        initialized = microcloud.is_initialized()
        result: dict[str, Any] = {
            "mode": "observe-only" if initialized else "deploy",
            "initialized": initialized,
            "cluster-mode": MODE_ORCHESTRATOR if self._is_orchestrator() else MODE_MEMBER,
        }
        if self._is_orchestrator() and self.unit.is_leader():
            related = {
                relation.app.name
                for relation in self._relations.members_relations()
                if relation.app is not None
            }
            joined = {
                name: unit if unit.split("/")[0] in related else f"{unit} (detached)"
                for name, unit in self._coordinator.joined_units().items()
            }
            if joined:
                result["joined-units"] = json.dumps(joined, sort_keys=True)
        if initialized:
            try:
                cluster = lxd_cluster.members()
            except lxd_cluster.LXDClusterError as exc:
                result["members-error"] = str(exc)
            else:
                result["members"] = lxd_cluster.render_table(cluster)
                try:
                    result["control-plane-mode"] = self._control_plane.mode(cluster)
                except lxd_cluster.LXDClusterError as exc:
                    result["control-plane-mode-error"] = str(exc)
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
