# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Main charm module for the microcloud charm.

The charm operates in one of two auto-detected modes per node:

Deployment mode (MicroCloud not yet initialized)
------------------------------------------------
install / config-changed / peer-relation-changed
    • Install the microcloud, lxd, and (optionally) microceph/microovn snaps
      at the configured channels with a shared cohort, then hold refreshes.
    • Once every unit has reported its identity, the leader publishes its
      own bind address as the preseed initiator address. Every unit
      (leader and joiners alike) then renders and runs the identical
      "microcloud preseed" document; each daemon determines its own role
      (initiator vs. joiner) by matching its address against the published
      initiator address. Joining is unicast (not multicast): a joiner
      dials the initiator directly, so it must land after the initiator
      has actually opened its session. A generous session-timeout default
      (5 min) keeps the initiator's session open long enough to absorb
      timing skew between independently scheduled Juju hooks, and
      bootstrap retries "microcloud preseed" (10 times, 15s apart, both
      fixed) within the same hook execution if a unit's own attempt
      lands before the initiator's session has opened.

Observe-only mode (MicroCloud already initialized out-of-band)
--------------------------------------------------------------
    • No snaps are installed and no bootstrap is attempted.
    • The leader validates that every MicroCloud member has a corresponding
      Juju unit (matched on hostname) and blocks on any mismatch.

Observability (both modes, optional)
------------------------------------
cos-agent-relation-joined / changed
    • Ensure metrics endpoints exist (LXD metrics config, Ceph mgr module,
      ovn-exporter snap) and publish scrape jobs + dashboards.

cos-agent-relation-broken
    • Tear the metrics setup back down.
"""

import json
import logging
import re
import subprocess
import time
from typing import Any

import ops
import yaml
from charms.grafana_agent.v0.cos_agent import COSAgentProvider

import microcloud
import snap
from ceph_mgr import CephMgrError, CephMgrPrometheus
from cluster import ClusterCoordinator, validate_membership
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

class LXDConfigError(Exception):
    """Raised when an LXD configuration operation fails."""


class MicroCloudCharm(ops.CharmBase):
    """Deploys and operates a MicroCloud cluster, optionally wired to COS."""

    def __init__(self, *args: Any) -> None:
        super().__init__(*args)

        # Lazy-initialised observability helpers.
        self._ceph: CephMgrPrometheus | None = None
        self._ovn: OVNExporter | None = None

        self._coordinator = ClusterCoordinator(self)

        self._cos_agent = COSAgentProvider(
            self,
            scrape_configs=self._build_scrape_configs,
            dashboard_dirs=_DASHBOARD_DIRS,
            metrics_rules_dir=_ALERT_RULES_DIR,
            refresh_events=[
                self.on.config_changed,
                self.on.update_status,
            ],
        )

        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.upgrade_charm, self._on_install)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.update_status, self._on_update_status)
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
    # Reconciliation
    # ------------------------------------------------------------------

    def _reconcile(self) -> None:
        """Drive deployment or observe-only mode, then set status."""
        self._make_helpers()

        ovn_uplink_interface, problem = self._ovn_uplink_interface()
        if problem:
            self.unit.status = ops.BlockedStatus(problem)
            return

        # Publish our identity for peers as early as possible.
        self._coordinator.publish_identity(
            microcloud.hostname(),
            self._bind_address(),
            ovn_uplink_interface=ovn_uplink_interface,
            ovn_underlay_ip=self._space_bind_address("ovn-underlay"),
            storage_local_path=self._storage_local_path(),
            storage_ceph_paths=self._storage_ceph_paths(),
        )

        initialized = microcloud.is_initialized()

        if initialized:
            problem = self._reconcile_observe_only()
        else:
            problem = self._reconcile_deploy()

        if problem:
            self.unit.status = ops.BlockedStatus(problem)
            return

        if self._cos_related():
            obs_problem = self._reconcile_observability()
            if obs_problem:
                self.unit.status = ops.BlockedStatus(obs_problem)
                return

        self._set_status(initialized=microcloud.is_initialized())

    # ---- Deployment mode ----

    def _reconcile_deploy(self) -> str | None:
        """Install snaps, then run "microcloud preseed" on every unit.

        Every unit publishes readiness once its own prerequisites are met.
        The leader waits for all units to be ready before publishing its
        bind address as the preseed initiator.

        Juju only propagates a unit's relation-data writes to its peers
        once the writing hook exits successfully - a hook that is still
        running (e.g. blocked inside "microcloud preseed") has not
        committed anything yet, and peers cannot see it. This means the
        leader must not publish its address and immediately block on
        opening its own session in the very same hook: no joiner could
        ever learn the address in time to dial in, since the leader's
        hook would not exit (and so not commit) until that blocking call
        itself finished or timed out - a deadlock. Instead:

        1. The leader publishes ``initiator_address`` and returns (a fast,
           committing hook).
        2. Joiners see the address, "ack" it themselves (another fast,
           committing hook) rather than immediately trying to join, and
           only try in a later hook.
        3. Once the leader sees at least one peer's ack, it knows a joiner
           is actually about to dial in, and only then opens its own
           session by running "microcloud preseed".
        4. Joiners, having already acked in an earlier hook, now run their
           own "microcloud preseed", retrying a few times (unicast joining
           means dialing in before the initiator's session exists is
           rejected immediately rather than waited on).

        Returns a problem string to block on, or None.
        """
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
            self.unit.status = ops.WaitingStatus("Waiting for microcloud daemon")
            return None

        if not self._coordinator.all_identities_published():
            self.unit.status = ops.WaitingStatus("Waiting for all peers to report identity")
            return None

        passphrase = self._coordinator.ensure_passphrase()
        if len(self._coordinator.all_members()) > 1 and not passphrase:
            self.unit.status = ops.WaitingStatus("Waiting for session passphrase")
            return None

        # Every unit has now cleared all its own prerequisites: signal that
        # it is ready to bootstrap as soon as told to.
        self._coordinator.publish_ready()

        if self.unit.is_leader():
            if not self._coordinator.all_ready():
                self.unit.status = ops.WaitingStatus("Waiting for all peers to be ready")
                return None

            if not self._coordinator.initiator_address():
                # First time all units are ready: commit our address in
                # this fast hook and come back later to actually open the
                # session, once we know a peer has seen it (see docstring).
                self._coordinator.publish_initiator_address(self._bind_address())
                self.unit.status = ops.MaintenanceStatus(
                    "Initiator address published; waiting for a peer to acknowledge"
                )
                return None

            if not self._coordinator.any_peer_acked():
                self.unit.status = ops.WaitingStatus("Waiting for a peer to acknowledge initiator address")
                return None

        initiator_address = self._coordinator.initiator_address()
        if not initiator_address:
            self.unit.status = ops.WaitingStatus("Waiting for leader to select initiator")
            return None

        if not self.unit.is_leader() and not self._coordinator.has_acked():
            # First time seeing the address: ack it in this fast hook and
            # come back later to actually try joining (see docstring).
            self._coordinator.publish_ack()
            self.unit.status = ops.MaintenanceStatus("Acknowledged initiator address; will join shortly")
            return None

        return self._bootstrap(passphrase or "", initiator_address)

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
            ceph_public_network=self._space_network_cidr("ceph-public")
            if with_ceph_storage
            else "",
            ceph_internal_network=self._space_network_cidr("ceph-internal")
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

    def _bootstrap(self, passphrase: str, initiator_address: str) -> str | None:
        """Render preseed and run it on this unit. Returns problem or None."""
        systems = [
            SystemEntry(
                name=system.name,
                address=system.address,
                ovn_uplink_interface=system.ovn_uplink_interface,
                ovn_underlay_ip=system.ovn_underlay_ip,
                storage_local_path=system.storage_local_path,
                storage_ceph_paths=system.storage_ceph_paths,
            )
            for system in self._coordinator.all_systems()
        ]

        inputs = self._preseed_inputs(initiator_address, passphrase, systems)

        self.unit.status = ops.MaintenanceStatus("Bootstrapping MicroCloud cluster")

        rendered = render(inputs)
        retries = 10
        retry_delay = 15

        # We use unicast (not multicast) joining: a joiner must dial the
        # initiator's already-open session. If this unit's own hook runs
        # before the initiator has started its session (e.g. still
        # installing snaps, or its hook simply hasn't fired yet on Juju's
        # independent per-unit schedule), the daemon rejects the attempt
        # immediately with "No active session" rather than waiting - the
        # MicroCloud CLI's lookup_timeout only bounds the wait *after* a
        # session is found (and only applies to multicast discovery, which
        # this charm never uses), so it does not help here. Retrying a few
        # times lets this unit catch up once the initiator's session
        # opens, instead of waiting for Juju's much slower periodic
        # update-status reconciliation.
        last_exc: microcloud.MicroCloudError | None = None
        for attempt in range(retries + 1):
            try:
                microcloud.run_preseed(rendered)
                return None
            except microcloud.MicroCloudError as exc:
                last_exc = exc
                # microcloud.run_preseed() already logs the full
                # stdout/stderr; log here too so the failure is visible
                # from this module's logger context, since the returned
                # status string is truncated to a single line by Juju.
                logger.error(
                    "MicroCloud bootstrap attempt %d/%d failed: %s",
                    attempt + 1,
                    retries + 1,
                    exc,
                )
                if attempt < retries:
                    self.unit.status = ops.MaintenanceStatus(
                        f"Bootstrapping MicroCloud cluster (retry {attempt + 1}/{retries})"
                    )
                    time.sleep(retry_delay)

        return f"Bootstrap: {last_exc}"

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

    def _bind_address(self) -> str:
        """Return this unit's bind address for the peer relation."""
        return self._space_bind_address("cluster")

    def _binding_network(self, binding_name: str) -> ops.Network | None:
        """Return the Network for the given endpoint, or None if unavailable.

        Both ``get_binding()`` and accessing its ``.network`` property can
        raise "ModelError: no network config found for binding ..." when the
        endpoint has no usable network info at all (e.g. an extra-binding
        left unbound with no default space fallback), so both must be
        guarded by the same try/except.
        """
        try:
            binding = self.model.get_binding(binding_name)
            if not binding:
                return None
            return binding.network
        except ops.ModelError:
            return None

    def _space_bind_address(self, binding_name: str) -> str:
        """Return this unit's bind address for the given endpoint, if any.

        Since "network-get" is evaluated per-unit, binding an endpoint (e.g.
        the "ovn-underlay" extra-binding) to a Juju space naturally yields a
        different address per machine, unlike a single shared config value.
        Returns "" if the endpoint has no usable address (e.g. left unbound).
        """
        network = self._binding_network(binding_name)
        if not network or not network.bind_address:
            return ""
        return str(network.bind_address)

    def _space_network_cidr(self, binding_name: str) -> str:
        """Return the CIDR of the subnet bound to the given extra-binding, if any.

        Lets operators point Ceph's public/internal network at a Juju space
        (via "juju deploy --bind") instead of hard-coding a CIDR. Returns ""
        if the endpoint has no usable subnet (e.g. left unbound).
        """
        network = self._binding_network(binding_name)
        if not network or not network.interfaces:
            return ""
        subnet = network.interfaces[0].subnet
        return str(subnet) if subnet else ""

    def _ovn_uplink_interface(self) -> tuple[str, str | None]:
        """Resolve this unit's OVN uplink interface name.

        "ovn-uplink-interface" config is either a single interface name
        applied to every unit, or a YAML/JSON mapping of hostname to
        interface name (for hardware where the NIC name differs per
        machine). When it is a mapping, every unit must have an entry: a
        unit missing from the mapping cannot safely guess an interface
        name, so it must block rather than bootstrap without one (or
        silently omit the OVN uplink and produce a broken cluster).

        There is deliberately no extra-binding fallback here: Juju spaces
        only track addressed interfaces, but the OVN uplink NIC is
        normally a bare, L2-only interface with no IP, so "network-get"
        can never resolve it.

        Returns (interface, problem). "problem" is a human-readable status
        string if reconciliation should block; "interface" is only
        meaningful when "problem" is None.
        """
        raw = str(self.config.get("ovn-uplink-interface", "")).strip()
        if not raw:
            return "", None

        try:
            parsed = yaml.safe_load(raw)
        except yaml.YAMLError as exc:
            return "", f"Cannot parse ovn-uplink-interface: {exc}"

        if not isinstance(parsed, dict):
            return raw, None

        hostname = microcloud.hostname()
        interface = parsed.get(hostname)
        if not interface:
            known = sorted(str(key) for key in parsed)
            return "", (
                f"ovn-uplink-interface is missing an entry for hostname {hostname!r}; "
                f"known entries: {known}"
            )
        return str(interface), None


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


if __name__ == "__main__":
    ops.main(MicroCloudCharm)
