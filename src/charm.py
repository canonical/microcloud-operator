"""Main charm module for the microcloud charm.

This charm does NOT deploy or bootstrap MicroCloud. MicroCloud (LXD,
MicroCeph, MicroOVN) must already be deployed out-of-band, with each
MicroCloud member added to Juju via "juju add-machine".

Consistency check (every hook)
-------------------------------
    • MicroCloud must already be initialized on this node
    • Each unit independently verifies that its own hostname is a
      MicroCloud member (there is no peer relation, so a unit cannot know
      about its siblings).

Observability
------------------------
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
      The host is always forced to 127.0.0.1 since that endpoint is
      provided by a subordinate co-located on this same machine.

logging-relation-broken
    • Disable LXD's Loki streaming again.
"""

import json
import logging
import re
from typing import Any

import ops
from charms.grafana_agent.v0.cos_agent import COSAgentProvider
from charms.loki_k8s.v1.loki_push_api import LokiPushApiConsumer

import microcloud
import snap
from ceph import CephMgrError, CephMgrPrometheus
from consistency import validate_membership
from constants import CEPH_METRICS_PORT, LOCALHOST_ADDR, LXD_METRICS_PORT, OVN_METRICS_PORT
from lxd import (
    LXDConfigError,
    LXDManager,
)
from ovn import OVNExporter, OVNExporterError

logger = logging.getLogger(__name__)

# Dashboard JSON directories — one per service.
_DASHBOARD_DIRS = [
    "./src/dashboards/lxd",
    "./src/dashboards/microceph",
    "./src/dashboards/microovn",
]
_ALERT_RULES_DIR = "./src/prometheus_alert_rules"


class MicroCloudCharm(ops.CharmBase):
    """Validates and operates observability for a MicroCloud cluster deployed out-of-band."""

    def __init__(self, *args: Any) -> None:
        super().__init__(*args)

        # Lazy-initialised observability helpers.
        self._ceph: CephMgrPrometheus | None = None
        self._ovn: OVNExporter | None = None
        self._lxd: LXDManager | None = None

        self._loki_consumer = LokiPushApiConsumer(
            self, relation_name="logging", refresh_event=self.on.update_status
        )
        self.framework.observe(
            self._loki_consumer.on.loki_push_api_endpoint_joined,
            self._on_loki_push_api_endpoint_joined,
        )

        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.upgrade_charm, self._on_install)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.update_status, self._on_update_status)
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

        # Instantiated after the observers above so that ops (which dispatches
        # observers in registration order) runs our reconciliation handlers
        # first, ensuring self._ceph/self._ovn/self._lxd are populated before
        # COSAgentProvider._on_refresh reads them via _build_scrape_configs.
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

        self.framework.observe(
            self.on.logging_relation_departed, self._on_loki_push_api_endpoint_departed
        )
        self.framework.observe(
            self.on.logging_relation_broken, self._on_loki_push_api_endpoint_departed
        )

        # Actions
        self.framework.observe(self.on.status_action, self._on_status_action)
        self.framework.observe(self.on.dump_metrics_config_action, self._on_dump_metrics_config)

    def _make_helpers(self) -> None:
        self._ceph = CephMgrPrometheus(
            port=int(self.config.get("ceph-mgr-prometheus-port", CEPH_METRICS_PORT)),
            rbd_stats_pools=self.config.get("ceph-rbd-stats-pools", "lxd_remote"),
            enable_perf_metrics=self.config.get("ceph-enable-perf-metrics", True),
        )
        self._ovn = OVNExporter(
            channel=self.config.get("ovn-exporter-channel", "latest/edge"),
        )
        self._lxd = LXDManager(
            port=int(self.config.get("lxd-metrics-listen-port", LXD_METRICS_PORT))
        )

    # ------------------------------------------------------------------
    # Mode detection
    # ------------------------------------------------------------------

    def _cos_related(self) -> bool:
        return bool(self.model.relations.get("cos-agent"))

    def _log_slots(self) -> list[str]:
        """Snap log slots to advertise over cos-agent.

        The MicroCeph snap exposes its logs via a "ceph-logs" content-interface
        slot, so we can forward them without any custom log-shipping logic.
        MicroOVN does not (yet) expose an equivalent slot.
        LXD uses Loki.
        """
        slots: list[str] = []
        if snap.is_installed("microceph"):
            slots.append("microceph:ceph-logs")
        return slots

    def _on_install(self, event: ops.InstallEvent | ops.UpgradeCharmEvent) -> None:
        self._reconcile()

    def _on_config_changed(self, event: ops.ConfigChangedEvent) -> None:
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

    def _on_cos_agent_relation_joined(self, event: ops.RelationEvent) -> None:
        self._setup_observability()
        self._reconcile()

    def _on_cos_agent_relation_broken(self, event: ops.RelationBrokenEvent) -> None:
        self._teardown_observability(full_cleanup=True)
        self._reconcile()

    def _on_loki_push_api_endpoint_joined(self, event: ops.EventBase) -> None:
        self._make_helpers()
        self._lxd.ensure_loki_config(self._loki_consumer.loki_endpoints)

    def _on_loki_push_api_endpoint_departed(self, event: ops.RelationEvent) -> None:
        self._make_helpers()
        if endpoints := self._loki_consumer.loki_endpoints:
            self._lxd.ensure_loki_config(endpoints)
        else:
            self._lxd.teardown_loki_config()

    def _reconcile(self) -> None:
        """Validate MicroCloud membership consistency, then set status."""
        self._make_helpers()

        if not microcloud.is_initialized():
            self.unit.status = ops.BlockedStatus("MicroCloud is not initialized on this node")
            return

        try:
            members = microcloud.list_members()
        except microcloud.MicroCloudError as exc:
            self.unit.status = ops.BlockedStatus(f"Cannot read MicroCloud members: {exc}")
            return

        member_names = {m.name for m in members}
        problem = validate_membership(microcloud.hostname(), member_names)
        if problem:
            self.unit.status = ops.BlockedStatus(problem)
            return

        if self._cos_related():
            obs_problem = self._reconcile_observability()
            if obs_problem:
                self.unit.status = ops.BlockedStatus(obs_problem)
                return

        self._set_status()

    def _setup_observability(self) -> None:
        self._make_helpers()

    def _reconcile_observability(self) -> str | None:
        """Idempotently ensure metrics endpoints exist. Returns problem or None."""
        errors: list[str] = []

        try:
            self._lxd.ensure_metrics_config()
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

        return None

    def _teardown_observability(self, *, full_cleanup: bool) -> None:
        self._make_helpers()

        if not full_cleanup:
            return

        self._lxd.teardown_metrics_config()

        if snap.is_installed("microceph"):
            self._ceph.disable()

        if snap.is_installed("microovn"):
            try:
                self._ovn.remove()
            except OVNExporterError as exc:
                logger.warning("Cannot remove ovn-exporter during teardown: %s", exc)

    def _set_status(self) -> None:
        if self._cos_related():
            problems: list[str] = []
            if snap.is_installed("microovn"):
                healthy, reason = self._ovn.is_healthy()
                if not healthy:
                    problems.append(reason)
            if problems:
                self.unit.status = ops.BlockedStatus("; ".join(problems))
                return
            self.unit.status = ops.ActiveStatus("Observability active")
            return

        self.unit.status = ops.ActiveStatus("Waiting for observability relation")

    def _on_status_action(self, event: ops.ActionEvent) -> None:
        initialized = microcloud.is_initialized()
        result: dict[str, Any] = {
            "initialized": initialized,
        }
        if initialized:
            try:
                members = microcloud.list_members()
                result["members"] = json.dumps(
                    [{"name": m.name, "address": m.address, "status": m.status} for m in members]
                )
                member_names = {m.name for m in members}
                problem = validate_membership(microcloud.hostname(), member_names)
                result["consistent"] = problem is None
                if problem:
                    result["problem"] = problem
            except microcloud.MicroCloudError as exc:
                result["members-error"] = str(exc)
        event.set_results(result)

    def _on_dump_metrics_config(self, event: ops.ActionEvent) -> None:
        self._make_helpers()
        configs = self._build_scrape_configs()
        event.set_results({"scrape-configs": json.dumps(configs, indent=2)})

    def _build_scrape_configs(self) -> list[dict]:
        if self._ceph is None or self._ovn is None:
            self._make_helpers()

        configs: list[dict] = []
        cluster = self._cluster_label()
        member = self._member_label()
        interval = self.config.get("scrape-interval", "30s")

        # LXD.
        lxd_port = self.config.get("lxd-metrics-listen-port", LXD_METRICS_PORT)
        lxd_target = f"{LOCALHOST_ADDR}:{lxd_port}"
        configs.append(
            {
                # The base job name embeds the member name so that the
                # Prometheus "job" label (which cos_agent's
                # COSAgentProvider mangles into
                # "{app_name}_{job_name}_{hash}") stays human-readable,
                # instead of only differing per unit via an opaque hash
                # suffix.
                "job_name": f"microcloud-lxd-{member}",
                "scrape_interval": interval,
                "metrics_path": "/1.0/metrics",
                "scheme": "https",
                "tls_config": {
                    "insecure_skip_verify": True,
                },
                # The LXD metrics endpoint tags per-host metrics with their
                # own "instance" label identifying the owning host.
                # honor_labels keeps that label as-is instead of renaming
                # it to "exported_instance" and overwriting "instance" with
                # the scrape target address, which is identical
                # (127.0.0.1:<port>) on every unit and would collapse
                # per-host panels/variables in the bundled dashboard.
                "honor_labels": True,
                "static_configs": [
                    {
                        "targets": [lxd_target],
                        "labels": {
                            "microcloud_service": "lxd",
                            "microcloud_member": member,
                            "microcloud_cluster": cluster,
                        },
                    }
                ],
                "metric_relabel_configs": [
                    {
                        # Metrics without their own per-host "instance"
                        # label still fall back to the scrape target
                        # address, which is meaningless and identical
                        # across units. Replace it with this unit's member
                        # name so it stays unique and matches
                        # microcloud_member.
                        "source_labels": ["instance"],
                        "regex": re.escape(lxd_target),
                        "target_label": "instance",
                        "action": "replace",
                        "replacement": member,
                    },
                ],
            }
        )

        # MicroCeph.
        if snap.is_installed("microceph") and self._ceph.is_mgr_active():
            ceph_port = self.config.get("ceph-mgr-prometheus-port", CEPH_METRICS_PORT)
            ceph_target = f"{LOCALHOST_ADDR}:{ceph_port}"
            configs.append(
                {
                    # The base job name embeds the member name so that the
                    # Prometheus "job" label (which cos_agent's
                    # COSAgentProvider mangles into
                    # "{app_name}_{job_name}_{hash}") stays human-readable,
                    # instead of only differing per unit via an opaque hash
                    # suffix.
                    "job_name": f"microcloud-microceph-{member}",
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

        # MicroOVN.
        if snap.is_installed("microovn"):
            ovn_target = f"{LOCALHOST_ADDR}:{OVN_METRICS_PORT}"
            configs.append(
                {
                    # The base job name embeds the member name so that the
                    # Prometheus "job" label (which cos_agent's
                    # COSAgentProvider mangles into
                    # "{app_name}_{job_name}_{hash}") stays human-readable,
                    # instead of only differing per unit via an opaque hash
                    # suffix.
                    "job_name": f"microcloud-microovn-{member}",
                    "scrape_interval": interval,
                    "metrics_path": "/metrics",
                    # The OVN exporter tags per-host metrics with their own
                    # "instance" label identifying the owning host.
                    # honor_labels keeps that label as-is instead of
                    # renaming it to "exported_instance" and overwriting
                    # "instance" with the scrape target address, which is
                    # identical (127.0.0.1:<port>) on every unit and would
                    # collapse per-host panels/variables in the bundled
                    # dashboards.
                    "honor_labels": True,
                    "static_configs": [
                        {
                            "targets": [ovn_target],
                            "labels": {
                                "microcloud_service": "microovn",
                                "microcloud_member": member,
                                "microcloud_cluster": cluster,
                            },
                        }
                    ],
                    "metric_relabel_configs": [
                        {
                            # Metrics without their own per-host "instance"
                            # label (e.g. cluster-wide summaries) still
                            # fall back to the scrape target address, which
                            # is meaningless and identical across units.
                            # Replace it with this unit's member name so it
                            # stays unique and matches microcloud_member.
                            "source_labels": ["instance"],
                            "regex": re.escape(ovn_target),
                            "target_label": "instance",
                            "action": "replace",
                            "replacement": member,
                        },
                    ],
                }
            )

        return configs

    def _cluster_label(self) -> str:
        return self.app.name

    def _member_label(self) -> str:
        return microcloud.hostname()


if __name__ == "__main__":
    ops.main(MicroCloudCharm)
