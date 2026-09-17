# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Metrics endpoints, scrape jobs and LXD log streaming for COS.

The charm keeps the cos-agent and logging relation objects, which ops needs
attached to the charm, and hands the work to ``Observability``.
"""

import json
import logging
import re
import subprocess
from collections.abc import Mapping
from typing import Any

import microcloud
import snap
from ceph_mgr import CephMgrError, CephMgrPrometheus
from ovn_exporter import OVNExporter, OVNExporterError

logger = logging.getLogger(__name__)

# Dashboard JSON directories — one per service.
DASHBOARD_DIRS = [
    "./src/dashboards/lxd",
    "./src/dashboards/microceph",
    "./src/dashboards/microovn",
]
ALERT_RULES_DIR = "./src/prometheus_alert_rules"

# LXD metrics address — dedicated loopback listener, always TLS.
_LXD_METRICS_ADDRESS = "127.0.0.1:8444"


class LXDConfigError(Exception):
    """Raised when an LXD configuration operation fails."""


class Observability:
    """Helper that sets up and tears down this unit's metrics and log forwarding."""

    def __init__(self, config: Mapping[str, Any], app_name: str) -> None:
        self._config = config
        self._app_name = app_name
        self._ceph = CephMgrPrometheus(
            port=int(config.get("ceph-mgr-prometheus-port", 9283)),
            rbd_stats_pools=config.get("ceph-rbd-stats-pools", ""),
            enable_perf_metrics=config.get("ceph-enable-perf-metrics", False),
        )
        self._ovn = OVNExporter(
            channel=config.get("ovn-exporter-channel", "latest/edge"),
        )

    def log_slots(self) -> list[str]:
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
    # Metrics
    # ------------------------------------------------------------------

    def reconcile(self) -> str | None:
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
        return None

    def teardown(self, *, full_cleanup: bool) -> None:
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

    def health_problems(self) -> list[str]:
        """Return why the metrics setup is unhealthy, if it is."""
        problems: list[str] = []
        if snap.is_installed("microovn"):
            healthy, reason = self._ovn.is_healthy()
            if not healthy:
                problems.append(reason)
        return problems

    def _ensure_lxd_metrics_config(self) -> None:
        """Set core.metrics_address and core.metrics_authentication on LXD."""
        _lxc_config_set("core.metrics_address", _LXD_METRICS_ADDRESS)
        _lxc_config_set("core.metrics_authentication", "false")

    # ------------------------------------------------------------------
    # Logs
    # ------------------------------------------------------------------

    def ensure_loki(self, endpoints: list[dict]) -> None:
        """Point LXD's native Loki client at the related Loki push endpoint."""
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

    def teardown_loki(self) -> None:
        """Stop LXD from streaming logs to Loki."""
        try:
            _lxc_config_set("loki.api.url", "")
        except LXDConfigError as exc:
            logger.warning("Cannot reset LXD loki.api.url during teardown: %s", exc)
            return

        logger.info("LXD is no longer streaming logs to Loki")

    # ------------------------------------------------------------------
    # Scrape config builder (called by COSAgentProvider)
    # ------------------------------------------------------------------

    def scrape_configs(self) -> list[dict]:
        configs: list[dict] = []
        cluster = self._cluster_label()
        member = self._member_label()
        interval = self._config.get("scrape-interval", "30s")

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
            ceph_port = self._config.get("ceph-mgr-prometheus-port", 9283)
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
            ovn_port = self._config.get("ovn-exporter-listen-port", 9310)
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
    # Label helpers
    # ------------------------------------------------------------------

    def _cluster_label(self) -> str:
        return self._app_name

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
