# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manages the Ceph mgr Prometheus module for the microcloud charm.

Responsibilities
----------------
- Detect whether ceph-mgr is active on the local node (via microceph status).
- Enable the prometheus module and bind it to loopback.
- Report the current state so the charm can decide whether to advertise the
  scrape job.
"""

import logging
import re
import subprocess

logger = logging.getLogger(__name__)

# Default address / port used upstream by the Ceph mgr Prometheus module.
_DEFAULT_ADDR = "127.0.0.1"
_DEFAULT_PORT = 9283


class CephMgrError(Exception):
    """Raised when a microceph.ceph command fails unexpectedly."""


class CephMgrPrometheus:
    """Controls the Ceph mgr Prometheus module on the local node.

    Parameters
    ----------
    port:
        Port on which the exporter should listen.  Defaults to 9283.
    rbd_stats_pools:
        Comma-separated list of RBD pools to collect per-image IO stats for
        (mgr/prometheus/rbd_stats_pools). Empty (default) disables per-image
        RBD stats, matching upstream MicroCeph's default.
    enable_perf_metrics:
        Include Ceph performance counters in the exporter output. Maps onto
        mgr/prometheus/exclude_perf_counters (inverted), matching upstream
        MicroCeph's enable-perf-metrics config. Defaults to False.
    """

    def __init__(
        self, port: int = _DEFAULT_PORT, rbd_stats_pools: str = "", enable_perf_metrics: bool = False
    ) -> None:
        self._port = port
        self._rbd_stats_pools = rbd_stats_pools
        self._enable_perf_metrics = enable_perf_metrics

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def port(self) -> int:
        """Port the exporter will be configured to listen on."""
        return self._port

    def is_mgr_active(self) -> bool:
        """Return True if ceph-mgr is active on this node.

        Uses ``microceph status`` (or ``snap services microceph``) to
        determine whether the local MicroCeph deployment has a running
        mgr service.  Returns False when MicroCeph is not installed.
        """
        # First try: snap services gives a reliable per-service breakdown.
        try:
            result = subprocess.run(
                ["snap", "services", "microceph"],
                capture_output=True, text=True, check=False,
            )
            if result.returncode != 0:
                # microceph snap not installed or not initialised.
                return False
            # Lines look like:
            #   microceph.mgr   enabled  active    -
            for line in result.stdout.splitlines():
                if re.match(r"\s*microceph\.mgr\s+\w+\s+active", line, re.IGNORECASE):
                    return True
            return False
        except FileNotFoundError:
            # snap not available (unlikely on Ubuntu but be safe).
            return False

    def ensure_enabled(self) -> None:
        """Enable the Ceph mgr Prometheus module and bind it to loopback.

        Idempotent: calling this when the module is already enabled is safe.

        Raises
        ------
        CephMgrError
            If any microceph.ceph command fails.
        """
        if not self.is_mgr_active():
            logger.info("ceph-mgr is not active on this node; skipping prometheus module setup")
            return

        logger.info("Enabling Ceph mgr Prometheus module on %s:%d", _DEFAULT_ADDR, self._port)

        self._ceph("mgr", "module", "enable", "prometheus")
        self._ceph(
            "config", "set", "mgr", "mgr/prometheus/server_addr", _DEFAULT_ADDR,
        )
        self._ceph(
            "config", "set", "mgr", "mgr/prometheus/server_port", str(self._port),
        )
        if self._rbd_stats_pools:
            self._ceph(
                "config", "set", "mgr", "mgr/prometheus/rbd_stats_pools", self._rbd_stats_pools
            )
        self._ceph(
            "config", "set", "mgr", "mgr/prometheus/exclude_perf_counters",
            str(not self._enable_perf_metrics)
        )

    def disable(self) -> None:
        """Disable the Ceph mgr Prometheus module.

        No-op when MicroCeph is not installed or mgr is not active.
        """
        if not self.is_mgr_active():
            return
        try:
            self._ceph("mgr", "module", "disable", "prometheus")
            logger.info("Disabled Ceph mgr Prometheus module")
        except CephMgrError as exc:
            logger.warning("Cannot disable Ceph mgr Prometheus module: %s", exc)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _ceph(self, *args: str) -> str:
        """Run a microceph.ceph command and return stdout."""
        cmd = ["microceph.ceph", *args]
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip()
        except FileNotFoundError as exc:
            raise CephMgrError("microceph.ceph not found; is MicroCeph installed?") from exc
        except subprocess.CalledProcessError as exc:
            raise CephMgrError(
                f"Command {' '.join(cmd)} failed (rc={exc.returncode}): {exc.stderr.strip()}"
            ) from exc
