"""Manages the Ceph mgr Prometheus module for the microcloud charm.

Responsibilities
----------------
- Detect whether ceph-mgr is placed on the local node.
- Enable the prometheus module and bind it to loopback.
- Report the current state so the charm can decide whether to advertise the
  scrape job.
"""

import logging
import socket
import subprocess

from constants import CEPH_METRICS_PORT, LOCALHOST_ADDR
from unixsocket import UnixSocketError, request_json

logger = logging.getLogger(__name__)

# MicroCeph daemon socket.
_MICROCEPH_SOCKET = "/var/snap/microceph/common/state/control.socket"


class CephMgrError(Exception):
    """Raised when a ceph command fails unexpectedly."""


class ServiceUnavailableException(Exception):
    """Raised when the MicroCeph socket/API is unavailable."""


def _list_services() -> list:
    """Return the MicroCeph clusterd service placement list.

    Performs a ``GET /1.0/services`` against the MicroCeph unix socket.
    Each entry looks like:
    ``{"service": "mgr", "location": "<hostname>"}``.

    Raises
    ------
    ServiceUnavailableException
        If the clusterd socket/API cannot be reached or returns an
        unexpected response.
    """
    try:
        data = request_json(_MICROCEPH_SOCKET, "GET", "/1.0/services")
    except UnixSocketError as exc:
        raise ServiceUnavailableException(str(exc)) from exc

    return data.get("metadata") or []


class CephMgrPrometheus:
    """Controls the Ceph mgr Prometheus module on the local node.

    Parameters
    ----------
    port:
        Port on which the exporter should listen. Defaults to 9283.
    rbd_stats_pools:
        Comma-separated list of RBD pools to collect per-image IO stats for
        (mgr/prometheus/rbd_stats_pools).
    enable_perf_metrics:
        Include Ceph performance counters in the exporter output. Maps onto
        mgr/prometheus/exclude_perf_counters (inverted), matching upstream
        MicroCeph's enable-perf-metrics config.
    """

    def __init__(
        self,
        port: int = CEPH_METRICS_PORT,
        rbd_stats_pools: str = "",
        enable_perf_metrics: bool = False,
    ) -> None:
        self._port = port
        self._rbd_stats_pools = rbd_stats_pools
        self._enable_perf_metrics = enable_perf_metrics

    @property
    def port(self) -> int:
        """Port the exporter will be configured to listen on."""
        return self._port

    def is_mgr_active(self) -> bool:
        """Return True if the MicroCeph cluster has mgr placed on this node.

        Queries the MicroCeph API rather than inferring liveness from
        local snap service state. If the MicroCeph API cannot
        be reached, this returns False.
        """
        try:
            services = _list_services()
        except ServiceUnavailableException as exc:
            logger.warning(
                "Could not determine mgr placement for this node; assuming mgr is not active: %s",
                exc,
            )
            return False

        hostname = socket.gethostname()
        return any(
            svc.get("service") == "mgr" and svc.get("location") == hostname for svc in services
        )

    def ensure_enabled(self) -> None:
        """Enable the Ceph mgr Prometheus module and bind it to loopback.

        Idempotent: calling this when the module is already enabled is safe.

        Raises
        ------
        CephMgrError
            If any ceph command fails.
        """
        if not self.is_mgr_active():
            logger.info("ceph-mgr is not active on this node; skipping prometheus module setup")
            return

        logger.info("Enabling Ceph mgr Prometheus module on %s:%d", LOCALHOST_ADDR, self._port)

        self._ceph("mgr", "module", "enable", "prometheus")
        self._ceph(
            "config",
            "set",
            "mgr",
            "mgr/prometheus/server_addr",
            LOCALHOST_ADDR,
        )
        self._ceph(
            "config",
            "set",
            "mgr",
            "mgr/prometheus/server_port",
            str(self._port),
        )
        self._ceph(
            "config",
            "set",
            "mgr",
            "mgr/prometheus/rbd_stats_pools",
            self._rbd_stats_pools,
        )
        self._ceph(
            "config",
            "set",
            "mgr",
            "mgr/prometheus/exclude_perf_counters",
            str(not self._enable_perf_metrics),
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

    def _ceph(self, *args: str) -> str:
        """Run a ceph command and return stdout."""
        cmd = ["ceph", *args]
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip()
        except FileNotFoundError as exc:
            raise CephMgrError("ceph not found") from exc
        except subprocess.CalledProcessError as exc:
            raise CephMgrError(
                f"Command {' '.join(cmd)} failed (rc={exc.returncode}): {exc.stderr.strip()}"
            ) from exc
