"""Manages the ovn-exporter snap for the microcloud charm.

Responsibilities
----------------
- Install the ovn-exporter snap from the configured channel.
- Connect its ovn-chassis and ovn-central-data content plugs to MicroOVN.
- Verify that both connections are active and the service is running.
- Remove the snap on full charm/relation teardown.
"""

import logging
import re
import subprocess

from charmlibs import snap

from constants import OVN_METRICS_PORT
from snap import is_installed

logger = logging.getLogger(__name__)

SNAP_NAME = "ovn-exporter"
SERVICE_NAME = f"{SNAP_NAME}.{SNAP_NAME}"

# Default port the ovn-exporter snap listens on for Prometheus scraping.
_DEFAULT_PORT = OVN_METRICS_PORT

# The two content interface connections required by the snap, expressed as
# (plug_name, (slot_snap, slot_name)) pairs for charmlibs.snap.connect().
# The slot must be fully qualified: MicroOVN exposes multiple "content"
# interface slots, so a bare snap name is ambiguous and snapd rejects it.
_CONNECTIONS = [
    ("ovn-chassis", ("microovn", "ovn-chassis")),
    ("ovn-central-data", ("microovn", "ovn-central-data")),
]


class OVNExporterError(Exception):
    """Raised when a snap or snap-connect operation fails."""


class OVNExporter:
    """Manages the lifecycle of the ovn-exporter snap.

    Parameters
    ----------
    channel:
        Snap Store channel to install from (e.g. "latest/edge").
    """

    def __init__(self, channel: str = "latest/edge") -> None:
        self._channel = channel

    @property
    def channel(self) -> str:
        """Snap channel in use."""
        return self._channel

    def ensure_installed(self) -> None:
        """Install the snap (if not already installed) and wire connections.

        Raises
        ------
        OVNExporterError
            If snap install or snap connect fails.
        """
        if is_installed(SNAP_NAME):
            logger.debug("%s snap is already installed", SNAP_NAME)
            # Still re-verify the connections in case they were manually removed.
            self._ensure_connections()
            return

        logger.info("Installing %s snap from channel %s", SNAP_NAME, self._channel)
        try:
            snap.install(SNAP_NAME, self._channel)
        except snap.Error as exc:
            raise OVNExporterError(
                f"Cannot install {SNAP_NAME} snap from {self._channel}: {exc.message}"
            ) from exc

        self._ensure_connections()
        logger.info("%s snap installed", SNAP_NAME)

    def remove(self) -> None:
        """Stop and remove the snap.  No-op if it is not installed."""
        if not is_installed(SNAP_NAME):
            logger.debug("%s snap is not installed; nothing to remove", SNAP_NAME)
            return
        logger.info("Removing %s snap", SNAP_NAME)
        try:
            snap.stop(SNAP_NAME, disable=True)
            snap.remove(SNAP_NAME, purge=True)
        except snap.Error as exc:
            raise OVNExporterError(f"Cannot remove {SNAP_NAME} snap: {exc.message}") from exc

    def is_healthy(self) -> tuple[bool, str]:
        """Return (True, "") if the service is running and connections are intact.

        Returns (False, reason) with a human-readable explanation otherwise.
        """
        if not is_installed(SNAP_NAME):
            return False, f"{SNAP_NAME} snap is not installed"

        try:
            self._ensure_connections()
        except OVNExporterError as exc:
            return False, str(exc)

        if not self._is_service_active():
            return False, f"{SERVICE_NAME} service is not active"

        return True, ""

    def _ensure_connections(self) -> None:
        """Connect all required content interfaces.  Idempotent.

        charmlibs.snap's connect() silently succeeds if the plug and slot
        are already connected, so no manual "already connected" handling
        is needed here (unlike the raw `snap connect` CLI).
        """
        for plug, slot in _CONNECTIONS:
            try:
                snap.connect((SNAP_NAME, plug), slot)
                logger.info("Connected %s:%s -> %s:%s", SNAP_NAME, plug, *slot)
            except snap.Error as exc:
                raise OVNExporterError(
                    f"Cannot connect {SNAP_NAME}:{plug} to {slot[0]}:{slot[1]}: {exc.message}"
                ) from exc

    def _is_service_active(self) -> bool:
        """Check whether the exporter's service is active.

        charmlibs.snap has no API to query service status, so this still
        shells out to `snap services` directly.
        """
        result = subprocess.run(
            ["snap", "services", SERVICE_NAME],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            return False
        for line in result.stdout.splitlines():
            if re.match(
                r"\s*" + re.escape(SERVICE_NAME) + r"\s+\w+\s+active", line, re.IGNORECASE
            ):
                return True
        return False
