# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

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

logger = logging.getLogger(__name__)

SNAP_NAME = "ovn-exporter"
SERVICE_NAME = f"{SNAP_NAME}.{SNAP_NAME}"

# The two content interface connections required by the snap.
_CONNECTIONS = [
    (f"{SNAP_NAME}:ovn-chassis", "microovn:ovn-chassis"),
    (f"{SNAP_NAME}:ovn-central-data", "microovn:ovn-central-data"),
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

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

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
        if self._is_installed():
            logger.debug("%s snap is already installed", SNAP_NAME)
            # Still re-verify the connections in case they were manually removed.
            self._ensure_connections()
            return

        logger.info("Installing %s snap from channel %s", SNAP_NAME, self._channel)
        try:
            _run(["snap", "install", SNAP_NAME, "--channel", self._channel])
        except OVNExporterError as exc:
            raise OVNExporterError(
                f"Cannot install {SNAP_NAME} snap from {self._channel}: {exc}"
            ) from exc

        self._ensure_connections()
        # The service starts automatically after connections are established;
        # explicitly enable+start to be certain.
        _run(["snap", "start", "--enable", SERVICE_NAME])
        logger.info("%s snap installed and started", SNAP_NAME)

    def remove(self) -> None:
        """Stop and remove the snap.  No-op if it is not installed."""
        if not self._is_installed():
            logger.debug("%s snap is not installed; nothing to remove", SNAP_NAME)
            return
        logger.info("Removing %s snap", SNAP_NAME)
        _run(["snap", "stop", "--disable", SERVICE_NAME])
        _run(["snap", "remove", "--purge", SNAP_NAME])

    def is_healthy(self) -> tuple[bool, str]:
        """Return (True, "") if the service is running and connections are intact.

        Returns (False, reason) with a human-readable explanation otherwise.
        """
        if not self._is_installed():
            return False, f"{SNAP_NAME} snap is not installed"

        missing = self._missing_connections()
        if missing:
            names = ", ".join(m[0] for m in missing)
            return False, f"Missing snap connections: {names}"

        if not self._is_service_active():
            return False, f"{SERVICE_NAME} service is not active"

        return True, ""

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _is_installed(self) -> bool:
        result = subprocess.run(
            ["snap", "list", SNAP_NAME],
            capture_output=True, text=True, check=False,
        )
        return result.returncode == 0

    def _ensure_connections(self) -> None:
        """Connect all required content interfaces.  Idempotent."""
        for plug, slot in _CONNECTIONS:
            try:
                _run(["snap", "connect", plug, slot])
                logger.info("Connected %s → %s", plug, slot)
            except OVNExporterError as exc:
                # snap connect exits non-zero when already connected on some
                # snapd versions; treat that as success.
                if "already connected" in str(exc).lower():
                    logger.debug("Connection %s → %s already active", plug, slot)
                else:
                    raise OVNExporterError(
                        f"Cannot connect {plug} to {slot}: {exc}"
                    ) from exc

    def _missing_connections(self) -> list[tuple[str, str]]:
        """Return list of (plug, slot) pairs that are not currently connected."""
        result = subprocess.run(
            ["snap", "connections", SNAP_NAME],
            capture_output=True, text=True, check=False,
        )
        if result.returncode != 0:
            return list(_CONNECTIONS)

        connected: set[str] = set()
        for line in result.stdout.splitlines():
            # Lines: Interface  Plug  Slot  Notes
            parts = re.split(r"\s+", line.strip())
            if len(parts) >= 3 and parts[2] != "-":
                connected.add(parts[1])  # plug column

        missing = []
        for plug, slot in _CONNECTIONS:
            plug_short = plug.split(":")[1]  # e.g. "ovn-chassis"
            full_plug = f"{SNAP_NAME}:{plug_short}"
            if full_plug not in connected:
                missing.append((plug, slot))
        return missing

    def _is_service_active(self) -> bool:
        result = subprocess.run(
            ["snap", "services", SERVICE_NAME],
            capture_output=True, text=True, check=False,
        )
        if result.returncode != 0:
            return False
        for line in result.stdout.splitlines():
            if re.match(r"\s*" + re.escape(SERVICE_NAME) + r"\s+\w+\s+active", line, re.IGNORECASE):
                return True
        return False


def _run(cmd: list[str]) -> str:
    """Run a command and return stdout.  Raise OVNExporterError on failure."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as exc:
        raise OVNExporterError(
            f"Command {cmd[0]} failed (rc={exc.returncode}): {exc.stderr.strip()}"
        ) from exc
