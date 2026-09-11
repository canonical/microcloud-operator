"""Thin wrappers around the MicroCloud unix-socket API for the microcloud charm.

These helpers let the charm detect the current state of MicroCloud by
querying the microcluster control socket directly, rather than shelling out
to the ``microcloud`` CLI.
"""

import logging
import socket

import snap
from unixsocket import UnixSocketError, request_json

logger = logging.getLogger(__name__)

# MicroCloud daemon socket (microcluster control socket).
_MICROCLOUD_SOCKET = "/var/snap/microcloud/common/state/control.socket"


class MicroCloudError(Exception):
    """Raised when a MicroCloud API call fails unexpectedly."""


class Member:
    """A single MicroCloud cluster member."""

    def __init__(self, name: str, address: str, status: str = "") -> None:
        self.name = name
        self.address = address
        self.status = status

    def __repr__(self) -> str:
        return f"Member(name={self.name!r}, address={self.address!r}, status={self.status!r})"


def hostname() -> str:
    """Return this node's hostname (the MicroCloud member name)."""
    return socket.gethostname()


def is_initialized() -> bool:
    """Return True if MicroCloud is bootstrapped/initialized on this node.

    Queries ``GET /core/1.0`` on the MicroCloud control socket and checks the
    ``ready`` field of the returned status. Returns False if the snap is not
    installed or the socket cannot be reached (e.g. daemon not started yet).
    """
    if not snap.is_installed("microcloud"):
        return False

    try:
        data = request_json(_MICROCLOUD_SOCKET, "GET", "/core/1.0")
    except UnixSocketError as exc:
        logger.debug("Could not reach MicroCloud socket: %s", exc)
        return False

    metadata = data.get("metadata") or {}
    return bool(metadata.get("ready", False))


def list_members() -> list[Member]:
    """Return the current MicroCloud cluster members.

    Queries ``GET /core/1.0/cluster`` on the MicroCloud control socket.

    Raises
    ------
    MicroCloudError
        If the socket cannot be reached or its response cannot be parsed.
    """
    try:
        data = request_json(_MICROCLOUD_SOCKET, "GET", "/core/1.0/cluster")
    except UnixSocketError as exc:
        raise MicroCloudError(f"Cannot list MicroCloud members: {exc}") from exc

    raw = data.get("metadata") or []

    members: list[Member] = []
    for entry in raw:
        name = entry.get("name", "")
        address = entry.get("address", "")
        status = str(entry.get("status", "")).lower()
        if name:
            members.append(Member(name=name, address=address, status=status))
    return members
