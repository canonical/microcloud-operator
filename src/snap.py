# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Snap installation helpers for the microcloud charm (deployment mode).

Installs the MicroCloud component snaps at the configured channels using a
shared cohort so that later refreshes stay synchronized across members, then
holds refreshes to avoid uncoordinated auto-updates.
"""

import http.client
import json
import logging
import socket
import subprocess

import lxd_cluster

logger = logging.getLogger(__name__)

# Microcluster daemons answer GET /core/1.0 on their control socket with the
# version they report to MicroCloud when a system joins a session.
_CONTROL_SOCKETS = {
    "microcloud": "/var/snap/microcloud/common/state/control.socket",
    "microceph": "/var/snap/microceph/common/state/control.socket",
    "microovn": "/var/snap/microovn/common/state/control.socket",
}
_SNAPD_SOCKET = "/run/snapd.socket"


class SnapError(Exception):
    """Raised when a snap operation fails."""


def _run(cmd: list[str]) -> str:
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as exc:
        raise SnapError(
            f"Command {' '.join(cmd)} failed (rc={exc.returncode}): {exc.stderr.strip()}"
        ) from exc


def is_installed(name: str) -> bool:
    """Return True if the named snap is installed."""
    result = subprocess.run(
        ["snap", "list", name],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def install(name: str, channel: str) -> None:
    """Install (or refresh to) ``name`` at ``channel`` with a shared cohort.

    Idempotent: if already installed, switches it to the requested channel.
    """
    if is_installed(name):
        logger.debug("snap %s already installed; refreshing to %s", name, channel)
        _run(["snap", "refresh", name, "--channel", channel, "--cohort=+"])
    else:
        logger.info("Installing snap %s from %s", name, channel)
        _run(["snap", "install", name, "--channel", channel, "--cohort=+"])


def hold(name: str) -> None:
    """Hold refreshes for the named snap. Best-effort; logs on failure."""
    try:
        _run(["snap", "refresh", "--hold", name])
    except SnapError as exc:
        logger.warning("Cannot hold refreshes for snap %s: %s", name, exc)


class _UnixConnection(http.client.HTTPConnection):
    def __init__(self, path: str, timeout: float = 10) -> None:
        super().__init__("localhost", timeout=timeout)
        self._path = path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self._path)


def _get_json(socket_path: str, path: str) -> object:
    connection = _UnixConnection(socket_path)
    try:
        connection.request("GET", path)
        return json.loads(connection.getresponse().read() or b"{}")
    finally:
        connection.close()


def tracked_channels() -> dict[str, str]:
    """Return the channel each installed snap tracks, "" for one that tracks none.

    Raises SnapError if snapd cannot be read.
    """
    try:
        body = _get_json(_SNAPD_SOCKET, "/v2/snaps")
    except (OSError, ValueError, http.client.HTTPException) as exc:
        raise SnapError(f"Cannot read the installed snaps: {exc}") from exc
    snaps = body.get("result") if isinstance(body, dict) else None
    if not isinstance(snaps, list):
        raise SnapError("Cannot read the installed snaps: unexpected response from snapd")
    return {str(item["name"]): str(item.get("tracking-channel", "")) for item in snaps}


def _microcluster_version(name: str) -> str:
    try:
        body = _get_json(_CONTROL_SOCKETS[name], "/core/1.0")
    except (OSError, ValueError, http.client.HTTPException) as exc:
        raise SnapError(f"Cannot read the {name} version: {exc}") from exc
    metadata = body.get("metadata") if isinstance(body, dict) else None
    return str(metadata.get("version", "")) if isinstance(metadata, dict) else ""


def service_version(name: str) -> str:
    """Return the version the ``name`` daemon reports, as MicroCloud compares it.

    These are not snap versions: MicroOVN reports only the OVN major version,
    for example, so builds of the same channel match across architectures.

    Raises SnapError if the daemon cannot be read.
    """
    if name == "lxd":
        try:
            return lxd_cluster.server_version()[0]
        except lxd_cluster.LXDClusterError as exc:
            raise SnapError(f"Cannot read the lxd version: {exc}") from exc
    return _microcluster_version(name)


def service_versions(names: list[str]) -> dict[str, str]:
    """Return the reported version of each daemon in ``names`` that answers."""
    versions: dict[str, str] = {}
    for name in names:
        try:
            version = service_version(name)
        except SnapError as exc:
            logger.info("%s", exc)
            continue
        if version:
            versions[name] = version
    return versions


def version_mismatches(own: dict[str, str], wanted: dict[str, str]) -> dict[str, str]:
    """Return {snap: own version} for each snap in ``wanted`` that ``own`` differs on.

    A snap missing from ``own`` counts as differing, reported as "unknown".
    """
    return {
        name: own.get(name, "unknown")
        for name, version in sorted(wanted.items())
        if own.get(name) != version
    }


def ensure_installed(channels: dict[str, str]) -> None:
    """Install and hold each snap in ``channels`` that is not installed yet.

    Unlike :func:`ensure_snaps`, an installed snap is left alone, so a
    revision the operator has pinned by hand stays put.
    """
    for name, channel in channels.items():
        if channel and not is_installed(name):
            install(name, channel)
            hold(name)


def ensure_snaps(channels: dict[str, str]) -> None:
    """Install and hold all snaps described by ``channels``.

    ``channels`` maps snap name -> channel. Entries with an empty/falsy
    channel are skipped entirely (that component is not deployed).

    Raises
    ------
    SnapError
        If installing a required snap fails.
    """
    for name, channel in channels.items():
        if not channel:
            logger.info("Skipping snap %s (no channel configured)", name)
            continue
        install(name, channel)
        hold(name)
