# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Thin wrappers around the microcloud CLI for the microcloud charm.

These helpers let the charm detect the current state of MicroCloud on the
local node without any interactive prompts:

- ``is_snap_installed``  – is the microcloud snap present at all?
- ``waitready``          – block until the microcloud daemon is reachable.
- ``is_initialized``     – has the cluster been bootstrapped on this node?
- ``list_members``       – enumerate cluster members (name + address).
- ``hostname``           – this node's hostname, the key used to match
                           MicroCloud members against Juju units.
"""

import json
import logging
import socket
import subprocess

logger = logging.getLogger(__name__)


class MicroCloudError(Exception):
    """Raised when a microcloud CLI command fails unexpectedly."""


class Member:
    """A single MicroCloud cluster member."""

    def __init__(self, name: str, address: str, status: str = "") -> None:
        self.name = name
        self.address = address
        self.status = status

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return f"Member(name={self.name!r}, address={self.address!r}, status={self.status!r})"


def hostname() -> str:
    """Return this node's hostname (the MicroCloud member name)."""
    return socket.gethostname()


def is_snap_installed() -> bool:
    """Return True if the microcloud snap is installed on this node."""
    result = subprocess.run(
        ["snap", "list", "microcloud"],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def waitready(timeout: int = 60) -> bool:
    """Wait for the microcloud daemon to become reachable.

    Returns True if the daemon is ready within ``timeout`` seconds, False
    otherwise. Never raises.
    """
    result = subprocess.run(
        ["microcloud", "waitready", "--timeout", str(timeout)],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def is_initialized() -> bool:
    """Return True if MicroCloud is bootstrapped/initialized on this node.

    Uses the exit code of ``microcloud status``: exit 0 means the cluster is
    formed; a non-zero exit (typically with "uninitialized" on stderr) means
    it is not. Returns False if the snap is not installed.
    """
    if not is_snap_installed():
        return False

    result = subprocess.run(
        ["microcloud", "status"],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def list_members() -> list[Member]:
    """Return the current MicroCloud cluster members.

    Parses ``microcloud cluster list --format json``.

    Raises
    ------
    MicroCloudError
        If the command fails or its output cannot be parsed. Callers should
        only invoke this when :func:`is_initialized` returns True.
    """
    result = subprocess.run(
        ["microcloud", "cluster", "list", "--format", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise MicroCloudError(
            f"Cannot list MicroCloud members (rc={result.returncode}): {result.stderr.strip()}"
        )

    try:
        raw = json.loads(result.stdout or "[]")
    except json.JSONDecodeError as exc:
        raise MicroCloudError(f"Cannot parse MicroCloud member list: {exc}") from exc

    members: list[Member] = []
    for entry in raw:
        name = entry.get("name", "")
        address = entry.get("address", "")
        status = entry.get("status", "")
        if name:
            members.append(Member(name=name, address=address, status=status))
    return members


def run_preseed(preseed_yaml: str) -> str:
    """Run ``microcloud preseed`` feeding it ``preseed_yaml`` on stdin.

    Returns the command's stdout on success.

    Raises
    ------
    MicroCloudError
        If the command exits non-zero.
    """
    try:
        result = subprocess.run(
            ["microcloud", "preseed"],
            input=preseed_yaml,
            capture_output=True,
            text=True,
            check=True,
        )
        # Log full output even on success: `microcloud preseed` reports
        # per-step progress/warnings on stdout that are otherwise lost.
        logger.info("microcloud preseed stdout:\n%s", result.stdout.strip())
        if result.stderr.strip():
            logger.warning("microcloud preseed stderr:\n%s", result.stderr.strip())
        return result.stdout.strip()
    except FileNotFoundError as exc:
        raise MicroCloudError("microcloud CLI not found; is the snap installed?") from exc
    except subprocess.CalledProcessError as exc:
        stdout = (exc.stdout or "").strip()
        stderr = (exc.stderr or "").strip()
        # Log the full, untruncated output to the debug log: Juju status
        # messages are single-line and get truncated, so this is the only
        # place the complete preseed failure is visible.
        logger.error(
            "microcloud preseed failed (rc=%s)\nstdout:\n%s\nstderr:\n%s",
            exc.returncode,
            stdout,
            stderr,
        )
        raise MicroCloudError(
            f"microcloud preseed failed (rc={exc.returncode}): {stderr or stdout}"
        ) from exc
