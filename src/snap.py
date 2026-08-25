# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Snap installation helpers for the microcloud charm (deployment mode).

Installs the MicroCloud component snaps at the configured channels using a
shared cohort so that later refreshes stay synchronized across members, then
holds refreshes to avoid uncoordinated auto-updates.
"""

import logging
import subprocess

logger = logging.getLogger(__name__)


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
