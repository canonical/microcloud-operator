# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Start and follow join sessions that run outside of Juju hooks.

A Juju hook only commits its relation data once it exits, and a unit's own
write never triggers a hook on that unit. If the initiator ran its session
inside a hook, it could not tell the joiners that the session is open until
the session was over, and whichever unit learnt about the session last would
have nothing left to wake it up and dial in.

So each unit runs its part of a session in a background worker
(``join_session.py``), the way other machine charms run work that outlives a
hook: a detached process that dispatches an event on the unit once it is
done. The leader starts the worker and exits its hook straight away, which
commits the session marker that wakes the joiners. A joiner's worker can be
replaced when a new session supersedes the one it is waiting on.

The preseed document carries the session passphrase, so it is handed to the
worker on stdin and never written to disk.
"""

import contextlib
import json
import logging
import os
import signal
import subprocess
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import ops

import microcloud
from cluster import ClusterCoordinator, PeerSystem
from network import UnitNetwork

logger = logging.getLogger(__name__)

STATE_DIR = Path("/var/lib/charm-microcloud")
_RESULT_FILE = "session.json"
_LOG_FILE = "join-session.log"


class SessionError(Exception):
    """Raised when a join session worker cannot be started."""


def start(
    session_id: str,
    document: str,
    unit_name: str,
    charm_dir: Path,
    *,
    retry_until: float = 0,
    replacing: int = 0,
) -> int:
    """Start a worker running ``document`` for session ``session_id``.

    The worker retries until it succeeds or ``retry_until`` (seconds since the
    epoch) has passed, which lets a joiner wait for the initiator's session to
    open. ``replacing`` is the PID of this unit's previous worker, which is
    stopped first: a unit only ever takes part in the current session.

    Returns the new worker's PID as soon as it has been handed the document.
    """
    stop(replacing)

    STATE_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    clear()

    # juju-exec refuses to run in a hook context, which JUJU_CONTEXT_ID
    # would otherwise tell it this is.
    env = os.environ.copy()
    env.pop("JUJU_CONTEXT_ID", None)

    log = open(STATE_DIR / _LOG_FILE, "a")  # noqa: SIM115 - handed to the worker
    try:
        worker = subprocess.Popen(
            [
                "/usr/bin/python3",
                str(charm_dir / "src" / "join_session.py"),
                session_id,
                str(STATE_DIR / _RESULT_FILE),
                str(retry_until),
                unit_name,
                str(charm_dir),
            ],
            stdin=subprocess.PIPE,
            # Juju waits for a hook's output to close, so the worker must not
            # inherit it.
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            start_new_session=True,
        )
    except OSError as exc:
        raise SessionError(f"Cannot start the join session: {exc}") from exc
    finally:
        log.close()

    try:
        worker.stdin.write(document)
        worker.stdin.close()
    except OSError as exc:
        raise SessionError(f"Cannot hand the preseed to the join session: {exc}") from exc

    logger.info("Started join session %s (pid %d)", session_id, worker.pid)
    return worker.pid


def is_running(pid: int) -> bool:
    """Return True if ``pid`` is a join session worker that is still running.

    Checks the command line as well as the PID, which may have been reused
    since the worker was started.
    """
    if pid <= 0:
        return False
    try:
        return b"join_session.py" in Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return False


def stop(pid: int) -> None:
    """Stop the worker ``pid`` and its "microcloud preseed", if still running."""
    if not is_running(pid):
        return
    logger.info("Stopping join session worker %d", pid)
    # start() makes the worker a process group leader. Signal the whole group:
    # killing only the worker would leave its "microcloud preseed" running with
    # the old document. It may exit on its own in the meantime.
    with contextlib.suppress(OSError):
        os.killpg(pid, signal.SIGTERM)


def result(session_id: str) -> tuple[int, str] | None:
    """Return (exit code, output) of session ``session_id`` once it has ended.

    Returns None while it is still running, or if this unit never ran it.
    """
    try:
        data = json.loads((STATE_DIR / _RESULT_FILE).read_text())
    except (OSError, ValueError):
        return None
    if data.get("id") != session_id:
        return None
    return int(data["rc"]), str(data.get("output", ""))


def clear() -> None:
    """Forget the last session's result."""
    (STATE_DIR / _RESULT_FILE).unlink(missing_ok=True)


class JoinSessions:
    """Manages join sessions for forming or growing a MicroCloud cluster."""

    def __init__(
        self,
        unit: ops.Unit,
        coordinator: ClusterCoordinator,
        network: UnitNetwork,
        config: Mapping[str, Any],
        charm_dir: Path,
        stored: ops.StoredState,
        hold_status: Callable[[ops.StatusBase], None],
    ) -> None:
        self._unit = unit
        self._coordinator = coordinator
        self._network = network
        self._config = config
        self._charm_dir = charm_dir
        self._stored = stored
        self._hold_status = hold_status

    def pending(self, initialized: bool) -> list[PeerSystem]:
        """Return the systems not yet clustered.

        ``initialized`` is whether this unit is clustered. The published flags
        lag behind, so a clustered leader reads the membership back from
        MicroCloud instead.

        Raises microcloud.MicroCloudError if the membership cannot be read.
        """
        if initialized and self._unit.is_leader():
            members = {member.name for member in microcloud.list_members()}
            return self._coordinator.pending_systems(members)
        return self._coordinator.pending_systems()
