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
from preseed import PreseedInputs, SystemEntry, render

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

    def _worker_running(self, session_id: str) -> bool:
        """Return True while this unit's worker for ``session_id`` is running."""
        return self._stored.worker_session == session_id and is_running(self._stored.worker_pid)

    def _start_worker(self, session_id: str, document: str, retry_until: float = 0) -> None:
        """Replace this unit's join session worker with one for ``session_id``."""
        self._stored.worker_pid = start(
            session_id,
            document,
            self._unit.name,
            self._charm_dir,
            retry_until=retry_until,
            replacing=self._stored.worker_pid,
        )
        self._stored.worker_session = session_id

    def _preseed_inputs(
        self, initiator_address: str, passphrase: str, systems: list[SystemEntry]
    ) -> PreseedInputs:
        """Build the PreseedInputs for this unit's preseed document."""
        with_ceph_storage = any(s.storage_ceph_paths for s in systems)

        return PreseedInputs(
            initiator_address=initiator_address,
            session_passphrase=passphrase,
            systems=systems,
            session_timeout=int(self._config.get("session-timeout", 300)),
            # In preseed mode this is how long the initiator waits for every
            # listed system to reach out, not just a multicast setting. Its
            # 60s default would expire long before a joiner held up in
            # another hook dials in, so wait for the whole session.
            lookup_timeout=int(self._config.get("session-timeout", 300)),
            with_ceph=bool(self._config.get("snap-channel-microceph", "")),
            ceph_cephfs=bool(self._config.get("ceph-cephfs", False)),
            # "microcloud preseed" rejects a public/internal network without
            # Ceph storage disks ("Cannot specify a Ceph public network
            # without Ceph storage disks"), so only derive these from the
            # ceph-public/ceph-internal bindings when at least one system
            # has a "ceph" Juju storage volume attached. Otherwise Juju's
            # default-space fallback for an unbound extra-binding would
            # still resolve to *some* subnet and produce an invalid
            # preseed.
            ceph_public_network=self._network.space_network_cidr("ceph-public")
            if with_ceph_storage
            else "",
            ceph_internal_network=self._network.space_network_cidr("ceph-internal")
            if with_ceph_storage
            else "",
            with_ovn=bool(self._config.get("snap-channel-microovn", "")),
            ovn_ipv4_gateway=self._config.get("ovn-ipv4-gateway", ""),
            ovn_ipv4_range=self._config.get("ovn-ipv4-range", ""),
            ovn_ipv6_gateway=self._config.get("ovn-ipv6-gateway", ""),
            ovn_dns_servers=self._config.get("ovn-dns-servers", ""),
            storage_wipe=bool(self._config.get("storage-wipe", False)),
            storage_encrypt=bool(self._config.get("storage-encrypt", False)),
        )

    def join(self, passphrase: str) -> str | None:
        """Joiner side: dial in to the published join session if listed in it.

        The attempt runs in the background, like the initiator's session, so
        that a later session can replace it: a joiner still waiting on a
        session that failed would otherwise miss the next one. Once it
        succeeds, the hook it fires finds this unit clustered and publishes
        that, which is what tells the leader.
        """
        current = self._coordinator.session()
        if current is None:
            self._hold_status(ops.WaitingStatus("Waiting for the leader to open a join session"))
            return None

        if microcloud.hostname() not in current.systems:
            self._hold_status(ops.WaitingStatus("Waiting for the next join session"))
            return None

        # A leader that lost leadership mid-session is still its initiator.
        if current.address == self._network.bind_address():
            self._hold_status(
                ops.WaitingStatus("Waiting for the join session this unit opened to end")
            )
            return None

        outcome = result(current.id)
        if outcome is not None and outcome[0] == 0:
            # MicroCloud reports this unit as clustered only once the other
            # services have joined too, a little after the attempt succeeds.
            self._hold_status(ops.MaintenanceStatus("Joined the MicroCloud cluster"))
            return None
        if outcome is not None:
            # The leader opens the next session; this unit just waits for it.
            logger.error("Joining session %s failed:\n%s", current.id, outcome[1])
            self._hold_status(
                ops.WaitingStatus(
                    f"Waiting for the next join session; joining failed: {_last_line(outcome[1])}"
                )
            )
            return None

        if not self._worker_running(current.id):
            # Render exactly the systems the initiator listed, so this
            # document matches the one the session was opened with.
            by_name = {system.name: system for system in self._coordinator.all_systems()}
            listed = [by_name[name] for name in current.systems if name in by_name]
            document = render(
                self._preseed_inputs(current.address, passphrase, _system_entries(listed))
            )
            try:
                self._start_worker(current.id, document, retry_until=current.deadline)
            except SessionError as exc:
                return str(exc)

        self._hold_status(ops.MaintenanceStatus("Joining the MicroCloud cluster"))
        return None


def _system_entries(systems: list[PeerSystem]) -> list[SystemEntry]:
    """Convert published peer identities into preseed system entries."""
    return [
        SystemEntry(
            name=system.name,
            address=system.address,
            ovn_uplink_interface=system.ovn_uplink_interface,
            ovn_underlay_ip=system.ovn_underlay_ip,
            storage_local_path=system.storage_local_path,
            storage_ceph_paths=system.storage_ceph_paths,
        )
        for system in systems
    ]


def _last_line(output: str) -> str:
    """Return the last non-empty line of command output, for a status message."""
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    return lines[-1] if lines else "no output"
