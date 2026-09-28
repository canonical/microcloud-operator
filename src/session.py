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

The ``JoinSessions`` collaborator coordinates session leading and joining across
reconciliation passes, while the lower-level functions manage the worker processes
and result files.
"""

import contextlib
import json
import logging
import os
import signal
import subprocess
import time
import uuid
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import ops

import microcloud
import snap
from cluster import ClusterCoordinator, JoinSession, PeerSystem
from network import UnitNetwork
from preseed import PreseedInputs, SystemEntry, render

logger = logging.getLogger(__name__)

# How long past its session timeout a join session is assumed to have ended,
# for a new leader deciding whether it may open another.
SESSION_GRACE = 120

# How many join sessions in a row may fail before the leader stops opening
# the next one straight away and blocks, retrying only on later hooks.
_SESSION_RETRIES = 3

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
    """Manages join sessions for forming or growing a MicroCloud cluster.

    Juju only propagates a unit's relation-data writes to its peers
    once the writing hook exits, and a unit's own write never triggers a
    hook on that unit. A session opened inside a hook could therefore not
    be announced until it was already over. Instead:

    1. The leader starts "microcloud preseed" in the background (see
       ``session``), publishes the session it opened and exits. The
       commit triggers relation-changed on every other unit.
    2. Each joiner listed in the session starts dialling in from that
       hook, also in the background, retrying until the session opens.
       A later session replaces an attempt still waiting on this one.
    3. When the session ends, the leader picks up its result from a hook
       the background process fires, clears the session and opens the
       next one if units are still waiting to join.
    """

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

    def lead(self, initialized: bool, pending: list[PeerSystem], passphrase: str) -> str | None:
        """Leader side: follow the published join session, or open the next one."""
        address = self._network.bind_address()
        current = self._coordinator.session()
        failure = ""

        if current is not None and current.address != address:
            # Opened by a previous leader. This unit's own state for that
            # session is from dialling in to it, not the session's result.
            if time.time() < current.deadline:
                # It may still be running on that unit, and opening another
                # now would compete with it.
                self._hold_status(
                    ops.WaitingStatus("Waiting for the previous leader's join session to end")
                )
                return None
            self._coordinator.publish_session(None)

        elif current is not None:
            # Check for a result before whether the worker is running: the
            # hook the worker fires once it is done runs while the worker is
            # still waiting on that hook.
            outcome = result(current.id)
            if outcome is None and self._worker_running(current.id):
                self._hold_status(
                    ops.MaintenanceStatus(_session_message(current, microcloud.hostname()))
                )
                return None

            self._coordinator.publish_session(None)
            clear()
            if outcome is not None and outcome[0] != 0:
                logger.error("Join session %s failed:\n%s", current.id, outcome[1])
                failure = _last_line(outcome[1])
                self._stored.session_failures += 1
                # Nothing else wakes the leader once its session is cleared,
                # so open the next one from this hook, unless sessions keep
                # failing: then block and leave retrying to later hooks.
                if self._stored.session_failures >= _SESSION_RETRIES:
                    return f"Join session failed: {failure}"
            else:
                self._stored.session_failures = 0

        if current is not None:
            # The membership read at the start of this hook can predate the
            # session that just ended: its result may have landed while this
            # hook was already running. Read it again, or the next session
            # would list units that have just joined, and that never dial in.
            initialized = microcloud.is_initialized()
            try:
                pending = self.pending(initialized)
            except microcloud.MicroCloudError as exc:
                return f"Cannot read MicroCloud members: {exc}"

        if not pending:
            return None

        # Units already in the cluster have nothing left to get ready for,
        # and in a cluster formed outside Juju they never report ready.
        if not self._coordinator.all_ready(pending if initialized else None):
            self._hold_status(ops.WaitingStatus("Waiting for all peers to be ready"))
            return None

        # "microcloud preseed" cannot add systems to a cluster without
        # MicroCeph: it panics looking up the cluster's Ceph networks. Say so,
        # rather than open sessions that are bound to fail.
        if initialized and not snap.is_installed("microceph"):
            return (
                f"Cannot add {len(pending)} unit(s): MicroCloud cannot add systems "
                "to a cluster without MicroCeph"
            )

        # Only a clustered unit can add others to the cluster, so an
        # unclustered leader must not take over from a cluster that already
        # exists: it would bootstrap a second one instead.
        if not initialized and self._coordinator.any_initialized():
            return (
                f"Leader {microcloud.hostname()} is not a MicroCloud member "
                "but other units are; cannot initiate"
            )

        # MicroCloud infers whether to form a cluster or add to one from
        # whether the initiator is itself listed under "systems". Listing
        # every unit forms a cluster; listing only the units not yet
        # clustered adds them to the one the initiator belongs to.
        listed = pending if initialized else self._coordinator.all_systems()
        timeout = int(self._config.get("session-timeout", 300))
        opened = JoinSession(
            id=uuid.uuid4().hex,
            address=address,
            systems=[system.name for system in listed],
            deadline=time.time() + timeout + SESSION_GRACE,
        )

        document = render(self._preseed_inputs(address, passphrase, _system_entries(listed)))
        try:
            self._start_worker(opened.id, document)
        except SessionError as exc:
            return str(exc)

        self._coordinator.publish_session(opened)
        message = _session_message(opened, microcloud.hostname())
        if failure:
            message = f"{message}; retrying after: {failure}"
        self._hold_status(ops.MaintenanceStatus(message))
        return None

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


def _session_message(opened: JoinSession, initiator: str) -> str:
    """Describe a join session in progress for the leader's status."""
    # A session that does not list its initiator adds to an existing cluster.
    if initiator not in opened.systems:
        return f"Joining {len(opened.systems)} unit(s) to the MicroCloud cluster"
    return f"Forming the MicroCloud cluster with {len(opened.systems)} units"


def _last_line(output: str) -> str:
    """Return the last non-empty line of command output, for a status message."""
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    return lines[-1] if lines else "no output"
