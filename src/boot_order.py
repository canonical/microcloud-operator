# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Start Ceph only once BGP can carry its traffic.

On a host whose Ceph addresses sit on "lo" and are advertised by BIRD,
those addresses are unreachable until BIRD has a BGP session up. At boot
systemd may start MicroCeph's daemons first: monitors then fail to reach
their peers, and OSDs are marked down until the routes arrive.

When BIRD is enabled on the host, each MicroCeph daemon gets a systemd
drop-in that orders it after BIRD and, before it starts, waits for BIRD to
report an established BGP session. The wait has a limit and never fails,
so a host that boots while every router is down still starts Ceph.
"""

import logging
import os
import subprocess
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

BIRD_UNIT = "snap.microovn.bird.service"
# The MicroCeph daemons that use the Ceph networks.
CEPH_UNITS = tuple(f"snap.microceph.{service}.service" for service in ("mon", "mgr", "mds", "osd"))
SYSTEMD_DIR = Path("/etc/systemd/system")
DROPIN_NAME = "10-microcloud-after-bgp.conf"
WAIT_SCRIPT = Path("/usr/local/sbin/microcloud-wait-for-bgp")

# Seconds to wait for a BGP session before starting Ceph anyway.
WAIT_LIMIT = 120

WAIT_SCRIPT_CONTENT = """#!/bin/sh
# Written by the MicroCloud charm. Changes are overwritten.
# Wait up to $1 seconds for BIRD to have an established BGP session. Always
# succeeds, so that a host whose routers are all down still starts Ceph.
# Each birdc call has its own timeout, so one that hangs cannot hold the
# daemon past the limit.
limit="${1:-120}"
# Nothing to wait for once BIRD is disabled or MicroOVN removed, for example
# after the charm has gone and left these files behind.
if ! systemctl is-enabled --quiet snap.microovn.bird.service 2>/dev/null; then
    exit 0
fi
deadline=$(($(date +%s) + limit))
while [ "$(date +%s)" -lt "$deadline" ]; do
    if timeout 5 /snap/bin/microovn.birdc show protocols 2>/dev/null | grep -q Established; then
        exit 0
    fi
    sleep 1
done
echo "No established BGP session after ${limit}s; starting anyway" >&2
exit 0
"""


class BootOrderError(Exception):
    """Raised when the drop-ins cannot be written or systemd reloaded."""


def dropin() -> str:
    """Return the drop-in that starts a MicroCeph daemon after BGP is up."""
    return (
        "# Written by the MicroCloud charm. Changes are overwritten.\n"
        "[Unit]\n"
        f"After={BIRD_UNIT}\n"
        "\n"
        "[Service]\n"
        # "-": a missing script, as when systemd still has a removed drop-in
        # loaded, must not stop Ceph from starting.
        f"ExecStartPre=-{WAIT_SCRIPT} {WAIT_LIMIT}\n"
        # The wait counts towards the start timeout.
        f"TimeoutStartSec={WAIT_LIMIT + 180}\n"
    )


def _dropin_path(unit: str, root: Path) -> Path:
    return root / f"{unit}.d" / DROPIN_NAME


def _write(path: Path, content: str, mode: int) -> bool:
    """Replace ``path`` with ``content``. Returns True if it changed."""
    try:
        if path.read_text() == content and (path.stat().st_mode & 0o777) == mode:
            return False
    except OSError:
        pass
    # Through a new file renamed into place: a daemon starting now keeps
    # reading the old script, and a failed write leaves the old file whole.
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
        try:
            with os.fdopen(fd, "w") as handle:
                handle.write(content)
            os.chmod(tmp, mode)
            os.replace(tmp, path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
    except OSError as exc:
        raise BootOrderError(f"Cannot write {path}: {exc}") from exc
    return True


def _remove(path: Path) -> bool:
    """Remove ``path`` if it exists. Returns True if it did."""
    try:
        path.unlink()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise BootOrderError(f"Cannot remove {path}: {exc}") from exc
    return True


def _systemctl(*args: str) -> str:
    try:
        return subprocess.run(
            ["systemctl", *args],
            capture_output=True,
            text=True,
            check=True,
            timeout=60,
        ).stdout
    except subprocess.CalledProcessError as exc:
        raise BootOrderError(f"systemctl {args[0]}: {exc.stderr.strip()}") from exc
    except (OSError, subprocess.SubprocessError) as exc:
        raise BootOrderError(f"systemctl {args[0]}: {exc}") from exc


def _needs_reload() -> bool:
    """Return True if systemd has not loaded the Ceph units as they are on disk.

    This catches a reload that failed after the files were last changed. It
    runs on every reconcile, even where no drop-in is wanted, so a failure is
    logged rather than raised: it must not hold up the rest of the charm.
    """
    try:
        out = _systemctl("show", "--property=NeedDaemonReload", "--value", *CEPH_UNITS)
    except BootOrderError as exc:
        logger.warning("Cannot tell whether systemd needs a reload: %s", exc)
        return False
    return "yes" in out.split()


def ensure(enabled: bool, root: Path = SYSTEMD_DIR, script: Path = WAIT_SCRIPT) -> bool:
    """Install the drop-ins if ``enabled``, or remove them. Returns True if any changed.

    systemd is reloaded after a change, or when an earlier reload failed, so
    the next start of each daemon uses the files on disk. Running daemons
    are not restarted.
    """
    changed = False
    if enabled:
        changed |= _write(script, WAIT_SCRIPT_CONTENT, 0o755)
        for unit in CEPH_UNITS:
            changed |= _write(_dropin_path(unit, root), dropin(), 0o644)
    else:
        for unit in CEPH_UNITS:
            changed |= _remove(_dropin_path(unit, root))
        changed |= _remove(script)

    if changed or _needs_reload():
        _systemctl("daemon-reload")
    return changed
