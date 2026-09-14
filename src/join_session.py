# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Run one unit's part in a join session, outside of a Juju hook.

Started in the background by the charm (see ``session``), with the preseed
document on stdin, so that the hook which starts it can exit and commit. It
runs "microcloud preseed", retrying until it succeeds or the deadline passes,
writes the outcome to a result file and dispatches ``join_session_ended`` on
the unit so the charm picks it up.

Standard library only: it runs under the system Python, not the charm's
virtual environment.

Usage: join_session.py SESSION_ID RESULT_FILE RETRY_UNTIL UNIT CHARM_DIR
"""

import json
import os
import subprocess
import sys
import time

JUJU_EXEC = "/usr/bin/juju-exec"
RETRY_DELAY = 3


def run(session_id: str, document: str, result_file: str, retry_until: float) -> int:
    """Run "microcloud preseed" until it succeeds or ``retry_until`` passes.

    Writes ``{"id", "rc", "output"}`` to ``result_file`` and returns the exit
    code of the last attempt. The output of every attempt is kept.
    """
    output = ""
    while True:
        attempt = subprocess.run(
            ["microcloud", "preseed"],
            input=document,
            capture_output=True,
            text=True,
            check=False,
        )
        output += attempt.stdout + attempt.stderr
        if attempt.returncode == 0 or time.time() >= retry_until:
            break
        time.sleep(RETRY_DELAY)

    partial = f"{result_file}.tmp"
    with open(partial, "w") as f:
        json.dump({"id": session_id, "rc": attempt.returncode, "output": output}, f)
    os.replace(partial, result_file)
    return attempt.returncode


def dispatch(unit: str, charm_dir: str) -> None:
    """Fire ``join_session_ended`` on ``unit``."""
    command = f"JUJU_DISPATCH_PATH=hooks/join_session_ended {charm_dir}/dispatch"
    subprocess.run([JUJU_EXEC, "-u", unit, command], check=False)


def main() -> None:
    session_id, result_file, retry_until, unit, charm_dir = sys.argv[1:]
    run(session_id, sys.stdin.read(), result_file, float(retry_until))
    dispatch(unit, charm_dir)


if __name__ == "__main__":
    main()
