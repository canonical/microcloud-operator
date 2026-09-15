"""Shared helpers for the MicroCloud charm integration tests."""

from __future__ import annotations

import json
import shlex
import time
from pathlib import Path
from typing import Any

import jubilant

MICROCLOUD_CHARM = "microcloud"

# LXD-only profile.
#
# MicroCeph wants real block devices and MicroOVN wants openvswitch kernel
# modules; neither is dependable on a CI runner. Leaving both snap channels
# empty makes the charm omit the corresponding preseed sections entirely, which
# is also what lets the units run as LXD containers rather than virtual
# machines - a nested container has no spare disks to hand to Ceph anyway.
LXD_ONLY_CONFIG: dict[str, str] = {
    "snap-channel-microceph": "",
    "snap-channel-microovn": "",
}

# Forming or growing the cluster pulls the snaps on every new unit and runs a
# join session, well past jubilant's default wait. The slowest step on CI takes
# under ten minutes; anything much longer is stuck, and waiting for the full
# job timeout only hides why.
DEPLOY_TIMEOUT_IN_SECONDS = 20 * 60


def deploy_microcloud(
    juju: jubilant.Juju,
    charm_path: Path,
    num_units: int,
    constraints: dict[str, str] | None = None,
    config: dict[str, str] | None = None,
) -> None:
    """Deploy MicroCloud without waiting, so callers can test blocked states."""
    if constraints:
        juju.cli("set-model-constraints", *[f"{k}={v}" for k, v in constraints.items()])

    juju.deploy(
        charm_path,
        app=MICROCLOUD_CHARM,
        num_units=num_units,
        config={**LXD_ONLY_CONFIG, **(config or {})},
    )


def machine_error(status: jubilant.Status) -> bool:
    """Return True if Juju failed to create a machine.

    A unit on such a machine only ever reads "waiting for machine", so
    without this a wait runs its full timeout before failing. Running out of
    disk for a new container is the usual cause.
    """
    return any(
        machine.machine_status.current == "provisioning error"
        for machine in status.machines.values()
    )


def wait_active(juju: jubilant.Juju, *, recovering: bool = False) -> None:
    """Wait for convergence, allowing an old blocked status during recovery."""
    juju.wait(
        lambda status: jubilant.all_active(status, MICROCLOUD_CHARM),
        # A hook error is terminal: concierge disables automatic hook retries,
        # so without this the wait runs the full timeout before failing.
        error=lambda status: (
            jubilant.any_error(status, MICROCLOUD_CHARM)
            or machine_error(status)
            or (not recovering and jubilant.any_blocked(status, MICROCLOUD_CHARM))
        ),
        timeout=DEPLOY_TIMEOUT_IN_SECONDS,
        delay=10,
    )


def ssh(juju: jubilant.Juju, unit: str, command: str) -> str:
    """Run ``command`` on ``unit`` and return its stdout.

    "juju ssh" allocates a pseudo-terminal whenever a terminal is attached,
    which prefixes the output with control bytes, rewrites the line endings and
    appends "Connection to <ip> closed.". Whether that happens depends on how
    pytest was started, so force it off and get the same bytes everywhere.
    """
    return juju.cli("ssh", "--pty=false", unit, command)


def unit_names(juju: jubilant.Juju, app: str = MICROCLOUD_CHARM) -> list[str]:
    """Return every unit name of ``app``, sorted."""
    return sorted(juju.status().apps[app].units)


def leader_unit(juju: jubilant.Juju, app: str = MICROCLOUD_CHARM) -> str:
    """Return the name of the leader unit of ``app``."""
    for name, unit in juju.status().apps[app].units.items():
        if unit.leader:
            return name
    raise AssertionError(f"no leader unit found for {app}")


def run_action(juju: jubilant.Juju, action: str, unit: str | None = None) -> dict[str, Any]:
    """Run a charm action on the leader (or ``unit``) and return its results."""
    task = juju.run(unit or leader_unit(juju), action)
    assert task.success, f"action {action} failed: {task.message}"
    return task.results


def as_bool(value: Any) -> bool:
    """Coerce a Juju action result to a bool.

    Juju renders action results through YAML, so a boolean may come back as a
    real bool or as its string form depending on the client version.
    """
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "yes", "1")


def cluster_members(juju: jubilant.Juju, unit: str | None = None) -> list[dict[str, str]]:
    """Return the MicroCloud members reported by the "status" action."""
    results = run_action(juju, "status", unit)
    assert as_bool(results["initialized"]), f"MicroCloud is not initialized: {results}"
    assert "microcloud-members-error" not in results, results.get("microcloud-members-error")
    assert "microcloud-members" in results, f"status action reported no members: {results}"
    return json.loads(results["microcloud-members"])


def unit_hostnames(juju: jubilant.Juju) -> dict[str, str]:
    """Map each unit name to the hostname its machine reports.

    The charm matches Juju units to MicroCloud members on hostname, so the
    tests verify that same mapping rather than trusting unit ordering.
    """
    return {unit: ssh(juju, unit, "hostname").strip() for unit in unit_names(juju)}


def snap_installed(juju: jubilant.Juju, unit: str, snap: str) -> bool:
    """Return True if ``snap`` is installed on ``unit``'s machine.

    Matches the first column of ``snap list``, skipping its header row::

        Name        Version   Rev    Tracking       Publisher   Notes
        core24      20250808  1382   latest/stable  canonical   base
        lxd         6.5       35507  6/stable       canonical   -
        microcloud  2.1.0     1145   3/stable       canonical   -
    """
    # A transport error must fail the test, not masquerade as an absent snap.
    installed = ssh(juju, unit, "snap list")
    return any(line.split()[0] == snap for line in installed.splitlines()[1:] if line.split())


def member_addresses(juju: jubilant.Juju) -> dict[str, str]:
    """Map each MicroCloud member name to its address.

    A stable identity for the cluster, so a test can assert that an operation
    left membership untouched.
    """
    return {member["name"]: member["address"] for member in cluster_members(juju)}


def assert_membership(juju: jubilant.Juju, expected_count: int) -> None:
    """Check both the MicroCloud and LXD clusters against Juju."""
    hostnames = unit_hostnames(juju)
    assert len(hostnames) == expected_count, hostnames
    members = cluster_members(juju)
    assert len(members) == expected_count, members
    assert {member["name"] for member in members} == set(hostnames.values()), members
    for member in members:
        assert member["address"], member
        assert member["status"].upper() == "ONLINE", member

    lxd_members = json.loads(ssh(juju, leader_unit(juju), "sudo lxc cluster list --format json"))
    assert len(lxd_members) == expected_count, lxd_members
    assert {member["server_name"] for member in lxd_members} == set(hostnames.values())
    assert all(member["status"] == "Online" for member in lxd_members), lxd_members


def wait_units_active(juju: jubilant.Juju, count: int) -> None:
    """Wait until the application has ``count`` units, all of them active.

    Checking the count as well matters right after "juju add-unit": until the
    new units show up, the existing ones already read as all active.
    """

    def converged(status: jubilant.Status) -> bool:
        app = status.apps.get(MICROCLOUD_CHARM)
        return (
            app is not None
            and len(app.units) == count
            and jubilant.all_active(status, MICROCLOUD_CHARM)
        )

    juju.wait(
        converged,
        error=lambda status: (
            jubilant.any_error(status, MICROCLOUD_CHARM)
            or machine_error(status)
            or jubilant.any_blocked(status, MICROCLOUD_CHARM)
        ),
        timeout=DEPLOY_TIMEOUT_IN_SECONDS,
        delay=10,
    )


def microceph_members(juju: jubilant.Juju) -> set[str]:
    """Return the member names "microceph status" reports on the leader.

    Each member is summarised on a line of its own::

        MicroCeph deployment summary:
        - juju-5cdc9f-0 (10.42.254.71)
          Services: mds, mgr, mon
          Disks: 0
    """
    output = ssh(juju, leader_unit(juju), "sudo microceph status")
    return {line.split()[1] for line in output.splitlines() if line.startswith("- ")}


def cluster_app_data(juju: jubilant.Juju) -> dict[str, str]:
    """Return the application data the units share on their peer relation."""
    unit = leader_unit(juju)
    info = json.loads(juju.cli("show-unit", unit, "--format", "json"))[unit]
    for relation in info.get("relation-info", []):
        if relation["endpoint"] == "cluster":
            return relation.get("application-data", {})
    raise AssertionError(f"{unit} has no cluster relation: {info}")


def session_passphrase(juju: jubilant.Juju) -> str:
    """Return the join session passphrase the leader keeps in a Juju secret."""
    secret_id = cluster_app_data(juju)["session-passphrase-secret-id"]
    secret = json.loads(juju.cli("show-secret", secret_id, "--reveal", "--format", "json"))
    [details] = secret.values()
    content = details["content"]
    return (content.get("Data") or content.get("data"))["passphrase"]


def assert_no_session_state(juju: jubilant.Juju) -> None:
    """Check that no join session is left behind once the cluster converged.

    No session is still published for the units, no worker is still running
    on any of them, and the passphrase the preseed carries was never written
    to the charm's state or temporary files.
    """
    assert "join-session" not in cluster_app_data(juju), cluster_app_data(juju)

    # A worker exits right after the hook it fires, which may have only just
    # set the unit active.
    deadline = time.monotonic() + 60
    while True:
        workers = {
            unit: ssh(juju, unit, "pgrep -fa '[j]oin_session.py' || true").strip()
            for unit in unit_names(juju)
        }
        if not any(workers.values()) or time.monotonic() > deadline:
            break
        time.sleep(5)
    assert not any(workers.values()), workers

    pattern = shlex.quote(session_passphrase(juju))
    for unit in unit_names(juju):
        found = ssh(
            juju,
            unit,
            f"sudo grep -rlF -e {pattern} /var/lib/charm-microcloud /tmp 2>/dev/null || true",
        )
        assert not found.strip(), (unit, found)
