"""Shared helpers for the MicroCloud charm integration tests."""

from __future__ import annotations

import json
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

# Forming the cluster pulls the microcloud and lxd snaps on every unit and runs
# the initiator/joiner handshake, which is well past jubilant's default wait.
DEPLOY_TIMEOUT_IN_SECONDS = 40 * 60


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


def wait_active(juju: jubilant.Juju, *, recovering: bool = False) -> None:
    """Wait for convergence, allowing an old blocked status during recovery."""
    juju.wait(
        lambda status: jubilant.all_active(status, MICROCLOUD_CHARM),
        # A hook error is terminal: concierge disables automatic hook retries,
        # so without this the wait runs the full timeout before failing.
        error=lambda status: (
            jubilant.any_error(status, MICROCLOUD_CHARM)
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
    assert "members-error" not in results, results.get("members-error")
    assert "members" in results, f"status action reported no members: {results}"
    return json.loads(results["members"])


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
