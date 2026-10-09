"""Assigning LXD's control-plane role with the charm's actions.

Growing the cluster needs MicroCeph, so this module deploys it without disks,
as test_grow.py does. Tests share one model and run in file order: 3 units
take the role, a fourth joins as a spare, then takes the role and stands in
for a stopped voter. The charm's failure-domain write keeps the role, and
removing roles until 2 holders remain turns the mode off.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path

import jubilant
import pytest
import yaml

from tests.integration.helpers import (
    MICROCLOUD_CHARM,
    as_bool,
    deploy_microcloud,
    leader_unit,
    lxd_members,
    machine_zones,
    run_action,
    ssh,
    unit_hostnames,
    unit_names,
    wait_active,
    wait_units_active,
)

INITIAL_UNITS = 3
ROLE = "control-plane"
DATABASE_ROLES = {"database-leader", "database-voter", "database-standby"}

# LXD applies role changes on its heartbeat and marks a member offline after
# its offline threshold, both well under a minute by default.
ROLE_TIMEOUT_IN_SECONDS = 5 * 60


def microceph_config() -> dict[str, str]:
    """Enable MicroCeph at the charm's default channel, without CephFS."""
    options = yaml.safe_load(Path("charmcraft.yaml").read_text())["config"]["options"]
    return {
        "snap-channel-microceph": options["snap-channel-microceph"]["default"],
        "ceph-cephfs": "false",
    }


def roles(juju: jubilant.Juju) -> dict[str, set[str]]:
    """Map each LXD member to its roles."""
    return {name: set(member["roles"]) for name, member in lxd_members(juju).items()}


def wait_for_roles(
    juju: jubilant.Juju, check: Callable[[dict[str, set[str]]], bool], what: str
) -> dict[str, set[str]]:
    """Poll LXD until ``check`` holds for the members' roles."""
    deadline = time.monotonic() + ROLE_TIMEOUT_IN_SECONDS
    while True:
        current = roles(juju)
        if check(current):
            return current
        assert time.monotonic() < deadline, f"timed out waiting for {what}: {current}"
        time.sleep(10)


def database_members(current: dict[str, set[str]]) -> set[str]:
    return {name for name, member_roles in current.items() if member_roles & DATABASE_ROLES}


def holders(current: dict[str, set[str]]) -> set[str]:
    return {name for name, member_roles in current.items() if ROLE in member_roles}


def run_on(juju: jubilant.Juju, units: list[str], action: str) -> list[dict]:
    return [run_action(juju, action, unit) for unit in units]


def run_update_status(juju: jubilant.Juju, unit: str) -> None:
    """Run the update-status hook on ``unit`` now, instead of waiting for Juju."""
    charm_dir = f"/var/lib/juju/agents/unit-{unit.replace('/', '-')}/charm"
    juju.cli(
        "exec",
        "--unit",
        unit,
        "--",
        f"JUJU_DISPATCH_PATH=hooks/update-status {charm_dir}/dispatch",
    )


def wait_leader_message(juju: jubilant.Juju, message: str) -> None:
    """Wait until the leader's workload status message reads ``message``."""
    leader = leader_unit(juju)
    juju.wait(
        lambda status: (
            status.apps[MICROCLOUD_CHARM].units[leader].workload_status.message == message
        ),
        timeout=60,
    )


@pytest.mark.juju_setup
def test_deploy(juju: jubilant.Juju, charm_path: Path, constraints: dict[str, str]) -> None:
    deploy_microcloud(
        juju,
        charm_path,
        num_units=INITIAL_UNITS,
        constraints=constraints,
        config=microceph_config(),
    )
    wait_units_active(juju, INITIAL_UNITS)


def test_activation(juju: jubilant.Juju) -> None:
    """3 role holders turn control-plane mode on."""
    units = unit_names(juju)
    results = run_on(juju, units, "add-control-plane-role")

    assert all(as_bool(result["changed"]) for result in results), results
    assert results[-1]["control-plane-mode"] == "active (3 role holders)"
    assert holders(roles(juju)) == set(unit_hostnames(juju).values())
    assert run_action(juju, "status")["control-plane-mode"] == "active (3 role holders)"


def test_idempotency(juju: jubilant.Juju) -> None:
    """Adding the role again changes nothing."""
    [result] = run_on(juju, unit_names(juju)[:1], "add-control-plane-role")

    assert not as_bool(result["changed"]), result
    assert result["control-plane-mode"] == "active (3 role holders)"


def test_scale_out_joins_as_spare(juju: jubilant.Juju) -> None:
    """A member that joins while the mode is on gets no database role."""
    before = set(unit_names(juju))
    juju.cli("add-unit", MICROCLOUD_CHARM)
    wait_units_active(juju, INITIAL_UNITS + 1)

    [new_unit] = set(unit_names(juju)) - before
    new_member = unit_hostnames(juju)[new_unit]
    current = roles(juju)
    assert ROLE not in current[new_member], current
    assert not current[new_member] & DATABASE_ROLES, current
    assert database_members(current) <= holders(current), current


def test_spare_role_holder_is_promoted(juju: jubilant.Juju) -> None:
    """A role holder beyond the voter and standby slots replaces a stopped voter."""
    leader = leader_unit(juju)
    hostnames = unit_hostnames(juju)
    current = roles(juju)
    [spare_unit] = [unit for unit, name in hostnames.items() if ROLE not in current[name]]
    spare = hostnames[spare_unit]

    ssh(juju, leader, "sudo lxc config set cluster.max_standby=0")
    try:
        run_on(juju, [spare_unit], "add-control-plane-role")
        current = wait_for_roles(
            juju,
            lambda r: len(holders(r)) == 4 and len(database_members(r)) == 3,
            "3 voters among 4 role holders",
        )
        assert spare not in database_members(current), current

        voter_unit = next(
            unit
            for unit, name in hostnames.items()
            if unit != leader and "database-voter" in current[name]
        )
        ssh(juju, voter_unit, "sudo snap stop lxd.daemon")
        try:
            wait_for_roles(juju, lambda r: "database-voter" in r[spare], f"{spare} to be promoted")
        finally:
            ssh(juju, voter_unit, "sudo snap start lxd.daemon")
            ssh(juju, voter_unit, "sudo lxd waitready --timeout=300")
    finally:
        ssh(juju, leader, "sudo lxc config unset cluster.max_standby")

    deadline = time.monotonic() + ROLE_TIMEOUT_IN_SECONDS
    while any(member["status"] != "Online" for member in lxd_members(juju).values()):
        assert time.monotonic() < deadline, lxd_members(juju)
        time.sleep(10)
    for unit in unit_names(juju):
        run_update_status(juju, unit)
    wait_active(juju, recovering=True)


def test_failure_domain_write_keeps_the_role(juju: jubilant.Juju) -> None:
    """Setting a failure domain from the zone leaves the role in place."""
    current = roles(juju)
    hostnames = unit_hostnames(juju)
    unit = next(unit for unit, name in hostnames.items() if ROLE in current[name])
    hostname = hostnames[unit]
    zone = machine_zones(juju)[hostname]

    ssh(juju, unit, f"sudo lxc cluster failure-domain set {hostname} not-{zone}")
    run_update_status(juju, unit)

    member = lxd_members(juju)[hostname]
    assert member["failure_domain"] == zone, member
    assert ROLE in member["roles"], member

    # Every CI machine shares one zone, so the leader has no coverage to warn about.
    run_update_status(juju, leader_unit(juju))
    wait_leader_message(juju, "Cluster ready")


def test_removal_turns_the_mode_off(juju: jubilant.Juju) -> None:
    """Below 3 role holders, members without the role get database roles again."""
    hostnames = unit_hostnames(juju)
    leader = leader_unit(juju)
    first, second = [unit for unit in unit_names(juju) if unit != leader][:2]
    demoted = hostnames[first]

    # With 3 role holders left the mode stays on, so LXD demotes the first
    # member to a spare.
    [result] = run_on(juju, [first], "remove-control-plane-role")
    assert result["control-plane-mode"] == "active (3 role holders)"
    wait_for_roles(juju, lambda r: not r[demoted] & DATABASE_ROLES, f"{demoted} to be a spare")

    [result] = run_on(juju, [second], "remove-control-plane-role")
    assert result["control-plane-mode"] == "inactive (2 of 3 role holders)"
    wait_for_roles(
        juju, lambda r: bool(r[demoted] & DATABASE_ROLES), f"{demoted} to get a database role"
    )

    run_update_status(juju, leader)
    wait_leader_message(juju, "Cluster ready; control-plane: mode inactive (2 of 3 role holders)")
