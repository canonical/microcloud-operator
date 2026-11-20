"""Growing a formed cluster, and LXD's control-plane role.

MicroCloud can only add systems to a cluster that runs MicroCeph: without it,
"microcloud preseed" panics looking up the cluster's Ceph networks. This module
therefore deploys MicroCeph alongside LXD. No Ceph disks are attached, so the
units still fit in containers; only MicroCeph's own services run on them.

Tests share one model and run in file order: 3 units form the cluster and
take the role, then 2 units added at once join it as spares. One of them takes
the role and stands in for a stopped voter. The charm's failure-domain write
keeps the role, and removing roles until 2 holders remain turns the mode off.
"""

from __future__ import annotations

import time
from pathlib import Path

import jubilant
import pytest

from tests.integration.helpers import (
    DATABASE_ROLES,
    MICROCLOUD_CHARM,
    ROLE,
    ROLE_TIMEOUT_IN_SECONDS,
    as_bool,
    assert_membership,
    assert_no_session_state,
    database_members,
    deploy_microcloud,
    leader_unit,
    lxd_members,
    machine_zones,
    member_addresses,
    microceph_config,
    microceph_members,
    role_holders,
    roles,
    run_action,
    run_update_status,
    ssh,
    unit_hostnames,
    unit_names,
    wait_active,
    wait_for_roles,
    wait_units_active,
)

INITIAL_UNITS = 3
ADDED_UNITS = 2


def run_on(juju: jubilant.Juju, units: list[str], action: str) -> list[dict]:
    return [run_action(juju, action, unit) for unit in units]


def wait_leader_message(juju: jubilant.Juju, message: str) -> None:
    """Wait until the leader's workload status message reads ``message``."""
    leader = leader_unit(juju)
    juju.wait(
        lambda status: (
            status.apps[MICROCLOUD_CHARM].units[leader].workload_status.message == message
        ),
        timeout=60,
    )


def assert_grown(juju: jubilant.Juju, count: int, before: dict[str, str]) -> None:
    """Check every service agrees on ``count`` members and none were replaced."""
    assert_membership(juju, count)
    assert microceph_members(juju) == set(unit_hostnames(juju).values())
    assert_no_session_state(juju)

    after = member_addresses(juju)
    assert {name: after.get(name) for name in before} == before, (before, after)


@pytest.mark.juju_setup
def test_deploy_with_microceph(
    juju: jubilant.Juju, charm_path: Path, constraints: dict[str, str]
) -> None:
    """The cluster forms with MicroCeph running on every unit."""
    deploy_microcloud(
        juju,
        charm_path,
        num_units=INITIAL_UNITS,
        constraints=constraints,
        config=microceph_config(),
    )
    wait_units_active(juju, INITIAL_UNITS)

    assert_grown(juju, INITIAL_UNITS, before={})


def test_activation(juju: jubilant.Juju) -> None:
    """3 role holders turn control-plane mode on."""
    units = unit_names(juju)
    results = run_on(juju, units, "add-control-plane-role")

    assert all(as_bool(result["changed"]) for result in results), results
    assert results[-1]["control-plane-mode"] == "active (3 role holders)"
    assert role_holders(roles(juju)) == set(unit_hostnames(juju).values())
    assert run_action(juju, "status")["control-plane-mode"] == "active (3 role holders)"


def test_idempotency(juju: jubilant.Juju) -> None:
    """Adding the role again changes nothing."""
    [result] = run_on(juju, unit_names(juju)[:1], "add-control-plane-role")

    assert not as_bool(result["changed"]), result
    assert result["control-plane-mode"] == "active (3 role holders)"


def test_units_added_at_once_join_as_spares(juju: jubilant.Juju) -> None:
    """Units added together join the existing cluster, and get no database role."""
    before = member_addresses(juju)
    units_before = set(unit_names(juju))
    count = INITIAL_UNITS + ADDED_UNITS

    juju.cli("add-unit", MICROCLOUD_CHARM, "--num-units", str(ADDED_UNITS))
    wait_units_active(juju, count)

    assert_grown(juju, count, before)
    hostnames = unit_hostnames(juju)
    current = roles(juju)
    for unit in set(unit_names(juju)) - units_before:
        assert ROLE not in current[hostnames[unit]], current
        assert not current[hostnames[unit]] & DATABASE_ROLES, current
    assert database_members(current) <= role_holders(current), current


def test_spare_role_holder_is_promoted(juju: jubilant.Juju) -> None:
    """A role holder beyond the voter and standby slots replaces a stopped voter."""
    leader = leader_unit(juju)
    hostnames = unit_hostnames(juju)
    current = roles(juju)
    spare_unit = next(unit for unit, name in hostnames.items() if ROLE not in current[name])
    spare = hostnames[spare_unit]

    ssh(juju, leader, "sudo lxc config set cluster.max_standby=0")
    try:
        run_on(juju, [spare_unit], "add-control-plane-role")
        current = wait_for_roles(
            juju,
            lambda r: len(role_holders(r)) == 4 and len(database_members(r)) == 3,
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
        time.sleep(5)
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
    current = roles(juju)
    first, second = [
        unit for unit in unit_names(juju) if unit != leader and ROLE in current[hostnames[unit]]
    ][:2]
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
