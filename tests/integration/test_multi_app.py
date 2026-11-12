"""Orchestrator and member applications joined over "microcloud_cluster".

All test machines are amd64, so a second application stands in for a second
architecture. Growing the cluster needs MicroCeph, so every application runs
it without disks, as test_grow.py does, and the units fit in containers.

Tests share one model and run in file order:

- the orchestrator ("microcloud", 3 units) and a member application
  ("member", 2 units) are related before the cluster exists, and form one
  cluster of 5;
- the orchestrator units take LXD's control-plane role, and member units stay
  spares, including one added later;
- the guards block what they should, each on an application of its own that
  is removed again. The storage guards need disks, so they are unit tested
  only;
- removing and re-adding the relation leaves the cluster untouched;
- the orchestrator leader stopping mid-session, and a member that keeps
  failing its sessions, both recover;
- removing a member unit leaves a stale MicroCloud member, which blocks the
  orchestrator leader. This one runs last, as it leaves the model blocked.

The "Standalone" scenario is the rest of the integration suite, which deploys
with orchestrator=true.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import jubilant
import pytest
import yaml

from tests.integration.helpers import (
    DEPLOY_TIMEOUT_IN_SECONDS,
    MICROCLOUD_CHARM,
    cluster_members,
    deploy_microcloud,
    leader_relations,
    leader_unit,
    lxd_members,
    machine_error,
    member_addresses,
    run_action,
    ssh,
    unit_hostnames,
    unit_names,
)

ORCHESTRATOR = MICROCLOUD_CHARM
MEMBER = "member"
ORCHESTRATOR_UNITS = 3
MEMBER_UNITS = 2

ROLE = "control-plane"
DATABASE_ROLES = {"database-leader", "database-voter", "database-standby"}
ROLE_TIMEOUT_IN_SECONDS = 5 * 60
BLOCKED_TIMEOUT_IN_SECONDS = 10 * 60


def microceph_config() -> dict[str, str]:
    """Enable MicroCeph at the charm's default channel, without CephFS."""
    options = yaml.safe_load(Path("charmcraft.yaml").read_text())["config"]["options"]
    return {
        "snap-channel-microceph": options["snap-channel-microceph"]["default"],
        "ceph-cephfs": "false",
    }


def relate(juju: jubilant.Juju, member: str = MEMBER, orchestrator: str = ORCHESTRATOR) -> None:
    juju.integrate(f"{member}:orchestrator", f"{orchestrator}:members")


def unrelate(juju: jubilant.Juju, member: str = MEMBER, orchestrator: str = ORCHESTRATOR) -> None:
    juju.remove_relation(f"{member}:orchestrator", f"{orchestrator}:members")


def remove_app(juju: jubilant.Juju, app: str) -> None:
    juju.remove_application(app, destroy_storage=True, force=True)
    juju.wait(lambda status: app not in status.apps, timeout=DEPLOY_TIMEOUT_IN_SECONDS)


def wait_apps_active(juju: jubilant.Juju, counts: dict[str, int]) -> None:
    """Wait until each application has its unit count, all of them active."""

    def converged(status: jubilant.Status) -> bool:
        return all(
            app in status.apps
            and len(status.apps[app].units) == count
            and jubilant.all_active(status, app)
            for app, count in counts.items()
        )

    juju.wait(
        converged,
        error=lambda status: (
            jubilant.any_error(status, *counts)
            or machine_error(status)
            or jubilant.any_blocked(status, *counts)
        ),
        timeout=DEPLOY_TIMEOUT_IN_SECONDS,
        delay=10,
    )


def unit_message(status: jubilant.Status, unit: str) -> str:
    app = unit.split("/")[0]
    return status.apps[app].units[unit].workload_status.message


def wait_message(
    juju: jubilant.Juju,
    unit: str | Callable[[], str],
    check: Callable[[str], bool],
    timeout: float = BLOCKED_TIMEOUT_IN_SECONDS,
) -> str:
    """Wait until ``unit``'s workload status message passes ``check``."""
    name = unit if isinstance(unit, str) else unit()
    # One success: a session's messages can pass before a second poll.
    juju.wait(
        lambda status: check(unit_message(status, name)), timeout=timeout, delay=5, successes=1
    )
    return unit_message(juju.status(), name)


def all_hostnames(juju: jubilant.Juju) -> dict[str, str]:
    return {**unit_hostnames(juju, ORCHESTRATOR), **unit_hostnames(juju, MEMBER)}


def assert_one_cluster(juju: jubilant.Juju) -> None:
    """Check MicroCloud and LXD list exactly the units of both applications."""
    hostnames = set(all_hostnames(juju).values())
    members = cluster_members(juju)
    assert {member["name"] for member in members} == hostnames, members
    assert all(member["status"].upper() == "ONLINE" for member in members), members

    lxd = lxd_members(juju)
    assert set(lxd) == hostnames, lxd
    assert all(member["status"] == "Online" for member in lxd.values()), lxd


def roles(juju: jubilant.Juju) -> dict[str, set[str]]:
    return {name: set(member["roles"]) for name, member in lxd_members(juju).items()}


def database_members(current: dict[str, set[str]]) -> set[str]:
    return {name for name, member_roles in current.items() if member_roles & DATABASE_ROLES}


def wait_for_roles(
    juju: jubilant.Juju, check: Callable[[dict[str, set[str]]], bool], what: str
) -> dict[str, set[str]]:
    deadline = time.monotonic() + ROLE_TIMEOUT_IN_SECONDS
    while True:
        current = roles(juju)
        if check(current):
            return current
        assert time.monotonic() < deadline, f"timed out waiting for {what}: {current}"
        time.sleep(10)


def members_relation_data(juju: jubilant.Juju, member_app: str) -> dict[str, str]:
    """Return what the orchestrator leader published on its relation to ``member_app``.

    ``juju show-unit`` reports the other application's data, so read a member unit.
    """
    for relation in leader_relations(juju, member_app):
        if relation["endpoint"] == "orchestrator":
            return relation.get("application-data", {})
    return {}


def machine_of(juju: jubilant.Juju, unit: str) -> str:
    app = unit.split("/")[0]
    return juju.status().apps[app].units[unit].machine


# ---------------------------------------------------------------------------
# Formation, placement and scale-out
# ---------------------------------------------------------------------------


@pytest.mark.juju_setup
def test_members_related_before_bootstrap_join_the_one_cluster(
    juju: jubilant.Juju, charm_path: Path, constraints: dict[str, str]
) -> None:
    """Formation and ordering: member units never form a cluster of their own."""
    deploy_microcloud(
        juju,
        charm_path,
        num_units=ORCHESTRATOR_UNITS,
        constraints=constraints,
        config=microceph_config(),
    )
    deploy_microcloud(
        juju,
        charm_path,
        num_units=MEMBER_UNITS,
        config=microceph_config(),
        app=MEMBER,
        orchestrator=False,
    )
    relate(juju)

    wait_apps_active(juju, {ORCHESTRATOR: ORCHESTRATOR_UNITS, MEMBER: MEMBER_UNITS})

    assert_one_cluster(juju)
    assert run_action(juju, "status", unit_names(juju, MEMBER)[0])["cluster-mode"] == "member"
    assert run_action(juju, "status")["cluster-mode"] == "orchestrator"
    joined = json.loads(run_action(juju, "status")["joined-units"])
    assert sorted(joined.values()) == unit_names(juju, MEMBER), joined


def test_database_roles_stay_on_orchestrator_units(juju: jubilant.Juju) -> None:
    """Database placement and scale-out: member units are, and join as, spares."""
    for unit in unit_names(juju, ORCHESTRATOR):
        run_action(juju, "add-control-plane-role", unit)
    orchestrator_hosts = set(unit_hostnames(juju, ORCHESTRATOR).values())

    wait_for_roles(
        juju,
        lambda current: database_members(current) <= orchestrator_hosts,
        "database roles to move to the orchestrator units",
    )

    juju.cli("add-unit", MEMBER)
    wait_apps_active(juju, {ORCHESTRATOR: ORCHESTRATOR_UNITS, MEMBER: MEMBER_UNITS + 1})

    assert_one_cluster(juju)
    current = roles(juju)
    assert database_members(current) <= orchestrator_hosts, current


def test_events_reach_other_member_units(juju: jubilant.Juju) -> None:
    """Events: a spare member unit sees what another member unit does."""
    watcher, actor = unit_names(juju, MEMBER)[:2]

    with ThreadPoolExecutor(max_workers=1) as pool:
        monitor = pool.submit(
            ssh,
            juju,
            watcher,
            "sudo timeout 60 lxc monitor --type=lifecycle --format=json || true",
        )
        time.sleep(15)
        ssh(juju, actor, "sudo lxc profile create multi-app-events")
        output = monitor.result()

    ssh(juju, actor, "sudo lxc profile delete multi-app-events")
    assert "multi-app-events" in output, output


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------


def test_orchestrator_cannot_change_after_the_cluster_formed(juju: jubilant.Juju) -> None:
    juju.config(ORCHESTRATOR, {"orchestrator": "false"})
    message = wait_message(
        juju, lambda: leader_unit(juju, ORCHESTRATOR), lambda m: "cannot change" in m
    )
    assert message == (
        "orchestrator cannot change after the cluster formed (formed as orchestrator)"
    )

    # The units stay blocked until their hooks run, so blocked is no failure here.
    juju.config(ORCHESTRATOR, {"orchestrator": "true"})
    juju.wait(
        lambda status: jubilant.all_active(status, ORCHESTRATOR),
        error=lambda status: jubilant.any_error(status, ORCHESTRATOR),
        timeout=DEPLOY_TIMEOUT_IN_SECONDS,
        delay=10,
    )


def test_second_orchestrator_on_members_blocks(juju: jubilant.Juju, charm_path: Path) -> None:
    deploy_microcloud(juju, charm_path, num_units=1, config=microceph_config(), app="rogue")
    relate(juju, member="rogue")

    message = wait_message(
        juju, lambda: leader_unit(juju, ORCHESTRATOR), lambda m: m.startswith("rogue on")
    )
    assert message == "rogue on members has orchestrator=true"

    remove_app(juju, "rogue")
    wait_apps_active(juju, {ORCHESTRATOR: ORCHESTRATOR_UNITS})


def test_members_relation_without_microceph_blocks(juju: jubilant.Juju, charm_path: Path) -> None:
    deploy_microcloud(juju, charm_path, num_units=1, app="noceph")
    deploy_microcloud(juju, charm_path, num_units=1, app="noceph-member", orchestrator=False)
    relate(juju, member="noceph-member", orchestrator="noceph")

    message = wait_message(juju, "noceph/0", lambda m: "MicroCeph" in m)
    assert message == "members relation needs MicroCeph (snap-channel-microceph)"

    remove_app(juju, "noceph-member")
    remove_app(juju, "noceph")


# ---------------------------------------------------------------------------
# Relation removal, leader change and failures
# ---------------------------------------------------------------------------


def test_removing_and_readding_the_relation_keeps_the_cluster(juju: jubilant.Juju) -> None:
    before = member_addresses(juju)
    member_count = len(unit_names(juju, MEMBER))

    unrelate(juju)
    for unit in unit_names(juju, MEMBER):
        wait_message(juju, unit, lambda m: m == "Cluster ready; orchestrator relation removed")
    wait_apps_active(juju, {ORCHESTRATOR: ORCHESTRATOR_UNITS, MEMBER: member_count})

    joined = json.loads(run_action(juju, "status")["joined-units"])
    assert all(unit.endswith("(detached)") for unit in joined.values()), joined

    relate(juju)
    wait_apps_active(juju, {ORCHESTRATOR: ORCHESTRATOR_UNITS, MEMBER: member_count})
    assert member_addresses(juju) == before
    assert "join-session" not in members_relation_data(juju, MEMBER)
    joined = json.loads(run_action(juju, "status")["joined-units"])
    assert not any(unit.endswith("(detached)") for unit in joined.values()), joined


def test_member_joins_when_the_orchestrator_leaders_agent_stops(juju: jubilant.Juju) -> None:
    """Stopping the leader's agent ends its session, and the next leader joins the member."""
    leader = leader_unit(juju, ORCHESTRATOR)
    machine = machine_of(juju, leader)
    count = len(unit_names(juju, MEMBER)) + 1

    # Stopping the agent can kill a running hook, which then has to be retried.
    juju.model_config({"automatically-retry-hooks": True})
    juju.cli("add-unit", MEMBER)
    wait_message(juju, leader, lambda m: m.startswith("Joining 1 unit(s)"))
    ssh(juju, leader, f"sudo systemctl stop jujud-machine-{machine}")
    try:
        juju.wait(
            lambda status: (
                len(status.apps[MEMBER].units) == count and jubilant.all_active(status, MEMBER)
            ),
            error=lambda status: jubilant.any_error(status, MEMBER),
            timeout=DEPLOY_TIMEOUT_IN_SECONDS,
            delay=10,
        )
    finally:
        ssh(juju, leader, f"sudo systemctl start jujud-machine-{machine}")

    juju.wait(
        lambda status: jubilant.all_active(status, ORCHESTRATOR),
        timeout=DEPLOY_TIMEOUT_IN_SECONDS,
        delay=10,
    )
    juju.model_config(reset="automatically-retry-hooks")
    wait_apps_active(juju, {ORCHESTRATOR: ORCHESTRATOR_UNITS, MEMBER: count})
    assert_one_cluster(juju)
    assert "join-session" not in members_relation_data(juju, MEMBER)


def test_a_failing_member_is_excluded_until_the_relation_is_readded(
    juju: jubilant.Juju, charm_path: Path
) -> None:
    """Failure limit: 3 failed sessions exclude a unit, and re-relating clears it."""
    # Short sessions, so 3 of them fail in minutes.
    juju.config(ORCHESTRATOR, {"session-timeout": "60"})
    deploy_microcloud(
        juju, charm_path, num_units=1, config=microceph_config(), app="flaky", orchestrator=False
    )
    wait_message(juju, "flaky/0", lambda m: m.startswith("Waiting for orchestrator relation"))

    # A join takes seconds, so cut the route to the initiator before the first
    # session. Stopping MicroCloud instead would keep the unit from being listed.
    leader = leader_unit(juju, ORCHESTRATOR)
    address = juju.status().apps[ORCHESTRATOR].units[leader].public_address
    ssh(juju, "flaky/0", f"sudo ip route add blackhole {address}")
    relate(juju, member="flaky")
    wait_message(juju, leader, lambda m: m.startswith("Joining 1 unit(s)"))

    deadline = time.monotonic() + BLOCKED_TIMEOUT_IN_SECONDS
    while True:
        failures = json.loads(members_relation_data(juju, "flaky").get("join-failures", "{}"))
        if failures.get("count", 0) >= 3:
            break
        assert time.monotonic() < deadline, failures
        time.sleep(10)

    ssh(juju, "flaky/0", f"sudo ip route del blackhole {address}")
    message = wait_message(juju, "flaky/0", lambda m: m.startswith("excluded after"))
    assert message.startswith("excluded after 3 failed join sessions")
    # An excluded unit stops dialing. Its worker would otherwise retry until
    # the last session's deadline, and count against whichever session the
    # orchestrator has open by then.
    assert not ssh(juju, "flaky/0", "pgrep -f '[j]oin_session.py' || true").strip()

    unrelate(juju, member="flaky")
    juju.wait(lambda status: not status.apps["flaky"].relations.get("orchestrator"), timeout=300)
    relate(juju, member="flaky")
    wait_apps_active(juju, {ORCHESTRATOR: ORCHESTRATOR_UNITS, "flaky": 1})
    names = {member["name"] for member in cluster_members(juju)}
    assert set(unit_hostnames(juju, "flaky").values()) <= names

    juju.config(ORCHESTRATOR, reset="session-timeout")


# ---------------------------------------------------------------------------
# Member scale-down (last: it leaves the orchestrator leader blocked)
# ---------------------------------------------------------------------------


def test_removing_a_member_unit_blocks_on_the_stale_member(juju: jubilant.Juju) -> None:
    unit = unit_names(juju, MEMBER)[-1]
    hostname = unit_hostnames(juju, MEMBER)[unit]

    juju.cli("remove-unit", unit, "--force", "--no-wait", "--no-prompt")
    message = wait_message(juju, lambda: leader_unit(juju, ORCHESTRATOR), lambda m: hostname in m)
    assert f"MicroCloud member {hostname!r} is not deployed as a Juju unit" in message
