"""Orchestrator and member applications on Prodstack MAAS nodes with disks.

Skipped unless pytest runs with --prodstack ("tox -e integration-prodstack"),
as it needs 7 amd64 MAAS machines spread over 3 availability zones, which CI
does not have. Run it from lxd-dev with "just prodstack".

6 orchestrator units take 3 Ceph disks each, and one diskless member unit
joins them: Ceph has 18 OSDs, the "remote" pool exists on every member, and
a VM on that pool live-migrates between the member and an orchestrator unit.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import jubilant
import pytest
import yaml

from tests.integration.helpers import (
    DEPLOY_TIMEOUT_IN_SECONDS,
    MICROCLOUD_CHARM,
    deploy_microcloud,
    leader_unit,
    lxd_members,
    machine_error,
    ssh,
    unit_hostnames,
    unit_names,
)

pytestmark = pytest.mark.prodstack

ORCHESTRATOR = MICROCLOUD_CHARM
MEMBER = "member"
ORCHESTRATOR_UNITS = 6
CEPH_DISKS = 3
INSTANCE = "multi-app-vm"
MIGRATION_TIMEOUT_IN_SECONDS = 10 * 60


def microceph_config() -> dict[str, str]:
    options = yaml.safe_load(Path("charmcraft.yaml").read_text())["config"]["options"]
    return {"snap-channel-microceph": options["snap-channel-microceph"]["default"]}


def instance_location(juju: jubilant.Juju) -> str:
    output = ssh(juju, leader_unit(juju), f"sudo lxc query /1.0/instances/{INSTANCE}")
    return json.loads(output)["location"]


@pytest.mark.juju_setup
def test_deploy(juju: jubilant.Juju, charm_path: Path, request: pytest.FixtureRequest) -> None:
    pool = request.config.getoption("--ceph-storage-pool")
    deploy_microcloud(
        juju,
        charm_path,
        num_units=ORCHESTRATOR_UNITS,
        config=microceph_config(),
        storage={"ceph": f"{pool},{CEPH_DISKS},4G"},
    )
    deploy_microcloud(
        juju, charm_path, num_units=1, config=microceph_config(), app=MEMBER, orchestrator=False
    )
    juju.integrate(f"{MEMBER}:orchestrator", f"{ORCHESTRATOR}:members")

    juju.wait(
        lambda status: (
            len(status.apps[ORCHESTRATOR].units) == ORCHESTRATOR_UNITS
            and jubilant.all_active(status, ORCHESTRATOR, MEMBER)
        ),
        error=lambda status: (
            jubilant.any_error(status, ORCHESTRATOR, MEMBER)
            or machine_error(status)
            or jubilant.any_blocked(status, ORCHESTRATOR, MEMBER)
        ),
        timeout=2 * DEPLOY_TIMEOUT_IN_SECONDS,
        delay=30,
    )


def test_every_orchestrator_disk_is_an_osd(juju: jubilant.Juju) -> None:
    output = ssh(juju, leader_unit(juju), "sudo microceph.ceph osd stat --format=json")
    stat = json.loads(output)
    assert stat["num_osds"] == ORCHESTRATOR_UNITS * CEPH_DISKS, stat
    assert stat["num_up_osds"] == stat["num_osds"], stat


def test_remote_pool_exists_on_every_member(juju: jubilant.Juju) -> None:
    leader = leader_unit(juju)
    members = set(lxd_members(juju))
    hostnames = {**unit_hostnames(juju, ORCHESTRATOR), **unit_hostnames(juju, MEMBER)}
    assert members == set(hostnames.values()), members

    for member in sorted(members):
        output = ssh(juju, leader, f"sudo lxc query '/1.0/storage-pools/remote?target={member}'")
        assert json.loads(output)["status"] == "Created", (member, output)


def test_a_remote_vm_live_migrates_between_applications(juju: jubilant.Juju) -> None:
    leader = leader_unit(juju)
    member = unit_hostnames(juju, MEMBER)[unit_names(juju, MEMBER)[0]]
    orchestrator = unit_hostnames(juju, ORCHESTRATOR)[leader]

    ssh(
        juju,
        leader,
        f"sudo lxc launch ubuntu:24.04 {INSTANCE} --vm --storage remote --target {member} "
        "-c migration.stateful=true -d root,size.state=4GiB",
    )
    try:
        deadline = time.monotonic() + MIGRATION_TIMEOUT_IN_SECONDS
        while "RUNNING" not in ssh(juju, leader, f"sudo lxc list {INSTANCE} -c s -f csv"):
            assert time.monotonic() < deadline, f"{INSTANCE} did not start"
            time.sleep(10)
        assert instance_location(juju) == member

        ssh(juju, leader, f"sudo lxc move {INSTANCE} --target {orchestrator}")
        assert instance_location(juju) == orchestrator
        assert "RUNNING" in ssh(juju, leader, f"sudo lxc list {INSTANCE} -c s -f csv")

        ssh(juju, leader, f"sudo lxc move {INSTANCE} --target {member}")
        assert instance_location(juju) == member
    finally:
        ssh(juju, leader, f"sudo lxc delete --force {INSTANCE} || true")
