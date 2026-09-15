"""Growing a formed cluster with "juju add-unit".

MicroCloud can only add systems to a cluster that runs MicroCeph: without it,
"microcloud preseed" panics looking up the cluster's Ceph networks. This module
therefore deploys MicroCeph alongside LXD. No Ceph disks are attached, so the
units still fit in containers; only MicroCeph's own services run on them.

Tests share one model and run in file order: the first forms the cluster and
the second grows it. Two units are added at once, which covers a session
adding several systems while keeping the model small enough for a CI runner.
"""

from __future__ import annotations

from pathlib import Path

import jubilant
import pytest
import yaml

from tests.integration.helpers import (
    MICROCLOUD_CHARM,
    assert_membership,
    assert_no_session_state,
    deploy_microcloud,
    member_addresses,
    microceph_members,
    unit_hostnames,
    wait_units_active,
)

INITIAL_UNITS = 2


def microceph_config() -> dict[str, str]:
    """Enable MicroCeph at the charm's default channel, without CephFS.

    CephFS needs OSDs, and these units have no disks to give it.
    """
    options = yaml.safe_load(Path("charmcraft.yaml").read_text())["config"]["options"]
    return {
        "snap-channel-microceph": options["snap-channel-microceph"]["default"],
        "ceph-cephfs": "false",
    }


def assert_grown(juju: jubilant.Juju, count: int, before: dict[str, str]) -> None:
    """Check every service agrees on ``count`` members and none were replaced."""
    assert_membership(juju, count)
    assert microceph_members(juju) == set(unit_hostnames(juju).values())
    assert_no_session_state(juju)

    after = member_addresses(juju)
    assert {name: after.get(name) for name in before} == before, (before, after)


@pytest.mark.juju_setup
def test_deploy_with_microceph(
    juju: jubilant.Juju,
    charm_path: Path,
    constraints: dict[str, str],
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


def test_add_several_units(juju: jubilant.Juju) -> None:
    """Units added at once join the existing cluster together, not a new one."""
    before = member_addresses(juju)
    count = len(before) + 2

    juju.cli("add-unit", MICROCLOUD_CHARM, "--num-units", "2")
    wait_units_active(juju, count)

    assert_grown(juju, count, before)
