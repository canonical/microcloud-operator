"""MicroCloud deployment and configuration tests.

The charm is deployed as a multi-unit, LXD-only MicroCloud (see
``helpers.LXD_ONLY_CONFIG``), which exercises the initiator/joiner handshake,
the shared session passphrase, and the peer-relation coordination that a
single-node deployment skips entirely.

Tests share one model for the whole module, so they run in file order: the
first deploys, the rest assert against what it built.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import jubilant
import pytest

from tests.integration.helpers import (
    MICROCLOUD_CHARM,
    assert_membership,
    assert_no_session_state,
    cluster_members,
    deploy_microcloud,
    member_addresses,
    run_action,
    snap_installed,
    ssh,
    unit_hostnames,
    unit_names,
    wait_active,
)

logger = logging.getLogger(__name__)


@pytest.mark.juju_setup
def test_deploy_microcloud(
    juju: jubilant.Juju,
    charm_path: Path,
    constraints: dict[str, str],
    num_units: int,
) -> None:
    """The charm installs the snaps and forms a cluster across every unit."""
    deploy_microcloud(juju, charm_path, num_units=num_units, constraints=constraints)
    wait_active(juju)

    status = juju.status()
    assert status.apps[MICROCLOUD_CHARM].app_status.current == "active"
    assert len(status.apps[MICROCLOUD_CHARM].units) == num_units


def test_every_unit_is_a_cluster_member(juju: jubilant.Juju, num_units: int) -> None:
    """MicroCloud membership matches the deployed Juju units, host for host.

    This is the invariant the charm blocks on in observe-only mode
    (``validate_membership``), so proving it here proves the bootstrap left the
    cluster in the state the charm expects to find on every later hook.
    """
    members = cluster_members(juju)
    assert len(members) == num_units, f"expected {num_units} members, got {members}"

    member_names = {m["name"] for m in members}
    hostnames = unit_hostnames(juju)
    assert member_names == set(hostnames.values()), (
        f"MicroCloud members {member_names} do not match unit hostnames {hostnames}"
    )

    for member in members:
        assert member["address"], f"member {member['name']} has no address"
        assert member["status"].upper() == "ONLINE", (
            f"member {member['name']} is {member['status']}"
        )


def test_lxd_cluster_is_formed(juju: jubilant.Juju, num_units: int) -> None:
    """LXD itself is clustered, not N standalone daemons.

    The status action reports what MicroCloud believes; this asks LXD directly,
    which is what actually has to be true for the deployment to be useful.
    """
    unit = unit_names(juju)[0]
    output = ssh(juju, unit, "sudo lxc cluster list --format json")
    try:
        lxd_members = json.loads(output)
    except json.JSONDecodeError as exc:
        raise AssertionError(f"lxc cluster list on {unit} returned non-JSON: {output!r}") from exc

    assert len(lxd_members) == num_units, f"expected {num_units} LXD members, got {lxd_members}"
    for member in lxd_members:
        assert member["status"] == "Online", f"LXD member {member['server_name']} is not online"

    assert {m["server_name"] for m in lxd_members} == {m["name"] for m in cluster_members(juju)}


def test_disabled_subsystems_are_absent(juju: jubilant.Juju) -> None:
    """Empty snap channels really do leave MicroCeph and MicroOVN uninstalled.

    Guards against the preseed silently gaining a ceph or ovn section, which
    would change what the cluster needs from its hardware.
    """
    for unit in unit_names(juju):
        assert snap_installed(juju, unit, "lxd"), f"{unit} is missing the lxd snap"
        assert snap_installed(juju, unit, "microcloud"), f"{unit} is missing the microcloud snap"
        assert not snap_installed(juju, unit, "microceph"), f"{unit} unexpectedly has microceph"
        assert not snap_installed(juju, unit, "microovn"), f"{unit} unexpectedly has microovn"


def test_dump_metrics_config_action(juju: jubilant.Juju) -> None:
    """The debugging action stays callable with no cos-agent relation."""
    results = run_action(juju, "dump-metrics-config")
    scrape_configs = json.loads(results["scrape-configs"])

    assert isinstance(scrape_configs, list)
    assert any(c["job_name"] == "microcloud-lxd" for c in scrape_configs)


def test_status_action_on_every_unit(juju: jubilant.Juju, num_units: int) -> None:
    """Leader and followers give consistent, repeatable read-only membership."""
    assert_membership(juju, num_units)
    before = member_addresses(juju)
    for _ in range(2):
        for unit in unit_names(juju):
            members = cluster_members(juju, unit)
            assert len(members) == num_units, (unit, members)
            assert {member["name"]: member["address"] for member in members} == before
            assert all(member["status"].upper() == "ONLINE" for member in members), members
    assert_membership(juju, num_units)
    assert member_addresses(juju) == before


def test_no_session_state_left_behind(juju: jubilant.Juju) -> None:
    """Forming the cluster leaves no join session, worker or passphrase behind."""
    assert_no_session_state(juju)
