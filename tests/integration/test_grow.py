"""Grow an externally formed MicroCloud cluster behind an already deployed charm."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
import pytest_jubilant
from tests.integration.helpers import (
    MICROCLOUD_CHARM,
    assert_cluster_list,
    assert_status,
    deploy_charm,
    unit_names,
    wait_active,
    wait_blocked,
    wait_units_active,
)
from tests.integration.vms import VMEnvironment

logger = logging.getLogger(__name__)


@pytest.mark.vm
def test_deploy_before_cluster_then_grow(
    juju_factory: pytest_jubilant.JujuFactory,
    lxd_controller: str,
    vm_env: VMEnvironment,
    charm_path: Path,
) -> None:
    """The charm waits for MicroCloud, then follows a manually grown cluster."""
    juju = juju_factory.get_juju("grow", controller=lxd_controller or None)
    first = vm_env.launch_vm("mc-grow-a", with_ceph=True, with_ovn=True)
    second = None
    try:
        juju.model_config({"update-status-hook-interval": "10s"})

        first_machine = first.add_machine(juju)
        deploy_charm(juju, charm_path, first_machine)
        wait_blocked(juju, "MicroCloud is not initialized on this node")

        logger.info("Forming a single-node MicroCloud beneath the existing charm")
        first.install_microcloud_snaps()
        first.preseed_init()
        wait_active(juju, message="Waiting for observability relation")
        assert_status(juju, unit_names(juju)[0], 1, {first.hostname})
        assert_cluster_list(juju, unit_names(juju)[0], {first.hostname})

        logger.info("Adding a second MicroCloud member")
        second = vm_env.launch_vm("mc-grow-b", with_ceph=True, with_ovn=True)
        second.install_microcloud_snaps()
        first.preseed_add_member(second)

        second_machine = second.add_machine(juju)
        second.add_unit(juju, second_machine, app=MICROCLOUD_CHARM)
        wait_units_active(juju, 2)

        expected = {first.hostname, second.hostname}
        for unit in unit_names(juju):
            assert_status(juju, unit, 2, expected)
            assert_cluster_list(juju, unit, expected)
    finally:
        first.cleanup(juju)
        if second is not None:
            second.cleanup(juju)
