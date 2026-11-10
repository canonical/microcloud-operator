"""COS integration scenarios for an externally formed MicroCloud cluster."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Iterator
from pathlib import Path

import jubilant
import pytest
import pytest_jubilant
from tests.integration import cos
from tests.integration.helpers import (
    CEPH_METRICS_URL,
    LXD_METRICS_URL,
    OVN_METRICS_URL,
    assert_local_metrics,
    assert_no_ovn_exporter,
    assert_ovn_exporter_active,
    assert_status,
    deploy_charm,
    scrape_configs,
    unit_names,
    wait_active,
)
from tests.integration.vms import MicroCloudVM, VMEnvironment

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def cos_model(
    juju_factory: pytest_jubilant.JujuFactory,
    k8s_controller: str,
) -> Iterator[jubilant.Juju]:
    """Deploy and offer COS Lite once for every scenario in this module."""
    if not k8s_controller:
        pytest.skip("--k8s-controller or $K8S_CONTROLLER is required for COS tests")
    juju = juju_factory.get_juju("cos", controller=k8s_controller or None)
    cos.deploy_cos(juju)
    cos.offer_cos(juju)
    yield juju


def _deploy_scenario(
    juju_factory: pytest_jubilant.JujuFactory,
    lxd_controller: str,
    vm_env: VMEnvironment,
    charm_path: Path,
    *,
    name: str,
    with_ceph: bool,
    with_ovn: bool,
) -> tuple[jubilant.Juju, MicroCloudVM]:
    juju = juju_factory.get_juju(
        f"{name}-{uuid.uuid4().hex[:6]}", controller=lxd_controller or None
    )
    juju.model_config({"update-status-hook-interval": "10s"})

    vm = vm_env.launch_vm(name, with_ceph=with_ceph, with_ovn=with_ovn)
    try:
        vm.install_microcloud_snaps()
        vm.preseed_init()
        machine = vm.add_machine(juju)
        deploy_charm(juju, charm_path, machine)
        wait_active(juju, message="Waiting for observability relation")
        assert_status(juju, unit_names(juju)[0], 1, {vm.hostname})
    except BaseException:
        vm.cleanup(juju)
        raise
    return juju, vm


@pytest.fixture
def full_cloud(
    juju_factory: pytest_jubilant.JujuFactory,
    lxd_controller: str,
    vm_env: VMEnvironment,
    charm_path: Path,
) -> Iterator[tuple[jubilant.Juju, MicroCloudVM]]:
    juju, vm = _deploy_scenario(
        juju_factory,
        lxd_controller,
        vm_env,
        charm_path,
        name="mc-cos",
        with_ceph=True,
        with_ovn=True,
    )
    try:
        yield juju, vm
    finally:
        vm.cleanup(juju)


@pytest.fixture
def minimal_cloud(
    juju_factory: pytest_jubilant.JujuFactory,
    lxd_controller: str,
    vm_env: VMEnvironment,
    charm_path: Path,
) -> Iterator[tuple[jubilant.Juju, MicroCloudVM]]:
    juju, vm = _deploy_scenario(
        juju_factory,
        lxd_controller,
        vm_env,
        charm_path,
        name="mc-min",
        with_ceph=False,
        with_ovn=False,
    )
    try:
        yield juju, vm
    finally:
        vm.cleanup(juju)


@pytest.mark.cos
@pytest.mark.vm
def test_full_stack_metrics_and_logs(
    full_cloud: tuple[jubilant.Juju, MicroCloudVM],
    cos_model: jubilant.Juju,
) -> None:
    """MicroCloud with every component reports metrics and logs through COS."""
    juju, vm = full_cloud
    cos.relate_cos(juju, cos_model)
    wait_active(juju, message="Observability active", timeout=cos.COS_TIMEOUT_IN_SECONDS)

    unit = unit_names(juju)[0]
    assert_status(juju, unit, 1, {vm.hostname})

    jobs = {config["job_name"] for config in scrape_configs(juju, unit)}
    assert any(job.startswith("microcloud-lxd-") for job in jobs), jobs
    assert any(job.startswith("microcloud-microceph-") for job in jobs), jobs
    assert any(job.startswith("microcloud-microovn-") for job in jobs), jobs

    assert_local_metrics(juju, unit, LXD_METRICS_URL)
    assert_local_metrics(juju, unit, CEPH_METRICS_URL)
    assert_local_metrics(juju, unit, OVN_METRICS_URL)
    assert_ovn_exporter_active(juju, unit)

    cos.assert_prometheus_targets(cos_model, {"lxd", "microceph", "microovn"})
    cos.assert_loki_has_lxd_logs(cos_model)


@pytest.mark.cos
@pytest.mark.vm
def test_without_ceph_ovn_metrics_and_logs(
    minimal_cloud: tuple[jubilant.Juju, MicroCloudVM],
    cos_model: jubilant.Juju,
) -> None:
    """A MicroCloud without Ceph and OVN advertises only the LXD metrics job."""
    juju, vm = minimal_cloud
    cos.relate_cos(juju, cos_model)
    wait_active(juju, message="Observability active", timeout=cos.COS_TIMEOUT_IN_SECONDS)

    unit = unit_names(juju)[0]
    assert_status(juju, unit, 1, {vm.hostname})

    configs = scrape_configs(juju, unit)
    assert len(configs) == 1, configs
    assert configs[0]["job_name"].startswith("microcloud-lxd-"), configs

    assert_local_metrics(juju, unit, LXD_METRICS_URL)
    assert_no_ovn_exporter(juju, unit)

    cos.assert_prometheus_targets(cos_model, {"lxd"})
    cos.assert_loki_has_lxd_logs(cos_model)
