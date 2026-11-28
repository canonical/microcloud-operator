# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collector-to-planner integration tests; no charm deployment or enforcement.

The default tests use synthetic host-source files and the production example
profile. Only architecture and block-device stat metadata are simulated; the
collector reads CPU, cache, NUMA, device and cgroup files normally.

Run the default tests with ``tox -e integration -- -k cpu_planner``; no Juju
controller or built charm is required. This module overrides the shared
``report_resources`` fixture locally to avoid creating an unused Juju model.

Opt-in checks use environment variables. Run pytest directly in the existing
integration environment so tox's environment filtering cannot discard them.
On representative supported hardware:

    export CPU_PLANNER_OSD_DEVICES="/dev/nvme0n1 /dev/nvme1n1"
    export CPU_PLANNER_OVN_DEVICES="eth0 eth1"
    CPU_PLANNER_LIVE=1 PYTHONPATH=src .tox/integration/bin/pytest tests/integration/test_cpu_planner.py

Device lists are shell-style whitespace-separated names; use the host's configured
devices. These checks are read-only. Unknown profiles, unknown device locality
and insufficient eligible CPUs fail rather than substitute synthetic data.
Repeat them with LXD running, stopped and absent on appropriately prepared hosts.

The separate ``CPU_PLANNER_LXD_LIFECYCLE=1`` opt-in requires a disposable Linux
host with snapd and no existing LXD installation:

    CPU_PLANNER_LXD_LIFECYCLE=1 PYTHONPATH=src .tox/integration/bin/pytest tests/integration/test_cpu_planner.py

It installs LXD, tests absent/running/stopped states, and removes its installation
afterward. It refuses to change a pre-existing installation. Its test-only sizing
profile is not added to the production registry; passing does not validate VM
hardware. Never enable it on a shared or production host. Opt-ins default to
disabled; use 0 to disable or 1 to enable, and other values fail explicitly.
"""

from __future__ import annotations

import os
import shlex
import shutil
import stat
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass
from fractions import Fraction
from math import ceil
from pathlib import Path
from unittest.mock import Mock

import pytest

import cpu_planner
from cpu_planner import (
    CPUPlan,
    CPUPlannerError,
    CPUProfile,
    CPURequirement,
    HostTopology,
    allocate_cpu_pools,
    collect_host_topology,
    plan_cpu_pools,
)

COMPONENTS = (
    frozenset({"lxd"}),
    frozenset({"lxd", "microceph"}),
    frozenset({"lxd", "microovn"}),
    frozenset({"lxd", "microceph", "microovn"}),
)


@pytest.fixture(scope="module", autouse=True)
def report_resources() -> None:
    """Override shared model reporting: these host tests do not need Juju."""


def _enabled(variable: str) -> bool:
    value = os.environ.get(variable, "0")
    assert value in ("0", "1"), f"{variable} must be 0 or 1, not {value!r}"
    return value == "1"


def _write(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value)


def _ids(value: str) -> frozenset[int]:
    result = set()
    for part in value.strip().split(","):
        if not part:
            continue
        bounds = [int(number) for number in part.split("-")]
        result.update(range(bounds[0], bounds[-1] + 1))
    return frozenset(result)


@dataclass(frozen=True)
class SyntheticHost:
    sysfs: Path
    proc: Path
    cgroup: Path
    osds: tuple[str, ...]
    nics: tuple[str, ...]

    def collect(
        self, components: frozenset[str], *, previous: HostTopology | None = None
    ) -> HostTopology:
        return collect_host_topology(
            sysfs_root=self.sysfs,
            proc_root=self.proc,
            cgroup_root=self.cgroup,
            osd_devices=self.osds if "microceph" in components else (),
            ovn_devices=self.nics if "microovn" in components else (),
            reserved_cpus=frozenset(),
            previous=previous,
        )


@pytest.fixture
def synthetic_host(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SyntheticHost:
    sysfs, proc, cgroup = (tmp_path / name for name in ("sys", "proc", "cgroup"))
    cpu_root = sysfs / "devices/system/cpu"
    _write(cpu_root / "present", "0-575")
    _write(cpu_root / "online", "0-575")
    _write(cpu_root / "isolated", "")
    _write(sysfs / "devices/system/node/online", "0-1")
    _write(sysfs / "devices/system/node/has_memory", "0-1")
    _write(cgroup / "cpuset.cpus.effective", "0-575")
    _write(cgroup / "cpuset.mems.effective", "0-1")
    _write(proc / "cpuinfo", "model name : AMD EPYC 9825\n")
    _write(proc / "sys/kernel/random/boot_id", "32b5eeb0-4b89-400e-8e38-0ddc91b65a59")
    for core in range(288):
        siblings = f"{core},{core + 288}"
        cache_start = core - core % 8
        cache_cpus = f"{cache_start}-{cache_start + 7},{cache_start + 288}-{cache_start + 295}"
        for cpu in (core, core + 288):
            path = cpu_root / f"cpu{cpu}"
            _write(path / "topology/physical_package_id", str(core // 144))
            _write(path / "topology/die_id", "0")
            _write(path / "topology/core_id", str(core % 144))
            _write(path / "topology/thread_siblings_list", siblings)
            (path / f"node{core // 144}").mkdir()
            for index, level, kind, members in (
                (0, 1, "Data", siblings),
                (1, 3, "Unified", cache_cpus),
            ):
                cache = path / f"cache/index{index}"
                _write(cache / "level", str(level))
                _write(cache / "type", kind)
                _write(cache / "shared_cpu_list", members)
    osds = tuple(f"/dev/planner-osd{index}" for index in range(64))
    for index in range(len(osds)):
        device = sysfs / f"devices/pci0000:00/disk{index}"
        _write(device / "numa_node", str(index // 32))
        link = sysfs / f"dev/block/8:{index}"
        link.parent.mkdir(parents=True, exist_ok=True)
        link.symlink_to(device)
    for node in (0, 1):
        pci = sysfs / f"devices/pci0000:00/nic{node}"
        nic = pci / f"net/eth{node}"
        nic.mkdir(parents=True)
        _write(pci / "numa_node", str(node))
        (nic / "device").symlink_to(pci)
        link = sysfs / f"class/net/eth{node}"
        link.parent.mkdir(parents=True, exist_ok=True)
        link.symlink_to(nic)
    real_stat = os.stat
    block_metadata = {
        name: Mock(st_mode=stat.S_IFBLK, st_rdev=os.makedev(8, index))
        for index, name in enumerate(osds)
    }

    def device_stat(path, *args, **kwargs):
        if str(path) in block_metadata:
            return block_metadata[str(path)]
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(cpu_planner.os, "stat", device_stat)
    monkeypatch.setattr(cpu_planner.platform, "machine", lambda: "x86_64")
    return SyntheticHost(sysfs, proc, cgroup, osds, ("eth0", "eth1"))


def _assert_pools(plan: CPUPlan) -> None:
    eligible = tuple(core for core in plan.topology.cores if not core.exclusions)
    assigned = tuple(core for pool in plan.pools for core in pool.cores)
    assert len(assigned) == len(set(assigned)) == len(eligible)
    assert set(assigned) == set(eligible)
    cpus = [cpu for pool in plan.pools for _, ids in pool.cpu_ids_by_numa for cpu in ids]
    assert len(cpus) == len(set(cpus)) == sum(len(core.cpu_ids) for core in eligible)
    for pool in plan.pools:
        assert {cpu for _, ids in pool.cpu_ids_by_numa for cpu in ids} == {
            cpu for core in pool.cores for cpu in core.cpu_ids
        }
        assert all(
            tuple(
                sorted(
                    cpu for core in pool.cores if core.numa_node == node for cpu in core.cpu_ids
                )
            )
            == ids
            for node, ids in pool.cpu_ids_by_numa
        )
        if pool.name != "housekeeping":
            assert pool.memory_nodes == tuple(node for node, _ in pool.cpu_ids_by_numa)
            assert set(pool.memory_nodes) <= (
                plan.topology.constraints.online_memory_nodes
                & plan.topology.constraints.allowed_memory_nodes
            )
    assert next(pool for pool in plan.pools if pool.name == "lxd").cores
    housekeeping = next(pool for pool in plan.pools if pool.name == "housekeeping")
    assert {core.numa_node for core in housekeeping.cores} == {core.numa_node for core in eligible}


@pytest.mark.parametrize(
    ("components", "expected"),
    list(
        zip(
            COMPONENTS,
            ((230, 346), (86, 346, 144), (172, 346, 58), (28, 346, 144, 58)),
            strict=True,
        )
    ),
)
def test_collect_and_plan_component_combinations(
    synthetic_host: SyntheticHost, components: frozenset[str], expected: tuple[int, ...]
) -> None:
    topology = synthetic_host.collect(components)
    plan = plan_cpu_pools(topology, components=components)
    _assert_pools(plan)
    assert topology.constraints.present_cpus == frozenset(range(576))
    assert len(topology.cores) == 288
    assert len(topology.cache_domains) == 36
    assert [(device.name, device.numa_node) for device in topology.osd_devices] == (
        [(name, index // 32) for index, name in enumerate(synthetic_host.osds)]
        if "microceph" in components
        else []
    )
    assert [(device.name, device.numa_node) for device in topology.ovn_devices] == (
        [("eth0", 0), ("eth1", 1)] if "microovn" in components else []
    )
    assert (
        tuple(sum(len(ids) for _, ids in pool.cpu_ids_by_numa) for pool in plan.pools) == expected
    )
    assert plan_cpu_pools(synthetic_host.collect(components), components=components) == plan
    if components == COMPONENTS[-1]:
        assert tuple(
            tuple(len(ids) // 2 for _, ids in pool.cpu_ids_by_numa) for pool in plan.pools
        ) == ((6, 8), (87, 86), (36, 36), (15, 14))
        assert (plan.numa_score, plan.cache_score) == (2, 5)


def test_planning_does_not_modify_host_sources(synthetic_host: SyntheticHost) -> None:
    def sources() -> dict[Path, bytes]:
        return {
            path: path.read_bytes()
            for root in (synthetic_host.sysfs, synthetic_host.proc, synthetic_host.cgroup)
            for path in root.rglob("*")
            if path.is_file()
        }

    before = sources()
    plan_cpu_pools(synthetic_host.collect(COMPONENTS[-1]), components=COMPONENTS[-1])
    assert sources() == before


def test_collected_hotplug_snapshot_replans_whole_core(synthetic_host: SyntheticHost) -> None:
    components = COMPONENTS[-1]
    previous = synthetic_host.collect(components)
    cpu_root = synthetic_host.sysfs / "devices/system/cpu"
    _write(cpu_root / "online", "0-287,289-575")
    _write(synthetic_host.cgroup / "cpuset.cpus.effective", "0-287,289-575")
    _write(cpu_root / "cpu0/topology/thread_siblings_list", "0")
    topology = synthetic_host.collect(components, previous=previous)
    plan = plan_cpu_pools(topology, components=components)
    _assert_pools(plan)
    assert [(core.cpu_ids, core.exclusions) for core in plan.excluded_cores] == [
        ((0, 288), ("offline", "not allowed"))
    ]
    assert not {0, 288} & {
        cpu for pool in plan.pools for _, ids in pool.cpu_ids_by_numa for cpu in ids
    }
    assert [sum(len(ids) for _, ids in pool.cpu_ids_by_numa) for pool in plan.pools] == [
        26,
        346,
        144,
        58,
    ]


@pytest.mark.parametrize("device", ["disk0", "nic0"])
def test_unknown_device_locality_prevents_planning(
    synthetic_host: SyntheticHost, device: str
) -> None:
    _write(synthetic_host.sysfs / f"devices/pci0000:00/{device}/numa_node", "-1")
    with pytest.raises(CPUPlannerError, match="Cannot establish NUMA locality"):
        plan_cpu_pools(synthetic_host.collect(COMPONENTS[-1]), components=COMPONENTS[-1])


def test_insufficient_collected_constraints_prevent_planning(
    synthetic_host: SyntheticHost,
) -> None:
    _write(synthetic_host.cgroup / "cpuset.cpus.effective", "0,288")
    with pytest.raises(CPUPlannerError, match="requires at least"):
        plan_cpu_pools(synthetic_host.collect(COMPONENTS[-1]), components=COMPONENTS[-1])


def _device_node_source(path: Path) -> Path:
    resolved = path.resolve(strict=True)
    for ancestor in (resolved, *resolved.parents):
        for candidate in (ancestor / "device/numa_node", ancestor / "numa_node"):
            if candidate.is_file():
                return candidate
    raise AssertionError(f"No NUMA source for device {path}")


@pytest.mark.parametrize("components", COMPONENTS)
def test_supported_host_sources_and_profile(
    components: frozenset[str],
) -> None:
    if not _enabled("CPU_PLANNER_LIVE"):
        pytest.skip("requires CPU_PLANNER_LIVE=1 on representative supported hardware")
    osds = (
        tuple(shlex.split(os.environ.get("CPU_PLANNER_OSD_DEVICES", "")))
        if "microceph" in components
        else ()
    )
    nics = (
        tuple(shlex.split(os.environ.get("CPU_PLANNER_OVN_DEVICES", "")))
        if "microovn" in components
        else ()
    )
    topology = collect_host_topology(osd_devices=osds, ovn_devices=nics, reserved_cpus=frozenset())
    assert topology.constraints.present_cpus == _ids(
        Path("/sys/devices/system/cpu/present").read_text()
    )
    assert topology.constraints.online_cpus == _ids(
        Path("/sys/devices/system/cpu/online").read_text()
    )
    assert topology.constraints.online_nodes == _ids(
        Path("/sys/devices/system/node/online").read_text()
    )
    assert topology.constraints.online_memory_nodes == (
        _ids(Path("/sys/devices/system/node/has_memory").read_text())
        & topology.constraints.online_nodes
    )
    assert topology.constraints.parent_effective_cpus == _ids(
        Path("/sys/fs/cgroup/cpuset.cpus.effective").read_text()
    )
    assert topology.constraints.allowed_memory_nodes == _ids(
        Path("/sys/fs/cgroup/cpuset.mems.effective").read_text()
    )
    for core in topology.cores:
        for cpu in core.cpu_ids:
            if cpu not in topology.constraints.online_cpus:
                continue
            path = Path(f"/sys/devices/system/cpu/cpu{cpu}")
            assert (core.socket, core.die, core.core) == tuple(
                int((path / "topology" / name).read_text())
                for name in ("physical_package_id", "die_id", "core_id")
            )
            assert _ids((path / "topology/thread_siblings_list").read_text()) == frozenset(
                core.cpu_ids
            )
            assert (path / f"node{core.numa_node}").is_dir()
            caches = [
                (int((cache / "level").read_text()), _ids((cache / "shared_cpu_list").read_text()))
                for cache in (path / "cache").glob("index*")
                if (cache / "type").read_text().strip() in ("Data", "Unified")
            ]
            level = max(level for level, _ in caches)
            assert core.cache is not None
            assert core.cache.level == level
            assert all(
                members == frozenset(core.cache.cpu_ids)
                for cache_level, members in caches
                if cache_level == level
            )
    for device in topology.osd_devices:
        info = Path(device.name).stat()
        source = _device_node_source(
            Path(f"/sys/dev/block/{os.major(info.st_rdev)}:{os.minor(info.st_rdev)}")
        )
        assert device.numa_node == int(source.read_text())
    for device in topology.ovn_devices:
        source = _device_node_source(Path(f"/sys/class/net/{device.name}/device"))
        assert device.numa_node == int(source.read_text())
    plan = plan_cpu_pools(topology, components=components)
    _assert_pools(plan)
    assert plan.topology is topology
    profile = next(
        profile
        for profile in cpu_planner._CPU_PROFILES
        if (profile.name, profile.version) == (plan.profile_name, plan.profile_version)
    )
    eligible = [core for core in topology.cores if not core.exclusions]
    siblings = len(eligible[0].cpu_ids)
    total = len(eligible) * siblings
    targets = {
        group.name: siblings * ceil(ceil(total * Fraction(str(group.percent)) / 100) / siblings)
        for group in profile.groups
        if group.name in components
    }
    targets["housekeeping"] = total - sum(targets.values())
    for pool in plan.pools:
        count = sum(len(ids) for _, ids in pool.cpu_ids_by_numa)
        assert count == targets[pool.name]
        assert count >= next(group.min_cpus for group in profile.groups if group.name == pool.name)


def _run(*command: str, timeout: int = 120) -> str:
    return subprocess.run(
        command, check=True, capture_output=True, text=True, timeout=timeout
    ).stdout


def _installed_snaps() -> set[str]:
    return {parts[0] for line in _run("snap", "list").splitlines()[1:] if (parts := line.split())}


@pytest.fixture
def disposable_lxd() -> Iterator[None]:
    if not _enabled("CPU_PLANNER_LXD_LIFECYCLE"):
        pytest.skip("requires CPU_PLANNER_LXD_LIFECYCLE=1 on a disposable host without LXD")
    assert "lxd" not in _installed_snaps(), "Refusing to change a pre-existing LXD installation"
    assert shutil.which("lxd") is None, "Refusing to test absence with an existing LXD executable"
    try:
        yield
    finally:
        if "lxd" in _installed_snaps():
            _run("sudo", "-A", "snap", "remove", "lxd", "--purge", timeout=300)
            assert "lxd" not in _installed_snaps()


@pytest.mark.parametrize(
    ("opt_in", "error", "message"),
    [
        (None, pytest.skip.Exception, "requires CPU_PLANNER_LXD_LIFECYCLE=1"),
        ("0", pytest.skip.Exception, "requires CPU_PLANNER_LXD_LIFECYCLE=1"),
        ("yes", AssertionError, "must be 0 or 1"),
        ("1", AssertionError, "Refusing to change a pre-existing LXD installation"),
    ],
)
def test_lifecycle_opt_in_protects_existing_installations(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
    opt_in: str | None,
    error: type[BaseException],
    message: str,
) -> None:
    if opt_in is None:
        monkeypatch.delenv("CPU_PLANNER_LXD_LIFECYCLE", raising=False)
    else:
        monkeypatch.setenv("CPU_PLANNER_LXD_LIFECYCLE", opt_in)
    monkeypatch.setattr(f"{__name__}._installed_snaps", lambda: {"lxd"})

    def unexpected_command(*args, **kwargs) -> str:
        pytest.fail("Lifecycle guard must not execute host commands")

    monkeypatch.setattr(f"{__name__}._run", unexpected_command)
    with pytest.raises(error, match=message):
        request.getfixturevalue("disposable_lxd")


def test_planning_is_independent_of_actual_lxd_lifecycle(disposable_lxd: None) -> None:
    """Exercise actual daemon states, without approving a VM profile for production."""
    topology = collect_host_topology(osd_devices=(), ovn_devices=(), reserved_cpus=frozenset())
    sockets = {core.socket for core in topology.cores}
    profile = CPUProfile(
        name="lxd-lifecycle-test-only",
        version=1,
        architecture=topology.architecture,
        cpu_model=dict(topology.cpu_models).get("model name", "test"),
        sockets=len(sockets),
        cores_per_socket=len(topology.cores) // len(sockets),
        threads_per_core=len(topology.cores[0].cpu_ids),
        numa_nodes=len(topology.constraints.online_nodes),
        groups=(
            CPURequirement("housekeeping", 1),
            CPURequirement("lxd", 1, 50),
            CPURequirement("microceph", 1, 25),
            CPURequirement("microovn", 1, 25),
        ),
    )
    components = COMPONENTS[0]
    absent = allocate_cpu_pools(topology, profile, components=components)
    _assert_pools(absent)
    _run("sudo", "-A", "snap", "install", "lxd", "--channel=6/stable", timeout=600)
    _run("sudo", "-A", "snap", "start", "lxd")
    _run("sudo", "-A", "/snap/bin/lxd", "waitready", "--timeout=120", timeout=150)
    assert _run("systemctl", "is-active", "snap.lxd.daemon.service").strip() == "active"
    running = allocate_cpu_pools(
        collect_host_topology(osd_devices=(), ovn_devices=(), reserved_cpus=frozenset()),
        profile,
        components=components,
    )
    _assert_pools(running)
    assert running == absent
    _run("sudo", "-A", "snap", "stop", "lxd")
    services = {
        parts[0]: parts[2]
        for line in _run("snap", "services", "lxd").splitlines()[1:]
        if (parts := line.split())
    }
    assert services["lxd.daemon"] == "inactive"
    stopped = allocate_cpu_pools(
        collect_host_topology(osd_devices=(), ovn_devices=(), reserved_cpus=frozenset()),
        profile,
        components=components,
    )
    _assert_pools(stopped)
    assert stopped == absent
