# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for read-only host topology collection and optimal CPU pool allocation."""

import os
import stat
from dataclasses import replace
from itertools import product
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

import cpu_planner
from cpu_planner import (
    CacheDomain,
    CPUConstraints,
    CPUPlannerError,
    CPUProfile,
    CPURequirement,
    Device,
    HostTopology,
    PhysicalCore,
    allocate_cpu_pools,
    collect_host_topology,
    plan_cpu_pools,
)


def _write(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value)


@pytest.fixture
def host(tmp_path):
    sysfs = tmp_path / "sys"
    cpu = sysfs / "devices/system/cpu"
    node = sysfs / "devices/system/node"
    proc = tmp_path / "proc"
    cgroup = tmp_path / "cgroup"
    _write(cpu / "present", "0-3")
    _write(cpu / "online", "0-3")
    _write(cpu / "isolated", "")
    _write(node / "online", "0-1")
    _write(node / "has_memory", "0-1")
    _write(cgroup / "cpuset.cpus.effective", "0-3")
    _write(cgroup / "cpuset.mems.effective", "0-1")
    _write(proc / "cpuinfo", "processor : 0\nmodel name : Test CPU\n")
    _write(proc / "sys/kernel/random/boot_id", "32b5eeb0-4b89-400e-8e38-0ddc91b65a59")
    for cpu_id in range(4):
        cpu_path = cpu / f"cpu{cpu_id}"
        core = cpu_id // 2
        _write(cpu_path / "topology/physical_package_id", "0")
        _write(cpu_path / "topology/die_id", "0")
        _write(cpu_path / "topology/core_id", str(core))
        _write(cpu_path / "topology/thread_siblings_list", f"{core * 2}-{core * 2 + 1}")
        (cpu_path / f"node{core}").mkdir()
        _write(cpu_path / "cache/index0/level", "1")
        _write(cpu_path / "cache/index0/type", "Data")
        _write(cpu_path / "cache/index0/shared_cpu_list", f"{core * 2}-{core * 2 + 1}")
        _write(cpu_path / "cache/index1/level", "3")
        _write(cpu_path / "cache/index1/type", "Unified")
        _write(cpu_path / "cache/index1/shared_cpu_list", "0-3")
    return sysfs, proc, cgroup


def _collect(host, **kwargs):
    sysfs, proc, cgroup = host
    inputs = {
        "osd_devices": (),
        "ovn_devices": (),
        "reserved_cpus": frozenset(),
        "sysfs_root": sysfs,
        "proc_root": proc,
        "cgroup_root": cgroup,
    }
    inputs.update(kwargs)
    return collect_host_topology(**inputs)


def test_collects_whole_cores_numa_cache_and_host_facts(host):
    topology = _collect(host)

    assert topology.architecture
    assert topology.cpu_models == (("model name", "Test CPU"),)
    assert [(core.cpu_ids, core.numa_node, core.exclusions) for core in topology.cores] == [
        ((0, 1), 0, ()),
        ((2, 3), 1, ()),
    ]
    assert [(domain.level, domain.cpu_ids) for domain in topology.cache_domains] == [
        (3, (0, 1, 2, 3))
    ]
    assert all(core.cache == topology.cache_domains[0] for core in topology.cores)
    assert topology.constraints.online_memory_nodes == frozenset({0, 1})


def test_excludes_whole_cores_for_cpu_and_memory_constraints(host):
    sysfs, _, cgroup = host
    _write(sysfs / "devices/system/cpu/online", "0-2")
    _write(sysfs / "devices/system/cpu/isolated", "1")
    _write(sysfs / "devices/system/node/has_memory", "0")
    _write(cgroup / "cpuset.cpus.effective", "0-2")
    _write(cgroup / "cpuset.mems.effective", "0")
    _write(cgroup / "other/cpuset.cpus.partition", "root")
    _write(cgroup / "other/cpuset.cpus.exclusive.effective", "2")

    topology = _collect(host, reserved_cpus=frozenset({0}))

    assert topology.cores[0].exclusions == ("isolated", "reserved")
    assert topology.cores[1].cpu_ids == (2, 3)
    assert topology.cores[1].exclusions == (
        "offline",
        "not allowed",
        "reserved",
        "no online memory",
        "memory node not allowed",
    )
    assert topology.constraints.reserved_cpus == frozenset({0, 2})


def test_collects_configured_device_locality(host):
    sysfs, _, _ = host
    disk = sysfs / "devices/pci0/disk"
    partition = disk / "disk1"
    partition.mkdir(parents=True)
    _write(disk / "device/numa_node", "0")
    link = sysfs / "dev/block/8:1"
    link.parent.mkdir(parents=True)
    link.symlink_to(partition)
    _write(sysfs / "class/net/eth0/device/numa_node", "1")
    real_stat = os.stat

    def fake_stat(path, *args, **kwargs):
        if str(path) == "/dev/test-osd":
            return Mock(st_mode=stat.S_IFBLK, st_rdev=os.makedev(8, 1))
        return real_stat(path, *args, **kwargs)

    with patch("cpu_planner.os.stat", side_effect=fake_stat):
        topology = _collect(host, osd_devices=("/dev/test-osd",), ovn_devices=("eth0",))

    assert [(device.name, device.numa_node) for device in topology.osd_devices] == [
        ("/dev/test-osd", 0)
    ]
    assert [(device.name, device.numa_node) for device in topology.ovn_devices] == [("eth0", 1)]


def test_collects_nic_locality_from_pci_ancestor(host):
    sysfs, _, _ = host
    pci = sysfs / "devices/pci0/0000:01:00.0"
    nic = pci / "virtio0/net/eth0"
    nic.mkdir(parents=True)
    _write(pci / "numa_node", "1")
    (nic / "device").symlink_to(pci / "virtio0")
    (sysfs / "class/net").mkdir(parents=True)
    (sysfs / "class/net/eth0").symlink_to(nic)

    topology = _collect(host, ovn_devices=("eth0",))

    assert topology.ovn_devices[0].numa_node == 1


def test_does_not_replace_unknown_device_locality_with_ancestor(host):
    sysfs, _, _ = host
    pci = sysfs / "devices/pci0/0000:01:00.0"
    nic = pci / "virtio0/net/eth0"
    nic.mkdir(parents=True)
    _write(pci / "numa_node", "1")
    _write(pci / "virtio0/numa_node", "-1")
    (nic / "device").symlink_to(pci / "virtio0")
    (sysfs / "class/net").mkdir(parents=True)
    (sysfs / "class/net/eth0").symlink_to(nic)

    with pytest.raises(CPUPlannerError, match="Cannot establish NUMA locality"):
        _collect(host, ovn_devices=("eth0",))


@pytest.mark.parametrize(
    ("relative_path", "value", "message"),
    [
        ("devices/system/cpu/present", "0-3,3", "Duplicate IDs"),
        ("devices/system/cpu/online", "0-4", "not all present"),
        ("devices/system/cpu/cpu1/topology/thread_siblings_list", "1", "Inconsistent topology"),
        ("devices/system/cpu/cpu1/cache/index1/shared_cpu_list", "1", "online siblings"),
        ("devices/system/cpu/cpu2/cache/index1/shared_cpu_list", "2-3", "Inconsistent topology"),
        ("devices/system/cpu/cpu0/cache/index1/type", "Unknown", "Invalid cache metadata"),
    ],
)
def test_rejects_inconsistent_cpu_and_cache_topology(host, relative_path, value, message):
    _write(host[0] / relative_path, value)
    with pytest.raises(CPUPlannerError, match=message):
        _collect(host)


def test_rejects_unknown_offline_sibling_membership(host):
    _write(host[0] / "devices/system/cpu/present", "0-4")
    with pytest.raises(CPUPlannerError, match="sibling membership.*4"):
        _collect(host)


def test_includes_a_fully_offline_core_with_known_topology(host):
    cpu = host[0] / "devices/system/cpu"
    _write(cpu / "present", "0-5")
    topology = cpu / "cpu4/topology"
    _write(topology / "physical_package_id", "0")
    _write(topology / "die_id", "0")
    _write(topology / "core_id", "2")
    _write(topology / "thread_siblings_list", "4-5")
    (cpu / "cpu4/node1").mkdir()

    core = _collect(host).cores[-1]

    assert core.cpu_ids == (4, 5)
    assert core.cache is None
    assert core.exclusions == ("offline", "not allowed")


def test_retains_previously_known_offline_sibling(host):
    previous = _collect(host)
    cpu = host[0] / "devices/system/cpu"
    _write(cpu / "online", "0,2-3")
    _write(host[2] / "cpuset.cpus.effective", "0,2-3")
    _write(cpu / "cpu0/topology/thread_siblings_list", "0")
    _write(cpu / "cpu1/topology/thread_siblings_list", "")
    _write(cpu / "cpu0/cache/index0/shared_cpu_list", "0")
    for cpu_id in (0, 2, 3):
        _write(cpu / f"cpu{cpu_id}/cache/index1/shared_cpu_list", "0,2-3")

    topology = _collect(host, previous=previous)

    assert topology.cores[0].cpu_ids == (0, 1)
    assert topology.cores[0].exclusions == ("offline", "not allowed")
    with pytest.raises(CPUPlannerError):
        _collect(host)

    _write(cpu / "online", "0-3")
    _write(host[2] / "cpuset.cpus.effective", "0-3")
    for cpu_id in (0, 1):
        _write(cpu / f"cpu{cpu_id}/topology/thread_siblings_list", "0-1")
    _write(cpu / "cpu0/cache/index0/shared_cpu_list", "0-1")
    for cpu_id in (0, 2, 3):
        _write(cpu / f"cpu{cpu_id}/cache/index1/shared_cpu_list", "0-3")

    restored = _collect(host, previous=topology)
    assert restored.cores[0].cpu_ids == (0, 1)
    assert restored.cores[0].exclusions == ()


def test_retains_previously_known_fully_offline_core(host):
    previous = _collect(host)
    cpu = host[0] / "devices/system/cpu"
    _write(cpu / "online", "2-3")
    _write(host[2] / "cpuset.cpus.effective", "2-3")
    for cpu_id in (0, 1):
        _write(cpu / f"cpu{cpu_id}/topology/thread_siblings_list", "")
    for cpu_id in (2, 3):
        _write(cpu / f"cpu{cpu_id}/cache/index1/shared_cpu_list", "2-3")

    topology = _collect(host, previous=previous)

    assert topology.cores[0].cpu_ids == (0, 1)
    assert topology.cores[0].cache is None
    assert topology.cores[0].exclusions == ("offline", "not allowed")


def test_rejects_stale_sibling_snapshot_after_reboot(host):
    previous = _collect(host)
    _write(host[1] / "sys/kernel/random/boot_id", "44d8121d-541c-43c5-ab80-ae3d8d0bfa81")
    with pytest.raises(CPUPlannerError, match="boot"):
        _collect(host, previous=previous)


def test_rejects_previous_snapshot_when_online_topology_changes(host):
    previous = _collect(host)
    _write(host[0] / "devices/system/cpu/cpu0/topology/core_id", "3")
    with pytest.raises(CPUPlannerError, match="previous"):
        _collect(host, previous=previous)


def test_rejects_previous_snapshot_when_present_cpus_change(host):
    previous = _collect(host)
    _write(host[0] / "devices/system/cpu/present", "0-4")
    with pytest.raises(CPUPlannerError, match="Present CPUs differ"):
        _collect(host, previous=previous)


def test_rejects_cpu_with_missing_numa_locality(host):
    (host[0] / "devices/system/cpu/cpu0/node0").rmdir()
    with pytest.raises(CPUPlannerError, match="unique NUMA node"):
        _collect(host)


def test_rejects_missing_or_unknown_device_locality(host):
    node = host[0] / "class/net/eth0/device/numa_node"
    _write(node, "-1")
    with pytest.raises(CPUPlannerError, match="Cannot establish NUMA locality"):
        _collect(host, ovn_devices=("eth0",))
    node.unlink()
    with pytest.raises(CPUPlannerError, match="device eth0"):
        _collect(host, ovn_devices=("eth0",))


def test_rejects_device_on_unknown_numa_node(host):
    _write(host[0] / "class/net/eth0/device/numa_node", "7")
    with pytest.raises(CPUPlannerError, match="unknown NUMA node"):
        _collect(host, ovn_devices=("eth0",))


def test_rejects_invalid_cgroup_partition(host):
    _write(host[2] / "other/cpuset.cpus.partition", "root invalid (no CPUs)")
    with pytest.raises(CPUPlannerError, match="Invalid cgroup partition"):
        _collect(host)


def test_rejects_empty_partition_reservation(host):
    _write(host[2] / "other/cpuset.cpus.partition", "root")
    _write(host[2] / "other/cpuset.cpus.exclusive.effective", "")
    with pytest.raises(CPUPlannerError, match="Empty CPU or node list"):
        _collect(host)


def test_collects_exclusive_cpus_held_by_a_member_cgroup(host):
    _write(host[2] / "other/cpuset.cpus.partition", "member")
    _write(host[2] / "other/cpuset.cpus.exclusive.effective", "2-3")

    assert _collect(host).cores[1].exclusions == ("reserved",)


def test_distinguishes_owned_partitions_from_other_reservations(host):
    cgroup = host[2]
    _write(cgroup / "owned/cpuset.cpus.partition", "root")
    _write(cgroup / "owned/cpuset.cpus.exclusive.effective", "0-1")
    _write(cgroup / "other/cpuset.cpus.partition", "root")
    _write(cgroup / "other/cpuset.cpus.exclusive.effective", "2-3")

    topology = _collect(host, owned_cgroups=frozenset({Path("owned")}))

    assert topology.cores[0].exclusions == ()
    assert topology.cores[1].exclusions == ("reserved",)


def test_owned_partition_cpus_remain_available_when_parent_excludes_them(host):
    cgroup = host[2]
    _write(cgroup / "cpuset.cpus.effective", "0-1")
    _write(cgroup / "owned/cpuset.cpus.partition", "root")
    _write(cgroup / "owned/cpuset.cpus.exclusive.effective", "2-3")

    topology = _collect(host, owned_cgroups=frozenset({Path("owned")}))

    assert topology.constraints.parent_effective_cpus == frozenset({0, 1})
    assert topology.constraints.allowed_cpus == frozenset({0, 1, 2, 3})
    assert topology.cores[1].exclusions == ()


def test_owned_partition_does_not_hide_unowned_descendants(host):
    cgroup = host[2]
    _write(cgroup / "owned/cpuset.cpus.partition", "root")
    _write(cgroup / "owned/cpuset.cpus.exclusive.effective", "0-3")
    _write(cgroup / "owned/other/cpuset.cpus.partition", "root")
    _write(cgroup / "owned/other/cpuset.cpus.exclusive.effective", "2-3")

    topology = _collect(host, owned_cgroups=frozenset({Path("owned")}))

    assert topology.cores[0].exclusions == ()
    assert topology.cores[1].exclusions == ("reserved",)


@pytest.mark.parametrize("owned", [Path("."), Path("../other"), Path("/other")])
def test_rejects_invalid_owned_cgroup_paths(host, owned):
    with pytest.raises(CPUPlannerError, match="owned cgroup"):
        _collect(host, owned_cgroups=frozenset({owned}))


def test_rejects_overlapping_cache_domains(host):
    for cpu_id in (2, 3):
        _write(host[0] / f"devices/system/cpu/cpu{cpu_id}/cache/index1/shared_cpu_list", "2-3")
    with pytest.raises(CPUPlannerError, match="Inconsistent cache membership"):
        _collect(host)


def test_detects_constraint_changes_during_collection(host):
    node = host[0] / "class/net/eth0/device/numa_node"
    _write(node, "1")
    original = Path.read_text

    def read_text(path, *args, **kwargs):
        result = original(path, *args, **kwargs)
        if path == node:
            _write(host[0] / "devices/system/cpu/online", "0-2")
        return result

    with patch.object(Path, "read_text", read_text):
        with pytest.raises(CPUPlannerError, match="changed during topology collection"):
            _collect(host, ovn_devices=("eth0",))


def test_rejects_duplicate_devices(host):
    with pytest.raises(CPUPlannerError, match="Duplicate configured devices"):
        _collect(host, ovn_devices=("eth0", "eth0"))


def test_requires_effective_constraints(host):
    (host[2] / "cpuset.mems.effective").unlink()
    with pytest.raises(CPUPlannerError, match="cpuset.mems.effective"):
        _collect(host)


def _planner_topology(node_sizes=(4, 4), *, siblings=2, cache_size=2):
    count = sum(node_sizes)
    cache_domains = tuple(
        CacheDomain(
            3,
            tuple(
                cpu
                for thread in range(siblings)
                for cpu in range(
                    start + count * thread, min(start + cache_size, count) + count * thread
                )
            ),
        )
        for start in range(0, count, cache_size)
    )
    cores = []
    for node, size in enumerate(node_sizes):
        for core_id in range(size):
            index = len(cores)
            cores.append(
                PhysicalCore(
                    socket=node,
                    die=0,
                    core=core_id,
                    numa_node=node,
                    cpu_ids=tuple(index + count * thread for thread in range(siblings)),
                    cache=cache_domains[index // cache_size],
                    exclusions=(),
                )
            )
    cpus = frozenset(range(count * siblings))
    nodes = frozenset(range(len(node_sizes)))
    return HostTopology(
        architecture="x86_64",
        cpu_models=(("model name", "Test CPU"),),
        boot_id="test-boot",
        cores=tuple(cores),
        cache_domains=cache_domains,
        constraints=CPUConstraints(
            present_cpus=cpus,
            online_cpus=cpus,
            parent_effective_cpus=cpus,
            allowed_cpus=cpus,
            isolated_cpus=frozenset(),
            reserved_cpus=frozenset(),
            online_nodes=nodes,
            online_memory_nodes=nodes,
            allowed_memory_nodes=nodes,
        ),
        osd_devices=(),
        ovn_devices=(),
    )


def _planner_profile(topology, percentages=(25, 25, 25)):
    return CPUProfile(
        name="test",
        version=1,
        architecture="amd64",
        cpu_model="Test CPU",
        sockets=len(topology.constraints.online_nodes),
        cores_per_socket=len(topology.cores) // len(topology.constraints.online_nodes),
        threads_per_core=len(topology.cores[0].cpu_ids),
        numa_nodes=len(topology.constraints.online_nodes),
        groups=(
            CPURequirement("housekeeping", 1),
            *(
                CPURequirement(name, 1, percent)
                for name, percent in zip(
                    ("lxd", "microceph", "microovn"), percentages, strict=True
                )
            ),
        ),
    )


def test_selects_profile_and_returns_complete_pool_snapshot(monkeypatch):
    topology = _planner_topology()
    profile = _planner_profile(topology)
    monkeypatch.setattr(cpu_planner, "_CPU_PROFILES", (profile,))

    plan = plan_cpu_pools(topology, components=frozenset({"lxd", "microceph", "microovn"}))

    assert plan.profile_name == "test"
    assert plan.profile_version == 1
    assert plan.topology is topology
    assert [pool.name for pool in plan.pools] == ["housekeeping", "lxd", "microceph", "microovn"]
    assert [len(pool.cores) for pool in plan.pools] == [2, 2, 2, 2]
    cpus = [cpu for pool in plan.pools for _, ids in pool.cpu_ids_by_numa for cpu in ids]
    assert len(cpus) == len(set(cpus)) == 16
    assert plan.pools[0].memory_nodes == ()
    assert all(
        pool.memory_nodes == tuple(node for node, _ in pool.cpu_ids_by_numa)
        for pool in plan.pools[1:]
    )


def test_unknown_and_ambiguous_profiles_fail(monkeypatch):
    topology = _planner_topology()
    with pytest.raises(CPUPlannerError, match="Unsupported host.*Test CPU"):
        plan_cpu_pools(topology, components=frozenset({"lxd"}))
    profile = _planner_profile(topology)
    monkeypatch.setattr(cpu_planner, "_CPU_PROFILES", (profile, profile))
    with pytest.raises(CPUPlannerError, match="Ambiguous host profile"):
        plan_cpu_pools(topology, components=frozenset({"lxd"}))


@pytest.mark.parametrize(
    "percentages",
    [
        (-1, 25, 25),
        (101, 0, 0),
        (60, 30, 20),
        (float("nan"), 25, 25),
        (True, 25, 25),
    ],
)
def test_rejects_invalid_profile_percentages(percentages):
    topology = _planner_topology()
    with pytest.raises(CPUPlannerError, match="percent"):
        allocate_cpu_pools(
            topology,
            _planner_profile(topology, percentages),
            components=frozenset({"lxd", "microceph", "microovn"}),
        )


@pytest.mark.parametrize("minimum", [0, -1, 1.5, True])
def test_validates_minima_even_for_absent_components(minimum):
    topology = _planner_topology()
    profile = _planner_profile(topology)
    profile = replace(
        profile,
        groups=tuple(
            replace(group, min_cpus=minimum) if group.name == "microceph" else group
            for group in profile.groups
        ),
    )
    with pytest.raises(CPUPlannerError, match="Invalid minimum CPUs for microceph"):
        allocate_cpu_pools(topology, profile, components=frozenset({"lxd"}))


@pytest.mark.parametrize("components", [frozenset(), frozenset({"lxd", "unknown"})])
def test_rejects_invalid_component_inventory(components):
    topology = _planner_topology()
    with pytest.raises(CPUPlannerError, match="Components must include LXD"):
        allocate_cpu_pools(topology, _planner_profile(topology), components=components)


@pytest.mark.parametrize("cpus", [(), (0, 0)])
def test_rejects_invalid_eligible_siblings(cpus):
    topology = _planner_topology()
    topology = replace(
        topology, cores=(replace(topology.cores[0], cpu_ids=cpus), *topology.cores[1:])
    )
    with pytest.raises(CPUPlannerError, match="Invalid eligible core topology"):
        allocate_cpu_pools(topology, _planner_profile(topology), components=frozenset({"lxd"}))


def test_rounds_percentage_targets_up_to_whole_cores():
    topology = _planner_topology((9,))
    plan = allocate_cpu_pools(
        topology,
        _planner_profile(topology, (30, 20, 10)),
        components=frozenset({"lxd", "microceph", "microovn"}),
    )
    assert [len(pool.cores) for pool in plan.pools] == [3, 3, 2, 1]


def test_does_not_increase_percentage_target_to_meet_minimum():
    topology = _planner_topology()
    profile = _planner_profile(topology)
    profile = replace(
        profile,
        groups=tuple(
            replace(group, min_cpus=5) if group.name == "lxd" else group
            for group in profile.groups
        ),
    )
    with pytest.raises(CPUPlannerError, match="lxd requires at least 5"):
        allocate_cpu_pools(
            topology, profile, components=frozenset({"lxd", "microceph", "microovn"})
        )


def test_housekeeping_requires_a_core_on_every_eligible_node():
    topology = _planner_topology((3, 3))
    with pytest.raises(CPUPlannerError, match="Housekeeping requires"):
        allocate_cpu_pools(
            topology,
            _planner_profile(topology, (30, 30, 15)),
            components=frozenset({"lxd", "microceph", "microovn"}),
        )


def test_absent_components_leave_their_share_for_housekeeping():
    topology = _planner_topology()
    plan = allocate_cpu_pools(topology, _planner_profile(topology), components=frozenset({"lxd"}))
    assert [(pool.name, len(pool.cores)) for pool in plan.pools] == [
        ("housekeeping", 6),
        ("lxd", 2),
    ]


def test_excluded_cores_are_not_allocated():
    topology = _planner_topology((6,))
    excluded = replace(topology.cores[-1], exclusions=("reserved",))
    topology = replace(
        topology,
        cores=(*topology.cores[:-1], excluded),
        constraints=replace(topology.constraints, reserved_cpus=frozenset(excluded.cpu_ids)),
    )
    plan = allocate_cpu_pools(topology, _planner_profile(topology), components=frozenset({"lxd"}))
    assert plan.excluded_cores == (excluded,)
    assert excluded not in tuple(core for pool in plan.pools for core in pool.cores)


def test_rejects_nonuniform_eligible_sibling_counts():
    topology = _planner_topology()
    topology = replace(
        topology, cores=(replace(topology.cores[0], cpu_ids=(0,)), *topology.cores[1:])
    )
    with pytest.raises(CPUPlannerError, match="non-uniform"):
        allocate_cpu_pools(topology, _planner_profile(topology), components=frozenset({"lxd"}))


def _exhaustive_allocation(topology, totals, preferred):
    nodes = sorted(topology.constraints.online_nodes)
    cores = sorted(
        topology.cores, key=lambda core: (core.socket, core.die, core.core, min(core.cpu_ids))
    )
    best = None
    for assignment in product(range(len(totals)), repeat=len(cores)):
        if tuple(assignment.count(group) for group in range(len(totals))) != totals:
            continue
        counts = tuple(
            tuple(
                sum(
                    group == assigned and core.numa_node == node
                    for core, assigned in zip(cores, assignment, strict=True)
                )
                for node in nodes
            )
            for group in range(len(totals))
        )
        if any(count == 0 for count in counts[0]):
            continue
        numa_score = sum(
            abs(count - target)
            for row, targets in zip(counts, preferred, strict=True)
            for count, target in zip(row, targets, strict=True)
        )
        cache_score = 0
        for domain in topology.cache_domains:
            groups = {
                group
                for core, group in zip(cores, assignment, strict=True)
                if core.cache == domain
            }
            cache_score += len(groups) * (len(groups) - 1) // 2
        sequence = tuple(
            tuple(
                (core.socket, core.die, core.core, min(core.cpu_ids))
                for core, assigned in zip(cores, assignment, strict=True)
                if assigned == group
            )
            for group in range(len(totals))
        )
        score = (numa_score, cache_score, counts, sequence)
        if best is None or score < best:
            best = score
    assert best is not None
    return best


@pytest.mark.parametrize(
    ("sizes", "cache_size"), [((4, 4), 2), ((4, 4), 4), ((3, 3), 4), ((3, 3, 3), 3)]
)
def test_matches_exhaustive_optimum_and_both_tie_breaks(sizes, cache_size):
    topology = _planner_topology(sizes, cache_size=cache_size)
    percentages = (15, 15, 15)
    profile = _planner_profile(topology, percentages)
    plan = allocate_cpu_pools(
        topology, profile, components=frozenset({"lxd", "microceph", "microovn"})
    )
    totals = tuple(len(pool.cores) for pool in plan.pools)
    weights = tuple(sizes)
    preferred = tuple(cpu_planner._largest_remainder(total, weights) for total in totals)
    optimum = _exhaustive_allocation(topology, totals, preferred)
    counts = tuple(
        tuple(sum(core.numa_node == node for core in pool.cores) for node in range(len(sizes)))
        for pool in plan.pools
    )
    sequence = tuple(
        tuple((core.socket, core.die, core.core, min(core.cpu_ids)) for core in pool.cores)
        for pool in plan.pools
    )
    assert (plan.numa_score, plan.cache_score, counts, sequence) == optimum
    reordered = allocate_cpu_pools(
        replace(topology, cores=tuple(reversed(topology.cores))),
        profile,
        components=frozenset({"lxd", "microceph", "microovn"}),
    )
    assert reordered.pools == plan.pools
    assert (reordered.numa_score, reordered.cache_score) == (plan.numa_score, plan.cache_score)


def test_device_locality_follows_osd_and_network_device_counts():
    topology = _planner_topology()
    topology = replace(
        topology, osd_devices=(Device("osd0", 1),), ovn_devices=(Device("nic0", 0),)
    )
    plan = allocate_cpu_pools(
        topology,
        _planner_profile(topology),
        components=frozenset({"lxd", "microceph", "microovn"}),
    )
    assert plan.pools[2].memory_nodes == (1,)
    assert plan.pools[3].memory_nodes == (0,)
    assert plan.numa_score == 0


@pytest.mark.parametrize(
    "cache_members",
    [
        ((0, 2), (1, 3), (4, 6), (5, 7)),
        ((0, 4), (1, 5), (2, 6), (3, 7)),
        ((0, 1, 4), (2, 3), (5, 6, 7)),
    ],
)
def test_skewed_device_preferences_and_irregular_caches_match_exhaustive_optimum(cache_members):
    topology = _planner_topology()
    domains = tuple(
        CacheDomain(
            3, tuple(sorted(cpu for index in members for cpu in topology.cores[index].cpu_ids))
        )
        for members in cache_members
    )
    topology = replace(
        topology,
        cores=tuple(
            replace(
                core,
                cache=next(
                    domain
                    for domain, members in zip(domains, cache_members, strict=True)
                    if index in members
                ),
            )
            for index, core in enumerate(topology.cores)
        ),
        cache_domains=domains,
        osd_devices=(Device("osd0", 0),),
        ovn_devices=(Device("nic0", 0),),
    )
    plan = allocate_cpu_pools(
        topology,
        _planner_profile(topology),
        components=frozenset({"lxd", "microceph", "microovn"}),
    )
    optimum = _exhaustive_allocation(topology, (2, 2, 2, 2), ((1, 1), (1, 1), (2, 0), (2, 0)))
    counts = tuple(
        tuple(sum(core.numa_node == node for core in pool.cores) for node in (0, 1))
        for pool in plan.pools
    )
    sequence = tuple(
        tuple((core.socket, core.die, core.core, min(core.cpu_ids)) for core in pool.cores)
        for pool in plan.pools
    )
    assert (plan.numa_score, plan.cache_score, counts, sequence) == optimum


def test_unavailable_memory_node_is_excluded_even_with_local_osds():
    topology = _planner_topology()
    topology = replace(
        topology,
        cores=tuple(
            replace(core, exclusions=("memory",)) if core.numa_node == 1 else core
            for core in topology.cores
        ),
        constraints=replace(topology.constraints, allowed_memory_nodes=frozenset({0})),
        osd_devices=(Device("osd0", 1),),
    )
    plan = allocate_cpu_pools(
        topology, _planner_profile(topology), components=frozenset({"lxd", "microceph"})
    )
    assert [len(pool.cores) for pool in plan.pools] == [2, 1, 1]
    assert all(pool.memory_nodes == (0,) for pool in plan.pools[1:])
    assert plan.numa_score == 2


def test_largest_remainder_uses_actual_numa_ids_for_ties():
    topology = _planner_topology((3, 3, 3), cache_size=3)
    ids = (7, 2, 11)
    nodes = frozenset(ids)
    topology = replace(
        topology,
        cores=tuple(replace(core, numa_node=ids[core.numa_node]) for core in topology.cores),
        constraints=replace(
            topology.constraints,
            online_nodes=nodes,
            online_memory_nodes=nodes,
            allowed_memory_nodes=nodes,
        ),
    )
    plan = allocate_cpu_pools(
        topology, _planner_profile(topology, (55, 0, 0)), components=frozenset({"lxd"})
    )
    assert [(node, len(cpus) // 2) for node, cpus in plan.pools[1].cpu_ids_by_numa] == [
        (2, 2),
        (7, 2),
        (11, 1),
    ]
    assert plan.numa_score == 2


def test_spec_worked_example():
    topology = _planner_topology((144, 144), cache_size=8)
    topology = replace(
        topology,
        cpu_models=(("model name", "AMD EPYC 9825"),),
        osd_devices=tuple(
            Device(f"osd{node}-{index}", node) for node in (0, 1) for index in range(32)
        ),
        ovn_devices=(Device("nic0", 0), Device("nic1", 1)),
    )

    plan = plan_cpu_pools(topology, components=frozenset({"lxd", "microceph", "microovn"}))

    assert [sum(len(ids) for _, ids in pool.cpu_ids_by_numa) for pool in plan.pools] == [
        28,
        346,
        144,
        58,
    ]
    assert [tuple(len(ids) // 2 for _, ids in pool.cpu_ids_by_numa) for pool in plan.pools] == [
        (6, 8),
        (87, 86),
        (36, 36),
        (15, 14),
    ]
    assert plan.numa_score == 2
    assert plan.cache_score == 5
