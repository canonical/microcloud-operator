# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for read-only host topology collection."""

import os
import stat
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from cpu_planner import CPUPlannerError, collect_host_topology


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
