# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Read-only collection of host CPU, memory and device topology."""

import os
import platform
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID


class CPUPlannerError(Exception):
    """Raised when the host topology or constraints cannot be established."""


@dataclass(frozen=True)
class CacheDomain:
    """A highest-level data or unified cache shared by logical CPUs."""

    level: int
    cpu_ids: tuple[int, ...]


@dataclass(frozen=True)
class PhysicalCore:
    """A physical core, including any known offline siblings."""

    socket: int
    die: int
    core: int
    numa_node: int
    cpu_ids: tuple[int, ...]
    cache: CacheDomain | None
    exclusions: tuple[str, ...]


@dataclass(frozen=True)
class Device:
    """A configured device and its preferred NUMA node."""

    name: str
    numa_node: int


@dataclass(frozen=True)
class CPUConstraints:
    """The host state used to determine whether a core is eligible."""

    present_cpus: frozenset[int]
    online_cpus: frozenset[int]
    parent_effective_cpus: frozenset[int]
    allowed_cpus: frozenset[int]
    isolated_cpus: frozenset[int]
    reserved_cpus: frozenset[int]
    online_nodes: frozenset[int]
    online_memory_nodes: frozenset[int]
    allowed_memory_nodes: frozenset[int]


@dataclass(frozen=True)
class HostTopology:
    """An immutable snapshot of the inputs needed by the host planner."""

    architecture: str
    cpu_models: tuple[tuple[str, str], ...]
    boot_id: str
    cores: tuple[PhysicalCore, ...]
    cache_domains: tuple[CacheDomain, ...]
    constraints: CPUConstraints
    osd_devices: tuple[Device, ...]
    ovn_devices: tuple[Device, ...]


def _read(path: Path) -> str:
    try:
        return path.read_text().strip()
    except OSError as exc:
        raise CPUPlannerError(f"Cannot read {path}: {exc}") from exc


def _ids(value: str, source: str, *, empty: bool = False) -> frozenset[int]:
    if not value:
        if empty:
            return frozenset()
        raise CPUPlannerError(f"Empty CPU or node list in {source}")
    result: set[int] = set()
    for item in value.split(","):
        if not re.fullmatch(r"[0-9]+(?:-[0-9]+)?", item):
            raise CPUPlannerError(f"Invalid CPU or node list in {source}: {value!r}")
        bounds = item.split("-")
        first, last = int(bounds[0]), int(bounds[-1])
        if first > last:
            raise CPUPlannerError(f"Descending range in {source}: {item!r}")
        ids = set(range(first, last + 1))
        if result & ids:
            raise CPUPlannerError(f"Duplicate IDs in {source}: {item!r}")
        result.update(ids)
    return frozenset(result)


def _read_ids(path: Path, *, empty: bool = False) -> frozenset[int]:
    return _ids(_read(path), str(path), empty=empty)


def _number(path: Path) -> int:
    value = _read(path)
    if not re.fullmatch(r"[0-9]+", value):
        raise CPUPlannerError(f"Invalid non-negative integer in {path}: {value!r}")
    return int(value)


def _partition_cpus(
    root: Path, owned_cgroups: frozenset[Path]
) -> tuple[frozenset[int], frozenset[int]]:
    """Find CPUs held by owned and other descendant cgroups."""
    reserved: set[int] = set()
    owned: set[int] = set()

    def failed(exc: OSError) -> None:
        raise CPUPlannerError(f"Cannot inspect cgroup partitions: {exc}") from exc

    for directory, _, files in os.walk(root, onerror=failed):
        path = Path(directory)
        if path == root or "cpuset.cpus.partition" not in files:
            continue
        partition = _read(path / "cpuset.cpus.partition")
        if partition not in ("member", "root", "isolated"):
            raise CPUPlannerError(f"Invalid cgroup partition at {path}: {partition!r}")
        exclusive = path / "cpuset.cpus.exclusive.effective"
        if exclusive.exists():
            cpus = _read_ids(exclusive, empty=partition == "member")
        elif partition != "member":
            cpus = _read_ids(path / "cpuset.cpus.effective")
        else:
            continue
        if path.relative_to(root) in owned_cgroups:
            owned.update(cpus)
        else:
            reserved.update(cpus)
    return frozenset(reserved), frozenset(owned)


def _constraints(
    cpu_root: Path,
    node_root: Path,
    cgroup_root: Path,
    reserved_cpus: frozenset[int],
    owned_cgroups: frozenset[Path],
) -> CPUConstraints:
    present = _read_ids(cpu_root / "present")
    online = _read_ids(cpu_root / "online")
    if not online <= present:
        raise CPUPlannerError("Online CPUs are not all present")
    isolated = _read_ids(cpu_root / "isolated", empty=True)
    parent_effective = _read_ids(cgroup_root / "cpuset.cpus.effective")
    memory = _read_ids(node_root / "has_memory", empty=True)
    online_nodes = _read_ids(node_root / "online")
    allowed_memory = _read_ids(cgroup_root / "cpuset.mems.effective")
    partition_reserved, owned = _partition_cpus(cgroup_root, owned_cgroups)
    reserved = reserved_cpus | partition_reserved
    allowed = parent_effective | (owned & online)
    if not (isolated | allowed | reserved) <= present:
        raise CPUPlannerError("CPU constraints include CPUs not present on the host")
    return CPUConstraints(
        present,
        online,
        parent_effective,
        allowed,
        isolated,
        reserved,
        online_nodes,
        memory & online_nodes,
        allowed_memory,
    )


def _cpu_node(cpu_path: Path) -> int:
    try:
        nodes = [
            int(match.group(1))
            for entry in cpu_path.iterdir()
            if (match := re.fullmatch(r"node([0-9]+)", entry.name)) and entry.is_dir()
        ]
    except OSError as exc:
        raise CPUPlannerError(f"Cannot inspect NUMA nodes for {cpu_path}: {exc}") from exc
    if len(nodes) != 1:
        raise CPUPlannerError(f"Cannot establish a unique NUMA node for {cpu_path}")
    return nodes[0]


def _cache(cpu_path: Path, cpu_id: int, present: frozenset[int]) -> CacheDomain:
    cache_path = cpu_path / "cache"
    try:
        indexes = sorted(
            path for path in cache_path.iterdir() if re.fullmatch(r"index[0-9]+", path.name)
        )
    except OSError as exc:
        raise CPUPlannerError(f"Cannot inspect cache topology for CPU {cpu_id}: {exc}") from exc
    candidates = []
    for index in indexes:
        kind = _read(index / "type")
        level = _number(index / "level")
        if level == 0 or kind not in ("Data", "Instruction", "Unified"):
            raise CPUPlannerError(f"Invalid cache metadata in {index}")
        cpus = _read_ids(index / "shared_cpu_list")
        if cpu_id not in cpus or not cpus <= present:
            raise CPUPlannerError(f"Invalid cache membership in {index}")
        if kind in ("Data", "Unified"):
            candidates.append(CacheDomain(level, tuple(sorted(cpus))))
    if not candidates:
        raise CPUPlannerError(f"No data or unified cache for CPU {cpu_id}")
    level = max(cache.level for cache in candidates)
    highest = {cache for cache in candidates if cache.level == level}
    if len(highest) != 1:
        raise CPUPlannerError(f"Ambiguous highest-level cache for CPU {cpu_id}")
    return highest.pop()


_CoreKey = tuple[int, int, int]
_CoreGroup = tuple[frozenset[int], int, CacheDomain | None]


def _read_core_topology(cpu_path: Path) -> tuple[_CoreKey, frozenset[int]]:
    topology = cpu_path / "topology"
    key = (
        _number(topology / "physical_package_id"),
        _number(topology / "die_id"),
        _number(topology / "core_id"),
    )
    return key, _read_ids(topology / "thread_siblings_list")


def _previous_cpu_cores(
    previous: HostTopology | None, present_cpus: frozenset[int]
) -> dict[int, PhysicalCore]:
    previous_cpus: dict[int, PhysicalCore] = {}
    if previous is not None:
        for core in previous.cores:
            for cpu_id in core.cpu_ids:
                if cpu_id in previous_cpus:
                    raise CPUPlannerError(f"Duplicate CPU {cpu_id} in previous topology")
                previous_cpus[cpu_id] = core
        if previous_cpus.keys() != present_cpus:
            raise CPUPlannerError("Present CPUs differ from the previous topology")
    return previous_cpus


def _add_offline_cores(
    cpu_root: Path,
    constraints: CPUConstraints,
    previous: HostTopology | None,
    groups: dict[_CoreKey, _CoreGroup],
    membership: dict[int, _CoreKey],
) -> None:
    """Complete the collected groups with known offline core membership."""
    if previous is not None:
        for core in previous.cores:
            siblings = frozenset(core.cpu_ids)
            if siblings & constraints.online_cpus:
                continue
            key = (core.socket, core.die, core.core)
            if key in groups:
                raise CPUPlannerError(f"Duplicate core {key} in previous topology")
            groups[key] = (siblings, core.numa_node, None)
            for sibling in siblings:
                membership[sibling] = key

    for cpu_id in sorted(constraints.present_cpus - constraints.online_cpus):
        if previous is not None and cpu_id in membership:
            continue
        path = cpu_root / f"cpu{cpu_id}"
        if not (path / "topology").is_dir():
            continue
        key, siblings = _read_core_topology(path)
        if cpu_id not in siblings or not siblings <= constraints.present_cpus:
            raise CPUPlannerError(f"Inconsistent offline sibling membership for CPU {cpu_id}")
        node = _cpu_node(path)
        if key in groups:
            known_siblings, known_node, _ = groups[key]
            if siblings != known_siblings or node != known_node:
                raise CPUPlannerError(f"Inconsistent offline topology for CPU {cpu_id}")
        else:
            if siblings & constraints.online_cpus:
                raise CPUPlannerError(f"Inconsistent offline sibling membership for CPU {cpu_id}")
            groups[key] = (siblings, node, None)
        for sibling in siblings:
            other = membership.setdefault(sibling, key)
            if other != key:
                raise CPUPlannerError(f"CPU {sibling} belongs to multiple cores")


def _core_exclusions(
    siblings: frozenset[int], node: int, constraints: CPUConstraints
) -> tuple[str, ...]:
    exclusions = []
    if not siblings <= constraints.online_cpus:
        exclusions.append("offline")
    if not siblings <= constraints.allowed_cpus:
        exclusions.append("not allowed")
    if siblings & constraints.isolated_cpus:
        exclusions.append("isolated")
    if siblings & constraints.reserved_cpus:
        exclusions.append("reserved")
    if node not in constraints.online_memory_nodes:
        exclusions.append("no online memory")
    if node not in constraints.allowed_memory_nodes:
        exclusions.append("memory node not allowed")
    return tuple(exclusions)


def _cores(
    cpu_root: Path, constraints: CPUConstraints, previous: HostTopology | None
) -> tuple[PhysicalCore, ...]:
    groups: dict[_CoreKey, _CoreGroup] = {}
    membership: dict[int, _CoreKey] = {}
    caches: dict[int, CacheDomain] = {}
    previous_cpus = _previous_cpu_cores(previous, constraints.present_cpus)

    for cpu_id in sorted(constraints.online_cpus):
        path = cpu_root / f"cpu{cpu_id}"
        key, siblings = _read_core_topology(path)
        if cpu_id not in siblings or not siblings <= constraints.present_cpus:
            raise CPUPlannerError(f"Invalid sibling membership for CPU {cpu_id}")
        node = _cpu_node(path)
        if previous is not None:
            old_core = previous_cpus[cpu_id]
            old_siblings = frozenset(old_core.cpu_ids)
            if (
                key != (old_core.socket, old_core.die, old_core.core)
                or node != old_core.numa_node
                or not siblings <= old_siblings
                or siblings & constraints.online_cpus != old_siblings & constraints.online_cpus
            ):
                raise CPUPlannerError(f"CPU {cpu_id} differs from the previous topology")
            siblings = old_siblings
        cache = _cache(path, cpu_id, constraints.present_cpus)
        if not (siblings & constraints.online_cpus) <= set(cache.cpu_ids):
            raise CPUPlannerError(f"Cache does not contain all online siblings of CPU {cpu_id}")
        existing = groups.get(key)
        if existing is not None and existing != (siblings, node, cache):
            raise CPUPlannerError(f"Inconsistent topology for physical core {key}")
        groups[key] = (siblings, node, cache)
        caches[cpu_id] = cache
        for sibling in siblings:
            other = membership.setdefault(sibling, key)
            if other != key:
                raise CPUPlannerError(f"CPU {sibling} belongs to multiple cores")

    _add_offline_cores(cpu_root, constraints, previous, groups, membership)
    missing = constraints.present_cpus - membership.keys()
    if missing:
        raise CPUPlannerError(f"Cannot establish sibling membership for CPUs {sorted(missing)}")
    cache_membership: dict[int, CacheDomain] = {}
    for cache in caches.values():
        for shared in cache.cpu_ids:
            existing = cache_membership.setdefault(shared, cache)
            if existing != cache:
                raise CPUPlannerError(f"Inconsistent cache membership for CPU {shared}")

    cores = []
    for (socket, die, core), (siblings, node, cache) in sorted(groups.items()):
        cores.append(
            PhysicalCore(
                socket,
                die,
                core,
                node,
                tuple(sorted(siblings)),
                cache,
                _core_exclusions(siblings, node, constraints),
            )
        )
    return tuple(cores)


def _device_node(path: Path, name: str) -> Device:
    value = _read(path)
    if not re.fullmatch(r"[0-9]+", value):
        raise CPUPlannerError(f"Cannot establish NUMA locality for device {name}: {value!r}")
    return Device(name, int(value))


def _device_ancestor_node(device: Path, sysfs_root: Path, name: str) -> Device:
    try:
        device = device.resolve(strict=True)
        root = (sysfs_root / "devices").resolve(strict=True)
    except OSError as exc:
        raise CPUPlannerError(f"Cannot resolve sysfs topology for device {name}: {exc}") from exc
    if not device.is_relative_to(root):
        raise CPUPlannerError(f"Invalid sysfs topology for device {name}")
    for parent in (device, *device.parents):
        if not parent.is_relative_to(root):
            break
        for node in (parent / "device/numa_node", parent / "numa_node"):
            if node.exists():
                return _device_node(node, name)
    raise CPUPlannerError(f"Cannot establish NUMA locality for device {name}")


def _osd_device(name: str, sysfs_root: Path) -> Device:
    try:
        info = os.stat(name)
    except OSError as exc:
        raise CPUPlannerError(f"Cannot inspect OSD device {name}: {exc}") from exc
    if not stat.S_ISBLK(info.st_mode):
        raise CPUPlannerError(f"OSD device {name} is not a block device")
    link = sysfs_root / "dev/block" / f"{os.major(info.st_rdev)}:{os.minor(info.st_rdev)}"
    return _device_ancestor_node(link, sysfs_root, name)


def _ovn_device(name: str, sysfs_root: Path) -> Device:
    if not name or Path(name).name != name or name in (".", ".."):
        raise CPUPlannerError(f"Invalid network device name: {name!r}")
    device = sysfs_root / "class/net" / name / "device"
    if (device / "numa_node").exists():
        return _device_node(device / "numa_node", name)
    return _device_ancestor_node(device, sysfs_root, name)


def _cpu_models(proc_root: Path) -> tuple[tuple[str, str], ...]:
    fields = {
        "model name",
        "cpu model",
        "cpu implementer",
        "cpu part",
        "cpu variant",
        "cpu revision",
    }
    models = set()
    for line in _read(proc_root / "cpuinfo").splitlines():
        key, separator, value = line.partition(":")
        key, value = key.strip().lower(), value.strip()
        if separator and key in fields and value:
            models.add((key, value))
    if not models:
        raise CPUPlannerError("Cannot establish CPU model from /proc/cpuinfo")
    return tuple(sorted(models))


def collect_host_topology(
    *,
    osd_devices: tuple[str, ...],
    ovn_devices: tuple[str, ...],
    reserved_cpus: frozenset[int],
    owned_cgroups: frozenset[Path] = frozenset(),
    previous: HostTopology | None = None,
    sysfs_root: Path = Path("/sys"),
    proc_root: Path = Path("/proc"),
    cgroup_root: Path = Path("/sys/fs/cgroup"),
) -> HostTopology:
    """Collect host facts and configured device locality without using LXD.

    ``reserved_cpus`` contains reservations known outside cgroups.
    ``owned_cgroups`` contains caller-verified owned paths relative to the
    parent cgroup. ``previous`` is this host's snapshot from the same boot.
    """
    cpu_root = sysfs_root / "devices/system/cpu"
    node_root = sysfs_root / "devices/system/node"
    if any(path.is_absolute() or not path.parts or ".." in path.parts for path in owned_cgroups):
        raise CPUPlannerError("Invalid owned cgroup path")
    constraints = _constraints(cpu_root, node_root, cgroup_root, reserved_cpus, owned_cgroups)
    architecture = platform.machine()
    if not architecture:
        raise CPUPlannerError("Cannot establish host CPU architecture")
    models = _cpu_models(proc_root)
    boot_path = proc_root / "sys/kernel/random/boot_id"
    try:
        boot_id = str(UUID(_read(boot_path)))
    except ValueError as exc:
        raise CPUPlannerError(f"Invalid host boot ID in {boot_path}") from exc
    if previous is not None:
        if previous.boot_id != boot_id:
            raise CPUPlannerError("Previous topology is from a different host boot")
        if previous.architecture != architecture or previous.cpu_models != models:
            raise CPUPlannerError("Host CPU identity differs from the previous topology")
    cores = _cores(cpu_root, constraints, previous)
    if len(set(osd_devices)) != len(osd_devices) or len(set(ovn_devices)) != len(ovn_devices):
        raise CPUPlannerError("Duplicate configured devices")
    osds = tuple(_osd_device(name, sysfs_root) for name in osd_devices)
    ovn = tuple(_ovn_device(name, sysfs_root) for name in ovn_devices)
    for core in cores:
        if (
            core.numa_node not in constraints.online_nodes
            and set(core.cpu_ids) & constraints.online_cpus
        ):
            raise CPUPlannerError(f"Online core has an unknown NUMA node: {core.numa_node}")
    for device in (*osds, *ovn):
        if device.numa_node not in constraints.online_nodes:
            raise CPUPlannerError(
                f"Device {device.name} has an unknown NUMA node: {device.numa_node}"
            )
    if (
        constraints != _constraints(cpu_root, node_root, cgroup_root, reserved_cpus, owned_cgroups)
        or _read(boot_path) != boot_id
    ):
        raise CPUPlannerError("Host CPU or memory constraints changed during topology collection")
    return HostTopology(
        architecture=architecture,
        cpu_models=models,
        boot_id=boot_id,
        cores=cores,
        cache_domains=tuple(
            sorted(
                {core.cache for core in cores if core.cache is not None},
                key=lambda cache: (cache.level, cache.cpu_ids),
            )
        ),
        constraints=constraints,
        osd_devices=osds,
        ovn_devices=ovn,
    )
