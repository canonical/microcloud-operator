# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Read-only host topology collection and whole-core CPU pool planning."""

import os
import platform
import re
import stat
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from fractions import Fraction
from functools import lru_cache
from itertools import product
from pathlib import Path
from uuid import UUID


class CPUPlannerError(Exception):
    """Raised when host facts cannot be established or planning cannot succeed."""


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


_GROUPS = ("housekeeping", "lxd", "microceph", "microovn")


@dataclass(frozen=True)
class CPURequirement:
    """A group's minimum logical CPUs and percentage of eligible CPUs."""

    name: str
    min_cpus: int
    percent: int | float | None = None


@dataclass(frozen=True)
class CPUProfile:
    """Versioned sizing for a specific host topology and CPU model."""

    name: str
    version: int
    architecture: str
    cpu_model: str
    sockets: int
    cores_per_socket: int
    threads_per_core: int
    numa_nodes: int
    groups: tuple[CPURequirement, ...]


# Temporary profile from the spec's illustrative example, not yet hardware-validated.
_CPU_PROFILES = (
    CPUProfile(
        name="example-epyc-9825",
        version=1,
        architecture="amd64",
        cpu_model="AMD EPYC 9825",
        sockets=2,
        cores_per_socket=144,
        threads_per_core=2,
        numa_nodes=2,
        groups=(
            CPURequirement("housekeeping", 8),
            CPURequirement("lxd", 32, 60),
            CPURequirement("microceph", 64, 25),
            CPURequirement("microovn", 8, 10),
        ),
    ),
)


@dataclass(frozen=True)
class CPUPool:
    """A whole-core allocation and its logical CPUs grouped by NUMA node."""

    name: str
    cores: tuple[PhysicalCore, ...]
    cpu_ids_by_numa: tuple[tuple[int, tuple[int, ...]], ...]
    memory_nodes: tuple[int, ...]


@dataclass(frozen=True)
class CPUPlan:
    """A read-only plan with its input snapshot and optimization scores."""

    profile_name: str
    profile_version: int
    topology: HostTopology
    pools: tuple[CPUPool, ...]
    excluded_cores: tuple[PhysicalCore, ...]
    numa_score: int
    cache_score: int


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


def _requirements(profile: CPUProfile, components: frozenset[str]) -> tuple[CPURequirement, ...]:
    if "lxd" not in components or not components <= set(_GROUPS[1:]):
        raise CPUPlannerError("Components must include LXD and only supported snaps")
    if not profile.name or type(profile.version) is not int or profile.version <= 0:
        raise CPUPlannerError("Invalid profile name or version")
    requirements = {requirement.name: requirement for requirement in profile.groups}
    if len(requirements) != len(profile.groups) or set(requirements) != set(_GROUPS):
        raise CPUPlannerError("Profile must define each CPU group exactly once")
    percentages = []
    for name in _GROUPS:
        requirement = requirements[name]
        if type(requirement.min_cpus) is not int or requirement.min_cpus <= 0:
            raise CPUPlannerError(f"Invalid minimum CPUs for {name}")
        if name == "housekeeping":
            if requirement.percent is not None:
                raise CPUPlannerError("Housekeeping must receive the remainder, not a percentage")
            continue
        if type(requirement.percent) not in (int, float):
            raise CPUPlannerError(f"Invalid CPU percentage for {name}")
        try:
            percent = Fraction(str(requirement.percent))
        except ValueError as exc:
            raise CPUPlannerError(f"Invalid CPU percentage for {name}") from exc
        if not 0 <= percent <= 100:
            raise CPUPlannerError(f"Invalid CPU percentage for {name}")
        percentages.append(percent)
    if sum(percentages) > 100:
        raise CPUPlannerError("Profile CPU percentages exceed 100")
    return tuple(
        requirements[name] for name in _GROUPS if name == "housekeeping" or name in components
    )


def _select_profile(topology: HostTopology, components: frozenset[str]) -> CPUProfile:
    architecture = {"x86_64": "amd64", "aarch64": "arm64"}.get(
        topology.architecture, topology.architecture
    )
    models = {value for key, value in topology.cpu_models if key in ("model name", "cpu model")}
    sockets = Counter(core.socket for core in topology.cores)
    matches = []
    for profile in _CPU_PROFILES:
        if (
            architecture == profile.architecture
            and models
            and all(
                model == profile.cpu_model or model.startswith(profile.cpu_model + " ")
                for model in models
            )
            and len(sockets) == profile.sockets
            and set(sockets.values()) == {profile.cores_per_socket}
            and {len(core.cpu_ids) for core in topology.cores} == {profile.threads_per_core}
            and len(topology.constraints.online_nodes) == profile.numa_nodes
        ):
            _requirements(profile, components)
            matches.append(profile)
    if len(matches) != 1:
        reason = "Unsupported host" if not matches else "Ambiguous host profile"
        raise CPUPlannerError(
            f"{reason}: architecture={architecture}, CPU models={sorted(models)}, "
            f"cores per socket={dict(sockets)}, NUMA nodes={sorted(topology.constraints.online_nodes)}"
        )
    return matches[0]


def _core_order(core: PhysicalCore) -> tuple[int, int, int, int]:
    return core.socket, core.die, core.core, min(core.cpu_ids)


def _eligible_cores(topology: HostTopology) -> tuple[PhysicalCore, ...]:
    eligible = tuple(core for core in topology.cores if not core.exclusions)
    if any(not core.cpu_ids for core in eligible):
        raise CPUPlannerError("Invalid eligible core topology: no sibling CPUs")
    cores = tuple(sorted(eligible, key=_core_order))
    if not cores:
        raise CPUPlannerError("No eligible physical cores")
    if len({len(core.cpu_ids) for core in cores}) != 1:
        raise CPUPlannerError("Eligible cores have non-uniform sibling counts")
    seen: set[int] = set()
    for core in cores:
        if (
            len(set(core.cpu_ids)) != len(core.cpu_ids)
            or seen.intersection(core.cpu_ids)
            or _core_exclusions(frozenset(core.cpu_ids), core.numa_node, topology.constraints)
            or core.cache is None
        ):
            raise CPUPlannerError("Invalid eligible core topology")
        seen.update(core.cpu_ids)
    return cores


def _targets(
    requirements: tuple[CPURequirement, ...], cores: tuple[PhysicalCore, ...]
) -> tuple[int, ...]:
    siblings = len(cores[0].cpu_ids)
    targets = []
    for requirement in requirements[1:]:
        percentage = Fraction(str(requirement.percent))
        target = len(cores) * percentage / 100
        targets.append(-(-target.numerator // target.denominator))
    targets.insert(0, len(cores) - sum(targets))
    for requirement, count in zip(requirements, targets, strict=True):
        if count * siblings < requirement.min_cpus:
            raise CPUPlannerError(
                f"{requirement.name} requires at least {requirement.min_cpus} logical CPUs; "
                f"percentage sizing provides {count * siblings}"
            )
    node_count = len({core.numa_node for core in cores})
    if targets[0] < node_count:
        raise CPUPlannerError(
            "Housekeeping requires at least one whole core on each eligible NUMA node"
        )
    return tuple(targets)


def _largest_remainder(total: int, weights: tuple[int, ...]) -> tuple[int, ...]:
    denominator = sum(weights)
    parts = [divmod(total * weight, denominator) for weight in weights]
    counts = [part[0] for part in parts]
    for index in sorted(range(len(weights)), key=lambda index: (-parts[index][1], index))[
        : total - sum(counts)
    ]:
        counts[index] += 1
    return tuple(counts)


@dataclass
class _FlowEdge:
    destination: int
    reverse: int
    capacity: int
    cost: int


def _minimum_numa_score(
    totals: tuple[int, ...], preferred: tuple[tuple[int, ...], ...], capacities: tuple[int, ...]
) -> int:
    """Solve the absolute-deviation transportation problem by min-cost flow."""
    group_count, node_count = len(totals), len(capacities)
    source, sink = group_count + node_count, group_count + node_count + 1
    graph: list[list[_FlowEdge]] = [[] for _ in range(sink + 1)]

    def edge(start: int, end: int, capacity: int, cost: int) -> None:
        graph[start].append(_FlowEdge(end, len(graph[end]), capacity, cost))
        graph[end].append(_FlowEdge(start, len(graph[start]) - 1, 0, -cost))

    minima = tuple(int(capacity > 0) for capacity in capacities)
    score = 0
    for group, total in enumerate(totals):
        minimum = minima if group == 0 else (0,) * node_count
        edge(source, group, total - sum(minimum), 0)
        for node, capacity in enumerate(capacities):
            baseline = minimum[node]
            score += abs(baseline - preferred[group][node])
            cheaper = max(0, preferred[group][node] - baseline)
            # Each core reduces deviation until the preference, then increases it.
            edge(group, group_count + node, cheaper, -1)
            edge(group, group_count + node, capacity, 1)
    for node, capacity in enumerate(capacities):
        edge(group_count + node, sink, capacity - minima[node], 0)

    remaining = sum(totals) - sum(minima)
    while remaining:
        distance: list[int | None] = [None] * len(graph)
        parents: list[tuple[int, int] | None] = [None] * len(graph)
        distance[source] = 0
        for _ in range(len(graph) - 1):
            changed = False
            for start, edges in enumerate(graph):
                start_distance = distance[start]
                if start_distance is None:
                    continue
                for index, item in enumerate(edges):
                    candidate = start_distance + item.cost
                    destination_distance = distance[item.destination]
                    if item.capacity > 0 and (
                        destination_distance is None or candidate < destination_distance
                    ):
                        distance[item.destination] = candidate
                        parents[item.destination] = (start, index)
                        changed = True
            if not changed:
                break
        sink_distance = distance[sink]
        if sink_distance is None:
            raise CPUPlannerError("Cannot satisfy per-node housekeeping and group totals")
        amount, current = remaining, sink
        path: list[tuple[int, int]] = []
        while current != source:
            parent = parents[current]
            if parent is None:
                raise CPUPlannerError("Incomplete NUMA allocation path")
            start, index = parent
            path.append((start, index))
            amount = min(amount, graph[start][index].capacity)
            current = start
        for start, index in path:
            item = graph[start][index]
            item.capacity -= amount
            graph[item.destination][item.reverse].capacity += amount
        remaining -= amount
        score += amount * sink_distance
    return score


def _bounded_counts(
    total: int, limits: tuple[int, ...], minima: tuple[int, ...]
) -> Iterator[tuple[int, ...]]:
    if len(limits) == 1:
        if minima[0] <= total <= limits[0]:
            yield (total,)
        return
    low = max(minima[0], total - sum(limits[1:]))
    high = min(limits[0], total - sum(minima[1:]))
    for value in range(low, high + 1):
        for rest in _bounded_counts(total - value, limits[1:], minima[1:]):
            yield (value, *rest)


def _optimal_node_counts(
    totals: tuple[int, ...],
    preferred: tuple[tuple[int, ...], ...],
    capacities: tuple[int, ...],
    score: int,
) -> Iterator[tuple[tuple[int, ...], ...]]:
    """Enumerate only minimum-NUMA-score matrices, in count tie-break order."""

    def visit(
        group: int,
        available: tuple[int, ...],
        remaining_score: int,
        rows: tuple[tuple[int, ...], ...],
    ) -> Iterator[tuple[tuple[int, ...], ...]]:
        if group == len(totals):
            if not any(available) and remaining_score == 0:
                yield rows
            return
        minimum = (
            tuple(int(value > 0) for value in capacities) if group == 0 else (0,) * len(capacities)
        )
        limits = tuple(
            min(available[node], preferred[group][node] + remaining_score)
            for node in range(len(capacities))
        )
        minimum = tuple(
            max(minimum[node], preferred[group][node] - remaining_score)
            for node in range(len(capacities))
        )
        for row in _bounded_counts(totals[group], limits, minimum):
            deviation = sum(
                abs(value - target) for value, target in zip(row, preferred[group], strict=True)
            )
            if deviation > remaining_score:
                continue
            rest = tuple(value - used for value, used in zip(available, row, strict=True))
            lower_bound = sum(
                abs(rest[node] - sum(target[node] for target in preferred[group + 1 :]))
                for node in range(len(capacities))
            )
            if lower_bound <= remaining_score - deviation:
                yield from visit(group + 1, rest, remaining_score - deviation, (*rows, row))

    yield from visit(0, capacities, score, ())


_CacheState = tuple[tuple[tuple[int, ...], int], ...]


def _pairs(mask: int) -> int:
    groups = mask.bit_count()
    return groups * (groups - 1) // 2


class _CacheAllocator:
    """Exact cache-domain search, memoized by interchangeable domain states."""

    def __init__(self, group_count: int, node_count: int) -> None:
        self.group_count = group_count
        self.node_count = node_count
        self._cached_cost = lru_cache(maxsize=None)(self._cost)

    def minimum(self, domains: _CacheState, demands: tuple[int, ...]) -> int | None:
        finished = sum(_pairs(mask) for counts, mask in domains if not any(counts))
        live = tuple(
            sorted(
                ((counts, mask) for counts, mask in domains if any(counts)),
                key=lambda item: (item[1] == 0, sum(item[0]), item),
            )
        )
        cost = self._cached_cost(live, demands)
        return None if cost is None else finished + cost

    def _lower_bound(self, domains: _CacheState, demands: tuple[int, ...]) -> int:
        if any(sum(value > 0 for value in counts) != 1 for counts, _ in domains):
            return sum(_pairs(mask) for _, mask in domains)
        bound = 0
        for node in range(self.node_count):
            local = [(counts[node], mask) for counts, mask in domains if counts[node]]
            # Allowing domain reuse overestimates pure domains, so the sharing
            # lower bound remains safe for pruning the exact search.
            pure_limit = 0
            for group in range(self.group_count):
                available = demands[group * self.node_count + node]
                sizes = sorted(size for size, mask in local if mask in (0, 1 << group))
                for size in sizes:
                    if size > available:
                        break
                    available -= size
                    pure_limit += 1
            shared = sum(mask.bit_count() > 1 for _, mask in local)
            bound += max(shared, len(local) - pure_limit)
            bound += sum(max(0, _pairs(mask) - 1) for _, mask in local)
        return bound

    def _choices(
        self, counts: tuple[int, ...], mask: int, demands: tuple[int, ...]
    ) -> list[tuple[int, tuple[int, ...]]]:
        per_node = [
            tuple(
                _bounded_counts(
                    counts[node],
                    tuple(
                        demands[group * self.node_count + node]
                        for group in range(self.group_count)
                    ),
                    (0,) * self.group_count,
                )
            )
            for node in range(self.node_count)
        ]
        choices = []
        for parts in product(*per_node):
            assignment = tuple(
                parts[node][group]
                for group in range(self.group_count)
                for node in range(self.node_count)
            )
            used = mask
            for group in range(self.group_count):
                if any(parts[node][group] for node in range(self.node_count)):
                    used |= 1 << group
            choices.append((_pairs(used), assignment))
        return sorted(choices, key=lambda item: (item[0], tuple(-value for value in item[1])))

    def _cost(self, domains: _CacheState, demands: tuple[int, ...]) -> int | None:
        if not domains:
            return None if any(demands) else 0
        if any(
            sum(counts[node] for counts, _ in domains)
            != sum(demands[group * self.node_count + node] for group in range(self.group_count))
            for node in range(self.node_count)
        ):
            return None
        lower = self._lower_bound(domains, demands)
        best: int | None = None
        counts, mask = domains[0]
        for cost, assignment in self._choices(counts, mask, demands):
            if best is not None and cost >= best:
                continue
            remaining = tuple(
                value - used for value, used in zip(demands, assignment, strict=True)
            )
            if best is not None and cost + self._lower_bound(domains[1:], remaining) >= best:
                continue
            rest = self.minimum(domains[1:], remaining)
            if rest is not None and (best is None or cost + rest < best):
                best = cost + rest
                if best == lower:
                    break
        return best


def _cache_domains(
    cores: tuple[PhysicalCore, ...], nodes: tuple[int, ...]
) -> tuple[_CacheState, dict[CacheDomain, int]]:
    caches = sorted(
        {core.cache for core in cores if core.cache is not None},
        key=lambda cache: (cache.level, cache.cpu_ids),
    )
    indexes = {cache: index for index, cache in enumerate(caches)}
    capacities = [[0] * len(nodes) for _ in caches]
    for core in cores:
        if core.cache is None:
            raise CPUPlannerError("Cannot establish cache topology for an eligible core")
        capacities[indexes[core.cache]][nodes.index(core.numa_node)] += 1
    return tuple((tuple(counts), 0) for counts in capacities), indexes


def _lexicographic_cores(
    cores: tuple[PhysicalCore, ...],
    nodes: tuple[int, ...],
    counts: tuple[tuple[int, ...], ...],
    domains: _CacheState,
    indexes: dict[CacheDomain, int],
    solver: _CacheAllocator,
    cache_score: int,
) -> tuple[tuple[PhysicalCore, ...], ...]:
    """Take each earliest core only when an optimal completion still exists."""
    remaining = [value for row in counts for value in row]
    state = list(domains)
    selected: dict[int, int] = {}
    for group in range(len(counts)):
        for index, core in enumerate(cores):
            node = nodes.index(core.numa_node)
            demand_index = group * len(nodes) + node
            if index in selected or remaining[demand_index] == 0:
                continue
            if core.cache is None:
                raise CPUPlannerError("Cannot establish cache topology for an eligible core")
            cache_index = indexes[core.cache]
            original = state[cache_index]
            capacities, mask = original
            capacities = tuple(value - int(i == node) for i, value in enumerate(capacities))
            state[cache_index] = (capacities, mask | (1 << group))
            remaining[demand_index] -= 1
            if solver.minimum(tuple(state), tuple(remaining)) == cache_score:
                selected[index] = group
            else:
                state[cache_index] = original
                remaining[demand_index] += 1
    if any(remaining) or len(selected) != len(cores):
        raise CPUPlannerError("Cannot reconstruct the optimal CPU allocation")
    return tuple(
        tuple(core for index, core in enumerate(cores) if selected[index] == group)
        for group in range(len(counts))
    )


def _validate_allocation(
    assignments: tuple[tuple[PhysicalCore, ...], ...],
    cores: tuple[PhysicalCore, ...],
    totals: tuple[int, ...],
    nodes: tuple[int, ...],
    preferred: tuple[tuple[int, ...], ...],
    numa_score: int,
    cache_score: int,
) -> None:
    allocated = tuple(sorted((core for group in assignments for core in group), key=_core_order))
    if allocated != cores or tuple(len(group) for group in assignments) != totals:
        raise CPUPlannerError("CPU pools do not partition eligible cores with the required totals")
    if {core.numa_node for core in assignments[0]} != {core.numa_node for core in cores}:
        raise CPUPlannerError("Housekeeping does not cover every eligible NUMA node")
    actual_numa = sum(
        abs(sum(core.numa_node == node for core in assigned) - preferred[group][index])
        for group, assigned in enumerate(assignments)
        for index, node in enumerate(nodes)
    )
    cache_groups: dict[CacheDomain, set[int]] = {}
    for group, assigned in enumerate(assignments):
        for core in assigned:
            if core.cache is None:
                raise CPUPlannerError("Cannot validate an allocation without cache topology")
            cache_groups.setdefault(core.cache, set()).add(group)
    actual_cache = sum(len(groups) * (len(groups) - 1) // 2 for groups in cache_groups.values())
    if (actual_numa, actual_cache) != (numa_score, cache_score):
        raise CPUPlannerError("CPU pool allocation differs from the optimal scores")


def allocate_cpu_pools(
    topology: HostTopology, profile: CPUProfile, *, components: frozenset[str]
) -> CPUPlan:
    """Allocate globally optimal pools for a selected profile without changing the host."""
    requirements = _requirements(profile, components)
    cores = _eligible_cores(topology)
    totals = _targets(requirements, cores)
    devices = {
        "microceph": topology.osd_devices,
        "microovn": topology.ovn_devices,
    }
    node_ids = {core.numa_node for core in cores}
    for requirement in requirements:
        node_ids.update(device.numa_node for device in devices.get(requirement.name, ()))
    nodes = tuple(sorted(node_ids))
    capacities = tuple(sum(core.numa_node == node for core in cores) for node in nodes)
    preferences: list[tuple[int, ...]] = []
    for requirement, total in zip(requirements, totals, strict=True):
        inventory = devices.get(requirement.name, ())
        weights = (
            tuple(sum(device.numa_node == node for device in inventory) for node in nodes)
            if inventory
            else capacities
        )
        preferences.append(_largest_remainder(total, weights))
    preferred = tuple(preferences)
    numa_score = _minimum_numa_score(totals, preferred, capacities)
    domains, indexes = _cache_domains(cores, nodes)
    solver = _CacheAllocator(len(requirements), len(nodes))
    best_counts: tuple[tuple[int, ...], ...] | None = None
    cache_score: int | None = None
    for counts in _optimal_node_counts(totals, preferred, capacities, numa_score):
        cost = solver.minimum(domains, tuple(value for row in counts for value in row))
        if cost is not None and (cache_score is None or cost < cache_score):
            best_counts, cache_score = counts, cost
            if cost == 0:
                break
    if best_counts is None or cache_score is None:
        raise CPUPlannerError("Cannot satisfy CPU pool allocation requirements")
    assignments = _lexicographic_cores(
        cores, nodes, best_counts, domains, indexes, solver, cache_score
    )
    _validate_allocation(assignments, cores, totals, nodes, preferred, numa_score, cache_score)
    pools = []
    for requirement, assigned in zip(requirements, assignments, strict=True):
        cpus = tuple(
            (
                node,
                tuple(
                    sorted(
                        cpu for core in assigned if core.numa_node == node for cpu in core.cpu_ids
                    )
                ),
            )
            for node in nodes
            if any(core.numa_node == node for core in assigned)
        )
        pools.append(
            CPUPool(
                name=requirement.name,
                cores=assigned,
                cpu_ids_by_numa=cpus,
                memory_nodes=()
                if requirement.name == "housekeeping"
                else tuple(node for node, _ in cpus),
            )
        )
    return CPUPlan(
        profile_name=profile.name,
        profile_version=profile.version,
        topology=topology,
        pools=tuple(pools),
        excluded_cores=tuple(core for core in topology.cores if core.exclusions),
        numa_score=numa_score,
        cache_score=cache_score,
    )


def plan_cpu_pools(topology: HostTopology, *, components: frozenset[str]) -> CPUPlan:
    """Select a host profile and return a read-only CPU pool plan."""
    return allocate_cpu_pools(
        topology, _select_profile(topology, components), components=components
    )
