# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Bind each Ceph OSD on this unit to the addresses of its NUMA node.

A host with a network card on each NUMA node can keep an OSD's traffic on
the card attached to the same node as its disk. The machine's deployment
puts one shared address per NUMA node and Ceph network on "lo", inside a
/64 that belongs to that node, and routes it through that node's card. The
charm owns no routing: it only binds the OSDs to those addresses.

Two options map each NUMA node to its /64, one per Ceph network:
"ceph-public-numa-networks" for ``public_addr`` and
"ceph-internal-numa-networks" for ``cluster_addr``. Empty maps turn NUMA
steering off. On each unit, the address on "lo" inside a node's /64 is that
node's steered address; a node with none is unsteered on this host.

For each OSD, the charm reads its disk's NUMA node and sets ``public_addr``,
``cluster_addr`` and ``osd_numa_node`` to the node's steered addresses, or
the unsteered addresses of "ceph-public-network" and "ceph-internal-network"
without ``osd_numa_node``. It also sets ``ms_bind_before_connect`` for every
OSD, so that outgoing connections leave from the bound addresses too.

Ceph raises OSD_UNREACHABLE, a HEALTH_ERR, for an OSD whose public address
is outside "public_network", which cannot change once the cluster is
bootstrapped. An OSD whose steered public address it does not cover keeps
the unsteered public address, and still binds its steered cluster address.
Once "public_network" covers the NUMA subnets, the manager and metadata
server on the host are pinned to the unsteered public address, since Ceph
would otherwise bind them to the newest address in its networks.

Ceph reads these options when an OSD starts, so an OSD whose options change
is restarted, one at a time, and only while every other OSD is up.
"""

import ipaddress
import json
import os
import re
import subprocess
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import yaml

# The option that maps each NUMA node to its /64, per Ceph network, and the
# option of the network's unsteered /64 that the map must not overlap.
NUMA_OPTIONS = {
    "public": ("ceph-public-numa-networks", "ceph-public-network"),
    "internal": ("ceph-internal-numa-networks", "ceph-internal-network"),
}
NUMA_PREFIXLEN = 64

# How long to wait for a restarted OSD to report up again.
RESTART_TIMEOUT = 300

NumaNetworks = dict[str, dict[int, ipaddress.IPv6Network]]


class SteeringError(Exception):
    """Raised when the disks or Ceph cannot be read or changed."""


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------


def _parse_map(option: str, raw: str) -> tuple[dict[int, ipaddress.IPv6Network], str | None]:
    """Parse one NUMA network map: a JSON or YAML map of NUMA node to IPv6 /64."""
    try:
        parsed = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        return {}, f"Cannot parse {option}: {exc}"
    if not isinstance(parsed, dict):
        return {}, f"{option} must map each NUMA node to an IPv6 /64"

    found = {}
    for key, value in parsed.items():
        # YAML reads an unquoted 0 as an int, JSON gives "0"; neither may be a bool.
        if isinstance(key, bool) or not re.fullmatch(r"[0-9]+", str(key)):
            return {}, f"{option}: NUMA node {key!r} is not a non-negative integer"
        try:
            network = ipaddress.ip_network(str(value))
        except ValueError:
            network = None
        if not isinstance(network, ipaddress.IPv6Network) or network.prefixlen != NUMA_PREFIXLEN:
            return {}, f"{option}: {value!r} for NUMA node {key} is not an IPv6 /64"
        found[int(str(key))] = network
    return found, None


def numa_networks(config: Mapping[str, Any]) -> tuple[NumaNetworks, str | None]:
    """Return the NUMA network maps, by Ceph network, or a problem to block on.

    Both empty is steering turned off: an empty result and no problem.
    """
    maps: NumaNetworks = {}
    for name, (option, _) in NUMA_OPTIONS.items():
        raw = config.get(option, "")
        # Juju gives a string option as a string; anything else is unset.
        raw = raw.strip() if isinstance(raw, str) else ""
        if not raw:
            continue
        maps[name], problem = _parse_map(option, raw)
        if problem:
            return {}, problem
        if not maps[name]:
            del maps[name]

    if not maps:
        return {}, None
    if len(maps) != len(NUMA_OPTIONS):
        missing = next(option for name, (option, _) in NUMA_OPTIONS.items() if name not in maps)
        return {}, f"NUMA steering needs {missing} too"
    if set(maps["public"]) != set(maps["internal"]):
        public, internal = (NUMA_OPTIONS[name][0] for name in ("public", "internal"))
        return {}, f"{public} and {internal} must name the same NUMA nodes"

    # Every NUMA /64 must be apart from every other one and from both
    # unsteered networks. The unsteered networks may be unset or invalid,
    # which network_options_problem reports, or equal to each other.
    named: list[tuple[str, ipaddress.IPv6Network, bool]] = []
    for name, (option, unsteered_option) in NUMA_OPTIONS.items():
        named.extend(
            (f"{option} node {node}", net, True) for node, net in sorted(maps[name].items())
        )
        try:
            unsteered = ipaddress.ip_network(str(config.get(unsteered_option, "") or "").strip())
        except ValueError:
            continue
        if isinstance(unsteered, ipaddress.IPv6Network):
            named.append((unsteered_option, unsteered, False))
    for i, (first, a, a_numa) in enumerate(named):
        for second, b, b_numa in named[i + 1 :]:
            if (a_numa or b_numa) and a.overlaps(b):
                return {}, f"{first} ({a}) overlaps {second} ({b})"
    return maps, None


# ----------------------------------------------------------------------
# Addresses
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class Addresses:
    """The public and cluster addresses an OSD binds."""

    public: str
    cluster: str


def steered_addresses(
    maps: NumaNetworks, on_lo: Iterable[ipaddress.IPv6Address]
) -> tuple[dict[int, Addresses], str | None]:
    """Return this host's steered addresses by NUMA node, or a problem to block on.

    ``on_lo`` is every IPv6 address on "lo", deprecated ones included: the
    steered addresses have a preferred lifetime of 0. A node with no address
    in either of its /64s is unsteered on this host and left out.
    """
    on_lo = sorted(set(on_lo))
    found: dict[int, Addresses] = {}
    for node in sorted(maps.get("public", {})):
        matches = {}
        for name in ("public", "internal"):
            network = maps[name][node]
            inside = [address for address in on_lo if address in network]
            if len(inside) > 1:
                return {}, f"{len(inside)} addresses on lo in {network}"
            matches[name] = str(inside[0]) if inside else None
        if matches["public"] and matches["internal"]:
            found[node] = Addresses(matches["public"], matches["internal"])
        elif matches["public"] or matches["internal"]:
            have, lack = ("public", "internal") if matches["public"] else ("internal", "public")
            return {}, (
                f"NUMA node {node} has {matches[have]} on lo but no address in {maps[lack][node]}"
            )
    return found, None


def numa_node(device: str, sys_root: str = "/sys") -> int:
    """Return the NUMA node of block device ``device``, or -1 if unknown.

    ``device`` may be a /dev/disk/by-id link. The NUMA node is read from the
    nearest device above the block device that has one, such as the NVMe
    controller's PCI function.
    """
    name = os.path.basename(os.path.realpath(device))
    node = Path(os.path.realpath(os.path.join(sys_root, "class", "block", name)))
    root = Path(sys_root)
    while node != root and root in node.parents:
        candidate = node / "numa_node"
        if candidate.is_file():
            try:
                return int(candidate.read_text().strip())
            except (OSError, ValueError):
                return -1
        node = node.parent
    return -1


# ----------------------------------------------------------------------
# OSD options
# ----------------------------------------------------------------------

# Set for every OSD, in the "osd" section: without it an OSD's outgoing
# connections take the source address the kernel picks, the unsteered one.
BIND_BEFORE_CONNECT = "ms_bind_before_connect"


@dataclass(frozen=True)
class Binding:
    """The addresses and NUMA node one OSD should use."""

    osd: int
    public_addr: str
    cluster_addr: str
    # None leaves the OSD unpinned.
    numa_node: int | None = None

    def options(self) -> dict[str, str]:
        """Return the per-OSD Ceph options that apply this binding to ``osd.<n>``."""
        options = {"public_addr": self.public_addr, "cluster_addr": self.cluster_addr}
        if self.numa_node is not None:
            options["osd_numa_node"] = str(self.numa_node)
        return options


@dataclass(frozen=True)
class Change:
    """What to change for one OSD before restarting it."""

    set: dict[str, str] = field(default_factory=dict)
    remove: tuple[str, ...] = ()


def bindings(
    steered: Mapping[int, Addresses],
    unsteered: Addresses,
    disks: Mapping[int, str],
    numa_of: Callable[[str], int] | None = None,
) -> list[Binding]:
    """Return the binding of each OSD in ``disks`` (OSD number to device path).

    An OSD on a NUMA node with steered addresses binds them and is pinned to
    the node; any other OSD binds the unsteered addresses. A disk with no
    NUMA information (-1, as in VMs) counts as node 0, as the machine's
    deployment does for its network cards. ``numa_of`` defaults to
    ``numa_node``.
    """
    numa_of = numa_of or numa_node
    result = []
    for osd, device in sorted(disks.items()):
        node = max(numa_of(device), 0)
        addresses = steered.get(node)
        if addresses is None:
            result.append(Binding(osd, unsteered.public, unsteered.cluster))
        else:
            result.append(Binding(osd, addresses.public, addresses.cluster, node))
    return result


def changes(
    wanted: list[Binding],
    current: Mapping[int, Mapping[str, str]],
    running_bind: Mapping[int, bool],
) -> dict[int, Change]:
    """Return what to change on each OSD that needs a restart.

    ``current`` holds each OSD's options in Ceph's configuration and
    ``running_bind`` whether its daemon runs with ``ms_bind_before_connect``.
    An OSD whose daemon does not yet run with it needs a restart even if its
    own options are right.
    """
    found = {}
    for binding in wanted:
        have = current.get(binding.osd, {})
        diff = {k: v for k, v in binding.options().items() if not _same(k, have.get(k), v)}
        remove = (
            ("osd_numa_node",) if binding.numa_node is None and "osd_numa_node" in have else ()
        )
        if diff or remove or not running_bind.get(binding.osd, False):
            found[binding.osd] = Change(diff, remove)
    return found


# Ceph stores an address option as an address vector, such as
# "v2:[fd00::1]:0/0", not as the address it was set to.
_ADDRESS_OPTIONS = frozenset({"public_addr", "cluster_addr"})
_VECTOR_ADDRESS = re.compile(r"\[([0-9A-Fa-f:.]+)\]")


def _same(option: str, have: str | None, want: str) -> bool:
    """Return True if Ceph's stored value ``have`` already sets ``want``."""
    if have is None:
        return False
    if option not in _ADDRESS_OPTIONS:
        return have == want
    match = _VECTOR_ADDRESS.search(have)
    try:
        return ipaddress.ip_address(match.group(1) if match else have) == ipaddress.ip_address(
            want
        )
    except ValueError:
        return False


# ----------------------------------------------------------------------
# The host and Ceph
# ----------------------------------------------------------------------


def _run(*command: str, timeout: int = 60) -> str:
    try:
        return subprocess.run(
            command, capture_output=True, text=True, check=True, timeout=timeout
        ).stdout
    except subprocess.CalledProcessError as exc:
        raise SteeringError(f"{' '.join(command[:4])} failed: {exc.stderr.strip()}") from exc
    except (OSError, subprocess.SubprocessError) as exc:
        raise SteeringError(f"{' '.join(command[:4])} failed: {exc}") from exc


def _json(*command: str) -> Any:
    try:
        return json.loads(_run(*command))
    except json.JSONDecodeError as exc:
        raise SteeringError(f"Invalid JSON from {' '.join(command[:4])}: {exc}") from exc


def local_disks(hostname: str) -> dict[int, str]:
    """Return the OSD number and device path of each OSD on ``hostname``."""
    listed = _json("microceph", "disk", "list", "--json")
    disks = listed.get("ConfiguredDisks") if isinstance(listed, dict) else None
    return {
        int(disk["osd"]): str(disk["path"])
        for disk in disks or []
        if isinstance(disk, dict) and disk.get("location") == hostname and "osd" in disk
    }


def public_networks() -> list[ipaddress.IPv6Network]:
    """Return the networks of Ceph's "public_network", which may be a comma-separated list.

    Set when the cluster is bootstrapped; it cannot change after that.
    """
    raw = _run("microceph.ceph", "config", "get", "osd", "public_network").strip()
    try:
        return [ipaddress.IPv6Network(item.strip()) for item in raw.split(",") if item.strip()]
    except ValueError as exc:
        raise SteeringError(f"Ceph public_network {raw!r} is not IPv6: {exc}") from exc


def outside_public_network(
    wanted: list[Binding], networks: list[ipaddress.IPv6Network]
) -> list[str]:
    """Return the public addresses in ``wanted`` that ``networks`` does not cover.

    Ceph raises OSD_UNREACHABLE, a HEALTH_ERR, for an up OSD whose public
    address is outside "public_network", so such an address must not be
    bound. Ceph skips the check when "public_network" is unset, and so does
    this. "cluster_network" has no such check.
    """
    if not networks:
        return []
    return sorted(
        {binding.public_addr for binding in wanted if not covers(networks, binding.public_addr)}
    )


def covers(networks: Iterable[ipaddress.IPv6Network], address: str) -> bool:
    """Return True if ``address`` is in one of ``networks``."""
    return any(ipaddress.IPv6Address(address) in network for network in networks)


def inside_public_network(
    wanted: list[Binding], networks: list[ipaddress.IPv6Network], unsteered_public: str
) -> tuple[list[Binding], list[str]]:
    """Keep each OSD's public address inside Ceph's "public_network".

    Returns ``wanted`` with every public address that ``networks`` does not
    cover replaced by ``unsteered_public``, and the addresses replaced. Such
    an OSD still binds its steered cluster address and is still pinned to
    its NUMA node.
    """
    outside = outside_public_network(wanted, networks)
    if not outside:
        return wanted, []
    return [
        replace(binding, public_addr=unsteered_public)
        if binding.public_addr in outside
        else binding
        for binding in wanted
    ], outside


# The daemons other than OSDs that bind an address in "public_network". Ceph
# picks the first address on the host that is in one of its networks, and
# Linux lists the newest address first, so once "public_network" covers the
# NUMA subnets these would bind a steered address. They are pinned to the
# unsteered one. MicroCeph picks the monitor's address itself, from the
# networks in the order "public_network" lists them.
PUBLIC_DAEMONS = ("mgr", "mds")


def running_public_addr(who: str) -> str | None:
    """Return the ``public_addr`` daemon ``who`` runs with, "-" if none.

    None if the daemon cannot be asked: it does not run on this host, or is
    down.
    """
    result = subprocess.run(
        ["microceph.ceph", "config", "show", who, "public_addr"],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def daemon_pins(
    hostname: str,
    public: str,
    dump: Mapping[str, Mapping[str, str]],
    running: Mapping[str, str | None],
) -> tuple[list[str], list[str]]:
    """Return the daemons whose ``public_addr`` to set, and those to restart.

    ``running`` maps each of ``PUBLIC_DAEMONS`` to the address its daemon on
    this host runs with, as ``running_public_addr`` gives it. A daemon that
    is not running here is left alone.
    """
    here = [daemon for daemon in PUBLIC_DAEMONS if running.get(daemon) is not None]
    to_set = [
        daemon
        for daemon in here
        if not _same(
            "public_addr", dump.get(f"{daemon}.{hostname}", {}).get("public_addr"), public
        )
    ]
    to_restart = [daemon for daemon in here if not _same("public_addr", running[daemon], public)]
    return to_set, to_restart


def pin_daemon(daemon: str, hostname: str, public: str) -> None:
    """Set ``public_addr`` for this host's ``daemon``."""
    _run("microceph.ceph", "config", "set", f"{daemon}.{hostname}", "public_addr", public)


def restart_daemon(daemon: str) -> None:
    """Restart this host's ``daemon``; MicroCeph runs one of each per host."""
    _run("snap", "restart", f"microceph.{daemon}", timeout=180)


def config_dump() -> dict[str, dict[str, str]]:
    """Return the options in Ceph's configuration database, by section."""
    dump = _json("microceph.ceph", "config", "dump", "--format", "json")
    found: dict[str, dict[str, str]] = {}
    for entry in dump if isinstance(dump, list) else []:
        if isinstance(entry, dict):
            section = str(entry.get("section", ""))
            found.setdefault(section, {})[str(entry.get("name", ""))] = str(entry.get("value", ""))
    return found


def osd_options(
    dump: Mapping[str, Mapping[str, str]], osds: list[int]
) -> dict[int, dict[str, str]]:
    """Return each of ``osds``' own options from a ``config_dump``."""
    return {osd: dict(dump.get(f"osd.{osd}", {})) for osd in osds}


def ensure_bind_before_connect(dump: Mapping[str, Mapping[str, str]]) -> None:
    """Set ``ms_bind_before_connect`` for every OSD, unless it already is."""
    if dump.get("osd", {}).get(BIND_BEFORE_CONNECT) != "true":
        _run("microceph.ceph", "config", "set", "osd", BIND_BEFORE_CONNECT, "true")


def runs_bind_before_connect(osd: int) -> bool:
    """Return True if ``osd.<n>``'s daemon runs with ``ms_bind_before_connect``.

    False also when the daemon cannot be asked, for example while it is down.
    """
    result = subprocess.run(
        ["microceph.ceph", "config", "show", f"osd.{osd}", BIND_BEFORE_CONNECT],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    return result.returncode == 0 and result.stdout.strip() == "true"


def apply(osd: int, change: Change) -> None:
    """Set and remove ``osd.<n>``'s options in Ceph's configuration."""
    for name, value in change.set.items():
        _run("microceph.ceph", "config", "set", f"osd.{osd}", name, value)
    for name in change.remove:
        _run("microceph.ceph", "config", "rm", f"osd.{osd}", name)


def ok_to_stop(osd: int) -> bool:
    """Return True if Ceph says stopping ``osd.<n>`` keeps every placement group available."""
    result = subprocess.run(
        ["microceph.ceph", "osd", "ok-to-stop", f"osd.{osd}"],
        capture_output=True,
        check=False,
        timeout=60,
    )
    return result.returncode == 0


def down_osds() -> list[int]:
    """Return the OSDs that are in the cluster but not up."""
    dump = _json("microceph.ceph", "osd", "dump", "--format", "json")
    osds = dump.get("osds") if isinstance(dump, dict) else None
    return sorted(
        int(osd["osd"])
        for osd in osds or []
        if isinstance(osd, dict) and osd.get("in") == 1 and osd.get("up") != 1
    )


def restart(osd: int, timeout: float = RESTART_TIMEOUT) -> None:
    """Restart ``osd.<n>`` alone, then wait for it to be up again.

    MicroCeph runs every OSD of the host from one service, which starts any
    OSD that is not running when it is reloaded. Stopping this OSD's process
    and reloading the service restarts only this OSD.
    """
    _run("pkill", "-TERM", "-f", f"^ceph-osd --cluster ceph --id {osd}$")
    deadline = time.monotonic() + timeout
    while _process_running(osd):
        if time.monotonic() > deadline:
            raise SteeringError(f"osd.{osd} did not stop within {int(timeout)}s")
        time.sleep(2)
    _run("snap", "restart", "--reload", "microceph.osd")
    while osd in down_osds():
        if time.monotonic() > deadline:
            raise SteeringError(f"osd.{osd} is not up {int(timeout)}s after its restart")
        time.sleep(5)


def _process_running(osd: int) -> bool:
    result = subprocess.run(
        ["pgrep", "-f", f"^ceph-osd --cluster ceph --id {osd}$"],
        capture_output=True,
        check=False,
        timeout=30,
    )
    return result.returncode == 0
