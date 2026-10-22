# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""This unit's network: addresses and subnets from Juju space bindings, and its OVN uplink."""

import json
import logging
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import ops
import yaml

import microcloud

logger = logging.getLogger(__name__)

SYS_CLASS_NET = Path("/sys/class/net")

# Kernel interface flags (include/uapi/linux/if.h).
_IFF_UP = 0x1
_IFF_BROADCAST = 0x2

# Interface kinds Open vSwitch can take as an OVN uplink, as MicroCloud
# accepts them. Bridges are left out: an unaddressed bridge on a MAAS
# machine is not an uplink.
_UPLINK_KINDS = ("physical", "bond", "vlan")


def per_host_value(raw: str, option: str) -> tuple[str, str | None]:
    """Resolve a config option that is one value, or a mapping per hostname.

    A single value applies to every unit. A YAML/JSON mapping of hostname to
    value covers hardware where the value differs per machine; a unit
    missing from the mapping cannot guess, so it blocks.

    Returns (value, problem). "problem" is a human-readable status string if
    reconciliation should block; "value" is "" when the option is unset.
    """
    raw = raw.strip()
    if not raw:
        return "", None

    try:
        parsed = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        return "", f"Cannot parse {option}: {exc}"

    if not isinstance(parsed, dict):
        return raw, None

    hostname = microcloud.hostname()
    value = parsed.get(hostname)
    if not value:
        known = sorted(str(key) for key in parsed)
        return (
            "",
            f"{option} is missing an entry for hostname {hostname!r}; known entries: {known}",
        )
    return str(value), None


class UplinkError(Exception):
    """The OVN uplink interface cannot be chosen on this machine."""


class UnitNetwork:
    """Helper that resolves this unit's network settings from bindings and config."""

    def __init__(self, model: ops.Model, config: Mapping[str, Any]) -> None:
        self._model = model
        self._config = config

    def bind_address(self) -> str:
        """Return this unit's bind address for the peer relation."""
        return self.space_bind_address("cluster")

    def binding_network(self, binding_name: str) -> ops.Network | None:
        """Return the Network for the given endpoint, or None if unavailable.

        Both ``get_binding()`` and accessing its ``.network`` property can
        raise "ModelError: no network config found for binding ..." when the
        endpoint has no usable network info at all (e.g. an extra-binding
        left unbound with no default space fallback), so both must be
        guarded by the same try/except.
        """
        try:
            binding = self._model.get_binding(binding_name)
            if not binding:
                return None
            return binding.network
        except ops.ModelError:
            return None

    def space_bind_address(self, binding_name: str) -> str:
        """Return this unit's bind address for the given endpoint, if any.

        Since "network-get" is evaluated per-unit, binding an endpoint (e.g.
        the "ovn-underlay" extra-binding) to a Juju space naturally yields a
        different address per machine, unlike a single shared config value.
        Returns "" if the endpoint has no usable address (e.g. left unbound).
        """
        network = self.binding_network(binding_name)
        if not network or not network.bind_address:
            return ""
        return str(network.bind_address)

    def space_network_cidr(self, binding_name: str) -> str:
        """Return the CIDR of the subnet bound to the given extra-binding, if any.

        Lets operators point Ceph's public/internal network at a Juju space
        (via "juju deploy --bind") instead of hard-coding a CIDR. Returns ""
        if the endpoint has no usable subnet (e.g. left unbound).
        """
        network = self.binding_network(binding_name)
        if not network or not network.interfaces:
            return ""
        subnet = network.interfaces[0].subnet
        return str(subnet) if subnet else ""

    def uplink_space_unaddressed(self) -> bool:
        """Whether "ovn-uplink" is bound to a space this machine has no address in.

        That is how a binding to the uplink space looks from the unit: the NIC
        on that space is up without an IP, so "network-get" finds no address
        and fails. Left unbound, the endpoint falls back to the default space
        and does return an address. So this tells apart an operator who asked
        for an uplink through the binding from one who did not bind it at all.
        """
        return not self.space_bind_address("ovn-uplink")

    def configured_uplink_interface(self) -> tuple[str, str | None]:
        """Return the uplink interface named by "ovn-uplink-interface", if any.

        Only needed where discovery is ambiguous: several interfaces are up
        with no global address, as when unnumbered BGP NICs sit next to the
        uplink. A name that is not on this machine blocks, since MicroCloud
        accepts it and forms a cluster with no uplink at all.

        Returns (interface, problem). "interface" is "" when the option is
        unset.
        """
        raw = str(self._config.get("ovn-uplink-interface", ""))
        interface, problem = per_host_value(raw, "ovn-uplink-interface")
        if problem or not interface:
            return "", problem

        if not (SYS_CLASS_NET / interface).exists():
            try:
                present = sorted(p.name for p in SYS_CLASS_NET.iterdir() if p.name != "lo")
            except OSError:
                present = []
            return "", (
                f"ovn-uplink-interface {interface!r} does not exist on "
                f"{microcloud.hostname()}; interfaces: {present}"
            )
        return interface, None

    def discover_uplink_interface(self, required: bool) -> tuple[str, str | None]:
        """Find this machine's OVN uplink interface.

        Juju cannot name it: a space is a set of subnets, and "network-get"
        only lists interfaces through their addresses, while the uplink is an
        L2-only NIC with none (MAAS brings it up in "link_up" mode). Binding
        the "ovn-uplink" endpoint to its space still matters, because MAAS
        then only allocates machines with a NIC on that space's VLAN.

        So the interface is found on the host instead, using the rules
        MicroCloud applies when it offers uplink interfaces: up, broadcast,
        no global address, and a physical NIC, bond or VLAN. Bond members,
        bridge ports and the parents of VLANs or bonds are also left out,
        since they carry another interface's traffic.

        ``required`` is whether an uplink was asked for: "ovn-uplink" is bound
        to the uplink space, or an OVN gateway is configured. Otherwise a
        machine with no candidate omits the uplink, as an unset uplink always
        did.

        Returns (interface, problem). "problem" is a human-readable status
        string if reconciliation should block.
        """
        try:
            candidates = uplink_candidates()
        except UplinkError as exc:
            return "", f"Cannot list network interfaces: {exc}"

        if len(candidates) == 1:
            return candidates[0], None

        hostname = microcloud.hostname()
        if candidates:
            return "", (
                f"Cannot choose the OVN uplink on {hostname}: several unaddressed "
                f"interfaces are up: {candidates}; set ovn-uplink-interface"
            )

        if required:
            return "", (
                f"No OVN uplink on {hostname}: no unaddressed interface is up, "
                "but ovn-uplink is bound to a space or an OVN gateway is configured"
            )

        logger.warning("No unaddressed interface is up on %s: omitting the OVN uplink", hostname)
        return "", None


def uplink_candidates() -> list[str]:
    """Return the interfaces on this machine that can be the OVN uplink, sorted."""
    addressed = _addressed_interfaces()

    try:
        paths = sorted(SYS_CLASS_NET.iterdir())
    except OSError as exc:
        raise UplinkError(str(exc)) from exc

    candidates = []
    for path in paths:
        name = path.name
        if name in addressed or _interface_kind(path) not in _UPLINK_KINDS:
            continue
        if not _flags(path) & _IFF_UP or not _flags(path) & _IFF_BROADCAST:
            continue
        # Enslaved to a bond, bridge or OVS, or carrying a VLAN or bond.
        if (path / "master").exists() or any(path.glob("upper_*")):
            continue
        candidates.append(name)
    return candidates


def _interface_kind(path: Path) -> str:
    """Return "physical", "bond", "vlan", "bridge" or another kind for an interface."""
    devtype = ""
    try:
        for line in (path / "uevent").read_text().splitlines():
            key, _, value = line.partition("=")
            if key == "DEVTYPE":
                devtype = value
    except OSError:
        pass

    if devtype:
        return devtype
    # Physical NICs are backed by a device and carry no DEVTYPE.
    return "physical" if (path / "device").exists() else "virtual"


def _flags(path: Path) -> int:
    try:
        return int((path / "flags").read_text().strip(), 16)
    except (OSError, ValueError):
        return 0


def _addressed_interfaces() -> set[str]:
    """Return the interfaces that have a global address.

    Link-local addresses don't count: the kernel gives every IPv6-enabled
    interface one, including the uplink.
    """
    try:
        out = subprocess.run(
            ["ip", "-json", "address", "show"],
            capture_output=True,
            check=True,
            text=True,
            timeout=30,
        ).stdout
        links = json.loads(out)
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
        raise UplinkError(str(exc)) from exc

    return {
        link["ifname"]
        for link in links
        if any(addr.get("scope") == "global" for addr in link.get("addr_info", []))
    }
