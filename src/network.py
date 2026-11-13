# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""This unit's network: addresses and subnets from Juju space bindings or config, and its OVN uplink."""

import ipaddress
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import ops
import yaml

import microcloud

# The option that gives each space's /64 where its addresses sit on "lo".
NETWORK_OPTIONS = {
    "ceph-public": "ceph-public-network",
    "ceph-internal": "ceph-internal-network",
    "ovn-underlay": "ovn-underlay-network",
}


def derive_address(
    network: ipaddress.IPv6Network, source: ipaddress.IPv6Address
) -> ipaddress.IPv6Address:
    """Return the address in ``network`` that has the host part of ``source``."""
    return ipaddress.IPv6Address(
        int(network.network_address) | (int(source) & int(network.hostmask))
    )


def per_host_value(raw: str, option: str) -> tuple[str, str | None]:
    """Resolve a config option that is one value, or a mapping per hostname.

    Several options name something that differs per machine (an interface, a
    device path). Each accepts either a single value applied everywhere, or a
    YAML/JSON mapping of hostname to value; a unit missing from a mapping
    cannot guess, so it blocks.

    Returns (value, problem). "problem" is a human-readable status string if
    reconciliation should block; "value" is only meaningful when it is None.
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
        return "", (
            f"{option} is missing an entry for hostname {hostname!r}; known entries: {known}"
        )
    return str(value), None


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

    def configured_network(self, binding_name: str) -> ipaddress.IPv6Network | None:
        """Return the /64 configured for the given space, or None.

        None also covers a value that is not an IPv6 /64, which
        ``network_options_problem`` reports.
        """
        raw = str(self._config.get(NETWORK_OPTIONS.get(binding_name, ""), "")).strip()
        if not raw:
            return None
        try:
            network = ipaddress.ip_network(raw)
        except ValueError:
            return None
        if network.version != 6 or network.prefixlen != 64:
            return None
        return network

    def network_options_problem(self) -> str | None:
        """Return a problem if a space's network option cannot be used.

        Each option must be an IPv6 /64, and this unit's cluster address must
        be IPv6, since its host part is what the addresses are derived from.
        A unit with no cluster address yet has nothing to report.
        """
        configured = False
        for binding_name, option in NETWORK_OPTIONS.items():
            raw = str(self._config.get(option, "")).strip()
            if not raw:
                continue
            if self.configured_network(binding_name) is None:
                return f"{option}: {raw!r} is not an IPv6 /64"
            configured = True

        address = self.bind_address()
        if not configured or not address:
            return None
        try:
            ipaddress.IPv6Address(address)
        except ValueError:
            return f"Cluster address {address} is not IPv6: cannot derive addresses from it"
        return None

    def space_address(self, binding_name: str) -> str:
        """Return this unit's address in the given space, if any.

        With the space's network option set, this is that /64 with the host
        part of the unit's cluster address: the space has no interface, so the
        address sits on "lo" and no binding can report it. Otherwise it is the
        binding's address.
        """
        network = self.configured_network(binding_name)
        if network is None:
            return self.space_bind_address(binding_name)
        try:
            source = ipaddress.IPv6Address(self.bind_address())
        except ValueError:
            return ""
        return str(derive_address(network, source))

    def derived_addresses(self) -> dict[ipaddress.IPv6Network, ipaddress.IPv6Address]:
        """Return this unit's derived address in each network that has its option set.

        Empty until the unit has an IPv6 cluster address to derive from.
        """
        try:
            source = ipaddress.IPv6Address(self.bind_address())
        except ValueError:
            return {}

        addresses = {}
        for binding_name in NETWORK_OPTIONS:
            network = self.configured_network(binding_name)
            if network is not None:
                addresses[network] = derive_address(network, source)
        return addresses

    def space_cidr(self, binding_name: str) -> str:
        """Return the CIDR of the given space: its network option, or its binding's subnet."""
        network = self.configured_network(binding_name)
        if network is None:
            return self.space_network_cidr(binding_name)
        return str(network)

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

    def ovn_uplink_interface(self) -> tuple[str, str | None]:
        """Resolve this unit's OVN uplink interface name.

        "ovn-uplink-interface" config is either a single interface name
        applied to every unit, or a YAML/JSON mapping of hostname to
        interface name (for hardware where the NIC name differs per
        machine). When it is a mapping, every unit must have an entry: a
        unit missing from the mapping cannot safely guess an interface
        name, so it must block rather than bootstrap without one (or
        silently omit the OVN uplink and produce a broken cluster).

        There is deliberately no extra-binding fallback here: Juju spaces
        only track addressed interfaces, but the OVN uplink NIC is
        normally a bare, L2-only interface with no IP, so "network-get"
        can never resolve it.

        Returns (interface, problem). "problem" is a human-readable status
        string if reconciliation should block; "interface" is only
        meaningful when "problem" is None.
        """
        raw = str(self._config.get("ovn-uplink-interface", ""))
        return per_host_value(raw, "ovn-uplink-interface")

    def missing_uplink_interface(self, interface: str) -> str | None:
        """Return a problem if ``interface`` is not a NIC on this machine.

        MicroCloud accepts an interface that does not exist and simply leaves
        the uplink out: the cluster then forms and reports ready while having
        no UPLINK network and no default OVN network at all, so nothing it
        hosts can reach the outside. A name that is not there is a typo, not
        a configuration.

        Only worth checking where the name is actually used, which is why the
        caller decides: with MicroOVN disabled the value is never read, and
        blocking on it would refuse a deployment that is perfectly valid.
        """
        if not interface or Path("/sys/class/net", interface).exists():
            return None

        present = sorted(
            path.name for path in Path("/sys/class/net").iterdir() if path.name != "lo"
        )
        return (
            f"ovn-uplink-interface {interface!r} does not exist on "
            f"{microcloud.hostname()}; interfaces: {present}"
        )
