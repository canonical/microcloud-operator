# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""This unit's network: addresses and subnets from Juju space bindings, and its OVN uplink."""

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import ops
import yaml

import microcloud


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
