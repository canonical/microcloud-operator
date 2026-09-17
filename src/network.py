# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""This unit's network: addresses and subnets from Juju space bindings, and its OVN uplink."""

from collections.abc import Mapping
from typing import Any

import ops
import yaml

import microcloud


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
        raw = str(self._config.get("ovn-uplink-interface", "")).strip()
        if not raw:
            return "", None

        try:
            parsed = yaml.safe_load(raw)
        except yaml.YAMLError as exc:
            return "", f"Cannot parse ovn-uplink-interface: {exc}"

        if not isinstance(parsed, dict):
            return raw, None

        hostname = microcloud.hostname()
        interface = parsed.get(hostname)
        if not interface:
            known = sorted(str(key) for key in parsed)
            return "", (
                f"ovn-uplink-interface is missing an entry for hostname {hostname!r}; "
                f"known entries: {known}"
            )
        return str(interface), None
