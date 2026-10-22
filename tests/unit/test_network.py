# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for resolving this unit's network settings."""

import ipaddress
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, PropertyMock, patch

import ops
import pytest

from network import UnitNetwork


def _network(*, config=None, binding=None):
    model = MagicMock(spec=ops.Model)
    model.get_binding.return_value = binding
    return UnitNetwork(model, config or {})


# ---------------------------------------------------------------------------
# binding_network (shared get_binding + .network access, both error-guarded)
# ---------------------------------------------------------------------------


class TestBindingNetwork:
    def test_returns_network_from_binding(self):
        network = MagicMock()
        binding = MagicMock()
        binding.network = network

        unit_network = _network(binding=binding)

        assert unit_network.binding_network("ovn-uplink") is network
        unit_network._model.get_binding.assert_called_once_with("ovn-uplink")

    def test_returns_none_when_get_binding_raises(self):
        unit_network = _network()
        unit_network._model.get_binding.side_effect = ops.ModelError("no such binding")

        assert unit_network.binding_network("ovn-uplink") is None

    def test_returns_none_when_network_access_raises(self):
        """Regression test: juju raises "no network config found for binding"
        lazily, when accessing binding.network -- not when calling
        get_binding() itself -- so both must be guarded by the same
        try/except or an unbound extra-binding crashes the install hook."""
        binding = MagicMock()
        type(binding).network = property(
            lambda self: (_ for _ in ()).throw(
                ops.ModelError('no network config found for binding "ovn-uplink"')
            )
        )

        assert _network(binding=binding).binding_network("ovn-uplink") is None

    def test_returns_none_when_binding_falsy(self):
        assert _network(binding=None).binding_network("ovn-uplink") is None


# ---------------------------------------------------------------------------
# space_network_cidr (Juju space binding -> Ceph public/internal network)
# ---------------------------------------------------------------------------


class TestSpaceNetworkCidr:
    def test_returns_cidr_from_bound_subnet(self):
        interface = MagicMock()
        interface.subnet = ipaddress.ip_network("10.42.0.0/24")
        network = MagicMock()
        network.interfaces = [interface]

        unit_network = _network()
        with patch.object(unit_network, "binding_network", return_value=network) as binding:
            assert unit_network.space_network_cidr("ceph-public") == "10.42.0.0/24"
        binding.assert_called_once_with("ceph-public")

    def test_returns_empty_when_no_interfaces(self):
        network = MagicMock()
        network.interfaces = []

        unit_network = _network()
        with patch.object(unit_network, "binding_network", return_value=network):
            assert unit_network.space_network_cidr("ceph-internal") == ""

    def test_returns_empty_when_network_unavailable(self):
        unit_network = _network()
        with patch.object(unit_network, "binding_network", return_value=None):
            assert unit_network.space_network_cidr("ceph-public") == ""


# ---------------------------------------------------------------------------
# space_bind_address (Juju space binding -> per-unit address, e.g. OVN underlay)
# ---------------------------------------------------------------------------


class TestSpaceBindAddress:
    def test_returns_address_from_binding(self):
        network = MagicMock()
        network.bind_address = "10.42.0.5"

        unit_network = _network()
        with patch.object(unit_network, "binding_network", return_value=network) as binding:
            assert unit_network.space_bind_address("ovn-underlay") == "10.42.0.5"
        binding.assert_called_once_with("ovn-underlay")

    def test_returns_empty_when_no_bind_address(self):
        network = MagicMock()
        network.bind_address = None

        unit_network = _network()
        with patch.object(unit_network, "binding_network", return_value=network):
            assert unit_network.space_bind_address("ovn-underlay") == ""

    def test_returns_empty_when_network_unavailable(self):
        unit_network = _network()
        with patch.object(unit_network, "binding_network", return_value=None):
            assert unit_network.space_bind_address("ovn-underlay") == ""

    def test_bind_address_delegates_to_cluster_binding(self):
        unit_network = _network()
        with patch.object(unit_network, "space_bind_address", return_value="10.0.0.1") as bind:
            assert unit_network.bind_address() == "10.0.0.1"
        bind.assert_called_once_with("cluster")


# ---------------------------------------------------------------------------
# discover_uplink_interface (the unaddressed NIC that OVN takes as its uplink)
# ---------------------------------------------------------------------------

UP_BROADCAST = "0x1003"
DOWN_BROADCAST = "0x1002"


@pytest.fixture
def sysnet(tmp_path):
    """Stand in for /sys/class/net, with "ip -json address" reporting ``addressed``."""
    addressed = []

    def ip_address(*args, **kwargs):
        links = [
            {"ifname": name, "addr_info": [{"local": "10.0.0.1", "scope": "global"}]}
            for name in addressed
        ]
        links.append({"ifname": "lo", "addr_info": [{"local": "127.0.0.1", "scope": "host"}]})
        return SimpleNamespace(stdout=json.dumps(links))

    with (
        patch("network.SYS_CLASS_NET", tmp_path),
        patch("network.subprocess.run", side_effect=ip_address),
        patch("network.microcloud.hostname", return_value="node1"),
    ):
        yield SimpleNamespace(root=tmp_path, addressed=addressed)


def _nic(root, name, *, devtype="", physical=True, flags=UP_BROADCAST, master=False, upper=""):
    path = root / name
    path.mkdir()
    (path / "flags").write_text(flags + "\n")
    uevent = f"INTERFACE={name}\nIFINDEX=2\n"
    if devtype:
        uevent += f"DEVTYPE={devtype}\n"
    (path / "uevent").write_text(uevent)
    if physical:
        (path / "device").mkdir()
    if master:
        (path / "master").mkdir()
    if upper:
        (path / f"upper_{upper}").mkdir()
    return path


class TestDiscoverUplinkInterface:
    def test_the_one_unaddressed_nic_is_the_uplink(self, sysnet):
        _nic(sysnet.root, "enp5s0")
        _nic(sysnet.root, "enp9s0")
        sysnet.addressed.append("enp5s0")

        assert _network().discover_uplink_interface(required=True) == ("enp9s0", None)

    def test_several_unaddressed_nics_block(self, sysnet):
        _nic(sysnet.root, "enp8s0")
        _nic(sysnet.root, "enp9s0")

        interface, problem = _network().discover_uplink_interface(required=False)

        assert interface == ""
        assert problem == (
            "Cannot choose the OVN uplink on node1: several unaddressed interfaces "
            "are up: ['enp8s0', 'enp9s0']; set ovn-uplink-interface"
        )

    def test_none_omits_the_uplink_when_not_required(self, sysnet):
        _nic(sysnet.root, "enp5s0")
        sysnet.addressed.append("enp5s0")

        assert _network().discover_uplink_interface(required=False) == ("", None)

    def test_none_blocks_when_a_gateway_is_configured(self, sysnet):
        interface, problem = _network().discover_uplink_interface(required=True)

        assert interface == ""
        assert problem is not None
        assert "No OVN uplink on node1" in problem

    @pytest.mark.parametrize(
        "kwargs",
        [
            pytest.param({"flags": DOWN_BROADCAST}, id="down"),
            pytest.param({"flags": "0x1001"}, id="not-broadcast"),
            pytest.param({"master": True}, id="bond-member-or-bridge-port"),
            pytest.param({"upper": "enp9s0.100"}, id="vlan-parent"),
            pytest.param({"devtype": "bridge", "physical": False}, id="bridge"),
            pytest.param({"physical": False}, id="virtual"),
            pytest.param({"devtype": "wlan"}, id="wlan"),
        ],
    )
    def test_interfaces_that_cannot_be_the_uplink_are_skipped(self, sysnet, kwargs):
        _nic(sysnet.root, "enp9s0", **kwargs)

        assert _network().discover_uplink_interface(required=False) == ("", None)

    @pytest.mark.parametrize("devtype", ["bond", "vlan"])
    def test_bonds_and_vlans_can_be_the_uplink(self, sysnet, devtype):
        _nic(sysnet.root, "uplink0", devtype=devtype, physical=False)

        assert _network().discover_uplink_interface(required=True) == ("uplink0", None)

    def test_a_link_local_address_does_not_count(self, sysnet):
        """The kernel gives every IPv6-enabled NIC one, the uplink included."""
        _nic(sysnet.root, "enp9s0")

        with patch(
            "network.subprocess.run",
            return_value=SimpleNamespace(
                stdout=json.dumps(
                    [{"ifname": "enp9s0", "addr_info": [{"local": "fe80::1", "scope": "link"}]}]
                )
            ),
        ):
            assert _network().discover_uplink_interface(required=True) == ("enp9s0", None)

    def test_ip_failing_blocks(self, sysnet):
        with patch("network.subprocess.run", side_effect=OSError("no ip")):
            interface, problem = _network().discover_uplink_interface(required=False)

        assert interface == ""
        assert problem == "Cannot list network interfaces: no ip"

    def test_unreadable_sysfs_blocks(self, sysnet):
        with patch("network.SYS_CLASS_NET", sysnet.root / "missing"):
            interface, problem = _network().discover_uplink_interface(required=False)

        assert interface == ""
        assert problem is not None
        assert problem.startswith("Cannot list network interfaces: ")


class TestConfiguredUplinkInterface:
    def test_unset_leaves_it_to_discovery(self, sysnet):
        assert _network(config={}).configured_uplink_interface() == ("", None)

    def test_a_name_applies_to_every_unit(self, sysnet):
        _nic(sysnet.root, "eth4")

        network = _network(config={"ovn-uplink-interface": "eth4"})

        assert network.configured_uplink_interface() == ("eth4", None)

    def test_a_mapping_resolves_this_hostname(self, sysnet):
        _nic(sysnet.root, "enp9s0")

        network = _network(config={"ovn-uplink-interface": "{node1: enp9s0, node2: eno1}"})

        assert network.configured_uplink_interface() == ("enp9s0", None)

    def test_a_mapping_without_this_hostname_blocks(self, sysnet):
        network = _network(config={"ovn-uplink-interface": "{node2: eno1}"})

        assert network.configured_uplink_interface() == (
            "",
            "ovn-uplink-interface is missing an entry for hostname 'node1'; "
            "known entries: ['node2']",
        )

    def test_invalid_yaml_blocks(self, sysnet):
        network = _network(config={"ovn-uplink-interface": "{node1: ["})

        interface, problem = network.configured_uplink_interface()

        assert interface == ""
        assert problem.startswith("Cannot parse ovn-uplink-interface:")

    def test_an_interface_that_does_not_exist_blocks(self, sysnet):
        _nic(sysnet.root, "eth0")

        network = _network(config={"ovn-uplink-interface": "eth4"})

        assert network.configured_uplink_interface() == (
            "",
            "ovn-uplink-interface 'eth4' does not exist on node1; interfaces: ['eth0']",
        )


class TestUplinkSpaceUnaddressed:
    def _network(self, binding):
        model = MagicMock()
        model.get_binding.return_value = binding
        return UnitNetwork(model, {})

    def test_unbound_falls_back_to_an_addressed_space(self):
        binding = MagicMock()
        binding.network.bind_address = ipaddress.ip_address("10.200.2.10")

        assert not self._network(binding).uplink_space_unaddressed()

    def test_bound_to_the_uplink_space_has_no_address(self):
        """Juju fails "network-get" with "no network config found for binding"."""
        binding = MagicMock()
        type(binding).network = PropertyMock(side_effect=ops.ModelError("no network config"))

        assert self._network(binding).uplink_space_unaddressed()
