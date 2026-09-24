# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for resolving this unit's network settings."""

import ipaddress
from unittest.mock import MagicMock, patch

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
# ovn_uplink_interface (config-derived, per-unit OVN uplink interface name)
# ---------------------------------------------------------------------------


@pytest.fixture
def netdir(tmp_path):
    """Stand in for /sys/class/net, so tests decide which interfaces exist."""
    (tmp_path / "lo").mkdir()
    with patch("network.Path", lambda *parts: tmp_path.joinpath(*[str(p) for p in parts[1:]])):
        yield tmp_path


class TestOvnUplinkInterface:
    def test_empty_config_omits_uplink(self):
        interface, problem = _network(config={"ovn-uplink-interface": ""}).ovn_uplink_interface()
        assert interface == ""
        assert problem is None

    def test_plain_string_applies_to_every_unit(self):
        interface, problem = _network(
            config={"ovn-uplink-interface": "eth1"}
        ).ovn_uplink_interface()
        assert interface == "eth1"
        assert problem is None

    def test_an_interface_that_exists_is_accepted(self, netdir):
        (netdir / "enp9s0").mkdir()
        assert _network().missing_uplink_interface("enp9s0") is None

    def test_an_unset_interface_is_accepted(self, netdir):
        assert _network().missing_uplink_interface("") is None

    def test_an_interface_that_does_not_exist_is_reported(self, netdir):
        """MicroCloud drops an unknown uplink and forms a cluster with no UPLINK at all."""
        (netdir / "enp5s0").mkdir()

        with patch("network.microcloud.hostname", return_value="node1"):
            problem = _network().missing_uplink_interface("eth99")

        assert problem == (
            "ovn-uplink-interface 'eth99' does not exist on node1; interfaces: ['enp5s0']"
        )

    def test_mapping_resolves_own_hostname(self):
        unit_network = _network(config={"ovn-uplink-interface": "node1: eth1\nnode2: enp5s0\n"})

        with patch("network.microcloud.hostname", return_value="node2"):
            interface, problem = unit_network.ovn_uplink_interface()
        assert interface == "enp5s0"
        assert problem is None

    def test_mapping_missing_hostname_blocks(self):
        unit_network = _network(
            config={"ovn-uplink-interface": '{"node1": "eth1", "node2": "enp5s0"}'}
        )

        with patch("network.microcloud.hostname", return_value="node3"):
            interface, problem = unit_network.ovn_uplink_interface()
        assert interface == ""
        assert problem is not None
        assert "node3" in problem
        assert "node1" in problem
        assert "node2" in problem

    def test_invalid_yaml_blocks(self):
        interface, problem = _network(
            config={"ovn-uplink-interface": "{unbalanced: ["}
        ).ovn_uplink_interface()
        assert interface == ""
        assert problem is not None
