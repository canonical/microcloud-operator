# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for resolving this unit's network settings."""

import ipaddress
from unittest.mock import MagicMock, patch

import ops
import pytest

from network import UnitNetwork, derive_address


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


# ---------------------------------------------------------------------------
# Network options (spaces with no interface: addresses derived, on "lo")
# ---------------------------------------------------------------------------

CEPH_PUBLIC = "fd2b:5d1a:7c93:3::/64"
OVN_UNDERLAY = "fd2b:5d1a:7c93:1::/64"
OAM_ADDRESS = "fd2b:5d1a:7c93:2:0:1:0:5"


def _bound(address, subnet="fd2b:5d1a:7c93:2::/64"):
    """Return a binding whose network reports ``address`` in ``subnet``."""
    binding = MagicMock()
    binding.network.bind_address = ipaddress.ip_address(address) if address else None
    interface = MagicMock()
    interface.subnet = ipaddress.ip_network(subnet)
    binding.network.interfaces = [interface]
    return binding


class TestDeriveAddress:
    def test_keeps_the_host_part_of_the_source(self):
        derived = derive_address(
            ipaddress.IPv6Network(CEPH_PUBLIC), ipaddress.IPv6Address(OAM_ADDRESS)
        )
        assert str(derived) == "fd2b:5d1a:7c93:3:0:1:0:5"

    def test_two_sources_never_derive_the_same_address(self):
        network = ipaddress.IPv6Network(CEPH_PUBLIC)
        first = derive_address(network, ipaddress.IPv6Address("fd2b:5d1a:7c93:2:0:1::"))
        second = derive_address(network, ipaddress.IPv6Address("fd2b:5d1a:7c93:2:0:1:0:1"))
        assert first != second


class TestConfiguredNetwork:
    def test_none_when_unset(self):
        assert _network().configured_network("ceph-public") is None

    def test_returns_the_configured_network(self):
        network = _network(config={"ceph-public-network": CEPH_PUBLIC})
        assert network.configured_network("ceph-public") == ipaddress.IPv6Network(CEPH_PUBLIC)

    def test_a_space_without_an_option_has_none(self):
        network = _network(config={"ceph-public-network": CEPH_PUBLIC})
        assert network.configured_network("ovn-uplink") is None

    @pytest.mark.parametrize(
        "value",
        [
            "10.0.0.0/24",
            "fd2b:5d1a:7c93::/48",
            "fd2b:5d1a:7c93:3::1/64",
            "fd2b:5d1a:7c93:3::",
            "not a network",
        ],
    )
    def test_none_when_not_an_ipv6_64(self, value):
        network = _network(config={"ceph-public-network": value})
        assert network.configured_network("ceph-public") is None


class TestNetworkOptionsProblem:
    def test_none_when_no_option_is_set(self):
        assert _network(binding=_bound("10.0.0.1")).network_options_problem() is None

    def test_none_when_the_options_and_cluster_address_are_usable(self):
        network = _network(
            config={"ceph-public-network": CEPH_PUBLIC, "ovn-underlay-network": OVN_UNDERLAY},
            binding=_bound(OAM_ADDRESS),
        )
        assert network.network_options_problem() is None

    def test_an_option_that_is_not_an_ipv6_64_is_named(self):
        network = _network(
            config={"ceph-internal-network": "10.0.0.0/24"}, binding=_bound(OAM_ADDRESS)
        )
        assert network.network_options_problem() == (
            "ceph-internal-network: '10.0.0.0/24' is not an IPv6 /64"
        )

    def test_an_ipv4_cluster_address_cannot_be_derived_from(self):
        network = _network(config={"ceph-public-network": CEPH_PUBLIC}, binding=_bound("10.0.0.1"))
        assert network.network_options_problem() == (
            "Cluster address 10.0.0.1 is not IPv6: cannot derive addresses from it"
        )

    def test_no_cluster_address_yet_is_not_a_problem(self):
        network = _network(config={"ceph-public-network": CEPH_PUBLIC}, binding=_bound(""))
        assert network.network_options_problem() is None


class TestSpaceAddress:
    def test_derived_from_the_cluster_address_when_the_option_is_set(self):
        network = _network(
            config={"ovn-underlay-network": OVN_UNDERLAY}, binding=_bound(OAM_ADDRESS)
        )
        assert network.space_address("ovn-underlay") == "fd2b:5d1a:7c93:1:0:1:0:5"

    def test_binding_address_when_the_option_is_unset(self):
        network = _network(binding=_bound("10.0.0.7"))
        assert network.space_address("ovn-underlay") == "10.0.0.7"

    def test_empty_without_a_usable_cluster_address(self):
        network = _network(config={"ovn-underlay-network": OVN_UNDERLAY}, binding=_bound(""))
        assert network.space_address("ovn-underlay") == ""


class TestSpaceCidr:
    def test_the_option_when_set(self):
        network = _network(
            config={"ceph-public-network": CEPH_PUBLIC}, binding=_bound(OAM_ADDRESS)
        )
        assert network.space_cidr("ceph-public") == CEPH_PUBLIC

    def test_the_binding_subnet_when_the_option_is_unset(self):
        network = _network(binding=_bound("10.42.0.7", subnet="10.42.0.0/24"))
        assert network.space_cidr("ceph-public") == "10.42.0.0/24"
