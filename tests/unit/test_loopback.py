# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for keeping this unit's derived addresses on "lo"."""

import ipaddress
import json
import stat
import subprocess
from unittest.mock import patch

import pytest

import loopback

CEPH_PUBLIC = ipaddress.IPv6Network("fd2b:5d1a:7c93:3::/64")
OVN_UNDERLAY = ipaddress.IPv6Network("fd2b:5d1a:7c93:1::/64")
PUBLIC_ADDRESS = ipaddress.IPv6Address("fd2b:5d1a:7c93:3:0:1:0:5")
UNDERLAY_ADDRESS = ipaddress.IPv6Address("fd2b:5d1a:7c93:1:0:1:0:5")
OAM_ADDRESS = "fd2b:5d1a:7c93:2:0:1:0:5"


def _links(**interfaces):
    """Return ``ip -json address show`` output for the given interface addresses."""
    return json.dumps(
        [
            {
                "ifname": name,
                # As "ip" prints it: one object per address, and an empty one
                # for each address that "scope global" hides.
                "addr_info": [
                    {"family": "inet6", "local": address, "prefixlen": 128, "scope": "global"}
                    for address in addresses
                ]
                + [{}],
            }
            for name, addresses in interfaces.items()
        ]
    )


@pytest.fixture
def netplan(tmp_path):
    path = tmp_path / "90-microcloud-lo.yaml"
    with patch.object(loopback, "NETPLAN_FILE", path):
        yield path


@pytest.fixture
def ip():
    """Fake ``ip``: answers the listing from ``ip.links`` and records every call."""
    calls = []

    def run(command, **kwargs):
        calls.append(list(command))
        output = run.links if "show" in command else ""
        return subprocess.CompletedProcess(command, 0, stdout=output, stderr="")

    run.links = _links(eth0=[OAM_ADDRESS])
    run.calls = calls
    with patch("loopback.subprocess.run", side_effect=run):
        yield run


def _added(ip):
    return [call[4] for call in ip.calls if "replace" in call]


class TestRender:
    def test_lists_each_address_as_a_host_route_on_lo(self):
        assert loopback.render([UNDERLAY_ADDRESS, PUBLIC_ADDRESS]) == (
            "network:\n"
            "  version: 2\n"
            "  ethernets:\n"
            "    lo:\n"
            "      addresses:\n"
            "        - fd2b:5d1a:7c93:1:0:1:0:5/128\n"
            "        - fd2b:5d1a:7c93:3:0:1:0:5/128\n"
        )


class TestHostAddresses:
    def test_reads_global_addresses_by_interface(self, ip):
        ip.links = _links(eth0=[OAM_ADDRESS], lo=[str(PUBLIC_ADDRESS)])

        assert loopback.host_addresses() == {
            "eth0": {ipaddress.IPv6Address(OAM_ADDRESS)},
            "lo": {PUBLIC_ADDRESS},
        }

    def test_an_interface_with_only_hidden_addresses_is_left_out(self, ip):
        ip.links = json.dumps([{"ifname": "eth1", "addr_info": [{}]}])

        assert loopback.host_addresses() == {}

    def test_output_that_is_not_json_is_an_error(self, ip):
        ip.links = "not json"

        with pytest.raises(loopback.LoopbackError):
            loopback.host_addresses()


class TestEnsure:
    def test_nothing_to_do_without_addresses(self, netplan, ip):
        assert loopback.ensure({}) is None
        assert ip.calls == []
        assert not netplan.exists()

    def test_adds_the_addresses_and_writes_netplan(self, netplan, ip):
        problem = loopback.ensure({CEPH_PUBLIC: PUBLIC_ADDRESS, OVN_UNDERLAY: UNDERLAY_ADDRESS})

        assert problem is None
        assert _added(ip) == ["fd2b:5d1a:7c93:1:0:1:0:5/128", "fd2b:5d1a:7c93:3:0:1:0:5/128"]
        assert netplan.read_text() == loopback.render([UNDERLAY_ADDRESS, PUBLIC_ADDRESS])
        assert stat.S_IMODE(netplan.stat().st_mode) == 0o600

    def test_an_address_already_on_lo_is_not_added_again(self, netplan, ip):
        ip.links = _links(eth0=[OAM_ADDRESS], lo=[str(PUBLIC_ADDRESS)])

        assert loopback.ensure({CEPH_PUBLIC: PUBLIC_ADDRESS}) is None
        assert _added(ip) == []
        assert netplan.exists()

    def test_netplan_is_left_alone_when_current(self, netplan, ip):
        loopback.ensure({CEPH_PUBLIC: PUBLIC_ADDRESS})
        before = netplan.stat().st_mtime_ns

        with patch("loopback.os.replace") as replace:
            loopback.ensure({CEPH_PUBLIC: PUBLIC_ADDRESS})

        replace.assert_not_called()
        assert netplan.stat().st_mtime_ns == before

    def test_another_address_in_the_network_blocks(self, netplan, ip):
        ip.links = _links(eth0=[OAM_ADDRESS], lo=["fd2b:5d1a:7c93:3::7"])

        problem = loopback.ensure({CEPH_PUBLIC: PUBLIC_ADDRESS})

        assert problem == (
            "fd2b:5d1a:7c93:3::7 on lo is in fd2b:5d1a:7c93:3::/64, "
            "which fd2b:5d1a:7c93:3:0:1:0:5 on lo is derived for"
        )
        assert _added(ip) == []
        assert not netplan.exists()

    def test_a_failing_ip_command_is_reported(self, netplan):
        error = subprocess.CalledProcessError(1, ["ip"], stderr="RTNETLINK answers: denied\n")
        with patch("loopback.subprocess.run", side_effect=error):
            problem = loopback.ensure({CEPH_PUBLIC: PUBLIC_ADDRESS})

        assert problem == (
            "Cannot put addresses on lo: ip -json -6 address show scope global: "
            "RTNETLINK answers: denied"
        )


class TestReachable:
    def test_true_when_the_target_answers(self, ip):
        assert loopback.reachable("fd2b:5d1a:7c93:3:0:1:0:5", "fd2b:5d1a:7c93:3:0:1:0:6")
        assert ip.calls == [
            [
                "ping",
                "-6",
                "-c",
                "1",
                "-W",
                "2",
                "-I",
                "fd2b:5d1a:7c93:3:0:1:0:5",
                "fd2b:5d1a:7c93:3:0:1:0:6",
            ]
        ]

    def test_false_when_the_target_does_not_answer(self):
        error = subprocess.CalledProcessError(1, ["ping"], stderr="")
        with patch("loopback.subprocess.run", side_effect=error):
            assert not loopback.reachable("fd2b:5d1a:7c93:3:0:1:0:5", "fd2b:5d1a:7c93:3:0:1:0:6")
