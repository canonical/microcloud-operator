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
    path = tmp_path / "netplan" / "90-microcloud-lo.yaml"
    path.parent.mkdir()
    with (
        patch.object(loopback, "NETPLAN_FILE", path),
        patch.object(loopback, "NETWORKD_DROPIN", tmp_path / "networkd" / "90.conf"),
    ):
        yield path


@pytest.fixture
def ip(netplan):
    """Fake ``ip`` and ``netplan``: records every call.

    The listing comes from ``ip.links``. ``netplan apply`` puts the addresses
    of the netplan file on "lo", unless ``ip.apply_works`` is False.
    """
    calls = []

    def run(command, **kwargs):
        calls.append(list(command))
        if list(command) == ["netplan", "apply"] and run.apply_works:
            run.pending = [
                line.split()[1].split("/")[0]
                for line in netplan.read_text().splitlines()
                if "/128" in line
            ]
            run.countdown = run.delay
        if "show" in command and run.pending is not None:
            # networkd adds the addresses some listings after netplan returns.
            if run.countdown == 0:
                run.links = _links(eth0=[OAM_ADDRESS], lo=run.pending)
                run.pending = None
            else:
                run.countdown -= 1
        output = run.links if "show" in command else ""
        return subprocess.CompletedProcess(command, 0, stdout=output, stderr="")

    run.links = _links(eth0=[OAM_ADDRESS])
    run.apply_works = True
    run.delay = 0
    run.pending = None
    run.countdown = 0
    run.calls = calls
    with patch("loopback.subprocess.run", side_effect=run):
        yield run


def _applied(ip):
    return ip.calls.count(["netplan", "apply"])


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

    def test_writes_netplan_and_applies_it(self, netplan, ip):
        problem = loopback.ensure({CEPH_PUBLIC: PUBLIC_ADDRESS, OVN_UNDERLAY: UNDERLAY_ADDRESS})

        assert problem is None
        assert netplan.read_text() == loopback.render([UNDERLAY_ADDRESS, PUBLIC_ADDRESS])
        assert stat.S_IMODE(netplan.stat().st_mode) == 0o600
        assert _applied(ip) == 1
        # The addresses come from netplan only.
        assert not any("replace" in call for call in ip.calls)

    def test_networkd_keeps_foreign_routes_before_netplan_is_applied(self, netplan, ip):
        loopback.ensure({CEPH_PUBLIC: PUBLIC_ADDRESS})

        dropin = loopback.NETWORKD_DROPIN.read_text()
        assert "ManageForeignRoutes=no" in dropin
        assert "ManageForeignRoutingPolicyRules=no" in dropin
        assert stat.S_IMODE(loopback.NETWORKD_DROPIN.stat().st_mode) == 0o644
        restart = ip.calls.index(["systemctl", "restart", "systemd-networkd"])
        assert restart < ip.calls.index(["netplan", "apply"])

    def test_networkd_is_restarted_only_when_the_dropin_is_new(self, netplan, ip):
        loopback.ensure({CEPH_PUBLIC: PUBLIC_ADDRESS})
        loopback.ensure({CEPH_PUBLIC: PUBLIC_ADDRESS, OVN_UNDERLAY: UNDERLAY_ADDRESS})

        assert ip.calls.count(["systemctl", "restart", "systemd-networkd"]) == 1

    def test_netplan_is_not_applied_when_current(self, netplan, ip):
        loopback.ensure({CEPH_PUBLIC: PUBLIC_ADDRESS})
        before = netplan.stat().st_mtime_ns

        with patch("loopback.os.replace") as replace:
            assert loopback.ensure({CEPH_PUBLIC: PUBLIC_ADDRESS}) is None

        replace.assert_not_called()
        assert netplan.stat().st_mtime_ns == before
        assert _applied(ip) == 1

    def test_netplan_is_applied_again_when_an_address_went_missing(self, netplan, ip):
        loopback.ensure({CEPH_PUBLIC: PUBLIC_ADDRESS})
        ip.links = _links(eth0=[OAM_ADDRESS])

        assert loopback.ensure({CEPH_PUBLIC: PUBLIC_ADDRESS}) is None
        assert _applied(ip) == 2

    def test_addresses_that_arrive_after_netplan_apply_returns(self, netplan, ip):
        ip.delay = 3

        with patch("loopback.time.sleep") as sleep:
            assert loopback.ensure({CEPH_PUBLIC: PUBLIC_ADDRESS}) is None

        assert sleep.call_count == 3

    def test_an_address_netplan_did_not_add_is_reported(self, netplan, ip):
        ip.apply_works = False

        with (
            patch.object(loopback, "APPLY_WAIT", 0),
            patch("loopback.time.sleep"),
        ):
            problem = loopback.ensure({CEPH_PUBLIC: PUBLIC_ADDRESS})

        assert problem == "netplan apply left fd2b:5d1a:7c93:3:0:1:0:5 off lo"

    def test_another_address_in_the_network_blocks(self, netplan, ip):
        ip.links = _links(eth0=[OAM_ADDRESS], lo=["fd2b:5d1a:7c93:3::7"])

        problem = loopback.ensure({CEPH_PUBLIC: PUBLIC_ADDRESS})

        assert problem == (
            "fd2b:5d1a:7c93:3::7 on lo is in fd2b:5d1a:7c93:3::/64, "
            "which fd2b:5d1a:7c93:3:0:1:0:5 on lo is derived for"
        )
        assert _applied(ip) == 0
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
