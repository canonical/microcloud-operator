# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import ipaddress
import subprocess
from unittest.mock import patch

import pytest

import bgp

NETWORKS = [
    ipaddress.IPv6Network("fd2b:5d1a:7c93:7::/64"),
    ipaddress.IPv6Network("fd2b:5d1a:7c93:6::/64"),
]


def test_uplinks_parses_a_comma_separated_list():
    assert bgp.uplinks(" eth1, eth2 ,,") == ["eth1", "eth2"]
    assert bgp.uplinks("") == []


def test_identity_comes_from_the_low_32_bits():
    router_id, asn = bgp.identity(ipaddress.IPv6Address("fd2b:5d1a:7c93:6:0:1:0:7"))
    assert router_id == "0.0.0.7"
    assert asn == 4200000007


def test_identity_differs_between_machines_in_one_network():
    a = bgp.identity(ipaddress.IPv6Address("fd2b:5d1a:7c93:6:0:1:0:7"))
    b = bgp.identity(ipaddress.IPv6Address("fd2b:5d1a:7c93:6:0:1:0:8"))
    assert a[0] != b[0] and a[1] != b[1]


def test_identity_stays_in_the_private_asn_range():
    _, asn = bgp.identity(ipaddress.IPv6Address("fd00::ffff:ffff"))
    assert 4200000000 <= asn <= 4294967294


def test_identity_rejects_zero_host_bits():
    with pytest.raises(ValueError):
        bgp.identity(ipaddress.IPv6Address("fd2b:5d1a:7c93:6:0:1::"))


def test_render_peers_on_each_uplink_and_advertises_only_the_derived_addresses():
    conf = bgp.render("0.0.0.7", 4200000007, ["eth1", "eth2"], NETWORKS)
    prefixes = "fd2b:5d1a:7c93:6::/64{128,128}, fd2b:5d1a:7c93:7::/64{128,128}"

    assert "router id 0.0.0.7;" in conf
    assert conf.count("local as 4200000007;") == 2
    for name in ("eth1", "eth2"):
        assert f"protocol bgp uplink_{name} {{" in conf
        # BGP, router discovery and BFD each name the interface.
        assert conf.count(f'interface "{name}"') == 3
    assert f"if source = RTS_DEVICE && net ~ [ {prefixes} ] then accept;" in conf
    assert f"import filter {{ if net ~ [ {prefixes} ] then accept; reject; }};" in conf
    # No default route is taken from the fabric.
    assert "::/0" not in conf


class _Snap:
    """A fake "snap" command line for the MicroOVN snap's services."""

    def __init__(self, services):
        self.services = dict(services)
        self.calls = []

    def run(self, command, **kwargs):
        command = list(command)
        self.calls.append(command)
        if command[:2] == ["snap", "services"]:
            lines = ["Service Startup Current Notes"] + [
                f"{name} {'enabled' if on else 'disabled'} {'active' if up else 'inactive'} -"
                for name, (on, up) in self.services.items()
            ]
            return subprocess.CompletedProcess(command, 0, "\n".join(lines) + "\n", "")
        if command[:3] == ["snap", "stop", "--disable"]:
            self.services[command[3]] = (False, False)
        if command[:3] == ["snap", "start", "--enable"]:
            self.services[command[3]] = (True, True)
        return subprocess.CompletedProcess(command, 0, "", "")


def test_ensure_runs_bird_alone_and_writes_the_config(tmp_path):
    fake = _Snap(
        {
            "microovn.bird": (True, False),
            "microovn.daemon": (True, True),
            "microovn.chassis": (True, True),
            "microovn.refresh-expiring-certs": (True, False),
        }
    )
    conf_path = tmp_path / "bird" / "bird.conf"
    dropin = tmp_path / "networkd" / "90-microcloud-bgp.conf"
    with (
        patch.object(bgp, "BIRD_CONF", conf_path),
        patch.object(bgp, "NETWORKD_DROPIN", dropin),
        patch("subprocess.run", side_effect=fake.run),
    ):
        assert bgp.ensure("conf v1\n") is None

    assert conf_path.read_text() == "conf v1\n"
    # networkd leaves BIRD's routes alone, and is restarted to read that
    # before BIRD starts.
    assert "ManageForeignRoutes=no" in dropin.read_text()
    assert "ManageForeignRoutingPolicyRules=no" in dropin.read_text()
    restart = fake.calls.index(["systemctl", "restart", "systemd-networkd"])
    start = next(i for i, c in enumerate(fake.calls) if c[:3] == ["snap", "start", "--enable"])
    assert restart < start
    assert fake.services["microovn.bird"] == (True, True)
    for name in ("microovn.daemon", "microovn.chassis", "microovn.refresh-expiring-certs"):
        assert fake.services[name] == (False, False)
    # The daemon is off before BIRD starts.
    stops = [i for i, c in enumerate(fake.calls) if c[:3] == ["snap", "stop", "--disable"]]
    start = next(i for i, c in enumerate(fake.calls) if c[:3] == ["snap", "start", "--enable"])
    assert stops and max(stops) < start


def test_ensure_reconfigures_a_running_bird_only_when_the_config_changes(tmp_path):
    fake = _Snap({"microovn.bird": (True, True), "microovn.daemon": (False, False)})
    conf_path = tmp_path / "bird.conf"
    conf_path.write_text("conf v1\n")
    with (
        patch.object(bgp, "BIRD_CONF", conf_path),
        patch.object(bgp, "NETWORKD_DROPIN", tmp_path / "90.conf"),
        patch("subprocess.run", side_effect=fake.run),
    ):
        assert bgp.ensure("conf v1\n") is None
        assert ["microovn.birdc", "configure"] not in fake.calls
        assert bgp.ensure("conf v2\n") is None
        assert ["microovn.birdc", "configure"] in fake.calls
    # The drop-in is written once, so networkd is restarted once.
    assert fake.calls.count(["systemctl", "restart", "systemd-networkd"]) == 1


def test_ensure_reports_a_failed_command(tmp_path):
    def fail(command, **kwargs):
        raise subprocess.CalledProcessError(1, command, "", 'snap "microovn" not found')

    with (
        patch.object(bgp, "BIRD_CONF", tmp_path / "bird.conf"),
        patch.object(bgp, "NETWORKD_DROPIN", tmp_path / "90.conf"),
        patch("subprocess.run", side_effect=fail),
    ):
        problem = bgp.ensure("conf\n")
    assert problem is not None and problem.startswith("Cannot run BGP:")
