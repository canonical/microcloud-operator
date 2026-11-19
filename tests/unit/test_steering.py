# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for binding Ceph OSDs to their NUMA node's addresses."""

import ipaddress
import json
import subprocess
from unittest.mock import patch

import pytest

import steering
from steering import Addresses, Binding, Change, SteeringError

PUBLIC_MAP = '{"0": "fd00:0:0:401::/64", "1": "fd00:0:0:402::/64"}'
INTERNAL_MAP = '{"0": "fd00:0:0:301::/64", "1": "fd00:0:0:302::/64"}'
CONFIG = {
    "ceph-public-numa-networks": PUBLIC_MAP,
    "ceph-internal-numa-networks": INTERNAL_MAP,
    "ceph-public-network": "fd00:0:0:400::/64",
    "ceph-internal-network": "fd00:0:0:300::/64",
}
UNSTEERED = Addresses("fd00:0:0:400::6", "fd00:0:0:300::6")


def _maps(config=None):
    maps, problem = steering.numa_networks({**CONFIG, **(config or {})})
    assert problem is None
    return maps


def _lo(*addresses):
    return [ipaddress.IPv6Address(a) for a in addresses]


class TestNumaNetworks:
    def test_parses_json_maps(self):
        assert _maps() == {
            "public": {
                0: ipaddress.IPv6Network("fd00:0:0:401::/64"),
                1: ipaddress.IPv6Network("fd00:0:0:402::/64"),
            },
            "internal": {
                0: ipaddress.IPv6Network("fd00:0:0:301::/64"),
                1: ipaddress.IPv6Network("fd00:0:0:302::/64"),
            },
        }

    def test_parses_yaml_maps(self):
        maps = _maps(
            {
                "ceph-public-numa-networks": "0: fd00:0:0:401::/64\n1: fd00:0:0:402::/64\n",
                "ceph-internal-numa-networks": "{0: 'fd00:0:0:301::/64', 1: 'fd00:0:0:302::/64'}",
            }
        )
        assert sorted(maps["public"]) == sorted(maps["internal"]) == [0, 1]

    def test_both_empty_turns_steering_off(self):
        config = {"ceph-public-numa-networks": "", "ceph-internal-numa-networks": " "}
        assert steering.numa_networks({**CONFIG, **config}) == ({}, None)

    @pytest.mark.parametrize(
        ("value", "problem"),
        [
            ("{", "Cannot parse ceph-public-numa-networks"),
            ('["fd00:0:0:401::/64"]', "must map each NUMA node to an IPv6 /64"),
            ('{"a": "fd00:0:0:401::/64"}', "NUMA node 'a' is not a non-negative integer"),
            ('{"-1": "fd00:0:0:401::/64"}', "NUMA node '-1' is not a non-negative integer"),
            ("{true: 'fd00:0:0:401::/64'}", "NUMA node True is not a non-negative integer"),
            ('{"0": "fd00:0:0:401::/56", "1": "fd00:0:0:402::/64"}', "is not an IPv6 /64"),
            ('{"0": "10.0.0.0/24", "1": "fd00:0:0:402::/64"}', "is not an IPv6 /64"),
            ('{"0": "fd00:0:0:401::1/64", "1": "fd00:0:0:402::/64"}', "is not an IPv6 /64"),
        ],
    )
    def test_rejects_a_bad_map(self, value, problem):
        maps, found = steering.numa_networks({**CONFIG, "ceph-public-numa-networks": value})
        assert maps == {}
        assert problem in found

    def test_needs_both_maps(self):
        maps, problem = steering.numa_networks({**CONFIG, "ceph-internal-numa-networks": ""})
        assert maps == {}
        assert problem == "NUMA steering needs ceph-internal-numa-networks too"

    def test_needs_the_same_nodes_in_both(self):
        maps, problem = steering.numa_networks(
            {**CONFIG, "ceph-internal-numa-networks": '{"0": "fd00:0:0:301::/64"}'}
        )
        assert maps == {}
        assert problem == (
            "ceph-public-numa-networks and ceph-internal-numa-networks must name the same NUMA nodes"
        )

    def test_a_map_entry_may_not_overlap_another(self):
        _, problem = steering.numa_networks(
            {
                **CONFIG,
                "ceph-internal-numa-networks": '{"0": "fd00:0:0:401::/64", "1": "fd00:0:0:302::/64"}',
            }
        )
        assert problem == (
            "ceph-public-numa-networks node 0 (fd00:0:0:401::/64) overlaps "
            "ceph-internal-numa-networks node 0 (fd00:0:0:401::/64)"
        )

    def test_a_map_entry_may_not_overlap_an_unsteered_network(self):
        """A Ceph /56 pool as the unsteered network would hold the NUMA /64s."""
        _, problem = steering.numa_networks({**CONFIG, "ceph-public-network": "fd00:0:0:400::/56"})
        assert problem == (
            "ceph-public-numa-networks node 0 (fd00:0:0:401::/64) overlaps "
            "ceph-public-network (fd00:0:0:400::/56)"
        )

    def test_the_unsteered_networks_may_be_the_same(self):
        _maps({"ceph-internal-network": "fd00:0:0:400::/64"})


class TestSteeredAddresses:
    def test_finds_each_nodes_address_on_lo(self):
        steered, problem = steering.steered_addresses(
            _maps(),
            _lo("fd00:0:0:400::6", "fd00:0:0:401::6", "fd00:0:0:301::6")
            + _lo("fd00:0:0:402::6", "fd00:0:0:302::6"),
        )
        assert problem is None
        assert steered == {
            0: Addresses("fd00:0:0:401::6", "fd00:0:0:301::6"),
            1: Addresses("fd00:0:0:402::6", "fd00:0:0:302::6"),
        }

    def test_a_node_with_no_address_is_unsteered(self):
        steered, problem = steering.steered_addresses(
            _maps(), _lo("fd00:0:0:401::6", "fd00:0:0:301::6")
        )
        assert problem is None
        assert list(steered) == [0]

    def test_a_host_with_no_steered_address(self):
        assert steering.steered_addresses(_maps(), _lo("fd00:0:0:400::6")) == ({}, None)

    def test_blocks_on_two_addresses_in_one_subnet(self):
        steered, problem = steering.steered_addresses(
            _maps(), _lo("fd00:0:0:401::6", "fd00:0:0:401::7", "fd00:0:0:301::6")
        )
        assert steered == {}
        assert problem == "2 addresses on lo in fd00:0:0:401::/64"

    def test_blocks_on_one_network_without_the_other(self):
        steered, problem = steering.steered_addresses(_maps(), _lo("fd00:0:0:302::6"))
        assert steered == {}
        assert problem == (
            "NUMA node 1 has fd00:0:0:302::6 on lo but no address in fd00:0:0:402::/64"
        )


class TestNumaNode:
    def _sysfs(self, tmp_path, numa):
        """Lay out an NVMe namespace under its PCI function, as sysfs does."""
        sys_root = tmp_path / "sys"
        pci = sys_root / "devices" / "pci0000:00" / "0000:00:02.0"
        block = pci / "nvme" / "nvme0" / "nvme0n1"
        block.mkdir(parents=True)
        if numa is not None:
            (pci / "numa_node").write_text(f"{numa}\n")
        (sys_root / "class" / "block").mkdir(parents=True)
        (sys_root / "class" / "block" / "nvme0n1").symlink_to(block)

        dev = tmp_path / "dev"
        (dev / "disk" / "by-id").mkdir(parents=True)
        (dev / "nvme0n1").touch()
        (dev / "disk" / "by-id" / "nvme-disk").symlink_to(dev / "nvme0n1")
        return str(sys_root), str(dev / "disk" / "by-id" / "nvme-disk")

    @pytest.mark.parametrize("numa", [0, 1, -1])
    def test_reads_the_nearest_numa_node(self, tmp_path, numa):
        sys_root, device = self._sysfs(tmp_path, numa)
        assert steering.numa_node(device, sys_root) == numa

    def test_unknown_without_a_numa_node(self, tmp_path):
        sys_root, device = self._sysfs(tmp_path, None)
        assert steering.numa_node(device, sys_root) == -1

    def test_unknown_for_a_device_not_in_sysfs(self, tmp_path):
        assert steering.numa_node("/dev/nonexistent", str(tmp_path)) == -1


STEERED = {
    0: Addresses("fd00:0:0:401::6", "fd00:0:0:301::6"),
    1: Addresses("fd00:0:0:402::6", "fd00:0:0:302::6"),
}


class TestBindings:
    def _bindings(self, steered, nodes):
        disks = {osd: f"/dev/{osd}" for osd in nodes}
        return steering.bindings(steered, UNSTEERED, disks, lambda d: nodes[int(d[5:])])

    def test_steers_each_osd_to_its_disk_numa_node(self):
        assert self._bindings(STEERED, {1: 0, 2: 1}) == [
            Binding(1, "fd00:0:0:401::6", "fd00:0:0:301::6", 0),
            Binding(2, "fd00:0:0:402::6", "fd00:0:0:302::6", 1),
        ]

    def test_no_numa_information_counts_as_node_0(self):
        """As the machine's deployment does for its network cards."""
        assert self._bindings(STEERED, {3: -1}) == [
            Binding(3, "fd00:0:0:401::6", "fd00:0:0:301::6", 0)
        ]

    @pytest.mark.parametrize("node", [1, 2])
    def test_a_node_without_steered_addresses_is_unsteered(self, node):
        assert self._bindings({0: STEERED[0]}, {4: node}) == [
            Binding(4, "fd00:0:0:400::6", "fd00:0:0:300::6")
        ]


class TestChanges:
    BOUND = {1: True}

    def test_options_of_a_steered_osd(self):
        wanted = [Binding(1, "fd00:0:0:401::6", "fd00:0:0:301::6", 0)]
        assert steering.changes(wanted, {}, self.BOUND) == {
            1: Change(
                {
                    "public_addr": "fd00:0:0:401::6",
                    "cluster_addr": "fd00:0:0:301::6",
                    "osd_numa_node": "0",
                }
            )
        }

    def test_an_unsteered_osd_loses_its_pin(self):
        wanted = [Binding(1, "fd00:0:0:400::6", "fd00:0:0:300::6")]
        current = {
            1: {
                "public_addr": "fd00:0:0:400::6",
                "cluster_addr": "fd00:0:0:300::6",
                "osd_numa_node": "1",
            }
        }
        assert steering.changes(wanted, current, self.BOUND) == {1: Change({}, ("osd_numa_node",))}

    def test_a_bound_osd_is_left_alone(self):
        wanted = [Binding(1, "fd00:0:0:401::6", "fd00:0:0:301::6", 0)]
        assert steering.changes(wanted, {1: wanted[0].options()}, self.BOUND) == {}

    def test_an_osd_not_running_with_bind_before_connect_is_restarted(self):
        wanted = [Binding(1, "fd00:0:0:401::6", "fd00:0:0:301::6", 0)]
        assert steering.changes(wanted, {1: wanted[0].options()}, {1: False}) == {1: Change()}

    @pytest.mark.parametrize(
        "have",
        [
            "v2:[fd00:0:0:400:0:1:0:8]:0/0",
            "fd00:0:0:400:0:1:0:8",
            "[fd00:0:0:400:0:1:0:8]:6800",
        ],
    )
    def test_an_address_matches_as_ceph_stores_it(self, have):
        wanted = [Binding(1, "fd00:0:0:400:0:1:0:8", "fd00:300::6")]
        current = {1: {"public_addr": have, "cluster_addr": "fd00:300::6"}}
        assert steering.changes(wanted, current, self.BOUND) == {}

    def test_a_different_address_still_differs(self):
        wanted = [Binding(1, "fd00:400::7", "fd00:300::7")]
        current = {
            1: {"public_addr": "v2:[fd00:400::6]:0/0", "cluster_addr": "v2:[fd00:300::7]:0/0"}
        }
        assert steering.changes(wanted, current, self.BOUND) == {
            1: Change({"public_addr": "fd00:400::7"})
        }


class TestOutsidePublicNetwork:
    WANTED = [
        Binding(1, "fd00:0:0:401::6", "fd00:0:0:301::6", 0),
        Binding(2, "fd00:0:0:400::6", "fd00:0:0:300::6"),
    ]

    def test_the_unsteered_network_alone_leaves_the_steered_addresses_out(self):
        networks = [ipaddress.IPv6Network("fd00:0:0:400::/64")]
        assert steering.outside_public_network(self.WANTED, networks) == ["fd00:0:0:401::6"]

    @pytest.mark.parametrize(
        "networks",
        [
            ["fd00:0:0:400::/56"],
            ["fd00:0:0:400::/64", "fd00:0:0:401::/64"],
            [],
        ],
    )
    def test_covered_or_unchecked(self, networks):
        networks = [ipaddress.IPv6Network(n) for n in networks]
        assert steering.outside_public_network(self.WANTED, networks) == []

    def test_inside_keeps_the_cluster_address_and_numa_node(self):
        networks = [ipaddress.IPv6Network("fd00:0:0:400::/64")]
        wanted, outside = steering.inside_public_network(self.WANTED, networks, "fd00:0:0:400::6")
        assert outside == ["fd00:0:0:401::6"]
        assert wanted == [
            Binding(1, "fd00:0:0:400::6", "fd00:0:0:301::6", 0),
            Binding(2, "fd00:0:0:400::6", "fd00:0:0:300::6"),
        ]

    def test_inside_changes_nothing_when_covered(self):
        networks = [ipaddress.IPv6Network("fd00:0:0:400::/56")]
        wanted, outside = steering.inside_public_network(self.WANTED, networks, "fd00:0:0:400::6")
        assert (wanted, outside) == (self.WANTED, [])


class TestDaemonPins:
    PUBLIC = "fd00:0:0:400::6"

    def test_a_daemon_on_a_steered_address_is_pinned_and_restarted(self):
        running = {"mgr": "-", "mds": "v2:[fd00:0:0:402::6]:0/0"}
        assert steering.daemon_pins("node1", self.PUBLIC, {}, running) == (
            ["mgr", "mds"],
            ["mgr", "mds"],
        )

    def test_a_pinned_daemon_that_has_not_restarted_is_only_restarted(self):
        dump = {"mgr.node1": {"public_addr": "v2:[fd00:0:0:400::6]:0/0"}}
        assert steering.daemon_pins("node1", self.PUBLIC, dump, {"mgr": "-"}) == ([], ["mgr"])

    def test_daemons_that_do_not_run_here_are_left_alone(self):
        assert steering.daemon_pins("node1", self.PUBLIC, {}, {"mgr": None}) == ([], [])

    def test_running_public_addr(self):
        done = _completed("v2:[fd00:0:0:400::6]:0/0\n")
        with patch("steering.subprocess.run", return_value=done) as run:
            assert steering.running_public_addr("mgr.node1") == "v2:[fd00:0:0:400::6]:0/0"
        assert list(run.call_args.args[0]) == [
            "microceph.ceph",
            "config",
            "show",
            "mgr.node1",
            "public_addr",
        ]

    def test_running_public_addr_of_a_daemon_that_is_not_running(self):
        done = subprocess.CompletedProcess([], 2, "", "no such daemon")
        with patch("steering.subprocess.run", return_value=done):
            assert steering.running_public_addr("mds.node1") is None


class TestPublicNetworks:
    def test_reads_a_list(self):
        done = _completed("fd00:0:0:400::/64, fd00:0:0:401::/64\n")
        with patch("steering.subprocess.run", return_value=done) as run:
            assert steering.public_networks() == [
                ipaddress.IPv6Network("fd00:0:0:400::/64"),
                ipaddress.IPv6Network("fd00:0:0:401::/64"),
            ]
        assert list(run.call_args.args[0]) == [
            "microceph.ceph",
            "config",
            "get",
            "osd",
            "public_network",
        ]

    def test_unset(self):
        with patch("steering.subprocess.run", return_value=_completed("\n")):
            assert steering.public_networks() == []

    def test_ipv4_is_an_error(self):
        with patch("steering.subprocess.run", return_value=_completed("10.0.0.0/24\n")):
            with pytest.raises(SteeringError, match="is not IPv6"):
                steering.public_networks()


def _completed(stdout="", returncode=0):
    return subprocess.CompletedProcess([], returncode, stdout=stdout, stderr="")


class TestCeph:
    def test_local_disks(self):
        listed = {
            "ConfiguredDisks": [
                {"osd": 1, "path": "/dev/disk/by-id/a", "location": "node1"},
                {"osd": 2, "path": "/dev/disk/by-id/b", "location": "node2"},
                {"osd": 3, "path": "/dev/disk/by-id/c", "location": "node1"},
            ]
        }
        with patch("steering.subprocess.run", return_value=_completed(json.dumps(listed))):
            assert steering.local_disks("node1") == {
                1: "/dev/disk/by-id/a",
                3: "/dev/disk/by-id/c",
            }

    def test_config_dump_by_section(self):
        dump = [
            {"section": "global", "name": "public_network", "value": "fd00:400::/64"},
            {"section": "osd", "name": "ms_bind_before_connect", "value": "true"},
            {"section": "osd.1", "name": "public_addr", "value": "fd00:401::6"},
            {"section": "osd.2", "name": "public_addr", "value": "fd00:402::6"},
        ]
        with patch("steering.subprocess.run", return_value=_completed(json.dumps(dump))):
            found = steering.config_dump()
        assert found["osd"] == {"ms_bind_before_connect": "true"}
        assert steering.osd_options(found, [1, 3]) == {1: {"public_addr": "fd00:401::6"}, 3: {}}

    @pytest.mark.parametrize(
        ("dump", "sets"), [({}, True), ({"osd": {"ms_bind_before_connect": "true"}}, False)]
    )
    def test_ensure_bind_before_connect(self, dump, sets):
        with patch("steering.subprocess.run", return_value=_completed()) as run:
            steering.ensure_bind_before_connect(dump)
        if sets:
            assert list(run.call_args.args[0]) == [
                "microceph.ceph",
                "config",
                "set",
                "osd",
                "ms_bind_before_connect",
                "true",
            ]
        else:
            run.assert_not_called()

    @pytest.mark.parametrize(
        ("done", "running"),
        [(_completed("true\n"), True), (_completed("false\n"), False), (_completed("", 2), False)],
    )
    def test_runs_bind_before_connect(self, done, running):
        with patch("steering.subprocess.run", return_value=done) as run:
            assert steering.runs_bind_before_connect(3) is running
        assert run.call_args.args[0] == [
            "microceph.ceph",
            "config",
            "show",
            "osd.3",
            "ms_bind_before_connect",
        ]

    def test_apply_sets_then_removes(self):
        with patch("steering.subprocess.run", return_value=_completed()) as run:
            steering.apply(4, Change({"public_addr": "fd00:401::6"}, ("osd_numa_node",)))
        assert [list(c.args[0]) for c in run.call_args_list] == [
            ["microceph.ceph", "config", "set", "osd.4", "public_addr", "fd00:401::6"],
            ["microceph.ceph", "config", "rm", "osd.4", "osd_numa_node"],
        ]

    def test_down_osds_counts_only_osds_in_the_cluster(self):
        dump = {
            "osds": [
                {"osd": 0, "up": 1, "in": 1},
                {"osd": 1, "up": 0, "in": 1},
                {"osd": 2, "up": 0, "in": 0},
            ]
        }
        with patch("steering.subprocess.run", return_value=_completed(json.dumps(dump))):
            assert steering.down_osds() == [1]

    def test_a_failing_command_raises(self):
        error = subprocess.CalledProcessError(1, ["microceph"], stderr="no daemon\n")
        with patch("steering.subprocess.run", side_effect=error):
            with pytest.raises(SteeringError, match="no daemon"):
                steering.local_disks("node1")


class TestOkToStop:
    @pytest.mark.parametrize("returncode, ok", [(0, True), (16, False)])
    def test_follows_ceph(self, returncode, ok):
        with patch("steering.subprocess.run", return_value=_completed("", returncode)) as run:
            assert steering.ok_to_stop(3) is ok
        assert run.call_args.args[0] == ["microceph.ceph", "osd", "ok-to-stop", "osd.3"]


class TestRestart:
    def test_stops_the_osd_then_reloads_the_service(self):
        with (
            patch("steering._run") as run,
            patch("steering._process_running", side_effect=[True, False]),
            patch("steering.down_osds", side_effect=[[2], []]),
            patch("steering.time.sleep"),
        ):
            steering.restart(2)

        assert [c.args for c in run.call_args_list] == [
            ("pkill", "-TERM", "-f", "^ceph-osd --cluster ceph --id 2$"),
            ("snap", "restart", "--reload", "microceph.osd"),
        ]

    def test_gives_up_when_the_osd_does_not_come_back(self):
        with (
            patch("steering._run"),
            patch("steering._process_running", return_value=False),
            patch("steering.down_osds", return_value=[2]),
            patch("steering.time.sleep"),
            patch("steering.time.monotonic", side_effect=[0, 0, 400]),
        ):
            with pytest.raises(SteeringError, match="osd.2 is not up"):
                steering.restart(2)
