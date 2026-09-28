# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for microcloud charm."""

import json

# Stub out the cos_agent library so we don't need the full charm SDK installed
# during unit tests — the library is tested separately.
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

_cos_agent_stub = ModuleType("charms.grafana_agent.v0.cos_agent")


class _FakeCOSAgentProvider:
    def __init__(self, charm, **kwargs):
        self._charm = charm
        self._scrape_configs_fn = kwargs.get("scrape_configs")

    def _on_refresh(self, *_):
        pass


_cos_agent_stub.COSAgentProvider = _FakeCOSAgentProvider
sys.modules.setdefault("charms", ModuleType("charms"))
sys.modules.setdefault("charms.grafana_agent", ModuleType("charms.grafana_agent"))
sys.modules.setdefault("charms.grafana_agent.v0", ModuleType("charms.grafana_agent.v0"))
sys.modules["charms.grafana_agent.v0.cos_agent"] = _cos_agent_stub

# Stub out the loki_push_api library for the same reason.
_loki_push_api_stub = ModuleType("charms.loki_k8s.v1.loki_push_api")


class _FakeLokiPushApiConsumer:
    def __init__(self, charm, **kwargs):
        self._charm = charm
        self.loki_endpoints = []


_loki_push_api_stub.LokiPushApiConsumer = _FakeLokiPushApiConsumer
sys.modules.setdefault("charms.loki_k8s", ModuleType("charms.loki_k8s"))
sys.modules.setdefault("charms.loki_k8s.v1", ModuleType("charms.loki_k8s.v1"))
sys.modules["charms.loki_k8s.v1.loki_push_api"] = _loki_push_api_stub

# Now import the modules under test.
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import session
from ceph_mgr import CephMgrPrometheus
from failure_domains import FailureDomains
from network import UnitNetwork
from observability import Observability
from ovn_exporter import OVNExporter

# ---------------------------------------------------------------------------
# logging (Loki) relation handlers
# ---------------------------------------------------------------------------


class TestLokiRelationHandlers:
    def _make_charm_stub(self):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub._observability = MagicMock(spec=Observability)
        stub._loki_consumer = MagicMock()
        return stub

    def test_joined_points_lxd_at_the_published_endpoints(self):
        from charm import MicroCloudCharm

        stub = self._make_charm_stub()
        MicroCloudCharm._on_loki_push_api_endpoint_joined(stub, MagicMock())
        stub._observability.ensure_loki.assert_called_once_with(stub._loki_consumer.loki_endpoints)

    def test_departed_stops_lxd_streaming_logs(self):
        from charm import MicroCloudCharm

        stub = self._make_charm_stub()
        MicroCloudCharm._on_loki_push_api_endpoint_departed(stub, MagicMock())
        stub._observability.teardown_loki.assert_called_once()


# ---------------------------------------------------------------------------
# ceph_mgr tests
# ---------------------------------------------------------------------------


class TestCephMgrPrometheus:
    def test_default_port(self):
        ceph = CephMgrPrometheus()
        assert ceph.port == 9283

    def test_custom_port(self):
        ceph = CephMgrPrometheus(port=9284)
        assert ceph.port == 9284

    def test_is_mgr_active_true(self):
        ceph = CephMgrPrometheus()
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = (
            "Service              Startup  Current  Notes\n"
            "microceph.daemon     enabled  active   -\n"
            "microceph.mgr        enabled  active   -\n"
            "microceph.mon        enabled  active   -\n"
        )
        with patch("subprocess.run", return_value=mock_result):
            assert ceph.is_mgr_active() is True

    def test_is_mgr_active_false_when_not_installed(self):
        ceph = CephMgrPrometheus()
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        with patch("subprocess.run", return_value=mock_result):
            assert ceph.is_mgr_active() is False

    def test_is_mgr_active_false_when_inactive(self):
        ceph = CephMgrPrometheus()
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "microceph.mgr        enabled  inactive  -\n"
        with patch("subprocess.run", return_value=mock_result):
            assert ceph.is_mgr_active() is False

    def test_ensure_enabled_skips_when_no_mgr(self):
        ceph = CephMgrPrometheus()
        with patch.object(ceph, "is_mgr_active", return_value=False):
            with patch.object(ceph, "_ceph") as mock_ceph:
                ceph.ensure_enabled()
                mock_ceph.assert_not_called()

    def test_ensure_enabled_calls_ceph_commands(self):
        ceph = CephMgrPrometheus(port=9283)
        with patch.object(ceph, "is_mgr_active", return_value=True):
            with patch.object(ceph, "_ceph") as mock_ceph:
                ceph.ensure_enabled()
                calls = [str(c) for c in mock_ceph.call_args_list]
                assert any("enable" in c for c in calls)
                assert any("server_addr" in c for c in calls)
                assert any("server_port" in c for c in calls)

    def test_ensure_enabled_skips_rbd_stats_pools_when_empty(self):
        ceph = CephMgrPrometheus(port=9283)
        with patch.object(ceph, "is_mgr_active", return_value=True):
            with patch.object(ceph, "_ceph") as mock_ceph:
                ceph.ensure_enabled()
                calls = [str(c) for c in mock_ceph.call_args_list]
                assert not any("rbd_stats_pools" in c for c in calls)

    def test_ensure_enabled_sets_rbd_stats_pools_when_configured(self):
        ceph = CephMgrPrometheus(port=9283, rbd_stats_pools="pool1,pool2")
        with patch.object(ceph, "is_mgr_active", return_value=True):
            with patch.object(ceph, "_ceph") as mock_ceph:
                ceph.ensure_enabled()
                calls = [str(c) for c in mock_ceph.call_args_list]
                assert any("rbd_stats_pools" in c and "pool1,pool2" in c for c in calls)

    def test_ensure_enabled_excludes_perf_counters_by_default(self):
        ceph = CephMgrPrometheus(port=9283)
        with patch.object(ceph, "is_mgr_active", return_value=True):
            with patch.object(ceph, "_ceph") as mock_ceph:
                ceph.ensure_enabled()
                calls = [str(c) for c in mock_ceph.call_args_list]
                assert any("exclude_perf_counters" in c and "True" in c for c in calls)

    def test_ensure_enabled_includes_perf_counters_when_enabled(self):
        ceph = CephMgrPrometheus(port=9283, enable_perf_metrics=True)
        with patch.object(ceph, "is_mgr_active", return_value=True):
            with patch.object(ceph, "_ceph") as mock_ceph:
                ceph.ensure_enabled()
                calls = [str(c) for c in mock_ceph.call_args_list]
                assert any("exclude_perf_counters" in c and "False" in c for c in calls)


# ---------------------------------------------------------------------------
# ovn_exporter tests
# ---------------------------------------------------------------------------


class TestOVNExporter:
    def test_default_channel(self):
        ovn = OVNExporter()
        assert ovn.channel == "latest/edge"

    def test_custom_channel(self):
        ovn = OVNExporter(channel="1/stable")
        assert ovn.channel == "1/stable"

    def test_is_healthy_not_installed(self):
        ovn = OVNExporter()
        with patch.object(ovn, "_is_installed", return_value=False):
            ok, reason = ovn.is_healthy()
            assert ok is False
            assert "not installed" in reason

    def test_is_healthy_missing_connection(self):
        ovn = OVNExporter()
        with (
            patch.object(ovn, "_is_installed", return_value=True),
            patch.object(
                ovn,
                "_missing_connections",
                return_value=[("ovn-exporter:ovn-chassis", "microovn:ovn-chassis")],
            ),
            patch.object(ovn, "_is_service_active", return_value=True),
        ):
            ok, reason = ovn.is_healthy()
            assert ok is False
            assert "Missing snap connections" in reason

    def test_is_healthy_service_inactive(self):
        ovn = OVNExporter()
        with (
            patch.object(ovn, "_is_installed", return_value=True),
            patch.object(ovn, "_missing_connections", return_value=[]),
            patch.object(ovn, "_is_service_active", return_value=False),
        ):
            ok, reason = ovn.is_healthy()
            assert ok is False
            assert "not active" in reason

    def test_is_healthy_all_ok(self):
        ovn = OVNExporter()
        with (
            patch.object(ovn, "_is_installed", return_value=True),
            patch.object(ovn, "_missing_connections", return_value=[]),
            patch.object(ovn, "_is_service_active", return_value=True),
        ):
            ok, reason = ovn.is_healthy()
            assert ok is True
            assert reason == ""

    def test_remove_noop_when_not_installed(self):
        ovn = OVNExporter()
        with patch.object(ovn, "_is_installed", return_value=False):
            with patch("ovn_exporter._run") as mock_run:
                ovn.remove()
                mock_run.assert_not_called()


# ---------------------------------------------------------------------------
# preseed YAML generation
# ---------------------------------------------------------------------------


class TestPreseed:
    def _inputs(self, **overrides):
        from preseed import PreseedInputs, SystemEntry

        base = {
            "initiator_address": "10.0.0.1",
            "session_passphrase": "secret",
            "systems": [
                SystemEntry(name="node1", address="10.0.0.1"),
                SystemEntry(name="node2", address="10.0.0.2"),
            ],
        }
        base.update(overrides)
        return PreseedInputs(**base)

    def test_unicast_systems_have_addresses(self):
        from preseed import build_preseed

        doc = build_preseed(self._inputs())
        assert doc["initiator_address"] == "10.0.0.1"
        assert "lookup_subnet" not in doc
        assert doc["systems"][0] == {"name": "node1", "address": "10.0.0.1"}
        assert doc["systems"][1]["address"] == "10.0.0.2"

    def test_passphrase_present(self):
        from preseed import build_preseed

        doc = build_preseed(self._inputs())
        assert doc["session_passphrase"] == "secret"

    def test_ceph_section_included(self):
        from preseed import build_preseed

        doc = build_preseed(
            self._inputs(with_ceph=True, ceph_cephfs=True, ceph_public_network="10.0.0.0/24")
        )
        assert doc["ceph"]["cephfs"] is True
        assert doc["ceph"]["public_network"] == "10.0.0.0/24"

    def test_ceph_omitted_when_disabled(self):
        from preseed import build_preseed

        doc = build_preseed(self._inputs(with_ceph=False, ceph_cephfs=True))
        assert "ceph" not in doc

    def test_ovn_uplink_section(self):
        from preseed import SystemEntry, build_preseed

        doc = build_preseed(
            self._inputs(
                systems=[
                    SystemEntry(name="node1", address="10.0.0.1", ovn_uplink_interface="eth1"),
                    SystemEntry(name="node2", address="10.0.0.2", ovn_uplink_interface="eth2"),
                ],
                with_ovn=True,
                ovn_ipv4_gateway="192.0.2.1/24",
            )
        )
        assert doc["ovn"]["ipv4_gateway"] == "192.0.2.1/24"
        assert doc["systems"][0]["ovn_uplink_interface"] == "eth1"
        assert doc["systems"][1]["ovn_uplink_interface"] == "eth2"

    def test_ovn_omitted_without_interface(self):
        from preseed import build_preseed

        doc = build_preseed(self._inputs(with_ovn=True))
        assert "ovn" not in doc
        assert "ovn_uplink_interface" not in doc["systems"][0]

    def test_storage_direct_paths(self):
        from preseed import SystemEntry, build_preseed

        doc = build_preseed(
            self._inputs(
                systems=[
                    SystemEntry(
                        name="node1",
                        address="10.0.0.1",
                        storage_local_path="/dev/nvme0n1",
                        storage_ceph_paths=["/dev/nvme1n1", "/dev/nvme2n1"],
                    ),
                    SystemEntry(name="node2", address="10.0.0.2"),
                ],
                with_ceph=True,
                storage_wipe=True,
            )
        )
        assert doc["systems"][0]["storage"]["local"] == {
            "path": "/dev/nvme0n1",
            "wipe": True,
        }
        assert doc["systems"][0]["storage"]["ceph"] == [
            {"path": "/dev/nvme1n1", "wipe": True},
            {"path": "/dev/nvme2n1", "wipe": True},
        ]
        assert "storage" not in doc["systems"][1]

    def test_storage_omitted_without_paths(self):
        from preseed import build_preseed

        doc = build_preseed(self._inputs())
        assert "storage" not in doc["systems"][0]

    def test_lookup_timeout_included_when_set(self):
        from preseed import build_preseed

        assert "lookup_timeout" not in build_preseed(self._inputs())
        assert build_preseed(self._inputs(lookup_timeout=300))["lookup_timeout"] == 300

    def test_render_produces_yaml(self):
        import yaml

        from preseed import render

        text = render(self._inputs())
        parsed = yaml.safe_load(text)
        assert parsed["initiator_address"] == "10.0.0.1"


# ---------------------------------------------------------------------------
# snap installation helpers
# ---------------------------------------------------------------------------


class TestSnap:
    def test_ensure_snaps_skips_empty_channel(self):
        import snap

        with patch("snap.install") as mock_install, patch("snap.hold") as mock_hold:
            snap.ensure_snaps({"lxd": "6/stable", "microceph": "", "microcloud": "3/stable"})
            installed = [c.args[0] for c in mock_install.call_args_list]
            assert "microceph" not in installed
            assert "lxd" in installed
            assert "microcloud" in installed
            assert mock_hold.call_count == 2

    def test_install_uses_cohort(self):
        import snap

        with patch("snap.is_installed", return_value=False):
            with patch("snap._run") as mock_run:
                snap.install("lxd", "6/stable")
                mock_run.assert_called_once_with(
                    ["snap", "install", "lxd", "--channel", "6/stable", "--cohort=+"]
                )

    def test_install_refreshes_when_present(self):
        import snap

        with patch("snap.is_installed", return_value=True):
            with patch("snap._run") as mock_run:
                snap.install("lxd", "6/stable")
                mock_run.assert_called_once_with(
                    ["snap", "refresh", "lxd", "--channel", "6/stable", "--cohort=+"]
                )


# ---------------------------------------------------------------------------
# membership validation
# ---------------------------------------------------------------------------


class TestMembershipValidation:
    def test_consistent(self):
        from cluster import validate_membership

        problems = validate_membership({"node1", "node2"}, {"node1", "node2"})
        assert problems == []

    def test_member_without_juju_unit_blocks(self):
        from cluster import validate_membership

        problems = validate_membership({"node1"}, {"node1", "node2"})
        assert any("node2" in p and "not deployed as a Juju unit" in p for p in problems)

    def test_juju_unit_not_a_member_blocks(self):
        from cluster import validate_membership

        problems = validate_membership({"node1", "node3"}, {"node1"})
        assert any("node3" in p and "not a MicroCloud member" in p for p in problems)


# ---------------------------------------------------------------------------
# ClusterCoordinator: per-unit OVN uplink interface / underlay IP
# ---------------------------------------------------------------------------


class TestClusterCoordinatorOvnFields:
    def _harness(self):
        import ops.testing

        harness = ops.testing.Harness(
            ops.testing.CharmBase,
            meta="""
name: test-charm
peers:
  cluster:
    interface: microcloud-peer
""",
        )
        harness.begin()
        return harness

    def test_all_systems_includes_ovn_fields(self):
        from cluster import ClusterCoordinator, PeerSystem

        harness = self._harness()
        try:
            coordinator = ClusterCoordinator(harness.charm)
            rel_id = harness.add_relation("cluster", "test-charm")
            harness.add_relation_unit(rel_id, "test-charm/1")

            coordinator.publish_identity(
                "node0",
                "10.0.0.1",
                ovn_uplink_interface="eth0",
                ovn_underlay_ip="10.0.1.1",
                storage_local_path="/dev/nvme0n1",
                storage_ceph_paths=["/dev/nvme1n1", "/dev/nvme2n1"],
            )
            harness.update_relation_data(
                rel_id,
                "test-charm/1",
                {
                    "microcloud-name": "node1",
                    "microcloud-address": "10.0.0.2",
                    "microcloud-ovn-uplink-interface": "eth1",
                    "microcloud-ovn-underlay-ip": "10.0.1.2",
                    "microcloud-storage-local-path": "/dev/nvme0n1",
                    "microcloud-storage-ceph-paths": '["/dev/nvme1n1"]',
                },
            )

            systems = coordinator.all_systems()
            assert (
                PeerSystem(
                    name="node0",
                    address="10.0.0.1",
                    ovn_uplink_interface="eth0",
                    ovn_underlay_ip="10.0.1.1",
                    storage_local_path="/dev/nvme0n1",
                    storage_ceph_paths=["/dev/nvme1n1", "/dev/nvme2n1"],
                )
                in systems
            )
            assert (
                PeerSystem(
                    name="node1",
                    address="10.0.0.2",
                    ovn_uplink_interface="eth1",
                    ovn_underlay_ip="10.0.1.2",
                    storage_local_path="/dev/nvme0n1",
                    storage_ceph_paths=["/dev/nvme1n1"],
                )
                in systems
            )

            members = coordinator.all_members()
            assert ("node0", "10.0.0.1") in members
            assert ("node1", "10.0.0.2") in members
        finally:
            harness.cleanup()

    def test_all_systems_defaults_empty_ovn_fields(self):
        from cluster import ClusterCoordinator, PeerSystem

        harness = self._harness()
        try:
            coordinator = ClusterCoordinator(harness.charm)
            harness.add_relation("cluster", "test-charm")

            coordinator.publish_identity("node0", "10.0.0.1")

            assert coordinator.all_systems() == [PeerSystem(name="node0", address="10.0.0.1")]
        finally:
            harness.cleanup()


# ---------------------------------------------------------------------------
# _preseed_inputs (ceph public/internal network only derived with Ceph disks)
# ---------------------------------------------------------------------------


class TestPreseedInputs:
    def _stub(self, config: dict, cidrs: dict):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub.config = config
        stub._network = MagicMock(spec=UnitNetwork)
        stub._network.space_network_cidr.side_effect = lambda name: cidrs.get(name, "")
        return stub

    def test_ceph_networks_omitted_without_ceph_storage_disks(self):
        from charm import MicroCloudCharm

        stub = self._stub(
            config={"snap-channel-microceph": "squid/stable"},
            cidrs={"ceph-public": "10.42.0.0/24", "ceph-internal": "10.42.1.0/24"},
        )

        inputs = MicroCloudCharm._preseed_inputs(stub, "10.0.0.1", "secret", [])

        assert inputs.ceph_public_network == ""
        assert inputs.ceph_internal_network == ""
        # Regardless of a resolvable binding (e.g. Juju's default-space
        # fallback for an endpoint that was never explicitly --bind-ed),
        # the network must not be looked up at all without Ceph disks -
        # "microcloud preseed" would otherwise reject the whole document.
        stub._network.space_network_cidr.assert_not_called()

    def test_ceph_networks_derived_with_ceph_storage_disks(self):
        from charm import MicroCloudCharm
        from preseed import SystemEntry

        stub = self._stub(
            config={"snap-channel-microceph": "squid/stable"},
            cidrs={"ceph-public": "10.42.0.0/24", "ceph-internal": "10.42.1.0/24"},
        )

        systems = [
            SystemEntry(name="node1", address="10.0.0.1", storage_ceph_paths=["/dev/nvme1n1"])
        ]
        inputs = MicroCloudCharm._preseed_inputs(stub, "10.0.0.1", "secret", systems)

        assert inputs.ceph_public_network == "10.42.0.0/24"
        assert inputs.ceph_internal_network == "10.42.1.0/24"


# ---------------------------------------------------------------------------
# _storage_local_path / _storage_ceph_paths (device paths from Juju storage)
# ---------------------------------------------------------------------------


class TestStoragePaths:
    def _harness(self):
        import ops.testing

        harness = ops.testing.Harness(
            ops.testing.CharmBase,
            meta="""
name: test-charm
storage:
  local:
    type: block
    multiple:
      range: 0-1
  ceph:
    type: block
    multiple:
      range: 0-
""",
            config="""
options:
  local-device:
    type: string
    default: ""
""",
        )
        harness.begin()
        return harness

    def test_local_path_empty_when_unattached(self):
        from charm import MicroCloudCharm

        harness = self._harness()
        try:
            assert MicroCloudCharm._storage_local_path(harness.charm) == ("", None)
        finally:
            harness.cleanup()

    def test_ceph_paths_empty_when_unattached(self):
        from charm import MicroCloudCharm

        harness = self._harness()
        try:
            assert MicroCloudCharm._storage_ceph_paths(harness.charm) == []
        finally:
            harness.cleanup()

    def test_local_path_returns_attached_device(self):
        from charm import MicroCloudCharm

        harness = self._harness()
        try:
            harness.add_storage("local", count=1, attach=True)
            location = harness.charm.model.storages["local"][0].location
            assert MicroCloudCharm._storage_local_path(harness.charm) == (str(location), None)
        finally:
            harness.cleanup()

    def test_local_device_config_names_the_device(self):
        """Juju cannot attach a RAID, LVM volume or partition on MAAS."""
        from charm import MicroCloudCharm

        harness = self._harness()
        try:
            harness.update_config({"local-device": "/dev/md1"})
            with patch("charm.microcloud.hostname", return_value="node1"):
                assert MicroCloudCharm._storage_local_path(harness.charm) == ("/dev/md1", None)
        finally:
            harness.cleanup()

    def test_local_device_accepts_a_hostname_mapping(self):
        from charm import MicroCloudCharm

        harness = self._harness()
        try:
            harness.update_config({"local-device": "{node1: /dev/md1, node2: /dev/nvme0n1p3}"})
            with patch("charm.microcloud.hostname", return_value="node2"):
                path, problem = MicroCloudCharm._storage_local_path(harness.charm)
            assert (path, problem) == ("/dev/nvme0n1p3", None)
        finally:
            harness.cleanup()

    def test_local_device_mapping_without_this_host_blocks(self):
        from charm import MicroCloudCharm

        harness = self._harness()
        try:
            harness.update_config({"local-device": "{node1: /dev/md1}"})
            with patch("charm.microcloud.hostname", return_value="node9"):
                path, problem = MicroCloudCharm._storage_local_path(harness.charm)
            assert path == ""
            assert problem == (
                "local-device is missing an entry for hostname 'node9'; known entries: ['node1']"
            )
        finally:
            harness.cleanup()

    def test_attached_storage_wins_over_local_device(self):
        from charm import MicroCloudCharm

        harness = self._harness()
        try:
            harness.add_storage("local", count=1, attach=True)
            harness.update_config({"local-device": "/dev/md1"})
            location = harness.charm.model.storages["local"][0].location
            assert MicroCloudCharm._storage_local_path(harness.charm) == (str(location), None)
        finally:
            harness.cleanup()

    def test_ceph_paths_returns_all_attached_devices(self):
        from charm import MicroCloudCharm

        harness = self._harness()
        try:
            harness.add_storage("ceph", count=3, attach=True)
            paths = MicroCloudCharm._storage_ceph_paths(harness.charm)
            assert len(paths) == 3
            assert all(path for path in paths)
        finally:
            harness.cleanup()


# ---------------------------------------------------------------------------
# microcloud CLI wrappers
# ---------------------------------------------------------------------------


class TestMicroCloudWrappers:
    def test_is_initialized_false_when_snap_absent(self):
        import microcloud

        with patch("microcloud.is_snap_installed", return_value=False):
            assert microcloud.is_initialized() is False

    def test_is_initialized_true_on_exit_zero(self):
        import microcloud

        with patch("microcloud.is_snap_installed", return_value=True):
            with patch("microcloud.subprocess.run", return_value=MagicMock(returncode=0)):
                assert microcloud.is_initialized() is True

    def test_is_initialized_false_on_nonzero(self):
        import microcloud

        with patch("microcloud.is_snap_installed", return_value=True):
            with patch(
                "microcloud.subprocess.run",
                return_value=MagicMock(returncode=1, stderr="uninitialized"),
            ):
                assert microcloud.is_initialized() is False

    def test_list_members_parses_json(self):
        import microcloud

        payload = json.dumps(
            [
                {"name": "node1", "address": "10.0.0.1:9443", "status": "ONLINE"},
                {"name": "node2", "address": "10.0.0.2:9443", "status": "ONLINE"},
            ]
        )
        with patch(
            "microcloud.subprocess.run", return_value=MagicMock(returncode=0, stdout=payload)
        ):
            members = microcloud.list_members()
        assert [m.name for m in members] == ["node1", "node2"]
        assert members[0].address == "10.0.0.1:9443"

    def test_list_members_raises_on_error(self):
        import microcloud

        with patch(
            "microcloud.subprocess.run", return_value=MagicMock(returncode=1, stderr="boom")
        ):
            with pytest.raises(microcloud.MicroCloudError):
                microcloud.list_members()


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------


class TestHeldStatus:
    def test_held_status_is_not_overwritten(self):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub._status_held = False
        stub._network = MagicMock(spec=UnitNetwork)
        stub._network.ovn_uplink_interface.return_value = ("", None)
        stub._storage_local_path.return_value = ("", None)
        stub._cos_related.return_value = False
        stub._coordinator = MagicMock()
        stub._sessions = MagicMock()

        def deploy(*args):
            stub._status_held = True

        stub._reconcile_deploy.side_effect = deploy
        with (
            patch("charm.microcloud.hostname", return_value="node1"),
            patch("charm.microcloud.is_initialized", return_value=False),
        ):
            MicroCloudCharm._reconcile(stub)

        stub._set_status.assert_not_called()

    def test_status_set_when_nothing_is_held(self):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub._status_held = False
        stub._network = MagicMock(spec=UnitNetwork)
        stub._network.ovn_uplink_interface.return_value = ("", None)
        stub._storage_local_path.return_value = ("", None)
        stub._cos_related.return_value = False
        stub._coordinator = MagicMock()
        stub._sessions = MagicMock()
        stub._reconcile_deploy.return_value = None
        with (
            patch("charm.microcloud.hostname", return_value="node1"),
            patch("charm.microcloud.is_initialized", return_value=False),
        ):
            MicroCloudCharm._reconcile(stub)

        stub._set_status.assert_called_once_with(initialized=False)


# ---------------------------------------------------------------------------
# Join sessions
# ---------------------------------------------------------------------------


class TestSessionCoordinator:
    """The join session the leader publishes on the peer relation."""

    def _harness(self):
        import ops.testing

        harness = ops.testing.Harness(
            ops.testing.CharmBase,
            meta="""
name: test-charm
peers:
  cluster:
    interface: microcloud-peer
""",
        )
        harness.set_leader(True)
        harness.begin()
        return harness

    def test_session_round_trips(self):
        from cluster import ClusterCoordinator, JoinSession

        harness = self._harness()
        try:
            coordinator = ClusterCoordinator(harness.charm)
            harness.add_relation("cluster", "test-charm")
            assert coordinator.session() is None

            opened = JoinSession(
                id="abc", address="10.0.0.1", systems=["node1", "node2"], deadline=123.0
            )
            coordinator.publish_session(opened)
            assert coordinator.session() == opened

            coordinator.publish_session(None)
            assert coordinator.session() is None
        finally:
            harness.cleanup()

    def test_only_the_leader_publishes_a_session(self):
        from cluster import ClusterCoordinator, JoinSession

        harness = self._harness()
        try:
            coordinator = ClusterCoordinator(harness.charm)
            harness.add_relation("cluster", "test-charm")
            harness.set_leader(False)

            coordinator.publish_session(JoinSession("abc", "10.0.0.1", ["node1"], 1.0))

            assert coordinator.session() is None
        finally:
            harness.cleanup()

    def test_malformed_session_is_ignored(self):
        from cluster import ClusterCoordinator

        harness = self._harness()
        try:
            coordinator = ClusterCoordinator(harness.charm)
            rel_id = harness.add_relation("cluster", "test-charm")
            harness.update_relation_data(rel_id, "test-charm", {"join-session": '{"id": 1}'})

            assert coordinator.session() is None
        finally:
            harness.cleanup()


class TestSession:
    """Starting and following join session workers."""

    @pytest.fixture(autouse=True)
    def _state_dir(self, tmp_path):
        with patch("session.STATE_DIR", tmp_path):
            yield tmp_path

    def test_start_hands_the_document_to_a_detached_worker(self, _state_dir):
        import subprocess

        import session

        worker = MagicMock(pid=42)
        with (
            patch("session.subprocess.Popen", return_value=worker) as popen,
            patch("session.stop") as stop,
            patch.dict("os.environ", {"JUJU_CONTEXT_ID": "microcloud/0-hook-1", "PATH": "/bin"}),
        ):
            pid = session.start(
                "abc",
                "passphrase: secret\n",
                "microcloud/0",
                Path("/charm"),
                retry_until=123.5,
                replacing=7,
            )

        assert pid == 42
        stop.assert_called_once_with(7)
        assert popen.call_args.args[0] == [
            "/usr/bin/python3",
            "/charm/src/join_session.py",
            "abc",
            str(_state_dir / "session.json"),
            "123.5",
            "microcloud/0",
            "/charm",
        ]

        kwargs = popen.call_args.kwargs
        assert kwargs["stdin"] == subprocess.PIPE
        # The hook's own output must not be inherited, or Juju waits on it.
        assert kwargs["stdout"] not in (None, subprocess.PIPE)
        assert kwargs["start_new_session"] is True
        # juju-exec refuses to run from what looks like a hook context.
        assert "JUJU_CONTEXT_ID" not in kwargs["env"]
        assert kwargs["env"]["PATH"] == "/bin"

        worker.stdin.write.assert_called_once_with("passphrase: secret\n")
        worker.stdin.close.assert_called_once()
        assert "secret" not in "".join(p.read_text() for p in _state_dir.iterdir())

    def test_start_failure_raises(self):
        import session

        with (
            patch("session.subprocess.Popen", side_effect=OSError("no python3")),
            pytest.raises(session.SessionError),
        ):
            session.start("abc", "doc", "microcloud/0", Path("/charm"))

    def test_result_waits_for_the_worker(self, _state_dir):
        import session

        assert session.result("abc") is None

        (_state_dir / "session.json").write_text(
            json.dumps({"id": "abc", "rc": 1, "output": "Error: boom\n"})
        )
        assert session.result("abc") == (1, "Error: boom\n")

    def test_result_ignores_another_session(self, _state_dir):
        import session

        (_state_dir / "session.json").write_text(json.dumps({"id": "abc", "rc": 0}))

        assert session.result("def") is None

    def test_clear_forgets_the_result(self, _state_dir):
        import session

        (_state_dir / "session.json").write_text(json.dumps({"id": "abc", "rc": 0}))

        session.clear()

        assert session.result("abc") is None

    def test_is_running_only_matches_a_worker(self):
        import session

        assert session.is_running(0) is False
        with patch(
            "session.Path.read_bytes", return_value=b"/usr/bin/python3\0x/join_session.py\0"
        ):
            assert session.is_running(42) is True
        # A reused PID belongs to something else.
        with patch("session.Path.read_bytes", return_value=b"/usr/sbin/sshd\0"):
            assert session.is_running(42) is False
        with patch("session.Path.read_bytes", side_effect=FileNotFoundError):
            assert session.is_running(42) is False

    def test_stop_terminates_only_a_running_worker(self):
        import signal

        import session

        with patch("session.is_running", return_value=True), patch("session.os.killpg") as kill:
            session.stop(42)
        kill.assert_called_once_with(42, signal.SIGTERM)

        with patch("session.is_running", return_value=False), patch("session.os.killpg") as kill:
            session.stop(42)
        kill.assert_not_called()


class TestJoinSessionWorker:
    """The background worker itself."""

    def _run(self, tmp_path, attempts, now):
        import join_session

        results = [MagicMock(returncode=rc, stdout=out, stderr="") for rc, out in attempts]
        result_file = tmp_path / "session.json"
        with (
            patch("join_session.subprocess.run", side_effect=results) as run,
            patch("join_session.time.time", side_effect=now),
            patch("join_session.time.sleep") as sleep,
        ):
            rc = join_session.run("abc", "doc", str(result_file), retry_until=100.0)
        return rc, json.loads(result_file.read_text()), run, sleep

    def test_retries_until_the_session_opens(self, tmp_path):
        rc, data, run, sleep = self._run(
            tmp_path,
            attempts=[(1, "No active session\n"), (1, "No active session\n"), (0, "joined\n")],
            now=[10.0, 20.0, 30.0],
        )

        assert rc == 0
        assert data == {
            "id": "abc",
            "rc": 0,
            "output": "No active session\nNo active session\njoined\n",
        }
        assert run.call_count == 3
        assert all(c.kwargs["input"] == "doc" for c in run.call_args_list)
        assert run.call_args.args[0] == ["microcloud", "preseed"]
        assert sleep.call_count == 2

    def test_gives_up_at_the_deadline(self, tmp_path):
        rc, data, run, _ = self._run(
            tmp_path,
            attempts=[(1, "No active session\n"), (1, "No active session\n")],
            now=[50.0, 150.0],
        )

        assert rc == 1
        assert data["rc"] == 1
        assert run.call_count == 2

    def test_dispatches_join_session_ended(self):
        import join_session

        with patch("join_session.subprocess.run") as run:
            join_session.dispatch("microcloud/0", "/charm")

        run.assert_called_once_with(
            [
                "/usr/bin/juju-exec",
                "-u",
                "microcloud/0",
                "JUJU_DISPATCH_PATH=hooks/join_session_ended /charm/dispatch",
            ],
            check=False,
        )


class TestGrowCoordinator:
    """Peer relation state the leader uses to tell forming from growing."""

    def _harness(self):
        import ops.testing

        harness = ops.testing.Harness(
            ops.testing.CharmBase,
            meta="""
name: test-charm
peers:
  cluster:
    interface: microcloud-peer
""",
        )
        harness.set_leader(True)
        harness.begin()
        return harness

    def _publish(self, harness, rel_id, unit, name, address, initialized):
        harness.add_relation_unit(rel_id, unit)
        harness.update_relation_data(
            rel_id,
            unit,
            {
                "microcloud-name": name,
                "microcloud-address": address,
                "microcloud-initialized": "true" if initialized else "false",
            },
        )

    def test_pending_systems_excludes_clustered_units(self):
        from cluster import ClusterCoordinator

        harness = self._harness()
        try:
            coordinator = ClusterCoordinator(harness.charm)
            rel_id = harness.add_relation("cluster", "test-charm")
            self._publish(harness, rel_id, "test-charm/1", "node1", "10.0.0.1", True)
            self._publish(harness, rel_id, "test-charm/2", "node2", "10.0.0.2", False)

            assert {s.name for s in coordinator.pending_systems()} == {"node2"}
        finally:
            harness.cleanup()

    def test_membership_overrides_lagging_flags(self):
        """A unit that has just joined still publishes "false" until its next hook."""
        from cluster import ClusterCoordinator

        harness = self._harness()
        try:
            coordinator = ClusterCoordinator(harness.charm)
            rel_id = harness.add_relation("cluster", "test-charm")
            self._publish(harness, rel_id, "test-charm/1", "node1", "10.0.0.1", False)
            self._publish(harness, rel_id, "test-charm/2", "node2", "10.0.0.2", False)

            pending = coordinator.pending_systems({"node0", "node1"})
            assert {s.name for s in pending} == {"node2"}
        finally:
            harness.cleanup()

    def test_all_ready_for_pending_units_ignores_members(self):
        from cluster import ClusterCoordinator

        harness = self._harness()
        try:
            coordinator = ClusterCoordinator(harness.charm)
            rel_id = harness.add_relation("cluster", "test-charm")
            # A member of a cluster formed outside Juju, which never reports ready.
            self._publish(harness, rel_id, "test-charm/1", "node1", "10.0.0.1", True)
            self._publish(harness, rel_id, "test-charm/2", "node2", "10.0.0.2", False)
            pending = [system for system in coordinator.all_systems() if system.name == "node2"]

            assert coordinator.all_ready(pending) is False

            harness.update_relation_data(rel_id, "test-charm/2", {"microcloud-ready": "true"})
            assert coordinator.all_ready(pending) is True
        finally:
            harness.cleanup()

    def test_any_initialized(self):
        from cluster import ClusterCoordinator

        harness = self._harness()
        try:
            coordinator = ClusterCoordinator(harness.charm)
            rel_id = harness.add_relation("cluster", "test-charm")
            self._publish(harness, rel_id, "test-charm/1", "node1", "10.0.0.1", False)
            assert coordinator.any_initialized() is False

            self._publish(harness, rel_id, "test-charm/2", "node2", "10.0.0.2", True)
            assert coordinator.any_initialized() is True
        finally:
            harness.cleanup()


class TestGrowCharm:
    """Forming and growing the cluster through join sessions."""

    @staticmethod
    def _system(name, address, initialized=False):
        from cluster import PeerSystem

        return PeerSystem(name=name, address=address, initialized=initialized)

    def _stub(self, *, leader, systems=(), current=None):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub.unit.is_leader.return_value = leader
        stub.unit.name = "microcloud/0"
        stub.charm_dir = Path("/charm")
        stub.config = {"session-timeout": 300}
        stub._status_held = False
        stub._stored = SimpleNamespace(session_failures=0)
        stub._worker_running = MagicMock(return_value=False)
        stub._start_worker = MagicMock()
        stub._network = MagicMock(spec=UnitNetwork)
        stub._network.bind_address.return_value = "10.0.0.1"
        stub._network.ovn_uplink_interface.return_value = ("", None)
        stub._storage_local_path.return_value = ("", None)
        stub._preseed_inputs.side_effect = lambda address, passphrase, entries: (
            address,
            passphrase,
            [entry.name for entry in entries],
        )
        stub._hold_status.side_effect = lambda status: setattr(stub.unit, "status", status)
        coordinator = stub._coordinator = MagicMock()
        coordinator.all_systems.return_value = list(systems)
        coordinator.all_ready.return_value = True
        coordinator.any_initialized.return_value = False
        coordinator.session.return_value = current
        stub._sessions = MagicMock()
        stub._sessions._worker_running = stub._worker_running
        stub._sessions._start_worker = stub._start_worker
        return stub

    def _lead(
        self,
        stub,
        *,
        initialized,
        pending,
        running=False,
        outcome=None,
        now=1000.0,
        ceph=True,
        pending_after=None,
    ):
        """Run ``_lead_session``.

        ``pending_after`` is the membership read again once a session has
        ended, which defaults to ``pending``.
        """
        from charm import MicroCloudCharm

        stub._sessions.pending.return_value = pending if pending_after is None else pending_after
        with (
            patch("charm.snap.is_installed", return_value=ceph),
            patch("charm.render", side_effect=lambda inputs: inputs),
            patch("charm.session.result", return_value=outcome),
            patch("charm.session.clear") as clear,
            patch("charm.time.time", return_value=now),
            patch("charm.microcloud.hostname", return_value="node0"),
            patch("charm.microcloud.is_initialized", return_value=initialized),
        ):
            stub._worker_running.return_value = running
            problem = MicroCloudCharm._lead_session(stub, initialized, pending, "secret")
        return problem, stub._start_worker, clear

    # ---- _lead_session: opening a session ----

    def test_unclustered_leader_forms_the_cluster_with_every_unit(self):
        import ops

        systems = [self._system("node0", "10.0.0.1"), self._system("node1", "10.0.0.2")]
        stub = self._stub(leader=True, systems=systems)

        problem, start, _ = self._lead(stub, initialized=False, pending=systems)

        assert problem is None
        session_id, document = start.call_args.args
        assert document == ("10.0.0.1", "secret", ["node0", "node1"])

        opened = stub._coordinator.publish_session.call_args.args[0]
        assert opened.id == session_id
        assert opened.address == "10.0.0.1"
        assert opened.systems == ["node0", "node1"]
        assert opened.deadline > 1000.0 + 300
        assert isinstance(stub.unit.status, ops.MaintenanceStatus)
        assert "Forming" in stub.unit.status.message

    def test_leader_still_reports_forming_once_it_has_joined(self):
        """The leader is clustered well before the joiners it lists have finished."""
        from cluster import JoinSession

        current = JoinSession("abc", "10.0.0.1", ["node0", "node1"], 2000.0)
        stub = self._stub(leader=True, current=current)

        self._lead(stub, initialized=True, pending=[], running=True)

        assert "Forming" in stub.unit.status.message

    def test_clustered_leader_lists_only_pending_units(self):
        """Leaving the initiator out of "systems" is what makes it an add."""
        node0 = self._system("node0", "10.0.0.1", initialized=True)
        node1 = self._system("node1", "10.0.0.2")
        stub = self._stub(leader=True, systems=[node0, node1])

        problem, start, _ = self._lead(stub, initialized=True, pending=[node1])

        assert problem is None
        assert start.call_args.args[1] == ("10.0.0.1", "secret", ["node1"])
        assert stub._coordinator.publish_session.call_args.args[0].systems == ["node1"]
        assert "Joining 1 unit(s)" in stub.unit.status.message

    def test_clustered_leader_without_microceph_blocks_instead_of_adding(self):
        """MicroCloud's preseed panics adding systems to a cluster without MicroCeph."""
        stub = self._stub(leader=True)

        problem, start, _ = self._lead(
            stub, initialized=True, pending=[self._system("node1", "10.0.0.2")], ceph=False
        )

        assert problem is not None
        assert "MicroCeph" in problem
        assert "1 unit(s)" in problem
        start.assert_not_called()
        stub._coordinator.publish_session.assert_not_called()

    def test_forming_a_cluster_does_not_need_microceph(self):
        systems = [self._system("node0", "10.0.0.1"), self._system("node1", "10.0.0.2")]
        stub = self._stub(leader=True, systems=systems)

        problem, start, _ = self._lead(stub, initialized=False, pending=systems, ceph=False)

        assert problem is None
        start.assert_called_once()

    def test_clustered_leader_only_waits_for_joining_units_to_be_ready(self):
        """Members of a cluster formed outside Juju never report ready."""
        pending = [self._system("node1", "10.0.0.2")]
        stub = self._stub(leader=True)

        self._lead(stub, initialized=True, pending=pending)

        stub._coordinator.all_ready.assert_called_once_with(pending)

    def test_leader_waits_for_every_unit_to_be_ready(self):
        import ops

        stub = self._stub(leader=True)
        stub._coordinator.all_ready.return_value = False

        problem, start, _ = self._lead(
            stub, initialized=False, pending=[self._system("node0", "10.0.0.1")]
        )

        assert problem is None
        start.assert_not_called()
        assert isinstance(stub.unit.status, ops.WaitingStatus)

    def test_unclustered_leader_does_not_take_over_an_existing_cluster(self):
        """It would bootstrap a second cluster rather than add to the first."""
        stub = self._stub(leader=True)
        stub._coordinator.any_initialized.return_value = True

        problem, start, _ = self._lead(
            stub, initialized=False, pending=[self._system("node0", "10.0.0.1")]
        )

        assert problem is not None
        assert "node0" in problem
        start.assert_not_called()

    def test_failure_to_start_the_session_blocks(self):
        import session

        stub = self._stub(leader=True)

        from charm import MicroCloudCharm

        with (
            patch("charm.snap.is_installed", return_value=True),
            patch("charm.render", side_effect=lambda inputs: inputs),
        ):
            stub._start_worker.side_effect = session.SessionError("no worker")
            problem = MicroCloudCharm._lead_session(
                stub, True, [self._system("node1", "10.0.0.2")], "secret"
            )

        assert problem == "no worker"
        stub._coordinator.publish_session.assert_not_called()

    def test_nothing_to_do_without_pending_units(self):
        stub = self._stub(leader=True)

        problem, start, _ = self._lead(stub, initialized=True, pending=[])

        assert problem is None
        start.assert_not_called()
        stub._hold_status.assert_not_called()

    # ---- _lead_session: following a published session ----

    def _current(self, address="10.0.0.1", deadline=2000.0):
        from cluster import JoinSession

        return JoinSession(id="abc", address=address, systems=["node1"], deadline=deadline)

    def test_session_in_progress_is_left_alone(self):
        import ops

        stub = self._stub(leader=True, current=self._current())

        problem, start, clear = self._lead(
            stub, initialized=True, pending=[self._system("node1", "10.0.0.2")], running=True
        )

        assert problem is None
        start.assert_not_called()
        clear.assert_not_called()
        stub._coordinator.publish_session.assert_not_called()
        assert isinstance(stub.unit.status, ops.MaintenanceStatus)

    def test_result_counts_even_while_the_worker_is_still_running(self):
        """The hook the worker fires once it is done runs while the worker waits on it."""
        stub = self._stub(leader=True, current=self._current())

        problem, start, clear = self._lead(
            stub, initialized=True, pending=[], running=True, outcome=(0, "")
        )

        assert problem is None
        clear.assert_called_once()
        stub._coordinator.publish_session.assert_called_once_with(None)

    def test_successful_session_is_cleared_and_the_next_one_opened(self):
        node2 = self._system("node2", "10.0.0.3")
        stub = self._stub(leader=True, current=self._current())

        problem, start, clear = self._lead(
            stub, initialized=True, pending=[node2], outcome=(0, "MicroCloud is ready\n")
        )

        assert problem is None
        clear.assert_called_once()
        assert stub._coordinator.publish_session.call_args_list[0].args == (None,)
        assert start.call_args.args[1] == ("10.0.0.1", "secret", ["node2"])

    def test_membership_is_read_again_once_a_session_ends(self):
        """The session's result can land after this hook read the membership."""
        stub = self._stub(leader=True, current=self._current())

        problem, start, clear = self._lead(
            stub,
            initialized=True,
            pending=[self._system("node1", "10.0.0.2")],
            outcome=(0, "MicroCloud is ready\n"),
            pending_after=[],
        )

        assert problem is None
        clear.assert_called_once()
        stub._sessions.pending.assert_called_once_with(True)
        stub._coordinator.publish_session.assert_called_once_with(None)
        start.assert_not_called()

    def test_unreadable_membership_after_a_session_blocks(self):
        import microcloud

        stub = self._stub(leader=True, current=self._current())
        stub._sessions.pending.side_effect = microcloud.MicroCloudError("boom")

        problem, start, _ = self._lead(
            stub, initialized=True, pending=[self._system("node1", "10.0.0.2")], outcome=(0, "")
        )

        assert problem == "Cannot read MicroCloud members: boom"
        start.assert_not_called()

    _REACHED_OUT = 'Searching for joining systems\nError: System "node1" hasn\'t reached out\n'

    def test_failed_session_is_retried_from_the_same_hook(self):
        """Nothing else wakes the leader once it has cleared its session."""
        import ops

        stub = self._stub(leader=True, current=self._current())

        problem, start, clear = self._lead(
            stub,
            initialized=True,
            pending=[self._system("node1", "10.0.0.2")],
            outcome=(1, self._REACHED_OUT),
        )

        assert problem is None
        clear.assert_called_once()
        assert stub._coordinator.publish_session.call_args_list[0].args == (None,)
        start.assert_called_once()
        assert stub._stored.session_failures == 1
        assert isinstance(stub.unit.status, ops.MaintenanceStatus)
        assert "hasn't reached out" in stub.unit.status.message

    def test_sessions_that_keep_failing_block(self):
        stub = self._stub(leader=True, current=self._current())
        stub._stored.session_failures = 2

        problem, start, clear = self._lead(
            stub,
            initialized=True,
            pending=[self._system("node1", "10.0.0.2")],
            outcome=(1, self._REACHED_OUT),
        )

        assert problem == 'Join session failed: Error: System "node1" hasn\'t reached out'
        clear.assert_called_once()
        stub._coordinator.publish_session.assert_called_once_with(None)
        start.assert_not_called()

    def test_successful_session_resets_the_failure_count(self):
        stub = self._stub(leader=True, current=self._current())
        stub._stored.session_failures = 2

        self._lead(stub, initialized=True, pending=[], outcome=(0, ""))

        assert stub._stored.session_failures == 0

    def test_new_leader_ignores_its_own_attempt_at_a_previous_leaders_session(self):
        """Its local result and worker are from dialling in, not from the session itself."""
        import ops

        stub = self._stub(leader=True, current=self._current(address="10.0.0.9", deadline=2000.0))

        problem, start, clear = self._lead(
            stub,
            initialized=True,
            pending=[self._system("node2", "10.0.0.3")],
            running=False,
            outcome=(0, "Successfully joined"),
            now=1000.0,
        )

        assert problem is None
        start.assert_not_called()
        clear.assert_not_called()
        stub._coordinator.publish_session.assert_not_called()
        assert isinstance(stub.unit.status, ops.WaitingStatus)

    def test_waits_for_a_previous_leaders_session_to_end(self):
        import ops

        stub = self._stub(leader=True, current=self._current(address="10.0.0.9", deadline=2000.0))

        problem, start, clear = self._lead(
            stub, initialized=True, pending=[self._system("node1", "10.0.0.2")], now=1000.0
        )

        assert problem is None
        start.assert_not_called()
        clear.assert_not_called()
        assert isinstance(stub.unit.status, ops.WaitingStatus)

    def test_takes_over_once_a_previous_leaders_session_has_ended(self):
        stub = self._stub(leader=True, current=self._current(address="10.0.0.9", deadline=2000.0))

        problem, start, _ = self._lead(
            stub, initialized=True, pending=[self._system("node1", "10.0.0.2")], now=2001.0
        )

        assert problem is None
        assert stub._coordinator.publish_session.call_args_list[0].args == (None,)
        opened = stub._coordinator.publish_session.call_args_list[1].args[0]
        assert opened.address == "10.0.0.1"
        start.assert_called_once()

    def test_own_session_that_vanished_is_replaced(self):
        """E.g. the leader rebooted mid-session and lost it without a result."""
        stub = self._stub(leader=True, current=self._current())

        problem, start, _ = self._lead(
            stub, initialized=True, pending=[self._system("node1", "10.0.0.2")]
        )

        assert problem is None
        start.assert_called_once()

    # ---- _join_session ----

    def _join(self, stub, *, hostname="node1", running=False, outcome=None):
        from charm import MicroCloudCharm

        with (
            patch("charm.render", side_effect=lambda inputs: inputs),
            patch("charm.microcloud.hostname", return_value=hostname),
            patch("charm.session.result", return_value=outcome),
        ):
            stub._worker_running.return_value = running
            problem = MicroCloudCharm._join_session(stub, "secret")
        return problem, stub._start_worker

    def _joiner(self, systems=(), current=None):
        stub = self._stub(leader=False, systems=systems, current=current)
        stub._network.bind_address.return_value = "10.0.0.2"
        return stub

    def test_joiner_waits_without_a_session(self):
        import ops

        stub = self._joiner()

        problem, start = self._join(stub)

        assert problem is None
        start.assert_not_called()
        assert isinstance(stub.unit.status, ops.WaitingStatus)

    def test_joiner_not_listed_waits_for_the_next_session(self):
        stub = self._joiner(current=self._current())

        problem, start = self._join(stub, hostname="node7")

        assert problem is None
        start.assert_not_called()
        assert "next join session" in stub.unit.status.message

    def test_former_leader_does_not_dial_its_own_session(self):
        """A leader that lost leadership mid-session is still that session's initiator."""
        stub = self._joiner(current=self._current(address="10.0.0.2"))

        problem, start = self._join(stub)

        assert problem is None
        start.assert_not_called()
        assert "this unit opened" in stub.unit.status.message

    def test_joiner_dials_in_the_background_with_exactly_the_listed_systems(self):
        import ops

        from cluster import JoinSession

        systems = [
            self._system("node0", "10.0.0.1", initialized=True),
            self._system("node1", "10.0.0.2"),
            self._system("node2", "10.0.0.3"),
        ]
        current = JoinSession("abc", "10.0.0.1", ["node1", "node2"], 2000.0)
        stub = self._joiner(systems=systems, current=current)

        problem, start = self._join(stub)

        assert problem is None
        start.assert_called_once_with(
            "abc", ("10.0.0.1", "secret", ["node1", "node2"]), retry_until=2000.0
        )
        assert isinstance(stub.unit.status, ops.MaintenanceStatus)

    def test_joiner_follows_its_attempt_without_restarting_it(self):
        import ops

        stub = self._joiner(current=self._current())

        problem, start = self._join(stub, running=True)

        assert problem is None
        start.assert_not_called()
        assert isinstance(stub.unit.status, ops.MaintenanceStatus)

    def test_joiner_reports_a_successful_attempt_while_services_finish_joining(self):
        import ops

        stub = self._joiner(current=self._current())

        problem, start = self._join(stub, outcome=(0, "Successfully joined\n"))

        assert problem is None
        start.assert_not_called()
        assert isinstance(stub.unit.status, ops.MaintenanceStatus)
        assert "Joined" in stub.unit.status.message

    def test_joiner_waits_for_the_next_session_after_a_failed_attempt(self):
        """The attempt already retried until the deadline; the leader opens the next session."""
        import ops

        stub = self._joiner(current=self._current())

        problem, start = self._join(stub, outcome=(1, "Error: No active session\n"))

        assert problem is None
        start.assert_not_called()
        assert isinstance(stub.unit.status, ops.WaitingStatus)
        assert "Error: No active session" in stub.unit.status.message

    def test_joiner_blocks_when_the_attempt_cannot_start(self):
        import session
        from charm import MicroCloudCharm

        stub = self._joiner(current=self._current())
        with (
            patch("charm.render", side_effect=lambda inputs: inputs),
            patch("charm.microcloud.hostname", return_value="node1"),
            patch("charm.session.result", return_value=None),
        ):
            stub._start_worker.side_effect = session.SessionError("no worker")
            problem = MicroCloudCharm._join_session(stub, "secret")

        assert problem == "no worker"

    # ---- _reconcile_deploy ----

    def _deploy(self, stub, *, initialized, pending, uplink_problem=None):
        from charm import MicroCloudCharm

        coordinator = stub._coordinator
        coordinator.all_identities_published.return_value = True
        coordinator.ensure_passphrase.return_value = "secret"
        coordinator.all_members.return_value = [("node0", "10.0.0.1"), ("node1", "10.0.0.2")]
        stub._lead_session.return_value = None
        stub._join_session.return_value = None
        stub._network.ovn_uplink_interface.return_value = ("enp9s0", None)
        stub._network.missing_uplink_interface.return_value = uplink_problem

        with (
            patch("charm.snap.ensure_snaps") as ensure_snaps,
            patch("charm.microcloud.waitready", return_value=True),
        ):
            problem = MicroCloudCharm._reconcile_deploy(stub, initialized, pending)
        return problem, ensure_snaps

    def test_an_uplink_nic_that_is_missing_blocks_the_deploy(self):
        """A cluster formed with an unknown uplink has no external network at all."""
        stub = self._stub(leader=True)
        stub.config = {"snap-channel-microovn": "24.03/stable"}

        problem, _ = self._deploy(
            stub,
            initialized=False,
            pending=[],
            uplink_problem="ovn-uplink-interface 'eth99' does not exist on node1; interfaces: []",
        )

        assert problem is not None
        assert "does not exist" in problem
        stub._lead_session.assert_not_called()

    def test_the_uplink_nic_is_not_checked_without_microovn(self):
        """With MicroOVN off the name never reaches the preseed, so a stale value is harmless."""
        stub = self._stub(leader=True)
        stub.config = {"snap-channel-microovn": ""}

        problem, _ = self._deploy(
            stub,
            initialized=False,
            pending=[],
            uplink_problem="ovn-uplink-interface 'eth99' does not exist on node1; interfaces: []",
        )

        assert problem is None
        stub._network.missing_uplink_interface.assert_not_called()
        stub._lead_session.assert_called_once()

    def test_clustered_leader_does_not_refresh_snaps(self):
        """Refreshing here would upgrade the leader ahead of the rest of the cluster."""
        stub = self._stub(leader=True)
        pending = [self._system("node1", "10.0.0.2")]

        problem, ensure_snaps = self._deploy(stub, initialized=True, pending=pending)

        assert problem is None
        ensure_snaps.assert_not_called()
        stub._lead_session.assert_called_once_with(True, pending, "secret")

    def test_unclustered_joiner_installs_snaps_and_joins(self):
        stub = self._stub(leader=False)

        problem, ensure_snaps = self._deploy(stub, initialized=False, pending=[])

        assert problem is None
        ensure_snaps.assert_called_once()
        stub._coordinator.publish_ready.assert_called_once()
        stub._join_session.assert_called_once_with("secret")
        stub._lead_session.assert_not_called()

    # ---- _reconcile ----

    def _reconcile(self, stub, *, initialized, members):
        from charm import MicroCloudCharm
        from microcloud import Member

        stub._cos_related.return_value = False
        stub._reconcile_observe_only.return_value = None
        stub._reconcile_deploy.return_value = None
        stub._failure_domains = MagicMock(spec=FailureDomains)
        stub._failure_domains.reconcile.return_value = None
        stub._sessions.pending.side_effect = lambda initialized: session.JoinSessions(
            stub.unit,
            stub._coordinator,
            stub._network,
            stub.config,
            stub.charm_dir,
            stub._stored,
            stub._hold_status,
        ).pending(initialized)

        with (
            patch("charm.microcloud.hostname", return_value="node0"),
            patch("charm.microcloud.is_initialized", return_value=initialized),
            patch("charm.lxd_cluster.is_clustered", return_value=True),
            patch(
                "charm.microcloud.list_members",
                return_value=[Member(name, "") for name in members],
            ) as list_members,
        ):
            MicroCloudCharm._reconcile(stub)
        return list_members

    def _lagging_coordinator(self, stub, systems, current=None):
        """Coordinator whose published flags all still read "not clustered"."""
        from cluster import ClusterCoordinator

        def pending_systems(members=None):
            if members is None:
                return list(systems)
            return [system for system in systems if system.name not in members]

        stub._coordinator = MagicMock(spec=ClusterCoordinator)
        stub._coordinator.pending_systems.side_effect = pending_systems
        stub._coordinator.session.return_value = current

    def test_clustered_leader_observes_once_every_unit_is_a_member(self):
        """Lagging flags right after a bootstrap must not start a new session."""
        systems = [self._system("node1", "10.0.0.2"), self._system("node2", "10.0.0.3")]
        stub = self._stub(leader=True)
        self._lagging_coordinator(stub, systems)

        self._reconcile(stub, initialized=True, members=["node0", "node1", "node2"])

        stub._reconcile_observe_only.assert_called_once()
        stub._reconcile_deploy.assert_not_called()

    def test_clustered_leader_initiates_for_units_missing_from_membership(self):
        node1 = self._system("node1", "10.0.0.2")
        node2 = self._system("node2", "10.0.0.3")
        stub = self._stub(leader=True)
        self._lagging_coordinator(stub, [node1, node2])

        self._reconcile(stub, initialized=True, members=["node0", "node1"])

        stub._reconcile_deploy.assert_called_once_with(True, [node2])
        stub._reconcile_observe_only.assert_not_called()

    def test_clustered_leader_follows_its_session_to_the_end(self):
        """Every unit is a member, but the published session still needs clearing."""
        stub = self._stub(leader=True)
        self._lagging_coordinator(stub, [], current=self._current())

        self._reconcile(stub, initialized=True, members=["node0", "node1"])

        stub._reconcile_deploy.assert_called_once_with(True, [])

    def test_clustered_non_leader_observes_without_reading_membership(self):
        stub = self._stub(leader=False)
        self._lagging_coordinator(stub, [self._system("node1", "10.0.0.2")])

        list_members = self._reconcile(stub, initialized=True, members=[])

        list_members.assert_not_called()
        stub._reconcile_observe_only.assert_called_once()
        stub._reconcile_deploy.assert_not_called()

    def test_unreadable_membership_blocks(self):
        import ops

        import microcloud
        from charm import MicroCloudCharm

        stub = self._stub(leader=True)
        self._lagging_coordinator(stub, [self._system("node1", "10.0.0.2")])
        stub._sessions.pending.side_effect = lambda initialized: session.JoinSessions(
            stub.unit,
            stub._coordinator,
            stub._network,
            stub.config,
            stub.charm_dir,
            stub._stored,
            stub._hold_status,
        ).pending(initialized)

        with (
            patch("charm.microcloud.hostname", return_value="node0"),
            patch("charm.microcloud.is_initialized", return_value=True),
            patch(
                "charm.microcloud.list_members",
                side_effect=microcloud.MicroCloudError("boom"),
            ),
        ):
            MicroCloudCharm._reconcile(stub)

        assert isinstance(stub.unit.status, ops.BlockedStatus)
        stub._reconcile_deploy.assert_not_called()
        stub._reconcile_observe_only.assert_not_called()


class TestJoinSessions:
    """JoinSessions helper in session.py."""

    def _sessions(self, *, leader=True, coordinator=None, network=None, config=None, stored=None):
        import ops

        unit = MagicMock(spec=ops.Unit)
        unit.is_leader.return_value = leader
        unit.name = "microcloud/0"
        coordinator = coordinator or MagicMock()
        network = network or MagicMock(spec=UnitNetwork)
        config = config if config is not None else {"session-timeout": 300}
        charm_dir = Path("/charm")
        stored = stored or SimpleNamespace(session_failures=0, worker_pid=0, worker_session="")
        hold_status = MagicMock()
        return session.JoinSessions(
            unit=unit,
            coordinator=coordinator,
            network=network,
            config=config,
            charm_dir=charm_dir,
            stored=stored,
            hold_status=hold_status,
        )

    def test_pending_clustered_leader_reads_microcloud_members(self):
        from microcloud import Member

        coordinator = MagicMock()
        coordinator.pending_systems.return_value = ["pending"]
        sessions = self._sessions(leader=True, coordinator=coordinator)

        with patch(
            "session.microcloud.list_members",
            return_value=[Member("node0", ""), Member("node1", "")],
        ) as list_members:
            pending = sessions.pending(initialized=True)

        assert pending == ["pending"]
        list_members.assert_called_once()
        coordinator.pending_systems.assert_called_once_with({"node0", "node1"})

    def test_pending_unclustered_leader_uses_published_flags(self):
        coordinator = MagicMock()
        coordinator.pending_systems.return_value = ["pending"]
        sessions = self._sessions(leader=True, coordinator=coordinator)

        with patch("session.microcloud.list_members") as list_members:
            pending = sessions.pending(initialized=False)

        assert pending == ["pending"]
        list_members.assert_not_called()
        coordinator.pending_systems.assert_called_once_with()

    def test_pending_clustered_non_leader_uses_published_flags(self):
        coordinator = MagicMock()
        coordinator.pending_systems.return_value = ["pending"]
        sessions = self._sessions(leader=False, coordinator=coordinator)

        with patch("session.microcloud.list_members") as list_members:
            pending = sessions.pending(initialized=True)

        assert pending == ["pending"]
        list_members.assert_not_called()
        coordinator.pending_systems.assert_called_once_with()


class TestSessionWorkerHelpers:
    """The charm's bookkeeping for its one join session worker."""

    def _sessions(self, pid=0, session_id=""):
        import ops

        unit = MagicMock(spec=ops.Unit)
        unit.name = "microcloud/0"
        stored = SimpleNamespace(worker_pid=pid, worker_session=session_id)
        return session.JoinSessions(
            unit=unit,
            coordinator=MagicMock(),
            network=MagicMock(spec=UnitNetwork),
            config={},
            charm_dir=Path("/charm"),
            stored=stored,
            hold_status=MagicMock(),
        )

    def test_worker_for_another_session_is_not_running(self):
        sessions = self._sessions(pid=42, session_id="old")
        with patch("session.is_running", return_value=True):
            assert sessions._worker_running("abc") is False
            assert sessions._worker_running("old") is True

    def test_start_worker_replaces_the_previous_one(self):
        sessions = self._sessions(pid=42, session_id="old")
        with patch("session.start", return_value=99) as start:
            sessions._start_worker("abc", "doc", retry_until=5.0)

        start.assert_called_once_with(
            "abc", "doc", "microcloud/0", Path("/charm"), retry_until=5.0, replacing=42
        )
        assert (sessions._stored.worker_pid, sessions._stored.worker_session) == (99, "abc")


# ---------------------------------------------------------------------------
# Failure domains
# ---------------------------------------------------------------------------


def _member(name, domain="zone-1", roles=(), status="Online", arch="x86_64"):
    from lxd_cluster import Member

    return Member(
        name=name, status=status, failure_domain=domain, architecture=arch, roles=list(roles)
    )


class TestLxdCluster:
    """LXD cluster members and their failure domains."""

    def test_members_parses_the_cluster_member_list(self):
        import lxd_cluster

        payload = json.dumps(
            [
                {
                    "server_name": "node1",
                    "status": "Online",
                    "failure_domain": "zone-1",
                    "architecture": "x86_64",
                    "roles": ["database-leader", "database-voter"],
                }
            ]
        )
        with patch("lxd_cluster.subprocess.run", return_value=MagicMock(stdout=payload)) as run:
            [member] = lxd_cluster.members()

        assert run.call_args.args[0] == [
            "lxc",
            "query",
            "-X",
            "GET",
            "/1.0/cluster/members?recursion=1",
        ]
        assert member.name == "node1"

    @pytest.mark.parametrize(
        ("payload", "clustered"), [({"enabled": True}, True), ({"enabled": False}, False)]
    )
    def test_is_clustered_reads_the_cluster_state(self, payload, clustered):
        import lxd_cluster

        with patch(
            "lxd_cluster.subprocess.run", return_value=MagicMock(stdout=json.dumps(payload))
        ) as run:
            assert lxd_cluster.is_clustered() is clustered

        assert run.call_args.args[0] == ["lxc", "query", "-X", "GET", "/1.0/cluster"]

    def test_set_failure_domain_writes_back_the_whole_member(self):
        """LXD empties any writable field left out, and refuses a member without groups."""
        import lxd_cluster

        member = {
            "server_name": "node1",
            "config": {"scheduler.instance": "all"},
            "description": "rack 4",
            "groups": ["default", "gpu"],
            "roles": ["database-voter", "database-leader", "control-plane"],
            "failure_domain": "default",
            "status": "Online",
        }
        with patch(
            "lxd_cluster.subprocess.run",
            side_effect=[MagicMock(stdout=json.dumps(member)), MagicMock(stdout="")],
        ) as run:
            lxd_cluster.set_failure_domain("node1", "zone-2")

        put = run.call_args_list[1].args[0]
        assert put[:4] == ["lxc", "query", "-X", "PUT"]
        assert put[-1] == "/1.0/cluster/members/node1"
        assert json.loads(put[5]) == {
            "config": {"scheduler.instance": "all"},
            "description": "rack 4",
            "groups": ["default", "gpu"],
            "roles": ["control-plane"],
            "failure_domain": "zone-2",
        }

    def test_set_failure_domain_keeps_the_legacy_database_role(self):
        """Older LXD refuses a PUT that drops or adds "database", so it is written back as read."""
        import lxd_cluster

        member = {"groups": ["default"], "roles": ["database", "database-standby"]}
        with patch(
            "lxd_cluster.subprocess.run",
            side_effect=[MagicMock(stdout=json.dumps(member)), MagicMock(stdout="")],
        ) as run:
            lxd_cluster.set_failure_domain("node1", "zone-2")

        assert json.loads(run.call_args_list[1].args[0][5])["roles"] == ["database"]

    def test_failed_query_raises(self):
        import subprocess

        import lxd_cluster

        with (
            patch(
                "lxd_cluster.subprocess.run",
                side_effect=subprocess.CalledProcessError(1, "lxc", stderr="Error: not clustered"),
            ),
            pytest.raises(lxd_cluster.LXDClusterError, match="not clustered"),
        ):
            lxd_cluster.members()

    def test_render_table(self):
        import lxd_cluster

        table = lxd_cluster.render_table(
            [
                _member("node-02", "zone-2", ["database-voter", "control-plane"]),
                _member("node-01", "zone-1", ["database-leader", "control-plane"]),
                _member("node-10", "zone-3", [], arch="aarch64"),
            ]
        )

        assert table.splitlines() == [
            "NAME     ROLES                          FAILURE DOMAIN  ARCHITECTURE",
            "node-01  database-leader,control-plane  zone-1          x86_64",
            "node-02  database-voter,control-plane   zone-2          x86_64",
            "node-10  -                              zone-3          aarch64",
        ]


class TestFailureDomains:
    """How the charm uses this unit's zone and LXD failure domain."""

    def _stub(self):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub._failure_domains = MagicMock(spec=FailureDomains)
        stub._failure_domains.reconcile.return_value = None
        stub._sessions = MagicMock()
        return stub

    def _reconcile(self, stub, *, initialized=True, lxd_clustered=True):
        from charm import MicroCloudCharm

        stub._status_held = False
        stub._network = MagicMock(spec=UnitNetwork)
        stub._network.ovn_uplink_interface.return_value = ("", None)
        stub._storage_local_path.return_value = ("", None)
        stub._coordinator = MagicMock()
        stub._reconcile_deploy.return_value = None
        stub._reconcile_observe_only.return_value = None
        stub._cos_related.return_value = False

        def hold(status):
            stub.unit.status = status
            stub._status_held = True

        stub._hold_status.side_effect = hold
        with (
            patch("charm.microcloud.hostname", return_value="node1"),
            patch("charm.microcloud.is_initialized", side_effect=[initialized, True]),
            patch("charm.lxd_cluster.is_clustered", return_value=lxd_clustered),
        ):
            MicroCloudCharm._reconcile(stub)

    @pytest.mark.parametrize("initialized", [True, False])
    def test_only_a_unit_clustered_when_the_hook_starts_sets_its_failure_domain(self, initialized):
        """A join finishing in the background may not have brought LXD in yet."""
        stub = self._stub()

        self._reconcile(stub, initialized=initialized)

        assert stub._failure_domains.reconcile.called is initialized

    def _await_stub(self, *, since=0.0, session=None, now=1000.0):
        from charm import MicroCloudCharm

        stub = self._stub()
        stub._status_held = False
        stub.config = {"session-timeout": 300}
        stub._stored = SimpleNamespace(lxd_join_since=since)
        stub._coordinator = MagicMock()
        stub._coordinator.session.return_value = session
        stub._hold_status.side_effect = lambda status: setattr(stub.unit, "status", status)
        with patch("charm.time.time", return_value=now):
            problem = MicroCloudCharm._await_lxd_join(stub)
        return stub, problem

    def test_await_lxd_join_waits_while_a_session_is_open(self):
        """The initiator is still working; LXD joins at the end of it."""
        import ops

        from cluster import JoinSession

        session = JoinSession(id="abc", address="10.0.0.1", systems=["node1"], deadline=2000.0)
        stub, problem = self._await_stub(since=1.0, session=session, now=100000.0)

        assert problem is None
        assert stub.unit.status == ops.WaitingStatus("Waiting for LXD to join the cluster")

    def test_await_lxd_join_restarts_the_clock_while_a_session_runs(self):
        """Otherwise a long session eats the grace and the next hook blocks for nothing."""
        from cluster import JoinSession

        session = JoinSession(id="abc", address="10.0.0.1", systems=["node1"], deadline=2000.0)
        stub, _ = self._await_stub(since=1.0, session=session, now=100000.0)

        assert stub._stored.lxd_join_since == 100000.0

    def test_await_lxd_join_waits_inside_the_grace(self):
        import ops

        stub, problem = self._await_stub(since=900.0, now=1000.0)

        assert problem is None
        assert stub.unit.status == ops.WaitingStatus("Waiting for LXD to join the cluster")

    def test_await_lxd_join_starts_the_clock_on_the_first_hook(self):
        stub, problem = self._await_stub(since=0.0, now=1000.0)

        assert problem is None
        assert stub._stored.lxd_join_since == 1000.0

    def test_await_lxd_join_reports_a_bootstrap_that_stopped_part_way(self):
        """Growth cannot fix this: it needs the LXD that never arrived."""
        stub, problem = self._await_stub(since=1000.0, now=1000.0 + 421)

        assert problem is not None
        assert "MicroCloud is initialized but LXD has not joined after 421s" in problem
        assert "join-session.log" in problem
        stub._hold_status.assert_not_called()

    def test_waits_for_lxd_to_join_before_setting_the_failure_domain(self):
        """MicroCloud reports a joiner clustered before the initiator has added its LXD."""
        stub = self._stub()
        stub._await_lxd_join.return_value = None

        self._reconcile(stub, lxd_clustered=False)

        stub._failure_domains.reconcile.assert_not_called()
        stub._await_lxd_join.assert_called_once()

    def test_unreadable_lxd_cluster_blocks(self):
        import ops

        import lxd_cluster
        from charm import MicroCloudCharm

        stub = self._stub()
        stub.unit.is_leader.return_value = False
        stub._status_held = False
        stub._network = MagicMock(spec=UnitNetwork)
        stub._network.ovn_uplink_interface.return_value = ("", None)
        stub._storage_local_path.return_value = ("", None)
        stub._coordinator = MagicMock()
        stub._reconcile_observe_only.return_value = None
        with (
            patch("charm.microcloud.hostname", return_value="node1"),
            patch("charm.microcloud.is_initialized", return_value=True),
            patch(
                "charm.lxd_cluster.is_clustered", side_effect=lxd_cluster.LXDClusterError("boom")
            ),
        ):
            MicroCloudCharm._reconcile(stub)

        assert stub.unit.status == ops.BlockedStatus("Cannot read the LXD cluster: boom")
        stub._failure_domains.reconcile.assert_not_called()

    def test_status_action_renders_the_member_table(self):
        from charm import MicroCloudCharm
        from microcloud import Member

        stub = MagicMock(spec=MicroCloudCharm)
        event = MagicMock()
        with (
            patch("charm.microcloud.is_initialized", return_value=True),
            patch("charm.lxd_cluster.members", return_value=[_member("node1", "zone-1")]),
            patch("charm.microcloud.list_members", return_value=[Member("node1", "10.0.0.1")]),
        ):
            MicroCloudCharm._on_status_action(stub, event)

        results = event.set_results.call_args.args[0]
        assert results["members"].splitlines()[1].split() == ["node1", "-", "zone-1", "x86_64"]
        assert json.loads(results["microcloud-members"])[0]["name"] == "node1"
