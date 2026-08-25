# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for microcloud charm."""

import json

# Stub out the cos_agent library so we don't need the full charm SDK installed
# during unit tests — the library is tested separately.
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, call, patch

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

# Now import the modules under test.
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from ceph_mgr import CephMgrPrometheus
from ovn_exporter import OVNExporter

# ---------------------------------------------------------------------------
# _lxc_config_set helper
# ---------------------------------------------------------------------------


class TestLxcConfigSet:
    def test_calls_lxc_config_set(self):
        from charm import _lxc_config_set

        with patch("charm.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            _lxc_config_set("core.metrics_address", "127.0.0.1:8444")
            mock_run.assert_called_once_with(
                ["lxc", "config", "set", "core.metrics_address", "127.0.0.1:8444"],
                capture_output=True,
                text=True,
                check=True,
            )

    def test_raises_on_failure(self):
        import subprocess

        from charm import LXDConfigError, _lxc_config_set

        with patch(
            "charm.subprocess.run",
            side_effect=subprocess.CalledProcessError(1, "lxc", stderr="permission denied"),
        ):
            with pytest.raises(LXDConfigError, match="Cannot set LXD config"):
                _lxc_config_set("core.metrics_address", "127.0.0.1:8444")


# ---------------------------------------------------------------------------
# _ensure_lxd_metrics_config
# ---------------------------------------------------------------------------


class TestEnsureLxdMetricsConfig:
    def _make_charm_stub(self):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        return stub

    def test_sets_metrics_address_and_disables_auth(self):
        from charm import _LXD_METRICS_ADDRESS, MicroCloudCharm

        stub = self._make_charm_stub()
        with patch("charm._lxc_config_set") as mock_set:
            MicroCloudCharm._ensure_lxd_metrics_config(stub)
            assert mock_set.call_args_list == [
                call("core.metrics_address", _LXD_METRICS_ADDRESS),
                call("core.metrics_authentication", "false"),
            ]

    def test_propagates_lxd_config_error(self):
        from charm import LXDConfigError, MicroCloudCharm

        stub = self._make_charm_stub()
        with patch("charm._lxc_config_set", side_effect=LXDConfigError("lxd not running")):
            with pytest.raises(LXDConfigError, match="lxd not running"):
                MicroCloudCharm._ensure_lxd_metrics_config(stub)


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
# Scrape config generation (via charm._build_scrape_configs)
# ---------------------------------------------------------------------------


class TestScrapeConfigs:
    """Test the scrape config generation logic without a full Harness."""

    def _make_charm_stub(self, config: dict, unit_name: str = "microcloud/0"):
        """Create a minimal stub that exercises _build_scrape_configs."""
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub._ceph = None
        stub._ovn = None
        stub.config = config
        stub.app.name = "microcloud"
        stub.unit.name = unit_name
        stub._build_scrape_configs = lambda: MicroCloudCharm._build_scrape_configs(stub)
        stub._cluster_label = lambda: MicroCloudCharm._cluster_label(stub)
        stub._member_label = lambda: MicroCloudCharm._member_label(stub)
        return stub

    def test_all_services_enabled(self, tmp_path):
        stub = self._make_charm_stub(
            {
                "scrape-interval": "30s",
                "ceph-mgr-prometheus-port": 9283,
                "ovn-exporter-listen-port": 9310,
            }
        )

        stub._ceph = CephMgrPrometheus(port=9283)
        stub._ovn = OVNExporter()

        with (
            patch.object(stub._ceph, "is_mgr_active", return_value=True),
            patch("charm.snap.is_installed", return_value=True),
            patch("microcloud.socket.gethostname", return_value="node1"),
            patch("charm.subprocess.run", return_value=MagicMock(returncode=1, stdout="")),
        ):
            from charm import MicroCloudCharm

            configs = MicroCloudCharm._build_scrape_configs(stub)

        job_names = [c["job_name"] for c in configs]
        assert "microcloud-lxd" in job_names
        assert "microcloud-microceph" in job_names
        assert "microcloud-microovn" in job_names

    def test_microceph_job_honors_intrinsic_instance_labels(self):
        """Ceph mgr's own per-host "instance" labels (e.g. ceph_disk_occupation)

        must survive scraping untouched, otherwise Prometheus renames them to
        "exported_instance" and overwrites "instance" with the scrape target
        address, which is identical on every unit and collapses per-host
        panels/variables in the bundled dashboards.
        """
        stub = self._make_charm_stub(
            {
                "scrape-interval": "30s",
                "ceph-mgr-prometheus-port": 9283,
            }
        )
        stub._ceph = CephMgrPrometheus(port=9283)
        stub._ovn = OVNExporter()

        with (
            patch.object(stub._ceph, "is_mgr_active", return_value=True),
            patch("charm.snap.is_installed", side_effect=lambda name: name == "microceph"),
            patch("microcloud.socket.gethostname", return_value="node1"),
            patch("charm.subprocess.run", return_value=MagicMock(returncode=1, stdout="")),
        ):
            from charm import MicroCloudCharm

            configs = MicroCloudCharm._build_scrape_configs(stub)

        ceph_job = next(c for c in configs if c["job_name"] == "microcloud-microceph")
        assert ceph_job["honor_labels"] is True

    def test_microceph_job_relabels_fallback_instance_to_member(self):
        """Metrics lacking their own "instance" label fall back to the scrape

        target (127.0.0.1:<port>), which is meaningless and identical across
        units; it must be rewritten to this unit's member name.
        """
        stub = self._make_charm_stub(
            {
                "scrape-interval": "30s",
                "ceph-mgr-prometheus-port": 9283,
            }
        )
        stub._ceph = CephMgrPrometheus(port=9283)
        stub._ovn = OVNExporter()

        with (
            patch.object(stub._ceph, "is_mgr_active", return_value=True),
            patch("charm.snap.is_installed", side_effect=lambda name: name == "microceph"),
            patch("microcloud.socket.gethostname", return_value="node1"),
            patch("charm.subprocess.run", return_value=MagicMock(returncode=1, stdout="")),
        ):
            from charm import MicroCloudCharm

            configs = MicroCloudCharm._build_scrape_configs(stub)
            expected_member = stub._member_label()

        ceph_job = next(c for c in configs if c["job_name"] == "microcloud-microceph")
        relabel = ceph_job["metric_relabel_configs"][0]
        assert relabel["source_labels"] == ["instance"]
        assert relabel["regex"] == "127\\.0\\.0\\.1:9283"
        assert relabel["replacement"] == expected_member

    def test_lxd_job_uses_https_with_ca_file(self):
        """LXD scrape job must use https scheme and trust the cluster cert via ca_file.

        core.metrics_authentication=false removes the need for a client cert,
        but the metrics endpoint always speaks TLS — the scrape job must use
        https and supply ca_file so the collector can verify the self-signed
        LXD cluster certificate.
        """
        from charm import _LXD_METRICS_ADDRESS, MicroCloudCharm

        stub = self._make_charm_stub(
            {
                "scrape-interval": "30s",
                "ceph-mgr-prometheus-port": 9283,
                "ovn-exporter-listen-port": 9310,
            }
        )
        stub._ceph = CephMgrPrometheus()
        stub._ovn = OVNExporter()

        with (
            patch("charm.snap.is_installed", return_value=False),
            patch("charm.subprocess.run", return_value=MagicMock(returncode=1, stdout="")),
        ):
            configs = MicroCloudCharm._build_scrape_configs(stub)

        lxd_job = next(c for c in configs if c["job_name"] == "microcloud-lxd")
        assert lxd_job["scheme"] == "https", "LXD scrape job must use https"
        assert lxd_job["static_configs"][0]["targets"] == [_LXD_METRICS_ADDRESS]

        tls = lxd_job.get("tls_config", {})
        assert tls, "LXD scrape job must have tls_config"
        assert "cert_file" not in tls, "No client cert required — metrics_authentication=false"
        assert "key_file" not in tls, "No client key required — metrics_authentication=false"

    def test_lxd_always_enabled_when_no_other_snaps(self):
        stub = self._make_charm_stub(
            {
                "scrape-interval": "30s",
                "ceph-mgr-prometheus-port": 9283,
                "ovn-exporter-listen-port": 9310,
            }
        )
        stub._ceph = CephMgrPrometheus()
        stub._ovn = OVNExporter()

        with (
            patch("charm.snap.is_installed", return_value=False),
            patch("charm.subprocess.run", return_value=MagicMock(returncode=1, stdout="")),
        ):
            from charm import MicroCloudCharm

            configs = MicroCloudCharm._build_scrape_configs(stub)

        job_names = [c["job_name"] for c in configs]
        assert job_names == ["microcloud-lxd"]

    def test_cluster_label_falls_back_to_app_name(self):
        stub = self._make_charm_stub(
            {
                "scrape-interval": "30s",
                "ceph-mgr-prometheus-port": 9283,
                "ovn-exporter-listen-port": 9310,
            }
        )
        stub._ceph = CephMgrPrometheus()
        stub._ovn = OVNExporter()

        from charm import MicroCloudCharm

        assert MicroCloudCharm._cluster_label(stub) == "microcloud"


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
# _binding_network (shared get_binding + .network access, both error-guarded)
# ---------------------------------------------------------------------------


class TestBindingNetwork:
    def test_returns_network_from_binding(self):
        from charm import MicroCloudCharm

        network = MagicMock()
        binding = MagicMock()
        binding.network = network

        stub = MagicMock(spec=MicroCloudCharm)
        stub.model.get_binding.return_value = binding

        assert MicroCloudCharm._binding_network(stub, "ovn-uplink") is network
        stub.model.get_binding.assert_called_once_with("ovn-uplink")

    def test_returns_none_when_get_binding_raises(self):
        import ops

        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub.model.get_binding.side_effect = ops.ModelError("no such binding")

        assert MicroCloudCharm._binding_network(stub, "ovn-uplink") is None

    def test_returns_none_when_network_access_raises(self):
        """Regression test: juju raises "no network config found for binding"
        lazily, when accessing binding.network -- not when calling
        get_binding() itself -- so both must be guarded by the same
        try/except or an unbound extra-binding crashes the install hook."""
        import ops

        from charm import MicroCloudCharm

        binding = MagicMock()
        type(binding).network = property(
            lambda self: (_ for _ in ()).throw(
                ops.ModelError('no network config found for binding "ovn-uplink"')
            )
        )

        stub = MagicMock(spec=MicroCloudCharm)
        stub.model.get_binding.return_value = binding

        assert MicroCloudCharm._binding_network(stub, "ovn-uplink") is None

    def test_returns_none_when_binding_falsy(self):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub.model.get_binding.return_value = None

        assert MicroCloudCharm._binding_network(stub, "ovn-uplink") is None


# ---------------------------------------------------------------------------
# _space_network_cidr (Juju space binding -> Ceph public/internal network)
# ---------------------------------------------------------------------------


class TestSpaceNetworkCidr:
    def test_returns_cidr_from_bound_subnet(self):
        import ipaddress

        from charm import MicroCloudCharm

        interface = MagicMock()
        interface.subnet = ipaddress.ip_network("10.42.0.0/24")
        network = MagicMock()
        network.interfaces = [interface]

        stub = MagicMock(spec=MicroCloudCharm)
        stub._binding_network.return_value = network

        assert MicroCloudCharm._space_network_cidr(stub, "ceph-public") == "10.42.0.0/24"
        stub._binding_network.assert_called_once_with("ceph-public")

    def test_returns_empty_when_no_interfaces(self):
        from charm import MicroCloudCharm

        network = MagicMock()
        network.interfaces = []

        stub = MagicMock(spec=MicroCloudCharm)
        stub._binding_network.return_value = network

        assert MicroCloudCharm._space_network_cidr(stub, "ceph-internal") == ""

    def test_returns_empty_when_network_unavailable(self):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub._binding_network.return_value = None

        assert MicroCloudCharm._space_network_cidr(stub, "ceph-public") == ""


# ---------------------------------------------------------------------------
# _ovn_uplink_interface (config-derived, per-unit OVN uplink interface name)
# ---------------------------------------------------------------------------


class TestOvnUplinkInterface:
    def test_empty_config_omits_uplink(self):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub.config = {"ovn-uplink-interface": ""}

        interface, problem = MicroCloudCharm._ovn_uplink_interface(stub)
        assert interface == ""
        assert problem is None

    def test_plain_string_applies_to_every_unit(self):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub.config = {"ovn-uplink-interface": "eth1"}

        interface, problem = MicroCloudCharm._ovn_uplink_interface(stub)
        assert interface == "eth1"
        assert problem is None

    def test_mapping_resolves_own_hostname(self):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub.config = {"ovn-uplink-interface": "node1: eth1\nnode2: enp5s0\n"}

        with patch("charm.microcloud.hostname", return_value="node2"):
            interface, problem = MicroCloudCharm._ovn_uplink_interface(stub)
        assert interface == "enp5s0"
        assert problem is None

    def test_mapping_missing_hostname_blocks(self):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub.config = {"ovn-uplink-interface": '{"node1": "eth1", "node2": "enp5s0"}'}

        with patch("charm.microcloud.hostname", return_value="node3"):
            interface, problem = MicroCloudCharm._ovn_uplink_interface(stub)
        assert interface == ""
        assert problem is not None
        assert "node3" in problem
        assert "node1" in problem
        assert "node2" in problem

    def test_invalid_yaml_blocks(self):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub.config = {"ovn-uplink-interface": "{unbalanced: ["}

        interface, problem = MicroCloudCharm._ovn_uplink_interface(stub)
        assert interface == ""
        assert problem is not None


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
# _space_bind_address (Juju space binding -> per-unit address, e.g. OVN underlay)
# ---------------------------------------------------------------------------


class TestSpaceBindAddress:
    def test_returns_address_from_binding(self):
        from charm import MicroCloudCharm

        network = MagicMock()
        network.bind_address = "10.42.0.5"

        stub = MagicMock(spec=MicroCloudCharm)
        stub._binding_network.return_value = network

        assert MicroCloudCharm._space_bind_address(stub, "ovn-underlay") == "10.42.0.5"
        stub._binding_network.assert_called_once_with("ovn-underlay")

    def test_returns_empty_when_no_bind_address(self):
        from charm import MicroCloudCharm

        network = MagicMock()
        network.bind_address = None

        stub = MagicMock(spec=MicroCloudCharm)
        stub._binding_network.return_value = network

        assert MicroCloudCharm._space_bind_address(stub, "ovn-underlay") == ""

    def test_returns_empty_when_network_unavailable(self):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub._binding_network.return_value = None

        assert MicroCloudCharm._space_bind_address(stub, "ovn-underlay") == ""

    def test_bind_address_delegates_to_cluster_binding(self):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub._space_bind_address.return_value = "10.0.0.1"

        assert MicroCloudCharm._bind_address(stub) == "10.0.0.1"
        stub._space_bind_address.assert_called_once_with("cluster")


# ---------------------------------------------------------------------------
# _preseed_inputs (ceph public/internal network only derived with Ceph disks)
# ---------------------------------------------------------------------------


class TestPreseedInputs:
    def _stub(self, config: dict, cidrs: dict):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub.config = config
        stub._space_network_cidr.side_effect = lambda name: cidrs.get(name, "")
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
        stub._space_network_cidr.assert_not_called()

    def test_ceph_networks_derived_with_ceph_storage_disks(self):
        from charm import MicroCloudCharm
        from preseed import SystemEntry

        stub = self._stub(
            config={"snap-channel-microceph": "squid/stable"},
            cidrs={"ceph-public": "10.42.0.0/24", "ceph-internal": "10.42.1.0/24"},
        )

        systems = [
            SystemEntry(
                name="node1", address="10.0.0.1", storage_ceph_paths=["/dev/nvme1n1"]
            )
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
        )
        harness.begin()
        return harness

    def test_local_path_empty_when_unattached(self):
        from charm import MicroCloudCharm

        harness = self._harness()
        try:
            assert MicroCloudCharm._storage_local_path(harness.charm) == ""
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
            assert MicroCloudCharm._storage_local_path(harness.charm) == str(location)
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
