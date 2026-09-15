# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for microcloud charm."""

import json

# Stub out the cos_agent library so we don't need the full charm SDK installed
# during unit tests — the library is tested separately.
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
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
# _log_slots
# ---------------------------------------------------------------------------


class TestLogSlots:
    def _make_charm_stub(self):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        return stub

    def test_includes_microceph_slot_when_installed(self):
        from charm import MicroCloudCharm

        stub = self._make_charm_stub()
        with patch("charm.snap.is_installed", side_effect=lambda name: name == "microceph"):
            assert MicroCloudCharm._log_slots(stub) == ["microceph:ceph-logs"]

    def test_omits_microceph_slot_when_not_installed(self):
        from charm import MicroCloudCharm

        stub = self._make_charm_stub()
        with patch("charm.snap.is_installed", return_value=False):
            assert MicroCloudCharm._log_slots(stub) == []


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
# _ensure_lxd_loki_config / _teardown_lxd_loki_config
# ---------------------------------------------------------------------------


class TestLxdLokiConfig:
    def _make_charm_stub(self, loki_endpoints=None):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub._loki_consumer = MagicMock()
        stub._loki_consumer.loki_endpoints = loki_endpoints or []
        return stub

    def test_does_nothing_when_no_endpoints_published_yet(self):
        from charm import MicroCloudCharm

        stub = self._make_charm_stub(loki_endpoints=[])
        with patch("charm._lxc_config_set") as mock_set:
            MicroCloudCharm._ensure_lxd_loki_config(stub)
            mock_set.assert_not_called()

    def test_sets_loki_api_url_stripping_push_suffix(self):
        from charm import MicroCloudCharm

        stub = self._make_charm_stub(
            loki_endpoints=[{"url": "http://otelcol:3500/loki/api/v1/push"}]
        )
        with (
            patch("charm._lxd_has_api_extension", return_value=True),
            patch("charm._lxc_config_set") as mock_set,
        ):
            MicroCloudCharm._ensure_lxd_loki_config(stub)
            mock_set.assert_called_once_with("loki.api.url", "http://otelcol:3500")

    def test_skips_when_loki_api_extension_missing(self):
        from charm import MicroCloudCharm

        stub = self._make_charm_stub(
            loki_endpoints=[{"url": "http://otelcol:3500/loki/api/v1/push"}]
        )
        with (
            patch("charm._lxd_has_api_extension", return_value=False),
            patch("charm._lxc_config_set") as mock_set,
        ):
            MicroCloudCharm._ensure_lxd_loki_config(stub)
            mock_set.assert_not_called()

    def test_swallows_lxd_config_error(self):
        from charm import LXDConfigError, MicroCloudCharm

        stub = self._make_charm_stub(loki_endpoints=[{"url": "http://otelcol:3500"}])
        with (
            patch("charm._lxd_has_api_extension", return_value=True),
            patch("charm._lxc_config_set", side_effect=LXDConfigError("lxd not running")),
        ):
            # Should not raise.
            MicroCloudCharm._ensure_lxd_loki_config(stub)

    def test_ignores_endpoint_missing_url(self):
        from charm import MicroCloudCharm

        stub = self._make_charm_stub(loki_endpoints=[{}])
        with (
            patch("charm._lxd_has_api_extension", return_value=True),
            patch("charm._lxc_config_set") as mock_set,
        ):
            MicroCloudCharm._ensure_lxd_loki_config(stub)
            mock_set.assert_not_called()

    def test_teardown_clears_loki_api_url(self):
        from charm import MicroCloudCharm

        stub = self._make_charm_stub()
        with patch("charm._lxc_config_set") as mock_set:
            MicroCloudCharm._teardown_lxd_loki_config(stub)
            mock_set.assert_called_once_with("loki.api.url", "")

    def test_teardown_swallows_lxd_config_error(self):
        from charm import LXDConfigError, MicroCloudCharm

        stub = self._make_charm_stub()
        with patch("charm._lxc_config_set", side_effect=LXDConfigError("lxd not running")):
            # Should not raise.
            MicroCloudCharm._teardown_lxd_loki_config(stub)


# ---------------------------------------------------------------------------
# logging (Loki) relation handlers
# ---------------------------------------------------------------------------


class TestLokiRelationHandlers:
    def _make_charm_stub(self):
        from charm import MicroCloudCharm

        return MagicMock(spec=MicroCloudCharm)

    def test_joined_calls_ensure_lxd_loki_config(self):
        from charm import MicroCloudCharm

        stub = self._make_charm_stub()
        MicroCloudCharm._on_loki_push_api_endpoint_joined(stub, MagicMock())
        stub._ensure_lxd_loki_config.assert_called_once()

    def test_departed_calls_teardown_lxd_loki_config(self):
        from charm import MicroCloudCharm

        stub = self._make_charm_stub()
        MicroCloudCharm._on_loki_push_api_endpoint_departed(stub, MagicMock())
        stub._teardown_lxd_loki_config.assert_called_once()


# ---------------------------------------------------------------------------
# _lxd_has_api_extension
# ---------------------------------------------------------------------------


class TestLxdHasApiExtension:
    def test_true_when_extension_present(self):
        from charm import _lxd_has_api_extension

        result = MagicMock(stdout=json.dumps({"api_extensions": ["loki", "other"]}))
        with patch("charm.subprocess.run", return_value=result):
            assert _lxd_has_api_extension("loki") is True

    def test_false_when_extension_absent(self):
        from charm import _lxd_has_api_extension

        result = MagicMock(stdout=json.dumps({"api_extensions": ["other"]}))
        with patch("charm.subprocess.run", return_value=result):
            assert _lxd_has_api_extension("loki") is False

    def test_false_on_subprocess_error(self):
        import subprocess

        from charm import _lxd_has_api_extension

        with patch(
            "charm.subprocess.run",
            side_effect=subprocess.CalledProcessError(1, "lxc"),
        ):
            assert _lxd_has_api_extension("loki") is False

    def test_false_on_invalid_json(self):
        from charm import _lxd_has_api_extension

        result = MagicMock(stdout="not json")
        with patch("charm.subprocess.run", return_value=result):
            assert _lxd_has_api_extension("loki") is False


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


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------


class TestHeldStatus:
    def test_held_status_is_not_overwritten(self):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub._status_held = False
        stub._ovn_uplink_interface.return_value = ("", None)
        stub._cos_related.return_value = False
        stub._coordinator = MagicMock()

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
        stub._ovn_uplink_interface.return_value = ("", None)
        stub._cos_related.return_value = False
        stub._coordinator = MagicMock()
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
        stub._worker_running.return_value = False
        stub._bind_address.return_value = "10.0.0.1"
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

        stub._pending_systems.return_value = pending if pending_after is None else pending_after
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
        stub._pending_systems.assert_called_once_with(True)
        stub._coordinator.publish_session.assert_called_once_with(None)
        start.assert_not_called()

    def test_unreadable_membership_after_a_session_blocks(self):
        import microcloud

        stub = self._stub(leader=True, current=self._current())
        stub._pending_systems.side_effect = microcloud.MicroCloudError("boom")

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
        stub._bind_address.return_value = "10.0.0.2"
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

    def _deploy(self, stub, *, initialized, pending):
        from charm import MicroCloudCharm

        coordinator = stub._coordinator
        coordinator.all_identities_published.return_value = True
        coordinator.ensure_passphrase.return_value = "secret"
        coordinator.all_members.return_value = [("node0", "10.0.0.1"), ("node1", "10.0.0.2")]
        stub._lead_session.return_value = None
        stub._join_session.return_value = None
        stub._zone_missing.return_value = False

        with (
            patch("charm.snap.ensure_snaps") as ensure_snaps,
            patch("charm.microcloud.waitready", return_value=True),
        ):
            problem = MicroCloudCharm._reconcile_deploy(stub, initialized, pending)
        return problem, ensure_snaps

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

        stub._ovn_uplink_interface.return_value = ("", None)
        stub._cos_related.return_value = False
        stub._reconcile_observe_only.return_value = None
        stub._reconcile_deploy.return_value = None
        stub._pending_systems.side_effect = lambda initialized: MicroCloudCharm._pending_systems(
            stub, initialized
        )

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
        stub._ovn_uplink_interface.return_value = ("", None)
        stub._pending_systems.side_effect = lambda initialized: MicroCloudCharm._pending_systems(
            stub, initialized
        )

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


class TestSessionWorkerHelpers:
    """The charm's bookkeeping for its one join session worker."""

    def _stub(self, pid=0, session_id=""):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub.unit.name = "microcloud/0"
        stub.charm_dir = Path("/charm")
        stub._stored = SimpleNamespace(worker_pid=pid, worker_session=session_id)
        return stub

    def test_worker_for_another_session_is_not_running(self):
        from charm import MicroCloudCharm

        stub = self._stub(pid=42, session_id="old")
        with patch("charm.session.is_running", return_value=True):
            assert MicroCloudCharm._worker_running(stub, "abc") is False
            assert MicroCloudCharm._worker_running(stub, "old") is True

    def test_start_worker_replaces_the_previous_one(self):
        from charm import MicroCloudCharm

        stub = self._stub(pid=42, session_id="old")
        with patch("charm.session.start", return_value=99) as start:
            MicroCloudCharm._start_worker(stub, "abc", "doc", retry_until=5.0)

        start.assert_called_once_with(
            "abc", "doc", "microcloud/0", Path("/charm"), retry_until=5.0, replacing=42
        )
        assert (stub._stored.worker_pid, stub._stored.worker_session) == (99, "abc")


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


class TestFailureDomains:
    """Carrying each unit's zone through to its LXD failure domain."""

    def _stub(self, *, require_zone=True, zone="zone-1"):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        stub.config = {"require-zone": require_zone}
        stub._zone.return_value = zone
        stub._zone_missing.side_effect = lambda: MicroCloudCharm._zone_missing(stub)
        return stub

    def test_zone_comes_from_the_hook_environment(self):
        from charm import MicroCloudCharm

        stub = MagicMock(spec=MicroCloudCharm)
        with patch.dict("os.environ", {"JUJU_AVAILABILITY_ZONE": " zone-1 "}):
            assert MicroCloudCharm._zone(stub) == "zone-1"
        with patch.dict("os.environ", {}, clear=True):
            assert MicroCloudCharm._zone(stub) == ""

    def test_zone_missing_honours_the_opt_out(self):
        from charm import MicroCloudCharm

        assert MicroCloudCharm._zone_missing(self._stub(zone="")) is True
        assert MicroCloudCharm._zone_missing(self._stub(zone="", require_zone=False)) is False
        assert MicroCloudCharm._zone_missing(self._stub()) is False

    def test_deploy_blocks_without_a_zone_before_installing_anything(self):
        from charm import MicroCloudCharm

        stub = self._stub(zone="")
        with (
            patch("charm.snap.ensure_snaps") as ensure_snaps,
            patch("charm.microcloud.hostname", return_value="node1"),
        ):
            problem = MicroCloudCharm._reconcile_deploy(stub, False, [])

        assert problem == "node1 has no zone; cannot set its failure domain"
        ensure_snaps.assert_not_called()

    def test_clustered_leader_without_a_zone_still_runs_join_sessions(self):
        """The zone only gates this unit joining; it is already a member."""
        from charm import MicroCloudCharm

        stub = self._stub(zone="")
        with (
            patch("charm.snap.ensure_snaps") as ensure_snaps,
            patch("charm.microcloud.waitready", return_value=False),
        ):
            problem = MicroCloudCharm._reconcile_deploy(stub, True, [])

        assert problem is None
        ensure_snaps.assert_not_called()
        stub._hold_status.assert_called_once()

    def _set(self, stub, members, set_side_effect=None):
        from charm import MicroCloudCharm

        with (
            patch("charm.microcloud.hostname", return_value="node1"),
            patch("charm.lxd_cluster.members", return_value=members),
            patch(
                "charm.lxd_cluster.set_failure_domain", side_effect=set_side_effect
            ) as set_failure_domain,
        ):
            problem = MicroCloudCharm._reconcile_failure_domain(stub)
        return problem, set_failure_domain

    def test_sets_the_failure_domain_to_the_zone(self):
        problem, set_failure_domain = self._set(self._stub(), [_member("node1", "default")])

        assert problem is None
        set_failure_domain.assert_called_once_with("node1", "zone-1")

    def test_leaves_a_matching_failure_domain_alone(self):
        problem, set_failure_domain = self._set(self._stub(), [_member("node1", "zone-1")])

        assert problem is None
        set_failure_domain.assert_not_called()

    @pytest.mark.parametrize("require_zone", [True, False])
    def test_leaves_the_failure_domain_alone_without_a_zone(self, require_zone):
        """A clustered unit without a zone joined outside Juju, or zones are opted out of."""
        problem, set_failure_domain = self._set(
            self._stub(zone="", require_zone=require_zone), [_member("node1", "rack-a")]
        )

        assert problem is None
        set_failure_domain.assert_not_called()

    def test_blocks_when_not_an_lxd_member(self):
        problem, _ = self._set(self._stub(), [_member("node2")])

        assert problem == "node1 is not an LXD cluster member"

    def test_blocks_when_lxd_refuses(self):
        import lxd_cluster

        problem, _ = self._set(
            self._stub(),
            [_member("node1", "default")],
            set_side_effect=lxd_cluster.LXDClusterError("denied"),
        )

        assert problem == "Cannot set the failure domain of node1 to 'zone-1': denied"

    def _reconcile(self, stub, *, initialized=True, lxd_clustered=True):
        from charm import MicroCloudCharm

        stub._status_held = False
        stub._ovn_uplink_interface.return_value = ("", None)
        stub._coordinator = MagicMock()
        stub._reconcile_deploy.return_value = None
        stub._reconcile_observe_only.return_value = None
        stub._reconcile_failure_domain.return_value = None
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

        assert stub._reconcile_failure_domain.called is initialized

    def test_waits_for_lxd_to_join_before_setting_the_failure_domain(self):
        """MicroCloud reports a joiner clustered before the initiator has added its LXD."""
        import ops

        stub = self._stub()

        self._reconcile(stub, lxd_clustered=False)

        stub._reconcile_failure_domain.assert_not_called()
        assert stub.unit.status == ops.WaitingStatus("Waiting for LXD to join the cluster")
        stub._set_status.assert_not_called()

    def test_unreadable_lxd_cluster_blocks(self):
        import ops

        import lxd_cluster
        from charm import MicroCloudCharm

        stub = self._stub()
        stub.unit.is_leader.return_value = False
        stub._status_held = False
        stub._ovn_uplink_interface.return_value = ("", None)
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
        stub._reconcile_failure_domain.assert_not_called()
