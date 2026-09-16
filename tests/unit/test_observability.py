# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for metrics endpoints, scrape jobs and LXD log streaming."""

import json
import subprocess
from unittest.mock import MagicMock, call, patch

import pytest

from observability import (
    _LXD_METRICS_ADDRESS,
    LXDConfigError,
    Observability,
    _lxc_config_set,
    _lxd_has_api_extension,
)

_CONFIG = {
    "scrape-interval": "30s",
    "ceph-mgr-prometheus-port": 9283,
    "ovn-exporter-listen-port": 9310,
}

# ---------------------------------------------------------------------------
# _lxc_config_set helper
# ---------------------------------------------------------------------------


class TestLxcConfigSet:
    def test_calls_lxc_config_set(self):
        with patch("observability.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            _lxc_config_set("core.metrics_address", "127.0.0.1:8444")
            mock_run.assert_called_once_with(
                ["lxc", "config", "set", "core.metrics_address", "127.0.0.1:8444"],
                capture_output=True,
                text=True,
                check=True,
            )

    def test_raises_on_failure(self):
        with patch(
            "observability.subprocess.run",
            side_effect=subprocess.CalledProcessError(1, "lxc", stderr="permission denied"),
        ):
            with pytest.raises(LXDConfigError, match="Cannot set LXD config"):
                _lxc_config_set("core.metrics_address", "127.0.0.1:8444")


# ---------------------------------------------------------------------------
# log_slots
# ---------------------------------------------------------------------------


class TestLogSlots:
    def test_includes_microceph_slot_when_installed(self):
        with patch(
            "observability.snap.is_installed", side_effect=lambda name: name == "microceph"
        ):
            assert Observability({}, "microcloud").log_slots() == ["microceph:ceph-logs"]

    def test_omits_microceph_slot_when_not_installed(self):
        with patch("observability.snap.is_installed", return_value=False):
            assert Observability({}, "microcloud").log_slots() == []


# ---------------------------------------------------------------------------
# _ensure_lxd_metrics_config
# ---------------------------------------------------------------------------


class TestEnsureLxdMetricsConfig:
    def test_sets_metrics_address_and_disables_auth(self):
        with patch("observability._lxc_config_set") as mock_set:
            Observability({}, "microcloud")._ensure_lxd_metrics_config()
            assert mock_set.call_args_list == [
                call("core.metrics_address", _LXD_METRICS_ADDRESS),
                call("core.metrics_authentication", "false"),
            ]

    def test_propagates_lxd_config_error(self):
        with patch("observability._lxc_config_set", side_effect=LXDConfigError("lxd not running")):
            with pytest.raises(LXDConfigError, match="lxd not running"):
                Observability({}, "microcloud")._ensure_lxd_metrics_config()


# ---------------------------------------------------------------------------
# ensure_loki / teardown_loki
# ---------------------------------------------------------------------------


class TestLxdLokiConfig:
    def test_does_nothing_when_no_endpoints_published_yet(self):
        with patch("observability._lxc_config_set") as mock_set:
            Observability({}, "microcloud").ensure_loki([])
            mock_set.assert_not_called()

    def test_sets_loki_api_url_stripping_push_suffix(self):
        with (
            patch("observability._lxd_has_api_extension", return_value=True),
            patch("observability._lxc_config_set") as mock_set,
        ):
            Observability({}, "microcloud").ensure_loki(
                [{"url": "http://otelcol:3500/loki/api/v1/push"}]
            )
            mock_set.assert_called_once_with("loki.api.url", "http://otelcol:3500")

    def test_skips_when_loki_api_extension_missing(self):
        with (
            patch("observability._lxd_has_api_extension", return_value=False),
            patch("observability._lxc_config_set") as mock_set,
        ):
            Observability({}, "microcloud").ensure_loki(
                [{"url": "http://otelcol:3500/loki/api/v1/push"}]
            )
            mock_set.assert_not_called()

    def test_swallows_lxd_config_error(self):
        with (
            patch("observability._lxd_has_api_extension", return_value=True),
            patch("observability._lxc_config_set", side_effect=LXDConfigError("lxd not running")),
        ):
            # Should not raise.
            Observability({}, "microcloud").ensure_loki([{"url": "http://otelcol:3500"}])

    def test_ignores_endpoint_missing_url(self):
        with (
            patch("observability._lxd_has_api_extension", return_value=True),
            patch("observability._lxc_config_set") as mock_set,
        ):
            Observability({}, "microcloud").ensure_loki([{}])
            mock_set.assert_not_called()

    def test_teardown_clears_loki_api_url(self):
        with patch("observability._lxc_config_set") as mock_set:
            Observability({}, "microcloud").teardown_loki()
            mock_set.assert_called_once_with("loki.api.url", "")

    def test_teardown_swallows_lxd_config_error(self):
        with patch("observability._lxc_config_set", side_effect=LXDConfigError("lxd not running")):
            # Should not raise.
            Observability({}, "microcloud").teardown_loki()


# ---------------------------------------------------------------------------
# _lxd_has_api_extension
# ---------------------------------------------------------------------------


class TestLxdHasApiExtension:
    def test_true_when_extension_present(self):
        result = MagicMock(stdout=json.dumps({"api_extensions": ["loki", "other"]}))
        with patch("observability.subprocess.run", return_value=result):
            assert _lxd_has_api_extension("loki") is True

    def test_false_when_extension_absent(self):
        result = MagicMock(stdout=json.dumps({"api_extensions": ["other"]}))
        with patch("observability.subprocess.run", return_value=result):
            assert _lxd_has_api_extension("loki") is False

    def test_false_on_subprocess_error(self):
        with patch(
            "observability.subprocess.run",
            side_effect=subprocess.CalledProcessError(1, "lxc"),
        ):
            assert _lxd_has_api_extension("loki") is False

    def test_false_on_invalid_json(self):
        result = MagicMock(stdout="not json")
        with patch("observability.subprocess.run", return_value=result):
            assert _lxd_has_api_extension("loki") is False


# ---------------------------------------------------------------------------
# Scrape config generation
# ---------------------------------------------------------------------------


class TestScrapeConfigs:
    """Test the scrape config generation logic."""

    def _observability(self, config: dict):
        return Observability(config, "microcloud")

    def test_all_services_enabled(self):
        observability = self._observability(_CONFIG)

        with (
            patch.object(observability._ceph, "is_mgr_active", return_value=True),
            patch("observability.snap.is_installed", return_value=True),
            patch("microcloud.socket.gethostname", return_value="node1"),
            patch("observability.subprocess.run", return_value=MagicMock(returncode=1, stdout="")),
        ):
            configs = observability.scrape_configs()

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
        observability = self._observability(
            {"scrape-interval": "30s", "ceph-mgr-prometheus-port": 9283}
        )

        with (
            patch.object(observability._ceph, "is_mgr_active", return_value=True),
            patch("observability.snap.is_installed", side_effect=lambda name: name == "microceph"),
            patch("microcloud.socket.gethostname", return_value="node1"),
            patch("observability.subprocess.run", return_value=MagicMock(returncode=1, stdout="")),
        ):
            configs = observability.scrape_configs()

        ceph_job = next(c for c in configs if c["job_name"] == "microcloud-microceph")
        assert ceph_job["honor_labels"] is True

    def test_microceph_job_relabels_fallback_instance_to_member(self):
        """Metrics lacking their own "instance" label fall back to the scrape

        target (127.0.0.1:<port>), which is meaningless and identical across
        units; it must be rewritten to this unit's member name.
        """
        observability = self._observability(
            {"scrape-interval": "30s", "ceph-mgr-prometheus-port": 9283}
        )

        with (
            patch.object(observability._ceph, "is_mgr_active", return_value=True),
            patch("observability.snap.is_installed", side_effect=lambda name: name == "microceph"),
            patch("microcloud.socket.gethostname", return_value="node1"),
            patch("observability.subprocess.run", return_value=MagicMock(returncode=1, stdout="")),
        ):
            configs = observability.scrape_configs()
            expected_member = observability._member_label()

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
        observability = self._observability(_CONFIG)

        with (
            patch("observability.snap.is_installed", return_value=False),
            patch("observability.subprocess.run", return_value=MagicMock(returncode=1, stdout="")),
        ):
            configs = observability.scrape_configs()

        lxd_job = next(c for c in configs if c["job_name"] == "microcloud-lxd")
        assert lxd_job["scheme"] == "https", "LXD scrape job must use https"
        assert lxd_job["static_configs"][0]["targets"] == [_LXD_METRICS_ADDRESS]

        tls = lxd_job.get("tls_config", {})
        assert tls, "LXD scrape job must have tls_config"
        assert "cert_file" not in tls, "No client cert required — metrics_authentication=false"
        assert "key_file" not in tls, "No client key required — metrics_authentication=false"

    def test_lxd_always_enabled_when_no_other_snaps(self):
        observability = self._observability(_CONFIG)

        with (
            patch("observability.snap.is_installed", return_value=False),
            patch("observability.subprocess.run", return_value=MagicMock(returncode=1, stdout="")),
        ):
            configs = observability.scrape_configs()

        job_names = [c["job_name"] for c in configs]
        assert job_names == ["microcloud-lxd"]

    def test_cluster_label_falls_back_to_app_name(self):
        assert self._observability(_CONFIG)._cluster_label() == "microcloud"
