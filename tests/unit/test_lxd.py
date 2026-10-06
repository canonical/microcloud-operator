"""Unit tests for lxd.py (LXDManager)."""

from unittest.mock import MagicMock, patch

import pylxd
import pytest

from lxd import LXDConfigError, LXDManager


def _mock_client(config=None):
    """Build a MagicMock standing in for a pylxd.Client with a given server config."""
    client = MagicMock()
    client.api.get.return_value.json.return_value = {"metadata": {"config": dict(config or {})}}
    return client


def _api_exception():
    """Build a pylxd.exceptions.LXDAPIException usable as a side_effect."""
    response = MagicMock()
    response.status_code = 400
    response.json.return_value = {"error": "boom"}
    return pylxd.exceptions.LXDAPIException(response)


def test_ensure_metrics_config_calls_lxd_api():
    manager = LXDManager(port=8444)
    client = _mock_client()
    with patch("lxd.pylxd.Client", return_value=client):
        manager.ensure_metrics_config()

    client.api.put.assert_called_once_with(
        json={
            "config": {
                "core.metrics_address": "127.0.0.1:8444",
                "core.metrics_authentication": "false",
            }
        }
    )


def test_ensure_metrics_config_raises_on_failure():
    manager = LXDManager(port=8444)
    client = _mock_client()
    client.api.put.side_effect = _api_exception()
    with patch("lxd.pylxd.Client", return_value=client):
        with pytest.raises(LXDConfigError):
            manager.ensure_metrics_config()


def test_teardown_metrics_config_calls_lxd_api():
    manager = LXDManager(port=8444)
    client = _mock_client(
        {
            "core.metrics_address": "127.0.0.1:8444",
            "core.metrics_authentication": "false",
        }
    )
    with patch("lxd.pylxd.Client", return_value=client):
        manager.teardown_metrics_config()

    client.api.put.assert_called_once_with(json={"config": {}})


def test_teardown_metrics_config_swallows_errors():
    manager = LXDManager(port=8444)
    client = _mock_client({"core.metrics_address": "127.0.0.1:8444"})
    client.api.put.side_effect = _api_exception()
    with patch("lxd.pylxd.Client", return_value=client):
        # Should not raise.
        manager.teardown_metrics_config()


def test_ensure_loki_config_sets_url():
    manager = LXDManager()
    endpoints = [{"url": "http://loki:3100/loki/api/v1/push"}]
    client = _mock_client()
    with patch("lxd.pylxd.Client", return_value=client):
        manager.ensure_loki_config(endpoints)

    client.api.put.assert_called_once_with(
        json={
            "config": {
                "loki.api.url": "http://127.0.0.1:3100",
                "loki.api.check_ready": "false",
            }
        }
    )


def test_ensure_loki_config_no_endpoints_is_noop():
    manager = LXDManager()
    with patch("lxd.pylxd.Client") as client_cls:
        manager.ensure_loki_config([])
    client_cls.assert_not_called()


def test_teardown_loki_config_calls_lxd_api():
    manager = LXDManager()
    client = _mock_client(
        {
            "loki.api.url": "http://127.0.0.1:3100",
            "loki.api.check_ready": "false",
        }
    )
    with patch("lxd.pylxd.Client", return_value=client):
        manager.teardown_loki_config()

    client.api.put.assert_called_once_with(json={"config": {}})
