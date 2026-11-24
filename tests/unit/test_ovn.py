"""Unit tests for OVNExporter.ensure_installed."""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

import ovn


def _exporter(channel="latest/edge"):
    return ovn.OVNExporter(channel)


def test_same_channel_no_reinstall():
    with (
        patch("ovn.is_installed", return_value=True),
        patch("ovn.snap.list_one", return_value=SimpleNamespace(tracking="latest/edge")),
        patch("ovn.snap.install") as install,
        patch("ovn.snap.connect"),
        patch.object(ovn.OVNExporter, "remove") as remove,
    ):
        _exporter("latest/edge").ensure_installed()
    remove.assert_not_called()
    install.assert_not_called()


def test_different_channel_reinstalls():
    with (
        patch("ovn.is_installed", return_value=True),
        patch("ovn.snap.list_one", return_value=SimpleNamespace(tracking="latest/edge")),
        patch("ovn.snap.install") as install,
        patch("ovn.snap.connect"),
        patch.object(ovn.OVNExporter, "remove") as remove,
    ):
        _exporter("latest/stable").ensure_installed()
    remove.assert_called_once()
    install.assert_called_once_with("ovn-exporter", "latest/stable")


def test_list_failure_raises():
    with (
        patch("ovn.is_installed", return_value=True),
        patch("ovn.snap.list_one", side_effect=ovn.snap.Error("boom")),
        pytest.raises(ovn.OVNExporterError),
    ):
        _exporter().ensure_installed()
