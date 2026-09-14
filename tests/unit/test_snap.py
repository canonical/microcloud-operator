"""Unit tests for the snap detection helper."""

from unittest.mock import patch

import snap


class _FakeSnapError(Exception):
    pass


def test_is_installed_true():
    with patch("snap._snap.list_one", return_value=object()):
        assert snap.is_installed("microceph") is True


def test_is_installed_not_installed():
    error = snap._snap.NotInstalledError("not installed", kind="not-installed", value="microceph")
    with patch("snap._snap.list_one", side_effect=error):
        assert snap.is_installed("microceph") is False


def test_is_installed_snapd_unreachable():
    with patch("snap._snap.list_one", side_effect=snap._snap.Error("boom")):
        assert snap.is_installed("microceph") is False
