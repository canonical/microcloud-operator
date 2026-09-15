"""Unit tests for microcloud.py helpers (MicroCloud control-socket wrappers)."""

from unittest.mock import patch

import pytest

import microcloud
from unixsocket import UnixSocketError


def test_hostname():
    with patch("microcloud.socket.gethostname", return_value="node-1"):
        assert microcloud.hostname() == "node-1"


def test_is_initialized_snap_not_installed():
    with patch("snap.is_installed", return_value=False):
        assert microcloud.is_initialized() is False


def test_is_initialized_socket_unreachable():
    with (
        patch("snap.is_installed", return_value=True),
        patch("microcloud.request_json", side_effect=UnixSocketError("no socket")),
    ):
        assert microcloud.is_initialized() is False


def test_is_initialized_ready_true():
    with (
        patch("snap.is_installed", return_value=True),
        patch("microcloud.request_json", return_value={"metadata": {"ready": True}}),
    ):
        assert microcloud.is_initialized() is True


def test_is_initialized_ready_false():
    with (
        patch("snap.is_installed", return_value=True),
        patch("microcloud.request_json", return_value={"metadata": {"ready": False}}),
    ):
        assert microcloud.is_initialized() is False


def test_list_members_parses_entries():
    data = {
        "metadata": [
            {"name": "node-1", "address": "10.0.0.1", "status": "READY"},
            {"name": "node-2", "address": "10.0.0.2", "status": "PENDING"},
            {"name": "", "address": "10.0.0.3", "status": "READY"},  # skipped: no name
        ]
    }
    with patch("microcloud.request_json", return_value=data):
        members = microcloud.list_members()

    assert len(members) == 2
    assert members[0].name == "node-1"
    assert members[0].address == "10.0.0.1"
    assert members[0].status == "ready"
    assert members[1].name == "node-2"


def test_list_members_socket_error_raises():
    with patch("microcloud.request_json", side_effect=UnixSocketError("boom")):
        with pytest.raises(microcloud.MicroCloudError):
            microcloud.list_members()
