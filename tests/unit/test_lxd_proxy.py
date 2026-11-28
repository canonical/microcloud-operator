# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for carrying the model's proxy into LXD."""

from unittest.mock import patch

import pytest

import lxd_proxy

PROXY = "http://[fd00::1]:3142"


def _reconcile(current, *, env=None, applied=None):
    with (
        patch.dict("os.environ", env or {}, clear=True),
        patch("lxd_proxy.lxd_cluster.server_config", return_value=current),
        patch("lxd_proxy.lxd_cluster.update_server_config") as update,
    ):
        owned = lxd_proxy.reconcile(applied or {})
    return owned, update


def test_sets_the_model_proxy_on_lxd():
    env = {
        "JUJU_CHARM_HTTP_PROXY": PROXY,
        "JUJU_CHARM_HTTPS_PROXY": PROXY,
        "JUJU_CHARM_NO_PROXY": "localhost,[fd00::1]",
    }
    owned, update = _reconcile({"core.https_address": "[::]:8443"}, env=env)

    expected = {
        "core.proxy_http": PROXY,
        "core.proxy_https": PROXY,
        "core.proxy_ignore_hosts": "localhost,[fd00::1]",
    }
    update.assert_called_once_with(expected)
    assert owned == expected


def test_leaves_lxd_alone_when_it_already_matches():
    owned, update = _reconcile({"core.proxy_https": PROXY}, env={"JUJU_CHARM_HTTPS_PROXY": PROXY})

    update.assert_not_called()
    assert owned == {"core.proxy_https": PROXY}


def test_the_model_overrides_a_proxy_set_in_lxd():
    _, update = _reconcile(
        {"core.proxy_https": "http://other"}, env={"JUJU_CHARM_HTTPS_PROXY": PROXY}
    )

    update.assert_called_once_with({"core.proxy_https": PROXY})


def test_unsets_what_it_set_once_the_model_drops_the_proxy():
    owned, update = _reconcile({"core.proxy_https": PROXY}, applied={"core.proxy_https": PROXY})

    update.assert_called_once_with({"core.proxy_https": ""})
    assert owned == {}


@pytest.mark.parametrize(
    "applied",
    [{}, {"core.proxy_https": "http://what-the-charm-set"}],
    ids=["never-set-by-the-charm", "changed-since"],
)
def test_keeps_a_proxy_the_operator_set_in_lxd(applied):
    _, update = _reconcile({"core.proxy_https": "http://operator"}, applied=applied)

    update.assert_not_called()
