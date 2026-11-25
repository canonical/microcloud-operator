# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the "lxd_dpu" relation with the DPUs in this unit's host."""

import base64
import json
import subprocess
from datetime import datetime, timezone
from unittest.mock import patch

import ops
import ops.testing
import pytest

import lxd_cluster
from dpu_relation import DPURelation

_META = """
name: microcloud
provides:
  dpus:
    interface: lxd_dpu
"""

_OAM = "fd2b:5d1a:7c93::10"


def _token(name="node-1-dpu-0", expires_at="2026-12-10T18:00:00.123456789Z"):
    data = {"client_name": name, "secret": "s3cr3t", "expires_at": expires_at}
    return base64.b64encode(json.dumps(data).encode()).decode()


class FakeLXD:
    """Constituents and join tokens of the local LXD member."""

    def __init__(self):
        self.constituents: list[lxd_cluster.Constituent] = []
        self.issued: list[str] = []
        self.conflicts: set[str] = set()

    def trust(self, name, member="node-1", reachable=True):
        self.constituents.append(
            lxd_cluster.Constituent(name, member, "[fd2b::15]:8443", True, reachable)
        )

    def add_constituent(self, name):
        if name in self.conflicts:
            raise lxd_cluster.ConstituentConflictError(f'A constituent named "{name}" exists')
        self.issued.append(name)
        return _token(name)


@pytest.fixture
def lxd():
    fake = FakeLXD()
    with (
        patch.object(lxd_cluster, "constituents", side_effect=lambda: list(fake.constituents)),
        patch.object(lxd_cluster, "add_constituent", side_effect=fake.add_constituent),
    ):
        yield fake


@pytest.fixture
def harness():
    harness = ops.testing.Harness(ops.CharmBase, meta=_META)
    harness.begin()
    yield harness
    harness.cleanup()


def _relate(harness, *dpus):
    rel_id = harness.add_relation("dpus", "microcloud-dpu")
    for index, data in enumerate(dpus):
        unit = f"microcloud-dpu/{index}"
        harness.add_relation_unit(rel_id, unit)
        harness.update_relation_data(rel_id, unit, data)
    return rel_id


def _dpu(name="node-1-dpu-0", host="node-1", **extra):
    return {"name": name, "host": host, "address": "[fd2b::15]:8443", **extra}


def _local(harness, rel_id):
    return harness.get_relation_data(rel_id, harness.charm.unit)


def _tokens(harness, rel_id):
    return json.loads(_local(harness, rel_id).get("tokens", "{}"))


def test_without_the_relation_nothing_is_published(harness, lxd):
    dpus = DPURelation(harness.charm)

    assert dpus.relation() is None
    dpus.reconcile("node-1", _OAM)
    assert lxd.issued == []


def test_publishes_the_member_and_oam_address(harness, lxd):
    rel_id = _relate(harness)

    DPURelation(harness.charm).reconcile("node-1", _OAM)

    assert _local(harness, rel_id) == {"member": "node-1", "oam-address": _OAM}


def test_an_unbound_endpoint_publishes_no_oam_address(harness, lxd):
    rel_id = _relate(harness)
    harness.update_relation_data(
        rel_id, harness.charm.unit.name, {"member": "node-1", "oam-address": "10.0.0.10"}
    )

    DPURelation(harness.charm).reconcile("node-1", "")

    assert _local(harness, rel_id) == {"member": "node-1"}


def test_issues_a_token_in_a_secret_granted_to_that_dpu_only(harness, lxd):
    rel_id = _relate(harness, _dpu(), _dpu("node-1-dpu-1"))

    DPURelation(harness.charm).reconcile("node-1", _OAM)

    assert lxd.issued == ["node-1-dpu-0", "node-1-dpu-1"]
    tokens = _tokens(harness, rel_id)
    secret = harness.model.get_secret(label="lxd-dpu-token-node-1-dpu-0")
    assert secret.get_info().id == tokens["microcloud-dpu/0"]
    assert secret.get_content(refresh=True) == {"token": _token("node-1-dpu-0")}
    assert secret.get_info().expires == datetime(
        2026, 12, 10, 18, 0, 0, 123456, tzinfo=timezone.utc
    )
    assert harness.get_secret_grants(tokens["microcloud-dpu/0"], rel_id) == {"microcloud-dpu/0"}


def test_skips_dpus_of_other_members(harness, lxd):
    rel_id = _relate(harness, _dpu("node-2-dpu-0", host="node-2"))

    DPURelation(harness.charm).reconcile("node-1", _OAM)

    assert lxd.issued == []
    assert "tokens" not in _local(harness, rel_id)


def test_skips_a_dpu_that_has_not_published_itself(harness, lxd):
    rel_id = _relate(harness, {"address": "[fd2b::15]:8443"})

    DPURelation(harness.charm).reconcile("node-1", _OAM)

    assert lxd.issued == []
    assert "tokens" not in _local(harness, rel_id)


def test_a_pending_token_is_not_issued_again(harness, lxd):
    rel_id = _relate(harness, _dpu())
    dpus = DPURelation(harness.charm)
    dpus.reconcile("node-1", _OAM)

    dpus.reconcile("node-1", _OAM)

    assert lxd.issued == ["node-1-dpu-0"]
    assert list(_tokens(harness, rel_id)) == ["microcloud-dpu/0"]


def test_a_joined_dpu_has_its_secret_removed(harness, lxd):
    rel_id = _relate(harness, _dpu())
    dpus = DPURelation(harness.charm)
    dpus.reconcile("node-1", _OAM)
    secret_id = _tokens(harness, rel_id)["microcloud-dpu/0"]
    lxd.trust("node-1-dpu-0")
    harness.update_relation_data(rel_id, "microcloud-dpu/0", {"joined": "true"})

    dpus.reconcile("node-1", _OAM)

    assert "tokens" not in _local(harness, rel_id)
    with pytest.raises(ops.SecretNotFoundError):
        harness.model.get_secret(id=secret_id).get_content()


def test_a_trusted_dpu_keeps_its_secret_until_it_reports_joined(harness, lxd):
    rel_id = _relate(harness, _dpu())
    dpus = DPURelation(harness.charm)
    dpus.reconcile("node-1", _OAM)
    lxd.trust("node-1-dpu-0")

    dpus.reconcile("node-1", _OAM)

    assert list(_tokens(harness, rel_id)) == ["microcloud-dpu/0"]


def test_a_taken_name_is_skipped(harness, lxd):
    lxd.conflicts.add("node-1-dpu-0")
    rel_id = _relate(harness, _dpu())

    DPURelation(harness.charm).reconcile("node-1", _OAM)

    assert "tokens" not in _local(harness, rel_id)


def test_two_dpus_with_one_name_get_one_token(harness, lxd):
    rel_id = _relate(harness, _dpu(), _dpu())

    DPURelation(harness.charm).reconcile("node-1", _OAM)

    assert lxd.issued == ["node-1-dpu-0"]
    assert list(_tokens(harness, rel_id)) == ["microcloud-dpu/0"]


def test_an_expired_token_is_dropped_and_issued_again(harness, lxd):
    rel_id = _relate(harness, _dpu())
    dpus = DPURelation(harness.charm)
    dpus.reconcile("node-1", _OAM)
    secret = harness.model.get_secret(id=_tokens(harness, rel_id)["microcloud-dpu/0"])

    assert dpus.expired(secret)
    assert "tokens" not in _local(harness, rel_id)

    dpus.reconcile("node-1", _OAM)
    assert lxd.issued == ["node-1-dpu-0", "node-1-dpu-0"]


def test_other_secrets_are_not_treated_as_tokens(harness, lxd):
    secret = harness.charm.app.add_secret({"passphrase": "x"}, label="session-passphrase")

    assert not DPURelation(harness.charm).expired(secret)


def test_add_constituent_returns_the_token():
    def run(cmd, **kwargs):
        return subprocess.CompletedProcess(cmd, 0, _token() + "\n", "")

    with patch("subprocess.run", side_effect=run) as mock:
        assert lxd_cluster.add_constituent("node-1-dpu-0") == _token()

    assert mock.call_args.args[0] == [
        "lxc",
        "cluster",
        "constituent",
        "add",
        "node-1-dpu-0",
        "--type",
        "dpu",
        "--quiet",
    ]


@pytest.mark.parametrize(
    "stderr",
    [
        'Error: A constituent named "node-1-dpu-0" already exists',
        'Error: A constituent token for "node-1-dpu-0" is already pending',
    ],
)
def test_add_constituent_reports_a_conflict(stderr):
    error = subprocess.CalledProcessError(1, ["lxc"], "", stderr)

    with (
        patch("subprocess.run", side_effect=error),
        pytest.raises(lxd_cluster.ConstituentConflictError),
    ):
        lxd_cluster.add_constituent("node-1-dpu-0")


def test_constituents_reads_trust_and_reachability():
    raw = [
        {
            "name": "node-1-dpu-0",
            "member": "node-1",
            "address": "[fd2b::15]:8443",
            "state": {"trusted": True, "reachable": False},
        },
        {"name": ""},
    ]
    with patch.object(lxd_cluster, "_query", return_value=raw):
        assert lxd_cluster.constituents() == [
            lxd_cluster.Constituent("node-1-dpu-0", "node-1", "[fd2b::15]:8443", True, False)
        ]


@pytest.mark.parametrize("token", ["not base64!", base64.b64encode(b"{}").decode()])
def test_token_expiry_rejects_a_bad_token(token):
    with pytest.raises(lxd_cluster.LXDClusterError):
        lxd_cluster.token_expiry(token)
