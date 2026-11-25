# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the "lxd_dpu" relation with the DPUs in this unit's host."""

import ops
import ops.testing
import pytest

from dpu_relation import DPURelation

_META = """
name: microcloud
provides:
  dpus:
    interface: lxd_dpu
"""


@pytest.fixture
def harness():
    harness = ops.testing.Harness(ops.CharmBase, meta=_META)
    harness.begin()
    yield harness
    harness.cleanup()


def test_without_the_relation_nothing_is_published(harness):
    dpus = DPURelation(harness.charm)

    assert dpus.relation() is None
    dpus.reconcile("node-1", "fd2b:5d1a:7c93::10")


def test_publishes_the_member_and_oam_address(harness):
    rel_id = harness.add_relation("dpus", "microcloud-dpu")

    DPURelation(harness.charm).reconcile("node-1", "fd2b:5d1a:7c93::10")

    assert harness.get_relation_data(rel_id, harness.charm.unit) == {
        "member": "node-1",
        "oam-address": "fd2b:5d1a:7c93::10",
    }


def test_an_unbound_endpoint_publishes_no_oam_address(harness):
    rel_id = harness.add_relation("dpus", "microcloud-dpu")
    harness.update_relation_data(
        rel_id, harness.charm.unit.name, {"member": "node-1", "oam-address": "10.0.0.10"}
    )

    DPURelation(harness.charm).reconcile("node-1", "")

    assert harness.get_relation_data(rel_id, harness.charm.unit) == {"member": "node-1"}
