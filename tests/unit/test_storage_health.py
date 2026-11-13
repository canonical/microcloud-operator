# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for checking that a unit's storage made it into the cluster."""

import json
import subprocess
from unittest.mock import patch

import pytest

import lxd_cluster
import storage_health

# As "microceph disk list --json" prints it.
NO_DISKS = '{"ConfiguredDisks":[],"AvailableDisks":[{"Model":"QEMU HARDDISK","Size":"4.00GiB"}]}'
DISKS = json.dumps(
    {
        "ConfiguredDisks": [
            {"osd": 1, "path": "/dev/disk/by-id/a", "location": "node1"},
            {"osd": 2, "path": "/dev/disk/by-id/b", "location": "node2"},
            {"osd": 3, "path": "/dev/disk/by-id/c", "location": "node1"},
        ],
        "AvailableDisks": [],
    }
)
POOLS = {"local": {"node1", "node2"}, "remote": {"node1", "node2"}, "remote-fs": {"node1"}}


def _microceph(stdout):
    return patch(
        "storage_health.subprocess.run",
        return_value=subprocess.CompletedProcess([], 0, stdout=stdout, stderr=""),
    )


def _pools(pools):
    return patch("storage_health.lxd_cluster.storage_pools", return_value=pools)


class TestConfiguredOsds:
    def test_counts_the_disks_on_the_host(self):
        with _microceph(DISKS):
            assert storage_health.configured_osds("node1") == 2
            assert storage_health.configured_osds("node3") == 0

    def test_none_when_no_disk_was_added(self):
        with _microceph(NO_DISKS):
            assert storage_health.configured_osds("node1") == 0

    def test_a_failing_command_is_an_error(self):
        error = subprocess.CalledProcessError(1, ["microceph"], stderr="not initialized\n")
        with (
            patch("storage_health.subprocess.run", side_effect=error),
            pytest.raises(storage_health.StorageHealthError, match="not initialized"),
        ):
            storage_health.configured_osds("node1")

    def test_output_that_is_not_json_is_an_error(self):
        with _microceph("oops"), pytest.raises(storage_health.StorageHealthError):
            storage_health.configured_osds("node1")


class TestProblems:
    def test_a_unit_without_storage_is_not_checked(self):
        with _pools({}) as pools, _microceph(NO_DISKS) as microceph:
            assert storage_health.problems("node1", "node1", local=False, ceph_disks=0) == []
        pools.assert_not_called()
        microceph.assert_not_called()

    def test_nothing_missing(self):
        with _pools(POOLS), _microceph(DISKS):
            assert storage_health.problems("node1", "node1", local=True, ceph_disks=2) == []

    def test_a_cluster_formed_without_any_storage(self):
        with _pools({}), _microceph(NO_DISKS):
            assert storage_health.problems("node1", "node1", local=True, ceph_disks=1) == [
                'no "local" storage pool on node1',
                "0 of 1 Ceph disks on node1 are in MicroCeph",
                'no "remote" storage pool',
            ]

    def test_fewer_osds_than_disks(self):
        with _pools(POOLS), _microceph(DISKS):
            assert storage_health.problems("node1", "node1", local=False, ceph_disks=3) == [
                "2 of 3 Ceph disks on node1 are in MicroCeph"
            ]

    def test_a_local_pool_that_is_not_on_this_member(self):
        with _pools({"local": {"node2"}}):
            assert storage_health.problems("node1", "node1", local=True, ceph_disks=0) == [
                'no "local" storage pool on node1'
            ]

    def test_a_missing_cephfs_pool(self):
        with _pools({"local": {"node1"}, "remote": {"node1"}}), _microceph(DISKS):
            assert storage_health.problems(
                "node1", "node1", local=True, ceph_disks=2, cephfs=True
            ) == ['no "remote-fs" storage pool']

    def test_cephfs_is_not_checked_without_ceph_disks(self):
        with _pools({"local": {"node1"}}):
            assert (
                storage_health.problems("node1", "node1", local=True, ceph_disks=0, cephfs=True)
                == []
            )

    def test_the_cephfs_pool_is_found(self):
        with _pools(POOLS), _microceph(DISKS):
            assert (
                storage_health.problems("node1", "node1", local=True, ceph_disks=2, cephfs=True)
                == []
            )

    def test_the_member_name_can_differ_from_the_hostname(self):
        with _pools({"local": {"member-a"}, "remote": {"member-a"}}), _microceph(DISKS):
            assert storage_health.problems("node1", "member-a", local=True, ceph_disks=2) == []


class TestStoragePools:
    def test_names_and_members(self):
        pools = [
            {"name": "local", "driver": "zfs", "locations": ["node1", "node2"]},
            {"name": "remote", "driver": "ceph", "locations": None},
        ]
        with patch("lxd_cluster._query", return_value=pools) as query:
            assert lxd_cluster.storage_pools() == {"local": {"node1", "node2"}, "remote": set()}
        query.assert_called_once_with("/1.0/storage-pools?recursion=1")

    def test_an_unexpected_response_is_an_error(self):
        with (
            patch("lxd_cluster._query", return_value=None),
            pytest.raises(lxd_cluster.LXDClusterError),
        ):
            lxd_cluster.storage_pools()
