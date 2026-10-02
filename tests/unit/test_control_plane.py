# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the control-plane role actions."""

import json
import subprocess
from unittest.mock import MagicMock, patch

import pytest

import lxd_cluster
from control_plane import ControlPlane


def _member(name, domain="zone-1", roles=(), status="Online"):
    return lxd_cluster.Member(
        name=name, status=status, failure_domain=domain, architecture="x86_64", roles=list(roles)
    )


def _holder(name, domain="zone-1", status="Online"):
    return _member(name, domain, ["database-voter", "control-plane"], status)


class TestServerVersion:
    def test_reads_the_version_and_extensions(self):
        payload = {
            "api_extensions": ["clustering", "clustering_control_plane"],
            "environment": {"server_version": "6.9"},
        }
        with patch(
            "lxd_cluster.subprocess.run", return_value=MagicMock(stdout=json.dumps(payload))
        ) as run:
            version, extensions = lxd_cluster.server_version()

        assert run.call_args.args[0] == ["lxc", "query", "-X", "GET", "/1.0"]
        assert version == "6.9"
        assert "clustering_control_plane" in extensions


class TestSetControlPlaneRole:
    @pytest.mark.parametrize(("present", "verb"), [(True, "add"), (False, "remove")])
    def test_runs_lxc_cluster_role(self, present, verb):
        """lxc cluster role reads and writes the member with its ETag."""
        with patch("lxd_cluster.subprocess.run") as run:
            assert lxd_cluster.set_control_plane_role("node1", present) is True

        assert run.call_args.args[0] == ["lxc", "cluster", "role", verb, "node1", "control-plane"]

    @pytest.mark.parametrize(
        ("present", "stderr"),
        [
            (True, 'Error: Member "node1" already has role "control-plane"'),
            (False, 'Error: Member "node1" does not have role "control-plane"'),
        ],
    )
    def test_a_member_that_already_matches_is_unchanged(self, present, stderr):
        error = subprocess.CalledProcessError(1, "lxc", stderr=stderr)
        with patch("lxd_cluster.subprocess.run", side_effect=error):
            assert lxd_cluster.set_control_plane_role("node1", present) is False

    def test_a_member_changed_in_between_fails(self):
        error = subprocess.CalledProcessError(
            1, "lxc", stderr="Error: ETag does not match: abc vs def."
        )
        with (
            patch("lxd_cluster.subprocess.run", side_effect=error),
            pytest.raises(
                lxd_cluster.LXDClusterError, match="cluster member changed; run the action again"
            ),
        ):
            lxd_cluster.set_control_plane_role("node1", True)

    def test_other_failures_carry_lxd_s_message(self):
        error = subprocess.CalledProcessError(1, "lxc", stderr="Error: boom")
        with (
            patch("lxd_cluster.subprocess.run", side_effect=error),
            pytest.raises(lxd_cluster.LXDClusterError, match="Error: boom"),
        ):
            lxd_cluster.set_control_plane_role("node1", True)


class TestControlPlaneSummary:
    def test_counts_holders_online_or_not(self):
        summary = lxd_cluster.control_plane_summary(
            [
                _holder("node1", "rack1"),
                _holder("node2", "rack2", status="Offline"),
                _holder("node3", "rack2", status="Evacuated"),
                _member("node4", "rack3"),
            ]
        )

        assert summary.holders == ["node1", "node2", "node3"]
        assert summary.online_by_failure_domain == {"rack1": 1, "rack2": 0, "rack3": 0}
        assert summary.mode == "active (3 role holders)"

    @pytest.mark.parametrize("count", [0, 2])
    def test_fewer_than_3_holders_is_inactive(self, count):
        cluster = [_holder(f"node{i}") for i in range(count)] + [_member("node9")]

        assert lxd_cluster.control_plane_summary(cluster).mode == (
            f"inactive ({count} of 3 role holders)"
        )


class TestActions:
    def _run(
        self,
        *,
        present=True,
        members=None,
        initialized=True,
        clustered=True,
        version="6.9",
        extensions=("clustering_control_plane",),
        set_result=True,
        set_side_effect=None,
    ):
        if members is None:
            members = [_member("node1"), _holder("node2"), _holder("node3")]
        event = MagicMock()
        with (
            patch("control_plane.microcloud.hostname", return_value="node1"),
            patch("control_plane.microcloud.is_initialized", return_value=initialized),
            patch("control_plane.lxd_cluster.is_clustered", return_value=clustered),
            patch(
                "control_plane.lxd_cluster.server_version", return_value=(version, set(extensions))
            ),
            patch("control_plane.lxd_cluster.members", return_value=members),
            patch(
                "control_plane.lxd_cluster.set_control_plane_role",
                return_value=set_result,
                side_effect=set_side_effect,
            ) as set_role,
        ):
            if present:
                ControlPlane().on_add_action(event)
            else:
                ControlPlane().on_remove_action(event)
        return event, set_role

    def test_add_gives_this_unit_s_member_the_role(self):
        event, set_role = self._run()

        set_role.assert_called_once_with("node1", True)
        event.fail.assert_not_called()
        results = event.set_results.call_args.args[0]
        assert results["changed"] is True
        assert results["role-holders"] == "node2,node3"
        assert results["control-plane-mode"] == "inactive (2 of 3 role holders)"
        assert json.loads(results["role-holders-by-failure-domain"]) == {"zone-1": 2}

    def test_remove_takes_the_role_from_this_unit_s_member(self):
        event, set_role = self._run(present=False, members=[_holder("node1")])

        set_role.assert_called_once_with("node1", False)
        assert event.set_results.call_args.args[0]["changed"] is True

    @pytest.mark.parametrize(
        ("present", "roles"), [(True, ["control-plane"]), (False, ["database-voter"])]
    )
    def test_a_member_that_already_matches_is_left_alone(self, present, roles):
        event, set_role = self._run(present=present, members=[_member("node1", roles=roles)])

        set_role.assert_not_called()
        assert event.set_results.call_args.args[0]["changed"] is False

    def test_a_race_lxc_reports_as_unchanged_is_unchanged(self):
        event, _ = self._run(set_result=False)

        assert event.set_results.call_args.args[0]["changed"] is False

    @pytest.mark.parametrize(
        ("initialized", "clustered", "members"),
        [(False, False, []), (True, False, []), (True, True, [_member("node2")])],
    )
    def test_fails_before_this_unit_is_an_lxd_member(self, initialized, clustered, members):
        event, set_role = self._run(initialized=initialized, clustered=clustered, members=members)

        event.fail.assert_called_once_with("node1 is not an LXD cluster member yet")
        set_role.assert_not_called()
        event.set_results.assert_not_called()

    def test_fails_on_lxd_without_the_role(self):
        event, set_role = self._run(version="5.21.4", extensions=())

        event.fail.assert_called_once_with(
            "LXD 5.21.4 has no control-plane role; need 6.8 or later"
        )
        set_role.assert_not_called()

    def test_fails_when_lxd_refuses(self):
        event, _ = self._run(
            set_side_effect=lxd_cluster.LXDClusterError(
                "cluster member changed; run the action again"
            )
        )

        event.fail.assert_called_once_with("cluster member changed; run the action again")
        event.set_results.assert_not_called()
