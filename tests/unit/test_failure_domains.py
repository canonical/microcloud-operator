# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for carrying zones through to LXD failure domains."""

from unittest.mock import patch

import pytest

import lxd_cluster
from failure_domains import FailureDomains


def _member(name, domain="zone-1", roles=(), status="Online"):
    return lxd_cluster.Member(
        name=name, status=status, failure_domain=domain, architecture="x86_64", roles=list(roles)
    )


def _zone(zone):
    return patch.dict("os.environ", {"JUJU_AVAILABILITY_ZONE": zone})


class TestZone:
    def test_zone_comes_from_the_hook_environment(self):
        failure_domains = FailureDomains({})
        with patch.dict("os.environ", {"JUJU_AVAILABILITY_ZONE": " zone-1 "}):
            assert failure_domains.zone() == "zone-1"
        with patch.dict("os.environ", {}, clear=True):
            assert failure_domains.zone() == ""

    def test_zone_missing_honours_the_opt_out(self):
        with _zone(""):
            assert FailureDomains({"require-zone": True}).zone_missing() is True
            assert FailureDomains({"require-zone": False}).zone_missing() is False
        with _zone("zone-1"):
            assert FailureDomains({"require-zone": True}).zone_missing() is False


class TestReconcile:
    def _set(self, members, *, zone="zone-1", require_zone=True, set_side_effect=None):
        with (
            _zone(zone),
            patch("failure_domains.microcloud.hostname", return_value="node1"),
            patch("failure_domains.lxd_cluster.members", return_value=members),
            patch(
                "failure_domains.lxd_cluster.set_failure_domain", side_effect=set_side_effect
            ) as set_failure_domain,
        ):
            problem, changed = FailureDomains({"require-zone": require_zone}).reconcile()
        return problem, changed, set_failure_domain

    def test_sets_the_failure_domain_to_the_zone(self):
        problem, changed, set_failure_domain = self._set([_member("node1", "default")])

        assert (problem, changed) == (None, True)
        set_failure_domain.assert_called_once_with("node1", "zone-1")

    def test_leaves_a_matching_failure_domain_alone(self):
        problem, changed, set_failure_domain = self._set([_member("node1", "zone-1")])

        assert (problem, changed) == (None, False)
        set_failure_domain.assert_not_called()

    @pytest.mark.parametrize("require_zone", [True, False])
    def test_leaves_the_failure_domain_alone_without_a_zone(self, require_zone):
        """A clustered unit without a zone joined outside Juju, or zones are opted out of."""
        problem, changed, set_failure_domain = self._set(
            [_member("node1", "rack-a")], zone="", require_zone=require_zone
        )

        assert (problem, changed) == (None, False)
        set_failure_domain.assert_not_called()

    def test_blocks_when_not_an_lxd_member(self):
        problem, _, _ = self._set([_member("node2")])

        assert problem == "node1 is not an LXD cluster member"

    def test_blocks_when_lxd_refuses(self):
        problem, _, _ = self._set(
            [_member("node1", "default")],
            set_side_effect=lxd_cluster.LXDClusterError("denied"),
        )

        assert problem == "Cannot set the failure domain of node1 to 'zone-1': denied"


class TestWaitForSpread:
    _UNSPREAD = [
        ("node1", "zone-1", ["database-leader"]),
        ("node2", "zone-1", ["database-voter"]),
        ("node3", "zone-2", ["database-voter"]),
        ("node4", "zone-3", ["database-standby"]),
    ]
    _SPREAD = [
        ("node1", "zone-1", ["database-leader"]),
        ("node2", "zone-1", ["database-standby"]),
        ("node3", "zone-2", ["database-voter"]),
        ("node4", "zone-3", ["database-voter"]),
    ]

    def _spread(self, *snapshots, wait=True):
        clock = iter(range(0, 1000, 5))
        with (
            patch(
                "failure_domains.lxd_cluster.members",
                side_effect=[[_member(*m) for m in snapshot] for snapshot in snapshots],
            ) as members,
            patch("failure_domains.time.monotonic", side_effect=lambda: next(clock)),
            patch("failure_domains.time.sleep") as sleep,
        ):
            waiting = FailureDomains({}).wait_for_spread(wait=wait)
        return waiting, members, sleep

    def test_spread_roles_need_no_wait(self):
        waiting, members, sleep = self._spread(self._SPREAD)

        assert waiting is None
        assert members.call_count == 1
        sleep.assert_not_called()

    def test_waits_in_the_hook_for_lxd_to_spread_roles(self):
        """LXD spreads voters on its own shortly after failure domains change."""
        waiting, members, sleep = self._spread(self._UNSPREAD, self._UNSPREAD, self._SPREAD)

        assert waiting is None
        assert members.call_count == 3
        assert sleep.call_count == 2

    def test_reports_waiting_once_the_hook_has_waited_long_enough(self):
        waiting, members, _ = self._spread(*[self._UNSPREAD] * 20)

        assert waiting == "Spreading database roles across failure domains (2 of 3)"
        assert members.call_count < 20

    def test_only_checks_once_unless_the_failure_domain_just_changed(self):
        """Waiting in every hook would hold the machine's hook lock for nothing."""
        waiting, members, sleep = self._spread(*[self._UNSPREAD] * 20, wait=False)

        assert waiting == "Spreading database roles across failure domains (2 of 3)"
        assert members.call_count == 1
        sleep.assert_not_called()

    def test_unreadable_cluster_does_not_hold_status(self):
        with patch(
            "failure_domains.lxd_cluster.members",
            side_effect=lxd_cluster.LXDClusterError("boom"),
        ):
            assert FailureDomains({}).wait_for_spread(wait=True) is None
