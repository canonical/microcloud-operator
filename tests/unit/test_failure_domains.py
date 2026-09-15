# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for carrying zones through to LXD failure domains."""

from unittest.mock import patch

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
        failure_domains = FailureDomains()
        with patch.dict("os.environ", {"JUJU_AVAILABILITY_ZONE": " zone-1 "}):
            assert failure_domains.zone() == "zone-1"
        with patch.dict("os.environ", {}, clear=True):
            assert failure_domains.zone() == ""


class TestReconcile:
    def _set(self, members, *, zone="zone-1", set_side_effect=None):
        with (
            _zone(zone),
            patch("failure_domains.microcloud.hostname", return_value="node1"),
            patch("failure_domains.lxd_cluster.members", return_value=members),
            patch(
                "failure_domains.lxd_cluster.set_failure_domain", side_effect=set_side_effect
            ) as set_failure_domain,
        ):
            problem = FailureDomains().reconcile()
        return problem, set_failure_domain

    def test_sets_the_failure_domain_to_the_zone(self):
        problem, set_failure_domain = self._set([_member("node1", "default")])

        assert problem is None
        set_failure_domain.assert_called_once_with("node1", "zone-1")

    def test_leaves_a_matching_failure_domain_alone(self):
        problem, set_failure_domain = self._set([_member("node1", "zone-1")])

        assert problem is None
        set_failure_domain.assert_not_called()

    def test_leaves_the_failure_domain_alone_without_a_zone(self):
        """Substrates without zones keep LXD's own failure domains."""
        problem, set_failure_domain = self._set([_member("node1", "rack-a")], zone="")

        assert problem is None
        set_failure_domain.assert_not_called()

    def test_blocks_when_not_an_lxd_member(self):
        problem, _ = self._set([_member("node2")])

        assert problem == "node1 is not an LXD cluster member"

    def test_blocks_when_lxd_refuses(self):
        problem, _ = self._set(
            [_member("node1", "default")],
            set_side_effect=lxd_cluster.LXDClusterError("denied"),
        )

        assert problem == "Cannot set the failure domain of node1 to 'zone-1': denied"
