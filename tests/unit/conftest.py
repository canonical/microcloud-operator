# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import patch

import pytest

import bgp
import cluster_relation


@pytest.fixture(autouse=True)
def _no_microovn_snap():
    """Keep unit tests from asking the real machine for the MicroOVN snap.

    The charm checks for it on every reconcile; a test that needs it
    installed patches bgp.installed itself.
    """
    with patch.object(bgp, "installed", return_value=False):
        yield


@pytest.fixture(autouse=True)
def _no_goal_state():
    """Keep unit tests from running the goal-state hook tool.

    None publishes every peer unit; a test of the filtering patches it itself.
    """
    with patch.object(cluster_relation, "goal_state_units", return_value=None):
        yield
