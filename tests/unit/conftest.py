# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import patch

import pytest

import bgp


@pytest.fixture(autouse=True)
def _no_microovn_snap():
    """Keep unit tests from asking the real machine for the MicroOVN snap.

    The charm checks for it on every reconcile; a test that needs it
    installed patches bgp.installed itself.
    """
    with patch.object(bgp, "installed", return_value=False):
        yield
