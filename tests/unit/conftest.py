# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import patch

import pytest

import bgp
import snap


@pytest.fixture(autouse=True)
def _no_microovn_snap():
    """Keep unit tests from asking the real machine for the MicroOVN and MicroCeph snaps.

    The charm checks for them on every reconcile; a test that needs one
    installed patches bgp.present or snap.present itself.
    """
    with (
        patch.object(bgp, "present", return_value=False),
        patch.object(snap, "present", return_value=False),
    ):
        yield
