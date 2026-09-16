# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Carrying this unit's Juju availability zone through to its LXD failure domain.

The zone becomes the failure domain unchanged, so the zone an operator reads
in the substrate is the failure domain they read in LXD. Each unit only sets
its own member, so no coordination is needed across units or applications.
"""

import logging
import os
import time
from collections.abc import Mapping
from typing import Any

import lxd_cluster
import microcloud

logger = logging.getLogger(__name__)

# How long a hook waits for LXD to spread database roles across newly set
# failure domains, and how often it checks.
_SPREAD_WAIT = 60
_SPREAD_POLL = 5


class FailureDomains:
    """Helper that sets this unit's LXD failure domain from its zone."""

    def __init__(self, config: Mapping[str, Any]) -> None:
        self._config = config

    def zone(self) -> str:
        """Return the zone Juju reports for this unit's machine, or ""."""
        return os.environ.get("JUJU_AVAILABILITY_ZONE", "").strip()

    def zone_missing(self) -> bool:
        """Return True if this unit has no zone although one is required."""
        return not self.zone() and bool(self._config.get("require-zone", True))

    def reconcile(self) -> tuple[str | None, bool]:
        """Set this unit's LXD failure domain to its zone.

        A clustered unit without a zone keeps whatever failure domain it has:
        units only join with a zone, so the substrate has opted out of zones.

        Returns (problem or None, whether the failure domain was changed).
        """
        hostname = microcloud.hostname()
        zone = self.zone()
        if not zone:
            return None, False

        try:
            member = next((m for m in lxd_cluster.members() if m.name == hostname), None)
            if member is None:
                return f"{hostname} is not an LXD cluster member", False
            if member.failure_domain == zone:
                return None, False
            logger.info("Setting the failure domain of %s to %s", hostname, zone)
            lxd_cluster.set_failure_domain(hostname, zone)
        except lxd_cluster.LXDClusterError as exc:
            return f"Cannot set the failure domain of {hostname} to {zone!r}: {exc}", False
        return None, True

    def wait_for_spread(self, *, wait: bool) -> str | None:
        """Wait for database roles to spread across failure domains. Returns a wait or None.

        Members join before the charm has set their failure domains, so LXD
        first hands out database roles without them. It spreads voters
        across failure domains on its own shortly after they change. The
        hook that has just changed this unit's failure domain gives it a
        little time rather than report active too early; any other hook
        only checks.
        """
        deadline = time.monotonic() + (_SPREAD_WAIT if wait else 0)
        while True:
            try:
                unspread = lxd_cluster.unspread_voters(lxd_cluster.members())
            except lxd_cluster.LXDClusterError as exc:
                logger.warning("Cannot check database roles across failure domains: %s", exc)
                return None
            if unspread is None:
                return None
            if time.monotonic() >= deadline:
                break
            time.sleep(_SPREAD_POLL)

        spread, possible = unspread
        return f"Spreading database roles across failure domains ({spread} of {possible})"
