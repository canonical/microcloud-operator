# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Carrying this unit's Juju availability zone through to its LXD failure domain.

The zone becomes the failure domain unchanged, so the zone an operator reads
in the substrate is the failure domain they read in LXD. Each unit only sets
its own member, so no coordination is needed across units or applications.
"""

import logging
import os

import lxd_cluster
import microcloud

logger = logging.getLogger(__name__)


class FailureDomains:
    """Helper that sets this unit's LXD failure domain from its zone."""

    def zone(self) -> str:
        """Return the zone Juju reports for this unit's machine, or ""."""
        return os.environ.get("JUJU_AVAILABILITY_ZONE", "").strip()

    def reconcile(self) -> str | None:
        """Set this unit's LXD failure domain to its zone. Returns problem or None.

        A unit whose machine reports no zone keeps whatever failure domain it
        has, which on substrates without zones is LXD's "default".
        """
        hostname = microcloud.hostname()
        zone = self.zone()
        if not zone:
            logger.debug("%s has no zone; leaving its failure domain alone", hostname)
            return None

        try:
            member = next((m for m in lxd_cluster.members() if m.name == hostname), None)
            if member is None:
                return f"{hostname} is not an LXD cluster member"
            if member.failure_domain == zone:
                return None
            logger.info("Setting the failure domain of %s to %s", hostname, zone)
            lxd_cluster.set_failure_domain(hostname, zone)
        except lxd_cluster.LXDClusterError as exc:
            return f"Cannot set the failure domain of {hostname} to {zone!r}: {exc}"
        return None
