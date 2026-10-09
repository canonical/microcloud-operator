# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Assigning LXD's control-plane role to this unit's cluster member.

Once at least 3 members hold the role, only role holders can hold the dqlite
database: LXD picks voters and standbys from among them. Role holders beyond
cluster.max_voters plus cluster.max_standby stay spares until LXD needs to
promote one.

Every role holder is also an event hub, whether or not it holds part of the
database.

The operator picks the holders by running the actions on their units. Each
unit changes only its own member, and the role lives only in the LXD database.
"""

import json
import logging

import ops

import lxd_cluster
import microcloud

logger = logging.getLogger(__name__)


class ControlPlane:
    """Helper behind the control-plane role actions."""

    def on_add_action(self, event: ops.ActionEvent) -> None:
        """Give this unit's member the control-plane role."""
        self._set_role(event, present=True)

    def on_remove_action(self, event: ops.ActionEvent) -> None:
        """Take the control-plane role from this unit's member."""
        self._set_role(event, present=False)

    def _set_role(self, event: ops.ActionEvent, *, present: bool) -> None:
        hostname = microcloud.hostname()
        try:
            member, problem = self._own_member(hostname)
            if member is None:
                event.fail(problem)
                return

            changed = False
            if (lxd_cluster.CONTROL_PLANE_ROLE in member.roles) != present:
                logger.info(
                    "%s the control-plane role of %s",
                    "Adding" if present else "Removing",
                    hostname,
                )
                changed = lxd_cluster.set_control_plane_role(hostname, present)

            summary = lxd_cluster.control_plane_summary(lxd_cluster.members())
        except lxd_cluster.LXDClusterError as exc:
            event.fail(str(exc))
            return

        event.set_results(
            {
                "changed": changed,
                "role-holders": ",".join(summary.holders),
                "control-plane-mode": summary.mode,
                "role-holders-by-failure-domain": json.dumps(
                    summary.online_by_failure_domain, sort_keys=True
                ),
            }
        )

    def _own_member(self, hostname: str) -> tuple[lxd_cluster.Member | None, str]:
        """Return this unit's LXD member, or None and why it cannot take the role."""
        not_member = f"{hostname} is not an LXD cluster member yet"
        if not microcloud.is_initialized() or not lxd_cluster.is_clustered():
            return None, not_member
        version, extensions = lxd_cluster.server_version()
        if lxd_cluster.CONTROL_PLANE_EXTENSION not in extensions:
            return None, f"LXD {version} has no control-plane role; need 6.8 or later"
        member = next((m for m in lxd_cluster.members() if m.name == hostname), None)
        return member, "" if member else not_member
