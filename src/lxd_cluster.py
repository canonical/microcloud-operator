# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""LXD cluster members, their failure domains and database roles.

MicroCloud cannot set a member's failure domain when it forms or grows the
cluster, so the charm sets it afterwards through the local LXD API, and LXD
then spreads database voters across the failure domains.
"""

import json
import subprocess
from dataclasses import dataclass, field

# Roles that make a member a database voter. LXD reports "database-voter" on
# current releases; "database" is the name older ones used.
_VOTER_ROLES = {"database-voter", "database-leader", "database"}
_CONTROL_PLANE_ROLE = "control-plane"
# Roles LXD assigns and preserves itself on current releases, and refuses to
# have added back once a member has lost them. Older releases skip them too,
# but insist "database" matches the member's, so that one is kept.
_AUTOMATIC_ROLES = {"database-voter", "database-standby", "database-leader"}


class LXDClusterError(Exception):
    """Raised when an LXD cluster API call fails."""


@dataclass
class Member:
    """An LXD cluster member, as far as the charm is concerned."""

    name: str
    status: str
    failure_domain: str
    architecture: str
    roles: list[str] = field(default_factory=list)

    @property
    def is_voter(self) -> bool:
        return bool(_VOTER_ROLES.intersection(self.roles))


def _query(path: str, method: str = "GET", data: dict | None = None) -> object:
    command = ["lxc", "query", "-X", method]
    if data is not None:
        command += ["-d", json.dumps(data)]
    command.append(path)
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=30)
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        detail = getattr(exc, "stderr", "") or str(exc)
        raise LXDClusterError(f"lxc query {method} {path} failed: {detail.strip()}") from exc
    if not result.stdout.strip():
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise LXDClusterError(f"Invalid JSON from lxc query {path}: {exc}") from exc


def is_clustered() -> bool:
    """Return True once this server's LXD is part of a cluster."""
    cluster = _query("/1.0/cluster")
    return isinstance(cluster, dict) and bool(cluster.get("enabled"))


def members() -> list[Member]:
    """Return every LXD cluster member."""
    raw = _query("/1.0/cluster/members?recursion=1") or []
    return [
        Member(
            name=entry.get("server_name", ""),
            status=entry.get("status", ""),
            failure_domain=entry.get("failure_domain", ""),
            architecture=entry.get("architecture", ""),
            roles=list(entry.get("roles") or []),
        )
        for entry in raw
    ]


def set_failure_domain(name: str, failure_domain: str) -> None:
    """Set member ``name``'s failure domain, leaving the rest of it unchanged.

    Like ``lxc cluster failure-domain set``, this writes back every writable
    field: LXD treats one left out as emptied, and refuses a member without
    any cluster group. Automatic roles are left out, so a member demoted
    between the read and the write is not refused for adding one back.
    """
    path = f"/1.0/cluster/members/{name}"
    member = _query(path)
    if not isinstance(member, dict):
        raise LXDClusterError(f"Unexpected response for cluster member {name}")
    writable = {key: member.get(key) for key in ("config", "description", "groups")}
    writable["roles"] = [
        role for role in member.get("roles") or [] if role not in _AUTOMATIC_ROLES
    ]
    writable["failure_domain"] = failure_domain
    _query(path, "PUT", writable)


def unspread_voters(cluster: list[Member]) -> tuple[int, int] | None:
    """Return (domains with voters, domains LXD can spread voters to) if it will spread them.

    Mirrors LXD's own rule for spreading voters: online voters cover fewer
    failure domains than there are online failure domains, and fewer than
    there are online voters, and some member it may promote sits in a
    failure domain without a voter. It only promotes online members that
    are not voters yet and, once any member has the control-plane role,
    only members with that role. The domains it can spread to are therefore
    those with voters plus those with a member it may promote, and never
    more than there are voters. Returns None when LXD has nothing it can
    spread.
    """
    online = [member for member in cluster if member.status == "Online"]
    voters = [member for member in online if member.is_voter]
    domains = {member.failure_domain for member in online}
    with_voters = {member.failure_domain for member in voters}
    if len(with_voters) >= min(len(domains), len(voters)):
        return None

    control_plane = any(_CONTROL_PLANE_ROLE in member.roles for member in cluster)
    candidates = [
        member
        for member in online
        if not member.is_voter
        and member.failure_domain not in with_voters
        and (not control_plane or _CONTROL_PLANE_ROLE in member.roles)
    ]
    reachable = min(
        len(voters), len(with_voters | {member.failure_domain for member in candidates})
    )
    if len(with_voters) >= reachable:
        return None
    return len(with_voters), reachable
