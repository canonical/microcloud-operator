# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""LXD cluster members, their failure domains and roles.

MicroCloud cannot set a member's failure domain when it forms or grows the
cluster, so the charm sets it afterwards through the local LXD API.
"""

import json
import subprocess
from dataclasses import dataclass, field

# Roles LXD assigns and preserves itself on current releases, and refuses to
# have added back once a member has lost them. Older releases skip them too,
# but insist "database" matches the member's, so that one is kept.
_AUTOMATIC_ROLES = {"database-voter", "database-standby", "database-leader"}

CONTROL_PLANE_ROLE = "control-plane"
CONTROL_PLANE_EXTENSION = "clustering_control_plane"
# LXD keeps database roles on role holders once at least this many members
# hold the role, counting offline ones.
CONTROL_PLANE_MIN_HOLDERS = 3


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


def server_version() -> tuple[str, set[str]]:
    """Return this server's LXD version and its API extensions."""
    server = _query("/1.0")
    if not isinstance(server, dict):
        raise LXDClusterError("Unexpected response for /1.0")
    environment = server.get("environment") or {}
    return environment.get("server_version", ""), set(server.get("api_extensions") or [])


def set_control_plane_role(name: str, present: bool) -> bool:
    """Add or remove member ``name``'s control-plane role. Returns False if unchanged.

    ``lxc cluster role`` reads the member and writes it back with its ETag,
    so a member changed in between fails the write instead of being
    overwritten. ``lxc query`` cannot send the ETag.
    """
    command = ["lxc", "cluster", "role", "add" if present else "remove", name, CONTROL_PLANE_ROLE]
    try:
        subprocess.run(command, capture_output=True, text=True, check=True, timeout=30)
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or "").strip()
        if "already has role" in detail or "does not have role" in detail:
            return False
        if "ETag does not match" in detail:
            raise LXDClusterError("cluster member changed; run the action again") from exc
        raise LXDClusterError(f"{' '.join(command)} failed: {detail}") from exc
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise LXDClusterError(f"{' '.join(command)} failed: {exc}") from exc
    return True


@dataclass
class ControlPlaneSummary:
    """Control-plane role holders across the cluster."""

    holders: list[str]
    online_by_failure_domain: dict[str, int]

    @property
    def mode(self) -> str:
        """Describe control-plane mode the way the actions report it."""
        if len(self.holders) >= CONTROL_PLANE_MIN_HOLDERS:
            return f"active ({len(self.holders)} role holders)"
        return f"inactive ({len(self.holders)} of {CONTROL_PLANE_MIN_HOLDERS} role holders)"


def control_plane_summary(cluster: list[Member]) -> ControlPlaneSummary:
    """Summarize role holders, online or not, and online holders per failure domain."""
    holders = sorted(member.name for member in cluster if CONTROL_PLANE_ROLE in member.roles)
    online: dict[str, int] = {member.failure_domain: 0 for member in cluster}
    for member in cluster:
        if CONTROL_PLANE_ROLE in member.roles and member.status == "Online":
            online[member.failure_domain] += 1
    return ControlPlaneSummary(holders=holders, online_by_failure_domain=online)


def render_table(cluster: list[Member]) -> str:
    """Render members as the table the status action prints."""
    rows = [("NAME", "ROLES", "FAILURE DOMAIN", "ARCHITECTURE")]
    rows += [
        (member.name, ",".join(member.roles) or "-", member.failure_domain, member.architecture)
        for member in sorted(cluster, key=lambda member: member.name)
    ]
    widths = [max(len(row[column]) for row in rows) for column in range(3)]
    return "\n".join(
        "  ".join(cell.ljust(width) for cell, width in zip(row[:3], widths, strict=True))
        + "  "
        + row[3]
        for row in rows
    )
