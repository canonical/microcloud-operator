# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""LXD cluster members, their failure domains and database roles.

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
