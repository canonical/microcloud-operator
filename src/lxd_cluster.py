# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""LXD cluster members, their failure domains and roles.

MicroCloud cannot set a member's failure domain when it forms or grows the
cluster, so the charm sets it afterwards through the local LXD API.
"""

import base64
import binascii
import json
import re
import subprocess
from dataclasses import dataclass, field
from datetime import datetime

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


class ConstituentConflictError(LXDClusterError):
    """Raised when a constituent name is taken or already has a pending token."""


@dataclass
class Constituent:
    """Something an LXD member manages outside the cluster, such as a DPU."""

    name: str
    member: str
    address: str
    trusted: bool
    reachable: bool


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


def certificate_fingerprint() -> str:
    """Return the SHA-256 fingerprint of this server's certificate.

    Every member of an LXD cluster shares the cluster certificate, so on a
    clustered server this identifies the cluster.
    """
    server = _query("/1.0")
    environment = server.get("environment") if isinstance(server, dict) else None
    fingerprint = (
        environment.get("certificate_fingerprint") if isinstance(environment, dict) else None
    )
    if not fingerprint:
        raise LXDClusterError("Unexpected response for /1.0: no certificate fingerprint")
    return str(fingerprint)


def storage_pool_kinds() -> list[str]:
    """Return the kinds of MicroCloud storage pool this cluster has.

    "local" for MicroCloud's local pool, "ceph" for a Ceph RBD pool and
    "cephfs" for a CephFS pool, sorted.
    """
    raw = _query("/1.0/storage-pools?recursion=1") or []
    kinds: set[str] = set()
    for pool in raw if isinstance(raw, list) else []:
        driver = pool.get("driver", "")
        if driver in ("ceph", "cephfs"):
            kinds.add(driver)
        elif pool.get("name") == "local":
            kinds.add("local")
    return sorted(kinds)


def storage_pools() -> dict[str, set[str]]:
    """Return every storage pool's name and the members it is created on."""
    pools = _query("/1.0/storage-pools?recursion=1")
    if not isinstance(pools, list):
        raise LXDClusterError("Unexpected response for /1.0/storage-pools: not a list")
    return {
        pool["name"]: set(pool.get("locations") or [])
        for pool in pools
        if isinstance(pool, dict) and pool.get("name")
    }


def own_name() -> str:
    """Return this server's name as an LXD cluster member.

    The member name is not always the hostname: members can be renamed, and
    two hosts could even hold each other's names.
    """
    server = _query("/1.0")
    environment = server.get("environment") if isinstance(server, dict) else None
    name = environment.get("server_name") if isinstance(environment, dict) else None
    if not name:
        raise LXDClusterError("Unexpected response for /1.0: no server name")
    return name


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


def constituents() -> list[Constituent]:
    """Return every constituent of the cluster, with its trust and reachability."""
    raw = _query("/1.0/cluster/constituents?recursion=1")
    if not isinstance(raw, list):
        raise LXDClusterError("Unexpected response for /1.0/cluster/constituents: not a list")
    result = []
    for entry in raw:
        if not isinstance(entry, dict) or not entry.get("name"):
            continue
        state = entry.get("state") or {}
        result.append(
            Constituent(
                name=entry["name"],
                member=entry.get("member", ""),
                address=entry.get("address", ""),
                trusted=bool(state.get("trusted")),
                reachable=bool(state.get("reachable")),
            )
        )
    return result


def remove_constituent(name: str) -> None:
    """Remove constituent ``name``, if it exists. LXD forgets its identity with it."""
    try:
        _query(f"/1.0/cluster/constituents/{name}", "DELETE")
    except LXDClusterError as exc:
        if "not found" not in str(exc).lower():
            raise


def add_constituent(name: str) -> str:
    """Issue a join token for DPU ``name`` on this member, and return it.

    Only this member accepts the join. The token is a credential: it is never
    put in an error or a log.
    """
    command = ["lxc", "cluster", "constituent", "add", name, "--type", "dpu", "--quiet"]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=30)
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or "").strip()
        if "already exists" in detail or "already pending" in detail:
            raise ConstituentConflictError(detail) from exc
        raise LXDClusterError(f"{' '.join(command)} failed: {detail}") from exc
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise LXDClusterError(f"{' '.join(command)} failed: {exc}") from exc
    token = result.stdout.strip()
    if not token:
        raise LXDClusterError(f"{' '.join(command)} printed no token")
    return token


def token_expiry(token: str) -> datetime:
    """Return when a join token expires. LXD always sets an expiry on constituent tokens."""
    try:
        expires_at = json.loads(base64.b64decode(token, validate=True))["expires_at"]
    except (binascii.Error, ValueError, KeyError, TypeError) as exc:
        raise LXDClusterError("Cannot read the join token's expiry") from exc
    # Go writes nanoseconds; datetime takes at most microseconds.
    expires_at = re.sub(r"(\.\d{6})\d+", r"\1", str(expires_at)).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(expires_at)
    except ValueError as exc:
        raise LXDClusterError("Cannot read the join token's expiry") from exc
