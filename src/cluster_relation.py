# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""The "microcloud_cluster" relation between orchestrator and member applications.

Each hardware profile, such as an architecture, is its own Juju application of
this charm. One application has ``orchestrator=true``: its leader bootstraps
the cluster and grows it. Every other application joins that cluster over this
relation, with ``members`` (provides) on the orchestrator and ``orchestrator``
(requires, ``limit: 1``) on the member application.

Relation data
-------------
Each application's leader publishes ``orchestrator`` in its application data,
so each side can tell a remote with the wrong option.

The orchestrator leader also publishes, in its application data, what member
units need to join: the snap channels and service versions to match, whether
the cluster exists and its LXD certificate fingerprint, which storage pools it
has, the session passphrase secret, the join session for this relation's units
and the failures of past sessions.

Each member unit publishes the same identity it publishes to its peers, plus
its service versions and, once its LXD has joined, the cluster fingerprint.
The member leader publishes the names of its units, so the orchestrator can
tell a member unit that was removed from a relation that was.
"""

import hashlib
import json
import logging
from dataclasses import dataclass, field

import ops

from cluster import (
    APP_KEY_SECRET_ID,
    APP_KEY_SESSION,
    JoinSession,
    PeerSystem,
    is_ready,
    read_passphrase,
    ready_fields,
    system_from_data,
)

logger = logging.getLogger(__name__)

MEMBERS = "members"
ORCHESTRATOR = "orchestrator"

# Application data, both sides.
_KEY_ORCHESTRATOR = "orchestrator"

# Application data, orchestrator leader.
_KEY_SNAP_CHANNELS = "snap-channels"
_KEY_SERVICE_VERSIONS = "service-versions"
_KEY_CLUSTER_INITIALIZED = "cluster-initialized"
_KEY_CLUSTER_FINGERPRINT = "cluster-fingerprint"
_KEY_CLUSTER_STORAGE = "cluster-storage"
_KEY_SESSION_TIMEOUT = "session-timeout"
_KEY_JOIN_FAILURES = "join-failures"

# Application data, member leader.
_KEY_UNITS = "units"


@dataclass
class JoinFailures:
    """Failed join sessions in a row for one "members" relation.

    ``candidates`` maps each system the failing sessions listed to its
    signature. See :meth:`session.JoinSessions.member_candidates` for when the
    count resets.
    """

    count: int = 0
    error: str = ""
    candidates: dict[str, str] = field(default_factory=dict)


@dataclass
class MemberSystem:
    """A member unit's identity as published on the "members" relation."""

    unit: str
    system: PeerSystem
    ready: bool = False
    service_versions: dict[str, str] = field(default_factory=dict)
    fingerprint: str = ""


@dataclass
class OrchestratorFacts:
    """What the orchestrator leader publishes for member units."""

    snap_channels: dict[str, str] | None = None
    service_versions: dict[str, str] = field(default_factory=dict)
    initialized: bool = False
    fingerprint: str = ""
    # None until the cluster has published it.
    storage: list[str] | None = None
    secret_id: str = ""
    session: JoinSession | None = None
    session_timeout: int = 300
    failures: JoinFailures = field(default_factory=JoinFailures)


def candidate_signatures(systems: list[MemberSystem]) -> dict[str, str]:
    """Return {system name: signature of everything that can change its join's outcome}.

    That is its address, service versions, OVN settings and storage.
    """
    return {
        member.system.name: hashlib.sha256(
            json.dumps(
                [
                    member.system.address,
                    member.service_versions,
                    member.system.ovn_uplink_interface,
                    member.system.ovn_underlay_ip,
                    member.system.storage_local_path,
                    member.system.storage_ceph_paths,
                    member.system.storage_wipe,
                    member.system.storage_encrypt,
                ],
                sort_keys=True,
            ).encode()
        ).hexdigest()[:16]
        for member in systems
    }


def _loads(raw: str | None, default: object) -> object:
    if not raw:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Ignoring malformed relation data: %r", raw)
        return default


def _str_dict(value: object) -> dict[str, str]:
    return {str(k): str(v) for k, v in value.items()} if isinstance(value, dict) else {}


def _set(data: ops.RelationDataContent, key: str, value: str | None) -> None:
    """Write ``key``, or remove it for None or "", writing only on change.

    Juju drops a key that is set to "", so an empty value never compares
    equal to what is stored.
    """
    if not value:
        data.pop(key, None)
    elif data.get(key) != value:
        data[key] = value


class ClusterRelation:
    """Reads and writes both ends of the "microcloud_cluster" relation."""

    def __init__(self, charm: ops.CharmBase) -> None:
        self._charm = charm

    # ------------------------------------------------------------------
    # Relations
    # ------------------------------------------------------------------

    def members_relations(self) -> list[ops.Relation]:
        return list(self._charm.model.relations.get(MEMBERS, []))

    def members_relation(self, relation_id: int) -> ops.Relation | None:
        for relation in self.members_relations():
            if relation.id == relation_id:
                return relation
        return None

    def orchestrator_relation(self) -> ops.Relation | None:
        relations = self._charm.model.relations.get(ORCHESTRATOR, [])
        return relations[0] if relations else None

    # ------------------------------------------------------------------
    # Both sides
    # ------------------------------------------------------------------

    def publish_role(self, orchestrator: bool) -> None:
        """Leader publishes this application's orchestrator option on every relation."""
        if not self._charm.unit.is_leader():
            return
        value = "true" if orchestrator else "false"
        relations = self.members_relations()
        orchestrator_relation = self.orchestrator_relation()
        if orchestrator_relation is not None:
            relations.append(orchestrator_relation)
        for relation in relations:
            _set(relation.data[self._charm.app], _KEY_ORCHESTRATOR, value)

    @staticmethod
    def remote_role(relation: ops.Relation) -> str:
        """Return the remote application's published orchestrator option.

        That is "true", "false", or "" until its leader has published it.
        """
        if relation.app is None:
            return ""
        return relation.data[relation.app].get(_KEY_ORCHESTRATOR, "")

    # ------------------------------------------------------------------
    # Orchestrator side
    # ------------------------------------------------------------------

    def publish_facts(
        self,
        relation: ops.Relation,
        *,
        snap_channels: dict[str, str] | None,
        service_versions: dict[str, str] | None,
        initialized: bool,
        fingerprint: str | None,
        storage: list[str] | None,
        secret_id: str | None,
        session_timeout: int,
    ) -> None:
        """Leader publishes the cluster facts member units need to join.

        ``None`` leaves a value as last published, for values this hook could
        not read or this unit does not know.
        """
        data = relation.data[self._charm.app]
        if snap_channels is not None:
            _set(data, _KEY_SNAP_CHANNELS, json.dumps(snap_channels, sort_keys=True))
        if service_versions is not None:
            _set(data, _KEY_SERVICE_VERSIONS, json.dumps(service_versions, sort_keys=True))
        _set(data, _KEY_CLUSTER_INITIALIZED, "true" if initialized else "false")
        if fingerprint is not None:
            _set(data, _KEY_CLUSTER_FINGERPRINT, fingerprint)
        if storage is not None:
            _set(data, _KEY_CLUSTER_STORAGE, json.dumps(sorted(storage)))
        if secret_id is not None:
            _set(data, APP_KEY_SECRET_ID, secret_id)
        _set(data, _KEY_SESSION_TIMEOUT, str(session_timeout))

    def published_service_versions(self, relation: ops.Relation) -> dict[str, str]:
        """Return the service versions this application published on ``relation``."""
        return _str_dict(_loads(relation.data[self._charm.app].get(_KEY_SERVICE_VERSIONS), {}))

    def published_fingerprint(self, relation: ops.Relation) -> str:
        """Return the cluster fingerprint this application published on ``relation``."""
        return relation.data[self._charm.app].get(_KEY_CLUSTER_FINGERPRINT, "")

    def member_systems(self, relation: ops.Relation) -> list[MemberSystem]:
        """Return the identity of each member unit on ``relation`` that published one.

        As for peers, one entry per MicroCloud name: the first unit in name
        order that published it.
        """
        systems: list[MemberSystem] = []
        seen: set[str] = set()
        for unit in sorted(relation.units, key=lambda unit: unit.name):
            data = relation.data[unit]
            system = system_from_data(data)
            if system is None or system.name in seen:
                continue
            seen.add(system.name)
            systems.append(
                MemberSystem(
                    unit=unit.name,
                    system=system,
                    ready=is_ready(data),
                    service_versions=_str_dict(_loads(data.get(_KEY_SERVICE_VERSIONS), {})),
                    fingerprint=data.get(_KEY_CLUSTER_FINGERPRINT, ""),
                )
            )
        return systems

    @staticmethod
    def member_units(relation: ops.Relation) -> list[str] | None:
        """Return the unit names the member leader published, or None if it has not."""
        if relation.app is None:
            return None
        raw = relation.data[relation.app].get(_KEY_UNITS)
        if not raw:
            return None
        units = _loads(raw, None)
        return [str(unit) for unit in units] if isinstance(units, list) else None

    def publish_session(self, relation: ops.Relation, session: JoinSession | None) -> None:
        """Leader publishes the join session for ``relation``'s units, or clears it."""
        _set(
            relation.data[self._charm.app],
            APP_KEY_SESSION,
            None if session is None else session.to_json(),
        )

    def failures(self, relation: ops.Relation) -> JoinFailures:
        return _parse_failures(relation.data[self._charm.app].get(_KEY_JOIN_FAILURES))

    def record_failures(self, relation: ops.Relation, failures: JoinFailures) -> None:
        _set(
            relation.data[self._charm.app],
            _KEY_JOIN_FAILURES,
            json.dumps(
                {
                    "count": failures.count,
                    "error": failures.error,
                    "candidates": failures.candidates,
                },
                sort_keys=True,
            ),
        )

    # ------------------------------------------------------------------
    # Member side
    # ------------------------------------------------------------------

    def facts(self) -> OrchestratorFacts | None:
        """Return what the orchestrator published, or None without the relation."""
        relation = self.orchestrator_relation()
        if relation is None or relation.app is None:
            return None
        data = relation.data[relation.app]
        channels = _loads(data.get(_KEY_SNAP_CHANNELS), None)
        storage = _loads(data.get(_KEY_CLUSTER_STORAGE), None)
        try:
            timeout = int(data.get(_KEY_SESSION_TIMEOUT) or 300)
        except ValueError:
            timeout = 300
        return OrchestratorFacts(
            snap_channels=_str_dict(channels) if isinstance(channels, dict) else None,
            service_versions=_str_dict(_loads(data.get(_KEY_SERVICE_VERSIONS), {})),
            initialized=data.get(_KEY_CLUSTER_INITIALIZED) == "true",
            fingerprint=data.get(_KEY_CLUSTER_FINGERPRINT, ""),
            storage=[str(kind) for kind in storage] if isinstance(storage, list) else None,
            secret_id=data.get(APP_KEY_SECRET_ID, ""),
            session=JoinSession.from_json(data.get(APP_KEY_SESSION)),
            session_timeout=timeout,
            failures=_parse_failures(data.get(_KEY_JOIN_FAILURES)),
        )

    def publish_member(
        self,
        identity: dict[str, str],
        *,
        service_versions: dict[str, str] | None = None,
        fingerprint: str | None = None,
        ready: bool | None = None,
    ) -> None:
        """Member unit publishes its identity on the orchestrator relation.

        ``ready`` marks the unit ready to be listed in a session, or withdraws
        that mark; None leaves it as it is.
        """
        relation = self.orchestrator_relation()
        if relation is None:
            return
        data = relation.data[self._charm.unit]
        for key, value in identity.items():
            _set(data, key, value)
        if ready is not None:
            for key, value in ready_fields().items():
                _set(data, key, value if ready else None)
        if service_versions is not None:
            _set(data, _KEY_SERVICE_VERSIONS, json.dumps(service_versions, sort_keys=True))
        if fingerprint is not None:
            _set(data, _KEY_CLUSTER_FINGERPRINT, fingerprint)

    def publish_units(self, units: list[str]) -> None:
        """Member leader publishes the names of its application's units."""
        relation = self.orchestrator_relation()
        if relation is None or not self._charm.unit.is_leader():
            return
        _set(relation.data[self._charm.app], _KEY_UNITS, json.dumps(sorted(units)))

    def passphrase(self, facts: OrchestratorFacts) -> str | None:
        """Read the session passphrase the orchestrator granted, or None."""
        return read_passphrase(self._charm.model, facts.secret_id)


def _parse_failures(raw: str | None) -> JoinFailures:
    data = _loads(raw, {})
    if not isinstance(data, dict):
        return JoinFailures()
    try:
        return JoinFailures(
            count=int(data.get("count", 0)),
            error=str(data.get("error", "")),
            candidates=_str_dict(data.get("candidates")),
        )
    except (TypeError, ValueError):
        return JoinFailures()
