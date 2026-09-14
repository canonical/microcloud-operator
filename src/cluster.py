# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Peer-relation coordination for the microcloud charm.

Responsibilities
----------------
- Publish this unit's MicroCloud identity (hostname + bind address, plus
  any attached "local"/"ceph" Juju storage device paths) on the peer
  relation databag.
- Collect the identities of all peer units so the leader can assemble the
  full ``systems`` list for preseed.
- Manage the shared session passphrase via a Juju secret (leader-owned),
  falling back to the configured value when provided.
- Validate consistency between Juju units and MicroCloud members (matched on
  hostname), in both directions.
"""

import json
import logging
import secrets
from dataclasses import dataclass, field

import ops

logger = logging.getLogger(__name__)

PEER_RELATION = "cluster"

# Peer databag keys (per-unit).
_KEY_NAME = "microcloud-name"
_KEY_ADDRESS = "microcloud-address"
_KEY_OVN_UPLINK_INTERFACE = "microcloud-ovn-uplink-interface"
_KEY_OVN_UNDERLAY_IP = "microcloud-ovn-underlay-ip"
_KEY_STORAGE_LOCAL_PATH = "microcloud-storage-local-path"
_KEY_STORAGE_CEPH_PATHS = "microcloud-storage-ceph-paths"
_KEY_READY = "microcloud-ready"

# App databag keys / secret label (leader-owned).
_APP_KEY_SECRET_ID = "session-passphrase-secret-id"
_SECRET_LABEL = "microcloud-session-passphrase"
_SECRET_FIELD = "passphrase"
_APP_KEY_SESSION = "join-session"


@dataclass
class PeerSystem:
    """A peer unit's published MicroCloud identity."""

    name: str
    address: str
    ovn_uplink_interface: str = ""
    ovn_underlay_ip: str = ""
    storage_local_path: str = ""
    storage_ceph_paths: list[str] = field(default_factory=list)


@dataclass
class JoinSession:
    """A join session the leader has opened.

    ``systems`` names every system listed in the session's preseed, which
    joiners render verbatim so that every document matches the
    initiator's. ``deadline`` (seconds since the epoch) is when the session
    has certainly ended, after which a new leader may open another.
    """

    id: str
    address: str
    systems: list[str]
    deadline: float


class ClusterCoordinator:
    """Helper that reads/writes the peer relation for the charm."""

    def __init__(self, charm: ops.CharmBase) -> None:
        self._charm = charm

    # ------------------------------------------------------------------
    # Relation access
    # ------------------------------------------------------------------

    @property
    def relation(self) -> ops.Relation | None:
        return self._charm.model.get_relation(PEER_RELATION)

    def is_ready(self) -> bool:
        """Return True if the peer relation exists."""
        return self.relation is not None

    # ------------------------------------------------------------------
    # Unit identity
    # ------------------------------------------------------------------

    def publish_identity(
        self,
        name: str,
        address: str,
        ovn_uplink_interface: str = "",
        ovn_underlay_ip: str = "",
        storage_local_path: str = "",
        storage_ceph_paths: list[str] | None = None,
    ) -> None:
        """Publish this unit's MicroCloud identity on the peer databag.

        ``ovn_uplink_interface``, ``ovn_underlay_ip``, and the storage
        device paths are naturally per-unit (derived from this unit's own
        space bindings / attached Juju storage volumes), so they are
        published and collected the same way as name/address, rather than
        being a single value shared application-wide.
        """
        relation = self.relation
        if relation is None:
            return
        relation.data[self._charm.unit][_KEY_NAME] = name
        relation.data[self._charm.unit][_KEY_ADDRESS] = address
        relation.data[self._charm.unit][_KEY_OVN_UPLINK_INTERFACE] = ovn_uplink_interface
        relation.data[self._charm.unit][_KEY_OVN_UNDERLAY_IP] = ovn_underlay_ip
        relation.data[self._charm.unit][_KEY_STORAGE_LOCAL_PATH] = storage_local_path
        relation.data[self._charm.unit][_KEY_STORAGE_CEPH_PATHS] = json.dumps(
            storage_ceph_paths or []
        )

    def all_members(self) -> list[tuple[str, str]]:
        """Return (name, address) for every peer unit that has published, plus self.

        Only entries with both a name and an address are returned.
        """
        return [(system.name, system.address) for system in self.all_systems()]

    def all_systems(self) -> list[PeerSystem]:
        """Return the published identity of every peer unit, plus self.

        Covers every peer unit that has published a name and address, plus
        self. Only entries with both a name and an address are returned; the
        OVN and storage fields may be empty if unset or not applicable.
        """
        relation = self.relation
        if relation is None:
            return []

        members: list[PeerSystem] = []
        units = list(relation.units) + [self._charm.unit]
        for unit in units:
            data = relation.data.get(unit, {})
            name = data.get(_KEY_NAME, "")
            address = data.get(_KEY_ADDRESS, "")
            ovn_uplink_interface = data.get(_KEY_OVN_UPLINK_INTERFACE, "")
            ovn_underlay_ip = data.get(_KEY_OVN_UNDERLAY_IP, "")
            storage_local_path = data.get(_KEY_STORAGE_LOCAL_PATH, "")
            try:
                storage_ceph_paths = json.loads(data.get(_KEY_STORAGE_CEPH_PATHS) or "[]")
            except (json.JSONDecodeError, TypeError):
                storage_ceph_paths = []
            if name and address:
                members.append(
                    PeerSystem(
                        name=name,
                        address=address,
                        ovn_uplink_interface=ovn_uplink_interface,
                        ovn_underlay_ip=ovn_underlay_ip,
                        storage_local_path=storage_local_path,
                        storage_ceph_paths=storage_ceph_paths,
                    )
                )
        # De-duplicate while preserving order.
        seen: set[str] = set()
        unique: list[PeerSystem] = []
        for system in members:
            if system.name not in seen:
                seen.add(system.name)
                unique.append(system)
        return unique

    def expected_unit_count(self) -> int:
        """Total number of units targeted for this application (e.g. -n 3).

        Uses ``planned_units()`` rather than ``len(relation.units) + 1``:
        the peer relation only reflects units that have already joined,
        which can transiently undercount the target scale if this hook
        runs before Juju has delivered relation-joined for all peers
        (causing the leader to bootstrap a single-node cluster too early).
        """
        return self._charm.app.planned_units()

    def all_identities_published(self) -> bool:
        """Return True once every peer unit (and self) has published identity."""
        return len(self.all_members()) >= self.expected_unit_count()

    def publish_ready(self) -> None:
        """Mark this unit as ready to run "microcloud preseed".

        "Ready" means every prerequisite gate before bootstrapping has
        passed: the local MicroCloud daemon is up, every peer has
        published its identity, and the session passphrase is known. Used
        so the initiator can wait for every unit to reach this point
        before starting its session and notifying joiners, instead of
        firing before some units have even installed their snaps yet.
        """
        relation = self.relation
        if relation is None:
            return
        relation.data[self._charm.unit][_KEY_READY] = "true"

    def all_ready(self) -> bool:
        """Return True once every peer unit (and self) has published ready."""
        relation = self.relation
        if relation is None:
            return False
        units = list(relation.units) + [self._charm.unit]
        ready_count = sum(
            1 for unit in units if relation.data.get(unit, {}).get(_KEY_READY) == "true"
        )
        return ready_count >= self.expected_unit_count()

    def pending_systems(self, members: set[str] | None = None) -> list[PeerSystem]:
        """Return the published systems whose MicroCloud is not yet clustered.

        ``members`` is the cluster's own member list, which only a unit that
        is already clustered can read, and it takes precedence when given.
        Each peer's published flag lags behind: it is computed at the start
        of a hook, before that hook runs "microcloud preseed", so a unit
        that has just joined still reads as unclustered until its next
        hook. Trusting the flag there would have the initiator open a
        session for units that will never dial in.
        """
        if members is not None:
            return [system for system in self.all_systems() if system.name not in members]
        return [system for system in self.all_systems() if not system.initialized]

    def any_initialized(self) -> bool:
        """Return True if any unit has published that it is clustered.

        The flag can lag behind (see ``pending_systems``) but never leads,
        so True reliably means a cluster already exists.
        """
        return any(system.initialized for system in self.all_systems())

    # ------------------------------------------------------------------
    # Join session (leader-owned, app databag)
    # ------------------------------------------------------------------

    def publish_session(self, session: JoinSession | None) -> None:
        """Leader publishes the join session it has opened, or clears it.

        Publishing is what tells the joiners to dial in: the write commits
        when the leader's hook exits, and a change to application data
        triggers relation-changed on every other unit. Only the leader may
        write application data.
        """
        relation = self.relation
        if relation is None or not self._charm.unit.is_leader():
            return
        app_data = relation.data[self._charm.app]
        if session is None:
            app_data.pop(_APP_KEY_SESSION, None)
            return
        app_data[_APP_KEY_SESSION] = json.dumps(
            {
                "id": session.id,
                "address": session.address,
                "systems": session.systems,
                "deadline": session.deadline,
            }
        )

    def session(self) -> JoinSession | None:
        """Return the published join session, or None if there is none."""
        relation = self.relation
        if relation is None:
            return None
        raw = relation.data[self._charm.app].get(_APP_KEY_SESSION)
        if not raw:
            return None
        try:
            data = json.loads(raw)
            return JoinSession(
                id=str(data["id"]),
                address=str(data["address"]),
                systems=[str(name) for name in data["systems"]],
                deadline=float(data["deadline"]),
            )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            logger.warning("Ignoring malformed join session data: %r", raw)
            return None

    # ------------------------------------------------------------------
    # Session passphrase (Juju secret, leader-owned)
    # ------------------------------------------------------------------

    def ensure_passphrase(self) -> str | None:
        """Return the shared session passphrase.

        The leader generates a random passphrase and stores it in a Juju
        secret shared with the peer relation. Non-leader units read it back
        from the secret. Returns None if the passphrase is not yet available
        (e.g. a non-leader unit before the leader has created the secret).
        """
        relation = self.relation
        if relation is None:
            return None

        if self._charm.unit.is_leader():
            return self._leader_ensure_secret(relation)

        return self._read_secret(relation)

    def _leader_ensure_secret(self, relation: ops.Relation) -> str:
        app_data = relation.data[self._charm.app]
        secret_id = app_data.get(_APP_KEY_SECRET_ID)
        if secret_id:
            secret = self._charm.model.get_secret(id=secret_id)
        else:
            # Recover from a prior partially-completed attempt: `secret-add`
            # may already have been applied even though this hook crashed
            # on a later step before recording the secret id in app data.
            # Reuse the existing secret by label instead of trying to
            # create a duplicate (which fails with "already exists").
            try:
                secret = self._charm.model.get_secret(label=_SECRET_LABEL)
            except ops.SecretNotFoundError:
                passphrase = secrets.token_urlsafe(24)
                secret = self._charm.app.add_secret(
                    {_SECRET_FIELD: passphrase},
                    label=_SECRET_LABEL,
                )
            app_data[_APP_KEY_SECRET_ID] = secret.id or ""

        # Grant read access per peer unit, not at application/relation
        # scope: for a peer relation the "remote application" is the same
        # application that owns this secret, so an application-scoped
        # grant collides with the owner's own implicit grant
        # (InvalidSecretPermissionChange / "cannot change secret
        # permission scope"). Granting a unit that already has access is a
        # documented no-op, so it is safe to repeat this every hook as new
        # peers join.
        for peer_unit in relation.units:
            try:
                secret.grant(relation, unit=peer_unit)
            except ops.ModelError as exc:
                logger.warning(
                    "Cannot grant session-passphrase secret to %s: %s", peer_unit.name, exc
                )

        return secret.get_content(refresh=True)[_SECRET_FIELD]

    def _read_secret(self, relation: ops.Relation) -> str | None:
        secret_id = relation.data[self._charm.app].get(_APP_KEY_SECRET_ID)
        if not secret_id:
            return None
        try:
            secret = self._charm.model.get_secret(id=secret_id)
            return secret.get_content(refresh=True)[_SECRET_FIELD]
        except ops.SecretNotFoundError:
            return None


def validate_membership(
    juju_hostnames: set[str],
    member_names: set[str],
) -> list[str]:
    """Validate Juju units against MicroCloud members, matched on hostname.

    Returns a list of human-readable problems (empty if consistent):

    - A MicroCloud member with no corresponding Juju unit.
    - A Juju unit whose node is not a MicroCloud member.
    """
    problems: list[str] = []

    missing_units = sorted(member_names - juju_hostnames)
    for name in missing_units:
        problems.append(f"MicroCloud member {name!r} is not deployed as a Juju unit")

    non_members = sorted(juju_hostnames - member_names)
    for name in non_members:
        problems.append(f"Juju unit on {name!r} is not a MicroCloud member")

    return problems
