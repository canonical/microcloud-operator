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
_KEY_STORAGE_WIPE = "microcloud-storage-wipe"
_KEY_STORAGE_ENCRYPT = "microcloud-storage-encrypt"
_KEY_READY = "microcloud-ready"
_KEY_INITIALIZED = "microcloud-initialized"

# App databag keys / secret label (leader-owned). The "microcloud_cluster"
# relation publishes the secret ID and the session under the same keys.
APP_KEY_SECRET_ID = "session-passphrase-secret-id"
_SECRET_LABEL = "microcloud-session-passphrase"
_SECRET_FIELD = "passphrase"
APP_KEY_SESSION = "join-session"
_APP_KEY_CLUSTER_MODE = "cluster-mode"
_APP_KEY_JOINED_UNITS = "joined-units"

# Values of "cluster-mode": which side of the "microcloud_cluster" relation
# this application formed or joined its cluster as.
MODE_ORCHESTRATOR = "orchestrator"
MODE_MEMBER = "member"


@dataclass
class PeerSystem:
    """A peer unit's published MicroCloud identity."""

    name: str
    address: str
    ovn_uplink_interface: str = ""
    ovn_underlay_ip: str = ""
    storage_local_path: str = ""
    storage_ceph_paths: list[str] = field(default_factory=list)
    storage_wipe: bool = False
    storage_encrypt: bool = False
    initialized: bool = False


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
    # The "members" relation whose units the session lists, or None for a
    # session of this application's own units.
    relation_id: int | None = None

    def to_json(self) -> str:
        data: dict = {
            "id": self.id,
            "address": self.address,
            "systems": self.systems,
            "deadline": self.deadline,
        }
        if self.relation_id is not None:
            data["relation-id"] = self.relation_id
        return json.dumps(data, sort_keys=True)

    @classmethod
    def from_json(cls, raw: str | None) -> "JoinSession | None":
        """Parse a published session, or return None if absent or malformed."""
        if not raw:
            return None
        try:
            data = json.loads(raw)
            relation_id = data.get("relation-id")
            return cls(
                id=str(data["id"]),
                address=str(data["address"]),
                systems=[str(name) for name in data["systems"]],
                deadline=float(data["deadline"]),
                relation_id=None if relation_id is None else int(relation_id),
            )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError, AttributeError):
            logger.warning("Ignoring malformed join session data: %r", raw)
            return None


def identity_fields(
    name: str,
    address: str,
    ovn_uplink_interface: str = "",
    ovn_underlay_ip: str = "",
    storage_local_path: str = "",
    storage_ceph_paths: list[str] | None = None,
    storage_wipe: bool = False,
    storage_encrypt: bool = False,
    initialized: bool = False,
) -> dict[str, str]:
    """Return the databag fields that publish a unit's MicroCloud identity."""
    return {
        _KEY_NAME: name,
        _KEY_ADDRESS: address,
        _KEY_OVN_UPLINK_INTERFACE: ovn_uplink_interface,
        _KEY_OVN_UNDERLAY_IP: ovn_underlay_ip,
        _KEY_STORAGE_LOCAL_PATH: storage_local_path,
        _KEY_STORAGE_CEPH_PATHS: json.dumps(storage_ceph_paths or []),
        _KEY_STORAGE_WIPE: "true" if storage_wipe else "false",
        _KEY_STORAGE_ENCRYPT: "true" if storage_encrypt else "false",
        # Whether this unit's own MicroCloud daemon is already clustered. The
        # leader reads this to tell a first bootstrap from growing an existing
        # cluster, and to decide which systems belong in the preseed.
        _KEY_INITIALIZED: "true" if initialized else "false",
    }


def system_from_data(data: "ops.RelationDataContent | dict[str, str]") -> PeerSystem | None:
    """Parse a unit's published identity, or return None without name and address."""
    name = data.get(_KEY_NAME, "")
    address = data.get(_KEY_ADDRESS, "")
    if not name or not address:
        return None
    try:
        storage_ceph_paths = json.loads(data.get(_KEY_STORAGE_CEPH_PATHS) or "[]")
    except (json.JSONDecodeError, TypeError):
        storage_ceph_paths = []
    return PeerSystem(
        name=name,
        address=address,
        ovn_uplink_interface=data.get(_KEY_OVN_UPLINK_INTERFACE, ""),
        ovn_underlay_ip=data.get(_KEY_OVN_UNDERLAY_IP, ""),
        storage_local_path=data.get(_KEY_STORAGE_LOCAL_PATH, ""),
        storage_ceph_paths=storage_ceph_paths,
        storage_wipe=data.get(_KEY_STORAGE_WIPE) == "true",
        storage_encrypt=data.get(_KEY_STORAGE_ENCRYPT) == "true",
        initialized=data.get(_KEY_INITIALIZED) == "true",
    )


def read_passphrase(model: ops.Model, secret_id: str) -> str | None:
    """Read the session passphrase from the secret ``secret_id``.

    Returns None while it cannot be read, such as before this unit has been
    granted the secret.
    """
    if not secret_id:
        return None
    try:
        return model.get_secret(id=secret_id).get_content(refresh=True)[_SECRET_FIELD]
    except (ops.ModelError, KeyError) as exc:
        logger.info("Cannot read the session passphrase yet: %s", exc)
        return None


def is_ready(data: "ops.RelationDataContent | dict[str, str]") -> bool:
    """Return True if a unit's databag says it is ready to be listed in a session."""
    return data.get(_KEY_READY) == "true"


def ready_fields() -> dict[str, str]:
    """Return the databag fields that mark a unit ready."""
    return {_KEY_READY: "true"}


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
        storage_wipe: bool = False,
        storage_encrypt: bool = False,
        initialized: bool = False,
    ) -> None:
        """Publish this unit's MicroCloud identity on the peer databag.

        ``ovn_uplink_interface``, ``ovn_underlay_ip``, and the storage
        device paths are naturally per-unit (derived from this unit's own
        space bindings / attached Juju storage volumes), so they are
        published and collected the same way as name/address, rather than
        being a single value shared application-wide. ``storage_wipe`` and
        ``storage_encrypt`` travel with them because the initiator renders
        them for this unit's disks, and it may be of another application.
        """
        relation = self.relation
        if relation is None:
            return
        relation.data[self._charm.unit].update(
            identity_fields(
                name,
                address,
                ovn_uplink_interface=ovn_uplink_interface,
                ovn_underlay_ip=ovn_underlay_ip,
                storage_local_path=storage_local_path,
                storage_ceph_paths=storage_ceph_paths,
                storage_wipe=storage_wipe,
                storage_encrypt=storage_encrypt,
                initialized=initialized,
            )
        )

    def all_members(self) -> list[tuple[str, str]]:
        """Return (name, address) for every peer unit that has published, plus self.

        Only entries with both a name and an address are returned.
        """
        return [(system.name, system.address) for system in self.peer_systems()]

    def peer_systems(self) -> list[PeerSystem]:
        """Return the published identity of every peer unit, plus self.

        Covers every peer unit that has published a name and address, plus
        self. Only entries with both a name and an address are returned; the
        OVN and storage fields may be empty if unset or not applicable. Units
        of other applications on the "members" relation are not included.
        """
        relation = self.relation
        if relation is None:
            return []

        unique: list[PeerSystem] = []
        seen: set[str] = set()
        for unit in list(relation.units) + [self._charm.unit]:
            system = system_from_data(relation.data.get(unit, {}))
            # De-duplicate while preserving order.
            if system is not None and system.name not in seen:
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
        relation.data[self._charm.unit].update(ready_fields())

    def all_ready(self, pending: list[PeerSystem] | None = None) -> bool:
        """Return True once every unit that is to be listed has published ready.

        That is every peer unit (and self), or with ``pending`` only the
        units still waiting to join an existing cluster.
        """
        relation = self.relation
        if relation is None:
            return False
        units = list(relation.units) + [self._charm.unit]
        if pending is not None:
            names = {system.name for system in pending}
            return all(
                relation.data.get(unit, {}).get(_KEY_READY) == "true"
                for unit in units
                if relation.data.get(unit, {}).get(_KEY_NAME) in names
            )
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
            return [system for system in self.peer_systems() if system.name not in members]
        return [system for system in self.peer_systems() if not system.initialized]

    def any_initialized(self) -> bool:
        """Return True if any unit has published that it is clustered.

        The flag can lag behind (see ``pending_systems``) but never leads,
        so True reliably means a cluster already exists.
        """
        return any(system.initialized for system in self.peer_systems())

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
            app_data.pop(APP_KEY_SESSION, None)
            return
        app_data[APP_KEY_SESSION] = session.to_json()

    def session(self) -> JoinSession | None:
        """Return the published join session, or None if there is none."""
        relation = self.relation
        if relation is None:
            return None
        return JoinSession.from_json(relation.data[self._charm.app].get(APP_KEY_SESSION))

    # ------------------------------------------------------------------
    # Cluster mode and member units (leader-owned, app databag)
    # ------------------------------------------------------------------

    def cluster_mode(self) -> str:
        """Return the recorded cluster mode, or "" if none is recorded yet."""
        relation = self.relation
        if relation is None:
            return ""
        return relation.data[self._charm.app].get(_APP_KEY_CLUSTER_MODE, "")

    def record_cluster_mode(self, mode: str) -> None:
        """Leader records which side of "microcloud_cluster" this application is on."""
        relation = self.relation
        if relation is None or not self._charm.unit.is_leader():
            return
        relation.data[self._charm.app][_APP_KEY_CLUSTER_MODE] = mode

    def joined_units(self) -> dict[str, str]:
        """Return {MicroCloud member name: unit name} for joined member units.

        Kept by the orchestrator leader, so that member units stay known after
        their whole "members" relation is removed.
        """
        relation = self.relation
        if relation is None:
            return {}
        try:
            data = json.loads(relation.data[self._charm.app].get(_APP_KEY_JOINED_UNITS) or "{}")
        except json.JSONDecodeError:
            return {}
        return {str(k): str(v) for k, v in data.items()} if isinstance(data, dict) else {}

    def record_joined_units(self, units: dict[str, str]) -> None:
        """Leader records the joined member units."""
        relation = self.relation
        if relation is None or not self._charm.unit.is_leader():
            return
        app_data = relation.data[self._charm.app]
        if units:
            app_data[_APP_KEY_JOINED_UNITS] = json.dumps(units, sort_keys=True)
        else:
            app_data.pop(_APP_KEY_JOINED_UNITS, None)

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
            return self._leader_ensure_secret(relation).get_content(refresh=True)[_SECRET_FIELD]

        return self._read_secret(relation)

    def grant_passphrase(self, relations: list[ops.Relation]) -> dict[int, str]:
        """Leader grants the passphrase secret to each relation's remote application.

        Juju revokes a grant once its relation is removed. Returns the secret
        ID by relation ID, for the relations it was granted to. It is left out
        for any it could not be, and they are tried again on the next hook.
        """
        peer = self.relation
        if peer is None or not relations or not self._charm.unit.is_leader():
            return {}
        try:
            secret = self._leader_ensure_secret(peer)
        except ops.ModelError as exc:
            logger.warning("Cannot read the session passphrase secret: %s", exc)
            return {}
        granted: dict[int, str] = {}
        for relation in relations:
            try:
                secret.grant(relation)
            except ops.ModelError as exc:
                logger.warning("Cannot grant the passphrase to %s: %s", relation, exc)
            else:
                granted[relation.id] = secret.id or ""
        return granted

    def _leader_ensure_secret(self, relation: ops.Relation) -> ops.Secret:
        app_data = relation.data[self._charm.app]
        secret_id = app_data.get(APP_KEY_SECRET_ID)
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
            app_data[APP_KEY_SECRET_ID] = secret.id or ""

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

        return secret

    def _read_secret(self, relation: ops.Relation) -> str | None:
        secret_id = relation.data[self._charm.app].get(APP_KEY_SECRET_ID, "")
        return read_passphrase(self._charm.model, secret_id)


def validate_membership(
    juju_hostnames: set[str],
    member_names: set[str],
    known: set[str] | None = None,
) -> list[str]:
    """Validate Juju units against MicroCloud members, matched on hostname.

    ``juju_hostnames`` are the units expected to be MicroCloud members.
    ``known`` adds units allowed to be members without being expected to,
    such as member units still joining or of a removed "members" relation.

    Returns a list of human-readable problems (empty if consistent):

    - A MicroCloud member with no corresponding Juju unit.
    - A Juju unit whose node is not a MicroCloud member.
    """
    problems: list[str] = []

    missing_units = sorted(member_names - juju_hostnames - (known or set()))
    for name in missing_units:
        problems.append(f"MicroCloud member {name!r} is not deployed as a Juju unit")

    non_members = sorted(juju_hostnames - member_names)
    for name in non_members:
        problems.append(f"Juju unit on {name!r} is not a MicroCloud member")

    return problems
