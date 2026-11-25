# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""The "lxd_dpu" relation between this charm and the DPUs in its hosts.

Each DPU in a host runs the microcloud-dpu charm, whose "host" endpoint
relates to this charm's "dpus" endpoint. Every pairing is between one DPU unit
and the MicroCloud unit of the LXD member it sits in, so each side writes only
its own unit databag; there is no application data.

Relation data
-------------
Each DPU unit publishes ``name`` (its constituent name), ``host`` (the LXD
member it belongs to), ``address`` (its agent's address) and, once it has
joined, ``joined``.

Each MicroCloud unit publishes ``member`` (its LXD member name),
``oam-address`` (its address on this endpoint's binding, the only source the
DPU agents accept) and ``tokens``: a JSON object that maps each DPU unit
waiting to join to the ID of a secret holding its join token.

Join tokens
-----------
A MicroCloud unit issues a token only to a DPU whose ``host`` is its own
member, because LXD accepts a join only on the member that issued the token.
The token is a credential until used, so it goes in a secret owned by this
unit and granted to that DPU unit only, which expires with the token. Once the
DPU has joined and LXD trusts it, the unit removes the secret.
"""

import contextlib
import json
import logging
from dataclasses import dataclass

import ops

import lxd_cluster

logger = logging.getLogger(__name__)

DPUS = "dpus"

_KEY_MEMBER = "member"
_KEY_OAM_ADDRESS = "oam-address"
_KEY_TOKENS = "tokens"

_TOKEN_LABEL_PREFIX = "lxd-dpu-token-"
_TOKEN_FIELD = "token"


@dataclass(frozen=True)
class DPU:
    """A DPU unit, as its databag describes it."""

    unit: ops.Unit
    name: str
    host: str
    address: str
    joined: bool


def _dpu(unit: ops.Unit, data: ops.RelationDataContent) -> DPU | None:
    name, host = data.get("name", ""), data.get("host", "")
    if not name or not host:
        return None
    return DPU(
        unit=unit,
        name=name,
        host=host,
        address=data.get("address", ""),
        joined=data.get("joined") == "true",
    )


def _set(data: ops.RelationDataContent, key: str, value: str) -> None:
    """Write ``value`` under ``key`` if it changed; an empty value removes the key."""
    if data.get(key, "") == value:
        return
    if value:
        data[key] = value
    else:
        data.pop(key, None)


class DPURelation:
    """This unit's side of the "dpus" relation."""

    def __init__(self, charm: ops.CharmBase) -> None:
        self._charm = charm

    def relation(self) -> ops.Relation | None:
        """Return the "dpus" relation, or None if no DPU application is related."""
        return self._charm.model.get_relation(DPUS)

    def reconcile(self, member: str, oam_address: str) -> None:
        """Publish this unit's member and OAM address, and pair the DPUs it owns."""
        relation = self.relation()
        if relation is None:
            return

        data = relation.data[self._charm.unit]
        _set(data, _KEY_MEMBER, member)
        _set(data, _KEY_OAM_ADDRESS, oam_address)

        tokens = self._tokens(data)
        trusted = {
            constituent.name
            for constituent in lxd_cluster.constituents()
            if constituent.member == member and constituent.trusted
        }

        # Constituent name -> DPU unit, to spot two DPUs with one name.
        names: dict[str, str] = {}
        for unit in sorted(relation.units, key=lambda unit: unit.name):
            dpu = _dpu(unit, relation.data[unit])
            if dpu is None or dpu.host != member:
                continue

            if dpu.name in names:
                # LXD would refuse the second token anyway.
                logger.warning(
                    "DPUs %s and %s are both named %s", names[dpu.name], unit.name, dpu.name
                )
                continue
            names[dpu.name] = unit.name

            pending = tokens.get(unit.name)
            if dpu.name in trusted:
                if pending and dpu.joined:
                    self._remove_secret(pending)
                    del tokens[unit.name]
                continue

            if pending:
                continue

            secret_id = self._issue(relation, dpu)
            if secret_id:
                tokens[unit.name] = secret_id

        _set(data, _KEY_TOKENS, json.dumps(tokens, sort_keys=True) if tokens else "")

    def expired(self, secret: ops.Secret) -> bool:
        """Drop a join token that expired unused. Returns False if it is not one.

        LXD releases the name of an expired token, so the next reconcile issues
        a new one.
        """
        secret_id = secret.id or ""
        relation = self.relation()
        data = relation.data[self._charm.unit] if relation is not None else None
        tokens = self._tokens(data) if data is not None else {}
        listed = secret_id in tokens.values()
        if not listed and not (secret.label or "").startswith(_TOKEN_LABEL_PREFIX):
            return False

        secret.remove_all_revisions()
        if data is not None and listed:
            tokens = {unit: token for unit, token in tokens.items() if token != secret_id}
            _set(data, _KEY_TOKENS, json.dumps(tokens, sort_keys=True) if tokens else "")
        return True

    @staticmethod
    def _tokens(data: ops.RelationDataContent) -> dict[str, str]:
        try:
            tokens = json.loads(data.get(_KEY_TOKENS) or "{}")
        except json.JSONDecodeError:
            return {}
        return {str(unit): str(token) for unit, token in tokens.items()}

    def _issue(self, relation: ops.Relation, dpu: DPU) -> str | None:
        """Issue a join token for ``dpu`` and share it in a secret; return the secret ID."""
        try:
            token = lxd_cluster.add_constituent(dpu.name)
        except lxd_cluster.ConstituentConflictError as exc:
            # Another DPU holds the name, or a token issued outside the charm is
            # pending: LXD keeps it until it expires.
            logger.warning("Cannot issue a join token for %s: %s", dpu.name, exc)
            return None
        except lxd_cluster.LXDClusterError as exc:
            logger.error("Cannot issue a join token for %s: %s", dpu.name, exc)
            return None

        content = {_TOKEN_FIELD: token}
        label = _TOKEN_LABEL_PREFIX + dpu.name
        try:
            expire = lxd_cluster.token_expiry(token)
            try:
                # Left from a hook that failed after creating it.
                secret = self._charm.model.get_secret(label=label)
                secret.set_content(content)
                secret.set_info(expire=expire)
            except ops.SecretNotFoundError:
                secret = self._charm.unit.add_secret(content, label=label, expire=expire)
            secret.grant(relation, unit=dpu.unit)
        except (lxd_cluster.LXDClusterError, ops.ModelError) as exc:
            logger.error("Cannot share the join token for %s: %s", dpu.name, exc)
            return None

        logger.info("Issued a join token for %s to %s", dpu.name, dpu.unit.name)
        return secret.id

    def _remove_secret(self, secret_id: str) -> None:
        with contextlib.suppress(ops.SecretNotFoundError):
            self._charm.model.get_secret(id=secret_id).remove_all_revisions()
