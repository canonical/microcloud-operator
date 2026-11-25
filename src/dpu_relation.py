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

Each MicroCloud unit publishes ``member`` (its LXD member name) and
``oam-address`` (its address on this endpoint's binding, the only source the
DPU agents accept).
"""

import logging

import ops

logger = logging.getLogger(__name__)

DPUS = "dpus"

_KEY_MEMBER = "member"
_KEY_OAM_ADDRESS = "oam-address"


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
        """Publish this unit's LXD member name and OAM address to its DPUs."""
        relation = self.relation()
        if relation is None:
            return

        data = relation.data[self._charm.unit]
        _set(data, _KEY_MEMBER, member)
        _set(data, _KEY_OAM_ADDRESS, oam_address)
