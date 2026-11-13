# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Whether the storage attached to this unit made it into the cluster.

MicroCloud adds a system's disks and creates the storage pools after the
system has joined. When that step fails, for example on a disk that still
holds data, the cluster stays formed without that storage, and
``microcloud status`` reports it as healthy.
"""

import json
import subprocess

import lxd_cluster

# The pools MicroCloud creates for local storage, Ceph and CephFS.
LOCAL_POOL = "local"
CEPH_POOL = "remote"
CEPHFS_POOL = "remote-fs"


class StorageHealthError(Exception):
    """Raised when the cluster's storage cannot be read."""


def configured_osds(hostname: str) -> int:
    """Return how many disks on ``hostname`` MicroCeph runs as OSDs."""
    command = ["microceph", "disk", "list", "--json"]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=60)
        disks = json.loads(result.stdout).get("ConfiguredDisks") or []
    except (OSError, subprocess.SubprocessError) as exc:
        detail = getattr(exc, "stderr", "") or str(exc)
        raise StorageHealthError(f"microceph disk list failed: {detail.strip()}") from exc
    except (json.JSONDecodeError, AttributeError) as exc:
        raise StorageHealthError(f"Invalid JSON from microceph disk list: {exc}") from exc

    return sum(1 for disk in disks if isinstance(disk, dict) and disk.get("location") == hostname)


def problems(
    hostname: str, member: str, *, local: bool, ceph_disks: int, cephfs: bool = False
) -> list[str]:
    """Return what storage of this unit is missing from the cluster.

    ``local`` is whether the unit has local storage attached, and
    ``ceph_disks`` how many Ceph disks. ``cephfs`` is whether CephFS was
    asked for, which MicroCloud sets up only with Ceph disks. ``member`` is
    its LXD member name.

    Raises StorageHealthError or lxd_cluster.LXDClusterError if the cluster's
    storage cannot be read.
    """
    if not local and not ceph_disks:
        return []

    pools = lxd_cluster.storage_pools()
    found = []
    if local and (LOCAL_POOL not in pools or member not in pools[LOCAL_POOL]):
        found.append(f'no "{LOCAL_POOL}" storage pool on {member}')

    if ceph_disks:
        osds = configured_osds(hostname)
        if osds < ceph_disks:
            found.append(f"{osds} of {ceph_disks} Ceph disks on {hostname} are in MicroCeph")
        if CEPH_POOL not in pools:
            found.append(f'no "{CEPH_POOL}" storage pool')
        if cephfs and CEPHFS_POOL not in pools:
            found.append(f'no "{CEPHFS_POOL}" storage pool')
    return found
