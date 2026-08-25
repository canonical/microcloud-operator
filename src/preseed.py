# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Build MicroCloud preseed YAML for the microcloud charm.

The charm deploys in *unicast* mode: the initiator advertises its address via
``initiator_address`` and every system lists an explicit ``address``. No
``lookup_subnet`` is used (multicast is unreliable on Juju/cloud networks).

The same preseed document is rendered identically on every unit; each
MicroCloud daemon selects its own role by matching its hostname/address
against the ``initiator_address`` field.
"""

from dataclasses import dataclass, field

import yaml


@dataclass
class SystemEntry:
    """A single system participating in the cluster."""

    name: str
    address: str
    # Naturally per-machine (NIC naming and Geneve tunnel IP can differ
    # across hardware), unlike the cluster-wide OVN network settings below.
    ovn_uplink_interface: str = ""
    ovn_underlay_ip: str = ""
    # Device paths for Juju storage volumes attached to this specific
    # system (e.g. a MAAS-provisioned physical NVMe disk). These are
    # passed straight through to "microcloud preseed" as explicit paths
    # ("systems[*].storage.local.path" / "systems[*].storage.ceph[*].path")
    # rather than as an LXD resource-filter expression, since Juju has
    # already resolved exactly which block device to use.
    storage_local_path: str = ""
    storage_ceph_paths: list[str] = field(default_factory=list)


@dataclass
class PreseedInputs:
    """All inputs required to render a preseed document."""

    initiator_address: str
    session_passphrase: str
    systems: list[SystemEntry]
    session_timeout: int = 3600

    # Ceph
    with_ceph: bool = True
    ceph_cephfs: bool = False
    ceph_public_network: str = ""
    ceph_internal_network: str = ""

    # OVN
    with_ovn: bool = True
    ovn_ipv4_gateway: str = ""
    ovn_ipv4_range: str = ""
    ovn_ipv6_gateway: str = ""
    ovn_dns_servers: str = ""

    # Storage (applies to any Juju-attached "local"/"ceph" storage paths
    # on the systems above)
    storage_wipe: bool = False
    storage_encrypt: bool = False

    extra_ports: dict = field(default_factory=dict)


def build_preseed(inputs: PreseedInputs) -> dict:
    """Return the preseed document as a plain dict."""
    systems: list[dict] = []
    for sys_entry in inputs.systems:
        entry: dict = {"name": sys_entry.name, "address": sys_entry.address}
        if inputs.with_ovn and sys_entry.ovn_uplink_interface:
            entry["ovn_uplink_interface"] = sys_entry.ovn_uplink_interface
        if inputs.with_ovn and sys_entry.ovn_underlay_ip:
            entry["ovn_underlay_ip"] = sys_entry.ovn_underlay_ip

        storage: dict = {}
        if sys_entry.storage_local_path:
            storage["local"] = _direct_storage(sys_entry.storage_local_path, inputs)
        if inputs.with_ceph and sys_entry.storage_ceph_paths:
            storage["ceph"] = [
                _direct_storage(path, inputs) for path in sys_entry.storage_ceph_paths
            ]
        if storage:
            entry["storage"] = storage

        systems.append(entry)

    doc: dict = {
        "initiator_address": inputs.initiator_address,
        "session_passphrase": inputs.session_passphrase,
        "session_timeout": inputs.session_timeout,
        "systems": systems,
    }

    # ---- Ceph ----
    if inputs.with_ceph:
        ceph: dict = {}
        if inputs.ceph_cephfs:
            ceph["cephfs"] = True
        if inputs.ceph_public_network:
            ceph["public_network"] = inputs.ceph_public_network
        if inputs.ceph_internal_network:
            ceph["internal_network"] = inputs.ceph_internal_network
        if ceph:
            doc["ceph"] = ceph

    # ---- OVN ----
    if inputs.with_ovn and any(s.ovn_uplink_interface for s in inputs.systems):
        ovn: dict = {}
        if inputs.ovn_ipv4_gateway:
            ovn["ipv4_gateway"] = inputs.ovn_ipv4_gateway
        if inputs.ovn_ipv4_range:
            ovn["ipv4_range"] = inputs.ovn_ipv4_range
        if inputs.ovn_ipv6_gateway:
            ovn["ipv6_gateway"] = inputs.ovn_ipv6_gateway
        if inputs.ovn_dns_servers:
            ovn["dns_servers"] = inputs.ovn_dns_servers
        if ovn:
            doc["ovn"] = ovn

    return doc


def _direct_storage(path: str, inputs: PreseedInputs) -> dict:
    """Build a "systems[*].storage.local"/"...ceph[*]" entry for a device path."""
    entry: dict = {"path": path}
    if inputs.storage_wipe:
        entry["wipe"] = True
    if inputs.storage_encrypt:
        entry["encrypt"] = True
    return entry


def render(inputs: PreseedInputs) -> str:
    """Render the preseed document to a YAML string."""
    return yaml.safe_dump(build_preseed(inputs), sort_keys=False, default_flow_style=False)
