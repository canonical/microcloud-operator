# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Configure the DPU bridge using system OVS without installing MicroOVN."""

import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

import ops

SYS_NET = Path("/sys/class/net")
NETWORKD_FILE = Path("/etc/systemd/network/90-microcloud-dpu.network")


class DPUError(Exception):
    """Raised when DPU bridge reconciliation fails."""


def _run(*args: str, missing_ok: bool = False) -> str | None:
    try:
        result = subprocess.run(args, text=True, capture_output=True, timeout=300, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        raise DPUError(f"{' '.join(args)}: {exc}") from exc
    if result.returncode:
        if missing_ok and (
            result.returncode == 1 or (result.returncode == 2 and "br-exists" in args)
        ):
            return None
        raise DPUError(f"{' '.join(args)}: {result.stderr.strip()}")
    return result.stdout.strip()


def validate(bridge: str, bond: str, representor: str) -> None:
    """Require safe interface names and an existing MAAS active-backup bond."""
    for name in (bridge, bond, representor):
        if not re.fullmatch(r"[a-zA-Z0-9_.-]{1,15}", name):
            raise DPUError(f"Invalid DPU interface name: {name!r}")
    if len({bridge, bond, representor}) != 3:
        raise DPUError("DPU bridge, bond and representor names must differ")
    if {bridge, bond, representor} & {"lo", "eth0", "oob_net0"}:
        raise DPUError("DPU bridge configuration must not use OAM or lo")
    try:
        mode = (SYS_NET / bond / "bonding/mode").read_text().split()[0]
        slaves = (SYS_NET / bond / "bonding/slaves").read_text().split()
    except (OSError, IndexError) as exc:
        raise DPUError(f"MAAS bond {bond} is unavailable: {exc}") from exc
    if mode != "active-backup" or set(slaves) != {"p0", "p1"}:
        raise DPUError(f"MAAS bond {bond} must use active-backup with p0 and p1")
    if not (SYS_NET / representor).exists():
        raise DPUError(f"Host representor {representor} does not exist")


def ensure_bridge(bridge: str, bond: str, representor: str) -> None:
    """Join the MAAS bond and host representor to a persistent system OVS bridge."""
    validate(bridge, bond, representor)
    if shutil.which("ovs-vsctl") is None:
        _run("apt-get", "update")
        _run(
            "env",
            "DEBIAN_FRONTEND=noninteractive",
            "apt-get",
            "install",
            "-y",
            "openvswitch-switch",
        )
    _run("systemctl", "enable", "--now", "openvswitch-switch")
    for port in (bond, representor):
        owner = _run("ovs-vsctl", "--timeout=15", "port-to-br", port, missing_ok=True)
        if owner and owner != bridge:
            raise DPUError(f"{port} already belongs to bridge {owner}")
    existing = _run("ovs-vsctl", "--timeout=15", "br-exists", bridge, missing_ok=True)
    if existing is not None:
        datapath = _run("ovs-vsctl", "--timeout=15", "get", "Bridge", bridge, "datapath_type")
        if datapath not in {'""', '"system"', "system", ""}:
            raise DPUError(f"{bridge} has a non-system datapath; refusing to replace it")
    _run(
        "ovs-vsctl",
        "--timeout=15",
        "--may-exist",
        "add-br",
        bridge,
        "--",
        "set",
        "Bridge",
        bridge,
        "datapath_type=system",
        "--",
        "--may-exist",
        "add-port",
        bridge,
        bond,
        "--",
        "--may-exist",
        "add-port",
        bridge,
        representor,
    )
    content = (
        f"[Match]\nName={bridge}\n"
        "[Link]\nRequiredForOnline=no\nActivationPolicy=always-up\n"
        "[Network]\nDHCP=no\nIPv6AcceptRA=no\nLinkLocalAddressing=no\nConfigureWithoutCarrier=yes\n"
    )
    try:
        if not NETWORKD_FILE.exists() or NETWORKD_FILE.read_text() != content:
            NETWORKD_FILE.parent.mkdir(parents=True, exist_ok=True)
            fd, temporary = tempfile.mkstemp(dir=NETWORKD_FILE.parent, prefix=".microcloud-dpu-")
            try:
                with os.fdopen(fd, "w") as handle:
                    handle.write(content)
                os.chmod(temporary, 0o644)
                os.replace(temporary, NETWORKD_FILE)
            finally:
                Path(temporary).unlink(missing_ok=True)
            _run("networkctl", "reload")
        _run("networkctl", "reconfigure", bridge)
    except OSError as exc:
        raise DPUError(f"Cannot persist DPU bridge interface: {exc}") from exc
    _run("ip", "link", "set", "dev", bridge, "up")


class DPURole(ops.Object):
    """Handle DPU bridge hooks without initializing host services."""

    def __init__(self, charm: ops.CharmBase):
        super().__init__(charm, "dpu")
        self.charm = charm
        for event in (
            charm.on.install,
            charm.on.start,
            charm.on.upgrade_charm,
            charm.on.config_changed,
            charm.on.update_status,
        ):
            self.framework.observe(event, self._reconcile)
        self.framework.observe(charm.on.status_action, self._status)
        for action in (
            charm.on.add_control_plane_role_action,
            charm.on.remove_control_plane_role_action,
            charm.on.dump_metrics_config_action,
        ):
            self.framework.observe(action, self._unsupported)

    def _reconcile(self, event: ops.EventBase) -> None:
        config = self.charm.config
        try:
            if config["node-role"] != "dpu":
                raise DPUError(
                    "node-role cannot change on an existing deployment; redeploy the application"
                )
            if self.model.storages.get("local") or self.model.storages.get("ceph"):
                raise DPUError("DPU units must not receive local or ceph storage")
            ensure_bridge(
                str(config["dpu-bridge"]),
                str(config["dpu-bond"]),
                str(config["dpu-host-representor"]),
            )
        except DPUError as exc:
            self.charm.unit.status = ops.BlockedStatus(str(exc))
            return
        self.charm.unit.status = ops.ActiveStatus(f"DPU bridge {config['dpu-bridge']} ready")

    def _status(self, event: ops.ActionEvent) -> None:
        event.set_results(
            {
                "node-role": "dpu",
                "bridge": str(self.charm.config["dpu-bridge"]),
                "status": self.charm.unit.status.message,
            }
        )

    def _unsupported(self, event: ops.ActionEvent) -> None:
        event.fail("This action requires node-role=host")
