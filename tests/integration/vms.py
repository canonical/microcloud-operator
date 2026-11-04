"""LXD VM, snap and MicroCloud preseed helpers for integration tests.

The charm under test is observe-only, so the cluster is formed here with the
MicroCloud CLI exactly as an operator would do it out of band. The helpers are
deliberately independent of the charm's action output.
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import shlex
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

import jubilant
import yaml
from tests.integration.helpers import retry

logger = logging.getLogger(__name__)


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


class VMEnvironment:
    """Shared LXD workspace, SSH key and optional OVN uplink network."""

    def __init__(self) -> None:
        self.image = _env("VM_IMAGE", "ubuntu-minimal-daily:26.04")
        self.cpus = _env("VM_CPUS", "4")
        self.memory = _env("VM_MEMORY", "4GiB")
        self.root_disk = _env("VM_ROOT_DISK", "20GiB")
        self.ceph_disk_size = _env("CEPH_DISK_SIZE", "20GiB")
        self.storage_pool = _env("LXD_STORAGE_POOL", "default")
        self.bridge = _env("LXD_BRIDGE", "lxdbr0")

        self.lxd_channel = _env("LXD_CHANNEL", "6/edge")
        self.microcloud_channel = _env("MICROCLOUD_CHANNEL", "3/edge")
        self.microceph_channel = _env("MICROCEPH_CHANNEL", "tentacle/stable")
        self.microovn_channel = _env("MICROOVN_CHANNEL", "26.03/edge")

        self.uplink_network = _env("UPLINK_NETWORK", "mc-uplink")
        self.uplink_gateway = _env("UPLINK_GATEWAY", "10.99.0.1")
        self.uplink_range = _env("UPLINK_RANGE", "10.99.0.100-10.99.0.254")

        base = (
            os.environ.get("MICROCLOUD_WORK_DIR")
            or os.environ.get("RUNNER_TEMP")
            or os.environ.get("GITHUB_WORKSPACE")
            or os.getcwd()
        )
        Path(base).mkdir(parents=True, exist_ok=True)
        self.work_dir = Path(tempfile.mkdtemp(prefix="microcloud-integration.", dir=base))
        self.ssh_key = self.work_dir / "id_ed25519"
        self.run(
            ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(self.ssh_key)],
            check=True,
        )

        self._suffix = self._run_suffix()
        self._vms: list[MicroCloudVM] = []
        self._uplink_created = False

    @staticmethod
    def _run_suffix() -> str:
        run_id = os.environ.get("GITHUB_RUN_ID")
        if run_id:
            return f"{run_id}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"
        return f"{os.getpid()}-{secrets.token_hex(3)}"

    def run(
        self,
        args: list[str],
        *,
        check: bool = True,
        capture_output: bool = True,
        timeout: int | None = None,
    ) -> subprocess.CompletedProcess[str]:
        logger.info("+ %s", shlex.join(args))
        result = subprocess.run(
            args,
            text=True,
            capture_output=capture_output,
            check=False,
            timeout=timeout,
        )
        if check and result.returncode != 0:
            details = (result.stderr or result.stdout or "").strip()
            raise RuntimeError(
                f"command failed ({result.returncode}): {shlex.join(args)}\n{details}"
            )
        return result

    def launch_vm(
        self,
        name_prefix: str,
        *,
        with_ceph: bool,
        with_ovn: bool,
    ) -> MicroCloudVM:
        name = f"{name_prefix}-{self._suffix}-{secrets.token_hex(3)}"
        vm = MicroCloudVM(self, name, with_ceph=with_ceph, with_ovn=with_ovn)
        self._vms.append(vm)
        vm.launch()
        return vm

    def ensure_uplink_network(self) -> None:
        if self.run(["lxc", "network", "show", self.uplink_network], check=False).returncode == 0:
            return

        logger.info("Creating isolated OVN uplink network %s", self.uplink_network)
        self.run(
            [
                "lxc",
                "network",
                "create",
                self.uplink_network,
                "--type=bridge",
                f"ipv4.address={self.uplink_gateway}/24",
                "ipv4.dhcp=false",
                "ipv6.address=none",
            ]
        )
        self._uplink_created = True

    def cleanup(self) -> None:
        for vm in self._vms:
            vm.cleanup()
        if self._uplink_created:
            logger.info("Deleting network %s", self.uplink_network)
            self.run(["lxc", "network", "delete", self.uplink_network], check=False)
        shutil.rmtree(self.work_dir, ignore_errors=True)


class MicroCloudVM:
    """One LXD VM that runs a MicroCloud member."""

    def __init__(
        self,
        environment: VMEnvironment,
        name: str,
        *,
        with_ceph: bool,
        with_ovn: bool,
    ) -> None:
        self.environment = environment
        self.name = name
        self.with_ceph = with_ceph
        self.with_ovn = with_ovn
        self._hostname: str | None = None
        self._ip: str | None = None
        self._machine_id: str | None = None

    def _lxc(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return self.environment.run(["lxc", *args], check=check)

    def _exec(
        self,
        command: str,
        *,
        check: bool = True,
        timeout: int | None = None,
    ) -> subprocess.CompletedProcess[str]:
        return self.environment.run(
            ["lxc", "exec", self.name, "--", "sh", "-c", command],
            check=check,
            timeout=timeout,
        )

    def _exec_output(self, command: str) -> str:
        return self._exec(command).stdout.strip()

    def launch(self) -> None:
        pubkey = (self.environment.ssh_key.with_suffix(".pub")).read_text().strip()
        user_data = f"""#cloud-config
hostname: {self.name}
manage_etc_hosts: true
ssh_authorized_keys:
  - {pubkey}
packages:
  # juju requires rsyslog to be installed on manually added machines.
  - rsyslog
package_update: true
"""

        logger.info(
            "Launching VM %s (ceph=%s, ovn=%s)",
            self.name,
            self.with_ceph,
            self.with_ovn,
        )
        self._lxc("delete", "--force", self.name, check=False)
        self._lxc(
            "storage",
            "volume",
            "delete",
            self.environment.storage_pool,
            self._ceph_volume_name,
            check=False,
        )

        self._lxc(
            "init",
            self.environment.image,
            self.name,
            "--vm",
            f"--config=limits.cpu={self.environment.cpus}",
            f"--config=limits.memory={self.environment.memory}",
            f"--config=cloud-init.user-data={user_data}",
        )
        self._lxc(
            "config",
            "device",
            "override",
            self.name,
            "root",
            f"size={self.environment.root_disk}",
            check=False,
        )

        if self.with_ovn:
            self.environment.ensure_uplink_network()
            self._lxc(
                "config",
                "device",
                "add",
                self.name,
                "eth1",
                "nic",
                f"network={self.environment.uplink_network}",
                "name=eth1",
            )

        if self.with_ceph:
            self._lxc(
                "storage",
                "volume",
                "create",
                self.environment.storage_pool,
                self._ceph_volume_name,
                "--type=block",
                f"size={self.environment.ceph_disk_size}",
            )
            self._lxc(
                "config",
                "device",
                "add",
                self.name,
                "ceph",
                "disk",
                f"pool={self.environment.storage_pool}",
                f"source={self._ceph_volume_name}",
            )

        self._lxc("start", self.name)
        self.wait_ready()
        if self.with_ovn:
            self.prepare_uplink_interface()

    @property
    def _ceph_volume_name(self) -> str:
        return f"{self.name}-ceph"

    @property
    def hostname(self) -> str:
        if self._hostname is None:
            self._hostname = self._exec_output("hostname")
        return self._hostname

    @property
    def ip(self) -> str:
        if self._ip is None:
            self._ip = self._find_ip()
        return self._ip

    def _find_ip(self) -> str:
        result = self._lxc("list", self.name, "--format=json", check=False)
        if result.returncode == 0:
            try:
                instances = json.loads(result.stdout)
                addresses = instances[0]["state"]["network"]["eth0"]["addresses"]
                for address in addresses:
                    if address.get("family") == "inet" and address.get("scope") == "global":
                        return address["address"]
            except (KeyError, IndexError, TypeError, json.JSONDecodeError):
                pass

        ip = self._exec("hostname -I | awk '{print $1}'", check=False).stdout.strip()
        if not ip:
            raise RuntimeError(f"Could not determine the primary address on {self.name}")
        return ip

    def wait_ready(self) -> None:
        logger.info("Waiting for cloud-init on %s", self.name)
        for _ in range(120):
            if self._exec("cloud-init status --wait", check=False).returncode == 0:
                break
            time.sleep(2)
        self._exec("cloud-init status --long", check=False)

        for _ in range(120):
            try:
                ip = self._find_ip()
            except Exception as exc:  # noqa: BLE001 - the VM may still be booting
                logger.debug("VM %s is not reachable yet: %s", self.name, exc)
                ip = ""
            if ip and self._ssh(ip, "true", check=False).returncode == 0:
                self._ip = ip
                logger.info("VM %s is ready at %s", self.name, ip)
                return
            time.sleep(2)
        raise RuntimeError(f"Timed out waiting for VM {self.name} to become reachable over SSH")

    def _ssh(
        self, ip: str, command: str, *, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        return self.environment.run(
            [
                "ssh",
                "-i",
                str(self.environment.ssh_key),
                "-o",
                "BatchMode=yes",
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "ConnectTimeout=5",
                "-o",
                "LogLevel=ERROR",
                f"ubuntu@{ip}",
                command,
            ],
            check=check,
        )

    def _primary_interface(self) -> str:
        interface = self._exec_output(
            f"ip -o -4 addr show | grep -F ' {self.ip}/' | awk '{{print $2; exit}}'"
        )
        if not interface:
            raise RuntimeError(f"Could not determine the primary interface on {self.name}")
        return interface

    def _uplink_interface(self) -> str:
        primary = self._primary_interface()
        # Variables are expanded inside the VM.
        script = f"""
primary={shlex.quote(primary)}
for path in /sys/class/net/*; do
    iface=${{path##*/}}
    [ "${{iface}}" = "lo" ] && continue
    [ "${{iface}}" = "${{primary}}" ] && continue
    printf "%s\\n" "${{iface}}"
    exit 0
done
exit 1
"""
        return self._exec_output(script)

    def prepare_uplink_interface(self) -> None:
        interface = self._uplink_interface()
        logger.info("Using %s as the OVN uplink interface on %s", interface, self.name)
        self._exec(
            f"ip -4 addr flush dev {shlex.quote(interface)} || true; "
            f"ip -6 addr flush dev {shlex.quote(interface)} || true; "
            f"ip link set {shlex.quote(interface)} up"
        )

    def _snap_install_or_refresh(self, snap: str, channel: str) -> None:
        if self._exec(f"snap list {snap}", check=False).returncode == 0:
            logger.info("Refreshing %s on %s from %s", snap, self.name, channel)
            self._exec(f"snap refresh {snap} --channel={channel}")
        else:
            logger.info("Installing %s on %s from %s", snap, self.name, channel)
            self._exec(f"snap install {snap} --channel={channel}")

    def install_microcloud_snaps(self) -> None:
        hold = ["lxd", "microcloud"]
        self._snap_install_or_refresh("lxd", self.environment.lxd_channel)
        if self.with_ceph:
            self._snap_install_or_refresh("microceph", self.environment.microceph_channel)
            hold.append("microceph")
        if self.with_ovn:
            self._snap_install_or_refresh("microovn", self.environment.microovn_channel)
            hold.append("microovn")
        self._snap_install_or_refresh("microcloud", self.environment.microcloud_channel)
        self._exec(f"snap refresh --hold {' '.join(hold)}")

    def _ceph_disk(self) -> str:
        # Select the largest writable, unmounted disk. This skips the root disk
        # (its partitions are mounted) and LXD's cloud-init config drive.
        script = r"""
for disk in /dev/sd? /dev/vd? /dev/nvme?n?; do
    [ -b "$disk" ] || continue
    lsblk -no MOUNTPOINT "$disk" | grep -q "[^[:space:]]" && continue
    [ "$(lsblk -dno RO "$disk")" = "0" ] || continue
    printf "%s %s\n" "$(lsblk -bdno SIZE "$disk")" "$disk"
done | sort -nr | head -1 | cut -d" " -f2
"""
        disk = self._exec_output(script)
        if not disk:
            raise RuntimeError(f"Could not find the Ceph disk on {self.name}")
        return disk

    def _preseed_header(self, initiator: str, passphrase: str) -> dict[str, Any]:
        subnet = self.environment.run(
            ["lxc", "network", "get", self.environment.bridge, "ipv4.address"]
        ).stdout.strip()
        return {
            "lookup_subnet": subnet,
            "initiator": initiator,
            "session_passphrase": passphrase,
        }

    def _preseed_system(self) -> dict[str, Any]:
        system: dict[str, Any] = {"name": self.hostname}
        if self.with_ovn:
            system["ovn_uplink_interface"] = self._uplink_interface()
        if self.with_ceph:
            system["storage"] = {"ceph": [{"path": self._ceph_disk(), "wipe": True}]}
        return system

    def _preseed_global_config(self) -> dict[str, Any]:
        config: dict[str, Any] = {}
        if self.with_ceph:
            config["ceph"] = {"cephfs": True}
        if self.with_ovn:
            config["ovn"] = {
                "ipv4_gateway": f"{self.environment.uplink_gateway}/24",
                "ipv4_range": self.environment.uplink_range,
            }
        return config

    def _preseed_document(self, initiator: str, passphrase: str) -> dict[str, Any]:
        document = self._preseed_header(initiator, passphrase)
        document["systems"] = [self._preseed_system()]
        document.update(self._preseed_global_config())
        return document

    def _push_preseed(self, vm: MicroCloudVM, document: dict[str, Any]) -> Path:
        path = self.environment.work_dir / f"preseed-{vm.name}.yaml"
        path.write_text(yaml.safe_dump(document, sort_keys=False))
        self._lxc("file", "push", str(path), f"{vm.name}/tmp/microcloud-preseed.yaml")
        return path

    def _run_preseed(self, vm: MicroCloudVM) -> subprocess.CompletedProcess[str]:
        return self.environment.run(
            [
                "lxc",
                "exec",
                vm.name,
                "--",
                "sh",
                "-c",
                "microcloud preseed < /tmp/microcloud-preseed.yaml > /tmp/microcloud-preseed.out 2>&1",
            ],
            check=False,
        )

    def _preseed_output(self) -> str:
        return self._exec_output("cat /tmp/microcloud-preseed.out")

    def preseed_init(self) -> None:
        passphrase = secrets.token_hex(16)
        document = self._preseed_document(self.hostname, passphrase)
        self._push_preseed(self, document)

        logger.info("Initialising MicroCloud on %s", self.name)
        result = self._run_preseed(self)
        output = self._preseed_output()
        if result.returncode != 0 or "MicroCloud is ready" not in output:
            raise RuntimeError(f"microcloud preseed failed on {self.name}:\n{output}")

    def preseed_add_member(self, member: MicroCloudVM) -> None:
        passphrase = secrets.token_hex(16)
        document = member._preseed_document(self.hostname, passphrase)
        self._push_preseed(self, document)
        member._push_preseed(member, document)

        logger.info("Adding %s to the MicroCloud cluster on %s", member.name, self.name)
        member_process = subprocess.Popen(
            [
                "lxc",
                "exec",
                member.name,
                "--",
                "sh",
                "-c",
                "microcloud preseed < /tmp/microcloud-preseed.yaml > /tmp/microcloud-preseed.out 2>&1",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        initiator_process = self._run_preseed(self)
        member_stdout, member_stderr = member_process.communicate()

        initiator_output = self._preseed_output()
        member_output = member._preseed_output()
        if initiator_process.returncode != 0 or member_process.returncode != 0:
            raise RuntimeError(
                "microcloud preseed failed while adding "
                f"{member.name}\ninitiator:\n{initiator_output}\nmember:\n{member_output}\n"
                f"{member_stdout}\n{member_stderr}"
            )
        if "MicroCloud is ready" not in initiator_output:
            raise RuntimeError(
                f"The initiator did not report a ready MicroCloud:\n{initiator_output}"
            )
        if "Successfully joined the MicroCloud cluster" not in member_output:
            raise RuntimeError(
                f"{member.name} did not join the MicroCloud cluster:\n{member_output}"
            )

        for _ in range(60):
            cluster_list = self._exec("microcloud cluster list", check=False).stdout
            if member.hostname in cluster_list:
                logger.info("%s is now part of the MicroCloud cluster", member.hostname)
                return
            time.sleep(5)
        raise RuntimeError(
            f"Timed out waiting for {member.hostname} to appear in the cluster list:\n"
            f"{self._exec('microcloud cluster list', check=False).stdout}"
        )

    def add_machine(self, juju: jubilant.Juju) -> str:
        """Add this VM to ``juju`` as a machine and return its machine ID."""
        ip = self.ip
        key = self.environment.ssh_key
        logger.info("Adding %s as a Juju machine", ip)

        for _ in range(12):
            if self._ssh(ip, "true", check=False).returncode == 0:
                break
            time.sleep(5)
        else:
            raise RuntimeError(f"Could not SSH to {ip} before adding it as a Juju machine")

        before = set(juju.status().machines)
        juju.add_machine(
            f"ssh:ubuntu@{ip}",
            private_key=key,
            public_key=key.with_suffix(".pub"),
        )

        # jubilant.add_machine() returns None, so the new machine is whichever
        # ID appears in status that was not there before. Poll because a freshly
        # added manual machine is not always listed in status immediately.
        # Record the ID as soon as it appears so cleanup can still remove the
        # machine if a later status read fails.
        def _new_machine() -> str:
            added = set(juju.status().machines) - before
            if not added:
                raise RuntimeError(f"no new Juju machine appeared for {ip}")
            self._machine_id = sorted(added)[0]
            return self._machine_id

        return retry(
            _new_machine,
            attempts=12,
            delay=5,
            description=f"Juju machine for {ip}",
        )

    def add_unit(self, juju: jubilant.Juju, machine: str, *, app: str = "microcloud") -> None:
        juju.add_unit(app, to=machine)

    def cleanup(self, juju: jubilant.Juju | None = None) -> None:
        # Remove the manual Juju machine before deleting the VM: otherwise the
        # model's `destroy-model --force` can hang on a machine whose host is
        # already gone. Wait for the removal to finish (no --no-wait) so it does
        # not race the VM deletion below.
        if juju is not None and self._machine_id is not None:
            logger.info("Removing Juju machine %s", self._machine_id)
            try:
                juju.cli("remove-machine", self._machine_id, "--force", "--no-prompt")
            except jubilant.CLIError as exc:
                logger.warning("remove-machine %s failed: %s", self._machine_id, exc)
        logger.info("Deleting VM %s", self.name)
        self._lxc("delete", "--force", self.name, check=False)
        self._lxc(
            "storage",
            "volume",
            "delete",
            self.environment.storage_pool,
            self._ceph_volume_name,
            check=False,
        )
