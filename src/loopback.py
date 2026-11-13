# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""This unit's addresses on "lo", for networks with no interface on the machine.

Such an address is reached over BGP, which advertises what sits on "lo". The
addresses are written to netplan and applied with "netplan apply", so netplan
is the one place they come from, now and on boot. systemd-networkd removes the
routes and policy rules it did not create when netplan is applied, and those
belong to the routing daemon, so it is told to leave them alone first.
"""

import ipaddress
import json
import os
import subprocess
import tempfile
from collections.abc import Mapping
from pathlib import Path

NETPLAN_FILE = Path("/etc/netplan/90-microcloud-lo.yaml")
# Keeps systemd-networkd from removing the routes and rules of a routing
# daemon, such as BIRD's (protocol "bird"), when netplan is applied or
# networkd restarts.
NETWORKD_DROPIN = Path("/etc/systemd/networkd.conf.d/90-microcloud-bgp.conf")
NETWORKD_CONF = "[Network]\nManageForeignRoutes=no\nManageForeignRoutingPolicyRules=no\n"


class LoopbackError(Exception):
    """Raised when the machine's addresses cannot be read or changed."""


def _run(*command: str, timeout: int = 30) -> str:
    try:
        return subprocess.run(
            command, capture_output=True, check=True, text=True, timeout=timeout
        ).stdout
    except subprocess.CalledProcessError as exc:
        raise LoopbackError(f"{' '.join(command)}: {exc.stderr.strip()}") from exc
    except (OSError, subprocess.SubprocessError) as exc:
        raise LoopbackError(f"{' '.join(command)}: {exc}") from exc


def host_addresses() -> dict[str, set[ipaddress.IPv6Address]]:
    """Return this machine's global IPv6 addresses, by interface."""
    try:
        links = json.loads(_run("ip", "-json", "-6", "address", "show", "scope", "global"))
    except json.JSONDecodeError as exc:
        raise LoopbackError(f"ip address show: {exc}") from exc

    addresses: dict[str, set[ipaddress.IPv6Address]] = {}
    for link in links:
        for addr in link.get("addr_info", []):
            # "ip" leaves an empty object for each address the scope filter hides.
            if addr.get("family") != "inet6" or addr.get("scope") != "global":
                continue
            try:
                address = ipaddress.IPv6Address(addr.get("local", ""))
            except ValueError:
                continue
            addresses.setdefault(link.get("ifname", ""), set()).add(address)
    return addresses


def render(addresses: list[ipaddress.IPv6Address]) -> str:
    """Return the netplan document that keeps ``addresses`` on "lo"."""
    lines = ["network:", "  version: 2", "  ethernets:", "    lo:", "      addresses:"]
    lines += [f"        - {address}/128" for address in addresses]
    return "\n".join(lines) + "\n"


def _write(path: Path, content: str, mode: int = 0o600) -> bool:
    """Replace ``path`` with ``content``. Returns True if it changed.

    The default mode is root only, as netplan expects of its files.
    """
    try:
        if path.read_text() == content:
            return False
    except OSError:
        pass

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
        with os.fdopen(fd, "w") as handle:
            handle.write(content)
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    except OSError as exc:
        raise LoopbackError(f"Cannot write {path}: {exc}") from exc
    return True


def keep_foreign_routes() -> None:
    """Tell systemd-networkd to leave routes and rules it did not create alone.

    networkd reads its own configuration only when it starts, so it is
    restarted when the setting is new. With the setting in place, the restart
    keeps the routes already there.
    """
    if _write(NETWORKD_DROPIN, NETWORKD_CONF, mode=0o644):
        _run("systemctl", "restart", "systemd-networkd", timeout=60)


def ensure(addresses: Mapping[ipaddress.IPv6Network, ipaddress.IPv6Address]) -> str | None:
    """Put this unit's address in each network on "lo". Returns a problem or None.

    ``addresses`` maps a network to the unit's address in it. Any other address
    of this machine inside one of the networks is a problem: MicroCeph and
    MicroCloud take the first local address they find in a network, so two
    would leave the choice to the order the kernel lists them in.
    """
    if not addresses:
        return None

    try:
        present = host_addresses()
        for network, address in addresses.items():
            for interface, held in sorted(present.items()):
                for other in sorted(held):
                    if other in network and other != address:
                        return (
                            f"{other} on {interface} is in {network}, "
                            f"which {address} on lo is derived for"
                        )

        wanted = sorted(addresses.values())
        keep_foreign_routes()
        changed = _write(NETPLAN_FILE, render(wanted))
        if changed or any(address not in present.get("lo", set()) for address in wanted):
            _run("netplan", "apply", timeout=120)
            on_lo = host_addresses().get("lo", set())
            missing = [str(address) for address in wanted if address not in on_lo]
            if missing:
                return f"netplan apply left {', '.join(missing)} off lo"
    except LoopbackError as exc:
        return f"Cannot put addresses on lo: {exc}"
    return None


def reachable(source: str, target: str) -> bool:
    """Return True if ``target`` answers a ping sent from ``source``.

    An answer needs a route in both directions, so it shows that both
    addresses are advertised.
    """
    try:
        _run("ping", "-6", "-c", "1", "-W", "2", "-I", source, target)
    except LoopbackError:
        return False
    return True
