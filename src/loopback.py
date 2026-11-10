# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""This unit's addresses on "lo", for networks with no interface on the machine.

Such an address is reached over BGP, which advertises what sits on "lo". Each
address is added at once and written to netplan, so that it returns on boot.
"netplan apply" is never run: it has systemd-networkd remove the routes and
policy rules it did not create, and those are the routing daemon's.
"""

import ipaddress
import json
import os
import subprocess
import tempfile
from collections.abc import Mapping
from pathlib import Path

NETPLAN_FILE = Path("/etc/netplan/90-microcloud-lo.yaml")


class LoopbackError(Exception):
    """Raised when the machine's addresses cannot be read or changed."""


def _run(*command: str) -> str:
    try:
        return subprocess.run(
            command, capture_output=True, check=True, text=True, timeout=30
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


def _write(path: Path, content: str) -> None:
    """Replace ``path`` with ``content``, readable by root only as netplan expects."""
    try:
        if path.read_text() == content:
            return
    except OSError:
        pass

    try:
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
        with os.fdopen(fd, "w") as handle:
            handle.write(content)
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    except OSError as exc:
        raise LoopbackError(f"Cannot write {path}: {exc}") from exc


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
        _write(NETPLAN_FILE, render(wanted))
        for address in wanted:
            if address not in present.get("lo", set()):
                _run("ip", "-6", "address", "replace", f"{address}/128", "dev", "lo")
    except LoopbackError as exc:
        return f"Cannot put addresses on lo: {exc}"
    return None
