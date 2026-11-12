# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""BGP on this machine, for networks whose addresses sit on "lo".

On a host whose only links to the fabric are routed, such as PS10's DPU
uplinks, the derived addresses are reached over BGP. The charm runs BIRD from
the MicroOVN snap for that, and nothing else from it: MicroCloud adds MicroOVN
to the cluster whenever its daemon runs, whatever the preseed says, so every
other MicroOVN service is stopped and disabled before the cluster forms.

Each uplink peers with whatever router it discovers from Router
Advertisements, on link-local addresses. The machine advertises its derived
/128s and learns its peers'. It takes no default route from the fabric.
"""

import ipaddress
import os
import subprocess
import tempfile
from collections.abc import Iterable
from pathlib import Path

SNAP = "microovn"
BIRD_SERVICE = "microovn.bird"
BIRD_CONF = Path("/var/snap/microovn/common/data/bird/bird.conf")

# The private four-byte ASN range (RFC 6996): 4200000000 to 4294967294.
_ASN_BASE = 4200000000
_ASN_COUNT = 4294967294 - _ASN_BASE + 1


class BGPError(Exception):
    """Raised when BIRD cannot be configured or started."""


def uplinks(raw: str) -> list[str]:
    """Return the interfaces named in a comma-separated option value."""
    return [name.strip() for name in raw.split(",") if name.strip()]


def identity(source: ipaddress.IPv6Address) -> tuple[str, int]:
    """Return the BGP router ID and ASN for a machine, from its address.

    Both come from the low 32 bits of the address, which differ between any
    two machines in one /64 whose host parts differ there. Raises ValueError
    if those bits are zero, since 0.0.0.0 is not a router ID.
    """
    low = int(source) & 0xFFFFFFFF
    if low == 0:
        raise ValueError(f"{source} has no host bits to derive a BGP router ID from")
    return str(ipaddress.IPv4Address(low)), _ASN_BASE + low % _ASN_COUNT


def render(
    router_id: str,
    asn: int,
    interfaces: Iterable[str],
    networks: Iterable[ipaddress.IPv6Network],
) -> str:
    """Return the BIRD configuration that advertises the /128s in ``networks``."""
    interfaces = list(interfaces)
    prefixes = ", ".join(f"{network}{{128,128}}" for network in sorted(networks))

    lines = [
        "# Written by the MicroCloud charm. Changes are overwritten.",
        "log syslog all;",
        f"router id {router_id};",
        "neighbor table peers;",
        "",
        "protocol device {}",
        'protocol direct { ipv6; interface "lo"; }',
        "",
        "protocol kernel {",
        "  ipv6 {",
        "    export filter {",
        "      if source = RTS_DEVICE then reject;",
        f"      if net ~ [ {prefixes} ] then accept;",
        "      reject;",
        "    };",
        "    import none;",
        "  };",
        "  merge paths yes;",
        "}",
    ]
    for name in interfaces:
        lines += [
            "",
            f"protocol bgp uplink_{name} {{",
            f"  local as {asn};",
            "  neighbor range fe80::/10 external;",
            f'  interface "{name}";',
            "  direct;",
            f'  dynamic name "{name}_peer_";',
            "  graceful restart off;",
            "  bfd on;",
            "  connect delay time 1;",
            "  connect retry time 2;",
            "  error wait time 1, 5;",
            "  error forget time 5;",
            "  neighbors {",
            "    table peers;",
            "    export all;",
            "  };",
            "  ipv6 {",
            "    next hop self;",
            f"    import filter {{ if net ~ [ {prefixes} ] then accept; reject; }};",
            "    export filter {",
            f"      if source = RTS_DEVICE && net ~ [ {prefixes} ] then accept;",
            "      reject;",
            "    };",
            "  };",
            "}",
        ]
    lines += ["", "protocol radv {", "  neighbors { table peers; };"]
    for name in interfaces:
        lines += [
            f'  interface "{name}" {{',
            "    router discovery yes;",
            "    max ra interval 4;",
            "    min ra interval 3;",
            "    default lifetime 0;",
            "  };",
        ]
    lines += ["}", "", "protocol bfd {"]
    for name in interfaces:
        lines += [
            f'  interface "{name}" {{',
            "    min rx interval 100ms;",
            "    min tx interval 100ms;",
            "    multiplier 3;",
            "  };",
        ]
    lines.append("}")
    return "\n".join(lines) + "\n"


def _run(*command: str) -> str:
    try:
        return subprocess.run(
            command, capture_output=True, check=True, text=True, timeout=60
        ).stdout
    except subprocess.CalledProcessError as exc:
        raise BGPError(f"{' '.join(command)}: {exc.stderr.strip()}") from exc
    except (OSError, subprocess.SubprocessError) as exc:
        raise BGPError(f"{' '.join(command)}: {exc}") from exc


def installed() -> bool:
    """Return True if the MicroOVN snap is installed."""
    try:
        _run("snap", "list", SNAP)
    except BGPError:
        return False
    return True


def services() -> dict[str, tuple[bool, bool]]:
    """Return each MicroOVN snap service with whether it is enabled and active."""
    out = {}
    for line in _run("snap", "services", SNAP).splitlines()[1:]:
        fields = line.split()
        if len(fields) >= 3:
            out[fields[0]] = (fields[1] == "enabled", fields[2] == "active")
    return out


def _write(path: Path, content: str) -> bool:
    """Replace ``path`` with ``content``. Returns True if it changed."""
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
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except OSError as exc:
        raise BGPError(f"Cannot write {path}: {exc}") from exc
    return True


def ensure(conf: str) -> str | None:
    """Run BIRD alone from the MicroOVN snap with ``conf``. Returns a problem or None.

    Every other service of the snap is stopped and disabled first, so that
    MicroCloud does not add MicroOVN to the cluster.
    """
    try:
        for name, (enabled, active) in services().items():
            if name != BIRD_SERVICE and (enabled or active):
                _run("snap", "stop", "--disable", name)

        changed = _write(BIRD_CONF, conf)
        enabled, active = services().get(BIRD_SERVICE, (False, False))
        if not (enabled and active):
            _run("snap", "start", "--enable", BIRD_SERVICE)
        elif changed:
            _run("microovn.birdc", "configure")
    except BGPError as exc:
        return f"Cannot run BGP: {exc}"
    return None
