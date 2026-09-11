#!/usr/bin/env python3

"""Fetch upstream Grafana dashboards at charmcraft pack time.

This script fetches dashboards, resolves the `${DS_X}` datasource
placeholders, and writes them into all dashboards under
src/dashboards/{lxd,microceph,microovn}/, injecting a few extra tags too.

The dashboard JSON files are NOT committed to the repository.

Dashboard sources:
* LXD
    https://github.com/canonical/lxd/blob/main/grafana/
* MicroCeph
    https://github.com/canonical/charm-microceph/tree/main/files/grafana_dashboards
* MicroOVN
    https://github.com/canonical/microovn-operator/tree/main/src/dashboards
"""

import json
import pathlib
import sys
import urllib.request
import urllib.error

# Maps: __inputs__ variable name -> template variable name.
# The consumer library (charms.grafana_k8s.v0.grafana_dashboard) injects
# "prometheusds"/"lokids" template variables unconditionally, so we only need
# to rewrite the placeholder names to match; no need to declare them ourselves.
_DATASOURCE_INPUTS: dict[str, str] = {
    "DS_PROMETHEUS": "prometheusds",
    "DS_LOKI": "lokids",
}

# Each entry: (destination_filename, url, extra_tags_to_inject)
DASHBOARDS: dict[str, list[tuple[str, str, list[str]]]] = {
    "lxd": [
        (
            "lxd.json",
            "https://raw.githubusercontent.com/canonical/lxd/refs/heads/main/grafana/LXD.json",
            ["microcloud", "lxd"],
        ),
    ],
    "microceph": [
        (f"{name}.json",
         f"https://raw.githubusercontent.com/canonical/charm-microceph/main/files/grafana_dashboards/{name}.json",
         ["microcloud", "microceph"])
        for name in [
            "ceph-cluster-advanced",
            "ceph-cluster",
            "cephfs-overview",
            "host-details",
            "hosts-overview",
            "osd-device-details",
            "osds-overview",
            "pool-detail",
            "pool-overview",
            "radosgw-detail",
            "radosgw-overview",
            "radosgw-sync-overview",
            "rbd-details",
            "rbd-overview",
        ]
    ],
    "microovn": [
        (f"{name}.json",
         f"https://raw.githubusercontent.com/canonical/microovn-operator/main/src/dashboards/{name}.json",
         ["microcloud", "microovn"])
        for name in [
            "central-north-daemon",
            "central-northbound-db",
            "central-southbound-db",
            "host-controller",
            "host-ovs",
        ]
    ],
}


def fetch(url: str) -> bytes:
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()


def inject_tags(data: dict, extra: list[str]) -> dict:
    tags: list[str] = data.get("tags") or []
    seen = set(tags)
    for tag in extra:
        if tag not in seen:
            tags.append(tag)
            seen.add(tag)
    data["tags"] = tags
    return data


def resolve_inputs(data: dict) -> dict:
    """Rewrite `${DS_X}` datasource placeholders to `${xds}` everywhere.

    This is a plain string replacement across the whole serialised dashboard,
    so it also fixes panels nested inside collapsed rows, which the Grafana
    consumer library's own resolution logic doesn't reach. We don't inject
    "datasource" template variables ourselves; the consumer library already
    does that unconditionally for "prometheusds"/"lokids".
    """
    raw = json.dumps(data)
    for input_name, template_name in _DATASOURCE_INPUTS.items():
        raw = raw.replace(f"${{{input_name}}}", f"${{{template_name}}}")
    return json.loads(raw)


def main() -> None:
    # The script is called from the charmcraft source directory.
    src_dir = pathlib.Path(__file__).parent.parent / "src" / "dashboards"

    errors: list[str] = []
    total = 0

    for service, entries in DASHBOARDS.items():
        dest_dir = src_dir / service
        dest_dir.mkdir(parents=True, exist_ok=True)

        # Remove any non-JSON files (e.g. .gitkeep placeholders) that would
        # cause cos_agent's glob("*") + json.load() to fail at runtime.
        for stale in dest_dir.iterdir():
            if not stale.name.endswith(".json"):
                stale.unlink()
                print(f"  removed {stale}")

        for filename, url, tags in entries:
            dest = dest_dir / filename
            print(f"  fetching {url}", flush=True)
            try:
                raw = fetch(url)
            except urllib.error.URLError as exc:
                errors.append(f"{service}/{filename}: {exc}")
                continue

            try:
                data = json.loads(raw)
            except json.JSONDecodeError as exc:
                errors.append(f"{service}/{filename}: invalid JSON from {url}: {exc}")
                continue

            inject_tags(data, tags)
            data = resolve_inputs(data)
            dest.write_text(json.dumps(data, indent=2))
            total += 1

    if errors:
        print("\nERROR: failed to fetch the following dashboards:", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        sys.exit(1)

    print(f"\nFetched {total} dashboards into {src_dir}")


if __name__ == "__main__":
    main()
