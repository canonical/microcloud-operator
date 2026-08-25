#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Fetch upstream Grafana dashboards at charmcraft pack time.

This script is invoked by the charmcraft.yaml override-build step before
craftctl default runs.  It downloads the canonical upstream dashboard JSON
files, injects the "microcloud" and per-service tags, resolves any
__inputs__ datasource variables, and writes them into
src/dashboards/{lxd,microceph,microovn}/ so that the uv plugin includes
them in the built charm.

The dashboard JSON files are NOT committed to the repository.  This script
is the single source of truth for which dashboards are bundled and where
they come from.

Sources
-------
LXD
    Grafana dashboard #19131 (https://grafana.com/grafana/dashboards/19131-lxd/)
    Downloaded via the Grafana API: GET /api/dashboards/{id}/revisions/latest/download

MicroCeph
    canonical/charm-microceph, files/grafana_dashboards/
    https://github.com/canonical/charm-microceph/tree/main/files/grafana_dashboards

MicroOVN
    canonical/microovn-operator, src/dashboards/
    https://github.com/canonical/microovn-operator/tree/main/src/dashboards
"""

import json
import pathlib
import sys
import urllib.request
import urllib.error

# ---------------------------------------------------------------------------
# Datasource input resolution
# ---------------------------------------------------------------------------

# Dashboards from grafana.com embed ${DS_PROMETHEUS} / ${DS_LOKI} as named
# __inputs__ placeholders meant to be filled in by Grafana's import wizard.
# The cos-agent pipeline bypasses the import wizard, so we rewrite them at
# build time.
#
# The correct pattern (used by all grafana-k8s-operator bundled dashboards) is
# to replace ${DS_PROMETHEUS} with a template variable reference ${prometheusds}
# and inject a "datasource" type template variable named "prometheusds" with
# query "prometheus".  Grafana then resolves ${prometheusds} to the first
# available Prometheus datasource at render time — no hardcoded UID needed.
#
# Maps: __inputs__ variable name → (template_var_name, datasource_type)
_DATASOURCE_INPUTS: dict[str, tuple[str, str]] = {
    "DS_PROMETHEUS": ("prometheusds", "prometheus"),
    "DS_LOKI":       ("lokids",       "loki"),
}

# ---------------------------------------------------------------------------
# Dashboard catalogue
# ---------------------------------------------------------------------------

# Each entry: (destination_filename, url, extra_tags_to_inject)
DASHBOARDS: dict[str, list[tuple[str, str, list[str]]]] = {
    "lxd": [
        (
            "lxd.json",
            "https://grafana.com/api/dashboards/19131/revisions/latest/download",
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fetch(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "charmcraft-fetch-dashboards/1.0"})
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
    """Replace __inputs__ datasource variables with Grafana template variables.

    Dashboards from grafana.com use ${DS_PROMETHEUS} / ${DS_LOKI} as named
    input placeholders populated by Grafana's import wizard.  The cos-agent
    pipeline bypasses the import wizard.

    We rewrite each ${DS_X} occurrence to a template variable reference
    ${xds} and inject a "datasource" type template variable into
    templating.list so Grafana resolves it to the first available datasource
    of the matching type at render time.  This is the same approach used by
    all grafana-k8s-operator bundled dashboards.

    We also strip __inputs__, __requires__, __elements__ which are only
    meaningful to the import wizard.
    """
    # Find which input variables are actually used in this dashboard.
    raw = json.dumps(data)
    used: dict[str, tuple[str, str]] = {
        input_var: (tpl_var, ds_type)
        for input_var, (tpl_var, ds_type) in _DATASOURCE_INPUTS.items()
        if f"${{{input_var}}}" in raw
    }

    # String-replace ${DS_X} → ${xds} throughout the serialised JSON.
    for input_var, (tpl_var, _) in used.items():
        raw = raw.replace(f"${{{input_var}}}", f"${{{tpl_var}}}")

    result = json.loads(raw)

    # Inject a datasource template variable for each replaced input so Grafana
    # can resolve the ${xds} reference dynamically.
    templating = result.setdefault("templating", {})
    tpl_list: list[dict] = templating.setdefault("list", [])
    existing_names = {t.get("name") for t in tpl_list}
    for _, (tpl_var, ds_type) in used.items():
        if tpl_var not in existing_names:
            tpl_list.insert(0, {
                "name": tpl_var,
                "type": "datasource",
                "query": ds_type,
                "hide": 0,
                "refresh": 1,
                "label": ds_type.capitalize(),
            })

    # Strip import-wizard-only keys (both single and double underscore variants).
    for key in ("__inputs", "__inputs__", "__requires", "__requires__", "__elements", "__elements__"):
        result.pop(key, None)

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

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
