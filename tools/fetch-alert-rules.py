#!/usr/bin/env python3

"""Fetch upstream Prometheus alert rules at charmcraft pack time.

This script fetches Prometheus alert rule YAML files and writes them into
src/prometheus_alert_rules/{microceph,microovn}/.

The alert rule YAML files are NOT committed to the repository.

Sources
-------
Alert rule sources:
* MicroCeph
    https://github.com/canonical/charm-microceph/blob/{rev}/files/prometheus_alert_rules/prometheus_alerts.yaml
* MicroOVN
    https://github.com/canonical/microovn-operator/blob/{rev}/src/prometheus_alert_rules/alerts.yaml
"""

import pathlib
import sys
import urllib.error
import urllib.request

# ---------------------------------------------------------------------------
# Pinned upstream revisions
# ---------------------------------------------------------------------------

# Commits in the upstream charm repositories.
MICROCEPH_REF = "9e8e20aadd5cf8434cd24bb1b6770f3b1f7a2205"
MICROOVN_REF = "eeb85d826a3602e3fb6bddeead9d2788d83d914e"

# ---------------------------------------------------------------------------
# Alert rule catalogue
# ---------------------------------------------------------------------------

# Each entry: (destination_filename, url)
ALERT_RULES: dict[str, list[tuple[str, str]]] = {
    "microceph": [
        (
            "prometheus_alerts.yaml",
            f"https://raw.githubusercontent.com/canonical/charm-microceph/{MICROCEPH_REF}/files/prometheus_alert_rules/prometheus_alerts.yaml",
        ),
    ],
    "microovn": [
        (
            "alerts.yaml",
            f"https://raw.githubusercontent.com/canonical/microovn-operator/{MICROOVN_REF}/src/prometheus_alert_rules/alerts.yaml",
        ),
    ],
}


def fetch(url: str) -> bytes:
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()


def main() -> None:
    # The script is called from the charmcraft source directory.
    src_dir = pathlib.Path(__file__).parent.parent / "src" / "prometheus_alert_rules"

    errors: list[str] = []
    total = 0

    for service, entries in ALERT_RULES.items():
        dest_dir = src_dir / service
        dest_dir.mkdir(parents=True, exist_ok=True)

        # Remove any stale files (e.g. .gitkeep placeholders) that would
        # confuse cos_agent's AlertRules.add_path() at runtime.
        for stale in dest_dir.iterdir():
            if not stale.name.endswith((".yaml", ".yml")):
                stale.unlink()
                print(f"  removed {stale}")

        for filename, url in entries:
            dest = dest_dir / filename
            print(f"  fetching {url}", flush=True)
            try:
                raw = fetch(url)
            except urllib.error.URLError as exc:
                errors.append(f"{service}/{filename}: {exc}")
                continue

            dest.write_bytes(raw)
            total += 1

    if errors:
        print("\nERROR: failed to fetch the following alert rules:", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        sys.exit(1)

    print(f"\nFetched {total} alert rule files into {src_dir}")


if __name__ == "__main__":
    main()
