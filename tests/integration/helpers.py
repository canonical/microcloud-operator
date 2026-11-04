"""Shared helpers for the MicroCloud charm integration tests.

These helpers target the current ``main`` charm, which is observe-only: the
MicroCloud cluster is formed by the test harness before the charm is deployed.
Keep charm-specific parsing here so the VM and COS helpers stay reusable when
the ``dev`` branch's deployment mode lands.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import jubilant

MICROCLOUD_CHARM = "microcloud"

DEPLOY_TIMEOUT_IN_SECONDS = 20 * 60
COS_TIMEOUT_IN_SECONDS = 30 * 60

LXD_METRICS_URL = "https://127.0.0.1:8444/1.0/metrics"
CEPH_METRICS_URL = "http://127.0.0.1:9283/metrics"
OVN_METRICS_URL = "http://127.0.0.1:9310/metrics"

logger = logging.getLogger(__name__)


def retry(
    function: Callable[[], Any],
    *,
    attempts: int = 60,
    delay: int = 5,
    description: str = "",
) -> Any:
    """Call ``function`` until it succeeds or the attempt budget runs out."""
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return function()
        except Exception as exc:  # noqa: BLE001 - callers assert on the final error
            last_error = exc
            if attempt == attempts:
                break
            logger.debug("%s failed (attempt %d/%d): %s", description, attempt, attempts, exc)
            time.sleep(delay)
    raise AssertionError(f"Timed out waiting for {description or 'condition'}") from last_error


def as_bool(value: Any) -> bool:
    """Coerce a Juju action result to a bool.

    Juju renders action results through YAML, so a boolean may come back as a
    real bool or as its string form depending on the client version.
    """
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "yes", "1")


def unit_names(juju: jubilant.Juju, app: str = MICROCLOUD_CHARM) -> list[str]:
    """Return every unit name of ``app``, sorted."""
    return sorted(juju.status().apps[app].units)


def leader_unit(juju: jubilant.Juju, app: str = MICROCLOUD_CHARM) -> str:
    """Return the name of the leader unit of ``app``."""
    for name, unit in juju.status().apps[app].units.items():
        if unit.leader:
            return name
    raise AssertionError(f"no leader unit found for {app}")


def ssh(juju: jubilant.Juju, unit: str, command: str) -> str:
    """Run ``command`` on ``unit`` without allocating a pseudo-terminal."""
    return juju.cli("ssh", "--pty=false", unit, command)


def microcloud(juju: jubilant.Juju, unit: str, subcommand: str) -> str:
    """Run a ``microcloud`` subcommand as root on ``unit``."""
    return ssh(juju, unit, f"sudo microcloud {subcommand}")


def run_action(juju: jubilant.Juju, action: str, unit: str | None = None) -> dict[str, Any]:
    """Run a charm action on the leader (or ``unit``) and return its results."""
    task = juju.run(unit or leader_unit(juju), action)
    assert task.success, f"action {action} failed: {task.message}"
    return task.results


def cluster_members(juju: jubilant.Juju, unit: str | None = None) -> list[dict[str, str]]:
    """Return the MicroCloud members reported by the charm's "status" action."""
    results = run_action(juju, "status", unit)
    assert as_bool(results["initialized"]), f"MicroCloud is not initialized: {results}"
    assert as_bool(results.get("consistent")), f"MicroCloud membership is inconsistent: {results}"
    assert "members-error" not in results, results.get("members-error")
    assert "members" in results, f"status action reported no members: {results}"
    return json.loads(results["members"])


def member_addresses(juju: jubilant.Juju, unit: str | None = None) -> dict[str, str]:
    """Map each MicroCloud member name to its address."""
    return {member["name"]: member["address"] for member in cluster_members(juju, unit)}


def unit_hostnames(juju: jubilant.Juju) -> dict[str, str]:
    """Map each unit name to the hostname its machine reports."""
    return {unit: ssh(juju, unit, "hostname").strip() for unit in unit_names(juju)}


def snap_installed(juju: jubilant.Juju, unit: str, snap: str) -> bool:
    """Return True if ``snap`` is installed on ``unit``'s machine."""
    installed = ssh(juju, unit, "snap list")
    return any(line.split()[0] == snap for line in installed.splitlines()[1:] if line.split())


def scrape_configs(juju: jubilant.Juju, unit: str | None = None) -> list[dict[str, Any]]:
    """Return the cos-agent scrape configs advertised by a unit."""
    results = run_action(juju, "dump-metrics-config", unit)
    return json.loads(results["scrape-configs"])


def scrape_job_names(juju: jubilant.Juju, unit: str | None = None) -> set[str]:
    """Return the job names from the advertised scrape configs."""
    return {config["job_name"] for config in scrape_configs(juju, unit)}


def assert_status(
    juju: jubilant.Juju,
    unit: str,
    expected_count: int,
    expected_names: set[str] | None = None,
) -> None:
    """Assert the status action reports the expected MicroCloud membership."""
    members = cluster_members(juju, unit)
    assert len(members) == expected_count, (unit, members)
    names = {member["name"] for member in members}
    if expected_names is not None:
        assert names == expected_names, (unit, names, expected_names)
    for member in members:
        assert member["address"], (unit, member)
        assert member["status"].lower() in ("online", "ready"), (unit, member)


def assert_membership(
    juju: jubilant.Juju,
    expected_count: int,
    expected_names: set[str] | None = None,
) -> None:
    """Check MicroCloud membership against the Juju units."""
    hostnames = set(unit_hostnames(juju).values())
    if expected_names is not None:
        assert hostnames == expected_names, (hostnames, expected_names)
    assert len(hostnames) == expected_count, hostnames
    assert_status(juju, leader_unit(juju), expected_count, hostnames)


def assert_cluster_list(juju: jubilant.Juju, unit: str, expected_names: set[str]) -> None:
    """Assert ``microcloud cluster list`` on ``unit`` contains every expected member."""
    output = microcloud(juju, unit, "cluster list")
    missing = {name for name in expected_names if name not in output}
    assert not missing, f"{unit} is missing {missing} from the cluster list:\n{output}"


def wait_active(
    juju: jubilant.Juju,
    *,
    message: str | None = None,
    expected_units: int | None = None,
    timeout: int = DEPLOY_TIMEOUT_IN_SECONDS,
) -> None:
    """Wait for the MicroCloud application to become active."""

    def converged(status: jubilant.Status) -> bool:
        app = status.apps.get(MICROCLOUD_CHARM)
        if app is None:
            return False
        if expected_units is not None and len(app.units) != expected_units:
            return False
        if not jubilant.all_active(status, MICROCLOUD_CHARM):
            return False
        if message is not None:
            return all(message in unit.workload_status.message for unit in app.units.values())
        return True

    juju.wait(
        converged,
        error=lambda status: jubilant.any_error(status, MICROCLOUD_CHARM),
        timeout=timeout,
        delay=5,
    )


def wait_blocked(
    juju: jubilant.Juju,
    message: str,
    *,
    expected_units: int | None = None,
    timeout: int = DEPLOY_TIMEOUT_IN_SECONDS,
) -> None:
    """Wait for every MicroCloud unit to report a specific blocked message."""

    def blocked(status: jubilant.Status) -> bool:
        app = status.apps.get(MICROCLOUD_CHARM)
        if app is None:
            return False
        if expected_units is not None and len(app.units) != expected_units:
            return False
        return bool(app.units) and all(
            unit.workload_status.current == "blocked" and message in unit.workload_status.message
            for unit in app.units.values()
        )

    juju.wait(
        blocked,
        error=lambda status: jubilant.any_error(status, MICROCLOUD_CHARM),
        timeout=timeout,
        delay=5,
    )


def wait_units_active(
    juju: jubilant.Juju,
    count: int,
    *,
    timeout: int = DEPLOY_TIMEOUT_IN_SECONDS,
) -> None:
    """Wait until the application has ``count`` units, all of them active."""

    def converged(status: jubilant.Status) -> bool:
        app = status.apps.get(MICROCLOUD_CHARM)
        return (
            app is not None
            and len(app.units) == count
            and jubilant.all_active(status, MICROCLOUD_CHARM)
        )

    juju.wait(
        converged,
        error=lambda status: jubilant.any_error(status, MICROCLOUD_CHARM),
        timeout=timeout,
        delay=5,
    )


def deploy_charm(juju: jubilant.Juju, charm_path: Path, machine: str) -> None:
    """Deploy the packed charm onto an existing Juju machine."""
    juju.deploy(charm_path, to=machine)


def assert_local_metrics(juju: jubilant.Juju, unit: str, url: str) -> None:
    """Assert a local metrics endpoint responds from inside ``unit``."""
    retry(
        lambda: ssh(juju, unit, f"curl -ksf '{url}' >/dev/null"),
        attempts=60,
        delay=5,
        description=f"metrics endpoint {url} on {unit}",
    )


def assert_ovn_exporter_active(juju: jubilant.Juju, unit: str) -> None:
    """Assert the ovn-exporter snap service is active on ``unit``."""

    def check() -> None:
        output = ssh(juju, unit, "snap services ovn-exporter.ovn-exporter")
        states = [
            fields[2]
            for line in output.splitlines()[1:]  # skip the header row
            if (fields := line.split()) and fields[0] == "ovn-exporter.ovn-exporter"
        ]
        assert states == ["active"], output

    retry(check, attempts=60, delay=5, description=f"ovn-exporter on {unit}")


def assert_no_ovn_exporter(juju: jubilant.Juju, unit: str) -> None:
    """Assert the ovn-exporter snap is not installed on ``unit``."""
    assert not snap_installed(juju, unit, "ovn-exporter"), f"ovn-exporter is installed on {unit}"
