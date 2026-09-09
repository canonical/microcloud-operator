"""Configuration failures recover through Juju alone, before and after bootstrap.

This module owns a separate model so failures cannot poison the smoke tests.
The setup test leaves a formed cluster for the subsequent recovery test.
"""

from __future__ import annotations

from pathlib import Path

import jubilant
import pytest

from tests.integration.helpers import (
    DEPLOY_TIMEOUT_IN_SECONDS,
    MICROCLOUD_CHARM,
    as_bool,
    assert_membership,
    deploy_microcloud,
    member_addresses,
    run_action,
    unit_hostnames,
    unit_names,
    wait_active,
)

# An empty mapping cannot contain any host, regardless of substrate naming.
INVALID_UPLINK = "{}"


def wait_missing_uplink(juju: jubilant.Juju, num_units: int) -> None:
    """Wait for each unit to report the expected, actionable validation error."""

    def blocked(status: jubilant.Status) -> bool:
        app = status.apps.get(MICROCLOUD_CHARM)
        if app is None or len(app.units) != num_units:
            return False
        return all(
            unit.workload_status.current == "blocked"
            and "ovn-uplink-interface is missing an entry for hostname"
            in unit.workload_status.message
            for unit in app.units.values()
        )

    juju.wait(
        blocked,
        error=lambda status: jubilant.any_error(status, MICROCLOUD_CHARM),
        timeout=DEPLOY_TIMEOUT_IN_SECONDS,
        delay=10,
    )
    hostnames = unit_hostnames(juju)
    status = juju.status()
    for unit, hostname in hostnames.items():
        assert repr(hostname) in status.apps[MICROCLOUD_CHARM].units[unit].workload_status.message


@pytest.mark.juju_setup
def test_invalid_config_blocks_before_bootstrap(
    juju: jubilant.Juju,
    charm_path: Path,
    constraints: dict[str, str],
    num_units: int,
) -> None:
    """Missing host mappings block early; correcting config bootstraps all units."""
    deploy_microcloud(
        juju,
        charm_path,
        num_units,
        constraints=constraints,
        config={"ovn-uplink-interface": INVALID_UPLINK},
    )
    wait_missing_uplink(juju, num_units)
    for unit in unit_names(juju):
        results = run_action(juju, "status", unit)
        assert not as_bool(results["initialized"]), (unit, results)
        assert "members" not in results, (unit, results)

    juju.cli("config", MICROCLOUD_CHARM, "ovn-uplink-interface=")
    wait_active(juju, recovering=True)
    assert_membership(juju, num_units)


def test_config_recovery_preserves_cluster(juju: jubilant.Juju, num_units: int) -> None:
    """Repeated config reconciliation recovers without replacing cluster members."""
    assert_membership(juju, num_units)
    before = member_addresses(juju)
    for _ in range(2):
        juju.cli("config", MICROCLOUD_CHARM, f"ovn-uplink-interface={INVALID_UPLINK}")
        try:
            wait_missing_uplink(juju, num_units)
            assert_membership(juju, num_units)
            assert member_addresses(juju) == before
        finally:
            juju.cli("config", MICROCLOUD_CHARM, "ovn-uplink-interface=")
        # Observing blocked first avoids accepting stale active status before
        # config-changed has actually run on every unit.
        wait_active(juju, recovering=True)
        assert_membership(juju, num_units)
        assert member_addresses(juju) == before
