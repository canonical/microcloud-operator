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
    configure,
    deploy_microcloud,
    member_addresses,
    run_action,
    unit_hostnames,
    unit_names,
    wait_active,
)

# Enabling MicroOVN with a gateway makes the uplink mandatory. The units are
# LXD containers whose only NIC is an addressed veth, so there is no unaddressed
# interface to take as the uplink and the charm blocks before installing any
# snap. Clearing the MicroOVN channel goes back to the LXD-only profile.
NO_UPLINK_CONFIG: dict[str, str] = {
    "snap-channel-microovn": "latest/stable",
    "ovn-ipv4-gateway": "192.0.2.1/24",
}
RECOVERED_CONFIG = "snap-channel-microovn="


def wait_missing_uplink(juju: jubilant.Juju, num_units: int) -> None:
    """Wait for each unit to report the expected, actionable validation error."""

    def blocked(status: jubilant.Status) -> bool:
        app = status.apps.get(MICROCLOUD_CHARM)
        if app is None or len(app.units) != num_units:
            return False
        return all(
            unit.workload_status.current == "blocked"
            and unit.workload_status.message.startswith("No OVN uplink on ")
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
        assert hostname in status.apps[MICROCLOUD_CHARM].units[unit].workload_status.message


@pytest.mark.juju_setup
def test_invalid_config_blocks_before_bootstrap(
    juju: jubilant.Juju,
    charm_path: Path,
    constraints: dict[str, str],
    num_units: int,
) -> None:
    """A missing uplink blocks early; correcting config bootstraps all units."""
    deploy_microcloud(
        juju,
        charm_path,
        num_units,
        constraints=constraints,
        config=NO_UPLINK_CONFIG,
    )
    wait_missing_uplink(juju, num_units)
    for unit in unit_names(juju):
        results = run_action(juju, "status", unit)
        assert not as_bool(results["initialized"]), (unit, results)
        assert "members" not in results, (unit, results)

    juju.cli("config", MICROCLOUD_CHARM, RECOVERED_CONFIG)
    wait_active(juju, recovering=True)
    assert_membership(juju, num_units)


def test_config_after_bootstrap_preserves_cluster(juju: jubilant.Juju, num_units: int) -> None:
    """The uplink is only discovered before bootstrap; later config leaves members alone."""
    assert_membership(juju, num_units)
    before = member_addresses(juju)
    configure(juju, *(f"{key}={value}" for key, value in NO_UPLINK_CONFIG.items()))
    try:
        wait_active(juju)
        assert_membership(juju, num_units)
        assert member_addresses(juju) == before
    finally:
        configure(juju, RECOVERED_CONFIG, "ovn-ipv4-gateway=")
    wait_active(juju)
    assert_membership(juju, num_units)
    assert member_addresses(juju) == before
