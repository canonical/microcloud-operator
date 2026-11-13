# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import patch

import pytest

import dpu


@pytest.fixture
def interfaces(tmp_path):
    bond = tmp_path / "bond0/bonding"
    bond.mkdir(parents=True)
    (bond / "mode").write_text("active-backup 1\n")
    (bond / "slaves").write_text("p0 p1\n")
    (tmp_path / "pf0hpf").mkdir()
    with (
        patch.object(dpu, "SYS_NET", tmp_path),
        patch.object(dpu, "NETWORKD_FILE", tmp_path / "90-dpu.network"),
    ):
        yield tmp_path


def test_dpu_bridge_uses_system_ovs_and_preserves_the_maas_bond(interfaces):
    calls = []

    def run(*args, **kwargs):
        calls.append(args)
        if "port-to-br" in args or "br-exists" in args:
            return None
        return ""

    with (
        patch.object(dpu, "_run", side_effect=run),
        patch("shutil.which", return_value="/usr/bin/ovs-vsctl"),
    ):
        dpu.ensure_bridge("br-dpu", "bond0", "pf0hpf")
    assert any("add-br" in args and "datapath_type=system" in args for args in calls)
    bridge = next(args for args in calls if "add-br" in args)
    assert bridge.count("add-port") == 2
    assert "bond0" in bridge and "pf0hpf" in bridge
    assert not any(
        "microovn" in " ".join(args) or "snap" in args or "add-bond" in args or "netplan" in args
        for args in calls
    )


def test_existing_foreign_bridge_is_not_replaced(interfaces):
    with (
        patch.object(dpu, "_run", return_value="other-bridge") as run,
        patch("shutil.which", return_value="/usr/bin/ovs-vsctl"),
    ):
        with pytest.raises(dpu.DPUError, match="already belongs"):
            dpu.ensure_bridge("br-dpu", "bond0", "pf0hpf")
        assert not any("add-br" in call.args for call in run.call_args_list)


def test_missing_maas_bond_is_not_created(tmp_path):
    with patch.object(dpu, "SYS_NET", tmp_path), patch.object(dpu, "_run") as run:
        with pytest.raises(dpu.DPUError, match="MAAS bond"):
            dpu.ensure_bridge("br-dpu", "bond0", "pf0hpf")
        run.assert_not_called()


@pytest.mark.parametrize("bond", ["oob_net0", "eth0", "lo", "../bond0"])
def test_unsafe_or_management_interfaces_are_rejected(interfaces, bond):
    with pytest.raises(dpu.DPUError):
        dpu.validate("br-dpu", bond, "pf0hpf")
