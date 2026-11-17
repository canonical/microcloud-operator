# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for starting Ceph only once BGP can carry its traffic."""

import subprocess
import time
from unittest.mock import patch

import pytest

import boot_order
from boot_order import BootOrderError

ESTABLISHED = "Name       Proto      Table      State  Since         Info\nuplink_a   BGP        ---        up     12:00:00.000  Established\n"
CONNECTING = "Name       Proto      Table      State  Since         Info\nuplink_a   BGP        ---        start  12:00:00.000  Connect\n"


class _Systemctl:
    """A fake systemctl that records each call and reports NeedDaemonReload."""

    def __init__(self, need_reload="no", fail_reload=False):
        self.need_reload = need_reload
        self.fail_reload = fail_reload
        self.calls = []

    def __call__(self, command, **kwargs):
        self.calls.append(command)
        if command[1] == "daemon-reload" and self.fail_reload:
            raise subprocess.CalledProcessError(1, command, stderr="Access denied\n")
        stdout = ""
        if command[1] == "show":
            stdout = "\n".join(self.need_reload for _ in boot_order.CEPH_UNITS) + "\n"
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    @property
    def reloads(self):
        return sum(1 for command in self.calls if command[1] == "daemon-reload")


def _systemctl(**kwargs):
    fake = _Systemctl(**kwargs)
    return patch("boot_order.subprocess.run", side_effect=fake), fake


def _reload():
    return _systemctl()[0]


def _paths(tmp_path):
    root = tmp_path / "systemd"
    script = tmp_path / "sbin" / "microcloud-wait-for-bgp"
    return root, script


class TestDropin:
    def test_orders_ceph_after_bird_and_waits_for_bgp(self):
        text = boot_order.dropin()
        assert "After=snap.microovn.bird.service\n" in text
        assert f"ExecStartPre=-{boot_order.WAIT_SCRIPT} {boot_order.WAIT_LIMIT}\n" in text

    def test_the_start_timeout_outlasts_the_wait(self):
        timeout = int(boot_order.dropin().split("TimeoutStartSec=")[1])
        assert timeout > boot_order.WAIT_LIMIT


class TestEnsure:
    def test_installs_a_dropin_for_every_ceph_daemon(self, tmp_path):
        root, script = _paths(tmp_path)
        patcher, fake = _systemctl()
        with patcher:
            assert boot_order.ensure(True, root, script) is True

        for unit in boot_order.CEPH_UNITS:
            path = root / f"{unit}.d" / boot_order.DROPIN_NAME
            assert path.read_text() == boot_order.dropin()
        assert script.read_text() == boot_order.WAIT_SCRIPT_CONTENT
        assert script.stat().st_mode & 0o777 == 0o755
        assert fake.calls == [["systemctl", "daemon-reload"]]

    def test_does_not_reload_when_nothing_changed(self, tmp_path):
        root, script = _paths(tmp_path)
        with _reload():
            boot_order.ensure(True, root, script)
        patcher, fake = _systemctl()
        with patcher:
            assert boot_order.ensure(True, root, script) is False
        assert fake.reloads == 0

    def test_rewrites_an_edited_dropin(self, tmp_path):
        root, script = _paths(tmp_path)
        with _reload():
            boot_order.ensure(True, root, script)
        edited = root / f"{boot_order.CEPH_UNITS[0]}.d" / boot_order.DROPIN_NAME
        edited.write_text("[Unit]\n")
        patcher, fake = _systemctl()
        with patcher:
            assert boot_order.ensure(True, root, script) is True
        assert edited.read_text() == boot_order.dropin()
        assert fake.reloads == 1

    def test_removes_everything_once_bird_is_not_in_use(self, tmp_path):
        root, script = _paths(tmp_path)
        with _reload():
            boot_order.ensure(True, root, script)
        patcher, fake = _systemctl()
        with patcher:
            assert boot_order.ensure(False, root, script) is True

        assert not list(root.rglob(boot_order.DROPIN_NAME))
        assert not script.exists()
        assert fake.reloads == 1

    def test_nothing_to_remove_does_not_reload(self, tmp_path):
        root, script = _paths(tmp_path)
        patcher, fake = _systemctl()
        with patcher:
            assert boot_order.ensure(False, root, script) is False
        assert fake.reloads == 0

    def test_a_failed_reload_raises(self, tmp_path):
        root, script = _paths(tmp_path)
        patcher, _ = _systemctl(fail_reload=True)
        with patcher:
            with pytest.raises(BootOrderError, match="Access denied"):
                boot_order.ensure(True, root, script)

    @pytest.mark.parametrize("enabled", [True, False])
    def test_retries_a_reload_that_failed(self, tmp_path, enabled):
        root, script = _paths(tmp_path)
        if not enabled:
            with _reload():
                boot_order.ensure(True, root, script)
        patcher, _ = _systemctl(fail_reload=True)
        with patcher:
            with pytest.raises(BootOrderError):
                boot_order.ensure(enabled, root, script)

        # The files are now as wanted, but systemd still has the old units.
        patcher, fake = _systemctl(need_reload="yes")
        with patcher:
            assert boot_order.ensure(enabled, root, script) is False
        assert fake.reloads == 1

    def test_an_unreadable_reload_state_does_not_block(self, tmp_path, caplog):
        # The check runs on every reconcile, even where no drop-in is wanted.
        root, script = _paths(tmp_path)
        error = subprocess.CalledProcessError(1, ["systemctl"], stderr="Failed to connect\n")
        with patch("boot_order.subprocess.run", side_effect=error):
            assert boot_order.ensure(False, root, script) is False
        assert "systemctl show: Failed to connect" in caplog.text

    def test_files_are_replaced_not_rewritten_in_place(self, tmp_path):
        # A daemon running the old script keeps reading the old file.
        root, script = _paths(tmp_path)
        with _reload():
            boot_order.ensure(True, root, script)
        script.write_text("#!/bin/sh\n")
        old = script.open()
        try:
            with _reload():
                boot_order.ensure(True, root, script)
            assert old.read() == "#!/bin/sh\n"
        finally:
            old.close()
        assert script.read_text() == boot_order.WAIT_SCRIPT_CONTENT
        assert script.stat().st_mode & 0o777 == 0o755
        assert not list(script.parent.glob(".*"))

    def test_a_failed_write_leaves_no_temporary_file(self, tmp_path):
        root, script = _paths(tmp_path)
        with patch("boot_order.os.replace", side_effect=OSError("read-only")):
            with pytest.raises(BootOrderError, match="read-only"):
                boot_order.ensure(True, root, script)
        assert not list(script.parent.iterdir())


class TestWaitScript:
    """The script systemd runs before each MicroCeph daemon."""

    def _script(self, tmp_path, birdc_output, hang=False, bird_enabled=True):
        birdc = tmp_path / "birdc"
        body = "exec sleep 60\n" if hang else f"cat <<'END'\n{birdc_output}END\n"
        birdc.write_text(f"#!/bin/sh\ntouch {tmp_path}/birdc-ran\n{body}")
        birdc.chmod(0o755)
        systemctl = tmp_path / "systemctl"
        systemctl.write_text(f"#!/bin/sh\nexit {0 if bird_enabled else 1}\n")
        systemctl.chmod(0o755)
        script = tmp_path / "wait-for-bgp"
        script.write_text(
            boot_order.WAIT_SCRIPT_CONTENT.replace("/snap/bin/microovn.birdc", str(birdc)).replace(
                "systemctl is-enabled", f"{systemctl} is-enabled"
            )
        )
        script.chmod(0o755)
        return script

    def test_returns_at_once_when_bird_is_not_enabled(self, tmp_path):
        # As when the charm has gone and left the drop-ins behind.
        start = time.monotonic()
        result = subprocess.run(
            [str(self._script(tmp_path, CONNECTING, bird_enabled=False)), "5"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert time.monotonic() - start < 2
        assert not (tmp_path / "birdc-ran").exists()

    def test_returns_at_once_with_a_session_established(self, tmp_path):
        start = time.monotonic()
        result = subprocess.run(
            [str(self._script(tmp_path, ESTABLISHED)), "5"], capture_output=True, text=True
        )
        assert result.returncode == 0
        assert time.monotonic() - start < 2
        assert result.stderr == ""

    def test_gives_up_after_the_limit_but_still_succeeds(self, tmp_path):
        result = subprocess.run(
            [str(self._script(tmp_path, CONNECTING)), "1"], capture_output=True, text=True
        )
        assert result.returncode == 0
        assert "starting anyway" in result.stderr

    def test_a_hung_birdc_does_not_outlast_the_limit(self, tmp_path):
        start = time.monotonic()
        result = subprocess.run(
            [str(self._script(tmp_path, "", hang=True)), "1"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0
        assert "starting anyway" in result.stderr
        # One birdc call (5 s timeout) and one sleep, not the 60 s hang.
        assert time.monotonic() - start < 10
