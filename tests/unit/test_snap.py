# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from pathlib import Path

import yaml

import snap


def test_default_channels_match_charmcraft_yaml():
    charmcraft = yaml.safe_load((Path(__file__).parents[2] / "charmcraft.yaml").read_text())
    options = charmcraft["config"]["options"]

    assert {
        name: options[f"snap-channel-{name}"]["default"] for name in snap.DEFAULT_CHANNELS
    } == snap.DEFAULT_CHANNELS


def test_channel_falls_back_to_the_default():
    assert snap.channel({}, "microceph") == snap.DEFAULT_CHANNEL_MICROCEPH


def test_an_empty_channel_leaves_the_snap_out():
    assert snap.channel({"snap-channel-microovn": ""}, "microovn") == ""
    assert not snap.channel({"snap-channel-microceph": ""}, "microceph")


def test_channels_lists_every_snap():
    channels = snap.channels({"snap-channel-lxd": "5.21/stable"})

    assert channels == {**snap.DEFAULT_CHANNELS, "lxd": "5.21/stable"}
