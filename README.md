# MicroCloud charm for MAAS

<img align="right" alt="MicroCloud logo" src="https://documentation.ubuntu.com/microcloud/en/latest/_static/microcloud_tag.png">

This charm deploys MicroCloud using Juju.

## Build the charm

* Install `charmcraft` to build the charm: `snap install charmcraft --classic`
* Build the charm: `charmcraft pack`
  * For example use `--platform ubuntu@24.04:amd64` to only build the charm for a specific base and architecture

## Deploy the charm

Run `juju deploy <charm>`.
