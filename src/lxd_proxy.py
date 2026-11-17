# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Carry the Juju model's proxy into LXD.

LXD downloads images itself, and as a snap service it sees no proxy from the
machine's environment. Juju hands the charm the model's ``juju-http-proxy``,
``juju-https-proxy`` and ``juju-no-proxy`` in the hook environment, so the
orchestrator's leader mirrors them into LXD's cluster-wide ``core.proxy_*``
keys.

While the model sets a proxy, the model wins. Once it no longer does, the
charm unsets only the values it set itself, so a proxy an operator configured
in LXD directly stays.
"""

import os
from collections.abc import Mapping

import lxd_cluster

# LXD key -> the hook environment variable Juju fills from the model.
KEYS = {
    "core.proxy_http": "JUJU_CHARM_HTTP_PROXY",
    "core.proxy_https": "JUJU_CHARM_HTTPS_PROXY",
    "core.proxy_ignore_hosts": "JUJU_CHARM_NO_PROXY",
}


def model_proxy() -> dict[str, str]:
    """Return the model's proxy settings as LXD keys; a key the model does not set is empty."""
    return {key: os.environ.get(variable, "").strip() for key, variable in KEYS.items()}


def reconcile(applied: Mapping[str, str]) -> dict[str, str]:
    """Bring LXD's proxy keys in line with the model."""
    current = lxd_cluster.server_config()
    changes: dict[str, str] = {}
    owned: dict[str, str] = {}
    for key, wanted in model_proxy().items():
        have = current.get(key, "")
        if wanted:
            owned[key] = wanted
            if have != wanted:
                changes[key] = wanted
        elif applied.get(key) and have == applied[key]:
            changes[key] = ""
    if changes:
        lxd_cluster.update_server_config(changes)
    return owned
