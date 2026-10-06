# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

module "microcloud" {
  # Same repository, so a relative source: it is always the charm module from
  # the commit this module was fetched at.
  source = "../"

  app_name           = var.microcloud.app_name
  base               = var.microcloud.base
  channel            = local.channels.microcloud
  config             = var.microcloud.config
  constraints        = var.microcloud.constraints
  endpoint_bindings  = var.microcloud.endpoint_bindings
  machines           = var.microcloud.machines
  model_uuid         = local.model_uuid
  revision           = var.microcloud.revision
  storage_directives = var.microcloud.storage_directives
  units              = local.microcloud_units

  # A storage directive naming a pool fails if the pool does not exist yet.
  depends_on = [juju_storage_pool.microcloud]
}

# Deployed directly rather than through the charm's own module, which only
# accepts dev/ channels, cannot set a base (a subordinate's must match its
# principal's) and sets units, which a subordinate must not.
# TODO: Switch to the upstream module, pinned to a tag, once it allows these.
resource "juju_application" "opentelemetry_collector" {
  count = local.cos_enabled ? 1 : 0

  name       = var.opentelemetry_collector.app_name
  model_uuid = local.model_uuid
  config     = var.opentelemetry_collector.config

  charm {
    name     = "opentelemetry-collector"
    base     = var.microcloud.base
    channel  = local.channels.opentelemetry_collector
    revision = var.opentelemetry_collector.revision
  }
}
