# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

resource "juju_application" "microcloud" {
  name       = var.app_name
  model_uuid = var.model_uuid

  charm {
    name     = "microcloud"
    channel  = var.channel
    revision = var.revision
    base     = var.base
  }

  config             = var.config
  constraints        = var.constraints
  endpoint_bindings  = local.endpoint_bindings
  machines           = local.placed ? var.machines : null
  storage_directives = local.storage_directives

  # The provider refuses both at once, and a set of machines already fixes the
  # unit count. variables.tf rejects a units value that disagrees.
  units = local.placed ? null : var.units
}
