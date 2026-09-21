# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

locals {
  # Emptiness, not null: a caller computing "machines" from another resource
  # can hand us [], and the provider treats even an empty set as placement,
  # refusing "units" alongside it. Normalise it away so [] behaves as unset.
  placed = length(coalesce(var.machines, [])) > 0
}

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
  endpoint_bindings  = var.endpoint_bindings
  machines           = local.placed ? var.machines : null
  storage_directives = var.storage_directives

  # The provider refuses both at once, and a set of machines already fixes the
  # unit count. variables.tf rejects a units value that disagrees.
  units = local.placed ? null : var.units
}
