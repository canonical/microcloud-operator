# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

resource "juju_model" "microcloud" {
  count = local.create_model ? 1 : 0

  name = local.model_name

  dynamic "cloud" {
    for_each = var.model.cloud != null ? [var.model.cloud] : []
    content {
      name   = cloud.value.name
      region = cloud.value.region
    }
  }

  annotations       = var.model.annotations
  config            = length(local.model_config) > 0 ? local.model_config : null
  constraints       = var.model.constraints
  credential        = var.model.credential
  target_controller = var.model.target_controller
}

data "juju_model" "microcloud" {
  count = local.create_model ? 0 : 1

  uuid = var.model.uuid
}

resource "juju_storage_pool" "microcloud" {
  for_each = var.storage_pools

  name             = each.key
  model_uuid       = local.model_uuid
  storage_provider = each.value.storage_provider
  attributes       = each.value.attributes
}

resource "terraform_data" "deployed_at" {
  input = timestamp()

  lifecycle {
    ignore_changes = [input]
  }
}

# Stamped again only when what is deployed changes: an input, or a default
# this module resolves it to. A converged deployment then plans no changes,
# not even to outputs.
resource "terraform_data" "updated_at" {
  input = timestamp()

  triggers_replace = [
    local.version,
    local.channels,
    local.microcloud_units,
    local.model_config,
    local.model_name,
    local.tracks,
    var.cos,
    var.logging_config,
    var.microcloud,
    var.model,
    var.opentelemetry_collector,
    var.proxy,
    var.risk,
    var.storage_pools,
  ]

  lifecycle {
    ignore_changes = [input]
  }
}
