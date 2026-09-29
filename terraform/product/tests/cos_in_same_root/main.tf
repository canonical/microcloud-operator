# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

# COS in the same root, as the README suggests: the offer's url is only known
# after apply, and the product module must still plan.

terraform {
  required_providers {
    juju = {
      source = "juju/juju"
    }
  }
}

resource "juju_offer" "metrics" {
  model_uuid       = "22222222-2222-2222-2222-222222222222"
  application_name = "mimir"
  endpoints        = ["receive-remote-write"]
}

module "microcloud" {
  source = "../.."

  cos = {
    metrics = { url = juju_offer.metrics.url }
  }
}

output "components" {
  value = keys(module.microcloud.models.microcloud.components)
}
