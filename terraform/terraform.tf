# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

terraform {
  # 1.9 lets the "units" validation refer to "machines".
  required_version = ">= 1.9"

  required_providers {
    juju = {
      source = "juju/juju"
      # v1.0.0 renamed "model" to "model_uuid" and replaced "placement" with
      # "machines", both of which this module uses.
      version = ">= 1.0.0, < 3.0.0"
    }
  }
}
