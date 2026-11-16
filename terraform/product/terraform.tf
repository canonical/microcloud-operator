# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

terraform {
  # 1.9 lets a variable's validation refer to another variable, which the
  # model, proxy and COS inputs rely on. The charm module needs it too.
  required_version = ">= 1.9"

  required_providers {
    juju = {
      source = "juju/juju"
      # v2.0.0 is the first that supports Juju 4 controllers, and it keeps
      # juju_model's target_controller (added in 1.2.0), which this module sets.
      version = ">= 2.0.0, < 3.0.0"
    }
  }
}
