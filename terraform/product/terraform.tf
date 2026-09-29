# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

terraform {
  # 1.9 lets a variable's validation refer to another variable, which the
  # model, proxy and COS inputs rely on. The charm module needs it too.
  required_version = ">= 1.9"

  required_providers {
    juju = {
      source = "juju/juju"
      # 1.2.0 is the first with juju_model's target_controller, which this
      # module sets; the charm module it calls accepts 1.0.0 and up.
      version = ">= 1.2.0, < 3.0.0"
    }
  }
}
