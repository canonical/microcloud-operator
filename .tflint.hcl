# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

config {
  call_module_type = "local"
}

plugin "terraform" {
  enabled = true
  preset  = "recommended"
}
