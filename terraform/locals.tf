# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

locals {
  endpoint_bindings = length(var.endpoint_bindings) > 0 ? var.endpoint_bindings : null

  # Emptiness, not null: a caller computing "machines" from another resource
  # can hand us [], and the provider treats even an empty set as placement,
  # refusing "units" alongside it. Normalize it away so [] behaves as unset.
  placed = length(var.machines) > 0

  # CC008 defaults the collections to empty; the provider still gets null
  # for them, as before, rather than an empty value it may diff against.
  storage_directives = length(var.storage_directives) > 0 ? var.storage_directives : null
}
