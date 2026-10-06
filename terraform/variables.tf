# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

variable "app_name" {
  description = "Name of the application in the Juju model."
  type        = string
  default     = "microcloud"

  validation {
    condition     = can(regex("^[a-z]([a-z0-9-]*[a-z0-9])?$", var.app_name))
    error_message = "app_name must be lowercase alphanumeric with dashes, e.g. \"microcloud\"."
  }
}

variable "base" {
  description = "Operating system base to deploy on, e.g. ubuntu@24.04."
  type        = string
  default     = "ubuntu@24.04"

  validation {
    condition     = var.base == null || contains(["ubuntu@24.04", "ubuntu@26.04"], coalesce(var.base, "unset"))
    error_message = "base must be ubuntu@24.04 or ubuntu@26.04, the platforms the charm declares."
  }
}

variable "channel" {
  description = "Charm channel to deploy, as <track>/<risk>[/<branch>]."
  type        = string
  default     = "3/edge"

  validation {
    condition     = can(regex("^[a-zA-Z0-9.-]+/(stable|candidate|beta|edge)(/[a-zA-Z0-9.-]+)?$", var.channel))
    error_message = "channel must be <track>/<risk>[/<branch>], e.g. \"3/stable\" or \"3/edge/eken\"."
  }
}

variable "config" {
  description = <<-EOT
    Charm config options, passed through unchanged. Every value is a string:
    "true" for booleans, "300" for integers. Options that take a mapping of
    hostname to value ("local-device", "ovn-uplink-interface") take a
    yamlencode(...) of that map. See the charm's charmcraft.yaml for the
    available options; this module deliberately does not restate them.
  EOT
  type        = map(string)
  default     = {}

  validation {
    # lookup() rather than an index guarded by contains(): "||" is not lazy
    # in older Terraform, so the index is evaluated even when the key is
    # absent and an empty config fails validation.
    condition = alltrue([
      for key in ["ovn-ipv4-gateway", "ovn-ipv6-gateway"] :
      lookup(var.config, key, "") == "" ||
      can(regex("^[0-9a-fA-F:.]+/[0-9]{1,3}$", lookup(var.config, key, "")))
    ])
    error_message = "ovn-ipv4-gateway and ovn-ipv6-gateway must be CIDR notation, e.g. \"192.0.2.1/24\"."
  }
}

variable "constraints" {
  description = "Juju constraints for the application, e.g. \"arch=amd64 tags=microcloud\"."
  type        = string
  default     = null
}

variable "endpoint_bindings" {
  description = <<-EOT
    Bindings of endpoints to Juju spaces. An entry with no endpoint sets the
    application's default space. On MAAS the extra-bindings are what put each
    MicroCloud network on its own space.
  EOT
  type = set(object({
    endpoint = optional(string)
    space    = string
  }))
  default  = []
  nullable = false

  validation {
    # A conditional rather than "binding.endpoint == null || contains(...)":
    # "||" is not lazy in older Terraform, so contains() is called with a
    # null and errors. An absent endpoint means the default space, same as "".
    condition = alltrue([
      for binding in var.endpoint_bindings :
      contains(
        ["", "cluster", "cos-agent", "logging",
        "ceph-public", "ceph-internal", "ovn-underlay", "ovn-uplink"],
        binding.endpoint == null ? "" : binding.endpoint
      )
    ])
    error_message = "endpoint must be \"\" or one of the charm's endpoints: cluster, cos-agent, logging, ceph-public, ceph-internal, ovn-underlay, ovn-uplink."
  }
}

variable "machines" {
  description = <<-EOT
    Machines to place units on, e.g. ["0", "1", "2"]. The unit count becomes
    the size of this set; "units" must be left at 1 or match it.
  EOT
  type        = set(string)
  default     = []
  nullable    = false

  validation {
    condition = alltrue([
      for machine in var.machines :
      can(regex("^[0-9]+(/(lxd|kvm)/[0-9]+)?$", machine))
    ])
    error_message = "machines must be Juju machine IDs, e.g. \"0\" or \"0/lxd/1\"."
  }
}

variable "model_uuid" {
  description = "UUID of the Juju model to deploy into."
  type        = string
  nullable    = false
}

variable "revision" {
  description = "Charm revision to deploy. Defaults to the channel's latest."
  type        = number
  default     = null

  validation {
    condition     = var.revision == null || coalesce(var.revision, 1) > 0
    error_message = "revision must be a positive number."
  }
}

variable "storage_directives" {
  description = <<-EOT
    Juju storage directives keyed by storage name, e.g.
    { local = "maas,1,100G", ceph = "maas,3,100G" }. Both are optional: the
    charm accepts zero local disks and zero Ceph disks.
  EOT
  type        = map(string)
  default     = {}
  nullable    = false

  validation {
    condition = alltrue([
      for name in keys(var.storage_directives) :
      contains(["local", "ceph"], name)
    ])
    error_message = "storage_directives keys must be \"local\" or \"ceph\", the storage the charm declares."
  }
}

variable "units" {
  description = <<-EOT
    Number of units to deploy. With "machines" set, the unit count is the
    number of machines, so this must be left at 1 or match it.
  EOT
  type        = number
  default     = 1
  nullable    = false

  validation {
    condition     = var.units >= 1
    error_message = "units must be at least 1."
  }

  # CC008 fixes the default at 1, so a deliberate 1 cannot be told apart from
  # an untouched default; any other value must agree with machines. An empty
  # machines set counts as unset, matching main.tf.
  validation {
    condition = (
      length(var.machines) == 0 ||
      var.units == 1 ||
      var.units == length(var.machines)
    )
    error_message = "units contradicts machines: with machines set, the unit count is length(machines). Leave units at 1 or match it."
  }
}
