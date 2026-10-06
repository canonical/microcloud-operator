# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

variable "cos" {
  description = <<-EOT
    Integrations with a Canonical Observability Stack, each optional, in
    CC008's external integration shape. Setting any of them deploys an
    opentelemetry-collector subordinate on MicroCloud and relates it to that
    offer:
      dashboards: grafana_dashboard, e.g. COS's "grafana-dashboards".
      logging: loki_push_api, e.g. COS's "loki-logging". Also routes LXD's
        own log stream through the collector.
      metrics: prometheus_remote_write, e.g. COS's
        "mimir-receive-remote-write".
    Each takes the offer url, and controller when the offer lives on another
    controller; that name must be configured in the provider's
    offering_controllers. COS runs on Kubernetes, so only kind "offer" is
    supported.
  EOT
  type = object({
    dashboards = optional(object({
      kind       = optional(string, "offer")
      url        = string
      controller = optional(string)
    }))
    logging = optional(object({
      kind       = optional(string, "offer")
      url        = string
      controller = optional(string)
    }))
    metrics = optional(object({
      kind       = optional(string, "offer")
      url        = string
      controller = optional(string)
    }))
  })
  default  = {}
  nullable = false

  validation {
    condition     = alltrue([for integration in values(var.cos) : integration.kind == "offer" if integration != null])
    error_message = "COS integrations must be of kind \"offer\"; COS is in another model."
  }

  validation {
    # [<owner>/]<model>.<offer>. The provider refuses a "<controller>:"
    # prefix; controller names the controller instead.
    condition = alltrue([
      for integration in values(var.cos) :
      can(regex("^([^:/\\s]+/)?[^:/.\\s]+\\.[^:/.\\s]+$", integration.url)) if integration != null
    ])
    error_message = "COS offer urls must not have a controller prefix, e.g. \"admin/cos.grafana-dashboards\"; name another controller with controller."
  }
}

variable "logging_config" {
  description = "Juju logging-config for a model this module creates, e.g. \"<root>=INFO;unit=DEBUG\"."
  type        = string
  default     = null

  validation {
    condition     = var.logging_config == null || var.model.uuid == null
    error_message = "logging_config is only applied to a model this module creates; set it on the existing model instead."
  }
}

variable "microcloud" {
  description = <<-EOT
    The MicroCloud application, with the charm module's inputs. channel
    defaults to "3/<risk>". units defaults to the number of machines when
    machines is set, and to 3 otherwise. See the charm module's README for
    each field.
  EOT
  type = object({
    app_name    = optional(string, "microcloud")
    base        = optional(string, "ubuntu@24.04")
    channel     = optional(string)
    config      = optional(map(string), {})
    constraints = optional(string)
    endpoint_bindings = optional(set(object({
      endpoint = optional(string)
      space    = string
    })), [])
    machines           = optional(set(string), [])
    revision           = optional(number)
    storage_directives = optional(map(string), {})
    units              = optional(number)
  })
  default  = {}
  nullable = false
}

variable "model" {
  description = <<-EOT
    The model to deploy into. With uuid set, that existing model is used and
    no other field may be set. The uuid must be known at plan time, so it
    cannot come from a juju_model created in the same apply. Otherwise a
    model is created from the rest, named "microcloud" unless name is set;
    see the juju_model resource for each field. Switching a model this
    module created to its uuid destroys it; see the README.
  EOT
  type = object({
    uuid = optional(string)
    name = optional(string)
    cloud = optional(object({
      name   = string
      region = optional(string)
    }))
    annotations       = optional(map(string))
    config            = optional(map(string))
    constraints       = optional(string)
    credential        = optional(string)
    target_controller = optional(string)
  })
  default  = {}
  nullable = false

  validation {
    condition = var.model.uuid == null || (
      var.model.name == null &&
      var.model.annotations == null &&
      var.model.cloud == null &&
      var.model.config == null &&
      var.model.constraints == null &&
      var.model.credential == null &&
      var.model.target_controller == null
    )
    error_message = "With model.uuid set the model already exists; do not also set name, annotations, cloud, config, constraints, credential or target_controller."
  }

  validation {
    condition     = var.model.name != ""
    error_message = "model.name must not be empty; leave it unset for \"microcloud\"."
  }
}

variable "opentelemetry_collector" {
  description = <<-EOT
    The opentelemetry-collector subordinate deployed when any COS integration
    is set. channel defaults to "0.130/<risk>". It takes MicroCloud's base, and a
    subordinate has no unit count of its own.
  EOT
  type = object({
    app_name = optional(string, "opentelemetry-collector")
    channel  = optional(string)
    config   = optional(map(string), {})
    revision = optional(number)
  })
  default  = {}
  nullable = false
}

variable "proxy" {
  description = <<-EOT
    Proxies for a model this module creates. They are set as the model's
    juju-, apt- and snap- proxy config, which is what the charm's snap
    installs go through. snapd has no no-proxy setting, so no-proxy only
    reaches Juju and apt.
  EOT
  type = object({
    http     = optional(string)
    https    = optional(string)
    no-proxy = optional(string)
  })
  default  = {}
  nullable = false

  validation {
    condition     = var.model.uuid == null || (var.proxy.http == null && var.proxy.https == null && var.proxy["no-proxy"] == null)
    error_message = "proxy is only applied to a model this module creates; set it on the existing model instead."
  }
}

variable "risk" {
  description = <<-EOT
    Risk every charm is deployed from, unless its own channel is set. The
    MicroCloud charm is only published to edge so far.
  EOT
  type        = string
  default     = "edge"
  nullable    = false

  validation {
    condition     = contains(["stable", "candidate", "beta", "edge"], var.risk)
    error_message = "risk must be stable, candidate, beta or edge."
  }
}

variable "storage_pools" {
  description = <<-EOT
    Juju storage pools to create in the model, keyed by name, for
    microcloud.storage_directives to refer to. On MAAS, e.g.
    { maas-ceph = { storage_provider = "maas", attributes = { tags = "ceph" } } }.
  EOT
  type = map(object({
    storage_provider = string
    attributes       = optional(map(string))
  }))
  default  = {}
  nullable = false
}
