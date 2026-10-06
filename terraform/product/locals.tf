# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

locals {
  channels = {
    microcloud              = coalesce(var.microcloud.channel, "${local.tracks.microcloud}/${var.risk}")
    opentelemetry_collector = coalesce(var.opentelemetry_collector.channel, "${local.tracks.opentelemetry_collector}/${var.risk}")
  }

  # Any COS integration brings in the collector; each then adds its own, so a
  # partial COS (say, metrics only) is still valid. Keyed on whether each
  # object is set, not on its url, so an offer url only known after apply
  # (from a COS module in the same root) still leaves every count known.
  cos_enabled = anytrue([for integration in values(var.cos) : integration != null])

  create_model = var.model.uuid == null

  # Unset units follow machines when they are given, so the two cannot
  # disagree; otherwise three, the smallest cluster MicroCeph can replicate on.
  microcloud_units = (
    var.microcloud.units != null ? var.microcloud.units :
    length(var.microcloud.machines) > 0 ? length(var.microcloud.machines) : 3
  )

  # proxy and logging_config are model config underneath. An explicit
  # model.config wins, so a caller can still override a single key.
  model_config = merge(
    {
      for key, value in {
        "apt-http-proxy"   = var.proxy.http
        "apt-https-proxy"  = var.proxy.https
        "apt-no-proxy"     = var.proxy["no-proxy"]
        "juju-http-proxy"  = var.proxy.http
        "juju-https-proxy" = var.proxy.https
        "juju-no-proxy"    = var.proxy["no-proxy"]
        "snap-http-proxy"  = var.proxy.http
        "snap-https-proxy" = var.proxy.https
      } : key => value if value != null
    },
    var.logging_config == null ? {} : { "logging-config" = var.logging_config },
    coalesce(var.model.config, {}),
  )

  model_name = var.model.name != null ? var.model.name : "microcloud"

  model_uuid = local.create_model ? juju_model.microcloud[0].uuid : data.juju_model.microcloud[0].uuid

  tracks = {
    microcloud = "3"
    # Charmhub's default track, and unlike "2", built for ubuntu@26.04.
    opentelemetry_collector = "0.130"
  }

  # Bump alongside the tf-X.Y.Z tag that releases this module.
  version = "0.1.0"
}
