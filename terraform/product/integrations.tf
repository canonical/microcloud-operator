# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

# In-model: MicroCloud to its collector subordinate.

resource "juju_integration" "microcloud_cos_agent" {
  count = local.cos_enabled ? 1 : 0

  model_uuid = local.model_uuid

  application {
    name     = module.microcloud.provides.cos_agent.name
    endpoint = module.microcloud.provides.cos_agent.endpoint
  }

  application {
    name     = juju_application.opentelemetry_collector[0].name
    endpoint = "cos-agent"
  }
}

# LXD streams its logs to a Loki endpoint itself rather than through
# cos-agent, so point it at the collector, which forwards them on.
resource "juju_integration" "microcloud_logging" {
  count = var.cos.logging != null ? 1 : 0

  model_uuid = local.model_uuid

  application {
    name     = module.microcloud.requires.logging.name
    endpoint = module.microcloud.requires.logging.endpoint
  }

  application {
    name     = juju_application.opentelemetry_collector[0].name
    endpoint = "receive-loki-logs"
  }
}

# Cross-model: the collector to COS.

resource "juju_integration" "cos_dashboards" {
  count = var.cos.dashboards != null ? 1 : 0

  model_uuid = local.model_uuid

  application {
    name     = juju_application.opentelemetry_collector[0].name
    endpoint = "grafana-dashboards-provider"
  }

  application {
    offer_url           = var.cos.dashboards.url
    offering_controller = var.cos.dashboards.controller
  }
}

resource "juju_integration" "cos_logging" {
  count = var.cos.logging != null ? 1 : 0

  model_uuid = local.model_uuid

  application {
    name     = juju_application.opentelemetry_collector[0].name
    endpoint = "send-loki-logs"
  }

  application {
    offer_url           = var.cos.logging.url
    offering_controller = var.cos.logging.controller
  }
}

resource "juju_integration" "cos_metrics" {
  count = var.cos.metrics != null ? 1 : 0

  model_uuid = local.model_uuid

  application {
    name     = juju_application.opentelemetry_collector[0].name
    endpoint = "send-remote-write"
  }

  application {
    offer_url           = var.cos.metrics.url
    offering_controller = var.cos.metrics.controller
  }
}
