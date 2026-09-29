# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

# mock_provider means no Juju controller is needed. Runs that assert on
# values only known after apply use "command = apply", which against a mock
# creates nothing.
mock_provider "juju" {
  # The provider checks model_uuid is a UUID, which a random mock value is not.
  mock_resource "juju_model" {
    defaults = {
      uuid = "11111111-1111-1111-1111-111111111111"
    }
  }
}

run "defaults_create_a_model_and_a_three_unit_cluster" {
  command = apply

  assert {
    condition     = juju_model.microcloud[0].name == "microcloud"
    error_message = "a model named microcloud should be created by default"
  }

  assert {
    condition     = juju_model.microcloud[0].config == null
    error_message = "no model config should be set by default"
  }

  assert {
    condition     = module.microcloud.application.units == 3
    error_message = "units should default to 3"
  }

  assert {
    condition     = one(module.microcloud.application.charm).channel == "3/edge"
    error_message = "the charm channel should default to 3/<risk>"
  }

  assert {
    condition     = module.microcloud.application.model_uuid == juju_model.microcloud[0].uuid
    error_message = "MicroCloud should be deployed into the created model"
  }

  assert {
    condition     = length(juju_application.opentelemetry_collector) == 0
    error_message = "no collector should be deployed without a COS offer"
  }

  assert {
    condition     = keys(output.models.microcloud.components) == ["microcloud"]
    error_message = "only MicroCloud should be listed without COS"
  }

  assert {
    condition     = output.models.microcloud.model_uuid == juju_model.microcloud[0].uuid
    error_message = "models should report the created model"
  }

  assert {
    condition     = output.metadata.version == "0.1.0"
    error_message = "metadata should report the module version"
  }
}

run "a_converged_deployment_keeps_its_metadata" {
  command = plan

  assert {
    condition = (
      output.metadata.deployed_at == run.defaults_create_a_model_and_a_three_unit_cluster.metadata.deployed_at &&
      output.metadata.updated_at == run.defaults_create_a_model_and_a_three_unit_cluster.metadata.updated_at
    )
    error_message = "with inputs unchanged, metadata should be known at plan and unchanged"
  }
}

run "an_existing_model_is_used_as_is" {
  command = plan

  variables {
    model = { uuid = "00000000-0000-0000-0000-000000000000" }
  }

  assert {
    condition     = length(juju_model.microcloud) == 0
    error_message = "no model should be created when a uuid is given"
  }

  assert {
    condition     = module.microcloud.application.model_uuid == "00000000-0000-0000-0000-000000000000"
    error_message = "MicroCloud should be deployed into the given model"
  }
}

run "risk_sets_every_channel" {
  command = plan

  variables {
    risk = "stable"
    cos  = { metrics = { url = "admin/cos.mimir-receive-remote-write" } }
  }

  assert {
    condition     = one(module.microcloud.application.charm).channel == "3/stable"
    error_message = "the MicroCloud channel should follow risk"
  }

  assert {
    condition     = one(juju_application.opentelemetry_collector[0].charm).channel == "0.130/stable"
    error_message = "the collector channel should follow risk"
  }
}

run "an_explicit_channel_overrides_risk" {
  command = plan

  variables {
    risk       = "stable"
    microcloud = { channel = "3/edge/eken" }
  }

  assert {
    condition     = one(module.microcloud.application.charm).channel == "3/edge/eken"
    error_message = "an explicit channel should win over risk"
  }
}

run "full_cos_wiring" {
  command = plan

  variables {
    microcloud = { base = "ubuntu@26.04" }
    opentelemetry_collector = {
      channel  = "dev/edge"
      revision = 580
    }
    cos = {
      dashboards = { kind = "offer", url = "admin/cos.grafana-dashboards", controller = "cos" }
      logging    = { url = "admin/cos.loki-logging", controller = "cos" }
      metrics    = { url = "admin/cos.mimir-receive-remote-write", controller = "cos" }
    }
  }

  assert {
    condition     = one(juju_application.opentelemetry_collector[0].charm).base == "ubuntu@26.04"
    error_message = "the subordinate must take its principal's base"
  }

  assert {
    condition     = one(juju_application.opentelemetry_collector[0].charm).revision == 580
    error_message = "the collector revision should be pinnable"
  }

  assert {
    condition = [for app in juju_integration.microcloud_cos_agent[0].application : app.endpoint] == [
      "cos-agent", "cos-agent"
    ]
    error_message = "MicroCloud should relate to the collector over cos-agent"
  }

  assert {
    condition     = contains([for app in juju_integration.microcloud_logging[0].application : app.endpoint], "receive-loki-logs")
    error_message = "LXD's log stream should be routed through the collector"
  }

  assert {
    condition     = contains([for app in juju_integration.cos_dashboards[0].application : app.offer_url], "admin/cos.grafana-dashboards")
    error_message = "the collector should relate to the dashboards offer"
  }

  assert {
    condition     = contains([for app in juju_integration.cos_logging[0].application : app.offer_url], "admin/cos.loki-logging")
    error_message = "the collector should relate to the logging offer"
  }

  assert {
    condition     = contains([for app in juju_integration.cos_metrics[0].application : app.offering_controller], "cos")
    error_message = "each integration's controller should reach its cross-model integration"
  }

  assert {
    condition     = keys(output.models.microcloud.components) == ["microcloud", "opentelemetry_collector"]
    error_message = "the collector should be listed alongside MicroCloud"
  }
}

run "metrics_only_cos" {
  command = plan

  variables {
    cos = { metrics = { url = "cos.mimir-receive-remote-write" } }
  }

  assert {
    condition     = length(juju_application.opentelemetry_collector) == 1 && length(juju_integration.microcloud_cos_agent) == 1
    error_message = "a single offer should still bring in the collector"
  }

  assert {
    condition = (
      length(juju_integration.cos_metrics) == 1 &&
      length(juju_integration.cos_dashboards) == 0 &&
      length(juju_integration.cos_logging) == 0 &&
      length(juju_integration.microcloud_logging) == 0
    )
    error_message = "only the offers given should be integrated"
  }
}

run "proxy_and_logging_config_become_model_config" {
  command = plan

  variables {
    proxy = {
      http       = "http://proxy:3128"
      https      = "http://proxy:3128"
      "no-proxy" = "10.0.0.0/8"
    }
    logging_config = "<root>=INFO"
    model = {
      config = { "juju-no-proxy" = "10.0.0.0/8,127.0.0.1" }
    }
  }

  assert {
    condition = juju_model.microcloud[0].config == tomap({
      "apt-http-proxy"   = "http://proxy:3128"
      "apt-https-proxy"  = "http://proxy:3128"
      "apt-no-proxy"     = "10.0.0.0/8"
      "juju-http-proxy"  = "http://proxy:3128"
      "juju-https-proxy" = "http://proxy:3128"
      "juju-no-proxy"    = "10.0.0.0/8,127.0.0.1"
      "logging-config"   = "<root>=INFO"
      "snap-http-proxy"  = "http://proxy:3128"
      "snap-https-proxy" = "http://proxy:3128"
    })
    error_message = "proxy and logging_config should become model config, with model.config winning"
  }
}

run "storage_pools_are_created_in_the_model" {
  command = apply

  variables {
    storage_pools = {
      maas-ceph = { storage_provider = "maas", attributes = { tags = "ceph" } }
    }
    microcloud = { storage_directives = { ceph = "maas-ceph,3,8G" } }
  }

  assert {
    condition     = juju_storage_pool.microcloud["maas-ceph"].model_uuid == juju_model.microcloud[0].uuid
    error_message = "the pool should be created in the model"
  }

  assert {
    condition     = module.microcloud.application.storage_directives["ceph"] == "maas-ceph,3,8G"
    error_message = "storage directives should reach MicroCloud"
  }
}

run "units_follow_machines" {
  command = plan

  variables {
    model      = { uuid = "00000000-0000-0000-0000-000000000000" }
    microcloud = { machines = ["0", "1", "2", "3"] }
  }

  assert {
    condition     = module.microcloud.application.machines == toset(["0", "1", "2", "3"])
    error_message = "machines should reach MicroCloud; the default of 3 units must not contradict them"
  }
}

run "rejects_model_settings_alongside_a_uuid" {
  command = plan

  variables {
    model = {
      uuid   = "00000000-0000-0000-0000-000000000000"
      config = { "logging-config" = "<root>=DEBUG" }
    }
  }

  expect_failures = [var.model]
}

run "rejects_a_name_alongside_a_uuid" {
  command = plan

  variables {
    model = {
      uuid = "00000000-0000-0000-0000-000000000000"
      name = "staging"
    }
  }

  expect_failures = [var.model]
}

run "rejects_an_empty_model_name" {
  command = plan

  variables {
    model = { name = "" }
  }

  expect_failures = [var.model]
}

run "rejects_a_proxy_for_an_existing_model" {
  command = plan

  variables {
    model = { uuid = "00000000-0000-0000-0000-000000000000" }
    proxy = { http = "http://proxy:3128" }
  }

  expect_failures = [var.proxy]
}

run "rejects_logging_config_for_an_existing_model" {
  command = plan

  variables {
    model          = { uuid = "00000000-0000-0000-0000-000000000000" }
    logging_config = "<root>=DEBUG"
  }

  expect_failures = [var.logging_config]
}

run "rejects_an_unknown_risk" {
  command = plan

  variables {
    risk = "latest"
  }

  expect_failures = [var.risk]
}

run "rejects_an_offer_that_is_not_a_url" {
  command = plan

  variables {
    cos = { metrics = { url = "mimir-receive-remote-write" } }
  }

  expect_failures = [var.cos]
}

run "rejects_an_offer_with_a_controller_prefix" {
  command = plan

  variables {
    cos = { metrics = { url = "cos-ctrl:admin/cos.mimir-receive-remote-write" } }
  }

  expect_failures = [var.cos]
}

run "rejects_an_in_model_cos_endpoint" {
  command = plan

  variables {
    cos = { metrics = { kind = "endpoint", url = "admin/cos.mimir-receive-remote-write" } }
  }

  expect_failures = [var.cos]
}

run "plans_with_cos_offers_known_only_after_apply" {
  command = plan

  module {
    source = "./tests/cos_in_same_root"
  }

  assert {
    condition     = output.components == ["microcloud", "opentelemetry_collector"]
    error_message = "an offer url unknown at plan should still bring in the collector"
  }
}
