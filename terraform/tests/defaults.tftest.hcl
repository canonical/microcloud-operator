# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

# Plan-only tests: mock_provider means no Juju controller is needed.
mock_provider "juju" {}

variables {
  model_uuid = "00000000-0000-0000-0000-000000000000"
}

run "defaults" {
  command = plan

  assert {
    condition     = juju_application.microcloud.name == "microcloud"
    error_message = "app_name should default to microcloud"
  }

  assert {
    condition     = juju_application.microcloud.units == 1
    error_message = "units should default to 1"
  }

  assert {
    condition     = one(juju_application.microcloud.charm).name == "microcloud"
    error_message = "the charm name is fixed by this module"
  }

  assert {
    condition     = one(juju_application.microcloud.charm).base == "ubuntu@24.04"
    error_message = "base should default to ubuntu@24.04"
  }
}

run "config_passes_through_unchanged" {
  command = plan

  variables {
    config = {
      "snap-channel-microceph" = ""
      "session-timeout"        = "600"
      "ovn-uplink-interface"   = "{\"node1\":\"enp9s0\"}"
    }
  }

  assert {
    # An empty snap channel is meaningful: it disables that component, so the
    # module must not drop it.
    condition     = juju_application.microcloud.config["snap-channel-microceph"] == ""
    error_message = "an empty config value must survive passthrough"
  }

  assert {
    condition     = juju_application.microcloud.config["session-timeout"] == "600"
    error_message = "config values must reach the application unchanged"
  }
}

run "machines_replace_units" {
  command = plan

  variables {
    machines = ["0", "1", "2"]
  }

  # "units" is Optional+Computed in the provider, so at plan time it reads as
  # unknown rather than null and cannot be asserted on here. main.tf sends
  # null, which is what keeps the provider from seeing both.
  assert {
    condition     = juju_application.microcloud.machines == toset(["0", "1", "2"])
    error_message = "machines should be passed through for placement"
  }
}

run "rejects_units_that_contradict_machines" {
  command = plan

  variables {
    machines = ["0", "1", "2"]
    units    = 2
  }

  expect_failures = [var.units]
}

run "accepts_units_that_match_machines" {
  command = plan

  variables {
    machines = ["0", "1", "2"]
    units    = 3
  }

  assert {
    condition     = juju_application.microcloud.machines == toset(["0", "1", "2"])
    error_message = "a units value matching machines should be accepted"
  }
}

run "storage_and_bindings_reach_the_application" {
  command = plan

  variables {
    storage_directives = {
      local = "maas,1,100G"
      ceph  = "maas,3,100G"
    }
    endpoint_bindings = [
      { space = "management" },
      { endpoint = "ceph-public", space = "ceph-public" },
      { endpoint = "ovn-underlay", space = "ovn-underlay" },
    ]
  }

  assert {
    condition     = juju_application.microcloud.storage_directives["ceph"] == "maas,3,100G"
    error_message = "storage directives should be passed through"
  }

  assert {
    condition     = length(juju_application.microcloud.endpoint_bindings) == 3
    error_message = "every endpoint binding should be passed through"
  }
}

run "rejects_a_channel_without_a_risk" {
  command = plan

  variables {
    channel = "3"
  }

  expect_failures = [var.channel]
}

run "rejects_an_unknown_endpoint" {
  command = plan

  variables {
    endpoint_bindings = [{ endpoint = "ovn-uplnik", space = "ovn-uplink" }]
  }

  expect_failures = [var.endpoint_bindings]
}

run "rejects_an_unknown_storage_name" {
  command = plan

  variables {
    storage_directives = { osd = "maas,3,100G" }
  }

  expect_failures = [var.storage_directives]
}

run "rejects_a_gateway_that_is_not_cidr" {
  command = plan

  variables {
    config = { "ovn-ipv4-gateway" = "10.200.5.1" }
  }

  expect_failures = [var.config]
}

run "rejects_an_unsupported_base" {
  command = plan

  variables {
    base = "ubuntu@22.04"
  }

  expect_failures = [var.base]
}

run "empty_machines_still_gets_units" {
  command = plan

  variables {
    machines = []
    units    = 3
  }

  # An empty set is not "placement given": it must fall back to units rather
  # than deploy nothing.
  assert {
    condition     = juju_application.microcloud.units == 3
    error_message = "an empty machines set should fall back to units"
  }
}

run "accepts_a_single_character_app_name" {
  command = plan

  variables {
    app_name = "m"
  }

  assert {
    condition     = juju_application.microcloud.name == "m"
    error_message = "a one-character app name is valid in Juju"
  }
}

run "endpoints_use_the_cc008_shape" {
  command = plan

  variables {
    app_name = "cloud"
  }

  assert {
    condition = output.provides.cos_agent == {
      kind     = "endpoint"
      name     = "cloud"
      endpoint = "cos-agent"
    }
    error_message = "provides.cos_agent should be a CC008 endpoint object naming the application"
  }

  assert {
    condition = output.requires.logging == {
      kind     = "endpoint"
      name     = "cloud"
      endpoint = "logging"
    }
    error_message = "requires.logging should be a CC008 endpoint object naming the application"
  }
}
