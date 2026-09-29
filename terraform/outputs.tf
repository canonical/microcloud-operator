# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

output "application" {
  description = "The deployed juju_application object."
  value       = juju_application.microcloud
}

output "provides" {
  description = "Endpoints this charm provides, keyed by alias, in the CC008 endpoint shape."
  value = {
    cos_agent = {
      kind     = "endpoint"
      name     = juju_application.microcloud.name
      endpoint = "cos-agent"
    }
  }
}

output "requires" {
  description = "Endpoints this charm requires, keyed by alias, in the CC008 endpoint shape."
  value = {
    logging = {
      kind     = "endpoint"
      name     = juju_application.microcloud.name
      endpoint = "logging"
    }
  }
}
