# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

output "application" {
  description = "The deployed juju_application object."
  value       = juju_application.microcloud
}

output "provides" {
  description = "Endpoints this charm provides, keyed by alias."
  value = {
    cos_agent = "cos-agent"
  }
}

output "requires" {
  description = "Endpoints this charm requires, keyed by alias."
  value = {
    logging = "logging"
  }
}
