# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

output "metadata" {
  description = "Version of this module, when the deployment was first applied, and when an apply last changed what it deploys."
  value = {
    version     = local.version
    deployed_at = terraform_data.deployed_at.output
    updated_at  = terraform_data.updated_at.output
  }
}

output "models" {
  description = "The model this module deploys into, and the juju_application objects deployed in it, keyed by component."
  value = {
    microcloud = {
      model_uuid = local.model_uuid
      components = merge(
        { microcloud = module.microcloud.application },
        { for app in juju_application.opentelemetry_collector : "opentelemetry_collector" => app },
      )
    }
  }
}
