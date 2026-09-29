# MicroCloud product Terraform module

Deploys a MicroCloud cluster ready to use: the model, any storage pools it
needs, the MicroCloud application, and optionally the observability wiring to a
Canonical Observability Stack (COS). This is a *product module* in the sense of
Canonical's CC008 Terraform standard; the single application comes from the
[charm module](../README.md) one directory up, which it calls by relative path
so the two always come from the same commit.

What it adds over the charm module:

- **The model.** It creates one, or uses an existing one given its UUID.
- **Storage pools**, so that `storage_directives` can name a MAAS-tagged pool
  in a model created in the same apply.
- **`risk`**, one knob for every charm's channel.
- **COS.** Given COS offers, it deploys an `opentelemetry-collector`
  subordinate on every MicroCloud machine and relates it to them.

## Usage

Pin `ref` to a release tag, never a branch.

### A development cluster in an existing model

```hcl
module "microcloud" {
  source = "git::https://github.com/canonical/charm-microcloud//terraform/product?ref=<tag>"

  # The UUID of a model that already exists, e.g. from `juju show-model dev`.
  model = { uuid = "4a2c9b1e-7f3d-4e8a-9c6b-2d1f0e5a8b7c" }

  microcloud = {
    config = {
      "snap-channel-microceph" = ""
      "snap-channel-microovn"  = ""
    }
  }
}
```

### A MAAS cluster, observed by COS

The module creates the model on the MAAS cloud, and the storage pools for the
partition tagged `partition,local` and the disks tagged `ceph` in MAAS. See the
charm module's README for the partition pool caveats.

```hcl
module "microcloud" {
  source = "git::https://github.com/canonical/charm-microcloud//terraform/product?ref=<tag>"

  risk = "edge"

  model = {
    name  = "microcloud"
    cloud = { name = "maas" }
  }

  proxy = {
    http       = "http://squid.internal:3128"
    https      = "http://squid.internal:3128"
    "no-proxy" = "10.0.0.0/8,127.0.0.1,localhost"
  }

  storage_pools = {
    maas-local = { storage_provider = "maas", attributes = { tags = "partition,local" } }
    maas-ceph  = { storage_provider = "maas", attributes = { tags = "ceph" } }
  }

  microcloud = {
    constraints        = "arch=amd64"
    storage_directives = {
      local = "maas-local,1,2G"
      ceph  = "maas-ceph,3,8G"
    }
    endpoint_bindings = [
      { space = "management" },
      { endpoint = "ceph-public", space = "ceph-public" },
      { endpoint = "ceph-internal", space = "ceph-internal" },
      { endpoint = "ovn-underlay", space = "ovn-underlay" },
      { endpoint = "ovn-uplink", space = "ovn-uplink" },
    ]
    config = {
      "storage-wipe"         = "true"
      "ovn-uplink-interface" = "enp9s0"
      "ovn-ipv4-gateway"     = "10.0.5.1/24"
      "ovn-ipv4-range"       = "10.0.5.100-10.0.5.150"
      "ovn-dns-servers"      = "10.0.5.1"
    }
  }

  cos = {
    dashboards = { url = "admin/cos.grafana-dashboards", controller = "cos" }
    logging    = { url = "admin/cos.loki-logging", controller = "cos" }
    metrics    = { url = "admin/cos.mimir-receive-remote-write", controller = "cos" }
  }
}
```

COS lives on another controller here, so the provider has to be able to reach
it too. Name it in `offering_controllers`, under the same key as `controller`:

```hcl
provider "juju" {
  offering_controllers = {
    cos = {
      controller_addresses = "10.0.1.10:17070"
      username             = "admin"
      password             = var.cos_password
      ca_certificate       = file("cos-ca.crt")
    }
  }
}
```

Each COS entry has CC008's external integration shape, `kind`, `url` and
`controller`. `kind` defaults to, and can only be, `"offer"`. The offer names
above are the ones the
[COS product module](https://github.com/canonical/observability-stack/tree/main/terraform/cos)
creates. With COS in the same Terraform root, use its `offers` output instead,
e.g. `url = module.cos.offers.grafana_dashboards.url`. That url is only known
after apply, which is fine: whether each integration is set is known at plan.

## How the COS wiring works

The COS stack lives in a Kubernetes model, so everything leaving the MicroCloud
model is a cross-model integration against an offer. Each integration is
optional, and setting any one of them deploys the collector.

| From                                          | To                             | When              |
|-----------------------------------------------|--------------------------------|-------------------|
| `microcloud:cos-agent`                        | `opentelemetry-collector:cos-agent` | any COS integration set |
| `microcloud:logging`                          | `opentelemetry-collector:receive-loki-logs` | `cos.logging` |
| `opentelemetry-collector:grafana-dashboards-provider` | `cos.dashboards.url` | `cos.dashboards` |
| `opentelemetry-collector:send-loki-logs`      | `cos.logging.url`              | `cos.logging`     |
| `opentelemetry-collector:send-remote-write`   | `cos.metrics.url`              | `cos.metrics`     |

LXD streams its own logs to a Loki endpoint rather than through `cos-agent`,
which is why `microcloud:logging` goes to the collector as well.

## Things that are easy to get wrong

**Offer URLs have no controller prefix.** The provider refuses
`cos:admin/cos.loki-logging`. Write `admin/cos.loki-logging`, and name the
controller with the integration's `controller`.

**A COS `controller` must also be in the provider's `offering_controllers`.**
The module only passes the name on. Plan succeeds without it, and the
cross-model integrations then fail at apply.

**`model.uuid` must be known at plan time.** Whether the module creates a
model depends on it, so it cannot come from a `juju_model` created in the same
apply: Terraform stops with "Invalid count argument". Pass the UUID of a model
that already exists, or let this module create the model.

**Do not switch a model this module created over to `model.uuid`.** The model
is only created while `model.uuid` is unset, so setting it to the created
model's UUID plans to destroy that model and the cluster in it. To hand the
model over, remove it from state first, then set `model.uuid`:

```shell
terraform state rm 'module.microcloud.juju_model.microcloud[0]'
```

**`proxy` and `logging_config` only reach a model this module creates.** They
are written as model config when the model is created, so they are rejected
alongside `model.uuid`; set them on an existing model yourself. `proxy` covers
Juju, apt and snapd, which is what the charm's snap installs go through. snapd
has no no-proxy setting. LXD's own image downloads are not covered.

**The collector follows MicroCloud's base.** A subordinate's base has to match
its principal's, so `opentelemetry_collector` has no `base` of its own. Pick a
collector channel that is built for that base.

**`units` follows `machines`.** Left unset, `microcloud.units` is the number of
`machines` when those are given, and 3 otherwise.

Everything else about the MicroCloud application is documented in the
[charm module's README](../README.md#things-that-are-easy-to-get-wrong).

## Development

Run the same commands as for the charm module, from this directory:

```shell
terraform init -backend=false
terraform validate
tflint --init --config ../../.tflint.hcl && tflint --config ../../.tflint.hcl
terraform test
terraform-docs --config ../../.terraform-docs.yml .
```

Bump `version` in `locals.tf` alongside the `tf-X.Y.Z` tag that releases this
module; the `metadata` output reports it.

<!-- BEGIN_TF_DOCS -->
## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | >= 1.9 |
| <a name="requirement_juju"></a> [juju](#requirement\_juju) | >= 1.2.0, < 3.0.0 |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_microcloud"></a> [microcloud](#module\_microcloud) | ../ | n/a |

## Resources

| Name | Type |
|------|------|
| [juju_application.opentelemetry_collector](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/application) | resource |
| [juju_integration.cos_dashboards](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | resource |
| [juju_integration.cos_logging](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | resource |
| [juju_integration.cos_metrics](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | resource |
| [juju_integration.microcloud_cos_agent](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | resource |
| [juju_integration.microcloud_logging](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/integration) | resource |
| [juju_model.microcloud](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/model) | resource |
| [juju_storage_pool.microcloud](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/storage_pool) | resource |
| [terraform_data.deployed_at](https://registry.terraform.io/providers/hashicorp/terraform/latest/docs/resources/data) | resource |
| [terraform_data.updated_at](https://registry.terraform.io/providers/hashicorp/terraform/latest/docs/resources/data) | resource |
| [juju_model.microcloud](https://registry.terraform.io/providers/juju/juju/latest/docs/data-sources/model) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_cos"></a> [cos](#input\_cos) | Integrations with a Canonical Observability Stack, each optional, in<br/>CC008's external integration shape. Setting any of them deploys an<br/>opentelemetry-collector subordinate on MicroCloud and relates it to that<br/>offer:<br/>  dashboards: grafana\_dashboard, e.g. COS's "grafana-dashboards".<br/>  logging: loki\_push\_api, e.g. COS's "loki-logging". Also routes LXD's<br/>    own log stream through the collector.<br/>  metrics: prometheus\_remote\_write, e.g. COS's<br/>    "mimir-receive-remote-write".<br/>Each takes the offer url, and controller when the offer lives on another<br/>controller; that name must be configured in the provider's<br/>offering\_controllers. COS runs on Kubernetes, so only kind "offer" is<br/>supported. | <pre>object({<br/>    dashboards = optional(object({<br/>      kind       = optional(string, "offer")<br/>      url        = string<br/>      controller = optional(string)<br/>    }))<br/>    logging = optional(object({<br/>      kind       = optional(string, "offer")<br/>      url        = string<br/>      controller = optional(string)<br/>    }))<br/>    metrics = optional(object({<br/>      kind       = optional(string, "offer")<br/>      url        = string<br/>      controller = optional(string)<br/>    }))<br/>  })</pre> | `{}` | no |
| <a name="input_logging_config"></a> [logging\_config](#input\_logging\_config) | Juju logging-config for a model this module creates, e.g. "<root>=INFO;unit=DEBUG". | `string` | `null` | no |
| <a name="input_microcloud"></a> [microcloud](#input\_microcloud) | The MicroCloud application, with the charm module's inputs. channel<br/>defaults to "3/<risk>". units defaults to the number of machines when<br/>machines is set, and to 3 otherwise. See the charm module's README for<br/>each field. | <pre>object({<br/>    app_name    = optional(string, "microcloud")<br/>    base        = optional(string, "ubuntu@24.04")<br/>    channel     = optional(string)<br/>    config      = optional(map(string), {})<br/>    constraints = optional(string)<br/>    endpoint_bindings = optional(set(object({<br/>      endpoint = optional(string)<br/>      space    = string<br/>    })), [])<br/>    machines           = optional(set(string), [])<br/>    revision           = optional(number)<br/>    storage_directives = optional(map(string), {})<br/>    units              = optional(number)<br/>  })</pre> | `{}` | no |
| <a name="input_model"></a> [model](#input\_model) | The model to deploy into. With uuid set, that existing model is used and<br/>no other field may be set. The uuid must be known at plan time, so it<br/>cannot come from a juju\_model created in the same apply. Otherwise a<br/>model is created from the rest, named "microcloud" unless name is set;<br/>see the juju\_model resource for each field. Switching a model this<br/>module created to its uuid destroys it; see the README. | <pre>object({<br/>    uuid = optional(string)<br/>    name = optional(string)<br/>    cloud = optional(object({<br/>      name   = string<br/>      region = optional(string)<br/>    }))<br/>    annotations       = optional(map(string))<br/>    config            = optional(map(string))<br/>    constraints       = optional(string)<br/>    credential        = optional(string)<br/>    target_controller = optional(string)<br/>  })</pre> | `{}` | no |
| <a name="input_opentelemetry_collector"></a> [opentelemetry\_collector](#input\_opentelemetry\_collector) | The opentelemetry-collector subordinate deployed when any COS integration<br/>is set. channel defaults to "0.130/<risk>". It takes MicroCloud's base, and a<br/>subordinate has no unit count of its own. | <pre>object({<br/>    app_name = optional(string, "opentelemetry-collector")<br/>    channel  = optional(string)<br/>    config   = optional(map(string), {})<br/>    revision = optional(number)<br/>  })</pre> | `{}` | no |
| <a name="input_proxy"></a> [proxy](#input\_proxy) | Proxies for a model this module creates. They are set as the model's<br/>juju-, apt- and snap- proxy config, which is what the charm's snap<br/>installs go through. snapd has no no-proxy setting, so no-proxy only<br/>reaches Juju and apt. | <pre>object({<br/>    http     = optional(string)<br/>    https    = optional(string)<br/>    no-proxy = optional(string)<br/>  })</pre> | `{}` | no |
| <a name="input_risk"></a> [risk](#input\_risk) | Risk every charm is deployed from, unless its own channel is set. The<br/>MicroCloud charm is only published to edge so far. | `string` | `"edge"` | no |
| <a name="input_storage_pools"></a> [storage\_pools](#input\_storage\_pools) | Juju storage pools to create in the model, keyed by name, for<br/>microcloud.storage\_directives to refer to. On MAAS, e.g.<br/>{ maas-ceph = { storage\_provider = "maas", attributes = { tags = "ceph" } } }. | <pre>map(object({<br/>    storage_provider = string<br/>    attributes       = optional(map(string))<br/>  }))</pre> | `{}` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_metadata"></a> [metadata](#output\_metadata) | Version of this module, when the deployment was first applied, and when an apply last changed what it deploys. |
| <a name="output_models"></a> [models](#output\_models) | The model this module deploys into, and the juju\_application objects deployed in it, keyed by component. |
<!-- END_TF_DOCS -->
