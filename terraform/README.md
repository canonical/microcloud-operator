# MicroCloud charm Terraform module

Deploys the MicroCloud charm as a single Juju application. This is a *charm
module*: it takes an existing model and creates one `juju_application`. It does
not create the model, and it does not integrate MicroCloud with anything —
compose it from a product module for that.

`charmcraft.yaml` in the repository root is the source of truth for the charm's
config options, storage and endpoints. This module deliberately does not restate
them.

Terraform and provider versions are in the generated table at the bottom of
this file. Running the tests needs Terraform >= 1.7 for `mock_provider`, and
the charm `assumes` Juju >= 3.6 on Charmhub track `3`.

## Usage

### A development cluster on LXD

MicroCeph and MicroOVN are disabled by setting their snap channels to the empty
string, which is what the charm's own integration tests do.

```hcl
module "microcloud" {
  source = "github.com/canonical/charm-microcloud//terraform"

  model_uuid = juju_model.dev.uuid
  channel    = "3/edge"
  units      = 3

  config = {
    "snap-channel-microceph" = ""
    "snap-channel-microovn"  = ""
  }
}
```

### A MAAS cluster

Each MicroCloud network gets its own Juju space, the Ceph OSDs and local
storage partition come from storage pools backed by MAAS tags, and Juju asks
MAAS for one machine per unit. The pools have to exist in the model first:

```sh
juju create-storage-pool maas-local maas tags=partition,local
juju create-storage-pool maas-ceph maas tags=ceph
```

```hcl
module "microcloud" {
  source = "github.com/canonical/charm-microcloud//terraform"

  model_uuid  = juju_model.prod.uuid
  channel     = "3/edge"
  base        = "ubuntu@24.04"
  units       = 3
  constraints = "arch=amd64"

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
    "ovn-dns-servers"      = join(",", ["10.0.5.1"])
  }
}
```

### Wiring up observability

Integrations belong to the caller, not to this module. The `provides` and
`requires` outputs describe each endpoint in the CC008 shape, `{ kind =
"endpoint", name = <application>, endpoint = <endpoint> }`:

```hcl
resource "juju_integration" "cos_agent" {
  model_uuid = juju_model.prod.uuid

  application {
    name     = module.microcloud.provides.cos_agent.name
    endpoint = module.microcloud.provides.cos_agent.endpoint
  }

  application {
    name     = juju_application.otel_collector.name
    endpoint = "cos-agent"
  }
}
```

## Things that are easy to get wrong

**Every config value is a string.** Booleans are `"true"` and `"false"`,
integers are `"300"`.

**An empty snap channel disables that component.** `"snap-channel-microceph" =
""` deploys without Ceph and omits the Ceph section of the preseed entirely; the
same holds for `snap-channel-microovn`. An empty string is meaningful, not
absent.

**Two options take a mapping of hostname to value**, for hardware where the name
differs per machine. Use `yamlencode`, and key it on the machine's hostname:

```hcl
config = {
  "ovn-uplink-interface" = yamlencode({ node1 = "enp9s0", node2 = "eno1" })
  "local-device"         = yamlencode({ node1 = "/dev/nvme0n1p3", node2 = "/dev/sda3" })
}
```

Every unit's hostname must appear in the mapping. A unit that is missing blocks
rather than deploying without an uplink or local disk.

**Local storage on MAAS comes from a partition pool.** Create a storage pool
tagged `partition,<tag>` (e.g. `tags=partition,local`) and pass it in
`storage_directives` as `local = "maas-local,1,2G"`, with no `local-device`
config. Keep three caveats in mind:
1. MAAS selects the smallest tagged partition that fits the requested size,
   so tag exactly one partition per node.
2. A partition that already has a filesystem in the MAAS layout never matches,
   and the storage sits `pending` forever.
3. MAAS storage only attaches when Juju allocates the machine; placing units
   with `machines` or `--to` fails (see "MAAS storage needs Juju to allocate the machine" below).

**Give `local-device` a stable path.** On clouds without partition pools where
`local-device` is used, `/dev/sdX` names can change between boots. On MAAS
(when not using a partition pool), the `/dev/disk/by-dname/` links that MAAS
creates for the partitions it lays out (e.g. `/dev/disk/by-dname/sda-part3`)
keep their names, and are the same on every node deployed with the same layout,
so a single path covers them all. `local-device` is only understood by charm
revisions that have it; an older revision fails the apply with an unknown
config key.

**Binding `ovn-uplink` only constrains placement.** The uplink NIC usually has
no address, and Juju spaces only track addressed interfaces, so the interface
name has to come from `ovn-uplink-interface`. Binding the endpoint is still
useful on MAAS: it keeps units on machines that have a NIC on the right fabric.

**The Ceph space bindings are only read when Ceph disks are attached.**
MicroCloud rejects a Ceph public or internal network without Ceph storage, so
the charm leaves them out otherwise, even when the bindings resolve.

**`machines` sets the unit count.** With `machines`, the unit count is the
number of machines. `units` must then be left at its default of 1 or match
it; any other value fails validation rather than being dropped.

**MAAS storage needs Juju to allocate the machine.** MAAS attaches tagged disks
only when it hands a machine over, so it cannot add them to one already in the
model. Placing units with `machines` alongside a MAAS `storage_directives`
fails with `"maas" storage provider does not support dynamic storage`. Use
`units` and `constraints` instead, as in the MAAS example above; `machines` is
fine on MAAS only without MAAS storage.

## What is not validated

Config keys are not checked against the charm's options, and values other than
the OVN gateways are passed through untouched. That keeps this module working
across charm upgrades that add options. Mistyped keys surface as a Juju error at
apply time.

<!-- BEGIN_TF_DOCS -->
## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | >= 1.9 |
| <a name="requirement_juju"></a> [juju](#requirement\_juju) | ~> 1.0 |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [juju_application.microcloud](https://registry.terraform.io/providers/juju/juju/latest/docs/resources/application) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_app_name"></a> [app\_name](#input\_app\_name) | Name of the application in the Juju model. | `string` | `"microcloud"` | no |
| <a name="input_base"></a> [base](#input\_base) | Operating system base to deploy on, e.g. ubuntu@24.04. | `string` | `"ubuntu@24.04"` | no |
| <a name="input_channel"></a> [channel](#input\_channel) | Charm channel to deploy, as <track>/<risk>[/<branch>]. | `string` | `"3/edge"` | no |
| <a name="input_config"></a> [config](#input\_config) | Charm config options, passed through unchanged. Every value is a string:<br/>"true" for booleans, "300" for integers. Options that take a mapping of<br/>hostname to value ("local-device", "ovn-uplink-interface") take a<br/>yamlencode(...) of that map. See the charm's charmcraft.yaml for the<br/>available options; this module deliberately does not restate them. | `map(string)` | `{}` | no |
| <a name="input_constraints"></a> [constraints](#input\_constraints) | Juju constraints for the application, e.g. "arch=amd64 tags=microcloud". | `string` | `null` | no |
| <a name="input_endpoint_bindings"></a> [endpoint\_bindings](#input\_endpoint\_bindings) | Bindings of endpoints to Juju spaces. An entry with no endpoint sets the<br/>application's default space. On MAAS the extra-bindings are what put each<br/>MicroCloud network on its own space. | <pre>set(object({<br/>    endpoint = optional(string)<br/>    space    = string<br/>  }))</pre> | `null` | no |
| <a name="input_machines"></a> [machines](#input\_machines) | Machines to place units on, e.g. ["0", "1", "2"]. The unit count becomes<br/>the size of this set; "units" must be left at 1 or match it. | `set(string)` | `null` | no |
| <a name="input_model_uuid"></a> [model\_uuid](#input\_model\_uuid) | UUID of the Juju model to deploy into. | `string` | n/a | yes |
| <a name="input_revision"></a> [revision](#input\_revision) | Charm revision to deploy. Defaults to the channel's latest. | `number` | `null` | no |
| <a name="input_storage_directives"></a> [storage\_directives](#input\_storage\_directives) | Juju storage directives keyed by storage name, e.g.<br/>{ local = "maas,1,100G", ceph = "maas,3,100G" }. Both are optional: the<br/>charm accepts zero local disks and zero Ceph disks. | `map(string)` | `null` | no |
| <a name="input_units"></a> [units](#input\_units) | Number of units to deploy. With "machines" set, the unit count is the<br/>number of machines, so this must be left at 1 or match it. | `number` | `1` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_application"></a> [application](#output\_application) | The deployed juju\_application object. |
| <a name="output_provides"></a> [provides](#output\_provides) | Endpoints this charm provides, keyed by alias, in the CC008 endpoint shape. |
| <a name="output_requires"></a> [requires](#output\_requires) | Endpoints this charm requires, keyed by alias, in the CC008 endpoint shape. |
<!-- END_TF_DOCS -->
