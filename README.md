# MicroCloud charm for MAAS

This charm deploys MicroCloud using Juju.

## Build the charm

* Install `charmcraft` to build the charm: `snap install charmcraft --classic`
* Build the charm: `charmcraft pack`
  * For example use `--platform ubuntu@24.04:amd64` to only build the charm for a specific base and architecture

## Deploy the charm

Run `juju deploy <charm>`.

## Control plane

Recent LXD releases can keep the cluster database on chosen members: those with the `control-plane` role. Once at least 3 members hold the role, LXD only gives database roles to role holders. Members without the role, including new ones, stay spares. See [Control plane mode](https://documentation.ubuntu.com/lxd/latest/explanation/clusters/#clustering-control-plane) in the LXD documentation.

Pick the units that should hold the role and run the action on each:

```sh
juju run microcloud/0 microcloud/1 microcloud/2 add-control-plane-role
juju run microcloud/3 remove-control-plane-role
```

Each action changes only its own unit's LXD member, and running it twice changes nothing. Both report the role holders and whether the mode is active. The `status` action reports the mode too.

The role makes a member eligible for the database; it does not make it a voter. By default LXD keeps 3 voters and 2 standbys (`cluster.max_voters` and `cluster.max_standby`). Extra role holders stay spares until LXD needs to promote one.

Recommendations:

* Use at least 3 failure domains (Juju zones) with 2 role holders each. Losing a whole failure domain then leaves at least 4 role holders online, and LXD restores 3 voters.
* Don't give the role to more units than that without a reason. Every role holder is an LXD event hub, and every other member connects to each one.

The leader adds a warning to its status when the role holders drop to 1 or 2, or when a failure domain has fewer than 2 online role holders. The warning keeps the unit active.

Replacing a role holder:

1. `juju remove-unit` leaves the LXD member, and its role, in place. LXD keeps counting it. Remove it with `microcloud remove <name>` on a remaining unit.
2. Add the replacement unit. It joins without the role.
3. Run `add-control-plane-role` on the new unit.

Replace a failure domain's last member before removing it. The leader only checks failure domains that still have members.


## PS10 host and DPU roles

Deploy separate applications of this charm for hypervisors and DPUs.
The default `node-role=host` retains the MicroCloud workflow. The DPU role
uses standard system OVS to create `br-dpu` with the MAAS-created `bond0`
and `pf0hpf`. It installs `openvswitch-switch` if needed. It does not install
or start MicroOVN, MicroCloud, LXD or MicroCeph. MAAS owns the bond; the
DPU unit never recreates it. A networkd file keeps the bridge interface up
after reboot without changing MAAS configuration. Role changes require
application redeployment.

`just prodstack add-node` leaves both kinds of VM Ready in MAAS, with
`prodstack-host` and `prodstack-dpu` tags for placement. For three hosts:

```sh
juju deploy ./microcloud.charm prodstack-dpus -n 6 \
  --constraints tags=prodstack-dpu --config node-role=dpu
```

Host PFs must already have VLANs 100, 101, 300 and 400 configured by MAAS
in `LINK_UP` mode. Bind the host application's `cluster` endpoint to
`management`; the service addresses belong on `lo`. Supply host config:

```yaml
prodstack-hosts:
  node-role: host
  snap-channel-microovn: latest/edge
  microcloud-access-network: fd2b:5d1a:7c93:100::/64
  microcloud-internal-network: fd2b:5d1a:7c93:101::/64
  ceph-public-network: fd2b:5d1a:7c93:400::/56
  ceph-internal-network: fd2b:5d1a:7c93:300::/56
  bgp-uplinks: dpu1p1.100,dpu2p1.100,dpu1p1.101,dpu2p1.101,dpu1p1.300,dpu2p1.300,dpu1p1.400,dpu2p1.400
```

Deploy the host application with the existing storage pool options, using
`--constraints tags=prodstack-host --bind cluster=management` and that
configuration file. The Ceph pools select their slot-0 host addresses
without copying OAM subnet bits into the slot. Per-OSD address allocation
remains separate work; this change configures the host addresses.

The host uses MicroOVN only for `microovn.bird`. Every other MicroOVN snap
service is stopped and disabled, and MicroCloud preseed omits OVN when
BGP uplinks are configured. Each VLAN imports only its service's `/128`s
and exports only its corresponding locally derived `/128`s on `lo`.
No fabric default, OAM prefix or learned BGP route is advertised by a host.

MAAS does not create a DPU bridge. DPU MicroOVN/OVN services and production
OVS-DOCA configuration are later steps. The simulator's DPUs remain VMs
because MAAS requires PXE commissioning and deployment; a Linux container
cannot exercise that provisioning path.
