# MicroCloud charm for MAAS

This charm deploys MicroCloud using Juju.

## Build the charm

* Install `charmcraft` to build the charm: `snap install charmcraft --classic`
* Build the charm: `charmcraft pack`
  * For example use `--platform ubuntu@24.04:amd64` to only build the charm for a specific base and architecture

## Deploy the charm

Run `juju deploy <charm> --config orchestrator=true`.

The application with `orchestrator=true` starts the MicroCloud cluster. Without it, units wait for an `orchestrator` relation and never form a cluster of their own.

## Orchestrator and member applications

A Juju application has one architecture and one hardware profile. To add machines of another architecture or profile to the same MicroCloud, deploy them as a *member* application and relate it to the *orchestrator* application:

```sh
juju deploy <charm> microcloud -n 3 --constraints arch=amd64 \
    --config orchestrator=true --config snap-channel-microceph=tentacle/stable
juju deploy <charm> microcloud-arm64 -n 2 --constraints arch=arm64
juju integrate microcloud-arm64:orchestrator microcloud:members
```

The two endpoints share the `microcloud_cluster` interface:

* `members` (provides) on the orchestrator. Relate any number of member applications.
* `orchestrator` (requires, limit 1) on each member application.

The orchestrator leader bootstraps with its own units, then grows the cluster with member units, one member application at a time. A member unit never bootstraps. Member units can be related before or after the orchestrator bootstraps. Units added to the orchestrator application later join before member units do, but member units do not wait for an orchestrator unit that is not ready yet or whose join sessions keep failing.

`orchestrator` cannot change after the cluster forms; the units block until it is changed back. A cluster that formed before the charm had this option, or outside Juju, has neither side recorded: its units report `Cluster ready; no orchestrator relation, set orchestrator=true` until you set the option. The charm also blocks:

* an application with `orchestrator=true` related on `orchestrator`, or one with `orchestrator=false` on `members`;
* the `members` relation while the orchestrator has no MicroCeph. MicroCloud only adds members to clusters that have MicroCeph.

Member applications:

* **Snaps.** Member units install the channels the cluster's snaps track and ignore their own `snap-channel-*` options. Once the cluster has formed, changing `snap-channel-*` on the orchestrator does not change what member units install, nor what a unit added to the orchestrator application installs: both follow the channels the cluster tracks. MicroCloud only joins a system whose services report the same versions, so a member unit refreshes a snap whose version differs, and blocks while it still does.
* **MicroCeph across architectures.** Use a MicroCeph channel whose builds match on every architecture: `tentacle/stable` (the default) or `squid/edge`. `squid/stable` builds differ between amd64 and arm64.
* **Storage.** A member unit's storage must match the cluster's pools, which the orchestrator created at bootstrap. A unit blocks if it has Ceph disks on a cluster without Ceph pools, has no local device on a cluster with a `local` pool, or has a local device on a cluster without one. Ceph disks also follow `ceph-osd-architectures`. `storage-wipe` and `storage-encrypt` apply to each application's own disks, so a member application sets them itself.
* **Failed joins.** A member application whose join sessions fail 3 times in a row is excluded; its units block with `excluded after 3 failed join sessions: <error>`. Adding or removing one of its units, changing what a unit publishes (its address, service versions, OVN settings or storage), or removing and re-adding the relation clears the count.
* **Relation removal.** Removing the relation leaves the cluster as it is. Member units report `Cluster ready; orchestrator relation removed`, and the orchestrator's `status` action marks them `(detached)`. Re-adding the relation opens no new session.
* **Scale-down.** As for orchestrator units, `juju remove-unit` leaves the MicroCloud member behind. While the relation exists, the orchestrator leader blocks until you remove it with `microcloud remove <name>`. A unit of a detached application stays known, so its stale member is not flagged; remove it the same way.
* **Observability.** Relate each application to its own `cos-agent`.
* **Evacuation.** LXD only moves instances to members of the same architecture. Evacuating the only member of an architecture fails with `No suitable cluster member could be found`.

Assign the `control-plane` role (below) to orchestrator units before relating member applications. Member units then join as spares and the cluster database stays on the orchestrator's hardware.

The `status` action reports `cluster-mode` and, on the orchestrator leader, `joined-units`: the member units that joined, by MicroCloud member name.

Relate member applications in the same model only. Offering `members` across models would let anyone with `consume` access join machines to the cluster.

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

## DPUs

The [`microcloud-dpu`](https://github.com/canonical/microcloud-dpu-operator) charm runs the LXD DPU agent on each BlueField DPU of a host. Relate it on the `dpus` endpoint, and bind that endpoint to the OAM space the DPUs serve on:

```sh
juju deploy <charm> microcloud ... --bind dpus=oam
juju integrate microcloud-dpu:host microcloud:dpus
```

Each unit pairs the DPUs of its own LXD member. For each one, it adds an LXD constituent of type `dpu` and shares the join token in a Juju secret granted to that DPU unit only. The secret expires with the token, and the unit removes it once LXD trusts the DPU. Removing a DPU unit or the relation removes its constituent.

This needs an LXD with the `cluster_constituents` API extension; set `snap-channel-lxd` to a build that has it. A unit only pairs DPUs once its LXD is a cluster member.

DPU problems never block the unit. It adds them to its status instead, for example `Cluster ready; DPU node-1-dpu-0 unreachable`:

* `DPU <name> unreachable`: LXD trusts the DPU but its health probe fails.
* `DPU <name> not in LXD; run refresh-pairing on <unit>`: the constituent was removed in LXD. The unit doesn't pair it again by itself; run the `refresh-pairing` action on the DPU unit it names.
* `DPUs <unit> and <unit> share a name`: two DPUs have the same hostname. Rename one.
* `DPU name <name> is taken`: LXD already has a constituent or a pending token with that name that this unit didn't issue. Remove it, or wait for the token to expire.
* `cannot pair DPU <name>` or `cannot pair DPUs; see juju debug-log`: the LXD call failed.

The `lxd_dpu` interface has no application data. Each unit publishes `member` (its LXD member name), `oam-address` (its `dpus` binding address, the only source the DPU agents accept) and `tokens` (the secret ID per DPU unit waiting to join). Each DPU unit publishes `name`, `host`, `address` and, once joined, `joined`. The `microcloud-dpu` README describes both sides.
