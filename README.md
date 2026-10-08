# MicroCloud operator charm

This charm manages observability for a MicroCloud cluster.

**This charm does not deploy or bootstrap MicroCloud.** MicroCloud must
already be deployed out-of-band. Once it is:

1. Add each MicroCloud machine as a manual Juju machine, e.g.
   `juju add-machine ssh:user@host`, for every member of the MicroCloud
   cluster.
2. Deploy this charm across all of those machines, one unit per machine
   (`juju deploy ./microcloud_ubuntu@24.04-amd64.charm microcloud -n <N> \
   --to <machine-ids>`).

On every unit the charm requires MicroCloud to already be initialized and
verifies that this unit's own hostname is a MicroCloud member. If it is
not, the unit goes into an error state, since the charm is only supported
on machines that are already MicroCloud members.

## Build the charm

* Install `charmcraft` to build the charm: `snap install charmcraft --classic`
* Build the charm: `charmcraft pack`
  * For example use `--platform ubuntu@24.04:amd64` to only build the charm for a specific base and architecture

## Deploy the charm

Prerequisites:
* MicroCloud is already deployed and bootstrapped out-of-band on the target machines.
* A Juju controller reachable from those machines.
* Each target machine added to the model as a manual machine, e.g.
  `juju add-machine ssh:user@host`.

Then deploy the charm across those machines, one unit per machine:

`juju deploy microcloud --channel 3/stable -n 3 --to 0,1,2`

## Add a cluster member

Grow MicroCloud itself out-of-band first (adding the new member to the
MicroCloud cluster), then add the corresponding Juju machine and a new
charm unit on it (`juju add-machine` + `juju add-unit --to <machine-id>`).
Until both have happened, the new machine's unit will report an error
since its hostname is not yet a MicroCloud member.

## Observability

Integrate with COS:

As a prerequisite we need the COS stack.
Then offer the loki, prometheus and grafana SAAS bindings.
Later consume them and connect with the MicroCloud charm:

* `juju deploy opentelemetry-collector`
* `juju relate opentelemetry-collector <microcloud charm>:cos_agent`
* `juju relate opentelemetry-collector <microcloud charm>:logging`
* `juju relate opentelemetry-collector loki`
* `juju relate opentelemetry-collector prometheus`
* `juju relate opentelemetry-collector grafana`
