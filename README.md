<p align="left">
    <img alt="MicroCloud logo" width="10%" src="doc/images/microcloud_logo_dark.svg#gh-dark-mode-only">
    <img alt="MicroCloud logo" width="10%" src="doc/images/microcloud_logo_light.svg#gh-light-mode-only">
</p>

# MicroCloud Operator

The MicroCloud Operator charm brings observability to an existing
[MicroCloud](https://canonical.com/microcloud) cluster. Once integrated with the
[Canonical Observability Stack (COS)](https://charmhub.io/topics/canonical-observability-stack),
it exposes cluster metrics and logs so you can monitor the health of your MicroCloud deployment.

**This charm does not deploy or bootstrap MicroCloud.** MicroCloud must
already be deployed out-of-band. The charm is installed alongside an
existing MicroCloud cluster, one unit per cluster member, and verifies on
every unit that the machine is in fact a MicroCloud member. If it is not,
the unit goes into an error state, since the charm is only supported on
machines that are already part of a MicroCloud cluster.

## Requirements

* A MicroCloud cluster already deployed and bootstrapped out-of-band.
* A Juju controller that can reach the MicroCloud machines.
* Each MicroCloud machine added to the Juju model as a manual machine, e.g.
  `juju add-machine ssh:user@host`.

## How to get started

Add each MicroCloud machine as a manual Juju machine:

```shell
juju add-machine ssh:user@host
```

Then deploy the charm across those machines, one unit per machine:

```shell
juju deploy microcloud --channel 3/stable -n <N> --to <machine-ids>
```

For example, for a 3-member MicroCloud cluster mapped to Juju machines `0,1,2`:

```shell
juju deploy microcloud --channel 3/stable -n 3 --to 0,1,2
```

## Relations

Integrate the charm with the Canonical Observability Stack (COS) to collect
metrics and logs from your MicroCloud cluster:

```shell
juju deploy opentelemetry-collector
juju relate opentelemetry-collector <microcloud charm>:cos_agent
juju relate opentelemetry-collector <microcloud charm>:logging
juju relate opentelemetry-collector loki
juju relate opentelemetry-collector prometheus
juju relate opentelemetry-collector grafana
```

This requires the COS stack to already be deployed, with the `loki`,
`prometheus`, and `grafana` SAAS offers available to consume.

## Add a cluster member

To grow the deployment, first add the new member to the MicroCloud cluster
itself, out-of-band. Once the machine is a MicroCloud member, add the
corresponding Juju machine and a new charm unit on it:

```shell
juju add-machine ssh:user@host
juju add-unit microcloud --to <machine-id>
```

## Security

Security issues in the MicroCloud Operator can be reported through
[private security reports](https://github.com/canonical/microcloud-operator/security/advisories/new)
on GitHub. For more information, see the [Security policy](SECURITY.md).

## Contributing

* For best practices on how to write and contribute to charms, see the [Juju SDK docs](https://juju.is/docs/sdk/how-to)
* For developer guidance specific to this charm, see [CONTRIBUTING.md](CONTRIBUTING.md)
* Report bugs and feature requests through the [GitHub Issues tab](https://github.com/canonical/microcloud-operator/issues)

## License

The MicroCloud Operator is distributed under the [Apache Software License, version 2.0](LICENSE).
