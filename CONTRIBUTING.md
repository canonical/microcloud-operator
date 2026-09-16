# Contributing

To make contributions to this charm, you'll need a working [development setup](https://juju.is/docs/sdk/dev-setup).

You can create an environment for development with `tox`:

```shell
tox devenv -e unit
source venv/bin/activate
```

## Testing

This project uses `tox` for managing test environments. There are some pre-configured environments
that can be used for linting and formatting code when you're preparing contributions to the charm:

```shell
tox run -e fmt           # update your code according to linting rules
tox run -e lint          # code style
tox run -e unit          # unit tests
tox                      # runs 'lint' and 'unit'
```

## Build the charm

Build the charm in this git repository using:

```shell
charmcraft pack
```

## Integration tests

The suite deploys a multi-unit, **LXD-only MicroCloud** (MicroCeph and MicroOVN are disabled by
setting their snap channels to the empty string) and asserts that the charm forms the cluster and
that MicroCloud, LXD and the Juju units all agree on membership. It also checks repeated
status actions on every unit. A separate model tests invalid configuration before bootstrap,
correction through `juju config`, and repeated configuration failure/recovery after bootstrap
without changing cluster membership.

A third model grows a two unit cluster by adding two units at once with `juju add-unit`.
MicroCloud can only add systems to a cluster that runs MicroCeph, so that model enables MicroCeph
without Ceph disks, which still fits in containers. Every model also checks that no join session,
worker or passphrase is left behind once the cluster has converged.

These tests cover the current single-application charm. Bounded join batches, cross-application
roles, and interrupted-join recovery need implementation before they can become passing
acceptance tests. MicroCeph storage and MicroOVN health require a separate VM-backed suite with
disks and suitable networking.

It needs a bootstrapped Juju controller on a LXD cloud and a charm built with `charmcraft pack`.
[`concierge`](https://github.com/canonical/concierge) provisions both from the `concierge.yaml` in
this repo — the same file CI uses:

```shell
sudo snap install --classic concierge
sudo concierge prepare -c concierge.yaml --verbose

charmcraft pack --platform=ubuntu@24.04:amd64
tox -e integration -- --charm ./microcloud_ubuntu@24.04-amd64.charm
```

Every argument is forwarded to `pytest`, so `--num-units`, `--constraints` and the usual
[`pytest-jubilant`](https://pypi.org/project/pytest-jubilant/) options work.

## CI

`.github/workflows/pr.yaml` runs on every pull request — except those touching only Markdown,
`LICENSE`, `.gitignore` or `.jujuignore`. Pushes to `main` and `dev` are tested by the Release
workflow instead (see [Publishing](#publishing)). Both call the reusable
`.github/workflows/build-and-test.yaml`, which:

1. **static** — `tox -e lint` and `tox -e unit`;
2. **build** — one job per base and architecture, packing `ubuntu@<base>:<arch>`, running
   `charmcraft analyse` on the result and uploading it as its own artifact.
   the result and uploading it as its own artifact;
3. **integration** — deploys each packed charm with `concierge` and runs the suite against it.

`build` depends on `static`, so a lint error never reaches a charm build. Every job runs with
`contents: read` and no other permission. The matrix covers every platform `charmcraft.yaml`
declares.

## Publishing

Every push to `main` or `dev` runs the **Release** workflow (`.github/workflows/release.yaml`):
it builds and tests the pushed commit with the same `build-and-test.yaml` pull requests use, then
uploads the packed charms to Charmhub:

| Branch | Channel       |
| ------ | ------------- |
| `main` | `3/edge`      |
| `dev`  | `3/edge/eken` |

The track, `3`, mirrors the MicroCloud snap's major version track. Each `main` revision also gets
a `3-rev<N>` GitHub release and tag; `dev` revisions do not.

`3/edge/eken` is a Charmhub branch. Branches are temporary and close if nothing is released to
them for a while, so a quiet `dev` can leave it empty.

Nothing above `edge` is published automatically. Moving a revision up a risk level is a separate
**Promote** workflow run (`.github/workflows/promote.yaml`), choosing the risk to promote from and
to; it refuses anything that is not a step up.

Both need a `CHARMHUB_TOKEN` repository secret:

```shell
charmcraft login --export=token.txt --charm=microcloud \
  --permission=package-manage-releases \
  --permission=package-manage-revisions \
  --permission=package-view-revisions --ttl=<seconds>
```

GitHub only offers a `workflow_dispatch` workflow once it exists on the default branch, so Promote
cannot be run until it has landed on `main`.
