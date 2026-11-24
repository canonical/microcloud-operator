# Contributing

To make contributions to this charm, you'll need a working [development setup](https://canonical.com/juju/docs/ops/latest/).

You can create an environment for development with `tox`:

```shell
tox devenv -e unit
source venv/bin/activate
```

## Testing

This project uses `tox` for managing test environments. There are some pre-configured environments
that can be used for linting and formatting code when you're preparing contributions to the charm:

```shell
tox run -e format        # update your code according to linting rules
tox run -e lint          # code style
tox run -e unit          # unit tests
tox                      # runs 'lint' and 'unit' environments
```

The integration tests use [`jubilant`](https://github.com/canonical/jubilant)
and exercise a real MicroCloud cluster in LXD VMs. They need an LXD-backed Juju
controller, a K8s-backed controller for the COS scenarios, and a packed
charm:

```shell
sudo snap install --classic concierge
sudo concierge prepare -c concierge.yaml --verbose

charmcraft pack --platform=ubuntu@26.04:amd64
tox -e integration -- \
  --charm ./microcloud_ubuntu@26.04-amd64.charm \
  --lxd-controller integration-lxd \
  --k8s-controller integration-k8s
```

`concierge.yaml` uses the same controller names for local and CI runs. The VM
image, snap channels and COS channels can be overridden with environment
variables (see `tests/integration/vms.py` and `tests/integration/cos.py`).

Use the usual `pytest-jubilant` options when iterating locally.
`--juju-dump-logs logs/` writes the Juju debug logs before the models are torn
down.

## Build the charm

Build the charm in this git repository using:

```shell
charmcraft pack
```

## CI

`.github/workflows/pr.yaml` runs on every pull request — except those touching only Markdown,
`LICENSE`, `.gitignore` or `.jujuignore`. Pushes to `main` and `dev` are tested by the Release
workflow instead (see [Publishing](#publishing)). Both call the reusable
`.github/workflows/build-and-test.yaml`, which:

1. **static** — `tox -e lint` and `tox -e unit`;
2. **build** — one job per base and architecture, packing `ubuntu@<base>:<arch>`, running
   `charmcraft analyse` on the result and uploading it as its own artifact;
3. **integration** — deploys COS Lite, creates MicroCloud clusters in
   `ubuntu-minimal-daily:26.04` LXD VMs and runs the jubilant scenarios in `tests/integration/`
   against the packed `26.04`/`amd64` charm.

`build` depends on `static`, so a lint error never reaches a charm build. `integration` depends on
`build` and tests the exact charm artifact produced there. Every job runs with `contents: read`
and no other permission. The build matrix covers every platform `charmcraft.yaml` declares.

## Publishing

Every push to `release-3` or `dev` runs the **Release** workflow (`.github/workflows/release.yaml`):
it builds and tests the pushed commit with the same `build-and-test.yaml` pull requests use, then
uploads the packed charms to Charmhub:

| Branch      | Channel       |
| ----------- | ------------- |
| `release-3` | `3/edge`      |
| `dev`       | `3/edge/eken` |

The track, `3`, mirrors the MicroCloud snap's major version track. Each `release-3` revision also gets
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
