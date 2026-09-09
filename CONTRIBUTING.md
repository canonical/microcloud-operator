# Contributing

To make contributions to this charm, you'll need a working [development setup](https://juju.is/docs/sdk/dev-setup).

You can create an environment for development with `tox`:

```shell
tox devenv -e integration
source venv/bin/activate
```

## Testing

This project uses `tox` for managing test environments. There are some pre-configured environments
that can be used for linting and formatting code when you're preparing contributions to the charm:

```shell
tox run -e format        # update your code according to linting rules
tox run -e lint          # code style
tox run -e static        # static type checking
tox run -e unit          # unit tests
tox run -e integration   # integration tests
tox                      # runs 'format', 'lint', 'static', and 'unit' environments
```

## Build the charm

Build the charm in this git repository using:

```shell
charmcraft pack
```

## CI

`.github/workflows/pr.yaml` runs on every pull request — except those touching only Markdown,
`LICENSE`, `.gitignore` or `.jujuignore` — and calls the reusable
`.github/workflows/build-and-test.yaml`, which:

1. **static** — `tox -e lint` and `tox -e unit`;
2. **build** — one job per base and architecture, packing `ubuntu@<base>:<arch>`, running
   `charmcraft analyse` on the result and uploading it as its own artifact.

`build` depends on `static`, so a lint error never reaches a charm build. Every job runs with
`contents: read` and no other permission. The matrix covers every platform `charmcraft.yaml`
declares.

## Publishing

Publishing is manual. Run the **Release** workflow (`.github/workflows/release.yaml`) from the
Actions tab against the branch you want to publish: it builds and tests that branch, then uploads
to Charmhub. A `<track>-rev<N>` git tag is pushed for each revision.

The `track` input selects the Charmhub track and defaults to `3`, mirroring the MicroCloud
snap's major version tracks.

Nothing above `edge` is published automatically. Moving a revision up a risk level is a separate
**Promote** workflow run (`.github/workflows/promote.yaml`), choosing the risk to promote from and
to; it refuses anything that is not a step up.

Both workflows need a `CHARMHUB_TOKEN` repository secret:

```shell
charmcraft login --export=token.txt --charm=microcloud \
  --permission=package-manage-releases \
  --permission=package-manage-revisions \
  --permission=package-view-revisions --ttl=<seconds>
```

GitHub only offers a `workflow_dispatch` workflow once it exists on the default branch, so neither
appears in the Actions tab until they have landed on `main`.
