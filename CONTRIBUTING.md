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
