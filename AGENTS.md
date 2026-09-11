# Agent instructions

Conventions and gotchas for this repository. Generic Juju charm guidance lives in the
`juju-charm-*` skills; this file covers only what is specific to `charm-microcloud`.

## Branches and remotes

- **Pull requests target `dev`, not `main`.** GitHub's default branch is still `main`, which holds
  the older charmcraft 3 tree (`metadata.yaml` + `config.yaml`, `bases:`). Active work is on `dev`.
- **There is usually no `canonical` remote.** `origin` is a personal fork. To see upstream `dev`:

  ```shell
  git fetch https://github.com/canonical/charm-microcloud.git dev:refs/remotes/canonical/dev
  ```

  Branch from `canonical/dev`, not from a stale local `dev`.
- A `workflow_dispatch` workflow only appears in the Actions tab once it exists on the **default**
  branch. Landing one on `dev` alone is not enough; use `gh workflow run` until it reaches `main`.

## Commits

- **Commits must be signed and signed off.** SSH signing is configured
  (`gpg.format=ssh`, `commit.gpgsign=true`). Never pass `--no-gpg-sign`: it silently produces
  unsigned commits that GitHub reports as unverified. Check with
  `git log --format='%h %G? %s'` (want `G`), and use `-s -S` when committing by hand.
- To re-sign an existing branch: `git rebase --force-rebase --gpg-sign --signoff <base>`.
  `--force-rebase` is required, otherwise an already-current branch no-ops without rewriting.
- Subjects are succinct and carry a path-like prefix: `github/workflows:`, `test/integration:`,
  `charmcraft:`, `CONTRIBUTING:`, `src/charm:`.
- One logical change per commit. Prefer `git commit --fixup <sha>` plus
  `git rebase --autosquash` over appending follow-up commits.
- Do not add AI attribution trailers.

## Pull requests

- Small and stacked. Open as drafts, with a short description covering what changed and why.
- Do not open a PR unless asked.
- Force-pushing dismisses an existing approval; check `reviewDecision` afterwards.

## Layout

| Path | What |
|---|---|
| `src/` | `charm.py` plus `cluster.py`, `preseed.py`, `microcloud.py`, `snap.py`, `ceph_mgr.py`, `ovn_exporter.py` |
| `tests/unit/` | `ops.testing` State/Context tests |
| `tests/integration/` | jubilant tests, need a real Juju controller |
| `charmcraft.yaml` | Metadata, config, actions, relations, storage (charmcraft 4, no `metadata.yaml`) |
| `concierge.yaml` | Provisions Juju + LXD for the integration suite |
| `.github/workflows/` | `build-and-test.yaml` (reusable), `pr.yaml`, `update-libs.yml`, `zizmor.yml` |

## Testing

```shell
tox -e lint     # ruff check + format --check, pinned to ruff 0.16.6
tox -e fmt      # auto-fix
tox -e unit     # ops.testing
```

`tox` defaults to `lint, unit`. Integration tests need a bootstrapped controller and a packed
charm, and **cannot run in the Workshop container** (no LXD, no nested virtualisation). Launch a
dev machine instead, per the `dev-machines` skill:

```shell
devctl machine launch microcloud     # block devices for MicroCeph
devctl machine launch juju           # charm dev, then: just setup-juju
```

Then, on the machine:

```shell
charmcraft pack --platform=ubuntu@24.04:amd64
tox -e integration -- --charm ./microcloud_ubuntu@24.04-amd64.charm
```

Never claim something is untestable because there is no local LXD; launch a dev machine.

## Gotchas

- **`charmcraft analyse` needs `--ignore entrypoint,pydeps`.** Both are upstream charmcraft bugs,
  not problems with this charm: the `uv` plugin writes an unexpanded `${dispatch_path}` entrypoint,
  and the `pydeps` check crashes. CI already passes these flags.
- **Reusable workflows use `$/`, not `./`.** `uses: $/.github/workflows/build-and-test.yaml` is
  GitHub's self-repository form; zizmor flags `./`. Verified working at job level.
- **A charm cannot read MAAS tags or constraints.** No hook tool exposes them. The availability
  zone is the exception, via `JUJU_AVAILABILITY_ZONE`, and it is empty on the LXD provider used
  for testing.
- **`juju add-unit` cannot be refused by a charm.** The unit is created and a machine allocated
  before any charm code runs, so sizing constraints surface as a blocked or waiting unit, never as
  a rejected command.
- **`juju ssh` needs `--pty=false`** when capturing output in tests.
- **Integration tests run LXD-only** by setting `snap-channel-microceph=""` and
  `snap-channel-microovn=""`; a nested container has no spare disks for Ceph.

## Publishing

Manual, via the `Release` workflow (`workflow_dispatch`), with `track` as an input. Nothing above
`edge` is automatic; `Promote` moves a revision up one risk level. Requires a `CHARMHUB_TOKEN`
repository secret. See CONTRIBUTING for the `charmcraft login --export` invocation.
