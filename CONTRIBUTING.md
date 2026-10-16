# Contributing to Igloo

Thanks for helping. This page covers how to build and test Igloo, how the
repository is laid out, the conventions we follow, and what a pull request
needs before it can be merged.

For anything larger than a small fix, open an issue first so we can agree on
the approach before you write the code. Igloo is designed by the
[product principles](docs/principles.md); say which ones a larger change serves
and which it costs. Report security problems privately as
described in [SECURITY.md](SECURITY.md), not in an issue.

## Building and testing

Only Docker is required (with Compose v2.20 or later). Every `make` target runs
its tools in containers.

| Command | What it does |
|---|---|
| `make test` | Run the Go tests in a `golang` container, with a throwaway PostgreSQL for the user-store tests |
| `make lint` | Check gofmt and run `go vet` |
| `make build` | Build the `igloo:dev` image |
| `make up` | Start the single-machine stack: Igloo, the Lakekeeper catalog, PostgreSQL and local object storage |
| `make psql` | Open a SQL shell on the running stack |
| `make helm-e2e` | Install the Helm chart on a throwaway kind cluster and test it end to end |
| `make e2e` | Start an isolated copy of the stack and run `scripts/e2e.sh` against it, including the JDBC and psycopg checks in `scripts/clients/` |
| `make down`, `make reset` | Stop the stack; `reset` also deletes its data |

`make help` lists every target.

### With a local Go toolchain

For faster iteration, install Go 1.27 and a C compiler (gcc or clang). Igloo
embeds DuckDB through cgo, so `CGO_ENABLED` must be `1` (the default when a C
compiler is installed).

```sh
go test ./...
go vet ./...
gofmt -l cmd internal    # must print nothing
```

The first build links DuckDB and is slow; later builds are cached.

### What CI checks

Every pull request runs [`.github/workflows/ci.yml`](.github/workflows/ci.yml):

- gofmt, `go vet` and `go test`
- the container image, built natively for linux/amd64 and linux/arm64
- `make e2e`
- `helm lint` and `helm template` for `deploy/helm/igloo`
- `tofu fmt`, `tofu validate` and `tofu test` for `deploy/aws`, `deploy/gcp` and `deploy/azure`
- ShellCheck on shell scripts and actionlint on the workflows

The Helm and OpenTofu versions CI uses are set at the top of `ci.yml`; use the
same ones to check chart or module changes locally.

## Repository layout

```
cmd/igloo/               server entry point and subcommands
internal/                server packages: configuration, DuckDB engine, Postgres wire protocol
deploy/single-vm/        Docker Compose stack for one machine
deploy/helm/igloo/       Helm chart
deploy/aws, gcp, azure/  OpenTofu modules for each cloud
docs/                    user documentation; configuration.md is the settings reference
scripts/                 helper scripts, including the end-to-end test
.github/                 CI and release workflows, issue and pull request templates
```

## Conventions

- **Formatting.** Code is formatted with gofmt and `go vet ./...` is clean.
- **Comments explain why.** Say what constraint, trade-off or past bug the code
  is shaped by, not what the next line does. Exported identifiers get doc
  comments.
- **Errors carry context.** Wrap with `fmt.Errorf("load TLS certificate: %w", err)`.
  An error a user will see should tell them what to change.
- **Tests use real Postgres clients.** Test protocol and SQL behavior by starting
  the server in-process and connecting with [pgx](https://github.com/jackc/pgx),
  under both the simple and the extended query protocol, as
  `internal/pgwire/server_test.go` does. Real clients catch protocol details
  that hand-built messages miss.
- **Tests need no cloud account.** Unit tests run offline from cloud services
  and need no credentials. Behavior that needs the catalog and object storage
  belongs in `scripts/e2e.sh`.
- **Configuration is environment variables.** A new setting is an `IGLOO_*`
  variable with a safe default, documented in
  [docs/configuration.md](docs/configuration.md).
- **Dependencies stay permissively licensed.** New dependencies must use a
  license compatible with Apache 2.0 (for example Apache 2.0, MIT, BSD or the
  PostgreSQL License). Say in the pull request why the dependency is worth it.

## Commits and pull requests

- Keep each pull request to one logical change, and keep refactoring separate
  from behavior changes.
- Write commit messages as a short imperative summary line ("Reject COPY to
  locations outside the allowlist"), a blank line, then why the change is
  needed.
- Add a line to [CHANGELOG.md](CHANGELOG.md) under Unreleased for anything
  users will notice.
- CI must pass, and a maintainer reviews every pull request before it merges.

## Sign-off

Igloo uses the [Developer Certificate of Origin](https://developercertificate.org/)
(DCO) instead of a Contributor License Agreement. There is nothing to sign
before you contribute. Instead, every commit carries a `Signed-off-by` line,
which certifies that you wrote the change or otherwise have the right to submit
it under the project's license, Apache 2.0.

`git commit -s` adds the line from your `user.name` and `user.email`:

```
Signed-off-by: Jane Doe <jane@example.com>
```

To sign off commits you have already made, run `git commit --amend -s --no-edit`
for the last one, or `git rebase --signoff main` for a whole branch, then force-push.
Pull requests with commits that are not signed off cannot be merged.

## Releasing

For maintainers:

1. In `CHANGELOG.md`, move the Unreleased entries under a new heading
   `## [X.Y.Z] - YYYY-MM-DD` and merge that change.
2. Tag the merge commit and push the tag:
   `git tag -a vX.Y.Z -m vX.Y.Z && git push origin vX.Y.Z`.
3. [`.github/workflows/release.yml`](.github/workflows/release.yml) runs CI,
   then publishes `ghcr.io/akkio-inc/igloo:X.Y.Z` (and `X.Y` and `latest` for
   stable releases) for linux/amd64 and linux/arm64, signs it, pushes the Helm
   chart to `oci://ghcr.io/akkio-inc/charts/igloo`, and creates a GitHub
   Release whose notes are that version's changelog section.

A tag with a suffix, such as `v1.2.0-rc.1`, is a prerelease: it gets only the
`1.2.0-rc.1` image tag.
