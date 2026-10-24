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
| `make migrate-test` | Run the [migration report's](migrate/README.md) unit tests in a Python container |
| `make build` | Build the `igloo:dev` image |
| `make up` | Start the single-machine stack: Igloo, the Lakekeeper catalog, PostgreSQL and local object storage |
| `make psql` | Open a SQL shell on the running stack |
| `make helm-e2e` | Install the Helm chart on a throwaway kind cluster and test it end to end |
| `make e2e` | Start an isolated copy of the stack and run `scripts/e2e.sh` against it, including the JDBC, psycopg, PyIceberg, notebook (SQLAlchemy, pandas, polars) and dbt checks in `scripts/clients/` |
| `make bi-e2e` | Run `scripts/bi-e2e.sh`: Metabase, Apache Superset and Npgsql (Power BI) against an isolated stack. Pulls about 3 GB of images |
| `make engines-e2e` | Run Spark and Trino against an isolated copy of the stack's Iceberg REST catalog (`scripts/engines-e2e.sh`; pulls about 3 GB, not part of CI) |
| `make sharing-e2e` | Test Delta Sharing with the official Python client and the reference sharing server (`scripts/sharing-e2e.sh`); slower, and not part of CI |
| `make down`, `make reset` | Stop the stack; `reset` also deletes its data |
| `make site` | Build the website and documentation into `site/dist` and check its links ([details](#website)) |
| `make site-dev` | Serve the website at `http://localhost:4321`, updating as you edit the docs |

`make help` lists every target.

### With a local Go toolchain

For faster iteration, install Go 1.27 and a C compiler (gcc or clang). Igloo
embeds DuckDB through cgo, so `CGO_ENABLED` must be `1` (the default when a C
compiler is installed). The `duckdb_arrow` build tag builds DuckDB's Arrow
results, which the Arrow Flight SQL endpoint streams; every build needs it
(set `GOFLAGS=-tags=duckdb_arrow` for your editor).

```sh
go test -tags duckdb_arrow ./...
go vet -tags duckdb_arrow ./...
gofmt -l cmd internal    # must print nothing
```

The first build links DuckDB and is slow; later builds are cached.

### What CI checks

Every pull request runs [`.github/workflows/ci.yml`](.github/workflows/ci.yml):

- gofmt, `go vet` and `go test`
- `make migrate-test`
- the container image, built natively for linux/amd64 and linux/arm64
- `make e2e`
- `helm lint` and `helm template` for `deploy/helm/igloo`
- `tofu fmt`, `tofu validate` and `tofu test` for `deploy/aws`, `deploy/gcp` and `deploy/azure`
- ShellCheck on shell scripts and actionlint on the workflows
- `make site`: the website builds and has no broken links

[`.github/workflows/bi-tools.yml`](.github/workflows/bi-tools.yml) runs
`make bi-e2e` weekly and on demand.

The Helm and OpenTofu versions CI uses are set at the top of `ci.yml`; use the
same ones to check chart or module changes locally.

## Repository layout

```
cmd/igloo/               server entry point and subcommands
internal/                server packages: configuration, DuckDB engine, Postgres wire protocol
extensions/rb/           Igloo's DuckDB extension (C++): exact distinct counts in roaring bitmaps
third_party/             patched or vendored dependencies (duckdb-go, iceberg-go, psql-wire, CRoaring)
deploy/single-vm/        Docker Compose stack for one machine
deploy/helm/igloo/       Helm chart
deploy/aws, gcp, azure/  OpenTofu modules for each cloud
docs/                    user documentation; configuration.md is the settings reference
scripts/                 helper scripts, including the end-to-end test
bench/                   the price-performance benchmark (Python)
migrate/                 the migration report, python -m migrate (Python)
site/                    the website: landing page, and the docs built from the markdown above
.github/                 CI and release workflows, issue and pull request templates
```

## Website

`site/` is the website: a landing page and the documentation, built with
[Astro](https://astro.build/) and [Starlight](https://starlight.astro.build/)
into a static site in `site/dist`. The documentation pages are the
repository's markdown, so there is nothing to keep in sync by hand: edit
`docs/sql.md` and the site's SQL page changes with it.

`make site` builds it in a pinned Node container and fails on a broken link;
`make site-dev` serves it at `http://localhost:4321` and reloads as you edit
the docs. With Node 22.12 or later installed, `npm ci` and then
`npm run build` or `npm run dev` in `site/` do the same without Docker
(`npm ci` again when switching between the two, since `node_modules` holds
platform-specific binaries).

**Which files are pages.** `site/src/docs-manifest.mjs` lists them, with
their URL and sidebar group: the README (as the overview), `docs/*.md`,
`docs/design/`, the Helm chart's and cloud modules' READMEs, the benchmark,
and `SECURITY.md`, `CONTRIBUTING.md` and `CHANGELOG.md`. A new document
needs an entry there to appear on the site. `docs/upstream/` (drafts of issues
for upstream projects) is left out.

**How the sync works.** `site/scripts/sync-docs.mjs` runs before every build.
It copies each page into `site/src/content/docs/`, taking the page title from
the first `# Heading`, and rewrites links: a link to another page
(`flight-sql.md`, `../docs/sso.md#tokens`) becomes the page's URL on the site
with its anchor, an image is copied into the site (as WebP), and a link to
any other file or directory (Go code, scripts, compose files) goes to the
file on GitHub at `main`. GitHub's `<picture>` with a dark-mode `<source>`
becomes two images that follow the site's theme switch. A relative link to a
file that does not exist fails the sync. Write links as you would for GitHub;
they work in both places. The landing page (`site/src/pages/index.astro` and
`site/src/components/landing/`) quotes the README; its quick start and "How
it works" text are taken from the README at build time, the rest is written
out, so update it when the README's features or benchmark summary change.

**Link check.** After the build, `site/scripts/check-links.mjs` checks every
link, image and `#anchor` in `site/dist`, and that links to this repository
on GitHub name files that exist. External links are not fetched.

**Not indexed until launch.** By default every page carries
`<meta name="robots" content="noindex, nofollow">`, `robots.txt` disallows
everything, and `site/dist/_headers` makes Cloudflare send
`X-Robots-Tag: noindex, nofollow`. Build with `make site SITE_PUBLIC=1` to
drop all three when the site goes public; add
`SITE_URL=https://<domain>` for canonical URLs and a sitemap.

**Deploying.** The site is static. It is deployed as a Cloudflare Worker
that serves `site/dist` (what Cloudflare Pages became), configured in
`site/wrangler.jsonc`, to https://igloo.<account>.workers.dev:

```sh
make site
cd site && npx wrangler@4.147.0 deploy
```

`make site` replaces `site/node_modules` with Linux builds, so run
`npx wrangler` through `npx` as above rather than from `site/node_modules`.

## Conventions

- **Formatting.** Code is formatted with gofmt and `go vet -tags duckdb_arrow ./...` is clean.
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
- A change to erasure, or to what it relies on (the catalog, files, history,
  caches, sharing), keeps [what erasure
  guarantees](docs/privacy.md#what-erasure-guarantees): one that can let a
  request report itself completed while the person's data is still readable
  or recoverable within that guarantee does not merge. Anything else such a
  change leaves, or that can keep a request from completing, is documented
  under [Known limitations](docs/privacy.md#known-limitations), with what an
  admin can do about it.

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
