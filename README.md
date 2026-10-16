<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/banner-dark.png">
    <img alt="igloo: open source SQL warehouse on Apache Iceberg. 5x faster, 25x cheaper per query." src="docs/assets/banner-light.png" width="100%">
  </picture>
</p>

# Igloo

Igloo is an open-source SQL data warehouse that runs in your own cloud
account or on your own hardware.

On TPC-H at scale factor 10 with tables in Amazon S3, Igloo on one 16-core
machine ran the 22 queries at least 5 times faster than a Snowflake X-Small
or Databricks SQL Serverless 2X-Small warehouse, for less than a twenty-fifth
of the cost per query at list prices. See the [benchmark](bench/README.md)
for the method, every result and its caveats.

- **Postgres-compatible.** Clients connect over the PostgreSQL wire protocol,
  so `psql`, JDBC, psycopg, pgx and BI tools work without an Igloo driver.
- **Open table format.** Tables are [Apache Iceberg](https://iceberg.apache.org/)
  in object storage (S3, GCS, Azure Data Lake Storage, or any S3-compatible
  store). Spark, Trino, Snowflake and Databricks can read the same data; there
  is no proprietary storage to migrate out of.
- **Fast on one machine, cheap to run.** Queries run on an embedded
  [DuckDB](https://duckdb.org/). Most analytical workloads fit on a single
  node; you pay for your own VMs and storage, nothing else.
- **Runs anywhere.** Docker Compose for one machine, a Helm chart for any
  Kubernetes cluster, and OpenTofu modules that stand up the whole stack on
  AWS, Google Cloud or Azure.

Licensed under [Apache 2.0](LICENSE).

## Quick start

Requires Docker with Compose v2.20 or later.

```sh
make up      # generates deploy/single-vm/.env with random secrets and starts the stack
make psql    # SQL shell as the admin user
```

```sql
CREATE TABLE events (id INTEGER, kind VARCHAR, at_time TIMESTAMP);
INSERT INTO events VALUES (1, 'signup', now()), (2, 'login', now());
SELECT kind, count(*) FROM events GROUP BY kind;
```

Any Postgres client can connect to `127.0.0.1:5433` as user `igloo` with the
password `IGLOO_ADMIN_PASSWORD` from `deploy/single-vm/.env`:

```sh
psql "host=127.0.0.1 port=5433 user=igloo dbname=igloo"
```

Try it on real data with the TPC-H benchmark:

```sh
make load-tpch    # scale factor 1: 6 million lineitem rows
make tpch         # runs and times all 22 TPC-H queries
```

On a laptop, the scale factor 1 load takes about 20 seconds and the 22
queries take about 4 seconds in total with warm caches; their results match
the official TPC-H answers.

## What you get

- **Iceberg tables** with `CREATE [OR REPLACE] TABLE`, `CREATE TABLE AS`,
  `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `DROP`, schemas, views, comments,
  and time travel (`SELECT * FROM t AT (TIMESTAMP => TIMESTAMP '2026-10-01 00:00')`).
- **Users and roles**: SCRAM-SHA-256 passwords and three roles managed in SQL.

  ```sql
  CREATE USER ana WITH PASSWORD 'a-long-password' ROLE writer;  -- admin, writer or reader
  ALTER USER ana ROLE reader;
  SHOW USERS;
  ```
- **A locked-down query engine**: SQL cannot read the server's files or reach
  any location except the warehouse and locations you allow.
- **Automatic table maintenance**: small files are compacted, snapshots older
  than the retention (7 days by default) are expired, and unreferenced files
  are deleted, with nothing to schedule.
- **Several replicas** behind one endpoint, all reading and writing the same
  tables; concurrent writes to a table are retried automatically.
- **[Scale to zero](docs/scale-to-zero.md)**: a small gateway stops Igloo's
  machines or pods when nobody uses them and starts them when a client
  connects, on AWS, Google Cloud, Azure or Kubernetes.
- **Operations basics**: health and readiness endpoints, Prometheus metrics,
  JSON logs, statement timeouts, connection limits, TLS, query cancel.

## Deploy

| Where | How |
|---|---|
| One machine (laptop, VM, on-prem server) | [deploy/single-vm](deploy/single-vm/docker-compose.yml) with Docker Compose: `make up` |
| Any Kubernetes cluster | [Helm chart](deploy/helm/igloo/README.md) |
| AWS (EKS, S3, RDS) | [deploy/aws](deploy/aws/README.md) OpenTofu module |
| Google Cloud (GKE, GCS, Cloud SQL) | [deploy/gcp](deploy/gcp/README.md) OpenTofu module |
| Azure (AKS, ADLS Gen2, PostgreSQL Flexible Server) | [deploy/azure](deploy/azure/README.md) OpenTofu module |

The single-machine stack can keep tables in Amazon S3 instead of the bundled
local store; see the bottom of `deploy/single-vm/.env`.

## How it works

```
 psql / JDBC / BI tool
          │  PostgreSQL wire protocol (SCRAM, optional TLS)
          ▼
 ┌──────────────────────┐   Iceberg REST   ┌────────────┐     ┌────────────┐
 │ igloo (1..n replicas)│ ───────────────▶ │ Lakekeeper │ ──▶ │ PostgreSQL │
 │  DuckDB inside       │                  │  catalog   │     │  metadata  │
 └──────────────────────┘                  └────────────┘     └────────────┘
          │  Parquet data and Iceberg metadata                       ▲
          ▼                                                          │
 ┌─────────────────────────────────────────────┐        users and roles
 │ Object storage: S3, GCS, ADLS, S3-compatible │        (schema "igloo_auth")
 └─────────────────────────────────────────────┘
```

- **igloo** (this repository, Go) accepts Postgres connections, checks each
  statement against the user's role, and runs it on an embedded DuckDB
  attached to the catalog. On start it sets up the catalog and warehouse
  itself, so every deployment works the same way.
- **[Lakekeeper](https://github.com/lakekeeper/lakekeeper)** is the Iceberg
  REST catalog: it tracks tables, schemas and snapshots. Only Igloo talks to
  it.
- **PostgreSQL** stores the catalog's metadata and Igloo's users.
- **Object storage** holds the table data as Parquet files.

Every component uses a permissive open-source license (Apache 2.0, MIT or the
PostgreSQL License).

## Documentation

- [Product principles](docs/principles.md): what Igloo optimizes for, in order
- [Configuration](docs/configuration.md): every setting
- [Operations](docs/operations.md): sizing, scaling, users, monitoring,
  backups, table maintenance, upgrades, troubleshooting
- [Scale to zero](docs/scale-to-zero.md): stop compute when idle and start it
  on the next connection
- [SQL and clients](docs/sql.md): the SQL dialect, Iceberg features, type
  mapping, tested drivers and tools
- [Benchmark](bench/README.md): price-performance against Snowflake and
  Databricks on the same data and queries
- [Security](SECURITY.md): the security model and how to report a
  vulnerability
- [Contributing](CONTRIBUTING.md)

## Limitations in 1.0

- **One engine per query.** Each query runs on one Igloo replica. Scale up
  with bigger machines and out with more replicas; very large queries that
  need a cluster are not supported yet.
- **Roles apply to the whole warehouse.** There are no per-schema or per-table
  grants yet.
- **Keys are not enforced.** `PRIMARY KEY` and `UNIQUE` on Iceberg tables are
  accepted but not checked, so duplicates are possible. `NOT NULL` is
  enforced.

## Repository layout

```
cmd/igloo/          server entry point and subcommands
internal/auth/      users, roles, SCRAM verifiers
internal/catalog/   catalog and warehouse setup
internal/config/    environment-variable configuration
internal/engine/    embedded DuckDB, storage, sandbox
internal/pgwire/    Postgres wire protocol, authorization, compatibility
deploy/             Docker Compose, Helm chart, AWS/GCP/Azure modules
docs/               documentation
scripts/            test and end-to-end scripts
```
