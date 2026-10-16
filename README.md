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
- **Arrow-native extracts.** [Arrow Flight SQL](docs/flight-sql.md) streams
  results to pandas, polars, Spark and BI tools as Arrow record batches,
  through ADBC or the Flight SQL JDBC driver, several times faster than the
  PostgreSQL protocol.
- **Open table format.** Tables are [Apache Iceberg](https://iceberg.apache.org/)
  in object storage (S3, GCS, Azure Data Lake Storage, or any S3-compatible
  store), with no proprietary storage to migrate out of. Spark, Trino and
  PyIceberg read and write them through Igloo's
  [Iceberg REST catalog](docs/external-engines.md), under Igloo's users and
  grants; Databricks and Snowflake connect the same way.
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

Or open the [web console](docs/console.md) at `http://127.0.0.1:8443` and sign
in with the same user and password, or fetch results as Arrow over
[Flight SQL](docs/flight-sql.md) at `grpc://localhost:31337`:

```python
import adbc_driver_flightsql.dbapi as flightsql

with flightsql.connect("grpc://localhost:31337",
                       db_kwargs={"username": "igloo", "password": "..."}) as conn, conn.cursor() as cur:
    cur.execute("SELECT * FROM events")
    df = cur.fetch_df()  # or fetch_arrow_table(), or polars.read_database(query, conn)
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
- **Users, roles and grants**: SCRAM-SHA-256 passwords, built-in roles for
  the whole warehouse, and custom roles with grants on schemas and tables,
  all managed in SQL.

  ```sql
  CREATE USER ana WITH PASSWORD 'a-long-password' ROLE writer;  -- admin, writer, reader or none
  CREATE USER nora WITH PASSWORD 'another-password' ROLE none;
  CREATE ROLE analysts;
  GRANT analysts TO nora;
  GRANT SELECT ON SCHEMA marts TO analysts;
  ```
- **[Single sign-on](docs/sso.md)** to the web console with Okta, Microsoft
  Entra ID, Google Workspace or any OpenID Connect provider, with users
  created at first sign-in (or provisioned with SCIM) and the provider's
  groups mapped to roles; **access tokens** that BI tools, dbt and scripts
  send instead of a password; and **service accounts** for programs.

  ```sql
  GRANT analysts TO SSO GROUP 'Data Team';
  CREATE USER dbt_prod TYPE SERVICE ROLE none;
  CREATE TOKEN ci FOR USER dbt_prod EXPIRES '90d';   -- shown once, stored hashed
  ```
- **[Row access and masking policies, and tags](docs/security-policies.md)**,
  as in Snowflake: show each team only its clients' rows, and mask emails,
  device IDs and other identifiers for everyone not allowed to see them,
  in every statement, the web console included.

  ```sql
  CREATE ROW ACCESS POLICY client_rows AS (cid VARCHAR) RETURNS BOOLEAN ->
    EXISTS (SELECT 1 FROM governance.client_access a
            WHERE a.client_id = cid AND is_role_in_session(a.role_name));
  ALTER TABLE marts.impressions ADD ROW ACCESS POLICY client_rows ON (client_id);
  CREATE MASKING POLICY mask_pii AS (val VARCHAR) RETURNS VARCHAR ->
    CASE WHEN is_role_in_session('pii_reader') THEN val ELSE sha256(val) END;
  CREATE TAG pii ALLOWED_VALUES 'email', 'device_id';
  ALTER TAG pii SET MASKING POLICY mask_pii;
  ALTER TABLE marts.impressions ALTER COLUMN email SET TAG pii = 'email';
  ```
- **[Delta Sharing](docs/sharing.md)**, both ways: share tables with clients
  and partners, who read them with Databricks, Spark, pandas, Power BI or
  Tableau through the open protocol, and query partners' shares as
  external tables, with grants as for any table.

  ```sql
  CREATE SHARE acme_reporting;
  ALTER SHARE acme_reporting ADD TABLE marts.campaign_daily;
  CREATE RECIPIENT acme;                       -- returns a one-time activation link
  GRANT SELECT ON SHARE acme_reporting TO RECIPIENT acme;
  CREATE EXTERNAL TABLE partners.viewership FROM SHARE tvco_ads.tv.viewing.daily;
  ```
- **Query and login history** in `igloo.query_history` and
  `igloo.login_history`: every statement and login, kept for 90 days, with
  each statement's CPU time, bytes and memory.
- **[Loading and scheduling](docs/loading.md)** with nothing else to run:
  `COPY INTO` loads CSV, TSV, Parquet and JSON drops from a bucket, each
  file once; pipes load new files as they land; tasks run SQL on a cron or
  interval schedule in any time zone; materialized views keep a query's
  result as a table. Everything runs as its owner, and a scaled-to-zero
  Igloo is started when work is due.

  ```sql
  COPY INTO sales FROM 's3://drops/sales/' (FORMAT csv, PATTERN '.*[.]csv[.]gz');
  CREATE TASK nightly SCHEDULE '0 6 * * *' TIMEZONE 'America/New_York' AS
    CREATE OR REPLACE TABLE report AS SELECT day, sum(spend) FROM sales GROUP BY day;
  ```
- **[Media analytics](docs/media-analytics.md)**: HLL and theta sketches
  that merge into deduplicated reach and audience overlap across campaigns,
  platforms and days; geometry and H3 functions for DMA, zip code and
  store-radius reporting; DuckDB's approximate, statistical and time zone
  functions.
- **[Other engines](docs/external-engines.md)** through an Iceberg REST
  catalog on port 8181: Spark, Trino, PyIceberg, Databricks and Snowflake
  sign in as Igloo users, Igloo's grants decide what they may read and change,
  their requests go to the query history, and they get storage credentials
  that last an hour and reach one table's files only.
- **[Workload management](docs/workload-management.md)**: query tags
  (`SET query_tag = 'client:acme'`), per-client usage and estimated cost in
  `igloo.usage`, per-role concurrency, timeout and memory limits
  (`ALTER ROLE analysts SET max_concurrent_statements = 4`), and compute
  pools that give a client or team compute and a bill of its own.
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
- **[Read cache](docs/operations.md#read-cache)**: table files read from
  object storage are kept on local disk across restarts, so the first
  queries after a start (or a scale-to-zero resume) do not fetch them again.
- **[Web console](docs/console.md)** at `http://127.0.0.1:8443`: SQL
  worksheets, a data browser, query history, tasks and loads, and user and
  compute administration, signed in with the same users as SQL clients.
- **[Arrow Flight SQL](docs/flight-sql.md)** at `grpc://localhost:31337`:
  query results as DuckDB's Arrow record batches for ADBC (pandas, polars),
  pyarrow and the Flight SQL JDBC driver, with the same users, grants,
  timeouts and history as SQL clients.
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
 psql / JDBC / BI tool          pandas / polars / Spark (ADBC), Flight JDBC
          │  PostgreSQL wire protocol      │  Arrow Flight SQL
          │  (SCRAM, optional TLS)         │  (TLS, or localhost only)
          │            ┌───────────────────┘
          ▼            ▼
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

- **igloo** (this repository, Go) accepts Postgres and Arrow Flight SQL
  connections, checks each statement against the user's role and grants,
  and runs it on an embedded DuckDB attached to the catalog. On start it
  sets up the catalog and warehouse itself, so every deployment works the
  same way.
- **[Lakekeeper](https://github.com/lakekeeper/lakekeeper)** is the Iceberg
  REST catalog: it tracks tables, schemas and snapshots. Only Igloo talks to
  it; other engines use Igloo's own Iceberg REST endpoint (port 8181), which
  checks their requests against the same users and grants and passes them
  on.
- **PostgreSQL** stores the catalog's metadata and Igloo's users.
- **Object storage** holds the table data as Parquet files.

Every component uses a permissive open-source license (Apache 2.0, MIT or the
PostgreSQL License), except GEOS, under the LGPL 2.1, inside DuckDB's spatial
extension (see [media analytics](docs/media-analytics.md#security)).

## Documentation

- [Product principles](docs/principles.md): what Igloo optimizes for, in order
- [Configuration](docs/configuration.md): every setting
- [Web console](docs/console.md): worksheets, data browser, history and
  administration in the browser, and their security
- [Arrow Flight SQL](docs/flight-sql.md): fast columnar results for ADBC,
  pandas, polars and JDBC, and their security
- [Single sign-on](docs/sso.md): identity providers, access tokens for SQL
  clients, service accounts
- [Operations](docs/operations.md): sizing, scaling, users, monitoring,
  backups, table maintenance, upgrades, troubleshooting
- [Scale to zero](docs/scale-to-zero.md): stop compute when idle and start it
  on the next connection
- [Other engines](docs/external-engines.md): Spark, Trino, PyIceberg,
  Databricks and Snowflake on Igloo's tables, through its Iceberg REST
  catalog
- [SQL and clients](docs/sql.md): the SQL dialect, Iceberg features, type
  mapping, tested drivers and tools
- [Loading and scheduling](docs/loading.md): `COPY INTO`, pipes, tasks and
  materialized views
- [Masking, row access policies and tags](docs/security-policies.md): what
  each user sees of tables' rows and columns
- [Media analytics](docs/media-analytics.md): reach and frequency with
  sketches, geospatial reporting, approximate and statistical functions
- [Workload management](docs/workload-management.md): query tags, usage and
  cost per client, role limits, compute pools
- [Delta Sharing](docs/sharing.md): sharing tables with outside recipients,
  reading partners' shares, and their security
- [Benchmark](bench/README.md): price-performance against Snowflake and
  Databricks on the same data and queries
- [Security](SECURITY.md): the security model and how to report a
  vulnerability
- [Contributing](CONTRIBUTING.md)

## Limitations in 1.0

- **One engine per query.** Each query runs on one Igloo replica. Scale up
  with bigger machines and out with more replicas; very large queries that
  need a cluster are not supported yet.
- **Grants stop at tables.** Views run with the querying user's privileges,
  and there are no column privileges: use masking policies instead. Table
  and column names are visible to every user. Policies do not bind admins,
  who manage them, nor engines that read the Iceberg tables directly.
- **Scale to zero saves node costs on one machine, not yet in the cloud
  modules.** The AWS, Google Cloud and Azure modules run Igloo on their
  general node pool, so scaling its pods to zero does not release nodes
  ([details](docs/scale-to-zero.md#on-kubernetes)).
- **Keys are not enforced.** `PRIMARY KEY` and `UNIQUE` on Iceberg tables are
  accepted but not checked, so duplicates are possible. `NOT NULL` is
  enforced.
- **Other engines get storage credentials on S3 only.** On Google Cloud
  Storage and Azure, engines using the Iceberg REST catalog need their own
  access to the files ([details](docs/external-engines.md#storage-credentials)).

## Repository layout

```
cmd/igloo/          server entry point and subcommands
internal/audit/     query and login history
internal/auth/      users, roles, grants, SCRAM verifiers, access tokens
internal/console/   web console
internal/scim/      SCIM 2.0 provisioning of users and groups
internal/sso/       single sign-on with OpenID Connect
internal/catalog/   catalog and warehouse setup
internal/config/    environment-variable configuration
internal/engine/    embedded DuckDB, storage, sandbox
internal/flightsql/ Arrow Flight SQL endpoint
internal/icebergrest/ Iceberg REST catalog for other engines
internal/pgwire/    Postgres wire protocol, authorization, policies, compatibility
internal/policy/    masking and row access policies and tags, stored
internal/schedule/  cron and interval schedules
internal/sharing/   Delta Sharing: the provider endpoint and external tables
internal/tasks/     tasks, pipes, materialized views, load history, scheduler
internal/workload/  query tags, role limits and their enforcement
deploy/             Docker Compose, Helm chart, AWS/GCP/Azure modules
docs/               documentation
scripts/            test and end-to-end scripts
```
