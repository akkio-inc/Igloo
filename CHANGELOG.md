# Changelog

All notable changes to Igloo are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and Igloo uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The release workflow publishes the section whose heading matches the tag
(`## [1.2.3] - YYYY-MM-DD` for `v1.2.3`) as that GitHub Release's notes.

## [Unreleased]

First release. Move this section under `## [1.0.0] - YYYY-MM-DD` when tagging.

### SQL and tables

- SQL endpoint that speaks the PostgreSQL wire protocol (simple and extended
  query protocols, prepared statements, transactions, query cancel), so
  `psql`, JDBC, psycopg, pgx and BI tools connect unchanged. Compatibility
  with the PostgreSQL catalog queries of `psql` meta-commands, JDBC metadata
  calls and schema browsers.
- Apache Iceberg tables through the Lakekeeper REST catalog, on Amazon S3 and
  S3-compatible stores, Google Cloud Storage and Azure Data Lake Storage Gen2:
  `CREATE [OR REPLACE] TABLE`, `CREATE TABLE AS`, `INSERT`, `UPDATE`,
  `DELETE`, `MERGE`, schemas and time travel. Igloo bootstraps the catalog
  and creates its warehouse on start.
- Views on the Iceberg catalog (`CREATE [OR REPLACE] VIEW`, `DROP VIEW`),
  stored as Iceberg views and kept in sync across replicas. Replacing a view
  may change its columns and the tables it reads.
- `COMMENT ON TABLE`, `VIEW` and `COLUMN`, stored as Iceberg metadata and
  shown through `obj_description`, `col_description`, `pg_description` and
  `igloo_comments()`.
- SQL cursors (`DECLARE`, `FETCH`, `CLOSE`) and array parameters, so
  psycopg's server-side cursors and Ibis's PostgreSQL backend work.

### Loading and scheduling

- `COPY INTO table FROM 'location'` loads CSV, TSV, Parquet, JSON and
  NDJSON files, gzip- or zstd-compressed or not (detected from the name or
  the file), from the warehouse and `IGLOO_ALLOWED_LOCATIONS`: a prefix, a
  glob or one file, filtered by `PATTERN`. Columns match by name or
  position; `CREATE_TABLE` creates the table from the files;
  `ON_ERROR abort | skip_file | continue` stops, skips bad files, or skips
  and counts bad rows. The files of one load commit together.
- A load history (`igloo.load_history`) records each file loaded into each
  table, by path, size and time, so a file is loaded once unless it changes
  or `FORCE` is given; a load cut short by a crash is marked `unknown`
  rather than repeated.
- Pipes (`CREATE PIPE ... AS COPY INTO ...`) load new files every minute or
  on a schedule. Event notifications are not supported yet.
- Tasks (`CREATE TASK ... SCHEDULE ... AS statement`, `ALTER TASK ...
  RESUME | SUSPEND | SET SCHEDULE`, `EXECUTE TASK`, `DROP TASK`,
  `SHOW TASKS`) run one statement on an interval or cron schedule in any
  time zone, on one replica at a time, never overlapping, with their runs
  in `igloo.task_history`.
- Materialized views (`CREATE [OR REPLACE] MATERIALIZED VIEW ... [SCHEDULE]
  AS query`, `REFRESH MATERIALIZED VIEW`) keep a query's result as an
  Iceberg table, refreshed in one transaction on demand or on a schedule.
- Tasks, pipes and refreshes run as their owner, with the owner's role and
  grants at the time they run; scheduling needs the writer role.
- With scale to zero, Igloo reports when work is next due on
  `GET /activity`, and the gateway starts the compute for it.
- Several replicas can serve the same tables; conflicting concurrent writes
  are retried automatically.

### Security and governance

- Users with SCRAM-SHA-256 passwords, managed in SQL (`CREATE USER`,
  `ALTER USER`, `DROP USER`, `SHOW USERS`) and stored in PostgreSQL so every
  replica shares them. Built-in roles `admin`, `writer`, `reader` and `none`.
- Custom roles and grants: `CREATE ROLE`, `DROP ROLE`, `GRANT role TO user`,
  and `GRANT` and `REVOKE` of `SELECT`, `INSERT`, `UPDATE`, `DELETE` and
  `CREATE` on tables, views, schemas and `ALL TABLES IN SCHEMA`, to users or
  roles; `SHOW ROLES` and `SHOW GRANTS`. The role `none` allows only what is
  granted. Grants on a schema cover tables created later; views run with the
  querying user's privileges.
- Query and login history: every statement and login attempt, with its
  outcome, duration, rows and the tables it used, in `igloo.query_history`
  and `igloo.login_history`. Admins see everyone's history, other users
  their own. Kept in the metadata database for `IGLOO_AUDIT_RETENTION` (90
  days); `IGLOO_AUDIT=false` turns it off. Passwords are never recorded.
- A query sandbox: SQL cannot read the server's files or reach locations
  outside the warehouse and `IGLOO_ALLOWED_LOCATIONS`, nor run SQL built
  from strings (`query()`, `json_execute_serialized_sql()`).
- TLS for SQL clients and the web console.

### Web console

- A web console on port 8443 (`IGLOO_CONSOLE_ADDR`): SQL worksheets with
  cancel and CSV download, a data browser, query history, tasks, pipes and
  materialized views with their runs, the load history, and admin pages
  for users, roles and grants, and the replica's sessions. Users sign in
  with their Igloo password; statements run as them through the same
  authorization as SQL clients. Sessions are stateless sealed cookies, so
  the console works behind a load balancer.

### Performance and cost

- Scale to zero: `igloo gateway` accepts client connections in Igloo's
  place, starts Igloo's EC2 instances, Kubernetes Deployment or (through
  commands) any VM when a client connects, and stops them after
  `IGLOO_GATEWAY_SUSPEND_AFTER` without traffic or running work. It can
  forward the web console too (`IGLOO_GATEWAY_CONSOLE_BACKEND`). Igloo
  reports running statements and maintenance on `GET /activity`.
- A read cache on local disk (DuckDB's `cache_httpfs` extension) for the
  warehouse's files on Amazon S3, S3-compatible stores and Google Cloud
  Storage. It survives restarts and machine stops, so the first queries
  after a start read local disk instead of object storage. It sizes itself
  from the disk's free space; `IGLOO_CACHE_SIZE` overrides that or turns it
  off. Only admins may use its functions and settings from SQL, and where it
  lives and what it caches cannot be changed from SQL.
- Automatic table maintenance: Igloo compacts small files and applies
  deletes, expires snapshots older than `IGLOO_SNAPSHOT_RETENTION` (7 days by
  default, overridable per table with `history.expire.max-snapshot-age-ms`),
  and deletes unreferenced files. Compacted files use DuckDB's own Parquet
  layout, so compaction makes scans faster, not slower. Replicas take turns
  through a lock in the metadata database, and record passes there so a
  pass that is due runs soon after a start.

### Operations and deployment

- Statement timeouts, connection limits, health and readiness endpoints,
  Prometheus metrics, and text or JSON logs.
- Docker Compose for one machine, with the bundled object store or Amazon S3
  through the machine's IAM role.
- A Helm chart for any Kubernetes cluster, with the web console and
  `scaleToZero.enabled` (the gateway in front of Igloo's Deployment, with the
  RBAC and network policies it needs).
- OpenTofu modules that deploy the whole stack on AWS (EKS, S3, RDS), Google
  Cloud (GKE, GCS, Cloud SQL) and Azure (AKS, ADLS Gen2, PostgreSQL Flexible
  Server), each able to pull Igloo from a mirror or private registry.
- A price-performance benchmark (`bench/`) that runs the same TPC-H queries
  on Igloo, Snowflake and Databricks and prices them by each one's billing.

[Unreleased]: https://github.com/akkio-inc/Igloo/commits/main
