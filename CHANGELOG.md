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
- Arrow Flight SQL on port 31337 (`IGLOO_FLIGHT_ADDR`): queries, updates,
  prepared statements with parameters, catalog metadata and cancellation,
  with results streamed as DuckDB's own Arrow record batches, for the ADBC
  driver (pandas, polars), pyarrow and the Flight SQL JDBC driver. Pulling 3
  million TPC-H lineitem rows into Python is 29 times faster than with
  psycopg as Arrow, and 14 times faster as a pandas DataFrame
  (`bench/extract.py`). Users sign in with their Igloo password or an access
  token, sent in the handshake or as each call's bearer token; statements
  go through the same authorization, statement timeout and query history as
  SQL clients. Without TLS it serves only clients at localhost. The Compose
  stack publishes it on loopback, the Helm chart on the SQL Service
  (`flightSQL.enabled`), and the scale-to-zero gateway forwards it
  (`IGLOO_GATEWAY_FLIGHT_BACKEND`).
- Media analytics functions, from DuckDB extensions baked into the image:
  HLL and theta sketches (datasketches) that can be stored in tables and
  merged into deduplicated reach and audience overlaps across campaigns,
  platforms and days; geometries, points in polygons and distances
  (spatial), with geometries sent to clients as WKT; and the H3 grid (h3).
  See docs/media-analytics.md for reach, overlap and frequency examples and
  DuckDB's approximate, statistical and time zone functions.
- Connections to PostgreSQL and MySQL databases (`CREATE`, `ALTER`, `DROP`
  and `SHOW CONNECTIONS`): each replica attaches them read-only as catalogs
  of their own, such as `crm.public.customers`, which queries join with
  Iceberg tables, with filters and columns pushed down. Credentials are
  stored encrypted or read from files of `IGLOO_CONNECTION_SECRETS_DIR`,
  never shown again, and rotate without a restart; a new connection is
  attached when each replica next starts. A database that is down or stops
  answering fails only the queries that read it. Connections verify their
  server's certificate and host name by default (`tls 'verify-full'`);
  the modes that do not are explicit choices that draw a warning, and
  `require` checks the certificate's chain.

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
- Single sign-on to the web console with OpenID Connect (Okta, Microsoft
  Entra ID, Google Workspace, Keycloak, others): the authorization code flow
  with PKCE, verified ID tokens, allowed domains and groups
  (`IGLOO_OIDC_*`), users created at first sign-in with role `none` and
  matched by subject, verified email or name, and the provider's groups
  mapped to custom and built-in roles at each sign-in
  (`GRANT role TO SSO GROUP 'name'`, `SHOW SSO GROUPS`).
  `IGLOO_OIDC_PASSWORD_SIGNIN=false` makes linked users use single sign-on.
- SCIM 2.0 provisioning from Okta and Microsoft Entra ID at `/scim/v2` on
  the console's port, with an admin's access token: users created, updated
  and deactivated (disabled at once) as they change at the provider, and
  groups and members kept, with their role mappings applied at each change.
- Access tokens for SQL clients: `CREATE TOKEN`, `SHOW TOKENS`, `DROP TOKEN`,
  sent as the password, checked with SCRAM-SHA-256 so they never cross the
  network, stored as hashes, always expiring (`IGLOO_TOKEN_MAX_LIFETIME`),
  with their last use and every login recorded. Optionally the identity
  provider's own access tokens, inside TLS
  (`IGLOO_OIDC_ACCESS_TOKEN_AUDIENCE`). `auth.TokenChecker` verifies tokens
  for HTTP APIs.
- Service accounts (`CREATE USER ... TYPE SERVICE`), owned by an admin,
  without a password, signing in with tokens only. Users can be disabled
  (`ALTER USER ... NOLOGIN`), which ends their sessions, and given an email
  (`EMAIL`) for single sign-on.
- Limits on failed logins: 10 per user and 50 per address in 15 minutes.
- Masking policies, row access policies and tags, after Snowflake's:
  `CREATE [OR REPLACE] MASKING POLICY`, `CREATE [OR REPLACE] ROW ACCESS
  POLICY`, `ALTER TABLE ... SET | UNSET MASKING POLICY`, `ADD | DROP ROW ACCESS
  POLICY`, `CREATE TAG`, `SET | UNSET TAG`, tag-based masking with `ALTER TAG
  ... SET MASKING POLICY`, `SHOW POLICIES`, `SHOW TAGS`, and the views
  `igloo.policy_references` and `igloo.tag_references`. Policy expressions can
  call `current_user`, `is_role_in_session(role)`, `current_available_roles()`
  and `tag_value(tag)`, and read mapping tables. Policies apply to every user
  and statement (queries, views, joins, subqueries, `INSERT ... SELECT`,
  `CREATE TABLE AS`, `COPY`, `UPDATE`, `DELETE`, `MERGE`, cursors, time
  travel, the web console, and tasks, pipes and materialized views, as
  their owners), by rewriting statements to read protected tables
  through them; statements that cannot be rewritten with certainty are
  refused. The query history records which policies applied, and the
  console's data browser shows policies and tags. Igloo's checks of
  statements now call DuckDB's built-in functions by their qualified names,
  so a session's temporary macros cannot change what grants or policies
  find.
- Confined the files users other than admins may name, for every statement
  whether or not a table has a policy: a file must be a plain string literal
  (not a value built as the statement runs) that cannot climb out of an
  allowed prefix (no `.`/`..` segments, percent-encoding, backslashes, `?`,
  `#` or empty segments), in file-reading functions (`read_parquet`,
  `read_csv`, `glob`, `iceberg_scan`, `delta_scan`, `read_xlsx`, `ST_ReadSHP`,
  `ST_ReadOSM` and the like), `COPY FROM`/`TO`, `COPY INTO`, pipes and
  replacement scans. The check runs before DuckDB opens the file. This closes
  a path-traversal gap: DuckDB 1.5.6 prefix-matches a remote path against
  `allowed_directories` without canonicalising it, so
  `https://host/allowed/../other` or an `s3://` path with a `?s3_endpoint=`
  query would otherwise reach elsewhere. `IGLOO_ALLOWED_LOCATIONS` entries are
  validated and given a trailing slash at startup so a prefix cannot match a
  sibling. Without a policy, readers and writers may still read the
  warehouse's own files; admins are unchanged.
- Query and login history: every statement and login attempt, with its
  outcome, duration, rows and the tables it used, in `igloo.query_history`
  and `igloo.login_history`, which records how each login signed in
  (`auth_method`) and with which token (`token_name`). Admins see everyone's history, other users
  their own. Kept in the metadata database for `IGLOO_AUDIT_RETENTION` (90
  days); `IGLOO_AUDIT=false` turns it off. Passwords are never recorded.
- A query sandbox: SQL cannot read the server's files or reach locations
  outside the warehouse and `IGLOO_ALLOWED_LOCATIONS`, nor run SQL built
  from strings (`query()`, `json_execute_serialized_sql()`), nor use the
  spatial extension's GDAL readers and writers (`ST_Read`, `ST_Read_Meta`,
  `FROM '*.gpkg'`, `COPY ... (FORMAT GDAL)`), which open files a dataset
  names outside the sandbox. `ST_ReadSHP` and `ST_ReadOSM` stay confined to
  the warehouse and allowed locations.
- Grants on connections to other databases (`GRANT SELECT ON CONNECTION`,
  `ON SCHEMA conn.schema`, `ON TABLE conn.schema.table`), required for
  every role but admin, over SQL clients, Arrow Flight SQL and the web
  console alike. Every statement's reads of connections are checked
  against DuckDB's plan, again whenever connections, views or grants
  change, and recorded in the query history. Creating a view of a
  connection's table needs a grant on it. Tables whose names differ only
  in case from another's in the same connection are refused. Remote
  sessions are read-only, and the functions that run SQL on remote
  databases (`postgres_query`, `mysql_query` and the like) are refused for
  everyone.
- Masking and row access policies, and tags, on connections' tables, as on
  Iceberg tables (`ALTER TABLE crm.public.customers ALTER COLUMN email SET
  MASKING POLICY ...`).
- TLS for SQL clients, the web console and Arrow Flight SQL.

### Data sharing

- Delta Sharing as a provider: admins share Iceberg tables with outside
  recipients (`CREATE SHARE`, `ALTER SHARE ... ADD TABLE [AS alias] [WITH
  HISTORY]`, `CREATE RECIPIENT`, `GRANT SELECT ON SHARE ... TO RECIPIENT`),
  who read them with any Delta Sharing client (Databricks and Spark, pandas,
  Power BI, Tableau) from a new endpoint on port 8444
  (`IGLOO_SHARING_ADDR`, `IGLOO_SHARING_URL`), over TLS or at `localhost`.
  Recipients get a one-time activation link to their profile file; tokens
  are stored hashed, expire and can be rotated. Data files download from
  hour-long URLs that Igloo signs and serves itself, so recipients never see
  storage credentials, and revoking access ends downloads at once. Rows
  deleted from a table are never served: data files with deletes are served
  as copies with them applied. Every recipient request is in the query
  history. See [docs/sharing.md](docs/sharing.md).
- Delta Sharing as a consumer: `CREATE PROVIDER` registers a partner's
  profile (its token stored encrypted), and `CREATE EXTERNAL TABLE ... FROM
  SHARE provider.share.schema.table` makes a shared table queryable, and
  grantable, like any table. Queries read the partner's current version
  through a proxy that is the only address the sandbox gains.
- Delta Sharing and policies: tables with masking or row access policies
  cannot be shared, and a shared table that gains one is refused to
  recipients, downloads of files listed before included, with each refusal
  and its policies in the query history. Policies and tags do not attach
  to external tables, and while tables have policies only admins read
  external tables' files or the copies of shared files directly.

### Web console

- A web console on port 8443 (`IGLOO_CONSOLE_ADDR`): SQL worksheets with
  cancel and CSV download, a data browser, query history, tasks, pipes and
  materialized views with their runs, the load history, and admin pages
  for users, roles and grants, and the replica's sessions. Users sign in
  with their Igloo password or single sign-on; statements run as them
  through the same authorization as SQL clients. Sessions are stateless
  sealed cookies, so the console works behind a load balancer, and end when
  the user is disabled or their password changes.
- Console pages for access tokens (everyone's own), service accounts and
  every token (admins), and single sign-on: settings, whether the provider
  answers, group mappings and who signed in with which groups.

### Other engines

- An Iceberg REST catalog on port 8181 (`IGLOO_ICEBERG_ADDR`) through which
  Spark, Trino, PyIceberg, Databricks and Snowflake read and write Igloo's
  tables. Engines sign in as Igloo users with the OAuth2 client-credentials
  flow, with a password or an access token (so service accounts can), and
  get Igloo's own tokens, which end with the access token they came from;
  every request is checked against the user's role and grants and recorded
  in the query history, and token requests in the login history. Tables
  with masking or row access policies are refused to everyone but admins,
  since engines would read their files unmasked. Lakekeeper stays private.
  Tested with
  Spark 3.5 and Iceberg 1.11 (reads, `INSERT`, `MERGE INTO`,
  `CREATE TABLE AS`), Trino 483 (reads, `INSERT`, `UPDATE`, `DELETE`) and
  PyIceberg 0.12 (`scripts/engines-e2e.sh`, `scripts/e2e.sh`).
- Storage credentials for engines that last an hour and reach one table's
  files only, read-only unless the user has `INSERT`, `UPDATE` and `DELETE`
  on the table: STS with a session policy, on AWS through
  `IGLOO_S3_STS_ROLE_ARN` (created by the AWS module), on S3-compatible stores
  through their own STS after checking that they enforce the policy. None on
  Google Cloud Storage and Azure yet.
- The Compose stack publishes the endpoint on loopback, the Helm chart on the
  SQL Service (`iceberg.enabled`, with NetworkPolicy), and the scale-to-zero
  gateway forwards it (`IGLOO_GATEWAY_ICEBERG_BACKEND`).

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
- Workload management and chargeback (docs/workload-management.md): query
  tags (`SET query_tag`, and defaults per user or role), recorded with each
  statement's compute pool, CPU time, bytes read, memory allocated and time
  waiting in `igloo.query_history`; `igloo.usage`, the history summed by
  day, pool, user, role and tag with an estimated compute cost from each
  replica's running time and `IGLOO_PRICE_PER_HOUR`; and
  per-user and per-role limits (`ALTER ROLE ... SET statement_timeout`,
  `max_concurrent_statements`, `memory_limit`) under which statements wait
  in line, in arrival order, on each replica.
- Compute pools ("virtual warehouses") that share the catalog, users and
  history but run on their own machines and scale to zero on their own:
  `pools` in the Helm chart (a Deployment, Services and optional gateway per
  pool), and `deploy/single-vm/compute-pool.yml` for Docker Compose.
  `IGLOO_POOL` names a replica's pool in the history and usage.

[Unreleased]: https://github.com/akkio-inc/Igloo/commits/main
