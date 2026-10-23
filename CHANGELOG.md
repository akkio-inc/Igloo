# Changelog

All notable changes to Igloo are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and Igloo uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The release workflow publishes the section whose heading matches the tag
(`## [1.2.3] - YYYY-MM-DD` for `v1.2.3`) as that GitHub Release's notes.

## [Unreleased]

First release. Move this section under `## [1.0.0] - YYYY-MM-DD` when tagging.

### SQL and tables

- A prepared `CREATE TABLE ... AS` a client runs again makes the table it
  says. DuckDB 1.5.6 runs a prepared CREATE as written only the first time,
  and from the second on makes the table with no columns, so that the next
  statement to read it fails with "the FROM clause is missing". Clients run
  a prepared statement again whenever they reuse one: psycopg does with any
  statement it has run five times, and so do JDBC drivers. Igloo prepares a
  CREATE statement afresh each time it runs again.
- A schema created after a statement looked for it can be used. DuckDB's
  Iceberg catalog remembers a schema as missing once a lookup of a table in
  it found neither, until it restarts, in every session and whoever creates
  the schema afterwards: `SELECT * FROM lake.staging.t`, then `CREATE SCHEMA
  lake.staging`, left `staging` missing on that replica for good. When a
  statement fails for want of a schema the catalog has, Igloo now makes
  DuckDB forget it and prepares the statement again.
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
  may change its columns and the tables it reads. An unqualified name in a
  view is the view's own schema's table or view, as in Snowflake, whatever
  the search path of the session that created or reads it; a view a replica
  cannot load fails with the reason when read.
- `COMMENT ON TABLE`, `VIEW` and `COLUMN`, stored as Iceberg metadata and
  shown through `obj_description`, `col_description`, `pg_description`,
  `igloo_comments()`, and `information_schema.tables` and `.columns`.
- SQL cursors (`DECLARE`, `FETCH`, `CLOSE`) and array parameters, so
  psycopg's server-side cursors and Ibis's PostgreSQL backend work.
- Semi-structured columns. Lists, maps and structs are Iceberg's own
  `list`, `map` and `struct`, which Spark, Trino and PyIceberg read. A `JSON`
  or `VARIANT` column, which Iceberg tables of format version 1 and 2 cannot
  hold ("Type JSON is not a valid Iceberg Type"), is a `VARCHAR` column of
  its values' JSON text, in `CREATE TABLE`, `ALTER TABLE ... ADD COLUMN` and
  `CREATE TABLE ... AS` alike; JSON inside a list, map or struct is a string
  of its JSON text. In Snowflake's dialect an `ARRAY` column is a list of
  strings (`VARCHAR[]`) and an `OBJECT` column a map of strings
  (`MAP(VARCHAR, VARCHAR)`), each element as Snowflake's `TO_VARCHAR`
  writes it, as Snowflake converts them to `ARRAY(VARCHAR)` and
  `MAP(VARCHAR, VARCHAR)`; structured `ARRAY(type)`, `OBJECT(name type,
  ...)` and `MAP(type, type)` are lists, structs and maps of their types,
  and `CREATE TABLE ... AS` makes `ARRAY` and `OBJECT` columns of
  `ARRAY_AGG`, `OBJECT_CONSTRUCT` and the like.
  Paths, `FLATTEN`, `GET`, `TO_JSON`, `OBJECT_CONSTRUCT`, casts and the
  other semi-structured functions read them as Snowflake's values, objects
  inside arrays included, and read JSON text as before; masking policies
  written in Snowflake's dialect may return `OBJECT_CONSTRUCT(...)` for an
  `OBJECT` column.
- Zero-copy clones, with Snowflake's syntax: `CREATE [OR REPLACE] TABLE
  ... CLONE source [AT (TIMESTAMP | SNAPSHOT | BRANCH | TAG => ...)]` and
  `CREATE [OR REPLACE] SCHEMA ... CLONE source`, its tables and views. A
  clone is a new Iceberg table whose metadata reads its source's data and
  delete files, so it takes moments and no storage, however large the table;
  the two then change independently. Cloning a table needs `SELECT` on it
  and `CREATE` on the clone's schema. A clone carries its source's masking
  and row access policies and tags, gets no grants but a schema clone's
  schema-wide ones, and is recorded in the query history with its source.
  Table maintenance never deletes a file a clone still reads, and through
  the Iceberg REST catalog other engines cannot either: a table whose files
  clones read is not purged (409, naming the clones), write credentials
  for it delete nothing, and clones get no storage credentials.
- Iceberg branches and tags: `ALTER TABLE ... CREATE [OR REPLACE] BRANCH |
  TAG ... [AT (...)]`, `DROP BRANCH`, `DROP TAG`, `SHOW BRANCHES ON`,
  reading them with `AT (BRANCH => ...)` and `AT (TAG => ...)`, and `ALTER
  TABLE ... FAST FORWARD main TO branch`. Branches are read-only in Igloo,
  as DuckDB writes only to `main`; tags keep their snapshots from expiring.
  Table maintenance keeps branches and tags until they are dropped, or as
  long as their own or the table's `history.expire.max-ref-age-ms` says, as
  Java's Iceberg does: iceberg-go dropped every one older than the snapshot
  retention, seven days by default, a clone's too. A max-ref-age-ms of
  Java's `Long.MAX_VALUE` reads as forever.
- Snowflake's SQL dialect, opt-in per session (`SET sql_dialect =
  'snowflake'` or `ALTER SESSION SET`), per connection (the startup option
  `-c sql_dialect=snowflake`) or per user and role (`ALTER USER | ROLE ...
  SET sql_dialect`). Igloo translates each statement to DuckDB's dialect
  before it checks and runs it, so the checks of grants and policies see
  only the DuckDB statement: Snowflake's string escapes, `$$` strings and
  `//` comments, session variables (`SET x = ...`, `$x`), `IDENTIFIER()`,
  semi-structured paths (`v:a.b[0]::string`), `FLATTEN`, `SPLIT_TO_TABLE`,
  `GENERATOR`, `QUALIFY`, `MINUS`, `TOP`, `SAMPLE`, `PIVOT`, `ASOF JOIN ...
  MATCH_CONDITION`, `LIKE ANY`, Snowflake's types, `CREATE OR REPLACE
  TRANSIENT TABLE ... CLUSTER BY` (ignored with a notice), `ALTER SESSION`
  parameters, NULL ordering, result column names in upper case, and some
  200 functions (`IFF`, `NVL`, `DECODE`, `DATEADD`, `DATEDIFF`,
  `DATE_TRUNC` keeping dates dates, `TO_CHAR` and `TO_DATE` with
  Snowflake's formats, `TRY_` conversions, `LISTAGG`, `ARRAY_AGG`,
  `OBJECT_CONSTRUCT`, `PARSE_JSON`, the `REGEXP_` family, `TIME_SLICE` and
  more). Constructs it cannot translate faithfully are refused with
  SQLSTATE `0A000` and a hint; docs/snowflake-sql.md lists what is
  translated, the known differences and what is refused.

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
  grants at the time they run; scheduling needs the writer role. When a
  user is dropped, their tasks, pipes, materialized views and alerts are
  suspended and marked as a dropped user's, so a user created later with
  the same name does not inherit them.
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
  DuckDB's approximate, statistical and time zone functions. With spatial
  loaded, DuckDB 1.5.6 cannot plan some filters on a temporary table's
  `rowid`, such as `rowid % 3 = 0`; they fail with SQLSTATE 0A000 and what
  to write instead, not DuckDB's internal error and stack trace.
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
- Erasure requests rewrite a table's files within `IGLOO_MAINTENANCE_MEMORY`,
  one file at a time, as compaction does, instead of reading them all at
  once in full-size batches; a request whose rows do not fit stops with the
  reason rather than running Igloo out of memory.
- A connection whose database cannot be reached when a replica starts (no
  route, refused, timed out, name not resolved), as when the machine's
  network is not up yet after a restart, is tried again for up to 30
  seconds before it is left out until the next restart.
- Connections to Snowflake (`CREATE CONNECTION ... TYPE snowflake`), signed
  in with a key pair (`private_key_secret`, `private_key`) or a password,
  with a warehouse and role. Igloo reads them through a gateway of its own,
  with Snowflake's Go driver: tables are listed with `SHOW` commands, at no
  warehouse cost, and each scan becomes a Snowflake `SELECT` of the columns
  it needs, with its filters and `LIMIT`, whose Arrow result streams into
  DuckDB as it downloads, so `CREATE TABLE AS` copies large tables into
  Iceberg without holding them in memory. Filters compare strings byte by
  byte, as DuckDB does, whatever a column's Snowflake collation, so row
  access policies hold. `count(*)` is counted in Snowflake.
  `query_timeout` stops queries on Snowflake too. Grants,
  policies, the query history and `ALTER CONNECTION ... REFRESH` work as for
  other connections; credentials stay in Igloo, encrypted or in files. An
  `ARRAY` column reads as a `VARCHAR[]`, so `CREATE TABLE AS SELECT *` copies
  it into an Iceberg `list<string>`; `VARIANT` and `OBJECT` read as their JSON
  text.
- Connections to other Iceberg REST catalogs (`CREATE CONNECTION ... TYPE
  iceberg`): Snowflake-managed Iceberg tables through Snowflake's Horizon
  Catalog, Databricks Delta Shares served to Iceberg clients and Unity
  Catalog's UniForm tables, Apache Polaris, AWS Glue and S3 Tables, other
  Lakekeepers, read in place, read-only, with the same grants, policies and
  history as other connections. Igloo signs in with a bearer token, an
  OAuth2 client (programmatic access tokens included) or AWS Signature
  Version 4, with credentials stored like passwords, and reads files with
  the storage credentials the catalog hands out, or a static one. DuckDB
  never sees the catalog's credentials, reaches no other endpoint than
  Igloo's proxy, and may read only the locations of the tables found when
  the replica started, which must not overlap the warehouse or the allowed
  locations. Igloo's own storage credentials now apply only to the
  warehouse and the allowed locations.

### Alerts and notifications

- Notification integrations (`CREATE [OR REPLACE] NOTIFICATION INTEGRATION
  ... TYPE webhook | slack | teams | email`, `ALTER`, `DROP`,
  `SHOW INTEGRATIONS`), created by admins, with `GRANT USAGE ON INTEGRATION`
  to roles and users. Webhooks take `HEADERS` and a signing `SECRET`
  (HMAC-SHA256 of the timestamp and body); Slack and Microsoft Teams
  incoming webhooks get their own payloads; email goes through
  `IGLOO_SMTP_HOST` to `IGLOO_SMTP_ALLOWED_DOMAINS` only. URLs, headers and
  secrets are stored encrypted, never shown, and scrubbed from errors,
  logs and the query history.
- Alerts, with Snowflake's syntax: `CREATE [OR REPLACE] ALERT ... SCHEDULE
  ... IF (EXISTS (query)) THEN NOTIFY integration [MESSAGE '...'] [WITH
  RESULT]`, `ALTER ALERT ... RESUME | SUSPEND | SET SCHEDULE`,
  `EXECUTE ALERT`, `DROP ALERT`, `SHOW ALERTS`. They are scheduled work like
  tasks: run as their owner with the owner's grants and policies, skipped
  while the owner is disabled, at most once a minute, and the
  scale-to-zero gateway starts Igloo for them. A user owns at most 50.
  Messages take `{{column}}` values from the first row, up to 4 KiB;
  results are capped at 20 rows, 30 columns and 16 KiB, and values from
  data are escaped so they cannot mention people or make links in Slack or
  Teams. Each integration sends from an outbox of its own, so a slow
  destination holds up neither the scheduler nor other integrations.
- `igloo.alert_history`: every alert run, built-in alert and message, with
  its state (`condition_false`, `triggered`, `condition_failed`,
  `action_failed`, `skipped`), integration, attempts and error. Admins see
  everything, others their own.
- `CALL SYSTEM$SEND_NOTIFICATION('integration', 'message')`, from tasks or
  any session, with `USAGE` on the integration.
- Built-in operational alerts admins switch on per integration
  (`OPERATIONAL_ALERTS`): failed scheduled runs, `COPY INTO` and pipe load
  errors, pipes with no new files for N hours, statements running longer
  than N minutes, failed-login bursts from the login limiter (for
  addresses and existing users, at most 5 in 15 minutes from a replica,
  never at the other alerts' expense), and table maintenance failures, each
  sent at most once per problem in a while, and tried again 5 minutes after
  a failed send.
- Notifications reach only public addresses (no loopback, private,
  link-local or metadata, multicast or reserved addresses, in IPv4 or
  IPv6), checked again at every connection against DNS rebinding, over
  TLS, without redirects or proxies, unless `IGLOO_NOTIFICATION_ALLOW_HOSTS`
  names the host. Sends are rate-limited per integration, time out, and are
  retried with backoff; `igloo_notifications_total` counts them.
- The web console's Tasks page lists alerts, with their runs from
  `igloo.alert_history`.

### Distributed execution

- Queries larger than one machine: a statement whose largest table would
  take a replica more than a few seconds to scan runs in parts on worker
  machines (`igloo worker`). Each worker reads a share of the table's data
  files and every smaller table it is joined to, and returns partial results
  that the coordinator combines; aggregates such as `count`, `sum`, `min`,
  `max` and `avg`, aggregates over `DISTINCT` values, `GROUP BY` (with
  `ROLLUP`, `CUBE` and `GROUPING SETS`), filters, top-N and joins to smaller
  tables are covered. A join of the large table to a second table too large
  for every worker to read is partitioned by its key through object
  storage. Results are those of one machine. Igloo decides by itself when
  to distribute; `SET distributed_execution` forces it on or off for a
  session.
- Aggregates over `DISTINCT` values complete on workers: their partial
  results are partitioned by group and value through object storage, a
  worker per partition counts its values, and the coordinator adds up one
  row per group and partition instead of receiving every distinct value.
  Several at once work when one argument fixes the others, as
  `count(DISTINCT user_id)` next to `count(DISTINCT (user_id, day))`; in a
  join partitioned on the counted value no exchange is needed. The top
  groups of an aggregation by a column of a divided table
  (`GROUP BY device_id ORDER BY spend DESC LIMIT 10`) are completed and cut
  on workers too.
- Statements built in steps run step by step on workers: a part may read
  common table expressions and subqueries that workers compute (grouping,
  `DISTINCT` and `UNION ALL` included), whose rows go to object storage
  keyed by the column the reader joins or groups on. Joins of large
  intermediate results, `UNION ALL` of large branches feeding one
  aggregation, and a large table joined to itself run distributed; a
  common table expression is computed once, however many parts read it.
  Aggregates over all of a step's rows (`min(day) OVER ()`),
  uncorrelated one-row subqueries in filters over a divided table
  (`ver = (SELECT max(ver) FROM t ...)`) and `x IN (SELECT y ...)` over
  large tables, as semi joins partitioned by `y`, run on workers too, and so do aggregates that do not compute in parts (`median`,
  `quantile_cont`, `string_agg`) over a step partitioned by their groups.
  A common table expression one part reads that only filters tables is
  read in that part's place, the tasks that read a one-row subquery
  combine its partial results themselves, and a step computed partition
  by partition that keeps its rows' partitions is computed by the one step
  that reads it, so no stage only copies rows from one exchange to
  another.
  Cohort reports (distinct people per cohort, joined to their events and
  counted per signal) run on workers up to the signal-level tail.
- Workers start on demand and stop when idle: on Kubernetes the chart's
  `workers.enabled` adds a worker Deployment that Igloo scales between zero
  and `workers.maxReplicas`; on virtual machines `IGLOO_WORKER_COMPUTE`
  starts and stops them with EC2 or shell commands; Docker Compose has a
  `workers` profile for trying it out. The scale-to-zero gateway stops
  workers along with Igloo.
- Grants and the sandbox hold: statements are authorized before they are
  divided, workers read only the data files of the tables a statement
  reads, with the server's sandbox and no catalog, and accept tasks only
  over mutual TLS keyed by `IGLOO_WORKER_SECRET`. A failed worker's share
  runs on another worker, and a statement that workers cannot run runs on
  the coordinator. Canceling a statement or its `statement_timeout` stops
  its parts.
- Statements that masking or row access policies apply to, directly or
  through a view, run on one replica, as do parts that read connections'
  tables or Delta Sharing's external tables. Workers read only the files
  Igloo lists from Iceberg metadata, and refuse tasks whose SQL reads
  anything else; the part of a statement the coordinator runs itself reads
  a table at the same snapshot as the workers.
- Distributed statements count against their users' role limits and
  timeouts like any other. Workers belong to one compute pool: the chart
  gives each pool its own, with a secret of its own.
- `SET distributed_execution = on` starts workers only for admins; for
  other users it uses running workers, no more than a statement's size
  calls for. A worker that answers with an error is never set aside, and a
  statement whose partial results would pass 1 GiB of the coordinator's
  disk, or that only filters rows, runs on the coordinator.
- In `auto` mode a statement runs on workers only when they should be
  faster than the replica alone: when the replica would take over
  `IGLOO_WORKER_THRESHOLD` (a minute) or run short of memory, and workers
  are estimated at least 1.5 times faster. Statements over 4 GiB used to
  go to workers whatever the time, which made 1x reports of 6 to 56 s
  take 37 to 176 s. Each replica remembers what statements took each way,
  by `query_history.plan_key` (the text without literals and the tables'
  sizes), and runs a statement run again where it was faster.
- Workers run as many tasks at once as their cores and memory allow (a
  task per 4 cores and 8 GiB), and exchanges have more partitions as the
  divided tables grow, so the stages of a report that run side by side no
  longer spill or run out of memory on workers that one machine's memory
  would have held.
- Workers watch their memory as the server does: when one is about to run
  out, it cancels the task that has run longest instead of being killed by
  the kernel with every task on it, and the statement fails with SQLSTATE
  `53200` (`out_of_memory`) rather than run again on another worker or on
  the coordinator, which would run out the same way.
- `igloo.query_history` has `workers`, `stages` and `exchanges` columns;
  `igloo_distributed_*` and `igloo_worker_*` metrics report statements,
  fallbacks, tasks and workers.
- Tables with lists, maps and structs, Snowflake's `ARRAY` and `OBJECT`
  columns among them, run distributed: workers read nested fields by their
  Iceberg field IDs, and a data file written before a struct's field was
  renamed, or dropped and added again, runs on one machine.
- Unnests run distributed: `unnest`, `json_each` and `json_tree` of a
  row's values in `FROM`, lateral subqueries that read no table, and so
  Snowflake's `FLATTEN` of `ARRAY`, `OBJECT` and JSON text columns, whose
  `sf_` macros workers define too.
- Views run distributed: a view reads as its query, its names resolved as
  in the view, so views that pass a large table through, filter it or join
  small tables to it distribute as their queries do.
- Tables with rows deleted or updated by `DELETE`, `UPDATE` and `MERGE`
  run distributed without waiting for maintenance: workers apply the
  position delete files those write, and the equality delete files and
  deletion vectors of other engines.
- A statement's rows no longer wait for its exchanges to be deleted: the
  coordinator removes them in the background, with one listing of the
  statement's files and 32 deletes at a time, where it listed each of
  their directories and deleted each file one after another (333 requests
  for a report over 32 partitions, which spent 11 s after its stages had
  run). Every replica also sweeps the
  exchanges of statements that no longer run, such as those of a replica
  that was killed mid-statement, once nothing has been written under them
  for six hours; a statement that still runs writes there every half
  hour. Workers learn what they wrote to an exchange from DuckDB's `COPY`
  rather than listing the files and reading their footers back.

### Tools and integrations

- A migration report, `python -m migrate report --source snowflake|databricks`
  ([docs/migrating.md](docs/migrating.md)): reads the source's query history
  (Snowflake's `ACCOUNT_USAGE`, or `INFORMATION_SCHEMA.QUERY_HISTORY()`
  without it; Databricks' `system.query.history`, or the Query History
  API), groups it into query shapes, finds the workload's tables in Igloo
  (`--map` rules and connections), replays the queries on Igloo as written,
  in Igloo's own dialect of the source's SQL where there is one, and
  translated by sqlglot, and compares results with the history's row counts
  and, within a cost budget, with re-runs on the source. Its HTML, Markdown
  and JSON report gives the shares of shapes and executions that run
  unchanged, run after translation, fail (by error class, with examples) or
  lack data, the result match rate, timing, the most-used tables and
  whether Igloo has them, and the source's spend against an Igloo estimate,
  each figure labeled measured or estimated. Read-only on both sides by
  default: writes replay only with `--replay-writes`, into clones in a
  scratch schema. Literals are redacted from the report, and extracts and
  reports are written only under `--out`. Its unit tests run in CI
  (`make migrate-test`).
- dbt with the standard dbt-postgres adapter: seeds, views, tables,
  incremental models (`append`, `merge`, `delete+insert`), snapshots, tests,
  `persist_docs`, `docs generate` and `source freshness`, tested on every
  pull request with a sample agency project (`scripts/clients/dbt`). The
  profile's `dbname` is the catalog's name. See
  [docs/integrations.md](docs/integrations.md).
- Tested clients: SQLAlchemy 2 with psycopg 3, pandas and polars (through
  SQLAlchemy, connectorx and ADBC), and the JDBC metadata calls and SQL of
  Tableau and Looker, in `scripts/e2e.sh`; Metabase, Apache Superset and
  Npgsql (Power BI) weekly in the new `scripts/bi-e2e.sh` (`make bi-e2e`).
- A query string with several statements runs them one at a time, each
  checked as if sent alone, in one transaction, for every role (only admins
  could send several before, and DuckDB ran all but the last unchecked).
- Transaction blocks behave as in PostgreSQL: after a failed statement,
  everything but `COMMIT` and `ROLLBACK` fails with `25P02` and `COMMIT`
  rolls back (a failed block's other statements used to be committed);
  `BEGIN` in a block and `COMMIT` outside one only warn; `SAVEPOINT`,
  `RELEASE` and, when nothing changed since, `ROLLBACK TO SAVEPOINT` work.
  Changes to the catalog in a block, which used to be refused for
  `CREATE OR REPLACE TABLE`, views and comments, now commit the block first,
  as in Snowflake.
- `COPY (query) TO STDOUT` and `COPY table TO STDOUT` in text, CSV and binary
  format, checked as the query they run.
- `ALTER VIEW ... RENAME TO` (and `ALTER TABLE` on a view), comments on a
  view's columns, `DROP TABLE` and `DROP VIEW ... CASCADE`, `SHOW` and
  `current_setting()` of the PostgreSQL settings drivers read, `TO_CHAR` for
  dates and timestamps, and catalog queries of SQLAlchemy's inspector, JDBC's
  key and index metadata, ADBC and Npgsql.
- Views outside the default schema can be queried by schema-qualified name
  (`sales.v`) and, after `SET search_path`, unqualified, and views can read
  tables named by schema (`sales.orders`); all three failed before. Such
  table names are stored with the catalog's (`lake.sales.orders`).
- Timestamps with time zone go to clients in text as PostgreSQL writes them
  (`+00`), which psycopg2, and so dbt, Superset and pandas, could not parse
  before. Result metadata carries a `numeric` column's precision and scale.
- Renaming or dropping a table while another session lists its schema no
  longer fails spuriously after the catalog has made the change (a race in
  DuckDB's shared list of the schema's tables that dbt's threads hit).
  Other effects of that race remain, so run dbt with one thread.

### Security and governance

- Only admins may change DuckDB's own database, `memory`, where each
  replica keeps its copies of the catalog's views: the target of a
  statement is resolved through the session's current database and search
  path, so an unqualified or schema-qualified name cannot reach it either,
  and non-admins cannot put `memory` first on the search path. SQL with a
  NUL in it is refused.
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
- Igloo creates the first administrator only with an `IGLOO_ADMIN_PASSWORD`
  of at least 16 characters (was 8) that is not a placeholder such as
  `change-me` or easily guessed; the error says how to make one. An
  administrator that exists already keeps starting with a weaker value,
  with a warning that never includes it. The Helm chart refuses an
  `admin.password` that Igloo would; to upgrade a release with a shorter
  one, move it to `admin.existingSecret`.
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
- Refused DDL that would read a policied table's rows through an expression:
  a column type change, `ALTER ... USING`, a non-constant `DEFAULT`, a
  generated column, a `CHECK` constraint, or `CREATE INDEX`. These evaluate
  over every raw row, hidden ones included, so they could read masked values
  and rows a row access policy hides (through `error()`, a failing cast or
  division by zero); they are refused like a rename, for writers and admins
  alike, until the policies are unset. The Iceberg REST endpoint also refuses
  dropping a table with data policies, consistent with its other operations.
  The target of these checks is read with all of ALTER TABLE's optional
  keywords handled (`IF EXISTS`, `ONLY`, a trailing `*`, parentheses,
  comments, quoting), so `ALTER TABLE ONLY t ...` cannot slip the table past
  them; the target is read the same way wherever an ALTER is parsed, and a
  target that cannot be read is refused for non-admins.
- Turned commit retries off for Igloo's own maintenance commits (compaction,
  snapshot expiry and the rewrite of deleted rows), and for every commit
  that is not an append. A table that sets `commit.retry.num-retries` above
  0 (another engine may) would otherwise make iceberg-go rebuild a rewrite's
  manifest list from the fresh parent on a conflict and reintroduce the files
  the rewrite replaced, duplicating rows. Maintenance now fails such a commit
  and retries next pass instead (see `third_party/iceberg-go/IGLOO_PATCHES.md`).
  iceberg-go also retries no other commit but a plain append: not a rewrite,
  delete or overwrite, not a commit without a new snapshot (expiry, ref
  changes, a rollback), and not an append to a table that merges manifests.
- Compaction no longer brings back rows that a `DELETE` removed. iceberg-go
  removed every position delete file attached to the files it rewrote; a
  partition-scoped one (as DuckDB, iceberg-go and Spark before Iceberg 1.6
  write them) also deletes rows of files compaction keeps, and those rows
  came back. A position delete file now goes only when every file it
  deletes rows of, by exact path, is rewritten or no longer live. To tell,
  compaction reads a delete file's bounds, then its row groups' statistics,
  and only then its `file_path` column. Table maintenance also reads a
  `history.expire.max-snapshot-age-ms` of Java's `Long.MAX_VALUE`
  ("forever") as forever; it overflowed into a negative age.
- Erasure of people's data for GDPR and CCPA requests (`ERASE SUBJECT
  WHERE pii.email = '...'`), found through tagged columns, including
  columns of hex SHA-256, SHA-1 and MD5 digests (`pii = 'email:sha256'`),
  zero-copy clones of the tables and the tables they were cloned from, and
  other tables that share their files: it deletes the rows, rewrites the
  data files and manifests that held them, expires the snapshots that
  reference those, however recent, deletes them and old orphan files from
  object storage, deletes Delta Sharing copies made from them, drops them
  from every replica's read cache, redacts the identifiers from the query,
  load and task history, and verifies that none is left in rows, files,
  manifest bounds, Parquet statistics or history. Every branch and tag is
  rewritten, the rows a `DELETE` missed go when their files are rewritten,
  and columns tagged `:null` are set to NULL instead of deleting the row.
  Requests run in the background on the scheduler, survive restarts,
  resume after failures with growing waits, fail for good with a metric
  and an alert after 10 failures before their first `DELETE` (losing to
  other writers, or a replica with another erasure key, does not count),
  and once they have sent one never fail: a table they cannot go on with
  is set aside while the others go through every step (failures count
  per table), and the request
  is then blocked, with a metric and an alert, keeping its identifiers and
  taking it up again every hour until what stops it is fixed, or an admin
  closes it (`CLOSE ERASURE REQUEST`), which first deletes the sharing
  copies, redacts the history and has the replicas empty their caches,
  deletes what it recorded of tables dropped since, records what it left,
  files included, and forgets the identifiers. Every request sweeps the
  warehouse: the files under no table's or view's location, as those of
  tables dropped or replaced, before the request or during it, through
  Igloo or around it, while clones read them or not, go once three days
  old, and newer ones are waited for, looked at again every hour, so a
  request never completes while they are there; none is read. Drops and
  replaces through Igloo, clone refreshes included, delete the old
  table's files at once, so only those of a table clones still read, or
  may, are left to wait for. A table dropped while a request
  runs is not set aside, and one replaced is erased from under its name.
  A table
  that cannot be read, or a snapshot whose manifest list is missing,
  stops no one's request; they keep their identifiers sealed
  under `IGLOO_ERASURE_KEY` (or `IGLOO_ERASURE_KEY_FILE`'s) only while they
  run, wait for snapshots staged by write-audit-publish to be published or
  discarded, and are listed with what they did per table and step in
  `SHOW ERASURE REQUESTS` and
  `igloo.erasure_requests`, with the tables history says were written from
  the erased ones, and tagged tables of connections, which they cannot
  erase from. `ERASE SUBJECT ... DRY RUN` previews a request; one that would
  delete more than 1% of a table, with what requests of the past day
  deleted there, or all of it, waits for someone other than its requester
  to `CONFIRM ERASURE REQUEST`, and `CANCEL ERASURE REQUEST` cancels one
  before it sends a `DELETE`. Placeholder identifiers, such as the all-zero
  advertising ID, and their digests, are refused; so are, when a request
  is made, before its first `DELETE`, and again just before each `DELETE`
  from each table, tables it could not erase from with certainty: of
  Iceberg format version 3, whose deletion vectors Igloo cannot read yet,
  with files other than Parquet, or with a branch or tag it could not
  write again. Igloo has Lakekeeper refuse tables of format version 3 in
  its warehouse (and warns when `IGLOO_CATALOG_SETUP=false` skips that),
  and refuses itself to upgrade a table with tags, or one
  an open request erases from, to version 3, or to make one of version 3
  under its name, and to drop, replace or rename a table an open request
  erases from: in SQL, for every change a statement makes to the catalog,
  checked as DuckDB asks the catalog for it, whatever the statement's
  text. No one but `CREATE TABLE ... CLONE` sets Igloo's clone
  properties. A table dropped in the warehouse that no clone reads has
  its files purged with it, through SQL and the Iceberg REST endpoint
  alike, which also refuses snapshots whose manifest list is not in the
  warehouse; so does a table a clone replaces (`CREATE OR REPLACE TABLE |
  SCHEMA ... CLONE`), whose location's files Igloo deletes, never those
  of a source it read as a clone itself. Every drop and refresh decides
  the same way: the files stay when clones read them, as the clones'
  metadata declares or Igloo's record of them says, when another table
  is at their location, when they are outside the warehouse or in
  `.igloo/`, or when Igloo cannot tell; a drop that asks to purge them
  then is refused. Every branch but main is rewritten with its head's schema, and a
  tag with its snapshot's, so columns dropped since stay in them, and
  each file is written again with its own partition spec, or another
  whose columns that schema has. Admins and roles granted
  `ERASE ON ACCOUNT` make them. `docs/privacy.md` says what a completed
  request guarantees, what it trusts, and its known limitations. `iceberg-go`'s copy gains
  `Transaction.ForgetFiles`, which rewrites the manifests that list removed
  files, and `WithMaxRefAge`, so expiring snapshots keeps branches and
  tags, and writes the manifests of a spec whose column was dropped since
  with a schema that has it, so their files keep their partition values.
- Compaction and the rewrite of deleted rows keep a position delete file
  that also deletes rows of a file they do not rewrite, whose deleted rows
  came back before.
- Table maintenance reads `max-ref-age-ms` and
  `history.expire.max-snapshot-age-ms` of Java's `Long.MAX_VALUE`
  ("forever") as forever; it overflowed into a negative age that dropped
  every branch and tag.
- Data retention for tables that hold personal data (`ALTER TABLE ... SET
  (data_retention_days = N)`, at least a day, `igloo.data_retention`): table
  maintenance keeps no older snapshot, whatever else the table keeps, but
  those branches and tags point at, and rewrites the files whose rows were
  deleted longer ago. Privacy officers may set it on tables with tags.
- Renaming a table with tags or a data retention, or renaming, dropping or
  changing the type of a tagged column, is refused even when no table has
  policies, and through the Iceberg REST catalog too, as is `CREATE OR
  REPLACE TABLE` whose new table lacks a column with tags or a policy, and
  removing a tagged table's old schemas through the REST catalog.
- Table maintenance no longer drops branches and tags whose snapshots are
  older than the snapshot retention; they last as long as their own or the
  table's `max-ref-age-ms` says, forever by default.
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
- External tables read Delta Sharing's `delta` response format, in which
  Databricks shares every table with deletion vectors enabled: Igloo
  applies deletion vectors, so deleted rows never show, and column mapping
  (modes `name` and `id`), so columns have the table's names. A table that
  needs a Delta reader feature Igloo does not apply is refused, naming it.
  Providers that speak only the `parquet` format are read as before.
- External tables have the shared table's columns in its order: columns
  the files hold that the table does not, such as Delta's row tracking
  columns, are left out, and columns no file holds yet read as NULL.
- Delta Sharing and policies: tables with masking or row access policies
  cannot be shared, and a shared table that gains one is refused to
  recipients, downloads of files listed before included, with each refusal
  and its policies in the query history. Policies and tags do not attach
  to external tables, and while tables have policies only admins read
  external tables' files or the copies of shared files directly.
- Clean rooms (`CREATE CLEAN ROOM`, `ALTER CLEAN ROOM ... ADD COLLABORATOR |
  DATASET | TEMPLATE`, `APPROVE TEMPLATE`, `RUN CLEAN ROOM`, `SHOW ... IN
  CLEAN ROOM`): an agency and its partners measure audience overlap, reach
  and frequency, and conversion lift on data joined by hashed identifiers
  that neither sees. Datasets carry policies of identifiers (join and count
  only), dimensions and allowed aggregates; templates are checked on
  DuckDB's parse and run only after every data owner approved that version,
  as the room's own identity, with each result row counting at least a
  threshold of each dataset's people (50 by default). Datasets are tables
  and partners' external tables, which their owners or admins put in a
  room, readable, changeable, clonable and shown in the catalog only to
  their own party, through every front end, the Iceberg REST catalog and
  Delta Sharing. Results go to the requester alone; every
  run is in the room's history. Parameter checks, run budgets and optional
  Laplace noise defend against differencing. Built-in templates, and a
  partner API on the sharing endpoint with the recipient's token. See
  [docs/clean-rooms.md](docs/clean-rooms.md).

### Data catalog

- Owners of schemas, tables and views, by default their creator:
  `ALTER TABLE | VIEW | MATERIALIZED VIEW | SCHEMA ... OWNER TO`, shown in
  `SHOW OBJECTS`, `igloo.tables`, `igloo.schemata` and `information_schema`.
  Owners may set their objects' comments and hand them on; ownership grants
  no access to data. Engines' users own what they create through the
  Iceberg REST catalog, and admins the external tables they create.
- `COMMENT ON SCHEMA`, stored as the Iceberg namespace's `comment` property.
- `igloo.schemata`, `igloo.tables` and `igloo.columns`: owners, comments,
  columns, rows, bytes, files, snapshots and when the data last changed,
  read from the Iceberg metadata. Rows leave out those that updates,
  deletes and materialized view refreshes deleted; they are unknown while a
  table has equality deletes. `information_schema.tables` gains
  `table_owner`, `comment`, `row_count`, `bytes` and `last_altered`, as in
  Snowflake. `igloo.tables` and `igloo.columns` also list external tables,
  with their provider, and connections' tables, the latter to admins and
  to whoever has a grant on them only, from a listing that never waits for
  a remote database.
- `igloo.lineage`, table-level lineage from the query history: edges from
  the tables and locations that `INSERT ... SELECT`, `CREATE TABLE AS`,
  `MERGE`, `UPDATE ... FROM`, `COPY INTO`, tasks, pipes and materialized
  views read to the tables they wrote, with when they were last seen and by
  which statement or job; connections' tables are named in full, and reads
  of external tables count as reads of the tables, never of their files.
  `igloo.table_usage` counts reads, writes and users of each table over 30
  days, not Delta Sharing recipients' downloads.
- The query history records what every statement read and wrote, not only
  those of users with the role none, worked out in the background, and new
  columns `locations_read` and `job`.
- Lineage events for OpenLineage endpoints such as Marquez and DataHub
  (`IGLOO_OPENLINEAGE_URL`), without query results, and without statements'
  text unless `IGLOO_OPENLINEAGE_SEND_SQL` is set. Events go only to public
  addresses, or private ones when allowed, checked as connected, never
  through a proxy or a redirect.
- Everything the catalog shows is limited to what the user may see: users
  with the role none see only the tables they have a grant on or own, which
  now also applies to `igloo_comments()`, `pg_description`, and
  `igloo.policy_references` and `igloo.tag_references`.

### Web console

- A web console on port 8443 (`IGLOO_CONSOLE_ADDR`): SQL worksheets with
  cancel and CSV download, a data browser, query history (the console's
  own lookups, recorded under the application name `Igloo console lookup`,
  shown only when asked), tasks, pipes and
  materialized views with their runs, the load history, and admin pages
  for users, roles and grants, and the replica's sessions. Users sign in
  with their Igloo password or single sign-on; statements run as them
  through the same authorization as SQL clients. Sessions are stateless
  sealed cookies, so the console works behind a load balancer, and end when
  the user is disabled or their password changes.
- Console pages for access tokens (everyone's own), service accounts and
  every token (admins), and single sign-on: settings, whether the provider
  answers, group mappings and who signed in with which groups.
- A catalog page: search over tables' names, descriptions, columns, tags
  and owners, and a page for each table and view with its description,
  owner, size, freshness, usage, columns, sample rows and a lineage graph,
  showing tables the user may not see only as a count. Owners, writers and
  admins edit descriptions there.

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

- DuckDB runs two threads per core on the server and four on workers when
  the warehouse is in object storage and `IGLOO_DUCKDB_THREADS` is unset,
  instead of one. Reading object storage cold waits on requests, not on
  cores: on 8 cores, a cold scan of 23 GB read 64 MB/s on 8 threads and
  172 MB/s on 32, and a report that took 66 s cold took 33 s on 16 threads
  (12 s warm either way). Auto mode's estimates of distributed execution
  count cores rather than threads.
- Tables can be clustered. `ALTER TABLE ... SET SORTED BY (a, b)` (and
  Snowflake's `CLUSTER BY`, `ALTER TABLE ... CLUSTER BY`, `DROP CLUSTERING
  KEY`) sets a table's default Iceberg sort order, which `igloo.tables`,
  `information_schema.tables` and `SHOW OBJECTS` show as its clustering key.
  Table maintenance then rewrites, sorted, the files whose ranges of the
  order's first column overlap, within its memory budget, sorting range by
  range and spilling to local disk, so a filter on that column reads few
  files: on 1.4 billion rows written unsorted, a filter on one value read
  140 files, and 3 once clustered (0.65 s instead of 2.3 s with nothing
  cached). A clustered table is not rewritten again until new files overlap
  it. See [SQL](docs/sql.md#sort-orders-and-clustering) and
  [operations](docs/operations.md#clustering).
- `CREATE TABLE` in Snowflake's dialect translates the column types of a
  table named with its schema (`db.schema.t`) too.
- Tables Igloo creates are written in files of 512 MB, Iceberg's default
  target, rather than a file per row group: DuckDB's Iceberg writer aims
  for 8 MB when a table sets no `write.target-file-size-bytes`, so 1.3 GB of
  narrow rows came to 288 files, each read with its own requests, until
  compaction caught up at 8 GB a table an hour. Igloo now sets the property
  when it creates a table, unless the statement did. See
  [operations](docs/operations.md#table-maintenance).
- When memory runs low, Igloo lowers DuckDB's memory limit, so that DuckDB
  spills more and gives memory back, and raises it again once memory has
  stayed plentiful: under heavy work DuckDB keeps the memory it tracks at
  its limit and uses more besides, 8 GiB more for one statement over ten
  times the data on a 61 GiB machine. If memory still runs out, Igloo
  cancels the statement that has run longest (SQLSTATE `53200`) instead of
  being killed by the kernel with every session. DuckDB keeps to its memory limit only for the memory
  its buffer manager tracks: a reservoir sample of 600,000 rows of 60 text
  columns took 7.8 GB under a 500 MB limit, and one of a million rows of a
  384-column table took a replica down on a 32 GB machine. See
  [operations](docs/operations.md#sizing).
- A statement with thousands of casts, such as a column profile of a wide
  table in Snowflake's dialect, is prepared in time that grows with its
  length rather than its square: Igloo's rewrite of catalog casts lexed the
  whole statement again for each cast. A profile of 449 columns took 4.3
  seconds to prepare and run on an empty table, and now takes 0.43.
- Faster `FLATTEN` in Snowflake's dialect. `FLATTEN` of a column unnests it
  where it is read, without the dependent join a subquery of its own took,
  and unnests an `ARRAY` or `OBJECT` column's list or map without going
  through JSON text: 0.3 s instead of 1 s for 15 million elements of 3
  million rows. `FLATTEN` of an array constant, as reports build from each
  row's columns (`[OBJECT_CONSTRUCT(...), ...]`), unnests its elements
  without building the array and parsing it back, three times faster.
  `THIS` is computed only where a statement can read it. DuckDB plans a
  dependent join, which holds the rows' lists and maps at several times
  the cost of JSON text, for a `FLATTEN` in a select with a `WHERE`, whose
  `WHERE` is now the `ON` of the `FLATTEN`'s join when every join is an
  inner one (flattening an `OBJECT` column of 30 million rows ran out of
  18 GiB, and takes 14 s), and for one that a second `FLATTEN` reads
  (`FLATTEN(INPUT => f.value:items)`), whose `ARRAY` and `OBJECT` columns
  it builds objects from are turned into JSON before the join (a report
  that took 481 s that way takes 266 s). A `FLATTEN` of objects built with
  literal keys (`[OBJECT_CONSTRUCT('kind', 'genre', 'items', m.genres),
  ...]`), whose `VALUE` its select reads only by a key, as reports that
  flatten one member of each object do (`FLATTEN(INPUT =>
  GET_PATH(t.value, 'items'))`), keeps the objects as DuckDB structs rather
  than JSON, so their `ARRAY` columns are flattened from their lists
  without JSON text or a dependent join: 0.4 s instead of 1.6 s for 29
  million elements of 2 million rows' five lists. A `WHERE` there stays
  out of the joins, which DuckDB would plan as dependent joins: conditions
  on the tables go into the first `FLATTEN`'s `ON`, which DuckDB pushes
  down to them, and conditions on the objects' keys into the lists the
  second `FLATTEN` unnests (1.5 s, 1.9 s and 1.4 s, with conditions on the
  table, the keys and both, became 0.42 s, 0.62 s and 0.44 s). A string in
  such an object reads as itself, conditions on the objects' keys filter
  the objects, and conditions on a second `FLATTEN`'s elements filter
  those when the statement reads nothing of that `FLATTEN` but its
  `VALUE`, which is then all it unnests. Objects that no second `FLATTEN`
  reads stay structs whatever filters the select, since DuckDB's dependent
  join then holds the table's columns, as for JSON. Reports' signals,
  objects of a dimension and a row's string filtered on the strings, took
  3.9 s for 5 million rows and take 1.2 s, and objects of a dimension and
  a list whose elements are filtered 3.1 s for 3 million rows and 1.2 s.
  A `FLATTEN`'s `KEY`, `PATH`, `INDEX` and `THIS` count as read only where
  its own select names them, so a column of one of those names elsewhere,
  as reports name an `INDEX`, no longer keeps a second `FLATTEN`'s elements
  unfiltered and the objects JSON. Objects built from one `FROM` item's
  columns, and a second `FLATTEN` of one of their keys, are unnested in
  that item's select list rather than in lateral joins, and still run
  distributed: DuckDB pushes a filter, such as a BI tool's outer `WHERE`
  on a key a report groups by, down onto a lateral `FLATTEN`, which made
  it a dependent join, and one over structs of lists cost more than over
  JSON text, so reports' behaviors and signals ran slower on real data
  than as JSON. With such a filter, objects of three lists took 1.8 s for
  2 million rows and take 0.4 s; objects of strings 0.8 s and 0.4 s. A
  second `FLATTEN` whose elements every read casts to a type that is not
  semi-structured, as reports cast them `AS TEXT`, unnests their text
  instead of making JSON of each string and reading it back, which took
  20 to 24% of the behaviors reports' time on data shaped like real data:
  counting people by each element of 2 million rows' five lists took
  0.22 s and takes 0.13 s, and a behaviors report over them 0.15 s and
  0.09 s. A filter on that text stays above the select list that unnests
  it, as one through `json_extract_string` did, so that DuckDB, which takes
  a filtered unnest for a fifth of the rows it reads, builds a join's hash
  table on the other side, as reports join their signals to cohorts: it
  had built one on 90 million browsing elements. A key of the objects that
  every read casts so is one field of text, built once for each object,
  rather than a list, a string and a JSON part that each read combines:
  reports' media signals, objects of a dimension and a name filtered and
  grouped by the name, took 0.20 s for 5 million rows and take 0.18 s.
- Faster statements on large catalogs. DuckDB lists the Iceberg catalog's
  tables again for every statement that looks one up, and Igloo's checks of
  a user without the reader role listed every attached database's tables
  for each statement besides. The catalog proxy now keeps listings for up to
  10 seconds (changes through Igloo apply at once; a table made elsewhere is
  found at once by name, and listed within 10 seconds), the checks resolve
  names against an index of tables built again whenever that may have
  changed, and they plan a statement once rather than twice. On a catalog of
  753 tables plus a Snowflake connection of 812, small queries of a user
  with a schema grant went from 95–150 ms to 30–38 ms, and an admin's from
  35–45 ms to 21–25 ms.
- Iceberg tables' schemas, which the first statement that lists columns
  loads, load side by side and, after a start, in the background: on the
  same catalog that took 5 seconds instead of 11, and a first
  `information_schema.columns` lookup made once it is done takes 0.13
  seconds.
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
  pass that is due runs soon after a start. A table's maintenance deletes
  only files inside its own location, and keeps those that clones still
  read; `DROP TABLE` asks the catalog to purge files only when no clone
  reads them.
- Results reach clients in batches rather than one network write per row:
  large results arrive four to five times faster.
- `COUNT(*) OVER ()` beside a query's rows, as in Acme's table previews, no
  longer makes DuckDB hold every row before it returns the first: where it
  is the number of rows the `FROM` and `WHERE` produce, Igloo counts those
  once instead, in either dialect. Previewing 50 rows of a 37-million-row
  table with its row count took 7.9 s and takes 1.0 s (docs/sql.md#dialect).
- A subquery a statement repeats in `FROM`, as in a report's top and bottom
  selections from the same report, is computed once, as a materialized
  common table expression the copies read, in either dialect, where that
  cannot change the result (docs/sql.md#dialect). A report that selected
  twice from one subquery of 17 common table expressions took 597 s and
  needed over 98 GiB of spill on an 8-core, 32 GiB machine; it takes 128 s,
  the time of one report.

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
- A website (`site/`): a landing page and the documentation, built from the
  repository's markdown with search, light and dark themes, and a link check
  in CI (`make site`).
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
- DuckDB's spill directory on local NVMe where the cloud offers it
  (docs/operations.md#spill-on-local-nvme), instead of network block
  storage. The AWS module defaults to `r7gd.xlarge` nodes and, for node
  types with instance store, has nodeadm RAID0 it under kubelet, so the
  spill `emptyDir` is on NVMe. The Azure module defaults to
  `Standard_E4pds_v6` Igloo nodes with an ephemeral OS disk on their local
  NVMe disk (`igloo_node_os_disk_type`). The Google Cloud module's
  `local_ssd_machine_family` runs the igloo pods on Local SSD-backed
  ephemeral storage. The single-machine stack's `IGLOO_SPILL_DIR` puts spill
  files in a host directory, such as one on an instance store. AWS and Azure
  deployments that relied on the earlier node defaults get new nodes on
  their next apply; `node_instance_types = ["r7g.xlarge"]` and
  `igloo_node_vm_size = "Standard_E4ps_v6"` keep the old ones.

### Fixed

- A statement in Snowflake's dialect with a `FLATTEN` of an earlier
  `FLATTEN`'s elements and a `WHERE` on the table they flatten, such as
  `FROM t, LATERAL FLATTEN(t.a) f, LATERAL FLATTEN(f.value) g WHERE t.id >
  1`, failed with DuckDB's "Non-inner join on correlated columns not
  supported": the `WHERE` became the `ON` of the last `FLATTEN`'s join,
  which a comma kept from reading the table. Commas before that `FLATTEN`
  are now `CROSS JOIN`s, which nest from the left.
- A `JSON` value that is a string, such as `'"abc"'::JSON` or
  `json_extract(v, '$.name')`, reached PostgreSQL clients without its
  quotes (`abc`), so a client could not tell it from a number or `true`.
  It is now sent as JSON text (`"abc"`).
- A write could land twice, with the first copy's data file deleted, when
  the catalog applied a commit but answered with an error or not at all
  (seen under heavy load as a 503). DuckDB sent the commit again, the
  catalog refused the repeat as a conflict, DuckDB deleted the files of the
  commit it took for failed, and Igloo ran the statement again. DuckDB now
  reaches the catalog through a proxy inside Igloo that applies each commit
  exactly once: it sends commits again itself, with an `Idempotency-Key`,
  checks a refused commit's snapshot against the table before believing the
  refusal, and deletes the files of commits the catalog refused. DuckDB no
  longer deletes files itself, so a table never references a missing file;
  when the catalog cannot be reached long enough that a write's outcome is
  unknown, the statement fails with SQLSTATE `40003` instead of `40001`, and
  its files are kept (docs/operations.md#writes-when-the-catalog-fails).
- A dbt run could find a `__dbt_backup` table it had dropped still listed,
  and DuckDB could crash, when the replica synced its copies of the
  catalog's views at the time. Every 30 seconds the sync looked for copies
  that had gone missing by listing the views of every database DuckDB has
  attached, and DuckDB-Iceberg then lists the tables of each of the
  catalog's schemas, a list it does not keep in step with a session that
  renames or drops one of them at the same time. The background sync no
  longer looks; a copy that has gone missing is made again when a statement
  names it.
- A view a session had just created, replaced, renamed or dropped could
  seem unchanged on its own replica until the next sync, 30 seconds later:
  dbt failed with `view "..._dbt_tmp" does not exist` or `already exists`,
  left `__dbt_backup` views behind, or built a model from a view's previous
  definition. A sync lists the catalog's views without holding up view
  changes, and applied the list even when it was older than a change the
  replica had made meanwhile. Syncs now leave the views their replica
  changed meanwhile as those changes made them, and never apply a list older
  than one already applied; a statement that names a view another replica
  created finds it when any sync brought it, not only its own.
- A listing of the catalog (`information_schema`, `pg_catalog`,
  `duckdb_tables()`/`duckdb_views()`, `SHOW`, `DESCRIBE`), or even a plain
  read, could crash or wedge the whole replica, ending every session on it,
  when a table was renamed or dropped at the same time on that replica — a BI
  tool browsing schemas while a `dbt` run swapped tables, which any user with
  read access could set off. The embedded engine keeps one in-memory set of
  the catalog's tables, shared by every session and changed as tables are
  created, renamed, dropped or used; reading it while it changed corrupted
  it. Igloo now serializes the short steps that reach that set on each
  replica — preparing a statement, listing the catalog, and DDL that
  creates, renames, drops or alters a table, view or schema — and lets
  DuckDB bind a statement again as it starts to run only alongside other
  such binds, while reading and writing table data, which do not change the
  set, run alongside each other and those steps without waiting. `CREATE
  [OR REPLACE] TABLE ... AS` runs in steps, so that only creating and
  renaming the new table are serialized, not filling it
  (docs/operations.md#concurrency-on-a-replica).
- Table maintenance could run Igloo out of memory, and then kill it again
  after every restart. Compaction read a group's files all at once, one per
  core, in batches of 131,072 rows however wide, and queued 64 batches for
  the Parquet writer, so its memory grew with the width of the rows and the
  number of files: DuckDB stores a repetitive string in a byte or two, and
  Arrow in full. After 760 tables were mirrored from Snowflake, the first
  pass grew Igloo to 30 GB on a 30 GB machine, the kernel killed it, and the
  overdue pass killed it again about 35 seconds after each restart.
  Compaction now reads a group's files one at a time, in batches and row
  groups sized from how much memory a row of the table takes, and holds at
  most `IGLOO_MAINTENANCE_MEMORY` (by default a quarter of Go's memory
  limit, between 256 MiB and 4 GiB); a table that cannot be compacted within
  it is left as it is and reported. Compacting 255 MiB of DuckDB-written
  Parquet took 6.1 GiB of Go heap and now takes about 1.2 GiB, in half the
  time. Igloo also gives Go a memory limit, unless `GOMEMLIMIT` sets one,
  of what the container's or machine's memory leaves after DuckDB's limit;
  logs each table as it starts compacting it; and, if it died while
  rewriting a table's files, leaves that table's files alone for a day
  instead of rewriting them again within seconds of starting
  (docs/operations.md#compaction-memory).
- A name qualified with a schema, such as `media_demo."AUDIT_EVENTS"`, was
  checked against every database with a schema of that name, not just the
  one DuckDB reads. With a Snowflake connection that has a schema
  `MEDIA_DEMO` too, a user with role `none` and `SELECT` on the catalog's
  `media_demo` was refused (`SELECT on table sf.media_demo.audit_events needs
  a SELECT grant`) though DuckDB reads the catalog's table; so were their
  `EXPLAIN`, writes and DDL by grants, a reader's join of the table with a
  connection's table granted to them, every user's read of it once the
  connection's table had a row access policy, and a policy that read it.
  Names now mean, for these checks, the tables at the places DuckDB looks
  for them, as DuckDB 1.5 binds names: a schema-qualified name in the
  databases whose schema of that name is on the search path, or else in the
  current database. Names in views' bodies follow DuckDB 1.5 too, which
  looks for a schema-qualified one in the view's own database first and for
  an unqualified one not in that database's `main` schema; some statements
  that read views were refused as unverifiable before.
- Errors and column names that PostgreSQL clients rely on differed from
  PostgreSQL's. Every name DuckDB found nothing called failed with SQLSTATE
  `42P01` (undefined table): a missing function (`SELECT
  no_such_function(1)`) now fails with `42883`, a type (`1::NUMBER`) with
  `42704`, a schema with `3F000`. `SHOW no_such_setting` failed as a
  missing table (DuckDB's `SHOW` describes a table); it now fails as
  PostgreSQL's does, `42704` `unrecognized configuration parameter
  "no_such_setting"`. A non-admin's `SET` or `RESET` of a parameter no one
  has was refused as needing admin (`42501`); it now fails with `42704` as
  an admin's does. And `SELECT current_user` named its column `'igloo'`, the
  user's name quoted, so a client reading the column `current_user` found
  nothing; `current_user`, `session_user` and `current_role` that are a
  select list's items of their own now name their columns as PostgreSQL
  does, while the Snowflake dialect keeps Snowflake's names, such as
  `CURRENT_USER()`. Over Flight SQL, a missing schema or database is
  `NOT_FOUND`.
- In Snowflake's dialect, `TO_VARCHAR`, `TO_CHAR` and `::VARCHAR` without a
  format wrote DuckDB's text rather than Snowflake's: a `FLOAT` as
  `999.8899804651736` or `9244.0` where Snowflake writes `999.889980465`
  and `9244`, a `TIMESTAMP_LTZ` with the offset `+00` rather than `+0000`, a
  `TIME` with fractions of a second and `BINARY` as escapes. Acme's column
  profiles compare these strings. They now follow Snowflake's default output
  formats (a `FLOAT` with up to 12 significant digits;
  `TIMESTAMP_OUTPUT_FORMAT`, `TIME_OUTPUT_FORMAT` and `BINARY_OUTPUT_FORMAT`
  at their defaults), in the session's time zone; `TZH` and `TZM` in a
  format write the offset as Snowflake does; and `ALTER SESSION` accepts the
  output format parameters at their defaults and refuses others. Profiling
  four columns of 37 million rows takes 12% longer.
- In Snowflake's dialect, `APPROX_COUNT_DISTINCT` and `HLL` were DuckDB's
  `approx_count_distinct`, a HyperLogLog of 64 registers that errs by about
  13% and by over 50% at worst. On an Acme table of 1.25 million rows it
  counted 1,336 values where there were 1,099 and Snowflake counted 1,102,
  and Acme decides from these counts which columns are categories. They
  now count with DataSketches' HyperLogLog of 4,096 registers, as many as
  Snowflake's, within about 1.6%, and exactly up to about 300 values,
  as fast as before. In a select with `GROUP BY` and as window functions
  they count exactly, since a sketch per group is 25 times slower than
  `COUNT(DISTINCT)` with millions of groups; with a few hundred groups
  that takes up to three times as long as before.
- A table whose name the catalog spells with capitals, such as one copied
  from Snowflake as `ORDERS`, was listed in whatever case a statement or
  view had last named it in: once a view read it as `orders`, every replica
  that loaded the view listed `orders` in `duckdb_tables()`,
  `information_schema.tables` and `SHOW TABLES`. The catalog finds a table
  by its name in any case, and the embedded engine names its entry for a
  table as it was last looked up; the catalog's own name never changed.
  Igloo now notes, as it loads a table, when it was named otherwise than
  the catalog spells it, and looks it up again by the catalog's name before
  anything lists the catalog, so listings always show the catalog's name.
  Reading a table by its name in any case works as before.
- DuckDB's spill files outlived the Igloo process that wrote them when it
  stopped, cleanly or killed, while data was spilled, and the next start
  did not delete them: on a single machine whose replica restarted during
  large queries, 59 GB of them filled the spill volume, and later queries
  failed with `No space left on device` or `failed to offload data block`.
  DuckDB deletes them only as its database closes, and Igloo kept its
  database open until the process exited, through connections it had
  opened to register its functions. Igloo now closes those connections, so
  a clean stop deletes the files, and when it starts it deletes those a
  killed or crashed process left, before it runs any query, and logs the
  bytes freed.
  It deletes only DuckDB's own files in `IGLOO_DUCKDB_TEMP_DIR`
  (`duckdb_temp_storage_*`, `duckdb_temp_block-*`), and none while another
  Igloo process uses the directory, which it tells by a lock that each
  process holds there (docs/operations.md#spill-directory).

[Unreleased]: https://github.com/akkio-inc/Igloo/commits/main
