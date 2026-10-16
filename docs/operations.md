# Operating Igloo

This page covers running Igloo once it is deployed: users, sizing and
scaling, monitoring, backups, table maintenance, upgrades and
troubleshooting. Settings are described in [configuration.md](configuration.md).

## Users and roles

The first administrator comes from `IGLOO_ADMIN_USER` and
`IGLOO_ADMIN_PASSWORD`. The password is used to create that account on first
start only; afterwards, change it in SQL.

```sql
CREATE USER ana WITH PASSWORD 'a-long-password' ROLE writer;
ALTER USER ana ROLE reader;
ALTER USER ana PASSWORD 'another-long-password';
ALTER USER CURRENT_USER PASSWORD 'my-new-password';   -- any user, for themselves
DROP USER IF EXISTS ana;
SHOW USERS;                                            -- admins see everyone
```

| Role | Can |
|---|---|
| `reader` | Run queries and `EXPLAIN`, and change their own session settings such as `search_path` and `TimeZone` |
| `writer` | Everything a reader can, plus `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `COPY`, creating, altering and dropping tables, views and schemas, comments, and table properties such as snapshot retention |
| `admin` | Everything, including users, secrets, `ATTACH`, global settings, `EXPLAIN ANALYZE`, and several statements in one query |

Settings a reader or writer changes with `SET` apply to their own session only.

- Passwords must be at least 8 characters. Igloo stores SCRAM-SHA-256
  verifiers, never passwords. psql's `\password` works and sends a verifier
  computed on the client.
- Role changes and dropped users reach open connections within 30 seconds;
  on the replica that made the change, immediately.
- Users are stored in the metadata database (`IGLOO_METADATA_DATABASE_URL`,
  schema `igloo_auth`), so every replica sees the same accounts. Without that
  setting the administrator is the only account.
- Igloo refuses to remove or demote the last admin.
- Admins can do the same on the [web console](console.md)'s Users page,
  which runs these statements as them.

## Sizing

Each Igloo replica is a complete query engine. A query runs on one replica,
using all of its cores.

- **Memory** is what matters most. DuckDB uses up to 80% of the memory it
  sees, or `IGLOO_DUCKDB_MEMORY_LIMIT`. Under Kubernetes, set the limit to
  about 80% of the container's memory limit (the Helm chart does this) so the
  kernel does not kill the process.
- **Spill disk.** Queries that need more memory than the limit write
  temporary files to `IGLOO_DUCKDB_TEMP_DIR`. Give it fast local disk with
  room for your largest sorts and joins.
- **CPU.** DuckDB parallelizes each query across `IGLOO_DUCKDB_THREADS`
  threads (default: one per core).

A memory-optimized machine with 4 to 8 cores and 32 to 64 GB handles most
datasets up to a few hundred GB of compressed Parquet comfortably.

## Scaling out

Run more replicas behind one load balancer or Kubernetes Service. Replicas
share nothing but the catalog and object storage:

- A write committed through one replica is visible to new statements on every
  other replica straight away. Inside a transaction, a session keeps a
  consistent snapshot.
- Each replica accepts up to `IGLOO_MAX_CONNECTIONS` sessions (default 100).
- **Concurrent writes.** Iceberg commits are optimistic: when two sessions
  write the same table at once, the catalog rejects the later commit. Igloo
  re-runs a rejected statement automatically, up to 20 times with backoff,
  when it ran outside an explicit transaction and returned no rows. Inside
  `BEGIN ... COMMIT`, the client gets SQLSTATE `40001` and should retry the
  transaction. Many small concurrent writes to one table are slow; batch them
  into fewer, larger statements.

## Timeouts and limits

- `IGLOO_STATEMENT_TIMEOUT` cancels statements that run too long (default: no
  limit). Sessions can set their own with `SET statement_timeout = '5min'`
  and see it with `SHOW statement_timeout`.
- Clients can cancel a running query with a PostgreSQL cancel request (Ctrl-C
  in psql, `Statement.cancel()` in JDBC, context cancellation in Go).
- `IGLOO_MAX_CONNECTIONS` caps sessions per replica; further logins fail with
  SQLSTATE `53300`.

## TLS

Set `IGLOO_TLS_CERT_FILE` and `IGLOO_TLS_KEY_FILE` to a PEM certificate and
key. Clients then connect with `sslmode=require` (or `verify-full` with your
CA). Once a certificate is configured, Igloo refuses unencrypted sessions
unless `IGLOO_TLS_REQUIRED=false`. Turn TLS on before accepting connections
from outside a private network: SCRAM keeps passwords off the wire, but
queries and results are otherwise unencrypted. SCRAM channel binding
(`channel_binding=require`) is not supported yet.

## Monitoring

Port 8080 serves:

| Path | Meaning |
|---|---|
| `/healthz` | The process is up. Use for liveness. |
| `/readyz` | The engine runs queries, the metadata database answers, and the catalog answers. Use for readiness. |
| `/metrics` | Prometheus metrics |
| `/activity` | Statements and maintenance passes running now, and seconds since one last started or ended, as JSON. The [scale-to-zero gateway](scale-to-zero.md) reads it. |

Igloo's metrics, besides the standard Go and process metrics:

| Metric | Labels | Meaning |
|---|---|---|
| `igloo_sessions` | `role` | Open sessions |
| `igloo_queries_total` | `outcome` = `ok`, `error`, `denied`, `canceled` | Statements run |
| `igloo_query_duration_seconds` | `outcome` | Time to run a statement and send its results |
| `igloo_logins_total` | `result` = `success`, `failure` | Login attempts |
| `igloo_maintenance_passes_total` | `result` = `ok`, `error` | Table maintenance passes run by this replica |
| `igloo_maintenance_last_success_timestamp_seconds` | | When this replica last finished a maintenance pass without errors |
| `igloo_maintenance_compacted_files_total`, `igloo_maintenance_compaction_output_files_total` | | Data files replaced and written by compaction |
| `igloo_maintenance_expired_snapshots_total` | | Snapshots expired |
| `igloo_maintenance_orphan_files_deleted_total` | | Unreferenced files deleted |

Logs go to stderr, as text or JSON (`IGLOO_LOG_FORMAT=json`). Security-relevant
events are logged at info level or above: `login`, `login failed`,
`user changed`, `statement denied`, `statement canceled` (from the web
console's compute page), and refused connections. Sign-ins through the web
console log `client=console`. Query text and
DuckDB error details, which can quote the query, are logged only at debug
level; user-management statements, which carry passwords, are never logged.

## Backups

Two things hold state:

1. **The PostgreSQL database** (`IGLOO_METADATA_DATABASE_URL` and the
   catalog's database, normally the same one). It holds the catalog, which
   maps table names to their current metadata in object storage, and Igloo's
   users. Back it up like any production database: managed-service backups
   with point-in-time recovery, or regular `pg_dump`. Losing it loses track of
   every table, even though the data files are still in object storage.
2. **Object storage.** Table data and Iceberg metadata files. Turn on bucket
   versioning or replication if you need protection against deletion; the
   cloud modules leave versioning as an option.

Restore the database to a point in time no later than the storage contents
you have. Table maintenance deletes the files of expired snapshots, so a
database restore older than `IGLOO_SNAPSHOT_RETENTION` can point at files that
are gone unless bucket versioning kept them.

## Table maintenance

Every write creates an Iceberg snapshot and at least one data file, and
updates and deletes add delete files. Left alone, a table that takes frequent
small writes grows without bound and gets slower to read. Igloo maintains
tables itself, so nobody has to schedule anything. Every hour
(`IGLOO_MAINTENANCE_INTERVAL`) it makes one pass over every table:

1. **Compaction.** Small files, and files with many deletes, are rewritten
   into files of the table's target size (`write.target-file-size-bytes`,
   512 MB by default), with deletes applied. A pass rewrites at most 8 GB per
   table; larger backlogs finish over several passes.
2. **Snapshot expiry.** Snapshots older than `IGLOO_SNAPSHOT_RETENTION`
   (7 days by default) are removed, always keeping the current one, and the
   files that only they referenced are deleted. Files that compaction
   replaced stay until the snapshots that read them expire.
3. **Orphan file cleanup**, once a day per table. Files under the table's
   location that no snapshot references and that are more than 3 days old,
   such as the leftovers of failed writes, are deleted.

Retention decides how far back time travel reaches: a query `AT (TIMESTAMP => ...)`
older than the retention fails. Tables can override it with the standard
Iceberg properties:

```sql
-- keep 30 days of history for this table (writers and admins)
SELECT * FROM set_iceberg_table_properties(lake.public.events,
  {'history.expire.max-snapshot-age-ms': '2592000000'});
```

`history.expire.min-snapshots-to-keep` sets a minimum number of snapshots to
keep regardless of age.

Maintenance commits are ordinary Iceberg commits. A query that is running
keeps reading the snapshot it started with; a write that commits at the same
moment as compaction wins, and compaction tries again on the next pass. With
several replicas, only one maintains tables at a time; they coordinate through
a lock in the metadata database, where they also record when passes ran. A
replica that has just started, for example after [scaling to
zero](scale-to-zero.md), runs a pass within seconds if none finished within
the interval. Without `IGLOO_METADATA_DATABASE_URL`, run a single replica.

Watch `igloo_maintenance_last_success_timestamp_seconds`: if it stops
advancing, the logs (component `maintenance`) say which table failed and why.
Set `IGLOO_MAINTENANCE=false` only if another tool maintains the tables.

## Upgrades

Igloo itself is stateless: replace the binary or image and restart.

- **Kubernetes:** `helm upgrade` rolls replicas one at a time.
- **Docker Compose:** pull the new version of this repository and run
  `make up`.

Igloo creates and updates its own tables in the metadata database on start,
and the catalog runs its database migrations before it serves. Read
[CHANGELOG.md](../CHANGELOG.md) before upgrading across a minor version.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `password authentication failed` | Wrong user or password. Unknown users get the same error by design. |
| SQLSTATE `42501`, `permission denied: X requires the writer role` | The user's role does not allow the statement. An admin can change it with `ALTER USER`. |
| `send one statement at a time` | Only admins may send several statements in one query. Most tools send one at a time; for scripts, run them as an admin or split them. |
| `Permission Error: Cannot access file ...` | The sandbox blocked a file or URL outside the warehouse. To load from or export to another bucket, add it to `IGLOO_ALLOWED_LOCATIONS`. |
| SQLSTATE `40001`, `CatalogCommitConflicts` | Two writers committed to the same table at once inside explicit transactions. Retry the transaction. |
| SQLSTATE `57014` | The statement was canceled, by the client or by `statement_timeout`. |
| SQLSTATE `53300` | The replica is at `IGLOO_MAX_CONNECTIONS`. Use a connection pool, raise the limit, or add replicas. |
| Startup fails with `warehouse already exists in bucket ...` | `IGLOO_WAREHOUSE_LOCATION` does not match the existing warehouse. Point it at the existing data or use a different `IGLOO_CATALOG_WAREHOUSE`. |
| Startup retries `catalog not reachable yet` | The catalog or its database is not up. Igloo waits up to 3 minutes. |
| `/readyz` returns 503 | The body names the failing dependency: engine, metadata database or catalog. |
