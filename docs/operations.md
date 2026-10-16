# Operating Igloo

This page covers running Igloo once it is deployed: users, sizing and
scaling, monitoring, backups, table maintenance, scheduled work, upgrades
and troubleshooting. Settings are described in [configuration.md](configuration.md).

## Users and roles

The first administrator comes from `IGLOO_ADMIN_USER` and
`IGLOO_ADMIN_PASSWORD`. The password is used to create that account on first
start only; afterwards, change it in SQL.

```sql
CREATE USER ana WITH PASSWORD 'a-long-password' ROLE writer;
ALTER USER ana ROLE reader;
ALTER USER ana PASSWORD 'another-long-password';
ALTER USER CURRENT_USER PASSWORD 'my-new-password';   -- any user, for themselves
ALTER USER ana NOLOGIN;                                -- disable; LOGIN enables
DROP USER IF EXISTS ana;
SHOW USERS;                                            -- admins see everyone

CREATE USER bo PASSWORD NULL EMAIL 'bo@agency.com';    -- signs in with single sign-on
CREATE USER dbt_prod TYPE SERVICE ROLE none;           -- a service account, tokens only
CREATE TOKEN ci FOR USER dbt_prod EXPIRES '90d';       -- shown once
```

People can also sign in through the company's identity provider, and SQL
clients with access tokens instead of passwords: see
[single sign-on, access tokens and service accounts](sso.md).

Every user has one built-in role, which applies to the whole warehouse:

| Role | Can |
|---|---|
| `none` | Nothing on its own: only what [grants](#grants) allow, plus reading the system catalogs and their own history |
| `reader` | Run queries and `EXPLAIN`, and change their own session settings such as `search_path` and `TimeZone` |
| `writer` | Everything a reader can, plus `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `COPY`, creating, altering and dropping tables, views and schemas, comments, and table properties such as snapshot retention |
| `admin` | Everything, including users, roles and grants, secrets, `ATTACH`, global settings, `EXPLAIN ANALYZE`, and several statements in one prepared statement |

`CREATE USER` without `ROLE` makes a reader. Settings a non-admin changes
with `SET` apply to their own session only.

- Passwords must be at least 8 characters. Igloo stores SCRAM-SHA-256
  verifiers, never passwords. psql's `\password` works and sends a verifier
  computed on the client (except for users with access tokens; see
  [access tokens](sso.md#access-tokens)).
- Role changes, disabled and dropped users and revoked tokens reach open
  connections within 30 seconds; on the replica that made the change,
  immediately.
- After 10 failed logins for a user, or 50 from an address, within 15
  minutes, further logins for it are refused for a while
  ([details](sso.md#failed-logins)).
- Users are stored in the metadata database (`IGLOO_METADATA_DATABASE_URL`,
  schema `igloo_auth`), so every replica sees the same accounts. Without that
  setting the administrator is the only account.
- Igloo refuses to remove, demote or disable the last enabled admin who
  is a person: service accounts with the admin role do not count, since
  no one can sign in to the console as them.
- Admins can do the same on the [web console](console.md)'s Users and
  Service accounts pages, which run these statements as them.

### Grants

Grants give users access to particular schemas and tables, beyond their
built-in role: give a user the role `none` and grant what they need, or
give a reader `INSERT` on the tables they load. Grant to custom roles, and
give the roles to users:

```sql
CREATE USER nora WITH PASSWORD 'a-long-password' ROLE none;
CREATE ROLE analysts;
GRANT analysts TO nora;
GRANT SELECT ON SCHEMA marts TO analysts;        -- every table, now and later
GRANT SELECT, INSERT ON TABLE staging.events TO analysts;
GRANT CREATE ON SCHEMA sandbox TO nora;           -- create and drop tables there
SHOW GRANTS TO nora;
```

[SQL and clients](sql.md#roles-and-grants) describes every statement and
privilege and what a grant covers. In short:

- A statement runs if the built-in role allows it, or if every table it reads
  or writes is granted to the user or one of their roles. Users with role
  `none` need `SELECT` on what they read, views included: a view reads with
  the querying user's privileges.
- Users with role `none` cannot read files (`read_parquet`, `iceberg_scan`,
  `COPY ... FROM` and the like), which could read any table's data files.
- Grants never allow what needs `admin`, nor writing files.
- Roles and grants live in the metadata database, next to the users. Changes
  apply at once on the replica that made them and within 30 seconds on the
  others, as role changes do.
- For users with role `none`, Igloo plans each statement before running it
  to see what it reads. That costs a few milliseconds per statement; other
  roles are not affected. A statement whose reads it cannot account for is
  refused with SQLSTATE `42501`.
- The same grants apply to [other engines](external-engines.md#users-for-engines)
  using the Iceberg REST catalog, and take effect there on the next request.

### Policies and tags

Masking and row access policies decide what each user sees of a table's
columns and rows, for every statement; tags classify columns and can mask
all of them at once. [Masking, row access policies and
tags](security-policies.md) describes them.

- Admins create and attach them; anyone may list them. They live in the
  metadata database (schema `igloo_policy`); without one, in each replica's
  memory, lost on restart.
- Changes apply at once on the replica that made them and within 30 seconds
  on the others.
- Policies and tags stay with a table's name when the table is dropped, and
  renaming a table or column they protect or tag is refused: unset them
  first.
- While any table has policies, users other than admins cannot read the
  warehouse's files directly; give loading jobs files in
  `IGLOO_ALLOWED_LOCATIONS`.

### Erasure requests

[Erasure requests](privacy.md) (`ERASE SUBJECT`) delete a person's data from
every table whose columns are tagged with an identifier, and keep a record
that shows it. Admins and holders of `GRANT ERASE ON ACCOUNT` make them.

- Each request runs as a job on the [scheduler](#scheduled-work), every
  minute until it completes, one request at a time across replicas; its
  runs are in `igloo.task_history` as kind `erasure`. It reads the tagged
  columns of the tagged tables in full, with all of a replica's cores, and
  before it deletes files, the metadata of every table in the catalog, so
  it deletes none that another table references.
- Every replica checks every 15 seconds for files that requests deleted and
  drops them from its read cache; a request waits for the replicas seen in
  the last 2 minutes. A replica that was stopped drops them when it starts.
- Requests, and the files each deleted, are in the metadata database
  (schema `igloo_erasure`) and survive restarts; without one, in memory.
- `ALTER TABLE ... SET (data_retention_days = N)` limits how long a table
  keeps old versions of its rows; table maintenance enforces it.

## Query and login history

Igloo records every statement and every login attempt: who, when, how long,
the outcome and SQLSTATE, rows returned or changed, the statement text,
the tables and locations it read and the tables it wrote, the task, pipe or
materialized view it ran for, the client's address and `application_name`,
the query tag and compute pool, and what DuckDB measured: CPU time, bytes
read and memory allocated. The [catalog](catalog.md)'s lineage and table
usage are worked out from it. Read them with SQL:

```sql
SELECT start_time, user_name, outcome, duration_ms, query_text
FROM igloo.query_history ORDER BY start_time DESC LIMIT 50;
SELECT * FROM igloo.login_history WHERE result = 'failure';
```

[SQL and clients](sql.md#query-history) lists the columns. Admins see every
record; other users see only their own, whichever way they read them.
Requests from [other engines](external-engines.md#history) through the
Iceberg REST catalog are recorded the same way, with application name
`iceberg-rest` and the catalog operation, such as `loadTable`, as the
statement type.
Passwords never reach the history: string literals of user-management
statements, `CREATE SECRET` and credential settings are replaced by `'***'`.

- With `IGLOO_METADATA_DATABASE_URL`, history is kept in its schema
  `igloo_audit` and shared by all replicas. Records are written in the
  background in batches of up to a thousand, about once a second.
- Records older than `IGLOO_AUDIT_RETENTION` (90 days by default) are
  deleted every hour, by one replica at a time.
- Without a metadata database, each replica keeps its latest 10,000
  statements and 10,000 logins in memory, lost on restart.
- Recording never slows down or fails a statement. Records are written in
  batches every second; a batch that fails is written again for about a
  minute, so a slow or briefly unavailable metadata database loses nothing.
  If it stays unavailable, or falls far enough behind that 10,000 records
  wait, records are dropped: the log warns (`dropped history records`, at
  most once a minute) and `igloo_audit_dropped_total` counts them.
- What a statement read and wrote is worked out in the background after it
  runs (see [lineage](catalog.md#lineage)); when more than 10,000 statements
  wait for that, statements are recorded without it, and the log warns.
- `IGLOO_AUDIT=false` turns recording off, and with it `igloo.usage`,
  lineage, table usage and lineage events.
- Each replica also records, once a minute, how long it has been running,
  for the cost estimates of [`igloo.usage`](workload-management.md#usage-and-cost),
  in `igloo_audit.compute_time`; it is pruned with the history. A minute
  that cannot be written is counted in the next one.
- The views read the history page by page, newest first; `LIMIT` without
  `ORDER BY` reads only the newest pages, while other filters and sorts read
  everything visible. To analyze months of history from a busy warehouse,
  query `igloo_audit.query_history` in the metadata database directly.

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
- **Cache disk.** The [read cache](#read-cache) keeps table files on local
  disk. Size the disk so your frequently read tables fit in half of its free
  space, or set `IGLOO_CACHE_SIZE`.

A memory-optimized machine with 4 to 8 cores and 32 to 64 GB handles most
datasets up to a few hundred GB of compressed Parquet comfortably. To size
one client's or team's compute apart from everyone else's, give them a
[compute pool](workload-management.md#compute-pools).

### When one machine is not enough

Scale up first: one larger machine is the cheapest and simplest way to run
bigger queries, and DuckDB spills joins, aggregations and sorts that do not
fit in memory to disk, slowing down only modestly. Two things eventually
limit it:

- **Scan time.** A query's time grows with the bytes it reads divided by
  what one machine reads and decompresses per second, which is bounded by
  its cores and, for files in object storage, by its network. A query over
  a terabyte of compressed Parquet takes minutes on even the largest
  machines.
- **Memory.** Below some fraction of the memory a join or aggregation
  needs, DuckDB stops spilling and the query fails with an out-of-memory
  error. A larger machine moves that limit; on workers, a join of two
  large tables is partitioned so that each worker holds a share of it.

For those queries, run [workers](distributed-execution.md): a statement over
a table that a replica would take more than a few seconds to scan runs in
parts on them, and workers that are stopped are started for statements that
would take more than a minute. Size workers like large replicas (many
cores, about 4 GB per core, fast local disk, high network bandwidth) and put
them on a node pool or machines that are stopped while idle. The
[design document](design/distributed-execution.md#evidence) has the
measurements behind these numbers.

## Read cache

Reading table files from object storage is far slower than reading them
from local disk: on a c7g.4xlarge with tables in Amazon S3, the 22 TPC-H
queries at scale factor 10 took 31 to 59 seconds when every file came from
S3, and 5.5 seconds once the files were in memory. Igloo therefore keeps a
copy of what it reads from the warehouse on local disk, and later reads,
including those after a restart or a [scale-to-zero](scale-to-zero.md)
start, use the copy. Within a run, DuckDB also keeps recently read data in
memory, in front of the disk cache.

- **What is cached.** Files under the warehouse location on Amazon S3,
  S3-compatible stores and Google Cloud Storage, in blocks of 512 KiB, one
  file per block: table data and delete files, and whatever else users read
  there. Iceberg manifests are still fetched on every query; they are small.
  Files in `IGLOO_ALLOWED_LOCATIONS`, and tables on Azure Storage, are read
  directly.
- **Always current.** Iceberg writes every file once and never changes it,
  so a copy cannot go stale. Blocks of files read by path, such as
  `read_parquet('s3://...')`, also record the object's ETag and are used only
  while it matches, so a file overwritten in place is fetched again.
  Directory listings and file sizes are never cached.
- **Size.** Unless `IGLOO_CACHE_SIZE` says otherwise, the cache takes at most
  half of the disk space that other files leave free, and at most 100 GiB.
  It never fills the disk past 95%. Igloo remeasures every 5 minutes, so the
  cache gives way when other files (spill files, logs, a local object store)
  grow: it stops adding blocks, and drops the least recently used block each
  time a read misses. If less than 5% of the disk would be free even without
  the cache, Igloo empties it and pauses it until there is room.
- **Where it lives.** `/var/lib/igloo/cache` in the container
  (`IGLOO_CACHE_DIR`), on a filesystem with extended attributes, such as
  ext4, XFS or btrfs. Docker Compose keeps it in the `igloo-cache` volume,
  which survives restarts, `docker compose down` and machine stops. On
  Kubernetes, mount a volume there, and a small writable `/tmp` if the root
  filesystem is read-only (the extension that implements the cache needs
  one). A PersistentVolumeClaim keeps the cache across pod restarts and
  scaling to zero; any other volume is lost with the pod. Igloo sizes the
  cache from the free space of the filesystem it is on, which an `emptyDir`
  shares with the node: with an `emptyDir`, set `IGLOO_CACHE_SIZE` below its
  `sizeLimit`. Instance-store (local NVMe) disks are fast, but empty when
  the machine stops.
- **Watching it.** `igloo_cache_size_bytes` and `igloo_cache_limit_bytes`
  ([metrics](#monitoring)) show its size and how large it may grow. The log
  says `read cache on` at startup, or why it is off. An admin can count an
  own session's hits and misses:

  ```sql
  SET cache_httpfs_profile_type = 'temp';   -- every session that reads from now on
  SELECT count(*) FROM lineitem WHERE l_quantity > 10;
  SELECT cache_hit_count, cache_miss_count
  FROM cache_httpfs_cache_access_info_query() WHERE cache_type = 'data';
  SET cache_httpfs_profile_type = 'noop';   -- turn counting off again
  ```

- **Clearing it** is never needed for correctness. To free the space, an
  admin runs `SELECT cache_httpfs_clear_cache();`, or, with Igloo stopped,
  you delete the contents of the cache directory (with Docker Compose:
  `docker compose rm -sf igloo && docker volume rm igloo_igloo-cache &&
  make up`).
- **From SQL**, only admins may call the cache's functions and change its
  settings (`cache_httpfs_*`). Where the cache lives, whether it wraps other
  filesystems and that it caches only the warehouse are set at startup and
  cannot be changed from SQL, even by admins
  (`cache_httpfs_cache_directory`, `cache_httpfs_cache_directories_config`,
  `cache_httpfs_wrap_cache_filesystem`, `cache_httpfs_add_exclusion_regex`
  and `cache_httpfs_reset_exclusion_regex` are refused): the extension
  creates and deletes files in its directory, outside DuckDB's sandbox.

The cache is DuckDB's [`cache_httpfs`](https://github.com/dentiny/duck-read-cache-fs)
community extension, baked into the image like the other extensions; DuckDB
checks its signature when it loads. If it cannot load or its directory is
not writable, Igloo logs a warning and runs without it.

## Connections to other databases

[Connections](federation.md) to PostgreSQL and MySQL databases are kept in
the metadata database and attached by each replica when it starts, so after
`CREATE CONNECTION`, restart the replicas (`docker compose restart igloo`,
or `kubectl rollout restart deployment/igloo`); changes to credentials,
hosts and timeouts and `DROP CONNECTION` need none. Each replica checks
every connection's database when it starts, at once, and starts without the
ones it cannot reach, logging `connection not attached` with the reason;
`SHOW CONNECTIONS` shows each one's state on the replica that answers.

Igloo logs `cannot sign in to the remote database` the first time in a row
that it cannot, and `remote database reachable again` when it can again.
Queries of a connection whose database is down fail within its
`connect_timeout`, with an error that names the connection; nothing else is
affected, listing tables included.

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

## Writes when the catalog fails

Every write ends with a commit to the Iceberg catalog. An overloaded catalog,
or a load balancer in front of it, can apply a commit and still answer with
an error, or not answer at all. DuckDB reaches the catalog through a proxy
inside Igloo, on the loopback interface, that applies each commit exactly
once whatever the catalog answers:

- **Errors and timeouts.** A commit the catalog answers with a 5xx, 408 or
  429, or does not answer within 30 seconds, is sent again, with the same
  `Idempotency-Key`. Lakekeeper answers a repeat of a commit it already
  applied with that commit's success, and does not apply it again. DuckDB's
  own repeats of a request it stopped waiting for never reach the catalog.
- **Refusals.** When the catalog refuses a commit, Igloo first checks
  whether the snapshot it adds is in the table, which it is when an earlier
  attempt landed. If it is, the write succeeded. If not, another writer
  committed first: the refusal is a conflict, retried as described under
  [concurrent writes](#scaling-out), and the files written for the refused
  commit are deleted straight away.
- **Unknown outcome.** If the catalog cannot be reached for about 90
  seconds after an attempt whose outcome was lost, the statement fails with
  SQLSTATE `40003` (`statement_completion_unknown`): the write may or may
  not have landed. Igloo does not run it again, and clients that retry
  `40001` should not retry `40003`: check the table first, for example with
  `SELECT * FROM iceberg_snapshots('lake.public.events')`. The write's files
  are kept either way, so the table is never left referencing a missing
  file. If the write did not land, the [orphan
  cleanup](#table-maintenance) deletes them after three days, as it does
  the files of statements that fail or roll back before they commit.

The log (component `catalog-proxy`) records each commit sent again and each
outcome confirmed after an error. Catalogs that ignore `Idempotency-Key`
get the same guarantee for writes that change rows; a schema change whose
answer was lost may then fail even though it was applied.

## Workload management

[Workload management](workload-management.md) covers attributing and
limiting work:

- **Query tags** (`SET query_tag = 'client:acme'`, or a default per user or
  role) and the replica's **compute pool** (`IGLOO_POOL`) are recorded with
  every statement, as are the CPU time, bytes and memory DuckDB measured
  and the time the statement waited for its limits.
- **`igloo.usage`** sums the history per day, pool, user, role and tag, and
  estimates each group's compute cost from `IGLOO_PRICE_PER_HOUR`.
- **Role limits** (`ALTER ROLE analysts SET max_concurrent_statements = 4`,
  `statement_timeout`, `memory_limit`) hold statements in line on each
  replica.
- **Compute pools** give a client or team replicas of their own, sharing
  the catalog, users and history: `pools` in the Helm chart, or
  `deploy/single-vm/compute-pool.yml` with Docker Compose.

Statements over very large tables can also run in parts on
[workers](distributed-execution.md). Workers belong to one compute pool,
whose replicas share them and stop them once idle; each pool has its own.

## Timeouts and limits

- `IGLOO_STATEMENT_TIMEOUT` cancels statements that run too long (default: no
  limit). Sessions can set their own with `SET statement_timeout = '5min'`
  and see it with `SHOW statement_timeout`. A role's `statement_timeout`
  ([role limits](workload-management.md#role-limits)) caps both.
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
| `/activity` | Statements, maintenance passes and scheduled runs running now, seconds since one last started or ended, and when scheduled work is next due, as JSON. The [scale-to-zero gateway](scale-to-zero.md) reads it. |

Igloo's metrics, besides the standard Go and process metrics:

| Metric | Labels | Meaning |
|---|---|---|
| `igloo_sessions` | `role` | Open sessions |
| `igloo_queries_total` | `outcome` = `ok`, `error`, `denied`, `canceled` | Statements run |
| `igloo_query_duration_seconds` | `outcome` | Time to run a statement and send its results |
| `igloo_logins_total` | `result` = `success`, `failure` | Login attempts |
| `igloo_audit_written_total` | `kind` = `query`, `login` | History records written |
| `igloo_audit_dropped_total` | `kind` = `query`, `login` | History records dropped because the queue was full or the metadata database failed for about a minute |
| `igloo_maintenance_passes_total` | `result` = `ok`, `error` | Table maintenance passes run by this replica |
| `igloo_maintenance_last_success_timestamp_seconds` | | When this replica last finished a maintenance pass without errors |
| `igloo_maintenance_compacted_files_total`, `igloo_maintenance_compaction_output_files_total` | | Data files replaced and written by compaction |
| `igloo_maintenance_expired_snapshots_total` | | Snapshots expired |
| `igloo_maintenance_orphan_files_deleted_total` | | Unreferenced files deleted |
| `igloo_cache_size_bytes` | | Bytes in the [read cache](#read-cache), measured every 5 minutes |
| `igloo_cache_limit_bytes` | | Size the read cache may grow to, given the disk's free space; 0 while it is off or paused |
| `igloo_statements_waiting` | | Statements waiting for their [role limits](workload-management.md#role-limits) |
| `igloo_statement_wait_seconds_total` | | Time statements spent waiting for their role limits |
| `igloo_notifications_total` | `type`, `result` = `sent`, `failed`, `rate_limited` | [Notifications](alerts.md) sent or tried |
| `igloo_distributed_*` | | [Distributed execution](distributed-execution.md#seeing-it): statements run on workers, fallbacks, tasks, workers available |

Logs go to stderr, as text or JSON (`IGLOO_LOG_FORMAT=json`). Security-relevant
events are logged at info level or above: `login`, `login failed`,
`user changed`, `grants changed`, `role settings changed`, `job changed`
(tasks, pipes, materialized views and alerts created, changed or dropped),
`notification integration changed`, `erasure requested`, `erasure request
completed`, `data retention changed`, `statement denied`, `statement
canceled` (from the web console's compute page), and refused connections.
Failed scheduled runs log `scheduled run failed` (component `tasks`), and
notifications that could not be sent `notification failed` (component
`notifications`), with the integration's host, never its URL. Sign-ins
through the web console log `client=console`, and through Arrow Flight SQL
`client="flight sql"`. Query text and DuckDB error details, which can quote
the query, are logged only at debug level; user-management statements,
which carry passwords, are never logged.
The [query history](#query-and-login-history) keeps statement text, with
passwords and secrets removed.

## Backups

Two things hold state:

1. **The PostgreSQL database** (`IGLOO_METADATA_DATABASE_URL` and the
   catalog's database, normally the same one). It holds the catalog, which
   maps table names to their current metadata in object storage, and Igloo's
   users, roles, grants and history, its tasks, pipes, materialized views
   and alerts, its notification integrations, and the load history that
   keeps files from loading twice. Back it up like any production database: managed-service backups
   with point-in-time recovery, or regular `pg_dump`. Losing it loses track of
   every table, even though the data files are still in object storage.
2. **Object storage.** Table data and Iceberg metadata files. Turn on bucket
   versioning or replication if you need protection against deletion; the
   cloud modules leave versioning as an option.

Restore the database to a point in time no later than the storage contents
you have. Table maintenance deletes the files of expired snapshots, so a
database restore older than `IGLOO_SNAPSHOT_RETENTION` can point at files that
are gone unless bucket versioning kept them.

Backups, versions and soft-deleted objects keep what [erasure
requests](privacy.md) delete until they expire, and restoring one brings it
back: keep their retention within what your privacy obligations allow, and
erase again after a restore.

## Table maintenance

Every write creates an Iceberg snapshot and at least one data file, and
updates and deletes add delete files. Left alone, a table that takes frequent
small writes grows without bound and gets slower to read. Igloo maintains
tables itself, so nobody has to schedule anything. Every hour
(`IGLOO_MAINTENANCE_INTERVAL`) it makes one pass over every table:

1. **Compaction.** Small files, and files with many deletes, are rewritten
   into files of the table's target size (`write.target-file-size-bytes`,
   512 MB by default), with deletes applied. A pass rewrites at most 8 GB per
   table; larger backlogs finish over several passes. The new files are laid
   out the way DuckDB writes Parquet, so compaction does not make scans
   slower: row groups of 122,880 rows, Snappy compression, decimals stored as
   integers, and dictionary encoding only for columns in which at most a
   fifth of the values are distinct. A table's
   `write.parquet.compression-codec` and `write.parquet.row-group-limit`
   properties override the codec and row group size. File size does not
   limit parallelism: DuckDB divides a scan by row group, so a table in a few
   large files still uses every core.
2. **Snapshot expiry.** Snapshots older than `IGLOO_SNAPSHOT_RETENTION`
   (7 days by default) are removed, always keeping the current one, and the
   files that only they referenced are deleted. Files that compaction
   replaced stay until the snapshots that read them expire. Branches and
   tags stay, with the snapshots they name and the files those read, until
   they are dropped, or for as long as their own `max-ref-age-ms` or the
   table's `history.expire.max-ref-age-ms` says, as in Java's Iceberg.
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

A table's [data retention](privacy.md#data-retention) (`ALTER TABLE ... SET
(data_retention_days = N)`, for tables that hold personal data) caps both:
maintenance keeps no snapshot older than it but the current one, and
rewrites, with the deletes applied, the data files whose rows were deleted
longer ago, so they leave the files too.

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

### Files that clones share

A [clone](sql.md#clones) reads its source's data files, delete files and
manifests where they are, under the source's location. Every way Igloo
deletes files keeps those a clone still reads, whichever of the two tables
it works on:

- **Snapshot expiry** deletes the files that only expired snapshots
  referenced, and only files inside the table's own location: a clone's
  maintenance never deletes its source's files. A source's maintenance
  keeps every file a clone references in any of its snapshots.
- **Orphan cleanup** keeps them the same way.
- **Compaction** deletes nothing itself; the files it replaces go with the
  snapshots that read them.
- **`DROP TABLE`** never asks the catalog to purge the table, which would
  delete everything under its location: a dropped table's files stay in the
  bucket, clones keep reading theirs, and Igloo does not delete the rest.
  The catalog deletes files only when asked to purge, and only Igloo can
  reach it; other engines go through [Igloo's Iceberg REST
  catalog](external-engines.md#clones), which refuses to purge a table
  whose files clones read (409, naming the clones; dropping it without
  purging works). Igloo's warehouse uses the catalog's default hard deletion, in
  which a dropped table cannot be restored; with Lakekeeper's soft deletion
  turned on instead, a clone restored after it was dropped may miss
  files of its source that maintenance deleted meanwhile.

Which tables share which files is recorded twice: in each clone's own
metadata (the table property `igloo.clone.shares-files-of`, the locations
it reads from) and in the metadata database (schema `igloo_clones`). Each
pass first visits every table, recording what the clones declare, so a
replica without a metadata database, or a database restored from a
backup, has the record back before anything is deleted. Deleting a table's
files then takes a lock that cloning the table takes too (in the metadata
database, so replicas share it), and reads the metadata of each of its
clones. Whenever Igloo cannot tell what a clone needs (the metadata
database does not answer, a clone was renamed or dropped during the pass,
or was made less than an hour ago), it keeps the table's files; a later
pass deletes them.

A clone stops sharing once its own maintenance has replaced and expired
everything it read of its source. The source's files that only clones
needed are then orphans, which the orphan cleanup deletes once they are
three days old. Until then a shared file counts once in the bucket, but a
file the source no longer needs stays as long as a clone reads it.

To find every table that may hold a table's data, as erasing a person's
data must, look up the table's location (the `location` of its Iceberg
metadata, as the catalog returns it) in the metadata database:

```sql
SELECT clone_schema, clone_name, clone_uuid, created_at
FROM igloo_clones.shares
WHERE location = 's3://warehouse/0193.../0193...';  -- no trailing slash
```

Each row is a clone, or a clone of a clone, whose metadata may reference
files under that location; `clone_uuid` is its `table-uuid`. The name is
where the clone was last seen, so a renamed clone shows its new name only
after the next maintenance pass, and a dropped clone's row goes only at a
later one: check a row by loading the table and comparing its UUID. The clones' own
metadata says the same, readably without the metadata database: the table
property `igloo.clone.shares-files-of` is a JSON array of the locations a
clone may read from (`igloo.clone.source` and
`igloo.clone.source-snapshot-id` say what it was made from). Both are where
a clone may read, not what it reads: the files themselves are those the
clone's snapshots list, under each location.

## Scheduled work

Tasks, pipes, materialized views ([loading and scheduling](loading.md)) and
[alerts](alerts.md) run inside Igloo; nothing else needs scheduling. To
hear of failed runs and loads without watching for them, switch on the
[built-in alerts](alerts.md#built-in-operational-alerts). How they run:

- Every replica runs a scheduler, which looks for due work every few
  seconds. Each due run is claimed by one replica through the metadata
  database, and a session-level lock there keeps a job from running twice
  at once on any replica; a run that comes due while the last one still
  runs is recorded as `skipped`. Postgres releases the lock of a replica
  that dies, and the next run marks the dead one's run `failed`.
- A replica runs at most four scheduled runs at once; further due work
  waits for a free slot or another replica. Each run is a session of its
  owner, which counts against `IGLOO_MAX_CONNECTIONS` while it lasts and
  shows on the console's compute page as `task <name>`, `pipe <name>`,
  `materialized view <name>` or `alert <name>`.
- Runs and loads count as running work on `/activity`, so a scale-to-zero
  gateway does not stop Igloo during one. On shutdown, Igloo starts no new
  runs; those under way get the 30 seconds running queries get, then are
  canceled and recorded as failed.
- Work missed while Igloo was not running runs once when it starts, then
  on schedule. With [scale to zero](scale-to-zero.md#scheduled-work), the
  gateway starts Igloo when work is due.
- Without `IGLOO_METADATA_DATABASE_URL`, definitions and history are in
  memory and lost on restart; run a single replica.

Watch failures in `igloo.task_history` (as an admin, every run is there) and
`igloo.load_history`:

```sql
SELECT start_time, kind, name, user_name, error FROM igloo.task_history
WHERE outcome = 'failed' AND start_time > now() - INTERVAL 1 DAY;
```

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
| SQLSTATE `42501`, `permission denied for ana: SELECT on table s.t needs ... a SELECT grant` | Neither the user's role nor their grants allow it. An admin can `GRANT` the privilege named on the table, its schema or one of the user's roles. |
| SQLSTATE `42501`, `read_parquet reads data that grants do not cover` | Users with role `none` cannot read files or call functions that take a table's name; query the tables instead. |
| `cannot verify which tables this statement reads` | A user with role `none` sent a statement whose reads Igloo could not account for, such as one through a macro. Rewrite it over tables and views, or give the user the reader role. |
| SQLSTATE `55000`, `connection crm is not attached on this replica` | The connection was created after the replica started, or its database could not be reached then (`SHOW CONNECTIONS` says which). Restart the replica. |
| SQLSTATE `42501`, `SELECT on table crm.s.t needs a SELECT grant on it, its schema or connection crm` | Connections' tables need grants for every role but admin; `GRANT SELECT ON CONNECTION crm TO ...` or on the schema or table. |
| A new table of a connection's database is missing from listings | Replicas list a connection's tables when they attach it. Run `ALTER CONNECTION name REFRESH`, or restart. |
| SQLSTATE `08001` on `CREATE CONNECTION`, `certificate signed by unknown authority`, `server refused TLS connection` or `does not support TLS` | Connections verify their server by default (`tls 'verify-full'`). Give the authority that signed its certificate in `tls_ca` (managed databases publish theirs), or turn TLS on for the server; choose a weaker `tls` mode only on a network you trust ([TLS](federation.md#tls)). |
| `send one statement at a time` | A prepared statement (the extended protocol, such as a driver's statement with parameters) held several statements, which only admins may send. Send them one at a time, or without parameters, as one simple query. |
| SQLSTATE `42501`, `only admins may change the memory database` or `only admins may make memory the current database` | The statement would create, change or drop something in DuckDB's own database, where each replica keeps its copies of the catalog's views, by name or through the search path. Name the catalog's schema (`lake.public.t`), and keep a schema of the catalog first in `search_path`. |
| `Permission Error: Cannot access file ...` | The sandbox blocked a file or URL outside the warehouse. To load from or export to another bucket, add it to `IGLOO_ALLOWED_LOCATIONS`. |
| SQLSTATE `40001`, `CatalogCommitConflicts` | Two writers committed to the same table at once inside explicit transactions. Retry the transaction. |
| SQLSTATE `40003`, `CommitStateUnknownException` | The catalog did not answer whether a write was applied ([writes when the catalog fails](#writes-when-the-catalog-fails)). Check the table's snapshots before running the write again, and the catalog's health. |
| SQLSTATE `57014` | The statement was canceled, by the client or by `statement_timeout`. |
| SQLSTATE `57014`, `it waited 30s to start, behind the limit of 4 statements at once for role analysts` | The statement waited for its [role limits](workload-management.md#role-limits) until its timeout. Run fewer statements at once, raise the limit, or give the team a compute pool. |
| SQLSTATE `42501`, `ST_Read goes through GDAL` | GDAL-based readers and writers are refused; load spatial data as GeoParquet, shapefiles or GeoJSON ([media analytics](media-analytics.md#loading-boundaries)). |
| `CREATE TABLE` fails with `schema: data did not match any variant` | The table has a column the catalog cannot store, such as a `GEOMETRY`; store `ST_AsWKB(geom)` instead ([media analytics](media-analytics.md#loading-boundaries)). |
| SQLSTATE `53300` | The replica is at `IGLOO_MAX_CONNECTIONS`. Use a connection pool, raise the limit, or add replicas. |
| Startup fails with `warehouse already exists in bucket ...` | `IGLOO_WAREHOUSE_LOCATION` does not match the existing warehouse. Point it at the existing data or use a different `IGLOO_CATALOG_WAREHOUSE`. |
| Startup retries `catalog not reachable yet` | The catalog or its database is not up. Igloo waits up to 3 minutes. |
| `/readyz` returns 503 | The body names the failing dependency: engine, metadata database or catalog. |
| Log says `read cache off: cannot load the cache_httpfs extension ... Read-only file system` | The root filesystem is read-only and `/tmp` is not writable. Mount a small writable volume at `/tmp`. |
| Log says `read cache off: its directory is not writable` | Make the volume at `IGLOO_CACHE_DIR` writable by the container's user (UID 65532), e.g. with `fsGroup: 65532` on Kubernetes. |
| Log says `read cache paused` | Less than 5% of the cache's disk is free. Free space or give the cache its own volume; it resumes on its own. |
| A task, pipe or `COPY INTO` does not do what you expect | See [loading troubleshooting](loading.md#troubleshooting). |
| A large query did not run on workers | `SET distributed_execution = 'on'` and run it again with `IGLOO_LOG_LEVEL=debug`: the log says why it ran on the replica (for example a table with delete files, or an aggregate that does not combine). See [what runs distributed](distributed-execution.md#what-runs-distributed). |
| Log says `workers could not run a statement; running it here` | Workers failed or could not be reached (`igloo_distributed_fallbacks_total` counts why). The statement still completed on the replica. Check the workers' logs and that they share `IGLOO_WORKER_SECRET` and Igloo's version. |
| Log says `worker runs another version of Igloo; not using it` | Workers must run the same image as the server. Upgrade them together. |
