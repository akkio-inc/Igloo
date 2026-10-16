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
- **Cache disk.** The [read cache](#read-cache) keeps table files on local
  disk. Size the disk so your frequently read tables fit in half of its free
  space, or set `IGLOO_CACHE_SIZE`.

A memory-optimized machine with 4 to 8 cores and 32 to 64 GB handles most
datasets up to a few hundred GB of compressed Parquet comfortably.

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

The cache is DuckDB's [`cache_httpfs`](https://github.com/dentiny/duck-read-cache-fs)
community extension, baked into the image like the other extensions; DuckDB
checks its signature when it loads. If it cannot load or its directory is
not writable, Igloo logs a warning and runs without it.

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
| `igloo_cache_size_bytes` | | Bytes in the [read cache](#read-cache), measured every 5 minutes |
| `igloo_cache_limit_bytes` | | Size the read cache may grow to, given the disk's free space; 0 while it is off or paused |

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
| Log says `read cache off: cannot load the cache_httpfs extension ... Read-only file system` | The root filesystem is read-only and `/tmp` is not writable. Mount a small writable volume at `/tmp`. |
| Log says `read cache off: its directory is not writable` | Make the volume at `IGLOO_CACHE_DIR` writable by the container's user (UID 65532), e.g. with `fsGroup: 65532` on Kubernetes. |
| Log says `read cache paused` | Less than 5% of the cache's disk is free. Free space or give the cache its own volume; it resumes on its own. |
