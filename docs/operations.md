# Operating Igloo

This page covers running Igloo once it is deployed: users, sizing and
scaling, monitoring, backups, table maintenance, scheduled work, upgrades
and troubleshooting. Settings are described in [configuration.md](configuration.md).

## Users and roles

The first administrator comes from `IGLOO_ADMIN_USER` and
`IGLOO_ADMIN_PASSWORD`. The password is used to create that account on first
start only; afterwards, change it in SQL. Igloo refuses to create the
account with a password under 16 characters, a placeholder such as
`change-me`, or an easily guessed one
([details](configuration.md#users)); once the account exists, a weaker value
only logs a warning.

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

- Passwords must be at least 8 characters (16 for `IGLOO_ADMIN_PASSWORD`
  to create the first administrator). Igloo stores SCRAM-SHA-256
  verifiers, never passwords. psql's `\password` works and sends a verifier
  computed on the client (except for users with access tokens; see
  [access tokens](sso.md#access-tokens)).
- Role changes, disabled and dropped users and revoked tokens reach open
  connections within 30 seconds; on the replica that made the change,
  immediately. A replica that cannot read the metadata database goes on
  with the users, grants, policies and ID domains' members it read last
  for up to `IGLOO_ACCESS_MAX_STALENESS` (2 minutes), then refuses its
  sessions' statements with SQLSTATE `58000` until it can read them again.
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
  on the others; a replica that cannot read them refuses statements after
  `IGLOO_ACCESS_MAX_STALENESS`, as for role changes.
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

- Requests need `IGLOO_ERASURE_KEY` or `IGLOO_ERASURE_KEY_FILE`
  ([configuration](configuration.md)), the same on every replica and kept
  out of the metadata database. A replica with another key, or none, leaves
  open requests alone, logging at error level, until one with the right
  key runs them; set it back rather than making the requests again.
- Each request runs as a job on the [scheduler](#scheduled-work), one
  request at a time across replicas: a run that finds another running ends
  at once, so requests never take the scheduler's slots from tasks, pipes
  and materialized views. Runs are in `igloo.task_history` as kind
  `erasure`. A request reads the tagged columns of the tagged tables in
  full, with all of a replica's cores, every record of the history stores,
  and, before it deletes files, the metadata of every table in the catalog,
  so it deletes none that another table references.
- A failed run is tried again after a minute, then 2, 4, up to an hour; a
  request that fails 10 times in a row before its first `DELETE` fails for
  good: it logs `erasure request failed` at error level, raises the
  built-in alert, and counts in `igloo_erasure_requests_failed_total`. One
  that has sent a `DELETE` never fails: a table it cannot go on with is
  set aside, and the others go on; the request is then `blocked`, keeps
  its identifiers, logs `erasure request cannot go on` at error level,
  raises the built-in alert, counts in
  `igloo_erasure_requests_blocked_runs_total` and tries again every hour,
  until what its error says is fixed, or an admin closes it (`CLOSE
  ERASURE REQUEST`, see [privacy](privacy.md#what-a-request-does)). Alert
  on those metrics, and on `igloo_erasure_requests_held_total`, which
  counts requests waiting for `CONFIRM ERASURE REQUEST` by someone other
  than their requester. While a request is not over, its tables cannot be
  dropped or renamed, through SQL or the Iceberg REST endpoint: tools that
  swap tables by renaming them, as dbt does, wait until it is. Igloo has
  Lakekeeper (0.13 or later) refuse tables of Iceberg format version 3 in
  its warehouse, which erasure requests cannot rewrite yet. Runs
  that lose to other writers' commits, as on a table appended to without
  pause, do not count: the request tries again every minute until it wins.
- Every replica checks every 15 seconds whether a request deleted files
  since it last emptied its read cache, and empties it whole; a request
  waits for the replicas seen in the last 2 minutes. A replica that was
  stopped empties its cache when it starts, if a request deleted files
  since. A cache on disk keeps its identity in a file in its directory, so
  a container made again on the same volume (an image upgrade, a changed
  setting) keeps its cache. A copy of a cache's disk, as with a VM image,
  is told apart all the same: one running replica at a time uses a cache's
  identity, and each emptying gives the disk a new token that the erasure
  ledger records, so a copy that was not emptied with it takes a new
  identity and is emptied.
- Requests are in the metadata database (schema `igloo_erasure`) and
  survive restarts; without one, in memory.
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
  kernel does not kill the process. The rest of Igloo (the SQL endpoints,
  the catalog proxy, [table maintenance](#compaction-memory)) runs in the
  same process, in Go. Igloo limits Go's memory to what the container's
  memory limit, or else the machine's memory, leaves after DuckDB's limit
  and a reserve of a twentieth (at least 256 MiB), but to no less than
  256 MiB; it logs the division at startup (`memory`), and warns when the
  limits add up to more than there is. Set `GOMEMLIMIT` to choose Go's
  limit yourself. If other services share the machine, as the bundled
  Postgres, catalog and object store of the single-machine stack do, lower
  `IGLOO_DUCKDB_MEMORY_LIMIT` by what they use.
- **Running out of memory.** DuckDB keeps to its limit only for the memory
  its buffer manager tracks; some of its work does not (a reservoir sample,
  `USING SAMPLE 1000000 ROWS`, of a wide table can take many times the
  limit). So Igloo watches what it can still allocate, five times a
  second: what the container's memory limit leaves of its working set, and
  the machine's available memory. When that falls below an eighth of
  Igloo's memory (at least 512 MiB), Igloo first lowers DuckDB's memory
  limit, down to half of `IGLOO_MEMORY_LIMIT` at most, so that DuckDB
  writes buffers to its spill files and gives their memory back: under
  heavy work DuckDB keeps its tracked memory at its limit and uses more
  besides, 8 GiB more for one statement on a 61 GiB machine. It raises the
  limit back a step at a time once memory has stayed plentiful for 30
  seconds, and logs `memory is running low; lowered DuckDB's memory limit
  so that it spills` when it starts. Should memory still fall below a
  sixteenth of Igloo's memory (at least 256 MiB), Igloo cancels the statement that has
  run longest, which fails with SQLSTATE `53200` (`out_of_memory`), rather
  than let the kernel kill the process and every session with it. It logs
  `memory is running out; canceled the statement that had run longest`
  with the user and session, and cancels another only if memory has not
  come back within 15 seconds, or keeps running out meanwhile.
  A statement in a step of DuckDB that does not check for interruptions
  goes on after it is canceled, holding and taking memory, and the machine
  can run out of memory so slowly, paging, that the kernel never kills
  anything and the machine stops answering. So a canceled statement still
  running 10 seconds later, with memory still low, is interrupted again
  (`a statement canceled for memory has not stopped; interrupted it
  again`), and if memory then falls below a sixty-fourth of Igloo's
  memory (at least 64 MiB), Igloo gives up on it: it fails every other
  statement in flight with SQLSTATE `57P01` (`admin_shutdown`), saying
  Igloo is restarting and to run the statement again, logs `a statement
  canceled for running out of memory did not stop, and memory is nearly
  gone` with the session, the user, a hash of the statement's text, how
  long it ran and was canceled for and how far DuckDB says it got, counts
  it in `igloo_memory_exits_total`, and exits with status 75, for its
  orchestration (Docker's restart policy, Kubernetes) to start it again.
  A canceled statement can also return while DuckDB's other threads are
  still in such a step for it, taking memory no running statement holds.
  So when memory is below that floor with no statement running, within
  5 minutes of the watch canceling one, Igloo first frees what it can (Go's
  free heap, and DuckDB's caches, by asking DuckDB for a 256 MiB memory
  limit and then restoring it), logging `memory is nearly gone and no
  statement is running`; if memory is still below the floor 10 seconds
  later, with still nothing running, it exits the same way, logging
  `memory is nearly gone with no statement running` with the session,
  user and statement hash of the statement it canceled last. A statement
  that stops when canceled, and lets DuckDB stop, never leads to either.
- **Spill disk.** Queries that need more memory than the limit write
  temporary files to `IGLOO_DUCKDB_TEMP_DIR`. Give it fast local disk, local
  NVMe where the machine has it ([spill on local NVMe](#spill-on-local-nvme)),
  with room for your largest sorts and joins, and give each Igloo process its
  own ([spill directory](#spill-directory)).
- **CPU.** DuckDB parallelizes each query across `IGLOO_DUCKDB_THREADS`
  threads (default: two per core, four on workers, since reading object
  storage waits on requests more than on cores: cold reads ran two to four
  times faster on four threads a core than on one). A workload that reads
  from a warm read cache and is bound by its cores runs as fast or a little
  faster on one thread a core (a report took 15.9 s on 16 threads and
  17.2 s on 32, on 16 cores); set `IGLOO_DUCKDB_THREADS` to the core count
  for one that is. Threads past one a core each need 2 GiB of DuckDB's
  memory limit, or there are fewer of them, though never fewer than the
  cores: every thread keeps its own hash tables, and on 8 cores with a limit
  of 18.6 GiB, two reports of `COUNT(DISTINCT)`s over 93 million rows took
  120–139 s on 16 threads, spilling, and 46–51 s on the 9 the limit allows.
  Fewer threads also read object storage cold more slowly; where cold scans
  matter more than such aggregates, set `IGLOO_DUCKDB_THREADS`.
- **Cache disk.** The [read cache](#read-cache) keeps table files on local
  disk. Size the disk so your frequently read tables fit in half of its free
  space, or set `IGLOO_CACHE_SIZE`.

A memory-optimized machine with 4 to 8 cores and 32 to 64 GB handles most
datasets up to a few hundred GB of compressed Parquet comfortably. To size
one client's or team's compute apart from everyone else's, give them a
[compute pool](workload-management.md#compute-pools).

### Spill directory

DuckDB deletes its spill files as Igloo stops. When it does not get to,
because Igloo is killed (for running out of memory, or by Docker or
Kubernetes after the stop grace period) or crashes, the files stay, and
Igloo deletes them the next time it starts, before it runs any query. The
log says
`deleted DuckDB temporary files an earlier run left in the spill directory`,
with how many files and `bytes_freed`. Igloo deletes only the files DuckDB
spills to, named `duckdb_temp_storage_*` and `duckdb_temp_block-*`, at the
top of the directory, and the directories `igloo-sorted-write-*` where a
[write of rows in order](sql.md#writing-rows-in-order) keeps them until it
has written them, and leaves everything else.

Give each Igloo process (each replica, compute pool and worker) a spill
directory of its own, as the single-machine stack and the Helm chart do.
DuckDB gives its files the same names in every process, so two processes
spilling to one directory would write to the same files, and neither
could tell the other's files from those of a run that has ended. While
it runs, an Igloo process holds a lock on `igloo.lock` in the directory
(an `flock(2)` lock, which holds between containers on one machine that
mount the same volume). One that starts while another holds it deletes
nothing and logs `another process is using the spill directory`.

### Spill on local NVMe

Queries larger than DuckDB's memory limit, such as the big joins,
aggregations and sorts of heavy reports, write spill files and read them
back: often tens of GiB, sometimes hundreds. The disk under
`IGLOO_DUCKDB_TEMP_DIR` then sets the query's speed. Network block storage
is slow at this: a gp3 volume gives 3,000 IOPS and 125 MB/s unless you buy
more, a 128 GiB Azure Premium SSD 500 IOPS and 100 MB/s, and a Google
Cloud persistent disk's throughput grows only with its size. A local NVMe
SSD on the host is many times faster and costs little or nothing extra, so
put the spill directory on one wherever the machine has one.

Spill files are temporary. Each lives for one query, and Igloo deletes any
an earlier run left when it starts. Disks that are emptied when the machine
stops or is replaced (an EC2 instance store, a Local SSD, an Azure temp disk
or ephemeral OS disk) are therefore fine for them.

| Cloud | Machines with local NVMe | How the deployments use it |
|---|---|---|
| AWS | Types with a `d` after the generation (`r7gd`, `m7gd`, `c7gd`, `r8gd`, `m6id`) and storage-optimized types (`i4i`, `i4g`, `i8g`) | [deploy/aws](../deploy/aws/README.md#spill-on-local-nvme) defaults to `r7gd.xlarge`. For node types with instance store it sets nodeadm's `localStorage` strategy to `RAID0`, which moves kubelet's and containerd's directories onto the disks, so the spill `emptyDir` is on NVMe. |
| Google Cloud | Series with Local SSD: C3, C3D, C4, C4A and C4D (their `-lssd` machine types) and Z3; N2, N2D and N1 with Local SSDs attached | GKE Standard node pools with Local SSD-backed ephemeral storage (`--ephemeral-storage-local-ssd`) keep `emptyDir` volumes on it. [deploy/gcp](../deploy/gcp/README.md#spill-on-local-ssd) runs Autopilot, whose general-purpose class has none: `local_ssd_machine_family = "c3"` runs the igloo pods on that series with Local SSD-backed ephemeral storage, billed per node. |
| Azure | Sizes with a `d` after the vCPU count: a local NVMe disk on v6 sizes such as `Standard_E4pds_v6`, a local SSD on v5 sizes such as `Standard_E4ads_v5`; the L-series | [deploy/azure](../deploy/azure/README.md#spill-on-local-nvme) defaults to `Standard_E4pds_v6` and gives sizes with a local disk an ephemeral OS disk on it, which holds kubelet's directory and so the spill `emptyDir`. |
| One machine | Any of the above, formatted and mounted, e.g. at `/mnt/nvme` | Docker Compose keeps spill files in the `igloo-spill` volume, in Docker's data directory. Set `IGLOO_SPILL_DIR` in `.env` to a directory on the local disk owned by the user Igloo runs as (`sudo install -d -o 65532 -g 65532 /mnt/nvme/igloo-spill`), and `IGLOO_REPORTING_SPILL_DIR` to another one for the second compute pool of `compute-pool.yml`. An instance store comes back blank after the machine stops, so format and mount it and create the directories at boot, before Docker starts the stack (a systemd unit or cloud-init can). |

On Kubernetes the chart's spill volume, and the workers' spill and read
cache volumes, are `emptyDir` volumes, which kubelet keeps in its own
directory on the node: the node's setup decides the disk, not the chart.
Size the disk for every Igloo process that shares it, and on Kubernetes
for the container images too: each process spills up to
`IGLOO_DUCKDB_MAX_TEMP_SIZE` (in the chart, 90% of
`duckdb.spillVolume.sizeLimit`), and a query that would need more fails
with an error instead of filling the disk.

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

## Concurrency on a replica

A replica serves many sessions at once. Reads and writes of table data run
fully in parallel — each uses DuckDB's threads, and any number run side by
side — because none of them holds the catalog while it runs. What is
serialized on each replica is only the short work that reaches the catalog's
own structure, so that a table is never listed or resolved while it is being
created, renamed or dropped there:

- Preparing a statement other than a query (resolving the tables it names
  and checking it),
  listing the catalog (`information_schema`, `pg_catalog`,
  `duckdb_tables()`/`duckdb_views()`, `SHOW`, `DESCRIBE`, `PRAGMA`), and DDL
  (creating, renaming, dropping or altering a table, view or schema) take the
  catalog to themselves while they run.
- Binding a statement again as it starts to run, which DuckDB does on every
  run of a statement that reads the catalog, shares it with other sessions
  doing the same, but not with the steps above. So does preparing a query
  (one `SELECT`, outside a transaction, that calls no catalog function):
  binds are safe side by side. A query that would list the catalog as it is
  prepared, as Igloo's checks for some users do and as DuckDB does to
  suggest similar names for a table that does not exist (one another engine
  just dropped, say), is refused that listing (the catalog proxy refuses
  listings, drops and renames while the catalog is shared), and is
  prepared again on its own. `igloo_shared_prepares_total` counts both;
  `IGLOO_SHARED_PREPARES=false` prepares every statement on its own.
- Everything else — reading table data once the statement is bound, and
  `INSERT`, `UPDATE`, `DELETE` and `MERGE` writing and committing a new
  snapshot of the tables bound — holds nothing.

Opening a session shares the catalog too, as a bind does: it selects the
catalog's default schema. A login waits at most 30 seconds for a listing or
DDL that has the catalog to itself, and then fails with SQLSTATE `57P03`,
saying how long the catalog has been held, rather than waiting with it.

These serialized steps are brief (preparing or binding a statement, a
listing, most DDL), and they do not wait for data queries to finish, nor data
queries for them beyond their bind. So a `dbt` run swapping tables and a BI
tool browsing schemas take turns with each other and with other sessions'
statement preparation, for a moment each, while long reads and writes run
alongside them undisturbed. `CREATE [OR REPLACE] TABLE ... AS` of a catalog
table runs in steps for that reason: it creates the new table, empty, under
a temporary name, fills it with an `INSERT`, which holds nothing, and renames
it into place, so only the create and the rename take the catalog, and the
table appears under its name once it is complete. Other DDL holds the catalog
for its whole run, and so does `CREATE TABLE ... AS` inside a transaction
block or with parameters.

The turns cost throughput of short queries on a busy replica. On a shared
18-core test machine, against the same build without them, 8 to 64 sessions
of point lookups ran 18–25% fewer queries a second (with lower tail
latency), one session and TPC-H queries at 1 and 8 sessions were within
noise, and 8 sessions of point lookups ran about half as many queries while
a `dbt`-style loop swapped a small table, and about a third as many while it
rebuilt a 1.5-million-row table, as fast as it could — the loads in which
the build without them crashed. (That last was measured while `CREATE TABLE
... AS` held the catalog for its whole run; it now holds it only to create
and rename.) This keeps the embedded engine's one shared
view of the catalog consistent under concurrency. Changes made on another
replica, or by another engine, reach this one through the Iceberg REST
catalog within the 30-second view sync, and this serialization, which is per
replica, does not cover them.

A listing has the catalog for as long as DuckDB takes to list every attached
catalog, connections' included, which grows with their columns. On an
m7g.2xlarge with 900 tables of 5 to 40 columns, a lookup in
`information_schema.tables` or `.columns` took 30 to 55 ms, so lookups ran
about 35 a second however many sessions sent them; with a Snowflake
connection of 940 tables of about 100 columns attached as well, a lookup
took about 110 ms and they ran about 11 a second.

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


### Prefetch

Reading object storage cold waits on requests, not on cores: each DuckDB
thread reads a row group's column chunks a request round after another, so
how fast a cold scan reads follows the threads, which Igloo caps by memory
(see [sizing](#sizing)). With `IGLOO_PREFETCH=true`, Igloo reads ahead, into
the read cache, the blocks of the data files a statement's scans are about
to read, with `IGLOO_PREFETCH_CONCURRENCY` (256) requests in flight for all
statements together, which take them in turn; DuckDB then reads the blocks
from local disk. Against an object store with S3's latency, a cold scan on
12 threads with prefetch ran 1.9 times as fast as on 32 threads without.

- It reads each file's footer, then the blocks of the columns the statement
  reads (none for a scan that reads no column, as `COUNT(*)`'s), in the
  order the scan reads the files, and keeps at most a quarter
  of the cache's room for blocks DuckDB has not read yet, so they do not
  evict each other or other statements' blocks first.
- It tells the blocks DuckDB read by their access times. On a filesystem
  mounted `noatime`, which keeps access times still, it tells them by the
  kernel's events of opens of their files (inotify, on Linux), and the log
  says so at startup. Where neither works, it cannot tell, and says so with
  a warning: each statement then reads ahead at most 256 MiB in all, and
  never more than its share of the cache's free room, so reading ahead
  evicts nothing; with a full cache it reads nothing ahead.
- A block it has not fetched yet, or fails to, DuckDB fetches itself:
  prefetch only ever costs requests. Overfetch, blocks it read that the
  scans did not, shows in `igloo_prefetch_unread_bytes_total`.
- It reads ahead for queries and for the queries of `CREATE TABLE ... AS`
  and `INSERT ... SELECT` that read catalog tables, other than tables that
  policies or a clean room protect, when the statement runs on the replica:
  for a statement [workers](distributed-execution.md) run, they read ahead
  each task's table files, as their own `IGLOO_PREFETCH` says, and the
  replica reads nothing ahead; exchanges' files, which the read cache
  leaves out, are not read ahead. The scans are those distributed
  execution would plan; until filters prune their file lists, a scan of a
  table the statement filters, or a task's of a table its part filters,
  reads ahead at most 256 MiB, as does one of a table semi-joined, or
  inner-joined to a relation such as an audience, whose keys DuckDB skips
  row groups by. A session turns it off
  for itself with `SET igloo_prefetch = off` (`RESET`, `SHOW igloo_prefetch`).
- It needs the read cache, on a filesystem with extended attributes; it
  writes blocks as the cache's extension does. At startup the log says
  `prefetch on`, or why it is off.

## Result cache

A dashboard that several people open at once, or that asks the same
question in several of its charts, sends the same query again and again
over data that has not changed. Igloo keeps the result of such a query in
memory and sends it again, without running the query, to the next
statement that is the same text with the same parameters, from the same
user with the same roles and settings, as long as no table has changed
since. A statement that arrives while the same one is running waits for
that run's result instead of running too. Snowflake does the same
(`USE_CACHED_RESULT`).

- **What is kept.** Results of queries that read tables of the catalog,
  through views or not, and nothing else: not DuckDB's own tables,
  temporary tables, files, connections' tables or table functions such as
  the history. Queries that call a function whose value changes from one
  run or session to the next (`now()`, `current_date`, `random()`,
  `uuid()`, `current_setting()`, `getvariable()`, or a macro that calls
  one) are run every time, and so are statements in a transaction block.
  The statements of a session that may have temporary tables or views
  share no results with other sessions, since the same name may stand for
  something else there.
- **Always current.** DuckDB loads every table a statement reads when it
  prepares the statement, through Igloo's [catalog
  proxy](#writes-when-the-catalog-fails), which sees each table's metadata
  location, and each change Igloo makes. Once a table may have changed,
  through this replica, another replica or another engine, no result kept
  before is used again, and a result is kept only if no table changed
  while its statement ran. A change to the catalog's views or to policies
  makes the results kept before unused too.
- **Size.** Results take at most `IGLOO_RESULT_CACHE_SIZE` of memory, 256
  MiB unless set, each at most a sixteenth of it; the least recently used
  go first. `0` turns the cache off.
- **Turning it off.** A session runs every statement with `SET
  igloo_result_cache = off`, or Snowflake's `ALTER SESSION SET
  USE_CACHED_RESULT = FALSE` (`RESET igloo_result_cache`, `SHOW
  igloo_result_cache`).
- **Before preparing.** Queries are prepared side by side (see
  [concurrency on a replica](#concurrency-on-a-replica)), other statements
  one at a time. An admin's query whose result the cache kept, with no parameters, no
  policy that applies to it and nothing read from a connection, is also
  known by its text: the next one of the same text, user, roles, dialect
  and settings, with the catalog's tables, views and policies as they
  were, is answered without being prepared. The tables it read are loaded
  from the catalog again first, all at once and holding no lock, so a
  change made through another replica or engine shows as it would when
  preparing it.
- **In the history.** A statement answered from the cache is recorded with
  0 for `cpu_time_ms`, `bytes_read` and `memory_allocated_bytes`;
  `igloo_result_cache_total` counts them.

## Connections to other databases

[Connections](federation.md) to PostgreSQL, MySQL and Snowflake databases are kept in
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
  the files of statements that fail or roll back before they commit. A
  `COPY INTO` or pipe that fails this way marks the files it was loading
  `unknown` in the [load history](loading.md#loading-each-file-once), so
  that it does not load them again.

The log (component `catalog-proxy`) records each commit sent again and each
outcome confirmed after an error. Catalogs that ignore `Idempotency-Key`
get the same guarantee for writes that change rows; a schema change whose
answer was lost may then fail even though it was applied.

The proxy also keeps the catalog's answers to listings (of its namespaces,
and of their tables and views) for up to 10 seconds. DuckDB lists the
catalog again for every statement that looks a table up, and with hundreds
of tables each listing cost tens of milliseconds; so did the checks of
every statement of a user without the reader role. Changes made through
this replica, and through its Iceberg REST endpoint, apply at once. A table
another replica or engine creates or drops is found at once by a query that
names it, since DuckDB looks up a name its listing lacks; listings
(`SHOW TABLES`, `information_schema`) show it within 10 seconds.

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
  in psql, `Statement.cancel()` in JDBC, context cancellation in Go). The
  request, the statement timeout and the memory watch end a statement while
  it is still prepared too, which for a report whose CTEs become report API
  calls can take seconds; it then does not run.
- `IGLOO_MAX_CONNECTIONS` caps sessions per replica; further logins fail with
  SQLSTATE `53300`. Connections that have not signed in yet are limited too:
  as many may be signing in at once (at least 100), half of them from one
  public client address (not from a loopback or private one, which a
  gateway, pooler or load balancer gives all its clients), and each has `IGLOO_LOGIN_TIMEOUT` (30 seconds) to sign in
  before it is closed.

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
| `igloo_prefetch_bytes_total`, `igloo_prefetch_blocks_total` | | Bytes and blocks the [prefetcher](#prefetch) read ahead into the read cache |
| `igloo_prefetch_unread_bytes_total` | | Bytes it read ahead that the scans did not read (overfetch); not counted where it cannot tell which blocks DuckDB read |
| `igloo_prefetch_present_blocks_total`, `igloo_prefetch_errors_total` | | Blocks it found cached already, and reads ahead that failed (the scans then read the block themselves) |
| `igloo_id_domain_ids_assigned_total` | `domain` | Ids [ID domains](sql.md#id-domains) gave new values |
| `igloo_id_domain_rows_filled_total` | `domain` | Rows of member tables whose ids were filled in after they were written |
| `igloo_id_domain_assignments_skipped_total` | `reason` | Assignments left to maintenance: `busy` (another writer held the domain's lease) or `exhausted` (every id below 2^32 handed out) |
| `igloo_report_calls_total` | `path` = `index`, `index+scan`, `ids`, `scan` | [Report API](media-analytics.md#report-api) calls prepared, by how they count: from bitmap indexes, people from them and the rest from a scan, through ID domains' ids, or from the tables' values; `ids` and `scan` are both `scan` in the calls' `path` column |
| `igloo_report_ctes_recognized_total` | | Common table expressions of reports that `IGLOO_REPORT_REWRITE` answered through the report API ([reports as written](media-analytics.md#reports-as-written)) |
| `igloo_report_rewrite_fallbacks_total` | | Statements whose recognized reports failed to prepare and were prepared as written instead |
| `igloo_bitmap_index_statements_total` | | Statements prepared whose counts of distinct members in SQL read [bitmap indexes](sql.md#bitmap-indexes) instead of the tables (report API calls count in `igloo_report_calls_total`) |
| `igloo_memory_exits_total` | | Times Igloo exited because a statement it canceled for running out of memory did not stop, or DuckDB went on with its work after it did, and memory was nearly gone (see [sizing](#sizing)) |
| `igloo_snowflake_implicit_conversions_total` | | Statements in [Snowflake's dialect](snowflake-sql.md#implicit-conversions) that DuckDB could bind only with Snowflake's implicit conversions, which Igloo then made |
| `igloo_sorted_writes_total` | | Writes of rows in order too large to sort at once within `IGLOO_SORT_SPILL_BUDGET`, which Igloo sorted [a range of the order at a time](sql.md#writing-rows-in-order) |
| `igloo_aggregate_passes_total` | | Tables `CREATE TABLE ... AS` filled in passes over its aggregate's groups, which would not have fit in memory in one ([aggregates too large for memory](sql.md#aggregates-too-large-for-memory)) |
| `igloo_result_cache_total` | `outcome` = `hit`, `joined`, `missed`, `kept`, `unprepared` | Queries the [result cache](#result-cache) answered, answered with the result of the same query another session was running, or could not answer, and results it kept; `unprepared` counts the hits and joins answered before preparing the statement |
| `igloo_result_cache_bytes` | | Memory the result cache's results take, as Igloo estimates it |
| `igloo_statements_waiting` | | Statements waiting for their [role limits](workload-management.md#role-limits) |
| `igloo_statement_wait_seconds_total` | | Time statements spent waiting for their role limits |
| `igloo_notifications_total` | `type`, `result` = `sent`, `failed`, `rate_limited` | [Notifications](alerts.md) sent or tried |
| `igloo_distributed_*` | | [Distributed execution](distributed-execution.md#seeing-it): statements run on workers, fallbacks, tasks, workers available |

Logs go to stderr, as text or JSON (`IGLOO_LOG_FORMAT=json`). Security-relevant
events are logged at info level or above: `login`, `login failed`,
`user changed`, `grants changed`, `role settings changed`, `job changed`
(tasks, pipes, materialized views and alerts created, changed or dropped),
`notification integration changed`, `erasure requested`, `erasure request
completed`, `erasure request held`, `erasure request confirmed`, `erasure
request canceled`, `erasure request failed`, `erasure request cannot go
on`, `erasure request closed`, `data retention changed`,
`statement denied`, `statement canceled` (from the web console's compute
page), and refused connections.
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
   512 MB by default), with deletes applied, one table at a time and within
   a [memory budget](#compaction-memory). A pass rewrites at most 8 GB per
   table; larger backlogs finish over several passes. The new files are laid
   out the way DuckDB writes Parquet, so compaction does not make scans
   slower: Snappy compression, decimals stored as integers, and dictionary
   encoding only for columns in which at most a fifth of the values are
   distinct, but in [row groups](#row-groups) of about 16 MiB. A table's
   `write.parquet.compression-codec` and `write.parquet.row-group-limit`
   properties override the codec and row group size. File size does not
   limit parallelism: DuckDB divides a scan by row group, so a table in a few
   large files still uses every core.
   A table Igloo creates gets that target from the start (Igloo sets
   `write.target-file-size-bytes` when it creates the table, unless the
   statement did), so a large insert writes files of about that size.
   Without one, DuckDB writes a file per row group, a few megabytes each for
   narrow rows. Tables made by other engines keep what those engines set;
   change one with `set_iceberg_table_properties`.
   A table with a [clustering key](sql.md#sort-orders-and-clustering) is
   [clustered](#clustering) instead.
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
longer ago, so they leave the files too. Branches and tags, and the
snapshots they point at, are kept whatever the retentions, as long as the
ref's or the table's `max-ref-age-ms` says, forever by default.

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
Each table's compaction is logged as it starts (`compacting`, with the
files, bytes and memory budget) and ends (`compacted`); clustering logs
`clustering` and `clustered`. Set
`IGLOO_MAINTENANCE=false` only if another tool maintains the tables.

### Clustering

A table with a clustering key (`ALTER TABLE ... SET SORTED BY`, or
Snowflake's `CLUSTER BY`) is clustered instead of compacted, so that a
filter on the key's first column reads few files: Iceberg's manifests record each file's
range of every column, and a scan skips the files whose range cannot hold
the value. On 1.4 billion rows written unsorted, a filter on one value of
the first column read 140 files; clustered, 3, and it took 0.65 s instead
of 2.3 s with nothing cached.

Each pass rewrites, sorted by the whole order and with deletes applied:

- the files whose ranges of the first column overlap another file's (ranges
  that only share a bound, as a sorted rewrite leaves them, do not count);
- runs of small files (under 3/4 of the target size) that are next to each
  other in the order, which merging keeps apart from the rest;
- files with many deletes, as compaction rewrites them.

It rewrites them in islands, runs of such files with no other file between
them in the order, each into its own sorted files of the target size, so the
new files overlap none of those it left. A clustered table is not rewritten
again until writes add files that overlap it. A pass rewrites at most 8 GB
per table, island by island.

Rewriting an island rewrites every file in it, so a pass does so only when
that is cheap next to the bytes in it that overlap (at most 4 times as many,
or two files of the target size): a table written unsorted, or an insert
that falls within a file or two, as an insert of one audience does. An
insert whose keys are spread over the whole table overlaps every file, and
rewriting the table around it would rewrite all of it for a few new rows.
Such fresh files (files clustering did not write) wait instead, though
every filter on the key reads each of them. Once they stack more than 8
deep, take 5% of the table or 4 GB (whichever is less), or number 100,
they are sorted together, alone, into a run of level 0, which overlaps the
clustered files but not itself. Runs are tiered, as log-structured stores
tier theirs: 4 runs of a level are sorted together into a run of the next,
4 times as large. Each run deepens the table by one, so a filter on the key
reads at most 3 runs of each level beside the fresh files, and a row is
rewritten once a level. Once the runs have grown to a quarter of what they
overlap, rewriting the island is cheap again, and passes merge them into
the clustered files, island by island. Clustering puts the files it writes
in a directory of their own under the table's data
(`data/clustered-<hash of the key>/`, and a run's under `L<level>/<run>/`
there), which is how it knows them.

Sorting does not need the table in memory. Clustering samples the order's
columns, splits their values into ranges small enough to sort within the
[memory budget](#compaction-memory), and reads the rows once, appending each
to its range's spill file (compressed Arrow) under `IGLOO_DUCKDB_TEMP_DIR`
(or the system's temporary directory); then it sorts the ranges one at a
time, in order, into files of the target size. A value of the order that
more rows share than a range holds (all the members of one large audience,
say) is spread over several ranges, and so over several files whose ranges
only touch. The spill takes about as much disk as the files it rewrites, and
is deleted as each range is written. The new files record exact bounds of
the first column.

If clustering a table still needs more memory than the budget, maintenance
logs a warning and compacts the table unsorted for that pass, as it compacts
tables without a clustering key, so that small files and deletes do not pile up;
it tries clustering again once the table changes, or six hours later.
`igloo_maintenance_clustering_fallbacks_total` counts these passes.

A table's clustering depth is the average, over its files, of how many
files hold a value of the key's first column where the file's range stacks
deepest: 1 when no two files overlap. `igloo_maintenance_clustering_depth`
and `igloo_maintenance_clustering_max_depth` give each clustered table's
average and largest, as its last pass found them, and `SELECT
SYSTEM$CLUSTERING_DEPTH('table')` works it out when asked (see
[SQL](sql.md#sort-orders-and-clustering)).

Each table's clustering is logged as it starts (`clustering`, with the
files, bytes, groups, `overlapping_files`, `depth` and `max_depth`) and
ends (`clustered`, with `overlapping_files_before` and
`overlapping_files_after`); `igloo_maintenance_clustered_files_total` and
`igloo_maintenance_clustering_output_files_total` count the files replaced
and written. Partitioned tables, and sort orders with transforms (`bucket`,
`day`, ...) or nested columns, which other engines can set, are compacted as
before.

### Row groups

A cold scan reads a Parquet file a row group at a time, with requests of
its own for each, so DuckDB's row groups of 122,880 rows, a few hundred
kilobytes for narrow rows, make cold scans slow. Compaction and clustering
write row groups of about 16 MiB compressed instead, as many rows as that
takes at the bytes a row of the files they rewrite, between 122,880 and
16,777,216 rows (fewer if the [memory budget](#compaction-memory) needs).
On a 2.4 GB table of narrow rows, a cold scan took 24 to 29 s in row groups
of 0.3 MiB and 7 s in row groups of 11 MiB, and a report over the table
44 s and 21 s; warm, they took about the same. Larger row groups gained
scans nothing more, and made a filter on a sorted column read more: up to
twice as much at 90 MiB as at 15.

Most of a table's rows are written by inserts, though, which DuckDB writes
in row groups of the table's `write.parquet.row-group-size` rows, 122,880
by default. So Igloo marks the tables its SQL creates (property
`igloo.row-groups`), and maintenance sets their
`write.parquet.row-group-size` from the table's bytes a row as compaction
would, once the table has rows, and again when that size moves by more
than 15%. DuckDB holds a row group, uncompressed, on each of its threads
while an insert writes, so maintenance sets no more rows than fit in
`IGLOO_MAINTENANCE_MEMORY` over `IGLOO_DUCKDB_THREADS` (and never fewer
than 122,880). Your own large inserts into such a table therefore hold
more memory than before, up to about `IGLOO_MAINTENANCE_MEMORY` more across
DuckDB's threads: on 16 threads, an insert of 832 million narrow rows took
the container to 6.7 GB in row groups of 122,880 rows and to 14.6 GB in
row groups of 4 million. Lower `IGLOO_MAINTENANCE_MEMORY` if they need that
memory back, or set a table's `write.parquet.row-group-size` yourself:
maintenance leaves alone a size it did not set, as it does tables other
engines created. The rows a table gets before maintenance first sizes its
row groups, such as those of the `CREATE TABLE ... AS` that loads it, keep
DuckDB's row groups until compaction or clustering rewrites their files.

### Compaction memory

Compaction decodes the files it rewrites into memory, where a row takes far
more room than in Parquet: a repetitive string that Parquet stores in a byte
or two takes its full length, so a 30 MB file can take 300 MB. Igloo
compacts a table's files one at a time and sizes the batches it reads and
the row groups it writes from how much memory a row of the table takes, so
compaction holds no more than `IGLOO_MAINTENANCE_MEMORY` at once: by default
a quarter of Go's memory limit (see [sizing](#sizing)), between 256 MiB (or
half of Go's limit, if less) and 4 GiB, or 1 GiB when Igloo cannot tell how
much memory it has. Rows wide enough to need it, such as those of a table of
thousands of columns, get row groups smaller than DuckDB's 122,880 rows, down
to 8,192.

Deletes take memory too while they are applied. Where position deletes are
what does not fit, as after an `UPDATE` of every row of a large table,
whose delete files name every row, compaction applies them a file at a
time: it holds one file's deleted positions, read from the row groups of
the delete files that name that file, rather than all of them at once.

A table that cannot be compacted within the budget, because a row group of
8,192 of its rows (or one of its files' compressed row groups, or one
file's deletes) does not fit, is left as it is: maintenance reports it as a failure and tries again once
the table has changed, or after a restart. Raise `IGLOO_MAINTENANCE_MEMORY`
to compact it.

[Erasure requests](privacy.md) rewrite the files that hold a subject's rows
the same way, within the same `IGLOO_MAINTENANCE_MEMORY`. A request that
cannot rewrite a table's files within it stops for good with that reason
(`rewriting the files needs more memory than IGLOO_MAINTENANCE_MEMORY
allows`), raising the alert for failed or blocked requests, and is never
reported as completed; raise the setting and make the request again.

If Igloo dies while it rewrites a table's files (compaction, or the rewrite
that enforces a data retention), because the kernel killed it for its memory
or the machine went down, maintenance leaves that table's files alone for a
day, so the pass that starts soon after the restart cannot kill Igloo again;
the table's snapshots are still expired, and the failure is reported. Igloo
tells by recording in the metadata database, in `igloo_maintenance.runs`,
when each table's rewrite starts and ends (jobs
`rewrite/started/<schema>.<table>` and `rewrite/ended/<schema>.<table>`);
delete a table's two rows to let the next pass rewrite it sooner. A rewrite
that Igloo stops itself, as when it shuts down, counts as ended. Without
`IGLOO_METADATA_DATABASE_URL` there is no such record.

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
- **`DROP TABLE`**, and `CREATE OR REPLACE TABLE ... AS`, which drops the
  old table, ask the catalog to purge the table, which deletes everything
  under its location, only when its files may go (below); otherwise the
  table is dropped without purging, and its files stay in the bucket. A
  drop that asks for the files to go when they stay is refused: in SQL,
  and through [Igloo's Iceberg REST catalog](external-engines.md#clones)
  (409, naming the clones when they are why; 503 when Igloo cannot tell),
  through which other engines drop tables the same way. The catalog
  deletes files only when asked to purge, and only Igloo can reach it.
  Igloo's warehouse uses the catalog's default hard deletion, in which a
  dropped table cannot be restored; with Lakekeeper's soft deletion turned
  on instead, a table restored after it was dropped may miss its files,
  and a clone restored after it was dropped may miss files of its source
  that maintenance deleted meanwhile.
- **`CREATE OR REPLACE TABLE ... CLONE`** and **`CREATE OR REPLACE SCHEMA
  ... CLONE`** replace a table in the catalog without purging it, so Igloo
  deletes its files itself, with its own credentials, once the clone is
  made, when they may go: everything under the replaced table's location,
  as the catalog's purge does, and nothing elsewhere. A replaced clone's
  files go, and the files of its source that it read stay, under the
  source's location. The statement returns once they are deleted, so
  replacing a table of many files takes that much longer.

Whether a table's files may go is decided the same way for every drop
and every clone refresh. They stay when the location is outside the
warehouse or in Igloo's own files there (`.igloo/`); when another table of
the catalog is at the location, within it or around it (as the table
itself is, if it was renamed in the moment before it was dropped or
replaced); when a table's metadata says it reads files there, as every
clone's does (`igloo.clone.shares-files-of`), whether or not the record
below has it yet; when the record names a clone that reads there, or may;
and whenever Igloo cannot tell. Igloo checks the record under the lock
that making a clone of the table takes, and holds it until the drop is
through or the files are gone, so no clone starts reading them meanwhile.
When the files stay, the log says why.

What that costs: with a metadata database, the record below has every
clone, as clones are recorded when they are made and only Igloo sets
`igloo.clone.*`, and the catalog keeps live tables' locations apart, so a
drop reads the record alone, and loads only the clones it names, to see
that they are still there. Without a metadata database, until a
maintenance pass since Igloo started has visited every table, a drop
loads every table of the catalog to read what clones declare. A clone
refresh loads every table once per statement, however many tables it
replaces, to be sure none of them is still in the catalog under another
name. After restoring the metadata database from a backup, let a
maintenance pass run before dropping tables: until then the record may
miss clones made since the backup, and their sources' files would go.

Igloo knows which tables read another's files only through clones: files another engine added to a
table from a second table's location (Spark's `add_files`, say) go when
that second table is dropped. Whatever a drop leaves, as the files of a
table whose clones read them once the clones are gone too, [erasure
requests](privacy.md#what-is-left-of-dropped-tables) delete with the rest
of the warehouse's files under no table's or view's location, once three
days old: the warehouse is Igloo's alone.

Which tables share which files is recorded twice: in each clone's own
metadata (the table property `igloo.clone.shares-files-of`, the locations
it reads from) and in the metadata database (schema `igloo_clones`). Each
pass first visits every table, recording what the clones declare, so a
replica without a metadata database, or a database restored from a
backup, has the record back before anything is deleted. Deleting a table's
files then takes a lock that cloning the table takes too (in the metadata
database, so replicas share it), and reads the metadata of each of its
clones. The database releases the lock if the connection holding it
drops: a clone that lost it before its shares were recorded fails, and is
made again by running the statement again, and maintenance, or the
deletion of the files of a table a clone replaced, checks that it still
holds the lock before each thousand files it deletes, and keeps the rest
once it does not; a later pass deletes them. Whenever Igloo cannot tell what a clone needs (the metadata
database does not answer, a clone was renamed or dropped during the pass,
or was made less than an hour ago), it keeps the table's files; a later
pass deletes them.

A clone stops sharing once its own maintenance has replaced and expired
everything it read of its source. The source's files that only clones
needed are then orphans, which the orphan cleanup deletes once they are
three days old. Until then a shared file counts once in the bucket, but a
file the source no longer needs stays as long as a clone reads it. A
clone made of a snapshot that the source's compaction wrote keeps one of
the source's manifests for good, one that lists only the files the
compaction removed, so it never stops sharing: dropping the source then
keeps its files.

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
  database, and a session-level lock there, with a lease the run renews,
  keeps a job from running twice at once on any replica (see
  [Loading](loading.md#tasks)); a run that comes due while the last one
  still runs is recorded as `skipped`. A replica that dies holds its jobs
  up until their leases expire, within 30 seconds, and the next run marks
  the dead one's run `failed`.
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
| SQLSTATE `57014`, `statement queued timeout: it waited 30s to start, behind the limit of 4 statements at once for role analysts` | The statement waited for its [role limits](workload-management.md#role-limits), or [the replica's](workload-management.md#the-replicas-limit), until its `statement_queued_timeout`. Run fewer statements at once, raise the limit, or give the team a compute pool. |
| SQLSTATE `42501`, `ST_Read goes through GDAL` | GDAL-based readers and writers are refused; load spatial data as GeoParquet, shapefiles or GeoJSON ([media analytics](media-analytics.md#loading-boundaries)). |
| `CREATE TABLE` fails with `schema: data did not match any variant` | The table has a column the catalog cannot store, such as a `GEOMETRY`; store `ST_AsWKB(geom)` instead ([media analytics](media-analytics.md#loading-boundaries)). |
| SQLSTATE `53300` | The replica is at `IGLOO_MAX_CONNECTIONS`. Use a connection pool, raise the limit, or add replicas. |
| SQLSTATE `58000`, `cannot check this statement: the session's user could not be read from the metadata database for 2m0s` | The replica has not reached the metadata database (`IGLOO_METADATA_DATABASE_URL`) for longer than `IGLOO_ACCESS_MAX_STALENESS`, so it cannot tell whether the user, their grants or the policies changed. Statements run again once it can; check the database and the network to it. |
| SQLSTATE `57P03`, `cannot open a session: waited 30s for the catalog lock, which a statement listing or changing the catalog has held for ...` | A listing or DDL has had the catalog to itself for that long ([concurrency on a replica](#concurrency-on-a-replica)); logins wait for it no longer. Try again; if it lasts, find the statement among those running in the console and cancel it. |
| Log says `crashed outside Go code, in DuckDB or an extension; exiting`, and the replica restarts | DuckDB crashed on one of its own threads. Igloo exits with status 2 so that Docker or Kubernetes restarts it, rather than keep a session that will never finish; the goroutine dump that follows in the log shows the statements that were running. |
| Log says `igloo: a C++ exception escaped DuckDB or an extension (...); exiting`, and the replica or worker restarts with status 134 | An exception nothing caught reached C++'s terminate, often while DuckDB cleaned up after running out of memory (the type in parentheses, mangled, says which: `N6duckdb20OutOfMemoryExceptionE`). Igloo exits at once rather than abort, which left workers hung; look at the memory warnings before it, and lower the statements' memory or raise the limit. |
| Startup fails with `warehouse already exists in bucket ...` | `IGLOO_WAREHOUSE_LOCATION` does not match the existing warehouse. Point it at the existing data or use a different `IGLOO_CATALOG_WAREHOUSE`. |
| Startup retries `catalog not reachable yet` | The catalog or its database is not up. Igloo waits up to 3 minutes. |
| `/readyz` returns 503 | The body names the failing dependency: engine, metadata database or catalog. |
| Log says `read cache off: cannot load the cache_httpfs extension ... Read-only file system` | The root filesystem is read-only and `/tmp` is not writable. Mount a small writable volume at `/tmp`. |
| Log says `read cache off: its directory is not writable` | Make the volume at `IGLOO_CACHE_DIR` writable by the container's user (UID 65532), e.g. with `fsGroup: 65532` on Kubernetes. |
| `IO Error: Could not write file ".../duckdb_temp_storage_DEFAULT-4.tmp": No space left on device`, or `Out of Memory Error: failed to offload data block` | The spill disk is full, or the statements running at once spilled `IGLOO_DUCKDB_MAX_TEMP_SIZE` between them. Give the [spill directory](#spill-directory) more room, or run the largest statements on [workers](distributed-execution.md) or a [compute pool](workload-management.md#compute-pools) of their own. Igloo deletes earlier runs' spill files when it starts, but not other files on the disk. |
| Log says `read cache paused` | Less than 5% of the cache's disk is free. Free space or give the cache its own volume; it resumes on its own. |
| A task, pipe or `COPY INTO` does not do what you expect | See [loading troubleshooting](loading.md#troubleshooting). |
| A large query did not run on workers | `SET distributed_execution = 'on'` and run it again with `IGLOO_LOG_LEVEL=debug`: the log says why it ran on the replica (for example a view that groups its rows, or an aggregate that does not combine). See [what runs distributed](distributed-execution.md#what-runs-distributed). |
| Log says `workers could not run a statement; running it here` | Workers failed or could not be reached (`igloo_distributed_fallbacks_total` counts why). The statement still completed on the replica. Check the workers' logs and that they share `IGLOO_WORKER_SECRET` and Igloo's version. |
| Log says `worker runs another version of Igloo; not using it` | Workers must run the same image as the server. Upgrade them together. |
