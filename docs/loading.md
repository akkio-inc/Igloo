# Loading and scheduling

Igloo loads files from object storage and keeps tables fresh on a schedule
by itself, with no external scheduler:

- **`COPY INTO`** loads CSV, TSV, Parquet and JSON files, compressed or
  not, from a bucket into a table, and remembers what it loaded so that a
  file is never loaded into a table twice.
- **Pipes** run a `COPY INTO` every minute (or on a schedule), so files
  that land in a bucket are loaded without anyone running anything.
- **Tasks** run a SQL statement on a schedule: an interval or a cron
  expression, in any time zone.
- **Materialized views** and **dynamic tables** keep a query's result as a
  table, refreshed on demand, on a schedule or within a target lag, from
  what changed in the tables it reads when the query allows.
- **[Alerts](alerts.md)** run a query on a schedule and tell Slack, Teams,
  email or a webhook when it returns rows; built-in alerts report failed
  runs, failed loads and pipes that stop receiving files.

Everything runs inside Igloo, as the user who created it, with that user's
privileges at the time it runs. With [scale to zero](scale-to-zero.md), the
gateway starts Igloo when work is due.

A typical daily pipeline: an ad platform drops gzipped CSV files into
`s3://landing/dsp/` overnight, a pipe loads them as they arrive, and a task
rebuilds the client reporting table at 6 a.m. New York time.

```sql
CREATE TABLE raw.dsp_delivery (day DATE, campaign VARCHAR, impressions BIGINT, spend DECIMAL(12,2));

CREATE PIPE dsp_drops SCHEDULE '15 minutes' AS
  COPY INTO raw.dsp_delivery FROM 's3://landing/dsp/' (PATTERN '.*[.]csv[.]gz');

CREATE TASK client_report SCHEDULE '0 6 * * *' TIMEZONE 'America/New_York' AS
  CREATE OR REPLACE TABLE marts.client_report AS
    SELECT day, campaign, sum(impressions) AS impressions, sum(spend) AS spend
    FROM raw.dsp_delivery GROUP BY ALL;
ALTER TASK client_report RESUME;
```

## Where files come from

`COPY INTO` reads the warehouse location and the prefixes in
`IGLOO_ALLOWED_LOCATIONS` (see [configuration](configuration.md#storage)),
with Igloo's own storage credentials, and nothing else: not other buckets,
not the server's disk. To load from a landing bucket, allow it:

```sh
IGLOO_ALLOWED_LOCATIONS=s3://landing/,gs://partner-drops/exports/
```

and give Igloo's identity (instance profile, IAM role for its service
account, or the S3 keys it is configured with) read access to it. In the
single-machine stack, set `IGLOO_ALLOWED_LOCATIONS` in
`deploy/single-vm/.env`; the bundled object store can hold a landing area
too, such as `s3://igloo/landing/`.

## COPY INTO

```sql
COPY INTO table [(column, ...)] FROM 'location' [(option [value], ...)]
```

The location is a prefix ending in `/` (every file under it, in
subdirectories too), a glob such as `'s3://landing/dsp/2026-*.csv'`, or one
file. Options follow in parentheses, each a name and a value; a flag alone
means `true`, and `=` between them is optional.

```sql
COPY INTO sales FROM 's3://drops/sales/' (FORMAT csv, HEADER true, PATTERN '.*[.]csv[.]gz');
COPY INTO events FROM 's3://drops/events/2026-10-16.parquet';
COPY INTO clicks (day, campaign, clicks) FROM 's3://drops/clicks/' (HEADER false, ON_ERROR skip_file);
```

| Option | Values | Default | Meaning |
|---|---|---|---|
| `FORMAT` | `csv`, `tsv`, `parquet`, `json`, `ndjson` | from each file's name | `.csv`, `.tsv`, `.parquet`, `.json`, `.ndjson` and `.jsonl`, before any `.gz` or `.zst` |
| `HEADER` | `true`, `false` | `true` | Whether CSV and TSV files start with a header line |
| `DELIMITER` | one character | detected | The CSV column separator; TSV is tab |
| `COMPRESSION` | `auto`, `gzip`, `zstd`, `none` | `auto` | `auto` goes by `.gz` and `.zst`, and reads CSV and JSON files that say nothing as text, gzip or zstd, whichever works |
| `PATTERN` | regular expression | all files | Loads only files whose path after the location matches the whole expression |
| `MATCH_BY` | `name`, `position` | `name`, or `position` for CSV without a header | How file columns map to table columns |
| `ON_ERROR` | `abort`, `skip_file`, `continue` | `abort` (`skip_file` in a pipe) | What a file that cannot be loaded does |
| `FORCE` | `true`, `false` | `false` | Load files even if they were loaded before |
| `CREATE_TABLE` | `true`, `false` | `false` | Create the table from the first file's columns if it does not exist |

`COPY INTO` returns one row per file it tried, with the file, its status
(`loaded`, `partially loaded` or `failed`), the rows loaded, the rows
skipped and the first error, and the command tag `COPY n` with the rows
loaded in all. A run that finds nothing new returns no rows.

**Columns.** By name, a file's columns go to the table's columns of the
same name, whatever their order or case; columns the table does not have
are left out, and table columns the file does not have are `NULL`. A file
that shares no column with the table fails. By position, the file's
columns go to the table's columns, or to the column list, in order, and
their number must agree. Values are converted to the table's types as
`INSERT` would convert them; CSV dialects and date formats are detected
from the file.

**Errors.** With `ON_ERROR abort`, the first file that cannot be loaded
stops the statement, which fails and loads nothing. With `skip_file`, such
files are left out and recorded as `failed`, and the others are loaded.
With `continue`, rows that cannot be read or converted are skipped and
counted, and the rest of the file is loaded (`partially loaded`); a JSON
file that is not valid JSON, or any file that cannot be read at all, still
fails as a whole.

**Creating the table.** With `CREATE_TABLE true` and no such table,
`COPY INTO` creates it with the columns and types of the first new file,
as `CREATE TABLE ... AS SELECT * FROM read_csv(...)` would, then loads.

**Transactions.** The files of one `COPY INTO` are committed together, in
one transaction (each file adds an Iceberg snapshot, which
[table maintenance](operations.md#table-maintenance) cleans up). It cannot
run inside `BEGIN ... COMMIT`. Loads into the same table, from any replica,
take turns.

### Loading each file once

Igloo records every file it loads in the load history, by table and by the
file's path, size and time of last change. `COPY INTO` leaves out files
already loaded into the table, so running it again loads only what is new,
and a pipe can check a prefix every minute without loading anything twice.

- A file whose size or time changed, such as a corrected drop uploaded
  under the same name, counts as new and is loaded again, as in Snowflake.
  Its first version's rows stay in the table.
- `FORCE true` loads files whatever the history says.
- A file that failed is tried again by the next `COPY INTO`, but not by a
  pipe: a pipe tries it again only once the file changes.
- If Igloo stops in the middle of a load, the files it was loading may or
  may not be in the table. The next load into the table marks them
  `unknown` and leaves them alone, rather than perhaps loading them twice;
  check the table, and load them with `FORCE true` if they are missing.
- So may the files of a load whose commit failed other than by conflicting
  with another writer, as when the catalog stops answering before it
  confirms the commit (SQLSTATE `40003`, see [writes when the catalog
  fails](operations.md#writes-when-the-catalog-fails)). `COPY INTO` fails
  with that error and marks its files `unknown` at once; later loads and
  pipes leave them alone in the same way.
- The history of a table goes with it: renaming the table keeps it, and
  dropping the table forgets it, so a table created again under the same
  name loads every file again. `CREATE OR REPLACE TABLE` keeps it.

Read it in `igloo.load_history`. Admins see every load; other users see
the files they loaded, and those of their pipes.

```sql
SELECT start_time, file, status, row_count, error_count, first_error
FROM igloo.load_history
WHERE table_name = 'lake.raw.dsp_delivery' AND status <> 'loaded'
ORDER BY start_time DESC;
```

| Column | Meaning |
|---|---|
| `load_id` | Identifies the attempt |
| `table_name` | The table, as `lake.schema.table` |
| `file`, `size`, `last_modified` | The file, and the version of it that was loaded |
| `status` | `loaded`, `partially loaded`, `failed`, `unknown`, or `loading` while it runs |
| `row_count`, `error_count` | Rows loaded, and rows skipped because they could not be read |
| `first_error` | Why the file failed, or why the first row was skipped |
| `start_time`, `end_time` | When the load ran |
| `user_name`, `pipe_name` | Who loaded it, and the pipe that did, if any |

Loaded files stay in the history as long as their table exists, since that
is how Igloo knows not to load them again; failed attempts are deleted after
`IGLOO_AUDIT_RETENTION`.

### Who may load

`COPY INTO` is checked like the `INSERT ... SELECT * FROM read_csv(...)` it
amounts to, one file at a time, as the user's own statements:

- Writing needs `INSERT` on the table: the writer role, or an `INSERT`
  grant on the table or its schema. `CREATE_TABLE` needs what
  `CREATE TABLE ... AS` needs.
- Reading files needs the reader role. Users with the role `none` cannot
  use `COPY INTO`, whatever their grants, as they cannot `read_csv`: files
  are not covered by grants.
- The location must be in the warehouse or `IGLOO_ALLOWED_LOCATIONS`.
  While any table has [policies](security-policies.md), users other than
  admins may not load from the warehouse, nor from a location that holds
  it: its files are the tables' own, read through the tables.

## COPY FROM STDIN

`COPY table [(column, ...)] FROM STDIN` loads rows the client sends, as in
PostgreSQL: psql's `\copy`, psycopg's `cursor.copy`, pgx's `CopyFrom` and
the `COPY ... FROM stdin` sections of a `pg_dump`, which psql loads with the
settings a dump starts with. It takes the text format (tab-separated,
`\N` for NULL, backslash escapes), CSV and binary, and PostgreSQL's options
for them: `FORMAT`, `DELIMITER`, `NULL`, `HEADER` (or `HEADER MATCH`),
`QUOTE`, `ESCAPE`, `FORCE_NOT_NULL`, `FORCE_NULL`, `ENCODING 'UTF8'`, the
older `WITH CSV HEADER` form, and `WHERE`.

```sh
psql -c "\copy sales.leads FROM 'leads.csv' WITH (FORMAT csv, HEADER)"
pg_dump --data-only --table=public.accounts appdb | psql -h igloo.example.com -d igloo
```

It is the user's own `INSERT` of the rows, checked before the client sends
anything: it needs `INSERT` on the table, and writes it as an `INSERT` does,
in a transaction block too. Values become the column's type as PostgreSQL
writes them, booleans as `t` or `on`, `bytea` as `\x...`, arrays as
`{a,b}`, intervals as `-1 days +02:00:00`; a value that does not convert
fails the whole `COPY` with its line and column, and nothing is written.
`DEFAULT` and `ON_ERROR ignore` are not supported. The rows wait in the
session's memory, spilling to disk, until the client has sent them all.

## Pipes

A pipe runs a `COPY INTO` again and again, so new files are loaded soon
after they land:

```sql
CREATE [OR REPLACE] PIPE [IF NOT EXISTS] name [SCHEDULE 'schedule' [TIMEZONE 'zone']] AS COPY INTO ...
ALTER PIPE [IF EXISTS] name SUSPEND | RESUME | SET SCHEDULE 'schedule' [TIMEZONE 'zone']
DROP PIPE [IF EXISTS] name
SHOW PIPES
```

- A pipe checks for new files every minute, or on its schedule. It starts
  at once, and its first check loads every matching file not loaded yet.
- Each file is loaded once, through the load history; a pipe skips bad
  files (`ON_ERROR skip_file`, unless the pipe says otherwise) and does not
  try them again until they change.
- The table must exist when the pipe is created (or the `COPY INTO` must
  say `CREATE_TABLE true`). Its name is resolved then, so the pipe keeps
  loading into the same table whatever the session's search path was.
- Checks that find nothing are not recorded; those that load files or
  fail appear in `igloo.task_history`, and each file in `igloo.load_history`.
- Each check lists every file under the location, with its size and time,
  to tell new files from loaded ones. Keep pipes on prefixes that hold the
  drops of recent weeks rather than years of history, for instance by
  archiving loaded files elsewhere.

Pipes poll. Event notifications (Amazon S3 to SQS, Google Cloud Storage to
Pub/Sub, Azure Event Grid), which tell Igloo the moment a file lands, are
planned; until then a pipe finds a file within its schedule.

## Tasks

A task runs one SQL statement on a schedule:

```sql
CREATE [OR REPLACE] TASK [IF NOT EXISTS] name SCHEDULE 'schedule' [TIMEZONE 'zone'] AS statement
ALTER TASK [IF EXISTS] name RESUME | SUSPEND | SET SCHEDULE 'schedule' [TIMEZONE 'zone']
EXECUTE TASK name
DROP TASK [IF EXISTS] name
SHOW TASKS
```

```sql
CREATE TASK hourly_rollup SCHEDULE '1 hour' AS
  MERGE INTO marts.hourly AS h
  USING (
    SELECT date_trunc('hour', event_time) AS hour, count(*) AS events
    FROM raw.events
    WHERE event_time >= date_trunc('hour', now()) - INTERVAL '6 hours'
      AND event_time < date_trunc('hour', now())
    GROUP BY 1
  ) AS s ON h.hour = s.hour
  WHEN MATCHED AND h.events <> s.events THEN UPDATE SET events = s.events
  WHEN NOT MATCHED THEN INSERT VALUES (s.hour, s.events);
ALTER TASK hourly_rollup RESUME;
EXECUTE TASK hourly_rollup;   -- run it now, as it would run on its schedule
```

Each run counts the last six complete hours again and writes only the
hours whose count is new or changed: running it twice changes nothing, the
hour under way waits for the first run after it ends, and the next run
makes up for one that was skipped or failed. Events that arrive up to six
hours late are counted; later ones are not. Widen the window for later
events or longer outages, at the cost of reading more each run.

- **A new task is suspended**, as in Snowflake: `ALTER TASK ... RESUME`
  starts its schedule. `EXECUTE TASK` runs it once, now, suspended or not,
  and returns its error if it fails.
- **It runs as its owner**, the user who created it, in a new session of
  theirs, with the privileges they have when it runs. If they lose a
  privilege the task needs, its runs fail; while they are disabled
  (`NOLOGIN`), its runs fail; when they are dropped, their tasks, pipes,
  materialized views and alerts are suspended and marked as a dropped
  user's (`SHOW TASKS` gives the owner as `ana (dropped)`), so a user
  created later with the same name does not get them, nor runs of them
  that came due before the drop, which are skipped. A task never runs
  with more privileges than its owner has. Its statements are in the owner's query history, with client
  `task <name>`.
- **One run at a time, on one replica.** Each scheduled run is claimed by
  one replica, through the metadata database. A run that comes due while
  the previous one still runs is skipped and recorded as `skipped`. A run
  holds the task's lock in the metadata database, and a lease there that
  its replica renews every 5 seconds. The database releases the lock if
  the replica's connection to it drops (a failover, say), but no other
  replica starts the task until the run has ended and let the lease go,
  or the lease has expired, 30 seconds after it was last renewed. A run
  checks every two seconds that it still holds both, and once it does
  not, or has not renewed the lease for 20 seconds, it is canceled and
  recorded as `failed`: it then has 10 seconds to stop before the lease
  can expire and another run start. A replica that stops without
  letting the lease go, as when it crashes, holds up its tasks for up to
  those 30 seconds, during which their runs are skipped.
- **No retries.** A failed run is recorded with its error, and the task
  runs again at its next scheduled time. Runs missed while Igloo was not
  running are not made up: the task runs once when Igloo starts, if it is
  due, and then on schedule.
- **What it may run:** any one statement its owner could run: `INSERT`,
  `MERGE`, `CREATE OR REPLACE TABLE ... AS`, `COPY INTO`,
  `REFRESH MATERIALIZED VIEW`, `ALTER DYNAMIC TABLE ... REFRESH`, [`CALL SYSTEM$SEND_NOTIFICATION`](alerts.md#sending-from-tasks),
  and so on, but not statements on tasks, pipes and alerts. Unqualified names resolve in the default schema `public`, as in a
  new session. `IGLOO_STATEMENT_TIMEOUT` applies.

Runs of tasks, pipes, materialized views, dynamic tables and
[alerts](alerts.md) are in `igloo.task_history`. Admins see every run; other users see the runs of
what they own.

```sql
SELECT start_time, name, outcome, duration_ms, error
FROM igloo.task_history WHERE outcome <> 'succeeded' ORDER BY start_time DESC LIMIT 20;
```

| Column | Meaning |
|---|---|
| `run_id` | Identifies the run |
| `kind`, `name` | `task`, `pipe`, `materialized view`, `dynamic table` or `alert`, and its name |
| `user_name` | The owner it ran as |
| `trigger` | `schedule`, or `manual` for `EXECUTE TASK`, `REFRESH MATERIALIZED VIEW` and `ALTER DYNAMIC TABLE ... REFRESH` |
| `scheduled_time` | When a scheduled run was due |
| `start_time`, `end_time`, `duration_ms` | When it ran, and for how long |
| `outcome` | `running`, `succeeded`, `failed`, or `skipped` (the previous run was still running) |
| `error` | Why it failed |
| `row_count` | Rows the run loaded, wrote (`CREATE TABLE ... AS` included) or returned |
| `detail` | How a refresh went about it: incrementally, in full and why, or with nothing to do ([below](#incremental-refresh)) |

Task history is kept for `IGLOO_AUDIT_RETENTION` (90 days by default).

## Materialized views

A materialized view is a query whose result Igloo keeps as an Iceberg
table, so reading it is as fast as reading any table. A refresh reads
only what changed in the query's tables since the last refresh, when the
query's shape allows it: after a day's load into a fact table of a month,
it reads that day's rows.

```sql
CREATE [OR REPLACE] MATERIALIZED VIEW [IF NOT EXISTS] name [SCHEDULE 'schedule' [TIMEZONE 'zone']] AS query
REFRESH MATERIALIZED VIEW name
ALTER MATERIALIZED VIEW [IF EXISTS] name SUSPEND | RESUME | SET SCHEDULE 'schedule' [TIMEZONE 'zone']
DROP MATERIALIZED VIEW [IF EXISTS] name
SHOW MATERIALIZED VIEWS
```

```sql
CREATE MATERIALIZED VIEW marts.campaign_totals SCHEDULE '30 minutes' AS
  SELECT campaign, count(*) AS rows, sum(impressions) AS impressions, sum(spend) AS spend
  FROM raw.dsp_delivery GROUP BY campaign;
```

- Creating one runs the query once and keeps the result as the table
  `name`, in the catalog like any other table; other engines read it as an
  ordinary Iceberg table.
- A refresh, on demand or on its schedule, brings the table up to date in
  one transaction: a query sees the old rows or the new ones, never
  neither. Its result always equals running the query again. If the
  query's columns change (a source table changed), refreshing fails;
  `CREATE OR REPLACE MATERIALIZED VIEW` again.
- **Reading one needs `SELECT` on it**, not on the tables its query reads:
  grant it like a table. Refreshes run as the owner, with the owner's
  privileges at the time, and need what `CREATE OR REPLACE TABLE ... AS`
  with its query needs. [Policies](security-policies.md) apply to the
  query as they do to the owner's own: the view holds what its owner may
  see of protected tables, and whoever may read it sees that.
- Only its owner and admins refresh, change or drop it. Dropping it drops
  its table; dropping the table with `DROP TABLE` drops the view too.

### Incremental refresh

Iceberg keeps every version of a table as a snapshot, and each snapshot
says which data files it added and removed. A refresh reads the tables
of its query at their current snapshots, records them, and the next
refresh reads only the rows of the files added or removed since and, of
the files whose rows were deleted since, the rows at the positions the
new delete files name.

| Query shape | Rows appended | Rows deleted or updated |
|---|---|---|
| Filters and projections of a table, or of inner joins of tables | Incremental | Full |
| Outer joins (`LEFT`, `RIGHT`, `FULL`) | Incremental while the tables the join fills with NULLs are unchanged | Full |
| `GROUP BY` (or `SELECT DISTINCT`, or aggregates of all rows) with `count(*)`, `count(x)`, `sum(x)` of integers and decimals, and `avg(x)` of integers | Incremental | Incremental |
| ... and also `min(x)` or `max(x)` | Incremental | Full |
| ... and also `count(DISTINCT x)` of integers | Full, its query on workers, where auto mode would run it there; incremental otherwise | Full |
| `UNION` of such filters, projections and joins | Incremental | Incremental |

What these read may be a `WITH` clause (not recursive), a subquery in `FROM` or
a `UNION ALL` of them, as long as that filters, projects and joins tables. And a
query that only filters and computes from the rows of one aggregate, as
`WITH t AS (SELECT g, sum(x) AS s FROM f GROUP BY g) SELECT g, s * 2 FROM t WHERE s > 1`,
is that aggregate.

- **Joins.** Rows appended to a joined table add the rows they join to;
  when several of the tables changed, the refresh joins each one's
  changes with the others as they were before or after them (Δ(A⋈B) =
  ΔA⋈B' + A⋈ΔB). A dimension table that changes reads the fact table it
  joins whole, as a full refresh would. A `UNION ALL` gains what each of its
  branches does, each change read through its branch alone.
- **Distinct counts.** A view with `count(DISTINCT x)` refreshes in full,
  its query on workers, whenever auto mode would run that query there, and
  keeps no state; `refresh_mode_reason` says so. Going on incrementally
  means merging a bitmap of every changed group's values on the replica: for
  the reach of 1.5 billion impressions by advertiser and channel, where each
  batch changed all 800 groups, incremental refreshes on the replica took 64
  to 385 s and slowed the dashboards beside them by 13 to 36%, and full ones
  on 16 workers 41 to 140 s. It goes on incrementally where workers would not
  run the query (none are configured, or auto mode keeps it on the replica),
  and with `REFRESH_MODE = INCREMENTAL`.
- **Aggregates.** An aggregating view keeps each group's partial
  aggregates (counts, sums, minimums, maximums, and, where it goes on
  incrementally, `count(DISTINCT x)`'s values in a roaring bitmap, written in
  row groups of about 8 MB) in a table of Igloo's own schema
  `igloo_refresh`, which no user's statement reads or writes, admins'
  included. A refresh aggregates the changed rows, merges them into the
  state, and writes the groups that changed; rows deleted are subtracted
  from counts and sums. Expressions of aggregates and of the `GROUP BY`
  columns, and `HAVING`, work: they are computed from the state. A view
  whose rows do not show their groups, or of one row of no group, is written
  whole, as a full refresh writes it. The view's commit and the state's are
  two: a refresh stopped between them makes the next refresh a full one, so
  no change is counted twice.
- **Exact, always.** Every table a refresh reads, it reads at the
  snapshot it records, so the next refresh starts where it ended, and the
  result equals a full refresh's to the bit. So sums and averages of
  floating-point values (`DOUBLE`, `FLOAT`), whose results depend on the
  order the values are added in, and averages of decimals, refresh in
  full; `count(DISTINCT x)` kept incrementally needs integers from 0 to
  4,294,967,295 and the `rb` extension, which Igloo's image has.
- **Full refreshes** run the query over every row of its tables, and
  rebuild the state. They write the view's rows as new files, which one
  commit puts in its table in place of every file it had, so a full
  refresh takes about what `CREATE TABLE ... AS` of the query does (50
  million rows: 0.71 s against 0.58 s, where deleting the old rows and
  inserting the new took 23 s and left reads of the view slow until
  compaction), and readers, on every replica and through the result cache,
  see the old rows or the new ones. A refresh that fails or is stopped
  leaves the old rows (see [operations](operations.md)). Igloo refreshes
  in full, and says why,
  when the query has a shape the table above leaves out (a window
  function, a subquery in an expression, a subquery that aggregates and is
  joined or aggregated again, `LIMIT`, `INTERSECT`, `EXCEPT`, a recursive
  `WITH`, a view or a table of a connection, a function such as `now()` or
  `random()`, an aggregate such as `median`);
  when reading what changed would cost more than 80% of reading the
  tables whole (see the next item); when a table has a column named
  `filename` or `file_row_number`, DuckDB's names for each row's data
  file and position, by which a refresh picks what changed; when a
  table's snapshot of the last refresh is gone (table maintenance expired
  it, or the table was replaced or rolled back) or its columns changed;
  when a table has equality delete files or deletion vectors, which other
  engines write; when the query or the policies on its tables changed, or
  a statement other than a refresh wrote the view's table; and for the
  first refresh after `CREATE`.
- **Cost.** A read costs the bytes of the files it reads, and 2 KiB,
  about what DuckDB takes to apply a deletion as it scans, for each row a
  delete file deletes of them. A full refresh reads every table with all
  its deletions; reading what changed reads the changed files as the last
  refresh read them, the new delete files, joined to them by position,
  and, for a join, the other tables it joins the change to. So a load of
  several times the rows a table had refreshes in full, while an update
  of a few rows in every file reads each file once, without applying the
  new deletions.
- **What it did.** `SHOW MATERIALIZED VIEWS` gives each view's
  `refresh_mode` (`INCREMENTAL` when its query can be refreshed so,
  `FULL` otherwise), `refresh_mode_reason`, and what its last refresh did
  (`last_refresh_action`: `INCREMENTAL`, `FULL` or `NO_DATA` when nothing
  changed; `last_refresh_detail`), and `data_timestamp`, when the data
  it read was current. Each run's `detail` in `igloo.task_history` says
  the same, with the rows and bytes of files it read and the CPU time it
  took:

  ```
  incremental: read 200000 rows added in marts.facts (1317288 bytes of files); 50 groups changed; 15ms of CPU
  full: ran the query over every row (6200000 rows, 40835332 bytes of files): its query, or the policies on its tables, changed since the last refresh; 382ms of CPU
  ```

## Dynamic tables

A dynamic table is Snowflake's form of a materialized view that keeps
itself within a lag of its tables, and Igloo refreshes it the same way,
incrementally when it can:

```sql
CREATE [OR REPLACE] DYNAMIC TABLE [IF NOT EXISTS] name
  TARGET_LAG = '<n> { seconds | minutes | hours | days }' | DOWNSTREAM
  [WAREHOUSE = name]
  [REFRESH_MODE = AUTO | FULL | INCREMENTAL]
  [INITIALIZE = ON_CREATE | ON_SCHEDULE]
  AS query
ALTER DYNAMIC TABLE [IF EXISTS] name SUSPEND | RESUME | REFRESH
ALTER DYNAMIC TABLE [IF EXISTS] name SET TARGET_LAG = '...' | DOWNSTREAM [WAREHOUSE = name]
DROP DYNAMIC TABLE [IF EXISTS] name
SHOW DYNAMIC TABLES [LIKE 'pattern'] [IN ACCOUNT | DATABASE [name] | SCHEMA name]
```

```sql
CREATE DYNAMIC TABLE marts.daily_spend TARGET_LAG = '1 hour' WAREHOUSE = transform_wh AS
  SELECT day, client, sum(spend) AS spend, count(*) AS events
  FROM raw.delivery GROUP BY day, client;
```

- **Target lag.** Igloo refreshes it every half of its lag (at least 30
  seconds apart), so its data stays within the lag while a refresh takes
  less than half of it. The shortest lag is a minute. With `DOWNSTREAM`,
  it has no schedule of its own: a dynamic table that reads it refreshes
  it first, when it is not suspended.
- **It starts at once**, unlike a task: `INITIALIZE = ON_CREATE`, the
  default, fills it as it is created; `ON_SCHEDULE` makes it empty, for
  its first refresh to fill. `SUSPEND` stops its schedule and `RESUME`
  starts it again; `ALTER ... REFRESH` refreshes it now.
- **`REFRESH_MODE`**: `AUTO` (the default) refreshes incrementally when
  the query's shape allows it, as for materialized views, and a
  `count(DISTINCT x)` in full on workers where auto mode would run its query
  there (see [distinct counts](#incremental-refresh)); `FULL` always runs the
  query over every row; `INCREMENTAL` refuses, when the table is created, a
  query that cannot be refreshed incrementally, with the reason, and keeps a
  `count(DISTINCT x)` incremental.
- **`WAREHOUSE` is accepted and ignored**, with a notice: Igloo has no
  warehouses, and a refresh runs on the replica that holds its job.
  `COMMENT` and `DATA_RETENTION_TIME_IN_DAYS` are ignored with a notice
  too.
- In Snowflake's dialect the query is translated as any query is; the
  options are as written. A column list after the name is refused.
- **It is a table**, like a materialized view: privileges, ownership and
  dropping work the same, a dynamic table and a materialized view cannot
  have the same name, and creating one needs the writer role. `SHOW
  DYNAMIC TABLES` gives Snowflake's columns (`name`, `database_name`,
  `schema_name`, `owner`, `target_lag`, `refresh_mode`,
  `refresh_mode_reason`, `warehouse`, `text`, `scheduling_state`,
  `data_timestamp`), and its next and last runs and what the last
  refresh did. Runs are in `igloo.task_history` as kind `dynamic table`.

## Schedules

A schedule is an interval or a cron expression:

| Schedule | Runs |
|---|---|
| `'30 seconds'`, `'5 minutes'`, `'1 hour'`, `'1 day'` | Every interval, from when it was resumed (Snowflake's `'5 MINUTE'` works too) |
| `'0 6 * * *'` | Every day at 6:00 |
| `'*/15 * * * *'` | Every 15 minutes, on the quarter hour |
| `'0 9 * * MON-FRI'` | Weekdays at 9:00 |
| `'30 2 1 * *'` | The first of every month at 2:30 |
| `'@hourly'`, `'@daily'`, `'@weekly'`, `'@monthly'` | Shorthands |

Cron expressions have five fields: minute, hour, day of month, month and day
of week, with `*`, lists (`1,15`), ranges (`1-5`), steps (`*/10`) and
three-letter month and day names. When both day fields are given, a day
matching either runs, as in cron. They are read in `TIMEZONE`, an IANA name
such as `America/New_York` or `Europe/London` (UTC if left out), so a 6 a.m.
task runs at 6 a.m. local time across daylight saving changes; a time that
a clock change skips runs at the next time that matches. Snowflake's
`'USING CRON 0 6 * * * America/New_York'` works too.

Creating or changing a schedule (`SCHEDULE`, `RESUME`, `SET SCHEDULE`,
`TARGET_LAG`) needs the writer role. `SHOW TASKS`, `SHOW PIPES`,
`SHOW MATERIALIZED VIEWS` and `SHOW DYNAMIC TABLES` list each one's owner,
state, schedule, time zone, next and last run, last outcome and error, and
definition: admins see all of them, other users their own. The console's **Tasks** and
**Load history** pages show the same, with each one's runs and loads.

## With scale to zero

Igloo reports when its scheduled work is next due on `GET /activity`, and
the [scale-to-zero gateway](scale-to-zero.md#scheduled-work) starts the
compute in time for it, then stops it again once it is idle, as usual.
While Igloo runs, work due sooner than `IGLOO_GATEWAY_SUSPEND_AFTER` keeps
it running.

A pipe checks every minute by default, which keeps a scaled-to-zero Igloo
running. Give pipes a schedule that matches how often files arrive and how
late a load may be, such as `SCHEDULE '1 hour'` for daily drops: each check
then starts Igloo for a few minutes.

## Where it is kept

Definitions, runs, the load history and what each materialized view and
dynamic table's last refresh read are in the metadata database
(`IGLOO_METADATA_DATABASE_URL`, schema `igloo_tasks`), shared by every
replica, and included in its backups. Aggregating views' state is in
Iceberg tables of the catalog's schema `igloo_refresh`, one a view, which
go with the view. Without a metadata database they are
kept in memory, lost on restart: run one replica, and only for trying
things out.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `COPY INTO loads only from the warehouse and IGLOO_ALLOWED_LOCATIONS` | Add the bucket or prefix to `IGLOO_ALLOWED_LOCATIONS`. |
| `COPY INTO reads files, which needs the reader role` | The user has the role `none`. Give them the reader role, or load as another user. |
| `COPY 0` and no rows | Every file was loaded before, or none matches `PATTERN` (which must match the whole path after the location). Check `igloo.load_history`, or use `FORCE true`. |
| `none of the file's columns ... is a column of the table` | The header names do not match the table, or the file has no header: use `HEADER false` (which matches by position), or `MATCH_BY position`. |
| `cannot tell the format of ...` | The file name has no known extension; give `FORMAT`. |
| A task does not run | It is suspended: `ALTER TASK ... RESUME`. `SHOW TASKS` shows its state and next run. |
| A run failed with `permission denied for ...` | The owner lost a privilege the task needs. Grant it again, or `CREATE OR REPLACE` the task as someone who has it. |
| A task was suspended by itself, and its owner is `... (dropped)` | Its owner was dropped. An admin can recreate it, as themselves or another user, or drop it. |
| Nobody noticed a failed run, or a partner's file that never came | Switch on the [built-in alerts](alerts.md#built-in-operational-alerts) `FAILED_RUNS`, `LOAD_ERRORS` and `IDLE_PIPES` for a Slack, Teams or email integration. |
| Runs are `skipped` | Each run takes longer than the schedule's interval. Lengthen the interval. |
| A file is `unknown` in the load history | Igloo stopped while loading it. Check whether its rows are in the table; if not, load it with `FORCE true`. |
| A materialized view or dynamic table refreshes in full every time | `SHOW MATERIALIZED VIEWS` or `SHOW DYNAMIC TABLES` says why in `refresh_mode_reason` (the query's shape) or `last_refresh_detail` (what changed); see [incremental refresh](#incremental-refresh). |
| `cannot be refreshed incrementally` creating a dynamic table | `REFRESH_MODE = INCREMENTAL` with a query whose shape needs full refreshes; the error says why. Use `AUTO`, or change the query. |
