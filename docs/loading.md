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
- **Materialized views** keep a query's result as a table, refreshed on
  demand or on a schedule.
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
  holds the task's lock in the metadata database, which the database
  releases if the replica's connection to it drops (a failover, say), and
  another replica could then start the task. So a run checks every two
  seconds that it still holds the lock, and once it does not it is
  canceled and recorded as `failed`: two runs overlap by those seconds at
  most, and the time the first takes to stop.
- **No retries.** A failed run is recorded with its error, and the task
  runs again at its next scheduled time. Runs missed while Igloo was not
  running are not made up: the task runs once when Igloo starts, if it is
  due, and then on schedule.
- **What it may run:** any one statement its owner could run: `INSERT`,
  `MERGE`, `CREATE OR REPLACE TABLE ... AS`, `COPY INTO`,
  `REFRESH MATERIALIZED VIEW`, [`CALL SYSTEM$SEND_NOTIFICATION`](alerts.md#sending-from-tasks),
  and so on, but not statements on tasks, pipes and alerts. Unqualified names resolve in the default schema `public`, as in a
  new session. `IGLOO_STATEMENT_TIMEOUT` applies.

Runs of tasks, pipes, materialized views and [alerts](alerts.md) are in
`igloo.task_history`. Admins see every run; other users see the runs of
what they own.

```sql
SELECT start_time, name, outcome, duration_ms, error
FROM igloo.task_history WHERE outcome <> 'succeeded' ORDER BY start_time DESC LIMIT 20;
```

| Column | Meaning |
|---|---|
| `run_id` | Identifies the run |
| `kind`, `name` | `task`, `pipe`, `materialized view` or `alert`, and its name |
| `user_name` | The owner it ran as |
| `trigger` | `schedule`, or `manual` for `EXECUTE TASK` and `REFRESH MATERIALIZED VIEW` |
| `scheduled_time` | When a scheduled run was due |
| `start_time`, `end_time`, `duration_ms` | When it ran, and for how long |
| `outcome` | `running`, `succeeded`, `failed`, or `skipped` (the previous run was still running) |
| `error` | Why it failed |
| `row_count` | Rows the run loaded, wrote (`CREATE TABLE ... AS` included) or returned |

Task history is kept for `IGLOO_AUDIT_RETENTION` (90 days by default).

## Materialized views

A materialized view is a query whose result Igloo keeps as an Iceberg
table, so reading it is as fast as reading any table:

```sql
CREATE [OR REPLACE] MATERIALIZED VIEW [IF NOT EXISTS] name [SCHEDULE 'schedule' [TIMEZONE 'zone']] AS query
REFRESH MATERIALIZED VIEW name
ALTER MATERIALIZED VIEW [IF EXISTS] name SUSPEND | RESUME | SET SCHEDULE 'schedule' [TIMEZONE 'zone']
DROP MATERIALIZED VIEW [IF EXISTS] name
SHOW MATERIALIZED VIEWS
```

```sql
CREATE MATERIALIZED VIEW marts.campaign_totals SCHEDULE '30 minutes' AS
  SELECT campaign, sum(impressions) AS impressions, sum(spend) AS spend
  FROM raw.dsp_delivery GROUP BY campaign;
```

- Creating one runs the query once and keeps the result as the table
  `name`, in the catalog like any other table; other engines read it as an
  ordinary Iceberg table.
- A refresh, on demand or on its schedule, runs the query again and
  replaces the table's rows in one transaction: a query sees the old rows
  or the new ones, never neither. Refreshes are full; there is no
  incremental refresh yet. If the query's columns change (a source table
  changed), refreshing fails; `CREATE OR REPLACE MATERIALIZED VIEW` again.
- **Reading one needs `SELECT` on it**, not on the tables its query reads:
  grant it like a table. Refreshes run as the owner, with the owner's
  privileges at the time, and need what `CREATE OR REPLACE TABLE ... AS`
  with its query needs. [Policies](security-policies.md) apply to the
  query as they do to the owner's own: the view holds what its owner may
  see of protected tables, and whoever may read it sees that.
- Only its owner and admins refresh, change or drop it. Dropping it drops
  its table; dropping the table with `DROP TABLE` drops the view too.

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

Creating or changing a schedule (`SCHEDULE`, `RESUME`, `SET SCHEDULE`)
needs the writer role. `SHOW TASKS`, `SHOW PIPES` and
`SHOW MATERIALIZED VIEWS` list each one's owner, state, schedule, time
zone, next and last run, last outcome and error, and definition: admins see
all of them, other users their own. The console's **Tasks** and
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

Definitions, runs and the load history are in the metadata database
(`IGLOO_METADATA_DATABASE_URL`, schema `igloo_tasks`), shared by every
replica, and included in its backups. Without a metadata database they are
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
