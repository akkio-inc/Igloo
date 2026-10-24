# Distributed execution

A query that reads a very large table can run in parts on several machines.
Igloo decides this itself, for each statement: most statements run on the
Igloo replica the client is connected to, as always, and a statement that
would take long there is divided among workers, which Igloo starts when it
needs them and stops when they have been idle. Results are the same as on
one machine. [The design document](design/distributed-execution.md) explains
the choices behind it and the measurements.

```
 psql / JDBC / BI tool
          │
          ▼
 ┌────────────────────────┐  tasks: SQL + data files   ┌──────────┐
 │ igloo (coordinator)    │ ─────────────────────────▶ │ worker 1 │──┐
 │  checks grants, plans, │ ◀───────────────────────── │ DuckDB   │  │  data files of
 │  combines the results  │      partial results        └──────────┘  │  the tables the
 └────────────────────────┘           ...               ┌──────────┐  │  statement may read
                                                        │ worker n │──┤
                                                        └──────────┘  ▼
                                                    object storage (S3, GCS, ADLS)
```

## What runs distributed

Igloo looks for the part of a statement that reads its largest table and
can be split: a `SELECT` over one large table, joined to smaller ones, with
filters, grouping and aggregates. That covers the common heavy queries of
log-level data, such as

```sql
-- daily delivery per campaign over a quarter of impressions
SELECT campaign_id, date_trunc('day', ts) AS day, count(*) AS impressions,
       count(DISTINCT device_id) AS reach, sum(cost) AS spend
FROM impressions i JOIN campaigns c USING (campaign_id)
WHERE ts >= TIMESTAMP '2026-07-01' AND c.advertiser = 'acme'
GROUP BY ALL;
```

Each worker reads a share of the large table's data files and all of the
smaller tables, filters, joins and aggregates its rows, and sends back
partial results; the coordinator combines them (sums of sums, counts of
counts, minimums of minimums, and aggregates over `DISTINCT` values over the
values the workers found) and runs the rest of the statement: `HAVING`,
`ORDER BY`, `LIMIT`, window functions over the aggregates, other joins and
subqueries. Queries without aggregation are divided the same way when they
keep few rows: `SELECT DISTINCT`, and `ORDER BY ... LIMIT` of up to a
million rows. A plain `SELECT ... WHERE` is not: every matching row would
cross to the coordinator and wait on its disk before reaching the client.

When the large table is joined to a second table too large for every
worker to read (impressions to an identity graph, exposures to
conversions, lineitem to orders), and the two are joined on equal columns,
both are partitioned by the join key instead:

```sql
-- households reached per campaign, through a device graph of billions of edges
SELECT i.campaign_id, count(DISTINCT g.household_id) AS households
FROM impressions i JOIN device_graph g ON g.device_id = i.device_id
WHERE i.ts >= TIMESTAMP '2026-07-01'
GROUP BY 1;
```

Workers first read shares of both tables, apply the statement's filters on
each table alone, and write the columns the statement uses to object
storage, under `_igloo_shuffle/` in the warehouse, one directory per
partition of the key's hash; then each worker joins and aggregates one
partition of both, and the coordinator combines the results as before. The
exchanged rows are deleted in the background once those steps have run.

Aggregations whose partial results would be large complete their groups on
workers the same way. For an aggregate over `DISTINCT` values, such as the
reach `count(DISTINCT device_id)` above, each worker groups its rows by the
values too and writes them to object storage, partitioned by group and
value; then a worker per partition counts each group's values, which no
other partition holds, and the coordinator adds up the partitions' counts,
one row per group and partition, instead of receiving every value. `sum`,
`min`, `max` and `avg` over `DISTINCT` values add up the same way; other
aggregates over `DISTINCT` values (`median(DISTINCT x)`) are partitioned by
group alone, and each partition computes its groups whole. When the part is
a partitioned join on the counted value (devices reached, joining
impressions to a device table on the device), each partition of the join
already holds every row of its values, and counts them without a second
exchange. The top groups of an aggregation by a column of the large table
(`GROUP BY device_id ORDER BY spend DESC LIMIT 10`) are partitioned by
group, and each partition sends the coordinator only its first groups.

Where a session counts distinct 32-bit unsigned integers with roaring
bitmaps (the `rb_count_distinct` setting, `always`, which Igloo sets for
the large statements that count [ID domains](sql.md#id-domains)' IDs), a
`count(DISTINCT)` of such values (a `CAST(... AS UINTEGER)`, as Igloo
writes counts of people by their integer IDs, or a column of an unsigned
type, also through the common table expressions and subqueries that
compute it, and with a `FILTER`) crosses no values at all:
each worker sends a bitmap of its share's values a group, a few bytes a
value at most, and the coordinator, or a worker completing a partition of
the groups, counts their union. Aggregates over other `DISTINCT` values in
the same `SELECT` go as above.

Statements built in steps, with common table expressions or subqueries,
run step by step on workers, each step's rows written to object storage
for the next, keyed by the column the next step joins or groups on. A
cohort report, say:

```sql
WITH cohorts AS (
  SELECT 'audience' AS cohort, person_id FROM (SELECT DISTINCT person_id FROM audience_members WHERE audience_id = 42)
  UNION ALL
  SELECT 'everyone', person_id FROM (SELECT DISTINCT person_id FROM people)),
genres AS (
  SELECT m.genre, c.cohort, count(DISTINCT m.person_id) AS people
  FROM media_events m JOIN cohorts c ON c.person_id = m.person_id GROUP BY 1, 2),
sizes AS (SELECT cohort, count(DISTINCT person_id) AS size FROM cohorts GROUP BY 1)
SELECT g.genre, g.cohort, g.people / s.size AS reach FROM genres g JOIN sizes s USING (cohort);
```

Workers compute both cohorts' distinct people, write them once, keyed by
person, to an exchange that both branches of the `UNION ALL` fill, and
partition the media events by person as well; then each worker joins one
partition of both and counts its people per genre and cohort, which no
other partition holds, and so for the cohorts' sizes. The coordinator gets
a row per genre, cohort and partition, and runs the final join itself. A
common table expression is computed once, however many parts read it,
and a part the coordinator runs that reads it too reads what the workers
wrote. One that only one part reads and that only filters tables is read
as that part's subquery, with no step of its own. Joins of large
intermediate results, `UNION ALL` of large branches
feeding one aggregation, and a large table joined to itself on equal
columns run the same way.

In detail, a part runs distributed when:

- It is a `SELECT` (in the statement itself, a subquery in `FROM`, a common
  table expression or a branch of `UNION`) whose `FROM` clause reads catalog
  tables, through inner, cross, `LEFT` (keeping the large table's rows),
  `SEMI`, `ANTI` and `ASOF` joins and subqueries that only filter and
  compute columns. It may unnest its rows' values: `unnest`, `json_each`
  and `json_tree` of them, and lateral subqueries that read no table, which
  is what Snowflake's `FLATTEN` becomes (with `OUTER` and `RECURSIVE` too).
  It may join `range` and `generate_series` of integer literals, up to
  1,000 rows, which are the same rows on every worker.
  Its filters' subqueries that stand on their own may read large tables:
  `x IN (SELECT y ...)` is a semi join to a relation workers compute,
  partitioned by `y`, and a subquery of one row
  (`ver = (SELECT max(ver) FROM t WHERE ...)`) one that every task reads.
  It may also read relations workers compute: common table expressions
  and subqueries in `FROM` that are themselves such `SELECT`s, with
  grouping and `DISTINCT` but no `ORDER BY` or `LIMIT`, and no window
  functions but aggregates over all their rows (`min(day) OVER ()`), or a
  `UNION ALL` of them.
- Its aggregates are `count`, `sum`, `min`, `max`, `avg`, `count_if`,
  `bool_and`, `bool_or`, `bit_and`, `bit_or`, `bit_xor`, `any_value`,
  `fsum` and `product`, with or without `FILTER`, and any aggregate over
  `DISTINCT` values of one expression (`count(DISTINCT user_id)`), or of
  several when one of them is a column that each of the others fixes
  (`count(DISTINCT user_id)` with `count(DISTINCT (user_id, day))`).
  `count`, `sum`, `avg`, `min` and `max` over `DISTINCT` values may have a
  `FILTER`, which drops values as `CASE WHEN` would.
  `GROUP BY` may use positions, aliases, `ALL`, `ROLLUP`, `CUBE` and
  `GROUPING SETS`.
- The large table appears once in it, and the other tables it reads
  together fit comfortably in a worker's memory (an eighth of it, in
  compressed size); or the tables too large for that, and the relations it
  reads, are all joined to each other, by inner `JOIN ... ON` or `WHERE`
  conditions of the same `SELECT`, or of a subquery in its `FROM` that
  passes the join's rows through (`SELECT ... FROM (SELECT ... FROM a JOIN
  b ON ...) AS x GROUP BY ...`), on equal columns of the same type
  (integer types may mix).
- It may read views: a view reads as its query, its names resolved as in
  the view, so a view that passes a large table's rows through, filters
  them, or joins small tables to them, distributes like its query, and one
  that groups is a relation workers compute. A view that sorts or limits,
  or has common table expressions, keeps the part on one machine.
- Its tables may have deleted rows: workers leave out the rows that
  position delete files delete, as `UPDATE`, `DELETE` and `MERGE` write
  them, and those of the equality delete files and deletion vectors other
  engines write, so a table distributes without waiting for maintenance
  to merge them.
- Its tables' columns may be nested: lists, maps and structs, Snowflake's
  `ARRAY` (`VARCHAR[]`) and `OBJECT` (`MAP(VARCHAR, VARCHAR)`) columns
  among them. Workers read them, and the fields nested in them, by their
  Iceberg field IDs, as one machine does.
- It runs outside an explicit transaction, so it reads the tables' latest
  snapshots, and does not use time travel. A read at the snapshot that is
  a table's latest, `AT (VERSION => n)`, as Igloo pins the statements it
  answers from bitmap indexes, counts as such a read; at any other, the
  statement runs on one machine. Where such a statement filters a large
  table by a scalar subquery over an index's table, the coordinator
  computes that subquery once and workers get its value (`computed first`
  in `EXPLAIN DISTRIBUTED`). Values over 16 MiB keep the statement on one
  machine.

Everything else runs on one machine: median and other aggregates that do
not combine (`median`, `quantile`, `approx_count_distinct`, `string_agg`,
`list`, `stddev`), except over a relation workers compute, which is then
partitioned by the groups so that each worker computes whole groups;
aggregates over `DISTINCT` values of unrelated
expressions in one `SELECT` (`count(DISTINCT a), count(DISTINCT b)`),
window functions over rows, a large table read again in a correlated
subquery expression (`EXISTS`, a scalar subquery: TPC-H Q17, Q20, Q21),
large tables joined on different keys in one `SELECT`, statements that call user-defined macros
or session functions (`getvariable`, `current_setting`), tables with
equality deletes of fields nested in structs, with
data files that have no Iceberg field IDs (files added from outside and
read through a name mapping) or that nest a struct's fields otherwise
than the table does now (a field renamed, or dropped and added again,
since they were written), tables with
[masking or row access policies](security-policies.md) and statements those
policies rewrite (directly or through a view), views that group, sort or
limit, [connections](federation.md)' tables and Delta
Sharing's [external tables](sharing.md). Workers read table files
directly and could not apply policies; a connection's table or an external
table may still be read in the part of a statement the coordinator runs
itself, such as a scalar subquery over a distributed part, but never on a
worker.

### When

A statement never runs on workers when the replica alone would be faster.
Workers read cold data from object storage, and a statement built in steps
waits for each step to be written there and read back, so workers pay off
only for statements that take a while: at 1x, the audience reports that
took 6 to 56 seconds on one replica took 37 to 176 seconds on four workers.

So Igloo considers a statement for workers only when its largest table is
more than the replica reads from object storage cold in about four seconds
(about 570 MB for a replica of 16 cores and 24 threads), whether the
workers run or are stopped: stopped workers' start counts in their
estimate, and reasons say when they are stopped (`workers are stopped;
...`).
For such a statement it estimates two times, over what it reads of its
tables (each table's size times the share of its columns, weighed by type,
that the statement's parts name, and a hundredth of that for a table a part
filters by a constant, `IN`, or a subquery's value, as an audience or its
latest version is picked out of a lookup of every one): the replica's, at
what it reads from object storage cold per core (about 9 MB/s at one and a
half threads a core, 16 MB/s at four; its read cache holds what it read
lately, which a statement's first run seldom has), and the workers', at
the same rate for their cores and threads; each no less than the rows its
parts go through take at 150 million rows a second per core, once for the
scan and again for each join and grouping (a hundredth of a table's rows
where a part filters it so), for a statement that reads little but joins
much. Workers also take about 8 seconds to start a statement and 6 for each
of its stages, whose tasks write their exchanges to object storage for the
next to read, plus the time to start workers that are stopped: at ten times
the data on 16 workers, reports of 2 or 3 stages took 12 to 40 seconds, and
of 23 to 26 stages 153 to 183 seconds. A replica that would run short of
memory (the rows its joins and distinct counts hold, estimated at six times
what feeds them, over 70% of its memory limit) is estimated to take as many
times longer, spilling, up to twice as long. It runs the statement on
workers only if the replica would take longer than `IGLOO_WORKER_THRESHOLD`
(a minute), or would run short of memory, and the workers are estimated to
be at least 1.5 times faster. It uses the fewest workers estimated within a
fifth of the fastest time.

Against the scale environment's reports at 1x, 10x and 100x, timed on a
16-core replica and on 16 workers of 8 cores, the estimates pick the side
that was faster by half again, or either where the two were closer, but
for statements whose rows cost far more than their count says, such as
FLATTEN's unnested arrays (251 s on the replica at 10x, estimated at 7)
and joins through a view's computed keys counted distinct (691 s on the
replica and 170 on workers, estimated at 160 and 153). Those run on the
replica first, and their history sends them to workers after.

Estimates are rough, so Igloo corrects them with what the same statement
took before. It recognizes a statement run again, as BI tools run theirs, by
its text without its literals (other than integers) and the sizes of its
tables to a factor of two: `query_history.plan_key`. Each replica remembers
the last 4,096 such statements, and reads the past week's from the query
history when it starts. A statement that ran both ways runs where it was
faster. One that ran only on the replica within the threshold stays there.
One that ran only one way, in under ten minutes, is tried once the other
way, at most once a day, so that runs come to be compared with runs, if
the other way is estimated at no more than twice what it took, and, on the
replica, if it should fit in its memory; past that, one that ran only on
the replica, well within its estimate, stays there, and one that ran only
on workers stays there if it beat its estimates, or if the replica should
run short of memory. The first time, one that the replica should spill
runs on workers if they are estimated at no more than half again the
replica's estimate, which is then a floor. Runs on workers count with runs on about
as many: one that ran on a quarter of the workers it would run on now, as
in an earlier session on a smaller fleet, is tried on all of them, at most
once a day, whatever it took on the replica. The server logs each choice, `chose where a statement
runs`, with both estimates and the reason.

A relation filtered by a constant, a subquery's value or `IN`, as an
audience taken from a lookup of every audience is, is estimated at the size
of the table it filters, though it may hold a thousandth of it. Estimated
larger than every worker may read, it would be partitioned, and so would
every large table joined to it: 465 GB of tables for one report at a
hundred times the data, in 925 s, where the replica filters each table by
the audience as it reads it. So workers write and measure such a relation
before anything reads it, and the statement is planned again with its
size: a relation whose hash table fits comfortably in every task (at the
memory its rows take in one, within a quarter of what a task may plan
on), and that costs less to build in every task than partitioning the
table it meets, is read whole beside each table's shares, and the tables
are not partitioned at all; a larger one is read by partition as planned,
from what was written of it. At ten times the data, an audience of 45.9
million IDs is partitioned and one of 189,400 people read whole. While
the relation is measured the rest of the plan starts, but for what reads
the relation, and what started is canceled if the plan changes, so a
relation that comes out large costs no wait. A relation some stage read
by partition is planned with the size it came out at, without measuring
it first, for a day, while the tables it reads stay within a factor of two
of their size then, so the statement gets the plan it got once measured;
one every stage read whole is measured each time, first, with the workers
to itself. `EXPLAIN DISTRIBUTED` lists such relations as `uncertain size`, and
the server logs how each came out (`measured relations of uncertain
size`).

A statement that auto mode ran on the replica and that runs out of memory
there (DuckDB's out-of-memory error, or the replica's memory watch
canceling it) before it sends a row runs again on workers, and the client
gets its rows. The replica remembers that it ran out of memory, as it reads
statements that failed so on the replica from the query history at start,
and runs it on workers from then on.

The replica's estimate can also run far short, as for a statement that
spills: one estimated at 51 s took 1,798 s on the replica and 362 s on
workers. So a statement auto mode runs on the replica, though workers could
run it, is stopped there once it has run three times its estimate and at
least 90 seconds without sending a row, and runs on workers, started if they
are stopped, the client getting its rows as if it had run there; the server
logs `a statement ran on this replica past three times its estimate;
running it on workers instead`, counts it
(`igloo_auto_guard_reruns_total`), and runs the statement on workers from
then on. A statement that has begun sending rows runs to the end, and so
does a write. While such a statement runs on the replica, the workers count
as busy, so they are not stopped as idle under it.

A session can change that for testing and benchmarks:

```sql
SET distributed_execution = 'on';    -- whenever the statement can run distributed
SET distributed_execution = 'off';   -- never
SET distributed_execution = 'auto';  -- the default; RESET does the same
SHOW distributed_execution;
```

`on` lets admins start workers and use all of them for any statement,
whatever the estimates. For other users it distributes whatever can be
distributed, but only on workers already running and on no more of them
than the automatic decision would use; it never starts a worker. So no user can
start a fleet of machines, or keep one busy, by repeating a query.

## Same results as one machine

Workers run the same DuckDB as the coordinator, on the same snapshot of
every table, with the session's settings that change what SQL means
(`TimeZone`, `Calendar`, `default_collation`, `default_order`,
`default_null_order`, `integer_division` and the like); `now()` and
`current_date` are fixed when the statement starts, as DuckDB fixes them.
Integer, decimal, string, date and time results are identical. Results of
floating-point sums and averages can differ in their last digits, as they
do in DuckDB on one machine between runs with different numbers of threads.
Rows without `ORDER BY` come in no particular order, as on one machine.

A statement reads each table at one snapshot, its current one when the
statement's parts start, everywhere: workers read that snapshot's data
files, and where the part the coordinator runs itself reads the same table
again, it reads it at that snapshot too (`AT (VERSION => ...)`). Commits
made meanwhile, by Igloo or by other engines through the
[Iceberg REST catalog](external-engines.md), are not seen by either part,
whether they add rows or delete files or change the schema. A table whose
columns changed between planning and the start runs on one machine, as
does a statement whose coordinator part reads a view it cannot read as
its query (one with common table expressions, say), which could read a
table at another snapshot; other views read as their queries there too,
at the workers' snapshots. Tables may share files, as clones do: workers
read the files the metadata lists, wherever they lie in the warehouse.

If a worker cannot be reached or stops in the middle of a statement, its
share runs on another worker, and the failed worker is left out for 30
seconds. A worker that answers a task with an error ran it and is healthy:
the statement then runs on the coordinator at once, without trying other
workers or leaving any out. Object storage turning requests away for now
(S3's 503 SlowDown) is not such an error: DuckDB tries the request again
for half a minute, and the coordinator then runs the task again, up to four
times over about a minute and a half. If no worker can run it, its partial results
would take more than a quarter of the coordinator's free disk (at least
1 GiB), or the parts cannot
be combined, the statement also runs on the coordinator, so a client sees a
slower statement, not an error; but not one whose exchanged rows Igloo
estimates at more than the coordinator's memory holds, or that ran out of
memory there before, which would run out of memory there after as long as
one machine takes: that fails at once, with the workers' error. Workers stay
up while a statement that ran tasks on them runs on the coordinator, and
count as idle from when it ends. An error in the data itself, such as a failed cast
or a division by zero, is the statement's error, as it would be on one
machine. Canceling a statement (`Ctrl-C`, `statement_timeout`) cancels its
parts on the workers.

Workers watch their memory as the server does (see
[operations](operations.md#sizing)): DuckDB keeps to its limit only for the
memory its buffer manager tracks, and when what a worker can still allocate
falls below a sixteenth of its memory (at least 256 MiB), it cancels the
task that has run longest there rather than let the kernel kill it, and
every task on it. It logs `memory is running out; canceled the task that had
run longest` with the statement and task. A task that runs out of DuckDB's
memory limit on a worker (`Out of Memory Error`) counts the same. The
statement's parts then run again in smaller tasks, twice: each time over
twice the partitions (up to 256), with each task's share of a divided table
halved, and half the memory for grouping partitions into a task. A task
that read one partition is no smaller so, so when such a task ran out, the
partitions grow fourfold instead, up to 1,024, as far as leaves each
writer files of a million rows of the exchange it read. If those
run out too, the statement fails with SQLSTATE `53200` (`out_of_memory`), and
the server logs `a worker ran out of memory running part of a statement`:
run on another worker or on the coordinator, the part would run out of
memory the same way, so neither is tried. Run it on less data at a time, or
give the workers more memory.

## Security

- **Grants and the sandbox hold.** A statement is checked against the
  user's role and grants before Igloo plans it, as always. Its parts read
  only the tables the statement reads, resolved the way DuckDB resolves
  them in the session; for users with role `none`, only tables the grant
  check found and allowed, through their own grants or their roles'.
  Workers have no catalog and no credentials of their own beyond object
  storage: each task lists the data files to read, which Igloo took from
  the tables' Iceberg metadata, never from a path a user wrote. A worker
  refuses files outside the warehouse and `IGLOO_ALLOWED_LOCATIONS`, and a
  task whose SQL reads anything but the files it lists (a table function
  other than `unnest`, `json_each` and `json_tree` of values and `range`
  and `generate_series` of literals, a table by name). Their DuckDB is locked down as the server's is: no local
  files, no other locations, no SQL from strings.
- **Policies keep a statement on one machine.** A statement that masking
  or row access policies apply to, or that names anything they concern,
  runs on the replica, where Igloo applies them. Workers never see a masked
  column's raw values or a hidden row. So does one that reads a table a
  [clean room](clean-rooms.md) keeps to its party, a room's runs included.
- **Workers accept no one but Igloo.** Coordinators and workers talk TLS 1.3
  and authenticate each other with keys derived from `IGLOO_WORKER_SECRET`;
  without the secret, neither SQL users nor anything else on the network
  can connect to a worker's task port (7433) or pretend to be a worker.
  Treat the secret like the object storage keys: whoever holds it can make
  workers read any table. The task port is TLS only, on private networks
  too. The Helm chart generates the secret and lets only the igloo pods of
  the workers' own compute pool reach the task port; Docker Compose does
  not publish it. Workers' port 8080 serves only health checks and metrics,
  like the server's.
- **What crosses the network.** Tasks carry the statement's SQL fragments
  and file lists, and results carry aggregated or filtered rows, over TLS.
  Partial results live in the spill directories of the workers and the
  coordinator while a statement runs, and are deleted when it ends.
- **What exchanges leave in object storage.** The rows partitioned joins,
  aggregations and the steps of multi-stage plans exchange (the columns a
  join uses, of rows that passed the statement's filters; an aggregation's
  groups, the values its aggregates over `DISTINCT` values count, and
  partial aggregates; the rows of the common table expressions and
  subqueries workers compute) are
  Parquet files, not encrypted by Igloo, under `_igloo_shuffle/` in the
  warehouse, deleted once the statement's steps have run.
  They are protected as the tables' own files are: by access to the bucket
  and its server-side encryption. No credentials the Iceberg REST catalog
  hands out reach `_igloo_shuffle/`. What a coordinator that stopped
  mid-statement leaves behind, the replicas remove [within hours](#lifecycle-rule).
  With `IGLOO_EXCHANGES=local` they are instead files on the workers'
  disks, which workers send each other over the task port: see
  [exchanges on workers' disks](#exchanges-on-workers-disks).
- **Pools are kept apart by secret and network.** Each compute pool's
  workers have a secret of their own (the Helm chart generates one per
  pool), so one pool's igloo pods cannot use another's workers; the
  NetworkPolicy keeps them apart too, and is only as good as the cluster's
  CNI enforcing it: with several pools, use one that does. Docker Compose
  has no network isolation between its containers; give a pool's workers
  their own `IGLOO_WORKER_SECRET`.

## Exchanges on workers' disks

By default workers exchange rows through the warehouse's object storage,
which costs a request a file to write and several to read, tens of
milliseconds each, and keeps what they wrote if a worker is lost. With
`IGLOO_EXCHANGES=local` on the server, a worker writes them to its own disk
(`IGLOO_DUCKDB_TEMP_DIR`, with spill), and the workers that read them fetch
the files from it, whole, to their own disks first. In a lab of four
writers and four readers of 3 GB over 128 partitions, a stage that took 54 s
through an object store answering in 25 ms took 17.5 s through the writers'
disks, and the writers needed 0.9 GB of memory rather than 1.6.

- **Room.** A task writes to its disk only if twice what it is expected to
  write (what it reads; twice, for the copies its readers fetch), with what
  the worker's other tasks reserved, fits a quarter of the disk's free
  space, which spill and the read cache share; otherwise it writes to object
  storage, and readers read both. A task that fetches waits while other
  tasks hold the room it needs, and fails rather than fill the disk. What a
  task writes is taken to be at most what it reads, which a join that
  multiplies rows can exceed.
- **A worker lost.** What a worker wrote to its disk goes with it. A task
  that cannot fetch a file fails; if the worker no longer answers, or
  answers as a new run of itself, the statement's parts run again, once, on
  the other workers. Otherwise, or if another is lost, the statement fails
  (and runs on the server if it fits there). A worker too busy to send a
  file says so within five seconds and is asked again for up to five
  minutes; one that stops sending for a minute counts as lost. A statement
  never returns
  rows short of a lost file: a task checks that the files it reads hold the
  rows written.
- **Security.** Workers send exchange files only to peers that hold
  `IGLOO_WORKER_SECRET` (the task port's TLS), and only the files they wrote
  themselves, by the names they gave them: no other path, no symbolic link,
  no file of another size. They send a few at a time and refuse past a
  queue. A reader fetches only from the statement's own workers, and
  follows no redirect. The files are not encrypted by Igloo on the disk, as
  spill is not.
- **Cleanup.** The server removes a statement's files from its workers'
  disks when the statement is done, and tells them it still runs every 30
  minutes; a worker removes files nothing used for six hours, as after the
  server stopped, and all of them when it restarts.
- **Networks.** Workers must reach each other's task port (7433). With the
  Helm chart, set `workers.exchanges: local`, which sets `IGLOO_EXCHANGES`,
  lets the workers' NetworkPolicy admit each other on that port (only the
  workers of the same compute pool), and splits the workers' spill volume,
  whose free space Kubernetes does not show its pods: 60% for spill
  (`IGLOO_DUCKDB_MAX_TEMP_SIZE`), 30% for exchanges
  (`IGLOO_EXCHANGE_MAX_SIZE` on the workers, which caps what exchanges on a
  worker's disk may take). Docker Compose and virtual machines on one
  network need nothing more.

## Seeing it

- **`EXPLAIN DISTRIBUTED`** before a query says how it would run, without
  running it or starting workers, as rows of `explain_key` and
  `explain_value`: where it would run (`runs on`: `workers` or `this
  replica`) and why (`reason`: auto mode's choice, the session's
  `distributed_execution`, or what keeps it here); the `estimate` auto mode
  weighs (one machine's seconds, workers' seconds and how many, the stages'
  levels, what they read and exchange, the rows they go through, and
  whether it is too large for the replica's memory); the `partitions` its
  exchanges would have with the workers running now; each stage (`stage 0
  split public.events to exchange 1 (join)`) with the SQL its tasks
  run; what `the coordinator` runs over their results; and why parts of it
  `stay on the coordinator`. After the statement has run on workers,
  `measured` says what its exchanges held (rows and gigabytes) and the most
  rows a stage wrote for each row it read, as the replica remembers them:
  auto mode keeps on workers a statement whose exchanges would not fit in
  the replica's memory, but never moves one to workers by them. A stage
  that joins a table filtered by a constant to other rows, as an audience
  joins events, shows as a `semi-join estimate` of the share of rows it
  keeps, which the estimates do not count yet. The query is checked as it
  would be when run, grants and policies included, and the same rewrites
  apply.

  ```sql
  EXPLAIN DISTRIBUTED
  SELECT campaign_id, count(DISTINCT device_id) FROM impressions GROUP BY 1;
  ```

- **Query history.** `igloo.query_history` has a `workers` column: how many
  workers ran parts of the statement, 0 when it ran on one replica; and
  `stages` and `exchanges`: how many stages of tasks those parts took, and
  how many exchanges of rows, in object storage, the stages wrote for later
  ones. A statement built in steps waits for each stage that reads what an
  earlier one wrote.

  ```sql
  SELECT start_time, duration_ms, workers, stages, exchanges, query_text
  FROM igloo.query_history WHERE workers > 0 ORDER BY start_time DESC LIMIT 20;
  ```

  Otherwise a distributed statement's row is like any other's: its query
  tag and pool, the time it waited for its role's limits, and the tables
  it read and wrote, worked out from the statement as sent, which the
  [catalog](catalog.md)'s usage and lineage take up. `cpu_time_ms`,
  `bytes_read` and `memory_allocated_bytes` are `NULL`: what the workers
  used is not measured yet (`igloo_distributed_scanned_bytes_total` counts
  the bytes they were given).

- **Metrics** on port 8080 of the igloo pods:

  | Metric | Labels | Meaning |
  |---|---|---|
  | `igloo_distributed_queries_total` | `outcome` = `ok`, `error`, `out_of_memory`, `canceled`, `fallback` | Statements whose parts ran on workers (`error`: an error in the data; `out_of_memory`: a part ran out of memory on a worker, also in smaller tasks), and statements that started to and ran on the coordinator instead |
  | `igloo_distributed_fallbacks_total` | `reason` = `no_workers`, `worker_error`, `combine_error` | Why a statement ran on the coordinator after all |
  | `igloo_distributed_query_seconds` | | Time to run a statement's parts and combine them, before results are sent |
  | `igloo_distributed_tasks_total` | `result` = `ok`, `retried`, `failed` | Parts sent to workers |
  | `igloo_distributed_workers` | | Workers that answered the last check |
  | `igloo_distributed_scanned_bytes_total` | | Data file bytes divided among workers |
  | `igloo_distributed_received_bytes_total` | | Partial results received |
  | `igloo_distributed_worker_starts_total` | `action` = `start`, `stop`; `result` | Workers started and stopped |
  | `igloo_auto_guard_reruns_total` | | Statements auto mode ran on the replica that ran past three times their estimate there, and ran on workers instead |

  and on the workers' port 8080: `igloo_worker_tasks_total{outcome}`
  (`out_of_memory` counting tasks a worker canceled as it ran out),
  `igloo_worker_task_seconds`, `igloo_worker_sent_bytes_total`, and the read
  cache's `igloo_cache_*`.
- **Logs.** The coordinator logs `distributed execution is on` at start,
  `started workers` and `stopped idle workers`, and a warning when a task
  fails on a worker or a statement falls back to the coordinator, with the
  reason. Set `IGLOO_LOG_LEVEL=debug` to log why each statement ran where it
  did.

## Deploying workers

Workers are the same image as Igloo, started with `igloo worker`. They need
the warehouse location and object storage credentials (the same
`IGLOO_WAREHOUSE_LOCATION`, `IGLOO_S3_*`, `IGLOO_GCS_*` or `IGLOO_AZURE_*`
settings as the server), `IGLOO_WORKER_SECRET`, a spill directory and,
ideally, a disk for the [read cache](operations.md#read-cache). They need no
catalog, database or users.

### Lifecycle rule

Statements delete what they exchanged under `_igloo_shuffle/` (a thousand
files a request on S3), but a coordinator that stops mid-statement cannot.
Every replica sweeps `_igloo_shuffle/` a minute after it starts and
hourly, and removes the files of statements whose removal began
(`_igloo_shuffle/<statement>/removing`), and of statements under which
nothing was written for six hours (a statement that still runs writes
there every half hour). A lifecycle rule
on the warehouse's bucket that expires `_igloo_shuffle/` after a day is a
backstop for when no replica runs for that long. On S3, for a warehouse at
`s3://bucket/warehouse`:

```sh
aws s3api put-bucket-lifecycle-configuration --bucket bucket --lifecycle-configuration \
  '{"Rules": [{"ID": "igloo-shuffle", "Status": "Enabled", "Filter": {"Prefix": "warehouse/_igloo_shuffle/"}, "Expiration": {"Days": 1}}]}'
```

(`mc ilm rule add --prefix warehouse/_igloo_shuffle/ --expire-days 1` on
MinIO; an object lifecycle rule with `matchesPrefix` on Google Cloud
Storage, a lifecycle management rule with `prefixMatch` on Azure.)

### Kubernetes

```yaml
workers:
  enabled: true
  maxReplicas: 8            # the most worker pods Igloo starts
  resources:
    requests: { cpu: "16", memory: 64Gi }
    limits: { memory: 64Gi }
  nodeSelector: { pool: igloo-workers }   # a node pool that scales to zero
```

The chart adds a worker Deployment that starts at zero pods, a headless
Service the igloo pods find workers through, a Role that lets the igloo pods
change only that Deployment's replica count, a NetworkPolicy that admits
only igloo pods to the task port, and the secret. Each
[compute pool](workload-management.md#compute-pools) gets the same of its
own (`<release>-pool-<name>-workers`), used only by its igloo pods; a pool
can set `workers` to size them differently, or `workers.enabled: false`. Igloo scales the
Deployment up when a statement needs workers (never down while another
replica uses them) and to zero after `workers.idleAfter` (10 minutes)
without tasks; the cluster autoscaler or Karpenter then removes their
nodes. With [scale to zero](scale-to-zero.md), a pool's gateway stops the
pool's workers along with its igloo pods. See the
[chart's README](../deploy/helm/igloo/README.md#distributed-execution).

### Docker Compose

For trying it out and testing on one machine, where workers share the
machine's cores:

```sh
# in deploy/single-vm/.env
COMPOSE_PROFILES=local-storage,workers
IGLOO_WORKERS=worker
IGLOO_WORKER_REPLICAS=3
```

`make up` then starts three worker containers next to Igloo. An `.env`
created before this release has no `IGLOO_WORKER_SECRET`; add one of at
least 32 random characters.

### Virtual machines

Run `igloo worker` on each machine and list them in `IGLOO_WORKERS`. To
start and stop them on demand, set `IGLOO_WORKER_COMPUTE` to `ec2` with the
instances' IDs, or to `command` with commands that start and stop them all
(an instance group, for example), as for the [scale-to-zero
gateway](scale-to-zero.md#on-google-cloud-or-azure-vms).

### Settings

On the Igloo server:

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_WORKERS` | | Workers' addresses, `host[:port]` comma separated (port 7433 by default). A host may resolve to several workers, as a Kubernetes headless Service does. Empty turns distributed execution off. |
| `IGLOO_WORKER_SECRET` | (required with workers) | At least 32 characters, the same on the server and the workers |
| `IGLOO_WORKER_COMPUTE` | | How to start and stop workers: `kubernetes`, `ec2`, `command`, or empty when they always run |
| `IGLOO_WORKER_MAX` | `4` | The most workers Igloo starts |
| `IGLOO_WORKER_IDLE_AFTER` | `10m` | Stop workers after this long without tasks |
| `IGLOO_WORKER_START_TIMEOUT` | `3m` | How long a statement waits for workers to start before it runs on the server instead |
| `IGLOO_WORKER_THRESHOLD` | `1m` | In `auto` mode, how long a statement must take on the server alone before it may run on workers (see [when](#when)) |
| `IGLOO_EXCHANGES` | `object` | Where workers exchange rows: `object` storage, or `local` disks (see [exchanges on workers' disks](#exchanges-on-workers-disks)) |
| `IGLOO_WORKER_K8S_DEPLOYMENT`, `IGLOO_WORKER_K8S_NAMESPACE` | | `kubernetes`: the workers' Deployment, and its namespace if not the server's |
| `IGLOO_WORKER_EC2_INSTANCE_IDS` | | `ec2`: the workers' instances |
| `IGLOO_WORKER_START_COMMAND`, `IGLOO_WORKER_STOP_COMMAND` | | `command`: shell commands that start and stop all workers |

Distributed execution also needs `IGLOO_DUCKDB_TEMP_DIR` (set in the image),
where partial results are kept while a statement combines them.

On a worker (`igloo worker`): `IGLOO_WORKER_SECRET`,
`IGLOO_WORKER_LISTEN_ADDR` (`:7433`), the server's storage settings, and
`IGLOO_DUCKDB_*`, `IGLOO_CACHE_*`, `IGLOO_HTTP_ADDR` and `IGLOO_LOG_*` as on
the server; with local exchanges, `IGLOO_EXCHANGE_MAX_SIZE` caps what they
may take on its disk (empty: a quarter of its free space at a time).

## Sizing and cost

Give workers the same shape as a large Igloo replica: many cores, about 4 GB
of memory per core, fast local disk for spill and the read cache, and the
best network bandwidth the cloud offers for their size, since a worker
mostly reads object storage. Workers on their own node pool that scales to
zero cost nothing while idle; a statement that starts them waits for the
nodes (a minute or two on most clouds), which is why Igloo starts them only
for statements that would take longer than that. Statements that already
fit one replica gain little and are not distributed.

Fast local disk means local NVMe: instance-store types on AWS (`r7gd`,
`m7gd`, `c7gd`), Local SSD-backed ephemeral storage on Google Cloud, and on
Azure a size with a local disk and an ephemeral OS disk on it. Workers'
spill and read cache are `emptyDir` volumes, so they land on that disk when
the node keeps kubelet's directory there, as the OpenTofu modules set up
([spill on local NVMe](operations.md#spill-on-local-nvme)). A distributed
statement's partial results also live in the spill directories, of the
workers and of the coordinator, while it runs. Size each worker's
`workers.duckdb.spillVolume.sizeLimit` and `workers.cacheVolume.sizeLimit`,
together, within its node's local disk, and expect both to be empty after
the node stops: spill files only live for a statement, and the read cache
refills from object storage.

Distributing does not make a statement cheaper: the same work runs on more
machines for less time, plus the start and the idle tail. It makes a
statement that would take many minutes on one machine finish in a fraction
of that, and lets a join of two large tables whose state is many times one
machine's memory run where that machine would run out of memory.

## Limitations

- The large inputs of one `SELECT` are partitioned by one key. Large tables
  joined on different keys need a step each: a common table expression or
  subquery per key. A large table read again in a correlated subquery
  expression (`EXISTS`, a scalar subquery) runs on one machine; written as
  a join to a common table expression, it runs distributed. Give the
  machines that run such statements a lot of memory and fast spill disk.
- Partitioned joins, aggregations and every step of a multi-stage plan
  write what they exchange to object storage and read it back: that costs
  request charges and adds a few seconds per step, and
  leftovers of a coordinator that crashed mid-statement stay under
  `_igloo_shuffle/` for six hours or more, until a replica's sweep removes
  them.
- The coordinator receives every group of an aggregation, unless only its
  top rows are kept: a `GROUP BY` with billions of groups sends every
  worker's groups to it. (`count(DISTINCT x)` over billions of values sends
  it one row per group and partition.)
- Tables with data files without field IDs, views that group, sort or
  limit, and time travel to any snapshot but a table's latest run on one
  machine.
- Workers read each table's files directly and do not use Iceberg partition
  statistics to skip files; DuckDB skips row groups by their Parquet
  statistics instead.
