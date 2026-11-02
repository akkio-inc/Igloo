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
subqueries. A statement that ranks the groups with `ROW_NUMBER()`, `RANK()`
or `DENSE_RANK()` and keeps the first few of each partition (`WHERE rank <=
10`) has the workers send only the groups that may rank so. Queries without aggregation are divided the same way when they
keep few rows: `SELECT DISTINCT`, whose groups workers complete as a `GROUP
BY`'s, `ORDER BY ... LIMIT` of up to a
million rows, and a `SELECT ... WHERE` that filters the large table by a
column equal to a constant, in a list, or equal to a subquery's value (`WHERE
account_id = 42`), or looks up three characters or more of a column's text
(`WHERE note LIKE '%refund%'`, `contains(note, 'refund')`), as the statement or
as a subquery or common table
expression whose rows the coordinator reads to run the rest (a window over
all of them, say); and every row of a table, filtered or not, ordered by
`ORDER BY` without `LIMIT` by an order the coordinator merges by (below):
workers sort their shares, and the coordinator merges them as the client
reads them, where one machine sorts them all, past its memory spilling to
disk and taking more memory besides than its limit. A plain `SELECT ...
WHERE` that may keep many of the large table's rows is not: every matching
row would cross to the coordinator and wait on its disk before reaching the
client. A `LIMIT`
without `ORDER BY`, any so many rows, has each task keep that many, and
once the tasks that ended sent as many the others stop; auto mode leaves
a scan of a table to one machine, which stops at the first rows it reads,
unless its filters keep few of the table's rows (as above), or the rows
come from joins of relations or windows workers compute. A subquery or
common table expression without aggregation whose rows the coordinator
reads every one of, to run the rest (a window over all of them, an
aggregate that does not combine, a sample of a number of rows), is
divided too where its filters or its columns keep less than the table:
workers read the table in shares, rather than the coordinator alone,
and send it what they keep.

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

Aggregations whose groups may be many complete them on workers the same
way, so that the coordinator receives one row a group, final, which it only
filters, orders and sends on: a `GROUP BY` of a column of the large table
(spend per device), unless [`ANALYZE`](sql.md#distinct-counts) counted the
group columns' values and their combinations come to 10,000 or fewer, as
with a date's day or month, a flag, or a column of a table every worker
reads in full. Each worker writes its share's partial results to object
storage, partitioned by group, and a worker per partition completes its
groups, keeping only those `HAVING` keeps, its first under `ORDER BY ...
LIMIT`, and those a ranking may keep within its first k (`QUALIFY
row_number() OVER (PARTITION BY ...) <= 10`, in the statement or in a
`SELECT` that reads the groups); one worker completes them all when the
partial results are small. For an aggregate over `DISTINCT` values, such as
the reach `count(DISTINCT device_id)` above, each worker groups its rows by
the values too and writes them partitioned by group and value; then a
worker per partition counts each group's values, which no other partition
holds, instead of the coordinator receiving every value, and a last round
adds up each group's counts, partitioned by group, as above. `sum`, `min`,
`max` and `avg` over `DISTINCT` values add up the same way; other
aggregates over `DISTINCT` values (`median(DISTINCT x)`) are partitioned by
group alone, and each partition computes its groups whole. When the part is
a partitioned join on the counted value (devices reached, joining
impressions to a device table on the device), each partition of the join
already holds every row of its values, and counts them without exchanging
the values. Groups that are few, or of grouping sets (`ROLLUP`), the
coordinator combines from every worker's partial results, and adds up the
partitions' counts itself.
Workers complete the groups of an aggregation that forecasts each one too
([`AI.FORECAST`](ml-functions.md#bigquerys-and-databricks-forecasting-functions)
and the like, which gather a series' points with `list()`): each partition
completes its series and forecasts them, and the coordinator receives the
forecasts rather than the series.

A result ordered by `ORDER BY` without `LIMIT`, of the groups workers
complete or the rows they keep, each worker sorts as it completes them, and
the coordinator merges the workers' sorted rows rather than sorting them
all, holding a batch of rows a worker in memory whatever the result, and
sends the client each batch as it merges it, rather than once it has merged
them all. The merge runs on several cores: each worker's file is decoded
apart, groups of up to eight files are merged at once, and the groups'
rows merged in turn, ahead of DuckDB reading them. It merges by
columns of numbers, text and `BLOB`s (by their bytes), dates, times,
timestamps, decimals and booleans, the result's columns or the groups and
aggregates they show, in the session's default order and `NULLS` order.
An order by an expression, under a `default_collation`, or in a session
that does not keep the order rows are read in (`preserve_insertion_order
= false`), the coordinator sorts, as before.

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
other partition holds, and so for the cohorts' sizes, and a last round adds
up each genre and cohort's counts, and joins them by cohort. The coordinator
gets the result's rows. A
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
  compute columns. A `LEFT`, `RIGHT` or `FULL` join of one item to
  another whose side that supplies `NULL`s (either side, for `FULL`) is
  too large for every worker to read reads both sides by partition, by
  the key its condition makes equal (`ON o.customer = c.id`): each row
  meets its matches in its partition, and a row nothing matches is in one
  partition only, which keeps it once. The statement's filters on the side
  that supplies `NULL`s apply after the join, as they must (`WHERE o.id IS
  NULL`), and its groups by that side's columns are completed together,
  as its `NULL`s come from every partition. It may unnest its rows' values: `unnest`, `json_each`
  and `json_tree` of them, and lateral subqueries that read no table, which
  is what Snowflake's `FLATTEN` becomes (with `OUTER` and `RECURSIVE` too).
  It may join `range` and `generate_series` of integer literals, up to
  1,000 rows, which are the same rows on every worker.
  Its filters' subqueries that stand on their own may read large tables:
  `x IN (SELECT y ...)` is a semi join to a relation workers compute,
  partitioned by `y`, and a subquery of one row
  (`ver = (SELECT max(ver) FROM t WHERE ...)`) one that every task reads.
  A correlated subquery over a large table is the join it means, where its
  `WHERE` only makes its columns equal to the statement's and otherwise
  filters its own rows: `EXISTS` a semi join, `NOT EXISTS` an anti join, and a
  scalar subquery of `min`, `max`, `sum`, `avg` (or arithmetic on them) or a
  count a `LEFT JOIN` to its groups, in `WHERE` or in a select list that does
  not group.
  It may also read relations workers compute: common table expressions
  and subqueries in `FROM` that are themselves such `SELECT`s, with
  grouping and `DISTINCT` but no `ORDER BY` or `LIMIT`, and no window
  functions but aggregates over all their rows (`min(day) OVER ()`) or
  windows that all partition their rows by the same columns (see
  [window functions](#window-functions)), or a `UNION ALL` of them, or a
  `UNION` of distinct rows, an `INTERSECT` or an `EXCEPT`, whose branches
  write their rows partitioned by all of their columns and whose last stage
  computes it over each partition's rows. Those three run so as a statement
  too, with the coordinator reading what they keep, and sorting or limiting
  it. Joins are read by their conditions, `USING` and `NATURAL` among them.
  An `ORDER BY` without `LIMIT` in such a subquery or common table
  expression counts for nothing where what reads it sorts again,
  aggregates, or takes distinct rows, as a report's last `SELECT` sorts
  its rows and the client's query reads it, filters it and takes its first
  rows by an order of its own: Igloo leaves it out. It stays, and keeps the
  part on one machine, where the rows reach the client in its order, or a
  `LIMIT` without `ORDER BY`, `DISTINCT ON`, or a call that sees rows in
  the order they come (`list()` or `string_agg()` without an `ORDER BY` of
  its own, `first()`, `row_number() OVER ()`).
- Its aggregates are `count`, `sum`, `min`, `max`, `avg`, `count_if`,
  `bool_and`, `bool_or`, `bit_and`, `bit_or`, `bit_xor`, `any_value`,
  `fsum`, `product`, and `list` and `array_agg` (whose order, without
  `ORDER BY` in the call, is any), with or without `FILTER`, and any aggregate over
  `DISTINCT` values of one expression (`count(DISTINCT user_id)`), or of
  several when one of them is a column that each of the others fixes
  (`count(DISTINCT user_id)` with `count(DISTINCT (user_id, day))`).
  `count`, `sum`, `avg`, `min` and `max` over `DISTINCT` values may have a
  `FILTER`, which drops values as `CASE WHEN` would.
  HyperLogLog sketches combine too: `datasketch_hll(k, x)` and
  `datasketch_hll_union(k, sketch)` of a constant `k`, which is what
  Snowflake's `HLL_ACCUMULATE` and `HLL_COMBINE` become, and
  `APPROX_COUNT_DISTINCT` and `HLL` without `GROUP BY` (with it, they count
  `DISTINCT` values exactly, as above). Each worker sketches its share's values and the
  final stage merges the sketches, so the estimate is one machine's, within
  the sketch's error, and no values cross.
  `GROUP BY` may use positions, aliases, `ALL`, `ROLLUP`, `CUBE` and
  `GROUPING SETS`. A scalar subquery that stands on its own, in the select
  list, `HAVING` or `ORDER BY` of an aggregation (`count(*) / (SELECT
  count(*) FROM orders)`), the coordinator computes once over the combined
  groups, if what it reads is smaller than the large tables workers are
  considered for (see [when](#when)).
- Its large table may be sampled: `TABLESAMPLE bernoulli(p%)` (Snowflake's
  `SAMPLE ROW (p)`) keeps each row with that probability where a task
  reads it, and `TABLESAMPLE system(p%)` (`SAMPLE BLOCK (p)`) each vector of
  rows, so the shares' samples together are a sample of the table, at the
  same rate as on one machine, though not of the same rows. With
  `REPEATABLE (s)` (`SEED (s)`), every task samples with that seed, and the
  statement picks the same rows again over the same snapshot on the same
  workers. So may a table read by bucket, and a subquery in `FROM` that
  passes the large table's rows through. A sample of a number of rows
  (`reservoir`, `SAMPLE (100 ROWS)`), which shares do not add up to, or of a
  table every worker reads whole, keeps the part on one machine.
- The large table appears once in it, and the other tables it reads
  together fit comfortably in a worker's memory (an eighth of it, in
  compressed size); or the tables too large for that, and the relations it
  reads, are all joined to each other, by inner `JOIN ... ON` or `WHERE`
  conditions of the same `SELECT`, or of a subquery in its `FROM` that
  passes the join's rows through (`SELECT ... FROM (SELECT ... FROM a JOIN
  b ON ...) AS x GROUP BY ...`), on equal columns of the same type
  (integer types may mix), or, where no columns are equal, on the same
  `upper`, `lower` or `trim` of a text column on each side, as reports
  join their metrics (`upper(m.name) = upper(s.name)`): each is
  partitioned by that expression's value, so the rows it pairs meet. Not
  under a `default_collation`, by which text that hashes apart compares
  equal.
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
  table by a scalar subquery over an index's table, or by a cohort's
  bitmap the session computed first, the coordinator computes that value
  once and workers get it (`computed first` in `EXPLAIN DISTRIBUTED`).
  Values over 256 MiB keep the statement on one machine. Cohorts listed
  from such bitmaps, as a report call answered from bitmap indexes joins
  its source to, every task lists itself beside its share of the source.

Everything else runs on one machine: median and other aggregates that do
not combine (`median`, `quantile`, `approx_count_distinct`, `string_agg`,
`list(x ORDER BY y)`, `stddev`), except over a relation workers compute, which is then
partitioned by the groups so that each worker computes whole groups;
aggregates over `DISTINCT` values of unrelated
expressions in one `SELECT` (`count(DISTINCT a), count(DISTINCT b)`),
window functions over rows but those of the next section, a large table read again in a correlated
subquery expression of another shape (`NOT IN`, whose rows depend on whether
any of its subquery's is `NULL`; a correlation by anything but equal columns;
a scalar subquery of another aggregate, or that gives a value over no rows),
subqueries in `GROUP BY`, samples of a number of rows,
large tables joined on different keys in one `SELECT`, statements that call user-defined macros
or session functions (`getvariable`, `current_setting`), tables with
equality deletes of fields nested in structs, with
data files that have no Iceberg field IDs (files added from outside and
read through a name mapping) or that nest a struct's fields otherwise
than the table does now (a field renamed, or dropped and added again,
since they were written), tables of Iceberg format version 3 with a
column or nested field that has an initial default (workers would read it
as `NULL` from the files written before it was added), tables with
[masking or row access policies](security-policies.md) and statements that
read them (directly or through a view), views that group, sort or
limit, [connections](federation.md)' tables and Delta
Sharing's [external tables](sharing.md). Workers read table files
directly and could not apply policies; a connection's table or an external
table may still be read in the part of a statement the coordinator runs
itself, such as a scalar subquery over a distributed part, but never on a
worker. An [ID domain](sql.md#id-domains)'s member table is not one of these
for its columns of ids alone: statements read it without them, and
workers run them as they run any other. Igloo's own text names those
columns where it counts through ids. A member table with a policy of its
own stays on the replica.

### Fills

A statement that fills a table from a query runs the query where auto mode
would run it as a read, by the same estimates, history, grants and
policies: `CREATE TABLE ... AS` (dbt's table models' `create table ... as
(...)` too), `CREATE OR REPLACE TABLE ... AS`, `INSERT INTO ... SELECT`, and
a materialized view's or dynamic table's full refresh. Where the tasks of
its last stage hold the rows of its result, each its own, as a copy's and a
filter's do, [they write the table's files](#workers-writing-tables), and
the coordinator commits them. Otherwise the coordinator writes the table
from the workers' results, `INSERT INTO` the table the SQL that combines
them, as it sends a read's client what it combines. A `count(DISTINCT)` of a
billion IDs by day and group into a table, which held the replica at its
memory limit until the memory watch canceled it, sends the replica a
thousand rows to write.

Auto mode counts a fill's writing on each side: about the share of the bytes
the query reads that its rows are of the rows its scans read, by DuckDB's
plan (an aggregate's result is taken for a hundredth of its rows at most, as
DuckDB takes its groups for all of them), written at 30 MB of files a
second a core of whichever writes them, the replica's or, where tasks write
them, the workers', with what a coordinator that writes them fetches from
the workers at 400 MB/s. So a copy workers write runs there where they are
faster, and one the coordinator would write takes about as long either way
and stays on the replica unless workers are faster still by the margin auto
mode asks of any statement.
Fills keep their own history, apart from reads of the same query. A fill
auto mode runs on the replica is guarded as a read is (see [when](#when)):
past its time, or once it runs out of memory there, its query runs on
workers, into the table its run on the replica left empty. One whose
workers run out of memory goes on in passes on the replica where its
query allows (see [aggregates too large for memory](sql.md#aggregates-too-large-for-memory)). A fill from a table
masking or row access policies apply to, directly or through a view,
stays on the replica, as reads of it do. `query_history.placement` says
where the fill's query ran, and a refresh's detail says so where it ran
on workers. Fills of tables of member columns of ID domains, sorted
writes and fills in a transaction block run on the replica, as before.

### Workers writing tables

Each task of a fill's last stage writes its rows as Parquet, with the table's
field IDs and partitions, and says which files it wrote. The coordinator
reads each file's footer and commits them all in one Iceberg commit, an
append, or for a full refresh one that replaces the table's files. The
statement's rows are in the table when that commit is, or not at all.

- **Where.** Under one directory of the statement's under the table's
  location, `data/igloo-write-<random>/`, and in it one for its query, for
  each task and for each attempt of a task. The coordinator copies the files of
  the attempts it heard from, within the store, to `data/igloo-sealed-<random>/`,
  reads them there and commits those. So a commit takes one copy a file, at most
  32 at a time, and for that while the table's new files take twice their space.
  S3's copy stops at 5 GB a file, past which the replica writes the table.
- **Retries.** A worker that dies or does not answer has its task run again
  elsewhere, in a directory of its own. Only files of the attempt the
  coordinator heard from are committed, so rows are not lost or written twice.
  A commit another commit got ahead of goes again on the new snapshot, with
  the files as read. One whose answer was lost is read back: if the table
  holds its files, it was made; if that cannot be told, the statement fails
  rather than risk its rows twice, and the table's next write tells.
- **What goes.** When the statement ends, whatever it wrote that no commit
  holds goes: all of it if it failed or was canceled, otherwise what lost
  attempts wrote; and a minute later it looks again, for what tasks it no longer
  waited for finished meanwhile. A note in the catalog, as for
  [full refreshes](operations.md), says what a process that stopped left, for
  the table's next write, or the `CREATE TABLE ... AS` of its name, to delete;
  files a worker wrote later than that are orphan files of the table, for
  [maintenance](operations.md#table-maintenance) and erasure requests. The partition
  directories are named by a hash, so no value of the table is in a path.
- **Memory.** A task bounds what its writer holds by its share of the worker's
  memory: row groups of at most 128 MiB, an eighth of its share over its threads
  or the partitions' files it keeps open, which rows of many kilobytes need on a
  small worker. Auto mode takes rows wider than a task could hold (at least two
  vectors of 2,048 rows a thread, by what its scans read a row) to this replica
  where it has the room. A fill that still runs out of memory on workers, after
  the retries with smaller tasks, runs on the replica if it has the room for the
  query, or else with the replica writing the table from the workers' results.
- **Partitions.** Identity of booleans, integers, dates, timestamps and
  strings, truncate of integers and strings, and year, month, day and hour are
  written by workers, one file per partition per task until compaction merges
  them. So a table of many partitions gets many small files at first. A look at
  a fill's first rows, for up to 3 s, says how many partitions to expect: a task
  keeps that many files open, if they fit in half of a worker's memory at about 8
  MiB each, and otherwise sorts its rows by partition first, which takes longer but
  makes a file of each. Auto mode has this replica write a table whose files would
  be under 8 MiB on average, or need that sort; `distributed_execution = 'on'` still
  sends it to workers.
- **On the coordinator's way instead**, as the log says at debug level: tables
  partitioned by bucket, or identity of decimals, whose partition values
  cannot be read from a file's bounds or written to its manifest; tables with
  an Iceberg sort order (Igloo keeps [its own as a property](sql.md));
  fixed, nanosecond and format-version-1 columns; a query that sorts,
  limits or groups in its last step; a fill estimated at less than 256 MiB in
  auto mode, which would make small files; an `INSERT` that leaves out a column
  with a default.
  So do fills of ID domains' member tables, sorted writes and fills in a
  transaction block, on the replica, and fills from tables with policies. A
  commit that fails after workers wrote runs the fill on the replica, unless
  the commit may have been made.

An erasure request that deleted files since the fill began discards what its
tasks wrote, and the fill runs on the replica, reading the tables as they are
then; tasks of statements from before a deletion are refused and canceled as
for reads ([Erasure requests](#erasure-requests)).

### Window functions

A `SELECT` that does not aggregate, and whose window functions all
partition their rows by the same columns, runs on workers with its rows
partitioned by those columns, so that each worker holds whole partitions
of the windows and computes them as one machine would, frames, `lag` and
running sums included:

```sql
-- each device's latest impression
SELECT device_id, ts, campaign_id FROM impressions
QUALIFY row_number() OVER (PARTITION BY device_id ORDER BY ts DESC) = 1;
```

The coordinator receives the rows the windows keep: those the statement's
`QUALIFY` keeps, or a later `SELECT`'s `WHERE` (`WITH ranked AS (SELECT
..., rank() OVER (PARTITION BY ...) AS r FROM ...) SELECT ... FROM ranked
WHERE r <= 3`), which workers apply too; one that keeps every row runs on
one machine, as rows without `DISTINCT` or a top-N do. The windows may
partition by a column the statement's large tables are joined on. Windows
over all rows (no `PARTITION BY`), of different partitions in one `SELECT`,
or that rank an aggregation's groups the stages keep only the first of
(see above) run where the rows arrive, on the coordinator.

### Bucketed tables

A table [partitioned by `bucket(n, column)`](sql.md#bucketed-tables) keeps
every row of a key in its bucket's files. Where a part needs its rows
partitioned by that column, workers read the table by bucket instead of
writing its rows to an exchange and reading them back: each task reads the
files of some buckets, and of the same buckets of the other tables bucketed
by the column they are joined on. That holds for

- a join of tables bucketed by their join columns, one count dividing the
  others (16 and 64), whose columns hash alike (integers of any width, or
  the same type), even where one is small enough for every worker to read
  whole: each task reads a share of each;
- a `GROUP BY` that includes the bucket column, whose groups each bucket
  then holds whole, so that tasks complete them;
- aggregates over `DISTINCT` values of it, which each bucket counts alone;
- [window functions](#window-functions) partitioned by it,

when every file of the tables was written under a partition spec that
buckets so, and the buckets are at least as many as the workers: a part
reads them in as many tasks, two for each slot of the workers at most,
balanced by their files' bytes. Rows whose column is `NULL` are read by one
task.

The other side of a join need not be bucketed:

- A table not bucketed by the join column, or a relation workers compute (a
  common table expression several parts read, a subquery that aggregates), is
  **shuffled into the bucketed table's buckets**: its stage writes each row
  to the partition of its key's Iceberg bucket, and the bucketed table stays
  where it is. Its rows cross once, and the bucketed table's not at all.
- A subquery read once that **keeps the key as it is**, over tables bucketed
  by it, is read where it stands, with no exchange of its own: one that
  filters and projects such a table, joins such tables on the key, or groups
  one with the key among its groups. A `SELECT` renaming the key counts as
  keeping it.

Both apply to a key of one column, the column itself on the table's side, of
an integer type, `VARCHAR` without a collation, `DATE`, `TIMESTAMP`,
`TIMESTAMPTZ`, `UUID` or `DECIMAL`. DuckDB hashes `UUID`s otherwise than
Iceberg's spec, so a table's bucket of a `UUID` depends on the engine that wrote
it: its files' bounds tell which, rows are written to its buckets by that hash,
and tables written by different engines, or whose files do not tell, are
exchanged. A key computed from the column (`upper(person)`, `id + 1`), a
join on two columns, bucket counts that do not divide or columns of types
that hash apart are exchanged as before, by the hash of the key.

`EXPLAIN DISTRIBUTED` says, in `buckets` rows, which tables a part reads by
bucket with no exchange, what is written to their buckets, and why bucketed
tables were exchanged all the same; a stage that writes to buckets says so
(`to exchange 1 (relation) by bucket[32]`):

```
buckets | no exchange: public.orders by bucket[32](customer_id) and public.visits by bucket[64](customer_id), read by bucket, 32 groups of buckets a stage
buckets | no exchange: public.visits by bucket[64](customer_id), read by bucket, 64 groups of buckets a stage; active written to those buckets
buckets | exchanged although public.orders by bucket[32](customer_id), public.events by bucket[48](customer_id): public.events's 48 buckets are not a multiple of 32
```

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
0.4 of that rate for their cores and threads, as their tasks read column
chunks in parts and write and read exchanges; each no less than the rows
its parts go through take at 150 million rows a second per core, once for
the scan and again for each join and grouping (a hundredth of a table's
rows where a part filters it so), for a statement that reads little but
joins much. Where DuckDB's plan says (see spilling, below), the replica's
reading is what its scans project, each table's columns once, rather than
what the parts name, which for a part reading a view's every column was
77 GB of a table of 470 columns where the replica read 9.6 GB; and its
time is no less than its hash tables take, at 10 million rows a second per
core for a table larger than the processor's caches, as 16 joins of 400
million rows to 400 million took 2,166 core-seconds. Workers also take
about 8 seconds to start a statement and 5 for each level of its stages
(stages that read what another wrote wait for it; those of one level run
at once), plus the time to start workers that are stopped. Against the
second scale runs at ten times the data, seven reports on 16 workers and
two on 1 to 8, the workers' estimates are within a factor of 1.35 at the
root mean square of the logarithms, where 6 seconds a stage and the
replica's rate were within 2.05. It runs the statement on workers only if
the replica would take longer than `IGLOO_WORKER_THRESHOLD` (a minute), or
would run short of memory, and the workers are estimated to be at least
1.5 times faster. Short of memory by what it exchanges, workers need only be
no slower: a report estimated at 52 s, what it reads, took 174 s by way of the
guard, and 68 to 87 s on workers. A working set four or more times the
replica's room runs on workers whatever they are estimated to take, as long as
there are workers.

**Beside other statements.** Statements running at once on the replica
share its cores: one that runs beside others takes about as many times as
long, up to four, and takes their cores. So auto mode weighs the replica's
time at that many times its estimate, or what the statement took there
before: a heavy statement started while others run, as 16 users'
dashboards or an ELT stream, runs on workers rather than among them, where
it would take long enough to be worth workers (`IGLOO_WORKER_THRESHOLD`)
and the workers no slower than that (alone, 1.5 times faster); a statement of seconds stays
on the replica however many run, and one that ran only on workers is not
tried on the replica meanwhile. At ten times the data, beside 16 users'
dashboards and two ELT streams, a report that had run in 25 s on the replica
and 66 s on 16 workers alone ran 577 s on the replica, which auto mode had
chosen, the dashboards' median rising from 4 ms to 23 s; now it runs on the
workers, estimated at 73 s there against 100 s on the replica so shared. The reason in `EXPLAIN DISTRIBUTED` and `query_history.placement`
says how many other statements ran. Without workers, statements share the
replica's cores as DuckDB schedules them; [the replica's
limit](workload-management.md#the-replicas-limit) and roles' limits hold
heavy ones back.

**Rows to the client, and sorts.** Both ways count the client's taking the
result's rows, its fields at 2.8 million a second by DuckDB's plan, whether
one machine sends them or the coordinator merges the workers': 361 million
rows of four columns, ordered, took 516 s to reach the client from one
machine, which was estimated at 30 s. A sort or window that holds half of
the replica's room for a statement or more is risky there, as DuckDB's sort
takes about 40% more memory than its limit (that sort finished within 2 GB
of the memory watch's cancel): where workers are configured, it runs on
them as long as they are estimated at no more than a quarter longer than
one machine. When auto mode runs a statement on the replica, its reason
names the rule that decided: one machine's time within
`IGLOO_WORKER_THRESHOLD`, workers not 1.5 times as fast for a statement
running alone, or no faster than the replica shared, as `EXPLAIN
DISTRIBUTED` and `query_history.placement` show it (`this replica: auto mode
chose one machine: one machine should take 30 s, within the 60 s a
statement must take for workers to be worth it`).

**Spilling.** A machine that runs a statement whole spills to disk when the
rows its joins, groupings and distinct counts hold, its working set, are
more than the 70% of its memory limit it has for them. Igloo estimates the
working set from DuckDB's own plan of the statement, as the session would
run it (`EXPLAIN`): each join holds its build side, each grouping its groups
(as many as DuckDB estimates, but no more than the distinct counts of its
keys allow, from [`ANALYZE`](sql.md) or the theta sketches of Iceberg's
statistics files, and each of DuckDB's threads holds the groups it meets
before they are merged), each distinct aggregate the rows it is given, and
each sort and window its input, at the widths of their columns. Where the
distinct counts of an inner join's keys are known, its rows are the product
of its inputs' over the larger distinct count, as DuckDB, without them,
took a TPC-H join of customers and suppliers by nation for 2,000 rows where
it made 12 million. Where DuckDB's plan ran far off at ten times the data,
Igloo corrects it: the branches of a `UNION` hold no more than the largest
of them, as DuckDB runs them one after another where memory is short (16
branches, each joining 400 million rows to 400 million, were estimated at
793 GB by their sum and held 33 GB), while the operators above it take all
their rows, and the most any branch's filters keep (a report joining its
events to a union of an audience and everyone, taken for the audience
alone, was estimated at 5.6 GB and 15 s, and took 714 s, spilling 235 GB);
an inner join of rows to keys it knows nothing of keeps no fewer of the side with more rows than the other side's
filters keep, rather than the fifth DuckDB takes (157 million events
joined to an audience of everyone kept every row), and no more than that
side's rows: DuckDB's guesses at a join's inputs, each corrected as the last
was, took a report at a hundred times the data for 37.9 TB, 25 TB of it a
window over its metrics, where it is now taken for 4.7 TB; a grouping by keys
it computes keeps no more than a hundredth of its input for what is above it;
a semi-join keeps at least a row for each of its keys; a join to one row, as
of a scalar subquery's latest version, keeps what a filter by a constant
does, where DuckDB keeps every row, unless its scan is filtered so already,
as one audience's rows of a lookup are, which DuckDB counted; one group by
DuckDB's estimate of keys nothing knows is not one row; a `TOP_N` keeps what
it says; a cross product with a small source of
its own, as `FLATTEN` over a list of objects becomes, makes a row for each
pair; a grouping makes no more groups where its input has more rows than
DuckDB took; and a window over the whole input without an order (`OVER
()`) buffers its rows once. Three reports estimated at 793, 25 and 439 GB
that held 33, about 106 (51 in hash tables, 55 spilled) and 9.5 GB are now
estimated at 46, 215 and 14 GB; the values an unnest makes are not
counted. Fitted against what DuckDB was seen holding (the
replica's samples, as `query_history.memory_peak` records them) over joins,
groupings, distinct counts, sorts and windows of 16 million rows, the
estimates are within a quarter for most and a factor of two for all; over
TPC-H's queries at three times its scale that held 300 MB or more, 2.9
times off at the geometric mean, where six times what the stages read, the
estimate before, was 37 times off, and nothing for a join of 9.8 GB whose
smaller side every worker would read whole. Statements' peaks on the
replica correct the estimates further, by the average of peak over
estimate, within a factor of four. A statement is planned so once and
remembered by its plan key. Where the session cannot plan it, the working
set is six times what the stages that feed its joins and distinct counts
read, as filters leave it; for a statement whose shape ran to its end on
the replica, it is what that run allocated, if that is under 90% of the
replica's limit: at the limit, the run may have spilled any amount, and
says only that it held at least that. A working set estimated within half
again a machine's room counts as fitting, on the replica in the share of it
that joins' hash tables hold: their estimates ran up to half again high
where nothing spilled (one of 46 GB, 1.24 times a 49.3 GiB replica's room,
held 33 GB and ran in 98 s), and DuckDB spills a slight overflow at little
cost. Aggregates' and windows' estimates can run low: a report counting
people distinct over a `FLATTEN`'s values, estimated at 41 GB, 1.1 times
the room, held 51 GB and spilled 35 GB, so that much of a working set past
the room counts as spilling. A machine whose working set is over that takes longer by the
ratio to its room to the power of 0.55: twice its room, 1.5 times as long;
six times, 2.7 times as long. Against the scale runs on a
replica of 49.3 GiB, by the working sets they reached (what they held at the
memory limit and what they spilled), eleven reports took 1.2 to 2.9 times
as long as without spilling, at 2 to 10 times the room, and the curve is
within a factor of two of all but one, which took 11 times as long at 6.6.
Past twelve times its room, a machine is taken to run out of memory rather
than spill: reports that reached 12 to 23 times ran out on that replica,
after up to 820 GB of spill, where one at 10 times finished. The replica's
estimate counts its spilling; workers' does not, as their exchanges'
partitions are sized to their memory and a task that runs out runs again
over more of them. `EXPLAIN DISTRIBUTED`'s `spill` rows show the working
set against each machine's room, and what it would spill.

**How many workers.** Igloo weighs the statement on 1, 2, 4 and so on of
the workers, up to `IGLOO_WORKER_MAX`, and on the workers running now, each
with its own estimate (its start, its stages' levels, what its cores read,
and the start of workers it would start), and on the pool's [large
worker](#a-large-worker) if it has one. The trade-off is explicit: of the
ways whose time is within a fifth of the fastest way's, it takes the
cheapest, so it never takes a way that another beats on both time and cost
(costs within 2% count as the same, and the way on fewer workers wins
them). Where every way looks short of memory by its estimate it takes the one
with the most memory, not the replica: tasks spill, and run again over more
partitions with more memory each, and a report so estimated at a hundred
times the data, which 16 workers took 521 to 825 s over, ran on the replica
until it was cancelled at 632 s, 302 GB spilled. A way costs its machines' seconds at their prices an hour
(`IGLOO_WORKER_PRICE_PER_HOUR`, `IGLOO_LARGE_WORKER_PRICE_PER_HOUR` and
`IGLOO_PRICE_PER_HOUR` for the replica, or else the list price of the
workers' instance types, see [prices](#prices)): its workers', with the idle
time a worker started for it stays up after (see [sizing and
cost](#sizing-and-cost)), and the replica's, which waits for the statement
and runs its last step, so that a slower way costs more for as long as it
runs. Fewer workers save only where idle workers stop alone (see [idle
workers](#idle-workers)); where they do not (`IGLOO_WORKER_WARM=off`,
workers started by `command`, or a fixed list of addresses), every running
worker counts for as long as the statement runs, and the fastest way is the
cheapest. At ten times the data, one report took 678, 368, 200, 112 and 127
s on 1, 2, 4, 8 and 16 workers: 8, for $0.133, where 16 cost $0.271 and 4,
which an earlier rule took, estimated within a fifth of 16, cost $0.143 and
60% more time. Another took 44 s on 16 and 75 s on 8: 16, for 7% more than
8. Times a statement took on a number of workers stand for the estimates
(below); a number it has not run on, estimated, as corrected, within 1.4
times the fastest it ran on and not beaten by the way chosen, is tried
once a day to measure it: the first report ran on 16 first, the estimates'
fastest; its 127 s there took 8 to 164 s, so it was tried on 8, and stayed
on 8 at 112 s. A statement's first run takes the estimates' best way, never
a probe, and one of five minutes or more is tried only on a number estimated
within a fifth of its best way's time, so that a probe costs it at most
that: an ordered result of 361 million rows that took 470 s on 16 workers
was tried on one, and took 621 s. Workers sort their shares of an ordered
result whole, for the coordinator to merge, so a worker spills its share
of the sort past its memory as one machine does, which the estimates of
fewer workers count; their other work is partitioned to their memory.
Of more workers running, it takes those with the fewest
tasks; the others serve other statements, or stop when idle. The reason,
in `EXPLAIN DISTRIBUTED`'s `fleet` row and in `query_history.placement`,
says which it took and why, and its `options` row lists each way's time
and cost.

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
history when it starts. Runs count only under the memory they ran with, the
replica's limit or the smallest worker's, within a quarter of what it is
now, which the history keeps (runs from before it kept it are not read):
runs on a replica of 14 GB with workers of 6 GB beside it, read back on one
of 22 GB with workers of their own, had kept a dashboard's statements on
workers. A statement that ran both ways runs where it was faster. One that ran only on the replica within the threshold stays there.
One that ran only one way, in under ten minutes, is tried once the other
way, at most once a day, so that runs come to be compared with runs, if
the other way is estimated at no more than twice what it took, and, on the
replica, if its working set should fit in its memory, within half again
its room (see spilling, below); past that, one that ran only on
the replica, well within its estimate, stays there, and one that ran only
on workers stays there if it beat its estimates, or if the replica should
run short of memory, unless it took more than ten times the replica's
estimate there, and its working set should fit the replica: that one is
tried on the replica once a day, and may take as long there as it took on
workers before the guard (below) runs it on them again. Where its working
set should spill, so far over its estimate says the estimate is wrong, not
that the replica is better: a report estimated at 15 s that took 200 s on
workers took 714 s on the replica. A statement run on the replica with
`distributed_execution = 'off'`, or whose planning gave up, has no
`plan_key`; what it took stands, by its shape, its text with its literals
masked, as read back from the query history too, where its `plan_key` has
no run on the replica of its own, so that it is not estimated again. So does
what the guard (below) learned, which `query_history.placement` keeps. The
replica is not short of memory for a statement whose
shape, its text with its literals masked, ran to its end there, whatever
its estimate (a run cut short, as by the guard, says only that it needed at
least what it held, and does not count: one the guard stopped on a replica
of 53 GB was learned, from the query history, at all of it, which auto mode
took for a working set that fit, where DuckDB's plan put it at about three
times that, and it was tried on the replica, estimated at 47 s, and
spilled 54 GB), and it spills only if that run allocated more than its share
(see [reservations from history](workload-management.md#reservations-from-history)):
the estimate counts what a join exchanges, not what its filters leave, and
held a dashboard's joins of a lookup of billions of rows too large for
22 GB. What a run allocated counts every allocation over the run, not its
peak, so a heavy statement that fits may still count as spilling: such
statements of that dashboard, at 20 seconds on the replica, still ran on
workers. The first time, one that the replica should spill runs on workers
if they are estimated at no more than half again the replica's estimate,
which is then a floor. Runs on workers count with runs on about as many
(1, 2 to 3, 4 to 7, 8 to 15 and so on), kept apart for each, and runs on the
large worker apart from them: how many workers a statement runs on takes
what it took on each before for that one's time, and the others' estimates
times what the nearest it ran on took over its estimate, so a statement
that took 127 s on 16 workers, estimated at 89, is taken to need 164 s on
8, and one that then took 112 s on 8 runs on 8 next. A statement that
failed on a number of workers, or on the large worker, in the last day (a
worker out of memory, a read that failed) is taken to need one and a half
times as long as it ran there before it failed, so that other ways weigh
against it: one that took 729 s on 16 workers, and then failed there after
2,078 s, is taken to need 3,117 s there. The query history records where a
statement failed, and replicas read it when they start. One that
ran on a quarter of the workers it would run on now, as in an earlier
session on a smaller fleet, is tried on all of them, at most once a day,
whatever it took on the replica. The server logs each choice, `chose where
a statement runs`, with the estimates, how many workers and why, its
spilling, and the reason.

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
to itself. So is one the first time, where the statistics of the tables it
reads bound it under what every worker may read: a grouping (`GROUP BY` or
`DISTINCT`) of columns makes no more rows than the product of their
distinct counts (from [`ANALYZE`](sql.md) or Iceberg's theta sketches), and
a filter by `=` keeps no more of a table than its rows over the column's
distinct count. At a hundred times the data, partitioning 16 workers'
tables beside an audience of 1.9 million members, the distinct members of
a lookup's latest version, took a report 305 s, and 50 s measured first.
`EXPLAIN DISTRIBUTED` lists such relations as `uncertain size`, and
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
run it, is stopped there once, without sending a row, it has run three
times its estimate, or its estimate once while other statements run on the
replica beside it, which it takes cores from, and as long as the workers
are estimated to take it from scratch, their start included, and at least
15 seconds; the guard weighs it again every second, as statements come and
go beside it. It then runs on workers, started if they are stopped, the
client getting its rows as if it had run there. Moving once the run has taken what the workers
would take bounds the loss at about twice the faster side, as long as the
workers' estimate holds: a report estimated at 13 s on the replica and 30 s
on workers, which took 58 to 97 s on the replica and 50 s on workers at ten
times the data, moves after 39 s, where a fixed floor of 90 s let it run 90
s first. A statement whose workers are estimated slower than its run so far
is not moved, however long it runs. The server logs `a statement ran on
this replica far past its estimate; running it on workers instead`, counts
it (`igloo_auto_guard_reruns_total`), and runs the statement on workers
from then on, never back on the replica, so the guard cannot send it back
and forth. A statement that has begun sending rows runs to the end, and so
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

A statement whose last runs already decide that it stays on the replica, as
one that took under the threshold there, does not plan for workers again: its
first run pays for that, and a run far past what it took plans again.

Planning a statement for workers ends with the session's
`statement_timeout` and a cancel request, which end `EXPLAIN DISTRIBUTED`
as they end any statement; a part whose relations workers compute nest
more than 64 deep, or one that would compute a relation from itself, runs
on one machine. In auto mode planning stops once it stalls: a statement
runs on one machine once planning has planned no stage and described no
new part for a tenth of the time the statement is expected to take there
(what it took before, else the estimate), between half a second and a
minute, or once planning has taken that time in all. A report expected
to take minutes plans as long as it progresses; a cheap statement whose
planning stalls waits half a second for it. `distributed_execution = on`
and `EXPLAIN DISTRIBUTED` plan for as long as planning takes. Planning
describes each part of the statement in the session once, keeps the SQL
it printed of each part, and logs `planned a statement` with its time,
stages, descriptions and other lookups in the session when it takes a
second or more.

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

A stage keeps only some groups early (those `HAVING` keeps, the first under
`ORDER BY ... LIMIT`, those a ranking may keep) only where it applies first
everything SQL applies before that cut: the whole `HAVING`, and no window
function that sees every group on the way. Otherwise it sends every group and
the coordinator cuts them, as one machine would. Two tests run in the full
suites to hold the planner to this. One runs all 22 TPC-H and 99 TPC-DS queries
on one machine and on workers, forced with the planner's joins, forced with
every join partitioned, and in auto mode, and compares the rows. The other
writes random queries in the shapes the planner rewrites, in combination:
groups ranked, limited and filtered by `HAVING` with subqueries, windows,
`QUALIFY`, `DISTINCT`, set operations, outer joins, `IN`, `ANY`, `ALL` and
`EXISTS` subqueries, over `NULL`s and ties.

A statement reads each table at one snapshot, its current one when the
statement's parts start, everywhere: workers read that snapshot's data
files, and where the part the coordinator runs itself reads the same table
again, it reads it at that snapshot too (`AT (VERSION => ...)`). Commits
made meanwhile, by Igloo or by other engines through the
[Iceberg REST catalog](external-engines.md), are not seen by either part,
whether they add rows or delete files or change the schema. A table whose
columns changed between planning and the start, by name or by type (a
`FLOAT` promoted to `DOUBLE`, say, or a field nested in a struct), runs on
one machine, as does a statement whose coordinator part reads a view it
cannot read as its query (one with common table expressions, say), which
could read a table at another snapshot; other views read as their
queries there too, at the workers' snapshots. Tables may share files, as
clones do: workers read the files the metadata lists, wherever they lie in
the warehouse.

If a worker cannot be reached, stops in the middle of a statement or stops
answering its status (three answers missed, a second apart, about 7 seconds)
while it runs a task, what it held runs again on the others, and the failed
worker is left out for 30 seconds. A worker that answers a task with an error ran it and is healthy:
the statement then runs on the coordinator at once, without trying other
workers or leaving any out. Object storage turning requests away for now
(S3's 503 SlowDown) is not such an error: DuckDB tries the request again
for half a minute, and the coordinator then runs the task again, up to four
times over about a minute and a half. If no worker can run it, its partial results
would take more than a quarter of the coordinator's free disk (at least
1 GiB), or the parts cannot
be combined, the statement also runs on the coordinator, so a client sees a
slower statement, not an error; but not one whose exchanged rows Igloo
estimates at more than the coordinator's memory holds, that ran out of
memory there before, or whose working set by DuckDB's plan is four or more
times the coordinator's room for it, whatever a run of its shape allocated
there at a tenth of the data, which would run out of memory there or
spill for far longer than the workers take: that fails at once, with the
workers' error. One that auto mode ran on the [large
worker](#a-large-worker) runs on the other workers instead, as a statement
forced onto them does: at a hundred times the data, a report of 250 GB
whose large worker failed ran on the coordinator, spilled 167 GB and was
killed after 9 minutes, where 16 workers took 729 s. Workers stay
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
writer files of a million rows of the exchange it read. Partitions grow
only as far as the tasks that write the exchanges have memory for a row
group of each; where that keeps them from doubling, each task takes twice
the worker's slots instead, up to all of them, so that fewer run at once
and each has twice the memory, and its writers room for twice the
partitions. If those
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
- **Policies keep a statement on one machine.** A statement that reads a
  table masking or row access policies apply to, directly or through a
  view, runs on the replica, where Igloo applies them. Workers never see a
  masked column's raw values or a hidden row. So does one that reads a
  table a [clean room](clean-rooms.md) keeps to its party, a room's runs
  included. What a statement reads is what applying the policies finds,
  resolving its names: one that only spells a protected table's name, as a
  column named like it does, or a column named like an ID domain, whose
  dictionary is Igloo's own protected table, or that reads a view whose
  text does, runs on workers when it reads none. The planner refuses a
  protected table besides, whatever reaches it.
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
  Where workers have room on their disks (the default,
  `IGLOO_EXCHANGES=auto`), or with `IGLOO_EXCHANGES=local`, they are instead
  files on the workers' disks, which workers send each other over the task
  port: see [exchanges on workers' disks](#exchanges-on-workers-disks).
- **Workers write only a statement's directory.** A worker refuses a task
  that writes anywhere but a directory named `igloo-write-<hex>` in the
  warehouse or `IGLOO_ALLOWED_LOCATIONS`. Where the coordinator can make
  credentials limited to it (S3 with `IGLOO_S3_STS_ROLE_ARN`, and S3-compatible
  stores whose STS honors session policies, which Igloo checks first), each
  attempt of a task carries its own: its own directory, read and write, no
  delete, an hour at most, and its DuckDB uses them for that directory alone.
  The workers' own identity then needs no write access to tables at all, and
  the OpenTofu module for AWS gives it none. Google Cloud and Azure have nothing
  DuckDB could use of the kind: their modules give workers an identity that
  can only create files under `_igloo_shuffle/` and directories of that name
  under tables' locations, with no move or delete, and on Google Cloud no overwrite;
  on Azure, DuckDB's appends need the right to overwrite there. A worker told no gets the
  statement planned again, the coordinator writing the table
  (`IGLOO_WORKER_WRITES=off` always does), and no worker is asked for ten
  minutes.
- **A worker's word is not taken.** Before it commits a file, the coordinator
  checks that the attempt's answer names it under the directory it gave that
  attempt, plainly (no `..`, no escapes, no query), and adds up to the rows
  it says; then it reads the footer itself, with its own credentials, so the
  file must exist, match the table's schema by field ID, and hold the rows and
  bytes the worker said. The data file committed is made from what it read.
  Any mismatch commits nothing of the statement, which then runs on the
  replica, and logs an error. A compromised worker can put wrong rows in a
  table it is writing, as it can return wrong rows to any read, and its wrong
  rows can come with statistics that hide them: a file's partition and column
  bounds come from the statistics in its footer, which the worker wrote, so a
  filter on such a column, a `DELETE`'s too, can pass them over. Erasure requests
  hash every row's values, and no statistic hides a row from those. Until its
  credentials expire it can overwrite the files of its own attempt. Nothing
  reads those by then: what is checked and committed is the coordinator's
  copy, in a directory no worker's grant names. It cannot get a file outside
  its directory committed, nor write outside it with those credentials.
- **Pools are kept apart by secret and network.** Each compute pool's
  workers have a secret of their own (the Helm chart generates one per
  pool), so one pool's igloo pods cannot use another's workers; the
  NetworkPolicy keeps them apart too, and is only as good as the cluster's
  CNI enforcing it: with several pools, use one that does. Docker Compose
  has no network isolation between its containers; give a pool's workers
  their own `IGLOO_WORKER_SECRET`.

## Exchanges on workers' disks

Workers can exchange rows through the warehouse's object storage, which
costs a request a file to write and several to read, tens of milliseconds
each, and keeps what they wrote if a worker is lost; or a worker writes
them to its own disk (`IGLOO_DUCKDB_TEMP_DIR`, with spill), and the workers
that read them fetch the files from it, whole, to their own disks first. In a lab of four
writers and four readers of 3 GB over 128 partitions, a stage that took 54 s
through an object store answering in 25 ms took 17.5 s through the writers'
disks, and the writers needed 0.9 GB of memory rather than 1.6.

`IGLOO_EXCHANGES=auto`, the default, has workers exchange a statement's
rows on their disks when every worker it runs on reports at least 1 GiB of
room for them there, and through object storage otherwise. `object` always
uses object storage, and `local` the workers' disks wherever a task's share fits.
On 16 workers with NVMe, at ten times the data, two reports took 34 s and
62 s with exchanges on the workers' disks, where they took 95 s and 205 s
through S3 (warm medians of three; the same rows, the same peak memory on
the workers, and no exchange objects in S3 rather than 11,000 and 19,000),
and a statement through S3 at a hundred times the data had requests turned
away (503 SlowDown). The Helm chart's default, `workers.exchanges: auto`, sets
aside 30% of the workers' spill volume for them, an `emptyDir` on the node's
disk, which goes with the pod as they may, and lets workers reach each other's
task port; on a volume in memory it keeps them in object storage. Run workers on
other manifests only with that port open between them, or say `object`.

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
  answers as a new run of itself, what it wrote that readers still need runs
  again on the other workers, those tasks' inputs it held first, and the task
  on what is there now: nothing else runs again. Each of those tasks, and a
  task whose worker is lost while it runs it, runs as smaller tasks spread
  over the workers left, one each, divided by its files, buckets or
  partitions, so that a loss costs about a worker's share of a stage over
  the workers left rather than all of it on one. A statement with `LIMIT`,
  samples or a volatile function among those tasks could not mix two runs of
  them, and its parts all run again, once, as they do if a task writes other
  rows than it did. One that still answers as the same run was busy, and the
  task runs again a few times before the statement fails (and runs on the
  server if it fits there). A worker too busy to send a
  file says so within five seconds and is asked again for up to five
  minutes; one that stops sending for a minute, or answering its status for
  about 7 seconds, counts as lost. A statement
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
- **Networks.** Workers must reach each other's task port (7433). The Helm
  chart's `workers.exchanges`, `auto` by default, sets `IGLOO_EXCHANGES`,
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
  `explain_value`: where it would run (`runs on`: `workers`, `the large
  worker` or `this replica`) and why (`reason`: auto mode's choice, the
  session's `distributed_execution`, or what keeps it here); the `estimate`
  auto mode weighs (one machine's seconds, workers' seconds and how many,
  the stages' levels, what they read and exchange, the rows they go
  through, and whether it is too large for the replica's memory); `working
  set`, where it comes from and what DuckDB's plan says its largest holders
  hold; `spill`, its working set against the replica's room for it, and the large
  worker's, and what each would spill and how much longer it would take;
  `fleet`, how many workers, or the large worker, auto mode would take and
  why, and `options`, each way it weighed with its time, corrected by what
  the statement took before, and its cost; the `partitions` its
  exchanges would have with the workers running now; each stage (`stage 0
  split public.events to exchange 1 (join)`, `stage 1 buckets of
  public.orders buckets of public.visits to the coordinator`) with the SQL
  its tasks run; which bucketed tables are read by bucket with no exchange,
  and why others are exchanged (`buckets`, see [bucketed
  tables](#bucketed-tables)); what `the coordinator` runs over their
  results; and why parts of it
  `stay on the coordinator`. After the statement has run on workers,
  `measured` says what its exchanges held (rows and gigabytes) and the most
  rows a stage wrote for each row it read, as the replica remembers them:
  auto mode keeps on workers a statement whose exchanges would not fit in
  the replica's memory, but never moves one to workers by them. A stage
  that joins a table filtered by a constant to other rows, as an audience
  joins events, shows as a `semi-join estimate` of the share of rows it
  keeps, which the estimates do not count yet. The query is checked as it
  would be when run, grants and policies included, and the same rewrites
  apply: it is prepared again as its run prepares it, through ID domains'
  ids and bitmap indexes with the bitmaps it reads, and planned and checked
  against its tables' current snapshots as running it is. `runs on` is
  where running it now would run it; only what workers find in the data
  files themselves, such as files without field IDs, can still keep it
  here.

  ```sql
  EXPLAIN DISTRIBUTED
  SELECT campaign_id, count(DISTINCT device_id) FROM impressions GROUP BY 1;
  ```

- **Query history.** `igloo.query_history` has a `workers` column: how many
  workers ran parts of the statement, 0 when it ran on one replica; and
  `stages` and `exchanges`: how many stages of tasks those parts took, and
  how many exchanges of rows, in object storage, the stages wrote for later
  ones. A statement built in steps waits for each stage that reads what an
  earlier one wrote. `placement` says where a statement that could run on
  workers ran and why: `this replica`, how many workers or `the large
  worker`, auto mode's reason and how it chose the workers, and whether the
  guard moved it.

  ```sql
  SELECT start_time, duration_ms, workers, stages, exchanges, placement, query_text
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
  | `igloo_auto_guard_reruns_total` | | Statements auto mode ran on the replica that ran far past their estimate there (see [when](#when)), and ran on workers instead |

  and on the workers' port 8080: `igloo_worker_tasks_total{outcome}`
  (`out_of_memory` counting tasks a worker canceled as it ran out),
  `igloo_worker_task_seconds`, `igloo_worker_sent_bytes_total`, and the read
  cache's `igloo_cache_*`.
- **Logs.** The coordinator logs `distributed execution is on` at start,
  `started workers` and `stopped idle workers`, and a warning when a task
  fails on a worker or a statement falls back to the coordinator, with the
  reason. Set `IGLOO_LOG_LEVEL=debug` to log why each statement ran where it
  did.

## Erasure requests

Workers keep what they read in their read caches, and more, past a
statement. Table files workers wrote are a table's files like any others, and
what a stopped write left under a table's location is among its orphan files,
which a request deletes, or waits for and verifies are gone
([what a request does](privacy.md#what-a-request-does)). [Erasure requests](privacy.md#workers) have every worker purge
what they deleted: coordinators push the purge within 15 seconds, every
task carries the last deletion its coordinator knew of, and a worker that
was stopped meanwhile purges before it serves again. A worker that hears
from no coordinator for `IGLOO_WORKER_ISOLATED_PURGE_AFTER` (a minute)
empties its caches by itself. A request waits for the running workers,
and for a while for those no coordinator reaches, and is blocked while a
worker whose read cache outlives it (on a disk kept when it stops) does
not run and has not purged, or while a worker does not say what it
keeps; an admin deregisters a worker that will not come back with
`DEREGISTER WORKER`.

## Deploying workers

Workers are the same image as Igloo, started with `igloo worker`. They need
the warehouse location and object storage credentials (the same
`IGLOO_WAREHOUSE_LOCATION`, `IGLOO_S3_*`, `IGLOO_GCS_*` or `IGLOO_AZURE_*`
settings as the server), `IGLOO_WORKER_SECRET`, a spill directory and,
ideally, a disk for the [read cache](operations.md#read-cache). They need no
catalog, database or users.

Their object storage identity needs to read the warehouse and to write
`_igloo_shuffle/` in it. To [write tables' files](#workers-writing-tables), set
`IGLOO_S3_STS_ROLE_ARN` on the server (on AWS; the role must be able to write
the warehouse, and the server to assume it), or otherwise let the identity
create objects in directories named `igloo-write-*` under tables' locations
(`arn:aws:s3:::BUCKET/warehouse/*/data/igloo-write-*/*`). Give it no more than
that, and do not share the server's: without one or the other, a worker is
refused, and the coordinator writes the table. With the server's own
identity or keys, workers can write the whole warehouse, whatever they are
asked; the Helm chart turns worker writes off then, as with its bundled store,
unless `workers.writes` says otherwise.

### Lifecycle rule

Statements delete what they exchanged under `_igloo_shuffle/` (a thousand
files a request on S3), but a coordinator that stops mid-statement cannot.
A statement's files are spread over 16 directories,
`_igloo_shuffle/<digit>.<statement>/`, so that S3, which serves a prefix
so many requests a second, splits the load over prefixes every statement
shares. Every replica sweeps `_igloo_shuffle/` a minute after it starts
and hourly, and removes the files of statements whose removal began
(`_igloo_shuffle/0.<statement>/removing`), and of statements under which
nothing was written for six hours (a statement that still runs writes
there every half hour), taking a statement's directories together. A lifecycle rule
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
Deployment up when a statement needs workers, down to the workers busy
and one more while some work (see [idle workers](#idle-workers)), never
below what another replica uses, and to zero after `workers.idleAfter`
without tasks; the cluster autoscaler or Karpenter then removes their
nodes. With [scale to zero](scale-to-zero.md), a pool's gateway stops the
pool's workers along with its igloo pods. `workers.pricePerHour` tells
auto mode what one worker's machine costs an hour
(`IGLOO_WORKER_PRICE_PER_HOUR`). See the
[chart's README](../deploy/helm/igloo/README.md#distributed-execution).

The OpenTofu modules for [AWS](../deploy/aws/README.md#distributed-execution-workers),
[Azure](../deploy/azure/README.md#distributed-execution-workers) and
[Google Cloud](../deploy/gcp/README.md#distributed-execution-workers)
provision the workers' capacity by default (`workers_enabled`, as the chart's
`workers.enabled`), which costs nothing until a statement starts them: node pools that scale from zero, or on GKE Autopilot a compute
class, on [Spot capacity with an on-demand fallback](#spot-workers), and
the chart's `workers` values to match.

On Kubernetes, workers exchange rows on their spill volume where they have room
(`workers.exchanges: auto`): an `emptyDir` on whatever disk the node keeps
kubelet's directory on, which is local NVMe on instance-store node types, and
exchanges on it take a third of it from spill. The modules run workers on Spot, whose
nodes can go with the rows they hold: such a statement runs its parts again, once, on
the others. `object` keeps them in object storage; see [exchanges on workers'
disks](#exchanges-on-workers-disks) for what either gains.

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

Run `igloo worker` on each machine, under a supervisor that starts it again
when it exits (systemd's `Restart=always`, or `docker run --restart
unless-stopped`), and list them in `IGLOO_WORKERS`. A worker exits rather
than hang when DuckDB crashes or a C++ exception escapes it; the Compose
file and the Helm chart restart workers that exit. To
start and stop them on demand, set `IGLOO_WORKER_COMPUTE` to `ec2` with the
instances' IDs, or to `command` with commands that start and stop them all
(an instance group, for example), as for the [scale-to-zero
gateway](scale-to-zero.md#on-google-cloud-or-azure-vms). Set
`IGLOO_WORKER_PRICE_PER_HOUR` to what one worker machine costs an hour, for
auto mode's estimates.

### A large worker

Besides many small workers, a compute pool can have one large worker, a
machine of 512 GiB or so, on which auto mode may run a whole statement
alone. Its estimate is one machine's of its cores and memory, spilling as
the statement's working set says (see [when](#when)), plus a statement's
start, 2 seconds a level of its stages, its exchanges staying on its own
disk, and its own start if it is stopped; a statement that would run out
of memory there is never run there, and one whose working set would spill
there, past half again its room, runs there only once it ran there, or
where the small workers cannot run it: the spill curve holds for most
reports but runs far short for some, and at a hundred times the data a
report whose working set was 5.3 times the large worker's room, estimated
at 217 s there, spilling 2.5 times as long, ran past 1,800 s there, where
16 workers ran it in 729 s (at ten times the data it spilled 11 times as
long on one machine). The small workers share a working set over their
partitions instead. Auto mode runs a statement there when
that estimate is within a fifth of the fastest way and the cheapest of
those, at the prices the pool gives (see [when](#when)): at a hundred
times the data, a report whose stages read 49 GB and fed 296 GB of working
set fits a large worker of 440 GiB, and is estimated faster there than on
16 workers and at a fraction of their cost. It plans the statement again for it, with its
memory, so that fewer tables are partitioned. Runs on it are kept apart in
the history (`query_history.placement` begins `the large worker`), and its
read cache is purged for [erasure requests](#erasure-requests) as the other
workers' are. The large worker is the same image started with `igloo
worker`, with the same `IGLOO_WORKER_SECRET` and storage settings as the
other workers, and is stopped while it has no work. Statements an admin
forces onto workers (`distributed_execution = on`) use the small workers.

On Kubernetes, `workers.large.enabled: true` adds a second worker
Deployment, `<release>-workers-large`, of one pod with its own resources,
volumes and scheduling, starting at zero pods, and gives the igloo pods its
address and size; each compute pool gets one of its own
(`<release>-pool-<name>-workers-large`). Give it a node pool that scales
from zero:

```yaml
workers:
  enabled: true
  pricePerHour: 0.31
  large:
    enabled: true
    pricePerHour: 4.35
    resources:
      requests: { cpu: "60", memory: 448Gi }
      limits: { memory: 448Gi }
    nodeSelector: { pool: igloo-large-worker }
```

The OpenTofu modules provision that node pool with `large_worker_enabled =
true`: an on-demand `r7gd.16xlarge` on AWS, a `Standard_E64pds_v6` on Azure,
an `n4-highmem-64` through a compute class on GKE Autopilot, each Spot
first with `large_worker_spot = true`. See the [chart's
README](../deploy/helm/igloo/README.md#a-large-worker).

On virtual machines, run `igloo worker` on the large machine as on the
others, and point the server at it:

```sh
IGLOO_LARGE_WORKER=large-worker.internal          # host[:port], port 7433 by default
IGLOO_LARGE_WORKER_COMPUTE=ec2                    # or command, or empty when it always runs
IGLOO_LARGE_WORKER_EC2_INSTANCE_IDS=i-0123456789abcdef0
IGLOO_LARGE_WORKER_CORES=64
IGLOO_LARGE_WORKER_MEMORY=400GiB                  # its DuckDB memory limit
IGLOO_LARGE_WORKER_PRICE_PER_HOUR=4.35
IGLOO_WORKER_PRICE_PER_HOUR=0.31                  # one small worker
```

With `IGLOO_LARGE_WORKER_COMPUTE=command`, `IGLOO_LARGE_WORKER_START_COMMAND`
and `IGLOO_LARGE_WORKER_STOP_COMMAND` start and stop it instead (on Google
Cloud or Azure, for example). Igloo uses `IGLOO_LARGE_WORKER_CORES` and
`IGLOO_LARGE_WORKER_MEMORY` for its estimates while the large worker is
stopped and has never answered, so set them to the machine's cores and the
worker's `IGLOO_DUCKDB_MEMORY_LIMIT`.

### Settings

On the Igloo server:

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_WORKERS` | | Workers' addresses, `host[:port]` comma separated (port 7433 by default). A host may resolve to several workers, as a Kubernetes headless Service does. Empty turns distributed execution off. |
| `IGLOO_WORKER_SECRET` | (required with workers) | At least 32 characters, the same on the server and the workers |
| `IGLOO_WORKER_COMPUTE` | | How to start and stop workers: `kubernetes`, `ec2`, `command`, or empty when they always run |
| `IGLOO_WORKER_MAX` | `16` | The most workers Igloo starts, as it weighs how many a statement should run on |
| `IGLOO_WORKER_IDLE_AFTER` | `auto` | Stop workers after this long without tasks; `auto` for three times what their last start took, from 2 to 10 minutes |
| `IGLOO_WORKER_WARM` | `1` | While some workers work, how many idle ones past them stay up; the others stop alone, on Kubernetes and EC2 (see [idle workers](#idle-workers)); `off` stops workers only all together |
| `IGLOO_WORKER_START_TIMEOUT` | `3m` | How long a statement waits for workers to start before it runs on the server instead |
| `IGLOO_WORKER_THRESHOLD` | `1m` | In `auto` mode, how long a statement must take on the server alone before it may run on workers (see [when](#when)) |
| `IGLOO_EXCHANGES` | `auto` | Where workers exchange rows: `auto`, their disks where every worker has room, object storage otherwise; `object` storage; or `local` disks (see [exchanges on workers' disks](#exchanges-on-workers-disks)) |
| `IGLOO_WORKER_WRITES` | `on` | Whether workers [write tables' files](#workers-writing-tables) for statements that fill a table from a query; `off`: the coordinator writes them from their results |
| `IGLOO_WORKER_K8S_DEPLOYMENT`, `IGLOO_WORKER_K8S_NAMESPACE` | | `kubernetes`: the workers' Deployment, and its namespace if not the server's |
| `IGLOO_WORKER_EC2_INSTANCE_IDS` | | `ec2`: the workers' instances |
| `IGLOO_WORKER_START_COMMAND`, `IGLOO_WORKER_STOP_COMMAND` | | `command`: shell commands that start and stop all workers |
| `IGLOO_WORKER_PRICE_PER_HOUR` | `0` | What a worker costs an hour, for weighing how many workers, or the large worker, a statement runs on (see [when](#when)); `0` for unknown, when the list price of `IGLOO_WORKER_INSTANCE_TYPE` stands for it (see [prices](#prices)) |
| `IGLOO_WORKER_INSTANCE_TYPE`, `IGLOO_WORKER_SPOT`, `IGLOO_CLOUD_REGION` | | The workers' instance type (`m7gd.4xlarge`, `Standard_D16pds_v6`, `n4-standard-16`), whether they run on Spot (`false`), and the region, for their list price where no price is set |
| `IGLOO_LARGE_WORKER_INSTANCE_TYPE`, `IGLOO_LARGE_WORKER_SPOT` | | The same for the large worker |
| `IGLOO_LARGE_WORKER` | | The pool's [large worker](#a-large-worker), `host[:port]` (port 7433 by default), which auto mode may run a statement on alone; empty for none. Needs `IGLOO_WORKERS` |
| `IGLOO_LARGE_WORKER_COMPUTE` | | How to start and stop it: `kubernetes` (`IGLOO_LARGE_WORKER_K8S_DEPLOYMENT`, in the workers' namespace), `ec2` (`IGLOO_LARGE_WORKER_EC2_INSTANCE_IDS`), `command` (`IGLOO_LARGE_WORKER_START_COMMAND`, `IGLOO_LARGE_WORKER_STOP_COMMAND`), or empty when it always runs |
| `IGLOO_LARGE_WORKER_CORES`, `IGLOO_LARGE_WORKER_MEMORY` | | Its cores and DuckDB memory limit (a size such as `400GiB`), for estimates while it is stopped and has not answered; required with `IGLOO_LARGE_WORKER_COMPUTE` |
| `IGLOO_LARGE_WORKER_PRICE_PER_HOUR` | `0` | What it costs an hour; `0` for unknown |

Distributed execution also needs `IGLOO_DUCKDB_TEMP_DIR` (set in the image),
where partial results are kept while a statement combines them.

On a worker (`igloo worker`): `IGLOO_WORKER_SECRET`,
`IGLOO_WORKER_LISTEN_ADDR` (`:7433`), `IGLOO_WORKER_SPOT_NOTICE` (`on`; see
[Spot workers](#spot-workers)), `IGLOO_WORKER_ISOLATED_PURGE_AFTER` (`1m`,
at least `10s`: how long it goes without hearing from any coordinator
before it empties its caches by itself, for [erasure
requests](privacy.md#workers)), the server's storage settings, and
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
machine's memory run where that machine would run out of memory. The tail
is what workers cost after their last task until they stop: with
`IGLOO_WORKER_IDLE_AFTER=auto`, the default, three times what their last
start took, from 2 to 10 minutes, about as long as a start costs. Workers
that start in half a minute stop after 2 idle minutes; on a 32-core machine
at $1.71 an hour, a 10-minute tail cost $0.28 after each burst of work.

### Idle workers

Statements run on as many workers as they need (see [when](#when)), so
while one runs on 4 of 16, the other 12 may sit idle. While some workers
work, the workers idle for that tail, by their own count, which any
replica's tasks reset, stop alone, but for the `IGLOO_WORKER_WARM` of them
used last (one by default; `off` stops workers only all together): the
replica retires each, which then takes no more tasks and reports itself
not ready; 15 seconds later, on Kubernetes, the replica scales the
Deployment down from the workers that answered to those that remain, and
Kubernetes removes the pods that are not ready first; on EC2
(`IGLOO_WORKER_COMPUTE=ec2`) it stops the worker's instance, which the
worker named from its instance metadata, and a statement that needs
workers starts only as many instances as it needs. A replica that sees
pods still starting, or the Deployment changed by another, leaves it for
the next check, and a retired worker not stopped serves again when a
statement wants more workers than run, or after three minutes. Workers
started by `command` stop only all together. When every worker is idle,
they all stop as before. Auto mode's choice of how many workers counts
this: where idle workers stop alone, a statement on 4 of 16 running workers
costs 4 workers' time; where they do not, it costs all 16's, since they
stay up while it runs, and it runs on as many as are fastest (see
[when](#when)).

A worker stopped alone is one whose read cache goes with it, as a pod's
volumes do, or that was purged of every deletion of [erasure
requests](#erasure-requests) its replica knew of: one whose cache outlives
it and that had not purged would block requests until it ran again.

### Prices

Auto mode weighs the ways a statement could run by what their machines
cost (see [when](#when)). `IGLOO_WORKER_PRICE_PER_HOUR` and
`IGLOO_LARGE_WORKER_PRICE_PER_HOUR` set those prices; where they are not
set, Igloo takes the list price of the machines' instance types from a
table it carries (`internal/prices`): `IGLOO_WORKER_INSTANCE_TYPE` and
`IGLOO_LARGE_WORKER_INSTANCE_TYPE` (such as `m7gd.4xlarge`,
`Standard_D16pds_v6` or `n4-standard-16`), on Spot with `IGLOO_WORKER_SPOT`
and `IGLOO_LARGE_WORKER_SPOT`, in `IGLOO_CLOUD_REGION`. The Helm chart sets
them from `workers.instanceType`, `workers.spot`, `workers.region` and
`workers.large.instanceType` and `spot`, and the OpenTofu modules from their
worker variables. A worker on an EC2 virtual machine also reports its own
instance type, region and Spot from the instance metadata service, which
Igloo prices where it was told nothing.

The table's numbers are list prices, not anyone's bill: on-demand, Linux,
by the vCPU of each instance family as the providers list them for their
base regions (AWS us-east-1, Azure eastus, Google Cloud us-central1) in
October 2026, rounded. AWS's match the benchmarks' `bench/pricing.toml`;
Azure's and Google Cloud's are approximate. A machine of a family costs as
its vCPUs, which is what weighing a large machine against small ones of
the same family needs. Other regions are the base region's price times a
rough regional factor (eu-west-1 1.11, ap-northeast-1 1.28 and so on; a
region the table does not know counts as the base region), and Spot is a
typical discount (AWS 65%, Azure 70%, Google Cloud 65%) that in fact varies
by type, zone and hour. Set the prices for exact or negotiated rates. Without
a price or a known type, a machine's price goes with its cores; so does the
replica's, which counts for as long as a statement on workers runs, unless
`IGLOO_PRICE_PER_HOUR` sets it. `EXPLAIN
DISTRIBUTED`'s `options` row says whether the costs it weighed are in
dollars.

### Spot workers

Workers suit Spot capacity, 50 to 75% cheaper on AWS: they keep nothing a
statement needs once their tasks end, a task a lost worker ran runs again on
another, and with exchanges in object storage (`IGLOO_EXCHANGES=object`)
nothing they wrote is lost with them; with exchanges on their disks (`auto`,
the default), a statement that loses a worker holding
rows it exchanged runs its parts again, once, on the others (see
[exchanges on workers' disks](#exchanges-on-workers-disks)). Keep Igloo
itself, the catalog and its
database on regular capacity: igloo pods hold client sessions, and the
modules keep them on-demand unless asked otherwise. With `workers_enabled`,
the OpenTofu modules run workers on Spot by default (`workers_spot = true`)
and fall back to on-demand capacity when Spot has none:

- AWS: a Spot and an on-demand managed node group for workers, both from
  zero nodes. The Kubernetes cluster autoscaler's priority expander tries
  the Spot group first; a Spot group whose launch fails, or whose node has
  not joined within five minutes (`--max-node-provision-time`), is backed
  off, and the on-demand group gets the worker. EKS drains a Spot node once
  AWS sends a rebalance recommendation for it.
- Azure: a Spot and a Regular user node pool for workers, both from zero
  nodes. The AKS cluster autoscaler's priority expander (the
  `cluster-autoscaler-priority-expander` ConfigMap) tries the Spot pool
  first and backs it off the same way, after five minutes at the latest
  (`max_node_provisioning_time`); worker pods tolerate the Spot taint and
  select a label both pools carry.
- Google Cloud (GKE Autopilot): a compute class whose priorities are Spot,
  then on-demand, of one machine series; Autopilot creates a worker's node
  from the first it can get.

A statement waits for workers at most `IGLOO_WORKER_START_TIMEOUT` (3
minutes) and then runs on its replica, so a statement that meets a Spot
shortage may run without workers while the fallback nodes start. The large
worker stays on-demand unless `large_worker_spot = true`: it runs whole
statements, which a reclaimed Spot node would end.

AWS says two minutes ahead that it is taking a Spot Instance back; a
worker on EC2 watches for that (instance metadata, IMDSv2), and from then
on takes no more tasks, which coordinators send to other workers,
finishes those it runs, and serves the exchange files on its disk until
the statements that read them end, until ten seconds before the time AWS
gave: the statements run on, rather than run again the parts whose files
it holds, as for a worker lost. A worker stopped by a signal
(SIGTERM) does the same for up to 110 seconds, and one with nothing
running and no files stops at once. A worker
in a container reaches the metadata service only with host networking or
the instance's hop limit at 2 (`HttpPutResponseHopLimit`); without it, it
logs that it is not watching, and a Spot Instance taken back fails its
tasks over as a lost worker's. The AWS module's node groups keep the hop
limit at 1, so pods cannot take the node's credentials, and rely on EKS
draining the node. On Kubernetes, where a Spot node is drained first, a stopping
worker so drains for up to 110 seconds, within the pod's grace period
(`workers.terminationGracePeriodSeconds`, 125 by default). Ask for
several instance types and zones, so that capacity one lacks another has.

## Limitations

- The large inputs of one `SELECT` are partitioned by one key. Large tables
  joined on different keys need a step each: a common table expression or
  subquery per key. A large table read again in a correlated subquery
  expression that is not the join it means (`NOT IN`, a correlation by
  anything but equal columns) runs on one machine; written as a join to a
  common table expression, it runs distributed. Give the machines that run
  such statements a lot of memory and fast spill disk.
- `ROLLUP`, `CUBE` and `GROUPING SETS` run on workers up to the finest groups
  of their sets, and the coordinator, or in a subquery or common table
  expression one task, computes the sets from those groups: its memory grows
  with them, as a plain `GROUP BY`'s does not.
- Where workers have no room on their disks, or with `IGLOO_EXCHANGES=object`,
  partitioned joins, aggregations and every step of a multi-stage plan
  write what they exchange to object storage and read it back: that costs
  request charges and adds a few seconds per step, and
  leftovers of a coordinator that crashed mid-statement stay under
  `_igloo_shuffle/` for six hours or more, until a replica's sweep removes
  them.
- The coordinator receives an aggregation's result, one row a group, and
  keeps it on its disk until the statement has run: a `GROUP BY` whose
  result is billions of groups sends it all of them. Groups taken to be few
  (see [what runs distributed](#what-runs-distributed)) it receives from
  every worker, as partial results.
- Tables with data files without field IDs, views that group, sort or
  limit, and time travel to any snapshot but a table's latest run on one
  machine.
- Workers read each table's files directly and do not use Iceberg partition
  statistics to skip files; DuckDB skips row groups by their Parquet
  statistics instead.
