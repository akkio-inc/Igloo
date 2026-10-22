# Design: queries larger than one machine

Status: implemented in 1.0: scatter-gather with partial aggregation and
broadcast joins, partitioned joins, partitioned aggregation and
multi-stage plans through object storage. Serves principles 1 (price-performance), 2 (no knobs) and 4
(performance through fewer moving parts, distribution as the escape hatch);
costs some of 4, since distribution is a second moving part.

## The problem

Igloo runs each query on one embedded DuckDB in one replica. That is fast
and cheap for most analytical work, and it is the reason Igloo beats
Snowflake and Databricks on price-performance at the sizes the benchmark
measures. The analytics teams of large media agencies also have queries
that one machine serves badly:

- **Large scans with aggregation.** Log-level data runs to billions of
  impressions a month. A quarter of impressions is on the order of 100
  billion rows; a delivery or reach report over it reads a few columns of
  every row, hundreds of gigabytes to terabytes of compressed Parquet. One
  machine reads object storage at the speed of its network and decompresses
  and aggregates at the speed of its cores: minutes for such a report on the
  largest instances, where Snowflake and Databricks spread it over a
  cluster.
- **Large joins with grouping.** Identity-graph joins (impressions to
  households through device graphs of billions of edges) and clean-room
  joins (an advertiser's conversions against a publisher's exposures) join
  two large tables and then group. Their hash tables do not fit in one
  machine's memory; DuckDB spills them to disk, which costs little until
  memory is a fraction of what the join needs, and then the query fails.

The 1.0 README said: "One engine per query ... very large queries that need
a cluster are not supported yet." This design removes that limit for the
common big cases, without making the common small case slower or more
expensive, and without weakening grants, the sandbox or correctness.

## Options

### (a) Scale up first

The cheapest cluster is one machine. Cloud providers rent machines with
hundreds of cores and terabytes of memory (AWS's largest memory-optimized
instances have 192 vCPUs and 1.5 TB, and the u-series more), and DuckDB is
built to use them: it parallelizes every operator across all cores and
spills joins, aggregations and sorts that exceed memory to local disk.
Scaling up keeps one moving part, one failure domain and no network
exchange.

It stops being enough in two ways (see [Evidence](#evidence)):

- **Time grows with data.** Scan-bound queries take time proportional to
  the bytes read divided by one machine's throughput; for a terabyte that
  is minutes, and S3 throughput per machine is bounded by its network
  (tens of Gbit/s), not by how much the bucket can serve.
- **Memory ends in a wall, not a slope.** DuckDB spills well: in our
  measurements queries ran at most about twice as slow with memory cut to
  well below the size of the data, and then, with less still, join-heavy
  queries failed with out-of-memory errors rather than slowing down
  further. A join whose state is many times one machine's memory needs
  more machines' memory.

So scale-up is the default and the first recommendation
([operations.md](../operations.md#when-one-machine-is-not-enough)), and the
automatic decision below keeps every query on one machine unless that
machine would take a long time.

### (b) Route large queries to Trino or Spark

Igloo could start a Trino or Spark cluster on demand, attached to the same
Lakekeeper catalog, authorize a statement as it does today, and forward it.
Both are mature, Apache 2.0 licensed and read Iceberg. We rejected this for
1.0:

- **Different SQL, different answers.** Igloo's dialect is DuckDB's, with
  PostgreSQL compatibility on top. Trino and Spark SQL differ in integer
  division, `NULL` ordering defaults (Spark sorts `NULL`s first), implicit
  casts, decimal precision rules, string and date functions and time zone
  handling. A translator (sqlglot) gets most queries right, but "most" is
  not "identical to single-node", and a dashboard whose numbers change when
  its data grows past a threshold is a correctness bug.
- **Grants and the sandbox would have to be rebuilt.** Igloo authorizes
  statements by DuckDB's own parse and plan and confines SQL with DuckDB's
  sandbox. Trino and Spark would need their own access control configured
  to match Igloo's grants (Spark has none built in), and their own function
  restrictions (file-reading functions, UDFs), and the two would drift.
- **Operational cost.** A JVM cluster with its own memory settings, spill
  configuration, catalog credentials, upgrades and monitoring is exactly
  the babysitting principle 2 forbids. Starting one on demand takes a
  minute or more before the first task.
- **Per-core efficiency.** DuckDB's vectorized engine typically does more
  per core than Trino's and much more than Spark's for interactive SQL, so
  the cluster would need more machines for the same speed.

It remains a reasonable escape hatch for shapes no DuckDB-based design
covers, and Iceberg makes it possible without moving data.

### (c) Igloo-native distribution over DuckDB workers

Split the work of one statement across workers that each run DuckDB:

1. **Scatter-gather with partial aggregation.** Divide the data files of the
   statement's largest table among workers. Each worker runs the
   statement's `FROM` and `WHERE` over its share, joined to the smaller
   tables, which it reads in full (a broadcast join), and computes partial
   aggregates. The coordinator combines them. This is how every MPP
   warehouse runs a large scan with aggregation.
2. **Partitioned (shuffle) joins.** For two large tables, workers hash both
   on the join key into buckets through object storage or Arrow Flight;
   each worker then joins one bucket of each.

Same engine everywhere means the same semantics: the fragments are the
statement's own SQL, run by the same DuckDB version on the same snapshot,
so results match one machine's by construction for everything the
decomposition handles, and anything it does not handle stays on one
machine.

### (d) Existing DuckDB-based distributed engines

As of October 2026 we found nothing to adopt:

- **MotherDuck** runs DuckDB in a service and splits work between the
  client and the cloud ("dual execution"); it is proprietary and not a
  cluster engine.
- **smallpond** (DeepSeek, MIT) distributes DuckDB over Ray for batch data
  processing, partition by partition, with intermediate data on a shared
  file system (3FS); it is a Python DataFrame framework for offline jobs,
  not an engine for interactive SQL with Igloo's grants and sandbox.
- **DataFusion Ballista**, **datafusion-distributed**, **Daft** and **Sail**
  (Apache 2.0) are distributed engines, but not DuckDB: adopting one has
  the dialect and semantics problems of (b).
- Research and hobby projects (Quokka and various "DuckDB on Ray" or "DuckDB
  in Lambda" experiments) are unmaintained or narrow.

None gives an embeddable, permissively licensed, maintained, DuckDB-based
distributed SQL engine that Igloo could put its authorization in front of.

## Decision

Build (c), and keep (a) as the default:

- **Scatter-gather.** One large table per distributed part, divided by data
  file; every other table read in full by every worker; partial aggregation
  on workers and combination on the coordinator; the rest of the statement
  on the coordinator. It covers the large-scan-with-aggregation case
  completely and the large-fact-to-dimension join case (campaigns,
  placements, creatives, advertisers, date tables), which together are most
  of a media agency's heavy reporting. It needs no exchange between workers,
  so workers are stateless and a failed task simply runs again elsewhere.
- **Partitioned joins through object storage**, for fact-to-fact and
  identity-graph joins: when the second-largest table is too large for
  every worker to read and is joined to the largest on equal columns, both
  are hash-partitioned by the key, described in
  [Partitioned joins](#partitioned-joins) below. They reuse the same
  planner, fragments and worker protocol: a partitioned table is just a
  different view definition on the workers.
- **Everything else stays on one machine**, unchanged. The decision is per
  statement and automatic, and every failure falls back to one machine.

## How it works

The code is in `internal/distributed` (planner, coordinator, worker,
transport) with the integration in `internal/pgwire/distributed.go` and the
`igloo worker` command in `cmd/igloo/worker.go`.

### Where a statement runs

After a `SELECT` is authorized and prepared as always, and only outside
explicit transactions, the coordinator plans it:

1. **Parse** with DuckDB's own parser (`json_serialize_sql`), into the parse
   tree DuckDB itself binds.
2. **Find parts to distribute.** Walking down from the statement through
   common table expressions, `FROM` subqueries and set operations, the
   planner takes the first `SELECT` it can split: one whose `FROM` tree
   reads catalog tables through joins that keep the largest table's rows
   whole (inner and cross joins, `LEFT`/`SEMI`/`ANTI`/`ASOF` with the large
   table on the kept side, and subqueries that only filter and project) and
   whose aggregates can be computed in parts. A view reads as its query,
   with every table name in it qualified as DuckDB resolves names in the
   view's body, if that query describes as the view does in the session:
   a view that passes a table through, filters it or joins small tables to
   it distributes as its query would. Items of `FROM` that read no
   table compute their rows from the row they are lateral to alone, as a
   filter does, so they may run anywhere: `unnest`, `json_each` and
   `json_tree` of values, and subqueries without tables, which is what
   Snowflake's `FLATTEN` becomes, unless they call a volatile function
   (`random()`), which one machine might call once for all rows. The largest table, by its
   snapshot's file size in Iceberg metadata, is the split table; it must
   appear once. Every other table must be a catalog table small enough for
   every worker to read (an eighth of the smallest worker's memory, in
   compressed bytes).
3. **Decide.** The part is worth distributing when the split table is more
   than the coordinator scans in about four seconds (64 MiB/s per thread,
   a conservative rate for compressed Parquet from object storage), or a
   minute plus the expected start time when no worker is running, since
   started workers then run until idle. There is no threshold to tune; a
   session can force the decision with `SET distributed_execution`. Forced
   by an admin, a statement may start workers and uses every one; forced
   by anyone else, it uses only workers already running, no more than the
   tables' sizes call for, so no user can start or occupy the fleet at
   will. A part without aggregates is distributed only when it keeps few
   rows (`DISTINCT`, or `ORDER BY ... LIMIT` up to a million rows); a plain
   filter would only send every matching row through the coordinator's
   disk.
4. **Rewrite** the part into a worker fragment and a combine step:

   ```sql
   -- the part
   SELECT n_name, count(DISTINCT o_custkey) AS customers, sum(l_extendedprice) AS revenue
   FROM lineitem JOIN orders ON l_orderkey = o_orderkey JOIN nation ON ...
   WHERE l_shipdate >= DATE '1995-01-01' GROUP BY n_name HAVING revenue > 1e6 ORDER BY 3 DESC

   -- each worker, over its share of lineitem's files
   SELECT n_name AS __g0, o_custkey AS __dk, sum(l_extendedprice) AS __a0
   FROM __igloo_split AS lineitem JOIN __igloo_t0 AS orders ON ... JOIN __igloo_t1 AS nation ON ...
   WHERE l_shipdate >= DATE '1995-01-01' GROUP BY n_name, o_custkey

   -- the coordinator, in the part's place
   SELECT CAST(__g0 AS VARCHAR) AS n_name,
          CAST(count(DISTINCT __dk) AS BIGINT) AS customers,
          CAST(sum(__a0) AS DECIMAL(38,2)) AS revenue
   FROM <partial results> GROUP BY __g0 HAVING revenue > 1e6 ORDER BY 3 DESC
   ```

   Group expressions are resolved as DuckDB resolves them (positions,
   aliases after columns, `GROUP BY ALL`), and matched in the combine step
   by their parse trees with columns qualified, as DuckDB's binder matches
   them. Each aggregate is computed in two steps: `count` as the sum of
   counts, `sum`/`min`/`max`/`bool_*`/`bit_*` by themselves, `avg` as the
   sum of sums over the sum of counts. Aggregates over `DISTINCT` values
   group the workers' rows by their arguments too, and are computed whole
   over the distinct values, on the coordinator or, partition by partition,
   on workers ([Partitioned aggregation](#partitioned-aggregation)).
   Every combined value is cast to the type DuckDB gives the
   original, found by binding the aggregates in the session. Grouping sets
   group the workers' rows by all of their columns and are applied by the
   coordinator. `now()` and `current_date` become the statement's planning
   time everywhere. Statements translated from Snowflake's dialect call
   Igloo's own macros (`sf_flatten_values` and the like, plain expressions
   that every session defines); workers define the same macros for a
   fragment that calls them, and the planner counts them as built in only
   while the session's definitions are exactly Igloo's.
5. **Check the rewrite**: each generated query is printed with
   `json_deserialize_sql` and parsed again, and must give back the planned
   tree, so a construct DuckDB cannot print faithfully falls back instead
   of running wrong. Fragments that would need the session (parameters,
   user-defined macros, `getvariable`, `current_setting`) fall back too.

### Running it

The coordinator loads each table's current metadata from the catalog
(iceberg-go, with Igloo's own storage credentials), pins its current
snapshot and plans the scan: the data files, with their sizes. It assigns
the split table's files to workers by size with rendezvous hashing on the
file path and worker, so the same files go to the same worker across
statements and its [read cache](../operations.md#read-cache) holds them. A
task carries the fragment, the session settings that change what SQL means,
and the views the fragment reads: the files of each table and its columns
by Iceberg field ID, so files written before a column was renamed, added or
widened read as the current schema does.

A worker defines those views over `read_parquet`, runs the fragment on a
connection of its own, and writes the result as Parquet, which it streams
back. Two details decide whether DuckDB plans the fragment as well as it
plans the statement on one machine, where it reads through its Iceberg
extension:

- **Row counts.** Each view carries its row count from Iceberg's
  manifests, which the worker passes to `read_parquet`
  (`explicit_cardinality`). DuckDB's own estimate from Parquet footers is
  poor for tables of many small files: it took nation (25 rows in 8 files)
  for 8,000 rows, joined supplier, nation and customer first (12 million
  rows), and Q5's fragment ran eight times as long.
- **Reading by name.** When the footers of a view's files show every
  column under its current name and field ID, with one type across the
  files, the view reads by name; otherwise by field ID. The two read the
  same rows; without row counts, DuckDB's estimates were better by name
  (Q5's fragment once took 9 seconds by field ID and 0.4 by name). With
  them, Q5 plans alike either way. Footers are read once per file and
  remembered, since data files never change.

A file without field IDs, which only the table's name mapping can read,
makes the worker refuse the task, and the statement runs on the
coordinator. (iceberg-go gives every table it compacts a name mapping, but
its files have field IDs, so the mapping alone does not keep a table on
one machine.)

Rows that delete files delete are left out by the workers: position
delete files, which Igloo's `UPDATE`, `DELETE` and `MERGE` write, and the
deletion vectors and equality delete files of other engines. The scan's planning
matches delete files to data files (by partition and sequence number), and
each task lists the delete files of its data files. The worker reads the
`file_path` and `pos` columns of those files into a temporary table of the
deleted rows of its own data files, and its view reads files that have
deletes with each row's file name and position (`filename`,
`file_row_number`), leaving out the deleted ones by an anti join; files
without deletes read as before. A delete file may spell a data file's path
otherwise than the table's metadata, as after a migration from another
engine; paths are compared as `deletefiles.Canonical` spells them, and a
delete file deletes rows only of the data files the planning matched it
to. Deletion vectors (format version 3), blobs of Puffin files that the
planning matches to the data file they name, are read by the worker and
decoded (`deletefiles.VectorRuns`, a 64-bit Roaring bitmap, as Delta
Lake's are) into the same table. Equality delete files, which other
engines write, are read by the field IDs of the columns they compare and
leave rows out by anti joins whose conditions match `NULL` to `NULL`, for
the data files the planning matched them to (those written before them);
equality deletes of fields nested in structs keep the statement on one
machine.

Nested columns (lists, maps and structs) are found in each file by their
field IDs too, but DuckDB finds the fields nested in them by name, case
aside, and a list's element and a map's key and value by where they are.
So each task carries the nested field IDs of the table's columns, and the
worker checks every file's footer against them: a list's element and a
map's key and value must have the table's field IDs, and a struct field
found by its ID must have its current name. A field added since a file was
written is not in the file and reads as `NULL`, as Iceberg says; one
renamed since, or dropped and added again under its old name, would read
as `NULL` or as the old field's values, so such a file, like one whose
nested fields have no IDs, makes the worker refuse the task, and the
statement runs on the coordinator.

Column types Parquet would change (HUGEINT, INTERVAL, TIME WITH TIME ZONE)
cross as text and are cast back. The coordinator then runs the rest of the
statement on the session's own connection, with the part replaced by a
read of the partial results and the statement's parameters, and streams
rows to the client through the same encoders as any statement. The
result's columns must match the prepared statement's, or it falls back.

### Correctness

- **Same engine, same snapshot, same settings.** Workers run the same Igloo
  image (a worker of another version is not used) and read the snapshot the
  coordinator pinned. `TimeZone`, `Calendar`, collation, ordering and other
  settings that change results are sent with every task; a session that
  changed any other setting runs on one machine.
- **One snapshot per table, everywhere.** When a statement's parts start,
  the coordinator loads each table's current snapshot and lists that
  snapshot's files for the workers. Where the part it runs itself reads
  one of those tables again (a scalar subquery over the distributed part,
  say), it pins that read to the same snapshot with `AT (VERSION => ...)`,
  so a commit made while the workers run, by Igloo or by another engine
  through the Iceberg REST catalog, adding rows or delete files or changing
  the schema, is seen by neither part. A view in the coordinator's part
  reads as its query, whose reads are pinned in turn; one that cannot
  (with common table expressions, say) could read a table at another
  snapshot, so a statement with one is not distributed; nor is a table
  whose columns changed since planning, or one
  with snapshots but none yet (empty, it could be filled meanwhile). Data
  files are listed from metadata, so files that several tables share, as
  clones do, read like any others.
- **Exact where one machine is exact.** Sums of integers and decimals,
  counts, minimums, maximums and distinct aggregates are exact.
  Floating-point sums and averages may differ in their last digits, as they
  do in DuckDB on one machine between thread counts.
- **Proven by differential tests**: the 22 TPC-H queries and some forty
  other shapes run distributed over three workers and are compared row by
  row with DuckDB on one machine (`internal/distributed`), and again over
  real Iceberg tables in the Docker stack (`scripts/e2e-distributed.sh`).

### Security

- **Authorization comes first.** A statement reaches the planner only after
  it was checked against the user's role and grants and prepared. The
  planner resolves each table name through the same resolver the grant
  check uses; a name that could mean anything but exactly one catalog table
  or view (a temporary table, an ambiguous name) is not distributed. A view
  reads as its query, every name in it resolved as DuckDB resolves names
  in the view's body, and only if the query describes as the view does; the
  grant check expands views the same way. For users with role `none`, a
  table must be one the grant check found and allowed, or the part runs on
  one machine.
- **Policies keep statements on one machine.** Masking and row access
  policies are applied by rewriting statements, for the requesting user,
  on the replica; workers read table files directly and know nothing of
  the user. Rather than push the masked expressions, the row filters and
  their mapping-table reads and `is_role_in_session` to the workers, a
  statement that the rewriter changed, or that reads or names anything
  policies concern, is not distributed, and the planner refuses a protected
  table should a statement reach it anyway. Tested with masked columns
  aggregated by `count(DISTINCT)`, `min` and `max` and a row-filtered table,
  directly and through a view, over three workers: no raw value and no
  hidden row's contribution comes back (`internal/pgwire`).
- **Workers read only what tasks list.** Workers have no catalog and no
  users. A task names data files, which Igloo listed from Iceberg metadata
  and which must lie in the warehouse or the allowed locations, and the
  worker's DuckDB has the server's sandbox: `enable_external_access=false`
  with only the warehouse, the allowed locations, its spill directory and
  its cache allowed, no extensions loaded at run time. A worker parses a
  task's SQL and refuses it unless it reads only the task's views: no table
  function but `unnest`, `json_each` and `json_tree` of values, no table by
  name. The planner never distributes a part that reads another table
  function, a temporary table, a connection's table or an external table,
  or a view that reads one.
- **No new endpoint is open.** Workers accept tasks only over TLS 1.3 with a
  client certificate whose key is derived (HKDF-SHA256, Ed25519) from
  `IGLOO_WORKER_SECRET`, and coordinators accept results only from servers
  proving the same key: no certificate authority to run, nothing to rotate
  but the secret. Port 8080 on workers serves only health and metrics, like
  the server's. Unlike Igloo's other listeners, which serve plain text to
  loopback or private networks when TLS is off, the task port is TLS only.
  On Kubernetes a NetworkPolicy admits only the igloo pods of the workers'
  compute pool to the task port, and the igloo pods' Kubernetes token may
  only change workers' replica counts; Docker Compose does not publish the
  port.

### Failures, cancellation and limits

- A task whose worker cannot be reached, breaks off its answer or fails as
  a server (HTTP 5xx) runs on another worker, up to three tries, and the
  worker is left out for 30 seconds. TCP keepalives detect a worker whose
  machine vanished within about 25 seconds.
- A worker that answers a task with an error (out of memory, a binder
  error) ran it and is healthy, and another would answer the same: the
  statement runs on the coordinator at once, and no worker is left out, so
  one user's failing statement cannot take workers away from everyone.
- Partial results wait on the coordinator's disk until every task has
  answered; past 1 GiB for a statement, it stops its tasks and runs on the
  coordinator.
- If no worker can run a part, or the combine step fails before sending a
  row, the statement runs on the coordinator as usual
  (`igloo_distributed_fallbacks_total`).
- An error in the data (a failed cast, a division by zero) is the
  statement's error, as on one machine.
- Canceling the statement, or its `statement_timeout`, cancels the HTTP
  requests and so the workers' queries.
- A distributed statement runs where any statement does, inside the
  session's admission: it holds its role's statement slot and memory
  reservation on the coordinator until it ends, and its timeout includes
  the workers' time. Workers' memory is not reserved; statements on a
  pool's workers share it, as on one replica.

### Worker lifecycle

Workers are the Igloo image running `igloo worker`. The coordinator finds
them by resolving `IGLOO_WORKERS` (a Kubernetes headless Service resolves to
every ready worker pod) and asking each for its status. With a compute
driver, it starts them when a statement needs more than are running (on
Kubernetes by raising the worker Deployment's replica count with an
optimistic-concurrency patch, never lowering it, so replicas cannot undo
each other) and stops them once this coordinator has not used them for
`IGLOO_WORKER_IDLE_AFTER` and every worker reports as long without tasks.
The drivers are the scale-to-zero gateway's (`internal/gateway`), and the
gateway stops workers along with Igloo.

Workers belong to one compute pool. Pools exist so that a client's queries
do not compete with another's and its compute is billed to it; shared
workers would undo both. So the chart gives each pool a worker Deployment
of its own, which only its igloo pods reach (NetworkPolicy) and start, and
which its gateway stops, and a worker secret of its own, so that a pool's
igloo pods cannot use another pool's workers even where the network lets
them: the NetworkPolicy is defence in depth, effective only where the CNI
enforces it. Docker Compose isolates no containers from each other; its
example pool runs without workers, and gives its own a secret of their own
if added.

## Stages and exchanges

A plan is a list of stages (`internal/distributed/stage.go`). A stage is
a set of tasks that run one fragment each, over a share of the data files
of its split table, over one partition of the exchanges it reads, or, with
neither, once; and every task reads in full whatever else the stage reads.
Its rows go to the coordinator, as partial results that the statement's
final query reads, or to an exchange: Parquet files under
`_igloo_shuffle/<statement>/s<stage>/<task>/a<attempt>/__b=<p>/` in the
warehouse, written with `COPY ... (PARTITION_BY (__b))`, where
`__b = hash(key columns) % partitions`. Every exchange of a statement has
the same number of partitions, twice the workers' (at most 64), and hashes
integer keys as `BIGINT`, so a stage can read several exchanges keyed on
equal values partition by partition. Columns that Parquet would change
(`HUGEINT`, `INTERVAL`, `TIME WITH TIME ZONE`) cross exchanges as text and
are cast back, as results do. The coordinator runs each stage once every
exchange it reads is complete, and deletes the statement's directory when
it ends.

## Partitioned joins

When the tables a part reads besides the split table are too large for
every worker to read, but all of them except the largest would fit, and
that largest one (G) is joined to the split table (F) directly, by `JOIN ...
ON` or `WHERE` conjuncts of the same `SELECT`, on equal columns, both are
partitioned by a hash of the key (`internal/distributed/partition.go`):

1. **Stage one.** Workers read shares of each table's data files, apply the
   statement's conjuncts that read only that table, keep the columns the
   statement names anywhere (all of them after a `*` or a `NATURAL` join),
   and write the rows to an exchange keyed by the table's side of the join
   key. Each response lists the files written.
2. **Stage two.** One task per partition runs the job's unchanged fragment,
   with F's and G's views defined over that partition's files (read by
   column name) and the other tables read in full as before. Partial
   results combine as for any job.

Every pair of rows the join can produce has equal keys and so meets in one
partition; the other conditions are applied in stage two as before. Keys
must have the same type on both sides, except integer types, which DuckDB
hashes by value; text keys are not partitioned when the session has a
`default_collation`, since values equal under a collation hash apart.

Object storage rather than worker-to-worker transfer keeps workers
stateless: a stage-two task that fails reads the same files on another
worker, and each attempt of a stage-one task writes a directory of its own
whose files are read only if it succeeded. The coordinator deletes the
statement's directory when it ends. The cost is a write and a read of the
partitioned columns, at object-storage request prices, and the latency of
both; worker-to-worker Arrow Flight is the later optimization. The read
cache never keeps these files.

## Partitioned aggregation

Workers aggregate their share of the rows into partial results grouped by
the group expressions and, for aggregates over `DISTINCT` values, by their
arguments too. Sent to the coordinator, those are a row per group per
worker, and a row per group and distinct value: for `count(DISTINCT
device_id)` every device each worker saw, which made the media benchmark's
reach query (m02) slower distributed than on one machine. So when the
partial results may be large, they go to an exchange instead, and a second
stage completes the groups (`internal/distributed/aggregate.go`):

- **By value.** Partial results of aggregates over `DISTINCT` values that
  add up over disjoint sets of values (`count`, `sum`, `avg` as their
  ratio, and the idempotent `min`, `max`, `bool_*`, `bit_*`, `any_value`)
  are keyed by the groups and a distinct argument, so each value of it
  lies in one partition. A task per partition aggregates its rows by group,
  the other partial aggregates again (sums of sums) and the aggregates over
  `DISTINCT` values over its values; the coordinator combines the
  partitions' rows like any partial results, adding up the counts. Several
  `DISTINCT` arguments are allowed when one is a column that every other
  one's value fixes (the column itself, a row or struct holding it, or a
  `CASE` whose every branch does with no `ELSE`), as in Snowflake's
  `COUNT(DISTINCT user_id, day)` next to `COUNT(DISTINCT user_id)`;
  partitioning by that column keeps every argument's values apart.
- **By group.** Other aggregates over `DISTINCT` values (`median`,
  `string_agg`, arguments none of which fixes the others), and the top
  groups of an aggregation by a column of a divided table
  (`ORDER BY ... LIMIT`), are keyed by the groups alone. Each partition
  holds whole groups, which its task completes as the coordinator would,
  every aggregate included, keeps those `HAVING` keeps and, under
  `ORDER BY ... LIMIT n`, its first `n`; the coordinator reads one row per
  group and applies the rest of the statement (windows, `QUALIFY`, the
  final order and limit) as before.
- **Complete.** A stage whose partitions hold whole groups (the group
  expressions fix the key of an input it reads by partition) computes
  every aggregate of its groups itself, any aggregate at all (`median`,
  `quantile_cont`, `string_agg ... ORDER BY`), and the coordinator reads
  one row per group. A part that reads one relation and has such
  aggregates has the relation keyed by its group columns for that.
- **In place.** A partitioned join already holds every row of a join key's
  value in one partition. Aggregates over `DISTINCT` values of the key
  (or of what fixes it, as above) that add up are computed by the join's
  own stage and added up by the coordinator: no second exchange.

Grouping sets keep the partial results on the coordinator, which applies
them over the finest groups; distinct counts of different sets do not add
up. Partial results go to the coordinator as before for aggregations
without `DISTINCT` and without a top-N over a divided table's columns:
their groups are usually few, and the exchange would add a write, a read
and a second round of tasks. Exactness is unchanged: each partial
aggregate is combined once, sums of integers and decimals stay exact, and
counts are cast back to `BIGINT`.

## Multi-stage plans

Reports are built in steps: common table expressions that define cohorts,
explode signals and count people per signal and cohort, read by later
ones, some more than once. The steps over people and their events are
large; the last steps, over a few thousand signals, are not. A part whose
`FROM` reads a common table expression or a subquery that is not a mere
filter of tables (it groups, is `DISTINCT`, or is a `UNION ALL`) reads it
as a relation that workers compute (`internal/distributed/relation.go`):

- **Inputs by partition.** Relations estimated small enough for every
  worker to read, beside a larger table the part divides, every task
  reads in full, as it reads small tables. Otherwise a part that reads
  relations runs as a stage over partitions. Its large inputs (every
  relation but those of one row, and its tables until the rest fit every
  worker) must all be joined to
  each other, by inner joins or `WHERE` of the same `SELECT`, on equal
  columns; the classes of columns its equalities make equal give the key.
  Each table is scanned by a stage of its own into an exchange keyed by
  its column of the key, as for a partitioned join; each relation is
  written by its own stages to an exchange keyed by its column of the
  key; the part's stage reads partition `p` of all of them. A part that
  reads one relation and nothing large is keyed by the argument of its
  aggregates over `DISTINCT` values, else its group columns, so that
  counts add up or groups are whole in each partition. A subquery that
  only filters a large table but must be partitioned is computed as a
  relation too (its rows keyed by the join key), as a report's browsing
  events wrapped in `SELECT * REPLACE (...)` are.
- **Writing a relation.** A `SELECT` is planned like any part (its own
  inputs split, broadcast or by partition) and its rows go to the
  exchange instead of the coordinator. Without grouping, its stage runs it
  as written. With grouping, when its partitions hold whole groups (every
  group expression fixes the key of an input read by partition), its stage
  runs it as written too; otherwise its partial results go to an exchange
  keyed by the groups and a second stage runs the coordinator's combine
  step on each partition, `HAVING` included. `SELECT DISTINCT` is grouping
  by its items. A `UNION ALL`'s branches each write to the one exchange,
  their columns cast to the union's types. Aggregates over all of a
  relation's rows (`min(day) OVER () AS period_start`, as the reports
  compute for every source) make it the join of its rows, a relation of
  their own, to the one row of those aggregates, a relation over that one.
  `ORDER BY`, `LIMIT`, other window functions and grouping sets need every
  row, and keep the part that reads such a relation off the workers.
- **Computed once.** A common table expression's exchange is remembered by
  its key: every part that reads it by that key reads the same files, and
  one keyed otherwise reads it partitioned again rather than computed
  again. The planner visits a `WITH` clause's expressions readers first,
  so readers decide how a relation is keyed; one the coordinator's own
  part reads too is sent to it from the exchange by a stage that reads it,
  and common table expressions nothing reads any more are dropped from the
  coordinator's statement.
- **Read once.** A common table expression that one reference reads is
  not written for that reader alone. One that only filters and projects
  tables reads as a subquery in the reference's place, so its reader
  divides, partitions or broadcasts the tables itself and aggregates
  their rows where it reads them (a `GROUP BY` over it runs in one stage,
  and a `DISTINCT` over it writes each worker's distinct rows); a part
  that needs the relation after all (`median` over a relation keyed by its
  groups) is planned again reading it as one. A `SELECT` that only filters
  and projects one that groups or is a `UNION ALL` is computed by that
  relation's last stage, rather than by a stage that copies one exchange
  into another. References count only where the name means that
  expression, so a statement that holds a report twice, each copy with
  common table expressions of the same names, plans each copy as the
  report alone.
- **Filters' subqueries.** A subquery expression in a filter that reads a
  table the part divides, or one too large for every worker, is a
  relation too when it binds on its own (it is not correlated) and is
  small (an aggregate without `GROUP BY`): workers compute its partial
  results first, a row per task, and every task of the part combines them
  into the subquery's row itself and reads it in the subquery's place, as
  an audience's `ver = (SELECT max(ver) FROM members WHERE ...)` needs.
  So does every task that reads such a row in full, as a relation in
  `FROM`: no round of tasks writes the one row first. A conjunct
  `x IN (SELECT y ...)` of a part's `WHERE` whose subquery stands on its
  own and is larger than every worker may read becomes
  `SEMI JOIN (SELECT y ...) ON x = y`, which keeps the same rows (`NULL`s
  match nothing either way): the subquery is a relation keyed by `y`, and
  the part partitioned by `x`. A semi join's right side may be keyed
  though its rows cannot be divided arbitrarily, since each row on the
  left meets all of its matches in one partition; an anti or outer join's
  may not, where a row of a side every task reads would come back from
  every partition.
- **Where the coordinator takes over.** A part is distributed only if it
  reads something large; a relation's size is estimated as the compressed
  size of the tables it reads, or nothing for an aggregation without
  `GROUP BY`. The signal-level tail of a report joins aggregates on
  expressions (`upper(a.cohort) = upper(b.cohort)`), ranks them and picks
  the top ones; those parts run on the coordinator over the aggregates'
  combined rows.
- **Taking back.** A part that turns out not to qualify halfway (a
  relation of it with a window function, say) takes back the stages and
  exchanges it planned, and its readers run as before.

The behaviors and signals reports thus run in about a dozen stages: the
audience's and everyone's distinct people, written once to an exchange
keyed by person that both branches of the cohorts' `UNION ALL` fill; the
media, browsing (exploded by `unnest`) and attribute rows, each scanned
into an exchange keyed by person; and a stage per signal source and one
for the cohorts' sizes, which join one partition of each and count
distinct people per signal and cohort in place. Every step reads and
writes object storage, a few seconds each: about three levels of stages
before the coordinator's tail.

## Evidence

Measured on the development machine (18 cores, Docker with 15.6 GB of
memory shared with other projects, which kept it busy throughout).
Absolute times on that machine vary two- to fourfold with its load, so
every comparison below is interleaved in one session and reports the
fastest of several runs. Nothing larger than TPC-H scale factor 1 was run
there; the measurements that decide price-performance at the sizes this
design is for are planned for EC2
([bench/README.md](../../bench/README.md#distributed-execution)) and not
yet made.

### One machine as memory shrinks

TPC-H at scale factor 1 (lineitem is 207 MB of Parquet), DuckDB 1.5.6 with
4 threads and the memory limit lowered step by step; seconds, fastest of
three:

| Query | 4 GB | 128 MB | 64 MB | 40 MB |
|---|---:|---:|---:|---:|
| Q1 (scan, aggregate) | 0.24 | 0.16 | 0.15 | 0.20 |
| Q3 (3 joins) | 0.21 | 0.10 | 0.17 | out of memory |
| Q6 (scan, filter) | 0.04 | 0.05 | 0.04 | 0.04 |
| Q9 (5 joins) | 0.19 | 0.41 | out of memory | out of memory |
| Q13 (outer join, 2 aggregations) | 0.17 | 0.14 | out of memory | out of memory |
| Q18 (large aggregation, joins) | 0.17 | 0.21 | 0.39 | out of memory |
| Q21 (4 joins, `EXISTS`) | 0.26 | 0.26 | out of memory | out of memory |

With memory at 60% of the compressed data, no query slowed by more than
2.2 times; at 30%, three of the five join-heavy queries failed, and at 20%
all five did, while the scans ran as fast as ever. (Limits of 8 GB down to
250 MB changed nothing beyond noise.) DuckDB's spilling is good enough that
memory pressure alone is rarely a reason to distribute; running out of
memory is. These limits are small in absolute terms, where DuckDB's fixed
per-thread buffers weigh more than they would on a large machine, so the
fractions are indicative only.

### Correctness

- **In process** (`go test ./internal/distributed`): the 22 TPC-H queries
  at scale factor 0.05, seven files per table, over three workers; 13 run
  distributed and all 22 return what DuckDB returns on one machine. TPC-H
  again with orders and partsupp too large for every worker to read, so
  that Q3, Q5, Q7, Q8, Q10 and Q12 partition their joins; some forty other
  shapes (aggregates, grouping sets, `DISTINCT`, windows over aggregates,
  top-N, semi and anti joins, time functions, `UNION ALL`); session time
  zones and `integer_division`; files written under older schemas
  (renamed, added, dropped and re-added, widened columns, and struct
  fields added, renamed and re-added); lists, maps and structs, grouped,
  filtered and returned; position deletes of one data file and of a
  partition's, paths spelled otherwise, and delete files that apply to
  other data files only, deletion vectors, and equality deletes by one and
  two columns, one written under a column's old name; a worker that
  dies mid-task or is gone, every worker failing (the statement runs on
  the coordinator), cancellation and errors in the data; and, through the
  PostgreSQL protocol (`internal/pgwire`), grants, parameters and
  fallbacks.
- **Partitioned aggregation and multi-stage plans, in process**: the 22
  TPC-H queries with every aggregation completing its groups on workers;
  aggregates over `DISTINCT` values by value, by group and in place, with
  several arguments, `HAVING`, windows over them, top groups, and sums
  past 2^53; a report shaped like the behaviors and signals reports
  (cohorts in a `UNION ALL` of `DISTINCT`s, media, exploded browsing and
  attribute signals counted per cohort, compared and ranked), checked to
  compute the cohorts once and count distinct people in place; self-joins,
  joins of aggregated common table expressions, `UNION ALL` of tables
  feeding an aggregation, TPC-H Q18 written with a common table
  expression, and a common table expression read by workers and by the
  coordinator.
- **Governance, in process**: statements that policies apply to stay on
  one machine (and returned workers' raw values when the check was taken
  out); users of the `none` role read through grants of their own and of
  custom roles, and nothing else; connections' tables and external tables
  never reach workers; role limits hold a distributed statement's slot and
  memory, and its timeout ends its tasks; the coordinator's own reads are
  pinned to the workers' snapshot; workers refuse SQL over anything but
  their views.
- **In Docker** (`scripts/e2e-distributed.sh`): Igloo, Lakekeeper, MinIO and
  three workers; TPC-H at scale factor 0.1, six files per table. All 22
  queries return the same rows with and without workers, 13 of them run by
  all three workers (from the query history); a role-`none` user's granted
  table runs distributed and an ungranted one is refused; a worker killed
  mid-statement has its share rerun elsewhere and the statement returns
  the right rows; `statement_timeout` cancels a distributed statement
  (SQLSTATE 57014); tables that maintenance compacted (and so gave a name
  mapping) still run on workers; a statement keeps its snapshot while
  another session deletes rows and adds a column; a table with masking and
  row access policies runs on one replica; and the worker port
  refuses clients without the secret.

### Overhead and scaling on one machine

TPC-H at scale factor 1 and the media workload at 20 million impressions
(`bench/queries/media`), every Igloo process limited to 2 threads, tables
in MinIO on the same machine, read cache and table maintenance off;
`SET distributed_execution` forced each statement on or off (the automatic
decision distributes none of these: the coordinator scans them in well
under four seconds). One pass per worker count, each statement on one
machine and distributed, interleaved, fastest of three; milliseconds, one
machine / distributed. All 204 forced statements ran distributed (from the
query history). The machine was busier during the 2-worker pass, so
compare within a column:

| Query | 1 worker | 2 workers | 4 workers |
|---|---:|---:|---:|
| Q1 | 266 / 534 | 263 / 389 | 189 / 192 |
| Q3 | 197 / 748 | 182 / 480 | 160 / 428 |
| Q5 | 114 / 467 | 197 / 685 | 197 / 679 |
| Q6 | 84 / 173 | 118 / 265 | 90 / 236 |
| Q7 | 196 / 561 | 343 / 979 | 254 / 664 |
| Q8 | 194 / 491 | 345 / 784 | 334 / 973 |
| Q9 | 300 / 568 | 422 / 1,091 | 438 / 842 |
| Q10 | 212 / 647 | 239 / 691 | 251 / 601 |
| Q12 | 115 / 401 | 160 / 368 | 137 / 371 |
| Q14 | 138 / 424 | 178 / 421 | 131 / 239 |
| Q19 | 162 / 385 | 161 / 372 | 162 / 260 |
| m01 | 296 / 396 | 704 / 579 | 656 / 458 |
| m02 | 1,810 / 3,303 | 4,257 / 8,708 | 2,116 / 3,000 |
| m03 | 166 / 334 | 329 / 518 | 271 / 399 |
| m04 | 1,034 / 1,245 | 2,304 / 1,811 | 1,853 / 1,328 |
| m05 | 508 / 720 | 1,230 / 1,348 | 1,060 / 1,315 |
| m06 | 107 / 217 | 222 / 378 | 209 / 335 |
| Total | 5,900 / 11,612 | 11,654 / 19,868 | 8,507 / 12,321 |

These are not the speedups to expect; on one machine, workers add no
cores. They measure what distributing costs when there is little work to
divide:

- **Fixed cost.** Distributing added a median of about 0.3 seconds to a
  TPC-H statement (0 to 0.7), whatever the number of workers: loading
  each table's current snapshot from the catalog, a task round trip per
  worker (settings, views, the fragment, writing and sending Parquet), and
  the combine step. The automatic decision distributes only statements
  that the coordinator would take at least four seconds for, where the
  median is under a tenth of the time.
- **Work that divides.** The media queries that scan the 20 million
  impressions into few groups, m01 (weekly delivery) and m04 (household
  reach through the device graph), ran 1.2 to 1.4 times faster on 2 and 4
  workers, even sharing one machine's cores.
- **Work that does not.** m02 (distinct devices per advertiser) and m05
  (spend per device, top ten) ran slower: every worker sends its distinct
  devices or its per-device sums to the coordinator, which then does much
  of the work again. This is the coordinator bottleneck under
  [Risks](#risks).

Earlier runs of this benchmark found two problems, both fixed in the
numbers above: Q5 took 2 to 4 seconds distributed against 0.2 on one
machine, because workers' DuckDB misjudged the small tables' sizes (see
[Running it](#running-it)); and after table maintenance compacted a table
partway through, statements over it stopped running distributed, because
compaction gives a table a name mapping and tables with one were kept on
one machine. Maintenance was off for these runs, so that every statement
saw the same files.

### Cost

For a statement that takes `t1` seconds on the coordinator alone and `tn`
on the coordinator with `n` workers of the same machine type, at `p` per
machine-hour, the distributed run costs `(n + 1) * p * tn` against
`p * t1`, or `n * p * tn` when the coordinator runs anyway for other
sessions. It is cheaper only if the speedup `t1 / tn` exceeds `n + 1` (or
`n`), which happens when one machine would spill or reread from object
storage what the workers together hold in memory. Otherwise distribution
buys latency at a higher cost per query, and the automatic decision spends
it only on statements one machine would take seconds to minutes for.
Workers started for a statement run until idle (`IGLOO_WORKER_IDLE_AFTER`,
10 minutes), so a single large statement a day also pays for that idle
time; a busy warehouse amortizes it over many. `bench/distributed.py`
reports the cost per query on the EC2 runs.

## Risks

- **Planner gaps fall back silently.** Statements the planner does not
  recognize run on one machine; that is safe but may surprise. The debug
  log and `igloo_distributed_*` metrics say why.
- **Steps cost a round trip each.** A multi-stage plan writes and reads
  every step's rows through object storage, and a step that only projects
  a relation (a `UNION ALL` branch that adds a label) is a stage of its
  own. Running such steps in the stage that reads them, and worker-to-worker
  transfer, are the next optimizations. Relations' sizes are estimated
  from the tables they read, so a small aggregate joined to a large table
  is partitioned with it rather than sent to every worker.
- **Coordinator bottleneck.** Aggregates over `DISTINCT` values and top
  groups complete on workers ([Partitioned
  aggregation](#partitioned-aggregation)), but a high-cardinality `GROUP BY`
  whose every group the statement returns still sends every worker's
  groups to the coordinator. Whether to exchange is decided without
  knowing how many groups there are; Iceberg's statistics do not count
  distinct values.
- **DuckDB's printer.** Fragments go through `json_deserialize_sql`; the
  round-trip check catches printing that changes the tree, but a printer bug
  that keeps the tree yet changes meaning would not be caught. The
  differential tests are the guard.
- **Exchanged rows at rest.** A partitioned join writes the columns it
  joins on and reads, of rows that passed its filters, as plain Parquet
  under `_igloo_shuffle/` in the warehouse, for the length of the
  statement (and until a lifecycle rule expires them, if the coordinator
  crashed). They are as protected as the tables' own files, by bucket
  access and server-side encryption, and the Iceberg REST catalog vends no
  credentials that reach them; encrypting them with a per-statement key
  would protect them from holders of the warehouse's storage keys, who can
  read the tables anyway.
- **Deletes.** Workers apply delete files (see [Running it](#running-it)):
  each task reads every delete file that applies to its data files, and
  a worker reads a Puffin file whole for the deletion vectors in it. A
  table with many deletes costs every worker that, as it costs one
  machine; maintenance merges them hourly.
- **Files without identity-partition columns.** The Iceberg spec lets a
  data file omit a column that an identity partition holds, to be read from
  the file's partition value. Workers would read such a column as `NULL`.
  No writer Igloo is used with omits them (Igloo does not write partitioned
  tables; Spark, Flink, Trino and PyIceberg write the column); tables
  migrated from Hive layouts do, and their files have no field IDs, which
  workers refuse. Checking the footers, which workers already read, against
  the identity-partitioned columns would close this.
