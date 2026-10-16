# Design: queries larger than one machine

Status: implemented in 1.0: scatter-gather with partial aggregation and
broadcast joins, and partitioned joins of two large tables through object
storage. Serves principles 1 (price-performance), 2 (no knobs) and 4
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
  machine's memory, so DuckDB spills them to disk and slows down several
  times over.

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

It stops being enough in two ways, measured below (see
[Evidence](#evidence)):

- **Time grows with data.** Scan-bound queries take time proportional to
  the bytes read divided by one machine's throughput; for a terabyte that
  is minutes, and S3 throughput per machine is bounded by its network
  (tens of Gbit/s), not by how much the bucket can serve.
- **Spilling is a cliff, not a slope.** Join- and aggregation-heavy queries
  slow down several times once their state exceeds memory.

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
   whose aggregates can be computed in parts. The largest table, by its
   snapshot's file size in Iceberg metadata, is the split table; it must
   appear once. Every other table must be a catalog table small enough for
   every worker to read (an eighth of the smallest worker's memory, in
   compressed bytes).
3. **Decide.** The part is worth distributing when the split table is more
   than the coordinator scans in about four seconds (64 MiB/s per thread,
   a conservative rate for compressed Parquet from object storage), or a
   minute plus the expected start time when no worker is running, since
   started workers then run until idle. There is no threshold to tune; a
   session can force the decision with `SET distributed_execution`.
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
   sum of sums over the sum of counts. Aggregates over `DISTINCT` values,
   which must all take the same argument, group the workers' rows by that
   argument too and are computed whole on the coordinator over the distinct
   values. Every combined value is cast to the type DuckDB gives the
   original, found by binding the aggregates in the session. Grouping sets
   group the workers' rows by all of their columns and are applied by the
   coordinator. `now()` and `current_date` become the statement's planning
   time everywhere.
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
back. When the footers of a view's files show every column under its
current name and field ID, with one type across the files, the view reads
by name; otherwise by field ID. The two read the same rows, but DuckDB 1.5
orders joins much better over reads by name (TPC-H Q5 took 20 times as
long by field ID, joining customer to supplier first), so tables without
schema changes, the usual case, get the better plans. Footers are read
once per file and remembered, since data files never change.

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
  (a view, a temporary table, an ambiguous name) is not distributed. For
  users with role `none`, a table must be one the grant check found and
  allowed, or the part runs on one machine.
- **Workers read only what tasks list.** Workers have no catalog and no
  users. A task names data files, which must lie in the warehouse or the
  allowed locations, and the worker's DuckDB has the server's sandbox:
  `enable_external_access=false` with only the warehouse, the allowed
  locations, its spill directory and its cache allowed, no extensions
  loaded at run time.
- **No new endpoint is open.** Workers accept tasks only over TLS 1.3 with a
  client certificate whose key is derived (HKDF-SHA256, Ed25519) from
  `IGLOO_WORKER_SECRET`, and coordinators accept results only from servers
  proving the same key: no certificate authority to run, nothing to rotate
  but the secret. Port 8080 on workers serves only health and metrics, like
  the server's. On Kubernetes a NetworkPolicy admits only igloo pods to the
  task port, and the igloo pods' Kubernetes token may only change the
  workers' replica count.

### Failures, cancellation and limits

- A task that fails for any reason other than its data (connection lost,
  worker stopped, out of memory) runs on another worker, up to three
  tries; the failed worker is left out for 30 seconds. TCP keepalives
  detect a worker whose machine vanished within about 25 seconds.
- If no worker can run a part, or the combine step fails before sending a
  row, the statement runs on the coordinator as usual
  (`igloo_distributed_fallbacks_total`).
- An error in the data (a failed cast, a division by zero) is the
  statement's error, as on one machine.
- Canceling the statement, or its `statement_timeout`, cancels the HTTP
  requests and so the workers' queries.

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

## Partitioned joins

When the tables a part reads besides the split table are too large for
every worker to read, but all of them except the largest would fit, and
that largest one (G) is joined to the split table (F) directly, by `JOIN ...
ON` or `WHERE` conjuncts of the same `SELECT`, on equal columns, both are
partitioned by a hash of the key (`internal/distributed/partition.go`):

1. **Stage one.** Workers read shares of each table's data files, apply the
   statement's conjuncts that read only that table, keep the columns the
   statement names anywhere (all of them after a `*` or a `NATURAL` join),
   and write the rows to object storage under the warehouse
   (`_igloo_shuffle/<statement>/<job>/<side>/<task>/a<attempt>/__b=<p>/`)
   with `COPY ... (PARTITION_BY (__b))`, where
   `__b = hash(key columns) % partitions`, with twice as many partitions as
   workers. Each response lists the files written.
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

## Evidence

Measured on the development machine (18 cores, Docker with 15.6 GB of
memory shared with other projects). Absolute times on that machine vary
two- to fourfold with its load, so every comparison below is interleaved in
one session and reports the fastest of several runs.

(Filled in from `bench/distributed.py` runs; see below.)

## Risks

- **Planner gaps fall back silently.** Statements the planner does not
  recognize run on one machine; that is safe but may surprise. The debug
  log and `igloo_distributed_*` metrics say why.
- **Coordinator bottleneck.** Every partial result goes to the coordinator.
  High-cardinality `GROUP BY` and `count(DISTINCT)` over huge value sets
  move a lot of data to one machine; partitioned aggregation (each worker
  owning a range of groups) is the fix, using the same exchange as
  partitioned joins.
- **DuckDB's printer.** Fragments go through `json_deserialize_sql`; the
  round-trip check catches printing that changes the tree, but a printer bug
  that keeps the tree yet changes meaning would not be caught. The
  differential tests are the guard.
- **Deletes.** Tables with position deletes, equality deletes or deletion
  vectors run on one machine until maintenance merges them (hourly).
  Applying position deletes on workers is straightforward future work.
