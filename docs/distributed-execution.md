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
exchanged rows are deleted when the statement ends.

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
wrote. Joins of large intermediate results, `UNION ALL` of large branches
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
  `GROUP BY` may use positions, aliases, `ALL`, `ROLLUP`, `CUBE` and
  `GROUPING SETS`.
- The large table appears once in it, and the other tables it reads
  together fit comfortably in a worker's memory (an eighth of it, in
  compressed size); or the tables too large for that, and the relations it
  reads, are all joined to each other, by inner `JOIN ... ON` or `WHERE`
  conditions of the same `SELECT`, on equal columns of the same type
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
  snapshots, and does not use time travel.

Everything else runs on one machine: median and other aggregates that do
not combine (`median`, `quantile`, `approx_count_distinct`, `string_agg`,
`list`, `stddev`), except over a relation workers compute, which is then
partitioned by the groups so that each worker computes whole groups;
aggregates over `DISTINCT` values of unrelated
expressions in one `SELECT` (`count(DISTINCT a), count(DISTINCT b)`),
window functions over rows, a large table read again in a subquery
expression (`IN`, `EXISTS`, a scalar subquery: TPC-H Q17, Q18, Q20, Q21)
unless the subquery stands on its own and returns one row
(`ver = (SELECT max(ver) FROM t WHERE ...)`), which workers then compute
for every part to read,
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

With workers running, Igloo distributes a part when its large table is more
data than the replica scans in about four seconds: 4 seconds at 64 MiB/s
per thread, so 4 GiB for a 16-thread replica. Workers that are stopped are
started only for a part that would take over a minute, since a start takes a
while and the workers then run until they have been idle. Each worker is
given at least a gigabyte of the large table, so smaller statements use
fewer workers.

A session can change that for testing and benchmarks:

```sql
SET distributed_execution = 'on';    -- whenever the statement can run distributed
SET distributed_execution = 'off';   -- never
SET distributed_execution = 'auto';  -- the default; RESET does the same
SHOW distributed_execution;
```

`on` lets admins start workers and use all of them for any statement. For
other users it distributes whatever can be distributed, but only on
workers already running and on no more of them than the automatic decision
would use for the tables' sizes; it never starts a worker. So no user can
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
workers or leaving any out. If no worker can run it, its partial results
would take more than 1 GiB of the coordinator's disk, or the parts cannot
be combined, the statement also runs on the coordinator, so a client sees a
slower statement, not an error. An error in the data itself, such as a failed cast
or a division by zero, is the statement's error, as it would be on one
machine. Canceling a statement (`Ctrl-C`, `statement_timeout`) cancels its
parts on the workers.

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
  other than `unnest`, `json_each` and `json_tree` of values, a table by
  name). Their DuckDB is locked down as the server's is: no local
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
  warehouse, deleted when the statement ends.
  They are protected as the tables' own files are: by access to the bucket
  and its server-side encryption. No credentials the Iceberg REST catalog
  hands out reach `_igloo_shuffle/`. Add a lifecycle rule that expires
  `_igloo_shuffle/` after a day, for what a coordinator that crashed
  mid-statement leaves behind.
- **Pools are kept apart by secret and network.** Each compute pool's
  workers have a secret of their own (the Helm chart generates one per
  pool), so one pool's igloo pods cannot use another's workers; the
  NetworkPolicy keeps them apart too, and is only as good as the cluster's
  CNI enforcing it: with several pools, use one that does. Docker Compose
  has no network isolation between its containers; give a pool's workers
  their own `IGLOO_WORKER_SECRET`.

## Seeing it

- **Query history.** `igloo.query_history` has a `workers` column: how many
  workers ran parts of the statement, 0 when it ran on one replica.

  ```sql
  SELECT start_time, duration_ms, workers, query_text
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
  | `igloo_distributed_queries_total` | `outcome` = `ok`, `error`, `canceled`, `fallback` | Statements whose parts ran on workers (`error`: an error in the data), and statements that started to and ran on the coordinator instead |
  | `igloo_distributed_fallbacks_total` | `reason` = `no_workers`, `worker_error`, `combine_error` | Why a statement ran on the coordinator after all |
  | `igloo_distributed_query_seconds` | | Time to run a statement's parts and combine them, before results are sent |
  | `igloo_distributed_tasks_total` | `result` = `ok`, `retried`, `failed` | Parts sent to workers |
  | `igloo_distributed_workers` | | Workers that answered the last check |
  | `igloo_distributed_scanned_bytes_total` | | Data file bytes divided among workers |
  | `igloo_distributed_received_bytes_total` | | Partial results received |
  | `igloo_distributed_worker_starts_total` | `action` = `start`, `stop`; `result` | Workers started and stopped |

  and on the workers' port 8080: `igloo_worker_tasks_total{outcome}`,
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

Every deployment with workers needs a lifecycle rule on the warehouse's
bucket that expires `_igloo_shuffle/` under the warehouse after a day:
statements delete what their partitioned joins exchanged there, but not a
coordinator that crashed mid-statement. On S3, for a warehouse at
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
| `IGLOO_WORKER_K8S_DEPLOYMENT`, `IGLOO_WORKER_K8S_NAMESPACE` | | `kubernetes`: the workers' Deployment, and its namespace if not the server's |
| `IGLOO_WORKER_EC2_INSTANCE_IDS` | | `ec2`: the workers' instances |
| `IGLOO_WORKER_START_COMMAND`, `IGLOO_WORKER_STOP_COMMAND` | | `command`: shell commands that start and stop all workers |

Distributed execution also needs `IGLOO_DUCKDB_TEMP_DIR` (set in the image),
where partial results are kept while a statement combines them.

On a worker (`igloo worker`): `IGLOO_WORKER_SECRET`,
`IGLOO_WORKER_LISTEN_ADDR` (`:7433`), the server's storage settings, and
`IGLOO_DUCKDB_*`, `IGLOO_CACHE_*`, `IGLOO_HTTP_ADDR` and `IGLOO_LOG_*` as on
the server.

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
  subquery per key. A large table read again in a subquery expression
  (`IN`, `EXISTS`, a scalar subquery) that is correlated or returns more
  than a row runs on one machine; written as a join to a common table
  expression, it runs distributed. Give the
  machines that run such statements a lot of memory and fast spill disk.
- Partitioned joins, aggregations and every step of a multi-stage plan
  write what they exchange to object storage and read it back: that costs
  request charges and adds a few seconds per step, and
  leftovers of a coordinator that crashed mid-statement stay under
  `_igloo_shuffle/` until a lifecycle rule expires them, which every
  deployment with workers needs.
- The coordinator receives every group of an aggregation, unless only its
  top rows are kept: a `GROUP BY` with billions of groups sends every
  worker's groups to it. (`count(DISTINCT x)` over billions of values sends
  it one row per group and partition.)
- Tables with data files without field IDs, views that group, sort or
  limit, and time travel run on one machine.
- Workers read each table's files directly and do not use Iceberg partition
  statistics to skip files; DuckDB skips row groups by their Parquet
  statistics instead.
