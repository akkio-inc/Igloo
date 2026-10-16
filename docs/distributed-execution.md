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
subqueries. Selective queries without aggregation (`WHERE` filters, `ORDER
BY ... LIMIT`) are divided the same way.

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

In detail, a part runs distributed when:

- It is a `SELECT` (in the statement itself, a subquery in `FROM`, a common
  table expression or a branch of `UNION`) whose `FROM` clause reads catalog
  tables, through inner, cross, `LEFT` (keeping the large table's rows),
  `SEMI`, `ANTI` and `ASOF` joins and subqueries that only filter and
  compute columns.
- Its aggregates are `count`, `sum`, `min`, `max`, `avg`, `count_if`,
  `bool_and`, `bool_or`, `bit_and`, `bit_or`, `bit_xor`, `any_value`,
  `fsum` and `product`, with or without `FILTER`, and any aggregate over
  `DISTINCT` values of one expression (`count(DISTINCT user_id)`).
  `GROUP BY` may use positions, aliases, `ALL`, `ROLLUP`, `CUBE` and
  `GROUPING SETS`.
- The large table appears once in it, and the other tables it reads
  together fit comfortably in a worker's memory (an eighth of it, in
  compressed size); or all but the largest of them fit, and that one is
  joined to the large table directly (by `JOIN ... ON` or `WHERE`) on
  equal columns of the same type (integer types may mix).
- It runs outside an explicit transaction, so it reads the tables' latest
  snapshots, and does not use time travel.

Everything else runs on one machine: median and other aggregates that do
not combine (`median`, `quantile`, `approx_count_distinct`, `string_agg`,
`list`, `stddev`), window functions over rows, a large table joined to
itself or read again in a subquery (TPC-H Q17, Q18, Q20, Q21), three large
tables joined on different keys, statements that call user-defined macros
or session functions (`getvariable`, `current_setting`), tables with
pending delete files (maintenance merges them within the hour) or with
nested columns, and views (name the tables they read to distribute them).

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

If a worker fails or stops in the middle of a statement, its share runs on
another worker. If no worker can run it, or the parts cannot be combined,
the statement runs on the coordinator instead, so a client sees a slower
statement, not an error. An error in the data itself, such as a failed cast
or a division by zero, is the statement's error, as it would be on one
machine. Canceling a statement (`Ctrl-C`, `statement_timeout`) cancels its
parts on the workers.

## Security

- **Grants and the sandbox hold.** A statement is checked against the
  user's role and grants before Igloo plans it, as always. Its parts read
  only the tables the statement reads, resolved the way DuckDB resolves
  them in the session; for users with role `none`, only tables the grant
  check found and allowed. Workers have no catalog and no credentials of
  their own beyond object storage: each task lists the data files to read,
  and a worker refuses files outside the warehouse and
  `IGLOO_ALLOWED_LOCATIONS`. Their DuckDB is locked down as the server's is:
  no local files, no other locations, no SQL from strings.
- **Workers accept no one but Igloo.** Coordinators and workers talk TLS 1.3
  and authenticate each other with keys derived from `IGLOO_WORKER_SECRET`;
  without the secret, neither SQL users nor anything else on the network
  can connect to a worker's task port (7433) or pretend to be a worker.
  Treat the secret like the object storage keys: whoever holds it can make
  workers read any table. The Helm chart generates it and lets only igloo
  pods reach the task port; workers' port 8080 serves only health checks
  and metrics, like the server's.
- **What crosses the network.** Tasks carry the statement's SQL fragments
  and file lists; results carry aggregated or filtered rows, encrypted.
  Partial results live in the spill directories of the workers and the
  coordinator while a statement runs, and are deleted when it ends.

## Seeing it

- **Query history.** `igloo.query_history` has a `workers` column: how many
  workers ran parts of the statement, 0 when it ran on one replica.

  ```sql
  SELECT start_time, duration_ms, workers, query_text
  FROM igloo.query_history WHERE workers > 0 ORDER BY start_time DESC LIMIT 20;
  ```

- **Metrics** on port 8080 of the igloo pods:

  | Metric | Labels | Meaning |
  |---|---|---|
  | `igloo_distributed_queries_total` | `outcome` = `ok`, `fallback` | Statements whose parts ran on workers, and statements that started to and ran on the coordinator instead |
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
only igloo pods to the task port, and the secret. Igloo scales the
Deployment up when a statement needs workers (never down while another
replica uses them) and to zero after `workers.idleAfter` (10 minutes)
without tasks; the cluster autoscaler or Karpenter then removes their
nodes. With [scale to zero](scale-to-zero.md), the gateway stops the
workers along with the igloo pods. See the
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
| `IGLOO_WORKER_K8S_DEPLOYMENT`, `IGLOO_WORKER_K8S_NAMESPACE` | , the server's | `kubernetes`: the workers' Deployment |
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

Distributing does not make a statement cheaper: the same work runs on more
machines for less time, plus the start and the idle tail. It makes a
statement that would take many minutes on one machine finish in a fraction
of that, and lets statements whose joins and aggregations need more memory
than one machine has run without spilling.

## Limitations

- Partitioned joins cover two large tables joined on equal columns. A third
  large table on another key, a large table joined to itself, and a large
  table read again in a subquery run on one machine; give that machine a
  lot of memory and fast spill disk.
- Partitioned joins write the columns they use to object storage and read
  them back: that costs request charges and adds a few seconds, and
  leftovers of a coordinator that crashed mid-statement stay under
  `_igloo_shuffle/` (a lifecycle rule that expires it after a day cleans
  them up).
- The coordinator receives every partial result. A `GROUP BY` with billions
  of groups, or `count(DISTINCT x)` over billions of values, sends them all
  to it.
- Tables with delete files (from `UPDATE`, `DELETE` or `MERGE`) or nested
  columns, views, and time travel run on one machine.
- Workers read each table's files directly and do not use Iceberg partition
  statistics to skip files; DuckDB skips row groups by their Parquet
  statistics instead.
