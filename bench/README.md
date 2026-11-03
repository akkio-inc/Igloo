# Price-performance benchmark

Compares what the same work costs on Igloo, Snowflake and Databricks SQL
Serverless: the same data, the same queries, timed on each platform and
priced by each one's billing rules. It serves the first
[product principle](../docs/principles.md): dollars per workload, not cost
per query in isolation.

```sh
pip install -r bench/requirements.txt
python bench/run.py --platform igloo --size c7g.4xlarge --scale 10
python bench/run.py --platform snowflake --size M --scale 10
python bench/run.py --platform databricks --size S --scale 10
python bench/report.py bench/results/*.json > bench/results/report.md
```

`python -m unittest discover -s bench` tests the scripts without a
platform to run against; `make bench-test` runs those tests in a Python
container, as CI does.

## Method

**Data.** TPC-H at scale factor 10 (about 10 GB) and 100. TPC-H's generator
is deterministic, so every platform gets identical rows:

- Igloo: `make load-tpch SF=10`, or on a cloud deployment
  `CALL dbgen(sf = 10)`, one at a time, followed by `CREATE TABLE ... AS SELECT`
  per table, as `deploy/single-vm/sql/load-tpch.sql` does. Keep the tables in the
  cloud's object storage (Amazon S3 here), as Snowflake and Databricks do,
  and pass `--storage S3` so the report labels the run; results with tables
  on the machine's own disk are labeled `local disk` and are not a fair
  comparison. Scale factor 100 does not fit in memory at once, so generate
  it in chunks, `CALL dbgen(sf = 100, children = 4, step = 0)` through
  `step = 3`, creating the tables from the first chunk and inserting the
  others; the chunks together are exactly the rows of one `dbgen`. Put each
  scale factor in its own schema and point `run.py` at it with
  `IGLOO_SCHEMA` (e.g. `sf100`). Loading leaves many small files that
  Igloo's [table maintenance](../docs/operations.md#table-maintenance)
  compacts over its next passes (three for lineitem at scale 100); wait for
  compaction to finish before measuring, e.g. with
  `IGLOO_MAINTENANCE_INTERVAL=1m` until a pass compacts nothing.
- Snowflake: the built-in `SNOWFLAKE_SAMPLE_DATA.TPCH_SF10` and `TPCH_SF100`
  schemas hold the same data (set `SNOWFLAKE_SCHEMA`).
- Databricks: write Parquet with DuckDB
  (`COPY (FROM lineitem) TO 's3://bucket/tpch/sf10/lineitem' (FORMAT parquet)`
  after `CALL dbgen(sf = 10)`), then
  `CREATE TABLE lineitem AS SELECT * FROM read_files('s3://bucket/tpch/sf10/lineitem', format => 'parquet')`
  for each table (in chunks at scale 100, as for Igloo). Table formats
  differ (Iceberg, Snowflake's own, Delta), as they would in practice.

At scale 100 the three platforms' copies were checked against each other:
the same row counts per table, the same sums of `l_extendedprice`,
`l_quantity`, `l_orderkey` and `o_totalprice`, and the same number of result
rows for every query.

**Queries.** The 22 TPC-H queries in `queries/tpch`, from DuckDB's TPC-H
extension, translated with sqlglot for Snowflake and Databricks. Each query
runs once cold and then three times warm, one at a time. Result caches are
off on every platform, so warm runs measure the engine; `run.py` checks
that Igloo's is and records it in the results (`result_cache`).

**Sizes.** Each platform runs at the sizes a team would actually pick, and
the report compares them at matched cost as well as matched speed: at scale
10, Snowflake X-Small and Small, Databricks 2X-Small and Igloo on a few EC2
machine types; at scale 100, Snowflake X-Small to X-Large, Databricks
2X-Small to X-Large and Igloo on c7g.4xlarge, c7g.8xlarge and c7g.16xlarge.
Snowflake runs on standard warehouses of both hardware generations, Gen1
and Gen2 (the default for new warehouses), with query acceleration off;
`run.py` records the generation. Run every platform in the same region as
its data.

**Cost.** `report.py` prices each run with `pricing.toml`:

| | Billed for | Minimum | Idle before it stops |
|---|---|---|---|
| Snowflake | credits per hour by warehouse size and generation (Gen2 1.35x), per second | 60 s per resume | `AUTO_SUSPEND` (60 s here) |
| Databricks SQL Serverless | DBUs per hour by warehouse size, per second | none | auto-stop (5 min here) |
| Igloo, always on | the EC2 machine and its disk, per second | none | never stops |
| Igloo, [scale to zero](../docs/scale-to-zero.md) | the machine per second while it runs; the disk and a `t4g.nano` gateway all month | 60 s per start (EC2) | `IGLOO_GATEWAY_SUSPEND_AFTER` (60 s here) |

It reports the cost of one warm run, of one burst from a stopped warehouse
or machine (cold run, minimums and idle tail included), and per month at
`--runs-per-day` bursts. Always-on Igloo's monthly figure is the machine
running all month. For scale to zero, run the client through the gateway
with the machine stopped and pass `--scale-to-zero`: the connect time is
then the measured time to resume, and `report.py` bills each burst for it.
Repeat the burst a few times; `report.py` merges results for the same
platform and size. Igloo's disk is priced as gp3 from its size and
provisioned IOPS and throughput: name it with `run.py --disk` (disks are
listed in `pricing.toml`); runs that do not name one used the benchmark
machine's 100 GB volume at 6,000 IOPS and 500 MB/s, about $38 a month.
Object storage costs about the same everywhere and is left out.

The list prices in `pricing.toml` are a starting point. Replace them with
negotiated rates (`--set snowflake.price_per_credit=...`) before drawing
conclusions, and record which prices a published report used.

## Reading the Igloo results

- **Igloo caches table files on local disk.** The first time a machine reads
  a file from S3 it keeps a copy on its disk ([read cache](../docs/operations.md#read-cache)),
  and the copy survives restarts. On a c7g.4xlarge with tables in S3, the
  first run of the 22 queries after a scale-to-zero resume took about 8
  seconds with the cache warm, against 37 to 59 seconds before Igloo had a
  cache; repeat runs take 5.5 seconds either way. Results labeled
  `S3, empty disk cache` are the first burst after a fresh install, which
  still reads everything from S3.
- **Igloo restores its memory after a start.** It keeps a list of the parts
  of table files DuckDB holds in memory, and after a resume reads them back
  from S3, many requests at once
  ([memory after a start](../docs/operations.md#memory-after-a-start)).
  Results labeled `S3, disk cache` include it: their first run finds in
  memory what the burst before it read.
- **Repeat runs** are the same on S3 and on local disk, within a few
  percent.
- `report.py` merges repeated results for the same platform, size, storage
  and mode. A run's first ("cold") pass is only cold if nothing ran on the
  machine since it started, so keep one result per fresh start when
  comparing cold times.

## Scale factor 100

Run in October 2026: Igloo d208dd2 (DuckDB 1.5.6) with tables in S3 and
the read cache, scale-to-zero bursts as above (the first with an empty
cache, three with the cache the first filled); Snowflake X-Small to
X-Large, on Gen1 and on Gen2; Databricks SQL Serverless 2X-Small to
X-Large. c7g.8xlarge and c7g.16xlarge ran again the same way on November
3, 2026, on Igloo 4ba090ed, with the result cache off, on TPC-H loaded
afresh as above. All results are in `results/` and `results/report.md`.

**Warm runs.** Every Igloo size is faster than Snowflake X-Small (either
generation) and Databricks 2X-Small, which cost more per hour than any of
them, and Igloo is cheaper per warm run than every warehouse at every size.
But the lead narrows: c7g.4xlarge is slower than Databricks Medium and up
and Snowflake Gen2 Large and up, and about ties Snowflake Gen1 X-Large and
Gen2 Medium. From scale 10 to 100 Igloo's warm run grew about in line with
the data (c7g.4xlarge: 5.5 s to 58.5 s), while Snowflake Gen1 X-Small grew
5x (37 s to 185 s) and Databricks 2X-Small 4x (30 s to 127 s): at scale 10
much of their time is fixed per-query overhead. Igloo against each
warehouse, faster / cheaper per warm run (warm run in seconds):

| | c7g.4xlarge, 58.5 s | c7g.8xlarge, 25.2 s | c7g.16xlarge, 15.6 s |
|---|---|---|---|
| Snowflake XS Gen1, 185 s | 3.2x / 16x | 7.3x / 19x | 11.9x / 15x |
| Snowflake XS Gen2, 128 s | 2.2x / 15x | 5.1x / 18x | 8.2x / 14x |
| Snowflake S Gen1, 113 s | 1.9x / 20x | 4.5x / 23x | 7.2x / 19x |
| Snowflake S Gen2, 83 s | 1.4x / 20x | 3.3x / 23x | 5.3x / 19x |
| Snowflake M Gen1, 86 s | 1.5x / 30x | 3.4x / 35x | 5.5x / 28x |
| Snowflake M Gen2, 62 s | 1.1x / 30x | 2.4x / 34x | 3.9x / 28x |
| Snowflake L Gen1, 66 s | 1.1x / 47x | 2.6x / 54x | 4.2x / 44x |
| Snowflake L Gen2, 53 s | 0.9x / 51x | 2.1x / 59x | 3.4x / 47x |
| Snowflake XL Gen1, 57 s | 1.0x / 81x | 2.3x / 94x | 3.6x / 75x |
| Snowflake XL Gen2, 44 s | 0.8x / 84x | 1.7x / 98x | 2.8x / 79x |
| Databricks 2XS, 127 s | 2.2x / 10x | 5.0x / 12x | 8.1x / 10x |
| Databricks S, 63 s | 1.1x / 16x | 2.5x / 18x | 4.0x / 15x |
| Databricks M, 48 s | 0.8x / 24x | 1.9x / 28x | 3.1x / 22x |
| Databricks L, 44 s | 0.8x / 37x | 1.8x / 42x | 2.8x / 34x |
| Databricks XL, 43 s | 0.7x / 72x | 1.7x / 83x | 2.8x / 67x |

With the pairing used at scale 10 (c7g.4xlarge against Snowflake X-Small
and Databricks 2X-Small), Igloo is 3.2x faster and 16x cheaper per warm run
than Gen1 X-Small, 2.2x and 15x than Gen2 X-Small, and 2.2x and 10x than
Databricks 2X-Small; at scale 10 it was 6.8x and 35x against Gen1 X-Small,
4.3x and 30x against Gen2 X-Small, and 5.4x and 26x against Databricks
2X-Small. No pairing at scale 100 is both 5x faster and 30x cheaper; the
nearest is c7g.16xlarge against Snowflake Gen1 Medium (5.5x, 28x).
Snowflake and Databricks gain little beyond Large: X-Large cuts the warm
run by 14% (Snowflake Gen1), 17% (Gen2) and 2% (Databricks) for twice the
price.

**Snowflake Gen2** runs the suite 1.25x to 1.45x faster than Gen1 at scale
100 and about 1.5x faster at scale 10, for 1.35x the credits per hour. A
warm run therefore costs about the same on either (from 13% less to 8%
more), while a burst costs from 13% less (X-Small at scale 10, whose cold
run Gen2 cuts from 79 s to 30 s) to 23% more on Gen2, because the idle
tail is billed at the higher rate. Igloo's lead over Snowflake shrinks by
Gen2's speed-up. The scale 10 Gen1 X-Small result is the earlier run on
another warehouse (COMPUTE_WH, also Gen1); every other Snowflake run used
one warehouse, BENCH_WH, switched between generations.

**The first run after a resume.** Igloo waits about 15 s for the machine
to start (15.3 s on c7g.16xlarge, 15.6 s on c7g.8xlarge), then runs the
suite while it reads back from S3 what DuckDB held in memory before the
stop ([memory after a start](../docs/operations.md#memory-after-a-start)):
22 GB in 7.5 s on c7g.16xlarge and in 14.5 s on c7g.8xlarge, as fast as
their networks carry it. Their first runs took 17.3 s and 28.6 s, 1.1x
their warm runs; with an empty cache, which reads every table file from
S3, 58.2 s and 102.3 s. With the start, c7g.16xlarge's first run (32.6 s)
is faster than every warehouse's cold run (Snowflake Gen1 208 s at X-Small
to 71 s at X-Large, Gen2 154 s to 59 s, Databricks 164 s to 65 s), 1.8x
Snowflake Gen2 X-Large's, and c7g.8xlarge's (44.2 s) 1.3x. Snowflake
resumes within its first query, and Databricks' serverless start (about
10 s) is not counted. c7g.4xlarge's runs are October's (153 s with the
start). Igloo costs less per burst at every pairing (3.7x to 170x),
because the others bill an idle tail: at 24 bursts a day, c7g.4xlarge
with scale to zero comes to $66 a month, Snowflake X-Small to $163 (Gen1)
or $176 (Gen2) and Databricks 2X-Small to $263.

**Caveats.**

- Igloo's monthly figures include the disk the runs used, 100 GB of gp3 at
  6,000 IOPS and 500 MB/s ($38 a month), which the cached first runs depend
  on. Earlier reports charged a 20 GB disk ($1.61); the real disk raises
  every Igloo scale-to-zero month by $36, e.g. c7g.4xlarge from $14 to $51
  at scale 10 (Snowflake X-Small: $85 on Gen1, $74 on Gen2; Databricks
  2X-Small: $194) and from $30 to $66 at scale 100. Per-run costs do not
  change.
- Per query, Snowflake matches or beats Igloo on the date-range scans q06
  and q14 at most pairings (under a second on every Snowflake size, against
  0.9 s and 1.6 s on c7g.4xlarge): Snowflake's own copy of the data lets it
  skip most of lineitem, while the Igloo and Databricks copies are in
  generator order. Databricks X-Large comes close to c7g.16xlarge on q09 (2.4 s
  against 2.2 s).
- On d208dd2, c7g.4xlarge's 32 GB ran every query, so memory-optimized machines
  were not needed, but q09 took 4x as long as on c7g.8xlarge where most queries took
  1.8x: the sign of memory pressure.
- q11 returns no rows at scale 100 on every platform: its text keeps scale
  1's `FRACTION`. The work is the same everywhere.
- Igloo's timings are wall-clock at a client on the gateway machine, in the
  same zone; Snowflake's and Databricks' are the server's.

## Distributed execution

`distributed.py` measures [distributed execution](../docs/distributed-execution.md)
against one machine: it runs each query of a suite with
`SET distributed_execution = 'off'` and `'on'`, interleaved, with the
result cache off, and reports the fastest of `--runs` runs of each, the
speedup, how many workers ran it (from `igloo.query_history`) and the cost
of one run at the prices given:

```sh
python bench/distributed.py --suite tpch --runs 3 \
  --coordinator-price 1.157 --worker-price 1.157 --label "c7g.8xlarge + 8 c7g.8xlarge" \
  --out bench/results/distributed-sf1000-8.json
```

Two suites fit it: `tpch` and `media`, a synthetic ad-impressions workload
(delivery, reach, frequency, household reach through a device graph, top
spenders, click-through rates) generated by
[`load-media.sql`](../deploy/single-vm/sql/load-media.sql) at any size
(`psql -v rows=10000000000 -f load-media.sql`).

The development machine is no place for large data, so the scaling
measurements are meant for EC2:

1. **Data.** In one region, TPC-H at scale factors 100 and 1000 in S3 (as
   above: `CALL dbgen(sf = 100, children = 10, step = n)` for n = 0..9 into
   a scratch database, then `INSERT` into the Iceberg tables, so no machine
   holds a whole table in memory), and `load-media.sql` with
   `rows=10000000000` (about 10 billion impressions). Let a maintenance
   pass compact what loading wrote before measuring, and keep maintenance
   off during the runs (`IGLOO_MAINTENANCE=false`): a compaction in the
   middle changes how many files, and so how many workers, a statement
   has.
2. **Machines.** Igloo with the Helm chart on EKS, or `deploy/single-vm` on
   one instance with workers started by `IGLOO_WORKER_COMPUTE=ec2`; workers
   and coordinator of the same type (c7g.8xlarge, then c7gn.8xlarge for its
   network), workers on their own node group or instances.
3. **Runs.** For 2, 4, 8 and 16 workers (`IGLOO_WORKER_MAX`), with workers
   running: `distributed.py` for both suites at both sizes, prices from
   `pricing.toml`. Then once more with workers stopped, to record the start
   time a first statement pays.
4. **Single machines to compare with:** the same suites on one c7g.16xlarge
   and one r7g.16xlarge, to find where one larger machine stops being the
   cheaper answer, and the existing Snowflake and Databricks runs at the
   matching sizes.

Report per query: one machine's time, the distributed time per worker
count, the speedup, and the cost per query; and per worker count, the total
over the queries that ran distributed. Results go in `bench/results/`.

## Mixed load

`mixed.py` runs dashboards and ELT on Igloo at the same time, as a
warehouse sees them, and reports per class of work the throughput, the
latency percentiles (p50, p95, p99), the time statements waited for
[workload limits](../docs/workload-management.md) (`queued_ms` from the
query history), errors, how many statements ran on workers, and what the
window cost:

- **Dashboards:** `--dashboard-users` BI users, each opening one of three
  dashboards (delivery, reach, and tiles over the tables the ELT jobs
  build) after another, with an exponential think time (`--think`). A
  dashboard's tiles run at once, `--tile-concurrency` at a time, and
  filter by an advertiser from a small pool (`--param-pool`) and one of
  three date ranges, so dashboards repeat; the result cache stays as the
  server has it unless `--result-cache off`.
- **ELT:** `ingest` (`INSERT` of a new batch of impressions), `rollup`
  (`CREATE OR REPLACE TABLE ... AS`), `increment` (`INSERT ... SELECT` of
  new batches past a watermark), `merge` (`MERGE` of recent campaign
  totals) and `refresh` (`REFRESH MATERIALIZED VIEW`), each due every
  `--elt-every` seconds, run by `--elt-streams` sessions; a job still
  running when it is due again is skipped, as Igloo's schedules do, and
  the report counts the skips.
- **Heavy reports:** `--heavy-sessions` sessions running long reports over
  every impression one after another, their result cache off, as an
  analyst's session beside the dashboards.
- **Probes:** `--probes` runs a statement of each kind once a second, to
  tell where a dashboard waits: `ping` takes Igloo's path and no DuckDB
  task, `cpu` only DuckDB's threads, `catalog` also loads a table from the
  catalog and reads its one small file.

```sh
python bench/mixed.py setup --rows 2000000                    # synthetic schema "mixed"
python bench/mixed.py run --duration 120 --warmup 20 --dashboard-users 8 --elt-streams 2 \
    --metrics-url http://127.0.0.1:8432/metrics --coordinator m7g.4xlarge \
    --out bench/results/mixed-local.json
```

Statements run with `IGLOO_DSN`'s user, and their text starts with
`/* mixed RUN CLASS NAME */`, the same each time a tile repeats, so the
history can be read per run. Cost is the coordinator's price for the
window plus each worker's for the seconds it ran: with `--metrics-url`
the client samples `igloo_distributed_workers` every second, idle tails
included (a scrape that fails keeps the last count); without it, workers
are billed for the seconds their statements ran there, which leaves out
idle time but counts a worker once per concurrent statement, so it can come
out far above what the workers cost. Prices come from `pricing.toml`
(`--coordinator`, `--worker` name instance types) or are given per hour;
`--spot-factor` scales the worker price. On a fleet, raise `--rows` at
setup (10 billion takes the generator about as long as `load-media.sql`
does), `--dashboard-users`, `--elt-streams` and `--batch-rows`, and run
the client next to the coordinator. The data is synthetic, computed from
row numbers as in `load-media.sql`.

## Answers beyond one machine's memory

`overmemory.py` checks Igloo's answers where the data outgrows the
coordinator: TPC-H (with joins and aggregations on a key that holds 60% of
the rows) and TPC-DS, at a scale whose working sets exceed the
coordinator's memory, for example scale factor 100 with
`IGLOO_DUCKDB_MEMORY_LIMIT=8GB`. Plain DuckDB with ample memory generates
the data, writes it as Parquet and keeps every query's answer; Igloo loads
the same files and runs each query with distributed execution `off`, `on`
or `auto`. Per query it records whether the rows match, where the
statement ran, its time, the coordinator's and the workers' peak memory,
spill, and the bytes the workers exchanged; `--writes` adds a copy and a
bucketed copy of a table, each checked against it.

```sh
python bench/overmemory.py reference --suite tpcds --scale 100 --data /mnt/data/tpcds100 --out tpcds100.ref.json
aws s3 sync /mnt/data/tpcds100 s3://BUCKET/bench-data/tpcds100/
python bench/overmemory.py load --reference tpcds100.ref.json --schema lake.tpcds100 --source s3://BUCKET/bench-data/tpcds100
python bench/overmemory.py run --reference tpcds100.ref.json --schema lake.tpcds100 --mode on --label on \
    --out results.jsonl --workers 10.0.0.11,10.0.0.12 --worker-spill-cmd 'ssh {host} du -sb /var/lib/igloo/tmp'
python bench/overmemory.py report results.jsonl > report.md
```

Numbers compare to six significant digits, rows in any order. A query whose
answer has up to 1,000 rows lists the rows only one side returned.

## Catalog lock waits

`catalogload.py` runs dashboards, ingestion, schema changes as a dbt run
makes them, and BI metadata discovery (`information_schema`, `pg_catalog`
and `DESCRIBE`, each from a new session) on one replica at once, then
reports from the history how long statements waited for the catalog lock
(`catalog_wait_ms`) at p50 and p95 by stream and statement type, how long
BI sessions took to open, and the replica's threads.

```sh
python bench/catalogload.py setup --schema lake.catload
python bench/catalogload.py run --schema lake.catload --duration 600 --out catalogload.json > catalogload.md
```

## What it does not measure yet

- **Concurrency against other platforms.** `mixed.py` runs many users and
  jobs at once on Igloo only; the other platforms' runs are one query at a
  time. Many users at once favor platforms that add clusters automatically
  (Snowflake multi-cluster, Databricks serverless scaling); Igloo scales
  out with replicas and workers.
- **A real workload.** TPC-H is a stand-in. `python -m migrate report`
  ([migrating](../docs/migrating.md)) now replays a production query mix
  from Snowflake's or Databricks' history on Igloo and compares results,
  time and cost; pricing the same statements on every platform with their
  actual arrival pattern, instead of `--runs-per-day`, is still to do.
