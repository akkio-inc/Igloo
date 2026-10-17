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

## Method

**Data.** TPC-H at scale factor 10 (about 10 GB) and 100. TPC-H's generator
is deterministic, so every platform gets identical rows:

- Igloo: `make load-tpch SF=10`, or on a cloud deployment
  `CALL dbgen(sf = 10)` followed by `CREATE TABLE ... AS SELECT` per table,
  as `deploy/single-vm/sql/load-tpch.sql` does. Keep the tables in the
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
off on every platform, so warm runs measure the engine.

**Sizes.** Each platform runs at the sizes a team would actually pick, and
the report compares them at matched cost as well as matched speed: at scale
10, Snowflake X-Small, Databricks 2X-Small and Igloo on a few EC2 machine
types; at scale 100, Snowflake X-Small to X-Large (standard Gen1
warehouses, query acceleration off), Databricks 2X-Small to X-Large and Igloo
on c7g.4xlarge, c7g.8xlarge and c7g.16xlarge. Run every platform in the
same region as its data.

**Cost.** `report.py` prices each run with `pricing.toml`:

| | Billed for | Minimum | Idle before it stops |
|---|---|---|---|
| Snowflake | credits per hour by warehouse size, per second | 60 s per resume | `AUTO_SUSPEND` (60 s here) |
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
platform and size. Storage costs about the same everywhere (object storage)
and is left out.

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
X-Large; Databricks SQL Serverless 2X-Small to X-Large. All results are in
`results/` and `results/report.md`.

**Warm runs.** Every Igloo size is faster than Snowflake X-Small and
Databricks 2X-Small, which cost more per hour than any of them, and Igloo is
cheaper per warm run than every warehouse at every size. But the lead
narrows, and c7g.4xlarge is slower than Databricks Medium and up and only
ties Snowflake X-Large. From scale 10 to 100 Igloo's warm run grew about in
line with the data (c7g.4xlarge: 5.5 s to 58.5 s), while Snowflake X-Small
grew 5x (37 s to 185 s) and Databricks 2X-Small 4x (30 s to 127 s): at
scale 10 much of their time is fixed per-query overhead. Igloo against each
warehouse, faster / cheaper per warm run (warm run in seconds):

| | c7g.4xlarge, 58.5 s | c7g.8xlarge, 27.5 s | c7g.16xlarge, 17.8 s |
|---|---|---|---|
| Snowflake XS, 185 s | 3.2x / 16x | 6.7x / 18x | 10.4x / 13x |
| Snowflake S, 113 s | 1.9x / 20x | 4.1x / 21x | 6.4x / 16x |
| Snowflake M, 86 s | 1.5x / 30x | 3.1x / 32x | 4.8x / 25x |
| Snowflake L, 66 s | 1.1x / 47x | 2.4x / 50x | 3.7x / 38x |
| Snowflake XL, 57 s | 1.0x / 81x | 2.1x / 86x | 3.2x / 66x |
| Databricks 2XS, 127 s | 2.2x / 10x | 4.6x / 11x | 7.1x / 9x |
| Databricks S, 63 s | 1.1x / 16x | 2.3x / 17x | 3.5x / 13x |
| Databricks M, 48 s | 0.8x / 24x | 1.8x / 25x | 2.7x / 20x |
| Databricks L, 44 s | 0.8x / 37x | 1.6x / 39x | 2.5x / 30x |
| Databricks XL, 43 s | 0.7x / 72x | 1.6x / 76x | 2.4x / 59x |

With the pairing used at scale 10 (c7g.4xlarge against Snowflake X-Small
and Databricks 2X-Small), Igloo is 3.2x and 2.2x faster and 16x and 10x
cheaper per warm run, against 6.8x and 5.4x faster and 35x and 26x cheaper
at scale 10. No pairing at scale 100 is both 5x faster and 25x cheaper; the
nearest is c7g.16xlarge against Snowflake Medium (4.8x, 25x). Snowflake and
Databricks gain little beyond Large: X-Large cuts the warm run by 14% and 2%
for twice the price.

**The first run after a resume** is where Igloo's lead goes. It waits about
17 s for the machine to start, then reads table data from the disk cache
(136 s, 65 s and 57 s on the three sizes) or, with an empty cache, from S3
(333 s, 160 s and 102 s). The cached run is limited by the disk: the cache
holds 27 GB at scale 100, and the volume is provisioned for 500 MB/s, so
reading it takes about a minute on any machine. Snowflake resumes within
its first query, and Databricks' serverless start (about 10 s, not counted
here) is short, so their first runs (Snowflake 208 s at X-Small to 71 s at
X-Large, Databricks 164 s to 65 s) beat c7g.4xlarge's (153 s with the
start) from Snowflake Small and Databricks Small up, and match or beat
c7g.16xlarge's (75 s) from Snowflake X-Large and Databricks Medium up. Igloo
still costs less per burst at every pairing (2.6x to 166x), because the others
bill a minimum and an idle tail: at 24 bursts a day, c7g.4xlarge with scale
to zero comes to $30 a month, Snowflake X-Small to $163 and Databricks
2X-Small to $263.

**Caveats.**

- The disk is priced too low for this scale. `pricing.toml` charges a 20 GB
  volume ($1.61 a month); the machine used a 100 GB gp3 volume with 6,000
  IOPS and 500 MB/s, about $38 a month, which the cached first runs depend
  on. That adds about $36 to each Igloo scale-to-zero monthly figure at
  scale 100, and nothing to the cost per run.
- Per query, Snowflake matches or beats Igloo on the date-range scans q06
  and q14 at most pairings (under a second on every Snowflake size, against
  0.9 s and 1.6 s on c7g.4xlarge): Snowflake's own copy of the data lets it
  skip most of lineitem, while the Igloo and Databricks copies are in
  generator order. Databricks X-Large ties c7g.16xlarge on q09.
- c7g.4xlarge's 32 GB ran every query, so memory-optimized machines were not
  needed, but q09 took 4x as long as on c7g.8xlarge where most queries took
  1.8x: the sign of memory pressure.
- q11 returns no rows at scale 100 on every platform: its text keeps scale
  1's `FRACTION`. The work is the same everywhere.
- Igloo's timings are wall-clock at a client on the gateway machine, in the
  same zone; Snowflake's and Databricks' are the server's.

## Distributed execution

`distributed.py` measures [distributed execution](../docs/distributed-execution.md)
against one machine: it runs each query of a suite with
`SET distributed_execution = 'off'` and `'on'`, interleaved, and reports the
fastest of `--runs` runs of each, the speedup, how many workers ran it (from
`igloo.query_history`) and the cost of one run at the prices given:

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

## What it does not measure yet

- **Concurrency.** Queries run one at a time. Many users at once favor
  platforms that add clusters automatically (Snowflake multi-cluster,
  Databricks serverless scaling); Igloo scales out with replicas.
- **Loading and transformation.** Only queries are timed.
- **A real workload.** TPC-H is a stand-in. The next step replays a
  production query mix: export a week of query text and timings from
  Snowflake (`SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`) and Databricks
  (`system.query.history`), run the same statements against copies of the
  same tables on each platform, and price them with the actual arrival
  pattern instead of `--runs-per-day`.
