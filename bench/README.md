# Price-performance benchmark

Times the same queries on Igloo, Snowflake and Databricks SQL Serverless,
on the same data, and prices each run by that platform's billing rules. It
serves the first [product principle](../docs/principles.md): dollars per
workload, not cost per query in isolation.

This repository publishes no results: run it on your own accounts, at your
own prices. Snowflake's and Databricks' acceptable use policies set
conditions on publishing benchmarks of their services, such as including
what others need to repeat them; read them before you publish yours.

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


**Queries.** The 22 TPC-H queries in `queries/tpch`, from DuckDB's TPC-H
extension, translated with sqlglot for Snowflake and Databricks. Each query
runs once cold and then three times warm, one at a time. Result caches are
off on every platform, so warm runs measure the engine.

**Sizes.** Run each platform at the sizes a team would actually pick, and
compare them at matched cost as well as matched speed. On Snowflake, use
standard warehouses of either hardware generation, Gen1 or Gen2 (the
default for new warehouses), with query acceleration off; `run.py` records
the generation. Run every platform in the same region as its data.

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
listed in `pricing.toml`); runs that do not name one are priced with its
default disk.
Object storage costs about the same everywhere and is left out.

The list prices in `pricing.toml` are a starting point. Replace them with
negotiated rates (`--set snowflake.price_per_credit=...`) before drawing
conclusions, and record which prices a published report used.

## Reading the Igloo results

- **Igloo caches table files on local disk.** The first time a machine reads
  a file from S3 it keeps a copy on its disk ([read cache](../docs/operations.md#read-cache)),
  and the copy survives restarts, so the first run after a scale-to-zero
  resume is much faster once the cache is warm; repeat runs are not
  affected. Label the first burst after a fresh install
  `S3, empty disk cache`: it still reads everything from S3.
- **Repeat runs** with tables in S3 and on local disk should be close; a
  large gap points at the cache or the disk.
- `report.py` merges repeated results for the same platform, size, storage
  and mode. A run's first ("cold") pass is only cold if nothing ran on the
  machine since it started, so keep one result per fresh start when
  comparing cold times.

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
