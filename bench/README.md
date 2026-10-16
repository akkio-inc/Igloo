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
  comparison.
- Snowflake: the built-in `SNOWFLAKE_SAMPLE_DATA.TPCH_SF10` and `TPCH_SF100`
  schemas hold the same data (set `SNOWFLAKE_SCHEMA`).
- Databricks: write Parquet with DuckDB
  (`COPY (FROM lineitem) TO 's3://bucket/tpch/sf10/lineitem' (FORMAT parquet)`
  after `CALL dbgen(sf = 10)`), then
  `CREATE TABLE lineitem AS SELECT * FROM read_files('s3://bucket/tpch/sf10/lineitem', format => 'parquet')`
  for each table. Table formats differ (Iceberg, Snowflake's own, Delta), as
  they would in practice.

**Queries.** The 22 TPC-H queries in `queries/tpch`, from DuckDB's TPC-H
extension, translated with sqlglot for Snowflake and Databricks. Each query
runs once cold and then three times warm, one at a time. Result caches are
off on every platform, so warm runs measure the engine.

**Sizes.** Each platform runs at the sizes a team would actually pick, and
the report compares them at matched cost as well as matched speed: Snowflake
X-Small to Medium, Databricks 2X-Small to Small, Igloo on a few EC2 machine
types. Run every platform in the same region as its data.

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
