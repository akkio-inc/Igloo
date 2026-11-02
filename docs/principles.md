# Product principles

These are the rules Igloo is designed by. When a proposal is unclear, check
it against them; when two of them conflict, the one higher on this page wins.
Correctness and security are not on the list because they are never traded
away.

## 1. Price-performance is the north star; total cost breaks ties

Measure dollars per workload: compute and storage, the time compute sits idle,
and the hours people spend running it. Not cost per query in isolation.

Faster is usually cheaper, since compute is billed by time, so cost and
performance mostly agree. When they don't, cost wins, and a feature that
raises steady-state cost has to pay for itself.

In practice:
- Run in the customer's own cloud account, on their discounted compute.
- One node by default; spread a query over more only when it needs them.
- Idle compute should cost nothing.

## 2. People are the largest cost: no knobs, no babysitting

The main thing Snowflake sells is that nobody has to operate it. Anything an
operator must remember to do (compaction, snapshot cleanup, statistics,
memory sizing, scaling) is a cost, not a feature. Defaults must be right
without tuning; a setting that needs expertise to get right is a bug.

A compute saving that costs more in operator hours than it saves is not a
saving.

## 3. Open formats only; leaving costs nothing

All data lives as Apache Iceberg and Parquet in the customer's own bucket,
behind a standard Iceberg REST catalog. No proprietary storage, no export
step to leave. This keeps storage at the cloud provider's price, earns trust,
and makes migration cheap: Igloo reads existing tables where they are.

## 4. Performance through fewer moving parts, not more machines

A vectorized engine on one well-sized machine beats a cluster for most
analytical work. Prefer local caches of hot data, fewer network round trips
and less coordination. Distribution is the escape hatch for queries that do
not fit, not the default. Benchmarks measure it on real cloud machines, and
their results are published with their method ([benchmark](../bench/README.md)).
They run by hand, not in CI, so a slowdown shows at the next run.

## 5. Feature parity by what blocks a migration, not by checklist

Build Snowflake and Databricks features in the order their absence stops
real workloads from moving: the SQL dialect and semi-structured data first,
then views, `MERGE` and grants, then masking and governance, then tasks,
streams and sharing, then Snowpark-style UDFs. Skip features whose main value
is lock-in.

## 6. Installing takes one command and works from defaults

A trial is one container and a bucket. Production is one `helm install` or
one `tofu apply`. The only required input is where to keep the data.
Targets: under five minutes to the first query on a laptop, under thirty on a
new cloud account.

## 7. Migrating is a command, not a project

Moving from Snowflake or Databricks should take one command that copies (or,
better, attaches in place) the data, translates the SQL, and proves the result
matches. Applications should keep working with the clients and SQL they
already use.

## Using these principles

- Design documents and larger pull requests say which principles they serve
  and which they cost.
- Releases should report the measures behind them: price-performance on the
  benchmark suite, time to first query, and migration coverage.
