# Workload management and chargeback

An agency runs many clients' work on one warehouse. Igloo can attribute
every statement to a client or campaign, record what each one used, add
it up into a cost per client, keep one team's queries from crowding out
another's, and give a client or team compute of its own, with its own
bill.

| Need | Feature |
|---|---|
| Say which client a statement is for | [Query tags](#query-tags): `SET query_tag = 'client:acme;campaign:q4'` |
| See what each statement used | [Resources in the history](#resources-in-the-history): CPU time, bytes read, memory, time waiting |
| Add it up per client, user and day, with a cost | [`igloo.usage`](#usage-and-cost) |
| Keep a role's queries in bounds | [Role limits](#role-limits): `ALTER ROLE analysts SET max_concurrent_statements = 4` |
| Separate compute and a separate bill | [Compute pools](#compute-pools) |

## Query tags

A query tag is a free-form label on every statement a session runs, kept in
the [query history](sql.md#query-history) and summed in
[`igloo.usage`](#usage-and-cost):

```sql
SET query_tag = 'client:acme;campaign:q4-launch';
SHOW query_tag;
RESET query_tag;
ALTER SESSION SET QUERY_TAG = 'client:acme';   -- Snowflake's spelling works too
```

A tag is at most 2,000 bytes. Igloo does not interpret it; a convention
such as `key:value;key:value` lets you split it in SQL, for example
`regexp_extract(query_tag, 'client:([^;]+)', 1)`.

Admins can give users and roles a default tag, which their sessions use
until they set their own:

```sql
ALTER USER ana SET query_tag = 'client:acme';
ALTER ROLE acme_team SET query_tag = 'client:acme';   -- every member
```

Sessions can always set a tag of their own, so a tag says what a statement
claims to be for. Where attribution must not depend on what users say,
give the client users of its own or a [compute pool](#compute-pools): the
history records the user and the pool, which users cannot change.

## Resources in the history

Every statement in `igloo.query_history` also has:

| Column | Meaning |
|---|---|
| `query_tag` | The tag it ran under, or `NULL` |
| `pool` | The [compute pool](#compute-pools) of the replica that ran it |
| `queued_ms` | Time it waited for its [role limits](#role-limits) before starting |
| `cpu_time_ms` | CPU time DuckDB's operators spent on it, summed over threads |
| `bytes_read` | Bytes DuckDB read from files for it, such as the data and metadata files of the tables it read |
| `memory_allocated_bytes` | Memory DuckDB's buffer pool allocated for it over its run; memory freed and allocated again counts again, so this is not a peak |

The resource columns come from DuckDB's profiler, which Igloo keeps on for
every session with only these measurements. Reading them back takes about
0.2 milliseconds per statement. On the 22 TPC-H queries at scale factor 1,
the profiler added 1 to 4% of CPU time across three runs, on a busy machine
whose run-to-run noise was of the same size.
They are `NULL` for statements DuckDB does not run, such as `SET
query_tag` or `CREATE USER`, for `PRAGMA` statements, which DuckDB runs as
other statements, and for statements that failed. DuckDB 1.5 reports peak
memory only for the whole process since it started, not per statement, so
the history does not have a peak, and it does not count bytes written to
object storage, so the history has no bytes written. Parts of remote files
DuckDB still holds in memory from earlier statements are not read again,
so running a query twice can count fewer bytes the second time.

```sql
-- The heaviest statements of the last day, per client.
SELECT regexp_extract(query_tag, 'client:([^;]+)', 1) AS client, user_name,
       round(cpu_time_ms / 1000) AS cpu_s, duration_ms, left(query_text, 80) AS query
FROM igloo.query_history
WHERE start_time > now() - INTERVAL 1 DAY AND cpu_time_ms IS NOT NULL
ORDER BY cpu_time_ms DESC LIMIT 20;
```

## Usage and cost

`igloo.usage` sums the history by day (UTC), compute pool, user, role and
query tag. Admins see every row; other users see their own.

| Column | Meaning |
|---|---|
| `day`, `pool`, `user_name`, `role`, `query_tag` | The group |
| `statements`, `failed` | Statements run, and how many did not succeed |
| `run_ms`, `queued_ms` | Time the statements ran, and time they waited for their limits |
| `cpu_time_ms`, `bytes_read` | What DuckDB measured, summed |
| `time_share` | `run_ms` as a share of the running time of every statement in the pool that day, from 0 to 1 |
| `compute_hours` | How long the pool's replicas ran that day |
| `estimated_cost` | The pool's cost that day times `time_share`; `NULL` when a replica's price is unknown |

```sql
-- Last month's compute cost per client.
SELECT regexp_extract(query_tag, 'client:([^;]+)', 1) AS client,
       round(sum(estimated_cost), 2) AS cost, round(sum(cpu_time_ms) / 3.6e6, 1) AS cpu_hours,
       sum(statements) AS statements
FROM igloo.usage
WHERE day >= date_trunc('month', current_date) - INTERVAL 1 MONTH
  AND day < date_trunc('month', current_date)
GROUP BY ALL ORDER BY cost DESC;
```

How the cost is estimated:

- Each replica records how long it runs, every minute, with its machine's
  hourly price, `IGLOO_PRICE_PER_HOUR` (the Helm value `pricePerHour`).
  Set it to what one replica's machine costs you, with discounts; it is `0`,
  unknown, by default, and costs are then `NULL`. A replica stopped by
  [scale to zero](scale-to-zero.md) records nothing, so stopped hours cost
  nothing.
- A pool's cost for a day is the sum over its replicas of hours run times
  price. Each group's share of it is its share of the time statements ran
  in the pool that day. The statements that ran carry the whole day's cost,
  idle time included, so the shares add up to the bill for the machines.
- Storage, the catalog and the metadata database are not included: they are
  shared, and cost the same whatever the queries.
- A statement's running time counts the time it took to send results, as in
  the history, but not the time it waited for limits.
- Non-admins see their own `time_share` and cost, computed against every
  statement in the pool.

The history keeps `IGLOO_AUDIT_RETENTION` (90 days by default), and so does
the usage. To keep months of usage, copy `igloo.usage` into a table on a
schedule, for example monthly.

## Role limits

Admins set limits on users, custom roles and built-in roles:

```sql
ALTER ROLE analysts SET max_concurrent_statements = 4;
ALTER ROLE analysts SET statement_timeout = '15min';
ALTER ROLE analysts SET memory_limit = '8GB';
ALTER ROLE reader SET statement_timeout = '1h';      -- every user with the built-in role reader
ALTER USER etl_service SET statement_timeout = 0;    -- no limit for this user
ALTER ROLE analysts RESET memory_limit;              -- or RESET ALL, or SET memory_limit = DEFAULT
SHOW ROLE SETTINGS;                                  -- admins see all; others, those that apply to them
```

| Setting | Effect |
|---|---|
| `statement_timeout` | The longest a statement may run, waiting time included. A session's own `SET statement_timeout` can be shorter, not longer. `0` means no limit. |
| `max_concurrent_statements` | How many statements may run at once on a replica. On a user, it counts that user's statements; on a custom or built-in role, all its members' together. Further statements wait in line. |
| `memory_limit` | The memory each statement reserves out of the replica's query memory (DuckDB's memory limit) before it starts. A statement whose reservation does not fit next to those already running waits in line. |
| `query_tag` | The default [query tag](#query-tags) |

Which setting applies to a user: their own, if they have one; otherwise
their custom roles' (where several set it, the most generous: the longest
timeout, the most statements, the smallest reservation; for the tag, the
first role by name); otherwise their built-in role's. A concurrency limit
counts against the user or role it came from.

How limits are enforced:

- **In line, in order.** Statements that must wait do so in arrival order.
  A statement never overtakes an earlier one that waits for the same slots,
  nor, if it reserves memory, one that waits for memory, so a large
  reservation is not starved by small ones. Statements of other users and
  roles are not held up.
- **Until a slot frees, the timeout, or a cancel.** A waiting statement
  counts against its timeout, and clients can cancel it. When the timeout
  runs out while it waits, it fails with SQLSTATE `57014` and a message that
  says what it waited behind, such as `canceling statement due to statement
  timeout: it waited 30s to start, behind the limit of 4 statements at once
  for role analysts`. The history records the time waited as `queued_ms`.
- **Per replica.** Each replica enforces limits on the statements it runs;
  three replicas run up to three times a role's `max_concurrent_statements`.
- **Statements, not commands.** Limits apply to statements DuckDB runs,
  including views, comments and cursors. Igloo's own commands (`SET`,
  `SHOW`, user and role management) are never held up, so an admin can
  always lift a limit.
- **Memory is reserved, not capped.** DuckDB has one memory pool per
  process and cannot stop a single statement at its reservation; one that
  needs more spills to disk once the replica's memory is full, as any
  statement does. A reservation decides how many heavy statements start at
  once, and a reservation larger than the replica's memory makes a
  statement wait until no other reservation is held. To be sure a client's
  queries never compete for memory with another's, give the client its own
  [compute pool](#compute-pools).
- **Distributed statements too.** A statement whose parts run on
  [workers](distributed-execution.md) holds its slot and its memory
  reservation on the replica that runs it, from before its first task to
  its last row, and its `statement_timeout` ends its tasks on the workers.
  Workers' memory is the workers' own: a reservation does not reserve it,
  and statements on a pool's workers share them as statements on one
  replica share its memory.
- Limits reach open sessions at once on the replica that changed them and
  within 30 seconds on the others. They are kept in the metadata database,
  next to users and roles, and go when the user or role is dropped. If the
  metadata database cannot be read when a session starts, its limits apply
  from its first statement after the database is back.

`igloo_statements_waiting` and `igloo_statement_wait_seconds_total`
([metrics](operations.md#monitoring)) show how often limits hold statements
back.

## Compute pools

A compute pool, Snowflake's "virtual warehouse", is a group of Igloo
replicas with its own machines, size and scale-to-zero settings. Every pool
shares the release's catalog, metadata database and warehouse: the same
tables, users, grants and history. Give a client or a team its own pool
and its queries never compete with anyone else's for CPU or memory, its
compute stops when it is idle, and its cost is the cost of its own machines.

Each replica knows its pool from `IGLOO_POOL` (`default` if unset); the
history and `igloo.usage` record it with every statement, and nobody can
change it from SQL. Leave table maintenance to the `default` pool
(`IGLOO_MAINTENANCE=false` elsewhere, which the chart and the Compose
example set), so other pools run, and are billed, only for their own
statements.

[Workers](distributed-execution.md) belong to one pool too: the replicas
of a pool use that pool's workers and no other's, so one client's large
statements never take another's workers, and a pool's workers stop with it.
The chart gives every pool workers of its own while `workers.enabled` is
set (a pool can set `workers` to change them, or `workers.enabled: false`
to have none); in Docker Compose the example pool runs without workers. The
history records a distributed statement with the pool of the replica that
ran it, like any other; the workers' own machines are not in
`igloo.usage`'s costs yet.

### With the Helm chart

```yaml
# values.yaml
pricePerHour: 0.68              # the release's own replicas, the default pool
pools:
  - name: acme                  # Deployment and Service igloo-pool-acme
    replicaCount: 1
    pricePerHour: 2.72
    resources:
      requests: { cpu: "16", memory: 64Gi }
      limits: { memory: 64Gi }
    scaleToZero: { enabled: true, suspendAfter: 15m }
    nodeSelector: { node-pool: igloo-large }
  - name: reporting
    replicaCount: 2
    resources:
      requests: { cpu: "4", memory: 16Gi }
      limits: { memory: 16Gi }
```

Each pool gets a Deployment and a SQL Service named `<release>-pool-<name>`,
a metrics Service, NetworkPolicies, and with `scaleToZero.enabled` its own
gateway, which starts and stops only that pool. Clients connect to the
pool's Service (`igloo-pool-acme:5432` above); the release's own Service is
the `default` pool. A pool takes every value it does not set from the
release's values; `extraEnv` adds to the release's. With `workers.enabled`,
each pool also gets its own worker Deployment, `<release>-pool-<name>-workers`.
Pools have no HorizontalPodAutoscaler or PodDisruptionBudget of their own. See
[the chart's README](../deploy/helm/igloo/README.md#compute-pools).

### With Docker Compose

Run another `igloo` service from the same image against the same catalog,
metadata database and storage, with its own `IGLOO_POOL`, memory and port.
[`deploy/single-vm/compute-pool.yml`](../deploy/single-vm/compute-pool.yml)
is an example, a pool named `reporting` on port 5434:

```sh
cd deploy/single-vm
docker compose -f docker-compose.yml -f compute-pool.yml up -d
psql "host=127.0.0.1 port=5434 user=igloo dbname=igloo"
```

On one machine, pools share its CPU and memory; give each a memory limit
(`IGLOO_DUCKDB_MEMORY_LIMIT`) and threads (`IGLOO_DUCKDB_THREADS`) so that
together they fit. Pools on separate machines, each with its own
[scale-to-zero gateway](scale-to-zero.md), are isolated like the Helm ones.
