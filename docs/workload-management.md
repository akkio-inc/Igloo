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
| `queued_ms` | Time it waited for its [role limits](#role-limits) and [the replica's](#the-replicas-limit) before starting |
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
ALTER ROLE analysts SET statement_queued_timeout = '5min';
ALTER ROLE analysts SET memory_limit = '8GB';
ALTER ROLE reader SET statement_timeout = '1h';      -- every user with the built-in role reader
ALTER USER etl_service SET statement_timeout = 0;    -- no limit for this user
ALTER ROLE analysts RESET memory_limit;              -- or RESET ALL, or SET memory_limit = DEFAULT
SHOW ROLE SETTINGS;                                  -- admins see all; others, those that apply to them
```

| Setting | Effect |
|---|---|
| `statement_timeout` | The longest a statement may run, once it has started: the time it waited to start does not count, as in Snowflake. A session's own `SET statement_timeout` can be shorter, not longer. `0` means no limit. |
| `statement_queued_timeout` | The longest a statement may wait to start, behind the limits below and [the replica's](#the-replicas-limit) (Snowflake's `STATEMENT_QUEUED_TIMEOUT_IN_SECONDS`). A session's own `SET statement_queued_timeout` can be shorter, not longer. `0`, the default, means no limit. |
| `max_concurrent_statements` | How many statements may run at once on a replica. On a user, it counts that user's statements; on a custom or built-in role, all its members' together. Further statements wait in line. |
| `memory_limit` | The memory each statement reserves out of the replica's query memory (DuckDB's memory limit) before it starts. A statement whose reservation does not fit next to those already running waits in line. |
| `query_tag` | The default [query tag](#query-tags) |
| `sql_dialect` | The SQL dialect sessions start in, `duckdb` or `snowflake` ([Snowflake SQL](snowflake-sql.md)) |

Which setting applies to a user: their own, if they have one; otherwise
their custom roles' (where several set it, the most generous: the longest
timeout, the most statements, the smallest reservation; for the tag, the
first role by name, and so for the dialect); otherwise their built-in
role's. A concurrency limit
counts against the user or role it came from.

How limits are enforced:

- **In line, in order.** Statements that must wait do so in arrival order.
  A statement never overtakes an earlier one that waits for the same slots,
  nor, if it reserves memory, one that waits for memory, so a large
  reservation is not starved by small ones. Statements of other users and
  roles are not held up.
- **Until a slot frees, the queued timeout, or a cancel.** A waiting
  statement's `statement_timeout` has not started, and clients can cancel
  it. When its `statement_queued_timeout` runs out while it waits, it fails
  with SQLSTATE `57014` and a message that says what it waited behind, such
  as `canceling statement due to statement queued timeout: it waited 30s to
  start, behind the limit of 4 statements at once for role analysts`. The
  history records the time waited as `queued_ms`.
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

### Heavy statements beside short ones

With workers configured ([distributed
execution](distributed-execution.md)), nothing needs setting for a heavy
statement not to take the replica's cores from short ones: auto mode runs a
heavy statement on workers when other statements run on the replica beside
it, as it weighs the replica's time at as many times as long as the
statements sharing it, up to four; and the guard moves a heavy statement
that runs on the replica to workers once it has run its estimate while
others run beside it, rather than three times it (see [beside other
statements](distributed-execution.md#when)). `IGLOO_WORKER_THRESHOLD`
sets how long a statement must take for workers to be worth it.

Without workers, nothing needs setting either, on Linux. DuckDB runs every
statement on one pool of threads, and a thread stays with a statement's
pipeline until that ends, so beside one session's long reports a dashboard's
statement had little more than its own thread. The operating system can
prefer one thread to another, though, so a heavy statement runs in a second
DuckDB of the replica's process, opened at the first heavy statement, whose
threads give way to the first's (a nice value of 10). Alone it takes about
what it takes in the first; beside others it gets the cores they leave idle,
and about a tenth of those they use.

The two share the memory limit. A heavy statement alone may use it all, the
first's caches giving way to it. While the first reads tables it keeps what
it holds, up to three quarters of the limit, for its statements not to read
their files again, and the heavy statement has what is left and spills.

A statement is heavy if the last statement of its shape, its text with the
literals masked, used 5 seconds of all the replica's cores
(`cpu_time_ms` in the [history](#resources-in-the-history)). A read not seen
before runs in the first DuckDB; if it runs long enough to be heavy beside
the others running, 5 seconds for each statement sharing the cores, within
its first minute and before it sends the client a row, it starts again in
the second. A statement alone stays where it is. A write (`INSERT`,
`UPDATE`, `DELETE`, `MERGE`, the fill of `CREATE TABLE ... AS`) never starts
again, so runs in the second from the first run after one that was heavy,
and commits as another replica's would.

Only what gives the same result there runs there: a statement outside a
transaction block, of what the catalog holds, on which no masking or row
access policy applies, in a session with no temporary tables or views and
none of the settings tasks do not take over, calling no function tied to the
session, and not one that fills ID domains' ids; a statement the second
DuckDB cannot bind, such as one of a connection's tables or a view not yet
copied there, runs in the first. Materialized views' refreshes run in the
first. `placement` in the history says which reads ran there, and
`igloo_background_statements_total` counts them.
`IGLOO_HEAVY_IN_BACKGROUND=false` runs everything in the first. Without
workers, the replica's limit below and roles' limits hold back heavy
statements beyond that.

### The replica's limit

`IGLOO_MAX_CONCURRENT_STATEMENTS` limits the queries and changes of data
running at once on a replica, whoever runs them, as a Snowflake warehouse's
`MAX_CONCURRENCY_LEVEL` does; further ones wait in line, in arrival order,
as for the limits above. A number sets it, `auto` makes it the replica's
cores, and `off`, the default, means no limit.

DuckDB runs every statement on all of its threads, so a hundred statements
at once each get a hundredth of the replica: none finishes before the
others are nearly done, and together they hold the memory of all of them,
and spill. In line, the first ones finish in the time they take alone.
`SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `COPY` and `CREATE TABLE
... AS` count; settings, transaction control, `SHOW`, catalog listings and
other DDL do not, and neither does a result the [result
cache](operations.md#result-cache) answers. A statement waiting for this
limit says so: `behind the replica's limit of 8 statements at once`.

### Reservations from history

A replica's statements share its memory, so heavy ones that start together
spill to disk, and on a busy disk take far longer than one after the other;
a fixed limit, though, holds back the light statements with the heavy ones.
Igloo instead learns what each statement holds at once, by its shape (its
text with string and number literals masked, which a dashboard sends again
with other audiences, dates and limits), and the next statement of that
shape reserves `IGLOO_STATEMENT_RESERVATIONS` of it (`auto`, the default,
is all of it) of the replica's memory before it starts, as a role's
`memory_limit` reserves (see [role limits](#role-limits)). A statement whose
shape held more than the replica's memory waits until the others'
reservations are released; one whose reservation would be under a
sixteenth of the memory, or whose shape Igloo has not seen since the
replica started, reserves nothing and starts at once. A role's own
`memory_limit` comes first.

DuckDB measures what a statement allocated over its whole run, counting
memory it freed and allocated again, and the peak of the whole process,
not of one statement. So the replica reads what DuckDB holds for running
statements (their hash tables, sorts and the like, not its caches) every
100 ms, and charges each statement running then with what the statements
beside it had not reserved. A statement's shape learns the smaller of that
peak and what it allocated, or, for a statement too short for a reading,
what it allocated. A run that ends early, cancelled or out of memory,
raises its shape's footprint to what it was seen holding, so that heavy
statements cancelled at their timeout do not start together again, unseen,
next time. When it starts, a replica reads what the past week's statements
held from the [query history](sql.md#query-history), those that ran on a
replica with its memory limit, within a quarter, so that a restart does
not forget them; a run from before the history kept peaks counts by what it
allocated, and one cancelled after ten seconds or more as needing all the
memory it ran with.

On a dashboard's burst of queries over billions of rows, on 8 cores and
22 GB, the build took 548 to 574 seconds with 8 statements running at once
and 365 with 2, and takes 306 to 310 with reservations, its median query 21
to 22 seconds rather than 32 to 51; reserving half of what a shape allocated
took 314 to 353, and a quarter 367 to 371. The first time a dashboard runs
on a replica, its statements have no history and reserve nothing: its first
build took 558 to 590 seconds; after a restart, with what the history kept,
301. `off` turns the reservations off. Shapes are kept for the replica's life, up
to 4,096 of them, and statements that ran on [workers](distributed-execution.md)
are not learned from, since most of what they allocated was the workers'.
A shape that ran to its end on the replica also tells auto mode that the
replica is not short of memory for it (see [when](distributed-execution.md#when));
one whose latest run was cut short, as by the guard or a timeout, holding
more than it held to its end before, does not.

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
to have none), each pool's with a secret of its own; in Docker Compose the
example pool runs without workers. The
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
each pool also gets its own worker Deployment, `<release>-pool-<name>-workers`,
and with `workers.large.enabled` its own [large
worker](distributed-execution.md#a-large-worker),
`<release>-pool-<name>-workers-large`; a pool sets `workers.large.enabled:
false` to run without one.
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

### Sizing a pool

Machines are billed by the second, so a statement costs its work times
the machine's price whatever the machine's size, as long as it uses the
cores; a larger machine finishes the same work sooner and can stop sooner.
What a larger machine changes is what does not fit: statements that spill
to disk on a small one take many times their work. Size each pool to the
heaviest work it gets at once, not to its average:

| Pool's work | Machine | Why |
| --- | --- | --- |
| Dashboards: many statements of seconds each, together | Many cores, 4 GB a core or more, local NVMe (`m7gd`) | A dashboard's statements run at once; on too few cores they wait for each other's memory |
| dbt and other large transformations | Memory first (`r7gd`) | Joins and sorts of whole tables spill less |
| Ad hoc analysis, a few users | Small (`m7gd.xlarge` to `2xlarge`) with [scale to zero](scale-to-zero.md) | Idle most of the day |

A dashboard's burst of queries over billions of rows, all at once, took 300
to 310 s on 8 cores and 32 GB (`m7gd.2xlarge`, $0.43 an hour: $0.04 a run)
and 138 s on 32 cores and 128 GB (`m7gd.8xlarge`, $1.71 an hour: $0.07),
against 171 s on a Snowflake X-Small warehouse ($0.10 to $0.14 a run at $2
to $3 a credit). With [scale to zero](scale-to-zero.md) the larger
machine's idle time is what its suspend setting keeps it up for.
