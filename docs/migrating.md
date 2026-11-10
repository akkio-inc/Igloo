# Migrating from Snowflake or Databricks

Before moving a workload, you want to know how much of it Igloo runs as it
is, what fails and why, whether the answers match, and what it would cost.
`python -m migrate report` finds out from the workload itself: it reads a
Snowflake account's or a Databricks workspace's query history, runs the
same queries on Igloo against Igloo's copies of the tables, re-runs a small
sample on the source to compare results, and writes a report.

```sh
pip install -r migrate/requirements.txt
export SNOWFLAKE_ACCOUNT=myorg-myaccount SNOWFLAKE_USER=IGLOO_MIGRATE \
  SNOWFLAKE_PRIVATE_KEY_FILE=~/.snowflake/igloo_migrate.p8 \
  SNOWFLAKE_ROLE=IGLOO_MIGRATE SNOWFLAKE_WAREHOUSE=IGLOO_MIGRATE_WH
export IGLOO_PASSWORD=...
python -m migrate report --source snowflake --since 30d \
  --igloo 'host=igloo.example.com port=5432 user=migrate dbname=igloo sslmode=require' \
  --map 'PROD.ANALYTICS.*=lake.analytics.*' \
  --out ~/igloo-migration
```

Then open `~/igloo-migration/report.html`. Run it from the repository's root,
with Python 3.12. It reads both sides and writes to neither
(see [what it runs](#what-it-runs)), and keeps query text out of the report
unless asked (see [privacy](#privacy)).

## What it does

1. **Reads the history** of the period (`--since`, `--until`), for the
   users, roles, warehouses and statement classes given, the costliest
   query groups first (`--max-groups`, 20,000 by default).
2. **Groups it into shapes.** A shape is one query with its literals taken
   out, reading the same tables: the dashboard query run 4,000 times with
   different dates is one shape. Snowflake's `query_parameterized_hash`
   groups executions; groups whose text is the same once comments,
   literals, case and the length of `IN` lists are set aside are merged,
   and executions that read different tables are kept apart, so the same
   text run in two schemas is two shapes. Each shape keeps its number of
   executions, its total and median time, the rows its last run returned,
   and a class: `SELECT`, `CTAS`, `INSERT`, `MERGE`, `UPDATE`, `DELETE`,
   `DDL` or `other`.
3. **Takes stock of the tables** the workload reads and writes, with their
   sizes at the source, and finds each in Igloo: a table of Igloo's
   catalog (mirrored), a table of a [connection](federation.md), or
   missing. A shape can only be replayed where every table it reads is in
   Igloo.
4. **Replays the queries** whose tables are all in Igloo, up to `--sample`
   (200 by default, the most source time first): first as written, with only
   their table names changed to Igloo's; if that fails, in Igloo's own
   dialect of the source's SQL where Igloo has one
   ([`SET sql_dialect`](snowflake-sql.md)), and
   translated to DuckDB's dialect by [sqlglot](https://github.com/tobymao/sqlglot).
   Each run is recorded with its outcome, error, rows and time. Writes are
   replayed only with `--replay-writes`, into a scratch schema.
5. **Compares results.** Every query that runs on Igloo is checked against
   the row count the history recorded for the same statement. Up to
   `--compare` queries (10 by default) are run again on the source, within
   `--source-budget`, and compared by row count and a checksum of their
   values.
6. **Writes the report**: `report.html`, one self-contained page;
   `report.md`, the summary; and `report.json`, everything.

## Where it finds your tables

Igloo's copies of your tables seldom have the source's names: Snowflake's
`PROD.ANALYTICS.ORDERS` may be `lake.analytics.orders`. A source table is
found in Igloo by the first of:

1. a `--map SOURCE=TARGET` rule. Names are dotted, with `*` standing for one
   whole part of the name, and each `*` of the target takes the part its
   counterpart matched: `--map 'PROD.ANALYTICS.*=lake.analytics.*'`,
   `--map 'PROD.*.*=lake.*.*'`, `--map 'PROD.RAW.EVENTS=lake.raw.events_2026'`.
   A two-part target is in Igloo's catalog (`lake`).
2. a table of Igloo's catalog with the same schema and name;
3. a table of an Igloo connection named like the source database, with the
   same schema and name.

Names match without regard to case. Statements are rewritten to use the
Igloo names; unqualified names are left as written and resolve through
`search_path`, set to the Igloo schema of the statement's session schema.

A [Snowflake connection](federation.md#snowflake) reads one Snowflake
database in place: for a connection `wh` to the database `ANALYTICS`, map
`--map 'ANALYTICS.*.*=wh.*.*'`, or name the connection like the database.
Queries replayed through it read from Snowflake on the connection's
warehouse, which bills Snowflake credits that `--source-budget` does not
count; copy the tables Igloo will serve, and use the connection to cover
the rest.

Databricks' history does not record a statement's session catalog and
schema, so unqualified names there resolve in `--default-namespace
catalog.schema`, or the warehouse's defaults.

## What it reads

### Snowflake

| View | For | Without it |
|---|---|---|
| `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` | The history, summed per query group on the server | `INFORMATION_SCHEMA.QUERY_HISTORY()`: the last 7 days only, only the statements the role may see (its own, and those on warehouses it has `MONITOR` on), read a day at a time |
| `SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY` (Enterprise edition) | The tables and views each query read and wrote | The tables are worked out by parsing the statements |
| `SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY` | Credits per query | Credits are estimated from execution time and warehouse size |
| `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY` | Credits by warehouse over the period | `INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY()`, which needs `MONITOR USAGE`; else no spend |
| `<database>.INFORMATION_SCHEMA.TABLES` | Rows, bytes and last change of each table the workload names | No sizes |
| `SHOW WAREHOUSES` | The size and state of the warehouse the tool runs on | |

`ACCOUNT_USAGE` lags behind by up to 45 minutes. The history's row count is
`ROWS_WRITTEN_TO_RESULT`: `ROWS_PRODUCED` counts more than the result for
some queries (the rows of a scalar subquery) and none for queries answered
from metadata, such as `SELECT COUNT(*) FROM t`.

### Databricks

| Table | For | Without it |
|---|---|---|
| `system.query.history` | The history, summed per text on the server | The Query History API, which lists what the user may see. It may show `<REDACTED>` in place of the text, and such statements are counted but not replayed. |
| `system.billing.usage` | DBUs by warehouse over the period | No spend |
| `DESCRIBE DETAIL` of each table the workload names | Bytes and last change (Delta tables) | No sizes |

## Grants

Give the tool its own user and role on each side, with nothing but the
grants below. The tool runs only statements it has checked (see
[what it runs](#what-it-runs)); a role that can only read is what
guarantees that nothing it runs can write.

### Snowflake

As `ACCOUNTADMIN`, or a role with the privileges to grant these:

```sql
CREATE ROLE igloo_migrate;
CREATE WAREHOUSE igloo_migrate_wh WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 INITIALLY_SUSPENDED = TRUE;
GRANT USAGE ON WAREHOUSE igloo_migrate_wh TO ROLE igloo_migrate;

-- The history, access history, per-query credits and metering.
GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE igloo_migrate;

-- Table sizes and the comparison re-runs: read access to what the
-- workload reads, here every table and view of one database.
GRANT USAGE ON DATABASE prod TO ROLE igloo_migrate;
GRANT USAGE ON ALL SCHEMAS IN DATABASE prod TO ROLE igloo_migrate;
GRANT SELECT ON ALL TABLES IN DATABASE prod TO ROLE igloo_migrate;
GRANT SELECT ON ALL VIEWS IN DATABASE prod TO ROLE igloo_migrate;

CREATE USER igloo_migrate DEFAULT_ROLE = igloo_migrate DEFAULT_WAREHOUSE = igloo_migrate_wh
  RSA_PUBLIC_KEY = '<the public key>';
GRANT ROLE igloo_migrate TO USER igloo_migrate;
```

`IMPORTED PRIVILEGES` opens every `ACCOUNT_USAGE` view. To grant less, use
the `SNOWFLAKE` database's roles instead, such as `GOVERNANCE_VIEWER` for
the query and access history and `USAGE_VIEWER` for metering (`GRANT
DATABASE ROLE snowflake.governance_viewer TO ROLE igloo_migrate;`);
Snowflake's documentation lists the views each one opens.
Without `ACCOUNT_USAGE` at all, `GRANT MONITOR ON WAREHOUSE <name>` lets
the role see the warehouse's statements in `INFORMATION_SCHEMA`, and
`GRANT MONITOR USAGE ON ACCOUNT` its metering.

Give the role nothing else: no `OWNERSHIP`, no `CREATE`, `INSERT`,
`UPDATE` or `DELETE`, and no `USAGE` on functions or procedures, so that
even a query that calls a user-defined function fails rather than runs it.
Re-runs without `SELECT` on a table fail, and the report says so.

### Databricks

As a metastore or account admin, for a service principal or user
`igloo-migrate` (enable the `query` and `billing` system schemas first if
they are not):

```sql
GRANT USE CATALOG ON CATALOG system TO `igloo-migrate`;
GRANT USE SCHEMA ON SCHEMA system.query TO `igloo-migrate`;
GRANT SELECT ON TABLE system.query.history TO `igloo-migrate`;
GRANT USE SCHEMA ON SCHEMA system.billing TO `igloo-migrate`;
GRANT SELECT ON TABLE system.billing.usage TO `igloo-migrate`;

-- Table sizes and the comparison re-runs: read access to what the
-- workload reads.
GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG main TO `igloo-migrate`;
```

and the *Can use* permission on the SQL warehouse it runs on. No
`MODIFY`, `CREATE` or `EXECUTE`.

### Igloo

A user that can read Igloo's copies of the tables, and with
`--replay-writes`, write a scratch schema that an admin creates:

```sql
CREATE USER migrate WITH PASSWORD 'a-long-password' ROLE none;
GRANT SELECT ON SCHEMA analytics TO migrate;   -- each schema the workload reads
GRANT SELECT ON CONNECTION crm TO migrate;     -- each connection it reads through

-- for --replay-writes only
CREATE SCHEMA igloo_migrate_scratch;
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE ON SCHEMA igloo_migrate_scratch TO migrate;
```

Igloo refuses this user any write outside the scratch schema.

## What it runs

**On the source**, only its own read-only statements: the history, access
history, metering and table sizes above, and `SHOW WAREHOUSES` (Snowflake)
or the warehouse's settings (Databricks). Then the comparison re-runs: up to
`--compare` queries from the history, each run exactly as recorded, in its
session's database and schema (`USE SCHEMA`), and only if it passes these
checks:

- it is a single query: `SELECT`, `WITH ... SELECT` or a set operation of
  them, as sqlglot parses it, with no `INTO`, nothing that writes, sets or
  grants anywhere inside it;
- it calls none of the functions that change something: Snowflake's
  `SYSTEM$...`, a sequence's `NEXTVAL`, Databricks' `http_request` and the
  `ai_...` functions;
- it returned at most `--max-compare-rows` rows (100,000) when it last ran.

Every statement the tool sends starts with `/* igloo-migrate */`, and its
Snowflake sessions carry the query tag `igloo-migrate`; extraction leaves
these out of the workload. Every statement has a timeout, `--timeout`
seconds (120): Snowflake's `STATEMENT_TIMEOUT_IN_SECONDS` and Databricks'
`STATEMENT_TIMEOUT`, as well as the client's own. The tool never creates,
resizes, resumes or suspends a warehouse itself; set `SNOWFLAKE_WAREHOUSE`
(or `--source-warehouse`) to a small one, since without it Snowflake would
run on the user's default warehouse.

**What the source side costs.** The tool estimates its own use: the seconds
its statements ran at the warehouse's rate (from `bench/pricing.toml`), at
least Snowflake's 60-second minimum if it resumed the warehouse, plus the
warehouse's idle time before it suspends (`AUTO_SUSPEND`, Databricks'
auto-stop). The comparisons may cost at most `--source-budget`: 0.1
credits on Snowflake, 0.5 DBUs on Databricks, by default. Each re-run may
run only as long as what is left of the budget pays for, which is its
timeout, and is skipped if that is less than the time it took when it was
recorded; failed and canceled re-runs count too. Where the warehouse's size
is not in the pricing file, nothing is re-run. `--compare 0` re-runs
nothing. The report records the estimate for extraction and for
the comparisons separately. On an X-Small Snowflake warehouse, reading 30
days of `ACCOUNT_USAGE` history (1.2 million statements, with access
history and per-query credits) took about two minutes, and a whole run that
also re-ran 25 small queries came to an estimated 0.08 credits.

**On Igloo**: `igloo.tables` and `information_schema`, `SET
statement_timeout`, `SET query_tag = 'igloo-migrate'` (so the replays are
easy to find in [`igloo.query_history`](sql.md#query-history)), `SET
search_path`, and the replays. A query runs only if it passes the same
single-query check, or, where sqlglot cannot parse it, starts as a query
does and contains no word that writes. The source's functions that change
something need no check there: Igloo has none of them, and the replay
fails naming the missing function. Queries whose last run returned more
than `--max-compare-rows` rows are counted on Igloo (`SELECT count(*)
FROM (...)`) rather than fetched.

With `--replay-writes`, each `INSERT`, `MERGE`, `UPDATE` and `DELETE`
writes to a zero-copy [clone](sql.md#clones) of its target in the scratch
schema (`--scratch-schema`, `igloo_migrate_scratch`), and each `CTAS` to a
new table there. The tool checks, from the rewritten statement, that its
only target is in the scratch schema before running it, and drops the
scratch table afterwards unless `--keep-scratch` is given. `DDL` and
`other` statements (`SHOW`, `GRANT`, `CALL`, `COPY`, session settings) are
never replayed.

## Privacy

Nothing leaves the machine the tool runs on except its statements to the
source and to Igloo. Everything it writes goes under `--out`, which may not
be inside the repository:

| File | Holds |
|---|---|
| `extract/groups.jsonl`, `extract/meta.json` | The history as read: statement text with its literals, user and role names, timings |
| `replay.jsonl` | The statements run on Igloo, with their literals, and their errors |
| `report.json`, `report.md`, `report.html` | The report |

In the report, statements have their literals replaced by `?` and their
comments removed, error messages lose the values they quote and the excerpt
of the statement that follows them, and users appear only as counts.
`--show-literals` keeps the literals. Query results are never written: they
are digested as they arrive, and small ones (up to 2,000 rows) are held in
memory only to compare them.

To share a report outside your organization, send `report.html` or
`report.md`, not the extract or `replay.jsonl`; `extract` and the replays
can be rerun from a saved extract (`--from-extract`, `--no-source`)
without reading the history again.

## Reading the report

**How far the workload gets.** Every shape has one outcome, shown as shares
of shapes, executions and source time:

| Outcome | Meaning |
|---|---|
| Run unchanged | Ran on Igloo as written, apart from its table names |
| Run after translation | Ran in Igloo's dialect of the source's SQL, or after sqlglot's translation |
| Fail on Igloo | Its tables are in Igloo, and it failed every way it was tried |
| Lack data in Igloo | A table it reads, or the table it writes into, is not in Igloo |
| Read the source's own metadata | It reads `INFORMATION_SCHEMA`, `ACCOUNT_USAGE` or `system`, whose Igloo counterparts are its own [catalog views](catalog.md) |
| Failed on the source too | No execution of it succeeded at the source |
| Not replayed | `DDL` and `other`, writes without `--replay-writes`, statements that fail the read-only check, or whose text the source hides |
| Not in the sample | Igloo has its data, but it was beyond `--sample` |

**Failures, by error class**: unknown function, unknown column, unknown
type, syntax, type mismatch, conversion, not supported, timeout and so on,
each with its most common messages (names and values taken out, except a
missing function's name) and example shapes. An unknown function is the
most common class for SQL written for another warehouse: the list says
which functions to add or translate.

**Translation** compares Igloo's own dialect with sqlglot's translation on
the shapes that did not run as written. A translation that sqlglot reports
as imperfect (an argument it dropped, a function it could not map) can
run and return something else; such shapes are counted, and are the ones
to compare.

**Results compared.** For a re-run on the source:

| Verdict | Meaning |
|---|---|
| match | Same row count and checksum: the same values, in any order. Integers and decimals are exact; where one side's value is binary floating point (a double), numbers agree to 10 significant digits |
| match within precision | The checksums differ, but row by row the values agree to the precision of the less precise side: a decimal to its decimal places (Snowflake's `AVG` of a `NUMBER(12,2)` has 8; DuckDB's is a double), binary floating point also to one part in a billion. Integers, and decimals with the same decimal places, must be equal |
| values differ | Same row count, different values; the report names the columns |
| rows differ | Different row counts |
| not compared | It was not re-run (over budget, too many rows, refused by the check) or failed at the source |

Against the history, only row counts are compared (`rows match`, `rows
differ`). A difference is marked *explained*, and left out of the match
rate, where it need not be Igloo's: the query depends on the current time,
on chance or on the session (`CURRENT_USER`, `CURRENT_WAREHOUSE`), a
`LIMIT` picks among ties, Igloo's copy has a different row count from the
source's table, or the table changed at the source after Igloo's snapshot
or after the recorded run. Compare against copies taken shortly before the
run: the tables section shows each copy's snapshot time.

**Timing**: the source's median elapsed time for each shape, as the history
records it (queuing and compilation included, on the warehouse sizes
shown), against one run on Igloo, timed at the client and including
fetching the result. Run the tool near the Igloo it measures, and read the
figures as a first look, not a benchmark.

**Tables**: the tables and views the workload names, most executed first,
with their rows and size at the source, whether Igloo has them (mirrored,
through a connection, missing), Igloo's copy and its snapshot time, and
notes where its row count differs or the source changed after it.

**Cost**, every figure labeled measured or estimated:

- the source's spend over the period: credits by warehouse (Snowflake) or
  DBUs (Databricks), measured, and in dollars at the list price in
  `bench/pricing.toml`;
- the busy time of the shapes that ran on Igloo: their execution time at
  the source (measured) against their replay time on Igloo times their
  executions (estimated);
- what that busy time cost: the credits the source attributed to those
  executions (measured, from `QUERY_ATTRIBUTION_HISTORY`) or their
  execution time at the warehouse size's rate (estimated), against Igloo's
  busy time at `--igloo-machine`'s hourly price (estimated);
- Igloo always on for the period (estimated).

Prices are list prices; set yours with `--set snowflake.price_per_credit=2.4`,
`--set databricks.price_per_dbu=0.55` or `--igloo-price-per-hour`. The
source's spend includes idle time and work outside the extracted
workload, so compare like with like: busy time with busy time.

## Options

| Option | Default | Meaning |
|---|---|---|
| `--source` | (required) | `snowflake` or `databricks` |
| `--since`, `--until` | `30d`, now | The period: `30d`, `12h`, `90m`, or an ISO time |
| `--users`, `--roles`, `--warehouses` | all | Comma-separated lists to keep (roles: Snowflake only; Databricks warehouses by ID) |
| `--statement-types` | all | Classes to keep: `SELECT`, `CTAS`, `INSERT`, `MERGE`, `UPDATE`, `DELETE`, `DDL`, `other` |
| `--max-groups` | 20000 | Query groups to read, the costliest first |
| `--igloo` | (required) | A libpq connection string; the password from `IGLOO_PASSWORD` or `PGPASSWORD` |
| `--map SOURCE=TARGET` | | Where a source table is in Igloo (repeatable) |
| `--default-namespace` | the warehouse's | Databricks: the catalog and schema unqualified names resolve in |
| `--sample`, `--sample-by` | 200, `time` | How many shapes to replay, and which first: most source `time`, most `executions`, or `random` (`--seed`) |
| `--compare`, `--source-budget` | 10; 0.1 credits or 0.5 DBUs | Re-runs on the source, and the most they may cost |
| `--max-compare-rows` | 100000 | Results compared up to this size; larger ones are counted |
| `--timeout` | 120 | Seconds any one statement may run, on either side |
| `--replay-writes`, `--scratch-schema`, `--keep-scratch` | off, `igloo_migrate_scratch` | Replay writes into the scratch schema |
| `--no-native-dialect` | | Do not try Igloo's own dialect of the source's SQL |
| `--igloo-machine`, `--igloo-price-per-hour` | `c7g.4xlarge` | The machine to price Igloo's time on |
| `--pricing`, `--set` | `bench/pricing.toml` | Prices, and overrides |
| `--show-literals` | | Keep literal values in the report |
| `--source-warehouse` | `SNOWFLAKE_WAREHOUSE`, `DATABRICKS_WAREHOUSE_ID` | The warehouse the tool runs on |
| `--from-extract`, `--no-source` | | Use a saved extract; with `--no-source`, do not connect to the source at all |

`python -m migrate extract` takes the history options and `--out`, and only
reads the history. Connection settings come from the environment:
`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`,
`SNOWFLAKE_PRIVATE_KEY_FILE` (or `SNOWFLAKE_TOKEN_FILE` or
`SNOWFLAKE_PASSWORD`), `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`; and the
Databricks SDK's (`DATABRICKS_HOST` with `DATABRICKS_TOKEN`, or a profile
named by `DATABRICKS_CONFIG_PROFILE`) with `DATABRICKS_WAREHOUSE_ID` or
`DATABRICKS_HTTP_PATH`.
