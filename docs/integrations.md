# Tools and integrations

Igloo speaks the PostgreSQL wire protocol, so the tools an analytics team
already uses connect with their PostgreSQL drivers and adapters: dbt, BI
tools, notebooks. The protocol is PostgreSQL's; the SQL a tool then sends
runs as DuckDB's (or Snowflake's), so a tool works when its SQL does, which
the tests below check for each one. This page says how to set each tool up,
what each test runs, and what to expect. [SQL and clients](sql.md#clients)
lists every tested driver, and [what differs from PostgreSQL](sql.md#differences-from-postgresql).

| Tool | Connects with | What the test runs | Test |
|---|---|---|---|
| dbt Core 1.12 | dbt-postgres 1.11 | dbt itself, on a project of seeds, models, snapshots and tests | `make e2e` |
| Jupyter, scripts | SQLAlchemy 2.1 with psycopg 3, pandas 3, polars 2 (SQLAlchemy, connectorx, ADBC) | Those libraries, in a script | `make e2e` |
| Tableau, Looker, DBeaver | PostgreSQL JDBC 42.7 | The JDBC driver, making the metadata calls these tools make and running SQL shaped like theirs. The tools themselves do not run. | `make e2e` |
| Metabase 0.64 | its PostgreSQL driver | Metabase itself, through its API: schema sync, and questions written by hand and built in the editor | `make bi-e2e` |
| Apache Superset 6.1 | SQLAlchemy with psycopg2 | Superset itself, through its API: datasets, time-series charts and SQL Lab | `make bi-e2e` |
| Power BI | Npgsql 4.0 (bundled with Power BI Desktop) and 10.0 | Npgsql, replaying Power BI's type loading, navigator queries and a report's query. Power BI itself does not run. | `make bi-e2e` |

`make e2e` (`scripts/e2e.sh`) runs in CI on pull requests and before each
release, which publishes nothing unless it passes, and by hand. It does not
run on pushes to `main`. `make bi-e2e` (`scripts/bi-e2e.sh`) runs weekly on
GitHub Actions and on demand; it pulls about 3 GB of images.

## Connecting

Every tool needs the same five things:

| Setting | Value |
|---|---|
| Host and port | Igloo's address, port `5432` (`5433` with the single-machine stack) |
| Database | Any name; Igloo ignores it. dbt is the exception, see below. |
| User and password | An Igloo user (see [users and roles](operations.md#users-and-roles)) |
| SSL | `require` once [TLS](operations.md#tls) is on |

Give each tool its own user with the least role it needs:

- **BI tools and notebooks that only read**: the `reader` role, or `none`
  with `GRANT SELECT ON SCHEMA marts TO ...` to limit them to some schemas.
- **dbt and ingestion**: the `writer` role. dbt's incremental models use
  temporary tables, which grants do not cover, so a user with role `none`
  cannot run them.
- Grant reporting users access to whole schemas (`GRANT SELECT ON SCHEMA
  analytics TO reporting`): the grant covers tables dbt creates later, and
  replacing a table keeps its grants.

## dbt

Use dbt Core with the standard **dbt-postgres** adapter:

```sh
pip install dbt-core==1.12.6 dbt-postgres==1.11.0
```

```yaml
# ~/.dbt/profiles.yml
agency:
  target: prod
  outputs:
    prod:
      type: postgres
      host: igloo.example.com
      port: 5432
      user: dbt
      password: "{{ env_var('IGLOO_DBT_PASSWORD') }}"
      dbname: lake        # the Iceberg catalog's name, IGLOO_CATALOG_NAME
      schema: analytics
      threads: 1          # see below
      sslmode: require
```

`dbname` must be the catalog's name in SQL, `lake` unless
`IGLOO_CATALOG_NAME` says otherwise: dbt-postgres qualifies every relation
with it, as in `"lake"."analytics"."fct_spend"`.

What works, as `scripts/clients/dbt` exercises it in `make e2e`:

- `dbt seed`, `run`, `test`, `build`, `snapshot`, `show`, `docs generate`
  and `source freshness`.
- Materializations `view`, `table` and `incremental` with the `append`,
  `merge`, `delete+insert` and `microbatch` strategies, `--full-refresh`,
  and snapshots (the test uses the `timestamp` strategy).
- **Incremental runs write only what changed.** `merge`, `delete+insert`
  and `microbatch` delete the rows they replace with Iceberg delete files
  and add the new rows in new files; `append` adds files. No data file is
  rewritten: after the test's second day, the snapshots each run committed
  hold only the rows it changed or added. Maintenance compacts the small
  files later, leaving a table alone while a run's transaction uses it, so
  that it does not fail the run's `COMMIT` (see
  [operations](operations.md#table-maintenance)). dbt-postgres's
  `microbatch` needs a `unique_key` (it merges each batch), and an
  `event_time` on the models it reads, so each batch reads only its days;
  run a range of days with `--event-time-start` and `--event-time-end`.
- Generic and singular tests, `persist_docs` for tables, views and their
  columns (stored as Iceberg comments, shown by `docs generate` and BI
  tools), schemas per model (`+schema`).

What to know:

- **Transactions.** dbt builds each model in a transaction block. In a
  block, a statement that creates, renames or drops a table, view or
  schema commits the block first, as DDL does in Snowflake (see
  [SQL and clients](sql.md#transactions)). A model is replaced the way
  `CREATE OR REPLACE TABLE` replaces a table: the new table is built under a
  temporary name first, so a failure leaves the old one; the renames that
  swap it in are quick, but a query in that instant can find no table.
- **One thread.** Keep `threads: 1`, dbt's default. With several, dbt
  renames and drops tables in one schema at once, and DuckDB-Iceberg, which
  keeps one list of each schema's tables for all sessions, can fall out of
  step with the catalog: a `__dbt_backup` table can be left behind, failing
  the model's next run, or a run can stop responding. Igloo makes up for
  the errors it can detect, not for these.
- **Not supported:** `materialized_view`, `indexes` and `unlogged`
  configurations (Iceberg tables have no indexes), and dbt's `grants`
  configuration, since only admins grant; use schema grants instead.
- dbt v2 (the dbt Fusion engine) is not tested yet.

## BI tools

### Metabase

*Admin → Databases → Add database → PostgreSQL*. Database name: any
(`igloo`). Metabase syncs schemas, tables, views and their comments, and
its question builder's queries (date groupings, binning, filters) run as
written. Keep the default sync schedule; a sync reads every table's columns
once.

### Apache Superset

*Settings → Database connections → + Database → PostgreSQL*, or the
SQLAlchemy URI `postgresql+psycopg2://reporting:...@igloo.example.com:5432/igloo`.
Datasets, time-series charts with time grains, and SQL Lab work.

### Power BI

*Get data → PostgreSQL database*. Server: `igloo.example.com:5432`,
database: `igloo`. Power BI folds filters and aggregations into SQL that
Igloo runs. Its connector's driver, Npgsql, runs in the tests with Power BI's
type loading, navigator queries and a report's query replayed; Power BI
itself is not part of them. Power BI encrypts the connection
by default, so turn on [TLS](operations.md#tls), or clear *Encrypt
connection* only on a private network. Power BI reads `numeric` columns as
fixed decimals with their precision and scale.

### Tableau

*Connect → PostgreSQL*, with *Require SSL* once TLS is on. Tableau uses the
PostgreSQL JDBC driver: it lists schemas and tables through the driver's
metadata calls, describes custom SQL with `LIMIT 0`, and generates
`DATE_TRUNC`, `CASE`, window functions and percentiles. The tests make
those calls and run SQL shaped like Tableau's through the driver; Tableau
itself is not part of them.

### Looker

Dialect *PostgreSQL 9.5+*. SQL shaped like Looker's (CTEs, `TO_CHAR` of
truncated dates, window functions) runs in the tests through the JDBC driver;
Looker itself is not part of them. For persistent derived tables,
give Looker's user `CREATE`, `SELECT`, `INSERT` and `DELETE` on a scratch
schema and set it as the connection's temp database.

### DBeaver and other SQL clients

Use the PostgreSQL connection type. They browse schemas through the same
JDBC metadata calls as Tableau; they are not part of the test suite.

## Notebooks and Python

```python
import sqlalchemy as sa, pandas as pd, polars as pl

engine = sa.create_engine("postgresql+psycopg://ana:...@igloo.example.com:5432/igloo")
df = pd.read_sql("SELECT * FROM analytics.fct_campaign_daily", engine)
df.to_sql("uploads", engine, schema="sandbox", if_exists="replace", index=False)

uri = "postgresql://ana:...@igloo.example.com:5432/igloo"
frame = pl.read_database_uri("SELECT * FROM analytics.fct_campaign_daily", uri)  # connectorx
frame = pl.read_database_uri("SELECT * FROM analytics.fct_campaign_daily", uri, engine="adbc")
```

SQLAlchemy's inspector (`get_schema_names`, `get_table_names`,
`get_view_names`, `get_columns` with comments, `get_pk_constraint`,
`MetaData.reflect`) works, so do Ibis and the
[psycopg features](sql.md#clients) notebooks use.

connectorx and ADBC (polars' `read_database_uri`, `pandas.read_sql` with
an ADBC connection) fetch with `COPY (query) TO STDOUT` in PostgreSQL's
binary format and build Arrow columns, without a Python object per value,
which keeps a notebook's memory down. On one machine, pulling 2 million rows
of five columns (integers, a date, a decimal and a string) into a client in
a container next to Igloo took:

| Path | Rows per second |
|---|---|
| psycopg 3, `fetchall` (text) | about 440,000 |
| psycopg 3, a server-side cursor | about 420,000 |
| polars `read_database_uri` (connectorx) | about 290,000 |
| polars `read_database_uri` (ADBC) | about 250,000 |
| polars `read_database` through SQLAlchemy | about 215,000 |
| pandas `read_sql` through SQLAlchemy | about 150,000 |

`scripts/e2e.sh` prints these rates for TPC-H `lineitem` on every run.
Every path is bound by the PostgreSQL protocol, which encodes each value
on its own: Igloo sends a few hundred thousand rows a second per session,
far less than the query itself produces. For large extracts into pandas or
polars, use [Arrow Flight SQL](flight-sql.md) through ADBC's Flight SQL
driver instead, which sends DuckDB's Arrow batches as they are. To export
tens of millions of rows, `COPY (query) TO 's3://bucket/export.parquet'`
writes Parquet from Igloo directly, much faster (see
[loading and exporting](sql.md#loading-and-exporting-data)).

## Known limitations

- `COPY ... FROM STDIN` is not supported yet, so `psql`'s `\copy ... from`,
  `pandas.to_sql(method=...)` with COPY, and ADBC's bulk ingestion fail;
  `INSERT`, `to_sql` and `write_database` work. Load files with `COPY ...
  FROM` an allowed location instead.
- `ROLLBACK TO SAVEPOINT` works only when nothing changed since the
  savepoint (enough for psycopg's and SQLAlchemy's nested blocks that just
  read); otherwise roll back the whole transaction.
- Catalog metadata (`information_schema.columns`, `pg_attribute`, and so
  JDBC's `getColumns` and SQLAlchemy's `get_columns`) shows every column of
  an Iceberg table as nullable, although writes enforce `NOT NULL`.
- Primary and foreign keys are not reported: Iceberg does not keep them, so
  BI tools' automatic joins and Power BI's relationship detection have
  nothing to go on. Define relationships in the tool.
- `TO_CHAR` formats dates and timestamps (`YYYY`, `MM`, `DD`, `HH24`, `MI`,
  `SS`, `Month`, `Mon`, `Day`, `Dy` and the like), not numbers.
- The SQL is DuckDB's: PostgreSQL-only functions a tool's custom SQL uses
  may need rewriting (see [the dialect](sql.md#dialect)).
