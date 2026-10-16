# SQL and clients

## Dialect

Igloo speaks the PostgreSQL wire protocol, but queries run on DuckDB, so the
SQL dialect is [DuckDB's](https://duckdb.org/docs/stable/sql/introduction),
which is close to PostgreSQL's. Everyday SQL (joins, window functions, CTEs,
aggregates, `GROUP BY ALL`, `QUALIFY`) works as written; DuckDB's function
library applies.

Tables live in the Iceberg catalog, named `lake` in SQL (`IGLOO_CATALOG_NAME`),
and the default schema is `public`. Unqualified names resolve to
`lake.public`, so `events` and `lake.public.events` are the same table.

## Tables

| Statement | Supported |
|---|---|
| `CREATE TABLE`, `CREATE TABLE AS SELECT` | Yes |
| `INSERT`, `INSERT ... SELECT`, `INSERT ... RETURNING`, `MERGE INTO` | Yes |
| `UPDATE`, `DELETE` | Yes |
| `DROP TABLE`, `CREATE SCHEMA`, `DROP SCHEMA` | Yes |
| `ALTER TABLE ... RENAME TO`, `ADD COLUMN`, `DROP COLUMN` | Yes |
| `CREATE OR REPLACE TABLE` | Yes, outside transactions; see below |
| `CREATE [OR REPLACE] VIEW`, `DROP VIEW` | Yes, outside transactions; see [views](#views) |
| `COMMENT ON TABLE`, `VIEW`, `COLUMN` | Yes; see [comments](#comments) |
| `CREATE TEMP TABLE`, `CREATE TEMP VIEW` | Yes; held in memory for the session only |
| `COPY INTO table FROM 'location'` | Yes, outside transactions; see [loading](loading.md#copy-into) |
| `CREATE [OR REPLACE] MATERIALIZED VIEW`, `REFRESH MATERIALIZED VIEW` | Yes, outside transactions; see [materialized views](loading.md#materialized-views) |
| `CREATE TASK`, `CREATE PIPE` | Yes; see [tasks](loading.md#tasks) and [pipes](loading.md#pipes) |
| `PRIMARY KEY`, `UNIQUE` | Accepted but not enforced |
| `NOT NULL` | Enforced |

Writes are transactional. `BEGIN ... COMMIT` groups statements; a failed
statement inside a transaction aborts it until `ROLLBACK`, as in PostgreSQL.
Statements that change the catalog itself, `CREATE OR REPLACE TABLE`, views
and comments, take effect at once and cannot run inside a transaction block.

`CREATE OR REPLACE TABLE` writes the new table under a temporary name, then
drops the old table and renames the new one into place. A failure while
writing leaves the old table as it was. The swap is quick but not atomic: a
query that starts in the instant between drop and rename finds no table.

## Views

Views are stored in the Iceberg catalog as Iceberg views with DuckDB SQL, so
every replica and restart sees them:

```sql
CREATE VIEW recent_orders AS
  SELECT * FROM orders WHERE o_orderdate > current_date - INTERVAL 30 DAY;
CREATE OR REPLACE VIEW order_totals (customer, total) AS
  SELECT o_custkey, sum(o_totalprice) FROM lake.public.orders GROUP BY 1;
DROP VIEW IF EXISTS recent_orders;
```

- Query a view by any name: `recent_orders`, `public.recent_orders` or
  `lake.public.recent_orders`. In listings (`information_schema`, `\dv`)
  views appear under the database `memory`, where each replica keeps its
  copy.
- Unqualified table names in a view resolve when the view is queried,
  against the querying session's search path, as in DuckDB. Qualify the
  tables of views that sessions with other search paths will read.
- A view created on one replica reaches the others within 30 seconds, or at
  once when a query there names it.
- Writers create and drop views, as do users with `CREATE` on the view's
  schema. Other engines see the views in the catalog but cannot run their
  DuckDB SQL.
- A view runs with the privileges of whoever queries it: reading one needs
  `SELECT` on the tables it reads (see [grants](#roles-and-grants)).

## Comments

```sql
COMMENT ON TABLE orders IS 'One row per order';
COMMENT ON COLUMN orders.o_totalprice IS 'In dollars';
COMMENT ON VIEW order_totals IS 'Updated live';
COMMENT ON TABLE orders IS NULL;  -- remove
```

Comments are stored as Iceberg metadata: a `comment` table property and the
columns' doc strings, which Spark and Trino also read. Clients see them as
in PostgreSQL, through `obj_description`, `col_description` and
`pg_description` (psql's `\dt+` and `\d+` show them), and in one listing:

```sql
SELECT table_schema, table_name, column_name, comment FROM igloo_comments();
```

A replica rereads every table's comments at most every 30 seconds, when a
query reads them, so comments set on another replica can take that long to
appear.

## Roles and grants

Every user has one built-in role that applies to the whole warehouse, set
with `CREATE USER ... ROLE` and `ALTER USER ... ROLE` (see
[operations](operations.md#users-and-roles)): `admin`, `writer`, `reader`,
or `none`, which allows nothing on its own. Grants add privileges on
schemas and tables of the Iceberg catalog, to a user directly or to a custom
role that users are given. Only admins create roles and grant; the syntax is
PostgreSQL's:

```sql
CREATE USER nora WITH PASSWORD 'a-long-password' ROLE none;
CREATE ROLE analysts;
GRANT analysts TO nora;
GRANT SELECT ON TABLE sales.orders TO analysts;   -- TABLE is optional; views too
GRANT SELECT, INSERT ON SCHEMA staging TO analysts;  -- every table in it, now and later
GRANT SELECT ON ALL TABLES IN SCHEMA finance TO analysts;  -- the tables there now
GRANT CREATE ON SCHEMA sandbox TO nora;
GRANT ALL PRIVILEGES ON SCHEMA sandbox TO nora;  -- SELECT, INSERT, UPDATE, DELETE, CREATE
REVOKE INSERT ON SCHEMA staging FROM analysts;
REVOKE analysts FROM nora;
DROP ROLE [IF EXISTS] analysts;
SHOW ROLES;
SHOW GRANTS;                       -- or TO name, ON TABLE name, ON SCHEMA name
```

| Privilege | On | Allows |
|---|---|---|
| `SELECT` | table, view or schema | Reading the table |
| `INSERT` | table or schema | `INSERT`, `COPY ... FROM`, `COPY INTO` and `MERGE ... INSERT` into it |
| `UPDATE` | table or schema | `UPDATE` and `MERGE ... UPDATE` |
| `DELETE` | table or schema | `DELETE`, `TRUNCATE` and `MERGE ... DELETE` |
| `CREATE` | schema | Creating, replacing, altering, renaming, commenting on and dropping its tables and views, including `CREATE TABLE AS` |

A statement runs if the user's built-in role allows it, or if every access
it makes to the catalog is granted, to them or to one of their roles. So a
`reader` with `INSERT` on a table may insert into it, and a user with role
`none` may run only queries whose tables are all granted to them. How it
works and what it means:

- A grant on a schema covers every table and view in it, including ones
  created later. `ALL TABLES IN SCHEMA` covers the tables and views there
  at the time of the grant, as in PostgreSQL.
- Users with role `none` need `SELECT` on everything a statement reads,
  including the target of `UPDATE`, `DELETE` and `MERGE`, the query of
  `CREATE TABLE AS`, and the tables behind every view (see below). They can
  read the system catalogs (`information_schema`, `pg_catalog`, the
  `duckdb_*` functions, `igloo_comments()`) and their own
  [history](#query-history), but not files: `read_parquet`, `read_csv`,
  `iceberg_scan`, `iceberg_metadata`, `glob`, `COPY ... FROM` and similar
  functions could read any table's files directly. Statements Igloo cannot
  analyze, such as `PRAGMA`, are refused for them with SQLSTATE `42501`.
- Views run with the querying user's privileges: reading a view needs
  `SELECT` on the tables it reads. Owner's-rights views, column masking and
  row filters are not available yet.
- `CREATE` on a schema does not include reading or writing the tables made
  there; grant those as well.
- Grants never allow what needs `admin` (users, secrets, `ATTACH`, global
  settings), writing files with `COPY ... TO`, temporary tables, creating or
  dropping schemas, or changing table properties.
- Grants follow tables by name. Dropping a table or view through Igloo
  removes its grants, and renaming one moves them; `CREATE OR REPLACE`
  keeps them.
- Grant and role changes apply at once on the replica that made them and
  within 30 seconds on the others, also to statements prepared before.
- Table and column names are visible to every user, through the system
  catalogs.

A refused statement gets SQLSTATE `42501` and a message that names the
table or schema and the privilege it needs, such as `permission denied for
nora: SELECT on table public.orders needs the reader role or a SELECT
grant`.

## Query history

Every statement and login is recorded: `igloo.query_history` and
`igloo.login_history` list them, newest first. Admins see everyone's;
other users see their own.

```sql
SELECT start_time, user_name, outcome, duration_ms, query_text
FROM igloo.query_history
WHERE outcome <> 'ok'
ORDER BY start_time DESC LIMIT 20;

SELECT event_time, user_name, result, reason, client_address
FROM igloo.login_history WHERE result = 'failure';
```

| `igloo.query_history` column | Meaning |
|---|---|
| `query_id`, `session_id` | Identify the statement and the session that ran it |
| `user_name`, `role` | Who ran it, and their built-in role then |
| `start_time`, `end_time`, `duration_ms` | When it ran, and for how long, including sending results |
| `outcome`, `sqlstate` | `ok`, `error`, `denied` or `canceled`, and the error's SQLSTATE |
| `row_count` | Rows returned or changed, when known |
| `statement_type` | Such as `SELECT`, `CREATE TABLE` or `GRANT` |
| `query_text` | The statement as sent, with passwords and secrets replaced by `'***'` |
| `tables_read`, `tables_written` | Catalog tables the statement used, when Igloo checked them against grants; `NULL` otherwise |
| `client_address`, `application_name` | Where the session came from |

`igloo.login_history` has `event_time`, `user_name`, `result` (`success` or
`failure`), `reason`, `client_address`, `application_name` and `session_id`.
The runs of tasks, pipes and materialized views are in `igloo.task_history`,
and the files `COPY INTO` loaded in `igloo.load_history`, with the same
rule: admins see everything, other users their own (see
[loading](loading.md#tasks)).

History is kept for `IGLOO_AUDIT_RETENTION` (90 days by default). Rows arrive
in the background, usually within a second or two of the statement ending.
Reading the views fetches history page by page, so `LIMIT` without `ORDER BY`
reads only the newest pages; filters and sorts read everything visible.

## Time travel

Every write creates an Iceberg snapshot, and you can query earlier ones:

```sql
SELECT * FROM iceberg_snapshots('lake.public.events');
SELECT * FROM events AT (VERSION => 7983066988386993768);
SELECT * FROM events AT (TIMESTAMP => TIMESTAMP '2026-10-01 00:00:00');
```

Time travel reaches back as far as the snapshot retention: 7 days by default
(`IGLOO_SNAPSHOT_RETENTION`), or a table's own
`history.expire.max-snapshot-age-ms`. Older snapshots are expired by
[table maintenance](operations.md#table-maintenance).

## Loading and exporting data

Allow the locations to load from or export to with `IGLOO_ALLOWED_LOCATIONS`,
for example `s3://landing-bucket/`. Igloo's storage credentials are used to
read them. Writers can then:

```sql
CREATE TABLE orders AS SELECT * FROM read_parquet('s3://landing-bucket/orders/*.parquet');
INSERT INTO orders SELECT * FROM read_csv('s3://landing-bucket/new_orders.csv');
COPY orders FROM 's3://landing-bucket/more_orders.parquet' (FORMAT parquet);
COPY (SELECT * FROM orders WHERE o_orderdate >= DATE '2026-01-01')
  TO 's3://landing-bucket/export/orders_2026.parquet' (FORMAT parquet);
```

Every other file or URL is blocked, including the server's local disk.

To load drops from a landing bucket, `COPY INTO` loads every new file under
a prefix, in CSV, TSV, Parquet or JSON, compressed or not, and remembers
what it loaded so that running it again loads only new files. Pipes run it
every minute, tasks run any statement on a schedule, and materialized views
keep a query's result as a table; [Loading and scheduling](loading.md)
describes them all.

```sql
COPY INTO orders FROM 's3://landing-bucket/orders/' (PATTERN '.*[.]csv[.]gz');
CREATE PIPE orders_drops AS COPY INTO orders FROM 's3://landing-bucket/orders/';
CREATE TASK nightly SCHEDULE '0 6 * * *' TIMEZONE 'America/New_York' AS
  CREATE OR REPLACE TABLE order_summary AS SELECT o_orderdate, sum(o_totalprice) FROM orders GROUP BY 1;
```

## Types

Values go to clients as these PostgreSQL types:

| DuckDB | PostgreSQL |
|---|---|
| `BOOLEAN` | `boolean` |
| `TINYINT`, `SMALLINT`, `UTINYINT` | `smallint` |
| `INTEGER`, `USMALLINT` | `integer` |
| `BIGINT`, `UINTEGER` | `bigint` |
| `UBIGINT`, `HUGEINT`, `UHUGEINT`, `DECIMAL(p,s)` | `numeric` |
| `FLOAT`, `DOUBLE` | `real`, `double precision` |
| `VARCHAR`, `ENUM` | `text` |
| `BLOB` | `bytea` |
| `UUID` | `uuid` |
| `DATE`, `TIME` | `date`, `time` |
| `TIMESTAMP` (any precision), `TIMESTAMPTZ` | `timestamp`, `timestamptz` |
| `INTERVAL` | `interval` |
| `JSON`, `LIST`, `ARRAY`, `STRUCT`, `MAP`, `UNION` | `json` |
| others (`TIMETZ`, `BIT`, ...) | `text` |

Nested values arrive as JSON, which every PostgreSQL client can read. Over
[Arrow Flight SQL](flight-sql.md), values keep DuckDB's own Arrow types
instead, nested ones included.

## Clients

Tested with every release, by the test suite and `scripts/e2e.sh`:

| Client | Version | Notes |
|---|---|---|
| psql (libpq) | 17 | Including `\dt`, `\d`, `\d+`, `\dn`, `\l`, `\du`, `\password` |
| pgx (Go) | 5.11 | Simple protocol, and the extended protocol with named and unnamed statements |
| psycopg (Python) | 3.3 | Including server-side prepared statements, server-side cursors and transactions |
| PostgreSQL JDBC | 42.7 | `getTables` and `getColumns` with comments, views, prepared statements, fetch-size paging |
| Ibis (Python) | 10.8 | Through its PostgreSQL backend: `sql()`, `table()`, expressions, `to_pyarrow_batches()` |
| ADBC Flight SQL driver (Python) | 1.12 | Over [Arrow Flight SQL](flight-sql.md): Arrow tables, pandas and polars, parameters, `adbc_get_objects` |
| pyarrow.flight | 26.0 | Over Flight SQL: handshake, `get_flight_info`, `do_get` |
| Arrow Flight SQL JDBC | 19.0 | Over Flight SQL: `getTables`, `getColumns`, prepared statements, `executeUpdate`, large results |

Tools built on these drivers, such as DBeaver, Metabase, Superset, Tableau
and dbt (with `materialized='table'`), should work with their PostgreSQL
connector; they are not yet part of the test suite. Their schema browsers read
the `pg_catalog` and `information_schema` views, which Igloo fills in from the
catalog.

Connect with any PostgreSQL connection string, for example
`postgres://ana@igloo.example.com:5432/igloo?sslmode=require`. The database
name is ignored.

## Differences from PostgreSQL

- The SQL dialect is DuckDB's (above). PostgreSQL-only features such as
  stored procedures, triggers, sequences on Iceberg tables, `LISTEN/NOTIFY`
  and row-level security are not available.
- Users, roles and passwords are managed with `CREATE USER`, `ALTER USER`,
  `DROP USER` and `SHOW USERS` (see [operations](operations.md#users-and-roles)).
  `GRANT` and `REVOKE` cover tables and schemas of the Iceberg catalog (see
  [roles and grants](#roles-and-grants)); there are no column privileges,
  `GRANT OPTION`, `USAGE` or roles granted to roles.
- Only admins may send several statements in one query string. A `PIVOT`
  that takes its columns from the data (no `IN (...)` after `ON`) is one
  statement to the client, but DuckDB runs it as two: first a query that
  finds the columns, kept as a temporary enum type in the session. Readers
  and writers may run such a `PIVOT` in a query (a statement that starts
  with `SELECT`, `FROM`, `PIVOT`, `UNPIVOT` or a parenthesis), and the query
  that finds the columns is checked like the `PIVOT` before it runs. In
  other statements (`CREATE TABLE AS`, `INSERT`, `EXPLAIN`, or after
  `WITH`), DuckDB would run the whole statement while preparing it, before
  Igloo can check it, so only admins may; others, and users with role
  `none` everywhere, list the columns, as in
  `PIVOT sales ON quarter IN ('q1', 'q2') USING sum(amount)`.
- `query()` and `json_execute_serialized_sql()`, which run SQL given as a
  string, are refused for everyone, admins included: Igloo checks a
  statement's text, and cannot check SQL that a statement builds. Write the
  query itself instead. `query_table()` is limited to admins.
- Session settings that PostgreSQL drivers send on connect
  (`application_name`, `extra_float_digits`, `DateStyle` and similar) are
  accepted and ignored. `statement_timeout` is honored.
- `SET` of DuckDB settings that affect every session (memory, threads,
  secrets) is limited to admins, as are the read cache's functions and
  settings (`cache_httpfs_*`, see [operations](operations.md#read-cache)).
- `pg_catalog` views are compatible enough for the clients above but are not
  a complete copy of PostgreSQL's. Storage-level details (sizes, indexes,
  tablespaces) are empty. Temporary objects are in the schema `pg_temp`.
- SQL cursors support `DECLARE ... CURSOR [WITH HOLD] FOR`, `FETCH [FORWARD
  | NEXT] [n | ALL]` and `CLOSE`; they read forward only, and `MOVE` and
  scrollable cursors are not supported. A cursor's whole result is computed
  when it is declared.
- Array parameters (`= ANY($1)` with a list) work when the client declares
  the array type or the parameter is the operand of `ANY` or `ALL`, in which
  case it is a list of text.
