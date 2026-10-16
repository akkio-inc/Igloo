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
- Writers create and drop views. Other engines see the views in the catalog
  but cannot run their DuckDB SQL.

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

Nested values arrive as JSON, which every PostgreSQL client can read.

## Clients

Tested with every release, by the test suite and `scripts/e2e.sh`:

| Client | Version | Notes |
|---|---|---|
| psql (libpq) | 17 | Including `\dt`, `\d`, `\d+`, `\dn`, `\l`, `\du`, `\password` |
| pgx (Go) | 5.11 | Simple protocol, and the extended protocol with named and unnamed statements |
| psycopg (Python) | 3.3 | Including server-side prepared statements, server-side cursors and transactions |
| PostgreSQL JDBC | 42.7 | `getTables` and `getColumns` with comments, views, prepared statements, fetch-size paging |
| Ibis (Python) | 10.8 | Through its PostgreSQL backend: `sql()`, `table()`, expressions, `to_pyarrow_batches()` |

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
  `DROP USER` and `SHOW USERS` (see [operations](operations.md#users-and-roles));
  `GRANT` and `REVOKE` are not supported.
- Only admins may send several statements in one query string.
- Session settings that PostgreSQL drivers send on connect
  (`application_name`, `extra_float_digits`, `DateStyle` and similar) are
  accepted and ignored. `statement_timeout` is honored.
- `SET` of DuckDB settings that affect every session (memory, threads,
  secrets) is limited to admins.
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
