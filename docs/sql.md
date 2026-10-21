# SQL and clients

## Dialect

Igloo speaks the PostgreSQL wire protocol, but queries run on DuckDB, so the
SQL dialect is [DuckDB's](https://duckdb.org/docs/stable/sql/introduction),
which is close to PostgreSQL's. Everyday SQL (joins, window functions, CTEs,
aggregates, `GROUP BY ALL`, `QUALIFY`) works as written; DuckDB's function
library applies.

A session can choose [Snowflake's dialect](snowflake-sql.md) instead, with
`SET sql_dialect = 'snowflake'`, a connection option or a role setting:
Igloo then translates Snowflake SQL (`IFF`, `DATEADD`, `v:path::type`,
`FLATTEN` and the rest) to DuckDB's before it checks and runs it.

Sketches for deduplicated reach, geometry and H3 functions, and DuckDB's
approximate and statistical functions are described in
[media analytics](media-analytics.md).

Tables live in the Iceberg catalog, named `lake` in SQL (`IGLOO_CATALOG_NAME`),
and the default schema is `public`. Unqualified names resolve to
`lake.public`, so `events` and `lake.public.events` are the same table.
[Connections](#connections-to-other-databases) to PostgreSQL, MySQL and
Snowflake databases are catalogs of their own, such as
`crm.public.customers`.

## Tables

| Statement | Supported |
|---|---|
| `CREATE TABLE`, `CREATE TABLE AS SELECT` | Yes |
| `INSERT`, `INSERT ... SELECT`, `INSERT ... RETURNING`, `MERGE INTO` | Yes |
| `UPDATE`, `DELETE` | Yes |
| `DROP TABLE`, `CREATE SCHEMA`, `DROP SCHEMA` | Yes; `CASCADE` drops only the table, see below |
| `ALTER TABLE ... RENAME TO`, `ADD COLUMN`, `DROP COLUMN` | Yes |
| `CREATE OR REPLACE TABLE` | Yes; see below |
| `CREATE [OR REPLACE] VIEW`, `DROP VIEW`, `ALTER VIEW ... RENAME TO` | Yes; see [views](#views) |
| `COMMENT ON SCHEMA`, `TABLE`, `VIEW`, `COLUMN` | Yes; see [comments](#comments) |
| `ALTER TABLE`, `VIEW`, `SCHEMA ... OWNER TO` | Yes; see [the catalog](catalog.md#owners) |
| `CREATE TEMP TABLE`, `CREATE TEMP VIEW` | Yes; held in memory for the session only |
| `COPY INTO table FROM 'location'` | Yes, outside transactions; see [loading](loading.md#copy-into) |
| `CREATE [OR REPLACE] MATERIALIZED VIEW`, `REFRESH MATERIALIZED VIEW` | Yes, outside transactions; see [materialized views](loading.md#materialized-views) |
| `CREATE [OR REPLACE] TABLE ... CLONE`, `CREATE [OR REPLACE] SCHEMA ... CLONE` | Yes, outside transactions; see [clones](#clones) |
| `ALTER TABLE ... CREATE BRANCH`, `CREATE TAG`, `DROP BRANCH`, `DROP TAG`, `FAST FORWARD` | Yes, outside transactions; see [branches and tags](#branches-and-tags) |
| `CREATE TASK`, `CREATE PIPE` | Yes; see [tasks](loading.md#tasks) and [pipes](loading.md#pipes) |
| `PRIMARY KEY`, `UNIQUE` | Accepted but not enforced |
| `NOT NULL` | Enforced |

`CREATE OR REPLACE TABLE` writes the new table under a temporary name, then
drops the old table and renames the new one into place. A failure while
writing leaves the old table as it was. The swap is quick but not atomic: a
query that starts in the instant between drop and rename finds no table.

The catalog records nothing that depends on a table, so `DROP TABLE ...
CASCADE` drops just the table, like `DROP TABLE`: views that read it fail
when they are next queried. A dropped table's files are deleted with it,
as are those of a table `CREATE OR REPLACE TABLE` replaces, `... CLONE`
included, unless [clones](#clones) read them. What a drop leaves,
erasure requests delete ([privacy](privacy.md#what-is-left-of-dropped-tables)).
A dropped table cannot be restored.

### Transactions

Writes are transactional. `BEGIN ... COMMIT` groups statements, and as in
PostgreSQL:

- once a statement in a block fails, every statement until `COMMIT` or
  `ROLLBACK` fails with SQLSTATE `25P02`, and `COMMIT` rolls the block back;
- `BEGIN` inside a block, and `COMMIT` or `ROLLBACK` outside one, only warn;
- a query string with several statements (see
  [differences](#differences-from-postgresql)) runs them in one transaction,
  unless they hold transaction statements of their own.

As in Snowflake, a statement that creates, alters, renames, drops or
comments on a schema, table or view in the Iceberg catalog commits the
block first and then runs on its own; the block carries on in a new
transaction. A later `ROLLBACK` does not undo it, nor what came before it in
the block. Temporary tables and views stay in the block. dbt builds models
this way: `CREATE TABLE ... AS`, then `ALTER TABLE ... RENAME TO` to swap the
new table in, all in one block.

`SAVEPOINT` and `RELEASE SAVEPOINT` are accepted. `ROLLBACK TO SAVEPOINT`
cannot undo changes: it works when nothing changed since the savepoint,
which lets psycopg's and SQLAlchemy's nested blocks recover from a failed
query, and otherwise fails with SQLSTATE `0A000`.

## Views

Views are stored in the Iceberg catalog as Iceberg views with DuckDB SQL, so
every replica and restart sees them:

```sql
CREATE VIEW recent_orders AS
  SELECT * FROM orders WHERE o_orderdate > current_date - INTERVAL 30 DAY;
CREATE OR REPLACE VIEW order_totals (customer, total) AS
  SELECT o_custkey, sum(o_totalprice) FROM lake.public.orders GROUP BY 1;
ALTER VIEW order_totals RENAME TO customer_totals;  -- ALTER TABLE works too
DROP VIEW IF EXISTS recent_orders;
```

- Query a view by any name: `recent_orders`, `public.recent_orders` or
  `lake.public.recent_orders`. In listings (`information_schema`, `\dv`)
  views appear under the database `memory`, where each replica keeps its
  copy. An unqualified name finds views on the search path as it finds
  tables: after `SET search_path = sales`, `v` is `sales.v`. Only admins
  may change `memory`, and so only they may put it first on the search
  path.
- An unqualified name in a view is the view's own schema's table or view
  of that name, if it has one, as in Snowflake and as the view's Iceberg
  default namespace (its schema) says: a view in `sales` that reads
  `orders` reads `sales.orders`, whatever the search path of the session
  that created it or reads it. Igloo stores such names qualified
  (`lake.sales.orders`). Other unqualified names resolve when the view is
  queried, against the querying session's search path, as in DuckDB; a
  CTE keeps its name. Schema-qualified table names are stored with the
  catalog's name: `sales.orders` becomes `lake.sales.orders`.
- A view that a replica cannot load, because a table it reads is missing,
  say, fails when read with the reason (`view sales.v could not be loaded:
  ...`), and loads at the next sync after the cause is fixed.
- Creating, replacing, renaming or dropping a view takes effect at once for
  every session on the replica that does it. A view created on one replica
  reaches the others within 30 seconds, or at once when a query there names
  it.
- Writers create, rename and drop views, as do users with `CREATE` on the
  view's schema. Other engines see the views in the catalog but cannot run
  their DuckDB SQL. `ALTER VIEW` supports only `RENAME TO`; a renamed view
  keeps its grants and comments, and views that read it by its old name
  fail until they are replaced. `DROP VIEW ... CASCADE` drops only the view.
- A view runs with the privileges of whoever queries it: reading one needs
  `SELECT` on the tables it reads (see [grants](#roles-and-grants)).

## Comments

```sql
COMMENT ON SCHEMA public IS 'Shared tables';
COMMENT ON TABLE orders IS 'One row per order';
COMMENT ON COLUMN orders.o_totalprice IS 'In dollars';
COMMENT ON VIEW order_totals IS 'Updated live';
COMMENT ON COLUMN order_totals.total IS 'Lifetime revenue';
COMMENT ON TABLE orders IS NULL;  -- remove
```

Comments are stored as Iceberg metadata: a `comment` property of the table,
view or namespace and the columns' doc strings (a view's in a new version of
the view), which Spark and Trino also read. Clients see them as in
PostgreSQL, through `obj_description`, `col_description` and
`pg_description` (psql's `\dt+` and `\d+` show them), in
`information_schema`, in the [catalog's views](catalog.md) such as
`igloo.tables`, and in one listing:

```sql
SELECT table_schema, table_name, column_name, comment FROM igloo_comments();
```

Each user sees the comments of the objects they may see (see
[the catalog](catalog.md#who-sees-what)). Owners of an object may comment on
it too. A replica rereads every table's comments in the background every 30
seconds while queries read them, so comments set on another replica can take
that long to appear.

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
GRANT SELECT ON CONNECTION crm TO analysts;      -- a connection to another database
GRANT SELECT ON SCHEMA crm.public TO nora;       -- or one of its schemas, or crm.public.customers
REVOKE INSERT ON SCHEMA staging FROM analysts;
REVOKE analysts FROM nora;
DROP ROLE [IF EXISTS] analysts;
SHOW ROLES;
SHOW GRANTS;                       -- or TO name, ON TABLE name, ON SCHEMA name, ON CONNECTION name
```

| Privilege | On | Allows |
|---|---|---|
| `SELECT` | table, view or schema | Reading the table |
| `SELECT` | connection, or its schema or table | Reading the connection's tables, which the built-in roles do not cover (see [connections](#connections-to-other-databases)) |
| `INSERT` | table or schema | `INSERT`, `COPY ... FROM`, `COPY INTO` and `MERGE ... INSERT` into it |
| `UPDATE` | table or schema | `UPDATE` and `MERGE ... UPDATE` |
| `DELETE` | table or schema | `DELETE`, `TRUNCATE` and `MERGE ... DELETE` |
| `CREATE` | schema | Creating, replacing, altering, renaming, commenting on and dropping its tables and views, including `CREATE TABLE AS` |
| `ERASE` | `ACCOUNT` | [Erasure requests](privacy.md) and data retentions, which otherwise need admin |

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
  `duckdb_*` functions, the [catalog's views](catalog.md), which show them
  only what they may see) and their own
  [history](#query-history), but not files: `read_parquet`, `read_csv`,
  `iceberg_scan`, `iceberg_metadata`, `glob`, `COPY ... FROM` and similar
  functions could read any table's files directly. Statements Igloo cannot
  analyze, such as `PRAGMA`, are refused for them with SQLSTATE `42501`.
- Grants are checked for the table a name means as DuckDB binds it. A name
  with no schema, or with a schema such as `sales.orders`, is looked for
  along the session's search path; `sales.orders` only in the databases
  whose `sales` is on the path, or in the current database if none's is,
  never in a [connection](#connections-to-other-databases) whose schema of
  that name is not on the path. Where a name is found in several places on
  the path, users with role `none` need grants on each, as the statement
  would read the next if the first were dropped.
- Views run with the querying user's privileges: reading a view needs
  `SELECT` on the tables it reads. Owner's-rights views are not available.
  To show users only some rows or masked columns of a table, use
  [policies](#policies-and-tags).
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

## Policies and tags

Masking policies replace a column's values and row access policies hide
rows, for every user and statement, as in Snowflake; tags classify columns
and can mask every column that has them. Admins create and attach them:

```sql
CREATE MASKING POLICY mask_email AS (val VARCHAR) RETURNS VARCHAR ->
  CASE WHEN is_role_in_session('pii_reader') THEN val ELSE '***' END;
ALTER TABLE crm.contacts ALTER COLUMN email SET MASKING POLICY mask_email;

CREATE ROW ACCESS POLICY client_rows AS (cid VARCHAR) RETURNS BOOLEAN ->
  is_role_in_session('admin')
  OR EXISTS (SELECT 1 FROM governance.client_access a
             WHERE a.client_id = cid AND is_role_in_session(a.role_name));
ALTER TABLE crm.contacts ADD ROW ACCESS POLICY client_rows ON (client_id);

CREATE TAG pii ALLOWED_VALUES 'email', 'device_id';
ALTER TAG pii SET MASKING POLICY mask_email;
ALTER TABLE crm.leads ALTER COLUMN email SET TAG pii = 'email';

SHOW POLICIES;
SELECT * FROM igloo.policy_references;
```

[Masking, row access policies and tags](security-policies.md) describes every
statement, how policies apply and what they refuse.

## Erasure and data retention

[Erasure requests](privacy.md) delete a person's data from every table
whose columns are tagged with their identifiers, physically and with a
record that shows it, for GDPR and CCPA requests. Admins and holders of
`ERASE ON ACCOUNT` make them:

```sql
GRANT ERASE ON ACCOUNT TO privacy_officer;
ERASE SUBJECT WHERE pii.email = 'ann@example.com' DRY RUN;   -- what it would erase, and nothing more
ERASE SUBJECT WHERE pii.email = 'ann@example.com' OR pii.device_id IN ('6F9619FF-...', '0C8C2A9E-...');
SHOW ERASURE REQUESTS;                       -- or SELECT * FROM igloo.erasure_requests
CONFIRM ERASURE REQUEST '01a14588-...';      -- one held because it would delete much of a table; not by its requester
CANCEL ERASURE REQUEST '01a14588-...';       -- one that has not begun deleting
CLOSE ERASURE REQUEST '01a14588-...';        -- admins only: the last resort for a blocked one
ALTER TABLE marts.impressions SET (data_retention_days = 30);
ALTER TABLE marts.impressions RESET (data_retention_days);
SELECT * FROM igloo.data_retention;
```

## Query history

Every statement and login is recorded: `igloo.query_history` and
`igloo.login_history` list them, newest first. Admins see everyone's;
other users see their own. The [catalog](catalog.md) works out lineage and
table usage from it.

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
| `row_count` | Rows returned or changed, or written by `CREATE TABLE ... AS`, when known |
| `statement_type` | Such as `SELECT`, `CREATE TABLE` or `GRANT` |
| `query_text` | The statement as sent, with passwords and secrets replaced by `'***'` |
| `tables_read`, `tables_written` | Catalog tables the statement read and wrote, as `catalog.schema.table`, and in `tables_read` the [connections'](federation.md) tables it read, as `connection.schema.table`: for users with the role none, those their grants were checked for; for others, those its text names, views expanded, as worked out after it ran (see [lineage](catalog.md#lineage)); `NULL` when Igloo could not tell |
| `client_address`, `application_name` | Where the session came from |
| `policies` | The masking and row access policies that applied to the statement; `NULL` when none did |
| `query_tag`, `pool` | The session's [query tag](workload-management.md#query-tags), and the compute pool of the replica that ran it |
| `queued_ms` | Time it waited for its [role limits](workload-management.md#role-limits) |
| `cpu_time_ms`, `bytes_read`, `memory_allocated_bytes` | What DuckDB measured while it ran; `NULL` for statements DuckDB did not run and ones that failed (see [resources](workload-management.md#resources-in-the-history)) |
| `locations_read` | Locations outside the catalog that it read files from: `COPY INTO`'s, and the files a function such as `read_parquet` names; never external tables' files, which are read through the tables |
| `job` | The task, pipe or materialized view it ran for, such as `task nightly`; `NULL` for a session's own statements |
| `workers` | How many workers ran parts of the statement ([distributed execution](distributed-execution.md)); 0 when it ran on one replica |

`igloo.login_history` has `event_time`, `user_name`, `result` (`success` or
`failure`), `reason`, `client_address`, `application_name`, `session_id`,
`auth_method` (`password`, `token`, `sso` or `access_token`, when known) and
`token_name`, the [access token](sso.md#access-tokens) a login used.
The runs of tasks, pipes, materialized views and alerts are in
`igloo.task_history`, the files `COPY INTO` loaded in
`igloo.load_history`, and what alerts found and sent in
`igloo.alert_history`, with the same rule: admins see everything, other
users their own (see [loading](loading.md#tasks) and
[alerts](alerts.md#alert-history)).

History is kept for `IGLOO_AUDIT_RETENTION` (90 days by default). Rows arrive
in the background, usually within a second or two of the statement ending.
Reading the views fetches history page by page, so `LIMIT` without `ORDER BY`
reads only the newest pages; filters and sorts read everything visible.

`igloo.usage` sums the history by day, compute pool, user, role and query
tag, with each group's share of the day's compute cost; see
[usage and cost](workload-management.md#usage-and-cost).

## Query tags and role settings

```sql
SET query_tag = 'client:acme;campaign:q4';         -- recorded with every statement
ALTER ROLE analysts SET max_concurrent_statements = 4;
ALTER ROLE analysts SET statement_timeout = '15min';
ALTER ROLE analysts SET memory_limit = '8GB';
ALTER USER ana SET query_tag = 'client:acme';      -- a default for her sessions
ALTER ROLE analysts RESET ALL;
SHOW ROLE SETTINGS;
```

`ALTER ROLE` takes a custom role, a built-in role (`reader` means every
reader) or a user, and only admins may run it. The settings are
`query_tag`, `statement_timeout`, `max_concurrent_statements`,
`memory_limit` and `sql_dialect` (the [SQL dialect](snowflake-sql.md) its
sessions start in); [workload management](workload-management.md) says how
they combine and how they are enforced.

## Delta Sharing

Admins share tables with outside recipients and read partners' shares as
external tables, with statements Igloo carries out itself:

```sql
CREATE SHARE acme_reporting;
ALTER SHARE acme_reporting ADD TABLE marts.campaign_daily;
CREATE RECIPIENT acme;                        -- returns a one-time activation link
GRANT SELECT ON SHARE acme_reporting TO RECIPIENT acme;

CREATE PROVIDER tvco_ads FROM '<the partner''s profile file>';
CREATE EXTERNAL TABLE partners.viewership FROM SHARE tvco_ads.tv.viewing.daily;
SELECT count(*) FROM partners.viewership;
```

External tables are read-only and take grants like any table. See
[Delta Sharing](sharing.md) for every statement, what recipients receive,
and the security model.

## Connections to other databases

Admins connect PostgreSQL, MySQL and Snowflake databases, which Igloo then
queries in place, read-only, alongside its Iceberg tables; see
[connections](federation.md) for everything about them.

```sql
CREATE CONNECTION crm TYPE postgres
  WITH (host 'crm.db.example.com', database 'crm', user 'igloo_ro', password 'a-long-password');
-- each replica attaches it when it next starts
GRANT SELECT ON SCHEMA crm.public TO analysts;
SELECT count(*) FROM lake.public.audiences a JOIN crm.public.customers c ON c.email = a.email;
ALTER CONNECTION crm SET (password 'a-new-password');  -- applies without a restart
SHOW CONNECTIONS;
DROP CONNECTION crm;
```

A connection's tables need a `SELECT` grant on them, their schema or the
connection for everyone but admins, readers and writers included, and
masking and row access policies apply to them as to Iceberg tables. The
functions that run SQL on the remote database (`postgres_query`,
`mysql_query` and the like) are refused for everyone.

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
[table maintenance](operations.md#table-maintenance). A
[tag](#branches-and-tags) keeps a snapshot for as long as you need it.

## Clones

A clone is a full copy of a table, or of a schema's tables and views, that
costs nothing to make. It is a new Iceberg table whose metadata points at
its source's data and delete files, so no data is copied and no storage is
added, however large the table: making one takes moments. From then on the
two are separate tables. Writes to either go to new files of that table's
own, which the other never sees, and only those new files cost storage.

```sql
CREATE TABLE dev.orders CLONE prod.orders;
CREATE OR REPLACE TABLE dev.orders CLONE prod.orders AT (TIMESTAMP => now() - INTERVAL 1 DAY);
CREATE TABLE dev.orders_v7 CLONE prod.orders AT (SNAPSHOT => 7983066988386993768);
CREATE TABLE dev.orders_q3 CLONE prod.orders AT (TAG => 'q3_close');
CREATE SCHEMA dev CLONE prod;                  -- every table and view
CREATE OR REPLACE SCHEMA dev CLONE prod;       -- make dev a fresh copy of prod
```

- **Which snapshot.** Without `AT`, the source as it is now. `AT
  (TIMESTAMP => expression)` takes the source as of a time, as far back as
  [time travel](#time-travel) reaches; `AT (SNAPSHOT => id)`, or `VERSION =>`
  as in time travel, one snapshot; `AT (BRANCH => 'name')` or `AT (TAG =>
  'name')` the snapshot a [branch or tag](#branches-and-tags) names. A
  schema is cloned as it is now or as of a time.
- **What it has.** The source's columns, as of the snapshot cloned, its
  table and column comments and its table properties, snapshot retention
  included. Its history starts at the snapshot cloned: time travel on the
  clone reaches no further back, and the source's branches and tags are not
  cloned.
- **Schemas.** `CREATE SCHEMA ... CLONE` clones every table of the source
  schema and copies every view's SQL. As in Snowflake, a view's SQL is kept
  as it is, so a view that reads `lake.prod.orders` reads the source's table
  still; views that leave out the schema read whatever the reader's search
  path finds. A materialized view is cloned as a table, which nothing
  refreshes. `OR REPLACE` clones each table over the one of the same name,
  and drops the tables and views of the schema that the source does not
  have. A schema clone is not atomic: if a table cannot be cloned, the
  error names it, the tables cloned so far stay, and running the statement
  again finishes the job.
- **Who may clone.** Cloning a table needs `SELECT` on it, or the reader
  role, and `CREATE` on the clone's schema, or the writer role: what
  `CREATE TABLE ... AS SELECT * FROM` it needs. Cloning a schema needs the
  writer role, as creating a schema does.
- **Grants.** A clone starts with no grants of its own, as any new table
  does; grants follow names, so a clone made again under a name keeps the
  grants on that name. A schema's clone gets the source schema's own grants,
  which cover every table in it, now and later; grants on the source's
  tables are not copied.
- **Policies.** A clone gets every masking policy, row access policy and
  tag of its source, so no user sees more of the clone than of the source:
  cloning is no way around [policies](#policies-and-tags). They are in place
  before the clone exists. Other replicas take up to 30 seconds to learn of
  a policy change, so the first clone of a table with policies under a new
  name waits that long before it is created; making it again under that
  name, as a development copy is every day, does not wait. A table of the
  clone's name that has a different policy or tag value of its own is not
  replaced: an admin decides which applies first.
- **Readers.** Like a table made with `CREATE TABLE AS`, a clone may be read
  by whoever may read its schema. Unlike one, it holds every row of the
  source behind the source's policies, so a user whose roles a row access
  policy admits sees those rows in the clone, even without `SELECT` on the
  source. Clone tables with policies into schemas that only their intended
  readers can read.
- **History.** `CLONE` statements are recorded in
  [`igloo.query_history`](#query-history) with the source in `tables_read`
  and the clone in `tables_written`.
- **Storage.** Table maintenance never deletes a file a clone still reads,
  whichever table is compacted, expires snapshots, loses orphan files or is
  dropped; a source's old files are deleted once no clone reads them either.
  A clone made again under a name, with `OR REPLACE`, deletes the files of
  the one it replaces, unless clones read them, and none of its source's.
  See [files that clones share](operations.md#files-that-clones-share).
- **Other engines.** Through the [Iceberg REST
  catalog](external-engines.md#clones), engines get no storage credentials
  for a clone, and cannot purge a table whose files clones read nor delete
  its files with the credentials they get for it.
- Tables of format version 1 and encrypted tables cannot be cloned, nor can
  a view on its own. `CLONE` runs outside transactions.

## Branches and tags

Iceberg tables can have named references to their snapshots. A tag names
one snapshot and keeps it, however old it gets, until it is dropped: a
quarter-end close, the table before a backfill. A branch is a line of
snapshots of its own. (These are snapshot tags, not the
[tags](#policies-and-tags) that classify columns.)

```sql
ALTER TABLE events CREATE TAG q3_close;                         -- the current snapshot
ALTER TABLE events CREATE TAG before_fix AT (TIMESTAMP => TIMESTAMP '2026-10-01 00:00:00');
ALTER TABLE events CREATE BRANCH audit AT (TAG => 'q3_close');
ALTER TABLE events CREATE OR REPLACE BRANCH audit;               -- move it to the current snapshot
SELECT count(*) FROM events AT (TAG => 'q3_close');
SELECT * FROM events e AT (BRANCH => 'audit') WHERE e.kind = 'click';
SHOW BRANCHES ON events;                                        -- branches and tags, main first
ALTER TABLE events FAST FORWARD main TO audit;
ALTER TABLE events DROP TAG IF EXISTS q3_close;
ALTER TABLE events DROP BRANCH audit;
```

- `AT` takes what time travel and clones take: `TIMESTAMP`, `SNAPSHOT`,
  `BRANCH` and `TAG`. `CREATE ... IF NOT EXISTS` leaves an existing branch
  or tag alone; `OR REPLACE` moves it.
- **Branches are read-only in Igloo.** DuckDB 1.5's Iceberg writer commits
  every `INSERT`, `UPDATE`, `DELETE` and `MERGE` to `main`, the table
  itself. Other engines can write to a branch, as in Spark's
  write-audit-publish; `FAST FORWARD main TO audit` then publishes what they
  wrote, and is refused if `main` has commits the branch does not have. For
  changes made in Igloo, use a [clone](#clones).
- Reading a branch or tag reads the snapshot it names when the statement is
  prepared, through the table's grants and policies. `AT (BRANCH => ...)`
  and `AT (TAG => ...)` follow a table's name, or its alias, in `FROM` or
  `JOIN`.
- Branches and tags are kept until they are dropped, however old, unless
  a ref's own `max-ref-age-ms` or the table's
  `history.expire.max-ref-age-ms` says otherwise. Table maintenance keeps
  the snapshot each names, with the files it reads, a clone's included, and
  a branch's history within the snapshot retention.
- Making, moving and dropping them needs the writer role or `CREATE` on the
  table's schema; `SHOW BRANCHES` needs `SELECT` on the table. `main` cannot
  be dropped.

## A dbt development environment

Each analytics engineer can build models against a full copy of
production, made in seconds and costing no storage until their models
change it, with the same masking and row access policies as production.

As an admin, once: give Ana a schema of her own and read access to
production.

```sql
CREATE SCHEMA dbt_ana;
GRANT SELECT ON SCHEMA analytics TO ana;
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE ON SCHEMA dbt_ana TO ana;
CREATE TASK refresh_dbt_ana SCHEDULE '0 6 * * 1-5' TIMEZONE 'America/New_York' AS
  CREATE OR REPLACE SCHEMA dbt_ana CLONE analytics;
ALTER TASK refresh_dbt_ana RESUME;
```

Every weekday morning `dbt_ana` becomes a fresh copy of `analytics`, and
the tables she built there that production does not have are dropped. It
keeps the grants on `analytics` as well, so whoever may read production
may read her copy. Ana can also refresh one table herself whenever she
wants:

```sql
CREATE OR REPLACE TABLE dbt_ana.orders CLONE analytics.orders;
```

Point dbt's development target at her schema, with an
[access token](sso.md#access-tokens) as the password:

```yaml
# ~/.dbt/profiles.yml
agency:
  target: dev
  outputs:
    dev:
      type: postgres
      host: igloo.example.com
      port: 5432
      user: ana
      password: "{{ env_var('IGLOO_TOKEN') }}"
      dbname: igloo
      schema: dbt_ana
      threads: 4
```

`dbt build --select orders_daily+` then rebuilds just the models she
changed, and the ones downstream of them, in `dbt_ana`, reading every other
model from the clone; no `--defer` to production is needed. dbt's own
`dbt clone` command creates views with the PostgreSQL adapter; run `CREATE
... CLONE` instead.

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
| `GEOMETRY` | `text`, as WKT such as `POINT (-73.98 40.75)` |
| others (`TIMETZ`, `BIT`, ...) | `text` |

Nested values arrive as JSON, which every PostgreSQL client can read. Over
[Arrow Flight SQL](flight-sql.md), values keep DuckDB's own Arrow types
instead, nested ones included. Sketches (`sketch_hll`, `sketch_theta` and
the others) are `BLOB`s, sent as `bytea`.

## Clients

Tested with every release, by the test suite and `scripts/e2e.sh`, and
weekly by `scripts/bi-e2e.sh` (marked BI):

| Client | Version | Notes |
|---|---|---|
| psql (libpq) | 17 | Including `\dt`, `\d`, `\d+`, `\dn`, `\l`, `\du`, `\password`, `\copy ... to` |
| pgx (Go) | 5.11 | Simple protocol, and the extended protocol with named and unnamed statements |
| psycopg (Python) | 3.3 | Including server-side prepared statements, server-side cursors, transactions and `copy()` out |
| PostgreSQL JDBC | 42.7 | `DatabaseMetaData` as Tableau, Looker and DBeaver call it, result metadata of prepared and `LIMIT 0` queries, fetch-size paging |
| Ibis (Python) | 10.8 | Through its PostgreSQL backend: `sql()`, `table()`, expressions, `to_pyarrow_batches()` |
| ADBC Flight SQL driver (Python) | 1.12 | Over [Arrow Flight SQL](flight-sql.md): Arrow tables, pandas and polars, parameters, `adbc_get_objects` |
| pyarrow.flight | 26.0 | Over Flight SQL: handshake, `get_flight_info`, `do_get` |
| Arrow Flight SQL JDBC | 19.0 | Over Flight SQL: `getTables`, `getColumns`, prepared statements, `executeUpdate`, large results |
| dbt Core with dbt-postgres | 1.12, 1.11 | Seeds, views, tables, incremental `append`, `merge` and `delete+insert`, snapshots, tests, `persist_docs`, `docs generate`, `source freshness`; see [integrations](integrations.md#dbt) |
| SQLAlchemy with psycopg | 2.1 | The inspector and `MetaData.reflect`; pandas 3 `read_sql` and `to_sql` |
| polars | 2.0 | `read_database` and `write_database` through SQLAlchemy, `read_database_uri` through connectorx 0.4 and ADBC 1.12 |
| Npgsql (.NET, Power BI) | 4.0, 10.0 | BI: type loading, Power BI's navigator queries, `GetSchema`, `GetSchemaTable`, parameters |
| Metabase | 0.64 | BI: sync, native and editor-built questions |
| Apache Superset (psycopg2) | 6.1 | BI: datasets, time-series charts, SQL Lab |

[Tools and integrations](integrations.md) says how to connect dbt, BI tools
and notebooks. Schema browsers read the `pg_catalog` and
`information_schema` views, which Igloo fills in from the catalog.

Connect with any PostgreSQL connection string, for example
`postgres://ana@igloo.example.com:5432/igloo?sslmode=require`. The database
name is ignored; dbt needs it to be the catalog's name (see
[integrations](integrations.md#dbt)). The password may be an
[access token](sso.md#access-tokens) (`CREATE TOKEN`), which is how people
who sign in with single sign-on, and service accounts, connect BI tools, dbt
and scripts.

Engines that read Iceberg tables themselves, such as Spark, Trino and
PyIceberg, use Igloo's tables through its Iceberg REST catalog instead, under
the same users and grants; see [other engines](external-engines.md).

## Differences from PostgreSQL

- The SQL dialect is DuckDB's (above). PostgreSQL-only features such as
  stored procedures, triggers, sequences on Iceberg tables and
  `LISTEN/NOTIFY` are not available. Row-level security is Snowflake's
  kind: [row access policies](#policies-and-tags), not `CREATE POLICY`.
- Users, roles and passwords are managed with `CREATE USER`, `ALTER USER`,
  `DROP USER` and `SHOW USERS` (see [operations](operations.md#users-and-roles)),
  with Igloo's own options: `TYPE SERVICE`, `OWNER`, `EMAIL`, `LOGIN` and
  `NOLOGIN`, `PASSWORD NULL`. Access tokens are managed with `CREATE TOKEN`,
  `SHOW TOKENS` and `DROP TOKEN`, and single sign-on's group mappings with
  `GRANT role TO SSO GROUP 'name'` (see [single sign-on](sso.md)).
  `GRANT` and `REVOKE` cover tables and schemas of the Iceberg catalog (see
  [roles and grants](#roles-and-grants)); there are no column privileges,
  `GRANT OPTION`, `USAGE` or roles granted to roles.
- A query string with several statements, as `psql -c` and psycopg2 (and
  so dbt) send them, runs them one at a time, each checked and run as if
  sent alone, in one transaction unless they hold transaction statements of
  their own. The first that fails ends the query. A prepared statement
  (the extended protocol) holds one statement. Statements Igloo carries out
  outside transactions must be sent on their own (statements on users,
  connections, notification integrations, policies, tags, owners, tasks,
  pipes and materialized views, `COPY INTO`, `CLONE`, branches and tags);
  several user statements may come together.
- A `PIVOT` that takes its columns from the data (no `IN (...)` after
  `ON`) is one statement to the client, but DuckDB runs it as two: first a query that
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
  query itself instead. `query_table()` is limited to admins, and needs its
  table name written out as text, not built at run time. The functions
  that run SQL on other databases (`postgres_query`, `postgres_execute`,
  `postgres_scan`, `mysql_query`, `mysql_execute` and the like) are refused
  for everyone too, for the same reason; see
  [connections](federation.md#what-no-one-can-do).
- The spatial extension's GDAL readers and writers (`ST_Read`,
  `ST_Read_Meta`, `FROM 'x.gpkg'`, `FROM 'x.fgb'`, `COPY ... (FORMAT GDAL)`)
  are refused for everyone too: GDAL opens files that a dataset names
  outside the sandbox. `ST_ReadSHP`, `ST_ReadOSM` and GeoParquet work; see
  [media analytics](media-analytics.md#loading-boundaries).
- Temporary tables have DuckDB's `rowid` (Iceberg tables do not), but some
  filters on it, such as `rowid % 3 = 0` or `hash(rowid) % 3 = 0`, fail with
  SQLSTATE `0A000`, in views too: with the spatial extension loaded, DuckDB
  1.5.6 cannot plan a filter that reads only `rowid` unless it compares it
  with constants. `rowid < 1000`, `rowid BETWEEN 10 AND 20` and
  `rowid IN (1, 2)` work. To split a table into parts, filter on its own
  columns, as in `hash(id) % 3 = 0`.
- `ALTER ROLE ... SET` and `ALTER USER ... SET` take Igloo's
  [workload settings](#query-tags-and-role-settings) only, not other
  PostgreSQL parameters. `SET query_tag` and Snowflake's `ALTER SESSION SET
  QUERY_TAG` set the session's query tag.
- Session settings that PostgreSQL drivers send on connect
  (`application_name`, `extra_float_digits`, `DateStyle` and similar) are
  accepted and ignored. `statement_timeout` is honored.
  `distributed_execution` (`auto`, `on` or `off`) is Igloo's own: whether
  the session's statements may run in parts on workers
  ([distributed execution](distributed-execution.md#when)). `SHOW` and
  `current_setting()` answer the PostgreSQL settings drivers read, such as
  `transaction_isolation` (`repeatable read`: DuckDB transactions see a
  snapshot), `server_version` and `standard_conforming_strings`. Any other
  `SHOW name` describes the table `name`, as in DuckDB; with no such table,
  it fails as PostgreSQL's does for a setting it does not have (SQLSTATE
  `42704`, `unrecognized configuration parameter "name"`). `SET` and
  `RESET` of a parameter neither DuckDB nor Igloo has fail the same way,
  for every user, before any check of the user's role.
- Errors carry PostgreSQL's SQLSTATE for what is missing: `42P01` for a
  table, view or sequence, `42883` for a function (`no_such_function(1)`),
  `42704` for a type (`1::NUMBER`), `3F000` for a schema in which a table
  is created, `42703` for a column.
- `current_user`, `session_user` and `current_role` (also written with
  parentheses) are the signed-in user's name. As a select list's item of
  its own they name their column as in PostgreSQL (`current_user`), and in
  the [Snowflake dialect](snowflake-sql.md) as in Snowflake
  (`CURRENT_USER()`).
- Transaction blocks behave as in PostgreSQL, except that changes to the
  catalog commit the block first, and `ROLLBACK TO SAVEPOINT` cannot undo
  changes (see [transactions](#transactions)).
- `COPY (query) TO STDOUT` and `COPY table TO STDOUT` work, in text, CSV and
  binary format, with `HEADER`, `DELIMITER` and `NULL`; they need `SELECT`
  on what they read, like the query. `COPY ... FROM STDIN` is not supported.
- `TO_CHAR` formats dates and timestamps; number formats are not supported.
- `SET` of DuckDB settings that affect every session (memory, threads,
  secrets) is limited to admins, as are the read cache's functions and
  settings (`cache_httpfs_*`, see [operations](operations.md#read-cache)).
- `pg_catalog` views are compatible enough for the clients above but are not
  a complete copy of PostgreSQL's. Storage-level details (sizes, indexes,
  tablespaces) are empty, and so are keys: Iceberg does not keep primary
  or foreign keys. Temporary objects are in the schema `pg_temp`.
  `lake.information_schema` and `lake.pg_catalog` mean the same as
  `information_schema` and `pg_catalog`.
- SQL cursors support `DECLARE ... CURSOR [WITH HOLD] FOR`, `FETCH [FORWARD
  | NEXT] [n | ALL]` and `CLOSE`; they read forward only, and `MOVE` and
  scrollable cursors are not supported. A cursor's whole result is computed
  when it is declared.
- Array parameters (`= ANY($1)` with a list) work when the client declares
  the array type or the parameter is the operand of `ANY` or `ALL`, in which
  case it is a list of text.
