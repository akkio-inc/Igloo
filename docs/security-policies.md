# Masking, row access policies and tags

Grants decide which tables a user may read. Policies decide what a user sees
in them: a **masking policy** replaces a column's values, and a **row access
policy** hides rows. **Tags** classify columns, such as `pii = 'email'`, and
can carry masking policies, so every column with the tag is masked. The
statements follow Snowflake's.

Policies apply to every statement of every user, admins included, wherever
the table appears in it: `FROM`, joins, subqueries, common table
expressions, set operations, views, the source of `INSERT`, `CREATE TABLE
AS` and `COPY`, the target of `UPDATE`, `DELETE` and `MERGE`, cursors and
the web console. Nobody is exempt unless a policy's own expression says so.

## An agency's warehouse

An agency keeps several clients' data in one warehouse. Each client's team
sees only that client's rows, and only people with the `pii_reader` role see
email addresses and device IDs:

```sql
-- Who may see which client: the mapping table.
CREATE TABLE governance.client_access (role_name VARCHAR, client_id VARCHAR);
INSERT INTO governance.client_access VALUES
  ('acme_team', 'acme'), ('globex_team', 'globex'), ('analytics_leads', 'acme');

CREATE ROLE acme_team;
CREATE ROLE globex_team;
CREATE ROLE pii_reader;
GRANT acme_team TO nora;

-- Rows: admins see every client; others the clients their roles map to.
CREATE ROW ACCESS POLICY client_rows AS (cid VARCHAR) RETURNS BOOLEAN ->
  is_role_in_session('admin')
  OR EXISTS (SELECT 1 FROM governance.client_access a
             WHERE a.client_id = cid AND is_role_in_session(a.role_name));

-- Columns: identifiers stay joinable as hashes, emails keep their domain.
CREATE TAG pii ALLOWED_VALUES 'email', 'device_id', 'ip', 'household_id';
CREATE MASKING POLICY pii_text AS (val VARCHAR) RETURNS VARCHAR ->
  CASE
    WHEN is_role_in_session('pii_reader') THEN val
    WHEN tag_value('pii') = 'email' THEN '***@' || split_part(val, '@', 2)
    ELSE sha256(val)
  END;
ALTER TAG pii SET MASKING POLICY pii_text;

ALTER TABLE marts.impressions ADD ROW ACCESS POLICY client_rows ON (client_id);
ALTER TABLE marts.impressions ALTER COLUMN email SET TAG pii = 'email';
ALTER TABLE marts.impressions ALTER COLUMN device_id SET TAG pii = 'device_id';
```

Nora, in `acme_team`, sees only acme's impressions, with emails as
`***@example.com` and device IDs as hashes, however she queries them:
filtering on a raw email finds nothing, grouping by email groups masked
values, and a join on device ID joins hashes. Give a whole team a client by
adding a row to `governance.client_access`; no policy changes.

Users need `SELECT` on the tables they query, as before (see
[grants](sql.md#roles-and-grants)), but not on the tables a policy reads,
such as the mapping table: a policy reads with its own rights.

## Masking policies

```sql
CREATE [OR REPLACE] MASKING POLICY [IF NOT EXISTS] name
  AS (val type) RETURNS type -> expression [COMMENT = 'text'];
ALTER TABLE t ALTER [COLUMN] c SET MASKING POLICY name [FORCE];
ALTER TABLE t ALTER [COLUMN] c UNSET MASKING POLICY;
DROP MASKING POLICY [IF EXISTS] name;
```

The expression gets the column's value as `val` (any name) and returns what
the user sees, of the same type. The column's type must be the policy's.
`MODIFY` may be written for `ALTER`, as in Snowflake. A column has one
masking policy; `FORCE` replaces it. `CREATE OR REPLACE` changes a policy's
expression everywhere it is attached, at once, but not its types while it is
attached.

```sql
CREATE MASKING POLICY mask_ip AS (val VARCHAR) RETURNS VARCHAR ->
  CASE WHEN is_role_in_session('pii_reader') THEN val
       ELSE regexp_replace(val, '\.\d+$', '.0') END;
CREATE MASKING POLICY hide_spend AS (val DECIMAL(18,2)) RETURNS DECIMAL(18,2) ->
  CASE WHEN is_role_in_session('finance') THEN val END;   -- NULL for others
```

## Row access policies

```sql
CREATE [OR REPLACE] ROW ACCESS POLICY [IF NOT EXISTS] name
  AS (arg type, ...) RETURNS BOOLEAN -> expression [COMMENT = 'text'];
ALTER TABLE t ADD ROW ACCESS POLICY name ON (column, ...);
ALTER TABLE t DROP ROW ACCESS POLICY name;
ALTER TABLE t DROP ALL ROW ACCESS POLICIES;
DROP ROW ACCESS POLICY [IF EXISTS] name;
```

A row is visible when the expression is true for its columns, passed in the
order of `ON (...)` (up to five). A table has one row access policy.

## In policy expressions

| Function | Is |
|---|---|
| `current_user`, `current_user()`, `session_user`, `current_role` | The querying user's name |
| `is_role_in_session(role)` | Whether the user has `role`: one of their custom roles, or their built-in role (`admin`, `writer`, `reader` or `none`). `role` may be a column, as in the mapping table above. |
| `current_available_roles()` | Those roles, as a `VARCHAR[]` |
| `tag_value('tag')` | In a masking policy, the masked column's value of the tag, or NULL |

An expression may read tables of the catalog in subqueries, such as a
mapping table of users or roles to clients. Igloo stores it with every table
name qualified (`lake.governance.client_access`), so no session can make it
read another table, and refuses expressions that:

- read views, files or table functions other than generators and the
  system catalogs, or call macros;
- depend on what a session controls: `getvariable`, `current_setting`,
  `current_schema` and the like;
- have parameters, comments, or more than one expression.

Expressions run with the querying session's settings. Of those a user may
change, `integer_division`, `default_order`, `default_null_order` and
`calendar` change what `/`, `ORDER BY` and date parts mean, so Igloo refuses
statements that policies apply to in a session that changed one of them
(`RESET` it to go on). The time zone stays the session's own, as in
Snowflake: compare `TIMESTAMPTZ` values rather than dates or times of day in
the session's zone.

## Tags

```sql
CREATE [OR REPLACE] TAG [IF NOT EXISTS] name [ALLOWED_VALUES 'a', 'b', ...] [COMMENT = 'text'];
ALTER TABLE t ALTER [COLUMN] c SET TAG name = 'value' [, name = 'value'];
ALTER TABLE t ALTER [COLUMN] c UNSET TAG name [, name];
ALTER TAG name SET MASKING POLICY policy [, MASKING POLICY policy];
ALTER TAG name UNSET MASKING POLICY policy;
DROP TAG [IF EXISTS] name;
```

A tag may carry one masking policy per data type. A column with the tag is
masked by the policy for its type, unless it has a masking policy of its
own, which comes first. A column whose tag has masking policies, none for
its type, reads as NULL. A column with two tags that both mask its type is
refused until one is unset. A tag in use cannot be dropped, nor a policy
that is attached anywhere.

## Tables of connections

Policies and tags apply to the tables of
[connections to other databases](federation.md) as to the catalog's, named
in full:

```sql
ALTER TABLE crm.public.customers ALTER COLUMN email SET MASKING POLICY mask_email;
```

The remote database can rename a column without Igloo, so a connection's
table with a masked column it no longer has is refused until the policy
follows the column; and a policy protects only the table it is set on, not
other tables or views of that database that hold the same data. See
[connections](federation.md#masking-and-row-access-policies).

## Listing them

```sql
SHOW POLICIES;                -- or SHOW MASKING POLICIES, SHOW ROW ACCESS POLICIES
SHOW TAGS;
SELECT * FROM igloo.policy_references;  -- which policy applies where, also through tags
SELECT * FROM igloo.tag_references;     -- which column has which tag
```

Everyone may list policies and tags, and where they apply on the tables
they may see (see [the catalog](catalog.md#who-sees-what)): users with the
role none see only the references of the tables they have a grant on or
own. Only admins see policies' expressions. The web console's data browser
and catalog show a table's row access policy and each column's masking
policy and tags.

`igloo.policy_references` also lists, to their own party, the tables
[clean rooms](clean-rooms.md) keep to it, with `policy_kind` `CLEAN ROOM`
and the room's name. Igloo protects them as it protects tables with
policies, and refuses them to everyone else, who do not see them in the
catalog either. A clean room's dataset cannot have policies of its own: a
dataset whose table gains one stops running.

## How they apply

- **Statements.** Igloo rewrites each statement that reads a protected
  table so it reads the table through a subquery that applies its policies
  for the user, then checks the result: rewriting it again must change
  nothing, and DuckDB's plan of it must read exactly the tables its text
  names. A statement it cannot rewrite with certainty is refused with
  SQLSTATE `42501`. The query history records the statement as sent, with
  the policies that applied in its `policies` column.
- **Masked values everywhere.** The rest of the statement sees only masked
  values, in `WHERE`, `JOIN`, `GROUP BY`, window functions and error
  messages alike, so filtering on a raw value finds nothing. Each column is
  also guarded by the row access policy, so a condition DuckDB evaluates
  before the policy's sees `NULL` for hidden rows, not their values.
- **Writes.** `UPDATE`, `DELETE` and `MERGE` change only rows the row access
  policy shows, and their conditions and new values see masked values, as a
  query would. `INSERT` is not filtered. What a statement copies out of a
  protected table (`CREATE TABLE AS`, `INSERT ... SELECT`, `COPY ... TO`) is
  what its user sees, masked values included.
- **Exemptions** are written into the policy, as `is_role_in_session('admin')`
  above. For a user a policy exempts, the policy is a no-op: Igloo works this
  out with DuckDB's optimizer and runs their statements as written, so
  loading jobs keep their plans.
- **Views** run with the querying user's privileges, and read the tables
  behind them through their policies.
- **Every way in.** Statements over the Postgres protocol, the web console
  and [Arrow Flight SQL](flight-sql.md) take the same path, so policies
  apply to all of them alike, Flight SQL's Arrow results included.
- **Wherever they run.** A statement runs through its policies on
  [workers](distributed-execution.md#masking-and-row-access-policies) and in
  the replica's [second DuckDB](workload-management.md#heavy-statements-beside-short-ones)
  as on the replica: every stage reads a protected table through the
  policies' subquery, so no exchange holds a raw masked value or a hidden
  row.
- **Tasks, pipes and materialized views** ([loading](loading.md)) run as
  their owners, through the owner's policies at each run. A materialized
  view holds what its owner may see, and whoever may read the view sees
  that, as with a table the owner filled with `CREATE TABLE AS`.
- **Changes** apply at once on the replica that made them and within 30
  seconds on the others, to statements prepared before them too.
- **Time travel** (`AT (VERSION => ...)`) reads old snapshots through
  today's policies, as does reading a branch or tag.
- **Clones** ([details](sql.md#clones)) get their source's masking and row
  access policies and tags, attached to the clone's name before the clone
  exists. Unlike `CREATE TABLE AS`, a clone holds the source's raw rows and
  shows each reader what the source would, so a user with `SELECT` on a
  protected table and `CREATE` on a schema of their own sees no more of
  their clone than of the table. The first clone of a protected table under
  a new name waits 30 seconds, so that every replica protects it. If the
  name already has a different policy or tag value of its own, from an
  earlier table, the clone is refused until an admin changes one.

## What is refused

Renaming a table with policies, tags or a [data
retention](privacy.md#data-retention), renaming or dropping one of its
protected or tagged columns, and changing such a column's type, whether or
not any table has policies, in SQL and through the [Iceberg REST
catalog](external-engines.md), by admins too. Policies and tags stay with a
table's name and a column's, also when it is dropped, so a table created
again under the name is protected at once, and a renamed table would
escape them and [erasure requests](privacy.md), as would the old values of
a column dropped and added again. Unset them first.

While any table has policies:

- Statements Igloo cannot rewrite with certainty that name a protected table
  or a view or macro that reads one: several statements in one prepared
  statement (those of a simple query run, and are rewritten, one at a
  time), a `PIVOT` that finds its columns in the data, statements behind a
  `WITH` clause before `UPDATE`, `DELETE` or `MERGE`, macros that read
  protected tables, `stats()`, and table names in escaped strings.
- For a user the policies do not exempt: `TRUNCATE` of a table with a row
  access policy, `MERGE` into it with `WHEN NOT MATCHED` clauses (add new
  rows with `INSERT`), `INSERT OR REPLACE` and `ON CONFLICT` into it, and
  `RETURNING` masked columns; and any statement that policies apply to in
  a session whose `integer_division`, `default_order`, `default_null_order`
  or `calendar` was changed, or that has a macro named like a function a
  policy calls.
- For users other than admins: reading the warehouse's files directly
  (`read_parquet`, `parquet_metadata`, `glob`, `COPY ... FROM`, `COPY INTO`
  and pipes from the warehouse or a location that holds it, and the like;
  files elsewhere in `IGLOO_ALLOWED_LOCATIONS` remain readable, named as
  string literals), including the copies of shared files in its
  `.igloo/sharing/` directory, and external tables' files at Igloo's
  [Delta Sharing](sharing.md) proxy, which are read through the external
  tables, table functions that read tables by name or show their
  statistics (`duckdb_table_sample`, `pragma_storage_info`,
  `iceberg_metadata`, `iceberg_snapshots` of a protected table), and
  `PRAGMA` and `CALL` of anything but the catalog listings.

## Cost

Statements that name nothing protected run as written, after a check of
their words against the names of protected tables and of the views and
macros that read them. A statement that reads a protected table takes a few
milliseconds more to prepare, for the rewrite and its checks. A row access
policy's own condition can still be pushed into the table scan; the
statement's other conditions on the table's columns cannot, since they must
not see hidden rows, and neither can conditions on masked columns. Statements
of users a policy exempts run as written, after the same checks.

## Limitations

- **Admins are not a boundary.** They manage policies, can read the
  warehouse's files and attach other databases, and statements only admins
  may run (such as `PRAGMA tpch`) do not go through policies. Policies keep
  them from seeing raw data by accident; the query history records what
  they do.
- **External engines.** Spark, Trino and others read table files
  directly, so they cannot apply policies. Igloo's two ways of serving
  tables to them both refuse tables with policies to everyone but admins:
  the [Iceberg REST catalog](external-engines.md#tables-with-data-policies)
  will not load, hand out credentials for or commit to such a table, and
  [Delta Sharing](sharing.md#tables-with-masking-or-row-access-policies)
  will not add one to a share or serve a shared table that gains one.
  Storage credentials an engine got before a policy was attached work for
  up to an hour. Engines given the catalog database or object storage
  directly see raw data; do not give them access to tables with policies.
- **External tables** (Delta Sharing) take no policies or tags; copy what
  users may read into a table to protect it.
- **Existence of hidden rows.** A condition that fails on `NULL` can show,
  by failing, that a table has rows the user cannot see, though nothing
  about them.
- **Deterministic masks** keep what they do not hide: a hash keeps joins
  and distinct counts working, and so shows which rows share a value. Use a
  constant where that matters.
- `EXPLAIN` shows the rewritten statement, policy expressions included.
- Policies attach to tables, not views, and take one column per masking
  policy (no `USING` clause). There are no column privileges.
