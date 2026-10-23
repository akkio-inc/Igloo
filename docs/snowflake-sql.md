# Snowflake SQL

Igloo's SQL is DuckDB's (see [SQL and clients](sql.md#dialect)). A session
can choose Snowflake's dialect instead: Igloo then translates each statement
into DuckDB's dialect before it checks and runs it, so queries, dashboards
and dbt models written for Snowflake run without being rewritten.

```sql
SET sql_dialect = 'snowflake';

SELECT IFF(amount > 100, 'large', 'small') AS size,
       DATEADD(month, 1, order_date)       AS next_month,
       payload:customer.email::string      AS email
FROM orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) = 1;
```

The translation covers the functions, syntax and types that analytics SQL
uses most. What it cannot translate faithfully it refuses with an error
that names the construct; where DuckDB cannot reproduce Snowflake's
behaviour exactly, the translation is the closest one and the difference is
listed under [known differences](#known-differences).

## Turning it on

The dialect is a setting, `sql_dialect`, with two values: `duckdb`, the
default, and `snowflake`. Any user may change it for their own sessions; it
changes how their SQL is read, never what they may do.

| Where | How |
|---|---|
| A session | `SET sql_dialect = 'snowflake'`, or `ALTER SESSION SET sql_dialect = 'snowflake'`; `SET sql_dialect = 'duckdb'` turns it off, `RESET sql_dialect` returns to the connection's or the user's default, `SHOW sql_dialect` says which applies |
| A connection | The startup option `-c sql_dialect=snowflake`: `PGOPTIONS='-c sql_dialect=snowflake'` for psql and libpq, `options=-c%20sql_dialect=snowflake` in a JDBC URL, `options='-c sql_dialect=snowflake'` for psycopg. A startup parameter named `sql_dialect` works too. An unknown value fails the login with SQLSTATE `22023` |
| A user or role | `ALTER USER ana SET sql_dialect = 'snowflake'` or `ALTER ROLE analysts SET sql_dialect = 'snowflake'` (admins), a [role setting](workload-management.md#role-limits) like `query_tag`: the user's own, otherwise their custom roles' (the first role by name), otherwise their built-in role's |

A session's own `SET` wins over its connection's option, which wins over
the user's or role's setting.

- **Arrow Flight SQL** sessions take `SET sql_dialect` and the user's or
  role's setting. **The web console** takes the setting, and a worksheet
  can start with `SET sql_dialect = 'snowflake';`. The console's own pages
  and Flight SQL's catalog calls are always read as DuckDB's.
- **Drivers' and BI tools' catalog queries**, which name `pg_catalog`,
  `pg_` functions and tables or use PostgreSQL's `E'...'` strings, pass
  untranslated whatever the dialect. Other statements a tool writes for
  PostgreSQL are read as Snowflake's in a Snowflake session; give a role the
  setting only when its clients send Snowflake SQL.
- **Views, tasks, pipes, alerts and materialized views** created in
  Snowflake's dialect store their translation, which is DuckDB SQL: they run
  the same in every session, and `DESCRIBE` and `duckdb_views()` show the
  translation. Translations call macros named `sf_...` (such as
  `sf_add_days`) that every session has.
- **Scripts** sent as one query are split as Snowflake splits them, so a
  `;` inside a `'it\'s; fine'` string or after a `//` comment does not end
  a statement. A statement that changes `sql_dialect` applies to the ones
  after it.

## What Igloo checks

Translation is not a security boundary. Igloo checks the translated
statement (the user's role, grants, masking and row access policies, file
access, workload limits) exactly as it checks a statement sent in DuckDB's
dialect, so a statement in Snowflake's dialect reaches only what its
DuckDB translation would. `IDENTIFIER('name')` and `IDENTIFIER($variable)`
are resolved before the checks. The [query history](sql.md#query-history)
records the translated statement.

Errors use SQLSTATE codes:

| Code | Meaning |
|---|---|
| `0A000` | The construct is valid Snowflake SQL that Igloo does not translate. The message names it, and the hint says what to write instead where there is an alternative |
| `42601` | The statement is not valid SQL as Snowflake reads it, or a function has the wrong number of arguments |
| `22023` | A value is not valid, such as an unknown format element or a session variable that is not set |

Statements that Igloo accepts but that change nothing, or translates with a
caveat, send a notice (SQLSTATE `01000`), which psql and most drivers show:
`CLUSTER BY` of expressions, `USE WAREHOUSE`, `HASH` (whose values are not
Snowflake's) and the like. The web console and Flight SQL do not show
notices.

## Names

- Unquoted identifiers are case-insensitive, as in Snowflake.
- Result columns are named as Snowflake names them: an unquoted alias in
  upper case (`AS total` gives `TOTAL`), a quoted one as written, an
  expression after its text in upper case (`COUNT(*)`,
  `DATEADD(DAY, 1, D)`), and a table's column after its name in the
  catalog, in upper case unless it has upper-case letters or the statement
  quotes it.
- `CREATE TABLE ... AS` and views keep their columns' names as written
  (`AS total` stores `total`); reading them in Snowflake's dialect shows
  `TOTAL`.
- `IDENTIFIER('db.schema.table')`, `IDENTIFIER($name)`, `TABLE('name')` and
  `TABLE($name)` name an object.
- A three-part name's database is the catalog's name, `lake` unless
  `IGLOO_CATALOG_NAME` sets another: give the catalog the name of the
  Snowflake database whose SQL you run, or use two-part names.

## What is translated

### Literals and lexical rules

- Strings with Snowflake's backslash escapes (`'it\'s'`, `'\n'`,
  `'\u00e9'`), `$$...$$` strings, `//` comments, `X'...'` binary literals.
- Session variables: `SET x = 10` (or `SET x = (SELECT ...)`), `$x`,
  `UNSET x`, `SHOW VARIABLES`. `SET` of one of Igloo's or PostgreSQL's
  settings (such as `search_path` or `statement_timeout`) sets the setting.
- Typed literals such as `TIMESTAMP_LTZ '2024-01-01 10:00'`, `{'a': 1}`
  object constants and `[1, 2]` array constants.

### Semi-structured data

`VARIANT`, `OBJECT` and `ARRAY` values are JSON. Columns of them are
Iceberg's own types where Iceberg has one, so that other engines read them
and Igloo reads them without parsing text (see [columns](#semi-structured-columns)
below): an `ARRAY` column is a list of strings (`VARCHAR[]`), an `OBJECT`
column a map of strings (`MAP(VARCHAR, VARCHAR)`), and a `VARIANT` column
the JSON text of its values (`VARCHAR`). Everything below reads all three,
JSON and JSON text alike.

- Paths: `v:a.b[0]`, `v['a']`, `v:"Quoted Key"`, `x::VARIANT[0]`, with casts
  (`v:a::string`, `v:n::number(10,2)`). A path compared with a string, used
  in a string function or cast to a string reads the value as a string; one
  compared with a number or used in arithmetic reads it as a number.
- `PARSE_JSON`, `TRY_PARSE_JSON`, `CHECK_JSON`, `TO_JSON`, `TO_VARIANT`,
  `TO_OBJECT`, `TO_ARRAY`, `GET`, `GET_PATH`, `TYPEOF`, `IS_*` and `AS_*`,
  `STRIP_NULL_VALUE`.
- `OBJECT_CONSTRUCT`, `OBJECT_CONSTRUCT_KEEP_NULL`, `OBJECT_INSERT`,
  `OBJECT_KEYS`, `OBJECT_AGG`.
- `ARRAY_CONSTRUCT`, `ARRAY_CONSTRUCT_COMPACT`, `ARRAY_AGG` (with `WITHIN
  GROUP` and `DISTINCT`), `ARRAY_UNIQUE_AGG`, `ARRAY_SIZE`,
  `ARRAY_CONTAINS`, `ARRAY_POSITION`, `ARRAY_TO_STRING`, `ARRAY_SLICE`,
  `ARRAY_APPEND`, `ARRAY_PREPEND`, `ARRAY_CAT`, `ARRAY_COMPACT`,
  `ARRAY_DISTINCT`, `ARRAY_INTERSECTION`, `ARRAYS_OVERLAP`,
  `ARRAY_GENERATE_RANGE`, `SPLIT`, `STRTOK_TO_ARRAY`.
- `[LATERAL] FLATTEN(input => ..., path => ..., outer => ..., recursive =>
  ..., mode => ...)` and `TABLE(FLATTEN(...))`, with the columns `SEQ`,
  `KEY`, `PATH`, `INDEX`, `VALUE` and `THIS`. `FLATTEN` of an `ARRAY` or
  `OBJECT` column unnests its list or map directly, and `FLATTEN` of an
  array constant (`[OBJECT_CONSTRUCT(...), ...]`, `ARRAY_CONSTRUCT(...)`)
  unnests its elements without building the array: over 15 million
  elements, 0.3 s instead of 1 s from JSON text. A `WHERE` in the select
  of a `FLATTEN` that is its last `FROM` item becomes the `ON` of that
  join when every join is an inner one, and when a second `FLATTEN` reads
  the first's elements, the `ARRAY` and `OBJECT` columns the first builds
  them from are turned into JSON before the join, unless the select reads
  them elsewhere: in both cases DuckDB would otherwise hold each row's
  lists or maps in a dependent join, at several times the cost.
  `FLATTEN` of an array constant of `OBJECT_CONSTRUCT` and
  `OBJECT_CONSTRUCT_KEEP_NULL` calls with string literals for keys, whose
  `VALUE` its select reads only by a key (`f.value:k`, `f.value['k']`,
  `GET_PATH(f.value, 'k')`, `GET(f.value, 'k')`, with any cast or path
  after; CTEs that reuse the alias count apart), makes its objects DuckDB
  structs rather than JSON: a string reads as itself, and a later
  `FLATTEN(INPUT => f.value:items)` unnests an `ARRAY` column's list from
  the struct without writing it as JSON text, parsing it back or a
  dependent join (over 29 million elements of 2 million rows' five lists,
  0.4 s instead of 1.6 s). When the objects are built from one `FROM` item's
  columns and the select reads only their keys, that item's select list
  unnests them, and a second select list around it unnests a second
  `FLATTEN`'s key, with no lateral join at all. DuckDB makes a lateral
  `FLATTEN` of string columns a dependent join, and one of structs too once
  it pushes a filter down onto it, such as a BI tool's outer `WHERE` on a key
  the report groups by. When every read of the second `FLATTEN`'s elements
  casts them to a type that is not semi-structured, as
  `CAST(v.value AS TEXT)` does, it unnests their text rather than JSON of
  each, and a key that every read casts so is a field of its text alone.
  Where the objects are not unnested so, a `WHERE` there stays out of the
  joins: its conditions on the tables go into the `ON` of the first
  `FLATTEN`'s join, which DuckDB pushes down to the tables, those on the
  objects' keys filter the objects, and those on a second `FLATTEN`'s
  elements filter the elements when the statement reads nothing of that
  `FLATTEN` but its `VALUE`. Any other read of `VALUE`, a star at that level,
  a subquery that
  names the alias, or a second `FLATTEN` of it with `OUTER`, `PATH` or a
  longer path keeps the objects JSON; so do, when a second `FLATTEN` reads
  them, a `WHERE` that cannot stay out of the joins (a condition on another
  of the `FLATTEN`s' columns, a subquery or a select item's alias, or a
  table after the first `FLATTEN`), an `ON` after the `FLATTEN` and a
  `HAVING` on the groups' keys: DuckDB then plans a dependent join, which
  holds structs of lists at a higher cost than JSON text.

#### Semi-structured columns

Iceberg tables of the format versions Igloo's catalog allows (1 and 2) have
no semi-structured type; Iceberg's `variant` needs version 3, which
[erasure requests](privacy.md) cannot rewrite yet. Igloo stores Snowflake's
types as follows, in `CREATE TABLE`, `ALTER TABLE ... ADD COLUMN` and
tables copied from [Snowflake connections](federation.md#snowflake):

| Snowflake | Iceberg | DuckDB | What it holds |
|---|---|---|---|
| `ARRAY` | `list<string>` | `VARCHAR[]` | Each element as Snowflake's `TO_VARCHAR` writes it: a string as itself, any other value as its JSON text (`1`, `true`, `{"k":"v"}`); JSON `null` as `NULL` |
| `OBJECT` | `map<string, string>` | `MAP(VARCHAR, VARCHAR)` | Each value likewise |
| `VARIANT` | `string` | `VARCHAR` | The value's JSON text |
| `ARRAY(type)`, `OBJECT(name type, ...)`, `MAP(type, type)` | `list`, `struct`, `map` of the types | `type[]`, `STRUCT(...)`, `MAP(...)` | The values as typed |

This is how Snowflake itself converts semi-structured values to
`ARRAY(VARCHAR)` and `MAP(VARCHAR, VARCHAR)`, so `INSERT INTO t SELECT
ARRAY_CONSTRUCT(...)` and `PARSE_JSON(...)` store them that way. Reading
them back, an element or value that is the JSON text of an object or an
array reads as that object or array, so paths into an `ARRAY` of objects
(`f.value:name`) work; one that is a number or a boolean reads as a string,
which compares, casts and sums as the number (see [known
differences](#known-differences)). A `VARIANT` column, or JSON text in any
`VARCHAR` column, reads as the JSON it holds.

`CREATE TABLE ... AS SELECT` makes `ARRAY` and `OBJECT` columns of what is
known to be one: `ARRAY_AGG`, `ARRAY_CONSTRUCT`, `[...]`, `SPLIT`,
`::ARRAY` and the other functions that return an `ARRAY`, and
`OBJECT_CONSTRUCT`, `OBJECT_AGG`, `{...}` and `::OBJECT`, as columns of the
query's `SELECT` (not of a `UNION`'s). Any other semi-structured value,
`PARSE_JSON(...)` among them, makes a `VARIANT` column, the JSON text of its
values; cast it to `ARRAY` or `OBJECT` for a list or a map.

### Functions

| Kind | Functions |
|---|---|
| Conditional | `IFF`, `NVL`, `IFNULL`, `NVL2`, `ZEROIFNULL`, `NULLIFZERO`, `DECODE`, `EQUAL_NULL`, `DIV0`, `DIV0NULL`, `GREATEST`, `LEAST` (NULL if any argument is), `GREATEST_IGNORE_NULLS`, `LEAST_IGNORE_NULLS`, `BOOLAND`, `BOOLOR`, `BOOLXOR`, `BOOLNOT` |
| Dates and times | `DATEADD`, `TIMEADD`, `TIMESTAMPADD`, `DATEDIFF`, `TIMEDIFF`, `TIMESTAMPDIFF`, `DATE_TRUNC`, `TRUNC` of a date, `DATE_PART` and `EXTRACT` (including `DAYOFWEEK`, `DAYOFWEEKISO`, `WEEKISO`, `YEAROFWEEK`, `EPOCH_SECOND` and the other parts), `LAST_DAY`, `NEXT_DAY`, `PREVIOUS_DAY`, `DAYNAME`, `MONTHNAME`, `ADD_MONTHS`, `MONTHS_BETWEEN`, `TIME_SLICE`, `DATE_FROM_PARTS`, `TIME_FROM_PARTS`, `TIMESTAMP_FROM_PARTS`, `CONVERT_TIMEZONE`, `SYSDATE`, `CURRENT_DATE`, `CURRENT_TIMESTAMP` |
| Conversion | `TO_DATE`, `TO_TIMESTAMP`, `TO_TIMESTAMP_NTZ`, `_LTZ`, `_TZ`, `TO_TIME`, `TO_CHAR`, `TO_VARCHAR`, `TO_NUMBER`, `TO_DECIMAL`, `TO_NUMERIC`, `TO_DOUBLE`, `TO_BOOLEAN`, `TO_BINARY`, their `TRY_` forms, `CAST` and `::` to Snowflake's types; date, time and number formats such as `'YYYY-MM-DD HH24:MI:SS.FF3'`, `'MON DD, YYYY'` and `'999,999.00'`; epoch seconds and milliseconds |
| Strings | `LEN`, `LENGTH`, `CHARINDEX`, `POSITION`, `CONTAINS`, `STARTSWITH`, `ENDSWITH`, `LEFT`, `RIGHT`, `SUBSTR` (position 0 as 1), `SPLIT_PART` (part 0 as 1), `STRTOK`, `CONCAT`, `CONCAT_WS`, `LPAD`, `RPAD`, `REPLACE`, `INITCAP` (Snowflake's delimiters), `SPACE`, `CHR`, `EDITDISTANCE`, `JAROWINKLER_SIMILARITY`, `ILIKE`, `LIKE ANY`/`ALL`, `ILIKE ANY`/`ALL`, `RLIKE`, `REGEXP_LIKE`, `REGEXP_SUBSTR`, `REGEXP_SUBSTR_ALL`, `REGEXP_REPLACE`, `REGEXP_COUNT`, `REGEXP_INSTR` (with Snowflake's `c`, `i`, `m`, `e` and `s` parameters), `SEARCH` |
| Binary and hashes | `MD5`, `MD5_HEX`, `SHA1`, `SHA1_HEX`, `SHA2` (256 bits), `HASH` (see below), `HEX_ENCODE`, `HEX_DECODE_STRING`, `HEX_DECODE_BINARY`, `BASE64_ENCODE`, `BASE64_DECODE_STRING`, `BASE64_DECODE_BINARY`, `UUID_STRING` |
| Numbers | `SQUARE`, `TRUNC`, `ROUND`, `CEIL` and `FLOOR` with a scale, `DIV0`, `WIDTH_BUCKET`, `RANDOM`, `UNIFORM`, `NORMAL`, `BITAND`, `BITOR`, `BITXOR`, `BITNOT`, `BITSHIFTLEFT`, `BITSHIFTRIGHT`, `GETBIT` |
| Aggregates | `LISTAGG` (with `WITHIN GROUP` and `DISTINCT`), `COUNT(DISTINCT a, b)`, `MEDIAN`, `MODE`, `APPROX_COUNT_DISTINCT` and `HLL` (of one or more values), `HLL_ACCUMULATE`, `HLL_COMBINE`, `HLL_ESTIMATE`, `APPROX_PERCENTILE`, `APPROX_TOP_K`, `BOOLAND_AGG`, `BOOLOR_AGG`, `BITAND_AGG`, `BITOR_AGG`, `BITXOR_AGG`, `SKEW`, `VARIANCE_POP`, `VARIANCE_SAMP` |
| Windows | `RATIO_TO_REPORT`, `CONDITIONAL_TRUE_EVENT`, `IGNORE NULLS` after `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE` and `NTH_VALUE` |
| Generators | `SEQ1`, `SEQ2`, `SEQ4`, `SEQ8` with `TABLE(GENERATOR(ROWCOUNT => n))`, `TABLE(SPLIT_TO_TABLE(s, sep))` |
| Context | `CURRENT_USER`, `CURRENT_ROLE`, `CURRENT_DATABASE`, `CURRENT_SCHEMA`, `CURRENT_VERSION`; `CURRENT_WAREHOUSE`, `CURRENT_ACCOUNT`, `CURRENT_SESSION`, `CURRENT_REGION` and similar return `NULL` |

Functions not listed whose name and meaning DuckDB shares (`ABS`, `UPPER`,
`COALESCE`, `SUM`, `ROW_NUMBER` and most others) pass as written.

`TO_VARCHAR`, `TO_CHAR` and `::VARCHAR` without a format write values in
Snowflake's default output formats, so text compared or stored is the same
in both:

| Type | Text |
|---|---|
| `FLOAT` | Up to 12 significant digits, without trailing zeros: `9244`, `999.889980465`, `0.333333333333`, in scientific notation from `1e+12` and below `0.0001`; `NaN`, `inf`, `-inf` |
| `NUMBER` | With its scale: `1.50` |
| `TIMESTAMP_NTZ` | `YYYY-MM-DD HH24:MI:SS.FF3`: `2026-10-07 15:58:37.862` |
| `TIMESTAMP_LTZ`, `TIMESTAMP_TZ` | `YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM`, in the session's time zone: `2026-10-07 08:58:37.862 -0700` |
| `TIME`, `DATE` | `HH24:MI:SS`, `YYYY-MM-DD` |
| `BINARY` | Hex: `0A0B` |

A format's `TZH` and `TZM` write the offset as Snowflake does (`TZH:TZM` is
`+05:30`, `TZHTZM` is `+0530`).

### Syntax

- `QUALIFY`, `MINUS` (as `EXCEPT`), `SELECT TOP n`, `FROM VALUES (...)`
  with columns `COLUMN1`, `COLUMN2`..., `SAMPLE` and `TABLESAMPLE` (`BERNOULLI`,
  `ROW`, `SYSTEM`, `BLOCK`, `n ROWS`, `SEED`/`REPEATABLE`), `PIVOT` with a
  list of values and `UNPIVOT`, named as Snowflake names their columns,
  `* EXCLUDE`, `* RENAME`, `* REPLACE`, `* ILIKE`.
- `ASOF JOIN ... MATCH_CONDITION (...) [ON ...]`, which keeps the left
  rows without a match, as Snowflake does. `DIRECTED` joins.
- `WITH` whose CTE refers to itself, without `RECURSIVE`.
- `ORDER BY` puts NULLs last ascending and first descending, as Snowflake
  does, in queries, windows and `WITHIN GROUP`.
- Time travel with `AT (TIMESTAMP => ...)` and `AT (OFFSET => -seconds)`,
  the table's alias written after it as Snowflake does.

### Types

| Snowflake | Igloo |
|---|---|
| `NUMBER`, `DECIMAL`, `NUMERIC` (default `(38,0)`) | `DECIMAL(p,s)` |
| `INT`, `INTEGER`, `BIGINT`, `SMALLINT`, `TINYINT`, `BYTEINT` | `BIGINT` |
| `FLOAT`, `FLOAT4`, `FLOAT8`, `DOUBLE`, `DOUBLE PRECISION`, `REAL` | `DOUBLE` |
| `VARCHAR(n)`, `STRING`, `TEXT`, `CHAR(n)` | `VARCHAR` |
| `BINARY`, `VARBINARY` | `BLOB` |
| `DATE`, `TIME` | `DATE`, `TIME` |
| `TIMESTAMP`, `TIMESTAMP_NTZ`, `DATETIME` | `TIMESTAMP` |
| `TIMESTAMP_LTZ`, `TIMESTAMP_TZ` | `TIMESTAMPTZ` |
| `VARIANT`, `OBJECT`, `ARRAY` | `JSON`; columns of them as [above](#semi-structured-columns) |
| `ARRAY(type)`, `OBJECT(name type, ...)`, `MAP(type, type)` | `type[]`, `STRUCT(name type, ...)`, `MAP(type, type)` |
| `GEOGRAPHY`, `GEOMETRY` | `GEOMETRY` |

### Statements

- `CREATE [OR REPLACE] [TRANSIENT | TEMPORARY | VOLATILE] TABLE`, with
  Snowflake's types; `DATA_RETENTION_TIME_IN_DAYS` and similar options are
  accepted and ignored with a notice, `COPY GRANTS` silently (Igloo keeps a
  replaced table's grants as Snowflake's `COPY GRANTS` would). `COMMENT =
  '...'` is ignored with a notice: use `COMMENT ON`.
- Clustering keys: `CLUSTER BY (a, b)` on `CREATE TABLE` (once the table is
  created) and `ALTER TABLE ... CLUSTER BY (a, b)` set the table's [sort
  order](sql.md#sort-orders-and-clustering), and `ALTER TABLE ... DROP
  CLUSTERING KEY` resets it; table maintenance then clusters the table's
  files, as Snowflake's automatic clustering does. Keys must be columns:
  `CLUSTER BY` of expressions (`to_date(ts)`), on a temporary table or with
  `IF NOT EXISTS` is ignored with a notice. `ALTER TABLE ... RECLUSTER` and
  `SUSPEND | RESUME RECLUSTER` change nothing and say so.
- `CREATE [OR REPLACE] [SECURE] VIEW` (`SECURE` ignored with a notice),
  `ALTER TABLE ... ADD COLUMN` with Snowflake's types, `DESC[RIBE] TABLE`,
  `USE DATABASE`, `USE SCHEMA`.
- `USE WAREHOUSE`, `USE ROLE` and `GRANT USAGE ON DATABASE | SCHEMA |
  WAREHOUSE` change nothing and say so in a notice. `GRANT ... TO ROLE r`
  and `GRANT ROLE r TO USER u` are Igloo's `GRANT`.
- `ALTER SESSION SET`: `TIMEZONE` and `STATEMENT_TIMEOUT_IN_SECONDS` set
  Igloo's settings, `QUERY_TAG` the [query tag](sql.md#query-tags-and-role-settings);
  `WEEK_START`, `WEEK_OF_YEAR_POLICY`, `TIMESTAMP_TYPE_MAPPING`, the
  input formats and the output formats are accepted at the values Igloo
  follows (`0`, `TIMESTAMP_NTZ`, `AUTO`, the default output formats above)
  and refused at others; parameters about
  clients, caching and queuing change nothing. `ALTER SESSION UNSET`
  resets them.

## Known differences

What Igloo cannot reproduce exactly, and what it does instead.

**Numbers**

- `/` returns `DOUBLE`. Snowflake returns a `NUMBER` with up to 6 more
  decimal digits, rounded: `1/3*3` is `1` in Igloo, `0.999999` in
  Snowflake. Cast to `NUMBER(p,s)` where the scale matters.
- Division by zero returns `Infinity` (`NULL` for `//`); Snowflake fails.
  Use `DIV0` or `DIV0NULL` to get the same result in both.
- Integer types are `BIGINT`, not `NUMBER(38,0)`: arithmetic that leaves
  64 bits fails with an overflow error where Snowflake's 38 digits would
  hold it.
- A `FLOAT` or timestamp that becomes text without `TO_VARCHAR` or a cast,
  as in `x || 'a'` or `CONCAT(x, 'a')`, is DuckDB's text (`9244.0`, an
  offset of `+00`); `TO_VARCHAR(x)` and `x::VARCHAR` give Snowflake's.

**Semi-structured data**

- A JSON `null` read by a path is SQL `NULL` when sent to a client;
  Snowflake shows `null`. `ARRAY_CONSTRUCT(1, NULL)` shows `[1,null]`,
  where Snowflake shows `[1, undefined]`.
- A path's value used where a string is expected (`v:a = 'x'`, `UPPER(v:a)`)
  is the string itself; Snowflake compares the variant. The result is the
  same except for strings that hold JSON numbers or booleans.
- `ORDER BY`, `MIN` and `MAX` of a path order numbers before strings, each
  by value; Snowflake orders variants by type, then value, which differs
  for mixed types.
- `GROUP BY` a path groups `1` and `1.0` apart.
- Objects list their keys in alphabetical order, as Snowflake shows them;
  `OBJECT_CONSTRUCT_KEEP_NULL` and `OBJECT_INSERT` keep `null` values.
- An `ARRAY` or `OBJECT` column holds numbers and booleans as their text
  (`["1","true"]`): `TYPEOF(a[0])` is `VARCHAR` and `TO_JSON(a)` shows
  them quoted, where Snowflake shows `INTEGER` and `[1,true]`. Comparing,
  casting and summing them gives the number. Selecting such a column
  shows objects and arrays inside it as their JSON text in quotes;
  `TO_JSON(a)` and `a::VARIANT` show them as objects and arrays. Store a
  value whose types matter in a `VARIANT` column.
- A masking policy on an `ARRAY` or `OBJECT` column returns its type, as
  the column holds it: Igloo casts what its expression returns, so
  `OBJECT_CONSTRUCT(...)` works.
- `ARRAY_INTERSECTION` returns each common value once, without Snowflake's
  multiset counts.
- `FLATTEN`'s `SEQ` is `NULL`.

**Dates and times**

- Sessions start in UTC; Snowflake's in `America/Los_Angeles` unless the
  account sets another. `ALTER SESSION SET TIMEZONE = '...'` sets it.
  Until it does, a `TIMESTAMP_LTZ` shows as `2026-10-07 15:58:37.862 +0000`
  where Snowflake shows `2026-10-07 08:58:37.862 -0700`.
- `TIMESTAMP_TZ` is stored as an instant (`TIMESTAMPTZ`) and shown in the
  session's time zone: the offset it was written with is not kept.
- `CONVERT_TIMEZONE(target, ts)` with two arguments returns the wall-clock
  time in `target` without a zone (`TIMESTAMP`), not a `TIMESTAMP_TZ`.
- Times and timestamps have microseconds; nanoseconds are truncated.
- `DATEADD`, `DATE_TRUNC` and similar keep a `DATE` a `DATE`, as Snowflake
  does, except when the date is an aggregate or window function other than
  `MIN`, `MAX`, `ANY_VALUE`, `FIRST_VALUE`, `LAST_VALUE`, `LAG` and `LEAD`,
  or a statement parameter: then the result is a `TIMESTAMP` at midnight.
- Weeks start on Monday and `DAYOFWEEK` counts from Sunday = 0, as with
  Snowflake's defaults (`WEEK_START = 0`); other `WEEK_START` values are
  refused.

**Other functions**

- `HASH` returns DuckDB's 64-bit hash: as well distributed as Snowflake's,
  but different values. Do not compare them with hashes Snowflake stored.
- `RANDOM(seed)` and `UNIFORM(..., RANDOM(seed))` ignore the seed (with a
  notice); `SAMPLE ... SEED (n)` repeats.
- `APPROX_COUNT_DISTINCT` and `HLL` count with
  [DataSketches](media-analytics.md)' HyperLogLog of 4,096 registers, as
  many as Snowflake's, so their error is Snowflake's: about 1.6% (one
  standard deviation), exact up to about 300 values. Their results
  differ from Snowflake's within that error. In a select with `GROUP BY`
  and as window functions they count exactly, as `COUNT(DISTINCT)`.
  DuckDB's own `approx_count_distinct` has 64 registers and errs by about
  13%.
  `APPROX_PERCENTILE` is DuckDB's `approx_quantile`, approximate within its
  own error. `APPROX_TOP_K` counts exactly.
- `MODE` chooses among equally frequent values differently.
- `CURRENT_ROLE()` returns the user's name; `CURRENT_WAREHOUSE()`,
  `CURRENT_ACCOUNT()` and the like return `NULL`.

**Performance**

- `APPROX_COUNT_DISTINCT` and `HLL` count exactly when grouped, since a
  sketch per group is slower than `COUNT(DISTINCT x)` with many groups (13
  s rather than 0.5 s for 7.9 million groups of 37 million rows). Ungrouped,
  as when profiling columns, the sketch is as fast as DuckDB's
  `approx_count_distinct`. Grouped by a few thousand groups or fewer,
  counting exactly takes up to three times as long as DuckDB's
  approximation (0.8 s rather than 0.24 s for 270 groups of 37 million
  rows).
- Statements whose translations call the `sf_` macros (`FLATTEN`,
  `DATEADD`, `DATE_TRUNC`, `TO_DATE`, `TIME_SLICE` and others) run on
  [workers](distributed-execution.md#what-runs-distributed) like any
  other: workers define the same macros. A session that redefines one of
  them runs such statements on one machine.

**Names and catalog**

- Identifiers are case-insensitive even when quoted: `"col"` and `COL` name
  the same column, where Snowflake would see two.
- Views run with their reader's privileges, as Igloo's always do; `SECURE`
  does not hide their definition.
- `information_schema` and `SHOW` are DuckDB's: their columns and rows
  differ from Snowflake's. Igloo's own [catalog views](catalog.md) list
  tables, columns and their usage.

## Refused

These fail with SQLSTATE `0A000` and a hint:

- Stages (`@stage`, `CREATE STAGE`, `PUT`, `GET`, `LIST`, `REMOVE`): load
  files with `COPY INTO` from a location in `IGLOO_ALLOWED_LOCATIONS` (see
  [loading data](loading.md)).
- Snowflake Scripting (`BEGIN ... END`, `DECLARE`, `EXECUTE IMMEDIATE`,
  `CALL`) and `CREATE FUNCTION` or `PROCEDURE`: send the statements one at a
  time; admins can create DuckDB macros.
- `INSERT OVERWRITE`, `INSERT ALL` and `INSERT FIRST`; `ALTER TABLE ... SWAP
  WITH`; `UNDROP`; `CREATE DATABASE`, `WAREHOUSE`, `SEQUENCE`, `STREAM` and
  `DYNAMIC TABLE`; `AUTOINCREMENT` and `IDENTITY` columns; `COLLATE`.
- `CONNECT BY`, the `(+)` outer join, `MATCH_RECOGNIZE`, `CHANGES`,
  `BEFORE (...)` and `AT (STATEMENT => ...)`, `RESULT_SCAN`, `SEMANTIC_VIEW`
  and model methods (`model!PREDICT`).
- `PIVOT ... IN (ANY)`, `IN (SELECT ...)` and `DEFAULT ON NULL`: list the
  values.
- `OBJECT_CONSTRUCT(*)` and `{*}`: name the keys and values.
- Functions without a faithful translation, such as `OBJECT_DELETE`,
  `OBJECT_PICK`, `ARRAY_SORT`, `ARRAY_FLATTEN`, `PARSE_XML`, `SOUNDEX`,
  `CONDITIONAL_CHANGE_EVENT`, `ENCRYPT` and `DECRYPT`, `REGEXP_REPLACE`
  of one occurrence after the first, `SHA2` of other than 256 bits, and
  arguments Igloo must know when it translates (a format, a date part or a
  scale) given as expressions rather than literals.
- `VECTOR` columns: store embeddings as `ARRAY` or `FLOAT` columns.
- Session parameters at values Igloo does not follow (`WEEK_START = 1`,
  `TIMESTAMP_TYPE_MAPPING = TIMESTAMP_LTZ` and the like).

Functions Igloo does not know at all pass through, and DuckDB reports them
as unknown (SQLSTATE `42883`).
