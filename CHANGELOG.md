# Changelog

All notable changes to Igloo are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and Igloo uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The release workflow publishes the section whose heading matches the tag
(`## [1.2.3] - YYYY-MM-DD` for `v1.2.3`) as that GitHub Release's notes.

## [Unreleased]

First release. Move this section under `## [1.0.0] - YYYY-MM-DD` when tagging.

### SQL and tables

- A start no longer dies reading back what DuckDB held in memory. A list of 88
  GiB in ranges of up to 2.7 GiB, a view's column chunks that DuckDB kept whole,
  was read back all at once, and the kernel killed Igloo 25 s into each of 16
  starts in a row on a machine of 60 GiB. The read-back now holds at most 1 GiB
  of requests at once and leaves out ranges over 64 MiB. A start that finds the
  one before was killed during its read-back starts without the list.

- A start reads back at most a quarter of DuckDB's memory, not three quarters. A
  mixed load's first 10 minutes read 18% of a list of 22 GB, and with that load
  from the moment the replica was ready, its first minute's dashboards took 0.84 s
  at the median after a quarter came back, 1.17 s after three quarters and 0.93 s
  after none. On a machine of 60 GiB, 32.6 GB read back as users arrived had
  DuckDB's limit cut to half and raised back.

- Reads of the read cache start no threads of their own. The extension started one
  for each block of each read, warm or cold, and with dashboards, ELT and reports
  beside each other a replica of 16 cores reached 20,394 threads and a load of 240,
  and no statement finished for 20 minutes. Blocks are now read as DuckDB's tasks,
  on its threads, and `igloo_process_threads` tells how many the process has.
  Workers, which read nothing ahead, start up to `IGLOO_CACHE_READ_THREADS` a read,
  8 by default, as reading cold on their own threads took a quarter as long again.

- A small table's row groups no longer limit the threads that scan it. A table
  under 256 MiB loaded in row groups of half a million rows has few, and DuckDB reads
  each on one thread. A maintenance pass now rewrites one that has too few, into
  row groups of about a sixteenth of it, of at least 122,880 rows.

- A dashboard's first run, with no history, no longer runs its heavy statements
  together. A statement of a shape not seen yet that would wait is first estimated
  from DuckDB's plan and the tables it reads, as auto mode estimates it: one that
  holds under a 256th of the memory, such as a tile or a small query, starts at
  once, and any other waits while another such runs, for at most a minute in which
  none starts or ends. A statement whose text with its literals has not run goes by
  the most a run alike took, not the median, of which runs over values with no rows
  had been most. Its 324 queries at ten times the data, on 8 cores, took 1,787 s
  rather than 3,928, and none ran past its timeout, where three had; they used
  14,100 CPU-seconds, as after a run, where running together they used 29,400.

- A canceled statement holds nobody up. Statements behind one waited until DuckDB
  had stopped it, for its slots and its memory, and a statement of a shape not seen
  yet waited for memory for as long as what held it ran: behind a report estimated
  at 16 hours, it waited before it was even planned for workers. Canceled, past its
  timeout or with its client gone, a statement now gives up its place at once, and a
  shape not seen yet waits for memory for at most a minute.

- A statement stops when its client goes away. Nothing read a client's
  connection while its statement ran, so a report whose client was killed went on
  spilling, from 302 GB to 355 GB in two minutes, until Igloo was restarted. The
  server now looks at the sockets of sessions running a statement every second
  and cancels the statement of one that closed, as a cancel request does, on this
  replica, in the second DuckDB, on workers and while its rows stream.

- `CALL dbgen` from sessions at once no longer crashes the server. DuckDB's data
  generators keep their state in the process, and two at once ended it with a
  segmentation fault. One statement of a process may call them at a time, the
  others fail with SQLSTATE `55P03`, and a macro, view or prepared statement may
  not keep them for later. Only admins could call them, and still only they can.

- Models trained and scored in SQL, as Snowflake's ML classes:
  `SNOWFLAKE.ML.FORECAST` and `ANOMALY_DETECTION` (Snowflake's `fast`
  method, many series, exogenous columns, intervals from
  cross-validation), `CLASSIFICATION` (`m!PREDICT(INPUT_DATA => {*})`) and
  `TOP_INSIGHTS` (`GET_DRIVERS`), their methods by `CALL` or as
  `TABLE(m!...)`, and `SHOW`, `DROP` and `GRANT` of models; and
  Databricks' `ai_predict_class` and `ai_predict_value`, which train and
  score in one query (see [ML functions](docs/ml-functions.md)). Igloo's
  engine trains gradient-boosted trees in Go, repeatably; training reads
  its input as its user, policies applied, and `PREDICT` runs on workers.
  A model's row is in the metadata database and its blob in the
  warehouse.
- BigQuery ML's model types as Igloo's own classes, with the same
  statements: `IGLOO.ML.REGRESSION` and `IGLOO.ML.CLASSIFICATION` (boosted
  trees, random forests, linear and logistic regressions),
  `IGLOO.ML.DNN_REGRESSOR` and `DNN_CLASSIFIER` (networks trained by Adam,
  with dropout and early stopping), `IGLOO.ML.AUTOML_REGRESSOR` and
  `AUTOML_CLASSIFIER` (a cross-validated search over those within a time
  budget, `TRIAL_INFO`), `IGLOO.ML.KMEANS` (`CENTROIDS`, and
  `DETECT_ANOMALIES` by distance), `IGLOO.ML.PCA` (projections,
  components, and anomalies by reconstruction error),
  `IGLOO.ML.MATRIX_FACTORIZATION` (explicit and implicit feedback by ALS,
  `RECOMMEND`) and `IGLOO.ML.AUTOENCODER` (latent spaces,
  `RECONSTRUCTION_LOSS`, anomalies). `CONFIG_OBJECT` takes BigQuery's
  options and the models' methods are its functions: `PREDICT`,
  `EXPLAIN_PREDICT`, `DETECT_ANOMALIES` and `RECOMMEND` in a query or as
  tables of the input's rows, scored on workers, and `EVALUATE`,
  `CONFUSION_MATRIX`, `ROC_CURVE`, `GLOBAL_EXPLAIN`, `FEATURE_IMPORTANCE`,
  `WEIGHTS`, `TRAINING_INFO` and the like. Inputs above 10 million rows
  are sampled repeatably.
- Snowflake's `VECTOR(FLOAT | INT, n)` type and `VECTOR_COSINE_SIMILARITY`,
  `VECTOR_INNER_PRODUCT`, `VECTOR_L1_DISTANCE` and `VECTOR_L2_DISTANCE`,
  which Igloo refused. A vector is DuckDB's fixed-size array; an Iceberg
  table's column of one, or of any fixed-size array, is a list
  (`list<float>`), since DuckDB-Iceberg has no fixed-size arrays (see
  [Types](docs/snowflake-sql.md#types)).
- `ANALYZE table [(column, ...)]` counts the distinct values of a table's
  columns and keeps the counts in the table, where DuckDB's planner finds
  them (see [Distinct counts](docs/sql.md#distinct-counts)). DuckDB
  estimated any equality filter on an Iceberg table at a fifth of its rows,
  and so built joins' hash tables on the wrong side: with counts, most of
  the queries of a report workload that ran slower than Snowflake at ten
  times its data run faster.
- A Snowflake `FLATTEN` of a list computed from one table's columns, such
  as `LATERAL FLATTEN(INPUT => SPLIT(t.tags, ','))`, read only by its
  `VALUE`, is unnested in that table's select list, not joined to its rows
  as a lateral subquery, which DuckDB planned as a dependent join holding
  every row in a hash table: 98 s against 41 s for a report over tens of
  millions of rows. A `SPLIT`'s parts are unnested as text. So is a
  `FLATTEN` of a column whose `WHERE` cannot become its join's `ON`, which
  DuckDB joined the same way. A `FLATTEN` of a column whose `VALUE` alone is
  read unnests its values alone, and as text when every read casts them to
  text or tests them for NULL, without making JSON of each element and
  reading it back.
- `trim`, `ltrim` and `rtrim` of one argument run Igloo's own code, with
  DuckDB's results (see the rb extension's
  [README](extensions/rb/README.md#faster-trim)). DuckDB's `trim` took some
  300 ns a row, and reports trim the values they filter and group by: a
  filter of tens of millions of trimmed values took 4.9 s and takes 0.65 s.
- Snowflake's dialect makes Snowflake's implicit conversions: string
  functions and `LIKE` read numbers, dates and timestamps as text
  (`TRIM(5)`, `LENGTH(ts)`), and text compared with a timestamp, a date or
  a number is cast to it (`ts_text BETWEEN start_ts AND end_ts`). A
  statement that binds as translated is unchanged; one DuckDB cannot bind
  for want of such a cast is translated again with the conversions
  (`igloo_snowflake_implicit_conversions_total`). In an `ORDER BY`
  expression a select list's alias now comes before a column of the same
  name, as in Snowflake, which also fixes "ambiguous reference" errors for
  a name both joined tables have. `ARRAY_FLATTEN` is translated, and
  `CREATE VIEW db.schema.v ("COL" COMMENT $$...$$, ...)`, as dbt-snowflake
  writes views with documented columns, no longer fails to parse.
- A prepared `CREATE TABLE ... AS` a client runs again makes the table it
  says. DuckDB 1.5.6 runs a prepared CREATE as written only the first time,
  and from the second on makes the table with no columns, so that the next
  statement to read it fails with "the FROM clause is missing". Clients run
  a prepared statement again whenever they reuse one: psycopg does with any
  statement it has run five times, and so do JDBC drivers. Igloo prepares a
  CREATE statement afresh each time it runs again.
- A schema created after a statement looked for it can be used. DuckDB's
  Iceberg catalog remembers a schema as missing once a lookup of a table in
  it found neither, until it restarts, in every session and whoever creates
  the schema afterwards: `SELECT * FROM lake.staging.t`, then `CREATE SCHEMA
  lake.staging`, left `staging` missing on that replica for good. When a
  statement fails for want of a schema the catalog has, Igloo now makes
  DuckDB forget it and prepares the statement again.
- SQL endpoint that speaks the PostgreSQL wire protocol (simple and extended
  query protocols, prepared statements, transactions, query cancel), so
  `psql`, JDBC, psycopg, pgx and BI tools connect unchanged. Compatibility
  with the PostgreSQL catalog queries of `psql` meta-commands, JDBC metadata
  calls and schema browsers.
- Apache Iceberg tables through the Lakekeeper REST catalog, on Amazon S3 and
  S3-compatible stores, Google Cloud Storage and Azure Data Lake Storage Gen2:
  `CREATE [OR REPLACE] TABLE`, `CREATE TABLE AS`, `INSERT`, `UPDATE`,
  `DELETE`, `MERGE`, schemas and time travel. Igloo bootstraps the catalog
  and creates its warehouse on start.
- Views on the Iceberg catalog (`CREATE [OR REPLACE] VIEW`, `DROP VIEW`),
  stored as Iceberg views and kept in sync across replicas. Replacing a view
  may change its columns and the tables it reads. An unqualified name in a
  view is the view's own schema's table or view, as in Snowflake, whatever
  the search path of the session that created or reads it; a view a replica
  cannot load fails with the reason when read.
- `COMMENT ON TABLE`, `VIEW` and `COLUMN`, stored as Iceberg metadata and
  shown through `obj_description`, `col_description`, `pg_description`,
  `igloo_comments()`, and `information_schema.tables` and `.columns`.
- SQL cursors (`DECLARE`, `FETCH`, `CLOSE`) and array parameters, so
  psycopg's server-side cursors and Ibis's PostgreSQL backend work.
- `RESET ALL` and `DISCARD ALL`, which PgBouncer (its default
  `server_reset_query`) and drivers' connection pools send to clean a
  session for its next client, in either dialect. `RESET ALL` puts every
  setting a session can change back to what it had at login: DuckDB's
  session settings, the search path (the catalog's default schema),
  `statement_timeout`, `statement_queued_timeout`, `query_tag`,
  `sql_dialect`, `distributed_execution`, the `igloo_` switches, the AI
  settings, `application_name` and SQL variables, with the user's and
  roles' settings applying again. `DISCARD ALL` also closes cursors and
  drops prepared statements and temporary objects, keeping those every
  session starts with, and runs only outside a transaction block, as in
  PostgreSQL. `DISCARD TEMP` drops temporary objects alone. `DISCARD PLANS`
  and `DISCARD SEQUENCES` succeed and change nothing: DuckDB keeps no plans
  but prepared statements, and no sequence values per session. DuckDB
  refuses `RESET ALL` and does not parse `DISCARD`, so all of these failed
  before. What an admin set for the whole server, with `SET GLOBAL` or an
  unscoped `SET` that DuckDB applies globally, stays, as `ALTER SYSTEM`
  settings do in PostgreSQL.
- `pg_advisory_unlock_all()` and `UNLISTEN`, so asyncpg's connection pool
  can clean a session it takes back with its reset query (`SELECT
  pg_advisory_unlock_all(); CLOSE ALL; UNLISTEN *; RESET ALL;`), in either
  dialect. Igloo has no advisory locks and no `LISTEN`, so both succeed and
  change nothing. DuckDB has no `pg_advisory_unlock_all()` and does not
  parse `UNLISTEN`, so the query failed before. `LISTEN` is refused as not
  supported (`0A000`) instead of failing as a syntax error.
- Semi-structured columns. Lists, maps and structs are Iceberg's own
  `list`, `map` and `struct`, which Spark, Trino and PyIceberg read. A `JSON`
  or `VARIANT` column, which Iceberg tables of format version 1 and 2 cannot
  hold ("Type JSON is not a valid Iceberg Type"), is a `VARCHAR` column of
  its values' JSON text, in `CREATE TABLE`, `ALTER TABLE ... ADD COLUMN` and
  `CREATE TABLE ... AS` alike; JSON inside a list, map or struct is a string
  of its JSON text. In Snowflake's dialect an `ARRAY` column is a list of
  strings (`VARCHAR[]`) and an `OBJECT` column a map of strings
  (`MAP(VARCHAR, VARCHAR)`), each element as Snowflake's `TO_VARCHAR`
  writes it, as Snowflake converts them to `ARRAY(VARCHAR)` and
  `MAP(VARCHAR, VARCHAR)`; structured `ARRAY(type)`, `OBJECT(name type,
  ...)` and `MAP(type, type)` are lists, structs and maps of their types,
  and `CREATE TABLE ... AS` makes `ARRAY` and `OBJECT` columns of
  `ARRAY_AGG`, `OBJECT_CONSTRUCT` and the like.
  Paths, `FLATTEN`, `GET`, `TO_JSON`, `OBJECT_CONSTRUCT`, casts and the
  other semi-structured functions read them as Snowflake's values, objects
  inside arrays included, and read JSON text as before; masking policies
  written in Snowflake's dialect may return `OBJECT_CONSTRUCT(...)` for an
  `OBJECT` column.
- Zero-copy clones, with Snowflake's syntax: `CREATE [OR REPLACE] TABLE
  ... CLONE source [AT (TIMESTAMP | SNAPSHOT | BRANCH | TAG => ...)]` and
  `CREATE [OR REPLACE] SCHEMA ... CLONE source`, its tables and views. A
  clone is a new Iceberg table whose metadata reads its source's data and
  delete files, so it takes moments and no storage, however large the table;
  the two then change independently. Cloning a table needs `SELECT` on it
  and `CREATE` on the clone's schema. A clone carries its source's masking
  and row access policies and tags, gets no grants but a schema clone's
  schema-wide ones, and is recorded in the query history with its source.
  Table maintenance never deletes a file a clone still reads, and through
  the Iceberg REST catalog other engines cannot either: a table whose files
  clones read is not purged (409, naming the clones), write credentials
  for it delete nothing, and clones get no storage credentials.
- Iceberg branches and tags: `ALTER TABLE ... CREATE [OR REPLACE] BRANCH |
  TAG ... [AT (...)]`, `DROP BRANCH`, `DROP TAG`, `SHOW BRANCHES ON`,
  reading them with `AT (BRANCH => ...)` and `AT (TAG => ...)`, and `ALTER
  TABLE ... FAST FORWARD main TO branch`. Branches are read-only in Igloo,
  as DuckDB writes only to `main`; tags keep their snapshots from expiring.
  Table maintenance keeps branches and tags until they are dropped, or as
  long as their own or the table's `history.expire.max-ref-age-ms` says, as
  Java's Iceberg does: iceberg-go dropped every one older than the snapshot
  retention, seven days by default, a clone's too. A max-ref-age-ms of
  Java's `Long.MAX_VALUE` reads as forever.
- ID domains, off by default (`IGLOO_ID_DOMAINS`): `CREATE ID DOMAIN people
  (VARCHAR) TAG pii = 'person_id'` makes a dictionary that gives the values
  of identifier columns, across tables, dense integer ids below 2^32 that
  are never handed out twice, and `ALTER TABLE ... ALTER COLUMN c SET ID
  DOMAIN people` makes a column, which must carry the domain's tags, a
  member, with a `_igloo_id_c` column beside it for its ids. Writes keep
  the ids right: an `INSERT` that names no columns names the others, an
  `UPDATE` or `MERGE` that sets a member column sets its ids to `NULL`,
  and a `MERGE ... UPDATE BY NAME` into a member table is refused. Igloo
  fills the ids: `INSERT`, `COPY INTO` and `CREATE TABLE ... AS` write
  those the dictionary has and give new values theirs before they return,
  under a per-domain lease held only for the dictionary's append (writes
  of fewer rows than `IGLOO_ID_SYNC_MIN_ROWS`, `0` by default, leave that to
  a pass about 30 seconds later that they share), and maintenance fills what
  other writes left `NULL`; a fill of a million rows or more, such as a
  table's first, writes their files again in their rows' order with the ids
  added, so a table laid out by a column keeps its layout, where an `UPDATE`
  appended the rows again in its join's order and a filter on that column
  went from reading a few row groups to all of them. A query's
  `count(DISTINCT t.c)` of a member column counts its ids instead, at a
  snapshot of the table that has every row's, with the same result, when
  no policy applies to the table and nothing in the query could show the
  ids. Counting
  distinct people over integers, and keeping sets of them as bitmaps, is
  what the heaviest audience reports spend their time on; the work that
  fills the ids and counts through them follows.
- Erasure requests cover what Igloo derives from ID domains' ids, such as
  bitmap indexes: a request looks up the ids the dictionaries gave the
  person's values, before its first `DELETE` and again under the domain's
  lease just before its last delete from the dictionary, keeps them sealed
  with the identifiers and forgets them when it ends, has the derived sets
  erase them at the end of its expire step, and counts in its verification
  the sets still holding one (`id_sets`), which sends it back to expire.
- A star in a view over a member table of an ID domain leaves out the
  column of ids when the view is created and whenever a replica loads it,
  as in statements, so a column list names the columns it means and the
  catalog lists them. Views recorded with that column among theirs load
  without it, with the names they meant.
- Bitmap indexes, off by default (`IGLOO_BITMAP_INDEXES`, with ID domains):
  `ALTER TABLE t ADD BITMAP INDEX name ON (segment) FOR person_id` keeps a
  roaring bitmap of a member column's ids for each value of up to four other
  columns (`ON ()`, one for the whole table) in Igloo's own table
  `igloo_bitmaps.name`, which no user's statement reads or changes and which
  counts as protected wherever Igloo serves tables or files. Igloo brings
  the bitmaps to the table's current snapshot in the background after writes
  and at each maintenance pass: appends add the appended files' bitmaps,
  compaction and clustering keep them, anything else builds them again, and
  a snapshot whose values are not all mapped to ids waits for mapping. `SHOW
  BITMAP INDEXES` lists them and the snapshot each is of. Counts of
  distinct members by an index's columns, alone or within another index's
  set (as audience-by-segment reports count), read the bitmaps where the
  bitmaps are current and no policy applies, through views that select or
  LEFT JOIN the table too, and run again from the tables if the bitmaps
  outgrow their memory. Erasure requests empty the indexes whose bitmaps
  hold an erased person's id, with their older snapshots and files, and have
  them built again. `ON (ELEMENTS(list))` keeps a bitmap for each
  element of a list column (see [Bitmap indexes](docs/sql.md#bitmap-indexes)).
- The report API, version 1: `SELECT * FROM igloo.audience_profile('<spec>')`
  counts the people of labelled cohorts (an audience's latest version, the
  general population, or any query of members) in each value of a table's
  dimensions (columns, mapped columns, list or JSON array elements,
  constants), with per-value rows, sums, averages and distinct (member,
  column) pairs, totals across values and the cohorts' sizes, from a JSON
  spec: the counting that audience reports write as SQL.
  `igloo.audience_profile_sql` prints the SQL a call stands for, which Igloo
  runs in its place, so grants, policies, errors and execution are that
  SQL's; `igloo.audience_profile_check` compares a call with the caller's
  own SQL, and `igloo.report_api_versions()` lists the versions. Where the
  tables are an ID domain's members, a call joins and counts their ids, and
  where bitmap indexes hold all a call counts, it reads their bitmaps, and
  where they hold its people, it reads those and scans the source for the
  rest (see [Report API](docs/media-analytics.md#report-api)).
  With `IGLOO_REPORT_REWRITE=true`, reports that count as the API does
  without calling it get its answers too: each common table expression that
  counts cohorts' people by the values of a table's dimensions, in DuckDB's
  dialect or Snowflake's translated, becomes a call with the same columns
  and rows before the statement is checked, and anything Igloo cannot map
  with certainty, or a statement that then fails, runs as written (see
  [Reports as written](docs/media-analytics.md#reports-as-written)). Only
  expressions whose calls count ids or read bitmap indexes are replaced: one
  whose call would count the tables' values, and compute its cohorts again
  besides, stays as written, and so does a statement that would run on
  workers as written but not with its calls. A
  session turns recognition off for itself with `SET igloo_report_rewrite =
  off`, and its reads of bitmap indexes with `SET igloo_bitmap_indexes =
  off`.
- Snowflake's SQL dialect, opt-in per session (`SET sql_dialect =
  'snowflake'` or `ALTER SESSION SET`), per connection (the startup option
  `-c sql_dialect=snowflake`) or per user and role (`ALTER USER | ROLE ...
  SET sql_dialect`). Igloo translates each statement to DuckDB's dialect
  before it checks and runs it, so the checks of grants and policies see
  only the DuckDB statement: Snowflake's string escapes, `$$` strings and
  `//` comments, session variables (`SET x = ...`, `$x`), `IDENTIFIER()`,
  semi-structured paths (`v:a.b[0]::string`), `FLATTEN`, `SPLIT_TO_TABLE`,
  `GENERATOR`, `QUALIFY`, `MINUS`, `TOP`, `SAMPLE`, `PIVOT`, `ASOF JOIN ...
  MATCH_CONDITION`, `LIKE ANY`, Snowflake's types, `CREATE OR REPLACE
  TRANSIENT TABLE ... CLUSTER BY` (ignored with a notice), `ALTER SESSION`
  parameters, NULL ordering, result column names in upper case, and some
  200 functions (`IFF`, `NVL`, `DECODE`, `DATEADD`, `DATEDIFF`,
  `DATE_TRUNC` keeping dates dates, `TO_CHAR` and `TO_DATE` with
  Snowflake's formats, `TRY_` conversions, `LISTAGG`, `ARRAY_AGG`,
  `OBJECT_CONSTRUCT`, `PARSE_JSON`, the `REGEXP_` family, `TIME_SLICE` and
  more). Constructs it cannot translate faithfully are refused with
  SQLSTATE `0A000` and a hint; docs/snowflake-sql.md lists what is
  translated, the known differences and what is refused.

### AI functions

- Snowflake's `AI_COMPLETE`, `SNOWFLAKE.CORTEX.COMPLETE`, `PROMPT` and
  `AI_COUNT_TOKENS`, with the customer's own keys: `CREATE AI INTEGRATION`
  reaches OpenAI and the servers that speak its API (Azure OpenAI, vLLM,
  llama.cpp, Ollama, OpenRouter), Anthropic, Gemini, Vertex AI, Amazon
  Bedrock or a decision endpoint, with its keys encrypted, `MODELS`
  mapping the names SQL uses (an unmapped name fails), and `GRANT USAGE ON
  INTEGRATION` deciding who may call it; off until one exists. Calls run
  in Igloo's own process, through its egress guard, on the coordinator,
  never through a DuckDB extension. Each distinct input is sent once per
  statement, and answers are cached across statements for 30 days
  (`IGLOO_AI_CACHE_RETENTION`), so a rerun costs nothing; `refresh =>
  TRUE` asks again. A statement makes at most 1,000 calls costing at most
  $1 unless a role's `ai_max_calls` and `ai_max_cost_usd` say otherwise,
  `EXPLAIN` says what it would send and cost, and the history and
  `igloo.usage` record calls, tokens and cost by query tag. Erasure
  requests purge the cached answers derived from their tables, and
  complete only once none remain (see
  [AI functions](docs/ai-functions.md)).
- The task functions: `AI_CLASSIFY` (`{"labels": [...]}`, single or
  multi-label), `AI_FILTER` (BOOLEAN), `AI_SENTIMENT`, `AI_EXTRACT`,
  `AI_SUMMARIZE`, `AI_TRANSLATE`, `AI_REDACT` (redact or detect, Snowflake's
  13 categories), `AI_EMBED` (vectors for `VECTOR` columns, 64 texts a
  request) and `AI_SIMILARITY`, with Snowflake's result shapes and error
  semantics, and `SNOWFLAKE.CORTEX`'s `CLASSIFY_TEXT`, `SENTIMENT`,
  `EXTRACT_ANSWER`, `SUMMARIZE`, `TRANSLATE`, `EMBED_TEXT_768`,
  `EMBED_TEXT_1024` and `COUNT_TOKENS`. The Snowflake dialect's
  translation tests a filter's plain conditions before its AI predicates,
  which DuckDB would otherwise call for every row.
- Decision models: an integration's `DECISION_MODEL` (OpenAI's Decisions
  API, or an endpoint of the state-and-questions shape) answers
  `AI_FILTER` and `AI_CLASSIFY` with probabilities, kept from the
  session's `ai_decision_threshold`; `AI_DECIDE(state, questions)` returns
  Databricks' `ai_decide` shape, and `AI_COMPLETE` with a model mapped to
  it Snowflake's `snowflake-decision` shape.
- `AI_AGG(expr, task)` and `AI_SUMMARIZE_AGG(expr)` answer a task over a
  group's values however many there are: chunks of about 16,000 tokens,
  each answered with an intermediate result, combined until one request
  gives the answer, each request cached by its chunk.
- The model pack: `igloo models install` installs open-weight models
  (Chronos-Bolt, Chronos-2, TimesFM 2.5) into `IGLOO_MODEL_DIR`, each file
  pinned by SHA-256 to an upstream revision and checked as it downloads
  and again before a process uses it; only Apache-2.0 and MIT models are
  installed or loaded, and nothing is downloaded at query time. A mirror
  (`IGLOO_MODEL_MIRROR`) or a local directory can stand in for Hugging
  Face (see [the model pack](docs/models.md)).
- Igloo runs Chronos-Bolt, Chronos-2 and TimesFM 2.5 itself, in Go, from
  their published weights: no Python, GPU or runtime library, the same on
  amd64 and arm64 (NEON and AVX2 matrix products), and within 0.01% of
  the official implementations' forecasts.
- Forecasting with those models: `FORECAST` and `ANOMALY_DETECTION` take
  one as their `method` (`{'method': 'chronos-2'}`, zero-shot, with the
  model's quantiles for intervals), and BigQuery's `AI.FORECAST` and
  `AI.DETECT_ANOMALIES` and Databricks' `ai_forecast` and
  `ai_detect_anomalies` run in both dialects with their arguments and
  columns. On workers, a stage that holds whole series forecasts them; the
  distributed planner now combines `list()` and `array_agg()` from
  workers' partial lists.
- Hosted time series models through AI integrations, with your own
  credentials: `nixtla` (TimeGPT, also as Microsoft Foundry serves it),
  `sagemaker` (Chronos endpoints), `bigquery` (its `AI.FORECAST`, the way
  to TimesFM 3.0, sending the series to BigQuery), and TimesFM endpoints on
  `vertex` and Chronos on Bedrock Marketplace. `AI.FORECAST`,
  `ai_forecast` and their detection functions call a model an integration
  maps on the replica, in batches, cached, with the AI functions' grants,
  caps and usage. A name the model pack has always means the pack's model;
  `'integration/name'` names a hosted one, and `EXPLAIN` says which a name
  resolved to and whether its series leave Igloo.

### Loading and scheduling

- `COPY table FROM STDIN`, in PostgreSQL's text, CSV and binary formats, so
  psql's `\copy`, psycopg's `copy`, pgx's `CopyFrom` and a `pg_dump`'s data
  load. It is the user's own `INSERT`, checked before the client sends the
  rows, and fails whole with the line and column of a value that does not
  convert. psql loads a dump's settings too: `set_config('search_path', ...)`
  and the settings Igloo has no use for, as `row_security`.
- `DROP SCHEMA ... CASCADE` drops a schema of the catalog with its tables,
  views, materialized views and dynamic tables, as their own `DROP`s would,
  having first checked that the user may drop all of them.
- `COPY INTO table FROM 'location'` loads CSV, TSV, Parquet, JSON and
  NDJSON files, gzip- or zstd-compressed or not (detected from the name or
  the file), from the warehouse and `IGLOO_ALLOWED_LOCATIONS`: a prefix, a
  glob or one file, filtered by `PATTERN`. Columns match by name or
  position; `CREATE_TABLE` creates the table from the files;
  `ON_ERROR abort | skip_file | continue` stops, skips bad files, or skips
  and counts bad rows. The files of one load commit together.
- A load history (`igloo.load_history`) records each file loaded into each
  table, by path, size and time, so a file is loaded once unless it changes
  or `FORCE` is given; a load cut short by a crash is marked `unknown`
  rather than repeated.
- Pipes (`CREATE PIPE ... AS COPY INTO ...`) load new files every minute or
  on a schedule. Event notifications are not supported yet.
- Tasks (`CREATE TASK ... SCHEDULE ... AS statement`, `ALTER TASK ...
  RESUME | SUSPEND | SET SCHEDULE`, `EXECUTE TASK`, `DROP TASK`,
  `SHOW TASKS`) run one statement on an interval or cron schedule in any
  time zone, on one replica at a time, never overlapping, with their runs
  in `igloo.task_history`.
- Materialized views (`CREATE [OR REPLACE] MATERIALIZED VIEW ... [SCHEDULE]
  AS query`, `REFRESH MATERIALIZED VIEW`) keep a query's result as an
  Iceberg table, refreshed in one transaction on demand or on a schedule.
- Incremental refresh: a materialized view's refresh reads only what
  changed in its tables since the last refresh, as their Iceberg snapshots
  tell (data files added and removed, rows deleted), for filters,
  projections and inner joins of appended rows, and `GROUP BY` with
  `count`, `sum`, `avg`, `min`, `max` and `count(DISTINCT)` (kept in
  roaring bitmaps), rows deleted or updated included for counts, sums and
  averages. After a day's load into a 6.2-million-row fact table, a
  refresh read 1.3 MB of files where a full one read 41 MB, in 15 ms of
  CPU against 382 ms. Results equal a full refresh's exactly: every table
  is read at the snapshot the refresh records. Anything else refreshes in
  full, and `SHOW MATERIALIZED VIEWS` and the run's `detail` in
  `igloo.task_history` say why (see [incremental
  refresh](docs/loading.md#incremental-refresh)). What these read may be a
  `WITH` clause, a subquery in `FROM` or a `UNION ALL`; a query that filters
  and computes from one aggregate's rows is that aggregate, and `UNION` is
  `SELECT DISTINCT` over `UNION ALL`.
- Snowflake's dynamic tables: `CREATE [OR REPLACE] DYNAMIC TABLE ...
  TARGET_LAG = '...' | DOWNSTREAM WAREHOUSE = ... [REFRESH_MODE = AUTO |
  FULL | INCREMENTAL] [INITIALIZE = ...] AS query`, `ALTER DYNAMIC TABLE
  ... SUSPEND | RESUME | REFRESH | SET TARGET_LAG`, `DROP` and `SHOW
  DYNAMIC TABLES`, which Igloo refused, refreshed within their lag the
  same way, in both dialects; `WAREHOUSE` is accepted and ignored with a
  notice (see [dynamic tables](docs/loading.md#dynamic-tables)).
- Tasks, pipes and refreshes run as their owner, with the owner's role and
  grants at the time they run; scheduling needs the writer role. When a
  user is dropped, their tasks, pipes, materialized views and alerts are
  suspended and marked as a dropped user's, so a user created later with
  the same name does not inherit them.
- With scale to zero, Igloo reports when work is next due on
  `GET /activity`, and the gateway starts the compute for it.
- Several replicas can serve the same tables; conflicting concurrent writes
  are retried automatically.
- Arrow Flight SQL on port 31337 (`IGLOO_FLIGHT_ADDR`): queries, updates,
  prepared statements with parameters, catalog metadata and cancellation,
  with results streamed as DuckDB's own Arrow record batches, for the ADBC
  driver (pandas, polars), pyarrow and the Flight SQL JDBC driver. Pulling 3
  million TPC-H lineitem rows into Python is 29 times faster than with
  psycopg as Arrow, and 14 times faster as a pandas DataFrame
  (`bench/extract.py`). Users sign in with their Igloo password or an access
  token, sent in the handshake or as each call's bearer token; statements
  go through the same authorization, statement timeout and query history as
  SQL clients. Without TLS it serves only clients at localhost. The Compose
  stack publishes it on loopback, the Helm chart on the SQL Service
  (`flightSQL.enabled`), and the scale-to-zero gateway forwards it
  (`IGLOO_GATEWAY_FLIGHT_BACKEND`).
- Media analytics functions, from DuckDB extensions baked into the image:
  HLL and theta sketches (datasketches) that can be stored in tables and
  merged into deduplicated reach and audience overlaps across campaigns,
  platforms and days; geometries, points in polygons and distances
  (spatial), with geometries sent to clients as WKT; and the H3 grid (h3).
  See docs/media-analytics.md for reach, overlap and frequency examples and
  DuckDB's approximate, statistical and time zone functions. With spatial
  loaded, DuckDB 1.5.6 cannot plan some filters on a temporary table's
  `rowid`, such as `rowid % 3 = 0`; they fail with SQLSTATE 0A000 and what
  to write instead, not DuckDB's internal error and stack trace.
- Connections to PostgreSQL and MySQL databases (`CREATE`, `ALTER`, `DROP`
  and `SHOW CONNECTIONS`): each replica attaches them read-only as catalogs
  of their own, such as `crm.public.customers`, which queries join with
  Iceberg tables, with filters and columns pushed down. Credentials are
  stored encrypted or read from files of `IGLOO_CONNECTION_SECRETS_DIR`,
  never shown again, and rotate without a restart; a new connection is
  attached when each replica next starts. A database that is down or stops
  answering fails only the queries that read it. Connections verify their
  server's certificate and host name by default (`tls 'verify-full'`);
  the modes that do not are explicit choices that draw a warning, and
  `require` checks the certificate's chain.
- Erasure requests rewrite a table's files within `IGLOO_MAINTENANCE_MEMORY`,
  one file at a time, as compaction does, instead of reading them all at
  once in full-size batches; a request whose rows do not fit stops with the
  reason rather than running Igloo out of memory.
- A connection whose database cannot be reached when a replica starts (no
  route, refused, timed out, name not resolved), as when the machine's
  network is not up yet after a restart, is tried again for up to 30
  seconds before it is left out until the next restart.
- Connections to Snowflake (`CREATE CONNECTION ... TYPE snowflake`), signed
  in with a key pair (`private_key_secret`, `private_key`) or a password,
  with a warehouse and role. Igloo reads them through a gateway of its own,
  with Snowflake's Go driver: tables are listed with `SHOW` commands, at no
  warehouse cost, and each scan becomes a Snowflake `SELECT` of the columns
  it needs, with its filters and `LIMIT`, whose Arrow result streams into
  DuckDB as it downloads, so `CREATE TABLE AS` copies large tables into
  Iceberg without holding them in memory. Filters compare strings byte by
  byte, as DuckDB does, whatever a column's Snowflake collation, so row
  access policies hold. `count(*)` is counted in Snowflake.
  `query_timeout` stops queries on Snowflake too. Grants,
  policies, the query history and `ALTER CONNECTION ... REFRESH` work as for
  other connections; credentials stay in Igloo, encrypted or in files. An
  `ARRAY` column reads as a `VARCHAR[]`, so `CREATE TABLE AS SELECT *` copies
  it into an Iceberg `list<string>`; `VARIANT` and `OBJECT` read as their JSON
  text.
- Connections to other Iceberg REST catalogs (`CREATE CONNECTION ... TYPE
  iceberg`): Snowflake-managed Iceberg tables through Snowflake's Horizon
  Catalog, Databricks Delta Shares served to Iceberg clients and Unity
  Catalog's UniForm tables, Apache Polaris, AWS Glue and S3 Tables, other
  Lakekeepers, read in place, read-only, with the same grants, policies and
  history as other connections. Igloo signs in with a bearer token, an
  OAuth2 client (programmatic access tokens included) or AWS Signature
  Version 4, with credentials stored like passwords, and reads files with
  the storage credentials the catalog hands out, or a static one. DuckDB
  never sees the catalog's credentials, reaches no other endpoint than
  Igloo's proxy, and may read only the locations of the tables found when
  the replica started, which must not overlap the warehouse or the allowed
  locations. Igloo's own storage credentials now apply only to the
  warehouse and the allowed locations.

### Alerts and notifications

- Notification integrations (`CREATE [OR REPLACE] NOTIFICATION INTEGRATION
  ... TYPE webhook | slack | teams | email`, `ALTER`, `DROP`,
  `SHOW INTEGRATIONS`), created by admins, with `GRANT USAGE ON INTEGRATION`
  to roles and users. Webhooks take `HEADERS` and a signing `SECRET`
  (HMAC-SHA256 of the timestamp and body); Slack and Microsoft Teams
  incoming webhooks get their own payloads; email goes through
  `IGLOO_SMTP_HOST` to `IGLOO_SMTP_ALLOWED_DOMAINS` only. URLs, headers and
  secrets are stored encrypted, never shown, and scrubbed from errors,
  logs and the query history.
- Alerts, with Snowflake's syntax: `CREATE [OR REPLACE] ALERT ... SCHEDULE
  ... IF (EXISTS (query)) THEN NOTIFY integration [MESSAGE '...'] [WITH
  RESULT]`, `ALTER ALERT ... RESUME | SUSPEND | SET SCHEDULE`,
  `EXECUTE ALERT`, `DROP ALERT`, `SHOW ALERTS`. They are scheduled work like
  tasks: run as their owner with the owner's grants and policies, skipped
  while the owner is disabled, at most once a minute, and the
  scale-to-zero gateway starts Igloo for them. A user owns at most 50.
  Messages take `{{column}}` values from the first row, up to 4 KiB;
  results are capped at 20 rows, 30 columns and 16 KiB, and values from
  data are escaped so they cannot mention people or make links in Slack or
  Teams. Each integration sends from an outbox of its own, so a slow
  destination holds up neither the scheduler nor other integrations.
- `igloo.alert_history`: every alert run, built-in alert and message, with
  its state (`condition_false`, `triggered`, `condition_failed`,
  `action_failed`, `skipped`), integration, attempts and error. Admins see
  everything, others their own.
- `CALL SYSTEM$SEND_NOTIFICATION('integration', 'message')`, from tasks or
  any session, with `USAGE` on the integration.
- Built-in operational alerts admins switch on per integration
  (`OPERATIONAL_ALERTS`): failed scheduled runs, `COPY INTO` and pipe load
  errors, pipes with no new files for N hours, statements running longer
  than N minutes, failed-login bursts from the login limiter (for
  addresses and existing users, at most 5 in 15 minutes from a replica,
  never at the other alerts' expense), and table maintenance failures, each
  sent at most once per problem in a while, and tried again 5 minutes after
  a failed send.
- Notifications reach only public addresses (no loopback, private,
  link-local or metadata, multicast or reserved addresses, in IPv4 or
  IPv6), checked again at every connection against DNS rebinding, over
  TLS, without redirects or proxies, unless `IGLOO_NOTIFICATION_ALLOW_HOSTS`
  names the host. Sends are rate-limited per integration, time out, and are
  retried with backoff; `igloo_notifications_total` counts them.
- The web console's Tasks page lists alerts, with their runs from
  `igloo.alert_history`.

### Distributed execution

- A commit no longer makes the next statements over its table wait to plan. The
  table's metadata loads again in the background as it changes, where the next
  lookup of it waited 0.3 to 1.2 s for that load, and dashboards' cheap statements
  gave up planning for workers and ran on the replica: 17 of 19 such give-ups in a
  mixed window came within 4.4 s of a commit of the ELT.
- A slow worker no longer holds up every stage. The coordinator sends a stage's
  tasks to workers as they have room, rather than all at once, so a slow or busy
  worker runs fewer of them. A report on 16 workers took 573.9 s at the median
  against 677.7 s; with one worker at half speed, 626.2 s against 1,048.1 s; and a
  worker powered off cost it 1.4% against 3.5%.
- TPC-DS q09, q10, q16, q32, q35, q69, q92 and q94 run on workers, where they stayed on
  one machine. A scalar subquery in the columns of a statement that does not group, one
  aggregate of all the rows of a large table, is a relation of one row that the workers
  compute. An `OR` of `EXISTS` subqueries correlated by the same columns is one semi
  join to the union of their keys. An `EXISTS` or `NOT EXISTS` may also compare its
  columns to the statement's otherwise (`ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk`),
  which the join checks beside its keys. A correlated subquery, and a `LEFT JOIN`, of
  a statement that joins several tables divide its rows by the table the condition
  reads.
- Auto mode estimates a statement's time on one machine by what each of its
  operators takes, not by its hash tables' rows alone: scans by their rows and
  width, joins by the rows probed, built on and made, aggregates by their rows,
  groups and distinct or median-like aggregates, sorts and windows by their rows'
  order, and UNNEST by the rows it makes, a microsecond and more each where the
  statement writes the list, as FLATTEN of a row's object does. Of the estimate
  corpus's statements that took 2 s or more alone, 58% are now estimated within
  twice what they took on a first run, where 8% were; a report's profile of an
  audience, which FLATTENs people's objects and took 7 s alone, is estimated at
  3.7 s, where it was at 1.9.

- Auto mode estimates a join to a lookup's latest version, `ver = (SELECT max(ver)
  FROM versions)`, by the rows of the row groups that hold that version: Iceberg's
  bounds name the few files that reach it, and their Parquet footers the row
  groups. A lookup of 1.41 billion rows over versions 0 to 2 had 189,400 rows of
  version 2, in 2 files of 19.7 million rows and row groups of 384,204. Taking a
  third of its rows, by the versions' distinct count, put a report's working set
  at 191 GB where it held 19 to 20 GB; it is now estimated at 15 GB. Columns of
  `DECIMAL(p,0)`, as a Snowflake `NUMBER` of integers arrives, have bounds as
  integer columns do.

- Auto mode carries a statement's run on one number of workers over to others by
  its fixed costs first and its work after. TPC-DS q95 at a hundred times the data
  took 4.8 s on 16 workers; scaling its whole estimate by that run made 8 workers
  look as fast for less, and it took 14.7 s there. The estimates' fixed costs (8 s
  to start, 5 s a level) are what they overstate most for such statements. Carried
  over this way, two reports' runs on 16 workers predict their runs on 8, 4, 2 and
  1 within a factor of 1.18 at the root mean square, against 1.25 before, and q95
  stays on 16.

- Auto mode estimates what workers read for parts that write no exchange, which
  send their rows or partial aggregates to the replica, at the replica's cold rate
  per core, not 0.4 of it. On 16 workers just started, their disks empty, a
  dashboard's first four tiles of 16 GB took 12 to 15 s at once. Auto mode had
  estimated such a tile at 72 s on workers against 99 s on one machine, and kept
  its first run on the replica for 42 s and 844 CPU-s. It now estimates the tile
  at about 50 s and runs it on workers.

- Auto mode no longer gives up planning a statement for workers while its session
  waits for the catalog lock. A refresh beside a fleet's dashboards took the lock
  alone 12 times for up to 13 s, and 275 dashboard statements counted that wait
  as planning that made no progress and ran on one machine: a tile that took 10
  to 14 s on workers took 75 to 88 s there, and loaded the coordinator for
  everything else. `planned a statement` logs the wait.

- Statements that masking and row access policies apply to run on workers and in the
  replica's second DuckDB, for every user, where they ran only in the first DuckDB of
  the replica, so a team whose main tables have policies had neither. Every stage reads
  a protected table through the subquery Igloo writes for its policies, and the tables
  a row access policy reads only inside it; the planner checks each stage for that, from
  the tables' files, before any runs, so no exchange, and nothing a worker sends the
  coordinator, holds a masked column's raw value or a hidden row. A join or window that
  would partition such a table's rows, or a join of it to itself, partitions the rows the
  subquery makes instead. Across 17 statements and five users of a table with both kinds
  of policy, results match one machine's, and every statement that runs on workers over
  the table without policies does over it with them. A table a clean room keeps to its
  party stays on the replica, in its first DuckDB.
- Statements on workers return the rows one machine does in cases they did not. TPC-DS
  q44 returned no rows where one machine returned 10: a stage kept the groups a later
  ranking could keep before `HAVING`, which read a subquery, filtered them. A ranking's
  cut also fed window functions over the groups, and `ORDER BY` a name that was both an
  alias and a column ordered by the column. Every early cut now waits for what SQL
  applies before it. Tests compare all of TPC-H and TPC-DS, and random queries in the
  shapes the planner rewrites, with one machine's rows; see [same results as one
  machine](docs/distributed-execution.md#same-results-as-one-machine). TPC-DS q33, q56,
  q58, q60 and q83 run on workers too, where they stayed on the coordinator, and so does
  q24, which failed on workers when the coordinator had 16 cores.
- Variances and standard deviations (`stddev_samp`, `stddev_pop`, `var_samp`,
  `var_pop`, `stddev`, `variance`) are computed on workers: each part's count, mean
  and variance merge pairwise, as DuckDB merges its threads', which keeps their digits
  near a large mean where sums of squares would lose them. They kept the statement on
  the coordinator.
- A worker that stops answering is found in about 7 seconds, not a minute. Powered off,
  closing no socket, one of 16 workers took 69 s to find. Whoever waits on a worker,
  the coordinator on its tasks and readers on its exchange files, now asks it for its
  status every second and takes it for lost after three answers missed, of 2 s each,
  when its machine takes no connection either. One busy past answering, whose machine
  still takes connections, is lost only after a minute without an answer.
  And only what it held runs again: the tasks it ran of exchanges still to be read, those
  whose inputs it also held before them, and the readers that failed, rather than all
  of the statement's parts, unless those tasks could write something else each time.
  Each runs as smaller tasks spread over the workers left, as does a task whose worker
  is lost while it runs it, rather than as one on one of them.

- `ROLLUP`, `CUBE` and `GROUPING SETS` in a subquery or common table expression
  run on workers, as they do in a statement, where they left it to the
  coordinator.
- Workers read nothing ahead unless their `IGLOO_PREFETCH` says `true`. On a fleet
  reading cold, each task its share of the files on many threads, reading ahead
  saved no time beyond what runs differ by, and nearly half of what it fetched
  went unread. One machine still reads ahead by default, where it helps.
- Reads in order larger than memory sort in runs also with parameters, for a
  client that reads a few rows at a time through a portal (JDBC's fetch size),
  by expressions, and with columns the merge cannot compare or pass on, as
  `HUGEINT`, `UUID`, `INTERVAL`, lists, structs and maps. Arrow Flight results
  stream, computed as the client takes them, where they were held whole first.
  See [reads in order larger than
  memory](docs/operations.md#reads-in-order-larger-than-memory).
- Tables bucketed by `UUID` columns stay in place too, and those written by
  different engines no longer meet by bucket. DuckDB hashes a `UUID` otherwise
  than Iceberg's spec, so a table's files hold it in a bucket by the hash of the
  engine that wrote them, which their bounds tell.
- More shapes of statement run on workers, and return the rows of one machine.
  A correlated subquery over a large table is the join it means, by partition:
  `EXISTS` a semi join, `NOT EXISTS` an anti join, a scalar subquery of `min`,
  `max`, `sum`, `avg` or a count a `LEFT JOIN` to its groups. `INTERSECT`,
  `EXCEPT` and `UNION` of distinct rows compute partition by partition rather
  than on the coordinator, also in a subquery. `SELECT DISTINCT` completes its
  groups on workers. `USING` and `NATURAL` joins are read by the columns they
  name, outer ones among them, which left their largest side to the coordinator.
  A join inside a subquery on a column other than the statement's key is a
  relation. A scan that looks up three characters or more of a column's text
  (`LIKE '%word%'`, `contains`) runs on workers, as one by a constant does.
  See [what runs on workers](docs/distributed-execution.md).
- A statement that fails as its rows stream no longer reports that it was
  canceled when nothing canceled it. DuckDB's thread that reads the rows can
  report an interrupt in place of another thread's failure, which clients saw
  as SQLSTATE 57014 and Igloo as a cancel. Before a row has gone out, a
  statement that only reads runs again, read whole, which reports the real
  error, or returns the rows if it does not fail again. After that, it fails
  with SQLSTATE XX000 and says that it was not canceled.
- DuckDB's freed buffers go back to the system: Igloo fixes glibc's `mmap`
  threshold at 128 KiB, where it had risen to as much as 32 MiB with the
  largest allocation freed. After a sort that returned its rows a process kept
  0.5 GiB where it had kept 3.4 GiB, with the same CPU seconds over TPC-H,
  alone and with sessions at once. See
  [sizing](docs/operations.md#sizing).
- Bucketed tables stay in place beside what is not bucketed. A join of a table
  bucketed by the join column to another table, or to a relation workers
  compute, shuffles the other side into the table's buckets, by the key's
  Iceberg bucket, and leaves the bucketed table's rows where they are, where
  both were exchanged. A subquery read once that keeps the key as it is, by
  filtering and projecting a bucketed table, joining bucketed tables on the
  key, or grouping one with the key among its groups, is read by bucket where
  it stands. A key computed from the column, or counts that do not divide,
  are exchanged as before. `EXPLAIN DISTRIBUTED` says what is written to
  buckets. See [bucketed tables](docs/distributed-execution.md#bucketed-tables).
- A `SELECT ... ORDER BY` larger than memory on one machine sorts in runs and
  merges them as the client reads them: its rows go to files in the spill
  directory, each file is sorted within memory, and the merge streams them.
  DuckDB's own sort of such a read goes up to 40% past its memory limit as it
  returns rows, which is the memory watch's to cancel. On 72 million rows
  under a limit of 9.7 GiB the process peaked at 3.5 GiB, where it had peaked
  at 10.6 to 13.9 GiB, in the same time, with the first row after 6 seconds.
  See [reads in order larger than
  memory](docs/operations.md#reads-in-order-larger-than-memory).
- Workers complete an aggregation's groups whenever they may be many (a
  `GROUP BY` of a large table's column, unless `ANALYZE`'s distinct counts
  put the groups at 10,000 or fewer), partitioned by group, counts of
  distinct values (in roaring bitmaps too) by value and then by group, and
  keep what `HAVING`, `ORDER BY ... LIMIT` and rankings of the groups keep
  (`QUALIFY row_number() OVER (...) <= k`, also through `SELECT *`
  unions): the coordinator receives the result's rows and only projects,
  filters and orders them, where it combined every task's partial results
  of every group. A report shaped like the one that did not finish at a
  hundred times the data on 16 workers (its groups ranked on the
  coordinator, 55.6 GB of memory and 91 GB of spill) has the coordinator
  receive 6 KB at test scale and 19 KB at four times that. A result ordered
  by `ORDER BY` without `LIMIT` is sorted by each task and merged by the
  coordinator, a batch of rows a task in memory, rather than sorted there.
  Window functions that partition their rows by the same columns run on
  workers, partitioned so, under `QUALIFY` or a later filter (see [what
  runs distributed](docs/distributed-execution.md#what-runs-distributed)
  and [window functions](docs/distributed-execution.md#window-functions)).
- Tables partitioned by `bucket(n, column)` (`CREATE TABLE ... PARTITIONED
  BY`, `ALTER TABLE ... SET PARTITIONED BY`, or Snowflake's `CLUSTER BY
  (BUCKET(n, column))`) are read by bucket, without an exchange, where a
  part joins them on their bucket columns, one count dividing the others,
  groups by the column, counts its distinct values or partitions windows by
  it; `EXPLAIN DISTRIBUTED` says so, and why bucketed tables are exchanged
  all the same (see [bucketed
  tables](docs/distributed-execution.md#bucketed-tables)). Maintenance
  compacts each bucket's files into files of that bucket.
- More statements run on workers: `TABLESAMPLE` of a percentage of the
  large table (Snowflake's `SAMPLE ROW` and `SAMPLE BLOCK`), sampled where
  each task reads it, the same rows again with a seed; HyperLogLog
  sketches (`APPROX_COUNT_DISTINCT`, `HLL_ACCUMULATE`, `HLL_COMBINE`),
  sketched by each task and merged in the final stage; a plain `SELECT ...
  WHERE` that filters the large table by a column equal to a constant or a
  list, also as a subquery the coordinator reads; and scalar subqueries
  in an aggregation's select list, `HAVING` and `ORDER BY`, which the
  coordinator computes over the combined groups. A statement that only
  spells a protected table's name, or reads a view that does, as a column
  named like an ID domain did, runs on workers as written.
- Reports' last steps run on workers too, rather than on the coordinator
  over what workers sent: an `ORDER BY` without `LIMIT` that what reads it
  sorts again or aggregates is left out; inputs too large for every
  worker that are joined on `upper`, `lower` or `trim` of a text column
  are partitioned by it; a `UNION` of distinct rows is a relation workers
  compute; the large side of a `LEFT`, `RIGHT` or `FULL` join is read by
  partition with the other side partitioned alike; a `LIMIT` without
  `ORDER BY` takes any rows, its tasks stopped once those that ended sent
  enough; and a subquery whose every row the coordinator reads is divided
  where its filters or columns keep less than its table (see [what runs
  distributed](docs/distributed-execution.md#what-runs-distributed)).
- Auto mode picks how many workers a statement runs on: of 1, 2, 4 and so
  on up to `IGLOO_WORKER_MAX`, and the workers running, the cheapest of
  those whose time is within a fifth of the fastest, so never one that
  another beats on both time and cost. A way's cost counts the workers'
  seconds at their price (`IGLOO_WORKER_PRICE_PER_HOUR`), with the start
  and idle tail of workers it starts, and the replica's for as long as it
  runs (`IGLOO_PRICE_PER_HOUR`); where idle workers do not stop alone,
  every running worker's. Times a statement took on a number of workers
  stand for the estimates, and a number estimated near the fastest it ran
  on is tried once a day: at ten times the data, a report that took 112 s
  on 8 workers and 127 s on 16 runs on 8, where an earlier rule took 4, 200
  s, for an estimate within a fifth of 16's; another that took 44 s on 16
  and 75 s on 8 runs on 16. The workers' estimates count a statement's
  levels of stages (5 s each) rather than its stages, and read at 0.4 of
  one machine's rate, within a factor of 1.35 of 15 runs on 1 to 16
  workers where they were within 2.05. Of more running, it takes those with
  the fewest tasks. `EXPLAIN DISTRIBUTED` shows the choice (`fleet`) and
  the ways weighed (`options`), and `query_history.placement` where a
  statement ran and why (see [when](docs/distributed-execution.md#when)).
- A report whose aggregates and windows were estimated just past the
  replica's room now counts as spilling on its first run, and runs on
  workers: estimated at 41 GB, 1.1 times the room, so fitting, at 47 s, it
  held 51 GB, spilled 35 GB, and the guard moved it after 88 s. Estimates
  within half again the room count as fitting only in the share joins'
  hash tables hold, whose estimates run high. And a completed run of a
  statement's shape that held 90% or more of the replica's limit no longer
  proves it fits: it may have spilled any amount.
- Auto mode no longer tries a statement of five minutes or more on a
  number of workers estimated past a fifth of its best way's time, to
  measure it, and a first run takes the best way: an ordered result of 361 million rows that
  took 470 s on 16 workers was tried on one, and took 621 s, slower than
  one machine. The estimates of fewer workers count their shares of a sort
  spilling past their memory, as one worker's of that 41.6 GB sort did.
- Auto mode's estimates count the client's taking a result's rows, either
  way, where one machine's estimate of 361 million ordered rows was 30 s
  against 516 s; a sort or window that holds half of the replica's room or
  more runs on workers, where configured, unless they take a quarter
  longer than one machine, as DuckDB's sort exceeds its limit by about 40%
  and that one finished within 2 GB of the memory watch; and the reason
  for running on the replica names the rule that decided, where "one
  machine is faster: one machine should take 30 s and workers 22 s" did not.
- A statement that fills a table from a query, `CREATE [OR REPLACE] TABLE
  ... AS` (dbt's table models too), `INSERT INTO ... SELECT` and a
  materialized view's or dynamic table's full refresh, runs its query on
  workers where auto mode would run it there as a read, by the same
  estimates, history, grants and policies, the replica writing the table
  from their results: a `count(DISTINCT)` of a billion IDs into a table,
  which the memory watch canceled on the replica, sends it a thousand rows.
  Auto mode counts the replica's writing of the rows on both sides, and
  their fetch from the workers, so a copy stays on the replica unless
  workers are clearly faster. The guard, a run out of memory, the failure
  history and the fallbacks work as for reads, and a fill whose workers run
  out of memory goes on in passes.
- Workers write tables: where the tasks of such a fill hold its rows, as a
  copy's do, each writes its own as Parquet with the table's field IDs and
  partitions, under a directory of the statement's under the table's
  location, and the replica commits them all in one Iceberg commit, without
  fetching a row, so auto mode runs a copy where workers write it faster.
  Credentials limited to a task's directory, with no right to delete, for an
  hour (STS on S3, `IGLOO_S3_STS_ROLE_ARN`), or on Google Cloud and Azure an
  identity that can only create files in directories of that name; the Helm
  chart's `workers.serviceAccount` and the OpenTofu modules now give workers
  an identity of their own, without write access to tables, and the chart
  turns worker writes on only then (`workers.writes`,
  `IGLOO_WORKER_WRITES`). Before it commits, the replica checks that each file
  is under the attempt's directory, and reads its footer: rows, size and
  schema must be what the worker said. Only the winning attempt's files count,
  a failed or canceled statement removes what it wrote, and looks again a minute
  later for what its tasks finished meanwhile, the table's next write removes
  what a stopped process left, and erasure requests treat what is
  left as the table's orphan files. A worker refused the write leaves it to
  the replica. Tables partitioned by bucket or by a decimal stay with the replica. A task
  bounds its row groups by its share of the worker's memory: rows of 10.8 KB,
  which row groups of 122,880 rows took a worker of 1 GB out of memory with, now
  take about a third of it. Auto mode takes rows too wide for a task to the replica, and a
  fill that runs out of memory on workers runs there, or with the replica writing,
  rather than fail. What is committed is the replica's own copy of each file, in
  a directory no worker's grant names, read and checked there, with a footer that
  can say no more than it holds; what a task's statement left unfinished in the
  store's multipart uploads is aborted when it ends, and an erasure request does
  not complete while any could hold its rows. Workers also write `INSERT ... BY
  NAME` and of some of a table's columns, partitioned views' refreshes, and tables
  with NOT NULL or nested columns.
- DuckDB gets no more threads than leave each 125 MiB of its memory limit, its own
  minimum, however many cores the machine has. A pod of 1 GiB on 16 cores ran 16
  threads under a limit of 512 MiB, which held 190 to 270 MiB past the limit as it
  spilled, took seven times as long, and had the memory watch cancel about one run in
  three.
- Workers exchange rows on their own disks by default on Kubernetes too
  (`workers.exchanges: auto`), where every worker of a statement has room on its spill
  volume: 1.8 to 2.8 times faster than object storage on EC2 with NVMe. Object storage
  takes them otherwise, or with a spill volume in memory.
- Workers are on by default in the Helm chart and the OpenTofu modules, as they scale
  from zero and stop when idle, so a deployment with no heavy queries pays nothing
  for them. Single-VM stays as it was, as it would need more machines.
- Igloo starts up to 16 workers by default, where it was 4 (`IGLOO_WORKER_MAX`,
  `workers.maxReplicas`, `workers_max`): a report at ten times the data took 200 s
  on 4 and 112 s on 8 at about the same cost. Auto mode weighs how many a statement
  should run on, and idle workers stop on their own.
- A statement whose last runs keep it on the replica no longer plans for workers
  again, so that planning it does not take longer than it does.
- DuckDB's memory limit, where `IGLOO_DUCKDB_MEMORY_LIMIT` does not set it,
  is what Igloo's memory leaves after the eighth it keeps free before it
  lowers DuckDB's limit, its reserve and an eighth of the limit itself,
  about 73% of it, where it was DuckDB's own 80% of the machine (and the
  Helm chart's 80% of the container's limit): finishing a `count(DISTINCT)`
  it had spilled, DuckDB holds about 0.1 GiB a thread beyond its limit,
  and a `count(DISTINCT)` of a billion IDs into a table on a replica of
  61.6 GiB took 5 GiB more within a second, and was canceled with 2.6 GiB
  left. With 4.1 GiB less for DuckDB, it would have left 6.7.
- Auto mode no longer takes a run cut short on the replica, as one the
  guard stopped, for proof that a statement's shape fits there: such a
  run, learned at all of a replica's 53 GB from the query history, made a
  report whose plan's working set was about three times that count as
  fitting, and it was tried on the replica once a day, estimated at 47 s,
  spilling 54 GB until the guard moved it after 145 s. Nor is a statement
  tried on the replica where its working set is four or more times its
  room, however much slower it ran on workers than the replica's estimate.
- The large worker no longer runs a statement whose working set would
  spill there before it ran there, while the small workers can run it: at
  a hundred times the data, a report whose working set was 5.3 times the
  large worker's room, estimated at 217 s there spilling 2.5 times as
  long, ran past 1,800 s there, where 16 workers ran it in 729 s. The
  choice's reason and `EXPLAIN DISTRIBUTED` name the large worker passed
  over.
- A heavy statement no longer takes the replica's cores from short ones
  where workers are configured, with nothing to set: auto mode weighs the
  replica's time at as many times as long as the statements running on it
  beside the one it plans, up to four, so a heavy statement started among
  dashboards' runs on workers; and the guard moves a heavy statement that
  runs on the replica to workers once it has run its estimate while others
  run beside it, weighing it again every second, where it had waited
  three times its estimate times the statements running when it started,
  and so never fired under load. At ten times the data, beside 16 users'
  dashboards and two ELT streams, a report that had run in 25 s on the
  replica ran 577 s there, the dashboards' median rising from 4 ms to 23 s.
- A relation of uncertain size, as an audience filtered to one version of
  a lookup, whose tables' statistics bound it under what every worker may
  read (a `GROUP BY` or `DISTINCT` of columns by their distinct counts,
  from `ANALYZE` or Iceberg's theta sketches; a filter by `=` by the
  column's) is measured with the workers to itself the first time, as one
  measured small lately is, rather than beside the tables' partitioning to
  meet it: at a hundred times the data, a report took 305 s with that
  partitioning and 50 s without.
- A statement that fails on a number of workers, or on the large worker, is
  taken to need one and a half times as long as it ran there before it
  failed, for a day, so auto mode weighs other ways against it; the query
  history records where it failed, which replicas read when they start
  (a worker's running out of memory no longer reads back as the replica's).
  A statement whose large worker fails runs on the other workers where its
  working set by DuckDB's plan is four or more times the coordinator's
  room, rather than on the coordinator, and one whose workers fail fails
  there rather than run on the coordinator, whatever a run of its shape
  allocated there at a tenth of the data: at a hundred times the data, a
  report of 250 GB ran on the coordinator after its large worker failed,
  spilled 167 GB and was killed after 9 minutes.
- A compute pool may have a large worker (`IGLOO_LARGE_WORKER`, the chart's
  `workers.large`), on which auto mode runs a statement alone when it fits
  there and is estimated within a fifth of the fastest way and cheapest,
  planned again for its memory; erasure requests purge its read cache as
  the other workers' (see [a large
  worker](docs/distributed-execution.md#a-large-worker)).
- Auto mode estimates a statement's working set from DuckDB's own plan of
  it (`EXPLAIN`): joins' build sides, groupings' groups no more than the
  distinct counts of their keys allow (ANALYZE's, or Iceberg's theta
  sketches), distinct aggregates, sorts and windows, at their columns'
  widths, with inner joins' rows from their keys' distinct counts, and
  corrected by statements' peaks on the replica. Over TPC-H at three times
  its scale it is 2.9 times off at the geometric mean, where six times what
  the stages read was 37 times off (see
  [when](docs/distributed-execution.md#when)). Where DuckDB's plan ran far
  off at ten times the data, Igloo corrects it: a `UNION`'s branches hold
  no more than the largest; an inner join to keys DuckDB knows nothing of
  keeps the larger side's rows its other side's filters keep, not a fifth;
  a join to a scalar subquery's one row keeps as a filter by a constant; a
  cross product with a small source, as `FLATTEN` becomes, makes every
  pair; and `OVER ()` windows buffer their rows once. Three reports
  estimated at 793, 25 and 439 GB that held 33, about 106 and 9.5 GB are
  estimated at 46, 215 and 14 GB. The plan also gives one machine's
  reading, the columns its scans project (9.6 GB where the stages named 77
  GB of a view's table), and its hash tables' time, at 10 million rows a
  core-second past the processor's caches. A working set estimated within
  half again the replica's room counts as fitting, as the estimates ran up
  to half again high where nothing spilled: the first report, which held
  33 GB and took 98 s on the replica, is estimated at 46 GB and 127 s
  there, without spilling, where it was estimated at 2,939 s and sent to
  workers; the second, at 215 GB, still runs on workers.
- Where no worker price is set, auto mode prices workers and the large
  worker by their instance types' list prices from a built-in table of AWS,
  Azure and Google Cloud machines, on Spot and by region
  (`IGLOO_WORKER_INSTANCE_TYPE`, `IGLOO_WORKER_SPOT`, `IGLOO_CLOUD_REGION`;
  set by the chart and the OpenTofu modules; see
  [prices](docs/distributed-execution.md#prices)).
- While some workers work, idle ones past `IGLOO_WORKER_WARM` (one) stop
  alone: retired, not ready, then the Deployment scaled down past them or
  their EC2 instances stopped, only where their caches go with them or
  were purged for erasure requests (see [idle
  workers](docs/distributed-execution.md#idle-workers)).
- Auto mode's estimates of one machine spill by a curve fitted to the scale
  runs, the working set over the memory for it to the power of 0.55, within
  a factor of two for ten of eleven reports, where spilling was held to twice
  as long; past twelve times the memory a machine is taken to run out of
  it. `EXPLAIN DISTRIBUTED`'s `spill` rows show each machine's working set,
  room and spill.
- The OpenTofu modules can provision workers (`workers_enabled`), on Spot
  capacity by default with an on-demand fallback: the cluster autoscaler's
  priority expander on AWS and Azure, a compute class on GKE Autopilot. A
  harness test interrupts a worker's Spot Instance mid-statement: its tasks
  run elsewhere and the statement finishes with the same rows.
- Workers stop after an idle tail sized to their start
  (`IGLOO_WORKER_IDLE_AFTER=auto`, the default): three times what their
  last start took, from 2 to 10 minutes, where every burst of work kept
  them up 10 minutes. On EC2, a worker watches for AWS's two-minute notice
  that it is taking a Spot Instance back (`IGLOO_WORKER_SPOT_NOTICE`), takes
  no more tasks from then on, which coordinators send to other workers, and
  finishes those it runs until ten seconds before the time AWS gave; the
  chart gives workers a 45-second grace period
  (`workers.terminationGracePeriodSeconds`; see [Spot
  workers](docs/distributed-execution.md#spot-workers)).
- A statement canceled, or past its timeout, while the coordinator plans
  it ends with that error. Planning took the session's failure to describe
  a part in time for a reason to run the statement on one machine
  ("cannot distribute: describe: context deadline exceeded").
- Report API calls whose cohorts come from bitmap indexes run on workers.
  They join their source to each cohort's members, listed from the bitmap
  the replica computes first (`unnest(rb_to_list(...))`), and a relation of
  no table kept them on one machine, which at a hundred times the data runs
  out of memory where the report's own SQL runs on workers. A subquery or
  common table expression whose FROM clauses read no table, with tables
  only in subquery expressions, and no volatile function, is now computed
  by every task that reads it, beside its share of the divided table: the
  scan of the call's source is split among the workers, the bitmaps cross
  as computed values, and the cohorts' checks read their small tables in
  full. Calls through ids from the cohorts' tables ran on workers already.
- `EXPLAIN DISTRIBUTED` decides as running the statement does. It planned
  the statement as prepared, where a run prepares it again through ID
  domains' ids and bitmap indexes and counts in roaring bitmaps as set
  for it, and it checked neither the tables' current snapshots nor, as
  workers do once they have their tasks, that a nested column's nesting
  is known: a signals report said `runs on workers` and ran on one machine.
  It now readies the statement as its run does, its bitmaps' variables
  set, and loads and checks the snapshots as running does, and the planner
  refuses a table whose nested columns' nesting it is not told. Report
  recognition's guard, which keeps a report as written when it would run
  on workers as written, asks the same way.
- A worker that hangs no longer hangs its statements, and a C++ exception
  that escapes DuckDB no longer hangs a worker. Two workers whose memory ran
  out hit "terminate called after throwing an instance of
  'duckdb::OutOfMemoryException'": C++ aborted, Go's signal handler took
  the abort on one of DuckDB's threads, and the process neither exited nor
  went on, for over half an hour, while Docker restarted nothing and the
  coordinator waited on the tasks they held. Every Igloo process now ends
  at once with status 134, after one line naming the exception's type, when
  an exception escapes DuckDB or an extension, so that Docker or Kubernetes
  restarts it. The coordinator asks a worker running one of its tasks for
  its status every 10 seconds, and takes one that misses three answers in a
  row for unreachable, running its task on another worker, which the
  statement's other tasks then go to first; workers' connections to the
  coordinator and to each other give up a TLS handshake after 10 seconds.
- Queries larger than one machine: a statement whose largest table would
  take a replica more than a few seconds to scan runs in parts on worker
  machines (`igloo worker`). Each worker reads a share of the table's data
  files and every smaller table it is joined to, and returns partial results
  that the coordinator combines; aggregates such as `count`, `sum`, `min`,
  `max` and `avg`, aggregates over `DISTINCT` values, `GROUP BY` (with
  `ROLLUP`, `CUBE` and `GROUPING SETS`), filters, top-N and joins to smaller
  tables are covered. A join of the large table to a second table too large
  for every worker to read is partitioned by its key through object
  storage. Results are those of one machine. Igloo decides by itself when
  to distribute; `SET distributed_execution` forces it on or off for a
  session.
- Aggregates over `DISTINCT` values complete on workers: their partial
  results are partitioned by group and value through object storage, a
  worker per partition counts its values, and the coordinator adds up one
  row per group and partition instead of receiving every distinct value.
  Several at once work when one argument fixes the others, as
  `count(DISTINCT user_id)` next to `count(DISTINCT (user_id, day))`; in a
  join partitioned on the counted value no exchange is needed. The top
  groups of an aggregation by a column of a divided table
  (`GROUP BY device_id ORDER BY spend DESC LIMIT 10`) are completed and cut
  on workers too.
- Statements built in steps run step by step on workers: a part may read
  common table expressions and subqueries that workers compute (grouping,
  `DISTINCT` and `UNION ALL` included), whose rows go to object storage
  keyed by the column the reader joins or groups on. Joins of large
  intermediate results, `UNION ALL` of large branches feeding one
  aggregation, and a large table joined to itself run distributed; a
  common table expression is computed once, however many parts read it.
  Aggregates over all of a step's rows (`min(day) OVER ()`),
  uncorrelated one-row subqueries in filters over a divided table
  (`ver = (SELECT max(ver) FROM t ...)`) and `x IN (SELECT y ...)` over
  large tables, as semi joins partitioned by `y`, run on workers too, and so do aggregates that do not compute in parts (`median`,
  `quantile_cont`, `string_agg`) over a step partitioned by their groups.
  A common table expression one part reads that only filters tables is
  read in that part's place, the tasks that read a one-row subquery
  combine its partial results themselves, and a step computed partition
  by partition that keeps its rows' partitions is computed by the one step
  that reads it, so no stage only copies rows from one exchange to
  another.
  Cohort reports (distinct people per cohort, joined to their events and
  counted per signal) run on workers up to the signal-level tail.
  A join of large inputs inside a subquery that passes its rows through to
  an aggregation (`SELECT ... FROM (SELECT ... FROM a JOIN b ON ...) AS x
  GROUP BY ...`) is partitioned where it is, and aggregated where its rows
  are made.
  A stage that reads small exchanges by partition runs fewer tasks, each
  over several partitions, as many as fit in a task's share of a worker's
  memory at the memory their rows take in hash tables (by their columns'
  types, at least 64 bytes a row), and at least one for each task the
  workers run at once: a report's stages that joined an audience of
  189,000 rows to tables every task read in full ran 32 tasks each, and
  read those tables 448 times. A statement whose exchanges' rows, as
  estimated, would not fit a task's memory over eight times the workers'
  partitions gets more, up to 256, by the width of the columns its stages
  write, and no more than leave each writer files of a million rows: a
  report at ten times the data took 256 partitions where 32 had sufficed,
  by the width of every column its rows were made of, and ran in 362 s
  instead of 153.
  Workers writing an exchange to object storage upload its files in parts
  of 5.25 MiB: DuckDB holds a part's buffer for every open file, 76.5 MiB
  by default, and a task over 243 partitions held 18 GiB for files of a few
  megabytes, so that workers of 24.6 GiB running two such tasks ran out of
  memory at ten times the data, and again in smaller tasks. A filter that
  matches a column against a constant pattern (`LIKE`, `ILIKE`, `GLOB`)
  counts in the estimates exchanges' partitions are sized by, as a fifth of
  the table's rows. Workers keep an exchange's row groups small enough for
  one of every partition to fit a quarter of a task's share of their
  memory: a partitioned write holds a row group of each partition until it
  fills, and 128 partitions of 400 strings a row in groups of 122,880 took
  a 3 GB worker out of memory. Their threads gather fewer
  rows before writing them when rows are wide, and a statement whose
  exchanges' rows are very wide (hundreds of columns) gets no more
  partitions than its writers have room for.
  A task that runs out of DuckDB's memory on a worker counts as the memory
  watch's cancel does, not as a broken worker: the statement's parts run
  again in smaller tasks (twice the partitions, half of each task's share),
  up to twice, and then fail with SQLSTATE `53200`. When the task read one
  partition, which smaller tasks would not divide, the partitions grow
  fourfold, up to 1,024, as far as leaves each writer files of a million
  rows: a report's tasks over one partition of 52.5 million rows ran out of
  memory on every attempt at 202 partitions until it ran out of time. A statement whose
  workers fail runs on the coordinator only if it fits there: one whose
  exchanged rows would take more than the coordinator's memory, or that ran
  out of it before, fails at once, where it ran on the coordinator for half
  an hour at a hundred times the data and ran out of memory there too. Its
  workers stay up while it runs on the coordinator.
- A relation of uncertain size, one filtered by a constant, a subquery's
  value or `IN`, as an audience is, is written and measured on workers
  before any table is partitioned to meet it, and the statement is planned
  again with its size: if its hash table fits a quarter of what a task may
  plan on, and building it in every task costs less than partitioning the
  table it meets, it is read whole beside the tables' shares, and the
  tables are not partitioned; otherwise it is read by partition as
  planned, from what was written of it. An audience report partitioned 465
  GB of tables to meet its audience at a hundred times the data, in 925 s
  on 16 workers. The rest of the plan runs while the relation is measured,
  but for what reads it, and what started is canceled if the plan changes.
  A relation that came out large is planned as large, without measuring it
  first, for a day; one that came out small is measured first, with the
  workers to itself, and read whole. A statement that runs out of memory on
  a worker after measuring runs again with the sizes measured.
- `IGLOO_EXCHANGES=local` makes workers exchange rows through their own
  disks rather than the warehouse's object storage: a task writes its
  partitions to its worker's spill disk, room allowing (twice what it
  expects to write, with what other tasks reserved, within a quarter of
  the free space), and the tasks that read them fetch the files from that
  worker over the task port, whole. In a lab, a stage of 3 GB over 128
  partitions took 17.5 s instead of 54 s through an object store answering
  in 25 ms. A worker serves only the files it wrote, to holders of the
  worker secret, and readers fetch only from the statement's workers; a
  worker lost with files on its disk (not answering, or restarted) makes
  the statement's parts run again, once, without it; files are removed
  when the statement is done, or after six hours unused. The default stays
  `object`. The Helm chart's `workers.exchanges: local` sets it, lets the
  workers reach each other's task port, and splits their spill volume
  (60% spill, 30% exchanges, `IGLOO_EXCHANGE_MAX_SIZE`).
- Distributed `count(DISTINCT)` of 32-bit unsigned integers sends roaring
  bitmaps rather than values where the session counts with them
  (`rb_count_distinct` `always`, with integer IDs): each worker
  sends a bitmap of its values a group, and the coordinator, or a worker
  completing a partition of the groups, counts their union.
- The rb extension's aggregates distribute as aggregates. The planner's own
  DuckDB loads no extensions, so it took `rb_agg(member)` for a scalar
  function: a GROUP BY around it ran on one machine, and in a report that
  counts distinct (member, day) pairs in two levels the workers exchanged
  every row rather than their groups' bitmaps, several times slower than
  one machine. The planner now learns the aggregate functions of the
  extensions the server loads from the first session that plans a
  statement; `rb_agg`, `rb_or_agg` and `rb_count_distinct(id)` combine as
  unions of the workers' bitmaps (`rb_or_agg`), written before any
  exchange. `rb_count_distinct(id, key)`, which has no partials, stays on
  one machine rather than have workers exchange every row for it. A
  `count(DISTINCT)` of IDs selected past a star (`SELECT s.*, CAST(s.id AS
  UINTEGER) AS member`), as a report call whose cohorts come from bitmap
  indexes selects its source's rows, crosses as bitmaps too, rather than
  as every ID of every group.
- Auto mode's estimates are measured against the scale environment's
  reports at 1x, 10x and 100x. A table filtered by a subquery's value, as an
  audience's latest version is, counts as filtered, and a filtered table a
  hundredth of its bytes as well as of its rows; the replica reads cold,
  as first runs do, at its threads a core; workers read at theirs, which
  they now report, and pay 8 s a statement and 6 s a stage; and the
  replica's working set counts six times what feeds its joins. The first
  choice is now the faster side for all but statements whose rows cost
  far more than their count (unnested arrays; distinct counts through a
  view's computed keys), which their history then sends to workers. A
  report of 25 s on the replica and 161 s on workers at 10x, which the
  estimates sent to workers (110 s against 73 s), is estimated at 57 s and
  216 s. Forced statements run on as many workers as before. The size a
  statement's table must have for auto mode to consider workers follows
  the same cold rate: about 570 MB on a coordinator of 16 cores, rather
  than 160 GB at the 64 MiB/s a core it assumed with workers stopped, which
  kept a report of 259 s on one machine and 40 s on workers from them. The
  size is the same whether workers run or are stopped, their start counted
  in their estimate, and auto mode's reasons say when they are stopped: a
  report of 1,798 s on one machine, with tables of 6.2 GB, stayed there
  with workers stopped, though they had taken 362 s.
- After a statement runs on workers, the replica remembers what its
  exchanges held and the most rows a stage wrote for each row it read, by
  its plan key, and shows them in `EXPLAIN DISTRIBUTED` (`measured`) and
  in auto mode's log line; auto mode keeps on workers a statement whose
  exchanges would not fit in the replica's memory, but never moves one
  there by them: a report whose workers exchanged 14 GB took 38 s on one
  machine, where its plan streams. `EXPLAIN DISTRIBUTED` also shows the
  share of rows a join to a table filtered by a constant should keep
  (`semi-join estimate`), which the estimates do not count yet.
- Workers writing an exchange bound each partition's row group by the
  bytes of its rows as written, not only by the rows their types would
  take: rows of four lists of strings, which a list's type counts as
  eight short strings, took workers out of memory outside DuckDB's limit
  at a hundred times the data; in a lab, such rows held 1.2 GB a task over
  202 partitions where they had held 2.1 GB.
- A statement auto mode runs on the replica, though workers could run it,
  is stopped there once it has run three times its estimate and as long as
  the workers are estimated to take it, start included, and at least 15
  seconds, without sending a row, and runs on workers instead, which are
  started if stopped (`igloo_auto_guard_reruns_total`); auto mode runs it
  on workers from then on. That bounds the loss at about twice the faster
  side: a report estimated at 13 s on the replica and 30 s on workers moves
  after 39 s rather than 90.
  Workers are not stopped as idle while such a statement runs on the
  replica. Writes and statements that have sent rows are never stopped.
- Auto mode's history counts runs on workers with runs on about as many
  workers: a statement measured on a quarter of the workers it would run on
  now, as in an earlier session on a smaller fleet, is tried on all of them
  once a day. A report that took 230 s on 4 workers and 178 s on one
  machine stayed there, though 16 workers took 84 s. A statement is tried
  the other way only where that way is estimated at no more than twice
  what it took, and never on the replica where it should not fit: a report
  that took 57 s on workers was tried on the replica, estimated at 176 s
  and short of memory, and took 251 s, spilling 8.4 GB. Where the replica
  should run short of memory, a statement that ran on workers stays there,
  and one that has not run runs on workers if they are estimated at no
  more than half again the replica's estimate, which is then a floor: at
  ten times the data, reports the replica should spill took four to
  thirty-five times its estimate there (220 s for 56 s, against 66 s on
  workers).
- `EXPLAIN DISTRIBUTED <query>` says how a query would run without running
  it: where (workers or the replica) and why, auto mode's estimates, the
  partitions its exchanges would have, its stages with their SQL, what the
  coordinator runs over their results, and what keeps parts of it on the
  coordinator. See [distributed execution](docs/distributed-execution.md#seeing-it).
- Workers start on demand and stop when idle: on Kubernetes the chart's
  `workers.enabled` adds a worker Deployment that Igloo scales between zero
  and `workers.maxReplicas`; on virtual machines `IGLOO_WORKER_COMPUTE`
  starts and stops them with EC2 or shell commands; Docker Compose has a
  `workers` profile for trying it out. The scale-to-zero gateway stops
  workers along with Igloo.
- Grants and the sandbox hold: statements are authorized before they are
  divided, workers read only the data files of the tables a statement
  reads, with the server's sandbox and no catalog, and accept tasks only
  over mutual TLS keyed by `IGLOO_WORKER_SECRET`. A failed worker's share
  runs on another worker, and a statement that workers cannot run runs on
  the coordinator. Canceling a statement or its `statement_timeout` stops
  its parts.
- Statements that masking or row access policies apply to, directly or
  through a view, run on one replica, as do parts that read connections'
  tables or Delta Sharing's external tables. Workers read only the files
  Igloo lists from Iceberg metadata, and refuse tasks whose SQL reads
  anything else; the part of a statement the coordinator runs itself reads
  a table at the same snapshot as the workers.
- Distributed statements count against their users' role limits and
  timeouts like any other. Workers belong to one compute pool: the chart
  gives each pool its own, with a secret of its own.
- `SET distributed_execution = on` starts workers only for admins; for
  other users it uses running workers, no more than a statement's size
  calls for. A worker that answers with an error is never set aside, and a
  statement whose partial results would pass 1 GiB of the coordinator's
  disk, or that only filters rows, runs on the coordinator.
- In `auto` mode a statement runs on workers only when they should be
  faster than the replica alone: when the replica would take over
  `IGLOO_WORKER_THRESHOLD` (a minute) or run short of memory, and workers
  are estimated at least 1.5 times faster. Statements over 4 GiB used to
  go to workers whatever the time, which made 1x reports of 6 to 56 s
  take 37 to 176 s. Each replica remembers what statements took each way,
  by `query_history.plan_key` (the text without literals and the tables'
  sizes), and runs a statement run again where it was faster; one that
  ran only one way, in under ten minutes, is tried the other way once a
  day, so that runs are compared with runs. Both estimates count what the
  statement's parts read of each table: its size times the share of its
  columns, weighed by type, that they name, so a report that reads three
  of a table's 420 columns is not priced at all of them. Neither is less
  than the rows the parts go through take, once for the scan and again for
  each join and grouping, so a report that reads 19 GB but joins 4 billion
  rows to themselves 16 times goes to workers. The
  replica's estimate prices at its warm rate only what its read cache can
  hold, and the rest at its cold rate, and grows with how far its working
  set is over its share of memory: at 100x, where one machine ran reports
  for an hour and ran out of memory, they go to workers the first time.
  A statement
  auto mode ran on the replica that runs out of memory there before it
  sends a row runs again on workers, and the client gets its rows; it runs
  on workers from then on.
- Workers run as many tasks at once as their cores and memory allow (a
  task per 4 cores and 8 GiB), and exchanges have more partitions as the
  divided tables grow, so the stages of a report that run side by side no
  longer spill or run out of memory on workers that one machine's memory
  would have held.
- Workers watch their memory as the server does: when one is about to run
  out, it cancels the task that has run longest instead of being killed by
  the kernel with every task on it, and the statement fails with SQLSTATE
  `53200` (`out_of_memory`) rather than run again on another worker or on
  the coordinator, which would run out the same way.
- `igloo.query_history` has `workers`, `stages` and `exchanges` columns;
  `igloo_distributed_*` and `igloo_worker_*` metrics report statements,
  fallbacks, tasks and workers.
- Tables with lists, maps and structs, Snowflake's `ARRAY` and `OBJECT`
  columns among them, run distributed: workers read nested fields by their
  Iceberg field IDs, and a data file written before a struct's field was
  renamed, or dropped and added again, runs on one machine.
- Unnests run distributed: `unnest`, `json_each` and `json_tree` of a
  row's values in `FROM`, lateral subqueries that read no table, and so
  Snowflake's `FLATTEN` of `ARRAY`, `OBJECT` and JSON text columns, whose
  `sf_` macros workers define too. So do joins to `range` and
  `generate_series` of integer literals (up to 1,000 rows). A subquery in
  `FROM` that joins a table to an unnest no longer, now and then, reads as
  one that reads no table and runs on one machine.
- Views run distributed: a view reads as its query, its names resolved as
  in the view, so views that pass a large table through, filter it or join
  small tables to it distribute as their queries do.
- Tables with rows deleted or updated by `DELETE`, `UPDATE` and `MERGE`
  run distributed without waiting for maintenance: workers apply the
  position delete files those write, and the equality delete files and
  deletion vectors of other engines.
- While workers are stopped, a statement that would start them may have
  them read a table in full as large as the memory of the workers that ran
  last allows, rather than 1 GiB: a report at 100x that joins a 1.8 GB
  table on an expression, which it cannot partition, ran on the replica
  when forced on stopped workers of 24 GiB.
- Reports whose `min(x) OVER ()` the replica rewrites as a scalar
  subquery over the same rows run distributed again: a scalar subquery
  that stands on its own in the select list of a step that groups nothing
  is computed by workers once, as one row every task reads. Forced on
  workers, such reports had run on the replica.
- A statement Igloo writes to answer from bitmap indexes can filter a
  large table on workers by a cohort's bitmap: the coordinator computes the
  scalar subquery that reads it from the index's table, at the snapshot the
  statement pinned, once, and workers read its value from a DuckDB
  variable, never the table, which a policy of Igloo's own keeps from them.
  `EXPLAIN DISTRIBUTED` lists such values as `computed first`; one over
  256 MiB keeps the statement on one machine. The session variables in
  which a report answered from indexes and a scan holds its cohorts'
  bitmaps cross the same way. A table read at its latest
  snapshot by number, `AT (VERSION => n)`, as such statements read the
  indexed tables, runs on workers too; one read at an older snapshot, or at
  two, still runs on one machine.
- A statement that ranks an aggregation's groups with `ROW_NUMBER()`,
  `RANK()` or `DENSE_RANK()` and keeps the first k of each partition has
  the workers that complete the groups keep only those that may rank so
  (`QUALIFY rank() OVER (...) <= k`), and the coordinator ranks what
  arrives as before: a report at a hundred times the data sent it every
  group of four aggregations, 1.07 GB, to keep the first ten of each.
  Exact: a row's rank among a task's rows is at most its rank among all,
  and rows tied at the kth place all arrive. Two windows, or rows kept by
  a column the window does not partition by, keep every group.
- A statement whose relation of uncertain size came out read by
  partition, but in a plan made with its size, gets that plan the next
  time without measuring the relation again, rather than the plan made
  without its size: a report at a hundred times the data whose audience
  of 1.9 million rows its measured plan read whole in some branches ran
  its first plan the second time, 34 stages for 24, in 1,698 s for 248 s.
  The coordinator now remembers the size a relation came out at and plans
  with it, where it planned the relation past what every worker may read.
- Object storage turning requests away for now no longer fails a
  distributed statement: a HEAD of an exchange's file that S3 answered with
  503 SlowDown, on 16 workers at a hundred times the data, failed a report
  too large to run on one machine. Tasks' DuckDB sessions try such a
  request 8 more times over half a minute rather than 3 within half a
  second, the coordinator runs a task still turned away again up to four
  times, with backoff and jitter
  (`igloo_distributed_tasks_total{result="throttled"}`), and Igloo's own S3
  client makes 10 attempts of a request rather than 3. A statement's
  exchange files are spread over 16 directories,
  `_igloo_shuffle/<digit>.<statement>/`, whose prefixes every statement
  shares, so that S3 splits one statement's requests over them.
- A distributed statement's partial results may take a quarter of the
  coordinator's free disk, at least 1 GiB, rather than 1 GiB: a report at a
  hundred times the data, whose groups the coordinator ranks, passed 1 GiB
  after 54 minutes of stages and then ran on one machine until it timed
  out.
- Workers read ahead as their own `IGLOO_PREFETCH` says: they read ahead
  nothing unless the coordinating replica had it on too, though each logged
  "prefetch on". They no longer read ahead exchanges' files, which DuckDB
  reads around the read cache: on a report at ten times the data, workers
  read ahead 101 GB, 81 GB of which went unread, most of it exchanges.
  Opens cut short by a task's end no longer count in
  `igloo_prefetch_errors_total`, and the startup line gives the
  prefetcher's concurrency rather than 0 when it is unset.
- `IGLOO_EXCHANGES=auto`, the default, has workers exchange a statement's
  rows on their disks when every worker it runs on reports at least 1 GiB
  of room there and runs outside Kubernetes, and through object storage
  otherwise: two reports at ten times the data on 16 workers with NVMe
  took 34 s and 62 s rather than 95 s and 205 s through S3. `object`
  keeps every exchange in object storage, as before. Workers report their
  room and whether they run in Kubernetes;
  `igloo_distributed_exchanging_queries_total` counts statements by where
  they exchanged rows. The single-machine Compose stack passes
  `IGLOO_EXCHANGES` through. On Kubernetes the chart still sets `object`
  unless `workers.exchanges` is `local`.
- A filtered scan reads ahead what DuckDB will read of it, on replicas
  and workers (tasks tell workers which columns filter their part): a scan
  filtered by a `WHERE` clause, or by a semi join or an inner join to a
  relation such as an audience (whose keys DuckDB skips row groups by),
  reads ahead in full the files whose footers show that no filter on those
  columns could skip a row group (each row group holds values from across
  the file's range of every filtered column, the file's range is not
  narrow within the scan's, no Bloom filter), and only the footers of
  others, rather than at most 256 MiB of every filtered scan. Against an
  object store with S3's latency, reading ahead 256 MiB of a scan of the
  last tenth of a date-ordered 4 GB table fetched 58% more than DuckDB
  alone (145% more on three workers) for no gain; it now fetches 1% more.
  A join of the table to an audience of random devices took 8.8 s cold
  without prefetch, 8.4 s with 256 MiB read ahead, and takes 2.6 s; on
  three workers, 6.5 s, 5.2 s and 2.6 s.
- Prefetch tells the blocks DuckDB read where file systems keep access
  times: cache_httpfs sets a block's access and modification times both to
  the time it reads it, so the access time never passed the modification
  time, and every block read ahead counted as unread: workers whose cache
  was in a container's file system reported 99.9% of what they read ahead
  as overfetch, and a statement was never credited the blocks DuckDB read,
  so it read ahead a quarter of the cache's room at most. The prefetcher
  now writes a block's access time an hour behind and counts it read once
  the access time catches up.
- `count(DISTINCT x) FILTER (WHERE ...)`, and `sum`, `avg`, `min` and
  `max` over `DISTINCT` values with a `FILTER`, run distributed: the
  planner writes them as `count(DISTINCT CASE WHEN ... THEN x END)`, which
  is the same since they leave NULLs out. A report that counts each
  group's people among the rows it keeps so read its whole fact table on
  one machine. Such a count of 32-bit IDs sends bitmaps, as does one of a
  column that the common table expressions and subqueries it reads
  compute as an ID, as the report API's member is.
- A subquery that workers compute as a relation, as when a report joins a
  view of a wide table to itself, writes only the columns its reader
  reads to the exchange. A report whose 16 branches each read three
  columns of a 420-column view wrote every column, 159 GB, and ran past
  its hour on four workers.
- A statement's rows no longer wait for its exchanges to be deleted: the
  coordinator removes them in the background, with one listing of the
  statement's files and 32 deletes at a time, where it listed each of
  their directories and deleted each file one after another (333 requests
  for a report over 32 partitions, which spent 11 s after its stages had
  run). On S3 they are deleted a thousand a request: a statement's 55,000
  files, deleted one a request, outlived the coordinator deleting them.
  Every replica also sweeps the
  exchanges of statements that no longer run, such as those of a replica
  that was killed mid-statement, once nothing has been written under them
  for six hours, and those whose removal began, a minute after the replica
  starts; a statement that still runs writes there every half hour.
  Workers learn what they wrote to an exchange from DuckDB's `COPY`
  rather than listing the files and reading their footers back.

### Tools and integrations

- A migration report, `python -m migrate report --source snowflake|databricks`
  ([docs/migrating.md](docs/migrating.md)): reads the source's query history
  (Snowflake's `ACCOUNT_USAGE`, or `INFORMATION_SCHEMA.QUERY_HISTORY()`
  without it; Databricks' `system.query.history`, or the Query History
  API), groups it into query shapes, finds the workload's tables in Igloo
  (`--map` rules and connections), replays the queries on Igloo as written,
  in Igloo's own dialect of the source's SQL where there is one, and
  translated by sqlglot, and compares results with the history's row counts
  and, within a cost budget, with re-runs on the source. Its HTML, Markdown
  and JSON report gives the shares of shapes and executions that run
  unchanged, run after translation, fail (by error class, with examples) or
  lack data, the result match rate, timing, the most-used tables and
  whether Igloo has them, and the source's spend against an Igloo estimate,
  each figure labeled measured or estimated. Read-only on both sides by
  default: writes replay only with `--replay-writes`, into clones in a
  scratch schema. Literals are redacted from the report, and extracts and
  reports are written only under `--out`. Its unit tests run in CI
  (`make migrate-test`).
- dbt with the standard dbt-postgres adapter: seeds, views, tables,
  incremental models (`append`, `merge`, `delete+insert`), snapshots, tests,
  `persist_docs`, `docs generate` and `source freshness`, tested on every
  pull request with a sample agency project (`scripts/clients/dbt`). The
  profile's `dbname` is the catalog's name. See
  [docs/integrations.md](docs/integrations.md).
- dbt's `microbatch` incremental strategy, tested with the others: a run
  builds the days it is given, each merged into the model. Incremental
  runs of every strategy delete the rows they replace with delete files
  and add new files, rewriting no data file: the test checks that the
  snapshots each run commits write no more rows than it changed or added.
- Tested clients: SQLAlchemy 2 with psycopg 3, pandas and polars (through
  SQLAlchemy, connectorx and ADBC), and the JDBC metadata calls and SQL of
  Tableau and Looker, in `scripts/e2e.sh`; Metabase, Apache Superset and
  Npgsql (Power BI) weekly in the new `scripts/bi-e2e.sh` (`make bi-e2e`).
- A query string with several statements runs them one at a time, each
  checked as if sent alone, in one transaction, for every role (only admins
  could send several before, and DuckDB ran all but the last unchecked).
- Transaction blocks behave as in PostgreSQL: after a failed statement,
  everything but `COMMIT` and `ROLLBACK` fails with `25P02` and `COMMIT`
  rolls back (a failed block's other statements used to be committed);
  `BEGIN` in a block and `COMMIT` outside one only warn; `SAVEPOINT`,
  `RELEASE` and, when nothing changed since, `ROLLBACK TO SAVEPOINT` work.
  Changes to the catalog in a block, which used to be refused for
  `CREATE OR REPLACE TABLE`, views and comments, now commit the block first,
  as in Snowflake.
- `COPY (query) TO STDOUT` and `COPY table TO STDOUT` in text, CSV and binary
  format, checked as the query they run.
- `ALTER VIEW ... RENAME TO` (and `ALTER TABLE` on a view), comments on a
  view's columns, `DROP TABLE` and `DROP VIEW ... CASCADE`, `SHOW` and
  `current_setting()` of the PostgreSQL settings drivers read, `TO_CHAR` for
  dates and timestamps, and catalog queries of SQLAlchemy's inspector, JDBC's
  key and index metadata, ADBC and Npgsql.
- Views outside the default schema can be queried by schema-qualified name
  (`sales.v`) and, after `SET search_path`, unqualified, and views can read
  tables named by schema (`sales.orders`); all three failed before. Such
  table names are stored with the catalog's (`lake.sales.orders`).
- Timestamps with time zone go to clients in text as PostgreSQL writes them
  (`+00`), which psycopg2, and so dbt, Superset and pandas, could not parse
  before. Result metadata carries a `numeric` column's precision and scale.
- Renaming or dropping a table while another session lists its schema no
  longer fails spuriously after the catalog has made the change (a race in
  DuckDB's shared list of the schema's tables that dbt's threads hit).
  Other effects of that race remain, so run dbt with one thread.

### Security and governance

- Only admins may change DuckDB's own database, `memory`, where each
  replica keeps its copies of the catalog's views: the target of a
  statement is resolved through the session's current database and search
  path, so an unqualified or schema-qualified name cannot reach it either,
  and non-admins cannot put `memory` first on the search path. SQL with a
  NUL in it is refused.
- Users with SCRAM-SHA-256 passwords, managed in SQL (`CREATE USER`,
  `ALTER USER`, `DROP USER`, `SHOW USERS`) and stored in PostgreSQL so every
  replica shares them. Built-in roles `admin`, `writer`, `reader` and `none`.
- Custom roles and grants: `CREATE ROLE`, `DROP ROLE`, `GRANT role TO user`,
  and `GRANT` and `REVOKE` of `SELECT`, `INSERT`, `UPDATE`, `DELETE` and
  `CREATE` on tables, views, schemas and `ALL TABLES IN SCHEMA`, to users or
  roles; `SHOW ROLES` and `SHOW GRANTS`. The role `none` allows only what is
  granted. Grants on a schema cover tables created later; views run with the
  querying user's privileges.
- Single sign-on to the web console with OpenID Connect (Okta, Microsoft
  Entra ID, Google Workspace, Keycloak, others): the authorization code flow
  with PKCE, verified ID tokens, allowed domains and groups
  (`IGLOO_OIDC_*`), users created at first sign-in with role `none` and
  matched by subject, verified email or name, and the provider's groups
  mapped to custom and built-in roles at each sign-in
  (`GRANT role TO SSO GROUP 'name'`, `SHOW SSO GROUPS`).
  `IGLOO_OIDC_PASSWORD_SIGNIN=false` makes linked users use single sign-on.
- SCIM 2.0 provisioning from Okta and Microsoft Entra ID at `/scim/v2` on
  the console's port, with an admin's access token: users created, updated
  and deactivated (disabled at once) as they change at the provider, and
  groups and members kept, with their role mappings applied at each change.
- Access tokens for SQL clients: `CREATE TOKEN`, `SHOW TOKENS`, `DROP TOKEN`,
  sent as the password, checked with SCRAM-SHA-256 so they never cross the
  network, stored as hashes, always expiring (`IGLOO_TOKEN_MAX_LIFETIME`),
  with their last use and every login recorded. Optionally the identity
  provider's own access tokens, inside TLS
  (`IGLOO_OIDC_ACCESS_TOKEN_AUDIENCE`). `auth.TokenChecker` verifies tokens
  for HTTP APIs.
- Service accounts (`CREATE USER ... TYPE SERVICE`), owned by an admin,
  without a password, signing in with tokens only. Users can be disabled
  (`ALTER USER ... NOLOGIN`), which ends their sessions, and given an email
  (`EMAIL`) for single sign-on.
- Limits on failed logins: 10 per user and 50 per address in 15 minutes.
- Igloo creates the first administrator only with an `IGLOO_ADMIN_PASSWORD`
  of at least 16 characters (was 8) that is not a placeholder such as
  `change-me` or easily guessed; the error says how to make one. An
  administrator that exists already keeps starting with a weaker value,
  with a warning that never includes it. The Helm chart refuses an
  `admin.password` that Igloo would; to upgrade a release with a shorter
  one, move it to `admin.existingSecret`.
- Masking policies, row access policies and tags, after Snowflake's:
  `CREATE [OR REPLACE] MASKING POLICY`, `CREATE [OR REPLACE] ROW ACCESS
  POLICY`, `ALTER TABLE ... SET | UNSET MASKING POLICY`, `ADD | DROP ROW ACCESS
  POLICY`, `CREATE TAG`, `SET | UNSET TAG`, tag-based masking with `ALTER TAG
  ... SET MASKING POLICY`, `SHOW POLICIES`, `SHOW TAGS`, and the views
  `igloo.policy_references` and `igloo.tag_references`. Policy expressions can
  call `current_user`, `is_role_in_session(role)`, `current_available_roles()`
  and `tag_value(tag)`, and read mapping tables. Policies apply to every user
  and statement (queries, views, joins, subqueries, `INSERT ... SELECT`,
  `CREATE TABLE AS`, `COPY`, `UPDATE`, `DELETE`, `MERGE`, cursors, time
  travel, the web console, and tasks, pipes and materialized views, as
  their owners), by rewriting statements to read protected tables
  through them; statements that cannot be rewritten with certainty are
  refused. The query history records which policies applied, and the
  console's data browser shows policies and tags. Igloo's checks of
  statements now call DuckDB's built-in functions by their qualified names,
  so a session's temporary macros cannot change what grants or policies
  find.
- Confined the files users other than admins may name, for every statement
  whether or not a table has a policy: a file must be a plain string literal
  (not a value built as the statement runs) that cannot climb out of an
  allowed prefix (no `.`/`..` segments, percent-encoding, backslashes, `?`,
  `#` or empty segments), in file-reading functions (`read_parquet`,
  `read_csv`, `glob`, `iceberg_scan`, `delta_scan`, `read_xlsx`, `ST_ReadSHP`,
  `ST_ReadOSM` and the like), `COPY FROM`/`TO`, `COPY INTO`, pipes and
  replacement scans. The check runs before DuckDB opens the file. This closes
  a path-traversal gap: DuckDB 1.5.6 prefix-matches a remote path against
  `allowed_directories` without canonicalising it, so
  `https://host/allowed/../other` or an `s3://` path with a `?s3_endpoint=`
  query would otherwise reach elsewhere. `IGLOO_ALLOWED_LOCATIONS` entries are
  validated and given a trailing slash at startup so a prefix cannot match a
  sibling. Without a policy, readers and writers may still read the
  warehouse's own files; admins are unchanged.
- Refused DDL that would read a policied table's rows through an expression:
  a column type change, `ALTER ... USING`, a non-constant `DEFAULT`, a
  generated column, a `CHECK` constraint, or `CREATE INDEX`. These evaluate
  over every raw row, hidden ones included, so they could read masked values
  and rows a row access policy hides (through `error()`, a failing cast or
  division by zero); they are refused like a rename, for writers and admins
  alike, until the policies are unset. The Iceberg REST endpoint also refuses
  dropping a table with data policies, consistent with its other operations.
  The target of these checks is read with all of ALTER TABLE's optional
  keywords handled (`IF EXISTS`, `ONLY`, a trailing `*`, parentheses,
  comments, quoting), so `ALTER TABLE ONLY t ...` cannot slip the table past
  them; the target is read the same way wherever an ALTER is parsed, and a
  target that cannot be read is refused for non-admins.
- Turned commit retries off for Igloo's own maintenance commits (compaction,
  snapshot expiry and the rewrite of deleted rows), and for every commit
  that is not an append. A table that sets `commit.retry.num-retries` above
  0 (another engine may) would otherwise make iceberg-go rebuild a rewrite's
  manifest list from the fresh parent on a conflict and reintroduce the files
  the rewrite replaced, duplicating rows. Maintenance now fails such a commit
  and retries next pass instead (see `third_party/iceberg-go/IGLOO_PATCHES.md`).
  iceberg-go also retries no other commit but a plain append: not a rewrite,
  delete or overwrite, not a commit without a new snapshot (expiry, ref
  changes, a rollback), and not an append to a table that merges manifests.
- Compaction no longer brings back rows that a `DELETE` removed. iceberg-go
  removed every position delete file attached to the files it rewrote; a
  partition-scoped one (as DuckDB, iceberg-go and Spark before Iceberg 1.6
  write them) also deletes rows of files compaction keeps, and those rows
  came back. A position delete file now goes only when every file it
  deletes rows of, by exact path, is rewritten or no longer live. To tell,
  compaction reads a delete file's bounds, then its row groups' statistics,
  and only then its `file_path` column. Table maintenance also reads a
  `history.expire.max-snapshot-age-ms` of Java's `Long.MAX_VALUE`
  ("forever") as forever; it overflowed into a negative age.
- Erasure of people's data for GDPR and CCPA requests (`ERASE SUBJECT
  WHERE pii.email = '...'`), found through tagged columns, including
  columns of hex SHA-256, SHA-1 and MD5 digests (`pii = 'email:sha256'`),
  zero-copy clones of the tables and the tables they were cloned from, and
  other tables that share their files: it deletes the rows, rewrites the
  data files and manifests that held them, expires the snapshots that
  reference those, however recent, deletes them and old orphan files from
  object storage, deletes Delta Sharing copies made from them, drops them
  from every replica's read cache, redacts the identifiers from the query,
  load and task history, and verifies that none is left in rows, files,
  manifest bounds, Parquet statistics or history. Every branch and tag is
  rewritten, the rows a `DELETE` missed go when their files are rewritten,
  and columns tagged `:null` are set to NULL instead of deleting the row.
  Requests run in the background on the scheduler, survive restarts,
  resume after failures with growing waits, fail for good with a metric
  and an alert after 10 failures before their first `DELETE` (losing to
  other writers, or a replica with another erasure key, does not count),
  and once they have sent one never fail: a table they cannot go on with
  is set aside while the others go through every step (failures count
  per table), and the request
  is then blocked, with a metric and an alert, keeping its identifiers and
  taking it up again every hour until what stops it is fixed, or an admin
  closes it (`CLOSE ERASURE REQUEST`), which first deletes the sharing
  copies, redacts the history and has the replicas empty their caches,
  deletes what it recorded of tables dropped since, records what it left,
  files included, and forgets the identifiers. Every request sweeps the
  warehouse: the files under no table's or view's location, as those of
  tables dropped or replaced, before the request or during it, through
  Igloo or around it, while clones read them or not, go once three days
  old, and newer ones are waited for, looked at again every hour, so a
  request never completes while they are there; none is read. Drops and
  replaces through Igloo, clone refreshes included, delete the old
  table's files at once, so only those of a table clones still read, or
  may, are left to wait for. A table dropped while a request
  runs is not set aside, and one replaced is erased from under its name.
  A table
  that cannot be read, or a snapshot whose manifest list is missing,
  stops no one's request; they keep their identifiers sealed
  under `IGLOO_ERASURE_KEY` (or `IGLOO_ERASURE_KEY_FILE`'s) only while they
  run, wait for snapshots staged by write-audit-publish to be published or
  discarded, and are listed with what they did per table and step in
  `SHOW ERASURE REQUESTS` and
  `igloo.erasure_requests`, with the tables history says were written from
  the erased ones, and tagged tables of connections, which they cannot
  erase from. `ERASE SUBJECT ... DRY RUN` previews a request; one that would
  delete more than 1% of a table, with what requests of the past day
  deleted there, or all of it, waits for someone other than its requester
  to `CONFIRM ERASURE REQUEST`, and `CANCEL ERASURE REQUEST` cancels one
  before it sends a `DELETE`. Placeholder identifiers, such as the all-zero
  advertising ID, and their digests, are refused; so are, when a request
  is made, before its first `DELETE`, and again just before each `DELETE`
  from each table, tables it could not erase from with certainty: of
  Iceberg format version 3, whose deletion vectors Igloo cannot read yet,
  with files other than Parquet, or with a branch or tag it could not
  write again. Igloo has Lakekeeper refuse tables of format version 3 in
  its warehouse (and warns when `IGLOO_CATALOG_SETUP=false` skips that),
  and refuses itself to upgrade a table with tags, or one
  an open request erases from, to version 3, or to make one of version 3
  under its name, and to drop, replace or rename a table an open request
  erases from: in SQL, for every change a statement makes to the catalog,
  checked as DuckDB asks the catalog for it, whatever the statement's
  text. No one but `CREATE TABLE ... CLONE` sets Igloo's clone
  properties. A table dropped in the warehouse that no clone reads has
  its files purged with it, through SQL and the Iceberg REST endpoint
  alike, which also refuses snapshots whose manifest list is not in the
  warehouse; so does a table a clone replaces (`CREATE OR REPLACE TABLE |
  SCHEMA ... CLONE`), whose location's files Igloo deletes, never those
  of a source it read as a clone itself. Every drop and refresh decides
  the same way: the files stay when clones read them, as the clones'
  metadata declares or Igloo's record of them says, when another table
  is at their location, when they are outside the warehouse or in
  `.igloo/`, or when Igloo cannot tell; a drop that asks to purge them
  then is refused. Every branch but main is rewritten with its head's schema, and a
  tag with its snapshot's, so columns dropped since stay in them, and
  each file is written again with its own partition spec, or another
  whose columns that schema has. Admins and roles granted
  `ERASE ON ACCOUNT` make them. `docs/privacy.md` says what a completed
  request guarantees, what it trusts, and its known limitations. `iceberg-go`'s copy gains
  `Transaction.ForgetFiles`, which rewrites the manifests that list removed
  files, and `WithMaxRefAge`, so expiring snapshots keeps branches and
  tags, and writes the manifests of a spec whose column was dropped since
  with a schema that has it, so their files keep their partition values.
- Compaction and the rewrite of deleted rows keep a position delete file
  that also deletes rows of a file they do not rewrite, whose deleted rows
  came back before.
- Table maintenance reads `max-ref-age-ms` and
  `history.expire.max-snapshot-age-ms` of Java's `Long.MAX_VALUE`
  ("forever") as forever; it overflowed into a negative age that dropped
  every branch and tag.
- A schema's `igloo.*` properties can no longer be set or removed through the
  Iceberg REST catalog, by anyone: a writer could note a directory of another
  table's as what a stopped write of theirs left, which the next write deleted.
  A note whose directory is not its table's own now deletes nothing.
- Erasure requests erase from the tables being made to replace their tagged
  tables (`name__igloo_new_...`), which a stopped `CREATE OR REPLACE TABLE`
  left, committed rows and files of workers alike, and which no request
  found: tags stay with names. A verification looks again, and a request
  does not complete while such a fill that began before its deletes runs.
- Erasure requests train models again. A model records the tables its
  training read, through views and other models' predictions too; a
  request on any of them, once the rows are gone, trains it again from its
  `CREATE` statement as its owner (grants kept), deletes its old blob, and
  has every replica drop the models and methods' results it holds in
  memory, `ai_predict_class`'s and `ai_predict_value`'s among them, so a
  statement prepared before must be prepared again. A `CREATE` that read
  its rows before the request stores nothing and asks to be run again. A
  model that cannot be trained again keeps the request blocked, with an
  alert, until it can, is dropped, or an admin closes the request, which
  records it as left.
- Erasure requests replace the materialized views and dynamic tables made
  from their tables, tagged or not (a new `views` step, after `rewrite`):
  each is made again from its query as its owner, which deletes the old
  table's files and snapshots, and the state its incremental refreshes
  keep (partial aggregates, and bitmaps that can hold a person's id) is
  dropped, so its next refresh is full. Verification counts views whose
  table or state has a snapshot from before the step; a view that cannot
  be replaced keeps the request blocked, with an alert, until it can, is
  dropped, or an admin closes the request, which records it as left.
- Erasure requests purge distributed execution's workers: their read
  caches, with what the prefetcher read ahead, footers, models and
  exchange files on their disks. Coordinators record every worker that
  answers in the requests' ledger and push purges within 15 seconds;
  every task carries the last deletion its coordinator knew of, so a
  worker stopped or cut off meanwhile purges before it serves again, and
  refuses statements that began before a deletion it purged (they run
  on the coordinator). A purge cancels the tasks it would otherwise race,
  and holds the prefetcher, before it empties the cache. A request waits
  for running workers, and is blocked, with an alert naming them, while a
  worker whose read cache outlives it does not run and has not purged, or
  a worker does not say what it keeps; `DEREGISTER WORKER` forgets one
  that will not come back, and closing a request records those it leaves.
  A worker that hears from no coordinator for
  `IGLOO_WORKER_ISOLATED_PURGE_AFTER` (a minute) empties its caches by
  itself, which a request waits for, from when it deleted files, before
  it counts a worker no coordinator reaches as purged; a worker found
  purged has the requests blocked for it go on at once.
- Data retention for tables that hold personal data (`ALTER TABLE ... SET
  (data_retention_days = N)`, at least a day, `igloo.data_retention`): table
  maintenance keeps no older snapshot, whatever else the table keeps, but
  those branches and tags point at, and rewrites the files whose rows were
  deleted longer ago. Privacy officers may set it on tables with tags.
- Renaming a table with tags or a data retention, or renaming, dropping or
  changing the type of a tagged column, is refused even when no table has
  policies, and through the Iceberg REST catalog too, as is `CREATE OR
  REPLACE TABLE` whose new table lacks a column with tags or a policy, and
  removing a tagged table's old schemas through the REST catalog.
- Table maintenance no longer drops branches and tags whose snapshots are
  older than the snapshot retention; they last as long as their own or the
  table's `max-ref-age-ms` says, forever by default.
- Query and login history: every statement and login attempt, with its
  outcome, duration, rows and the tables it used, in `igloo.query_history`
  and `igloo.login_history`, which records how each login signed in
  (`auth_method`) and with which token (`token_name`). Admins see everyone's history, other users
  their own. Kept in the metadata database for `IGLOO_AUDIT_RETENTION` (90
  days); `IGLOO_AUDIT=false` turns it off. Passwords are never recorded.
- A query sandbox: SQL cannot read the server's files or reach locations
  outside the warehouse and `IGLOO_ALLOWED_LOCATIONS`, nor run SQL built
  from strings (`query()`, `json_execute_serialized_sql()`), nor use the
  spatial extension's GDAL readers and writers (`ST_Read`, `ST_Read_Meta`,
  `FROM '*.gpkg'`, `COPY ... (FORMAT GDAL)`), which open files a dataset
  names outside the sandbox. `ST_ReadSHP` and `ST_ReadOSM` stay confined to
  the warehouse and allowed locations.
- Grants on connections to other databases (`GRANT SELECT ON CONNECTION`,
  `ON SCHEMA conn.schema`, `ON TABLE conn.schema.table`), required for
  every role but admin, over SQL clients, Arrow Flight SQL and the web
  console alike. Every statement's reads of connections are checked
  against DuckDB's plan, again whenever connections, views or grants
  change, and recorded in the query history. Creating a view of a
  connection's table needs a grant on it. Tables whose names differ only
  in case from another's in the same connection are refused. Remote
  sessions are read-only, and the functions that run SQL on remote
  databases (`postgres_query`, `mysql_query` and the like) are refused for
  everyone.
- Masking and row access policies, and tags, on connections' tables, as on
  Iceberg tables (`ALTER TABLE crm.public.customers ALTER COLUMN email SET
  MASKING POLICY ...`).
- TLS for SQL clients, the web console and Arrow Flight SQL.

### Data sharing

- Delta Sharing as a provider: admins share Iceberg tables with outside
  recipients (`CREATE SHARE`, `ALTER SHARE ... ADD TABLE [AS alias] [WITH
  HISTORY]`, `CREATE RECIPIENT`, `GRANT SELECT ON SHARE ... TO RECIPIENT`),
  who read them with any Delta Sharing client (Databricks and Spark, pandas,
  Power BI, Tableau) from a new endpoint on port 8444
  (`IGLOO_SHARING_ADDR`, `IGLOO_SHARING_URL`), over TLS or at `localhost`.
  Recipients get a one-time activation link to their profile file; tokens
  are stored hashed, expire and can be rotated. Data files download from
  hour-long URLs that Igloo signs and serves itself, so recipients never see
  storage credentials, and revoking access ends downloads at once. Rows
  deleted from a table are never served: data files with deletes are served
  as copies with them applied. Every recipient request is in the query
  history. See [docs/sharing.md](docs/sharing.md).
- Delta Sharing as a consumer: `CREATE PROVIDER` registers a partner's
  profile (its token stored encrypted), and `CREATE EXTERNAL TABLE ... FROM
  SHARE provider.share.schema.table` makes a shared table queryable, and
  grantable, like any table. Queries read the partner's current version
  through a proxy that is the only address the sandbox gains.
- External tables read Delta Sharing's `delta` response format, in which
  Databricks shares every table with deletion vectors enabled: Igloo
  applies deletion vectors, so deleted rows never show, and column mapping
  (modes `name` and `id`), so columns have the table's names. A table that
  needs a Delta reader feature Igloo does not apply is refused, naming it.
  Providers that speak only the `parquet` format are read as before.
- External tables have the shared table's columns in its order: columns
  the files hold that the table does not, such as Delta's row tracking
  columns, are left out, and columns no file holds yet read as NULL.
- Delta Sharing and policies: tables with masking or row access policies
  cannot be shared, and a shared table that gains one is refused to
  recipients, downloads of files listed before included, with each refusal
  and its policies in the query history. Policies and tags do not attach
  to external tables, and while tables have policies only admins read
  external tables' files or the copies of shared files directly.
- Clean rooms (`CREATE CLEAN ROOM`, `ALTER CLEAN ROOM ... ADD COLLABORATOR |
  DATASET | TEMPLATE`, `APPROVE TEMPLATE`, `RUN CLEAN ROOM`, `SHOW ... IN
  CLEAN ROOM`): an agency and its partners measure audience overlap, reach
  and frequency, and conversion lift on data joined by hashed identifiers
  that neither sees. Datasets carry policies of identifiers (join and count
  only), dimensions and allowed aggregates; templates are checked on
  DuckDB's parse and run only after every data owner approved that version,
  as the room's own identity, with each result row counting at least a
  threshold of each dataset's people (50 by default). Datasets are tables
  and partners' external tables, which their owners or admins put in a
  room, readable, changeable, clonable and shown in the catalog only to
  their own party, through every front end, the Iceberg REST catalog and
  Delta Sharing. Results go to the requester alone; every
  run is in the room's history. Parameter checks, run budgets and optional
  Laplace noise defend against differencing. Built-in templates, and a
  partner API on the sharing endpoint with the recipient's token. See
  [docs/clean-rooms.md](docs/clean-rooms.md).

### Data catalog

- Owners of schemas, tables and views, by default their creator:
  `ALTER TABLE | VIEW | MATERIALIZED VIEW | SCHEMA ... OWNER TO`, shown in
  `SHOW OBJECTS`, `igloo.tables`, `igloo.schemata` and `information_schema`.
  Owners may set their objects' comments and hand them on; ownership grants
  no access to data. Engines' users own what they create through the
  Iceberg REST catalog, and admins the external tables they create.
- `COMMENT ON SCHEMA`, stored as the Iceberg namespace's `comment` property.
- `igloo.schemata`, `igloo.tables` and `igloo.columns`: owners, comments,
  columns, rows, bytes, files, snapshots and when the data last changed,
  read from the Iceberg metadata. Rows leave out those that updates,
  deletes and materialized view refreshes deleted; they are unknown while a
  table has equality deletes. `information_schema.tables` gains
  `table_owner`, `comment`, `row_count`, `bytes` and `last_altered`, as in
  Snowflake. `igloo.tables` and `igloo.columns` also list external tables,
  with their provider, and connections' tables, the latter to admins and
  to whoever has a grant on them only, from a listing that never waits for
  a remote database.
- `information_schema` has the Snowflake views and columns that tools and
  dbt macros check for: `information_schema.tables` has `is_transient`
  (always `NO`) and `is_temporary`, `information_schema.columns` has
  `comment`, `information_schema.functions` lists the macros admins
  create, with Snowflake's columns, and `information_schema.procedures` is
  there, empty.
- `igloo.lineage`, table-level lineage from the query history: edges from
  the tables and locations that `INSERT ... SELECT`, `CREATE TABLE AS`,
  `MERGE`, `UPDATE ... FROM`, `COPY INTO`, tasks, pipes and materialized
  views read to the tables they wrote, with when they were last seen and by
  which statement or job; connections' tables are named in full, and reads
  of external tables count as reads of the tables, never of their files.
  `igloo.table_usage` counts reads, writes and users of each table over 30
  days, not Delta Sharing recipients' downloads.
- The query history records what every statement read and wrote, not only
  those of users with the role none, worked out in the background, and new
  columns `locations_read` and `job`.
- Lineage events for OpenLineage endpoints such as Marquez and DataHub
  (`IGLOO_OPENLINEAGE_URL`), without query results, and without statements'
  text unless `IGLOO_OPENLINEAGE_SEND_SQL` is set. Events go only to public
  addresses, or private ones when allowed, checked as connected, never
  through a proxy or a redirect.
- Everything the catalog shows is limited to what the user may see: users
  with the role none see only the tables they have a grant on or own, which
  now also applies to `igloo_comments()`, `pg_description`, and
  `igloo.policy_references` and `igloo.tag_references`.

### Web console

- A web console on port 8443 (`IGLOO_CONSOLE_ADDR`): SQL worksheets with
  cancel and CSV download, a data browser, query history (the console's
  own lookups, recorded under the application name `Igloo console lookup`,
  shown only when asked), tasks, pipes and
  materialized views with their runs, the load history, and admin pages
  for users, roles and grants, and the replica's sessions. Users sign in
  with their Igloo password or single sign-on; statements run as them
  through the same authorization as SQL clients. Sessions are stateless
  sealed cookies, so the console works behind a load balancer, and end when
  the user is disabled or their password changes.
- Console pages for access tokens (everyone's own), service accounts and
  every token (admins), and single sign-on: settings, whether the provider
  answers, group mappings and who signed in with which groups.
- A catalog page: search over tables' names, descriptions, columns, tags
  and owners, and a page for each table and view with its description,
  owner, size, freshness, usage, columns, sample rows and a lineage graph,
  showing tables the user may not see only as a count. Owners, writers and
  admins edit descriptions there.

### Other engines

- An Iceberg REST catalog on port 8181 (`IGLOO_ICEBERG_ADDR`) through which
  Spark, Trino, PyIceberg, Databricks and Snowflake read and write Igloo's
  tables. Engines sign in as Igloo users with the OAuth2 client-credentials
  flow, with a password or an access token (so service accounts can), and
  get Igloo's own tokens, which end with the access token they came from;
  every request is checked against the user's role and grants and recorded
  in the query history, and token requests in the login history. Tables
  with masking or row access policies are refused to everyone but admins,
  since engines would read their files unmasked. Lakekeeper stays private.
  Tested with
  Spark 3.5 and Iceberg 1.11 (reads, `INSERT`, `MERGE INTO`,
  `CREATE TABLE AS`), Trino 483 (reads, `INSERT`, `UPDATE`, `DELETE`) and
  PyIceberg 0.12 (`scripts/engines-e2e.sh`, `scripts/e2e.sh`).
- Storage credentials for engines that last an hour and reach one table's
  files only, read-only unless the user has `INSERT`, `UPDATE` and `DELETE`
  on the table: STS with a session policy, on AWS through
  `IGLOO_S3_STS_ROLE_ARN` (created by the AWS module), on S3-compatible stores
  through their own STS after checking that they enforce the policy. None on
  Google Cloud Storage and Azure yet.
- The Compose stack publishes the endpoint on loopback, the Helm chart on the
  SQL Service (`iceberg.enabled`, with NetworkPolicy), and the scale-to-zero
  gateway forwards it (`IGLOO_GATEWAY_ICEBERG_BACKEND`).

### Performance and cost

- Large results reach clients four to eleven times faster. A result's rows went
  out a row at a time: DuckDB's driver made each value a Go value (an allocation
  for most, a call into DuckDB for each date, a big integer for each decimal),
  which was then encoded through a lookup of its type and copied twice on its way
  to the client. Igloo now reads each chunk of a result where DuckDB holds it and
  writes its rows once, in the client's text or binary format: a statement's rows,
  those `FETCH` reads from a cursor, those of a portal read with a row limit
  (JDBC's fetch size) and those of `COPY ... TO STDOUT` in each of its formats.
  The bytes are those a row at a time sent; tests compare the two for every type,
  at the edges of each. To a client that only reads them, 200,000 rows of sixteen
  columns shaped like TPC-H's `lineitem` take 41 ms in text where they took 282,
  34 ms in binary (341), 37 ms as a binary copy (316) and 68 ms as CSV (361).
  Python clients are now the slower side: of 2 million rows of five columns,
  psycopg's `fetchall` reads 3.5 million a second where it read 1.3 million, a
  server-side cursor 3.6 million (1.4), polars through connectorx 2.9 million
  (2.0) and pandas' `read_sql` 1.4 million (0.9) (see
  [notebooks and Python](docs/integrations.md#notebooks-and-python)).

- A dashboard's own heavy statements stay in the first DuckDB. With nothing set,
  only tasks' heavy statements run in the second DuckDB, whose threads give way
  to the first's, and clients' do only with `IGLOO_HEAVY_IN_BACKGROUND=true`,
  no longer the default. In the second DuckDB, beside the dashboard's other
  statements, a dashboard's FLATTEN statement at ten times its data took 57 to
  78 s, against 22 s in the first, and the dashboard waited for it. Its 96
  queries took 2 to 9% less time on 8 cores and 22 GB, and 3% less on 16
  cores. Set it where long reports run beside dashboards on a replica without
  workers (see [heavy statements beside short
  ones](docs/workload-management.md#heavy-statements-beside-short-ones)).

- Three or more common table expressions of one statement that group the same
  rows the same way, as reports count one join several ways, are grouped once, in
  a common table expression marked `MATERIALIZED` that each reads (see [SQL](docs/sql.md)):
  DuckDB scanned, joined and grouped the rows once for each. A dashboard's two
  reports of six such took 480 to 488 CPU-seconds rather than 781 to 898 at ten
  times its data on 8 cores and 22 GB, and its 324 queries 1,684 s rather than 1,808.

- With no workers, a replica runs tasks' statements known to be heavy (their shape
  last used 5 seconds of all its cores) in a second DuckDB of its process, whose
  threads give way to the first's, and with `IGLOO_HEAVY_IN_BACKGROUND=true`
  clients' too, where a read not yet known to be heavy starts again there when it
  runs long beside others. Beside one analyst's long reports a dashboard's
  statement had little more than its own thread. Over a billion rows on 12 cores,
  with the setting on, dashboards take 3.0 s at the median and 8.1 s at p95 beside
  them, 47 statements a minute, as they do alone, where they took 13.7 s, 74 s and
  10 a minute (see [heavy statements beside short
  ones](docs/workload-management.md#heavy-statements-beside-short-ones)).
- The result cache sends a dashboard's repeated queries again more often:
  it compares statements without their comments and spacing, which BI
  tools vary per run; shares a result between users when no policy
  changes the statement; and keeps a result until one of the tables its
  statement reads changes, not any table. In a test of three users
  opening a five-chart dashboard three times while a pipe loaded a raw
  table, 36 of 45 chart queries were answered from the cache, against none
  before. Erasure requests empty every replica's result cache.
- Prefetch is on by default (`IGLOO_PREFETCH`): replicas and workers read
  ahead, into the read cache, the blocks statements are about to read, with
  256 requests in flight rather than DuckDB's few per thread. Against an
  object store with S3's latency, cold, a scan of three columns of a 4 GB
  table took 2.4 s rather than 11.3 s on 16 cores and 2.4 s rather than
  6.6 s on three workers, fetching 1 to 3% more than DuckDB alone; filtered
  scans that a filter can prune fetch about 1% more and run as fast as
  without. The Compose stack passes `IGLOO_PREFETCH` through;
  `IGLOO_PREFETCH=false` turns it off. On Kubernetes the chart's igloo
  pods and workers read ahead into their cache volumes.
- The Helm chart's igloo pods have a read cache, which they had not: a
  volume at `/var/lib/igloo/cache` (`cacheVolume`), an `emptyDir` by
  default, or a volume of each pod's own from a storage class
  (`ephemeral`), or a claim that keeps the cache across restarts and
  scaling to zero for one igloo pod (`persistentVolumeClaim`), with a
  writable `/tmp` for the cache's extension; `cacheVolume.sizeLimit` sets
  the volume's size and `IGLOO_CACHE_SIZE` to 90% of it. Statements after a
  start, and prefetch, now read the files they read before from local disk
  rather than object storage. The volume is a 50Gi emptyDir unless
  `cacheVolume.sizeLimit` says otherwise, and the read cache, wherever it
  runs, now takes at most half of its disk's free space whatever
  `IGLOO_CACHE_SIZE` says, so it cannot fill a small node disk, unless it
  has a volume of its own (`IGLOO_CACHE_DEDICATED`, which the chart sets
  for an ephemeral volume or a claim), which it fills to
  `IGLOO_CACHE_SIZE`. The chart
  and the docs say the volume's file system must keep extended attributes (see [read
  cache](docs/operations.md#read-cache)). Erasure requests have every
  running replica empty it with the prefetcher held, as workers do, and,
  with `IGLOO_CACHE_EPHEMERAL` (which the chart sets but for a claim), count
  a replica whose volume went with its pod as holding nothing rather than
  as pending.
- A replica reads what the past week's statements allocated from the
  query history when it starts, those that ran under its memory limit, so
  a dashboard's first build after a restart reserves memory as later ones
  do: 301 seconds rather than 590 at ten times the data, on 8 cores. The
  history now keeps each statement's shape and the memory it ran with, and
  auto mode counts runs only under the memory they ran with, the replica's
  limit or the workers', so that runs from a smaller replica with workers
  beside it no longer steer one with workers of its own. A statement whose
  shape ran to its end on the replica is no longer estimated short of memory
  there (see [reservations from
  history](docs/workload-management.md#reservations-from-history)).
- Statements reserve memory by what statements like them allocated before
  (`IGLOO_STATEMENT_RESERVATIONS`; see [reservations from
  history](docs/workload-management.md#reservations-from-history)), so that
  heavy ones wait for room rather than start together and spill, while
  light ones start at once. A dashboard's burst of queries over billions of
  rows, on 8 cores, took 548 to 574 seconds and take 306 to 310, the median
  query 21 to 22 seconds rather than 32 to 51.
- Statements reserve memory by the most that statements of their shape held
  at once, which Igloo samples while they run, rather than by everything
  they allocated over a run, and runs cancelled at a limit count too (see
  [reservations from
  history](docs/workload-management.md#reservations-from-history)). A
  larger dashboard at ten times the data, which ran 80 minutes without
  finishing on 8 cores, takes 2,148 seconds; a smaller one takes 255 seconds
  rather than 301.
- Statements learn what they hold from what they allocated, the median of
  their shape's latest runs, not from what the replica held less what the
  statements beside them reserved, which said next to nothing beside a
  statement that had reserved much and held little yet: four in ten of a
  large dashboard's statements learned under a twentieth of what they
  allocated, started beside the heavy ones next time and spilled. Runs
  cancelled after ten seconds or more count as needing all the memory.
- A query ending in `LIMIT 0`, which tools send to learn its columns, has a
  shape of its own, and does not wait behind the replica's limit: 32 of a
  dashboard's 96 queries were such, and the shape they shared with the ones
  that ran said those used next to nothing.
- Reading ahead for a statement lists a table or view's name once, not each
  time: listing takes the catalog lock exclusively, several times a statement,
  which held up every prepare beside it.
- A statement reads ahead only the files no statement has lately for the same
  columns: a dashboard's 96 queries, in cache, each parsed the footers of every
  file they read and asked for each of their blocks.
- Queries with no policies to apply are prepared side by side from start to
  end, rather than each take the catalog lock exclusively at the last step:
  of the 60 s a dashboard's 136 prepares took, 50 s went waiting there.
- DuckDB keeps table files' parsed footers in memory: a dashboard's 96 queries
  at ten times the data took 214 s rather than 233, the slowest tenth 107 s
  rather than 132.
- A statement's thread sleeps while DuckDB's workers hold all its tasks,
  rather than ask for one again and again without a pause, a core for every
  statement waiting on its workers.
- A replica keeps the forms of `FLATTEN`s through a restart, from the query
  history, rather than try them again: a dashboard's first build after one took
  half as long again as the next.
- A statement whose `FLATTEN`s Igloo runs in either form keeps one history of its
  memory and time, by the text the client sent. By the text DuckDB ran, which
  differs between the forms, a dashboard's statements were new again in its second
  run, and waited for one another as statements nothing is known of do.
- A heavy statement in the second DuckDB gets the memory of the first's caches
  when statements wait in line behind it and none runs in the first: a dashboard's
  heavy statements, held to what those caches left them, spilled while the rest
  waited for them. Its 96 queries at ten times the data took 234 s rather than 260.
- While the first DuckDB reads, it goes only what the machine has to spare,
  between a sixteenth and a quarter of the memory limit, past what the second has let
  go of, and the second is told to let go of that much ahead of it. Free to take
  three quarters of the limit at once, whatever the
  second held, the first took memory faster than the second's spill files could
  give it back: beside a burst of heavy statements the two held 134% of the limit,
  and the kernel killed the process.
- A heavy statement stays in the first DuckDB while most of what runs there or
  waits to is of some size itself, known to take 5 seconds of all the cores alone or having run
  for a while, since there is nothing short for it to give way to. Under a
  dashboard's burst at ten times its data, statements sent to the second had a tenth
  of what the others left of the cores and ran past 15 minutes, and those moved
  there threw away what they had done.
- A statement that reserves memory also waits while what DuckDB holds for the
  statements running leaves no room for it, whatever they reserved. A dashboard's
  324 queries at ten times the data took 1,945 s rather than 2,086, with a median
  of 392 s rather than 429.
- Statements of a shape not seen yet start a few at a time, each counting for a
  quarter of the memory until it has had 30 s to hold what it will, or has ended.
  Five that started together when the memory emptied held 39 GB of spill files
  between them, and two ran past 15 minutes.
- A comparison with what a `CASE` of constants can never be, as of a view's `CASE`
  with a value it has none of, is false before DuckDB plans the statement, which
  then reads none of the tables behind it: a dashboard's statements that ask for
  such a value took a minute of the cores each at ten times its data.
- Snowflake's `x LIKE ANY ('%a%', '%b%', ...)` is one regular expression where that
  is faster, rather than a comparison for each pattern that works out `x` again:
  over `TRIM(LOWER(x))`, 25 patterns took 0.7 s of 10 million strings, not 6.4.
- A statement goes by the runs of the same text with the same literals, if there are
  any, for what it reserves and its place in line, and otherwise by those alike but
  for its literals: statements over a large audience allocated up to 12 times what
  the same statements over a small one did, and spilled, once they had run over one.
  A dashboard's 324 queries at ten times the data, on 8 cores, took 1,708 s rather
  than 2,207 after one of 96 over small audiences.
- Auto mode keeps a statement of seconds on the replica however busy it is, by its
  last run there or on workers, rather than go by an estimate of one machine that
  runs far past both: a dashboard's statements of 1.5 s ran on workers for good.
- Statements running on workers no longer count as sharing the replica's cores when
  auto mode weighs where another runs, or as what a heavy statement gives way to.
- A worker gives its task slots to the statement that has started the fewest tasks
  there, rather than in the order they came, so that a short statement's tasks do not
  wait for all of a long one's.
- A statement that runs on workers no longer holds the replica's memory or a place
  among its statements at once, and one whose latest run went there does not wait
  for them.
- A run that read nothing, as DuckDB plans a statement to no rows for some values
  of its literals, says nothing of what other statements of its shape reserve and
  take, or which `FLATTEN` form suits them.
- Statements wait in the order they would end if each ran alone from when it
  came, as their shape's runs say, rather than in arrival order; a shape that took
  under a second of CPU does not wait at all; and a statement of a shape not seen
  yet waits while the memory is nearly full (see [the
  line](docs/workload-management.md#the-line)). The slowest tenth of a dashboard's
  96 queries at ten times the data took 94 s rather than 102.
- A `FLATTEN` of a column takes the form DuckDB's estimates point to, then
  whichever form its shape's latest runs took the less of the replica in at best,
  the more of their CPU time over its cores and of their memory over its memory
  limit for as long as they ran (see [Snowflake SQL](docs/snowflake-sql.md)): two
  dashboard queries over tens of millions of rows took 88 seconds and take 56
  and 40. Judged by CPU time alone, a dashboard query at ten times its data, on
  16 cores, took the form that ran 40 s rather than 22 and allocated 82 GB rather
  than 63, and the slowest of the dashboard's first minute took 73 to 76 s; it
  takes 56 to 64. A statement that runs on workers is judged by what it took of
  them, its time on each: the replica measures none of their work, so the form
  tried second was never judged, and was tried again after each start.
- A statement's shape includes the session's search path and database, so
  the same text over another schema, another client's or the same data at
  ten times the size, learns its own memory and `FLATTEN` form: the first
  build of a dashboard at ten times the data took 486 seconds and takes
  328.
- Statements translated from Snowflake's dialect divide with the rb
  extension's `igloo_sf_div`, which DuckDB binds as one function, rather
  than with macros whose every branch it bound, each operand named some 25
  times (see [Snowflake's
  division](extensions/rb/README.md#snowflakes-division)); the results are
  the same, to the bit. Binding them was most of the time a dashboard's
  statements took to prepare, one at a time: its queries at 1× have their
  median query in 2.3 seconds rather than 3.9 to 4.0, and the 90th
  percentile in 9.5 to 10.4 rather than 13.6 to 14.5.
- A view that only renames a table (`SELECT * FROM t`) filters its star in
  place for ID domains' columns rather than reading the table through a
  subquery, which DuckDB bound again wherever a statement named the view:
  the dashboard's median query takes 1.9 seconds rather than 2.3.
- FLATTEN's lists of values, values as text, keys and indexes are the rb
  extension's `igloo_sf_flatten_values`, `_strings`, `_keys` and
  `_indexes`, which DuckDB binds as the branch of the input's type alone,
  rather than macros whose every branch it bound, casts, lambdas and nested
  macros, at about a millisecond a call (see [Snowflake's
  FLATTEN](extensions/rb/README.md#snowflakes-flatten)); the lists are the
  same, to the byte. A call binds in 0.01 to 0.03 ms, and a statement of 16
  calls prepares in 1.2 to 1.6 ms rather than 12 to 15. A FLATTEN of text
  read only as text runs in half the time.
- Preparing a dashboard's statements, which holds the catalog lock
  exclusively and so goes one statement at a time, takes less of that
  lock. A query the [result cache](docs/operations.md#result-cache) kept
  is answered the next time its text comes without being prepared, once
  the tables it read are loaded again, holding no lock, to see that none
  changed. Readying a statement to run takes the lock only when there is
  something to check again (grants, or policies, the catalog or the
  session's settings changed). And views and statements name the
  catalog's tables as the catalog spells them, so that a view whose body
  spelled a table in another case no longer had every hold of the lock
  look the table up again. A dashboard build spent about 26 seconds
  preparing, one statement at a time.
- Queries are prepared side by side: a `SELECT` outside a transaction that
  calls no catalog function is prepared under the shared side of the
  catalog lock, alongside other sessions' prepares and binds, rather than
  one statement at a time. While the lock is held shared, the catalog proxy
  refuses, with 423, the requests DuckDB-Iceberg makes before it walks its
  set of the catalog's tables or erases from it (listing a schema's tables,
  a drop, a rename), so a prepare that would walk it, as DuckDB does to
  suggest names for a table that does not exist, fails and is prepared
  again under the exclusive lock. 32 distinct 16 KB queries prepared on 16 connections at
  once took 56% of the time they took one after another, against 110%
  before. `igloo_shared_prepares_total` counts them, and
  `IGLOO_SHARED_PREPARES=false` prepares every statement on its own (see
  [concurrency on a replica](docs/operations.md#concurrency-on-a-replica)).
- `IGLOO_MAX_CONCURRENT_STATEMENTS` limits the queries and changes of data
  running at once on a replica, whoever runs them, as a Snowflake
  warehouse's `MAX_CONCURRENCY_LEVEL` does: [the replica's
  limit](docs/workload-management.md#the-replicas-limit). Further ones wait
  in line, in order; settings, listings, DDL and results from the result
  cache do not. DuckDB runs every statement on all of its threads, so a
  dashboard's burst of queries all running at once each took about as long
  as the whole burst, and at ten times the data spilled together.
- `CREATE TABLE ... AS` of an aggregate whose list, string or `DISTINCT`
  aggregates (`ARRAY_UNIQUE_AGG`, `LISTAGG(DISTINCT ...)` and the like) have
  more groups than memory holds their values for fills the table in passes,
  each over the groups whose keys hash to its number: [aggregates too large
  for memory](docs/sql.md#aggregates-too-large-for-memory). A dbt model of
  millions of groups with several of them ran out of memory after spilling
  307 GiB, where Snowflake built it in two minutes.
  `igloo_aggregate_passes_total` counts these tables.
- Preparing a statement that repeats a subquery or has unions to factor no
  longer lists DuckDB's functions each time, about 25 ms holding the
  catalog lock, which the statements of a burst waited for one after
  another: they are listed once for each catalog generation.
- The [result cache](docs/operations.md#result-cache): Igloo keeps the
  results of queries over the catalog's tables in memory and sends them
  again, without running the query, to the same statement (the same text
  and parameters, from the same user with the same roles and settings)
  while no table has changed, as Snowflake does with `USE_CACHED_RESULT`.
  A statement sent while the same one runs waits for that run's result. It
  sees every change, through this replica, another replica or another
  engine, in the table loads and commits that pass Igloo's catalog proxy.
  Only results running the statement again would give are kept: of
  queries that read nothing but the catalog's tables, call no function
  whose value changes between runs or sessions, outside transaction
  blocks. `IGLOO_RESULT_CACHE_SIZE` (256 MiB by default) bounds it, and
  `SET igloo_result_cache = off` or `ALTER SESSION SET USE_CACHED_RESULT =
  FALSE` turns it off for a session. `igloo_result_cache_total` counts its
  answers.
- `CREATE TABLE ... AS` and `INSERT` of a query's rows in order (an
  `ORDER BY` of the result's columns) too many to sort within
  `IGLOO_SORT_SPILL_BUDGET` (half the spill disk's free space by default)
  are sorted [a range of the order at a time](docs/sql.md#writing-rows-in-order):
  a sample bounds the ranges, one run of the query writes the rows to
  compressed Parquet in the spill directory, and ranges next to each other
  are sorted and written together, in order, as many as the budget allows.
  DuckDB sorts a result whole and spills it uncompressed: a sort of billions
  of rows ran out of a 300 GB disk. On 515 million rows with 78 GB free, one
  sort took 1,344 s and 71 GB more disk; a range at a time, 888 s and 30 GB,
  for the same rows in the same order. An `INSERT` commits once.
  `igloo_sorted_writes_total` counts these writes.
- A count of distinct members counts their ids where the member column
  reaches it unchanged through common table expressions, subqueries and
  views, as reports count people they joined, windowed and unnested a few
  steps before: the ids come up the same steps in a column of their own
  (see [ID domains](docs/sql.md#id-domains)). On the real-data mirror (one
  8-core machine), the two behaviors reports, whose counts read each
  source through three such steps and a view, went from 58-61 s to 30-32 s,
  with the same results; Snowflake takes 69-72 s on X-Small. A session
  counts the values with `SET igloo_id_counts = off`.
- While connections are attached, checking what a statement reads from
  them no longer lists DuckDB's table functions, which took some 25 ms of
  every statement's preparing, with the catalog to itself, so that
  statements and catalog listings queued behind one another: `SELECT 1`
  takes 0.5 ms rather than 26 ms, and catalog lookups on a replica with a
  Snowflake connection of 940 tables run 11 rather than 8 a second (see
  [concurrency](docs/operations.md#concurrency-on-a-replica)). Users with
  the none role, who are refused most table functions, still list them.
- Statements that count the integer IDs of [ID domains](docs/sql.md#id-domains)
  (`count(DISTINCT CAST(_igloo_id_c AS UINTEGER))`) and read at least
  `IGLOO_BITMAP_MIN_ROWS` rows (10 million by default) count them in roaring
  bitmaps, with Igloo's own DuckDB extension, rb: a few bytes an ID where
  DuckDB's hash keeps every pair of group and ID. On a DuckDB copy of
  seven heavy audience reports, with people's ids as integers, on one
  8-core machine (18.6 GiB), the behaviors reports went from 101 and 76 s
  to 19 s, the signals reports from 46-48 s to 15 s and, where they compare
  two audiences, from failing after 28 GB of spill to 29 s, with no spill
  and the same results. A statement whose bitmaps outgrow the memory DuckDB
  gives them runs once more with DuckDB's hash, logged and counted
  (`igloo_bitmap_reruns_total`).

- DuckDB runs two threads per core on the server and four on workers when
  the warehouse is in object storage and `IGLOO_DUCKDB_THREADS` is unset,
  instead of one. Reading object storage cold waits on requests, not on
  cores: on 8 cores, a cold scan of 23 GB read 64 MB/s on 8 threads and
  172 MB/s on 32, and a report that took 66 s cold took 33 s on 16 threads
  (12 s warm either way). Threads past one a core each need 2 GiB of
  DuckDB's memory limit, or there are fewer of them, never fewer than the
  cores, as every thread keeps its own hash tables: on 8 cores with a limit
  of 18.6 GiB, two reports of `COUNT(DISTINCT)`s over 93 million rows took
  120–139 s on 16 threads, spilling, and 46–51 s on the 9 the limit allows,
  and five others as long as on 16. Such a machine then keeps fewer reads of
  object storage in flight cold (a worker of 8 cores and 32 GB about 12
  rather than 32); `IGLOO_DUCKDB_THREADS` sets more where cold scans matter
  more. Auto mode's estimates of distributed execution count cores rather
  than threads.
- Tables can be clustered. `ALTER TABLE ... SET SORTED BY (a, b)` (and
  Snowflake's `CLUSTER BY`, `ALTER TABLE ... CLUSTER BY`, `DROP CLUSTERING
  KEY`) sets a table's clustering key, which `igloo.tables`,
  `information_schema.tables` and `SHOW OBJECTS` show. Igloo keeps it in
  the table property `igloo.clustering-key` rather than as the table's
  Iceberg sort order, which DuckDB cannot write to: `INSERT`, `UPDATE` and
  `MERGE` into a table another engine gave a sort order fail with an error
  that says to `SET SORTED BY` or `RESET SORTED BY`, which moves the order
  out of the table's Iceberg metadata. Inserts whose keys are spread over a
  clustered table do not have the whole table rewritten around them: they
  wait until they stack more than 8 deep or take 5% of the table (or 4 GB,
  or 100 files), are sorted into a run of their own, runs are tiered (4 of
  a level sorted into one of the next), and the runs are merged into the
  clustered files once they have grown to a quarter of the table, while an
  insert that falls within a file or two is clustered in at once. `igloo_maintenance_clustering_depth` and
  `SYSTEM$CLUSTERING_DEPTH('t')` show how deep a table's files stack.
  Table maintenance then rewrites, sorted, the files whose ranges of the
  order's first column overlap, within its memory budget, sorting range by
  range and spilling to local disk, so a filter on that column reads few
  files: on 1.4 billion rows written unsorted, a filter on one value read
  140 files, and 3 once clustered (0.65 s instead of 2.3 s with nothing
  cached). A clustered table is not rewritten again until new files overlap
  it. A value of the order that more rows share than fit in memory is
  spread over several files, and a table whose clustering still needs more
  memory than maintenance may use is compacted unsorted for that pass and
  clustered again once it changes. See
  [SQL](docs/sql.md#sort-orders-and-clustering) and
  [operations](docs/operations.md#clustering).
- `CREATE TABLE` in Snowflake's dialect translates the column types of a
  table named with its schema (`db.schema.t`) too.
- Tables Igloo creates are written in files of 512 MB, Iceberg's default
  target, rather than a file per row group: DuckDB's Iceberg writer aims
  for 8 MB when a table sets no `write.target-file-size-bytes`, so 1.3 GB of
  narrow rows came to 288 files, each read with its own requests, until
  compaction caught up at 8 GB a table an hour. Igloo now sets the property
  when it creates a table, unless the statement did. See
  [operations](docs/operations.md#table-maintenance).
- A table Igloo's SQL creates starts with row groups of 524,288 rows, fewer
  for wide rows, so the statement that loads it no longer writes DuckDB's
  122,880 rows a group, which stayed until something rewrote the files: over
  copies of TPC-H at scale factor 100, a cold pass took 115 s instead of
  197 s, and six reports 52 s instead of 60 s, with warm scans and the
  inserts within 2%. Such an insert holds more memory, 29.5 GiB rather than
  25.9 GiB for 600 million rows, and more again for long strings, which the
  types do not show. Maintenance still sets the size from the table's bytes
  a row once it has rows. See [operations](docs/operations.md#row-groups).
- Row groups of about 16 MiB. Compaction and clustering size the row groups
  they write from the files' bytes a row, instead of writing DuckDB's
  122,880 rows, a few hundred kilobytes of narrow rows, each read cold with
  requests of its own: on a 2.4 GB table, a cold scan took 7 s instead of
  24 to 29 s, and a report over it 21 s instead of 44 s. Table maintenance
  sets `write.parquet.row-group-size` the same way on the tables Igloo
  creates, so that DuckDB's inserts write such row groups too, as many rows
  as fit in `IGLOO_MAINTENANCE_MEMORY` over DuckDB's threads; a large insert
  into such a table holds more memory than before. A size set on a table
  is left alone. See [operations](docs/operations.md#row-groups).
- A `UNION ALL` of SELECTs over the same rows, as reports write to compare
  several measures of one audience, scans and joins those rows once: when
  the union is aggregated, Igloo makes each row of the branches' shared
  `FROM` and `WHERE` once per branch, in either dialect, instead of DuckDB
  scanning and joining once per branch. A report of 16 branches over 40
  million rows took 45 to 99 s and takes 2.3 to 2.6 s on one machine (over
  100 million, 177 s and 4.7 s); on
  workers it runs in 3 steps instead of 34, and exchanges the joined rows
  once instead of the union's 16 copies (docs/sql.md#dialect).
- When memory runs low, Igloo lowers DuckDB's memory limit, so that DuckDB
  spills more and gives memory back, and raises it again once memory has
  stayed plentiful: under heavy work DuckDB keeps the memory it tracks at
  its limit and uses more besides, 8 GiB more for one statement over ten
  times the data on a 61 GiB machine. If memory still runs out, Igloo
  cancels the statement that has run longest (SQLSTATE `53200`) instead of
  being killed by the kernel with every session. DuckDB keeps to its memory limit only for the memory
  its buffer manager tracks: a reservoir sample of 600,000 rows of 60 text
  columns took 7.8 GB under a 500 MB limit, and one of a million rows of a
  384-column table took a replica down on a 32 GB machine. See
  [operations](docs/operations.md#sizing).
- A statement with thousands of casts, such as a column profile of a wide
  table in Snowflake's dialect, is prepared in time that grows with its
  length rather than its square: Igloo's rewrite of catalog casts lexed the
  whole statement again for each cast. A profile of 449 columns took 4.3
  seconds to prepare and run on an empty table, and now takes 0.43.
- Faster `FLATTEN` in Snowflake's dialect. `FLATTEN` of a column unnests it
  where it is read, without the dependent join a subquery of its own took,
  and unnests an `ARRAY` or `OBJECT` column's list or map without going
  through JSON text: 0.3 s instead of 1 s for 15 million elements of 3
  million rows. `FLATTEN` of an array constant, as reports build from each
  row's columns (`[OBJECT_CONSTRUCT(...), ...]`), unnests its elements
  without building the array and parsing it back, three times faster.
  `THIS` is computed only where a statement can read it. DuckDB plans a
  dependent join, which holds the rows' lists and maps at several times
  the cost of JSON text, for a `FLATTEN` in a select with a `WHERE`, whose
  `WHERE` is now the `ON` of the `FLATTEN`'s join when every join is an
  inner one (flattening an `OBJECT` column of 30 million rows ran out of
  18 GiB, and takes 14 s), and for one that a second `FLATTEN` reads
  (`FLATTEN(INPUT => f.value:items)`), whose `ARRAY` and `OBJECT` columns
  it builds objects from are turned into JSON before the join (a report
  that took 481 s that way takes 266 s). A `FLATTEN` of objects built with
  literal keys (`[OBJECT_CONSTRUCT('kind', 'genre', 'items', m.genres),
  ...]`), whose `VALUE` its select reads only by a key, as reports that
  flatten one member of each object do (`FLATTEN(INPUT =>
  GET_PATH(t.value, 'items'))`), keeps the objects as DuckDB structs rather
  than JSON, so their `ARRAY` columns are flattened from their lists
  without JSON text or a dependent join: 0.4 s instead of 1.6 s for 29
  million elements of 2 million rows' five lists. A `WHERE` there stays
  out of the joins, which DuckDB would plan as dependent joins: conditions
  on the tables go into the first `FLATTEN`'s `ON`, which DuckDB pushes
  down to them, and conditions on the objects' keys into the lists the
  second `FLATTEN` unnests (1.5 s, 1.9 s and 1.4 s, with conditions on the
  table, the keys and both, became 0.42 s, 0.62 s and 0.44 s). A string in
  such an object reads as itself, conditions on the objects' keys filter
  the objects, and conditions on a second `FLATTEN`'s elements filter
  those when the statement reads nothing of that `FLATTEN` but its
  `VALUE`, which is then all it unnests. Objects that no second `FLATTEN`
  reads stay structs whatever filters the select, since DuckDB's dependent
  join then holds the table's columns, as for JSON. Reports' signals,
  objects of a dimension and a row's string filtered on the strings, took
  3.9 s for 5 million rows and take 1.2 s, and objects of a dimension and
  a list whose elements are filtered 3.1 s for 3 million rows and 1.2 s.
  A `FLATTEN`'s `KEY`, `PATH`, `INDEX` and `THIS` count as read only where
  its own select names them, so a column of one of those names elsewhere,
  as reports name an `INDEX`, no longer keeps a second `FLATTEN`'s elements
  unfiltered and the objects JSON. Objects built from one `FROM` item's
  columns, and a second `FLATTEN` of one of their keys, are unnested in
  that item's select list rather than in lateral joins, and still run
  distributed: DuckDB pushes a filter, such as a BI tool's outer `WHERE`
  on a key a report groups by, down onto a lateral `FLATTEN`, which made
  it a dependent join, and one over structs of lists cost more than over
  JSON text, so reports' behaviors and signals ran slower on real data
  than as JSON. With such a filter, objects of three lists took 1.8 s for
  2 million rows and take 0.4 s; objects of strings 0.8 s and 0.4 s. A
  second `FLATTEN` whose elements every read casts to a type that is not
  semi-structured, as reports cast them `AS TEXT`, unnests their text
  instead of making JSON of each string and reading it back, which took
  20 to 24% of the behaviors reports' time on data shaped like real data:
  counting people by each element of 2 million rows' five lists took
  0.22 s and takes 0.13 s, and a behaviors report over them 0.15 s and
  0.09 s. A key of the objects that every read casts so is one field of
  text, built once for each object, rather than a list, a string and a JSON
  part that each read combines: reports' media signals, objects of a
  dimension and a name filtered and grouped by the name, took 0.20 s for 5
  million rows and take 0.18 s. The objects are built in that select list
  for each row of the item joined to the range of their indexes,
  `CROSS JOIN range(n)`, rather than unnested from a list of them, and a
  filter in the `WHERE` on their text, a key's of string literals included,
  stays above that select list, where DuckDB would otherwise derive from its
  ORs a condition joining the item to the range, and take that join for a
  fraction of the objects: DuckDB, which takes an unnest for as many rows as
  it reads and a filtered one for a fifth of those, then estimates the
  objects at their number, and a join of them with a smaller table, as
  reports join their signals to cohorts, builds its hash table on that
  table. A signals report had built one on 90.7 million browsing elements
  rather than 1.0 million cohort members (94 s of 465 s thread time); in a
  lab at those proportions every such join now builds on the cohorts, and
  the signals take 1.29 s of thread time instead of 1.86 s.
- Faster statements on large catalogs. DuckDB lists the Iceberg catalog's
  tables again for every statement that looks one up, and Igloo's checks of
  a user without the reader role listed every attached database's tables
  for each statement besides. The catalog proxy now keeps listings for up to
  10 seconds (changes through Igloo apply at once; a table made elsewhere is
  found at once by name, and listed within 10 seconds), the checks resolve
  names against an index of tables built again whenever that may have
  changed, and they plan a statement once rather than twice. On a catalog of
  753 tables plus a Snowflake connection of 812, small queries of a user
  with a schema grant went from 95–150 ms to 30–38 ms, and an admin's from
  35–45 ms to 21–25 ms.
- Iceberg tables' schemas, which the first statement that lists columns
  loads, load side by side and, after a start, in the background: on the
  same catalog that took 5 seconds instead of 11, and a first
  `information_schema.columns` lookup made once it is done takes 0.13
  seconds.
- Scale to zero: `igloo gateway` accepts client connections in Igloo's
  place, starts Igloo's EC2 instances, Kubernetes Deployment or (through
  commands) any VM when a client connects, and stops them after
  `IGLOO_GATEWAY_SUSPEND_AFTER` without traffic or running work. It can
  forward the web console too (`IGLOO_GATEWAY_CONSOLE_BACKEND`). Igloo
  reports running statements and maintenance on `GET /activity`.
- A read cache on local disk (DuckDB's `cache_httpfs` extension) for the
  warehouse's files on Amazon S3, S3-compatible stores and Google Cloud
  Storage. It survives restarts and machine stops, so the first queries
  after a start read local disk instead of object storage. It sizes itself
  from the disk's free space; `IGLOO_CACHE_SIZE` overrides that or turns it
  off. Only admins may use its functions and settings from SQL, and where it
  lives and what it caches cannot be changed from SQL.
- Automatic table maintenance: Igloo compacts small files and applies
  deletes, expires snapshots older than `IGLOO_SNAPSHOT_RETENTION` (7 days by
  default, overridable per table with `history.expire.max-snapshot-age-ms`),
  and deletes unreferenced files. Compacted files use DuckDB's own Parquet
  layout, so compaction makes scans faster, not slower. Replicas take turns
  through a lock in the metadata database, and record passes there so a
  pass that is due runs soon after a start. A table's maintenance deletes
  only files inside its own location, and keeps those that clones still
  read; `DROP TABLE` asks the catalog to purge files only when no clone
  reads them.
- Results reach clients in batches rather than one network write per row:
  large results arrive four to five times faster.
- `COUNT(*) OVER ()` beside a query's rows, as in BI tools' table previews, no
  longer makes DuckDB hold every row before it returns the first: where it
  is the number of rows the `FROM` and `WHERE` produce, Igloo counts those
  once instead, in either dialect. Previewing 50 rows of a 37-million-row
  table with its row count took 7.9 s and takes 1.0 s (docs/sql.md#dialect).
- A subquery a statement repeats in `FROM`, as in a report's top and bottom
  selections from the same report, is computed once, as a materialized
  common table expression the copies read, in either dialect, where that
  cannot change the result (docs/sql.md#dialect). A report that selected
  twice from one subquery of 17 common table expressions took 597 s and
  needed over 98 GiB of spill on an 8-core, 32 GiB machine; it takes 128 s,
  the time of one report.

### Operations and deployment

- Statement timeouts, connection limits, health and readiness endpoints,
  Prometheus metrics, and text or JSON logs.
- Logging never holds a statement up. A log line went straight to stderr,
  and waited while whatever read it did not keep up: with stderr a pipe
  nobody read, statements logging at debug level stopped once its buffer
  was full, 167 of 300 in a test, and the rest waited for good. Lines now go
  to a buffer of 4 MiB that a goroutine writes out; when it is full, lines
  are dropped, counted in `igloo_log_lines_dropped_total`, and a warning says
  how many, at most once a minute. The same 300 statements took 0.84 s, as
  with the log discarded. A worker whose disk is short of room for a task's
  exchange warns at most once a minute, not for each task.
- Docker Compose for one machine, with the bundled object store or Amazon S3
  through the machine's IAM role.
- A Helm chart for any Kubernetes cluster, with the web console and
  `scaleToZero.enabled` (the gateway in front of Igloo's Deployment, with the
  RBAC and network policies it needs).
- OpenTofu modules that deploy the whole stack on AWS (EKS, S3, RDS), Google
  Cloud (GKE, GCS, Cloud SQL) and Azure (AKS, ADLS Gen2, PostgreSQL Flexible
  Server), each able to pull Igloo from a mirror or private registry.
- A price-performance benchmark (`bench/`) that runs the same TPC-H queries
  on Igloo, Snowflake and Databricks and prices them by each one's billing,
  and a mixed-load one (`bench/mixed.py`) that runs dashboards and ELT on
  Igloo at once and reports each class's throughput, latency percentiles,
  queueing, errors and cost.
- A website (`site/`): a landing page and the documentation, built from the
  repository's markdown with search, light and dark themes, and a link check
  in CI (`make site`).
- Workload management and chargeback (docs/workload-management.md): query
  tags (`SET query_tag`, and defaults per user or role), recorded with each
  statement's compute pool, CPU time, bytes read, memory allocated and time
  waiting in `igloo.query_history`; `igloo.usage`, the history summed by
  day, pool, user, role and tag with an estimated compute cost from each
  replica's running time and `IGLOO_PRICE_PER_HOUR`; and
  per-user and per-role limits (`ALTER ROLE ... SET statement_timeout`,
  `max_concurrent_statements`, `memory_limit`) under which statements wait
  in line, in arrival order, on each replica.
- Compute pools ("virtual warehouses") that share the catalog, users and
  history but run on their own machines and scale to zero on their own:
  `pools` in the Helm chart (a Deployment, Services and optional gateway per
  pool), and `deploy/single-vm/compute-pool.yml` for Docker Compose.
  `IGLOO_POOL` names a replica's pool in the history and usage.
- DuckDB's spill directory on local NVMe where the cloud offers it
  (docs/operations.md#spill-on-local-nvme), instead of network block
  storage. The AWS module defaults to `r7gd.xlarge` nodes and, for node
  types with instance store, has nodeadm RAID0 it under kubelet, so the
  spill `emptyDir` is on NVMe. The Azure module defaults to
  `Standard_E4pds_v6` Igloo nodes with an ephemeral OS disk on their local
  NVMe disk (`igloo_node_os_disk_type`). The Google Cloud module's
  `local_ssd_machine_family` runs the igloo pods on Local SSD-backed
  ephemeral storage. The single-machine stack's `IGLOO_SPILL_DIR` puts spill
  files in a host directory, such as one on an instance store. AWS and Azure
  deployments that relied on the earlier node defaults get new nodes on
  their next apply; `node_instance_types = ["r7g.xlarge"]` and
  `igloo_node_vm_size = "Standard_E4ps_v6"` keep the old ones.
- When a C++ exception ends the process, the log names the exception's
  message and the stack it was thrown from.

### Fixed

- A cancel always stops the statement. A statement is prepared, readied
  and run in steps, and a cancel request, the console's cancel, the client
  going or the memory watch stopped the step running then. It was lost as a
  step ended, between two steps as the statement waited for the catalog's
  lock, and after it ran out of memory, before it ran again, and the
  statement ran to its end: a test's ran for 30 minutes. The cancel now ends
  the statement's next step too.

- A statement that runs out of memory runs again rather than fail. DuckDB
  fails whichever statement asks for memory the others hold. In auto mode
  the statement runs again on workers, on those already running if auto
  mode had not planned it for them. Otherwise it runs again here beside the
  others, once a reservation of what it held and the block it failed to get
  is free; if it runs out again, it runs alone, and then on half DuckDB's
  threads, and so on down to one, since some operators hold memory for each
  thread: TPC-DS q67's ROLLUP at scale 10 in 700 MB ran out alone on 16, 8
  and 4 threads and finished on 2 in 8 s. One statement at a time waits to
  run alone, and holds nobody back as it waits: statements that fit start
  beside it. Another that comes to that step fails, saying so, as does the
  one waiting when the replica has no moment free of others in 5 minutes.
  Six statements that ran out of memory at once had each waited in line for
  all of it, and nothing started on the replica for 150 s; in a test of
  that, 1,660 statements of a dashboard's load started in 3 s, against 88
  and then none. A prepare that runs out of memory is prepared again alone. On a replica of 1.8 GiB with two dozen
  sessions of joins, aggregates and commits, 404 statements failed so in
  three minutes, and 10 with this: 8 were one aggregate whose states need
  more memory than the replica has, on any number of threads, and 2 ran out
  as they were prepared, which now waits too. A statement runs again only
  before it sent a row, outside a transaction block, and when it changes
  nothing until it commits. One that runs out alone on one thread fails
  with what happened and what would let it finish: a higher memory limit,
  or workers, when its session turned distributed execution off.

- Refreshes of materialized views and dynamic tables, and COPY INTO, a
  pipe's or a client's, run again alone when they run out of memory, as a
  client's statement does, rather than fail. Beside a mixed load on a
  replica of 44.2 GiB, 9 of 11 refreshes had failed, out of memory at
  43.4 GiB, and none ran again. A failed refresh leaves nothing a run from
  its start trips on, and a COPY INTO that runs out loads none of its files.
  COPY INTO now holds the catalog's lock only while it runs, not while it
  waits its turn. A statement that ran again keeps the larger of its runs'
  peaks for the next like it, so one that failed big and finished small
  reserves for the big one. Tasks and alerts had this already.

- A full refresh of a view that later refreshes go on from runs its query on
  workers where auto mode would run a read of it there; it computed its
  groups' partial aggregates on the replica. The log says where each of a
  refresh's queries ran, and the history where its query did.

- A view counting distinct values refreshes in full, its query on workers,
  wherever auto mode would run that query there, and keeps no bitmaps; it goes
  on incrementally, from a bitmap of each group's values, only where workers
  would not run it. Beside a mixed load on 16 workers, full refreshes of the
  reach of 1.5 billion impressions by advertiser and channel take 41 to 140 s,
  with the dashboards at their best rate. Where it goes on
  incrementally, its state is written in row groups of about 8 MB, not one row
  group of every group's bitmap, as DuckDB puts a vector of rows, up to 2,048,
  in each at least: 800 bitmaps, 2 GB, took more than 1 GB at once to read
  back. Alone, such a refresh after 8 million new impressions takes 8 to 9 s.

- A refresh writes a state of bitmaps in a few files, rather than a file for
  each 8 MB row group, each a request of its own to object storage. Writing
  5 GB of state, 650 bitmaps of 7.7 MB, took 93 s a file each and takes 15.5 s
  in 11 files; it was most of the 145 s an incremental refresh of a view of
  distinct devices over 1.55 billion impressions took after 4 million new ones.

- Igloo's other Parquet files keep their row groups near 8 MB too, and tables of
  wide rows get fewer rows a row group. A task's result was written in DuckDB's
  row groups of 122,880 rows, 250 MB for rows of 2 KB; it is now cut
  at 8 MB where its rows have no order to keep. Sorted results, a large sort's
  runs and a sorted write's ranges, which DuckDB bounds by rows only, take as
  many rows as their width makes 8 MB, and a merge's file ends a row group at
  8 MB. Tables aim at 16 MiB a row group, as before, now also where rows are
  wider than 137 bytes, which stayed at 122,880 rows: a table of 2 KB rows in
  one row group of 40 MB gets row groups of up to 16 MiB once maintenance
  rewrites it, and its inserts are sized the same. DuckDB puts a vector of rows,
  up to 2,048, in each row group it writes, so rows past 4 KB make larger ones.

- A statement that runs out of DuckDB's memory has Igloo log what held it:
  DuckDB's limit and its memory by tag, what the workload controller reserved
  and last saw held, and the five running statements that reserved the most.
  DuckDB's error says only what the statement failed to get. Statements that
  run out within 10 s of each other share one line.

- A replica no longer cancels its first statements for memory while it reads back,
  after a start, the parts of files DuckDB held. The read-back took a new buffer for
  each request, which Go's heap kept after it ended, until Go's next collection;
  and when memory ran low, Igloo lowered DuckDB's limit a step at a time from the
  limit, not from what DuckDB held, so its first steps freed nothing. On a machine
  of 60 GiB, a report was canceled 40 s after each of two starts. The read-back now
  reuses its buffers, returns them when it ends, and stops when what Igloo can still
  allocate falls below a quarter of its memory. A replica now lowers DuckDB's limit
  from what DuckDB holds, so DuckDB drops those parts first; a worker, whose DuckDB
  holds its tasks' work, lowers it from the limit as before. In a replica of 36 GiB, with
  16 dashboard users and a heavy report starting during a read-back of 19 GB, the
  least memory left rose from 2.0 GiB, under the 2.25 GiB at which Igloo cancels
  statements, to 5.1 GiB. Go held 0.1 GiB once the read-back ended, not 5.4 GiB.

- A long statement no longer starts again in the second DuckDB because a
  monitoring poll ran beside it. A read not yet known to be heavy started
  again there when another statement was running at a moment Igloo looked,
  once a second, after 5 s for each statement beside it; a poll of a few
  milliseconds counted, and TPC-DS q67 at scale 100, alone apart from a
  sampler polling once a second, started again after 23.5 s of its 115 s.
  Only statements that have run for a second now count. Beside a 0.3 s poll
  once a second, a statement that ran 43 s in the second DuckDB now ran 32 s
  where it started.

- Reads of object storage reuse their connections. DuckDB opened one for
  each file it read and closed it after, and each closed connection holds
  a local port for a minute. A replica of 1.8 GiB whose two dozen sessions
  read a table of 300 manifests beside commits to it held 28,403 such
  ports, every one it had, and 260 to 550 of its statements in three
  minutes failed with "Could not connect to server". With the
  connections kept and reused, it held 29, and none failed so.

- Igloo builds DuckDB's Iceberg extension itself (`extensions/iceberg`):
  DuckDB-Iceberg at the commit DuckDB 1.5.6 ships, with Igloo's patches,
  loaded as the rb extension is, checked against the SHA-256 the image
  build recorded. The patches fix two ways the official extension ended
  the process:
  - An Iceberg scan torn down after one of its manifest-reading tasks ran
    out of memory threw that error again from a destructor, and C++ ended
    the process with every session on it. The statement now fails with the
    error, and the process goes on.
  - The extension's in-memory set of a schema's tables, which every
    connection shares, was erased from without its lock when a drop or
    rename committed. A bind could also free entries a listing was still
    reading. Either segfaulted the process or left a later listing looping
    forever, and Igloo had to serialize listings and DDL against every bind
    with its catalog lock. The set is now safe on its own. Erases take its
    lock, an entry lives as long as any transaction holds it, and listings
    walk a copy. In a stress test of the extension alone (24 connections
    binding, listing, renaming, dropping and swapping tables dbt's way for
    10 minutes against a catalog 25 ms away, in two schemas), the official
    extension segfaulted 3 times in 3 runs, within 3 to 66 s; the patched
    one ran 50,000 statements with no crash, also under AddressSanitizer.
  - Binds no longer queue behind each other's requests to the catalog.
    The extension held the set's lock while it loaded a table or listed a
    schema, so a schema's binds took turns one round trip each. They now
    load side by side, up to 18 at once in the stress test, and bind 5.4
    times as fast.
  - A rename whose commit had landed no longer fails with "already created
    by a different transaction" when a listing saw the new name first.

- The coordinator merges workers' sorted rows on several cores and copies
  them into DuckDB a column at a time: rows of a table sorted on workers
  took longer to reach the client than one machine's own sort of them.
  Each worker's file is decoded in a goroutine of its own, groups of
  up to eight files are merged at once and the groups' rows merged in turn
  (by a tournament of losers, with comparisons typed per column), ahead
  of DuckDB reading them; numbers, dates, times, timestamps and decimals
  are copied into DuckDB's vectors in bulk, text of up to twelve bytes
  written into them, and longer text assigned in one call a chunk. On 4
  million synthetic rows from 32 files, merged rows read through DuckDB
  take about as long as DuckDB's sort of the same rows, three times faster
  than before. A coordinator of fewer than 4 cores, or DuckDB threads, has
  DuckDB sort the rows instead, which is faster there.
- A worker told to stop, by a Spot Instance's interruption notice or by
  SIGTERM, keeps serving the exchange files on its disk until the
  statements that read them end, or its deadline passes (ten seconds
  before the time AWS gave; 110 seconds after a SIGTERM), while it takes
  no more tasks and finishes those it runs. It stopped serving them at
  once, so that the statements reading them ran again the parts that
  wrote them, as for a worker lost, and draining saved nothing. Its status
  says it is retiring meanwhile, so that coordinators send it nothing. The
  Helm chart gives workers 125 seconds to stop
  (`workers.terminationGracePeriodSeconds`).
- A statement's rows reach its client as DuckDB computes them, rather than
  once it has computed them all: every result was held whole in memory
  first, so that every row of a table of hundreds of millions, ordered by
  `ORDER BY` without `LIMIT`, ran the server out of memory, and the memory
  watch canceled it, before its client got a row. A portal the client
  reads a few rows at a time, which may run other statements between its
  reads, still reads its rows whole first. Such a sort of every row of a
  table, filtered or not, now runs on workers, which sort their shares,
  and the coordinator merges them as the client reads them, holding a
  batch of rows a task, where it merged them into a file before the first
  row went out; the files it merges it reads a megabyte at a time. On one
  machine, DuckDB's sort of more rows than fit in memory takes up to 40%
  more memory than its limit as it starts to return them.
- Planning a statement no longer fetches AWS credentials, nor fails for
  want of them. iceberg-go builds an S3 file system for each table it
  loads, each with a provider of the credential chain of its own, so the
  first request of every statement's scan planning, on the replica and
  when workers ran it, fetched credentials from the EC2 instance metadata
  service under the statement's deadline; under load the service answered
  late, and statements ran on the replica instead. Igloo now holds one set
  for the process, fetched once for every caller waiting and refreshed in
  the background ahead of expiry, and hands out those held at once while
  they are valid.
- A statement that does not stop once canceled, or past its statement
  timeout, no longer holds its client: 30 seconds on, its client's
  connection is closed and the server logs DuckDB's progress; the session
  ends once DuckDB stops the statement. A report run on the replica after
  its workers failed went on after its client's cancel request, the client
  waiting in it, until Igloo was restarted.
- A statement whose task over one partition ran out of memory on a worker
  runs again with more memory for each task where more partitions would
  not fit the tasks that write its exchanges: its tasks take twice a
  worker's slots, up to all of them, and so twice its memory, and their
  writers room for twice the partitions. It ran again over about the same
  partitions, and ran out of memory the same way.
- A block of the read cache missing or rewritten under a read no longer
  fails the statement. A worker started with an empty disk read ahead as
  if its cache were still empty after it was full, by a measurement of
  the cache minutes old, while the cache's extension evicted
  a block on each miss, and statements reading the same blocks ahead wrote
  them through one temporary file each, the second truncating what the
  first had renamed into place. The extension, which deletes a block it
  takes for stale or evicts by checking that the file exists and then
  removing it, then failed a task with "Could not remove file", and a short
  block failed reads, or took the prefetcher down. Read-ahead now writes
  each block through a file of its own, keeps to the disk's free space
  beyond the extension's floor as it is now, reads nothing ahead with no
  room, writes no block of an object without an ETag, and reads a footer
  block that is not whole again from object storage; and a task or a
  statement whose read raced another over a block runs again, up to three
  times.
- Planning a statement for workers no longer recurses without end on a
  common table expression filtered by `IN` a subquery that joins two
  subqueries, one with a scalar subquery in its filter: the subquery was
  made a relation of the columns read, whose producing took it for more
  than a projection of the subquery, as the scalar subquery deep in its
  `FROM` counted against it, and made the subquery a relation again, and
  again, in auto mode too, until the statement timed out. A part whose
  relations nest more than 64 deep or would compute a relation from itself
  now runs on one machine, and `EXPLAIN DISTRIBUTED` ends on the session's
  `statement_timeout` and on a cancel request.
- A long report in auto mode is no longer sent to one machine because its
  planning took more than 10 seconds: a report of many stages, whose
  parts each took a while to describe in the session, ran on one machine
  for many times as long as on workers. Auto mode now stops planning only
  once it stalls, having planned no stage and described no new part for
  a tenth of the statement's expected time on one machine (between half a
  second and a minute), or after that whole time, so that a cheap
  statement whose planning stalls still waits half a second for it; and it
  no longer skips planning statements of a shape that ran past its limit
  lately. `distributed_execution = on` and `EXPLAIN DISTRIBUTED` plan for
  as long as planning takes, within the statement's timeout. Planning is
  quicker too: it describes each part of a statement in the session once,
  for both its plans and its plan again with measured sizes, looks each
  table up once per plan, and keeps the SQL it printed of each part, which
  took most of the time of planning a long chain of common table
  expressions. It logs `planned a statement` with its time, stages, and
  descriptions and other lookups in the session, and their times, when it
  takes a second or more.
- Workers write to exchanges only the columns of a table or view that the
  statement reads when a view joined to it has a star of its own, as
  every stored view has where it reads its tables through
  `SELECT COLUMNS(...)`: the view's star counted as the statement's, so a
  table read by partition was written whole when it was joined to a view,
  and a report's branches, factored into one `SELECT` over a wide view
  joined to a common table expression of the same view, wrote every one
  of the view's columns to an exchange and ran the workers out of memory.
- Such a report's stage that joins the view to the common table
  expression aggregates the rows it makes, once per branch, as it makes
  them. The planner made the whole factored `SELECT` a relation instead,
  when the two large tables it partitions lay in different subqueries of
  its `FROM` (the view's and the expression's copy of it), and wrote each
  of its rows to an exchange once per branch before another stage
  aggregated them.
- No one may put Igloo's own schemas (`igloo_domains`, `igloo_bitmaps`,
  `igloo_refresh`) on the search path or make one the current schema, in
  any case or quoting: through such a path any reader read bitmap
  indexes' rows, their groups' counts and members, including groups a row
  access policy on the indexed table hides. Only the reads of an index's
  table that Igloo writes to answer counts skip its policy; any other
  gets it and no rows, and writes, drops and policy changes are refused
  by the table a name resolves to, not only by the names in the text.
- Users' writes win over maintenance: compaction leaves the files of a table
  alone while a write that reads or writes it runs, or a transaction block
  that has read or written it is open (every table, once the block has
  listed the catalog, as dbt does before each `MERGE`, and from its `BEGIN`
  for a user who may write; a block idle for 15 minutes no longer counts),
  and checks again with sessions held off the table just before it commits.
  A compaction that committed between a MERGE's read and its commit made
  dbt's microbatch batches, each a `BEGIN` ... `COMMIT`, fail with
  `CatalogCommitConflicts`, and had writes outside blocks run again. So did
  one that committed after a batch's `BEGIN` and before the batch read the
  model: DuckDB reads a table changed since its transaction began as it was
  then, and the batch's `COMMIT`, or the comment dbt's `persist_docs` makes
  before it, was refused, in 1 of 5 e2e runs.
- `CREATE TABLE ... PARTITIONED BY (...) AS`, as Snowflake's `CLUSTER BY
  (BUCKET(n, column))` makes it, runs in steps like any `CREATE TABLE ...
  AS`, holding the catalog lock only to create and rename the table: as one
  statement, a 35-minute copy into a bucketed table kept every other
  session from preparing a statement or signing in.
- No long query holds the catalog lock past its bind, so none keeps other
  sessions from preparing statements, listing the catalog or signing in:
  `CREATE [OR REPLACE] TEMP TABLE ... AS` (which dbt runs for every
  incremental model) and `CREATE TABLE ... AS` in a transaction block run
  in steps too, `EXECUTE` of a query prepared in SQL holds no more than the
  query would, and a materialized view's or dynamic table's refresh holds
  the lock only to plan and to change the catalog. Each held it for its
  whole run.
- A `CREATE TABLE ... AS` that an earlier, stopped one left something under
  its name no longer fails after its whole fill: a copy of 833 million rows
  failed at its rename after 236 s with "Tabular with the same name already
  exists", over a staged create an Igloo killed mid-statement had left in
  Lakekeeper, unseen and undroppable. Igloo notes the tables it is making in
  their schema's properties, and clears what a stopped process of its own
  left (half-filled staging tables, uncommitted staged creates) at the next
  `CREATE TABLE ... AS` or `DROP TABLE IF EXISTS` of the name; a create
  checks before it fills anything that nothing else holds the name, and an
  admin's `DROP TABLE` of the name releases a hold whoever left it.
- ELT that makes, replaces, renames or drops tables, makes schemas or
  refreshes materialized views no longer empties the result cache of
  dashboards over other tables: `CREATE OR REPLACE TABLE`, `CREATE SCHEMA
  IF NOT EXISTS` and a refresh each made every kept result unused, as the
  mixed-load benchmark's rollup and refresh jobs did for every dashboard.
  Results stay tied to the versions of the tables they read, and a
  statement prepared before DDL binds again without the cache.
- A materialized view's or dynamic table's refresh chooses between reading
  what changed and reading its tables whole by what each read costs: the
  bytes of its files, and, for each row a position delete file deletes,
  about what DuckDB takes to apply the deletion as it scans, which made
  scans of a billion-row table on S3 six times slower after an update of
  2% of its rows. It chose by the share of each table's rows in changed
  files, so any update spread over every file refreshed in full, its
  deletions applied. Reading what changed now takes the rows new delete
  files delete by their positions, joined to the files as the last refresh
  read them, rather than reading those files twice and comparing every row
  (`EXCEPT ALL`). A table with a column named `filename` or
  `file_row_number`, which would hide DuckDB's columns of each row's file
  and position, refreshes in full.
- A full refresh of a materialized view or dynamic table writes the view's
  rows as new files and commits them in place of all of its table's files,
  in one Iceberg commit, rather than deleting every row and inserting the
  new ones: DuckDB wrote a position delete for each row deleted, so a full
  refresh of 50 million rows took 23 s where `CREATE TABLE ... AS` of the
  same rows took 0.58 s, and a count of the view took 5.9 s afterwards,
  until compaction. It now takes 0.71 s. Readers see the old rows or the
  new ones, on every replica and through the result cache, which the
  commit invalidates once. A refresh that fails or is canceled deletes its
  files; files a stopped one left go at the view's next refresh or an
  erasure request's replacement of the view, and an erasure request does
  not complete while any from before it may remain. Partitioned views and
  views with columns of nested, fixed-length or nanosecond types go the same
  way, their files written by DuckDB's `INSERT`, whose commit the catalog
  proxy keeps from the catalog. An incremental refresh that writes an
  aggregating view whole, of one row of no group or whose rows do not show
  their groups, overwrites it too, where it deleted every row: a refresh
  stopped between the view's commit and its state's is followed by a full
  one, so no change is counted twice.
- A `CREATE TABLE ... AS` of an aggregate that the memory watch stops while
  it fills its table runs the fill again, its groups a share at a time, as
  it does when DuckDB runs out of memory: the benchmark's copy of a
  `count(DISTINCT)` of a billion ids failed with SQLSTATE `53200`.
- A statement whose run lists a schema's tables as DuckDB binds it, as one
  over a table dropped since it was prepared does, binds again holding the
  catalog lock exclusively when other statements' binds hold it shared: it
  failed with `HTTP 423 Locked`, the catalog proxy's refusal to let a
  listing walk the tables alongside other binds, which preparing such a
  statement already retried. The proxy logs the refusal at debug level.
- A write into a table partitioned into more than 100 partitions, such as
  `bucket(256, column)`, writes a file a partition: DuckDB kept 100 files
  open and closed and reopened the others, writing thousands of files of a
  few thousand rows, each a request to object storage (16 times as many
  files as buckets for 16 million rows), at a fifth of a core. Maintenance
  sizes the row groups of inserts into bucketed tables for a row group in
  every partition's file.
- Connections signing in from a loopback or private address, such as the
  scale-to-zero gateway's, a pooler's or a Kubernetes Service's, which all
  the clients behind it share, count only toward the limit of all
  connections signing in, not toward half of it for one address: a burst
  of more than 50 logins through the gateway after it woke Igloo was
  refused with `53300`.
- ID domains' members are held to `IGLOO_ACCESS_MAX_STALENESS` as the
  users, grants and policies are: a replica that cannot read them for
  that long refuses statements, and Delta Sharing's and the Iceberg REST
  catalog's checks of a table, until it can, rather than protect tables
  as members by what it read last.
- Delta Sharing refuses shared tables with `503` once a replica could not
  read the policies for `IGLOO_ACCESS_MAX_STALENESS`, rather than serve
  them under the policies it read last.
- The scale-to-zero gateway waits up to `IGLOO_GATEWAY_FIRST_BYTE_TIMEOUT`
  (30 seconds) for a client's first bytes before it starts or reaches
  Igloo, and lets at most `IGLOO_GATEWAY_MAX_WAITING` (200) connections
  wait at once; the Helm chart sets them as `scaleToZero.firstByteTimeout`
  and `scaleToZero.maxWaiting`. Connections that sent nothing started Igloo
  and stayed open for good. A failed start the gateway makes as it starts,
  to learn the schedule, is tried again as one for scheduled work is.
- A task, pipe, materialized view or alert does not start again on another
  replica until its last run has stopped: each run holds a lease in the
  metadata database (`igloo_tasks.leases`), renewed every 5 seconds, which
  another run may take only 30 seconds after its last renewal, and a run
  that cannot renew it stops before then. Losing the advisory lock alone
  let a second run start before the first had noticed.
- Maintenance, and the purge of a table a clone replaced, stop deleting a
  table's files once they lose the lock of its location, and leave the
  rest to a later pass.
- Replicas apply one user's roles one at a time, at SCIM group changes and
  at sign-in, so the roles that stay follow the latest groups.
- After a `CREATE OR REPLACE TABLE` whose rename the catalog made though
  DuckDB heard it fail, DuckDB no longer lists the new table under its
  temporary name too.
- Models and model grants that users and roles dropped before `DROP` did
  this itself left behind are disowned and removed as each replica starts,
  those a user or role later created with the same name would have got
  included. A grant given again to such a name before upgrading must be
  given again.
- The migration report compares decimals of more than 28 digits row by
  row, which raised an error when their scales differed.
- `make bench-test` runs the benchmark's unit tests, and CI runs it.
- A SQL connection must sign in within `IGLOO_LOGIN_TIMEOUT` (30 seconds)
  or is closed, and at most `IGLOO_MAX_CONNECTIONS` (at least 100) may be
  signing in at once, half of them from one public address; others are
  refused with `53300`. A connection read through a 16 MiB buffer from its
  first byte, and logins left hanging held their user's login slots for
  good, so ten stalled handshakes blocked a user's password logins, and
  idle sockets that never signed in could fill a replica's memory. The buffer
  now starts at 8 KiB and grows to the 16 MiB message limit (see
  [configuration](docs/configuration.md#server)).
- A replica that cannot read the metadata database goes on with the users,
  tokens, grants and policies it read last for up to
  `IGLOO_ACCESS_MAX_STALENESS` (2 minutes), then refuses statements with
  `58000` until it can read them again, rather than apply them for as long
  as it ran: a user disabled, or a token, grant or policy changed, on a
  healthy replica did not reach one cut off from the database.
- Without TLS, the console, SCIM, Delta Sharing, the clean room API and
  Flight SQL take requests addressed to `localhost` only from a loopback or
  private address, as the Iceberg REST catalog does; a request from
  anywhere could name `localhost` in its Host.
- With SCIM on, removing a user from a group the identity provider pushes
  takes the group's roles away at once. The groups of the user's last
  sign-in token were added to SCIM's, so a group the token had listed kept
  its roles until the next console sign-in.
- A SCIM group change whose roles cannot be applied answers 500, and the
  users it touches are recorded with it (`igloo_auth.scim_changes`), so the
  provider's retry, or a sweep each minute, applies them. It answered 204,
  and a retry, finding the removed user already out of the group, left
  their roles.
- Access tokens of the identity provider sign SQL clients in as the linked
  user only by issuer and subject, or by the claim
  `IGLOO_OIDC_ACCESS_TOKEN_ID_CLAIM` names (`uid` for Okta, `oid` for Entra
  ID) as recorded at the user's last console sign-in, never by email: a new
  account given a reused address signed in as the old one. Users whose
  provider's access tokens have a subject of their own need that setting,
  and one console sign-in, to use them again (see [single
  sign-on](docs/sso.md)).
- Dropping a user disowns their models, shown as owned by `name (dropped)`,
  which only admins can then grant, replace or drop, and dropping a user or
  role removes the models' grants to it, as for tasks and alerts. A user
  or role created later with the same name, by SSO sign-in too, got them.
- A task's or alert's run that came due before its owner was dropped is
  skipped when its owner changed, rather than run as a new user of the same
  name.
- AI functions keep their cached answers per integration, by its ID,
  address and API settings, rather than per provider host, so an
  integration a user may not use cannot answer them through another one's
  cache. Providers' error messages in row errors, `error_details` and
  notices are scrubbed of the integration's keys and headers, as other
  errors were.
- `CREATE OR REPLACE TABLE` asks the catalog what happened when dropping the
  old table or renaming the new one into its place fails, and drops the new
  table only when the catalog refused the drop. When the catalog cannot
  say, the error names the new table and the `ALTER TABLE ... RENAME TO`
  that puts it in place. A drop or rename the catalog applied without
  confirming it lost both tables.
- A clone the catalog registered, though the answer to registering it was
  lost, is kept and reported as made, rather than undone with its metadata
  file and share records deleted while the catalog still served it.
- A run of a task, pipe, materialized view or alert stops within seconds
  of losing its lock in the metadata database, and is recorded as failed,
  rather than run on alongside the next; a clone that lost its lock undoes
  itself rather than let its source's files be purged.
- Dropping or replacing a model while another model with the same blob is
  being created no longer deletes that blob: writing and deleting a blob
  are serialized by its hash, across replicas.
- `COPY INTO` and pipes record their files as `unknown` in
  `igloo.load_history` when the load's commit fails other than by a
  conflict, as when the catalog applied it and stopped answering (`40003`),
  rather than forget them and load them again on the next run.
- A distributed statement runs on one machine when a column's type, nested
  fields included, changed between planning and running it, rather than
  read the new snapshot's files with the old types: a `FLOAT` promoted to
  `DOUBLE` lost precision.
- Tables of Iceberg format version 3 whose fields have initial defaults are
  read on one machine, which applies them, rather than by workers, which
  read `NULL`.
- Training on a sample keeps or drops each copy of a repeated row on its
  own, by `seed`, aims below the row and value limits, and says so when the
  sample is empty. Identical rows were all kept or all dropped, so a class
  could vanish from the sample.
- The scale-to-zero gateway stops Igloo only when every replica says it is
  idle, or when its SQL port refuses connections, and warns when it cannot
  tell. An activity endpoint it could not read counted as idle, and stopped
  running queries.
- A start for scheduled work that fails is tried again, after a wait that
  doubles up to 5 minutes, rather than forgotten until a client connects.
- Download CSV runs a worksheet's query with the settings the statements
  before it in the run made (`SET`, `USE`, `ALTER SESSION`), and refuses
  one that ran after temporary objects were made, inside a transaction
  block, or after a setting computed by an expression, rather than download
  another schema's table.
- The console saves each worksheet on its own, and its tabs see each
  other's changes, rather than one tab overwriting another's work with an
  older copy.
- The loading guide's hourly rollup task is a `MERGE` that can run again;
  the `INSERT` it showed did not parse, and counted the latest hour again
  on every run.
- The migration report compares integers and decimals exactly, and tells
  `NULL` from `\N` and one column's values from the next's; it matched
  results that differed in these. Only doubles are compared to 10
  significant digits.
- The benchmark turns Igloo's result cache off, as it does Snowflake's and
  Databricks', checks that it is off, and records it in its results.
- The benchmark report bills a burst for the longer of its work plus the
  idle tail and the platform's minimum, as `pricing.toml` describes, rather
  than the longer of its work and the minimum plus the idle tail. Four
  Snowflake burst and monthly figures fall, X-Small Gen2 at scale 10 from
  $99 to $74 a month; the warm-run comparisons do not change. Its Failed
  column shows queries that failed in only some of a row's merged runs.
- `RESET search_path` and `RESET schema`, and `SET search_path TO DEFAULT`
  and `SET schema TO DEFAULT`, put a session back on the schema and search
  path it started with, the catalog's default schema and its views' schema.
  DuckDB's own default is its own database with an empty path, so an
  admin's unqualified `CREATE SCHEMA` after such a `RESET` landed in
  `memory`, and a non-admin's `RESET` was refused for making `memory` the
  current database.
- `ALTER TABLE ... SET TAG`, `SET ID DOMAIN`, `ADD BITMAP INDEX`,
  `SET SORTED BY`, `ANALYZE`, `SYSTEM$CLUSTERING_DEPTH`, and the other
  commands that look up a table by name, `CREATE` and `DROP` of ID domains
  and bitmap indexes among them, listed the catalog's tables, and created
  and dropped Igloo's own, holding none or only the shared side of the
  catalog lock: a walk of DuckDB-Iceberg's set of tables, or an erase from
  it, alongside other sessions' binds, the crash the lock is there to
  prevent. They hold it exclusively for those steps, and describe a table's
  columns from a query of it, which lists nothing. The catalog proxy now
  refuses any listing, drop or rename while the lock is held shared, so a
  step that still did would fail rather than crash the replica.
- A query that lists the catalog in a subquery, `SELECT name FROM (SHOW
  TABLES ...)` or `FROM (DESCRIBE t)`, runs holding the catalog lock
  exclusively, as `SHOW` and `DESCRIBE` statements do, rather than list
  alongside other sessions' binds, which the catalog proxy then refused
  with HTTP 423 now and then.
- A view or materialized view created in Snowflake's dialect whose query
  needs Snowflake's implicit conversions, a number in a string function or
  text compared with a date (`TRIM(id)`, `d < '2024-02-01'`), gets them, as
  the same query does on its own, instead of failing with "No function
  matches the given name and argument types": DuckDB binds the query of a
  view in the catalog's view store, and of a materialized view, when the
  statement runs, after Igloo's retry with the conversions would have
  happened.
- A view created in Snowflake's dialect whose translation calls Igloo's
  macros for Snowflake's functions (`DATEADD`, `DATE_TRUNC`, conversions
  to text) loads on a replica that starts after it was made, and after a
  restart, instead of failing every read with "Scalar Function with name
  sf_... does not exist": the connection that makes the local copies of
  views has the macros too.
- `CREATE TABLE ... AS` of an aggregate too large for memory runs in passes
  over its groups when the statement calls `error()`, as Snowflake's
  dialect does to refuse `ARRAY_FLATTEN` of what is not an array of
  arrays; it ran in one and ran out of memory. A pass that runs out of
  memory, DuckDB's estimate of the groups short, runs again split in two,
  as do the passes after it, instead of failing the statement.
- In Snowflake's dialect, `CREATE VIEW v (a, b) AS ...` whose query returns
  more columns than the list names is refused, as Snowflake refuses it
  ("invalid column definition list"), instead of creating a view with the
  extra columns under their own names; dbt's views with documented columns
  over a table that has since gained columns failed in Snowflake and not
  in Igloo.
- `statement_timeout` counts a statement's run, not the time it waited to
  start behind role limits, as Snowflake's `STATEMENT_TIMEOUT_IN_SECONDS`
  does; the new `statement_queued_timeout` (a session or role setting, and
  Snowflake's `STATEMENT_QUEUED_TIMEOUT_IN_SECONDS`) bounds the wait, with
  no limit by default. The memory watch no longer picks a statement that is
  waiting to start, which holds no memory, as the one to cancel.
- In Snowflake's dialect, a temporary table or view named in a schema, as
  dbt's incremental models make `db.schema.m__dbt_tmp`, is created in
  DuckDB's `temp` catalog instead of failing ("TEMPORARY table names can
  only use the temp catalog"), and the session's later statements that name
  it in that schema read it there, until it is dropped.
- Snowflake's implicit conversions are made in a `WHERE` that a `FLATTEN`'s
  join takes as its `ON`, too: text compared with timestamps there failed
  with "Cannot mix values of type VARCHAR and TIMESTAMP in BETWEEN clause".
- Snowflake's implicit conversions in a `BETWEEN` convert each operand
  against both of the others, as Snowflake does: text between a timestamp
  and text failed with "Cannot mix values of type TIMESTAMP and VARCHAR in
  BETWEEN clause".
- A client that opens more than 10 sessions at once for one user, as a
  pool, dbt's threads or a dashboard's queries do, signs them all in. The
  failed-login limit counts logins still running as failures to come, and
  refused the 11th running at once with `too many failed logins`, though
  none had failed; it now waits for one of them to end, for up to 10
  seconds. Opening many sessions at once also no longer races on the
  sessions' connections.
- A cancel request sent while a statement is being prepared ends it. It was
  lost, and the statement ran to the end: preparing a report whose CTEs
  become report API calls takes seconds (recognizing them, computing their
  cohorts' bitmaps), and a cancel sent then came back 30 seconds later with
  the report's rows. The statement timeout and the memory watch end a
  statement being prepared too.
- A statement that does not stop when the memory watch cancels it can no
  longer take the machine down. One in a step of DuckDB that does not check
  for interruptions ran 19 minutes past its cancellation, until the machine
  had 25 MiB left and paged so hard that it stopped answering, without the
  kernel ever killing anything. The watch now interrupts such a statement
  again after 10 seconds, and if memory falls below a sixty-fourth of
  Igloo's, fails the other statements in flight with SQLSTATE 57P01, logs
  the culprit, counts it in `igloo_memory_exits_total` and exits with
  status 75 to be started again. It does the same, after freeing Go's and
  DuckDB's caches and waiting 10 seconds, when memory is that low with no
  statement running soon after it canceled one: a canceled statement can
  return while DuckDB's other threads go on with its work.
- Compaction no longer refuses a table whose position deletes alone exceed
  `IGLOO_MAINTENANCE_MEMORY`, as after an `UPDATE` of every row of a large
  table: iceberg-go loads every delete file of the files it reads, whole,
  so 141 million deleted positions took more than the default budget and
  the table kept its hundreds of files. Where position deletes are what
  does not fit, the rewrite applies them itself, a file at a time, reading
  only the row groups of the delete files that name the file it reads.
- A statement in Snowflake's dialect with a `FLATTEN` of an earlier
  `FLATTEN`'s elements and a `WHERE` on the table they flatten, such as
  `FROM t, LATERAL FLATTEN(t.a) f, LATERAL FLATTEN(f.value) g WHERE t.id >
  1`, failed with DuckDB's "Non-inner join on correlated columns not
  supported": the `WHERE` became the `ON` of the last `FLATTEN`'s join,
  which a comma kept from reading the table. Commas before that `FLATTEN`
  are now `CROSS JOIN`s, which nest from the left.
- A `JSON` value that is a string, such as `'"abc"'::JSON` or
  `json_extract(v, '$.name')`, reached PostgreSQL clients without its
  quotes (`abc`), so a client could not tell it from a number or `true`.
  It is now sent as JSON text (`"abc"`).
- A write could land twice, with the first copy's data file deleted, when
  the catalog applied a commit but answered with an error or not at all
  (seen under heavy load as a 503). DuckDB sent the commit again, the
  catalog refused the repeat as a conflict, DuckDB deleted the files of the
  commit it took for failed, and Igloo ran the statement again. DuckDB now
  reaches the catalog through a proxy inside Igloo that applies each commit
  exactly once: it sends commits again itself, with an `Idempotency-Key`,
  checks a refused commit's snapshot against the table before believing the
  refusal, and deletes the files of commits the catalog refused. DuckDB no
  longer deletes files itself, so a table never references a missing file;
  when the catalog cannot be reached long enough that a write's outcome is
  unknown, the statement fails with SQLSTATE `40003` instead of `40001`, and
  its files are kept (docs/operations.md#writes-when-the-catalog-fails).
- A dbt run could find a `__dbt_backup` table it had dropped still listed,
  and DuckDB could crash, when the replica synced its copies of the
  catalog's views at the time. Every 30 seconds the sync looked for copies
  that had gone missing by listing the views of every database DuckDB has
  attached, and DuckDB-Iceberg then lists the tables of each of the
  catalog's schemas, a list it does not keep in step with a session that
  renames or drops one of them at the same time. The background sync no
  longer looks; a copy that has gone missing is made again when a statement
  names it.
- A view a session had just created, replaced, renamed or dropped could
  seem unchanged on its own replica until the next sync, 30 seconds later:
  dbt failed with `view "..._dbt_tmp" does not exist` or `already exists`,
  left `__dbt_backup` views behind, or built a model from a view's previous
  definition. A sync lists the catalog's views without holding up view
  changes, and applied the list even when it was older than a change the
  replica had made meanwhile. Syncs now leave the views their replica
  changed meanwhile as those changes made them, and never apply a list older
  than one already applied; a statement that names a view another replica
  created finds it when any sync brought it, not only its own.
- A view that reads another, such as one dbt model's view read by the
  next, could fail with `view ... could not be loaded: ... Table with name
  ... does not exist!` on replicas other than the one running dbt, and go
  on failing until dbt next changed it. A replica that synced its copies
  of the catalog's views while dbt had a model's view renamed out of the
  way, its new one not yet in place, turned the views that read it into
  stubs that fail when read; later syncs made a stub again only if the
  catalog's view differed from the replica's last good copy, and every dbt
  run makes its views anew at the same versions. Syncs now make every stub
  again, and DuckDB cannot bind a stub: a statement that reads one,
  directly, through other views or in a new view's body, syncs before it
  fails, as one that names a view the replica has not synced does, and
  reads the view once the swap is done. A view the replica still cannot
  make fails it with why, and with the SQLSTATE of the cause (`42P01` for a
  missing table) rather than `22P02`.
- A listing of the catalog (`information_schema`, `pg_catalog`,
  `duckdb_tables()`/`duckdb_views()`, `SHOW`, `DESCRIBE`), or even a plain
  read, could crash or wedge the whole replica, ending every session on it,
  when a table was renamed or dropped at the same time on that replica — a BI
  tool browsing schemas while a `dbt` run swapped tables, which any user with
  read access could set off. The embedded engine keeps one in-memory set of
  the catalog's tables, shared by every session and changed as tables are
  created, renamed, dropped or used; reading it while it changed corrupted
  it. Igloo now serializes the short steps that reach that set on each
  replica — preparing a statement, listing the catalog, and DDL that
  creates, renames, drops or alters a table, view or schema — and lets
  DuckDB bind a statement again as it starts to run only alongside other
  such binds, while reading and writing table data, which do not change the
  set, run alongside each other and those steps without waiting. `CREATE
  [OR REPLACE] TABLE ... AS` runs in steps, so that only creating and
  renaming the new table are serialized, not filling it
  (docs/operations.md#concurrency-on-a-replica).
- Table maintenance could run Igloo out of memory, and then kill it again
  after every restart. Compaction read a group's files all at once, one per
  core, in batches of 131,072 rows however wide, and queued 64 batches for
  the Parquet writer, so its memory grew with the width of the rows and the
  number of files: DuckDB stores a repetitive string in a byte or two, and
  Arrow in full. After 760 tables were mirrored from Snowflake, the first
  pass grew Igloo to 30 GB on a 30 GB machine, the kernel killed it, and the
  overdue pass killed it again about 35 seconds after each restart.
  Compaction now reads a group's files one at a time, in batches and row
  groups sized from how much memory a row of the table takes, and holds at
  most `IGLOO_MAINTENANCE_MEMORY` (by default a quarter of Go's memory
  limit, between 256 MiB and 4 GiB); a table that cannot be compacted within
  it is left as it is and reported. Compacting 255 MiB of DuckDB-written
  Parquet took 6.1 GiB of Go heap and now takes about 1.2 GiB, in half the
  time. Igloo also gives Go a memory limit, unless `GOMEMLIMIT` sets one,
  of what the container's or machine's memory leaves after DuckDB's limit;
  logs each table as it starts compacting it; and, if it died while
  rewriting a table's files, leaves that table's files alone for a day
  instead of rewriting them again within seconds of starting
  (docs/operations.md#compaction-memory).
- A name qualified with a schema, such as `media_demo."AUDIT_EVENTS"`, was
  checked against every database with a schema of that name, not just the
  one DuckDB reads. With a Snowflake connection that has a schema
  `MEDIA_DEMO` too, a user with role `none` and `SELECT` on the catalog's
  `media_demo` was refused (`SELECT on table sf.media_demo.audit_events needs
  a SELECT grant`) though DuckDB reads the catalog's table; so were their
  `EXPLAIN`, writes and DDL by grants, a reader's join of the table with a
  connection's table granted to them, every user's read of it once the
  connection's table had a row access policy, and a policy that read it.
  Names now mean, for these checks, the tables at the places DuckDB looks
  for them, as DuckDB 1.5 binds names: a schema-qualified name in the
  databases whose schema of that name is on the search path, or else in the
  current database. Names in views' bodies follow DuckDB 1.5 too, which
  looks for a schema-qualified one in the view's own database first and for
  an unqualified one not in that database's `main` schema; some statements
  that read views were refused as unverifiable before.
- Errors and column names that PostgreSQL clients rely on differed from
  PostgreSQL's. Every name DuckDB found nothing called failed with SQLSTATE
  `42P01` (undefined table): a missing function (`SELECT
  no_such_function(1)`) now fails with `42883`, a type (`1::NUMBER`) with
  `42704`, a schema with `3F000`. `SHOW no_such_setting` failed as a
  missing table (DuckDB's `SHOW` describes a table); it now fails as
  PostgreSQL's does, `42704` `unrecognized configuration parameter
  "no_such_setting"`. A non-admin's `SET` or `RESET` of a parameter no one
  has was refused as needing admin (`42501`); it now fails with `42704` as
  an admin's does. And `SELECT current_user` named its column `'igloo'`, the
  user's name quoted, so a client reading the column `current_user` found
  nothing; `current_user`, `session_user` and `current_role` that are a
  select list's items of their own now name their columns as PostgreSQL
  does, while the Snowflake dialect keeps Snowflake's names, such as
  `CURRENT_USER()`. Over Flight SQL, a missing schema or database is
  `NOT_FOUND`.
- In Snowflake's dialect, `TO_VARCHAR`, `TO_CHAR` and `::VARCHAR` without a
  format wrote DuckDB's text rather than Snowflake's: a `FLOAT` as
  `999.8899804651736` or `9244.0` where Snowflake writes `999.889980465`
  and `9244`, a `TIMESTAMP_LTZ` with the offset `+00` rather than `+0000`, a
  `TIME` with fractions of a second and `BINARY` as escapes, so column
  profiles that compare these strings differed. They now follow
  Snowflake's default output formats (a `FLOAT` with up to 12 significant
  digits; `TIMESTAMP_OUTPUT_FORMAT`, `TIME_OUTPUT_FORMAT` and
  `BINARY_OUTPUT_FORMAT` at their defaults), in the session's time zone;
  `TZH` and `TZM` in a format write the offset as Snowflake does; and
  `ALTER SESSION` accepts the output format parameters at their defaults
  and refuses others. Profiling four columns of 37 million rows takes 12%
  longer.
- In Snowflake's dialect, `/` gave DuckDB's `DOUBLE` quotient, where
  Snowflake divides two `NUMBER`s into a `NUMBER` of the dividend's scale
  plus 6 digits (at least 6, at most 12), rounded half away from zero:
  `1/3` is `0.333333`, `41/640` is `0.064063` and `1/3*3` is `0.999999`.
  A report that takes a share as a ratio of two counts and then scales or
  compares it differed from Snowflake's in the sixth significant digit (a
  reach of 7.354377% where Snowflake's is 7.354400%, and an index of
  1.843202 against 1.843208), and could order or filter rows otherwise.
  Division of integers and decimals now gives Snowflake's value, as a
  `DOUBLE`; so do `DIV0`, `DIV0NULL` and `RATIO_TO_REPORT`. Integers are
  divided exactly, for about 1 ns a row more than before. A quotient
  divided again is not rounded again, since a `DOUBLE` does not carry its
  scale, and a `FLOAT` on either side still gives a `FLOAT`, unrounded.
  Dividing by zero, which gave `Infinity`, fails with SQLSTATE 22012 as in
  Snowflake, `FLOAT`s too; a `NULL` divided by zero is `NULL`, and `DIV0`
  and `DIV0NULL` still give 0.
- In Snowflake's dialect, `APPROX_COUNT_DISTINCT` and `HLL` were DuckDB's
  `approx_count_distinct`, a HyperLogLog of 64 registers that errs by about
  13% and by over 50% at worst. On a table of 1.25 million rows it
  counted 1,336 values where there were 1,099 and Snowflake counted 1,102,
  and tools that decide from these counts which columns are categories
  got it wrong. They now count with DataSketches' HyperLogLog of 4,096
  registers, as many as Snowflake's, within about 1.6%, and exactly up to
  about 300 values, as fast as before. In a select with `GROUP BY` and as
  window functions they count exactly, since a sketch per group is 25
  times slower than `COUNT(DISTINCT)` with millions of groups; with a few
  hundred groups that takes up to three times as long as before.
- A table whose name the catalog spells with capitals, such as one copied
  from Snowflake as `ORDERS`, was listed in whatever case a statement or
  view had last named it in: once a view read it as `orders`, every replica
  that loaded the view listed `orders` in `duckdb_tables()`,
  `information_schema.tables` and `SHOW TABLES`. The catalog finds a table
  by its name in any case, and the embedded engine names its entry for a
  table as it was last looked up; the catalog's own name never changed.
  Igloo now notes, as it loads a table, when it was named otherwise than
  the catalog spells it, and looks it up again by the catalog's name before
  anything lists the catalog, so listings always show the catalog's name.
  Reading a table by its name in any case works as before.
- DuckDB's spill files outlived the Igloo process that wrote them when it
  stopped, cleanly or killed, while data was spilled, and the next start
  did not delete them: on a single machine whose replica restarted during
  large queries, 59 GB of them filled the spill volume, and later queries
  failed with `No space left on device` or `failed to offload data block`.
  DuckDB deletes them only as its database closes, and Igloo kept its
  database open until the process exited, through connections it had
  opened to register its functions. Igloo now closes those connections, so
  a clean stop deletes the files, and when it starts it deletes those a
  killed or crashed process left, before it runs any query, and logs the
  bytes freed.
  It deletes only DuckDB's own files in `IGLOO_DUCKDB_TEMP_DIR`
  (`duckdb_temp_storage_*`, `duckdb_temp_block-*`), and none while another
  Igloo process uses the directory, which it tells by a lock that each
  process holds there (docs/operations.md#spill-directory).

[Unreleased]: https://github.com/akkio-inc/Igloo/commits/main
