# ML functions

Igloo trains and scores predictive models in SQL, with the statements and
methods of Snowflake's ML classes, so the SQL that uses them on Snowflake
runs on Igloo as written. A model is an object in a schema, like a table:
it is created from a table, view or query, used by its owner and whoever
they grant it to, and dropped.

```sql
CREATE SNOWFLAKE.ML.CLASSIFICATION demo.conversion(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'demo.lead_features'),
    TARGET_COLNAME => 'converted');

SELECT person_id,
       demo.conversion!PREDICT(INPUT_DATA => {*}):probability:True::FLOAT AS p_convert
FROM demo.new_leads;

CALL demo.conversion!SHOW_GLOBAL_EVALUATION_METRICS();
```

| Class | What it does | Methods |
|---|---|---|
| `SNOWFLAKE.ML.FORECAST` | Forecasts one or many time series, with prediction intervals | `FORECAST`, `SHOW_EVALUATION_METRICS`, `EXPLAIN_FEATURE_IMPORTANCE`, `SHOW_TRAINING_LOGS` |
| `SNOWFLAKE.ML.ANOMALY_DETECTION` | Flags new points of time series that fall outside their forecast's interval | `DETECT_ANOMALIES`, `SHOW_EVALUATION_METRICS`, `EXPLAIN_FEATURE_IMPORTANCE`, `SHOW_TRAINING_LOGS` |
| `SNOWFLAKE.ML.CLASSIFICATION` | Predicts a class (2 to 255) from a row's columns | `PREDICT`, `SHOW_EVALUATION_METRICS`, `SHOW_GLOBAL_EVALUATION_METRICS`, `SHOW_CONFUSION_MATRIX`, `SHOW_THRESHOLD_METRICS`, `SHOW_FEATURE_IMPORTANCE`, `SHOW_TRAINING_LOGS` |
| `SNOWFLAKE.ML.TOP_INSIGHTS` | Finds the segments that drove a metric's change between two groups | `GET_DRIVERS` |

Besides the classes, Databricks' `ai_predict_class` and `ai_predict_value`
train a model and score rows in one query, keeping no model (see
[below](#train-and-score-in-one-query)).

Models are trained by Igloo's own engine: gradient-boosted trees, as
Snowflake's classes use (see [the engine](#the-engine)). Training reads its
input as the user who creates the model, through their grants and
[policies](security-policies.md); scoring with `PREDICT` runs wherever the
query runs, on [workers](distributed-execution.md) too.

## Statements

These are Snowflake's statements, which Igloo reads in either SQL dialect.
In the DuckDB dialect, too, their arguments and the queries in them are
Snowflake's SQL.

```sql
CREATE [ OR REPLACE ] SNOWFLAKE.ML.<class> [ IF NOT EXISTS ] name( <arguments> )
    [ [ WITH ] TAG ( ... ) ] [ COMMENT = 'text' ]
DROP SNOWFLAKE.ML.<class> [ IF EXISTS ] name
SHOW SNOWFLAKE.ML.<class> [ INSTANCES ] [ LIKE 'pattern' ] [ IN ACCOUNT | DATABASE | SCHEMA [ name ] ]
CALL name!<method>( <arguments> )
SELECT ... FROM TABLE( name!<method>( <arguments> ) )
GRANT USAGE ON SNOWFLAKE.ML.<class> name TO [ ROLE | USER ] grantee
GRANT SNOWFLAKE.ML.<class> ROLE name!MLADMIN | MLCONSUMER TO [ ROLE | USER ] grantee
REVOKE ... FROM [ ROLE | USER ] grantee
```

- **Names.** A model's name is `name`, `schema.name` or
  `database.schema.name`, where the database is the catalog's. A name
  alone is in the session's current schema. Names are case-insensitive.
- **Arguments** are named (`TARGET_COLNAME => 'converted'`), as in
  Snowflake's documentation; they are literals, objects such as
  `{'evaluate': FALSE}`, and references to rows.
- **Training data.** `INPUT_DATA` takes a table or view,
  `TABLE(name)` or `SYSTEM$REFERENCE('TABLE' | 'VIEW', 'name')`, or a
  query, `SYSTEM$QUERY_REFERENCE('SELECT ...')`. Every column but the
  target is a feature: numbers are continuous, strings and booleans
  categories, and timestamps and dates their seconds since 1970. Other
  types are refused: cast them.
- **`CREATE OR REPLACE`** trains the model again, under the same name; its
  grants stay. Models never change otherwise: `ALTER` is refused, and
  `COMMENT` is set when a model is created. `TAG` is accepted and ignored,
  with a notice. A model's row records the `CREATE` statement that trained
  it, its configuration and a summary of its metrics.
- **Methods** that return rows run with `CALL`, or as a table in a query
  with `TABLE(name!METHOD(...))`, which can be filtered, joined and saved
  with `CREATE TABLE ... AS`. In Snowflake's dialect their columns are
  named in upper case. `name!PREDICT(...)` is an expression.
- **`SHOW`** lists the models of a class the user may use, with Snowflake's
  columns: `created_on`, `name`, `database_name`, `schema_name`,
  `current_version`, `comment` and `owner`.

### Privileges

| To | Takes |
|---|---|
| Create a model in a schema | What creating a table there takes: the writer role, or `CREATE` on the schema (`GRANT CREATE ON SCHEMA demo TO ...`) |
| Replace, drop or grant a model | Owning it (its creator), or the admin role |
| Call every method | Owning it, admin, or `USAGE` (Snowflake's `MLADMIN` instance role) |
| Predict: `PREDICT`, `FORECAST`, `DETECT_ANOMALIES`, `GET_DRIVERS` | Any of those, or Snowflake's `MLCONSUMER` instance role |

A user who may not use a model is told it does not exist. A model answers
from what its creator could read when it was trained; whoever may use it
gets its predictions, not its training rows. They read the rows they score
through their own grants and policies, as in any query. Snowflake's
`GRANT CREATE SNOWFLAKE.ML.<class> ON SCHEMA` is refused: grant `CREATE`
on the schema.

## FORECAST

```sql
CREATE [ OR REPLACE ] SNOWFLAKE.ML.FORECAST [ IF NOT EXISTS ] name(
    INPUT_DATA => <table, view or query>,
    [ SERIES_COLNAME => '<column>', ]
    TIMESTAMP_COLNAME => '<column>',
    TARGET_COLNAME => '<column>',
    [ CONFIG_OBJECT => { ... } ]
)

CALL name!FORECAST( FORECASTING_PERIODS => n [, SERIES_VALUE => 'series' ] [, CONFIG_OBJECT => { 'prediction_interval': 0.95 } ] )
CALL name!FORECAST( INPUT_DATA => <future rows>, TIMESTAMP_COLNAME => '<column>' [, SERIES_COLNAME => '<column>' ] [, CONFIG_OBJECT => ... ] )
```

```sql
CREATE SNOWFLAKE.ML.FORECAST demo.spend_forecast(
    INPUT_DATA => TABLE(demo.daily_spend),
    SERIES_COLNAME => 'campaign', TIMESTAMP_COLNAME => 'day', TARGET_COLNAME => 'spend');

CREATE TABLE demo.spend_next_week AS
    SELECT * FROM TABLE(demo.spend_forecast!FORECAST(FORECASTING_PERIODS => 7));
```

- **The method** is Snowflake's `fast` one, whatever `method` says
  (`'best'`, Snowflake's default, ensembles models Igloo does not have):
  gradient-boosted trees on lags and rolling means of the target, calendar
  features of the timestamp (day of week, month and the like, by the
  series' frequency), the other columns of the input (exogenous
  variables) and the series. Each series is scaled to its mean, and a
  series with a trend has its linear trend taken out first, which the
  forecast carries on. One model serves every series of a model.
  Forecasts are made a step at a time, each on the ones before.
- **Series.** `SERIES_COLNAME` names a column whose values identify series:
  a model forecasts each. A series needs at least 2 points; one of 2 to 11
  points forecasts its last value (a naive forecast), and fewer than 60
  training points in all train a linear model rather than trees, as in
  Snowflake. `FORECAST` forecasts every series, or `SERIES_VALUE`'s.
- **Timestamps** are timestamps or dates. The frequency is the most common
  gap between them, or `'frequency'`, such as `'1 day'` or `'15 minutes'`
  (seconds, minutes, hours, days, weeks, months, quarters and years).
  Several rows at one step are aggregated: the target and other numbers
  by their mean, categories by their most common value, unless
  `aggregation_target`, `aggregation_numeric`, `aggregation_categorical` or
  `aggregation_column` (`{'promo': 'MAX'}`) say otherwise (`MEAN`,
  `MEDIAN`, `MODE`, `MIN`, `MAX`, `SUM`, `FIRST`, `LAST`). Steps without a
  row are filled in from their neighbours for the lags, and do not train.
- **Exogenous columns.** A model trained with columns besides the series,
  timestamp and target forecasts with their future values:
  `INPUT_DATA` holds a row for each future timestamp (and series), and
  `FORECAST` returns a forecast for each. Without them it is refused.
- **Prediction intervals** come from cross-validation: models trained on
  each series but its last steps forecast them, and the spread of their
  errors, by how many steps ahead, sets an interval around each forecast
  (normal, at `prediction_interval`, 0.95 by default). They widen with the
  horizon. `lower_bound` and `upper_bound` clamp forecasts and intervals.
- **Evaluation** follows scikit-learn's `TimeSeriesSplit`, as Snowflake's:
  `evaluation_config`'s `n_splits` (default 1), `test_size` (default each
  series' length divided by `n_splits` + 1), `gap` and `max_train_size`.
  `'evaluate': FALSE` keeps no metrics; the intervals still come from
  cross-validation.
- `on_error: 'skip'` leaves out series that cannot be trained (fewer than
  two points) rather than failing; `SHOW_TRAINING_LOGS` says which.

| Method | Columns |
|---|---|
| `FORECAST(...)` | `SERIES` (`NULL` for one series), `TS`, `FORECAST`, `LOWER_BOUND`, `UPPER_BOUND` |
| `SHOW_EVALUATION_METRICS()` | `SERIES`, `ERROR_METRIC` (`MAE`, `MAPE`, `MDA`, `MSE`, `SMAPE`, `WINKLER_ALPHA=0.05`, `COVERAGE_INTERVAL=0.95`), `METRIC_VALUE`, `STANDARD_DEVIATION` (over the splits; `NULL` for one), `LOGS` |
| `SHOW_EVALUATION_METRICS(INPUT_DATA => ..., TIMESTAMP_COLNAME => ..., TARGET_COLNAME => ... [, SERIES_COLNAME => ...] [, CONFIG_OBJECT => {'prediction_interval': ...}])` | The same, of forecasts of new data, after the training data |
| `EXPLAIN_FEATURE_IMPORTANCE()` | `SERIES`, `RANK`, `FEATURE_NAME`, `IMPORTANCE_SCORE` (shares of the mean absolute SHAP value, adding up to 1 per series), `FEATURE_TYPE`: `derived_from_endogenous` for `aggregated_endogenous_features` (the target's lags and rolling means together), `derived_from_timestamp` for calendar features, `user_provided` for exogenous columns |
| `SHOW_TRAINING_LOGS()` | `SERIES`, `LOGS` |

## ANOMALY_DETECTION

```sql
CREATE [ OR REPLACE ] SNOWFLAKE.ML.ANOMALY_DETECTION [ IF NOT EXISTS ] name(
    INPUT_DATA => <table, view or query>,
    [ SERIES_COLNAME => '<column>', ]
    TIMESTAMP_COLNAME => '<column>',
    TARGET_COLNAME => '<column>',
    LABEL_COLNAME => '<column>' | '',
    [ CONFIG_OBJECT => { ... } ]
)

CALL name!DETECT_ANOMALIES(
    INPUT_DATA => <new rows>, TIMESTAMP_COLNAME => '<column>', TARGET_COLNAME => '<column>'
    [, SERIES_COLNAME => '<column>' ] [, CONFIG_OBJECT => { 'prediction_interval': 0.99 } ] )
```

- An anomaly model is a forecast model: `DETECT_ANOMALIES` forecasts the
  new points' timestamps, which must come after the training data's, and
  flags those outside their prediction interval (0.99 by default), as
  Snowflake's does. `CONFIG_OBJECT` takes what `FORECAST`'s does; its
  evaluation has 5 splits by default.
- `LABEL_COLNAME` names a boolean column of known anomalies, which do not
  train the model; `''` means none. Unlabeled anomalies in the training data
  are left out too: points far outside a first fit (five times its typical
  error) are dropped and the model trained again.

| Method | Columns |
|---|---|
| `DETECT_ANOMALIES(...)` | `SERIES`, `TS`, `Y`, `FORECAST`, `LOWER_BOUND`, `UPPER_BOUND`, `IS_ANOMALY`, `PERCENTILE` (where `Y` falls in the forecast's distribution), `DISTANCE` (its z-score) |
| `SHOW_EVALUATION_METRICS`, `EXPLAIN_FEATURE_IMPORTANCE`, `SHOW_TRAINING_LOGS` | As `FORECAST`'s |

## CLASSIFICATION

```sql
CREATE [ OR REPLACE ] SNOWFLAKE.ML.CLASSIFICATION [ IF NOT EXISTS ] name(
    INPUT_DATA => <table, view or query>,
    TARGET_COLNAME => '<column>',
    [ CONFIG_OBJECT => { 'evaluate': TRUE | FALSE,
                         'evaluation_config': { 'test_fraction': <fraction> },
                         'on_error': 'ABORT' | 'SKIP' } ]
)
```

- The target is a boolean, number or string column with 2 to 255 distinct
  values. Rows whose target is `NULL` are left out, and
  `SHOW_TRAINING_LOGS` says how many. Two classes train a binary model,
  more a multiclass one.
- **Evaluation.** Unless `'evaluate': FALSE`, a sample of each class is
  held out, the model trained on the rest is measured on it, and the
  model is then trained again on every row. The sample is Snowflake's
  default, enough for 500 rows of the rarest class but at most 20% of each,
  or `test_fraction` of each class.
- **`PREDICT`** takes `INPUT_DATA`, an object of the row's features:
  `{*}` (every column of the query's `FROM`), `{* EXCLUDE (id, label)}`,
  `{t.*}`, `OBJECT_CONSTRUCT(*)`, or keys and values such as
  `{'region': region, 'spend': spend}` or `OBJECT_CONSTRUCT('region',
  region)`. Features it does not give are missing. It returns Snowflake's
  object:

  ```json
  {"class": "True", "logs": null, "probability": {"False": 0.0118, "True": 0.9882}}
  ```

  Booleans' classes are `True` and `False`, numbers' as they read
  (`0`, `1`, `2.5`), strings as they are. Read the class with
  `:class::STRING` and a probability with `:probability:True::FLOAT`.

| Method | Columns |
|---|---|
| `SHOW_EVALUATION_METRICS()` | `DATASET_TYPE` (`EVAL`), `CLASS`, `ERROR_METRIC` (`precision`, `recall`, `f1`, `support`), `METRIC_VALUE`, `LOGS` |
| `SHOW_GLOBAL_EVALUATION_METRICS()` | `DATASET_TYPE`, `AVERAGE_TYPE` (`macro`, `weighted`, or `NULL` for `log_loss`), `ERROR_METRIC` (`precision`, `recall`, `f1`, `auc`, `log_loss`), `METRIC_VALUE`, `LOGS` |
| `SHOW_CONFUSION_MATRIX()` | `DATASET_TYPE`, `ACTUAL_CLASS`, `PREDICTED_CLASS`, `COUNT`, `LOGS` |
| `SHOW_THRESHOLD_METRICS()` | `DATASET_TYPE`, `CLASS`, `THRESHOLD` (0 to 1 by 0.01), `PRECISION`, `RECALL`, `F1`, `TPR`, `FPR`, `TP`, `FP`, `TN`, `FN`, `ACCURACY`, `SUPPORT`, `LOGS` |
| `SHOW_FEATURE_IMPORTANCE()` | `RANK`, `FEATURE`, `SCORE` (each feature's share of the mean absolute SHAP value, adding up to 1), `FEATURE_TYPE` (`user_provided`) |
| `SHOW_TRAINING_LOGS()` | `LOGS` |

The evaluation methods fail for a model trained with `'evaluate': FALSE`.

## TOP_INSIGHTS

```sql
CREATE [ OR REPLACE ] SNOWFLAKE.ML.TOP_INSIGHTS [ IF NOT EXISTS ] name();

CALL name!GET_DRIVERS(
    INPUT_DATA => <table, view or query>,
    LABEL_COLNAME => '<boolean column: TRUE for test rows, FALSE for control>',
    METRIC_COLNAME => '<number column>' );
```

```sql
CREATE SNOWFLAKE.ML.TOP_INSIGHTS demo.spend_drivers();

SELECT * FROM TABLE(demo.spend_drivers!GET_DRIVERS(
    INPUT_DATA => SYSTEM$QUERY_REFERENCE($$
        SELECT country, device, channel, day >= '2026-10-01' AS is_test, spend
        FROM demo.daily_spend WHERE day >= '2026-09-01'$$),
    LABEL_COLNAME => 'is_test', METRIC_COLNAME => 'spend'));
```

- Every column but the label and the metric is a **dimension**: strings
  and booleans are categories, numbers (and timestamps) continuous. A
  dimension's 25 most influential categories are considered.
- A tree splits the rows, at each segment by the condition whose rows'
  change departs most from the segment's own rate of change
  (`|test - control * test/control|` of the rows that meet it): a category
  (`COUNTRY = us` and `not COUNTRY = us`) or a threshold (`AGE <= 30.5` and
  `AGE > 30.5`). It goes three conditions deep, and splits only where that
  accounts for at least a twentieth of the change.
- Every segment of the tree is a row, `Overall` first, then by contribution:

| Column | Is |
|---|---|
| `CONTRIBUTOR` | The segment's conditions, an array such as `["COUNTRY = us","DEVICE = mobile"]`; `["Overall"]` for all the rows. Dimensions are named in upper case, as Snowflake names them, unless their name has upper-case letters |
| `METRIC_CONTROL`, `METRIC_TEST` | The segment's metric totals in the control and test rows |
| `CONTRIBUTION` | `METRIC_TEST - METRIC_CONTROL` |
| `RELATIVE_CONTRIBUTION` | Its share of the overall change |
| `GROWTH_RATE` | `CONTRIBUTION / METRIC_CONTROL` |

An instance keeps nothing: `GET_DRIVERS` analyses what it is given, as its
caller reads it.

## Train and score in one query

Databricks' predictive functions train a model on the rows whose target is
known and score the rows whose target is `NULL`, in one query, and keep no
model: the simplest way to score propensity or find lookalikes, with the
known converters as 1, a sample of non-converters as 0, and everyone else
`NULL`. Igloo has them in both dialects:

```sql
SELECT person_id, converted_prediction, converted_confidence
FROM ai_predict_class(
    input => TABLE(demo.lead_features),          -- or TABLE(SELECT ...)
    target_col => 'converted',
    feature_cols => ARRAY_CONSTRUCT('region', 'spend', 'visits'),  -- ['region', 'spend', 'visits'] in DuckDB's dialect
    seed => 7);

SELECT * FROM ai_predict_value(input => TABLE(demo.accounts), target_col => 'next_month_spend',
                               feature_cols => ['segment', 'spend', 'tenure']);
```

- They return the input's rows whose target is `NULL`, with every column,
  and `<target>_prediction`: for `ai_predict_class` the class, of the
  target's type, and `<target>_confidence`, its probability; for
  `ai_predict_value` a `DOUBLE`.
- The model is trained as `CLASSIFICATION`'s (2 to 255 classes) or as a
  regression, on the rows whose target is set, without held-out metrics;
  `seed` (default 0) seeds it. It is kept in memory for the session while
  the statement may run again, and never stored.
- They read their input as the user, policies applied, as the rest of the
  query does, and score on the replica that runs the query.

## Tasks

A task's statement may call models' methods: the model is the one the
name means when the task runs, checked against the task owner's
privileges then. A task runs in its owner's default schema, so name models
by their schema there.

## Where models live

- A model's row is in the metadata database (`IGLOO_METADATA_DATABASE_URL`),
  shared by every replica: its name, class, owner, comment, the `CREATE`
  statement, its configuration and metrics, and the SHA-256 of its blob.
  Without a metadata database, models are kept in memory and lost on
  restart.
- Its **blob**, the trained model, is a file in the warehouse's object
  storage under `.igloo/models/`, among Igloo's own files, named by its
  SHA-256. Workers read blobs there with the access to the warehouse they
  already have, and every process keeps the models it has used in memory.
  Users other than admins cannot read the warehouse's files from SQL.
  Without a warehouse, blobs are in memory.
- Dropping or replacing a model deletes its blob once no model uses it. A
  statement prepared before a model was replaced fails with a message to
  run it again.
- **Personal data.** A model keeps no training rows, but it keeps what it
  learned from them: the categories it splits on, and a time series
  model's series values and its series' last steps. Categories that fewer
  than five training rows have are never kept: they share one bin. A model
  row keeps its `CREATE` statement. [Erasure requests](privacy.md) do not
  reach models: a model trained on rows a request later deletes still
  answers from what it learned, so train it again after erasures.

## The engine

Models are trained by Igloo's engine, in Go: gradient-boosted trees (binary,
multiclass, regression and quantile objectives), with LightGBM's methods
for speed and accuracy, and SHAP values to explain them; it is Igloo's
own Apache-2.0 code and needs nothing besides Igloo.

- **Training** happens on the replica that runs `CREATE`, from the rows of
  its input in memory: up to 200 million values (rows times columns),
  which take a few gigabytes besides DuckDB's memory while it trains; train
  on a sample of larger inputs. A statement's timeout and cancelling it stop
  training.
  Boosting stops when a held-out tenth of the training rows stops
  improving, after at most 500 rounds. On one machine, 100 rounds over a
  million rows of 50 features take about 7 seconds on 18 cores and 13 on
  2; training a `CLASSIFICATION` model on such a table, holdout metrics
  included, takes about 15 seconds on 18 cores (`BenchmarkClassifier1M` in
  `internal/models`, run with `IGLOO_ML_BENCH=1`).
- **Scoring** with `PREDICT` runs as the query runs: a statement that
  scores a large table runs in parts on workers like any other, each
  scoring its share. Methods that return tables (`FORECAST`,
  `DETECT_ANOMALIES` and the `SHOW_` methods) are computed on the replica,
  from the data they are given, before the statement that reads them runs.
- **Models are repeatable.** The same rows, in any order, train the same
  model, with the same blob and SHA-256: the engine puts rows in an order
  of their own before training, and adds in a fixed order whatever the
  threads.

## Differences from Snowflake

- Models are objects of Igloo's metadata database, not of the Iceberg
  catalog: other engines reading Igloo's tables do not see them, and
  dropping a schema leaves its models.
- A **view** cannot call a model's methods (`m!PREDICT` or `TABLE(m!...)`),
  nor can a macro or a materialized view: a model is resolved, and its use
  checked, as each statement is prepared, which a view's stored SQL is
  not. Create a table from the query instead, or a
  [task](loading.md) that does so on a schedule. A task's statement is
  resolved each time it runs, with the model as it is then.
- Forecasts, intervals, feature importance and metrics come from Igloo's
  engine, so their values differ from Snowflake's for the same data.
  `FORECAST`'s `'best'` method is its `'fast'` one. A time series model's
  series train together on one replica, not split among workers. `SHOW_FEATURE_IMPORTANCE` and
  the model's features keep their columns' names as Igloo has them: in
  lower case for a table's columns, in upper case for a Snowflake query's
  aliases (`AS domain` is `DOMAIN`).
- `ALTER` of models, Snowflake's other ML classes (`CUSTOM_CLASSIFIER` and
  the like), the Model Registry (`MODEL(...)!method`) and
  `GRANT CREATE SNOWFLAKE.ML.<class>` are refused.
