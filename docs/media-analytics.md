# Media analytics

Igloo has the functions an agency's analytics team reaches for every day:
sketches that merge into deduplicated reach across campaigns, platforms and
days; geometry and H3 functions for DMA, zip code and store-radius reporting;
and DuckDB's approximate, statistical and time zone functions. They come from
three DuckDB extensions baked into the image, and from DuckDB itself:

| Extension | License | What it adds |
|---|---|---|
| [datasketches](https://github.com/query-farm/datasketches) (community) | MIT, around [Apache DataSketches](https://datasketches.apache.org/) (Apache 2.0) | HLL, CPC and theta sketches for distinct counts; KLL, REQ, t-digest and frequent-item sketches |
| [spatial](https://duckdb.org/docs/stable/core_extensions/spatial/overview) (core) | MIT, with GDAL and PROJ (MIT) and GEOS (LGPL 2.1) inside | Geometries, points in polygons, distances, coordinate systems |
| [h3](https://github.com/isaacbrodsky/h3-duckdb) (community) | Apache 2.0 | The H3 hexagonal grid |

They are loaded when Igloo starts, for every user; nothing is installed at
runtime. The examples below use these tables:

```sql
-- One row per ad impression.
CREATE TABLE impressions (
  served_at TIMESTAMPTZ, campaign_id INTEGER, platform VARCHAR,
  user_id VARCHAR, lon DOUBLE, lat DOUBLE, cost_usd DECIMAL(12, 6)
);
```

## Reach and frequency with sketches

Reach, the number of distinct people a campaign reached, does not add up:
the people who saw campaign A on Monday overlap with those who saw it on
Tuesday and with those who saw campaign B. Exact deduplication means
rereading every impression for every combination of campaigns, platforms and
dates someone asks about. A sketch is a small summary of a set of IDs that
can be stored and merged later: build one per campaign, platform and day
once, and any combination's reach is a merge of a few hundred small values.

Two kinds of sketch fit reach:

| Sketch | Size at `lg_k` 12 | Error (one standard deviation) | Merge | Overlap |
|---|---|---|---|---|
| HLL (`datasketch_hll`) | about 2 KB | about 1.6% | `datasketch_hll_union` | No |
| Theta (`datasketch_theta`) | up to 32 KB | about 1.6% | `datasketch_theta` over sketches, `datasketch_theta_union` | `datasketch_theta_intersect`, `datasketch_theta_a_not_b` |

`lg_k` trades size for accuracy: each step up doubles the size and divides
the error by about 1.4. Use HLL when you only need unions, theta when you
need overlaps. Build every sketch you will merge with the same `lg_k`, and
from IDs of the same type: the same ID as `VARCHAR` and as `BIGINT` hashes
differently.

### Store sketches per campaign, platform and day

```sql
CREATE TABLE daily_reach AS
SELECT campaign_id, platform, served_at::DATE AS day,
       count(*) AS impressions,
       datasketch_hll(12, user_id)::BLOB AS hll,
       datasketch_theta(12, user_id)::BLOB AS theta
FROM impressions
GROUP BY ALL;

-- Each night, add yesterday's.
INSERT INTO daily_reach
SELECT campaign_id, platform, served_at::DATE, count(*),
       datasketch_hll(12, user_id)::BLOB, datasketch_theta(12, user_id)::BLOB
FROM impressions
WHERE served_at::DATE = current_date - 1
GROUP BY ALL;
```

Sketches are stored as `BLOB` (Iceberg `binary`), which any engine can keep;
the functions take them back as they are, or with a cast such as
`theta::sketch_theta`.

### Deduplicated reach across a media plan

```sql
SELECT sum(impressions) AS impressions,
       round(datasketch_hll_estimate(datasketch_hll_union(12, hll))) AS reach,
       round(sum(impressions) / datasketch_hll_estimate(datasketch_hll_union(12, hll)), 2) AS avg_frequency
FROM daily_reach
WHERE campaign_id IN (101, 102, 117)
  AND day BETWEEN DATE '2026-10-01' AND DATE '2026-10-31';
```

Reach by platform, and for the plan as a whole, in one statement:

```sql
SELECT coalesce(platform, 'all platforms') AS platform,
       round(datasketch_hll_estimate(datasketch_hll_union(12, hll))) AS reach
FROM daily_reach
WHERE campaign_id IN (101, 102, 117)
GROUP BY ROLLUP (platform);
```

A reach curve, cumulative reach by day of the flight:

```sql
SELECT d.day, round(datasketch_hll_estimate(datasketch_hll_union(12, r.hll))) AS cumulative_reach
FROM (SELECT DISTINCT day FROM daily_reach WHERE campaign_id = 101) d
JOIN daily_reach r ON r.campaign_id = 101 AND r.day <= d.day
GROUP BY d.day ORDER BY d.day;
```

### Overlap between two platforms' audiences

```sql
WITH p AS (
  SELECT platform, datasketch_theta(12, theta::sketch_theta) AS s
  FROM daily_reach
  WHERE campaign_id = 101 AND platform IN ('ctv', 'social')
  GROUP BY platform
),
ctv AS (SELECT s FROM p WHERE platform = 'ctv'),
social AS (SELECT s FROM p WHERE platform = 'social')
SELECT round(datasketch_theta_estimate(ctv.s)) AS ctv_reach,
       round(datasketch_theta_estimate(social.s)) AS social_reach,
       round(datasketch_theta_estimate(datasketch_theta_intersect(ctv.s, social.s))) AS both,
       round(datasketch_theta_estimate(datasketch_theta_a_not_b(social.s, ctv.s))) AS social_only,
       round(datasketch_theta_estimate(datasketch_theta_union(ctv.s, social.s))) AS combined
FROM ctv, social;
```

`social_only` is the incremental reach social adds to CTV. Intersections
of small sets have larger relative errors than unions; use them for
overlaps of at least a few percent of the smaller audience.

### Frequency distribution

How many people saw a campaign once, twice, and so on is a question about
each person, which sketches do not keep, so it reads the impressions, with
ordinary SQL:

```sql
SELECT least(frequency, 10) AS frequency,   -- 10 means 10 or more
       count(*) AS people,
       round(100 * count(*) / sum(count(*)) OVER (), 1) AS pct
FROM (SELECT user_id, count(*) AS frequency
      FROM impressions WHERE campaign_id = 101
      GROUP BY user_id)
GROUP BY ALL ORDER BY 1;
```

People reached three or more times (effective reach at 3+), and the share of
impressions served beyond a frequency cap of 8:

```sql
SELECT count(*) FILTER (WHERE frequency >= 3) AS reach_3_plus,
       sum(greatest(frequency - 8, 0)) / sum(frequency) AS share_over_cap
FROM (SELECT user_id, count(*) AS frequency
      FROM impressions WHERE campaign_id = 101 GROUP BY user_id);
```

For an exact distinct count of one ad hoc set, use `count(DISTINCT
user_id)`; it needs no sketch. DuckDB's own `approx_count_distinct(user_id)`
is fast but rough: its HyperLogLog has 64 registers and errs by about 13%
(one standard deviation, measured from 1,000 to 10 million values), by
over 50% in the worst of 200 draws. For an approximate count within about
1.6%, use `datasketch_hll_estimate(datasketch_hll(12, user_id))`, which
Snowflake SQL's `APPROX_COUNT_DISTINCT` becomes when ungrouped; it is as
fast ungrouped and much slower with many groups, where Snowflake SQL's
`APPROX_COUNT_DISTINCT` counts exactly instead.

## Geospatial

Geometries use (longitude, latitude) order everywhere: `ST_Point(lon, lat)`.
Igloo sets spatial's `geometry_always_xy`, so `ST_Distance_Sphere` and
`ST_Transform` read coordinates that way too. Geometries reach clients as
text in WKT, such as `POINT (-73.98 40.75)`, which psql and BI tools show
and GIS libraries parse.

### Loading boundaries

Load DMA, zip code (ZCTA) or store boundaries from a location in
`IGLOO_ALLOWED_LOCATIONS`:

```sql
-- GeoParquet: DuckDB reads its geometry column as a geometry.
CREATE TABLE zips AS SELECT * FROM read_parquet('s3://landing/zcta.parquet');

-- Shapefiles, such as the Census Bureau's: .shp, .shx and .dbf side by side.
CREATE TABLE dmas AS
SELECT dma_code, dma_name, ST_AsWKB(geom) AS boundary
FROM ST_ReadSHP('s3://landing/dma/dma_boundaries.shp');

-- GeoJSON feature collections.
CREATE TABLE stores AS
SELECT f.properties.store_id::VARCHAR AS store_id,
       ST_AsWKB(ST_GeomFromGeoJSON(f.geometry)) AS location
FROM (SELECT unnest(features) AS f
      FROM read_json('s3://landing/stores.geojson', maximum_object_size = 1000000000));

-- Text in WKT, from a CSV.
CREATE TABLE regions AS
SELECT region, ST_AsWKB(ST_GeomFromText(wkt)) AS boundary FROM read_csv('s3://landing/regions.csv');
```

Keep geometries in Iceberg tables as WKB in a `BLOB` column, as above
(`ST_AsWKB`), and turn them back into geometries with `ST_GeomFromWKB` when
you query them. Other engines read WKB too. A `GEOMETRY` column cannot be
stored: the catalog refuses the table, with an error that the schema `did
not match any variant`.

`ST_Read`, and with it `SELECT * FROM 'file.gpkg'` and `.fgb`, and
`COPY ... (FORMAT GDAL)`, are not available: they go through GDAL, which
can open files and URLs outside Igloo's sandbox (see [security](#security)).
Convert GeoPackage, KML or FlatGeobuf files to GeoParquet or GeoJSON first,
for example with `ogr2ogr -f Parquet zips.parquet zips.gpkg`.

### Impressions by DMA and zip code

A point-in-polygon join. DuckDB plans it as a spatial join, so it does not
compare every impression with every boundary:

```sql
SELECT d.dma_name, count(*) AS impressions, count(DISTINCT i.user_id) AS reach
FROM impressions i
JOIN dmas d ON ST_Contains(ST_GeomFromWKB(d.boundary), ST_Point(i.lon, i.lat))
WHERE i.campaign_id = 101
GROUP BY d.dma_name ORDER BY impressions DESC;
```

For large impression tables, assign each impression's zip code or DMA once
when it lands (with the same join in an `INSERT ... SELECT`), and report on
the assigned column.

### Store radius

People who saw an ad within 5 km of a store:

```sql
SELECT s.store_id, count(DISTINCT i.user_id) AS reach
FROM impressions i
JOIN stores s ON ST_Distance_Sphere(ST_GeomFromWKB(s.location), ST_Point(i.lon, i.lat)) <= 5000
GROUP BY s.store_id;
```

`ST_Distance_Sphere` is in meters, on a sphere; `ST_Distance_Spheroid` is
slower and more exact.

### H3 hexagons

H3 cells turn geography into an ordinary column, which joins and groups as
fast as any other: aggregate impressions per cell for heat maps, or cover a
polygon with cells to approximate points in it with an equality join.

```sql
SELECT h3_h3_to_string(h3_latlng_to_cell(lat, lon, 7)) AS cell, count(*) AS impressions
FROM impressions GROUP BY ALL;

-- Cells of resolution 9 (about 0.1 km²) that cover each DMA.
CREATE TABLE dma_cells AS
SELECT dma_code, unnest(h3_polygon_wkt_to_cells(ST_AsText(ST_GeomFromWKB(boundary)), 9)) AS cell
FROM dmas;
SELECT c.dma_code, count(*) FROM impressions i
JOIN dma_cells c ON c.cell = h3_latlng_to_cell(i.lat, i.lon, 9)
GROUP BY ALL;
```

Note that `h3_latlng_to_cell` takes latitude first.

## Approximate and statistical functions

These are DuckDB's own and need no extension. Every user can call them.

| Need | Functions |
|---|---|
| Distinct counts | `count(DISTINCT x)`, `approx_count_distinct(x)` (about 13% error; see [above](#reach-and-frequency-with-sketches) for 1.6%) |
| Quantiles and medians | `quantile_cont(x, 0.5)`, `median(x)`, `approx_quantile(x, [0.5, 0.9, 0.99])` (t-digest), `reservoir_quantile(x, 0.9)` |
| Top values and distributions | `approx_top_k(x, 10)`, `mode(x)`, `histogram(x)` |
| Samples | `FROM t USING SAMPLE 10 PERCENT (bernoulli)`, `USING SAMPLE reservoir(10000 ROWS) REPEATABLE (42)`, `TABLESAMPLE 1%` |
| Regression and correlation | `regr_slope(y, x)`, `regr_intercept`, `regr_r2`, `regr_count`, `corr`, `covar_pop`, `stddev_samp` |
| Dates and time buckets | `date_trunc('week', d)`, `time_bucket(INTERVAL '15 minutes', ts)`, `dayofweek`, `isodow`, `yearweek`, `last_day`, `strftime` |
| Time zones | `ts AT TIME ZONE 'America/New_York'`, `timezone('America/Los_Angeles', ts)`, and `SET TimeZone = 'America/Chicago'` for the session |

`approx_quantile` and `reservoir_quantile` of a `DECIMAL` column return
whole numbers in DuckDB 1.5; cast the column to `DOUBLE` first, as below.
For example, cost per thousand impressions by local day in New York, and a
broadcast-calendar month (broadcast weeks run Monday to Sunday, and a
broadcast month is the month its weeks' Sundays fall in):

```sql
SELECT (served_at AT TIME ZONE 'America/New_York')::DATE AS local_day,
       1000 * sum(cost_usd) / count(*) AS cpm,
       approx_quantile(cost_usd::DOUBLE, 0.95) AS p95_cost
FROM impressions GROUP BY ALL ORDER BY 1;

SELECT strftime(date_trunc('week', served_at::DATE) + INTERVAL 6 DAY, '%Y-%m') AS broadcast_month,
       count(*) AS impressions
FROM impressions GROUP BY ALL;
```

## Report API

The report API counts the people of labelled cohorts (an audience, the
general population) in each value of a set of dimensions of one table, in
one call: the counting part of audience reports (cohort sizes, signals by
media, browsing and attributes, segment shares), whose SQL joins the
cohorts to a large table, expands each row into its dimensions and counts
distinct people by value. The rest of a report (shares, index ratios,
thresholds, rounding, ranking) stays SQL on top of the API's rows. This is
version 1, a contract: a released version does not change, and new fields
and columns come in a new version.

```sql
SELECT * FROM igloo.audience_profile('<spec>');                 -- the counts
SELECT igloo.audience_profile_sql('<spec>');                    -- the SQL they stand for (VARCHAR)
SELECT igloo.report_api_versions();                             -- INTEGER[]: [1]
SELECT * FROM igloo.audience_profile_check('<spec>', '<sql>');  -- parity check, below
```

`igloo.audience_profile` stands anywhere a table can: in `FROM`, in a common
table expression among others, in a subquery, under `DESCRIBE`; in
Snowflake's dialect, `TABLE(igloo.audience_profile('<spec>'))` too. The spec
is one string literal (`'...'` or `$$...$$`) holding a JSON object, and the
name is written unquoted. A caller can tell that a server has the API from
`igloo.report_api_versions()`; one without fails with SQLSTATE 42883.

### The spec

```json
{
  "version": 1,
  "source": {"table": "V_DAILY_MEDIA_VIEWING", "member": "PERSON_ID"},
  "cohorts": [
    {"label": "AUDIENCE", "table": "AUDIENCE_MEMBERS", "member": "PERSON_ID",
     "where": {"AUDIENCE_ID": "00000000-0000-4000-8000-0000000000a1"},
     "version": {"column": "VER", "latest_from": {"table": "AUDIENCE_REVISIONS",
                 "where": {"AUDIENCE_ID": "00000000-0000-4000-8000-0000000000a1"}}}},
    {"label": "BASE_POPULATION", "table": "V_PERSON_ATTRIBUTES", "member": "PERSON_ID"}
  ],
  "dimensions": [
    {"name": "GENRE", "column": "GENRE", "skip_blank": true},
    {"name": "VIEWING_MODE", "column": "LIVE", "map": {"true": "LIVE", "false": "ON_DEMAND"}, "skip_blank": true},
    {"name": "MEDIA_REACH", "constant": "ANY_VIEWING"}
  ],
  "metrics": [{"name": "seconds", "column": "TOTAL_SECONDS", "coalesce": 0}],
  "totals": false,
  "extent": "VIEWED_DATE"
}
```

Tables (`a`, `schema.a` or `catalog.schema.a`) and columns resolve as in the
caller's SQL: by the session's search path, through views, case aside.
Values are JSON literals; nothing in a spec becomes SQL text but a cohort's
`sql`, which is a statement of its own.

**Cohorts** (at most 8), each a labelled set of distinct, non-NULL members:

- A table: `{"label", "table", "member", "where", "version"}`, whose members
  are `SELECT DISTINCT member FROM table WHERE <where> AND <version> AND
  member IS NOT NULL`. `version` picks one version of a versioned table:
  `{"column": "VER", "equals": 3}`, or `{"column": "VER", "latest_from":
  {"table": "AUDIENCE_REVISIONS", "where": {...}}}` for `VER = (SELECT
  max(VER) FROM AUDIENCE_REVISIONS WHERE ...)`.
- A query: `{"label", "sql"}`, one `SELECT` (or `WITH ... SELECT`) of one
  column, the members, run as a subquery of the statement, exactly as if the
  caller ran it.

**Conditions** (`where`, `metric_where`): an object of columns' conditions,
all of which must hold. `"C": "v"` (any literal) is `C = 'v'`, `"C": null`
is `C IS NULL`, `"C": ["a", "b"]` is `C IN ('a', 'b')`, and `"C": {...}`
applies operators: `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `between` (`[low,
high]`), `like` (with LIKE's own `%` and `_`) and `is_null` (`true` or
`false`). A `metric_where` is one such object of operators, on the metric.

**Source and dimensions** (at most 64). `source` is the table whose rows
count, its member column and an optional `where`. Each row whose member is
in a cohort counts, for that cohort, in each value of each dimension:

| Dimension | The row's values |
|---|---|
| `{"name", "column": "C"}` | `CAST(C AS VARCHAR)` |
| `{"name", "column": "C", "map": {"true": "LIVE", "false": "ON_DEMAND"}}` | the mapped text of `CAST(C AS VARCHAR)`; NULL, and text the map lacks, give NULL |
| `{"name", "elements": "C"}` | each element of the list `C` (`UNNEST`) as `VARCHAR`; with `"path": "$.x"`, each element of the JSON array at that path of `C`, as `json_extract_string` reads it |
| `{"name", "constant": "ANY_VIEWING"}` | that text |

Options: `group` (passed through as `dim_group`); `skip_blank` (leave out
values that are NULL or blank after `trim`; by default NULL counts as a
value of its own); `metric` (a column, cast to `DOUBLE`, aggregated per
value in `metric_sum`, `metric_count` and `metric_avg`); and `metric_where`
(only rows whose cast metric meets it count in the dimension, for people,
rows and metrics alike). Dimension names may repeat: the result has one row
for each distinct (`dimension`, `dim_group`, `value`, `cohort`), and entries
that give the same one count together, as a `GROUP BY` over the `UNION ALL`
of their rows would. `"dimensions": []` with no `source` returns each
cohort's size alone, as a row with `is_total` true and `people` its size.

**Metrics** (at most 16), per row of the result:
`{"name": "seconds", "column": "C"}` gives `seconds_sum`, of the type `sum`
gives `C`, and `seconds_avg` (`DOUBLE`), with `"coalesce": 0` reading NULLs
as 0; `{"name": "days", "distinct_with": "C"}` gives `days_distinct`
(`BIGINT`), the distinct (member, `C`) pairs, leaving out those whose `C` is
NULL, as Snowflake's `COUNT(DISTINCT a, b)` does. Names are lower-case
letters, digits and `_`.

**Totals and extent.** `"totals": true` adds, for each dimension and cohort,
a row with `is_total` true and `value` NULL counting across the dimension's
values: the people with any of them (a union, not a sum), and their rows and
metrics. `"extent": "C"` adds `extent_min` and `extent_max`, the least and
greatest `C` of the source's rows (after its `where`) whose member is in a
cohort, on every row.

A spec of another version, with a field or operator this version does not
know, a value of the wrong kind, more than the counts above or more than 64
KiB fails with SQLSTATE 22023, naming what it refuses.

### The result

| Column | Type | |
|---|---|---|
| `dimension`, `dim_group`, `value`, `cohort` | `VARCHAR` | `value` is NULL on totals rows |
| `is_total` | `BOOLEAN` | |
| `people` | `BIGINT` | distinct members of the cohort with the value |
| `rows` | `BIGINT` | the source's rows (or list elements) of those members with the value |
| `cohort_size` | `BIGINT` | the cohort's members |
| `metric_sum`, `metric_count`, `metric_avg` | `DOUBLE`, `BIGINT`, `DOUBLE` | the dimension's metric; NULL without one |
| `<name>_sum` and `<name>_avg`, or `<name>_distinct` | | each of `metrics`, in order |
| `extent_min`, `extent_max` | the column's type | with `extent` only |
| `path` | `VARCHAR` | what counted the row: `scan` (the tables), `index` (bitmap indexes) or `index+scan` |

A row appears only where the SQL's `GROUP BY` has one: a value no member of
a cohort has gets no row for it, nor does a cohort without members. Counts
are exact; nothing is rounded; rows come in no particular order.

### Governance

A call is its SQL, `igloo.audience_profile_sql` prints it, as far as access
goes: Igloo puts that SQL in the call's place before it checks or runs the
statement. The caller needs the grants that SQL needs, gets the errors it
would, sees what row access policies, masking policies and clean rooms leave
of the tables as it would, and the statement runs as it would, on workers
when auto mode picks them.

Where the source and the cohorts are tables whose member columns belong to
one [ID domain](sql.md#id-domains), Igloo joins and counts the members' ids
instead of their values, as its counts of distinct members do, which counts
large cohorts in bitmaps; the result is the same. It does so when the
tables' current snapshots have every row's id (it reads them at those
snapshots, and a prepared call reads newer ones as they come), no row
access, clean room or masking policy of a member column applies to a table,
and no cohort is a query. A view is read through its table, as a [bitmap
index](sql.md#bitmap-indexes) reads it, only by users whose role reads the
whole warehouse, and, for the source, only a view of its table alone, whose
rows are the table's.
[Bitmap indexes](sql.md#bitmap-indexes) answer a call (`path` is `index`)
where they hold everything it counts and cannot differ from that SQL, under
the conditions of the rewrite to bitmap indexes: the caller's role reads
the whole warehouse, no policy applies to the tables or columns read, and
the bitmaps are of the tables' current snapshots. And:

- each cohort is a table whose conditions are on the columns of an index of
  it for its member;
- each dimension is a column or a constant, with an index of the source for
  its member by that column and the columns of the source's conditions (by
  those alone, for a constant: `ON ()` without conditions), and there are no
  metrics and no extent;
- every group of the bitmaps it reads has one row for each of its members,
  as a table of segment memberships does, so that a cohort's rows with a
  value are as many as its people. An index records each bitmap's rows and
  members to tell.

Anything else is counted from the tables (`path` is `scan`).

### Parity check

`igloo.audience_profile_check(spec, sql)` runs the call and `sql`, one
read-only `SELECT` (or `WITH ... SELECT`) whose columns all have names, in
the caller's statement, so with the caller's privileges, checks and
statement timeout, as if they had sent it. Its columns are matched to the
result's by name; it needs `dimension`, `value` and `cohort`, and any of the
others. The check returns each row that differs, with `side` (`api_only` or
`sql_only`) and the columns compared: none means the two agree. Sums and
averages compare to 9 significant digits, everything else exactly.

## Security

The extensions keep the [query sandbox](../SECURITY.md#security-model):

- **ST_Read and GDAL are refused**, for everyone, admins included:
  `ST_Read`, `ST_Read_Meta`, `FROM '*.gpkg'` and `FROM '*.fgb'` (which
  DuckDB turns into `ST_Read`), and `COPY ... (FORMAT GDAL)` however the
  format is written. GDAL hands DuckDB's file system only the path it is
  given; files that a dataset names inside itself, such as a VRT file's
  sources, GDAL opens on its own, from the server's disk or the network. A
  user who could write such a file to an allowed location could otherwise
  read the server's files. Igloo refuses these statements with SQLSTATE
  `42501` before DuckDB sees them.
- **ST_ReadSHP and ST_ReadOSM** read through DuckDB's file system, so they
  reach the warehouse and `IGLOO_ALLOWED_LOCATIONS` only, like
  `read_parquet`. Users with the role `none` cannot call them at all, as
  with every function that reads files.
- **PROJ**, which `ST_Transform` uses, opens a file that a coordinate system
  definition names, such as `+init=/path:key`. It never returns the file's
  contents; at most, whether a definition is accepted shows whether such a
  file exists.
- The extensions are installed when the image is built and DuckDB checks
  their signatures when Igloo loads them. They add about 90 MB to the image
  and about 0.1 seconds to startup.
- GEOS, inside the spatial extension, is under the LGPL 2.1. Its source,
  and the build that links it, are public in
  [duckdb-spatial](https://github.com/duckdb/duckdb-spatial).
