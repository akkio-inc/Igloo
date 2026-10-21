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
Snowflake SQL's `APPROX_COUNT_DISTINCT` becomes; it is as fast ungrouped
and slower with many groups.

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
