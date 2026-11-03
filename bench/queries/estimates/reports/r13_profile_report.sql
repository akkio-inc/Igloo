-- profileReport of internal/distributed/reportshapes_test.go, whole.
WITH audience AS (SELECT DISTINCT o_custkey AS id FROM orders WHERE o_orderpriority = '1-URGENT'),
total AS (SELECT count(*) AS size FROM audience),
items AS (
  SELECT a.id, 'parts' AS section, e.trait, e.kind, e.value
  FROM orders JOIN audience AS a ON a.id = o_custkey,
    unnest([{'trait': 'status', 'kind': 'category', 'value': o_orderstatus}, {'trait': 'clerk', 'kind': 'category', 'value': o_clerk},
      {'trait': 'price', 'kind': 'numeric', 'value': CAST(o_totalprice AS VARCHAR)}]) AS u(e)),
aggregates AS (
  SELECT section, trait, kind, CASE WHEN kind = 'category' THEN value ELSE 'numeric summary' END AS category,
         count(DISTINCT id) AS members, avg(TRY_CAST(value AS DOUBLE)) AS average, median(TRY_CAST(value AS DOUBLE)) AS middle
  FROM items GROUP BY section, trait, kind, 4),
ranked AS (
  SELECT p.section, p.trait, p.category, p.members, 100.0 * p.members / NULLIF(t.size, 0) AS pct, p.average, p.middle,
         row_number() OVER (PARTITION BY p.section, p.trait ORDER BY p.members DESC NULLS LAST, p.category) AS rnk
  FROM aggregates AS p CROSS JOIN total AS t),
size_row AS (SELECT 'overview' AS section, 'size' AS trait, 'all' AS category, t.size AS members, 100.0 AS pct,
  CAST(NULL AS DOUBLE) AS average, CAST(NULL AS DOUBLE) AS middle, CAST(1 AS BIGINT) AS rnk FROM total AS t),
profile AS (SELECT * FROM size_row UNION ALL SELECT * FROM ranked WHERE rnk <= 10)
SELECT p.*, p.members >= 5 AS significant FROM profile AS p CROSS JOIN total AS t ORDER BY section, trait, rnk, category;
