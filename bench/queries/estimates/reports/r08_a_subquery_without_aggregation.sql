-- a subquery without aggregation: a report shape of internal/distributed/reportshapes_test.go.
SELECT x.l_orderkey, x.l_comment, count(*) OVER () AS n FROM (SELECT l_orderkey, l_comment FROM lineitem WHERE l_quantity > 45) AS x ORDER BY 1, 2;
