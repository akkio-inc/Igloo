-- LIMIT without ORDER BY: a report shape of internal/distributed/reportshapes_test.go.
SELECT l_orderkey, l_comment FROM lineitem WHERE l_quantity > 10 LIMIT 25;
