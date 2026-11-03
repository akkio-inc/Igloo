-- UNION in a relation: a report shape of internal/distributed/reportshapes_test.go.
SELECT kind, count(*) FROM (SELECT l_shipmode AS kind, l_orderkey FROM lineitem UNION SELECT o_orderpriority, o_orderkey FROM orders) GROUP BY 1;
