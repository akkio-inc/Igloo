import datetime as dt
import unittest

from migrate import workload
from migrate.sources import Filters, snowflake
from migrate.tests import fixture


def group(key, text, **kw):
    base = dict(key=key, text=text, query_id=key, source_type="SELECT", database="SHOP", schema="SALES",
                executions=1, succeeded=1, failed=0, elapsed_ms_total=100.0, execution_ms_total=50.0,
                elapsed_ms_median=100.0, rows_last=1)
    base.update(kw)
    return workload.Group(**base)


class ShapesTest(unittest.TestCase):
    def test_queries_that_differ_in_comments_and_literals_are_one_shape(self):
        groups, _ = snowflake.groups_from_account_usage(fixture("snowflake_account_usage.json"))
        shapes = workload.build_shapes(groups, "snowflake")
        self.assertEqual(len(shapes), 2)
        s = shapes[0]
        self.assertEqual(s.executions, 130)
        self.assertEqual(s.groups, 2)
        self.assertEqual(s.cls, "SELECT")
        self.assertEqual(s.text, groups[0].text, "the representative is the most executed group's")
        self.assertEqual([r.name for r in s.reads], ["SHOP.SALES.ORDERS"])
        self.assertEqual(s.first_seen, "2026-09-21T00:00:01-07:00")
        self.assertEqual(s.credits, 0.25)
        self.assertEqual(s.warehouse_sizes, ["S", "XS"])
        self.assertEqual(s.elapsed_ms_median, 1800, "the median weighted by successful executions")
        merge = shapes[1]
        self.assertEqual(merge.cls, "MERGE")
        self.assertEqual(merge.succeeded, 0)
        self.assertIn("abc@example.com", merge.source_error)
        self.assertTrue(merge.target_must_exist)

    def test_same_hash_on_other_tables_is_another_shape(self):
        a = group("h1", "SELECT COUNT(*) FROM orders", schema="SF10")
        b = group("h1", "SELECT COUNT(*) FROM orders", schema="SF100")
        shapes = workload.build_shapes([a, b], "snowflake")
        self.assertEqual(len(shapes), 2)
        self.assertEqual({s.reads[0].name for s in shapes}, {"SHOP.SF10.ORDERS", "SHOP.SF100.ORDERS"})

    def test_same_hash_different_text_is_one_shape(self):
        # Snowflake's parameterized hash can join texts the normalizer keeps
        # apart (here, a cast written two ways).
        a = group("h2", "SELECT * FROM orders WHERE d = '2026-01-01'::DATE")
        b = group("h2", "SELECT * FROM orders WHERE d = CAST('2026-01-01' AS DATE)")
        self.assertEqual(len(workload.build_shapes([a, b], "snowflake")), 1)

    def test_access_history_overrides_parsing(self):
        g = group("h3", "SELECT * FROM orders_v JOIN regions USING (r)", tables_read=["SHOP.SALES.ORDERS_V",
                                                                                      "SHOP.SALES.REGIONS"],
                  tables_written=[])
        s = workload.build_shapes([g], "snowflake")[0]
        self.assertEqual(s.tables_from, "access history")
        self.assertEqual([r.key for r in s.reads], [("shop", "sales", "orders_v"), ("shop", "sales", "regions")])
        self.assertEqual(s.reads[0].written, ("orders_v",), "as the statement wrote it, for renaming")
        g = group("h6", "SELECT * FROM orders_v", tables_read=["SHOP.SALES.ORDERS_V", "SHOP.SALES.ORDERS"],
                  tables_written=[])
        s = workload.build_shapes([g], "snowflake")[0]
        self.assertEqual([r.written for r in s.reads], [("orders_v",), ("shop", "sales", "orders")],
                         "a table the text does not name keeps its full name")

    def test_volatility_and_unparseable(self):
        s = workload.build_shapes([group("h4", "SELECT * FROM t WHERE d > CURRENT_DATE() - 1")], "snowflake")[0]
        self.assertEqual(s.volatility, ["time"])
        s = workload.build_shapes([group("h5", "SELECT FROM WHERE (")], "snowflake")[0]
        self.assertFalse(s.parsed)

    def test_aggregate_rows(self):
        rows = snowflake.executions_from_info_schema(
            fixture("snowflake_info_schema.json"),
            Filters(start=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
                    end=dt.datetime(2027, 1, 1, tzinfo=dt.timezone.utc)))
        groups = {g.key: g for g in workload.aggregate_rows(rows)}
        g = groups["h-count"]
        self.assertEqual((g.executions, g.succeeded, g.failed), (3, 2, 1))
        self.assertEqual(g.query_id, "q2", "the latest successful execution represents the group")
        self.assertEqual(g.elapsed_ms_median, 300)
        self.assertEqual(g.elapsed_ms_total, 9600)
        self.assertEqual(g.users, ["ANA", "BOB"])
        self.assertIsNone(g.error)
        self.assertEqual(groups["h-load"].source_type, "INSERT")


if __name__ == "__main__":
    unittest.main()
