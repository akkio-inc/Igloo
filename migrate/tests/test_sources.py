import datetime as dt
import unittest

from migrate.sources import Filters, Meter, databricks, snowflake
from migrate.tests import fixture

UTC = dt.timezone.utc


def period(**kw):
    return Filters(start=dt.datetime(2026, 9, 20, tzinfo=UTC), end=dt.datetime(2026, 10, 20, tzinfo=UTC), **kw)


class SnowflakeTest(unittest.TestCase):
    def test_account_usage_groups(self):
        groups, totals = snowflake.groups_from_account_usage(fixture("snowflake_account_usage.json"))
        self.assertEqual(totals, {"groups": 3, "executions": 135, "elapsed_ms": 260000.0})
        g = groups[0]
        self.assertEqual((g.executions, g.succeeded, g.failed), (120, 118, 2))
        self.assertEqual(g.users, ["ANA", "REPORTING_SVC"])
        self.assertEqual(g.warehouse_sizes, ["XS", "S"])
        self.assertEqual(g.credits, 0.25)
        self.assertEqual(g.first_seen, "2026-09-21T00:00:01-07:00")
        self.assertIsNone(g.error, "an error is kept only for groups that never succeeded")
        self.assertEqual(groups[2].error, "Numeric value 'abc@example.com' is not recognized")
        self.assertIsNone(groups[1].credits)

    def test_account_usage_sql(self):
        sql, params = snowflake.account_usage_sql(
            period(users=["ana"], roles=["analyst"], warehouses=["wh"], classes=["SELECT", "CTAS"], max_groups=50),
            attribution=True, access=True)
        self.assertTrue(sql.startswith("/* igloo-migrate */"))
        self.assertIn("SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY", sql)
        self.assertIn("QUERY_ATTRIBUTION_HISTORY", sql)
        self.assertEqual(sql.count("ACCESS_HISTORY"), 1, "one pass over the access history")
        self.assertIn("rows_written_to_result", sql)
        self.assertIn("NOT ILIKE 'igloo-migrate%%'", sql)
        self.assertIn("LIMIT 50", sql)
        self.assertEqual(params["u0"], "ANA")
        self.assertEqual(params["r0"], "ANALYST")
        self.assertEqual(params["w0"], "WH")
        self.assertEqual({params["t0"], params["t1"]}, {"SELECT", "CREATE_TABLE_AS_SELECT"})
        self.assertEqual(params["start"], "2026-09-20T00:00:00+00:00")
        sql, _ = snowflake.account_usage_sql(period(classes=["SELECT", "other"]), attribution=False, access=False)
        self.assertNotIn("query_type IN", sql, "'other' has no server-side filter")
        self.assertNotIn("QUERY_ATTRIBUTION_HISTORY", sql)
        self.assertNotIn("ACCESS_HISTORY", sql)

    def test_account_usage_with_access_history(self):
        rows = fixture("snowflake_account_usage.json")
        access = {r["query_id"]: r for r in fixture("snowflake_access_history.json")}
        for r in rows:
            a = access.get(r["query_id"], {})
            r["direct_objects_accessed"] = a.get("direct_objects_accessed")
            r["objects_modified"] = a.get("objects_modified")
        groups, _ = snowflake.groups_from_account_usage(rows)
        self.assertEqual(groups[0].tables_read, ["SHOP.SALES.ORDERS_V", "SHOP.SALES.REGIONS"])
        self.assertIsNone(groups[1].tables_read, "no access history row: tables come from parsing")
        self.assertEqual(groups[2].tables_written, ["SHOP.SALES.CUSTOMERS"])

    def test_info_schema_rows(self):
        rows = fixture("snowflake_info_schema.json")
        execs = snowflake.executions_from_info_schema(rows, period())
        self.assertEqual([e["query_id"] for e in execs], ["q1", "q2", "q3", "q7"],
                         "the tool's own statements and unfinished ones are left out")
        self.assertEqual(execs[0]["rows"], 1, "rows written to the result, not ROWS_PRODUCED")
        self.assertEqual(execs[2]["status"], "failed")
        self.assertEqual(execs[0]["warehouse_size"], "XS")
        only_bob = snowflake.executions_from_info_schema(rows, period(users=["bob"]))
        self.assertEqual([e["query_id"] for e in only_bob], ["q3"])
        inserts = snowflake.executions_from_info_schema(rows, period(classes=["INSERT"]))
        self.assertEqual([e["query_id"] for e in inserts], ["q7"])

    def test_info_schema_sql_bounds_the_range(self):
        sql, params = snowflake.info_schema_sql(dt.datetime(2026, 10, 1, tzinfo=UTC),
                                                dt.datetime(2026, 10, 2, tzinfo=UTC))
        self.assertIn("END_TIME_RANGE_START", sql)
        self.assertIn("END_TIME_RANGE_END", sql)
        self.assertIn("RESULT_LIMIT => 10000", sql)
        self.assertEqual(params["end"], "2026-10-02T00:00:00+00:00")

    def test_access_history(self):
        rows = fixture("snowflake_access_history.json")
        read, written = snowflake.objects_from_access_history(rows[0])
        self.assertEqual(read, ["SHOP.SALES.ORDERS_V", "SHOP.SALES.REGIONS"], "stages are not tables")
        self.assertEqual(written, [])
        read, written = snowflake.objects_from_access_history(rows[1])
        self.assertEqual(read, ["SHOP.STAGING.NEW_CUSTOMERS"])
        self.assertEqual(written, ["SHOP.SALES.CUSTOMERS"])

    def test_metering(self):
        spend = snowflake.metering_rows_to_spend([{"warehouse_name": "A", "credits": 1.5},
                                                  {"warehouse_name": "B", "credits": None}], "x")
        self.assertEqual(spend.by_warehouse, {"A": 1.5, "B": 0.0})
        self.assertEqual(spend.total, 1.5)
        self.assertEqual(spend.unit, "credits")

    def test_sizes(self):
        self.assertEqual(snowflake.size_name("X-Small"), "XS")
        self.assertEqual(snowflake.size_name("X2LARGE"), "2XL")
        self.assertEqual(snowflake.size_name("Medium"), "M")
        self.assertIsNone(snowflake.size_name(None))


class DatabricksTest(unittest.TestCase):
    def test_system_history_groups(self):
        groups, totals = databricks.groups_from_system_history(fixture("databricks_system_history.json"),
                                                               ("main", "media"))
        self.assertEqual(totals["executions"], 43)
        self.assertEqual(groups[0].database, "main")
        self.assertEqual(groups[0].schema, "media")
        self.assertEqual(groups[0].users, ["ana@example.com", "svc-dashboards"])
        self.assertEqual(groups[1].source_type, "CREATE")

    def test_system_history_sql(self):
        sql, params = databricks.system_history_sql(period(users=["Ana@Example.com"], warehouses=["abc123"],
                                                           classes=["SELECT"]))
        self.assertIn("FROM system.query.history", sql)
        self.assertIn("NOT startswith(statement_text, '/* igloo-migrate')", sql)
        self.assertIn("compute.warehouse_id IN (:w0)", sql)
        self.assertEqual(params["u0"], "ana@example.com")
        self.assertEqual(params["t0"], "SELECT")
        self.assertIsNone(params["start"].tzinfo)

    def test_api_queries(self):
        execs = databricks.executions_from_api(fixture("databricks_api.json"), period(), ("main", "media"),
                                               {"abc123": "2XS"})
        self.assertEqual([e["query_id"] for e in execs], ["a1", "a2", "a3"])
        self.assertEqual(execs[0]["source_type"], "SELECT")
        self.assertEqual(execs[0]["warehouse_size"], "2XS")
        self.assertEqual(execs[0]["execution_ms"], 800.0)
        self.assertTrue(execs[0]["start"].startswith("2026-"))
        mine = databricks.executions_from_api(fixture("databricks_api.json"), period(warehouses=["abc123"]),
                                              (None, None), {})
        self.assertEqual([e["query_id"] for e in mine], ["a1", "a2"])

    def test_billing_sql(self):
        sql, params = databricks.billing_sql(period().start, period().end)
        self.assertIn("system.billing.usage", sql)
        self.assertIn("usage_metadata.warehouse_id", sql)

    def test_sizes(self):
        self.assertEqual(databricks.size_name("2X-Small"), "2XS")
        self.assertEqual(databricks.size_name("Medium"), "M")


class MeterTest(unittest.TestCase):
    def test_resumed_warehouse(self):
        m = Meter(rate_per_hour=1.0, was_running=False, minimum_seconds=60, idle_seconds=60, unit="credits")
        self.assertEqual(m.estimate(), 0.0)
        m.add("extraction", 10)
        self.assertAlmostEqual(m.estimate(), 120 / 3600)
        m.add("comparison", 70)
        self.assertAlmostEqual(m.estimate(), 140 / 3600)
        self.assertAlmostEqual(m.comparison_cost(), 20 / 3600)
        self.assertAlmostEqual(m.comparison_cost(extra=10), 30 / 3600)
        d = m.to_dict()
        self.assertEqual(d["busy_seconds"], {"extraction": 10, "comparison": 70}, "reading it changes nothing")
        self.assertAlmostEqual(d["estimated_comparison"], 20 / 3600)

    def test_running_warehouse_has_no_minimum(self):
        m = Meter(rate_per_hour=3.6, was_running=True, minimum_seconds=60, idle_seconds=60, unit="credits")
        m.add("comparison", 10)
        self.assertAlmostEqual(m.estimate(), 3.6 * 70 / 3600)
        self.assertIn("upper bound", m.to_dict()["how"])

    def test_seconds_within_a_budget(self):
        m = Meter(rate_per_hour=3600.0, was_running=False, minimum_seconds=60, idle_seconds=60, unit="credits")
        m.add("extraction", 30)
        # Comparisons cost nothing until the 60-second minimum is used up,
        # then 1 credit a second.
        self.assertAlmostEqual(m.seconds_within(budget=10, cap=120), 40, places=3)
        self.assertEqual(m.seconds_within(budget=10_000, cap=120), 120)
        m.add("comparison", 100)
        self.assertEqual(m.seconds_within(budget=10, cap=120), 0.0)
        self.assertIsNone(Meter(None, False, 60, 60, "DBUs").seconds_within(1, 10))

    def test_unknown_rate(self):
        m = Meter(None, False, 60, 60, "DBUs")
        m.add("extraction", 5)
        self.assertIsNone(m.estimate())
        self.assertIsNone(m.comparison_cost())


if __name__ == "__main__":
    unittest.main()
