import unittest

from migrate import inventory, workload
from migrate.igloo import IglooTable
from migrate.sources import TableInfo

TABLES = [
    IglooTable("lake", "sales", "orders", "mirrored", rows=100, bytes=1000, snapshot="2026-10-19T00:00:00+00:00"),
    IglooTable("lake", "sales", "regions", "mirrored", rows=5, snapshot="2026-10-19T00:00:00+00:00"),
    IglooTable("lake", "marts", "daily", "mirrored", rows=10, snapshot="2026-10-19T00:00:00+00:00"),
    IglooTable("crm", "public", "contacts", "connection"),
]


def shape(text, db="SHOP", schema="SALES", **kw):
    g = workload.Group(key=text, text=text, query_id="q", source_type=kw.pop("source_type", "SELECT"), database=db,
                       schema=schema, executions=kw.pop("executions", 1), succeeded=1, failed=0, **kw)
    return workload.build_shapes([g], "snowflake")[0]


class RuleTest(unittest.TestCase):
    def test_parse_and_apply(self):
        r = inventory.Rule.parse("PROD.ANALYTICS.*=marts.*", "lake")
        self.assertEqual(r.target, ("lake", "marts", "*"))
        self.assertEqual(r.apply(("prod", "analytics", "daily")), ("lake", "marts", "daily"))
        self.assertIsNone(r.apply(("prod", "other", "daily")))
        r = inventory.Rule.parse("*.*.ORDERS=lake.sales.orders", "lake")
        self.assertEqual(r.apply(("x", "y", "orders")), ("lake", "sales", "orders"))
        r = inventory.Rule.parse("SALES_*.*=lake.*.*", "lake")
        self.assertEqual(r.apply(("db", "sales_eu", "t")), ("lake", "sales_eu", "t"))

    def test_bad_rules(self):
        for text in ("no-equals", "a.b.c.d=x.y", "a.b=x", "a.b=*.*.*"):
            with self.subTest(text), self.assertRaises(ValueError):
                inventory.Rule.parse(text, "lake")


class MapperTest(unittest.TestCase):
    def mapper(self, *rules):
        return inventory.Mapper(TABLES, "lake", [inventory.Rule.parse(r, "lake") for r in rules])

    def test_order_of_matching(self):
        m = self.mapper("SHOP.MARTS.DAILY_V=lake.marts.daily")
        self.assertEqual(m.match(("shop", "marts", "daily_v")).how, "--map shop.marts.daily_v")
        self.assertEqual(m.match(("shop", "sales", "orders")).table.full, "lake.sales.orders")
        self.assertEqual(m.match(("crm", "public", "contacts")).table.kind, "connection")
        self.assertIsNone(m.match(("shop", "sales", "nothing")))
        self.assertIsNone(m.match(("", "sales", "orders")), "unresolved names do not match")

    def test_a_rule_to_a_missing_table(self):
        m = self.mapper("SHOP.SALES.*=lake.elsewhere.*")
        match = m.match(("shop", "sales", "orders"))
        self.assertIsNone(match.table)
        self.assertEqual(match.target, ("lake", "elsewhere", "orders"))

    def test_namespace(self):
        self.assertEqual(self.mapper().namespace("SHOP", "SALES"), "lake.sales")
        self.assertEqual(self.mapper("SHOP.STAGE.*=lake.marts.*").namespace("SHOP", "STAGE"), "lake.marts")
        self.assertIsNone(self.mapper().namespace("SHOP", "NOPE"))
        self.assertIsNone(self.mapper().namespace(None, None))


class CoverageTest(unittest.TestCase):
    mapper = inventory.Mapper(TABLES, "lake", [inventory.Rule.parse("SHOP.REPORTS.*=lake.marts.*", "lake")])

    def test_ready_through_the_search_path(self):
        c = inventory.coverage(shape("SELECT * FROM orders JOIN regions USING (r)"), self.mapper)
        self.assertEqual(c.status, "ready")
        self.assertEqual(c.search_path, "lake.sales")
        self.assertEqual(c.renames, {})

    def test_qualified_and_renamed_tables(self):
        c = inventory.coverage(shape("SELECT * FROM SHOP.SALES.ORDERS o JOIN reports.daily d ON 1 = 1"), self.mapper)
        self.assertEqual(c.status, "ready")
        self.assertEqual(c.renames, {("shop", "sales", "orders"): ("lake", "sales", "orders"),
                                     ("reports", "daily"): ("lake", "marts", "daily")})
        self.assertEqual(c.text_renames(), {("shop", "sales", "orders"): "lake.sales.orders",
                                            ("reports", "daily"): "lake.marts.daily"})

    def test_no_data(self):
        c = inventory.coverage(shape("SELECT * FROM orders JOIN missing USING (r)"), self.mapper)
        self.assertEqual(c.status, "no data")
        self.assertEqual(c.missing, ["SHOP.SALES.MISSING"])
        c = inventory.coverage(shape("SELECT * FROM orders", db=None, schema=None), self.mapper)
        self.assertEqual(c.status, "no data")
        self.assertIn("unknown", c.missing[0])
        c = inventory.coverage(shape("SELECT $1 FROM @landing"), self.mapper)
        self.assertIn("stage", c.missing[0])

    def test_source_metadata(self):
        c = inventory.coverage(shape("SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY"), self.mapper)
        self.assertEqual(c.status, "source metadata")

    def test_write_targets_must_exist_for_inserts(self):
        c = inventory.coverage(shape("INSERT INTO newtable SELECT * FROM orders", source_type="INSERT"), self.mapper)
        self.assertEqual(c.status, "no data")
        c = inventory.coverage(shape("CREATE TABLE newtable AS SELECT * FROM orders",
                                     source_type="CREATE_TABLE_AS_SELECT"), self.mapper)
        self.assertEqual(c.status, "ready")


class TableUsesTest(unittest.TestCase):
    def test_status_sizes_and_freshness(self):
        mapper = inventory.Mapper(TABLES, "lake", [])
        shapes = [shape("SELECT * FROM orders", executions=10), shape("SELECT * FROM orders JOIN gone USING (a)"),
                  shape("SHOW TABLES", source_type="SHOW")]
        info = {("shop", "sales", "orders"): TableInfo("SHOP.SALES.ORDERS", rows=120, bytes=5000,
                                                       last_altered="2026-10-20T00:00:00+00:00")}
        uses = {u.name: u for u in inventory.table_uses(shapes, mapper, info)}
        self.assertEqual(set(uses), {"SHOP.SALES.ORDERS", "SHOP.SALES.GONE"})
        o = uses["SHOP.SALES.ORDERS"]
        self.assertEqual((o.status, o.read_by, o.executions), ("mirrored", 2, 11))
        self.assertEqual(o.igloo["full"], "lake.sales.orders")
        self.assertEqual(o.source["rows"], 120)
        self.assertIn("rows differ: 120 at the source, 100 in Igloo", o.notes)
        self.assertIn("changed at the source after Igloo's snapshot", o.notes)
        self.assertEqual(uses["SHOP.SALES.GONE"].status, "missing")


class IdentTest(unittest.TestCase):
    def test_quoting(self):
        self.assertEqual(inventory.qualified(("lake", "sales", "orders")), "lake.sales.orders")
        self.assertEqual(inventory.qualified(("lake", "Sales", "my table")), 'lake."Sales"."my table"')
        self.assertEqual(inventory.qualified(("lake", "s", "a`b"), "`"), "lake.s.`a``b`")


if __name__ == "__main__":
    unittest.main()
