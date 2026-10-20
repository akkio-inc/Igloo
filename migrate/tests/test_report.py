import datetime as dt
import json
import pathlib
import tempfile
import unittest

from migrate import cli, cost, inventory, replay, report, workload
from migrate.igloo import Attempt, IglooTable
from migrate.sources import Meter, Spend, TableInfo

PRICING = {"snowflake": {"price_per_credit": 3.0, "credits_per_hour": {"XS": 1, "S": 2}},
           "igloo": {"instance_per_hour": {"c7g.4xlarge": 0.578}}}


def build(show_literals=False):
    groups = [
        workload.Group(key="a", text="SELECT region, SUM(amount) FROM orders WHERE email = 'ann@example.com' "
                                     "GROUP BY region", query_id="1", source_type="SELECT", database="SHOP",
                       schema="SALES", executions=100, succeeded=100, failed=0, elapsed_ms_total=200000.0,
                       execution_ms_total=150000.0, elapsed_ms_median=2000.0, rows_last=4, users=["ANA", "BOB"],
                       warehouse_sizes=["XS"], warehouses=["WH"]),
        workload.Group(key="b", text="SELECT IFF(x > 7, 'big', 'small') FROM orders", query_id="2",
                       source_type="SELECT", database="SHOP", schema="SALES", executions=10, succeeded=10, failed=0,
                       elapsed_ms_total=5000.0, execution_ms_total=4000.0, elapsed_ms_median=500.0, rows_last=3,
                       warehouse_sizes=["XS"]),
        workload.Group(key="c", text="SELECT ZEROIFNULL(x) FROM orders WHERE note = 'secret'", query_id="3",
                       source_type="SELECT", database="SHOP", schema="SALES", executions=3, succeeded=3, failed=0,
                       elapsed_ms_total=900.0, execution_ms_total=600.0, elapsed_ms_median=300.0, rows_last=3),
        workload.Group(key="d", text="SELECT * FROM customers WHERE id = 42", query_id="4", source_type="SELECT",
                       database="SHOP", schema="SALES", executions=50, succeeded=50, failed=0,
                       elapsed_ms_total=100000.0, execution_ms_total=90000.0, elapsed_ms_median=2000.0, rows_last=1),
        workload.Group(key="e", text="ALTER SESSION SET QUERY_TAG = 'x'", query_id="5", source_type="ALTER_SESSION",
                       database=None, schema=None, executions=20, succeeded=20, failed=0),
    ]
    shapes = workload.build_shapes(groups, "snowflake")
    tables = [IglooTable("lake", "sales", "orders", "mirrored", rows=10, bytes=1000,
                         snapshot="2026-10-19T00:00:00+00:00")]
    mapper = inventory.Mapper(tables, "lake", [])
    st = replay.Settings("snowflake", None, 1000, False, None)
    results, chosen, covs = replay.plan(shapes, mapper, st, 100, "time", 0)
    by_text = {s.text[:20]: s for s in shapes}
    for s in chosen:
        r = results[s.id]
        if "IFF" in s.text:
            r.attempts = [Attempt("as written", s.text, error_class="unknown function",
                                  error="Catalog Error: Scalar Function with name iff does not exist!"),
                          Attempt("sqlglot", "SELECT CASE WHEN x > 7 THEN 'big' ELSE 'small' END FROM orders",
                                  ok=True, seconds=0.05, rows=3, checksum="0" * 16, columns=["x"])]
        elif "ZEROIFNULL" in s.text:
            r.attempts = [Attempt("as written", s.text, error_class="conversion",
                                  error="Conversion Error: Could not convert string 'secret' to INT32"),
                          Attempt("sqlglot", "SELECT CASE WHEN x IS NULL THEN 0 ELSE x END FROM orders WHERE note "
                                             "= 'secret'", error_class="conversion",
                                  error="Conversion Error: Could not convert string 'secret' to INT32")]
        else:
            r.attempts = [Attempt("as written", s.text, ok=True, seconds=0.2, rows=4, checksum="1" * 16,
                                  columns=["region", "sum"])]
        replay._settle(r)
        if r.best:
            r.comparison = {"history": replay.history_check(s, r.best, {})}
    uses = inventory.table_uses(shapes, mapper, {("shop", "sales", "orders"): TableInfo("SHOP.SALES.ORDERS", rows=10,
                                                                                       bytes=2048)})
    meter = Meter(1.0, False, 60, 60, "credits")
    meter.add("extraction", 5)
    c = cost.section(shapes, results, Spend("credits", {"WH": 2.5}, 2.5, source="metering"), PRICING, "snowflake",
                     "credits", 24 * 30, "c7g.4xlarge", 0.578, meter)
    rep = report.build(shapes=shapes, results=results, uses=uses,
                       meta={"source": "snowflake", "history": "SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY",
                             "start": "2026-09-20T00:00:00+00:00", "end": "2026-10-20T00:00:00+00:00",
                             "notes": ["a note"], "totals": {"executions": 183}},
                       igloo_info={"version": "test", "address": "localhost:5433", "native_dialect": None,
                                   "native_dialect_error": "no such setting"},
                       settings={"generated": "2026-10-20T00:00:00+00:00", "max_groups": 20000, "digits": 10},
                       cost=c, dialect="snowflake", show_literals=show_literals)
    return rep, by_text


class ReportTest(unittest.TestCase):
    def test_summary(self):
        rep, _ = build()
        out = {o["outcome"]: o for o in rep["summary"]["outcomes"]}
        self.assertEqual(out["unchanged"]["shapes"], 1)
        self.assertEqual(out["translated"]["executions"], 10)
        self.assertEqual(out["failed"]["executions"], 3)
        self.assertEqual(out["no data"]["executions"], 50)
        self.assertEqual(out["not replayed"]["executions"], 20)
        self.assertEqual(rep["summary"]["not_replayed"], [{"reason": "other: ALTER_SESSION", "shapes": 1,
                                                           "executions": 20}])
        self.assertEqual(rep["summary"]["replayed_executions"], 113)
        self.assertEqual(rep["failures"][0]["class"], "conversion")
        self.assertEqual(sorted(rep["translation"]["rows"], key=lambda r: r["sqlglot"]),
                         [{"native_dialect": None, "sqlglot": False, "shapes": 1},
                          {"native_dialect": None, "sqlglot": True, "shapes": 1}])
        self.assertEqual(rep["comparison"]["history"], {"rows match": 2})
        self.assertEqual(rep["comparison"]["history_match_rate"], 1.0)
        self.assertEqual(rep["tables"][0]["status"], "mirrored")
        self.assertEqual(rep["timing"]["shapes"], 2)
        self.assertEqual(rep["cost"]["source_spend"]["dollars"], 7.5)
        self.assertEqual(rep["cost"]["source_spend"]["label"], "measured")
        self.assertEqual(rep["cost"]["replayed"]["source_units_label"], "estimated")
        self.assertAlmostEqual(rep["cost"]["igloo"]["always_on_dollars"], 0.578 * 720)
        json.dumps(rep, default=str)

    def test_literals_are_redacted_by_default(self):
        rep, _ = build()
        text = json.dumps(rep, default=str) + report.markdown(rep) + report.render_html(rep)
        for secret in ("ann@example.com", "secret", "'big'", "= 42"):
            self.assertNotIn(secret, text)
        self.assertNotIn('"users": ["ANA"', json.dumps(rep), "user names appear only as counts")

    def test_show_literals(self):
        rep, _ = build(show_literals=True)
        self.assertIn("ann@example.com", json.dumps(rep))

    def test_markdown_and_html(self):
        rep, _ = build()
        md = report.markdown(rep)
        for heading in ("## Outcome", "## Failures on Igloo, by error class", "## Translation", "## Results compared",
                        "## Timing", "## Most-used tables", "## Cost"):
            self.assertIn(heading, md)
        self.assertIn("| Run unchanged | 1 |", md)
        self.assertIn("(measured)", md)
        self.assertIn("(estimated)", md)
        page = report.render_html(rep)
        self.assertTrue(page.startswith("<!doctype html>"))
        self.assertIn("prefers-color-scheme: dark", page)
        self.assertNotIn("<script src", page)
        self.assertNotIn("http://", page.replace("http://www.w3.org", ""))
        self.assertIn("SHOP.SALES.ORDERS", page)

    def test_html_escapes(self):
        rep, _ = build()
        rep["source"]["notes"] = ["<script>alert(1)</script>"]
        self.assertNotIn("<script>alert", report.render_html(rep))


class CliTest(unittest.TestCase):
    def test_out_must_be_outside_the_repository(self):
        if (cli.ROOT / ".git").exists():
            with self.assertRaises(SystemExit):
                cli.check_out(cli.ROOT / "migrate-out")
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(cli.check_out(pathlib.Path(d) / "x"), (pathlib.Path(d) / "x").resolve())

    def test_times(self):
        now = dt.datetime(2026, 10, 20, tzinfo=dt.timezone.utc)
        self.assertEqual(cli.parse_time("30d", now), dt.datetime(2026, 9, 20, tzinfo=dt.timezone.utc))
        self.assertEqual(cli.parse_time("12h", now), dt.datetime(2026, 10, 19, 12, tzinfo=dt.timezone.utc))
        self.assertEqual(cli.parse_time("2026-10-01", now), dt.datetime(2026, 10, 1, tzinfo=dt.timezone.utc))

    def test_pricing_is_the_benchmarks(self):
        p = cli.load_pricing(cli.ROOT / "bench" / "pricing.toml", ["snowflake.price_per_credit=2.5"])
        self.assertEqual(p["snowflake"]["price_per_credit"], 2.5)
        self.assertIn("c7g.4xlarge", p["igloo"]["instance_per_hour"])

    def test_extract_round_trip(self):
        from migrate.sources import Extract
        with tempfile.TemporaryDirectory() as d:
            g = workload.Group(key="k", text="SELECT 1", query_id="q", source_type="SELECT", database=None,
                               schema=None, executions=1, succeeded=1, failed=0)
            cli.save_extract(pathlib.Path(d), Extract([g], {"source": "snowflake"}))
            back = cli.load_extract(pathlib.Path(d))
            self.assertEqual(back.groups, [g])
            self.assertEqual(back.meta, {"source": "snowflake"})

    def test_statement_type_filter(self):
        args = cli.main.__globals__["argparse"].Namespace(
            statement_types="select,ctas", since="1d", until=None, users="a, b", roles=None, warehouses=None,
            max_groups=10)
        f = cli.filters(args, dt.datetime(2026, 10, 20, tzinfo=dt.timezone.utc))
        self.assertEqual(f.classes, ["SELECT", "CTAS"])
        self.assertEqual(f.users, ["a", "b"])
        args.statement_types = "selects"
        with self.assertRaises(SystemExit):
            cli.filters(args, dt.datetime(2026, 10, 20, tzinfo=dt.timezone.utc))


if __name__ == "__main__":
    unittest.main()
