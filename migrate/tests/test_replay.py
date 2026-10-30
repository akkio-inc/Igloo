import decimal
import unittest

from migrate import compare, inventory, replay, workload
from migrate.igloo import Attempt, IglooTable
from migrate.sources import Rerun

TABLES = [IglooTable("lake", "sales", "orders", "mirrored", rows=3), IglooTable("lake", "sales", "regions", "mirrored")]
MAPPER = inventory.Mapper(TABLES, "lake", [])


def shape(text, source_type="SELECT", **kw):
    g = workload.Group(key=text, text=text, query_id="q", source_type=source_type, database="SHOP", schema="SALES",
                       executions=kw.pop("executions", 1), succeeded=kw.pop("succeeded", 1), failed=0,
                       elapsed_ms_median=100.0, rows_last=kw.pop("rows_last", 1), **kw)
    return workload.build_shapes([g], "snowflake")[0]


class FakeConn:
    def __init__(self, log):
        self.log = log

    def execute(self, sql):
        self.log.append(sql)


class FakeIgloo:
    """Runs a statement only if it is DuckDB SQL this test knows to work."""

    def __init__(self, works=("CASE WHEN",), rows=1):
        self.works = works
        self.rows = rows
        self.ran = []
        self.setup = []

    def conn(self, dialect=None):
        return FakeConn(self.setup)

    def run(self, sql, how, *, search_path, dialect=None, max_rows, count_only=False, warnings=None):
        self.ran.append((how, sql, search_path, dialect, count_only))
        if any(w in sql for w in self.works) and "IFF(" not in sql:
            d = compare.digest([(i,) for i in range(self.rows)], ["n"])
            return Attempt(how=how, sql=sql, ok=True, seconds=0.01, rows=d.rows, checksum=d.checksum,
                           columns=d.columns, counted=count_only, warnings=warnings, values=d.values)
        return Attempt(how=how, sql=sql, error_class="unknown function", error="Scalar Function with name iff does "
                                                                               "not exist!")


def settings(**kw):
    base = dict(source_dialect="snowflake", native=None, max_rows=1000, replay_writes=False,
                scratch=("lake", "igloo_migrate_scratch"))
    base.update(kw)
    return replay.Settings(**base)


class PlanTest(unittest.TestCase):
    def test_outcomes_before_replay(self):
        shapes = [
            shape("SELECT * FROM orders", executions=5),
            shape("SELECT * FROM missing"),
            shape("SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY"),
            shape("SHOW TABLES", source_type="SHOW"),
            shape("INSERT INTO orders SELECT * FROM orders", source_type="INSERT"),
            shape("SELECT * FROM regions", succeeded=0),
            shape("<REDACTED>", source_type="SELECT"),
            shape("", source_type="SHOW"),
            shape("SELECT * FROM regions JOIN orders USING (r)", executions=1),
        ]
        results, chosen, _ = replay.plan(shapes, MAPPER, settings(), sample=1, sample_by="executions", seed=0)
        by_text = {s.text: results[s.id] for s in shapes}
        self.assertEqual(by_text["SELECT * FROM missing"].outcome, "no data")
        self.assertEqual(by_text["SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY"].outcome, "source metadata")
        self.assertEqual(by_text["SHOW TABLES"].outcome, "not replayed")
        self.assertIn("--replay-writes", by_text["INSERT INTO orders SELECT * FROM orders"].reason)
        self.assertEqual(by_text["SELECT * FROM regions"].outcome, "failed on source")
        self.assertIn("hides", by_text["<REDACTED>"].reason)
        self.assertIn("hides", by_text[""].reason, "hidden text is the reason whatever the statement's class")
        self.assertEqual([s.text for s in chosen], ["SELECT * FROM orders"])
        self.assertEqual(by_text["SELECT * FROM regions JOIN orders USING (r)"].outcome, "not in sample")


class ReplayTest(unittest.TestCase):
    def run_one(self, text, igloo, st=None, **kw):
        s = shape(text, **kw)
        cov = inventory.coverage(s, MAPPER)
        result = replay.Result(s.id, "")
        return s, replay.replay(s, cov, igloo, st or settings(), result)

    def test_unchanged(self):
        igloo = FakeIgloo(works=("SELECT",))
        _, r = self.run_one("SELECT COUNT(*) FROM SHOP.SALES.ORDERS", igloo)
        self.assertEqual(r.outcome, "unchanged")
        how, sql, search_path, _, _ = igloo.ran[0]
        self.assertEqual(sql, "SELECT COUNT(*) FROM lake.sales.orders")
        self.assertEqual(search_path, "lake.sales")

    def test_translated_after_failing_as_written(self):
        igloo = FakeIgloo()
        _, r = self.run_one("SELECT IFF(a > 1, 'x', 'y') FROM orders", igloo)
        self.assertEqual(r.outcome, "translated")
        self.assertEqual([a.how for a in r.attempts], ["as written", "sqlglot"])
        self.assertEqual(r.works_with, ["sqlglot"])
        self.assertEqual(r.best.how, "sqlglot")

    def test_native_dialect_is_tried_where_igloo_has_it(self):
        igloo = FakeIgloo(works=("NOTHING",))
        _, r = self.run_one("SELECT IFF(a > 1, 'x', 'y') FROM orders", igloo, settings(native="snowflake"))
        self.assertEqual(r.outcome, "failed")
        self.assertEqual([(a[0], a[3]) for a in igloo.ran],
                         [("as written", None), ("native dialect", "snowflake"), ("sqlglot", None)])
        self.assertEqual(r.failure.how, "sqlglot")

    def test_large_results_are_counted(self):
        igloo = FakeIgloo(works=("SELECT",))
        self.run_one("SELECT * FROM orders", igloo, rows_last=10_000_000)
        self.assertTrue(igloo.ran[0][4])

    def test_statements_that_write_are_not_run_as_queries(self):
        igloo = FakeIgloo(works=("SELECT",))
        _, r = self.run_one("SELECT a INTO b FROM orders", igloo)
        self.assertEqual(r.outcome, "not replayed")
        self.assertEqual(igloo.ran, [])

    def test_source_functions_run_on_igloo_and_fail_there(self):
        # SYSTEM$ functions are refused on the source; Igloo has none, so
        # the replay says which is missing.
        igloo = FakeIgloo(works=("NOTHING",))
        _, r = self.run_one("SELECT SYSTEM$TYPEOF(a) FROM orders", igloo)
        self.assertEqual(r.outcome, "failed")

    def test_writes_go_to_a_clone_in_the_scratch_schema(self):
        igloo = FakeIgloo(works=("INSERT",))
        s, r = self.run_one("INSERT INTO orders (id) SELECT id FROM regions", igloo, settings(replay_writes=True),
                            source_type="INSERT")
        self.assertEqual(r.outcome, "unchanged")
        target = f"lake.igloo_migrate_scratch.orders__{s.id}"
        self.assertEqual(igloo.setup[0], f"CREATE OR REPLACE TABLE {target} CLONE lake.sales.orders")
        self.assertEqual(igloo.ran[0][1], f"INSERT INTO {target} (id) SELECT id FROM regions")
        self.assertEqual(igloo.setup[-1], f"DROP TABLE IF EXISTS {target}")

    def test_ctas_needs_no_clone(self):
        igloo = FakeIgloo(works=("CREATE",))
        s, r = self.run_one("CREATE TABLE summary AS SELECT COUNT(*) AS n FROM orders", igloo,
                            settings(replay_writes=True), source_type="CREATE_TABLE_AS_SELECT")
        self.assertEqual(r.outcome, "unchanged")
        self.assertTrue(igloo.setup[0].startswith("DROP TABLE IF EXISTS lake.igloo_migrate_scratch.summary__"))


class CompareTest(unittest.TestCase):
    def test_history_check(self):
        s = shape("SELECT * FROM orders", rows_last=3)
        ok = replay.history_check(s, Attempt("as written", "", ok=True, rows=3), {})
        self.assertEqual(ok["verdict"], "rows match")
        bad = replay.history_check(s, Attempt("as written", "", ok=True, rows=4), {})
        self.assertEqual(bad["verdict"], "rows differ")
        none = replay.history_check(shape("SELECT * FROM orders", rows_last=None),
                                    Attempt("as written", "", ok=True, rows=4), {})
        self.assertEqual(none["verdict"], "not compared")

    def test_rerun_check_precision_and_explanations(self):
        D = decimal.Decimal
        s = shape("SELECT avg(x) FROM orders WHERE d > CURRENT_DATE - 1")
        mine = compare.digest([(0.0500065745,)], ["a"])
        theirs = compare.digest([(D("0.05000657"),)], ["A"])
        a = Attempt("as written", "", ok=True, rows=1, checksum=mine.checksum, columns=mine.columns,
                    values=mine.values)
        out = replay.rerun_check(s, a, Rerun(ok=True, rows=1, checksum=theirs.checksum, columns=theirs.columns,
                                             values=theirs.values), {})
        self.assertEqual(out["verdict"], "match within precision")
        self.assertEqual(out["explained_by"], ["depends on the current time"])
        other = compare.digest([(D("0.06"),)], ["a"])
        out = replay.rerun_check(s, a, Rerun(ok=True, rows=1, checksum=other.checksum, columns=other.columns,
                                             values=other.values), {})
        self.assertEqual(out["verdict"], "values differ")
        self.assertIn("1 of 1 rows differ, in a", out["notes"])
        out = replay.rerun_check(s, a, Rerun(ok=False, skipped="returned more than 10 rows"), {})
        self.assertEqual(out["verdict"], "not compared")

    def test_rerun_check_floating_point_in_a_large_result(self):
        # Too many rows to compare one by one: the checksums decide.
        D = decimal.Decimal
        s = shape("SELECT avg(x) FROM orders")
        mine = compare.digest([(1234.567890123,)], ["a"], keep=0)
        theirs = compare.digest([(D("1234.56789012"),)], ["A"], keep=0)
        rerun = Rerun(ok=True, rows=1, checksum=theirs.checksum, loose=theirs.loose, columns=theirs.columns)
        a = Attempt("as written", "", ok=True, rows=1, checksum=mine.checksum, loose=mine.loose, columns=mine.columns)
        self.assertEqual(replay.rerun_check(s, a, rerun, {})["verdict"], "match")
        other = compare.digest([(D("1234.56789013"),)], ["a"], keep=0)
        a = Attempt("as written", "", ok=True, rows=1, checksum=other.checksum, loose=other.loose,
                    columns=other.columns)
        self.assertEqual(replay.rerun_check(s, a, rerun, {})["verdict"], "values differ")


if __name__ == "__main__":
    unittest.main()
