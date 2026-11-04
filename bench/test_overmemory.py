import contextlib
import datetime
import decimal
import io
import json
import pathlib
import tempfile
import unittest

import overmemory


class FingerprintTest(unittest.TestCase):
    def test_order_and_number_types_do_not_matter(self):
        a = [(1, decimal.Decimal("2.50"), "x", datetime.date(1998, 9, 2)), (2, None, "y", None)]
        b = [(2, None, "y", None), (1.0, 2.5, "x", datetime.date(1998, 9, 2))]
        self.assertEqual(overmemory.fingerprint(a), overmemory.fingerprint(b))

    def test_a_sum_in_another_order_matches_to_six_digits(self):
        self.assertEqual(overmemory.fingerprint([(0.1 + 0.2,)]), overmemory.fingerprint([(0.3,)]))
        self.assertNotEqual(overmemory.fingerprint([(1.0001,)]), overmemory.fingerprint([(1.0002,)]))

    def test_rows_differ(self):
        self.assertNotEqual(overmemory.fingerprint([(1,), (1,)]), overmemory.fingerprint([(1,)]))


class TimedTest(unittest.TestCase):
    def test_a_write_returns_no_rows_and_no_error(self):
        class Cursor:
            description = None

            def fetchall(self):
                raise AssertionError("the last operation didn't produce records")

        class Conn:
            def execute(self, sql):
                return Cursor()

            def cancel_safe(self):
                pass

        rows, _, err = overmemory.timed(Conn(), "CREATE TABLE t AS SELECT 1", 60)
        self.assertEqual((rows, err), ([], None))


class ReportTest(unittest.TestCase):
    def test_lists_mismatches_first(self):
        base = {"suite": "tpcds", "scale": 100, "mode": "on", "label": "on16", "memory_limit": "8.0 GiB", "seconds": 1.0,
                "coordinator_peak_bytes": 2**30, "coordinator_spill_bytes": 0, "worker_peak_bytes": 2**31,
                "worker_spill_bytes": None, "exchange_bytes": 2**30, "workers": 16, "outcome": "ok", "error": None}
        recs = [dict(base, query="q01", match=True), dict(base, query="q44", match=False, only_igloo=["[1]"], only_reference=["[2]"])]
        with tempfile.TemporaryDirectory() as d:
            p = pathlib.Path(d) / "r.jsonl"
            p.write_text("".join(json.dumps(r) + "\n" for r in recs))
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                overmemory.main(["report", str(p)])
        text = out.getvalue()
        self.assertIn("| tpcds | 100 | on | on16 | 8.0 GiB | 2 | 1 | 1 | 0 |", text)
        self.assertLess(text.index("q44"), text.index("| q01 |"))
        self.assertIn("1 / 1 rows differ", text)
        self.assertIn("16 workers", text)


if __name__ == "__main__":
    unittest.main()
