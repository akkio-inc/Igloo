import contextlib
import io
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

import report

PRICING = pathlib.Path(report.__file__).parent / "pricing.toml"


def burst(q01_error=None):
    """One run.py result of two queries, where q01 may have failed."""
    return {"platform": "snowflake", "size": "XS", "scale": 10.0, "suite": "tpch", "generation": 2,
            "connect_seconds": 1.0, "queries": [
                {"query": "q01", "cold": 2.0, "warm": [1.0], "error": q01_error},
                {"query": "q02", "cold": 1.0, "warm": [0.5], "error": None}]}


class MergeTest(unittest.TestCase):
    def test_failed_attempts_are_counted(self):
        [merged] = report.merge([burst(), burst("OperationalError: connection lost"), burst()])
        s = report.summarize(merged)
        self.assertEqual((s["failed"], s["incomplete"]), ([], {"q01": (1, 3)}))
        self.assertEqual(s["cold"], 3.0)
        [merged] = report.merge([burst("OperationalError: connection lost")])
        s = report.summarize(merged)
        self.assertEqual((s["failed"], s["incomplete"]), (["q01"], {}))

    def test_incomplete_runs_are_shown(self):
        with tempfile.TemporaryDirectory() as d:
            paths = []
            for i, r in enumerate([burst(), burst("OperationalError: connection lost"), burst()]):
                paths.append(pathlib.Path(d, f"{i}.json"))
                paths[-1].write_text(json.dumps(r))
            out = io.StringIO()
            with mock.patch.object(sys, "argv", ["report.py", *map(str, paths)]), contextlib.redirect_stdout(out):
                report.main()
        row = next(line for line in out.getvalue().splitlines() if line.startswith("| snowflake | XS (Gen2)"))
        self.assertTrue(row.endswith("| q01 (failed 1 of 3) |"), row)


class BurstTest(unittest.TestCase):
    def test_minimum_covers_the_idle_tail(self):
        # Billed from resume until the warehouse stops, at least the minimum.
        prices = report.load_pricing(PRICING, [])
        self.assertAlmostEqual(report.burst_seconds(prices, "snowflake", 29.6), 89.6)
        self.assertAlmostEqual(report.burst_seconds(prices, "snowflake", 120), 180)
        self.assertAlmostEqual(report.burst_seconds(prices, "databricks", 10), 310)
        self.assertAlmostEqual(report.burst_seconds(prices, "igloo", 10, resume=17), 87)
        prices = report.load_pricing(PRICING, ["snowflake.auto_suspend_seconds=30", "databricks.minimum_seconds=600"])
        self.assertAlmostEqual(report.burst_seconds(prices, "snowflake", 10), 60)
        self.assertAlmostEqual(report.burst_seconds(prices, "snowflake", 45), 75)
        self.assertAlmostEqual(report.burst_seconds(prices, "databricks", 10), 600)


if __name__ == "__main__":
    unittest.main()
