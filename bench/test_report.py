import pathlib
import unittest

import report

PRICING = pathlib.Path(report.__file__).parent / "pricing.toml"


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
