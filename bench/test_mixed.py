import contextlib
import io
import threading
import time
import unittest

import mixed


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.description = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        with self.conn.lock:
            self.conn.log.append(sql)
        if "fail-me" in sql:
            raise RuntimeError("boom: fail-me\nmore")
        if sql.startswith("SELECT coalesce(max(batch_id)"):
            self.description = [("max",)]
            self.rows = [(3,)]
            return
        self.description = [("x",)] if "SELECT" in sql and not sql.startswith("SET") else None
        self.rows = []
        time.sleep(self.conn.delay)

    def fetchone(self):
        return self.rows[0]

    def fetchall(self):
        return self.rows


class FakeConn:
    def __init__(self, log, lock, delay=0.0):
        self.log, self.lock, self.delay = log, lock, delay
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


class PercentileTest(unittest.TestCase):
    def test_interpolates(self):
        self.assertIsNone(mixed.percentile([], 50))
        self.assertEqual(mixed.percentile([3.0], 99), 3.0)
        self.assertEqual(mixed.percentile([1, 2, 3, 4], 50), 2.5)
        self.assertAlmostEqual(mixed.percentile(list(range(101)), 95), 95.0)
        self.assertEqual(mixed.percentile([5, 1, 3], 0), 1)
        self.assertEqual(mixed.percentile([5, 1, 3], 100), 5)


class SummarizeTest(unittest.TestCase):
    def test_window_classes_and_errors(self):
        recs = [
            {"class": "dashboard", "name": "a.x", "start": 9.0, "seconds": 1.0, "error": None},  # before the window
            {"class": "dashboard", "name": "a.x", "start": 10.0, "seconds": 0.5, "error": None},
            {"class": "dashboard", "name": "a.y", "start": 20.0, "seconds": 1.5, "error": None},
            {"class": "dashboard", "name": "a.y", "start": 30.0, "seconds": 9.0, "error": "timeout"},
            {"class": "elt", "name": "merge", "start": 40.0, "seconds": 4.0, "error": None},
            {"class": "elt", "name": "merge", "start": 70.0, "seconds": 4.0, "error": None},  # after
        ]
        out = mixed.summarize(recs, 10.0, 70.0)
        self.assertEqual(out["dashboard"]["statements"], 3)
        self.assertEqual(out["dashboard"]["errors"], 1)
        self.assertEqual(out["dashboard"]["error_samples"], ["timeout"])
        self.assertEqual(out["dashboard"]["per_minute"], 2.0)  # 2 finished in a 60 s window
        self.assertEqual(out["dashboard"]["p50"], 1.0)
        self.assertEqual(out["dashboard.a.x"]["statements"], 1)
        self.assertEqual(out["elt"]["statements"], 1)
        self.assertEqual(out["elt.merge"]["max"], 4.0)

    def test_history(self):
        rows = [("dashboard", "a.x", 0, 100, 0, 5.0), ("dashboard", "a.x", 2000, 300, 0, 1500.0), ("elt", "rollup", None, 10000, 4, None)]
        h = mixed.history_summary(rows)
        self.assertEqual(h["dashboard"]["statements"], 2)
        self.assertEqual(h["dashboard"]["queued_max"], 2.0)
        self.assertEqual(h["dashboard"]["on_workers"], 0)
        self.assertEqual(h["dashboard.a.x"]["queued_p50"], 1.0)
        self.assertEqual(h["dashboard"]["catalog_wait_max"], 1.5)
        self.assertEqual(h["elt"]["catalog_wait_p95"], 0.0)
        self.assertEqual(h["elt"]["on_workers"], 1)
        self.assertEqual(h["elt"]["worker_busy_seconds"], 40.0)


class HeavyTest(unittest.TestCase):
    def test_heavy_and_probes_run_until_the_window_ends(self):
        log, lock = [], threading.Lock()
        args = mixed.parse(["run", "--dashboard-users", "0", "--elt-streams", "0", "--heavy-sessions", "2", "--probes",
                            "--duration", "0.2", "--warmup", "0", "--run-id", "t"])
        res = mixed.run(args, lambda cls: FakeConn(log, lock, delay=0.01))
        names = {(r["class"], r["name"]) for r in res["records"]}
        self.assertLessEqual({("heavy", n) for n in mixed.HEAVY}, names)
        self.assertLessEqual({("probe", n) for n in mixed.PROBES}, names)
        self.assertTrue(any("/* mixed t heavy households */" in q for q in log))
        self.assertTrue(any("/* mixed t probe ping */" in q for q in log))


class CostTest(unittest.TestCase):
    def test_cost(self):
        c = mixed.cost(1.0, 0.5, 1800, 7200)
        self.assertEqual(c["coordinator_usd"], 0.5)
        self.assertEqual(c["workers_usd"], 1.0)
        self.assertEqual(c["total_usd"], 1.5)
        self.assertEqual(c["per_hour_usd"], 3.0)

    def test_prices(self):
        args = mixed.parse(["run", "--coordinator", "c7g.4xlarge", "--worker", "1.5", "--spot-factor", "0.4"])
        coord, worker = mixed.load_prices(args)
        self.assertEqual(coord, 0.578)
        self.assertAlmostEqual(worker, 0.6)
        with self.assertRaises(SystemExit):
            mixed.load_prices(mixed.parse(["run", "--coordinator", "no.such"]))

    def test_sampler(self):
        pages = iter(["# x\nigloo_distributed_workers 2\n", "igloo_distributed_workers 4\n"])
        s = mixed.WorkerSampler("http://x", fetch=lambda u: next(pages, "igloo_distributed_workers 4\n"))
        self.assertEqual(s.read(), 2.0)
        self.assertEqual(s.read(), 4.0)
        bad = mixed.WorkerSampler("http://x", fetch=lambda u: (_ for _ in ()).throw(OSError("down")))
        self.assertIsNone(bad.read())
        self.assertEqual(bad.failures, 1)

    def test_sampler_keeps_count_over_failed_scrape(self):
        s = mixed.WorkerSampler("http://x")
        s.tick(16, 100.0)            # before the window: not counted
        s.counting = True
        s.tick(16, 101.0)
        s.tick(None, 102.0)          # a scrape that failed keeps 16
        s.tick(8, 103.0)
        self.assertEqual(s.seconds, 16 + 16 + 8)
        self.assertEqual(len(s.samples), 3)

    def test_report_uses_metrics_despite_failed_scrapes(self):
        s = mixed.WorkerSampler("http://x")
        s.samples, s.seconds, s.failures = [(1.0, 16.0)], 9600.0, 2
        args = mixed.parse(["run"])
        res = {"t0": 0.0, "t1": 600.0, "records": [], "skipped": {}}
        hist = [("dashboard", "delivery.kpis", 0, 100000, 16, 0)] * 50   # 50 overlapping statements on 16 workers
        out = mixed.report(res, hist, args, 0.85, 0.43, s)
        self.assertEqual(out["worker_seconds"], 9600.0)
        self.assertTrue(out["worker_seconds_source"].startswith("metrics (2 failed scrapes"))
        out = mixed.report(res, hist, args, 0.85, 0.43, mixed.WorkerSampler("http://x"))
        self.assertTrue(out["worker_seconds_source"].startswith("history"))


class SQLTest(unittest.TestCase):
    def test_setup_and_jobs(self):
        stmts = mixed.setup_statements("m", 1000)
        self.assertTrue(any(s.startswith("CREATE MATERIALIZED VIEW m.advertiser_reach") for s in stmts))
        inserts = [s for s in stmts if s.startswith("INSERT INTO m.impressions")]
        self.assertEqual(len(inserts), 8)
        self.assertIn("range(0, 125)", inserts[0])
        self.assertIn("range(875, 1000)", inserts[-1])
        for job in mixed.ELT_JOBS:
            self.assertTrue(mixed.elt_sql(job, "m", 3, 1000, 1100, 50))
        self.assertIn("range(1000, 1100)", mixed.elt_sql("ingest", "m", 3, 1000, 1100, 50))
        self.assertIn("3 AS batch_id", mixed.elt_sql("ingest", "m", 3, 1000, 1100, 50))
        with self.assertRaises(ValueError):
            mixed.elt_sql("nope", "m", 1, 0, 1, 1)

    def test_tiles_format(self):
        params = {"s": "m", "adv": "advertiser_0", "lo": "2026-09-01", "hi": "2026-10-01"}
        for tiles in mixed.DASHBOARDS.values():
            for sql in tiles.values():
                text = sql.format(**params)
                self.assertNotIn("{", text)

    def test_parse_rejects(self):
        with contextlib.redirect_stderr(io.StringIO()) as err:
            with self.assertRaises(SystemExit):
                mixed.parse(["run", "--elt-jobs", "ingest,bogus"])
            with self.assertRaises(SystemExit):
                mixed.parse(["run", "--param-pool", "0"])
        self.assertIn("unknown ELT jobs", err.getvalue())


class RunTest(unittest.TestCase):
    def test_short_run(self):
        log, lock = [], threading.Lock()
        args = mixed.parse(["run", "--duration", "0.6", "--warmup", "0", "--dashboard-users", "2", "--tile-concurrency", "2",
                            "--think", "0.05", "--elt-streams", "2", "--elt-every", "0.2", "--rows", "1000",
                            "--batch-rows", "10", "--run-id", "t1"])
        conns = []

        def connect(cls):
            c = FakeConn(log, lock, delay=0.01)
            conns.append(c)
            return c

        res = mixed.run(args, connect)
        self.assertTrue(all(c.closed for c in conns))
        classes = {r["class"] for r in res["records"]}
        self.assertEqual(classes, {"dashboard", "elt"})
        texts = [s for s in log if s.startswith("/* mixed t1 ")]
        self.assertTrue(any(" dashboard " in s for s in texts))
        # Ingest continues after the batches already in the table (3 here).
        ingests = [s for s in texts if " elt ingest " in s]
        self.assertTrue(ingests)
        self.assertIn("4 AS batch_id", ingests[0])
        self.assertIn("range(1030, 1040)", ingests[0])
        out = mixed.report(res, [("dashboard", "delivery.kpis", 10, 20, 0, 0)], args, 1.0, 1.0, None)
        self.assertEqual(out["worker_seconds_source"], "history (statements' workers times duration)")
        self.assertIn("dashboard", out["classes"])

    def test_errors_recorded(self):
        rec = mixed.Recorder()
        mixed.run_one(FakeConn([], threading.Lock()), "SELECT fail-me", rec, "dashboard", "x")
        self.assertEqual(rec.records[0]["error"], "boom: fail-me")

    def test_skips_overlapping_runs(self):
        args = mixed.parse(["run", "--elt-jobs", "rollup", "--elt-every", "1"])
        elt = mixed.ELT(args, None, mixed.Recorder(), time.time() + 10, 0)
        self.assertEqual(elt.begin("rollup"), 0)
        self.assertIsNone(elt.begin("rollup"))
        self.assertEqual(elt.skipped["rollup"], 1)


if __name__ == "__main__":
    unittest.main()
