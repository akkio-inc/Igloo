#!/usr/bin/env python3
"""Run dashboards and ELT on Igloo at the same time, and report what each
class of work got: throughput, latency percentiles, queueing, errors and
cost.

Real warehouses do not run one query at a time. A BI tool fires many short
queries at once, the same dashboards over and over, while scheduled jobs
load, rebuild and merge tables underneath them. This benchmark runs both
against a synthetic ad-impressions schema:

- **Dashboards.** `--dashboard-users` users, each opening one of a few
  dashboards after another with a think time between (`--think`, an
  exponential mean in seconds). A dashboard's tiles run at once, up to
  `--tile-concurrency` at a time, as BI tools send them. Filters come from
  small pools (`--param-pool` advertisers, three date ranges), so
  dashboards repeat, as they do in practice; the result cache stays as the
  server has it.
- **ELT.** Jobs on schedules, run by `--elt-streams` sessions: `ingest`
  appends a batch of impressions (`INSERT`), `rollup` rebuilds a daily
  table (`CREATE OR REPLACE TABLE ... AS`), `increment` appends the totals
  of new batches (`INSERT ... SELECT` past a watermark), `merge` upserts
  campaign totals (`MERGE`), and `refresh` refreshes a materialized view.
  Each job is due every `--elt-every` seconds, the jobs spread over that
  period; a job still running when it is due again is skipped, as Igloo's
  own schedules do.

    python bench/mixed.py setup --rows 2000000
    python bench/mixed.py run --duration 120 --dashboard-users 8 --elt-streams 2 \\
        --metrics-url http://127.0.0.1:8432/metrics --coordinator m7g.4xlarge \\
        --out bench/results/mixed-local.json

IGLOO_DSN names the Igloo to connect to, as for run.py. Statements carry a
comment naming the run and the class, `/* mixed RUN CLASS NAME */`, the same
text each time a dashboard repeats; after the run, queueing time comes from
`igloo.query_history` (`queued_ms`), and the share of statements that ran on
workers too.

**Cost.** The coordinator runs for the whole window: `--coordinator`
(an instance type in pricing.toml) or `--coordinator-price`, per hour. With
`--metrics-url`, the client samples `igloo_distributed_workers` every
second, so workers are billed for the seconds they were running, idle tail
included; `--worker`/`--worker-price` price them (`--spot-factor` scales a
Spot price). Without it, workers are billed only for the seconds their
statements ran (`duration_ms` times `workers` from the history), a lower
bound. Results report cost over the window and per hour.

Synthetic data only: rows are computed from their number, as in
deploy/single-vm/sql/load-media.sql.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import pathlib
import random
import statistics
import threading
import time
import tomllib
import urllib.request

PRICING = pathlib.Path(__file__).parent / "pricing.toml"

# Impressions span one quarter, as in load-media.sql.
QUARTER_START = "2026-07-01"
QUARTER_MICROS = 7948800000000
# Date ranges dashboards filter by: the last week, month and the quarter.
RANGES = [("2026-09-24", "2026-10-01"), ("2026-09-01", "2026-10-01"), ("2026-07-01", "2026-10-01")]
CHANNELS = ["display", "video", "ctv", "audio"]


# --- Data -------------------------------------------------------------------


def impressions_select(lo: int, hi: int, devices: int, batch: int) -> str:
    """The SELECT that computes impressions lo..hi of ingestion batch batch.
    Each column hashes its own number, i * 8 + k (see load-media.sql)."""
    return (
        f"SELECT TIMESTAMP '{QUARTER_START}' + to_microseconds(CAST(hash(i * 8) % {QUARTER_MICROS} AS BIGINT)) AS ts, "
        f"CAST(hash(i * 8 + 1) % 2000 AS INTEGER) AS campaign_id, CAST(hash(i * 8 + 2) % {devices} AS BIGINT) AS device_id, "
        f"CAST((hash(i * 8 + 3) % 5000) / 1000.0 AS DECIMAL(10, 4)) AS cost, hash(i * 8 + 4) % 100 = 0 AS clicked, "
        f"{batch} AS batch_id FROM range({lo}, {hi}) t(i)"
    )


def setup_statements(schema: str, rows: int, parts: int = 8) -> list[str]:
    """Creates the schema, its tables, and the targets the ELT jobs write."""
    s = schema
    devices = max(rows // 20, 1)
    out = [
        f"CREATE SCHEMA IF NOT EXISTS {s}",
        f"DROP MATERIALIZED VIEW IF EXISTS {s}.advertiser_reach",
    ]
    for t in ("impressions", "campaigns", "device_graph", "daily_delivery", "batch_totals", "campaign_stats"):
        out.append(f"DROP TABLE IF EXISTS {s}.{t}")
    out += [
        f"CREATE TABLE {s}.campaigns AS SELECT i AS campaign_id, 'advertiser_' || (i % 200) AS advertiser, "
        f"['display', 'video', 'ctv', 'audio'][1 + (i // 200) % 4] AS channel FROM range(2000) t(i)",
        f"CREATE TABLE {s}.device_graph AS SELECT d AS device_id, CAST(hash(d * 8 + 5) % {max(rows // 60, 1)} AS BIGINT) AS household_id "
        f"FROM range({devices}) t(d)",
        f"CREATE TABLE {s}.impressions (ts TIMESTAMP, campaign_id INTEGER, device_id BIGINT, cost DECIMAL(10, 4), "
        "clicked BOOLEAN, batch_id INTEGER)",
    ]
    # In parts, so the table has several data files at any size.
    for k in range(parts):
        out.append(f"INSERT INTO {s}.impressions {impressions_select(k * rows // parts, (k + 1) * rows // parts, devices, 0)}")
    out += [
        f"CREATE TABLE {s}.daily_delivery AS {ELT_QUERIES['rollup'].format(s=s)}",
        f"CREATE TABLE {s}.batch_totals (batch_id INTEGER, channel VARCHAR, impressions BIGINT, spend DECIMAL(38, 4))",
        f"CREATE TABLE {s}.campaign_stats (campaign_id INTEGER, impressions BIGINT, spend DECIMAL(38, 4), clicks BIGINT)",
        f"CREATE MATERIALIZED VIEW {s}.advertiser_reach AS SELECT c.advertiser, c.channel, count(DISTINCT i.device_id) AS reach, "
        f"count(*) AS impressions, sum(i.cost) AS spend FROM {s}.impressions i JOIN {s}.campaigns c ON c.campaign_id = i.campaign_id "
        "GROUP BY 1, 2",
    ]
    return out


# --- Work -------------------------------------------------------------------

# Dashboards: tiles of short queries over the filters adv (an advertiser),
# lo and hi (a date range). Inlined, as BI tools send them.
DASHBOARDS = {
    "delivery": {
        "kpis": "SELECT count(*) AS impressions, sum(i.cost) AS spend, count_if(i.clicked) AS clicks FROM {s}.impressions i "
        "JOIN {s}.campaigns c ON c.campaign_id = i.campaign_id WHERE c.advertiser = '{adv}' AND i.ts >= DATE '{lo}' AND i.ts < DATE '{hi}'",
        "by_day": "SELECT date_trunc('day', i.ts) AS day, count(*) AS impressions, sum(i.cost) AS spend FROM {s}.impressions i "
        "JOIN {s}.campaigns c ON c.campaign_id = i.campaign_id WHERE c.advertiser = '{adv}' AND i.ts >= DATE '{lo}' AND i.ts < DATE '{hi}' "
        "GROUP BY 1 ORDER BY 1",
        "by_channel": "SELECT c.channel, count(*) AS impressions, sum(i.cost) AS spend, count_if(i.clicked) / count(*) AS ctr "
        "FROM {s}.impressions i JOIN {s}.campaigns c ON c.campaign_id = i.campaign_id WHERE c.advertiser = '{adv}' "
        "AND i.ts >= DATE '{lo}' AND i.ts < DATE '{hi}' GROUP BY 1 ORDER BY 1",
        "top_campaigns": "SELECT i.campaign_id, sum(i.cost) AS spend, count(*) AS impressions FROM {s}.impressions i "
        "JOIN {s}.campaigns c ON c.campaign_id = i.campaign_id WHERE c.advertiser = '{adv}' AND i.ts >= DATE '{lo}' AND i.ts < DATE '{hi}' "
        "GROUP BY 1 ORDER BY spend DESC, 1 LIMIT 10",
    },
    "reach": {
        "reach": "SELECT c.channel, count(DISTINCT i.device_id) AS reach FROM {s}.impressions i JOIN {s}.campaigns c "
        "ON c.campaign_id = i.campaign_id WHERE c.advertiser = '{adv}' AND i.ts >= DATE '{lo}' AND i.ts < DATE '{hi}' GROUP BY 1 ORDER BY 1",
        "households": "SELECT count(DISTINCT g.household_id) AS households FROM {s}.impressions i JOIN {s}.campaigns c "
        "ON c.campaign_id = i.campaign_id JOIN {s}.device_graph g ON g.device_id = i.device_id WHERE c.advertiser = '{adv}' "
        "AND i.ts >= DATE '{lo}' AND i.ts < DATE '{hi}'",
        "frequency": "SELECT least(f, 10) AS frequency, count(*) AS devices FROM (SELECT i.device_id, count(*) AS f "
        "FROM {s}.impressions i JOIN {s}.campaigns c ON c.campaign_id = i.campaign_id WHERE c.advertiser = '{adv}' "
        "AND i.ts >= DATE '{lo}' AND i.ts < DATE '{hi}' GROUP BY 1) GROUP BY 1 ORDER BY 1",
    },
    # Tiles over what the ELT jobs build.
    "marts": {
        "daily": "SELECT d.day, sum(d.impressions) AS impressions, sum(d.spend) AS spend FROM {s}.daily_delivery d "
        "JOIN {s}.campaigns c ON c.campaign_id = d.campaign_id WHERE c.advertiser = '{adv}' AND d.day >= DATE '{lo}' "
        "AND d.day < DATE '{hi}' GROUP BY 1 ORDER BY 1",
        "reach_mv": "SELECT channel, reach, impressions, spend FROM {s}.advertiser_reach WHERE advertiser = '{adv}' ORDER BY channel",
        "campaign_stats": "SELECT s.campaign_id, s.impressions, s.spend, s.clicks FROM {s}.campaign_stats s "
        "JOIN {s}.campaigns c ON c.campaign_id = s.campaign_id WHERE c.advertiser = '{adv}' ORDER BY s.spend DESC LIMIT 20",
        "batches": "SELECT channel, count(*) AS batches, sum(impressions) AS impressions FROM {s}.batch_totals GROUP BY 1 ORDER BY 1",
    },
}

ELT_QUERIES = {
    "rollup": "SELECT CAST(ts AS DATE) AS day, campaign_id, count(*) AS impressions, sum(cost) AS spend, "
    "count_if(clicked) AS clicks FROM {s}.impressions GROUP BY 1, 2",
}


def elt_sql(job: str, s: str, batch: int, lo: int, hi: int, devices: int) -> str:
    """The statement of one run of an ELT job."""
    if job == "ingest":
        return f"INSERT INTO {s}.impressions {impressions_select(lo, hi, devices, batch)}"
    if job == "rollup":
        return f"CREATE OR REPLACE TABLE {s}.daily_delivery AS {ELT_QUERIES['rollup'].format(s=s)}"
    if job == "increment":
        return (
            f"INSERT INTO {s}.batch_totals SELECT i.batch_id, c.channel, count(*), sum(i.cost) FROM {s}.impressions i "
            f"JOIN {s}.campaigns c ON c.campaign_id = i.campaign_id "
            f"WHERE i.batch_id > (SELECT coalesce(max(batch_id), -1) FROM {s}.batch_totals) GROUP BY 1, 2"
        )
    if job == "merge":
        return (
            f"MERGE INTO {s}.campaign_stats t USING (SELECT campaign_id, count(*) AS impressions, sum(cost) AS spend, "
            f"count_if(clicked) AS clicks FROM {s}.impressions WHERE batch_id >= {max(batch - 5, 0)} GROUP BY 1) u "
            "ON t.campaign_id = u.campaign_id WHEN MATCHED THEN UPDATE SET impressions = u.impressions, spend = u.spend, "
            "clicks = u.clicks WHEN NOT MATCHED THEN INSERT VALUES (u.campaign_id, u.impressions, u.spend, u.clicks)"
        )
    if job == "refresh":
        return f"REFRESH MATERIALIZED VIEW {s}.advertiser_reach"
    raise ValueError(f"unknown ELT job {job}")


ELT_JOBS = ["ingest", "rollup", "increment", "merge", "refresh"]


# --- Measuring --------------------------------------------------------------


def percentile(xs: list[float], p: float) -> float | None:
    """The p-th percentile (0..100) of xs, linearly interpolated; None for
    no values."""
    if not xs:
        return None
    s = sorted(xs)
    k = (len(s) - 1) * p / 100
    lo, hi = math.floor(k), math.ceil(k)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


class Recorder:
    """Collects one record per statement, from every thread."""

    def __init__(self):
        self.lock = threading.Lock()
        self.records: list[dict] = []

    def add(self, cls: str, name: str, start: float, seconds: float, error: str | None):
        with self.lock:
            self.records.append({"class": cls, "name": name, "start": start, "seconds": seconds, "error": error})


def summarize(records: list[dict], t0: float, t1: float) -> dict:
    """Per class and per statement name: statements started in [t0, t1) and
    finished, errors, throughput per minute and latency percentiles."""
    window = max(t1 - t0, 1e-9)
    groups: dict[str, list[dict]] = {}
    for r in records:
        if t0 <= r["start"] < t1:
            groups.setdefault(r["class"], []).append(r)
            groups.setdefault(f"{r['class']}.{r['name']}", []).append(r)
    out = {}
    for key, rs in sorted(groups.items()):
        ok = [r["seconds"] for r in rs if not r["error"]]
        errors = [r["error"] for r in rs if r["error"]]
        out[key] = {
            "statements": len(rs),
            "errors": len(errors),
            "error_samples": sorted(set(errors))[:3],
            "per_minute": round(len(ok) / window * 60, 2),
            "p50": percentile(ok, 50),
            "p95": percentile(ok, 95),
            "p99": percentile(ok, 99),
            "mean": statistics.fmean(ok) if ok else None,
            "max": max(ok) if ok else None,
        }
    return out


def history_summary(rows: list[tuple]) -> dict:
    """Per class and per statement name, from igloo.query_history rows
    (class, name, queued_ms, duration_ms, workers): queueing percentiles in
    seconds, how many ran on workers, and busy worker-seconds."""
    by: dict[str, list[tuple]] = {}
    for cls, name, queued, duration, workers in rows:
        x = (queued or 0, duration or 0, workers or 0)
        by.setdefault(cls, []).append(x)
        by.setdefault(f"{cls}.{name}", []).append(x)
    out = {}
    for key, rs in sorted(by.items()):
        q = [x[0] / 1000 for x in rs]
        out[key] = {
            "statements": len(rs),
            "queued_p50": percentile(q, 50),
            "queued_p95": percentile(q, 95),
            "queued_p99": percentile(q, 99),
            "queued_max": max(q),
            "on_workers": sum(1 for x in rs if x[2] > 0),
            "worker_busy_seconds": sum(x[1] / 1000 * x[2] for x in rs),
        }
    return out


def load_prices(args) -> tuple[float, float]:
    """The coordinator's and a worker's price per hour."""
    pricing = tomllib.loads(PRICING.read_text())["igloo"]["instance_per_hour"]

    def price(given, kind):
        if given is None:
            return 0.0
        try:
            return float(given)
        except ValueError:
            if given not in pricing:
                raise SystemExit(f"{kind} {given} is not in pricing.toml's igloo.instance_per_hour; pass a price instead")
            return pricing[given]

    return price(args.coordinator, "coordinator"), price(args.worker, "worker") * args.spot_factor


def cost(coord_price: float, worker_price: float, seconds: float, worker_seconds: float) -> dict:
    """What the window cost: the coordinator throughout, workers for their
    seconds."""
    total = coord_price * seconds / 3600 + worker_price * worker_seconds / 3600
    return {
        "coordinator_usd": round(coord_price * seconds / 3600, 4),
        "workers_usd": round(worker_price * worker_seconds / 3600, 4),
        "total_usd": round(total, 4),
        "per_hour_usd": round(total / seconds * 3600, 4) if seconds else None,
    }


class WorkerSampler(threading.Thread):
    """Samples igloo_distributed_workers (workers running) once a second and
    integrates it into worker-seconds between mark() and stop."""

    def __init__(self, url: str, fetch=None):
        super().__init__(daemon=True)
        self.url = url
        self.fetch = fetch or (lambda u: urllib.request.urlopen(u, timeout=5).read().decode())
        self.stop = threading.Event()
        self.seconds = 0.0
        self.counting = False
        self.samples: list[tuple[float, float]] = []
        self.failures = 0

    def read(self) -> float | None:
        try:
            for line in self.fetch(self.url).splitlines():
                if line.startswith("igloo_distributed_workers ") or line.startswith("igloo_distributed_workers{"):
                    return float(line.rsplit(" ", 1)[1])
            return 0.0
        except Exception:
            self.failures += 1
            return None

    def run(self):
        last = time.time()
        while not self.stop.is_set():
            n = self.read()
            now = time.time()
            if n is not None:
                self.samples.append((round(now, 1), n))
                if self.counting:
                    self.seconds += n * (now - last)
            last = now
            self.stop.wait(1)


# --- Running ----------------------------------------------------------------


def dashboard_user(uid: int, args, connect, rec: Recorder, until: float, seed: int):
    """One BI user: opens dashboards with think time between, each
    dashboard's tiles at once."""
    rng = random.Random(seed * 1000 + uid)
    conns = [connect("dashboard") for _ in range(args.tile_concurrency)]
    names = sorted(DASHBOARDS)
    try:
        while time.time() < until:
            dash = rng.choice(names)
            params = {"s": args.schema, "adv": f"advertiser_{rng.randrange(args.param_pool) * (200 // args.param_pool)}"}
            params["lo"], params["hi"] = rng.choice(RANGES)
            tiles = list(DASHBOARDS[dash].items())
            # The tiles run at once, a connection each, tile_concurrency at
            # a time.
            for k in range(0, len(tiles), len(conns)):
                batch = tiles[k:k + len(conns)]
                threads = []
                for conn, (tile, sql) in zip(conns, batch):
                    text = f"/* mixed {args.run_id} dashboard {dash}.{tile} */ " + sql.format(**params)
                    th = threading.Thread(target=run_one, args=(conn, text, rec, "dashboard", f"{dash}.{tile}"))
                    th.start()
                    threads.append(th)
                for th in threads:
                    th.join()
            if args.think > 0:
                time.sleep(min(rng.expovariate(1 / args.think), max(until - time.time(), 0)))
    finally:
        for c in conns:
            c.close()


def run_one(conn, text: str, rec: Recorder, cls: str, name: str):
    start = time.time()
    err = None
    try:
        with conn.cursor() as cur:
            cur.execute(text)
            if cur.description is not None:
                cur.fetchall()
    except Exception as e:  # recorded, the run goes on
        err = str(e).strip().splitlines()[0][:200] if str(e).strip() else type(e).__name__
    rec.add(cls, name, start, time.time() - start, err)


class ELT:
    """Runs the ELT jobs on their schedules with streams sessions. A job
    still running when it is due again is skipped."""

    def __init__(self, args, connect, rec: Recorder, until: float, first_batch: int):
        self.args, self.connect, self.rec, self.until = args, connect, rec, until
        self.jobs = [j for j in ELT_JOBS if j in args.elt_jobs.split(",")]
        self.running = {j: False for j in self.jobs}
        self.lock = threading.Lock()
        self.due: list[tuple[float, str]] = []
        now = time.time()
        for k, j in enumerate(self.jobs):
            # Spread over the period, so they do not all start together.
            self.due.append((now + args.elt_every * k / max(len(self.jobs), 1), j))
        self.batch = first_batch
        self.skipped = {j: 0 for j in self.jobs}
        self.devices = max(args.rows // 20, 1)

    def next_job(self) -> tuple[float, str] | None:
        """The next job due and when, rescheduled a period later."""
        with self.lock:
            if not self.due:
                return None
            self.due.sort()
            at, job = self.due.pop(0)
            self.due.append((at + self.args.elt_every, job))
            return at, job

    def begin(self, job: str) -> int | None:
        """Marks job running and returns the batch it works on, or None when
        its last run is still running (the run is skipped)."""
        with self.lock:
            if self.running[job]:
                self.skipped[job] += 1
                return None
            self.running[job] = True
            if job == "ingest":
                self.batch += 1
            return self.batch

    def stream(self, k: int):
        conn = self.connect("elt")
        try:
            while True:
                nxt = self.next_job()
                if nxt is None or nxt[0] >= self.until:
                    return
                at, job = nxt
                time.sleep(max(at - time.time(), 0))
                batch = self.begin(job)
                if batch is None:
                    continue
                lo = self.args.rows + (batch - 1) * self.args.batch_rows
                sql = elt_sql(job, self.args.schema, batch, lo, lo + self.args.batch_rows, self.devices)
                try:
                    run_one(conn, f"/* mixed {self.args.run_id} elt {job} */ " + sql, self.rec, "elt", job)
                finally:
                    with self.lock:
                        self.running[job] = False
        finally:
            conn.close()


def run(args, connect, sampler: WorkerSampler | None = None) -> dict:
    """Runs the mixed load and returns the results."""
    rec = Recorder()
    first_batch = 0
    with connect("setup") as c, c.cursor() as cur:
        if "ingest" in args.elt_jobs.split(",") and args.elt_streams:
            cur.execute(f"SELECT coalesce(max(batch_id), 0) FROM {args.schema}.impressions")
            first_batch = int(cur.fetchone()[0])
    start = time.time()
    t0, until = start + args.warmup, start + args.warmup + args.duration
    threads = [threading.Thread(target=dashboard_user, args=(u, args, connect, rec, until, args.seed), daemon=True)
               for u in range(args.dashboard_users)]
    elt = ELT(args, connect, rec, until, first_batch) if args.elt_streams else None
    if elt:
        threads += [threading.Thread(target=elt.stream, args=(k,), daemon=True) for k in range(args.elt_streams)]
    if sampler:
        sampler.start()
        threading.Timer(max(t0 - time.time(), 0), lambda: setattr(sampler, "counting", True)).start()
    for th in threads:
        th.start()
    # Statements still running at the end finish (and are left out of the
    # window, which counts statements by when they started).
    for th in threads:
        th.join()
    t1 = until
    end = time.time()
    if sampler:
        sampler.stop.set()
        sampler.join()
    return {"records": rec.records, "t0": t0, "t1": t1, "end": end, "skipped": elt.skipped if elt else {}}


def collect_history(conn, run_id: str, t0: float, t1: float, wait: float = 15.0) -> list[tuple]:
    """(class, name, queued_ms, duration_ms, workers) of the run's statements
    that started in the window [t0, t1) (Unix seconds), from the query
    history, which is written in the background."""
    prefix = f"/* mixed {run_id} "
    deadline = time.time() + wait
    rows: list[tuple] = []
    with conn.cursor() as cur:
        last = -1
        while time.time() < deadline:
            cur.execute(
                "SELECT split_part(query_text, ' ', 4), split_part(query_text, ' ', 5), queued_ms, duration_ms, workers "
                "FROM igloo.query_history "
                "WHERE starts_with(query_text, %s) AND epoch(start_time) >= %s AND epoch(start_time) < %s",
                (prefix, t0, t1),
            )
            rows = cur.fetchall()
            if len(rows) == last:
                break
            last = len(rows)
            time.sleep(3)
    return rows


def report(res: dict, hist: list[tuple], args, coord_price: float, worker_price: float, sampler) -> dict:
    window = res["t1"] - res["t0"]
    hs = history_summary(hist)
    busy = sum(v["worker_busy_seconds"] for k, v in hs.items() if "." not in k)
    worker_seconds = sampler.seconds if sampler and not sampler.failures else busy
    out = {
        "run_id": args.run_id,
        "label": args.label,
        "config": {k: getattr(args, k) for k in ("schema", "rows", "batch_rows", "duration", "warmup", "dashboard_users",
                                                  "tile_concurrency", "think", "param_pool", "elt_streams", "elt_every",
                                                  "elt_jobs", "seed", "coordinator", "worker", "spot_factor")},
        "window_seconds": round(window, 1),
        "classes": summarize(res["records"], res["t0"], res["t1"]),
        "history": hs,
        "elt_skipped": res["skipped"],
        "worker_seconds": round(worker_seconds, 1),
        "worker_seconds_source": "metrics" if sampler and not sampler.failures else "history (busy only)",
        "cost": cost(coord_price, worker_price, window, worker_seconds),
    }
    return out


def print_report(out: dict):
    print(f"\nrun {out['run_id']} {out['label']}: {out['window_seconds']:.0f} s window")
    print(f"{'class':<34} {'stmts':>6} {'err':>4} {'/min':>8} {'p50 s':>8} {'p95 s':>8} {'p99 s':>8} {'queue p95':>9} {'on wkrs':>7}")
    for key, v in out["classes"].items():
        h = out["history"].get(key, {})
        def fmt(x, width=8):
            return f"{x:{width}.3f}" if x is not None else "-".rjust(width)

        print(f"{key:<34} {v['statements']:>6} {v['errors']:>4} {v['per_minute']:>8.1f} {fmt(v['p50'])} {fmt(v['p95'])} {fmt(v['p99'])} "
              f"{fmt(h.get('queued_p95'), 9)} {str(h.get('on_workers', '-')):>7}")
    c = out["cost"]
    print(f"worker-seconds {out['worker_seconds']} ({out['worker_seconds_source']}); cost ${c['total_usd']} "
          f"(coordinator ${c['coordinator_usd']}, workers ${c['workers_usd']}), ${c['per_hour_usd']}/h")
    if any(out["elt_skipped"].values()):
        print(f"ELT runs skipped (still running when due again): {out['elt_skipped']}")
    for key, v in out["classes"].items():
        if v["errors"] and "." not in key:
            print(f"{key} errors, e.g.: {v['error_samples']}")


def parse(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("setup", help="create the synthetic schema")
    s.add_argument("--schema", default="mixed")
    s.add_argument("--rows", type=int, default=2_000_000, help="impressions to generate")
    r = sub.add_parser("run", help="run dashboards and ELT at once")
    r.add_argument("--schema", default="mixed")
    r.add_argument("--rows", type=int, default=2_000_000, help="impressions setup generated (ingested batches continue after them)")
    r.add_argument("--duration", type=float, default=300, help="measured seconds, after the warmup")
    r.add_argument("--warmup", type=float, default=30, help="seconds run before measuring")
    r.add_argument("--dashboard-users", type=int, default=8)
    r.add_argument("--tile-concurrency", type=int, default=4, help="tiles of one dashboard run at once")
    r.add_argument("--think", type=float, default=5, help="mean seconds between a user's dashboards (exponential)")
    r.add_argument("--param-pool", type=int, default=20, help="advertisers dashboards filter by (of 200)")
    r.add_argument("--elt-streams", type=int, default=2, help="sessions running ELT jobs; 0 for dashboards only")
    r.add_argument("--elt-every", type=float, default=60, help="each job is due every this many seconds")
    r.add_argument("--elt-jobs", default=",".join(ELT_JOBS))
    r.add_argument("--batch-rows", type=int, default=200_000, help="impressions each ingest appends")
    r.add_argument("--seed", type=int, default=1)
    r.add_argument("--metrics-url", help="Igloo's /metrics, to bill workers for the seconds they ran")
    r.add_argument("--coordinator", help="instance type in pricing.toml, or a price per hour")
    r.add_argument("--worker", help="instance type in pricing.toml, or a price per hour")
    r.add_argument("--spot-factor", type=float, default=1.0, help="multiplies the worker price, e.g. 0.35 for Spot")
    r.add_argument("--result-cache", choices=["server", "off"], default="server",
                   help="leave Igloo's result cache as the server has it (default), or turn it off per session")
    r.add_argument("--label", default="")
    r.add_argument("--run-id", default="")
    r.add_argument("--out", help="write results as JSON here")
    args = p.parse_args(argv)
    if args.cmd == "run":
        args.run_id = args.run_id or time.strftime("%Y%m%dT%H%M%S")
        if args.param_pool < 1 or args.param_pool > 200:
            p.error("--param-pool must be from 1 to 200")
        unknown = set(args.elt_jobs.split(",")) - set(ELT_JOBS)
        if unknown:
            p.error(f"unknown ELT jobs {sorted(unknown)}; known: {ELT_JOBS}")
    return args


def main(argv=None) -> None:
    args = parse(argv)
    import psycopg

    dsn = os.environ["IGLOO_DSN"]
    if args.cmd == "setup":
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            for sql in setup_statements(args.schema, args.rows):
                t = time.time()
                cur.execute(sql)
                print(f"{time.time() - t:7.1f}s  {sql[:100]}", flush=True)
        return

    def connect(cls: str):
        conn = psycopg.connect(dsn, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(f"SET query_tag = 'mixed:{args.run_id}:{cls}'")
            if args.result_cache == "off":
                cur.execute("SET igloo_result_cache = off")
        return conn

    coord_price, worker_price = load_prices(args)
    sampler = WorkerSampler(args.metrics_url) if args.metrics_url else None
    res = run(args, connect, sampler)
    with psycopg.connect(dsn, autocommit=True) as conn:
        hist = collect_history(conn, args.run_id, res["t0"], res["t1"])
    out = report(res, hist, args, coord_price, worker_price, sampler)
    print_report(out)
    if args.out:
        pathlib.Path(args.out).write_text(json.dumps(out, indent=1, default=str) + "\n")


if __name__ == "__main__":
    main()
