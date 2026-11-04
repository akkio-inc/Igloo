#!/usr/bin/env python3
"""Measure waits for the catalog lock while ingestion, schema changes, BI
metadata discovery and dashboards run together on one Igloo replica.

DDL and listings of the catalog (information_schema, pg_catalog, SHOW,
DESCRIBE) hold a replica's catalog lock alone, and binding any other
statement holds it shared (see docs/operations.md, concurrency on a
replica). This runs a stream of each for --duration seconds, then reads from
igloo.query_history how long each statement waited for the lock
(catalog_wait_ms), and reports the waits' p50 and p95 by stream and
statement type, with the replica's threads (igloo_process_threads) over the
run and how long BI sessions took to open, as a BI tool opens one per
listing.

    python bench/catalogload.py setup --schema lake.catload
    python bench/catalogload.py run --schema lake.catload --duration 600 \\
        --out catalogload.json > catalogload.md

IGLOO_DSN names the Igloo to connect to, as for run.py, as an admin. The
JSON keeps every stream's backend pids, to match the server's log lines
about long holds (`held the catalog lock exclusively`) to streams.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import random
import threading
import time
import urllib.request

TABLES = 50          # tables setup makes, for listings to walk and dashboards to read
TABLE_ROWS = 1_000_000


def connect():
    import psycopg

    conn = psycopg.connect(os.environ["IGLOO_DSN"], autocommit=True)
    conn.execute("SET igloo_result_cache = off")
    return conn


def cmd_setup(a):
    conn = connect()
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {a.schema}")
    for i in range(TABLES):
        conn.execute(f"CREATE OR REPLACE TABLE {a.schema}.t{i} AS SELECT range AS id, range % 50 AS grp, "
                     f"(range * 7919 % 10000) / 100.0 AS amt FROM range({TABLE_ROWS})")
    conn.execute(f"CREATE OR REPLACE TABLE {a.schema}.events AS SELECT range AS id, range % 50 AS grp, "
                 f"0.0::DOUBLE AS amt, now() AS at FROM range(0)")
    print(f"{TABLES} tables of {TABLE_ROWS} rows and an empty events table in {a.schema}")


class Stream(threading.Thread):
    """A session issuing one kind of statement, with a pause between them,
    until stop is set. Each statement starts with /* catalogload KIND */."""

    def __init__(self, kind, n, a, stop, reconnect=False):
        super().__init__(daemon=True)
        self.kind, self.n, self.a, self.stop, self.reconnect = kind, n, a, stop, reconnect
        self.pids, self.errors, self.connects, self.done = set(), [], [], 0
        self.rng = random.Random(f"{kind}{n}")

    def statements(self):
        s, r, k = self.a.schema, self.rng, self.done
        bare = s.split(".")[-1]
        t = f"{s}.t{r.randrange(TABLES)}"
        if self.kind == "dashboard":
            return [f"SELECT grp, count(*), sum(amt) FROM {t} WHERE id % 7 = {r.randrange(7)} GROUP BY grp"], 0.5
        if self.kind == "ingest":
            return [f"INSERT INTO {s}.events SELECT range, range % 50, (range % 1000) / 10.0, now() "
                    f"FROM range({self.a.batch_rows})"], 1.0
        if self.kind == "listing":
            return [r.choice([
                "SELECT table_schema, table_name, table_type FROM information_schema.tables",
                f"SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = '{bare}'",
                "SELECT n.nspname, c.relname FROM pg_catalog.pg_class c "
                "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace",
                f"DESCRIBE {t}",
            ])], 1.0
        # schema changes, as a dbt run makes them: a table built, altered, swapped in, the old one dropped, and a view
        name = f"{s}.ddl_{self.n}_{k % 4}"
        return [f"CREATE OR REPLACE TABLE {name}_new AS SELECT * FROM {t} LIMIT 1000",
                f"ALTER TABLE {name}_new ADD COLUMN extra INTEGER",
                f"DROP TABLE IF EXISTS {name}",
                f"ALTER TABLE {name}_new RENAME TO {name.split('.')[-1]}",
                f"CREATE OR REPLACE VIEW {s}.v_{self.n} AS SELECT grp, count(*) AS n FROM {t} GROUP BY grp"], 2.0

    def run(self):
        conn, pause = None, 1.0
        while not self.stop.is_set():
            try:
                if conn is None or self.reconnect:
                    if conn is not None:
                        conn.close()
                    t = time.perf_counter()
                    conn = connect()
                    self.connects.append(time.perf_counter() - t)
                    self.pids.add(conn.info.backend_pid)
                stmts, pause = self.statements()
                for q in stmts:
                    cur = conn.execute(f"/* catalogload {self.kind} */ {q}")
                    if cur.description:
                        cur.fetchall()
                self.done += 1
            except Exception as e:
                self.errors.append(str(e).strip().splitlines()[0][:200] if str(e).strip() else type(e).__name__)
                if conn is not None and conn.closed:
                    conn = None
            self.stop.wait(pause)
        if conn is not None:
            conn.close()


def threads_sampler(url, stop, out):
    while not stop.is_set():
        try:
            for line in urllib.request.urlopen(url, timeout=2).read().decode().splitlines():
                name, _, val = line.rpartition(" ")
                if name in ("igloo_process_threads", "igloo_statements_waiting"):
                    out.setdefault(name, []).append(float(val))
        except Exception:
            pass
        stop.wait(1)


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))]


def cmd_run(a):
    stop = threading.Event()
    kinds = {"dashboard": a.dashboards, "ingest": a.ingest, "ddl": a.ddl, "listing": a.listings}
    streams = [Stream(k, i, a, stop, reconnect=(k == "listing")) for k, n in kinds.items() for i in range(n)]
    gauges = {}
    sampler = threading.Thread(target=threads_sampler, args=(a.metrics, stop, gauges), daemon=True)
    started = datetime.datetime.now(datetime.timezone.utc)
    sampler.start()
    for s in streams:
        s.start()
    time.sleep(a.duration)
    stop.set()
    for s in streams:
        s.join(timeout=600)
    time.sleep(5)   # the history is written in the background
    for attempt in range(60):   # a replica that froze and was restarted takes a while to serve again
        try:
            conn = connect()
            break
        except Exception:
            if attempt == 59:
                raise
            time.sleep(5)
    conn.execute("SET distributed_execution = 'off'")
    rows = conn.execute(
        "SELECT query_text, statement_type, catalog_wait_ms, duration_ms, outcome FROM igloo.query_history "
        "WHERE start_time >= %s AND starts_with(query_text, '/* catalogload ')", (started.replace(tzinfo=None),)).fetchall()
    groups = {}
    for text, stype, wait, dur, outcome in rows:
        kind = text.split()[2]
        g = groups.setdefault((kind, stype), {"waits": [], "durations": [], "failed": 0})
        g["waits"].append(wait or 0)
        g["durations"].append(dur or 0)
        g["failed"] += outcome != "ok"
    of = lambda kind: [s for s in streams if s.kind == kind]
    opens = [c for s in streams if s.reconnect for c in s.connects]
    report = {
        "started": started.isoformat(timespec="seconds"), "duration_s": a.duration, "streams": kinds,
        "pids": {k: sorted(p for s in of(k) for p in s.pids) for k in kinds},
        "rounds_done": {k: sum(s.done for s in of(k)) for k in kinds},
        "client_errors": {k: sum(len(s.errors) for s in of(k)) for k in kinds},
        "error_samples": sorted({e for s in streams for e in s.errors})[:20],
        "session_open_s": {"p50": pct(opens, 50), "p95": pct(opens, 95)},
        "threads": {k: {"p50": pct(v, 50), "max": max(v)} for k, v in gauges.items()},
        "groups": [{"stream": k, "statement_type": t, "statements": len(g["waits"]), "failed": g["failed"],
                    "wait_ms": {"p50": pct(g["waits"], 50), "p95": pct(g["waits"], 95), "max": max(g["waits"])},
                    "duration_ms": {"p50": pct(g["durations"], 50), "p95": pct(g["durations"], 95)}}
                   for (k, t), g in sorted(groups.items())],
    }
    with open(a.out, "w") as f:
        json.dump(report, f, indent=1)
    print(f"# Catalog lock waits, {a.duration:.0f} s, {started:%Y-%m-%d %H:%M} UTC\n")
    print("| Stream | Statement type | Statements | Failed | Wait p50 / p95 / max (ms) | Duration p50 / p95 (ms) |")
    print("|---|---|---|---|---|---|")
    for g in report["groups"]:
        w, d = g["wait_ms"], g["duration_ms"]
        print(f"| {g['stream']} | {g['statement_type']} | {g['statements']} | {g['failed']} | "
              f"{w['p50']:.1f} / {w['p95']:.1f} / {w['max']:.1f} | {d['p50']:.0f} / {d['p95']:.0f} |")
    so = report["session_open_s"]
    if so["p50"] is not None:
        print(f"\nBI sessions opened in {so['p50']:.2f} s at p50, {so['p95']:.2f} s at p95.")
    for k, v in report["threads"].items():
        print(f"{k}: p50 {v['p50']:.0f}, max {v['max']:.0f}.")
    print(f"Rounds each stream kind finished: {report['rounds_done']}")
    print(f"Client errors: {report['client_errors']}")


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("setup")
    s.add_argument("--schema", required=True)
    r = sub.add_parser("run")
    r.add_argument("--schema", required=True)
    r.add_argument("--duration", type=float, default=600)
    r.add_argument("--dashboards", type=int, default=16)
    r.add_argument("--ingest", type=int, default=2)
    r.add_argument("--ddl", type=int, default=2)
    r.add_argument("--listings", type=int, default=4)
    r.add_argument("--batch-rows", type=int, default=100_000)
    r.add_argument("--metrics", default="http://127.0.0.1:8432/metrics")
    r.add_argument("--out", required=True)
    a = p.parse_args(argv)
    {"setup": cmd_setup, "run": cmd_run}[a.cmd](a)


if __name__ == "__main__":
    main()
