#!/usr/bin/env python3
"""Check Igloo's answers where the data outgrows one machine's memory.

TPC-H and TPC-DS at a scale whose working sets exceed the coordinator's
memory (for example scale factor 100 with IGLOO_DUCKDB_MEMORY_LIMIT=8GB), and
for TPC-H also joins and aggregations on a skewed key. Each query runs on
Igloo with distributed execution off, on or auto, and its rows are compared
with plain DuckDB's answer on the same data with ample memory.

1. reference: plain DuckDB (the duckdb package, on a machine with memory to
   spare) generates the data, writes each table as Parquet under --data, runs
   every query, and keeps its answer:

       python bench/overmemory.py reference --suite tpcds --scale 100 \\
           --data /mnt/data/tpcds100 --out tpcds100.ref.json

   Then copy --data where Igloo can read it, e.g. with `aws s3 sync`.

2. load: the tables in Igloo from those files, through IGLOO_DSN as for
   run.py, as an admin (only admins read files directly):

       python bench/overmemory.py load --reference tpcds100.ref.json \\
           --schema lake.tpcds100 --source s3://bucket/data/tpcds100

3. run: each query once in --mode, its rows against the reference. Per query
   it records where the statement ran, its time, the coordinator's and the
   workers' peak memory, spill, and the bytes the workers exchanged. --writes
   adds a copy and a partitioned copy of a table, each checked against it:

       python bench/overmemory.py run --reference tpcds100.ref.json \\
           --schema lake.tpcds100 --mode on --label on --out results.jsonl \\
           --workers 10.0.0.11,10.0.0.12 \\
           --worker-spill-cmd 'ssh {host} du -sb /var/lib/igloo/tmp' \\
           --writes store_sales:ss_item_sk

   Workers' memory and exchanged bytes come from their /metrics (port 8080);
   their spill only from --worker-spill-cmd, whose output starts with bytes.

4. report: a table a mode, with every mismatch listed first:

       python bench/overmemory.py report results.jsonl > report.md
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime
import decimal
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
import threading
import time
import urllib.request

# The TPC-H suite adds tables where 60% of the rows share one key, and
# queries that join and group on it: a distributed plan that partitions by
# the key sends most rows to one task.
SKEW_TABLES = {
    "skew_orders": "SELECT CASE WHEN o_orderkey % 10 < 6 THEN 1 ELSE o_custkey END AS k, "
                   "o_orderkey, o_totalprice AS v, o_orderdate AS d FROM orders",
    "skew_lineitem": "SELECT CASE WHEN l_orderkey % 10 < 6 THEN 1 ELSE l_orderkey END AS k, "
                     "l_extendedprice AS v, l_quantity AS q FROM lineitem",
}
SKEW_QUERIES = {
    "skew_join_one_build_row": "SELECT c_nationkey, count(*) AS n, sum(v) AS v "
                               "FROM skew_orders JOIN customer ON k = c_custkey GROUP BY c_nationkey",
    "skew_join_large": "SELECT o_orderpriority, count(*) AS n, sum(v) AS v, sum(q) AS q "
                       "FROM skew_lineitem JOIN orders ON k = o_orderkey GROUP BY o_orderpriority",
    "skew_group": "SELECT k, count(*) AS n, sum(v) AS v, min(d) AS first, max(d) AS last "
                  "FROM skew_orders GROUP BY k ORDER BY n DESC, k LIMIT 100",
    "skew_distinct": "SELECT k % 100 AS b, count(DISTINCT o_orderkey) AS n, count(DISTINCT v) AS nv "
                     "FROM skew_orders GROUP BY b",
}
ANSWER_ROWS = 1000   # answers up to this many rows are kept whole, to show a mismatch's rows


def norm(v):
    """A value as both sides of a comparison see it: numbers to 6 significant
    digits (a distributed sum of doubles adds in another order), times as
    ISO text."""
    if v is None or isinstance(v, bool):
        return v
    if isinstance(v, (int, float, decimal.Decimal)):
        x = float(v)
        return float(f"{x:.6g}") if x == x else "nan"
    if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
        return v.isoformat()
    return str(v)


def normalized(rows):
    return sorted(json.dumps([norm(v) for v in r]) for r in rows)


def fingerprint(rows) -> str:
    """The rows' hash, whatever their order."""
    return hashlib.sha256("\n".join(normalized(rows)).encode()).hexdigest()[:16]


def suite_queries(con, suite: str) -> dict[str, str]:
    fn = "tpch_queries" if suite == "tpch" else "tpcds_queries"
    qs = {f"q{n:02d}": sql for n, sql in con.execute(f"SELECT query_nr, query FROM {fn}()").fetchall()}
    if suite == "tpch":
        qs.update(SKEW_QUERIES)
    return qs


def cmd_reference(a):
    import duckdb

    con = duckdb.connect()
    con.execute(f"INSTALL {a.suite}")
    con.execute(f"LOAD {a.suite}")
    t = time.time()
    con.execute(f"CALL {'dbgen' if a.suite == 'tpch' else 'dsdgen'}(sf={a.scale})")
    generated = time.time() - t
    if a.suite == "tpch":
        for name, sql in SKEW_TABLES.items():
            con.execute(f"CREATE TABLE {name} AS {sql}")
    tables = [r[0] for r in con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main' ORDER BY 1").fetchall()]
    data = pathlib.Path(a.data)
    data.mkdir(parents=True, exist_ok=True)
    counts = {}
    for tb in tables:
        counts[tb] = con.execute(f"SELECT count(*) FROM {tb}").fetchone()[0]
        con.execute(f"COPY {tb} TO '{data / tb}' (FORMAT parquet, FILE_SIZE_BYTES '256MB', OVERWRITE_OR_IGNORE true)")
        print(f"{tb}: {counts[tb]} rows written", flush=True)
    ref = {"suite": a.suite, "scale": a.scale, "duckdb": duckdb.__version__, "generated_seconds": round(generated, 1),
           "tables": counts, "queries": {}}
    for name, sql in suite_queries(con, a.suite).items():
        t = time.perf_counter()
        rows = con.execute(sql).fetchall()
        s = time.perf_counter() - t
        ref["queries"][name] = {"sql": sql, "rows": len(rows), "fp": fingerprint(rows), "seconds": round(s, 3),
                                "answer": normalized(rows) if len(rows) <= ANSWER_ROWS else None}
        print(f"{name}: {len(rows)} rows in {s:.1f}s", flush=True)
    pathlib.Path(a.out).write_text(json.dumps(ref, indent=1) + "\n")


def igloo(mode: str | None = None, schema: str | None = None):
    import psycopg

    conn = psycopg.connect(os.environ["IGLOO_DSN"], autocommit=True)
    conn.execute("SET igloo_result_cache = off")
    if schema:
        conn.execute(f"SET search_path = '{schema}'")
    if mode:
        conn.execute(f"SET distributed_execution = '{mode}'")
    return conn


def cmd_load(a):
    ref = json.loads(pathlib.Path(a.reference).read_text())
    conn = igloo(a.mode)
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {a.schema}")
    for tb, n in ref["tables"].items():
        t = time.time()
        conn.execute(f"CREATE OR REPLACE TABLE {a.schema}.{tb} AS "
                     f"SELECT * FROM read_parquet('{a.source.rstrip('/')}/{tb}/*.parquet')")
        got = conn.execute(f"SELECT count(*) FROM {a.schema}.{tb}").fetchone()[0]
        print(f"{tb}: {got} rows in {time.time() - t:.0f}s{'' if got == n else f', expected {n}'}", flush=True)
        if got != n:
            sys.exit(1)


def scrape(url: str, names: tuple[str, ...]) -> dict[str, float]:
    out = {}
    try:
        for line in urllib.request.urlopen(url, timeout=2).read().decode().splitlines():
            name, _, val = line.rpartition(" ")
            if name in names:
                out[name] = float(val)
    except Exception:
        pass
    return out


RSS, SENT = "process_resident_memory_bytes", "igloo_worker_sent_bytes_total"


class Sampler(threading.Thread):
    """While a statement runs: the coordinator's resident memory and DuckDB's
    temporary storage (its spill) each second, the workers' resident memory
    every 2 seconds, and their spill every 5 when there is a command for it."""

    def __init__(self, a):
        super().__init__(daemon=True)
        self.a, self.stop = a, threading.Event()
        self.coord_rss = self.coord_spill = self.worker_rss = self.worker_spill = 0
        self.pool = concurrent.futures.ThreadPoolExecutor(max(4, len(a.hosts)))
        try:
            self.admin = igloo("off")
        except Exception:   # the server is down: the statement will say so
            self.admin = None

    def workers(self, fn):
        return list(self.pool.map(fn, self.a.hosts))

    def spill(self, host):
        cmd = self.a.worker_spill_cmd.replace("{host}", host)
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
        m = re.match(r"\s*(\d+)", r.stdout)
        return int(m.group(1)) if m else 0

    def run(self):
        n = 0
        while not self.stop.is_set():
            self.coord_rss = max(self.coord_rss, scrape(self.a.coordinator_metrics, (RSS,)).get(RSS, 0))
            try:
                if self.admin is not None:
                    tmp = self.admin.execute("SELECT coalesce(sum(temporary_storage_bytes), 0) FROM duckdb_memory()").fetchone()[0]
                    self.coord_spill = max(self.coord_spill, int(tmp))
            except Exception:
                pass
            if self.a.hosts and n % 2 == 0:
                rss = self.workers(lambda h: scrape(f"http://{h}:8080/metrics", (RSS,)).get(RSS, 0))
                self.worker_rss = max([self.worker_rss, *rss])
            if self.a.hosts and self.a.worker_spill_cmd and n % 5 == 0:
                try:
                    self.worker_spill = max(self.worker_spill, sum(self.workers(self.spill)))
                except Exception:
                    pass
            n += 1
            self.stop.wait(1)

    def sent(self) -> float:
        return sum(scrape(f"http://{h}:8080/metrics", (SENT,)).get(SENT, 0) for h in self.a.hosts)


def history(conn, tag: str) -> dict:
    cols = ["workers", "stages", "exchanges", "placement", "outcome", "sqlstate"]
    for _ in range(40):
        try:
            r = conn.execute(f"SELECT {', '.join(cols)} FROM igloo.query_history WHERE starts_with(query_text, %s) "
                             "ORDER BY start_time DESC LIMIT 1", (tag,)).fetchall()
        except Exception:   # the server went away; a statement it lost has no record
            return {}
        if r:
            return dict(zip(cols, r[0]))
        time.sleep(0.5)
    return {}


def reconnect(a):
    """Sessions again once the server answers, as after a crash and restart: up to 10 minutes."""
    for attempt in range(120):
        try:
            return igloo(a.mode, a.schema), igloo("off", a.schema)
        except Exception:
            if attempt == 119:
                raise
            time.sleep(5)


def timed(conn, sql: str, timeout: float):
    """Runs sql, canceling it after timeout seconds: rows, seconds, error."""
    timer = threading.Timer(timeout, conn.cancel_safe)
    timer.start()
    t = time.perf_counter()
    try:
        cur = conn.execute(sql)
        rows, err = (cur.fetchall() if cur.description else []), None   # a write returns no rows
    except Exception as e:
        rows, err = None, str(e).strip().splitlines()[0][:300] if str(e).strip() else type(e).__name__
    finally:
        timer.cancel()
    s = time.perf_counter() - t
    if err and s >= timeout:
        err = f"timeout after {timeout:.0f}s"
    return rows, s, err


def measure(a, conn, meta, label: str, sql: str):
    """Runs one statement with the sampler on; returns what it measured, and
    its rows. meta, a session with distributed execution off, reads the
    statement's history."""
    tag = f"/* overmemory {a.label} {label} {time.time_ns()} */ "
    s = Sampler(a)
    sent0 = s.sent()
    s.start()
    rows, secs, err = timed(conn, tag + sql, a.timeout)
    s.stop.set()
    s.join()
    s.pool.shutdown()
    if s.admin is not None:
        s.admin.close()
    rec = {"seconds": round(secs, 2), "error": err,
           "coordinator_peak_bytes": int(s.coord_rss), "coordinator_spill_bytes": s.coord_spill,
           "worker_peak_bytes": int(s.worker_rss), "worker_spill_bytes": s.worker_spill if a.worker_spill_cmd else None,
           "exchange_bytes": int(s.sent() - sent0) if a.hosts else None}
    rec.update(history(meta, tag))
    return rec, rows


def write_checks(a, conn, meta, table: str, key: str) -> list[dict]:
    """A copy and a bucketed copy of table, each against it: row count and
    an order-free hash of every column."""
    cols = [r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s "
        "ORDER BY ordinal_position", (a.schema.split(".")[-1], table)).fetchall()]
    check = f"SELECT count(*), bit_xor(hash({', '.join(cols)})) FROM "
    src = conn.execute(check + f"{a.schema}.{table}").fetchone()
    target = f"{a.schema}_writes"
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {target}")
    out = []
    for kind, clause in (("copy", ""), ("partitioned", f" PARTITIONED BY (bucket(64, {key}))")):
        name = f"{target}.{table}_{kind}_{re.sub(r'[^a-z0-9_]', '_', a.label.lower())}"
        rec, _ = measure(a, conn, meta, f"write-{kind}", f"CREATE OR REPLACE TABLE {name}{clause} AS SELECT * FROM {a.schema}.{table}")
        got = conn.execute(check + name).fetchone() if not rec["error"] else None
        rec.update({"query": f"write_{kind}_{table}", "rows": got[0] if got else None,
                    "match": None if got is None else list(got) == list(src)})
        out.append(rec)
    return out


def cmd_run(a):
    ref = json.loads(pathlib.Path(a.reference).read_text())
    a.hosts = [h for h in a.workers.split(",") if h]
    conn, meta = igloo(a.mode, a.schema), igloo("off", a.schema)
    limit = conn.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    names = [n for n in ref["queries"] if (not a.only or n in a.only.split(","))]
    base = {"suite": ref["suite"], "scale": ref["scale"], "mode": a.mode, "label": a.label, "build": a.build,
            "memory_limit": limit, "workers_given": len(a.hosts)}
    with open(a.out, "a") as out:
        for name in names:
            q = ref["queries"][name]
            rec, rows = measure(a, conn, meta, name, q["sql"].strip().rstrip(";"))
            rec.update(base, query=name, rows=None if rows is None else len(rows))
            if rows is not None:
                fp = fingerprint(rows)
                rec["match"] = fp == q["fp"]
                if not rec["match"] and q["answer"] is not None:
                    got, want = set(normalized(rows)), set(q["answer"])
                    rec["only_igloo"], rec["only_reference"] = sorted(got - want)[:20], sorted(want - got)[:20]
            else:
                rec["match"] = None
            if conn.closed or meta.closed:
                rec["server_lost"] = True
                conn, meta = reconnect(a)
            out.write(json.dumps(rec) + "\n")
            out.flush()
            print(line(rec), flush=True)
        for spec in a.writes:
            table, key = spec.split(":")
            for rec in write_checks(a, conn, meta, table, key):
                rec.update(base)
                out.write(json.dumps(rec) + "\n")
                out.flush()
                print(line(rec), flush=True)


def gib(b):
    return "–" if b is None else f"{b / 2**30:.1f}"


def where(r) -> str:
    """Where the history says the statement ran; – when it has no record."""
    if r.get("workers"):
        return f"{r['workers']} workers"
    return "one machine" if r.get("outcome") else "–"


def line(r) -> str:
    verdict = {True: "match", False: "MISMATCH", None: "no result"}[r.get("match")]
    return (f"[{r['label']} {r['query']}] {verdict} {r['seconds']}s {where(r)} coordinator {gib(r['coordinator_peak_bytes'])}G "
            f"spill {gib(r['coordinator_spill_bytes'])}G workers {gib(r['worker_peak_bytes'])}G spill {gib(r['worker_spill_bytes'])}G "
            f"exchanged {gib(r['exchange_bytes'])}G {r.get('error') or ''}")


def cmd_report(a):
    recs = [json.loads(l) for p in a.files for l in pathlib.Path(p).read_text().splitlines() if l.strip()]
    bad = [r for r in recs if r.get("match") is not True]
    print("# Beyond one machine's memory: results against plain DuckDB\n")
    print("| Suite | Scale | Mode | Label | Memory limit | Queries | Match | Mismatch | No result | Seconds in all |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    groups = {}
    for r in recs:
        groups.setdefault((r["suite"], r["scale"], r["mode"], r["label"], r["memory_limit"]), []).append(r)
    for (suite, scale, mode, label, limit), rs in groups.items():
        m = sum(r.get("match") is True for r in rs)
        x = sum(r.get("match") is False for r in rs)
        print(f"| {suite} | {scale} | {mode} | {label} | {limit} | {len(rs)} | {m} | {x} | {len(rs) - m - x} | "
              f"{sum(r['seconds'] for r in rs):.0f} |")
    if bad:
        print("\n## Mismatches and runs with no result\n")
        print("| Suite | Label | Query | Result | Error, or rows only Igloo / only the reference returned |")
        print("|---|---|---|---|---|")
        for r in bad:
            what = r.get("error") or f"{len(r.get('only_igloo', []))} / {len(r.get('only_reference', []))} rows differ"
            print(f"| {r['suite']} | {r['label']} | {r['query']} | {'MISMATCH' if r.get('match') is False else 'none'} | {what} |")
    for (suite, scale, mode, label, limit), rs in groups.items():
        print(f"\n## {suite} at scale {scale}, {label} (mode {mode}, coordinator limit {limit})\n")
        print("| Query | Result | Ran on | Seconds | Coordinator peak GiB | Coordinator spill GiB | "
              "Worker peak GiB | Workers' spill GiB | Exchanged GiB |")
        print("|---|---|---|---|---|---|---|---|---|")
        for r in rs:
            verdict = {True: "match", False: "**MISMATCH**", None: "no result"}[r.get("match")]
            print(f"| {r['query']} | {verdict} | {where(r)} | {r['seconds']:.1f} | {gib(r['coordinator_peak_bytes'])} | "
                  f"{gib(r['coordinator_spill_bytes'])} | {gib(r['worker_peak_bytes'])} | {gib(r['worker_spill_bytes'])} | "
                  f"{gib(r['exchange_bytes'])} |")


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("reference")
    r.add_argument("--suite", choices=["tpch", "tpcds"], required=True)
    r.add_argument("--scale", type=float, required=True)
    r.add_argument("--data", required=True)
    r.add_argument("--out", required=True)
    l = sub.add_parser("load")
    l.add_argument("--reference", required=True)
    l.add_argument("--schema", required=True)
    l.add_argument("--source", required=True)
    l.add_argument("--mode", default="auto")
    u = sub.add_parser("run")
    u.add_argument("--reference", required=True)
    u.add_argument("--schema", required=True)
    u.add_argument("--mode", choices=["off", "on", "auto"], required=True)
    u.add_argument("--label", required=True)
    u.add_argument("--out", required=True)
    u.add_argument("--build", default="")
    u.add_argument("--only", default="")
    u.add_argument("--timeout", type=float, default=1800)
    u.add_argument("--coordinator-metrics", default="http://127.0.0.1:8432/metrics")
    u.add_argument("--workers", default="", help="workers' hosts, comma separated, for their /metrics on port 8080")
    u.add_argument("--worker-spill-cmd", default="", help="prints a worker's spill bytes first; {host} is replaced")
    u.add_argument("--writes", action="append", default=[], help="TABLE:BUCKET_KEY, copied after the queries")
    o = sub.add_parser("report")
    o.add_argument("files", nargs="+")
    a = p.parse_args(argv)
    {"reference": cmd_reference, "load": cmd_load, "run": cmd_run, "report": cmd_report}[a.cmd](a)


if __name__ == "__main__":
    main()
