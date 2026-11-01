#!/usr/bin/env python3
"""Measure distributed execution against one machine.

Runs each query with `SET distributed_execution = 'off'` (the replica alone)
and `'on'` (divided among the workers), interleaved, several times each, and
reports the fastest run of each, the speedup, how many workers ran it, and
what one run costs on the machines given:

    cost(one machine) = coordinator_price * seconds
    cost(distributed) = (coordinator_price + workers * worker_price) * seconds

Prices are per hour, for machines that run only while the query does, as
with workers started on demand: the idle tail before workers stop is left
out, since a busy warehouse amortizes it over many queries.

    python bench/distributed.py --suite tpch --runs 3 \\
        --coordinator-price 0.578 --worker-price 0.578 --out bench/results/dist.json

IGLOO_DSN names the Igloo to connect to, as for run.py. The queries' tables
must exist; see bench/README.md.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import statistics
import time

QUERIES = pathlib.Path(__file__).parent / "queries"


def run(conn, mode: str, sql: str) -> tuple[float, str]:
    """Runs sql in mode; returns client seconds and the tag it ran with."""
    with conn.cursor() as cur:
        cur.execute(f"SET distributed_execution = '{mode}'")
        tag = f"/* bench {time.time_ns()} */ "
        start = time.perf_counter()
        cur.execute(tag + sql)
        cur.fetchall()
        elapsed = time.perf_counter() - start
    return elapsed, tag


def workers_of(conn, tag: str, wait: float = 10.0) -> int:
    """Reads how many workers ran the statement tagged tag from the query
    history, which is written in the background."""
    deadline = time.time() + wait
    with conn.cursor() as cur:
        while True:
            cur.execute(
                "SELECT workers FROM igloo.query_history WHERE starts_with(query_text, %s) "
                "ORDER BY start_time DESC LIMIT 1",
                (tag,),
            )
            row = cur.fetchone()
            if row is not None or time.time() > deadline:
                return row[0] if row else -1
            time.sleep(0.5)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--suite", default="tpch", help="directory under bench/queries")
    p.add_argument("--queries", help="comma-separated query names, e.g. q01,q06; default all")
    p.add_argument("--runs", type=int, default=3, help="runs per query and mode, after one untimed run each")
    p.add_argument("--coordinator-price", type=float, default=0.0, help="coordinator's price per hour")
    p.add_argument("--worker-price", type=float, default=0.0, help="each worker's price per hour")
    p.add_argument("--label", default="", help="describes the setup in the results")
    p.add_argument("--out", help="write results as JSON here")
    args = p.parse_args()

    import psycopg

    from run import igloo_result_cache

    conn = psycopg.connect(os.environ["IGLOO_DSN"], autocommit=True)
    # As in run.py: a run answered from the result cache times neither mode.
    with conn.cursor() as cur:
        cur.execute("SET igloo_result_cache = off")
    result_cache = igloo_result_cache(conn)
    names = set(args.queries.split(",")) if args.queries else None
    results = []
    for path in sorted((QUERIES / args.suite).glob("*.sql")):
        if names and path.stem not in names:
            continue
        sql = path.read_text().strip().rstrip(";")
        times = {"off": [], "on": []}
        workers = 0
        for mode in ("off", "on"):
            run(conn, mode, sql)  # warm the caches
        for _ in range(args.runs):
            for mode in ("off", "on"):
                elapsed, tag = run(conn, mode, sql)
                times[mode].append(elapsed)
                if mode == "on":
                    workers = max(workers, workers_of(conn, tag))
        local, dist = min(times["off"]), min(times["on"])
        r = {
            "query": path.stem,
            "local_seconds": local,
            "distributed_seconds": dist,
            "local_median": statistics.median(times["off"]),
            "distributed_median": statistics.median(times["on"]),
            "workers": workers,
            "speedup": local / dist if dist else None,
            "local_cost": args.coordinator_price * local / 3600,
            "distributed_cost": (args.coordinator_price + max(workers, 0) * args.worker_price) * dist / 3600,
        }
        results.append(r)
        print(
            f"{r['query']:>6}  one machine {local:7.3f}s  distributed {dist:7.3f}s on {workers} workers"
            f"  speedup {r['speedup']:5.2f}x",
            flush=True,
        )

    ran = [r for r in results if r["workers"] > 0]
    summary = {
        "label": args.label,
        "suite": args.suite,
        "runs": args.runs,
        "coordinator_price": args.coordinator_price,
        "worker_price": args.worker_price,
        "result_cache": result_cache,
        "results": results,
    }
    if ran:
        local = sum(r["local_seconds"] for r in ran)
        dist = sum(r["distributed_seconds"] for r in ran)
        summary["distributed_queries"] = len(ran)
        summary["total_local_seconds"] = local
        summary["total_distributed_seconds"] = dist
        print(f"\n{len(ran)} queries ran distributed: {local:.2f}s on one machine, {dist:.2f}s distributed ({local / dist:.2f}x)")
        if args.coordinator_price:
            lc = sum(r["local_cost"] for r in ran)
            dc = sum(r["distributed_cost"] for r in ran)
            print(f"cost: ${lc:.6f} on one machine, ${dc:.6f} distributed ({dc / lc:.2f}x)")
    if args.out:
        pathlib.Path(args.out).write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
