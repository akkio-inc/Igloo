#!/usr/bin/env python3
"""Run a query set on Igloo, Snowflake or Databricks and record how long
each query takes.

Each query runs once cold, then --runs more times; result caches are off on
every platform, so warm runs measure the engine, not a cache. Timings are the
server's elapsed time where the platform reports it (Snowflake and
Databricks), otherwise client wall-clock seconds including fetching the
(small) results; run the client in the same region as the server.

    python bench/run.py --platform igloo --size c7g.4xlarge --scale 10
    python bench/run.py --platform snowflake --size M --scale 10
    python bench/run.py --platform databricks --size S --scale 10

Connection settings come from the environment; see bench/README.md.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import pathlib
import time

QUERIES = pathlib.Path(__file__).parent / "queries"

# sqlglot dialect names.
DIALECTS = {"igloo": "duckdb", "snowflake": "snowflake", "databricks": "databricks"}


def connect(platform: str):
    """Returns a DB-API connection and the statements that set up a session."""
    env = os.environ
    if platform == "igloo":
        import psycopg

        return psycopg.connect(env["IGLOO_DSN"], autocommit=True), []
    if platform == "snowflake":
        import snowflake.connector

        kwargs = {
            "account": env["SNOWFLAKE_ACCOUNT"],
            "user": env["SNOWFLAKE_USER"],
            "warehouse": env["SNOWFLAKE_WAREHOUSE"],
            "database": env.get("SNOWFLAKE_DATABASE", "SNOWFLAKE_SAMPLE_DATA"),
            "schema": env["SNOWFLAKE_SCHEMA"],
            "session_parameters": {"QUERY_TAG": "igloo-bench"},
        }
        if "SNOWFLAKE_ROLE" in env:
            kwargs["role"] = env["SNOWFLAKE_ROLE"]
        if "SNOWFLAKE_TOKEN_FILE" in env:
            kwargs["authenticator"] = "PROGRAMMATIC_ACCESS_TOKEN"
            kwargs["token"] = pathlib.Path(env["SNOWFLAKE_TOKEN_FILE"]).read_text().strip()
        elif "SNOWFLAKE_PRIVATE_KEY_FILE" in env:
            kwargs["private_key_file"] = env["SNOWFLAKE_PRIVATE_KEY_FILE"]
        else:
            kwargs["password"] = env["SNOWFLAKE_PASSWORD"]
        return snowflake.connector.connect(**kwargs), ["ALTER SESSION SET USE_CACHED_RESULT = FALSE"]
    if platform == "databricks":
        from databricks import sql
        from databricks.sdk.core import Config

        # The SDK's usual settings: DATABRICKS_HOST with DATABRICKS_TOKEN or
        # DATABRICKS_CLIENT_ID/SECRET, or a `databricks auth login` profile
        # named by DATABRICKS_CONFIG_PROFILE.
        cfg = Config()
        conn = sql.connect(
            server_hostname=cfg.host.removeprefix("https://"),
            http_path=env["DATABRICKS_HTTP_PATH"],
            credentials_provider=lambda: cfg.authenticate,
        )
        setup = ["SET use_cached_result = false"]
        if "DATABRICKS_CATALOG" in env:
            setup.append(f"USE CATALOG {env['DATABRICKS_CATALOG']}")
        if "DATABRICKS_SCHEMA" in env:
            setup.append(f"USE SCHEMA {env['DATABRICKS_SCHEMA']}")
        return conn, setup
    raise SystemExit(f"unknown platform {platform}")


def load_queries(suite: str, platform: str) -> list[tuple[str, str]]:
    """Returns (name, SQL) pairs, translated from DuckDB SQL for platform."""
    queries = []
    for path in sorted((QUERIES / suite).glob("*.sql")):
        text = path.read_text().strip().rstrip(";")
        if platform != "igloo":
            import sqlglot

            text = sqlglot.transpile(text, read="duckdb", write=DIALECTS[platform])[0]
        queries.append((path.stem, text))
    return queries


def run_query(conn, sql: str) -> tuple[float, int, str | None]:
    """Runs sql; returns client seconds, rows and the server's query id."""
    cur = conn.cursor()
    start = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    elapsed = time.perf_counter() - start
    query_id = getattr(cur, "sfqid", None) or getattr(cur, "query_id", None)
    cur.close()
    return elapsed, len(rows), query_id


def server_seconds(conn, platform: str, query_ids: list[str]) -> dict[str, float]:
    """Server-side elapsed seconds by query id, where the platform reports
    them. They leave out the network between this client and the server."""
    if not query_ids:
        return {}
    if platform == "databricks":
        return databricks_seconds(query_ids)
    if platform != "snowflake":
        return {}
    cur = conn.cursor()
    cur.execute(
        "SELECT query_id, total_elapsed_time FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 10000))"
    )
    times = {qid: ms / 1000 for qid, ms in cur.fetchall()}
    cur.close()
    return {qid: times[qid] for qid in query_ids if qid in times}


def databricks_seconds(query_ids: list[str]) -> dict[str, float]:
    """Statement durations from Databricks query history, which lags a
    little behind execution."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import QueryFilter

    w = WorkspaceClient()
    times: dict[str, float] = {}
    for _ in range(20):
        missing = [i for i in query_ids if i not in times]
        for start in range(0, len(missing), 50):
            res = w.query_history.list(filter_by=QueryFilter(statement_ids=missing[start:start + 50]), max_results=100)
            for q in res.res or []:
                if q.is_final and q.duration is not None:
                    times[q.query_id] = q.duration / 1000
        if len(times) == len(query_ids):
            break
        time.sleep(10)
    return times


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--platform", required=True, choices=sorted(DIALECTS))
    p.add_argument("--size", required=True, help="warehouse size (XS, S, M, ...) or EC2 instance type")
    p.add_argument("--suite", default="tpch", help="directory under bench/queries")
    p.add_argument("--scale", type=float, required=True, help="data scale factor, for the report")
    p.add_argument("--runs", type=int, default=3, help="warm runs after the cold one")
    p.add_argument("--out", type=pathlib.Path, help="results file (default bench/results/<platform>-<size>-sf<scale>.json)")
    p.add_argument("--storage", help="Igloo only: where table data lives, e.g. S3 or local disk, for the report")
    p.add_argument("--scale-to-zero", action="store_true",
                   help="Igloo only: the client connects through the scale-to-zero gateway to a stopped machine, "
                        "so the connect time is the time to resume")
    args = p.parse_args()

    # Through Igloo's scale-to-zero gateway, connecting to a stopped
    # machine includes starting it: this is the resume time.
    start = time.perf_counter()
    conn, setup = connect(args.platform)
    cur = conn.cursor()
    for stmt in setup:
        cur.execute(stmt)
    cur.close()
    connect_seconds = time.perf_counter() - start
    print(f"{args.platform}: connected in {connect_seconds:.2f}s", flush=True)

    results = []
    for name, sql in load_queries(args.suite, args.platform):
        entry = {"query": name, "cold": None, "warm": [], "rows": None, "error": None, "ids": []}
        try:
            entry["cold"], entry["rows"], qid = run_query(conn, sql)
            entry["ids"].append(qid)
            for _ in range(args.runs):
                elapsed, _, qid = run_query(conn, sql)
                entry["warm"].append(elapsed)
                entry["ids"].append(qid)
        except Exception as exc:  # noqa: BLE001 - record and go on to the next query
            entry["error"] = f"{type(exc).__name__}: {exc}"
        results.append(entry)
        status = entry["error"] or f"cold {entry['cold']:.2f}s, warm {min(entry['warm'] or [0]):.2f}s"
        print(f"{args.platform} {name}: {status}", flush=True)

    # Prefer the server's timings where it reports them.
    try:
        server = server_seconds(conn, args.platform, [i for e in results for i in e["ids"] if i])
    except Exception as exc:  # noqa: BLE001 - client timings remain
        print(f"could not read server timings: {exc}", flush=True)
        server = {}
    for e in results:
        ids = e.pop("ids")
        if ids and all(i in server for i in ids):
            e["client_cold"], e["client_warm"] = e["cold"], e["warm"]
            e["cold"], e["warm"] = server[ids[0]], [server[i] for i in ids[1:]]
            e["timing"] = "server"
        else:
            e["timing"] = "client"
    conn.close()

    out = args.out or pathlib.Path(__file__).parent / "results" / f"{args.platform}-{args.size}-sf{args.scale:g}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({
        "platform": args.platform,
        "size": args.size,
        "suite": args.suite,
        "scale": args.scale,
        "runs": args.runs,
        "connect_seconds": connect_seconds,
        "scale_to_zero": args.scale_to_zero,
        "storage": args.storage,
        "started": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "queries": results,
    }, indent=2) + "\n")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
