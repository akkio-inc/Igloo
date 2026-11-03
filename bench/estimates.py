#!/usr/bin/env python3
"""Measure what statements hold against the working sets estimated for them.

Builds the corpus's tables at a scale on an Igloo replica, then runs each
statement alone, on the replica (distributed execution off), and writes a
JSON line for it: DuckDB's plan of it (EXPLAIN (FORMAT JSON)), which is what
the replica's estimate of its working set reads; DuckDB's profile of a run
(EXPLAIN (ANALYZE, FORMAT JSON)), with each operator's rows; and what DuckDB
held for it at its peak, sampled on four connections of its own as the server
samples it for the query history (the buffer manager's tags of statements'
memory, less what they held before it started); a statement of under two
seconds runs three times, and its peak is the most of them. A line per table gives
its rows and columns, which the estimate reads beside the plan.

internal/distributed's TestEstimateCorpus replays the estimate over the
lines in internal/distributed/testdata/estimates and reports how far off it
is, by operator.

    IGLOO_DSN=postgresql://igloo:...@localhost:5433/igloo \\
        python bench/estimates.py --suite tpch --scale 1 --setup --out tpch-1.jsonl

Suites: tpch (the 22 queries of queries/tpch), tpcds (TPC-DS's 99, which
this client's DuckDB generates, as Igloo does not load the extension),
reports (the report shapes of
queries/estimates/reports, over TPC-H), tiles (mixed.py's dashboards and
heavy reports, and queries/media), burst (queries/estimates/burst, synthetic
stand-ins for a dashboard burst's distinct counts, joins to audiences,
FLATTEN and windows). Scale 1 is TPC-H's scale factor 1, ten million
impressions and ten million events; each scale multiplies them.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import sys
import threading
import time

HERE = pathlib.Path(__file__).parent
QUERIES = HERE / "queries"

# The tags of DuckDB's memory the server's sampling reads for statements'
# peaks (internal/engine/meter.go, queryMemory).
STATEMENT_MEMORY = (
    "SELECT coalesce(sum(memory_usage_bytes), 0)::BIGINT FROM duckdb_memory() WHERE tag IN ('HASH_TABLE', 'ORDER_BY', "
    "'WINDOW', 'COLUMN_DATA', 'ALLOCATOR', 'OVERFLOW_STRINGS', 'PARQUET_READER', 'CSV_READER', 'TRANSACTION')"
)
# Samplers read at once; a statement shorter than SHORT runs up to RUNS
# times, its peak the most of them.
SAMPLERS = 4
SHORT = 2.0
RUNS = 3


def statements(path: pathlib.Path) -> list[tuple[str, str]]:
    """The statements of a directory's .sql files, one a file, by name."""
    out = []
    for f in sorted(path.glob("*.sql")):
        text = "\n".join(line for line in f.read_text().splitlines() if not line.lstrip().startswith("--"))
        out.append((f.stem, text.strip().rstrip(";")))
    return out


def tpch_setup(schema: str, scale: float) -> list[str]:
    tables = ["lineitem", "orders", "customer", "part", "partsupp", "supplier", "nation", "region"]
    out = [f"CREATE SCHEMA IF NOT EXISTS {schema}", "ATTACH ':memory:' AS tpch_gen",
           f"CALL dbgen(sf = {scale}, catalog = 'tpch_gen', schema = 'main')"]
    for t in tables:
        out += [f"DROP TABLE IF EXISTS {schema}.{t}", f"CREATE TABLE {schema}.{t} AS SELECT * FROM tpch_gen.main.{t}"]
    return out + ["DETACH tpch_gen"]


def burst_setup(schema: str, scale: float) -> list[str]:
    """People, the audiences they are members of (audience k holds about
    1/(k+2) of them, in up to three versions), and their events, each with a
    list of three tags."""
    people, events = int(1_000_000 * scale), int(10_000_000 * scale)
    s = schema
    out = [f"CREATE SCHEMA IF NOT EXISTS {s}"]
    for t in ("people", "memberships", "audience_meta", "events"):
        out.append(f"DROP TABLE IF EXISTS {s}.{t}")
    out += [
        f"CREATE TABLE {s}.people AS SELECT i AS person_id, CAST(18 + hash(i * 3) % 60 AS INTEGER) AS age, "
        f"['north', 'south', 'east', 'west', 'central'][CAST(1 + hash(i * 3 + 1) % 5 AS INTEGER)] AS region, "
        f"'zip-' || (hash(i * 3 + 2) % 30000) AS zip FROM range({people}) t(i)",
        f"CREATE TABLE {s}.audience_meta AS SELECT 'aud-' || k AS audience_id, v AS ver, 'audience ' || k AS name, v = 3 AS latest "
        f"FROM range(200) a(k), range(1, 4) b(v)",
        # From people, whose scan runs on every thread, as range() does not.
        f"CREATE TABLE {s}.memberships AS SELECT 'aud-' || k AS audience_id, p.person_id, CAST(1 + hash(p.person_id * 7 + k) % 3 AS INTEGER) AS ver "
        f"FROM {s}.people p, range(200) a(k) WHERE hash(p.person_id * 211 + k) % (k + 2) = 0",
        f"CREATE TABLE {s}.events AS SELECT CAST(hash(i * 5) % {people} AS BIGINT) AS person_id, "
        f"TIMESTAMP '2026-07-01' + to_seconds(CAST(hash(i * 5 + 1) % 7948800 AS BIGINT)) AS ts, "
        f"['view', 'click', 'search', 'purchase', 'share'][CAST(1 + hash(i * 5 + 2) % 5 AS INTEGER)] AS event_type, "
        f"'site-' || (hash(i * 5 + 3) % 500) AS site, (hash(i * 5 + 4) % 10000) / 100.0 AS value, "
        f"['tag-' || (hash(i * 7) % 50), 'tag-' || (hash(i * 7 + 1) % 200), 'tag-' || (hash(i * 7 + 2) % 1000)] AS tags "
        f"FROM range({events}) t(i)",
    ]
    return out


def operators_setup(schema: str, scale: float) -> list[str]:
    """facts: ten million rows a scale, with keys of every value (id), a
    tenth of the values (k1, s1), 1,000 (k2, s2) and 10 (k3), and a list of
    three tags; facts_copy, the same rows; dims, a row for each value of k1;
    lists, five rows for each."""
    n = int(10_000_000 * scale)
    s = schema
    out = [f"CREATE SCHEMA IF NOT EXISTS {s}"]
    for t in ("facts", "facts_copy", "dims", "lists"):
        out.append(f"DROP TABLE IF EXISTS {s}.{t}")
    return out + [
        f"CREATE TABLE {s}.facts AS SELECT i AS id, CAST(hash(i) % {n // 10} AS BIGINT) AS k1, CAST(hash(i * 3) % 1000 AS BIGINT) AS k2, "
        f"CAST(hash(i * 5) % 10 AS BIGINT) AS k3, 'value-' || (hash(i) % {n // 10}) AS s1, 'name-' || (hash(i * 3) % 1000) AS s2, "
        f"(hash(i * 7) % 1000000) / 100.0 AS v, ['t' || (hash(i * 11) % 20), 't' || (hash(i * 13) % 100), 't' || (hash(i * 17) % 500)] AS tags "
        f"FROM range({n}) t(i)",
        f"CREATE TABLE {s}.facts_copy AS SELECT * FROM {s}.facts",
        f"CREATE TABLE {s}.dims AS SELECT i AS k, 'dim-' || i AS name, CAST(i % 50 AS INTEGER) AS cat FROM range({n // 10}) t(i)",
        f"CREATE TABLE {s}.lists AS SELECT i AS k, j AS position FROM range({n // 10}) t(i), range(5) u(j)",
    ]


def suite(name: str, scale: float) -> tuple[str, list[str], list[tuple[str, str]]]:
    """A suite's schema, the statements that build its tables, and its
    statements by name."""
    tag = str(scale).replace(".", "_")
    if name in ("tpch", "reports"):
        schema = f"est_tpch_{tag}"
        stmts = statements(QUERIES / "tpch") if name == "tpch" else statements(QUERIES / "estimates" / "reports")
        return schema, tpch_setup(schema, scale), stmts
    if name == "operators":
        schema = f"est_operators_{tag}"
        return schema, operators_setup(schema, scale), statements(QUERIES / "estimates" / "operators")
    if name == "burst":
        schema = f"est_burst_{tag}"
        return schema, burst_setup(schema, scale), statements(QUERIES / "estimates" / "burst")
    if name == "tiles":
        sys.path.insert(0, str(HERE))
        import mixed

        schema = f"est_tiles_{tag}"
        stmts = []
        for dash, tiles in mixed.DASHBOARDS.items():
            for tile, text in tiles.items():
                for k, (lo, hi) in enumerate(mixed.RANGES):
                    stmts.append((f"{dash}_{tile}_{k}", text.format(s=schema, adv="advertiser_7", lo=lo, hi=hi)))
        for heavy, text in mixed.HEAVY.items():
            stmts.append((f"heavy_{heavy}", text.format(s=schema)))
        stmts += statements(QUERIES / "media")
        return schema, mixed.setup_statements(schema, int(10_000_000 * scale)), stmts
    if name == "tpcds":
        import duckdb

        schema = f"est_tpcds_{tag}"
        db = duckdb.connect()
        db.execute("INSTALL tpcds")
        db.execute("LOAD tpcds")
        stmts = [(f"q{n:02d}", q.strip().rstrip(";")) for n, q in db.execute("SELECT query_nr, query FROM tpcds_queries()").fetchall()]
        return schema, [], stmts
    raise SystemExit(f"unknown suite {name}")


def tpcds_load(cur, schema: str, scale: float, scratch: pathlib.Path) -> None:
    """Generates TPC-DS at scale with this client's DuckDB, whose extension
    Igloo does not load, and copies each table into schema through COPY ...
    FROM STDIN."""
    import duckdb

    db = duckdb.connect()
    db.execute("INSTALL tpcds")
    db.execute("LOAD tpcds")
    db.execute(f"CALL dsdgen(sf = {scale})")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    scratch.mkdir(parents=True, exist_ok=True)
    for (t,) in db.execute("SELECT table_name FROM duckdb_tables() ORDER BY 1").fetchall():
        t0 = time.time()
        cols = db.execute(f"SELECT column_name, data_type FROM duckdb_columns() WHERE table_name = '{t}' ORDER BY column_index").fetchall()
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{t}")
        cur.execute(f"CREATE TABLE {schema}.{t} ({', '.join(f'{n} {ty}' for n, ty in cols)})")
        path = scratch / f"{t}.csv"
        db.execute(f"COPY {t} TO '{path}' (HEADER false)")
        with cur.copy(f"COPY {schema}.{t} FROM STDIN (FORMAT csv)") as cp, open(path, "rb") as f:
            while chunk := f.read(1 << 20):
                cp.write(chunk)
        path.unlink()
        print(f"  {time.time() - t0:6.1f} s  {schema}.{t}", file=sys.stderr)


def operators(profile: dict) -> list[dict]:
    """A profile's operators, as the plan's tree: each operator's name, the
    rows it made and DuckDB's estimate of them, and its children, from below
    the EXPLAIN_ANALYZE operator at its root."""

    def node(n: dict) -> dict:
        est = (n.get("extra_info") or {}).get("Estimated Cardinality")
        out = {"name": n.get("operator_name"), "rows": n.get("operator_cardinality")}
        if est is not None:
            out["est"] = float(str(est).lstrip("~"))
        kids = [node(c) for c in n.get("children", [])]
        if kids:
            out["children"] = kids
        return out

    roots = profile.get("children", [])
    while len(roots) == 1 and roots[0].get("operator_type") == "EXPLAIN_ANALYZE":
        roots = roots[0].get("children", [])
    return [node(r) for r in roots]


def compact(rec: dict) -> dict:
    """rec with its profile cut to its operators' rows (see operators)."""
    if "profile" in rec:
        rec = dict(rec)
        rec["operators"] = operators(rec.pop("profile"))
    return rec


# Types whose bounds, as Iceberg's manifests keep them, bound how many
# distinct values a column has.
BOUNDED = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "DATE")


def table_stats(cur, table: str, cols: list[list[str]]) -> dict:
    """What a table's columns' statistics would be: their distinct counts,
    as ANALYZE takes them (approx_count_distinct), and the least and most
    values of those of integer types, dates (as days) and timestamps (as
    microseconds), as Iceberg's
    manifests keep them. Columns of nested types have neither."""
    exprs, names = [], []
    for name, ty in cols:
        if any(x in ty.upper() for x in ("[]", "STRUCT", "MAP", "UNION")):
            continue
        exprs.append(f'approx_count_distinct("{name}")')
        names.append(("distinct", name))
        if ty.upper() in BOUNDED or ty.upper().startswith("TIMESTAMP"):
            v = f'"{name}"'
            if ty.upper() == "DATE":
                v = f'"{name}" - DATE \'1970-01-01\''
            elif ty.upper().startswith("TIMESTAMP"):
                v = f'epoch_us("{name}")'
            exprs += [f"min({v})::DOUBLE", f"max({v})::DOUBLE"]
            names += [("min", name), ("max", name)]
    if not exprs:
        return {}
    cur.execute(f"SELECT {', '.join(exprs)} FROM {table}")
    row = cur.fetchone()
    distinct, bounds = {}, {}
    for (kind, name), v in zip(names, row):
        if v is None:
            continue
        if kind == "distinct":
            distinct[name.lower()] = int(v)
        else:
            bounds.setdefault(name.lower(), [None, None])[0 if kind == "min" else 1] = float(v)
    return {"distinct": distinct, "bounds": {k: b for k, b in bounds.items() if None not in b}}


def tables_of(text: str, tables: list[str]) -> list[str]:
    """The tables of the suite's that a statement names."""
    words = set(re.findall(r"[a-z_][a-z0-9_]*", text.lower()))
    return [t for t in tables if t in words]


def read_memory(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(STATEMENT_MEMORY)
        return int(cur.fetchone()[0])


class Sampler:
    """Reads what DuckDB holds for statements on several connections at
    once, each as often as a read through Igloo allows (a few milliseconds),
    keeping the most."""

    def __init__(self, conns):
        self.conns, self.peak, self.samples, self.done = conns, 0, 0, threading.Event()
        self.lock = threading.Lock()
        self.threads = [threading.Thread(target=self.run, args=(c,), daemon=True) for c in conns]
        for t in self.threads:
            t.start()

    def run(self, conn):
        while not self.done.is_set():
            v = read_memory(conn)
            with self.lock:
                self.peak, self.samples = max(self.peak, v), self.samples + 1

    def stop(self) -> int:
        self.done.set()
        for t in self.threads:
            t.join()
        return max(self.peak, read_memory(self.conns[0]))


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--suite", required=True, choices=["tpch", "tpcds", "reports", "tiles", "burst", "operators", "file"])
    p.add_argument("--statements", help="for --suite file: a file of JSON lines, each a statement's name and sql")
    p.add_argument("--schema", help="for --suite file: the schema whose tables the statements read")
    p.add_argument("--scale", type=float, default=1)
    p.add_argument("--setup", action="store_true", help="build the suite's tables first")
    p.add_argument("--stats", action="store_true", help="record each table's columns' distinct counts and bounds")
    p.add_argument("--stats-only", action="store_true", help="write the tables' lines, with --stats, and run no statement")
    p.add_argument("--analyze", action="store_true",
                   help="ANALYZE the suite's tables first, so DuckDB plans with their distinct counts; the lines' suite is SUITE+analyzed")
    p.add_argument("--only", help="run only the statements whose names match this regular expression")
    p.add_argument("--timeout", default="900s", help="statement_timeout of each run")
    p.add_argument("--out", required=True)
    args = p.parse_args(argv)

    import psycopg

    dsn = os.environ["IGLOO_DSN"]
    conn = psycopg.connect(dsn, autocommit=True)
    sampler_conns = [psycopg.connect(dsn, autocommit=True) for _ in range(SAMPLERS)]
    if args.suite == "file":
        schema, setup = args.schema, []
        stmts = [(d["name"], d["sql"]) for d in map(json.loads, open(args.statements))]
    else:
        schema, setup, stmts = suite(args.suite, args.scale)
    with conn.cursor() as cur:
        cur.execute("SET distributed_execution = 'off'")
        cur.execute("SET igloo_result_cache = off")
        cur.execute(f"SET statement_timeout = '{args.timeout}'")
        if args.setup and args.suite == "tpcds":
            tpcds_load(cur, schema, args.scale, pathlib.Path(args.out).parent / "tpcds-scratch")
        if args.setup:
            for s in setup:
                t0 = time.time()
                cur.execute(s)
                print(f"  {time.time() - t0:6.1f} s  {s[:100]}", file=sys.stderr)
        cur.execute(f"SET search_path = '{schema}'")
        cur.execute("SELECT current_setting('threads')")
        threads = int(cur.fetchone()[0])
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_type = 'BASE TABLE'", (schema,))
        tables = sorted(r[0] for r in cur.fetchall())
    out = open(args.out, "a")
    meta = {"suite": args.suite + ("+analyzed" if args.analyze else ""), "scale": args.scale}
    if args.analyze:
        with conn.cursor() as cur:
            for t in tables:
                cur.execute(f"ANALYZE lake.{schema}.{t}")
    with conn.cursor() as cur:
        for t in tables:
            cur.execute(f"SELECT count(*) FROM {schema}.{t}")
            rows = cur.fetchone()[0]
            cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = %s AND table_name = %s "
                        "ORDER BY ordinal_position", (schema, t))
            cols = [[n, ty] for n, ty in cur.fetchall()]
            rec = {**meta, "kind": "table", "name": t, "rows": rows, "columns": cols}
            if args.stats:
                rec.update(table_stats(cur, f"{schema}.{t}", cols))
            out.write(json.dumps(rec) + "\n")
    out.flush()
    if args.stats_only:
        return
    only = re.compile(args.only) if args.only else None
    for name, text in stmts:
        if only and not only.search(name):
            continue
        rec = {**meta, "kind": "statement", "name": name, "query": text, "tables": tables if args.suite == "file" else tables_of(text, tables),
               "threads": threads}
        with conn.cursor() as cur:
            try:
                cur.execute("EXPLAIN (FORMAT JSON) " + text)
                rec["plan"] = json.loads(cur.fetchall()[-1][1])
                peaks, samples = [], 0
                for _ in range(RUNS):
                    before = read_memory(conn)
                    s = Sampler(sampler_conns)
                    t0 = time.time()
                    cur.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + text)
                    profile = json.loads(cur.fetchall()[-1][1])
                    seconds = time.time() - t0
                    peaks.append(max(s.stop() - before, 0))
                    samples += s.samples
                    rec.setdefault("seconds", round(seconds, 3))
                    if seconds >= SHORT:
                        break
                rec.update(before=before, peak=max(peaks), peaks=peaks, samples=samples,
                           allocated=profile.get("total_memory_allocated"), spilled=profile.get("system_peak_temp_dir_size"),
                           operators=operators(profile))
            except Exception as e:  # noqa: BLE001 - recorded, and the corpus goes on
                rec["error"] = str(e).splitlines()[0][:500]
        out.write(json.dumps(rec) + "\n")
        out.flush()
        print(f"{args.suite} {args.scale:g} {name:40} {rec.get('seconds', 0):7.1f} s  peak {rec.get('peak', 0) / 2**20:8.0f} MiB  "
              f"{' '.join(f'{p / 2**20:.0f}' for p in rec.get('peaks', []))} ({rec.get('samples', 0)} samples)  {rec.get('error', '')[:120]}",
              file=sys.stderr)


if __name__ == "__main__":
    main()
