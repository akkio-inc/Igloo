"""Measures how fast a large query result reaches Python from Igloo: over
Arrow Flight SQL with the ADBC driver, and over the PostgreSQL protocol with
psycopg, into Python rows, a pandas DataFrame or a polars DataFrame. The
ways run interleaved, round after round, and each one's fastest round
counts, so a busy machine's swings affect them alike.

    pip install adbc-driver-flightsql pyarrow pandas polars 'psycopg[binary]'
    export IGLOO_PASSWORD=...
    python bench/extract.py --flight grpc://127.0.0.1:31337 \\
        --pg 'host=127.0.0.1 port=5433 user=igloo dbname=igloo' --rows 3000000

It prints rows per second for each way and the speedup over psycopg. Load
the data first with `make load-tpch` (6,001,215 lineitem rows).
"""

import argparse
import gc
import os
import time

import adbc_driver_flightsql.dbapi as flightsql
import pandas as pd
import polars as pl
import psycopg

parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
parser.add_argument("--flight", default="grpc://127.0.0.1:31337", help="Flight SQL URI")
parser.add_argument("--pg", default="host=127.0.0.1 port=5433 user=igloo dbname=igloo", help="libpq connection string")
parser.add_argument("--user", default="igloo")
parser.add_argument("--rows", type=int, default=0, help="rows of lineitem to fetch; 0 for all")
parser.add_argument("--rounds", type=int, default=3)
args = parser.parse_args()
password = os.environ["IGLOO_PASSWORD"]
query = "SELECT * FROM lineitem" + (f" LIMIT {args.rows}" if args.rows else "")

pg = psycopg.connect(args.pg, password=password)
flight = flightsql.connect(args.flight, db_kwargs={"username": args.user, "password": password})


def psycopg_rows():
    with pg.cursor() as cur:
        cur.execute(query)
        return len(cur.fetchall())


def psycopg_pandas():
    with pg.cursor() as cur:
        cur.execute(query)
        return len(pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description]))


def adbc_arrow():
    with flight.cursor() as cur:
        cur.execute(query)
        return cur.fetch_arrow_table().num_rows


def adbc_pandas():
    with flight.cursor() as cur:
        cur.execute(query)
        return len(cur.fetch_df())


def adbc_polars():
    return len(pl.read_database(query, flight))


ways = {
    "psycopg, Python rows": psycopg_rows,
    "psycopg, pandas": psycopg_pandas,
    "Flight SQL (ADBC), Arrow table": adbc_arrow,
    "Flight SQL (ADBC), pandas": adbc_pandas,
    "Flight SQL (ADBC), polars": adbc_polars,
}
best = dict.fromkeys(ways, float("inf"))
rows = None
for r in range(args.rounds):
    for name, fetch in ways.items():
        gc.collect()
        start = time.perf_counter()
        n = fetch()
        elapsed = time.perf_counter() - start
        if rows is None:
            rows = n
        assert n == rows, f"{name} returned {n} rows, not {rows}"
        best[name] = min(best[name], elapsed)
        print(f"round {r + 1}: {name}: {elapsed:.2f} s", flush=True)

print(f"\n{rows:,} rows of lineitem, best of {args.rounds} rounds\n")
print("| Client | Seconds | Rows per second | Speedup |")
print("|---|---:|---:|---:|")
for name, seconds in best.items():
    # DataFrames against psycopg's DataFrame, the rest against its rows.
    frame = "pandas" in name or "polars" in name
    baseline = best["psycopg, pandas" if frame else "psycopg, Python rows"]
    print(f"| {name} | {seconds:.2f} | {rows / seconds:,.0f} | {baseline / seconds:.1f}x |")
