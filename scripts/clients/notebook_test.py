"""Checks Igloo from a notebook's tools: SQLAlchemy 2 with psycopg 3
(the inspector, as schema browsers and pandas use it), pandas read_sql and
to_sql, and polars through SQLAlchemy, connectorx and ADBC. Then times
pulling a large result each way and prints rows per second. Expects the
TPC-H tables; the user needs the writer role. Connection settings come from
the PG* environment variables. Run from scripts/e2e.sh."""

import datetime
import decimal
import os
import sys
import time

import pandas as pd
import polars as pl
import sqlalchemy as sa

url = sa.engine.URL.create("postgresql+psycopg", username=os.environ["PGUSER"], password=os.environ["PGPASSWORD"],
                           host=os.environ["PGHOST"], port=int(os.environ.get("PGPORT", "5432")), database="igloo")
uri = url.set(drivername="postgresql").render_as_string(hide_password=False)
engine = sa.create_engine(url)


def check(ok, what):
    if not ok:
        sys.exit(f"  notebook: FAILED {what}")
    print(f"  notebook: {what}")


with engine.begin() as conn:
    conn.execute(sa.text("CREATE SCHEMA IF NOT EXISTS notebook"))
    conn.execute(sa.text("DROP TABLE IF EXISTS notebook.placements"))
    conn.execute(sa.text("""CREATE TABLE notebook.placements (
        placement_id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, cpm DECIMAL(8,2), booked_on DATE)"""))
    conn.execute(sa.text("COMMENT ON TABLE notebook.placements IS 'Booked placements'"))
    conn.execute(sa.text("COMMENT ON COLUMN notebook.placements.cpm IS 'Cost per thousand impressions'"))
    conn.execute(sa.text("CREATE OR REPLACE VIEW notebook.cheap_placements AS SELECT * FROM notebook.placements WHERE cpm < 5"))

insp = sa.inspect(engine)
check({"notebook", "public"} <= set(insp.get_schema_names()), "inspector lists schemas")
check({"orders", "lineitem"} <= set(insp.get_table_names()), "inspector lists tables in the default schema")
check(insp.get_table_names(schema="notebook") == ["placements"] and insp.get_view_names(schema="notebook") == ["cheap_placements"],
      "inspector lists tables and views")
columns = {c["name"]: c for c in insp.get_columns("placements", schema="notebook")}
check(list(columns) == ["placement_id", "name", "cpm", "booked_on"]
      and isinstance(columns["cpm"]["type"], sa.Numeric) and (columns["cpm"]["type"].precision, columns["cpm"]["type"].scale) == (8, 2)
      and isinstance(columns["booked_on"]["type"], sa.Date) and columns["cpm"]["comment"] == "Cost per thousand impressions",
      "inspector describes columns, with types and comments")
check(insp.get_table_comment("placements", schema="notebook")["text"] == "Booked placements", "inspector reads table comments")
check([c["name"] for c in insp.get_columns("cheap_placements", schema="notebook")] == list(columns), "inspector describes views")
check("FROM" in insp.get_view_definition("cheap_placements", schema="notebook"), "inspector reads view definitions")
# Keys are accepted but not enforced, and Iceberg has no indexes.
check(insp.get_pk_constraint("placements", schema="notebook")["constrained_columns"] == []
      and insp.get_foreign_keys("placements", schema="notebook") == [] and insp.get_indexes("placements", schema="notebook") == [],
      "inspector reads keys and indexes, of which Iceberg tables have none")
metadata = sa.MetaData()
metadata.reflect(engine, schema="notebook", views=True)
check(set(metadata.tables) == {"notebook.placements", "notebook.cheap_placements"}, "MetaData.reflect")

placements = pd.DataFrame({
    "placement_id": range(1, 1001),
    "name": [f"placement {i}" for i in range(1, 1001)],
    "cpm": [decimal.Decimal(i % 900) / 100 for i in range(1, 1001)],
    "booked_on": [datetime.date(2026, 1, 1) + datetime.timedelta(days=i % 300) for i in range(1, 1001)],
})
placements.to_sql("placements", engine, schema="notebook", if_exists="append", index=False, chunksize=500)
check(pd.read_sql("SELECT count(*) AS n FROM notebook.placements", engine)["n"][0] == 1000, "pandas to_sql appends rows")
pd.DataFrame({"a": [1, 2], "b": ["x", None]}).to_sql("scratch", engine, schema="notebook", if_exists="replace", index=False)
check(pd.read_sql("SELECT * FROM notebook.scratch ORDER BY a", engine).fillna("-").values.tolist() == [[1, "x"], [2, "-"]],
      "pandas to_sql creates and replaces tables")
df = pd.read_sql(sa.text("SELECT * FROM notebook.cheap_placements WHERE booked_on >= :since ORDER BY placement_id"),
                 engine, params={"since": datetime.date(2026, 6, 1)})
check(len(df) > 0 and (df["cpm"] < 5).all(), "pandas read_sql with parameters")

query = "SELECT o_orderkey, o_orderdate, o_totalprice, o_orderpriority FROM orders ORDER BY o_orderkey LIMIT 1000"
frames = {
    "SQLAlchemy": pl.read_database(query, engine),
    "connectorx": pl.read_database_uri(query, uri, engine="connectorx"),
    "ADBC": pl.read_database_uri(query, uri, engine="adbc"),
}
for name, frame in frames.items():
    check(frame.height == 1000 and frame.columns == ["o_orderkey", "o_orderdate", "o_totalprice", "o_orderpriority"]
          and frame["o_orderdate"].dtype == pl.Date, f"polars read_database through {name}")
# ADBC reads numeric as text, as it does from PostgreSQL.
cx, adbc = frames["connectorx"], frames["ADBC"]
check(cx.drop("o_totalprice").equals(adbc.drop("o_totalprice"))
      and [float(v) for v in cx["o_totalprice"]] == [float(v) for v in adbc["o_totalprice"]],
      "connectorx and ADBC read the same values")
pl.DataFrame({"a": [1, 2], "b": ["x", "y"]}).write_database("notebook.polars_frame", engine, if_table_exists="replace")
check(pl.read_database("SELECT sum(a) AS a FROM notebook.polars_frame", engine)["a"][0] == 3, "polars write_database")

# Pulling a large result: six columns of TPC-H lineitem, every row.
big = "SELECT l_orderkey, l_partkey, l_quantity, l_extendedprice, l_shipdate, l_shipmode FROM lineitem"
expected = pl.read_database_uri(f"SELECT count(*) AS n FROM ({big})", uri, engine="connectorx")["n"][0]
for name, read in [
    ("pandas read_sql (SQLAlchemy, psycopg)", lambda: len(pd.read_sql(big, engine))),
    ("polars read_database (SQLAlchemy, psycopg)", lambda: pl.read_database(big, engine).height),
    ("polars read_database_uri (connectorx)", lambda: pl.read_database_uri(big, uri, engine="connectorx").height),
    ("polars read_database_uri (ADBC)", lambda: pl.read_database_uri(big, uri, engine="adbc").height),
]:
    start = time.perf_counter()
    rows = read()
    seconds = time.perf_counter() - start
    check(rows == expected, f"{name}: {rows} rows in {seconds:.1f} s, {rows / seconds:,.0f} rows/s")
