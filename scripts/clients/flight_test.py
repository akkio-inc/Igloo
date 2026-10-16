"""Checks Igloo's Arrow Flight SQL endpoint through the ADBC Flight SQL
driver (with pandas and polars) and through pyarrow.flight directly.
Expects the TPC-H tables at scale factor 0.1. Run from scripts/e2e.sh,
which sets FLIGHT_URI and the users' passwords."""

import os

import adbc_driver_flightsql.dbapi as flightsql
import adbc_driver_manager
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.flight as flight

uri = os.environ["FLIGHT_URI"]
admin = ("igloo", os.environ["IGLOO_ADMIN_PASSWORD"])
reader = ("rita", "rita-password-1")
lineitem_rows = 600572


def connect(user, password):
    return flightsql.connect(uri, db_kwargs={"username": user, "password": password})


with connect(*admin) as conn, conn.cursor() as cur:
    cur.execute("SELECT l_orderkey, l_extendedprice, l_shipdate, l_comment FROM lineitem")
    table = cur.fetch_arrow_table()
    assert table.num_rows == lineitem_rows, table.num_rows
    types = [f.type for f in table.schema]
    assert types == [pa.int64(), pa.decimal128(15, 2), pa.date32(), pa.string()], types
    print("  ADBC: Arrow table with DuckDB's types")

    cur.execute("SELECT l_returnflag, sum(l_quantity) AS qty FROM lineitem GROUP BY 1 ORDER BY 1")
    df = cur.fetch_df()
    assert isinstance(df, pd.DataFrame) and list(df["l_returnflag"]) == ["A", "N", "R"], df
    frame = pl.read_database("SELECT l_linestatus, count(*) AS n FROM lineitem GROUP BY 1 ORDER BY 1", conn)
    assert frame["n"].sum() == lineitem_rows, frame
    print("  ADBC: pandas and polars")

    # DDL and DML run when executed, even if their empty results are never
    # read: ADBC cancels an unread result when the cursor runs the next one.
    cur.execute("CREATE OR REPLACE TABLE flight_adbc AS SELECT * FROM region")
    cur.execute("INSERT INTO flight_adbc SELECT * FROM region WHERE r_regionkey < 2")
    assert cur.rowcount == 2, cur.rowcount
    cur.execute("SELECT count(*) FROM flight_adbc")
    assert cur.fetchone()[0] == 7
    print("  ADBC: DDL and DML")

    cur.execute("SELECT count(*) FROM lineitem WHERE l_orderkey <= ? AND l_shipmode = ?", parameters=(1000, "AIR"))
    want = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM lineitem WHERE l_orderkey <= 1000 AND l_shipmode = 'AIR'")
    assert cur.fetchone()[0] == want > 0, want
    print("  ADBC: parameters")

    objects = conn.adbc_get_objects(depth="columns", table_name_filter="lineitem").read_all().to_pylist()
    columns = [c["column_name"] for cat in objects for s in cat["catalog_db_schemas"] or []
               for t in s["db_schema_tables"] or [] for c in t["table_columns"] or []]
    assert "l_extendedprice" in columns and len(columns) == 16, columns
    assert conn.adbc_get_info()["vendor_name"] == "Igloo"
    print("  ADBC: catalog metadata")

with connect(*reader) as conn, conn.cursor() as cur:
    try:
        cur.execute("INSERT INTO e2e VALUES (9, 'nope', 1)")
        raise AssertionError("a reader inserted over Flight SQL")
    except adbc_driver_manager.Error as e:
        assert "SQLSTATE 42501" in str(e), e
    print("  ADBC: statements are authorized as SQL ones")

try:
    connect(reader[0], "not-the-password").close()
    raise AssertionError("a wrong password was accepted")
except adbc_driver_manager.Error as e:
    assert "password authentication failed" in str(e), e
print("  ADBC: wrong password is rejected")


def any_command(name, query):
    """A Flight SQL command, a protobuf Any, encoded by hand: pyarrow knows
    Flight but not Flight SQL's messages."""

    def field(number, data):
        length, size = bytearray(), len(data)
        while size > 0x7F:
            length.append(size & 0x7F | 0x80)
            size >>= 7
        length.append(size)
        return bytes([number << 3 | 2]) + bytes(length) + data

    url = b"type.googleapis.com/arrow.flight.protocol.sql." + name.encode()
    return field(1, url) + field(2, field(1, query.encode()))


client = flight.connect(uri)
options = flight.FlightCallOptions(headers=[client.authenticate_basic_token(*admin)])
info = client.get_flight_info(flight.FlightDescriptor.for_command(any_command(
    "CommandStatementQuery", "SELECT l_orderkey, l_partkey FROM lineitem WHERE l_orderkey < 100")), options)
table = client.do_get(info.endpoints[0].ticket, options).read_all()
assert table.num_rows > 0 and table.schema.names == ["l_orderkey", "l_partkey"], table.schema
client.close()
print("  pyarrow.flight: query")

# The client names Igloo as another machine: without TLS, Igloo refuses to
# sign it in.
remote = flight.connect(uri, generic_options=[("grpc.default_authority", "igloo.example.com:31337")])
try:
    remote.authenticate_basic_token(*admin)
    raise AssertionError("an unencrypted connection from afar was accepted")
except flight.FlightError as e:
    assert "only at localhost" in str(e), e
print("  pyarrow.flight: unencrypted connections from afar are refused")
