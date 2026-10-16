"""Checks Igloo through psycopg 3: parameters, server-side prepared
statements and transactions. Expects the TPC-H tables. Run from
scripts/e2e.sh."""

import datetime
import decimal

import psycopg

# Connection settings come from the PG* environment variables.
for label, kwargs in [("extended protocol", {}), ("prepared statements", {"prepare_threshold": 0})]:
    with psycopg.connect(**kwargs) as conn:
        row = conn.execute(
            "SELECT %s::INTEGER + 1, %s::VARCHAR || '!', %s::DATE, %s::DECIMAL(10,2) * 2",
            (41, "hi", datetime.date(2026, 10, 15), decimal.Decimal("1.25")),
        ).fetchone()
        assert row == (42, "hi!", datetime.date(2026, 10, 15), decimal.Decimal("2.50")), row
        assert conn.execute("SELECT r_name FROM region WHERE r_regionkey = %s", (2,)).fetchone() == ("ASIA",)
        print(f"  psycopg: {label}")

with psycopg.connect(autocommit=True) as conn:
    conn.execute("CREATE TABLE IF NOT EXISTS psycopg_tx (i INTEGER)")
with psycopg.connect() as conn:  # psycopg opens a transaction implicitly
    conn.execute("INSERT INTO psycopg_tx VALUES (1)")
    conn.rollback()
    conn.execute("INSERT INTO psycopg_tx VALUES (2)")
    conn.commit()
    got = conn.execute("SELECT count(*), max(i) FROM psycopg_tx").fetchone()
    assert got == (1, 2), got
    print("  psycopg: transactions")

# Server-side cursors, which Ibis, SQLAlchemy's stream_results and pandas'
# chunked reads use: DECLARE, a Describe of the cursor's portal, FETCH, CLOSE.
with psycopg.connect() as conn:
    with conn.cursor(name="lineitems") as cur:
        cur.execute("SELECT l_orderkey, l_quantity FROM lineitem WHERE l_orderkey < %s", (100,))
        assert [d.name for d in cur.description] == ["l_orderkey", "l_quantity"], cur.description
        first = cur.fetchmany(10)
        rest = cur.fetchall()
    expected = conn.execute("SELECT count(*) FROM lineitem WHERE l_orderkey < 100").fetchone()[0]
    assert len(first) == 10 and len(first) + len(rest) == expected, (len(first), len(rest), expected)
    # Any DDL makes psycopg send DEALLOCATE ALL before its next statement.
    conn.execute("CREATE TEMPORARY TABLE psycopg_tmp AS SELECT 1 AS x")
    assert conn.execute("SELECT x FROM psycopg_tmp").fetchone() == (1,)
    conn.commit()
    print("  psycopg: server-side cursors")
