"""Reads a share from Igloo with the official Delta Sharing Python client.

Run by scripts/sharing-e2e.sh. PROFILE is the recipient's profile file; the
share "clients" holds public.campaigns, written by the script with two rows
and shared WITH HISTORY.
"""

import os
import sys
from decimal import Decimal

import delta_sharing

profile = os.environ["PROFILE"]
url = profile + "#clients.public.campaigns"
failures = 0


def check(name, ok, detail=""):
    global failures
    print(("  ok    " if ok else "  FAIL  ") + name + ("" if ok else ": " + str(detail)))
    if not ok:
        failures += 1


client = delta_sharing.SharingClient(profile)
shares = [s.name for s in client.list_shares()]
check("list shares", shares == ["clients"], shares)
tables = [(t.share, t.schema, t.name) for t in client.list_all_tables()]
check("list tables", ("clients", "public", "campaigns") in tables, tables)

df = delta_sharing.load_as_pandas(url).sort_values("id").reset_index(drop=True)
check("load as pandas", list(df["id"]) == [1, 2] and list(df["client"]) == ["acme", "globex"], df)
check("decimals", df["spend"][0] == Decimal("100.50"), df["spend"].tolist())
check("dates and timestamps", str(df["day"][0]) == "2026-10-01" and str(df["ts"][1]).startswith("2026-10-02 11:00:00"),
      df[["day", "ts"]].to_dict())
check("nested columns", list(df["tags"][0]) == ["x", "y"] and df["info"][0]["b"] == "q", df[["tags", "info"]].to_dict())

limited = delta_sharing.load_as_pandas(url, limit=1)
check("limit", len(limited) == 1, len(limited))

version = delta_sharing.get_table_version(url)
check("table version", isinstance(version, int) and version >= 1, version)
metadata = delta_sharing.get_table_metadata(url)
check("metadata", '"name":"spend","type":"decimal(12,2)"' in metadata.schema_string, metadata.schema_string)

# The script inserts a third row after reading the first version.
expected_rows = int(os.environ.get("EXPECTED_ROWS", "2"))
at_version = delta_sharing.load_as_pandas(url, version=int(os.environ.get("OLD_VERSION", version)))
check("time travel to a version", len(at_version) == expected_rows, len(at_version))

sys.exit(1 if failures else 0)
