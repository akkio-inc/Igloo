"""Checks Igloo's Iceberg REST catalog through PyIceberg: sign-in, grants,
reads and writes with vended credentials. Run from scripts/e2e.sh, which
creates the users and tables first:

  ice_events (id INTEGER, kind VARCHAR) with rows 1 and 2
  ice_private (id INTEGER)
  ivy   role none, SELECT on ice_events
  ian   role none, SELECT, INSERT, UPDATE, DELETE on ice_events
  walt  role writer
"""

import os

import pyarrow as pa
from pyarrow import fs
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import ForbiddenError, OAuthError
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType

URI = os.environ["ICEBERG_URI"]


def catalog(user, password):
    return RestCatalog("igloo", uri=URI, warehouse="igloo", credential=f"{user}:{password}")


def rows(table):
    return sorted(table.scan().to_arrow().to_pylist(), key=lambda r: r["id"])


def refused(fn, *errors):
    try:
        fn()
    except errors:
        return True
    return False


assert refused(lambda: catalog("ivy", "wrong-password").list_namespaces(), OAuthError)
print("  pyiceberg: a wrong password is refused")

ivy = catalog("ivy", "ivy-password-1")
tables = [name for _, name in ivy.list_tables("public")]
assert "ice_events" in tables and "ice_private" not in tables, tables
print("  pyiceberg: listings show only granted tables")

events = ivy.load_table("public.ice_events")
assert rows(events) == [{"id": 1, "kind": "signup"}, {"id": 2, "kind": "login"}], rows(events)
print("  pyiceberg: reads a granted table with vended credentials")

# The credentials are short-lived ones, never Igloo's own keys, and reach
# this table's files and nothing else.
props = events.io.properties
assert props["s3.session-token"] and props["s3.access-key-id"] != os.environ.get("IGLOO_ACCESS_KEY_ID"), props
endpoint = props["s3.endpoint"]
s3 = fs.S3FileSystem(
    access_key=props["s3.access-key-id"],
    secret_key=props["s3.secret-access-key"],
    session_token=props["s3.session-token"],
    region=props["s3.region"],
    endpoint_override=endpoint.split("://", 1)[1],
    scheme=endpoint.split("://", 1)[0],
)
location = events.metadata.location.removeprefix("s3://")
assert any(f.path.endswith(".parquet") for f in s3.get_file_info(fs.FileSelector(location, recursive=True)))
warehouse = location.rsplit("/", 2)[0]
assert refused(lambda: s3.get_file_info(fs.FileSelector(warehouse, recursive=True)), OSError)
assert refused(lambda: s3.open_output_stream(location + "/data/forged.parquet").close(), OSError)
print("  pyiceberg: vended credentials reach only the table's files, read-only")

assert refused(lambda: ivy.load_table("public.ice_private"), ForbiddenError)
print("  pyiceberg: a table without a grant is refused")

batch = pa.Table.from_pylist([{"id": 9, "kind": "nope"}], schema=events.schema().as_arrow())
assert refused(lambda: events.append(batch), OSError)
assert len(rows(ivy.load_table("public.ice_events"))) == 2
print("  pyiceberg: SELECT alone cannot write")

ian = catalog("ian", "ian-password-1")
table = ian.load_table("public.ice_events")
table.append(pa.Table.from_pylist([{"id": 3, "kind": "purchase"}], schema=table.schema().as_arrow()))
print("  pyiceberg: appends with granted INSERT, UPDATE and DELETE")

walt = catalog("walt", "walt-password-1")
created = walt.create_table(
    "public.ice_created",
    schema=Schema(NestedField(1, "id", IntegerType(), required=False), NestedField(2, "kind", StringType(), required=False)),
)
created.append(pa.Table.from_pylist([{"id": 1, "kind": "from-pyiceberg"}], schema=created.schema().as_arrow()))
print("  pyiceberg: a writer creates and fills a table")

# Staged, as CREATE TABLE AS: the commit restates the table's UUID and
# location, which Igloo leaves out, so it keeps the catalog's.
staged_schema = Schema(NestedField(1, "id", IntegerType(), required=False), NestedField(2, "kind", StringType(), required=False))
with walt.create_table_transaction("public.ice_staged", schema=staged_schema) as txn:
    txn.append(pa.Table.from_pylist([{"id": 1, "kind": "staged"}], schema=staged_schema.as_arrow()))
staged = walt.load_table("public.ice_staged")
assert rows(staged) == [{"id": 1, "kind": "staged"}], rows(staged)
assert staged.metadata.location.rstrip("/").endswith("/" + str(staged.metadata.table_uuid)), staged.metadata.location
print("  pyiceberg: a writer creates and fills a table in one transaction")
