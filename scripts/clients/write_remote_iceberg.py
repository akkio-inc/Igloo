"""Stands in for another team's Iceberg catalog, for scripts/e2e.sh: makes a
second warehouse in the stack's Lakekeeper, apart from Igloo's, and writes a
table to it with PyIceberg, which Igloo then reads through a connection of
TYPE iceberg.

Environment: LAKEKEEPER (its base URL), BUCKET, S3_ENDPOINT, S3_REGION,
S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY.
"""

import json
import os
import urllib.error
import urllib.request

import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog

base = os.environ["LAKEKEEPER"]
key, secret = os.environ["S3_ACCESS_KEY_ID"], os.environ["S3_SECRET_ACCESS_KEY"]
region, endpoint = os.environ["S3_REGION"], os.environ["S3_ENDPOINT"]

# A warehouse without STS, so the catalog hands out no storage
# credentials: the connection brings its own.
warehouse = {
    "warehouse-name": "partner",
    "storage-profile": {
        "type": "s3", "bucket": os.environ["BUCKET"], "key-prefix": "partner", "region": region,
        "path-style-access": True, "endpoint": endpoint, "flavor": "s3-compat", "sts-enabled": False,
    },
    "storage-credential": {"type": "s3", "credential-type": "access-key", "access-key-id": key, "secret-access-key": secret},
}
req = urllib.request.Request(base + "/management/v1/warehouse", data=json.dumps(warehouse).encode(),
                             headers={"Content-Type": "application/json"}, method="POST")
try:
    urllib.request.urlopen(req)
except urllib.error.HTTPError as e:
    if e.code != 409:
        raise

catalog = RestCatalog("partner", uri=base + "/catalog", warehouse="partner", **{
    "s3.endpoint": endpoint, "s3.region": region, "s3.access-key-id": key, "s3.secret-access-key": secret,
    "s3.path-style-access": "true", "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
})
catalog.create_namespace_if_not_exists("campaigns")
rows = pa.table({
    "id": pa.array(range(1, 1001), pa.int32()),
    "channel": pa.array(["search" if i % 4 == 0 else "social" for i in range(1, 1001)]),
    "spend": pa.array([float(i % 50) for i in range(1, 1001)]),
})
table = catalog.create_table_if_not_exists("campaigns.spend", schema=rows.schema)
table.append(rows)
catalog.create_table_if_not_exists("campaigns.private", schema=pa.schema([("id", pa.int32()), ("note", pa.string())])).append(
    pa.table({"id": pa.array([1], pa.int32()), "note": ["confidential"]}))
print("wrote", len(table.scan().to_arrow()), "rows at", table.location())
