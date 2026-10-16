"""Writes two small Delta tables to the stack's object store, for the
reference Delta Sharing server to share with Igloo in scripts/sharing-e2e.sh.

TABLE_ROOT is an s3:// prefix; the S3_* variables reach the bundled store.
"""

import datetime
import os

import pyarrow as pa
from deltalake import write_deltalake

root = os.environ["TABLE_ROOT"].rstrip("/")
options = {
    "AWS_ENDPOINT_URL": os.environ["S3_ENDPOINT_URL"],
    "AWS_REGION": os.environ["S3_REGION"],
    "AWS_ACCESS_KEY_ID": os.environ["S3_ACCESS_KEY_ID"],
    "AWS_SECRET_ACCESS_KEY": os.environ["S3_SECRET_ACCESS_KEY"],
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

trips = pa.table({
    "id": pa.array(range(100), pa.int64()),
    "city": pa.array(["nyc", "sf", "la", "chi"] * 25, pa.string()),
    "fare": pa.array([i * 0.5 for i in range(100)], pa.float64()),
})
write_deltalake(root + "/trips", trips, storage_options=options)

# Partitioned: Delta keeps partition values out of the files, so Igloo must
# put them back.
viewing = pa.table({
    "day": pa.array([datetime.date(2026, 10, 1 + i % 3) for i in range(30)], pa.date32()),
    "network": pa.array(["abc", "cbs", "nbc"] * 10, pa.string()),
    "minutes": pa.array(range(30), pa.int32()),
})
write_deltalake(root + "/viewing", viewing, storage_options=options, partition_by=["day", "network"])
print("wrote", root + "/trips", "and", root + "/viewing")
