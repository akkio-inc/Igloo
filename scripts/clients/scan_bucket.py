"""Looks for strings in every object of an S3 bucket, to prove an erasure:
in the raw bytes, in the decompressed blocks of Avro files (Iceberg
manifests, whose column bounds can hold values), and in the values and
column statistics of Parquet files, whose pages are compressed. Compares
without regard to case. Run from scripts/e2e.sh with S3_ENDPOINT,
S3_BUCKET, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, NEEDLES (the strings,
one per line) and CONTROL (a string that must be found, so a scan that
reads nothing cannot pass). Prints what it finds and exits 1 if a needle
is anywhere."""

import io
import os
import sys
import zlib

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT"],
    aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
    region_name=os.environ.get("S3_REGION", "us-east-1"),
)
bucket = os.environ["S3_BUCKET"]
needles = [n.lower().encode() for n in os.environ["NEEDLES"].splitlines() if n.strip()]
control = os.environ["CONTROL"].lower().encode()


def varint(buf, pos):
    shift = result = 0
    while True:
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        shift += 7
        if not b & 0x80:
            return result, pos


def long(buf, pos):
    v, pos = varint(buf, pos)
    return (v >> 1) ^ -(v & 1), pos


def avro_blocks(data):
    """Yields an Avro object container file's blocks, decompressed."""
    pos, meta = 4, {}
    while True:
        n, pos = long(data, pos)
        if n == 0:
            break
        if n < 0:
            n = -n
            _, pos = long(data, pos)
        for _ in range(n):
            klen, pos = long(data, pos)
            key = data[pos : pos + klen].decode()
            pos += klen
            vlen, pos = long(data, pos)
            meta[key] = data[pos : pos + vlen]
            pos += vlen
    sync, pos = data[pos : pos + 16], pos + 16
    codec = meta.get("avro.codec", b"null").decode()
    while pos < len(data):
        _, pos = long(data, pos)
        size, pos = long(data, pos)
        block, pos = data[pos : pos + size], pos + size
        assert data[pos : pos + 16] == sync, "bad sync marker"
        pos += 16
        if codec == "deflate":
            yield zlib.decompress(block, -15)
        elif codec == "snappy":
            raw = block[:-4]
            length, _ = varint(raw, 0)
            yield pa.Codec("snappy").decompress(raw, decompressed_size=length).to_pybytes()
        elif codec == "zstandard":
            yield pa.Codec("zstd").decompress(block).to_pybytes()
        elif codec == "null":
            yield block
        else:
            raise ValueError("Avro codec " + codec)


def parquet_texts(data):
    """Yields a Parquet file's values and its column chunks' statistics."""
    f = pq.ParquetFile(io.BytesIO(data))
    for rg in range(f.metadata.num_row_groups):
        for c in range(f.metadata.num_columns):
            stats = f.metadata.row_group(rg).column(c).statistics
            if stats is not None and stats.has_min_max:
                yield repr(stats.min).encode() + b"\n" + repr(stats.max).encode()
    for column in f.read().columns:
        yield "\n".join(map(repr, column.to_pylist())).encode()


found, saw_control, objects = [], False, 0
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
    for obj in page.get("Contents", []):
        key = obj["Key"]
        data = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        objects += 1
        parts = [("raw", data)]
        if data.startswith(b"Obj\x01"):
            parts += [("decompressed", b) for b in avro_blocks(data)]
        elif data.startswith(b"PAR1"):
            parts += [("decoded", b) for b in parquet_texts(data)]
        for how, part in parts:
            lower = part.lower()
            saw_control = saw_control or control in lower
            for n in needles:
                if n in lower:
                    found.append(f"{key} ({how}): {n.decode()}")

for f in sorted(set(found)):
    print("FOUND", f)
print(f"scanned {objects} objects; control {'found' if saw_control else 'NOT found'}")
sys.exit(1 if found or not saw_control else 0)
