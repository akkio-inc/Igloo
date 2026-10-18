"""Deletes the objects of an S3 bucket under a prefix, as an admin does with
the bucket's own tools. Run from scripts/e2e.sh with S3_ENDPOINT, S3_BUCKET,
S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY and PREFIX. Prints how many objects
it deleted."""

import os

import boto3

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT"],
    aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
    region_name=os.environ.get("S3_REGION", "us-east-1"),
)
bucket = os.environ["S3_BUCKET"]
deleted = 0
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=os.environ["PREFIX"]):
    for obj in page.get("Contents", []):
        s3.delete_object(Bucket=bucket, Key=obj["Key"])
        deleted += 1
print(deleted)
