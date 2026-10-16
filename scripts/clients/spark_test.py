"""Checks Igloo's Iceberg REST catalog from Apache Spark with the Iceberg
runtime: reads, appends, MERGE and CREATE TABLE AS with vended credentials,
and refusals by Igloo's grants. Run from scripts/engines-e2e.sh, which
creates the users and tables first:

  engines.orders (id INTEGER, customer VARCHAR, amount DECIMAL(10,2)), rows 1 and 2
  engines.secret (id INTEGER)
  engines_out, an empty schema
  sam  role none, SELECT, INSERT, UPDATE, DELETE on engines.orders, ALL on schema engines_out
  rae  role none, SELECT on engines.orders
"""

import os
import sys

from pyspark.sql import SparkSession

URI = os.environ["ICEBERG_URI"]

builder = (
    SparkSession.builder.master("local[2]")
    .appName("igloo-engines-e2e")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.ui.enabled", "false")
)
# One catalog per Igloo user, configured as for any Iceberg REST catalog.
for name, user, password in [("igloo", "sam", "sam-password-1"), ("as_rae", "rae", "rae-password-1"), ("as_eve", "eve", "eve-password-1")]:
    prefix = f"spark.sql.catalog.{name}"
    builder = (
        builder.config(prefix, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{prefix}.type", "rest")
        .config(f"{prefix}.uri", URI)
        .config(f"{prefix}.warehouse", "igloo")
        .config(f"{prefix}.credential", f"{user}:{password}")
        .config(f"{prefix}.header.X-Iceberg-Access-Delegation", "vended-credentials")
    )
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def rows(sql):
    return [tuple(r) for r in spark.sql(sql).collect()]


def fails(sql, *needles):
    try:
        spark.sql(sql).collect()
    except Exception as e:  # noqa: BLE001 - Spark wraps Java exceptions
        text = str(e)
        if all(n in text for n in needles):
            return True
        print(f"unexpected error for {sql}: {text[:2000]}", file=sys.stderr)
        return False
    return False


got = rows("SELECT id, customer, CAST(amount AS STRING) FROM igloo.engines.orders ORDER BY id")
assert got == [(1, "acme", "10.00"), (2, "globex", "20.00")], got
print("  spark: reads an Igloo table")

assert rows("SHOW TABLES IN as_rae.engines") == [("engines", "orders", False)], rows("SHOW TABLES IN as_rae.engines")
print("  spark: lists only granted tables")

spark.sql("INSERT INTO igloo.engines.orders VALUES (3, 'initech', 30.00)")
print("  spark: appends")

spark.sql("""
    MERGE INTO igloo.engines.orders t
    USING (SELECT 2 AS id, 'globex' AS customer, CAST(25.00 AS DECIMAL(10,2)) AS amount
           UNION ALL SELECT 4, 'umbrella', CAST(40.00 AS DECIMAL(10,2))) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET amount = s.amount
    WHEN NOT MATCHED THEN INSERT *
""")
got = rows("SELECT id, CAST(amount AS STRING) FROM igloo.engines.orders ORDER BY id")
assert got == [(1, "10.00"), (2, "25.00"), (3, "30.00"), (4, "40.00")], got
print("  spark: MERGE INTO")

spark.sql("""
    CREATE TABLE igloo.engines_out.order_totals AS
    SELECT customer, sum(amount) AS total FROM igloo.engines.orders GROUP BY customer
""")
assert rows("SELECT count(*) FROM igloo.engines_out.order_totals") == [(4,)]
print("  spark: CREATE TABLE AS")

assert fails("SELECT * FROM igloo.engines.secret", "Forbidden", "permission denied for sam")
print("  spark: a table without a grant is refused")

assert rows("SELECT count(*) FROM as_rae.engines.orders") == [(4,)]
assert fails("INSERT INTO as_rae.engines.orders VALUES (9, 'nope', 1.00)", "403")
assert rows("SELECT count(*) FROM as_rae.engines.orders") == [(4,)]
print("  spark: SELECT alone reads but cannot write")

assert fails("SHOW NAMESPACES IN as_eve", "NotAuthorized")
print("  spark: a wrong password is refused")
