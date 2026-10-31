"""Runs the dbt project in scripts/clients/dbt against Igloo with the
standard dbt-postgres adapter: seeds, sources, views, tables, the merge,
delete+insert, append and microbatch incremental strategies, a snapshot,
tests, persisted docs, docs generate and source freshness. It builds three
times: once on the first load, once more after the next day's data arrives,
and with --full-refresh, checking the models after each, and that the
incremental runs wrote only what changed: the second run's snapshots of
each incremental model wrote no more rows than it changed or added, and
rewrote none of the others. Connection settings come from the PG*
environment variables; the user needs the writer role. Run from
scripts/e2e.sh."""

import json
import os
import shutil
import subprocess
import sys
import tempfile

import psycopg2

here = os.path.dirname(os.path.abspath(__file__))
project = os.path.join(tempfile.mkdtemp(), "dbt")
shutil.copytree(os.path.join(here, "dbt"), project)
os.environ.update(DBT_PROFILES_DIR=project, DO_NOT_TRACK="1", DBT_SEND_ANONYMOUS_USAGE_STATS="false")

conn = psycopg2.connect(dbname="lake")
conn.autocommit = True


def query(sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def load(name):
    # One simple query with every statement of the file, as psycopg2 sends it.
    with open(os.path.join(project, "load", name)) as f, conn.cursor() as cur:
        cur.execute(f.read())


def dbt(*args):
    out = subprocess.run(["dbt", "--no-use-colors", *args], cwd=project, capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"dbt {' '.join(args)} failed:\n{out.stdout[-6000:]}{out.stderr[-2000:]}")
    print(f"  dbt: {' '.join(args)}")
    return out.stdout


def check(ok, what):
    if not ok:
        sys.exit(f"  dbt: FAILED {what}")
    print(f"  dbt: {what}")


def check_models(days, conversions, spend_rows, snapshot_rows):
    raw_spend = query("SELECT sum(spend) FROM raw.ad_platform_daily")[0][0]
    check(query("SELECT count(*), sum(spend) FROM analytics.fct_campaign_daily")[0] == (days, raw_spend),
          f"fct_campaign_daily (merge) has {days} days and every unit of spend")
    check(query("SELECT count(*), sum(spend) FROM analytics.fct_delivery_batches")[0] == (days, raw_spend),
          f"fct_delivery_batches (microbatch) has {days} days and every unit of spend")
    check(query("SELECT count(*), count(DISTINCT conversion_id) FROM analytics.fct_conversions")[0] == (conversions, conversions),
          f"fct_conversions (delete+insert) has {conversions} conversions, each once")
    check(query("SELECT count(*) FROM analytics.fct_spend_log")[0][0] == spend_rows,
          f"fct_spend_log (append) has {spend_rows} rows")
    check(query("SELECT count(*) FROM analytics_snapshots.campaign_settings_snapshot")[0][0] == snapshot_rows,
          f"the snapshot has {snapshot_rows} versions")
    leftovers = query("""SELECT table_schema || '.' || table_name FROM information_schema.tables
        WHERE table_schema LIKE 'analytics%' AND (table_name LIKE '%dbt_tmp%' OR table_name LIKE '%dbt_backup%')""")
    check(leftovers == [], f"no intermediate relations left {leftovers}")


# Each microbatch run builds the days it is given: the first 30 days, then
# the last reported day (late rows) and the new one.
first_days = ["--event-time-start", "2026-09-01", "--event-time-end", "2026-10-01"]
next_days = ["--event-time-start", "2026-09-30", "--event-time-end", "2026-10-02"]
# The rows of each incremental model the next day's run does not change:
# the days before it, and the rows of the first load (loaded five hours
# before the next day's), but the conversion it restates. The others are
# the most it may write.
first_load = "loaded_at < (SELECT min(loaded_at) + INTERVAL 2 HOUR FROM analytics.{})"
unchanged = {
    "fct_campaign_daily": "event_date < DATE '2026-09-30'",
    "fct_conversions": first_load.format("fct_conversions") + " AND conversion_id <> 'cv-1'",
    "fct_spend_log": first_load.format("fct_spend_log"),
    "fct_delivery_batches": "event_date < DATE '2026-09-30'",
}


def history(m):
    """Model m's snapshots, oldest first: sequence number, id, operation."""
    return sorted((int(seq), int(snap), op) for seq, snap, op in query(
        f"SELECT sequence_number, snapshot_id, operation FROM iceberg_snapshots(lake.analytics.{m})"))


def rows_written(m, since):
    """The rows of the data files that model m's snapshots after sequence
    number since wrote, but maintenance's compactions (replace), which
    may rewrite files at any time: a manifest's entries with its
    snapshot's sequence number are what that snapshot added or removed."""
    written = 0
    for seq, snap, op in history(m):
        if seq > since and op != "replace":
            written += int(query(f"""SELECT coalesce(sum(record_count), 0)
                FROM iceberg_metadata(lake.analytics.{m}, snapshot_from_id => {snap})
                WHERE manifest_sequence_number = {seq} AND manifest_content = 'DATA' AND status = 'ADDED'""")[0][0])
    return written


load("initial.sql")
dbt("build", *first_days)
check_models(days=240, conversions=400, spend_rows=240, snapshot_rows=8)
check(sorted(query("""SELECT table_name, table_type FROM information_schema.tables
    WHERE table_schema = 'analytics' AND table_name LIKE 'stg%'""")) ==
      [("stg_ad_platform_daily", "VIEW"), ("stg_campaigns", "VIEW"), ("stg_conversions", "VIEW")],
      "staging models are views")
check(query("""SELECT obj_description(c.oid, 'pg_class'), col_description(c.oid, a.attnum)
    FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid
    WHERE c.relname = 'fct_campaign_daily' AND a.attname = 'spend'""") ==
      [("Delivery and conversions per campaign and day.", "Media spend in the client's currency.")],
      "persist_docs comments on a table and its columns")
check(query("""SELECT obj_description(c.oid, 'pg_class'), col_description(c.oid, a.attnum)
    FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid
    WHERE c.relname = 'stg_ad_platform_daily' AND a.attname = 'campaign_id'""") ==
      [("One row per platform, campaign and day.", "Campaign the delivery belongs to.")],
      "persist_docs comments on a view and its columns")

load("next_day.sql")
before = {m: history(m)[-1] for m in unchanged}
dbt("build", *next_days)
check_models(days=248, conversions=420, spend_rows=249, snapshot_rows=9)
for m, w in unchanged.items():
    # Merge, delete+insert and microbatch delete the rows they replace
    # with delete files and write the new ones: no other row is
    # rewritten, and the table is the one the first run built.
    seq, snap, _ = before[m]
    written, most = rows_written(m, seq), query(f"SELECT count(*) FROM analytics.{m} WHERE NOT ({w})")[0][0]
    check(snap in {s for _, s, _ in history(m)} and 0 < written <= most,
          f"{m}'s incremental run wrote {written} rows, no more than it changed or added ({most}), into the same table")
check(query("SELECT impressions FROM analytics.fct_delivery_batches WHERE campaign_id = 101 AND event_date = '2026-09-30'")[0][0] ==
      1000 + (101 * 37 + 29 * 101) % 5000 + 1500, "a microbatch run merges a late row into its day")
check(query("SELECT impressions FROM analytics.fct_campaign_daily WHERE campaign_id = 101 AND event_date = '2026-09-30'")[0][0] ==
      1000 + (101 * 37 + 29 * 101) % 5000 + 1500, "a late row is merged into a day already reported")
check(query("SELECT revenue FROM analytics.fct_conversions WHERE conversion_id = 'cv-1'")[0][0] == 0,
      "a restated conversion replaces the old one")
check(query("""SELECT string_agg(status, ',' ORDER BY dbt_valid_from), count(dbt_valid_to)
    FROM analytics_snapshots.campaign_settings_snapshot WHERE campaign_id = 102""") == [("active,paused", 1)],
      "the snapshot closed the old version and added the new one")
check(query("SELECT status FROM analytics.dim_campaigns WHERE campaign_id = 102") == [("paused",)],
      "dim_campaigns reads the snapshot's current version")

dbt("build", "--full-refresh", "--event-time-start", "2026-09-01", "--event-time-end", "2026-10-02")
check_models(days=248, conversions=420, spend_rows=249, snapshot_rows=9)

out = dbt("show", "--select", "rpt_client_weekly", "--limit", "3")
check("Contoso Motors" in out or "Northwind Foods" in out or "Fabrikam Travel" in out, "dbt show previews a model")

dbt("docs", "generate")
with open(os.path.join(project, "target", "catalog.json")) as f:
    catalog = json.load(f)
node = catalog["nodes"]["model.agency_reporting.fct_campaign_daily"]
check(node["metadata"]["type"] == "BASE TABLE" and node["metadata"]["comment"] == "Delivery and conversions per campaign and day."
      and node["columns"]["spend"]["type"] == "numeric(38,2)" and len(node["columns"]) == 9,
      "docs generate describes tables, columns and comments")
check(catalog["nodes"]["model.agency_reporting.stg_campaigns"]["metadata"]["type"] == "VIEW", "docs generate describes views")

dbt("source", "freshness")
with open(os.path.join(project, "target", "sources.json")) as f:
    freshness = json.load(f)
check(sorted(r["status"] for r in freshness["results"]) == ["pass", "pass"], "source freshness")
