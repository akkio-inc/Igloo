"""Runs the dbt project in scripts/clients/dbt against Igloo with the
standard dbt-postgres adapter: seeds, sources, views, tables, the merge,
delete+insert and append incremental strategies, a snapshot, tests,
persisted docs, docs generate and source freshness. It builds three times:
once on the first load, once more after the next day's data arrives, and
with --full-refresh, checking the models after each. Connection settings
come from the PG* environment variables; the user needs the writer role.
Run from scripts/e2e.sh."""

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
    check(query("SELECT count(*), count(DISTINCT conversion_id) FROM analytics.fct_conversions")[0] == (conversions, conversions),
          f"fct_conversions (delete+insert) has {conversions} conversions, each once")
    check(query("SELECT count(*) FROM analytics.fct_spend_log")[0][0] == spend_rows,
          f"fct_spend_log (append) has {spend_rows} rows")
    check(query("SELECT count(*) FROM analytics_snapshots.campaign_settings_snapshot")[0][0] == snapshot_rows,
          f"the snapshot has {snapshot_rows} versions")
    leftovers = query("""SELECT table_schema || '.' || table_name FROM information_schema.tables
        WHERE table_schema LIKE 'analytics%' AND (table_name LIKE '%dbt_tmp%' OR table_name LIKE '%dbt_backup%')""")
    check(leftovers == [], f"no intermediate relations left {leftovers}")


load("initial.sql")
dbt("build")
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
dbt("build")
check_models(days=248, conversions=420, spend_rows=249, snapshot_rows=9)
check(query("SELECT impressions FROM analytics.fct_campaign_daily WHERE campaign_id = 101 AND event_date = '2026-09-30'")[0][0] ==
      1000 + (101 * 37 + 29 * 101) % 5000 + 1500, "a late row is merged into a day already reported")
check(query("SELECT revenue FROM analytics.fct_conversions WHERE conversion_id = 'cv-1'")[0][0] == 0,
      "a restated conversion replaces the old one")
check(query("""SELECT string_agg(status, ',' ORDER BY dbt_valid_from), count(dbt_valid_to)
    FROM analytics_snapshots.campaign_settings_snapshot WHERE campaign_id = 102""") == [("active,paused", 1)],
      "the snapshot closed the old version and added the new one")
check(query("SELECT status FROM analytics.dim_campaigns WHERE campaign_id = 102") == [("paused",)],
      "dim_campaigns reads the snapshot's current version")

dbt("build", "--full-refresh")
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
