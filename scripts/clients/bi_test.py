"""Checks Metabase and Apache Superset against Igloo through their HTTP
APIs, as an analyst would through their UIs: connect with the PostgreSQL
driver, sync or browse the schema, and run the queries a dashboard runs,
written by hand and built by the tool. Expects the TPC-H tables (any scale
factor) and the revenue_by_month view. Standard library only.
Run from scripts/bi-e2e.sh."""

import http.cookiejar
import json
import os
import sys
import time
import urllib.error
import urllib.request

host, user, password = os.environ.get("IGLOO_HOST", "igloo"), os.environ["IGLOO_USER"], os.environ["IGLOO_PASSWORD"]


def check(ok, what):
    if not ok:
        sys.exit(f"  bi: FAILED {what}")
    print(f"  bi: {what}")


class Client:
    def __init__(self, base, headers=None):
        self.base, self.headers = base, dict(headers or {})
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))

    def call(self, method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method,
                                     headers={"Content-Type": "application/json", **self.headers})
        try:
            with self.opener.open(req, timeout=600) as resp:
                text = resp.read().decode()
        except urllib.error.HTTPError as e:
            sys.exit(f"  bi: FAILED {method} {path}: {e.code} {e.read().decode()[:2000]}")
        return json.loads(text) if text.strip()[:1] in ("{", "[") else text

    def wait(self, path, ready, what, timeout=600):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with self.opener.open(self.base + path, timeout=10) as resp:
                    if ready(resp.read().decode()):
                        return
            except (urllib.error.URLError, OSError):
                pass
            time.sleep(3)
        sys.exit(f"  bi: FAILED {what} did not start in {timeout} s")


def metabase():
    mb = Client("http://metabase:3000")
    mb.wait("/api/health", lambda body: '"ok"' in body, "Metabase")
    token = mb.call("GET", "/api/session/properties")["setup-token"]
    session = mb.call("POST", "/api/setup", {
        "token": token,
        "user": {"email": "analyst@example.com", "password": "Igloo-e2e-1", "first_name": "Ana", "last_name": "Lyst",
                 "site_name": "Agency"},
        "prefs": {"site_name": "Agency", "allow_tracking": False},
    })["id"]
    mb.headers["X-Metabase-Session"] = session
    db = mb.call("POST", "/api/database", {
        "engine": "postgres", "name": "Igloo", "is_full_sync": True,
        "details": {"host": host, "port": 5432, "dbname": "igloo", "user": user, "password": password, "ssl": False},
    })["id"]
    check(True, "Metabase connects with its PostgreSQL driver")

    # Syncing reads the schema and columns, then fingerprints and samples
    # every column with queries of its own.
    deadline, tables = time.time() + 600, {}
    while time.time() < deadline:
        meta = mb.call("GET", f"/api/database/{db}/metadata")
        tables = {t["name"]: t for t in meta["tables"] if t["schema"] == "public"}
        if "orders" in tables and len(tables["orders"]["fields"]) == 9 and meta.get("initial_sync_status") == "complete":
            break
        time.sleep(3)
    check({"orders", "lineitem", "revenue_by_month"} <= set(tables), f"Metabase syncs tables and views ({len(tables)} in public)")
    fields = {f["name"]: f for f in tables["orders"]["fields"]}
    check(fields["o_totalprice"]["base_type"] == "type/Decimal" and fields["o_orderdate"]["base_type"] == "type/Date"
          and fields["o_orderpriority"]["base_type"] == "type/Text", "Metabase maps column types")
    check("public" in mb.call("GET", f"/api/database/{db}/schemas"), "Metabase browses schemas")
    if tables["orders"].get("description") is not None:
        check(tables["orders"]["description"] == "One row per order", "Metabase reads table comments")

    native = mb.call("POST", "/api/dataset", {"database": db, "type": "native", "native": {
        "query": "SELECT o_orderpriority, count(*) AS orders FROM orders GROUP BY 1 ORDER BY 1"}})
    check(native["status"] == "completed" and native["row_count"] == 5, "Metabase runs a native query")
    month = ["field", fields["o_orderdate"]["id"], {"temporal-unit": "month"}]
    gui = mb.call("POST", "/api/dataset", {"database": db, "type": "query", "query": {
        "source-table": tables["orders"]["id"],
        "aggregation": [["sum", ["field", fields["o_totalprice"]["id"], None]], ["count"],
                        ["distinct", ["field", fields["o_custkey"]["id"], None]]],
        "breakout": [month],
        "filter": ["=", ["field", fields["o_orderstatus"]["id"], None], "F", "O"],
        "order-by": [["asc", month]],
    }})
    check(gui["status"] == "completed" and gui["row_count"] > 12, f"Metabase runs a question built in its editor ({gui['row_count']} months)")
    # Binning needs the column's fingerprint, which the analysis after the
    # sync computes with queries of its own.
    deadline = time.time() + 600
    while time.time() < deadline and not mb.call("GET", f"/api/field/{fields['o_totalprice']['id']}").get("fingerprint"):
        time.sleep(3)
    binned = mb.call("POST", "/api/dataset", {"database": db, "type": "query", "query": {
        "source-table": tables["orders"]["id"], "aggregation": [["count"]],
        "breakout": [["field", fields["o_totalprice"]["id"], {"binning": {"strategy": "default"}}]],
    }})
    check(binned["status"] == "completed" and binned["row_count"] > 1, "Metabase bins a measure")


def superset():
    ss = Client("http://superset:8088")
    ss.wait("/health", lambda body: "OK" in body, "Superset")
    token = ss.call("POST", "/api/v1/security/login",
                    {"username": "admin", "password": "Igloo-e2e-1", "provider": "db", "refresh": True})["access_token"]
    ss.headers["Authorization"] = f"Bearer {token}"
    ss.headers["X-CSRFToken"] = ss.call("GET", "/api/v1/security/csrf_token/")["result"]
    ss.headers["Referer"] = "http://superset:8088"
    uri = f"postgresql+psycopg2://{user}:{password}@{host}:5432/igloo"
    ss.call("POST", "/api/v1/database/test_connection/", {"database_name": "Igloo", "sqlalchemy_uri": uri})
    db = ss.call("POST", "/api/v1/database/", {"database_name": "Igloo", "sqlalchemy_uri": uri, "expose_in_sqllab": True})["id"]
    check(True, "Superset connects with SQLAlchemy and psycopg2")

    check("public" in ss.call("GET", f"/api/v1/database/{db}/schemas/")["result"], "Superset lists schemas")
    tables = ss.call("GET", f"/api/v1/database/{db}/tables/?q=(schema_name:public)")["result"]
    check({"orders", "revenue_by_month"} <= {t["value"] for t in tables}, "Superset lists tables and views")
    dataset = ss.call("POST", "/api/v1/dataset/", {"database": db, "schema": "public", "table_name": "orders"})["id"]
    columns = {c["column_name"]: c for c in ss.call("GET", f"/api/v1/dataset/{dataset}")["result"]["columns"]}
    check(len(columns) == 9 and columns["o_orderdate"]["is_dttm"], "Superset reads a dataset's columns")

    chart = ss.call("POST", "/api/v1/chart/data", {
        "datasource": {"id": dataset, "type": "table"},
        "queries": [{
            "columns": [{"timeGrain": "P1M", "columnType": "BASE_AXIS", "sqlExpression": "o_orderdate",
                         "label": "o_orderdate", "expressionType": "SQL"}, "o_orderpriority"],
            "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "o_totalprice"}, "aggregate": "SUM",
                         "label": "revenue"}],
            "orderby": [[{"expressionType": "SIMPLE", "column": {"column_name": "o_totalprice"}, "aggregate": "SUM",
                          "label": "revenue"}, False]],
            "time_range": "No filter", "row_limit": 10000,
        }],
        "result_format": "json", "result_type": "full",
    })["result"][0]
    check(chart["status"] == "success" and chart["rowcount"] > 60, f"Superset renders a time-series chart ({chart['rowcount']} rows)")
    lab = ss.call("POST", "/api/v1/sqllab/execute/", {
        "database_id": db, "schema": "public", "runAsync": False,
        "sql": "SELECT month, revenue, rank() OVER (ORDER BY revenue DESC) AS rank FROM revenue_by_month ORDER BY month"})
    check(lab["status"] == "success" and len(lab["data"]) > 60, "Superset's SQL Lab runs a query on a view")


which = sys.argv[1:] or ["metabase", "superset"]
for tool in which:
    globals()[tool]()
