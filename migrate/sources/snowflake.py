"""Snowflake: query history from SNOWFLAKE.ACCOUNT_USAGE, or from
INFORMATION_SCHEMA.QUERY_HISTORY() where ACCOUNT_USAGE is not granted.

Connection settings are the benchmark's (bench/README.md): SNOWFLAKE_ACCOUNT,
SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_FILE (or SNOWFLAKE_TOKEN_FILE, or
SNOWFLAKE_PASSWORD), SNOWFLAKE_ROLE and SNOWFLAKE_WAREHOUSE. The warehouse
is required: without it Snowflake runs the tool's statements on the user's
default warehouse, which may be a large one.
"""

from __future__ import annotations

import datetime as dt
import os
import pathlib
import re
import time

from .. import compare, statements, workload
from . import MARKER, Extract, Filters, Meter, Rerun, Spend, TableInfo, iso, json_list, num, timed

DIALECT = "snowflake"
UNIT = "credits"

# What ACCOUNT_USAGE.QUERY_HISTORY and INFORMATION_SCHEMA.QUERY_HISTORY()
# call statuses; anything else (RUNNING, QUEUED, ...) has not finished.
SUCCESS = {"SUCCESS"}
FAILED = {"FAIL", "INCIDENT", "FAILED_WITH_ERROR", "FAILED_WITH_INCIDENT"}

# Source statement types for each class, to filter on the server where a
# class maps onto Snowflake's types exactly.
TYPES_BY_CLASS = {
    "SELECT": ["SELECT"], "CTAS": ["CREATE_TABLE_AS_SELECT"], "INSERT": ["INSERT", "MULTI_TABLE_INSERT"],
    "MERGE": ["MERGE"], "UPDATE": ["UPDATE"], "DELETE": ["DELETE"],
}

# Snowflake warehouse sizes as SHOW WAREHOUSES and the history write them,
# to the names bench/pricing.toml uses.
SIZES = {"X-SMALL": "XS", "XSMALL": "XS", "SMALL": "S", "MEDIUM": "M", "LARGE": "L", "X-LARGE": "XL", "XLARGE": "XL",
         "2X-LARGE": "2XL", "XXLARGE": "2XL", "X2LARGE": "2XL", "3X-LARGE": "3XL", "XXXLARGE": "3XL", "X3LARGE": "3XL",
         "4X-LARGE": "4XL", "X4LARGE": "4XL", "5X-LARGE": "5XL", "X5LARGE": "5XL", "6X-LARGE": "6XL", "X6LARGE": "6XL"}

# The rows a query returned. ROWS_PRODUCED counts more than the result for
# some queries (TPC-H q11, q15 and q22 by one, as the rows of a scalar
# subquery or CTE), and none for queries answered from metadata, such as
# SELECT COUNT(*) FROM t; ROWS_WRITTEN_TO_RESULT is the result's size.
ROWS = "COALESCE(q.rows_written_to_result, q.rows_produced)"


def _result_rows(r: dict) -> int | None:
    v = r.get("rows_written_to_result")
    if v is None:
        v = r.get("rows_produced")
    return None if v is None else int(v)


INFO_SCHEMA_DAYS = 7  # INFORMATION_SCHEMA.QUERY_HISTORY() keeps seven days
INFO_SCHEMA_LIMIT = 10000  # and returns at most this many rows a call
ACCESS_DOMAINS = {"table", "view", "materialized view", "external table", "dynamic table", "iceberg table",
                  "event table", "stream", "secure view"}


def size_name(size: str | None) -> str | None:
    if not size:
        return None
    return SIZES.get(size.upper().replace("_", "-"), size)


def _in_list(prefix: str, values: list[str], params: dict) -> str:
    names = []
    for i, v in enumerate(values):
        params[f"{prefix}{i}"] = v
        names.append(f"%({prefix}{i})s")
    return "(" + ", ".join(names) + ")"


def _filter_sql(f: Filters, params: dict, alias: str = "q") -> str:
    where = [f"{alias}.start_time >= %(start)s::TIMESTAMP_TZ", f"{alias}.start_time < %(end)s::TIMESTAMP_TZ",
             f"COALESCE({alias}.query_tag, '') NOT ILIKE '{MARKER}%%'",
             f"NOT STARTSWITH({alias}.query_text, '/* {MARKER}')"]
    params["start"], params["end"] = iso(f.start), iso(f.end)
    if f.users:
        where.append(f"UPPER({alias}.user_name) IN " + _in_list("u", [u.upper() for u in f.users], params))
    if f.roles:
        where.append(f"UPPER({alias}.role_name) IN " + _in_list("r", [r.upper() for r in f.roles], params))
    if f.warehouses:
        where.append(f"UPPER({alias}.warehouse_name) IN " + _in_list("w", [w.upper() for w in f.warehouses], params))
    if f.classes and all(c in TYPES_BY_CLASS for c in f.classes):
        types = [t for c in f.classes for t in TYPES_BY_CLASS[c]]
        where.append(f"{alias}.query_type IN " + _in_list("t", types, params))
    return " AND ".join(where)


def account_usage_sql(f: Filters, attribution: bool, access: bool) -> tuple[str, dict]:
    """One row per group: the executions with the same parameterized hash,
    session database and schema and statement type, summed on the server so
    that a long history comes back small. With attribution, joins the
    per-query credits of QUERY_ATTRIBUTION_HISTORY; with access, the tables
    each group's representative statement read and wrote, from
    ACCESS_HISTORY, in one pass over it (a lookup per batch of statements
    scanned the whole period's access history each time)."""
    params: dict = {}
    ok = "q.execution_status = 'SUCCESS'"
    credits = "SUM(a.credits_attributed_compute)" if attribution else "NULL"
    join = ("LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY a ON a.query_id = q.query_id"
            if attribution else "")
    groups = f"""SELECT
  COALESCE(q.query_parameterized_hash, q.query_hash, SHA2(q.query_text)) AS key,
  q.database_name AS database, q.schema_name AS schema, q.query_type AS source_type,
  COUNT(*) AS executions, COUNT_IF({ok}) AS succeeded, COUNT_IF(NOT ({ok})) AS failed,
  SUM(q.total_elapsed_time) AS elapsed_ms_total, SUM(q.execution_time) AS execution_ms_total,
  MEDIAN(IFF({ok}, q.total_elapsed_time, NULL)) AS elapsed_ms_median,
  MEDIAN(IFF({ok}, {ROWS}, NULL)) AS rows_median,
  SUM(q.bytes_scanned) AS bytes_scanned_total, {credits} AS credits,
  COALESCE(MAX_BY(q.query_text, IFF({ok}, q.start_time, NULL)), MAX_BY(q.query_text, q.start_time)) AS text,
  COALESCE(MAX_BY(q.query_id, IFF({ok}, q.start_time, NULL)), MAX_BY(q.query_id, q.start_time)) AS query_id,
  MAX_BY({ROWS}, IFF({ok}, q.start_time, NULL)) AS rows_last,
  MAX(IFF({ok}, q.start_time, NULL)) AS rep_start,
  MAX_BY(q.error_message, q.start_time) AS error,
  ARRAY_UNIQUE_AGG(q.user_name) AS users, ARRAY_UNIQUE_AGG(q.role_name) AS roles,
  ARRAY_UNIQUE_AGG(q.warehouse_name) AS warehouses, ARRAY_UNIQUE_AGG(q.warehouse_size) AS warehouse_sizes,
  MIN(q.start_time) AS first_seen, MAX(q.start_time) AS last_seen,
  COUNT(*) OVER () AS all_groups, SUM(COUNT(*)) OVER () AS all_executions,
  SUM(SUM(q.total_elapsed_time)) OVER () AS all_elapsed_ms
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q {join}
WHERE {_filter_sql(f, params)} AND q.execution_status IN ('SUCCESS', 'FAIL', 'INCIDENT')
GROUP BY 1, 2, 3, 4
ORDER BY elapsed_ms_total DESC
LIMIT {int(f.max_groups)}"""
    if not access:
        return f"/* {MARKER} */\n{groups}", params
    sql = f"""/* {MARKER} */
WITH g AS (
{groups}
)
SELECT g.*, h.direct_objects_accessed, h.objects_modified
FROM g LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY h
  ON h.query_id = g.query_id
  AND h.query_start_time >= %(start)s::TIMESTAMP_TZ AND h.query_start_time < %(end)s::TIMESTAMP_TZ
ORDER BY g.elapsed_ms_total DESC"""
    return sql, params


def groups_from_account_usage(rows: list[dict]) -> tuple[list[workload.Group], dict]:
    groups, totals = [], {}
    for r in rows:
        # Access history, where it was joined and has the statement (it has
        # none for SHOW, USE and the like): None leaves the tables to parsing.
        read = written = None
        if r.get("direct_objects_accessed") is not None or r.get("objects_modified") is not None:
            read, written = objects_from_access_history(r)
        groups.append(workload.Group(
            tables_read=read, tables_written=written,
            key=str(r["key"]), text=r["text"] or "", query_id=r.get("query_id"), source_type=r.get("source_type"),
            database=r.get("database"), schema=r.get("schema"), executions=int(r["executions"]),
            succeeded=int(r["succeeded"]), failed=int(r["failed"]), elapsed_ms_total=num(r["elapsed_ms_total"]) or 0,
            execution_ms_total=num(r["execution_ms_total"]) or 0, elapsed_ms_median=num(r.get("elapsed_ms_median")),
            rows_median=num(r.get("rows_median")),
            rows_last=None if r.get("rows_last") is None else int(r["rows_last"]),
            bytes_scanned_total=num(r.get("bytes_scanned_total")), credits=num(r.get("credits")),
            users=json_list(r.get("users")), roles=json_list(r.get("roles")),
            warehouses=json_list(r.get("warehouses")),
            warehouse_sizes=[size_name(s) for s in json_list(r.get("warehouse_sizes"))],
            first_seen=iso(r.get("first_seen")), last_seen=iso(r.get("last_seen")), rep_start=iso(r.get("rep_start")),
            error=r.get("error") if not int(r["succeeded"]) else None,
        ))
        totals = {"groups": int(r["all_groups"]), "executions": int(r["all_executions"]),
                  "elapsed_ms": num(r["all_elapsed_ms"])}
    return groups, totals


def info_schema_sql(start: dt.datetime, end: dt.datetime) -> tuple[str, dict]:
    """Executions that ended in [start, end), at most INFO_SCHEMA_LIMIT. The
    range must be given: without one the function took minutes."""
    sql = f"""/* {MARKER} */
SELECT query_id, query_text, database_name, schema_name, query_type, user_name, role_name, warehouse_name,
  warehouse_size, query_tag, execution_status, error_message, start_time, total_elapsed_time, execution_time,
  bytes_scanned, rows_produced, rows_written_to_result, query_parameterized_hash, query_hash
FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY(
  END_TIME_RANGE_START => %(start)s::TIMESTAMP_LTZ, END_TIME_RANGE_END => %(end)s::TIMESTAMP_LTZ,
  RESULT_LIMIT => {INFO_SCHEMA_LIMIT}))"""
    return sql, {"start": iso(start), "end": iso(end)}


def executions_from_info_schema(rows: list[dict], f: Filters) -> list[dict]:
    """The rows aggregate_rows takes, keeping those that finished and pass
    the filters (the table function cannot filter on the server)."""
    out = []
    users, roles, whs = ({x.upper() for x in xs} for xs in (f.users, f.roles, f.warehouses))
    types = {t for c in f.classes for t in TYPES_BY_CLASS.get(c, [])} if f.classes else set()
    for r in rows:
        status = (r.get("execution_status") or "").upper()
        if status not in SUCCESS | FAILED:
            continue
        text = r.get("query_text") or ""
        if (r.get("query_tag") or "").lower().startswith(MARKER) or text.startswith(f"/* {MARKER}"):
            continue
        if users and (r.get("user_name") or "").upper() not in users:
            continue
        if roles and (r.get("role_name") or "").upper() not in roles:
            continue
        if whs and (r.get("warehouse_name") or "").upper() not in whs:
            continue
        if types and all(c in TYPES_BY_CLASS for c in f.classes) and r.get("query_type") not in types:
            continue
        out.append({
            "key": str(r.get("query_parameterized_hash") or r.get("query_hash") or r["query_id"]), "text": text,
            "query_id": r["query_id"], "source_type": r.get("query_type"), "database": r.get("database_name"),
            "schema": r.get("schema_name"), "status": "success" if status in SUCCESS else "failed",
            "elapsed_ms": num(r.get("total_elapsed_time")), "execution_ms": num(r.get("execution_time")),
            "rows": _result_rows(r),
            "bytes_scanned": num(r.get("bytes_scanned")), "user": r.get("user_name"), "role": r.get("role_name"),
            "warehouse": r.get("warehouse_name"), "warehouse_size": size_name(r.get("warehouse_size")),
            "start": iso(r.get("start_time")), "error": r.get("error_message"),
        })
    return out


def objects_from_access_history(row: dict) -> tuple[list[str], list[str]]:
    """Tables and views a query named, and those it wrote, from
    ACCESS_HISTORY's JSON (direct objects: what the text names, views
    included, which is what Igloo needs to run the text)."""
    def names(v):
        return sorted({o["objectName"] for o in json_list(v)
                       if isinstance(o, dict) and str(o.get("objectDomain", "")).lower() in ACCESS_DOMAINS
                       and o.get("objectName")})
    written = names(row.get("objects_modified"))
    read = [n for n in names(row.get("direct_objects_accessed")) if n not in written]
    return read, written


def metering_rows_to_spend(rows: list[dict], source: str) -> Spend:
    by = {r["warehouse_name"]: float(r["credits"] or 0) for r in rows}
    return Spend(unit=UNIT, by_warehouse=by, total=sum(by.values()), source=source)


class Snowflake:
    name = "snowflake"
    dialect = DIALECT
    unit = UNIT

    def __init__(self, conn, warehouse: str, timeout: int, pricing: dict):
        self.conn = conn
        self.warehouse_name = warehouse
        self.timeout = timeout
        self.pricing = pricing
        self.meter: Meter | None = None
        self._namespace = None
        self.warehouse = self._warehouse_info()
        p = pricing.get("snowflake", {})
        rate = None
        if self.warehouse.get("size") in p.get("credits_per_hour", {}):
            factor = p.get("generation_factor", 1.0)
            if isinstance(factor, dict):
                factor = factor.get(str(self.warehouse.get("generation") or 1), 1.0)
            rate = p["credits_per_hour"][self.warehouse["size"]] * factor
        self.meter = Meter(rate, self.warehouse.get("state") in ("STARTED", "RESUMING"),
                           float(p.get("minimum_seconds", 60)), float(self.warehouse.get("auto_suspend") or 0), UNIT)

    @classmethod
    def connect(cls, timeout: int, pricing: dict, warehouse: str | None = None) -> Snowflake:
        import snowflake.connector

        env = os.environ
        warehouse = warehouse or env.get("SNOWFLAKE_WAREHOUSE")
        if not warehouse:
            raise SystemExit("set SNOWFLAKE_WAREHOUSE (or --source-warehouse): without it the tool's statements would "
                             "run on the user's default warehouse")
        kwargs = {
            "account": env["SNOWFLAKE_ACCOUNT"], "user": env["SNOWFLAKE_USER"], "warehouse": warehouse,
            "login_timeout": 60, "network_timeout": timeout + 60,
            "session_parameters": {"QUERY_TAG": MARKER, "STATEMENT_TIMEOUT_IN_SECONDS": timeout,
                                   "MULTI_STATEMENT_COUNT": 1},
        }
        if env.get("SNOWFLAKE_ROLE"):
            kwargs["role"] = env["SNOWFLAKE_ROLE"]
        if env.get("SNOWFLAKE_TOKEN_FILE"):
            kwargs["authenticator"] = "PROGRAMMATIC_ACCESS_TOKEN"
            kwargs["token"] = pathlib.Path(env["SNOWFLAKE_TOKEN_FILE"]).expanduser().read_text().strip()
        elif env.get("SNOWFLAKE_PRIVATE_KEY_FILE"):
            kwargs["private_key_file"] = str(pathlib.Path(env["SNOWFLAKE_PRIVATE_KEY_FILE"]).expanduser())
        else:
            kwargs["password"] = env["SNOWFLAKE_PASSWORD"]
        return cls(snowflake.connector.connect(**kwargs), warehouse, timeout, pricing)

    def close(self):
        self.conn.close()

    # Every statement the tool runs is one of its own, built here, except
    # the comparison re-runs, which go through rerun() and its checks.
    def _run(self, sql: str, params: dict | None = None, phase: str = "extraction", meter: bool = True) -> list[dict]:
        cur = self.conn.cursor()
        try:
            def go():
                cur.execute(sql, params, timeout=self.timeout)
                cols = [c[0].lower() for c in cur.description] if cur.description else []
                return [dict(zip(cols, r)) for r in cur.fetchall()] if cols else []
            rows, seconds = timed(go)
        finally:
            cur.close()
        if meter and self.meter is not None:
            self.meter.add(phase, seconds)
        return rows

    def _warehouse_info(self) -> dict:
        # SHOW runs in Snowflake's cloud services and does not resume the
        # warehouse.
        rows = self._run("SHOW WAREHOUSES LIKE %(w)s", {"w": self.warehouse_name}, meter=False)
        if not rows:
            return {"name": self.warehouse_name}
        r = rows[0]
        gen = r.get("generation")
        if not str(gen or "").isdigit():
            gen = 2 if str(r.get("resource_constraint") or "").endswith("GEN_2") else 1
        return {"name": r.get("name"), "size": size_name(r.get("size")), "state": r.get("state"),
                "auto_suspend": r.get("auto_suspend"), "generation": int(gen), "type": r.get("type")}

    def extract(self, f: Filters) -> Extract:
        meta = {"source": "snowflake", "warehouse": self.warehouse, "notes": []}
        try:
            groups, totals = self._account_usage(f, meta)
            meta.update(history="SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY", start=iso(f.start), end=iso(f.end),
                        totals=totals)
            meta["notes"].append("ACCOUNT_USAGE lags behind by up to 45 minutes; the newest statements may be missing.")
        except Exception as e:  # noqa: BLE001 - not granted: fall back
            meta["notes"].append(f"ACCOUNT_USAGE.QUERY_HISTORY could not be read ({_short(e)}); used "
                                 "INFORMATION_SCHEMA.QUERY_HISTORY(), which covers only the last 7 days, only the "
                                 "statements this role may see, and has no access history.")
            start = max(f.start, dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=INFO_SCHEMA_DAYS)
                        + dt.timedelta(minutes=5))
            if start > f.start:
                meta["notes"].append(f"The period was shortened to start at {iso(start)}.")
            rows = []
            self._info_schema(start, f.end, rows)
            execs = executions_from_info_schema(rows, f)
            groups = workload.aggregate_rows(execs)
            groups.sort(key=lambda g: -g.elapsed_ms_total)
            totals = {"groups": len(groups), "executions": sum(g.executions for g in groups),
                      "elapsed_ms": sum(g.elapsed_ms_total for g in groups)}
            groups = groups[:f.max_groups]
            meta.update(history="INFORMATION_SCHEMA.QUERY_HISTORY()", start=iso(start), end=iso(f.end), totals=totals,
                        rows_read=len(rows))
        return Extract(groups, meta)

    def _account_usage(self, f: Filters, meta: dict):
        """The history with per-query credits and access history, without
        either where the role may not read it."""
        attribution = access = True
        while True:
            try:
                rows = self._run(*account_usage_sql(f, attribution, access))
                break
            except Exception as e:  # noqa: BLE001
                text = str(e).upper()
                if access and "ACCESS_HISTORY" in text:
                    access = False
                    meta["notes"].append(f"ACCESS_HISTORY could not be read ({_short(e)}); tables come from parsing "
                                         "the statements.")
                elif attribution and "QUERY_ATTRIBUTION_HISTORY" in text:
                    attribution = False
                    meta["notes"].append(f"QUERY_ATTRIBUTION_HISTORY could not be read ({_short(e)}): no credits per "
                                         "query.")
                else:
                    raise
        if attribution:
            meta["credits_per_query"] = "SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY"
        if access:
            meta["tables"] = "SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY"
        return groups_from_account_usage(rows)

    def _info_schema(self, start: dt.datetime, end: dt.datetime, out: list[dict]):
        # A day at a time; a window that hits the row limit is split.
        step = dt.timedelta(days=1)
        t = start
        while t < end:
            self._window(t, min(t + step, end), out)
            t += step

    def _window(self, start, end, out):
        rows = self._run(*info_schema_sql(start, end))
        if len(rows) >= INFO_SCHEMA_LIMIT and end - start > dt.timedelta(minutes=1):
            mid = start + (end - start) / 2
            self._window(start, mid, out)
            self._window(mid, end, out)
        else:
            out.extend(rows)

    def table_info(self, refs) -> dict[tuple, TableInfo]:
        """Rows, bytes and last change of each table, from each database's
        INFORMATION_SCHEMA.TABLES."""
        by_db: dict[str, list] = {}
        for r in refs:
            by_db.setdefault(r.name.split(".")[0], []).append(r)
        out = {}
        for db, rs in by_db.items():
            params: dict = {}
            schemas = sorted({r.name.split(".")[1].upper() for r in rs if r.name.count(".") == 2})
            if not schemas or not re.fullmatch(r'[A-Za-z0-9_$"]+', db):
                continue
            sql = f"""/* {MARKER} */
SELECT table_catalog, table_schema, table_name, table_type, row_count, bytes, last_altered
FROM {_quote(db)}.INFORMATION_SCHEMA.TABLES
WHERE UPPER(table_schema) IN {_in_list("s", schemas, params)}"""
            try:
                rows = self._run(sql, params)
            except Exception:  # noqa: BLE001 - a database this role cannot see
                continue
            wanted = {r.key: r for r in rs}
            for row in rows:
                key = (row["table_catalog"].lower(), row["table_schema"].lower(), row["table_name"].lower())
                if key in wanted:
                    out[key] = TableInfo(name=".".join((row["table_catalog"], row["table_schema"], row["table_name"])),
                                         kind=row.get("table_type"),
                                         rows=None if row.get("row_count") is None else int(row["row_count"]),
                                         bytes=None if row.get("bytes") is None else int(row["bytes"]),
                                         last_altered=iso(row.get("last_altered")))
        return out

    def spend(self, start: dt.datetime, end: dt.datetime) -> Spend:
        """Credits by warehouse over the period, from WAREHOUSE_METERING_HISTORY."""
        params = {"start": iso(start), "end": iso(end)}
        tried = []
        for source, sql in (
            ("SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY", f"""/* {MARKER} */
SELECT warehouse_name, SUM(credits_used) AS credits FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= %(start)s::TIMESTAMP_TZ AND start_time < %(end)s::TIMESTAMP_TZ GROUP BY 1"""),
            ("INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY()", f"""/* {MARKER} */
SELECT warehouse_name, SUM(credits_used) AS credits FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
  DATE_RANGE_START => %(start)s::TIMESTAMP_LTZ, DATE_RANGE_END => %(end)s::TIMESTAMP_LTZ)) GROUP BY 1"""),
        ):
            try:
                rows = self._run(sql, params)
            except Exception as e:  # noqa: BLE001
                tried.append(f"{source}: {_short(e)}")
                continue
            if not rows:
                # Without MONITOR USAGE the table function answers with no
                # rows rather than an error: that is not zero credits.
                tried.append(f"{source}: no rows, which without MONITOR USAGE on the account means not visible")
                continue
            spend = metering_rows_to_spend(rows, source)
            spend.note = "; ".join(tried) or None
            return spend
        return Spend(unit=UNIT, note="could not read warehouse metering: " + "; ".join(tried))

    def rerun(self, shape, timeout: int, max_rows: int) -> Rerun:
        """Runs the shape's statement again, exactly as the history has it,
        after checking that it only reads. Its result is fetched and
        digested, never kept."""
        problem = statements.read_only_problem(shape.text, DIALECT)
        if problem:
            return Rerun(ok=False, skipped=f"not run on the source: {problem}")
        cur = self.conn.cursor()
        start = time.perf_counter()
        try:
            ns = (shape.database, shape.schema)
            if ns != self._namespace and shape.database:
                stmt = f"USE SCHEMA {_quote(shape.database)}.{_quote(shape.schema)}" if shape.schema else \
                    f"USE DATABASE {_quote(shape.database)}"
                cur.execute(stmt)
                self._namespace = ns

            def go():
                cur.execute(shape.text, timeout=timeout)
                cols = [c[0] for c in cur.description]
                semi = {i for i, c in enumerate(cur.description) if _type_name(c) in ("VARIANT", "OBJECT", "ARRAY")}
                return compare.digest(_fetch_capped(cur, max_rows), cols, semi)
            d, seconds = timed(go)
            return Rerun(ok=True, seconds=seconds, rows=d.rows, checksum=d.checksum, columns=d.columns,
                         values=d.values)
        except _TooMany:
            return Rerun(ok=False, skipped=f"returned more than {max_rows} rows")
        except Exception as e:  # noqa: BLE001
            return Rerun(ok=False, error=_short(e))
        finally:
            # Failed and canceled runs used the warehouse too.
            self.meter.add("comparison", time.perf_counter() - start)
            cur.close()


class _TooMany(Exception):
    pass


def _fetch_capped(cur, max_rows):
    n = 0
    while True:
        batch = cur.fetchmany(10000)
        if not batch:
            return
        n += len(batch)
        if n > max_rows:
            raise _TooMany()
        yield from batch


def _type_name(desc) -> str:
    try:
        from snowflake.connector.constants import FIELD_ID_TO_NAME

        return FIELD_ID_TO_NAME.get(desc[1], "")
    except Exception:  # noqa: BLE001
        return ""


def _quote(name: str) -> str:
    """A name from the history (stored without quotes) as an identifier."""
    return '"' + name.replace('"', '""') + '"'


def _short(e: Exception) -> str:
    text = str(e).strip().splitlines()
    return " ".join(text[:2])[:240] if text else type(e).__name__
