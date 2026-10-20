"""Databricks SQL: query history from system.query.history, or from the
Query History API where the system table is not granted.

Connection settings are the benchmark's (bench/README.md): the Databricks
SDK's (DATABRICKS_HOST with DATABRICKS_TOKEN, or a profile named by
DATABRICKS_CONFIG_PROFILE), and the SQL warehouse to run on, as
DATABRICKS_WAREHOUSE_ID or DATABRICKS_HTTP_PATH.

The history does not record a statement's session catalog and schema, so
unqualified table names resolve against --default-namespace, or the
warehouse's own defaults.
"""

from __future__ import annotations

import datetime as dt
import os
import re
import time

from .. import compare, statements, workload
from . import MARKER, Extract, Filters, Meter, Rerun, Spend, TableInfo, iso, json_list, num, timed

DIALECT = "databricks"
UNIT = "DBUs"

TYPES_BY_CLASS = {"SELECT": ["SELECT"], "INSERT": ["INSERT"], "MERGE": ["MERGE"], "UPDATE": ["UPDATE"],
                  "DELETE": ["DELETE"]}
SIZES = {"2X-SMALL": "2XS", "X-SMALL": "XS", "SMALL": "S", "MEDIUM": "M", "LARGE": "L", "X-LARGE": "XL",
         "2X-LARGE": "2XL", "3X-LARGE": "3XL", "4X-LARGE": "4XL"}


def size_name(size: str | None) -> str | None:
    return SIZES.get((size or "").upper(), size) if size else None


def _in_list(prefix: str, values: list[str], params: dict) -> str:
    names = []
    for i, v in enumerate(values):
        params[f"{prefix}{i}"] = v
        names.append(f":{prefix}{i}")
    return "(" + ", ".join(names) + ")"


def system_history_sql(f: Filters) -> tuple[str, dict]:
    """One row per group: executions of the same text and statement type,
    summed on the server."""
    params: dict = {"start": f.start.astimezone(dt.timezone.utc).replace(tzinfo=None),
                    "end": f.end.astimezone(dt.timezone.utc).replace(tzinfo=None)}
    where = ["start_time >= :start", "start_time < :end", "execution_status IN ('FINISHED', 'FAILED', 'CANCELED')",
             f"NOT startswith(statement_text, '/* {MARKER}')"]
    if f.users:
        where.append("lower(executed_by) IN " + _in_list("u", [u.lower() for u in f.users], params))
    if f.warehouses:
        where.append("compute.warehouse_id IN " + _in_list("w", list(f.warehouses), params))
    if f.classes and all(c in TYPES_BY_CLASS for c in f.classes):
        where.append("statement_type IN " + _in_list("t", [t for c in f.classes for t in TYPES_BY_CLASS[c]], params))
    ok = "execution_status = 'FINISHED'"
    sql = f"""/* {MARKER} */
SELECT
  sha2(statement_text, 256) AS key, statement_type AS source_type,
  count(*) AS executions, count_if({ok}) AS succeeded, count_if(NOT ({ok})) AS failed,
  sum(total_duration_ms) AS elapsed_ms_total, sum(execution_duration_ms) AS execution_ms_total,
  percentile(IF({ok}, total_duration_ms, NULL), 0.5) AS elapsed_ms_median,
  percentile(IF({ok}, produced_rows, NULL), 0.5) AS rows_median,
  sum(read_bytes) AS bytes_scanned_total,
  any_value(statement_text) AS text,
  coalesce(max_by(statement_id, IF({ok}, start_time, NULL)), max_by(statement_id, start_time)) AS query_id,
  max_by(produced_rows, IF({ok}, start_time, NULL)) AS rows_last,
  max(IF({ok}, start_time, NULL)) AS rep_start,
  max_by(error_message, start_time) AS error,
  collect_set(executed_by) AS users, collect_set(compute.warehouse_id) AS warehouses,
  min(start_time) AS first_seen, max(start_time) AS last_seen,
  count(*) OVER () AS all_groups, sum(count(*)) OVER () AS all_executions,
  sum(sum(total_duration_ms)) OVER () AS all_elapsed_ms
FROM system.query.history
WHERE {" AND ".join(where)}
GROUP BY sha2(statement_text, 256), statement_type
ORDER BY elapsed_ms_total DESC
LIMIT {int(f.max_groups)}"""
    return sql, params


def groups_from_system_history(rows: list[dict], namespace: tuple[str | None, str | None]) -> tuple[list, dict]:
    groups, totals = [], {}
    for r in rows:
        groups.append(workload.Group(
            key=str(r["key"]), text=r["text"] or "", query_id=r.get("query_id"), source_type=r.get("source_type"),
            database=namespace[0], schema=namespace[1], executions=int(r["executions"]), succeeded=int(r["succeeded"]),
            failed=int(r["failed"]), elapsed_ms_total=num(r["elapsed_ms_total"]) or 0,
            execution_ms_total=num(r["execution_ms_total"]) or 0, elapsed_ms_median=num(r.get("elapsed_ms_median")),
            rows_median=num(r.get("rows_median")),
            rows_last=None if r.get("rows_last") is None else int(r["rows_last"]),
            bytes_scanned_total=num(r.get("bytes_scanned_total")), users=json_list(r.get("users")),
            warehouses=json_list(r.get("warehouses")), first_seen=iso(r.get("first_seen")),
            last_seen=iso(r.get("last_seen")), rep_start=iso(r.get("rep_start")),
            error=r.get("error") if not int(r["succeeded"]) else None,
        ))
        totals = {"groups": int(r["all_groups"]), "executions": int(r["all_executions"]),
                  "elapsed_ms": num(r["all_elapsed_ms"])}
    return groups, totals


def executions_from_api(queries, f: Filters, namespace, sizes: dict[str, str]) -> list[dict]:
    """The rows aggregate_rows takes, from the Query History API's QueryInfo
    objects (or dicts of the same fields)."""
    out = []
    users = {u.lower() for u in f.users}
    types = {t for c in f.classes for t in TYPES_BY_CLASS.get(c, [])}

    def get(o, name, default=None):
        return o.get(name, default) if isinstance(o, dict) else getattr(o, name, default)

    for q in queries:
        status = str(get(q, "status") or "").split(".")[-1].upper()
        if status not in ("FINISHED", "FAILED", "CANCELED"):
            continue
        text = get(q, "query_text") or ""
        if text.startswith(f"/* {MARKER}"):
            continue
        user = get(q, "user_name") or get(q, "executed_as_user_name")
        if users and (user or "").lower() not in users:
            continue
        if f.warehouses and get(q, "warehouse_id") not in f.warehouses:
            continue
        stype = str(get(q, "statement_type") or "").split(".")[-1].upper() or None
        if types and all(c in TYPES_BY_CLASS for c in f.classes) and stype not in types:
            continue
        metrics = get(q, "metrics") or {}
        start_ms = get(q, "query_start_time_ms")
        out.append({
            "key": workload_key(text), "text": text, "query_id": get(q, "query_id"), "source_type": stype,
            "database": namespace[0], "schema": namespace[1], "status": "success" if status == "FINISHED" else "failed",
            "elapsed_ms": num(get(q, "duration")), "execution_ms": num(get(metrics, "execution_time_ms")),
            "rows": get(q, "rows_produced") if get(q, "rows_produced") is not None else get(metrics, "rows_produced_count"),
            "bytes_scanned": num(get(metrics, "read_bytes")), "user": user, "role": None,
            "warehouse": get(q, "warehouse_id"), "warehouse_size": sizes.get(get(q, "warehouse_id")),
            "start": iso(dt.datetime.fromtimestamp(start_ms / 1000, dt.timezone.utc)) if start_ms else None,
            "error": get(q, "error_message"),
        })
    return out


def workload_key(text: str) -> str:
    import hashlib

    return hashlib.sha256(text.encode()).hexdigest()


def billing_sql(start: dt.datetime, end: dt.datetime) -> tuple[str, dict]:
    return f"""/* {MARKER} */
SELECT usage_metadata.warehouse_id AS warehouse, sum(usage_quantity) AS dbus
FROM system.billing.usage
WHERE usage_start_time >= :start AND usage_start_time < :end AND usage_unit = 'DBU'
  AND usage_metadata.warehouse_id IS NOT NULL
GROUP BY 1""", {"start": start.astimezone(dt.timezone.utc).replace(tzinfo=None),
               "end": end.astimezone(dt.timezone.utc).replace(tzinfo=None)}


class Databricks:
    name = "databricks"
    dialect = DIALECT
    unit = UNIT

    def __init__(self, conn, workspace, warehouse_id: str, timeout: int, pricing: dict,
                 namespace: tuple[str | None, str | None] | None, warehouse: dict | None = None):
        self.conn = conn
        self.w = workspace
        self.warehouse_id = warehouse_id
        self.timeout = timeout
        self.pricing = pricing
        self.meter = None
        self._namespace_in_use = None
        self.warehouse = warehouse or self._warehouse_info()
        p = pricing.get("databricks", {})
        rate = p.get("dbu_per_hour", {}).get(self.warehouse.get("size"))
        self.meter = Meter(rate, self.warehouse.get("state") == "RUNNING", float(p.get("minimum_seconds", 0)),
                           60.0 * float(self.warehouse.get("auto_stop_mins") or 0), UNIT)
        self._run(f"/* {MARKER} */ SET STATEMENT_TIMEOUT = {int(timeout)}", meter=False)
        if namespace is None:
            row = self._run(f"/* {MARKER} */ SELECT current_catalog() AS c, current_schema() AS s")[0]
            namespace = (row["c"], row["s"])
        self.namespace = namespace

    @classmethod
    def connect(cls, timeout: int, pricing: dict, warehouse: str | None = None,
                namespace: tuple[str | None, str | None] | None = None) -> Databricks:
        from databricks import sql
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.core import Config

        env = os.environ
        cfg = Config()
        warehouse = warehouse or env.get("DATABRICKS_WAREHOUSE_ID")
        http_path = env.get("DATABRICKS_HTTP_PATH")
        if warehouse:
            http_path = f"/sql/1.0/warehouses/{warehouse}"
        elif http_path:
            warehouse = http_path.rstrip("/").split("/")[-1]
        else:
            raise SystemExit("set DATABRICKS_WAREHOUSE_ID or DATABRICKS_HTTP_PATH (or --source-warehouse)")
        w = WorkspaceClient(config=cfg)
        # Its state before connecting starts it: whether the tool pays for
        # starting it and for its idle time after.
        info = cls._warehouse_info_of(w, warehouse)
        conn = sql.connect(server_hostname=cfg.host.removeprefix("https://"), http_path=http_path,
                           credentials_provider=lambda: cfg.authenticate, _socket_timeout=timeout + 60)
        return cls(conn, w, warehouse, timeout, pricing, namespace, info)

    def close(self):
        self.conn.close()

    def _run(self, sql: str, params: dict | None = None, phase: str = "extraction", meter: bool = True) -> list[dict]:
        cur = self.conn.cursor()
        try:
            def go():
                cur.execute(sql, params) if params else cur.execute(sql)
                cols = [c[0].lower() for c in cur.description] if cur.description else []
                return [dict(zip(cols, r)) for r in cur.fetchall()] if cols else []
            rows, seconds = timed(go)
        finally:
            cur.close()
        if meter and self.meter is not None:
            self.meter.add(phase, seconds)
        return rows

    def _warehouse_info(self) -> dict:
        return self._warehouse_info_of(self.w, self.warehouse_id)

    @staticmethod
    def _warehouse_info_of(w, warehouse_id: str) -> dict:
        try:
            wh = w.warehouses.get(warehouse_id)
        except Exception as e:  # noqa: BLE001
            return {"name": warehouse_id, "error": str(e)[:200]}
        state = getattr(wh.state, "value", wh.state)
        return {"name": wh.name, "id": warehouse_id, "size": size_name(wh.cluster_size), "state": state,
                "auto_stop_mins": wh.auto_stop_mins, "serverless": wh.enable_serverless_compute}

    def extract(self, f: Filters) -> Extract:
        meta = {"source": "databricks", "warehouse": self.warehouse, "notes": [],
                "default_namespace": ".".join(p for p in self.namespace if p)}
        meta["notes"].append("Databricks' history does not record each statement's session catalog and schema: "
                             f"unqualified names were resolved in {meta['default_namespace'] or 'no namespace'}.")
        try:
            rows = self._run(*system_history_sql(f))
            groups, totals = groups_from_system_history(rows, self.namespace)
            meta.update(history="system.query.history", start=iso(f.start), end=iso(f.end), totals=totals)
        except Exception as e:  # noqa: BLE001 - not granted: the API
            meta["notes"].append(f"system.query.history could not be read ({_short(e)}); used the Query History API, "
                                 "which lists the statements this user may see.")
            if f.roles:
                meta["notes"].append("Databricks has no roles: --roles was ignored.")
            execs = executions_from_api(self._api_queries(f), f, self.namespace,
                                        {self.warehouse_id: self.warehouse.get("size")})
            groups = workload.aggregate_rows(execs)
            groups.sort(key=lambda g: -g.elapsed_ms_total)
            totals = {"groups": len(groups), "executions": sum(g.executions for g in groups),
                      "elapsed_ms": sum(g.elapsed_ms_total for g in groups)}
            groups = groups[:f.max_groups]
            meta.update(history="Query History API", start=iso(f.start), end=iso(f.end), totals=totals)
        return Extract(groups, meta)

    def _api_queries(self, f: Filters):
        from databricks.sdk.service.sql import QueryFilter, TimeRange

        flt = QueryFilter(query_start_time_range=TimeRange(start_time_ms=int(f.start.timestamp() * 1000),
                                                           end_time_ms=int(f.end.timestamp() * 1000)),
                          warehouse_ids=list(f.warehouses) or None)
        token = None
        while True:
            res = self.w.query_history.list(filter_by=flt, include_metrics=True, max_results=1000, page_token=token)
            yield from res.res or []
            if not res.has_next_page or not res.next_page_token:
                return
            token = res.next_page_token

    def table_info(self, refs) -> dict[tuple, TableInfo]:
        out = {}
        for r in list(refs)[:200]:
            if not r.resolved or not all(re.fullmatch(r"[\w$-]+", p) for p in r.key):
                continue
            try:
                row = self._run(f"/* {MARKER} */ DESCRIBE DETAIL " + ".".join(f"`{p}`" for p in r.key))[0]
            except Exception:  # noqa: BLE001 - a view, or not visible
                continue
            out[r.key] = TableInfo(name=r.name, kind=row.get("format"), bytes=row.get("sizeinbytes"),
                                   last_altered=iso(row.get("lastmodified")))
        return out

    def spend(self, start: dt.datetime, end: dt.datetime) -> Spend:
        try:
            rows = self._run(*billing_sql(start, end))
        except Exception as e:  # noqa: BLE001
            return Spend(unit=UNIT, note=f"could not read system.billing.usage ({_short(e)})")
        by = {r["warehouse"]: float(r["dbus"] or 0) for r in rows}
        return Spend(unit=UNIT, by_warehouse=by, total=sum(by.values()), source="system.billing.usage")

    def rerun(self, shape, timeout: int, max_rows: int) -> Rerun:
        problem = statements.read_only_problem(shape.text, DIALECT)
        if problem:
            return Rerun(ok=False, skipped=f"not run on the source: {problem}")
        cur = self.conn.cursor()
        start = time.perf_counter()
        try:
            ns = (shape.database, shape.schema)
            if ns != self._namespace_in_use and shape.database:
                cur.execute(f"USE CATALOG {_tick(shape.database)}")
                if shape.schema:
                    cur.execute(f"USE SCHEMA {_tick(shape.schema)}")
                self._namespace_in_use = ns
            cur.execute(f"/* {MARKER} */ SET STATEMENT_TIMEOUT = {int(timeout)}")

            def go():
                cur.execute(shape.text)
                cols = [c[0] for c in cur.description]
                rows = cur.fetchmany(max_rows + 1)
                if len(rows) > max_rows:
                    raise OverflowError
                return compare.digest(rows, cols)
            d, seconds = timed(go)
            return Rerun(ok=True, seconds=seconds, rows=d.rows, checksum=d.checksum, columns=d.columns,
                         values=d.values)
        except OverflowError:
            return Rerun(ok=False, skipped=f"returned more than {max_rows} rows")
        except Exception as e:  # noqa: BLE001
            return Rerun(ok=False, error=_short(e))
        finally:
            # Failed and canceled runs used the warehouse too.
            self.meter.add("comparison", time.perf_counter() - start)
            try:
                cur.execute(f"/* {MARKER} */ SET STATEMENT_TIMEOUT = {int(self.timeout)}")
            except Exception:  # noqa: BLE001
                pass
            cur.close()


def _tick(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _short(e: Exception) -> str:
    text = str(e).strip().splitlines()
    return " ".join(text[:2])[:240] if text else type(e).__name__
