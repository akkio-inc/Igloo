"""Igloo: the tables it has, and running statements on it with a timeout."""

from __future__ import annotations

import os
import re
import threading
import time
from dataclasses import asdict, dataclass, field

from . import compare, sqltext

APPLICATION = "igloo-migrate"
SKIP_CATALOGS = {"memory", "system", "temp"}


@dataclass
class IglooTable:
    catalog: str
    schema: str
    name: str
    kind: str  # mirrored (a table of Igloo's catalog) or connection (reached through a connection)
    rows: int | None = None
    bytes: int | None = None
    snapshot: str | None = None  # when its data last changed: the mirror's snapshot time

    @property
    def full(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.name}"

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.catalog.lower(), self.schema.lower(), self.name.lower())

    def to_dict(self):
        return asdict(self) | {"full": self.full}


@dataclass
class Attempt:
    """One statement run on Igloo."""

    how: str  # as written, native dialect, sqlglot
    sql: str | None
    ok: bool = False
    seconds: float | None = None
    rows: int | None = None
    checksum: str | None = None
    columns: list[str] | None = None
    counted: bool = False  # rows counted on the server instead of fetched
    error_class: str | None = None
    sqlstate: str | None = None
    error: str | None = None
    warnings: list[str] | None = None  # sqlglot's notes on what it could not translate
    values: list[tuple] | None = field(default=None, repr=False)  # a small result's rows, kept in memory only

    def to_dict(self):
        d = asdict(self)
        d.pop("values")
        return d


# (class, pattern) in order: DuckDB's message prefixes are more specific
# than the SQLSTATEs Igloo maps them to (a missing function is 42P01, as a
# missing table is).
ERROR_CLASSES = [
    ("timeout", r"statement timeout|canceling statement|57014"),
    ("permission denied", r"permission denied|42501"),
    ("connection lost", r"server closed the connection|connection (?:is )?(?:lost|closed)|terminating connection"),
    ("syntax", r"^Parser Error|syntax error|42601"),
    ("unknown function", r"(?:Scalar|Table|Aggregate|Pragma|Macro) Function with name .* "
                         r"(?:does not exist|is not in the catalog)|function .* does not exist|42883"),
    ("unknown type", r"Type with name .* does not exist|type \"?[\w.]+\"? does not exist|42704"),
    ("unknown table", r"Table with name .* does not exist|Table Function with name|relation .* does not exist|"
                      r"Catalog .* does not exist|Schema with name .* does not exist|42P01"),
    ("unknown column", r"Referenced column .* not found|column .* (?:not found|does not exist)|42703"),
    ("type mismatch", r"No function matches|Could not choose a best candidate|Cannot compare|Cannot mix|"
                      r"Binder Error: .*type|42804|42725"),
    ("conversion", r"^Conversion Error|Could not convert|Invalid Input Error|22P02|22018|22007|22008|22003"),
    ("not supported", r"Not implemented|not supported|0A000"),
    ("out of memory", r"Out of Memory|53200"),
    ("internal error", r"INTERNAL Error|XX000"),
    ("binder", r"^Binder Error"),
    ("catalog", r"^Catalog Error"),
]


def error_class(sqlstate: str | None, message: str) -> str:
    text = f"{message}\n{sqlstate or ''}"
    for name, pattern in ERROR_CLASSES:
        if re.search(pattern, text, re.I | re.M):
            return name
    return "other"


def error_pattern(cls: str, message: str | None) -> str:
    """A message reduced to what is common to every statement it fails: the
    first line without names and values, except the missing function's
    name, which says what to add."""
    line = (sqltext.first_line(sqltext.redact_message(message)) or "").strip()
    line = re.sub(r"\s*Did you mean.*$", "", line)
    if cls == "unknown function":
        m = re.search(r'Function with name "?([\w$]+)"? (?:does not exist|is not in the catalog)', line) or \
            re.search(r'function "?([\w$.]+)', line)
        if m:
            return f"function {m.group(1).lower()} does not exist"
    line = re.sub(r'"[^"]*"', '"…"', line)
    line = re.sub(r"\b\d+\b", "?", line)
    return line


class Igloo:
    def __init__(self, dsn: str, timeout: int, password: str | None = None):
        self.dsn = dsn
        self.timeout = timeout
        self.password = password or os.environ.get("IGLOO_PASSWORD")
        self.conns: dict[str | None, object] = {}

    def connect(self, dialect: str | None = None):
        import psycopg

        conn = psycopg.connect(self.dsn, autocommit=True, connect_timeout=30, application_name=APPLICATION,
                               **({"password": self.password} if self.password else {}))
        conn.execute(f"SET statement_timeout = '{int(self.timeout)}s'")
        conn.execute(f"SET query_tag = '{APPLICATION}'")
        if dialect:
            conn.execute(f"SET sql_dialect = '{dialect}'")
        return conn

    def conn(self, dialect: str | None = None):
        c = self.conns.get(dialect)
        if c is None or c.closed or c.broken:
            c = self.conns[dialect] = self.connect(dialect)
        return c

    def close(self):
        for c in self.conns.values():
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass

    def native_dialect(self, source: str) -> tuple[str | None, str | None]:
        """Whether this Igloo has a dialect of the source's own SQL, which
        `SET sql_dialect` turns on; (name, None) if so, else (None, why)."""
        try:
            self.conn(source)
            return source, None
        except Exception as e:  # noqa: BLE001
            self.conns.pop(source, None)
            return None, sqltext.first_line(str(e))

    def tables(self) -> tuple[list[IglooTable], str]:
        """Igloo's tables and views, and those of its connections, with the
        size and snapshot time of the catalog's own tables."""
        conn = self.conn()
        out = {}
        lake = None
        for cat, sch, name, rows, nbytes, altered in conn.execute(
                "SELECT table_catalog, table_schema, table_name, row_count, bytes, last_altered FROM igloo.tables"
        ).fetchall():
            lake = lake or cat
            t = IglooTable(cat, sch, name, "mirrored", rows, nbytes, altered.isoformat() if altered else None)
            out[t.key] = t
        lake = lake or "lake"
        for cat, sch, name in conn.execute(
                "SELECT table_catalog, table_schema, table_name FROM information_schema.tables").fetchall():
            if cat in SKIP_CATALOGS or sch in ("information_schema", "pg_catalog"):
                continue
            t = IglooTable(cat, sch, name, "mirrored" if cat == lake else "connection")
            out.setdefault(t.key, t)
        return sorted(out.values(), key=lambda t: t.key), lake

    def run(self, sql: str, how: str, *, search_path: str | None, dialect: str | None = None, max_rows: int,
            count_only: bool = False, warnings: list[str] | None = None) -> Attempt:
        """Runs one statement and digests its result. With count_only, the
        server counts the rows instead of sending them."""
        attempt = Attempt(how=how, sql=sql, warnings=warnings or None)
        try:
            conn = self.conn(dialect)
        except Exception as e:  # noqa: BLE001
            attempt.error_class, attempt.error = "connection lost", sqltext.first_line(str(e))
            return attempt
        text = sql.strip().rstrip(";")
        if count_only:
            text = f"SELECT count(*) FROM (\n{text}\n) AS igloo_migrate_q"
        # statement_timeout bounds the server; this bounds the client, should
        # the server not answer at all.
        timer = threading.Timer(self.timeout + 30, lambda: _cancel(conn))
        timer.start()
        try:
            with conn.cursor() as cur:
                if search_path:
                    cur.execute(f"SET search_path = '{search_path}'")
                start = time.perf_counter()
                cur.execute(text)
                if cur.description is None:
                    attempt.rows = cur.rowcount
                    d = None
                elif count_only:
                    attempt.rows = cur.fetchone()[0]
                    attempt.counted = True
                    d = None
                else:
                    d = compare.digest(cur.fetchall(), [c.name for c in cur.description])
                attempt.seconds = time.perf_counter() - start
            if d is not None:
                attempt.rows, attempt.checksum, attempt.columns = d.rows, d.checksum, d.columns
                attempt.values = d.values
                if d.rows > max_rows:
                    attempt.checksum = None
            attempt.ok = True
        except Exception as e:  # noqa: BLE001 - record and go on
            state = getattr(e, "sqlstate", None)
            attempt.sqlstate = state
            attempt.error = str(e).strip()
            attempt.error_class = error_class(state, attempt.error)
            if getattr(conn, "broken", False) or getattr(conn, "closed", False):
                self.conns.pop(dialect, None)
        finally:
            timer.cancel()
        return attempt


def _cancel(conn):
    try:
        conn.cancel_safe()
    except Exception:  # noqa: BLE001
        pass
