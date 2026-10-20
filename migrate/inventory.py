"""Which of the workload's tables Igloo has, and under what names.

A source table is found in Igloo by the first of:

1. a --map rule, SOURCE=TARGET, such as
   `SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.*=lake.tpch_sf10.*`: dotted names with
   `*` for any one part, each `*` of the target taking the part its
   counterpart matched (a rule names the table it maps to whether or not
   Igloo has it);
2. a table of Igloo's catalog with the same schema and name;
3. a table in an Igloo connection named like the source database, with the
   same schema and name.

Names match without regard to case.
"""

from __future__ import annotations

import datetime as dt
import fnmatch
import re
from dataclasses import asdict, dataclass, field

from .igloo import IglooTable


@dataclass
class Rule:
    source: tuple[str, ...]
    target: tuple[str, ...]

    @classmethod
    def parse(cls, text: str, lake: str) -> Rule:
        if "=" not in text:
            raise ValueError(f"--map {text!r}: expected SOURCE=TARGET")
        src, dst = (s.strip() for s in text.split("=", 1))
        sp, dp = _split(src), _split(dst)
        if not 1 <= len(sp) <= 3 or not 2 <= len(dp) <= 3:
            raise ValueError(f"--map {text!r}: the source has 1 to 3 parts, the target schema.table or "
                             "catalog.schema.table")
        if len(dp) == 2:
            dp = [lake] + dp
        if sum("*" in p for p in dp) > sum("*" in p for p in sp) or any("*" in p and p != "*" for p in dp):
            raise ValueError(f"--map {text!r}: each * of the target must be a whole part, with one in the source "
                             "for it")
        return cls(tuple(p.lower() for p in sp), tuple(p.lower() for p in dp))

    def apply(self, key: tuple[str, str, str]) -> tuple[str, str, str] | None:
        parts = key[-len(self.source):]
        if len(self.source) > len(key) or not all(fnmatch.fnmatchcase(p, s) for p, s in zip(parts, self.source)):
            return None
        stars = iter(p for p, s in zip(parts, self.source) if "*" in s)
        return tuple(next(stars) if d == "*" else d for d in self.target)


def _split(name: str) -> list[str]:
    return [p.strip('"`') for p in re.findall(r'"[^"]*"|`[^`]*`|[^.]+', name)]


@dataclass
class Match:
    table: IglooTable | None  # None: mapped by a rule to a table Igloo does not have
    target: tuple[str, str, str]
    how: str  # a rule, same schema and name, a connection named like the database


class Mapper:
    def __init__(self, tables: list[IglooTable], lake: str, rules: list[Rule]):
        self.tables = {t.key: t for t in tables}
        self.lake = lake.lower()
        self.rules = rules

    def match(self, key: tuple[str, str, str]) -> Match | None:
        if not all(key):
            return None
        for rule in self.rules:
            target = rule.apply(key)
            if target is not None:
                return Match(self.tables.get(target), target, f"--map {'.'.join(rule.source)}")
        same = (self.lake, key[1], key[2])
        if same in self.tables:
            return Match(self.tables[same], same, "same schema and name")
        conn = (key[0], key[1], key[2])
        if conn in self.tables and self.tables[conn].kind == "connection":
            return Match(self.tables[conn], conn, "connection named like the database")
        return None

    def namespace(self, db: str | None, schema: str | None) -> str | None:
        """The Igloo search path for statements whose session was in db.schema:
        where their unqualified table names should resolve."""
        if not db or not schema:
            return None
        probe = (db.lower(), schema.lower(), "\x00")
        for rule in self.rules:
            target = rule.apply(probe)
            if target is not None:
                return f"{target[0]}.{target[1]}"
        if any(k[0] == self.lake and k[1] == schema.lower() for k in self.tables):
            return f"{self.lake}.{schema.lower()}"
        if any(k[0] == db.lower() and k[1] == schema.lower() for k in self.tables):
            return f"{db.lower()}.{schema.lower()}"
        return None


def ident(name: str, quote: str = '"') -> str:
    """A name part as SQL: bare when it is a plain lower-case name."""
    if re.fullmatch(r"[a-z_][a-z0-9_$]*", name):
        return name
    end = {"`": "`"}.get(quote, '"')
    return quote + name.replace(end, end * 2) + end


def qualified(target: tuple[str, str, str], quote: str = '"') -> str:
    return ".".join(ident(p, quote) for p in target)


@dataclass
class Coverage:
    """Whether a shape's data is in Igloo."""

    status: str  # ready, no data, source metadata, unresolved
    missing: list[str] = field(default_factory=list)
    search_path: str | None = None
    renames: dict[tuple[str, ...], tuple[str, str, str]] = field(default_factory=dict)  # written -> Igloo target
    note: str | None = None

    def text_renames(self, quote: str = '"') -> dict[tuple[str, ...], str]:
        return {k: qualified(v, quote) for k, v in self.renames.items()}


def coverage(shape, mapper: Mapper) -> Coverage:
    search_path = mapper.namespace(shape.database, shape.schema)
    reads = list(shape.reads)
    writes = list(shape.writes) if shape.target_must_exist else []
    if any(r.kind == "system" for r in reads):
        return Coverage("source metadata", [r.name for r in reads if r.kind == "system"])
    if not shape.parsed and shape.tables_from == "parsed":
        return Coverage("ready", [], search_path, note="its tables are unknown: sqlglot cannot parse it")
    missing, renames = [], {}
    for r in reads + writes:
        if r.kind == "stage":
            missing.append(f"{r.name} (files in a stage)")
            continue
        if r.kind != "table":
            continue
        m = mapper.match(r.key)
        if m is None or m.table is None:
            missing.append(r.name if r.resolved else f"{r.name} (session database or schema unknown)")
            continue
        target = m.table.key
        if not r.written:
            continue
        if len(r.written) == 1 and search_path and f"{target[0]}.{target[1]}" == search_path and \
                target[2] == r.written[0]:
            continue  # resolves through the search path
        renames[r.written] = (m.table.catalog, m.table.schema, m.table.name)
    if missing:
        return Coverage("no data", missing, search_path, renames)
    return Coverage("ready", [], search_path, renames)


@dataclass
class TableUse:
    key: tuple[str, str, str]
    name: str
    read_by: int = 0  # shapes
    written_by: int = 0
    executions: int = 0
    elapsed_ms: float = 0.0
    source: dict | None = None  # rows, bytes, last_altered at the source
    igloo: dict | None = None  # the Igloo table it maps to
    status: str = "missing"  # mirrored, connection, missing
    how: str | None = None
    notes: list[str] = field(default_factory=list)

    def to_dict(self):
        d = asdict(self)
        d["key"] = ".".join(self.key)
        return d


def table_uses(shapes, mapper: Mapper, source_info: dict) -> list[TableUse]:
    uses: dict[tuple, TableUse] = {}
    for s in shapes:
        if s.cls in ("DDL", "other"):
            continue  # SHOW, GRANT and the like name objects without reading their data
        for refs, attr in ((s.reads, "read_by"), (s.writes, "written_by")):
            for r in refs:
                if r.kind != "table":
                    continue
                u = uses.setdefault(r.key, TableUse(r.key, r.name))
                setattr(u, attr, getattr(u, attr) + 1)
                u.executions += s.executions
                u.elapsed_ms += s.elapsed_ms_total
    for u in uses.values():
        info = source_info.get(u.key)
        if info is not None:
            u.source = {"rows": info.rows, "bytes": info.bytes, "last_altered": info.last_altered, "kind": info.kind}
        m = mapper.match(u.key) if all(u.key) else None
        if m is not None:
            u.how = m.how
        if m is None or m.table is None:
            u.status = "missing"
            if m is not None:
                u.notes.append(f"mapped to {'.'.join(m.target)}, which Igloo does not have")
            if not all(u.key):
                u.notes.append("the session's database or schema is unknown")
            continue
        t = m.table
        u.status = t.kind
        u.igloo = t.to_dict()
        if info is not None and info.rows is not None and t.rows is not None and info.rows != t.rows:
            u.notes.append(f"rows differ: {info.rows:,} at the source, {t.rows:,} in Igloo")
        if info is not None and info.last_altered and t.snapshot and _ts(info.last_altered) > _ts(t.snapshot):
            u.notes.append("changed at the source after Igloo's snapshot")
    return sorted(uses.values(), key=lambda u: (-u.executions, u.name))


def _ts(s: str) -> dt.datetime:
    t = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    return t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)
