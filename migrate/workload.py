"""The workload: executions from the source's history, grouped into
shapes.

A source returns groups: executions it already considers the same query
(Snowflake: the same query_parameterized_hash, Databricks: the same text)
in the same session database and schema. Groups become shapes here: groups
whose normalized text is the same, or that share a parameterized hash, and
that read and write the same tables. Snowflake's hash counts comments, so
the same query tagged differently on each run is many groups and one shape;
and it ignores the session's schema, so the same text run against two
schemas is one hash and two shapes.
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass, field

from . import sqltext, statements


@dataclass
class Group:
    """Executions the source considers one query, with what the history
    recorded about them."""

    key: str  # the source's grouping key: a parameterized hash, or a hash of the text
    text: str  # the most recent successful execution's statement (else the most recent)
    query_id: str | None
    source_type: str | None  # Snowflake's QUERY_TYPE, Databricks' statement_type
    database: str | None  # the session's database (Databricks: catalog) when it ran
    schema: str | None
    executions: int
    succeeded: int
    failed: int
    elapsed_ms_total: float = 0.0  # total elapsed time, including queuing and compilation
    execution_ms_total: float = 0.0  # time executing
    elapsed_ms_median: float | None = None  # of successful executions
    rows_median: float | None = None  # rows produced by successful executions
    rows_last: int | None = None  # rows the representative execution produced
    bytes_scanned_total: float | None = None
    credits: float | None = None  # compute credits attributed to these executions, where the source records them
    users: list[str] = field(default_factory=list)
    roles: list[str] = field(default_factory=list)
    warehouses: list[str] = field(default_factory=list)
    warehouse_sizes: list[str] = field(default_factory=list)
    first_seen: str | None = None
    last_seen: str | None = None
    rep_start: str | None = None  # when the representative execution started
    error: str | None = None  # the source's error, for groups that never succeeded
    tables_read: list[str] | None = None  # from the source's access history, when available
    tables_written: list[str] | None = None

    @classmethod
    def from_dict(cls, d: dict) -> Group:
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class Shape:
    id: str
    cls: str  # one of statements.CLASSES
    normalized: str
    text: str  # the representative statement, literals and all; reports redact it unless asked not to
    query_id: str | None
    database: str | None
    schema: str | None
    source_types: list[str]
    executions: int
    succeeded: int
    failed: int
    elapsed_ms_total: float
    execution_ms_total: float
    elapsed_ms_median: float | None
    rows_median: float | None
    rows_last: int | None
    bytes_scanned_total: float | None
    credits: float | None
    users: list[str]
    roles: list[str]
    warehouses: list[str]
    warehouse_sizes: list[str]
    first_seen: str | None
    last_seen: str | None
    rep_start: str | None
    source_error: str | None
    reads: list[statements.TableRef]
    writes: list[statements.TableRef]
    target_must_exist: bool
    parsed: bool
    volatility: list[str]
    groups: int
    tables_from: str  # "access history" or "parsed"

    def to_dict(self) -> dict:
        d = asdict(self)
        d["reads"] = [asdict(r) for r in self.reads]
        d["writes"] = [asdict(r) for r in self.writes]
        return d


def _weighted_median(pairs: list[tuple[float, int]]) -> float | None:
    pairs = sorted((v, w) for v, w in pairs if v is not None and w > 0)
    if not pairs:
        return None
    half, run = sum(w for _, w in pairs) / 2, 0
    for v, w in pairs:
        run += w
        if run >= half:
            return v
    return pairs[-1][0]


def _refs_from_names(names: list[str], dialect: str) -> list[statements.TableRef]:
    out = []
    for n in names:
        parts = [p.strip('"') for p in _split_name(n)]
        key = tuple(p.lower() for p in ([""] * (3 - len(parts)) + parts)[-3:])
        kind = "table"
        if key[0] in statements.SYSTEM_DATABASES.get(dialect, set()) or key[1] in statements.SYSTEM_SCHEMAS:
            kind = "system"
        out.append(statements.TableRef(written=tuple(p.lower() for p in parts), key=key, name=".".join(parts),
                                       kind=kind))
    return out


def _split_name(name: str) -> list[str]:
    """DB.SCHEMA."Mixed.Case" -> ['DB', 'SCHEMA', '"Mixed.Case"']"""
    parts, cur, quoted = [], "", False
    for ch in name:
        if ch == '"':
            quoted = not quoted
            cur += ch
        elif ch == "." and not quoted:
            parts.append(cur)
            cur = ""
        else:
            cur += ch
    parts.append(cur)
    return parts


@dataclass
class _Analyzed:
    group: Group
    cls: str
    normalized: str
    reads: list[statements.TableRef]
    writes: list[statements.TableRef]
    target_must_exist: bool
    parsed: bool
    tables_from: str


def analyze(group: Group, dialect: str) -> _Analyzed:
    tree = statements.try_parse(group.text, dialect)
    cls = statements.classify(group.source_type, group.text, dialect, tree)
    found = statements.tables(tree, dialect, group.database, group.schema) if tree is not None else statements.Tables(
        parsed=False)
    if tree is None:
        found.parsed = False
    reads, writes, tables_from = found.reads, found.writes, "parsed"
    if group.tables_read is not None or group.tables_written is not None:
        # The source's access history is authoritative for what a statement
        # touched; keep parsed stages and table functions, which it omits,
        # and the parsed reference where it is the same table, which knows
        # how the statement wrote its name (for renaming it).
        parsed = {r.key: r for r in found.reads + found.writes}
        reads = [parsed.get(r.key, r) for r in _refs_from_names(group.tables_read or [], dialect)]
        reads += [r for r in found.reads if r.kind != "table"]
        writes = [parsed.get(r.key, r) for r in _refs_from_names(group.tables_written or [], dialect)] or found.writes
        tables_from = "access history"
    return _Analyzed(group, cls, sqltext.normalize(group.text, dialect), reads, writes, found.target_must_exist,
                     tree is not None, tables_from)


def build_shapes(groups: list[Group], dialect: str) -> list[Shape]:
    """Merges groups into shapes; see the module's description."""
    analyzed = [analyze(g, dialect) for g in groups]
    parent = list(range(len(analyzed)))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def tables_key(a: _Analyzed):
        return (tuple(sorted(r.key for r in a.reads)), tuple(sorted(r.key for r in a.writes)))

    first: dict = {}
    for i, a in enumerate(analyzed):
        for k in (("text", a.normalized), ("hash", a.group.key)):
            k = (k, tables_key(a))
            if k in first:
                parent[find(i)] = find(first[k])
            else:
                first[k] = i
    members: dict[int, list[_Analyzed]] = {}
    for i, a in enumerate(analyzed):
        members.setdefault(find(i), []).append(a)

    shapes = []
    for items in members.values():
        # The representative: the most executed group's latest successful run.
        rep = max(items, key=lambda a: (a.group.succeeded > 0, a.group.executions, a.group.rep_start or ""))
        g = rep.group
        gs = [a.group for a in items]
        tk = tables_key(rep)
        shape_id = hashlib.sha256((rep.normalized + repr(tk)).encode()).hexdigest()[:12]
        succeeded = sum(x.succeeded for x in gs)

        def union(attr):
            return sorted({v for x in gs for v in (getattr(x, attr) or []) if v})

        def total(attr):
            vals = [getattr(x, attr) for x in gs if getattr(x, attr) is not None]
            return sum(vals) if vals else None

        shapes.append(Shape(
            id=shape_id, cls=rep.cls, normalized=rep.normalized, text=g.text, query_id=g.query_id,
            database=g.database, schema=g.schema, source_types=sorted({x.source_type or "" for x in gs} - {""}),
            executions=sum(x.executions for x in gs), succeeded=succeeded, failed=sum(x.failed for x in gs),
            elapsed_ms_total=sum(x.elapsed_ms_total or 0 for x in gs),
            execution_ms_total=sum(x.execution_ms_total or 0 for x in gs),
            elapsed_ms_median=_weighted_median([(x.elapsed_ms_median, x.succeeded) for x in gs]),
            rows_median=_weighted_median([(x.rows_median, x.succeeded) for x in gs]), rows_last=g.rows_last,
            bytes_scanned_total=total("bytes_scanned_total"), credits=total("credits"),
            users=union("users"), roles=union("roles"), warehouses=union("warehouses"),
            warehouse_sizes=union("warehouse_sizes"),
            first_seen=min((x.first_seen for x in gs if x.first_seen), default=None),
            last_seen=max((x.last_seen for x in gs if x.last_seen), default=None), rep_start=g.rep_start,
            source_error=None if succeeded else next((x.error for x in gs if x.error), None),
            reads=rep.reads, writes=rep.writes, target_must_exist=rep.target_must_exist, parsed=rep.parsed,
            volatility=sorted(sqltext.volatility(g.text, dialect)), groups=len(gs), tables_from=rep.tables_from,
        ))
    shapes.sort(key=lambda s: (-s.elapsed_ms_total, -s.executions, s.id))
    return shapes


def aggregate_rows(rows: list[dict]) -> list[Group]:
    """Groups per-execution rows (the sources' fallbacks return executions,
    not groups). Each row has the Group's per-execution fields: key, text,
    query_id, source_type, database, schema, status ('success', 'failed'),
    elapsed_ms, execution_ms, rows, bytes_scanned, credits, user, role,
    warehouse, warehouse_size, start, error."""
    by_key: dict[tuple, list[dict]] = {}
    for r in rows:
        by_key.setdefault((r["key"], r.get("database"), r.get("schema"), r.get("source_type")), []).append(r)
    groups = []
    for (key, database, schema, source_type), rs in by_key.items():
        ok = [r for r in rs if r["status"] == "success"]
        rep = max(ok or rs, key=lambda r: r.get("start") or "")
        elapsed_ok = sorted(r["elapsed_ms"] for r in ok if r.get("elapsed_ms") is not None)
        rows_ok = sorted(r["rows"] for r in ok if r.get("rows") is not None)
        credits = [r["credits"] for r in rs if r.get("credits") is not None]
        scanned = [r["bytes_scanned"] for r in rs if r.get("bytes_scanned") is not None]
        groups.append(Group(
            key=key, text=rep["text"], query_id=rep.get("query_id"), source_type=source_type, database=database,
            schema=schema, executions=len(rs), succeeded=len(ok), failed=len(rs) - len(ok),
            elapsed_ms_total=sum(r.get("elapsed_ms") or 0 for r in rs),
            execution_ms_total=sum(r.get("execution_ms") or 0 for r in rs),
            elapsed_ms_median=_median(elapsed_ok), rows_median=_median(rows_ok), rows_last=rep.get("rows"),
            bytes_scanned_total=sum(scanned) if scanned else None, credits=sum(credits) if credits else None,
            users=sorted({r["user"] for r in rs if r.get("user")}), roles=sorted({r["role"] for r in rs if r.get("role")}),
            warehouses=sorted({r["warehouse"] for r in rs if r.get("warehouse")}),
            warehouse_sizes=sorted({r["warehouse_size"] for r in rs if r.get("warehouse_size")}),
            first_seen=min((r["start"] for r in rs if r.get("start")), default=None),
            last_seen=max((r["start"] for r in rs if r.get("start")), default=None), rep_start=rep.get("start"),
            error=None if ok else rep.get("error"),
        ))
    return groups


def _median(vals: list[float]) -> float | None:
    if not vals:
        return None
    n = len(vals)
    return vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2
