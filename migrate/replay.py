"""Replaying shapes on Igloo, and comparing their results with the source.

Each shape that reads data Igloo has is run as written first, with only its
table names changed to Igloo's. If that fails, it runs in Igloo's own
dialect of the source's SQL (`SET sql_dialect`), where this Igloo has one,
and translated to DuckDB's dialect by sqlglot; both are recorded, so the
report can compare them.
"""

from __future__ import annotations

import datetime as dt
import random
import re
from dataclasses import asdict, dataclass, field

from . import compare, inventory, sqltext, statements
from .igloo import Attempt, Igloo

OUTCOMES = ("unchanged", "translated", "failed", "no data", "source metadata", "failed on source", "not replayed",
            "not in sample")
HOWS = ("as written", "native dialect", "sqlglot")
# What a history shows in place of text the reader may not see (Databricks'
# Query History API, for other users' statements).
HIDDEN_TEXT = {"", "<REDACTED>"}


@dataclass
class Result:
    shape_id: str
    outcome: str
    reason: str | None = None
    coverage: dict | None = None
    attempts: list[Attempt] = field(default_factory=list)
    works_with: list[str] = field(default_factory=list)
    comparison: dict | None = None

    @property
    def best(self) -> Attempt | None:
        for how in HOWS:
            for a in self.attempts:
                if a.how == how and a.ok:
                    return a
        return None

    @property
    def failure(self) -> Attempt | None:
        """The attempt whose error the report shows: the translation's, which
        is closest to running, else the first."""
        failed = [a for a in self.attempts if not a.ok]
        for how in ("sqlglot", "native dialect", "as written"):
            for a in failed:
                if a.how == how and a.error_class != "translation":
                    return a
        return failed[0] if failed else None

    def to_dict(self):
        d = asdict(self)
        d["attempts"] = [a.to_dict() for a in self.attempts]
        return d


@dataclass
class Settings:
    source_dialect: str
    native: str | None
    max_rows: int
    replay_writes: bool
    scratch: tuple[str, str] | None  # (catalog, schema)
    keep_scratch: bool = False


def plan(shapes, mapper: inventory.Mapper, settings: Settings, sample: int, sample_by: str, seed: int):
    """The outcome of every shape that will not be replayed, and the shapes
    that will: up to `sample` of those whose data Igloo has."""
    results, eligible, coverages = {}, [], {}
    for s in shapes:
        cov = inventory.coverage(s, mapper)
        coverages[s.id] = cov
        cd = {"status": cov.status, "missing": cov.missing, "search_path": cov.search_path,
              "renames": {".".join(k): ".".join(v) for k, v in cov.renames.items()}, "note": cov.note}
        r = Result(s.id, "", coverage=cd)
        results[s.id] = r
        if s.text.strip() in HIDDEN_TEXT:
            r.outcome, r.reason = "not replayed", "the source hides the statement's text from this user"
        elif s.succeeded == 0:
            r.outcome, r.reason = "failed on source", sqltext.first_line(sqltext.redact_message(s.source_error))
        elif s.cls in ("DDL", "other"):
            r.outcome, r.reason = "not replayed", f"{s.cls} statements are not replayed"
        elif s.cls in statements.WRITE_CLASSES and not settings.replay_writes:
            r.outcome, r.reason = "not replayed", "writes are replayed only with --replay-writes"
        elif cov.status == "source metadata":
            r.outcome, r.reason = "source metadata", "reads the source's own metadata: " + ", ".join(cov.missing[:3])
        elif cov.status == "no data":
            r.outcome, r.reason = "no data", "Igloo lacks " + ", ".join(cov.missing[:5])
        else:
            eligible.append(s)
    if sample_by == "random":
        random.Random(seed).shuffle(eligible)
    elif sample_by == "executions":
        eligible.sort(key=lambda s: (-s.executions, -s.elapsed_ms_total))
    else:
        eligible.sort(key=lambda s: (-s.elapsed_ms_total, -s.executions))
    chosen = eligible[:sample]
    for s in eligible[sample:]:
        results[s.id].outcome, results[s.id].reason = "not in sample", f"beyond --sample {sample}"
    return results, chosen, coverages


def replay(shape, cov: inventory.Coverage, igloo: Igloo, settings: Settings, result: Result) -> Result:
    if shape.cls in statements.WRITE_CLASSES:
        return _replay_write(shape, cov, igloo, settings, result)
    count_only = max(shape.rows_last or 0, shape.rows_median or 0) > settings.max_rows
    d = settings.source_dialect
    try:
        as_written = sqltext.rename_tables(shape.text, d, cov.text_renames('"'))
    except Exception:  # noqa: BLE001 - the tokenizer rejects it; run it untouched
        as_written = shape.text
    problem = _read_only(as_written, d)
    if problem:
        result.outcome, result.reason = "not replayed", f"not run on Igloo: {problem}"
        return result
    kw = dict(search_path=cov.search_path, max_rows=settings.max_rows, count_only=count_only)
    result.attempts.append(igloo.run(as_written, "as written", **kw))
    if not result.attempts[0].ok:
        if settings.native:
            result.attempts.append(igloo.run(as_written, "native dialect", dialect=settings.native, **kw))
        result.attempts.append(_sqlglot_attempt(shape, cov, igloo, settings, kw))
    return _settle(result)


def _read_only(sql: str, dialect: str) -> str | None:
    problem = statements.read_only_problem(sql, dialect, functions=False)
    if problem and problem.startswith("cannot parse"):
        # The source recorded it as a query; check its words instead.
        return statements.read_only_words(sql, dialect, functions=False)
    return problem


def _sqlglot_attempt(shape, cov, igloo, settings, kw, renames=None) -> Attempt:
    d = settings.source_dialect
    quote = "`" if d == "databricks" else '"'
    try:
        src = sqltext.rename_tables(shape.text, d, renames if renames is not None else cov.text_renames(quote))
    except Exception:  # noqa: BLE001
        src = shape.text
    t = statements.translate(src, read=d)
    if t.sql is None:
        return Attempt(how="sqlglot", sql=None, error_class="translation", error=t.error)
    if shape.cls == "SELECT":
        problem = statements.read_only_problem(t.sql, "duckdb", functions=False)
        if problem:
            return Attempt(how="sqlglot", sql=t.sql, error_class="translation",
                           error=f"the translation is not read-only: {problem}")
    return igloo.run(t.sql, "sqlglot", warnings=t.warnings, **kw)


def _settle(result: Result) -> Result:
    result.works_with = [a.how for a in result.attempts if a.ok]
    if "as written" in result.works_with:
        result.outcome = "unchanged"
    elif result.works_with:
        result.outcome = "translated"
    else:
        result.outcome = "failed"
    return result


def _replay_write(shape, cov, igloo: Igloo, settings: Settings, result: Result) -> Result:
    """Writes run into the scratch schema: the target becomes a table there,
    a zero-copy clone of Igloo's copy of the target when the statement
    writes into an existing table."""
    catalog, scratch = settings.scratch
    target = shape.writes[0] if shape.writes else None
    if target is None or not target.written:
        result.outcome, result.reason = "not replayed", "cannot tell which table it writes"
        return result
    name = f"{target.key[2]}__{shape.id}"
    scratch_t = (catalog, scratch, name)
    renames = dict(cov.renames)
    renames[target.written] = scratch_t
    conn_sql = []
    if shape.target_must_exist:
        src = cov.renames.get(target.written)
        if src is None:
            m = (cov.search_path or "").split(".")
            src = (m[0], m[1], target.written[-1]) if len(m) == 2 else None
        if src is None:
            result.outcome, result.reason = "no data", "Igloo lacks the table it writes into"
            return result
        conn_sql.append(f"CREATE OR REPLACE TABLE {inventory.qualified(scratch_t)} CLONE {inventory.qualified(src)}")
    else:
        conn_sql.append(f"DROP TABLE IF EXISTS {inventory.qualified(scratch_t)}")
    d = settings.source_dialect
    text_renames = {k: inventory.qualified(v) for k, v in renames.items()}
    as_written = sqltext.rename_tables(shape.text, d, text_renames)
    problem = statements.write_target_problem(as_written, d, (catalog.lower(), scratch.lower()))
    if problem:
        result.outcome, result.reason = "not replayed", f"not run on Igloo: {problem}"
        return result
    try:
        conn = igloo.conn()
        for stmt in conn_sql:
            conn.execute(stmt)
    except Exception as e:  # noqa: BLE001
        result.outcome, result.reason = "not replayed", f"could not prepare the scratch table: {sqltext.first_line(str(e))}"
        return result
    kw = dict(search_path=cov.search_path, max_rows=settings.max_rows)
    result.attempts.append(igloo.run(as_written, "as written", **kw))
    if not result.attempts[0].ok:
        if settings.native:
            result.attempts.append(igloo.run(as_written, "native dialect", dialect=settings.native, **kw))
        quote = "`" if d == "databricks" else '"'
        att = _sqlglot_attempt(shape, cov, igloo, settings, kw,
                               renames={k: inventory.qualified(v, quote) for k, v in renames.items()})
        if att.sql and statements.write_target_problem(att.sql, "duckdb", (catalog.lower(), scratch.lower())):
            att = Attempt(how="sqlglot", sql=att.sql, error_class="translation",
                          error="the translation writes outside the scratch schema")
        result.attempts.append(att)
    if not settings.keep_scratch:
        try:
            igloo.conn().execute(f"DROP TABLE IF EXISTS {inventory.qualified(scratch_t)}")
        except Exception:  # noqa: BLE001
            pass
    return _settle(result)


def explanations(shape, uses_by_key: dict) -> list[str]:
    """Why the source's result and Igloo's may differ for reasons other than
    Igloo: the statement depends on the time, on chance or on the session
    (CURRENT_USER and the like), or Igloo's copy of a table is not the
    source's table as it is now."""
    out = []
    for flag, text in (("time", "depends on the current time"), ("random", "depends on chance"),
                       ("session", "depends on the session (user, role, warehouse)")):
        if flag in shape.volatility:
            out.append(text)
    for r in shape.reads:
        u = uses_by_key.get(r.key)
        if u is None:
            continue
        for n in u.notes:
            if n.startswith("rows differ") or n.startswith("changed at the source"):
                out.append(f"{u.name}: {n}")
    return out


def history_check(shape, attempt: Attempt, uses_by_key: dict) -> dict:
    """Igloo's row count against the one the history recorded for the same
    statement, which needs no source warehouse."""
    out = {"method": "history row count", "source_rows": shape.rows_last, "igloo_rows": attempt.rows,
           "explained_by": explanations(shape, uses_by_key), "notes": []}
    if shape.rows_last is None or attempt.rows is None:
        out["verdict"] = "not compared"
        return out
    out["verdict"] = "rows match" if shape.rows_last == attempt.rows else "rows differ"
    # A table changed after the recorded run: the count then need not hold.
    for r in shape.reads:
        u = uses_by_key.get(r.key)
        changed = (u.source or {}).get("last_altered") if u else None
        if changed and shape.rep_start and _ts(changed) > _ts(shape.rep_start):
            out["explained_by"].append(f"{u.name} changed after the recorded run")
    return out


def rerun_check(shape, attempt: Attempt, rerun, uses_by_key: dict) -> dict:
    """The source's result now against Igloo's: row counts and checksums,
    then, for small results that differ, a comparison row by row with
    numbers to the less precise side's precision."""
    out = {"method": "re-run on the source", "source_rows": rerun.rows, "igloo_rows": attempt.rows,
           "source_seconds": rerun.seconds, "cost": rerun.cost, "explained_by": explanations(shape, uses_by_key),
           "notes": []}
    if not rerun.ok:
        out["verdict"] = "not compared"
        out["notes"].append(rerun.skipped or f"the source failed: {sqltext.redact_message(rerun.error)}")
        return out
    digest = compare.Digest(attempt.rows, attempt.checksum, attempt.columns or [],
                            loose=attempt.loose) if attempt.checksum else None
    out["verdict"] = compare.verdict(digest, rerun.rows, rerun.checksum, rerun.loose) if digest else (
        "rows match" if attempt.rows == rerun.rows else "rows differ")
    if out["verdict"] == "values differ" and attempt.values is not None and rerun.values is not None:
        t = compare.tolerant(rerun.values, attempt.values)
        if t.equal:
            out["verdict"] = "match within precision"
            out["notes"].append("numbers agree to the precision of the less precise side")
        else:
            cols = [(rerun.columns or [])[i] if i < len(rerun.columns or []) else str(i) for i in t.differing_columns]
            out["notes"].append(f"{t.differing_rows} of {rerun.rows} rows differ"
                                + (f", in {', '.join(cols)}" if cols else ""))
    if attempt.columns and rerun.columns and attempt.columns != rerun.columns:
        out["notes"].append("column names differ: " + ", ".join(
            f"{a} / {b}" for a, b in zip(rerun.columns, attempt.columns) if a != b)[:200])
    if out["verdict"] == "values differ" and re.search(r"\b(LIMIT|TOP|FETCH)\b", shape.normalized):
        out["explained_by"].append("a LIMIT can pick different rows among ties")
    return out


def _ts(s: str) -> dt.datetime:
    t = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    return t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)
