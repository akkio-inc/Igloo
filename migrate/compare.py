"""Comparing a query's results across engines: a row count and a checksum
that ignores row order, over values written in one canonical form.

Engines return the same value in different types (Snowflake NUMBER(38,2) as
Decimal('5.00'), DuckDB DOUBLE as 5.0, timestamps with or without a time
zone, JSON as text or as objects), and floating-point aggregates differ in
their last digits with the order rows were added in. So numbers are
compared to a number of significant digits, timestamps with a time zone in
UTC, and JSON by its parsed value.
"""

from __future__ import annotations

import datetime as dt
import decimal
import hashlib
import json
import math
import uuid
from dataclasses import dataclass

DIGITS = 10  # significant digits numbers are compared to
MASK = (1 << 64) - 1


def canon(v, digits: int = DIGITS, as_json: bool = False):
    """The canonical text of one value."""
    if v is None:
        return "\\N"
    if as_json and isinstance(v, str):
        try:
            v = json.loads(v)
        except ValueError:
            return v
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, (float, decimal.Decimal)):
        return _number(v, digits)
    if isinstance(v, dt.datetime):
        if v.tzinfo is not None:
            v = v.astimezone(dt.timezone.utc).replace(tzinfo=None).isoformat() + "Z"
            return v
        return v.isoformat()
    if isinstance(v, (dt.date, dt.time)):
        return v.isoformat()
    if isinstance(v, dt.timedelta):
        return _number(v.total_seconds(), digits) + "s"
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).hex()
    if isinstance(v, uuid.UUID):
        return str(v)
    if isinstance(v, (dict, list, tuple)):
        return json.dumps(_canon_json(v, digits), sort_keys=True, separators=(",", ":"))
    return str(v)


def _canon_json(v, digits):
    if isinstance(v, dict):
        return {str(k): _canon_json(x, digits) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_canon_json(x, digits) for x in v]
    if isinstance(v, bool) or v is None or isinstance(v, str):
        return v
    if isinstance(v, (int, float, decimal.Decimal)):
        return _number(v, digits)
    return str(v)


def _number(v, digits: int) -> str:
    if isinstance(v, decimal.Decimal):
        if not v.is_finite():
            return str(v).lower()
        if v == v.to_integral_value():
            return str(int(v))
        v = float(v)
    if math.isnan(v) or math.isinf(v):
        return str(v)
    if v == int(v) and abs(v) < 1e15:
        return str(int(v))
    out = f"{v:.{digits}g}"
    if "e" not in out and "." in out:
        out = out.rstrip("0").rstrip(".")
    return out


KEEP_ROWS = 2000  # results up to this size are kept in memory for a second, tolerant look


@dataclass
class Digest:
    rows: int
    checksum: str
    columns: list[str]
    values: list[tuple] | None = None  # the rows themselves, for small results; never written anywhere


def digest(rows, columns: list[str], json_columns: set[int] = frozenset(), digits: int = DIGITS,
           keep: int = KEEP_ROWS) -> Digest:
    """Row count and order-independent checksum of a result: the sum,
    modulo 2^64, of a hash of each row, so duplicate rows count."""
    total, n, kept = 0, 0, []
    for row in rows:
        text = "\x1f".join(canon(v, digits, i in json_columns) for i, v in enumerate(row))
        total = (total + int.from_bytes(hashlib.sha256(text.encode()).digest()[:8], "big")) & MASK
        n += 1
        if kept is not None:
            kept.append(tuple(_parse_json(v) if i in json_columns else v for i, v in enumerate(row)))
            if len(kept) > keep:
                kept = None
    return Digest(rows=n, checksum=f"{total:016x}", columns=[c.lower() for c in columns], values=kept)


def _parse_json(v):
    if isinstance(v, str):
        try:
            return json.loads(v)
        except ValueError:
            return v
    return v


def verdict(igloo: Digest | None, source_rows: int | None, source_checksum: str | None) -> str:
    """match, values differ, rows differ, or not compared."""
    if igloo is None or source_rows is None:
        return "not compared"
    if igloo.rows != source_rows:
        return "rows differ"
    if source_checksum is None:
        return "rows match"
    return "match" if igloo.checksum == source_checksum else "values differ"


def _scale(v) -> int | None:
    """Decimal places a number was given to: a DECIMAL's scale; None for
    binary floating point, whose last digits carry no meaning."""
    if isinstance(v, decimal.Decimal) and v.is_finite():
        return max(-v.as_tuple().exponent, 0)
    if isinstance(v, int) and not isinstance(v, bool):
        return 0
    return None


def close(a, b) -> bool:
    """Whether two values are the same, numbers to the precision of the less
    precise: Snowflake's AVG of a NUMBER(12,2) has 8 decimal places where
    DuckDB's is a double, and the two agree to those 8."""
    numeric = (int, float, decimal.Decimal)
    if isinstance(a, numeric) and isinstance(b, numeric) and not isinstance(a, bool) and not isinstance(b, bool):
        if a == b:
            return True
        fa, fb = float(a), float(b)
        if math.isnan(fa) or math.isnan(fb) or math.isinf(fa) or math.isinf(fb):
            return fa == fb or (math.isnan(fa) and math.isnan(fb))
        if abs(fa - fb) <= 1e-9 * max(abs(fa), abs(fb)):
            return True
        # The more precise, rounded or cut to the less precise one's decimal
        # places, is the less precise: 0.0500065745 and 0.05000657 agree,
        # 0.0500065745 and 0.06 do not, nor do 10.25 and 10.26.
        sa, sb = _scale(a), _scale(b)
        if sa == sb or (sa is None and sb is None):
            return False
        coarse, fine = (a, b) if sb is None or (sa is not None and sa < sb) else (b, a)
        if not _scale(coarse):
            return False  # an integer: 2 and 2.5 differ (integer division is a real difference)
        places = decimal.Decimal(1).scaleb(-_scale(coarse))
        fine = decimal.Decimal(str(fine))
        return decimal.Decimal(str(coarse)) in {fine.quantize(places, rounding=r) for r in
                                                (decimal.ROUND_HALF_UP, decimal.ROUND_HALF_EVEN, decimal.ROUND_DOWN)}
    return canon(a) == canon(b)


def _coarse(row) -> tuple:
    out = []
    for v in row:
        if isinstance(v, (int, float, decimal.Decimal)) and not isinstance(v, bool):
            f = float(v)
            out.append(("n", float(f"{f:.3g}") if math.isfinite(f) else str(f)))
        else:
            out.append(("s", canon(v)))
    return tuple(out)


@dataclass
class Tolerant:
    equal: bool
    differing_rows: int
    differing_columns: list[int]


def tolerant(a: list[tuple], b: list[tuple]) -> Tolerant:
    """Compares two results row by row after sorting, with numbers compared
    by close(); says how many rows and which columns differ."""
    if len(a) != len(b):
        return Tolerant(False, abs(len(a) - len(b)), [])
    sa, sb = sorted(a, key=_coarse), sorted(b, key=_coarse)
    rows, cols = 0, set()
    for x, y in zip(sa, sb):
        bad = [i for i, (u, v) in enumerate(zip(x, y)) if not close(u, v)]
        if bad or len(x) != len(y):
            rows += 1
            cols.update(bad)
    return Tolerant(rows == 0, rows, sorted(cols))
