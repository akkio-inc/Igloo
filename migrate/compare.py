"""Comparing a query's results across engines: a row count and a checksum
that ignores row order, over values written in one canonical form.

Engines return the same value in different types (Snowflake NUMBER(38,2) as
Decimal('5.00'), DuckDB DOUBLE as 5.0, timestamps with or without a time
zone, JSON as text or as objects), and floating-point aggregates differ in
their last digits with the order rows were added in. So integers and
decimals are compared exactly and binary floating point to a number of
significant digits, timestamps with a time zone in UTC, and JSON by its
parsed value. The checksum writes each value with its kind and length, so
that NULL is not the text \\N, nor 5 the text '5', and no value runs into
the next.
"""

from __future__ import annotations

import datetime as dt
import decimal
import hashlib
import json
import math
from dataclasses import dataclass

DIGITS = 10  # significant digits numbers are compared to
MASK = (1 << 64) - 1


def canon(v, digits: int = DIGITS, as_json: bool = False, exact: bool = True) -> str:
    """The canonical text of one value; with exact false, integers and
    decimals are to digits significant digits too."""
    return _canon(v, digits, as_json, exact)[1]


def _field(v, digits: int = DIGITS, as_json: bool = False, exact: bool = True) -> str:
    """One value as the checksum writes it: a letter for its kind, the
    length of its canonical text, then the text."""
    kind, text = _canon(v, digits, as_json, exact)
    return f"{kind}{len(text)}:{text}"


def _canon(v, digits: int, as_json: bool, exact: bool) -> tuple[str, str]:
    """A value's kind and canonical text."""
    if v is None:
        return "N", "\\N"
    if as_json and isinstance(v, str):
        try:
            v = json.loads(v)
        except ValueError:
            return "s", v
        return _canon(v, digits, False, exact)
    if isinstance(v, bool):
        return "b", "true" if v else "false"
    if isinstance(v, (int, float, decimal.Decimal)):
        return "n", _number(v, digits, exact)
    if isinstance(v, dt.datetime):
        if v.tzinfo is not None:
            return "t", v.astimezone(dt.timezone.utc).replace(tzinfo=None).isoformat() + "Z"
        return "t", v.isoformat()
    if isinstance(v, (dt.date, dt.time)):
        return "t", v.isoformat()
    if isinstance(v, dt.timedelta):
        return "i", _number(v.total_seconds(), digits, exact) + "s"
    if isinstance(v, (bytes, bytearray, memoryview)):
        return "x", bytes(v).hex()
    if isinstance(v, (dict, list, tuple)):
        return "j", json.dumps(_canon_json(v, digits, exact), sort_keys=True, separators=(",", ":"))
    # Text, and a UUID, which a source without the type has as text.
    return "s", str(v)


def _canon_json(v, digits, exact):
    if isinstance(v, dict):
        return {str(k): _canon_json(x, digits, exact) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_canon_json(x, digits, exact) for x in v]
    if isinstance(v, bool) or v is None:
        return v
    # Numbers and text both become JSON strings: a letter keeps them apart.
    if isinstance(v, (int, float, decimal.Decimal)):
        return "n" + _number(v, digits, exact)
    return "s" + str(v)


def _number(v, digits: int, exact: bool = True) -> str:
    """The same text for the same number whatever its type (5, 5.0 and
    Decimal('5.00') are 5): an integer or decimal exactly, unless exact is
    false; binary floating point, whose last digits vary with the order rows
    were added in, to digits significant digits unless it is a whole number
    it holds exactly."""
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return str(v)
        v = int(v) if v == int(v) and abs(v) < 1e15 else decimal.Decimal(f"{v:.{digits}g}")
    if isinstance(v, int) and exact:
        return str(v)
    if isinstance(v, decimal.Decimal) and not v.is_finite():
        return str(v).lower()
    d = decimal.Decimal(v)
    if not exact:
        d = decimal.Decimal(f"{d:.{digits}g}")
    # Fixed point, which unlike normalize() keeps all 38 digits of a NUMBER.
    out = f"{d:f}"
    if "." in out:
        out = out.rstrip("0").rstrip(".")
    return "0" if out == "-0" else out


KEEP_ROWS = 2000  # results up to this size are kept in memory for a second, tolerant look


@dataclass
class Digest:
    rows: int
    checksum: str
    columns: list[str]
    values: list[tuple] | None = None  # the rows themselves, for small results; never written anywhere
    # The checksum with integers and decimals to DIGITS significant digits
    # too, then a letter per column: f if it held binary floating point, r
    # if that rounding changed a value in it, - if neither (see verdict).
    loose: str | None = None


def digest(rows, columns: list[str], json_columns: set[int] = frozenset(), digits: int = DIGITS,
           keep: int = KEEP_ROWS) -> Digest:
    """Row count and order-independent checksum of a result: the sum,
    modulo 2^64, of a hash of each row, so duplicate rows count."""
    total, loose, n, kept = 0, 0, 0, []
    kinds = ["-"] * len(columns)
    for row in rows:
        exact, rounded = [], []
        for i, v in enumerate(row):
            exact.append(_field(v, digits, i in json_columns))
            # Only numbers round, alone or in JSON or an interval.
            rounded.append(_field(v, digits, i in json_columns, exact=False) if exact[-1][0] in "nji" else exact[-1])
            if isinstance(v, float):
                kinds[i] = "f"
            elif rounded[-1] != exact[-1] and kinds[i] == "-":
                kinds[i] = "r"
        total = (total + _hash("".join(exact))) & MASK
        loose = (loose + _hash("".join(rounded))) & MASK
        n += 1
        if kept is not None:
            kept.append(tuple(_parse_json(v) if i in json_columns else v for i, v in enumerate(row)))
            if len(kept) > keep:
                kept = None
    return Digest(rows=n, checksum=f"{total:016x}", columns=[c.lower() for c in columns], values=kept,
                  loose=f"{loose:016x} {''.join(kinds)}")


def _hash(text: str) -> int:
    return int.from_bytes(hashlib.sha256(text.encode()).digest()[:8], "big")


def _parse_json(v):
    if isinstance(v, str):
        try:
            return json.loads(v)
        except ValueError:
            return v
    return v


def verdict(igloo: Digest | None, source_rows: int | None, source_checksum: str | None,
            source_loose: str | None = None) -> str:
    """match, values differ, rows differ, or not compared."""
    if igloo is None or source_rows is None:
        return "not compared"
    if igloo.rows != source_rows:
        return "rows differ"
    if source_checksum is None:
        return "rows match"
    if igloo.checksum == source_checksum or _loosely(igloo.loose, source_loose):
        return "match"
    return "values differ"


def _loosely(a: str | None, b: str | None) -> bool:
    """Whether results whose checksums differ are the same with every
    number to DIGITS significant digits, where only binary floating point
    needed it: a double agrees with a decimal of more digits to those
    (Snowflake's AVG of a NUMBER(12,2) has 8 decimal places, DuckDB's is a
    double), while two decimals or integers must be equal."""
    if not a or not b:
        return False
    (checksum_a, kinds_a), (checksum_b, kinds_b) = a.split(" "), b.split(" ")
    return (checksum_a == checksum_b and len(kinds_a) == len(kinds_b)
            and all("r" not in x + y or "f" in x + y for x, y in zip(kinds_a, kinds_b)))


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
    DuckDB's is a double, and the two agree to those 8. Only binary floating
    point has last digits that carry no meaning: integers, and decimals of
    the same scale, are equal or differ."""
    numeric = (int, float, decimal.Decimal)
    if isinstance(a, numeric) and isinstance(b, numeric) and not isinstance(a, bool) and not isinstance(b, bool):
        if a == b:
            return True
        fa, fb = float(a), float(b)
        if math.isnan(fa) or math.isnan(fb) or math.isinf(fa) or math.isinf(fb):
            return fa == fb or (math.isnan(fa) and math.isnan(fb))
        if (isinstance(a, float) or isinstance(b, float)) and abs(fa - fb) <= 1e-9 * max(abs(fa), abs(fb)):
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
    return _field(a) == _field(b)


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
