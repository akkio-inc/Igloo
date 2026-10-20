"""Connections to the warehouses a workload migrates from. Each reads the
query history, the sizes of tables, and what the warehouse cost, and can
run a query again to compare its result; none of them writes anything."""

from __future__ import annotations

import datetime as dt
import json
import time
from dataclasses import asdict, dataclass, field

# Every statement the tool sends to a source starts with this comment, and
# Snowflake sessions carry it as their query tag, so that extraction leaves
# the tool's own statements out of the workload.
MARKER = "igloo-migrate"


@dataclass
class Filters:
    start: dt.datetime
    end: dt.datetime
    users: list[str] = field(default_factory=list)
    roles: list[str] = field(default_factory=list)
    warehouses: list[str] = field(default_factory=list)
    classes: list[str] = field(default_factory=list)  # statement classes, see statements.CLASSES
    max_groups: int = 20000


@dataclass
class Extract:
    groups: list  # workload.Group
    meta: dict  # where the history came from, the period it covers, what it could not read


@dataclass
class TableInfo:
    name: str
    kind: str | None = None  # BASE TABLE, VIEW, ...
    rows: int | None = None
    bytes: int | None = None
    last_altered: str | None = None


@dataclass
class Spend:
    """What the source cost over the period, as its billing records it."""

    unit: str  # credits or DBUs
    by_warehouse: dict[str, float] = field(default_factory=dict)
    total: float | None = None
    price: float | None = None  # per unit, from the pricing file
    price_source: str | None = None
    source: str | None = None  # the view it came from
    note: str | None = None  # why it is missing, or what it covers

    def to_dict(self):
        return asdict(self)


@dataclass
class Rerun:
    ok: bool
    seconds: float | None = None
    rows: int | None = None
    checksum: str | None = None
    columns: list[str] | None = None
    error: str | None = None
    cost: float | None = None  # estimated, in the source's unit
    skipped: str | None = None
    values: list[tuple] | None = field(default=None, repr=False)  # a small result's rows, kept in memory only


class Meter:
    """The tool's own use of a source warehouse, estimated from how long
    its statements kept the warehouse busy: billed per second at the
    warehouse's rate, at least minimum_seconds if the tool resumed it, and
    for idle_seconds after the last statement until it suspends again. The
    idle time is counted even when the warehouse was already running, since
    the tool's statements keep it up that long, so the figure is an upper
    bound."""

    def __init__(self, rate_per_hour: float | None, was_running: bool | None, minimum_seconds: float,
                 idle_seconds: float, unit: str):
        self.rate = rate_per_hour
        self.was_running = was_running
        self.minimum = minimum_seconds
        self.idle = idle_seconds
        self.unit = unit
        self.busy = {"extraction": 0.0, "comparison": 0.0}
        self.statements = {"extraction": 0, "comparison": 0}

    def add(self, phase: str, seconds: float):
        self.busy[phase] += seconds
        self.statements[phase] += 1

    def _cost(self, busy: float) -> float | None:
        if self.rate is None:
            return None
        if busy == 0:
            return 0.0
        if not self.was_running:
            busy = max(busy, self.minimum)
        return self.rate * (busy + self.idle) / 3600

    def estimate(self, extra: float = 0.0) -> float | None:
        return self._cost(sum(self.busy.values()) + extra)

    def comparison_cost(self, extra: float = 0.0) -> float | None:
        """What the comparison re-runs add to the cost of extraction alone."""
        total = self.estimate(extra)
        if total is None:
            return None
        return total - self._cost(self.busy["extraction"])

    def seconds_within(self, budget: float, cap: float) -> float | None:
        """How many more seconds the comparisons may run, up to cap, and
        stay within budget; None if the rate is unknown."""
        if self.rate is None:
            return None
        if self.comparison_cost(extra=cap) <= budget:
            return cap
        if self.comparison_cost(extra=0.0) > budget:
            return 0.0
        lo, hi = 0.0, cap
        for _ in range(50):
            mid = (lo + hi) / 2
            lo, hi = (mid, hi) if self.comparison_cost(extra=mid) <= budget else (lo, mid)
        return lo

    def to_dict(self):
        return {"unit": self.unit, "rate_per_hour": self.rate, "warehouse_was_running": self.was_running,
                "busy_seconds": self.busy, "statements": self.statements, "estimated_total": self.estimate(),
                "estimated_comparison": self.comparison_cost(),
                "how": "estimated as an upper bound: the seconds the tool's statements ran"
                       + ("" if self.was_running or not self.minimum else f" (at least {self.minimum:g} s, as it"
                                                                           " resumed the warehouse)")
                       + f" and {self.idle:g} s idle before the warehouse stops, at its rate"}


def iso(v) -> str | None:
    if v is None:
        return None
    if isinstance(v, dt.datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=dt.timezone.utc)
        return v.astimezone(dt.timezone.utc).isoformat(timespec="seconds")
    return str(v)


def json_list(v) -> list:
    """A list from a source's array value, which may arrive as JSON text."""
    if v is None:
        return []
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except ValueError:
            return [v]
    return [x for x in v if x is not None] if isinstance(v, (list, tuple)) else [v]


def num(v) -> float | None:
    return None if v is None else float(v)


def timed(fn):
    start = time.perf_counter()
    out = fn()
    return out, time.perf_counter() - start
