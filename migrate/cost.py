"""The cost section: what the source spent, against what Igloo would.

Every figure is labeled measured (read from the source's billing or
history) or estimated (worked out here, from prices in bench/pricing.toml
and the replay's times).
"""

from __future__ import annotations

HOURS_PER_MONTH = 730


def unit_price(pricing: dict, source: str) -> float | None:
    if source == "snowflake":
        return pricing.get("snowflake", {}).get("price_per_credit")
    return pricing.get("databricks", {}).get("price_per_dbu")


def size_rate(pricing: dict, source: str, size: str | None) -> float | None:
    """Credits or DBUs an hour for a warehouse of this size (Snowflake: Gen1)."""
    if not size:
        return None
    if source == "snowflake":
        return pricing.get("snowflake", {}).get("credits_per_hour", {}).get(size)
    return pricing.get("databricks", {}).get("dbu_per_hour", {}).get(size)


def shape_source_cost(shape, pricing: dict, source: str) -> tuple[float | None, str]:
    """A shape's compute in the source's unit: what the source attributed to
    its executions, else their execution time at its warehouse size's rate."""
    if shape.credits is not None:
        return shape.credits, "measured"
    rates = [r for r in (size_rate(pricing, source, s) for s in shape.warehouse_sizes) if r is not None]
    if not rates:
        return None, "unknown"
    return shape.execution_ms_total / 3_600_000 * (sum(rates) / len(rates)), "estimated"


def section(shapes, results: dict, spend, pricing: dict, source: str, unit: str, period_hours: float,
            igloo_machine: str | None, igloo_price: float | None, meter) -> dict:
    price = unit_price(pricing, source)
    ran = [s for s in shapes if results[s.id].best is not None and results[s.id].best.seconds is not None]
    src_units, labels, src_hours = 0.0, set(), 0.0
    for s in ran:
        units, label = shape_source_cost(s, pricing, source)
        if units is not None:
            src_units += units
            labels.add(label)
        src_hours += s.execution_ms_total / 3_600_000
    igloo_hours = sum(results[s.id].best.seconds * s.executions for s in ran) / 3600
    attributed = [s.credits for s in shapes if s.credits is not None]
    out = {
        # What the source attributed to the extracted workload's executions
        # (Snowflake's QUERY_ATTRIBUTION_HISTORY): their compute, without
        # the warehouses' idle time.
        "workload_attributed": {
            "total": sum(attributed) if attributed else None,
            "dollars": sum(attributed) * price if attributed and price else None,
            "shapes": len(attributed),
            "label": "measured" if attributed else "unavailable",
        },
        "unit": unit,
        "price_per_unit": price,
        "source_spend": {
            "label": "measured" if spend and spend.total is not None else "unavailable",
            "total": spend.total if spend else None,
            "dollars": (spend.total * price) if spend and spend.total is not None and price else None,
            "by_warehouse": spend.by_warehouse if spend else {},
            "source": spend.source if spend else None,
            "note": spend.note if spend else "the source was not read",
        },
        "replayed": {
            "shapes": len(ran),
            "executions": sum(s.executions for s in ran),
            "source_hours": src_hours,
            "source_hours_label": "measured",
            "source_units": src_units if labels else None,
            "source_units_label": "measured" if labels == {"measured"} else ("estimated" if labels else "unknown"),
            "source_dollars": src_units * price if labels and price else None,
            "igloo_hours": igloo_hours,
            "igloo_hours_label": "estimated",
            "igloo_dollars": igloo_hours * igloo_price if igloo_price is not None else None,
        },
        "igloo": {
            "machine": igloo_machine,
            "price_per_hour": igloo_price,
            "period_hours": period_hours,
            "always_on_dollars": igloo_price * period_hours if igloo_price is not None else None,
            "label": "estimated",
        },
        "tool": meter.to_dict() if meter else None,
    }
    return out
