#!/usr/bin/env python3
"""Turn bench/run.py results into a cost comparison.

    python bench/report.py bench/results/*.json > bench/results/report.md
    python bench/report.py --runs-per-day 24 --set snowflake.price_per_credit=2.4 bench/results/*.json

For every result it reports the run time of the whole query suite, cold and
warm, and what that costs under each platform's billing:

- Snowflake bills a warehouse per second while it runs, at least 60 seconds
  each time it resumes, and keeps it running until AUTO_SUSPEND seconds of
  idleness. Gen2 warehouses use more credits per hour than Gen1; the report
  shows the two side by side where both were measured.
- Databricks SQL Serverless bills a warehouse per second while it runs,
  until it auto-stops.
- Igloo runs on a machine billed per second while it runs. Always on, its
  monthly cost is the machine's, however many runs it serves (while one
  machine keeps up). With scale to zero (results from run.py
  --scale-to-zero), the gateway starts the machine for each burst and
  stops it SUSPEND_AFTER seconds after the burst; a burst costs the
  measured resume, the cold run and the idle tail, at least EC2's
  one-minute minimum, and the gateway and disk run all month. Each run's
  disk (run.py --disk, or the default in pricing.toml) is priced as gp3.

Several results for the same platform, size and scale (repeated bursts)
are merged: per-query medians, and the median resume time.

Prices come from bench/pricing.toml; see the notes there.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import statistics
import sys
import tomllib

HOURS_PER_MONTH = 730
DAYS_PER_MONTH = 30.4


def load_pricing(path: pathlib.Path, overrides: list[str]) -> dict:
    pricing = tomllib.loads(path.read_text())
    for item in overrides:
        key, _, value = item.partition("=")
        section, _, name = key.partition(".")
        try:
            pricing.setdefault(section, {})[name] = float(value)
        except ValueError:  # a name, such as igloo.disk
            pricing[section][name] = value
    return pricing


def generation(result: dict) -> int | None:
    """A Snowflake warehouse's generation; results from before run.py
    recorded it were Gen1."""
    return (result.get("generation") or 1) if result["platform"] == "snowflake" else None


def credits_per_hour(pricing: dict, result: dict) -> float:
    p = pricing["snowflake"]
    factor = p["generation_factor"]
    if isinstance(factor, dict):
        factor = factor[str(generation(result))]
    return p["credits_per_hour"][result["size"]] * factor


def disk_name(pricing: dict, result: dict) -> str:
    return result.get("disk") or pricing["igloo"]["disk"]


def disk_per_month(pricing: dict, name: str) -> float:
    p = pricing["igloo"]
    d = p["disks"][name]
    return (d["gb"] * p["gp3_per_gb_month"] + max(d["iops"] - 3000, 0) * p["gp3_per_iops_month"]
            + max(d["mbps"] - 125, 0) * p["gp3_per_mbps_month"])


def disk_per_hour(pricing: dict, result: dict) -> float:
    return disk_per_month(pricing, disk_name(pricing, result)) / HOURS_PER_MONTH


def hourly_rate(pricing: dict, result: dict) -> float:
    """What the platform costs an hour while it runs; for Igloo, the machine
    and its disk."""
    platform, size = result["platform"], result["size"]
    if platform == "snowflake":
        return credits_per_hour(pricing, result) * pricing["snowflake"]["price_per_credit"]
    if platform == "databricks":
        p = pricing["databricks"]
        return p["dbu_per_hour"][size] * p["price_per_dbu"]
    if platform == "igloo":
        return pricing["igloo"]["instance_per_hour"][size] + disk_per_hour(pricing, result)
    raise ValueError(platform)


def burst_seconds(pricing: dict, platform: str, busy: float, resume: float = 0) -> float:
    """Billed seconds for one burst of work that starts from a stopped
    warehouse or machine and is followed by idleness: from start to stop,
    the idle tail included, at least the minimum."""
    if platform == "snowflake":
        p = pricing["snowflake"]
        return max(busy + p["auto_suspend_seconds"], p["minimum_seconds"])
    if platform == "databricks":
        p = pricing["databricks"]
        return max(busy + p["auto_stop_seconds"], p["minimum_seconds"])
    # Igloo with scale to zero: EC2 bills from start to stop.
    p = pricing["igloo"]
    return max(resume + busy + p["suspend_after_seconds"], p["minimum_seconds"])


def merge(results: list[dict]) -> list[dict]:
    """Combines results for the same platform, size, scale and mode: the
    median cold time per query, every warm time, the median resume."""
    groups: dict[tuple, list[dict]] = {}
    for r in results:
        key = (r["platform"], r["size"], r["scale"], r["suite"], bool(r.get("scale_to_zero")), r.get("storage"),
               generation(r), r.get("disk"))
        groups.setdefault(key, []).append(r)
    merged = []
    for rs in groups.values():
        by_query: dict[str, list[dict]] = {}
        for r in rs:
            for q in r["queries"]:
                by_query.setdefault(q["query"], []).append(q)
        queries = []
        for name, qs in sorted(by_query.items()):
            ok = [q for q in qs if not q["error"]]
            queries.append({
                "query": name,
                "error": None if ok else qs[0]["error"],
                "cold": statistics.median(q["cold"] for q in ok) if ok else None,
                "warm": [w for q in ok for w in q["warm"]],
            })
        result = {**rs[0], "queries": queries, "bursts": len(rs)}
        if result.get("scale_to_zero"):
            result["resume"] = statistics.median(r["connect_seconds"] for r in rs)
        merged.append(result)
    return merged


def summarize(result: dict) -> dict:
    ok = [q for q in result["queries"] if not q["error"]]
    return {
        "cold": sum(q["cold"] for q in ok),
        "warm": sum(statistics.median(q["warm"]) for q in ok if q["warm"]),
        "failed": [q["query"] for q in result["queries"] if q["error"]],
        "per_query": {q["query"]: (statistics.median(q["warm"]) if q["warm"] else None) for q in ok},
    }


def money(x: float) -> str:
    if x < 0.01:
        return f"${x:.4f}"
    if x < 1:
        return f"${x:.3f}"
    if x < 100:
        return f"${x:.2f}"
    return f"${x:,.0f}"


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("results", nargs="+", type=pathlib.Path)
    p.add_argument("--pricing", type=pathlib.Path, default=pathlib.Path(__file__).parent / "pricing.toml")
    p.add_argument("--set", action="append", default=[], metavar="SECTION.KEY=VALUE", help="override a price")
    p.add_argument("--runs-per-day", type=float, default=24, help="how often the suite runs, each a separate burst")
    args = p.parse_args()

    pricing = load_pricing(args.pricing, args.set)
    rows = []
    for result in merge([json.loads(path.read_text()) for path in args.results]):
        s = summarize(result)
        platform = result["platform"]
        rate = hourly_rate(pricing, result)
        resume = None
        disk = disk_name(pricing, result) if platform == "igloo" else None
        if platform == "igloo" and result.get("scale_to_zero"):
            # The machine is billed only while it runs; the disk and the
            # gateway all month.
            prices = pricing["igloo"]
            resume = result["resume"]
            rate -= disk_per_hour(pricing, result)
            burst = burst_seconds(pricing, platform, s["cold"], resume) * rate / 3600
            always_on = (disk_per_hour(pricing, result) + prices["gateway_per_hour"]) * HOURS_PER_MONTH
            monthly = always_on + burst * args.runs_per_day * DAYS_PER_MONTH
            platform = "igloo, scale to zero"
        elif platform == "igloo":
            burst = s["cold"] * rate / 3600
            monthly = rate * HOURS_PER_MONTH
        else:
            burst = burst_seconds(pricing, platform, s["cold"]) * rate / 3600
            monthly = burst * args.runs_per_day * DAYS_PER_MONTH
        label = result.get("storage")
        if generation(result):
            label = f"Gen{generation(result)}"
        size = result["size"] + (f" ({label})" if label else "")
        rows.append({**result, **s, "platform": platform, "size": size, "rate": rate, "resume": resume, "disk": disk,
                     "generation": generation(result), "base_size": result["size"], "queries_ok": len(s["per_query"]),
                     "warm_cost": s["warm"] * rate / 3600, "burst_cost": burst, "monthly": monthly})

    out = sys.stdout
    scales = sorted({r["scale"] for r in rows})
    out.write(f"# Cost comparison: {', '.join(sorted({r['suite'] for r in rows}))} at scale {', '.join(f'{s:g}' for s in scales)}\n\n")
    out.write(f"Monthly figures assume the suite runs {args.runs_per_day:g} times a day, each run a separate burst"
              " that starts from a stopped warehouse or machine. Always-on Igloo serves them all from one machine.\n\n")
    out.write("| Platform | Size | Scale | $/hour | Resume | Cold run | Warm run | $ per warm run | $ per burst | $ per month | Failed |\n")
    out.write("|---|---|---|---|---|---|---|---|---|---|---|\n")
    for r in sorted(rows, key=lambda r: (r["scale"], r["monthly"])):
        resume = "-" if r["resume"] is None else f"{r['resume']:.1f}s"
        out.write(f"| {r['platform']} | {r['size']} | {r['scale']:g} | {money(r['rate'])} | {resume} | {r['cold']:.1f}s | {r['warm']:.1f}s"
                  f" | {money(r['warm_cost'])} | {money(r['burst_cost'])} | {money(r['monthly'])} | {', '.join(r['failed']) or '-'} |\n")
    if any(r["resume"] is not None for r in rows):
        prices = pricing["igloo"]
        out.write(f"\nScale to zero: resume is the median time from a client connecting to a stopped machine until Igloo"
                  f" answered, over every burst measured. A burst is billed for the resume, the cold run and"
                  f" {prices['suspend_after_seconds']:g}s of idleness; the gateway ({money(prices['gateway_per_hour'] * HOURS_PER_MONTH)}/month)"
                  f" and the machine's disk run all month.\n")
    disks = sorted({r["disk"] for r in rows if r["disk"]})
    if disks:
        specs = "; ".join(f"{name}: {d['gb']} GB of gp3, {d['iops']:,} IOPS, {d['mbps']} MB/s,"
                          f" {money(disk_per_month(pricing, name))}/month"
                          for name in disks for d in [pricing["igloo"]["disks"][name]])
        out.write(f"\nIgloo's disk, included in its $/hour and monthly figures: {specs}.\n")
    if any(r["generation"] for r in rows):
        factors = pricing["snowflake"]["generation_factor"]
        if isinstance(factors, dict):
            factors = ", ".join(f"Gen{g} {f:g}x" for g, f in sorted(factors.items()))
        out.write(f"\nSnowflake credits per hour by generation: {factors} the Gen1 rate.\n")

    gens = [r for r in rows if r["generation"]]
    if len({r["generation"] for r in gens}) > 1:
        out.write("\n## Snowflake Gen1 and Gen2\n\n"
                  "Warm run and cost of the 22-query suite on the same warehouse at each generation; credits per warm"
                  " run include the Gen2 factor, and $ per query is the warm run's cost over its queries.\n\n")
        out.write("| Scale | Size | Gen1 warm run | Gen2 warm run | Gen2 speed-up | Gen1 credits | Gen2 credits"
                  " | Gen1 $ per query | Gen2 $ per query | Gen1 $ per burst | Gen2 $ per burst |\n")
        out.write("|---|---|---|---|---|---|---|---|---|---|---|\n")
        columns = [
            lambda r: f"{r['warm']:.1f}s",
            lambda r: f"{r['warm_cost'] / pricing['snowflake']['price_per_credit']:.3f}",
            lambda r: money(r["warm_cost"] / r["queries_ok"]),
            lambda r: money(r["burst_cost"]),
        ]
        order = list(pricing["snowflake"]["credits_per_hour"])
        for scale in scales:
            for size in sorted({r["base_size"] for r in gens if r["scale"] == scale}, key=order.index):
                by_gen = {r["generation"]: r for r in gens if r["scale"] == scale and r["base_size"] == size}
                g1, g2 = by_gen.get(1), by_gen.get(2)
                cells = [[("-" if r is None else col(r)) for r in (g1, g2)] for col in columns]
                speedup = f"{g1['warm'] / g2['warm']:.2f}x" if g1 and g2 else "-"
                out.write(f"| {scale:g} | {size} | {cells[0][0]} | {cells[0][1]} | {speedup} | {cells[1][0]} | {cells[1][1]}"
                          f" | {cells[2][0]} | {cells[2][1]} | {cells[3][0]} | {cells[3][1]} |\n")

    for scale in scales:
        primary = [r for r in rows if r["scale"] == scale]
        out.write(f"\n## Warm run time per query at scale {scale:g} (median seconds)\n\n")
        names = sorted({q for r in primary for q in r["per_query"]} | {q for r in primary for q in r["failed"]})
        out.write("| Query | " + " | ".join(f"{r['platform']} {r['size']}" for r in primary) + " |\n")
        out.write("|---|" + "---|" * len(primary) + "\n")
        for name in names:
            cells = []
            for r in primary:
                v = r["per_query"].get(name)
                cells.append("failed" if v is None else f"{v:.2f}")
            out.write(f"| {name} | " + " | ".join(cells) + " |\n")


if __name__ == "__main__":
    main()
