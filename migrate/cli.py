"""python -m migrate report: how far a Snowflake or Databricks workload gets
on Igloo. See migrate/README.md and docs/migrating.md.

    python -m migrate report --source snowflake --since 30d \\
        --igloo 'host=igloo.example.com port=5432 user=migrate dbname=igloo' \\
        --map 'PROD.ANALYTICS.*=lake.analytics.*' --out ~/igloo-migration
    python -m migrate extract --source databricks --since 7d --out ~/igloo-migration
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import pathlib
import re
import sys
from dataclasses import asdict

from . import cost, inventory, replay, report, statements, workload
from .igloo import Igloo
from .sources import Extract, Filters, Rerun

ROOT = pathlib.Path(__file__).resolve().parent.parent
DEFAULT_BUDGET = {"snowflake": 0.1, "databricks": 0.5}


def load_pricing(path: pathlib.Path, overrides: list[str]) -> dict:
    """bench/pricing.toml, read by the benchmark's own loader."""
    spec = importlib.util.spec_from_file_location("igloo_bench_report", ROOT / "bench" / "report.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.load_pricing(path, overrides)


def parse_time(text: str, now: dt.datetime) -> dt.datetime:
    m = re.fullmatch(r"(\d+)\s*([dhm])", text.strip())
    if m:
        unit = {"d": "days", "h": "hours", "m": "minutes"}[m.group(2)]
        return now - dt.timedelta(**{unit: int(m.group(1))})
    t = dt.datetime.fromisoformat(text)
    return t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)


def csv(text: str | None) -> list[str]:
    return [x.strip() for x in (text or "").split(",") if x.strip()]


def check_out(out: pathlib.Path) -> pathlib.Path:
    out = out.expanduser().resolve()
    if (ROOT / ".git").exists() and (out == ROOT or ROOT in out.parents):
        raise SystemExit(f"--out {out} is inside the repository; write extracts and reports somewhere else, so that "
                         "query text never ends up in a commit")
    out.mkdir(parents=True, exist_ok=True)
    return out


def connect_source(args, pricing):
    if args.source == "snowflake":
        from .sources.snowflake import Snowflake

        return Snowflake.connect(args.timeout, pricing, args.source_warehouse)
    from .sources.databricks import Databricks

    ns = tuple(args.default_namespace.split(".", 1)) if args.default_namespace else None
    if ns and len(ns) == 1:
        ns = (ns[0], None)
    return Databricks.connect(args.timeout, pricing, args.source_warehouse, ns)


def filters(args, now) -> Filters:
    classes = [c if c == "other" else c.upper() for c in csv(args.statement_types)]
    for c in classes:
        if c not in statements.CLASSES:
            raise SystemExit(f"--statement-types: {c} is not one of {', '.join(statements.CLASSES)}")
    return Filters(start=parse_time(args.since, now), end=parse_time(args.until, now) if args.until else now,
                   users=csv(args.users), roles=csv(args.roles), warehouses=csv(args.warehouses), classes=classes,
                   max_groups=args.max_groups)


def save_extract(out: pathlib.Path, ex: Extract):
    d = out / "extract"
    d.mkdir(exist_ok=True)
    with open(d / "groups.jsonl", "w") as f:
        for g in ex.groups:
            f.write(json.dumps(asdict(g), default=str) + "\n")
    (d / "meta.json").write_text(json.dumps(ex.meta, indent=2, default=str) + "\n")


def load_extract(d: pathlib.Path) -> Extract:
    d = d.expanduser()
    if (d / "extract").is_dir():
        d = d / "extract"
    lines = (d / "groups.jsonl").read_text().splitlines()
    groups = [workload.Group.from_dict(json.loads(line)) for line in lines if line.strip()]
    return Extract(groups, json.loads((d / "meta.json").read_text()))


def log(msg: str):
    print(msg, file=sys.stderr, flush=True)


def cmd_extract(args):
    out = check_out(args.out)
    pricing = load_pricing(args.pricing, args.set)
    src = connect_source(args, pricing)
    try:
        ex = src.extract(filters(args, dt.datetime.now(dt.timezone.utc)))
    finally:
        src.close()
    save_extract(out, ex)
    log(f"{len(ex.groups)} groups, {sum(g.executions for g in ex.groups):,} executions -> {out / 'extract'}")


def cmd_report(args):
    out = check_out(args.out)
    now = dt.datetime.now(dt.timezone.utc)
    pricing = load_pricing(args.pricing, args.set)
    f = filters(args, now)
    src = None if args.no_source else connect_source(args, pricing)
    dialect = {"snowflake": "snowflake", "databricks": "databricks"}[args.source]
    try:
        if args.from_extract:
            ex = load_extract(args.from_extract)
            log(f"extract: {len(ex.groups)} groups from {args.from_extract}")
        else:
            log(f"{args.source}: reading query history")
            ex = src.extract(f)
            save_extract(out, ex)
            log(f"{args.source}: {len(ex.groups)} groups, {sum(g.executions for g in ex.groups):,} executions "
                f"({ex.meta.get('history')})")
        shapes = workload.build_shapes(ex.groups, dialect)
        if f.classes:
            shapes = [s for s in shapes if s.cls in f.classes]
        log(f"{len(shapes)} shapes")
        return _report(args, out, now, pricing, f, src, dialect, ex, shapes)
    finally:
        if src is not None:
            src.close()


def _report(args, out, now, pricing, f, src, dialect, ex, shapes):
    igloo = Igloo(args.igloo, args.timeout)
    try:
        version = igloo.conn().execute("SELECT version()").fetchone()[0]
        tables, lake = igloo.tables()
        native, native_err = (None, "turned off by --no-native-dialect") if args.no_native_dialect else \
            igloo.native_dialect(args.source)
        log(f"igloo: {len(tables)} tables; native {args.source} dialect: {native or 'not available'}")
        rules = [inventory.Rule.parse(m, lake) for m in args.map]
        mapper = inventory.Mapper(tables, lake, rules)

        refs = {r.key: r for s in shapes for r in s.reads + s.writes if r.kind == "table" and r.resolved}
        info = src.table_info(refs.values()) if src is not None else {}
        uses = inventory.table_uses(shapes, mapper, info)
        uses_by_key = {u.key: u for u in uses}

        scratch = (lake, args.scratch_schema) if args.replay_writes else None
        settings = replay.Settings(dialect, native, args.max_compare_rows, args.replay_writes, scratch,
                                   args.keep_scratch)
        if args.replay_writes:
            # An admin may have created it for a user who cannot create schemas.
            found = igloo.conn().execute("SELECT 1 FROM information_schema.schemata WHERE catalog_name = %s AND "
                                         "schema_name = %s", scratch).fetchone()
            if not found:
                igloo.conn().execute(f"CREATE SCHEMA {inventory.qualified(scratch)}")
        results, chosen, coverages = replay.plan(shapes, mapper, settings, args.sample, args.sample_by, args.seed)
        log(f"replaying {len(chosen)} shapes on Igloo")
        with open(out / "replay.jsonl", "w") as rf:
            for i, s in enumerate(chosen, 1):
                r = replay.replay(s, coverages[s.id], igloo, settings, results[s.id])
                rf.write(json.dumps({"shape": s.id, **r.to_dict()}, default=str) + "\n")
                log(f"  [{i}/{len(chosen)}] {s.id} {s.cls}: {r.outcome}"
                    + (f" ({r.failure.error_class})" if r.outcome == "failed" and r.failure else ""))

        # Every query that ran: its row count against the history's.
        for s in chosen:
            r = results[s.id]
            if s.cls == "SELECT" and r.best is not None:
                r.comparison = {"history": replay.history_check(s, r.best, uses_by_key)}
        _compare_on_source(args, src, chosen, results, uses_by_key)

        # The period the history covers, which a source may have shortened.
        start, end = f.start, f.end
        try:
            start = parse_time(ex.meta["start"], now)
            end = parse_time(ex.meta["end"], now)
        except (KeyError, TypeError, ValueError):
            pass
        spend = src.spend(start, end) if src is not None else None
        machine = args.igloo_machine
        price = args.igloo_price_per_hour
        if price is None and machine:
            price = pricing.get("igloo", {}).get("instance_per_hour", {}).get(machine)
        period_hours = (end - start).total_seconds() / 3600
        cost_section = cost.section(shapes, results, spend, pricing, args.source, src.unit if src else
                                    ("credits" if args.source == "snowflake" else "DBUs"), period_hours, machine,
                                    price, src.meter if src else None)
        conninfo = _address(args.igloo)
        rep = report.build(
            shapes=shapes, results=results, uses=uses, meta=ex.meta,
            igloo_info={"version": version, "address": conninfo, "catalog": lake, "tables": len(tables),
                        "native_dialect": native, "native_dialect_error": native_err},
            settings={"generated": now.isoformat(timespec="seconds"), "source": args.source,
                      "since": args.since, "until": args.until, "users": f.users, "roles": f.roles,
                      "warehouses": f.warehouses, "statement_types": f.classes, "max_groups": f.max_groups,
                      "sample": args.sample, "sample_by": args.sample_by, "timeout_seconds": args.timeout,
                      "map": args.map, "replay_writes": args.replay_writes, "compare": args.compare,
                      "source_budget": _budget(args), "max_compare_rows": args.max_compare_rows,
                      "digits": 10, "igloo_machine": machine, "igloo_price_per_hour": price,
                      "show_literals": args.show_literals, "from_extract": str(args.from_extract or "") or None},
            cost=cost_section, dialect=dialect, show_literals=args.show_literals)
        (out / "report.json").write_text(json.dumps(rep, indent=2, default=str) + "\n")
        (out / "report.md").write_text(report.markdown(rep))
        (out / "report.html").write_text(report.render_html(rep))
        log(f"wrote {out / 'report.html'}, report.md and report.json")
        return rep
    finally:
        igloo.close()


def _budget(args) -> float:
    return DEFAULT_BUDGET[args.source] if args.source_budget is None else args.source_budget


def _compare_on_source(args, src, chosen, results, uses_by_key):
    """Re-runs up to --compare queries on the source, most executed first,
    while the estimated cost stays within --source-budget."""
    if src is None or args.compare <= 0:
        return
    budget = _budget(args)
    done = 0
    for s in sorted(chosen, key=lambda s: (-s.executions, s.id)):
        if done >= args.compare:
            break
        r = results[s.id]
        b = r.best
        if s.cls != "SELECT" or b is None or b.counted or (b.rows or 0) > args.max_compare_rows or b.checksum is None:
            continue
        # The re-run may take as long as the budget allows, and that is its
        # timeout, so the budget holds even if it runs longer than before.
        allowed = src.meter.seconds_within(budget, args.timeout)
        expect = (s.elapsed_ms_median or 1000) / 1000
        if allowed is None or allowed < expect:
            why = (f"the cost of {src.name}'s warehouse ({src.warehouse.get('size')}) is not in the pricing file"
                   if allowed is None else f"would pass --source-budget {budget:g} {src.unit}")
            r.comparison["rerun"] = {"method": "re-run on the source", "verdict": "not compared",
                                     "explained_by": [], "notes": [why]}
            continue
        before = src.meter.comparison_cost() or 0.0
        rr: Rerun = src.rerun(s, max(1, int(allowed)), args.max_compare_rows)
        rr.cost = (src.meter.comparison_cost() or 0.0) - before
        r.comparison["rerun"] = replay.rerun_check(s, b, rr, uses_by_key)
        log(f"  compared {s.id} on {args.source}: {r.comparison['rerun']['verdict']}")
        done += 1


def _address(dsn: str) -> str:
    try:
        from psycopg.conninfo import conninfo_to_dict

        d = conninfo_to_dict(dsn)
        return f"{d.get('host', 'localhost')}:{d.get('port', 5432)}"
    except Exception:  # noqa: BLE001
        return "?"


def main(argv=None):
    p = argparse.ArgumentParser(prog="python -m migrate", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="command", required=True)

    def common(sp):
        sp.add_argument("--source", required=True, choices=["snowflake", "databricks"])
        sp.add_argument("--since", default="30d", help="start of the period: 30d, 12h, or an ISO time (default 30d)")
        sp.add_argument("--until", help="end of the period, as --since (default now)")
        sp.add_argument("--users", help="comma-separated users to keep")
        sp.add_argument("--roles", help="comma-separated roles to keep (Snowflake)")
        sp.add_argument("--warehouses", help="comma-separated warehouses (Databricks: warehouse IDs) to keep")
        sp.add_argument("--statement-types", help=f"comma-separated classes to keep: {', '.join(statements.CLASSES)}")
        sp.add_argument("--max-groups", type=int, default=20000,
                        help="read at most this many query groups, the costliest first (default 20000)")
        sp.add_argument("--source-warehouse", help="warehouse to run on (default SNOWFLAKE_WAREHOUSE, "
                                                   "DATABRICKS_WAREHOUSE_ID)")
        sp.add_argument("--default-namespace", help="Databricks: catalog.schema that unqualified names resolve in "
                                                    "(default: the warehouse's)")
        sp.add_argument("--timeout", type=int, default=120, help="seconds any one statement may run (default 120)")
        sp.add_argument("--pricing", type=pathlib.Path, default=ROOT / "bench" / "pricing.toml")
        sp.add_argument("--set", action="append", default=[], metavar="SECTION.KEY=VALUE", help="override a price")
        sp.add_argument("--out", type=pathlib.Path, required=True,
                        help="directory for the extract, replay log and report (outside the repository)")

    ex = sub.add_parser("extract", help="read the query history only")
    common(ex)
    ex.set_defaults(fn=cmd_extract)

    rp = sub.add_parser("report", help="extract, replay on Igloo, compare and report")
    common(rp)
    rp.add_argument("--igloo", required=True, help="libpq connection string for Igloo; the password from "
                                                   "IGLOO_PASSWORD or PGPASSWORD")
    rp.add_argument("--map", action="append", default=[], metavar="SOURCE=TARGET",
                    help="where a source table is in Igloo, e.g. 'PROD.SALES.*=lake.sales.*' (repeatable)")
    rp.add_argument("--from-extract", type=pathlib.Path, help="use a saved extract instead of reading the history")
    rp.add_argument("--no-source", action="store_true",
                    help="do not connect to the source (needs --from-extract): no sizes, spend or re-runs")
    rp.add_argument("--sample", type=int, default=200, help="replay at most this many shapes (default 200)")
    rp.add_argument("--sample-by", choices=["time", "executions", "random"], default="time",
                    help="which shapes to replay first: most source time (default), most executions, or random")
    rp.add_argument("--seed", type=int, default=0)
    rp.add_argument("--compare", type=int, default=10,
                    help="re-run up to this many queries on the source to compare results (default 10; 0 for none)")
    rp.add_argument("--source-budget", type=float,
                    help="most the re-runs may cost, in credits (Snowflake, default 0.1) or DBUs (Databricks, "
                         "default 0.5), estimated from the warehouse's rate")
    rp.add_argument("--max-compare-rows", type=int, default=100000,
                    help="compare results of at most this many rows; larger ones are counted on Igloo")
    rp.add_argument("--replay-writes", action="store_true",
                    help="also replay CTAS, INSERT, MERGE, UPDATE and DELETE, into --scratch-schema")
    rp.add_argument("--scratch-schema", default="igloo_migrate_scratch")
    rp.add_argument("--keep-scratch", action="store_true", help="keep the scratch tables writes create")
    rp.add_argument("--no-native-dialect", action="store_true", help="do not try Igloo's own dialect of the source")
    rp.add_argument("--igloo-machine", default="c7g.4xlarge",
                    help="the machine to price Igloo on, from [igloo] instance_per_hour in the pricing file")
    rp.add_argument("--igloo-price-per-hour", type=float, help="or its price per hour")
    rp.add_argument("--show-literals", action="store_true", help="keep literal values in the report's statements")
    rp.set_defaults(fn=cmd_report)

    args = p.parse_args(argv)
    if getattr(args, "no_source", False) and not args.from_extract:
        p.error("--no-source needs --from-extract")
    if getattr(args, "no_source", False):
        args.compare = 0
    args.fn(args)


if __name__ == "__main__":
    main()
