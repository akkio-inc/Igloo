"""The report: report.json with everything, report.md with the summary, and
report.html, one self-contained page.

Statements in the report have their literals replaced by ? and their
comments removed, and error messages lose the values they quote, unless
--show-literals is given. User names appear only as counts.
"""

from __future__ import annotations

import html
import json
import math
import statistics
from collections import Counter, defaultdict

from . import replay, sqltext
from .igloo import error_pattern

MATCHES = ("match", "match within precision", "rows match")

OUTCOME_TEXT = {
    "unchanged": "Run unchanged",
    "translated": "Run after translation",
    "failed": "Fail on Igloo",
    "no data": "Lack data in Igloo",
    "source metadata": "Read the source's own metadata",
    "failed on source": "Failed on the source too",
    "not replayed": "Not replayed",
    "not in sample": "Not in the sample",
}


def build(*, shapes, results, uses, meta, igloo_info, settings, cost, dialect, show_literals) -> dict:
    total_exec = sum(s.executions for s in shapes) or 1
    total_ms = sum(s.elapsed_ms_total for s in shapes) or 1

    def red(sql, d=dialect):
        if sql is None:
            return None
        return sql if show_literals else sqltext.redact(sql, d)

    def red_msg(m):
        if m is None:
            return None
        return sqltext.first_line(m, 500) if show_literals else sqltext.first_line(sqltext.redact_message(m), 500)

    outcomes = []
    for o in replay.OUTCOMES:
        ss = [s for s in shapes if results[s.id].outcome == o]
        outcomes.append({"outcome": o, "label": OUTCOME_TEXT[o], "shapes": len(ss),
                         "executions": sum(s.executions for s in ss),
                         "elapsed_ms": sum(s.elapsed_ms_total for s in ss)})

    # Of the shapes Igloo could run (replayed), how they did.
    replayed = [s for s in shapes if results[s.id].outcome in ("unchanged", "translated", "failed")]

    skipped = defaultdict(lambda: [0, 0])
    for s in shapes:
        r = results[s.id]
        if r.outcome != "not replayed":
            continue
        reason = r.reason or ""
        if reason == f"{s.cls} statements are not replayed":
            reason = f"{s.cls}: {', '.join(s.source_types) or 'type unknown'}"
        skipped[reason][0] += 1
        skipped[reason][1] += s.executions
    not_replayed = [{"reason": k, "shapes": v[0], "executions": v[1]}
                    for k, v in sorted(skipped.items(), key=lambda kv: (-kv[1][1], kv[0]))]

    failures = defaultdict(lambda: {"shapes": 0, "executions": 0, "patterns": defaultdict(list)})
    for s in replayed:
        r = results[s.id]
        if r.outcome != "failed":
            continue
        a = r.failure
        cls = a.error_class if a else "other"
        f = failures[cls]
        f["shapes"] += 1
        f["executions"] += s.executions
        f["patterns"][error_pattern(cls, a.error if a else None)].append(s.id)
    failure_list = []
    for cls, f in sorted(failures.items(), key=lambda kv: -kv[1]["executions"]):
        pats = sorted(f["patterns"].items(), key=lambda kv: -len(kv[1]))
        failure_list.append({
            "class": cls, "shapes": f["shapes"], "executions": f["executions"],
            "patterns": [{"pattern": p, "shapes": len(ids), "examples": ids[:3]} for p, ids in pats],
        })

    translation = Counter()
    sqlglot_warned = 0
    for s in replayed:
        r = results[s.id]
        if "as written" in r.works_with:
            continue
        hows = {a.how: a.ok for a in r.attempts}
        key = (hows.get("native dialect"), hows.get("sqlglot"))
        translation[key] += 1
        if any(a.how == "sqlglot" and a.ok and a.warnings for a in r.attempts):
            sqlglot_warned += 1
    translation_rows = [{"native_dialect": k[0], "sqlglot": k[1], "shapes": n} for k, n in translation.items()]

    comparisons = {"rerun": Counter(), "history": Counter()}
    for s in shapes:
        c = results[s.id].comparison
        if not c:
            continue
        for part in ("rerun", "history"):
            if c.get(part):
                v = c[part]["verdict"]
                if v not in MATCHES and v != "not compared" and c[part].get("explained_by"):
                    v += " (explained)"
                comparisons[part][v] += 1

    def rate(counter):
        decided = sum(n for v, n in counter.items() if v != "not compared" and not v.endswith("(explained)"))
        return (sum(counter[g] for g in MATCHES) / decided) if decided else None

    timing_rows = []
    for s in replayed:
        r = results[s.id]
        b = r.best
        if b is None or b.seconds is None or s.elapsed_ms_median is None:
            continue
        src = s.elapsed_ms_median / 1000
        timing_rows.append({"shape": s.id, "executions": s.executions, "source_seconds": src,
                            "igloo_seconds": b.seconds, "how": b.how, "counted": b.counted,
                            "warehouse_sizes": s.warehouse_sizes})
    speedups = [t["source_seconds"] / t["igloo_seconds"] for t in timing_rows
                if t["igloo_seconds"] > 0 and t["source_seconds"] > 0]
    timing = {
        "shapes": len(timing_rows),
        "source_seconds_weighted": sum(t["source_seconds"] * t["executions"] for t in timing_rows),
        "igloo_seconds_weighted": sum(t["igloo_seconds"] * t["executions"] for t in timing_rows),
        "geomean_speedup": math.exp(statistics.fmean(math.log(x) for x in speedups)) if speedups else None,
        "faster_on_igloo": sum(1 for x in speedups if x > 1),
        "rows": sorted(timing_rows, key=lambda t: -t["source_seconds"] * t["executions"]),
    }

    shape_rows = []
    for s in shapes:
        r = results[s.id]
        fail = r.failure if r.outcome == "failed" else None
        sqlglot = next((a for a in r.attempts if a.how == "sqlglot"), None)
        shape_rows.append({
            "id": s.id, "class": s.cls, "executions": s.executions, "succeeded": s.succeeded, "failed": s.failed,
            "elapsed_ms_total": s.elapsed_ms_total, "elapsed_ms_median": s.elapsed_ms_median,
            "rows_last": s.rows_last, "users": len(s.users), "roles": len(s.roles), "warehouses": s.warehouses,
            "warehouse_sizes": s.warehouse_sizes, "first_seen": s.first_seen, "last_seen": s.last_seen,
            "groups": s.groups, "namespace": ".".join(p for p in (s.database, s.schema) if p) or None,
            "reads": [t.name for t in s.reads], "writes": [t.name for t in s.writes], "tables_from": s.tables_from,
            "volatility": s.volatility, "outcome": r.outcome, "reason": r.reason, "works_with": r.works_with,
            "coverage": r.coverage,
            "error_class": fail.error_class if fail else None, "error": red_msg(fail.error) if fail else None,
            "attempts": [{"how": a.how, "ok": a.ok, "seconds": a.seconds, "rows": a.rows, "counted": a.counted,
                          "error_class": a.error_class, "error": red_msg(a.error), "warnings": a.warnings}
                         for a in r.attempts],
            "sql": red(s.text),
            "translated_sql": red(sqlglot.sql, "duckdb") if sqlglot and sqlglot.sql else None,
            "comparison": r.comparison,
        })

    return {
        "source": {k: meta.get(k) for k in ("source", "history", "start", "end", "notes", "warehouse", "totals",
                                            "tables", "credits_per_query", "default_namespace", "rows_read")},
        "igloo": igloo_info,
        "settings": settings,
        "summary": {
            "shapes": len(shapes), "executions": sum(s.executions for s in shapes),
            "elapsed_ms": sum(s.elapsed_ms_total for s in shapes), "outcomes": outcomes,
            "replayed_shapes": len(replayed), "replayed_executions": sum(s.executions for s in replayed),
            "not_replayed": not_replayed,
            "classes": [{"class": c, "shapes": n,
                         "executions": sum(s.executions for s in shapes if s.cls == c)}
                        for c, n in Counter(s.cls for s in shapes).most_common()],
            "total_executions": total_exec, "total_ms": total_ms,
        },
        "failures": failure_list,
        "translation": {"rows": translation_rows, "sqlglot_with_warnings": sqlglot_warned},
        "comparison": {
            "rerun": dict(comparisons["rerun"]), "history": dict(comparisons["history"]),
            "rerun_match_rate": rate(comparisons["rerun"]),
            "history_match_rate": rate(comparisons["history"]),
        },
        "timing": timing,
        "tables": [u.to_dict() for u in uses],
        "cost": cost,
        "shapes": shape_rows,
        "show_literals": show_literals,
    }


# Formatting ------------------------------------------------------------------


def n(x) -> str:
    return "-" if x is None else f"{x:,.0f}"


def pct(a, b) -> str:
    return "-" if not b else f"{100 * a / b:.1f}%"


def dur_ms(ms) -> str:
    if ms is None:
        return "-"
    s = ms / 1000
    if s < 1:
        return f"{ms:.0f} ms"
    if s < 120:
        return f"{s:.1f} s"
    if s < 7200:
        return f"{s / 60:.1f} min"
    return f"{s / 3600:.1f} h"


def secs(s) -> str:
    return "-" if s is None else dur_ms(s * 1000)


def money(x) -> str:
    if x is None:
        return "-"
    if x < 0.01:
        return f"${x:.4f}"
    if x < 100:
        return f"${x:.2f}"
    return f"${x:,.0f}"


def units(x, unit) -> str:
    return "-" if x is None else f"{x:,.4g} {unit}"


def size(b) -> str:
    if b is None:
        return "-"
    for u in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024 or u == "TB":
            return f"{b:.0f} {u}" if u == "B" else f"{b:.1f} {u}"
        b /= 1024
    return str(b)


# Markdown --------------------------------------------------------------------


def markdown(rep: dict) -> str:
    s, src, sm = rep["settings"], rep["source"], rep["summary"]
    out = [f"# Migration report: {src['source']} to Igloo", ""]
    out.append(f"History: `{src['history']}`, {src['start']} to {src['end']}. "
               f"{n(sm['executions'])} executions in {n(sm['shapes'])} shapes "
               f"({dur_ms(sm['elapsed_ms'])} of source time).")
    tot = src.get("totals") or {}
    if tot.get("executions") and tot["executions"] != sm["executions"]:
        out.append(f"The period had {n(tot['executions'])} executions; the report covers the "
                   f"{n(sm['executions'])} in the {s['max_groups']:,} costliest groups.")
    out.append("")
    for note in src.get("notes") or []:
        out.append(f"- {note}")
    out += ["", "## Outcome", "", "| Outcome | Shapes | % | Executions | % | Source time | % |",
            "|---|---:|---:|---:|---:|---:|---:|"]
    for o in sm["outcomes"]:
        if not o["shapes"]:
            continue
        out.append(f"| {o['label']} | {n(o['shapes'])} | {pct(o['shapes'], sm['shapes'])} | {n(o['executions'])} | "
                   f"{pct(o['executions'], sm['executions'])} | {dur_ms(o['elapsed_ms'])} | "
                   f"{pct(o['elapsed_ms'], sm['elapsed_ms'])} |")
    if sm["not_replayed"]:
        out += ["", "Not replayed, by reason:", "", "| Reason | Shapes | Executions |", "|---|---:|---:|"]
        for x in sm["not_replayed"][:10]:
            out.append(f"| {_md(x['reason'])} | {n(x['shapes'])} | {n(x['executions'])} |")
    rp = {o["outcome"]: o for o in sm["outcomes"]}
    if sm["replayed_shapes"]:
        ok = rp["unchanged"]["executions"] + rp["translated"]["executions"]
        out += ["", f"Of the {n(sm['replayed_shapes'])} shapes replayed ({n(sm['replayed_executions'])} executions), "
                    f"{pct(rp['unchanged']['shapes'] + rp['translated']['shapes'], sm['replayed_shapes'])} of shapes "
                    f"and {pct(ok, sm['replayed_executions'])} of executions ran."]
    nd = rep["igloo"].get("native_dialect")
    out += ["", f"Igloo's own {src['source']} dialect: "
                + (f"used (`SET sql_dialect = '{nd}'`)." if nd else
                   f"not available on this Igloo ({rep['igloo'].get('native_dialect_error') or 'no such setting'})."), ""]
    if rep["failures"]:
        out += ["## Failures on Igloo, by error class", "", "| Class | Shapes | Executions | Most common |",
                "|---|---:|---:|---|"]
        for f in rep["failures"]:
            p = f["patterns"][0]
            out.append(f"| {f['class']} | {n(f['shapes'])} | {n(f['executions'])} | {_md(p['pattern'])} "
                       f"({p['shapes']} shapes, e.g. {', '.join(p['examples'])}) |")
        out.append("")
    tr = rep["translation"]
    if tr["rows"]:
        out += ["## Translation", "", "Shapes that did not run as written:", "",
                "| Igloo's dialect | sqlglot | Shapes |", "|---|---|---:|"]
        for r in tr["rows"]:
            out.append(f"| {_yn(r['native_dialect'])} | {_yn(r['sqlglot'])} | {r['shapes']} |")
        out += ["", f"{tr['sqlglot_with_warnings']} shapes ran after a sqlglot translation that reported constructs "
                    "it could not translate exactly; compare their results before relying on them.", ""]
    c = rep["comparison"]
    out += ["## Results compared", ""]
    if c["rerun"]:
        out.append(f"Re-run on the source: {_counts(c['rerun'])}; match rate {_rate(c['rerun_match_rate'])}.")
    else:
        out.append("No statements were re-run on the source.")
    if c["history"]:
        out.append(f"Row counts against the history: {_counts(c['history'])}; match rate "
                   f"{_rate(c['history_match_rate'])}.")
    out.append("")
    t = rep["timing"]
    if t["shapes"]:
        out += ["## Timing", "",
                f"{t['shapes']} shapes ran on Igloo and have source timings. Weighted by executions, they took "
                f"{secs(t['source_seconds_weighted'])} on the source and {secs(t['igloo_seconds_weighted'])} on Igloo; "
                f"{t['faster_on_igloo']} ran faster on Igloo; geometric mean speedup "
                f"{_x(t['geomean_speedup'])}.", ""]
    out += ["## Most-used tables", "", "| Table | Shapes | Executions | Source rows | Source size | Igloo | Notes |",
            "|---|---:|---:|---:|---:|---|---|"]
    for u in rep["tables"][:25]:
        srcinfo = u.get("source") or {}
        ig = u.get("igloo")
        out.append(f"| {u['name']} | {u['read_by'] + u['written_by']} | {n(u['executions'])} | {n(srcinfo.get('rows'))} "
                   f"| {size(srcinfo.get('bytes'))} | {u['status']}{': ' + ig['full'] if ig else ''} | "
                   f"{_md('; '.join(u['notes']))} |")
    out += ["", "## Cost", ""] + _cost_md(rep["cost"]) + [""]
    return "\n".join(out) + "\n"


def _md(text: str) -> str:
    return (text or "").replace("|", "\\|").replace("\n", " ")


def _x(v) -> str:
    return "-" if v is None else f"{v:.2f}x"


def _rate(r) -> str:
    return "-" if r is None else f"{100 * r:.0f}%"


def _yn(v) -> str:
    return "-" if v is None else ("runs" if v else "fails")


def _counts(counter: dict) -> str:
    return ", ".join(f"{k} {v}" for k, v in sorted(counter.items(), key=lambda kv: -kv[1]))


def _cost_md(c: dict) -> list[str]:
    if not c:
        return ["Not computed."]
    u = c["unit"]
    sp, rp, ig = c["source_spend"], c["replayed"], c["igloo"]
    lines = ["| | Source | Igloo |", "|---|---|---|"]
    lines.append(f"| The whole period | {units(sp['total'], u)}, {money(sp['dollars'])} ({sp['label']}) | always on: "
                 f"{money(ig['always_on_dollars'])} ({ig['machine'] or 'machine'} for {ig['period_hours']:.0f} h, "
                 f"estimated) |")
    wa = c.get("workload_attributed") or {}
    if wa.get("total") is not None:
        lines.append(f"| Compute attributed to the extracted workload's queries | {units(wa['total'], u)}, "
                     f"{money(wa['dollars'])} (measured) | |")
    lines.append(f"| Busy time of the {rp['shapes']} shapes that ran on Igloo | {rp['source_hours']:.3g} h (measured) | "
                 f"{rp['igloo_hours']:.3g} h (estimated) |")
    lines.append(f"| Cost of that busy time | {units(rp['source_units'], u)}, {money(rp['source_dollars'])} "
                 f"({rp['source_units_label']}) | {money(rp['igloo_dollars'])} (estimated) |")
    if sp.get("note"):
        lines += ["", f"Source spend: {sp['note']}"]
    t = c.get("tool")
    if t:
        lines += ["", f"This report's own use of the source warehouse: {units(t['estimated_total'], u)} "
                      f"(comparison re-runs: {units(t['estimated_comparison'], u)}), {t['how']}."]
    return lines


# HTML ------------------------------------------------------------------------

CSS = """
:root{--bg:#fbfbfa;--fg:#1d1d1b;--muted:#6b6b66;--line:#e4e3df;--card:#fff;--accent:#2f6f9f;
--ok:#2e7d4f;--warn:#b7791f;--bad:#b4433a;--na:#8a8a85;--code:#f3f2ee}
@media (prefers-color-scheme: dark){:root:not([data-theme="light"]){--bg:#161615;--fg:#ecebe7;--muted:#a3a29c;
--line:#33322f;--card:#1e1e1c;--accent:#7fb3db;--ok:#5fbf86;--warn:#e0a84a;--bad:#e2776d;--na:#8f8e88;--code:#262624}}
:root[data-theme="dark"]{--bg:#161615;--fg:#ecebe7;--muted:#a3a29c;--line:#33322f;--card:#1e1e1c;--accent:#7fb3db;
--ok:#5fbf86;--warn:#e0a84a;--bad:#e2776d;--na:#8f8e88;--code:#262624}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);
font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif}
main{max-width:1100px;margin:0 auto;padding:24px 16px 64px}h1{font-size:26px;margin:0 0 4px}
h2{font-size:19px;margin:36px 0 10px;padding-top:8px;border-top:1px solid var(--line)}h3{font-size:15px;margin:18px 0 6px}
p,li{color:var(--fg)}.muted{color:var(--muted)}small{color:var(--muted)}a{color:var(--accent)}
table{border-collapse:collapse;width:100%;margin:8px 0 4px;font-size:14px}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line);vertical-align:top}
th{font-weight:600;color:var(--muted);font-size:13px}td.r,th.r{text-align:right;font-variant-numeric:tabular-nums}
.wrap{overflow-x:auto}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;margin:16px 0}
.card{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:12px}
.card b{display:block;font-size:24px;font-variant-numeric:tabular-nums}.card span{color:var(--muted);font-size:13px}
.bar{display:flex;height:14px;border-radius:4px;overflow:hidden;margin:8px 0 4px;background:var(--line)}
.bar i{display:block;height:100%}.legend{display:flex;flex-wrap:wrap;gap:4px 14px;font-size:13px;color:var(--muted)}
.legend i{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:5px;vertical-align:-1px}
code,pre{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:12.5px}
pre{background:var(--code);padding:10px;border-radius:6px;white-space:pre-wrap;word-break:break-word;margin:6px 0}
details{border-bottom:1px solid var(--line);padding:6px 0}summary{cursor:pointer}
.tag{display:inline-block;font-size:12px;padding:1px 7px;border-radius:10px;border:1px solid var(--line);color:var(--muted)}
.o-unchanged{color:var(--ok)}.o-translated{color:var(--accent)}.o-failed{color:var(--bad)}.o-nodata{color:var(--warn)}
.label{font-size:11px;text-transform:uppercase;letter-spacing:.04em;color:var(--muted)}
"""

COLORS = {"unchanged": "var(--ok)", "translated": "var(--accent)", "failed": "var(--bad)", "no data": "var(--warn)",
          "source metadata": "#9b7fb8", "failed on source": "#7a5b55", "not replayed": "var(--na)",
          "not in sample": "var(--line)"}


def _e(x) -> str:
    return html.escape("" if x is None else str(x))


def _table(head: list[str], rows: list[list], right: set[int] = frozenset()) -> str:
    th = "".join(f'<th class="{"r" if i in right else ""}">{_e(h)}</th>' for i, h in enumerate(head))
    body = "".join("<tr>" + "".join(f'<td class="{"r" if i in right else ""}">{c}</td>' for i, c in enumerate(r))
                   + "</tr>" for r in rows)
    return f'<div class="wrap"><table><thead><tr>{th}</tr></thead><tbody>{body}</tbody></table></div>'


def render_html(rep: dict) -> str:
    src, sm, s = rep["source"], rep["summary"], rep["settings"]
    parts = []
    title = f"Migration report: {src['source']} to Igloo"
    parts.append(f"<h1>{_e(title)}</h1>")
    parts.append(f'<p class="muted">History from <code>{_e(src["history"])}</code>, {_e(src["start"])} to '
                 f'{_e(src["end"])}. Igloo {_e(rep["igloo"].get("version"))} at {_e(rep["igloo"].get("address"))}. '
                 f'Generated {_e(s.get("generated"))}.</p>')
    rp = {o["outcome"]: o for o in sm["outcomes"]}
    ran = rp["unchanged"]["executions"] + rp["translated"]["executions"]
    parts.append('<div class="cards">' + "".join(
        f'<div class="card"><b>{v}</b><span>{_e(k)}</span></div>' for k, v in [
            ("shapes", n(sm["shapes"])), ("executions", n(sm["executions"])),
            ("source time", dur_ms(sm["elapsed_ms"])),
            ("of replayed executions ran on Igloo", pct(ran, sm["replayed_executions"])),
            ("result match rate (re-run)", "-" if rep["comparison"]["rerun_match_rate"] is None
             else f'{100 * rep["comparison"]["rerun_match_rate"]:.0f}%'),
        ]) + "</div>")
    if src.get("notes"):
        parts.append("<ul>" + "".join(f"<li class='muted'>{_e(x)}</li>" for x in src["notes"]) + "</ul>")

    # Outcomes
    parts.append("<h2>How far the workload gets</h2>")
    for weight, label in (("executions", "Executions"), ("shapes", "Shapes"), ("elapsed_ms", "Source time")):
        total = sum(o[weight] for o in sm["outcomes"]) or 1
        bar = "".join(f'<i style="width:{100 * o[weight] / total:.3f}%;background:{COLORS[o["outcome"]]}" '
                      f'title="{_e(o["label"])}: {pct(o[weight], total)}"></i>' for o in sm["outcomes"] if o[weight])
        parts.append(f'<div class="label">{label}</div><div class="bar">{bar}</div>')
    parts.append('<div class="legend">' + "".join(
        f'<span><i style="background:{COLORS[o["outcome"]]}"></i>{_e(o["label"])}</span>'
        for o in sm["outcomes"] if o["shapes"]) + "</div>")
    rows = [[_e(o["label"]), n(o["shapes"]), pct(o["shapes"], sm["shapes"]), n(o["executions"]),
             pct(o["executions"], sm["executions"]), dur_ms(o["elapsed_ms"]), pct(o["elapsed_ms"], sm["elapsed_ms"])]
            for o in sm["outcomes"] if o["shapes"]]
    parts.append(_table(["Outcome", "Shapes", "%", "Executions", "%", "Source time", "%"], rows, {1, 2, 3, 4, 5, 6}))
    if sm["not_replayed"]:
        parts.append("<h3>Not replayed, by reason</h3>")
        parts.append(_table(["Reason", "Shapes", "Executions"],
                            [[_e(x["reason"]), n(x["shapes"]), n(x["executions"])] for x in sm["not_replayed"][:15]],
                            {1, 2}))
    parts.append('<p class="muted">A shape is a statement with its literals taken out, reading the same tables. '
                 '"Run unchanged" means the statement ran on Igloo as written, apart from its table names, which are '
                 "rewritten to Igloo's; \"after translation\" means it ran in Igloo's own dialect of the source's SQL "
                 "or after sqlglot translated it to DuckDB's. Only queries are replayed unless --replay-writes is "
                 "given.</p>")
    nd = rep["igloo"].get("native_dialect")
    parts.append(f"<p>Igloo's own {_e(src['source'])} dialect: " + (
        f"used (<code>SET sql_dialect = '{_e(nd)}'</code>).</p>" if nd else
        f"not available on this Igloo <span class='muted'>({_e(rep['igloo'].get('native_dialect_error'))})</span>."
        "</p>"))
    parts.append(_table(["Class", "Shapes", "Executions"],
                        [[_e(c["class"]), n(c["shapes"]), n(c["executions"])] for c in sm["classes"]], {1, 2}))

    # Failures
    parts.append("<h2>Failures on Igloo, by error class</h2>")
    if not rep["failures"]:
        parts.append("<p>None.</p>")
    shapes = {x["id"]: x for x in rep["shapes"]}
    for f in rep["failures"]:
        parts.append(f"<h3>{_e(f['class'])} <span class='muted'>· {n(f['shapes'])} shapes, "
                     f"{n(f['executions'])} executions</span></h3>")
        rows = []
        for p in f["patterns"][:8]:
            ex = shapes.get(p["examples"][0], {})
            rows.append([f"<code>{_e(p['pattern'])}</code>", n(p["shapes"]),
                         " ".join(f"<a href='#s-{_e(i)}'>{_e(i)}</a>" for i in p["examples"]),
                         f"<pre>{_e(_clip(ex.get('sql'), 400))}</pre>"])
        parts.append(_table(["Error", "Shapes", "Examples", "First example"], rows, {1}))

    # Translation
    tr = rep["translation"]
    parts.append("<h2>Translation</h2>")
    if tr["rows"]:
        parts.append(_table(["Igloo's dialect", "sqlglot", "Shapes"],
                            [[_yn(r["native_dialect"]), _yn(r["sqlglot"]), n(r["shapes"])] for r in tr["rows"]], {2}))
        parts.append(f"<p class='muted'>Of the shapes that did not run as written. {tr['sqlglot_with_warnings']} ran "
                     "after a sqlglot translation that reported constructs it could not translate exactly.</p>")
    else:
        parts.append("<p>Every replayed shape that ran, ran as written.</p>")

    # Comparison
    c = rep["comparison"]
    parts.append("<h2>Results compared</h2>")
    for key, label in (("rerun", "Re-run on the source"), ("history", "Row count recorded in the history")):
        if c[key]:
            parts.append(f"<p><b>{label}:</b> {_e(_counts(c[key]))}. Match rate "
                         f"{_rate(c[key + '_match_rate'])}.</p>")
    parts.append("<p class='muted'>Results are compared by row count and an order-independent checksum, integers and "
                 f"decimals exactly and doubles to {_e(s.get('digits'))} significant digits; small results that differ "
                 "are then compared row by row, "
                 "numbers to the precision of the less precise side (\"match within precision\"). \"(explained)\" "
                 "marks differences that need not be Igloo's: the statement depends on the time, on chance or on the "
                 "session, a LIMIT picks among ties, or Igloo's copy of a table differs from the source's table now. "
                 "Explained differences are left out of the match rate.</p>")
    rows = []
    for x in rep["shapes"]:
        cmp_ = (x.get("comparison") or {})
        for key in ("rerun", "history"):
            v = cmp_.get(key)
            if v and (key == "rerun" or v["verdict"] not in MATCHES):
                rows.append([f"<a href='#s-{_e(x['id'])}'>{_e(x['id'])}</a>", _e(v["method"]), _e(v["verdict"]),
                             n(v.get("source_rows")), n(v.get("igloo_rows")),
                             _e("; ".join(v.get("explained_by") or [])), _e("; ".join(v.get("notes") or []))])
    if rows:
        parts.append(_table(["Shape", "Method", "Verdict", "Source rows", "Igloo rows", "Explained by", "Notes"], rows,
                            {3, 4}))

    # Timing
    t = rep["timing"]
    parts.append("<h2>Timing</h2>")
    if t["shapes"]:
        parts.append(f"<p>{t['shapes']} shapes ran on Igloo and have source timings. Weighted by executions: "
                     f"{secs(t['source_seconds_weighted'])} on the source, {secs(t['igloo_seconds_weighted'])} on Igloo. "
                     f"{t['faster_on_igloo']} ran faster on Igloo; geometric mean speedup "
                     f"{_x(t['geomean_speedup'])}.</p>")
        parts.append("<p class='muted'>Source: the median elapsed time of its successful executions, as the history "
                     "records it (queuing and compilation included, on the warehouse sizes shown). Igloo: one run, "
                     "wall-clock at this client, fetching the result.</p>")
        rows = [[f"<a href='#s-{_e(r['shape'])}'>{_e(r['shape'])}</a>", n(r["executions"]), _e(", ".join(
            r["warehouse_sizes"])), secs(r["source_seconds"]), secs(r["igloo_seconds"]),
                 f"{r['source_seconds'] / r['igloo_seconds']:.2f}x" if r["igloo_seconds"] else "-",
                 _e(r["how"] + (", rows counted" if r["counted"] else ""))] for r in t["rows"][:40]]
        parts.append(_table(["Shape", "Executions", "Source size", "Source", "Igloo", "Speedup", "How"], rows,
                            {1, 3, 4, 5}))
    else:
        parts.append("<p>No shape ran on Igloo with a source timing to compare.</p>")

    # Tables
    parts.append("<h2>Tables</h2>")
    parts.append("<p class='muted'>The tables and views the workload names, most used first, and whether Igloo has "
                 "them: mirrored in its catalog, reachable through a connection, or missing. A shape replays only "
                 "where every table it reads is in Igloo. Snapshot: when Igloo's copy last changed.</p>")
    rows = []
    for u in rep["tables"][:100]:
        si, ig = u.get("source") or {}, u.get("igloo") or {}
        rows.append([_e(u["name"]), n(u["read_by"] + u["written_by"]), n(u["executions"]), n(si.get("rows")),
                     size(si.get("bytes")), _e(u["status"]), _e(ig.get("full")), n(ig.get("rows")),
                     _e((ig.get("snapshot") or "")[:19]), _e("; ".join(u["notes"]))])
    parts.append(_table(["Table", "Shapes", "Executions", "Source rows", "Source size", "In Igloo", "Igloo table",
                         "Igloo rows", "Snapshot (UTC)", "Notes"], rows, {1, 2, 3, 4, 7}))

    # Cost
    parts.append("<h2>Cost</h2>")
    parts.append(_cost_html(rep["cost"]))

    # Shapes
    parts.append("<h2>Shapes</h2>")
    parts.append("<p class='muted'>" + ("Statements as the history recorded them." if rep["show_literals"] else
                                        "Literals are shown as ? and comments removed (--show-literals keeps them).")
                 + "</p>")
    for x in rep["shapes"]:
        cls = "o-" + x["outcome"].replace(" ", "")
        head = (f"<code>{_e(x['id'])}</code> <span class='tag'>{_e(x['class'])}</span> "
                f"<span class='{cls}'>{_e(OUTCOME_TEXT[x['outcome']])}</span> · {n(x['executions'])} executions · "
                f"{dur_ms(x['elapsed_ms_total'])}")
        body = [f"<p class='muted'>{_e(x.get('reason') or '')}</p>" if x.get("reason") else ""]
        body.append(f"<p><span class='label'>Reads</span> {_e(', '.join(x['reads']) or '-')} "
                    f"<span class='label'>Writes</span> {_e(', '.join(x['writes']) or '-')} "
                    f"<span class='muted'>({_e(x['tables_from'])}; session {_e(x['namespace'] or '-')})</span></p>")
        body.append(f"<pre>{_e(x['sql'])}</pre>")
        for a in x["attempts"]:
            status = f"ran in {secs(a['seconds'])}, {n(a['rows'])} rows" if a["ok"] else \
                f"{_e(a['error_class'])}: {_e(a['error'])}"
            warn = f"<br><small>sqlglot: {_e('; '.join(a['warnings']))}</small>" if a.get("warnings") else ""
            body.append(f"<p><span class='label'>{_e(a['how'])}</span> {status}{warn}</p>")
        if x.get("translated_sql"):
            body.append(f"<details><summary class='muted'>sqlglot's translation</summary><pre>"
                        f"{_e(x['translated_sql'])}</pre></details>")
        cm = x.get("comparison") or {}
        for key in ("rerun", "history"):
            v = cm.get(key)
            if v:
                extra = (v.get("explained_by") or []) + (v.get("notes") or [])
                body.append(f"<p><span class='label'>{_e(v['method'])}</span> {_e(v['verdict'])}: source "
                            f"{n(v.get('source_rows'))} rows, Igloo {n(v.get('igloo_rows'))}"
                            + (f" <small>({_e('; '.join(extra))})</small>" if extra else "") + "</p>")
        parts.append(f"<details id='s-{_e(x['id'])}'><summary>{head}</summary>{''.join(body)}</details>")

    parts.append("<h2>Settings</h2>")
    parts.append(f"<pre>{_e(json.dumps(rep['settings'], indent=2, default=str))}</pre>")
    return (f"<!doctype html><html lang='en'><head><meta charset='utf-8'><meta name='viewport' "
            f"content='width=device-width,initial-scale=1'><title>Migration report</title><style>{CSS}</style></head>"
            f"<body><main>{''.join(parts)}</main></body></html>\n")


def _clip(text, limit):
    if text is None:
        return ""
    return text if len(text) <= limit else text[:limit] + " …"


def _cost_html(c: dict) -> str:
    if not c:
        return "<p>Not computed.</p>"
    u = c["unit"]
    sp, rp, ig = c["source_spend"], c["replayed"], c["igloo"]
    rows = [
        ["The whole period", f"{units(sp['total'], u)} · {money(sp['dollars'])} <small>{_e(sp['label'])}</small>",
         f"always on: {money(ig['always_on_dollars'])} <small>estimated: {_e(ig['machine'] or 'the machine')} for "
         f"{ig['period_hours']:.0f} h</small>"],
    ]
    wa = c.get("workload_attributed") or {}
    if wa.get("total") is not None:
        rows.append(["Compute attributed to the extracted workload's queries",
                     f"{units(wa['total'], u)} · {money(wa['dollars'])} <small>measured, idle time excluded</small>", ""])
    rows += [
        [f"Busy time of the {rp['shapes']} shapes that ran on Igloo",
         f"{rp['source_hours']:.3g} h <small>measured</small>", f"{rp['igloo_hours']:.3g} h <small>estimated</small>"],
        ["Cost of that busy time", f"{units(rp['source_units'], u)} · {money(rp['source_dollars'])} "
                                   f"<small>{_e(rp['source_units_label'])}</small>",
         f"{money(rp['igloo_dollars'])} <small>estimated</small>"],
    ]
    out = _table(["", "Source", "Igloo"], rows)
    if sp.get("by_warehouse"):
        out += _table(["Warehouse", f"{u} (measured)", "At list price"],
                      [[_e(w), units(v, u), money(v * c["price_per_unit"]) if c["price_per_unit"] else "-"]
                       for w, v in sorted(sp["by_warehouse"].items(), key=lambda kv: -kv[1])], {1, 2})
    notes = [f"Prices: {money(c['price_per_unit'])} per {u[:-1] if u.endswith('s') else u} and "
             f"{money(ig['price_per_hour'])} an hour for Igloo's machine, from the pricing file; replace them with "
             "your own rates (--set).",
             "Igloo's busy time is each shape's replay time times its executions; the source's is the execution time "
             "the history records. The source cost is what it attributed to those executions where it records that "
             "(measured), else their execution time at the warehouse size's rate (estimated)."]
    if sp.get("note"):
        notes.append(f"Source spend: {sp['note']}")
    t = c.get("tool")
    if t:
        notes.append(f"This report's own use of the source warehouse: {units(t['estimated_total'], u)}, of which "
                     f"comparison re-runs {units(t['estimated_comparison'], u)} ({t['how']}).")
    return out + "".join(f"<p class='muted'>{_e(x)}</p>" for x in notes)
