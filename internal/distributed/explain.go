package distributed

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
)

// Explain says how query would run in mode, without running it or starting
// workers: where it would run and why, with the estimates auto mode weighs;
// how many partitions its exchanges would have with the workers known now;
// its stages, each with the SQL its tasks run; what the coordinator runs
// over their results; and why parts of it stay on the coordinator. Each row
// is a key and a value, as DuckDB's EXPLAIN gives them. A statement that
// cannot run on workers says why. It decides as running the statement does
// (Plan, then Execute's preflight), and only runs nothing.
func (co *Coordinator) Explain(ctx context.Context, query string, env Env, mode Mode) ([][2]string, error) {
	plan, d, err := co.plan(ctx, query, env, mode, true, nil)
	if err == nil && d.onWorkers {
		_, _, err = co.preflight(ctx, plan)
	}
	if err != nil {
		if IsUnsupported(err) {
			return [][2]string{{"runs on", "this replica"}, {"reason", strings.TrimPrefix(err.Error(), "cannot distribute: ")}}, nil
		}
		return nil, err
	}
	where := "workers"
	if !d.onWorkers {
		where = "this replica"
	}
	e := d.e
	on := fmt.Sprintf("workers %.0f s on %d", math.Round(e.onWorkers), e.workers)
	if e.large {
		where = LargePlacement
		on = fmt.Sprintf("the large worker %.0f s", math.Round(e.onWorkers))
	}
	estimate := fmt.Sprintf("one machine %.0f s, %s; %d levels; reads %s, exchanges %s; %.3g rows",
		math.Round(e.oneMachine), on, e.levels, gigabytes(e.bytes), gigabytes(e.exchanged), e.rows)
	if plan.shortOfMemory {
		estimate += "; too large for this replica's memory"
	}
	rows := [][2]string{{"runs on", where}, {"reason", d.reason}, {"estimate", estimate}}
	// What this replica would spill, and the ways on workers weighed:
	// how many workers, or the large worker, and why (see pick).
	rows = append(rows, [2]string{"working set", describeSet(e)})
	rows = append(rows, [2]string{"spill", "this replica: " + e.local.String()})
	for _, o := range e.options {
		if o.spill.factor > 1 || o.large {
			rows = append(rows, [2]string{"spill", fmt.Sprintf("%s: %s", o, o.spill)})
		}
	}
	if e.fleet != "" {
		rows = append(rows, [2]string{"fleet", e.fleet}, [2]string{"options", describeOptions(e.options, e.dollars)})
	}
	if r, ok := co.history.get(plan.Key); ok && r.exchangeRows > 0 {
		rows = append(rows, [2]string{"measured", fmt.Sprintf("its last run on workers exchanged %.3g rows, %s; a stage wrote up to %.1f rows a row it read",
			float64(r.exchangeRows), gigabytes(r.exchangeBytes), r.fanout)})
	}
	for _, j := range semiJoins(plan.Stages) {
		rows = append(rows, [2]string{"semi-join estimate", j})
	}

	// The partitions Execute would give its exchanges, on the workers it
	// would take (see Execute).
	ws, n := d.ws, max(1, plan.workers)
	if plan.force && plan.mayStart && !plan.large {
		n = max(n, co.opts.Pool.MaxWorkers, len(ws))
	}
	if len(ws) > 0 {
		ws = leastBusy(ws, n)
		n = len(ws)
	}
	parts := exchangePartitions(plan.Stages, ws, n, uploadBytes(co.opts.Warehouse))
	rows = append(rows, [2]string{"partitions", strconv.Itoa(parts)})
	rows = append(rows, plan.computeLater()...)
	for _, s := range plan.Stages {
		rows = append(rows, [2]string{s.String(), s.SQL})
	}
	if final, err := co.parser.print(plan.final); err == nil {
		rows = append(rows, [2]string{"the coordinator", final})
	}
	for _, b := range plan.buckets {
		rows = append(rows, [2]string{"buckets", b})
	}
	for _, u := range plan.uncertain {
		rows = append(rows, [2]string{"uncertain size", u.name + ": written and measured first, then read whole if every task can hold it, " +
			"or by partition as planned"})
	}
	for _, reason := range plan.local {
		rows = append(rows, [2]string{"stays on the coordinator", reason})
	}
	return rows, nil
}

// semiJoins describes, for EXPLAIN DISTRIBUTED, the stages that join a
// relation of a table filtered by a constant (filtersSelectively) to other
// rows, as an audience joins people's events: such a join keeps about
// filterSelectivity of the other rows, which auto mode's estimates do not
// count yet. A report's audience, filtered by a constant, kept 189,400
// of 18.9 million rows, and left a FLATTEN 2.6 million rows to unnest
// where another, without one, unnested 140 million; counting it in the
// estimates and not the rows unnested is left to a calibration that can
// tell both apart.
func semiJoins(stages []*Stage) []string {
	selective := map[*Exchange]string{}
	var out []string
	for _, s := range stages {
		var sel string
		others := false
		for _, in := range s.Inputs {
			switch {
			case in.Table != nil && filtersSelectively(s.fragment, in.Table):
				sel = in.Table.String()
			case in.Table != nil:
				others = true
			case in.Exchange != nil && selective[in.Exchange] != "":
				sel = selective[in.Exchange]
			case in.Exchange != nil:
				others = true
			}
		}
		switch {
		case sel != "" && others:
			out = append(out, fmt.Sprintf("%s joins %s, filtered by a constant, to rows of which it keeps about %.0f%%", s, sel, filterSelectivity*100))
		case sel != "" && s.Out != nil:
			selective[s.Out] = sel
		}
	}
	return out
}

// describeSet says where an estimate's working set comes from and, from
// DuckDB's plan, what its largest holders hold.
func describeSet(e estimate) string {
	if !e.planned {
		return fmt.Sprintf("%s: six times what the stages feeding its joins and distinct counts read", gigabytes(int64(e.local.set)))
	}
	parts := slices.Clone(e.parts)
	slices.SortFunc(parts, func(a, b heldPart) int { return cmp.Compare(b.bytes, a.bytes) })
	var b strings.Builder
	fmt.Fprintf(&b, "%s by DuckDB's plan", gigabytes(int64(e.local.set)))
	for i, p := range parts {
		if i == 3 {
			fmt.Fprintf(&b, "; %d more", len(parts)-i)
			break
		}
		fmt.Fprintf(&b, "; %s of %.3g rows %s", p.op, p.rows, gigabytes(int64(p.bytes)))
	}
	return b.String()
}

// gigabytes writes a size in gigabytes.
func gigabytes(n int64) string {
	return strconv.FormatFloat(float64(n)/1e9, 'f', 1, 64) + " GB"
}
