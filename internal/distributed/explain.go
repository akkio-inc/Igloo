package distributed

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Explain says how query would run in mode, without running it or starting
// workers: where it would run and why, with the estimates auto mode weighs;
// how many partitions its exchanges would have with the workers known now;
// its stages, each with the SQL its tasks run; what the coordinator runs
// over their results; and why parts of it stay on the coordinator. Each row
// is a key and a value, as DuckDB's EXPLAIN gives them. A statement that
// cannot run on workers says why.
func (co *Coordinator) Explain(ctx context.Context, query string, env Env, mode Mode) ([][2]string, error) {
	plan, d, err := co.plan(ctx, query, env, mode, true, nil)
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
	estimate := fmt.Sprintf("one machine %.0f s, workers %.0f s on %d; %d levels; reads %s, exchanges %s; %.3g rows",
		math.Round(e.oneMachine), math.Round(e.onWorkers), e.workers, e.levels, gigabytes(e.bytes), gigabytes(e.exchanged), e.rows)
	if plan.shortOfMemory {
		estimate += "; too large for this replica's memory"
	}
	rows := [][2]string{{"runs on", where}, {"reason", d.reason}, {"estimate", estimate}}
	if r, ok := co.history.get(plan.Key); ok && r.exchangeRows > 0 {
		rows = append(rows, [2]string{"measured", fmt.Sprintf("its last run on workers exchanged %.3g rows, %s; a stage wrote up to %.1f rows a row it read",
			float64(r.exchangeRows), gigabytes(r.exchangeBytes), r.fanout)})
	}
	for _, j := range semiJoins(plan.Stages) {
		rows = append(rows, [2]string{"semi-join estimate", j})
	}

	// The partitions Execute would give its exchanges.
	n := len(d.ws)
	if n == 0 {
		n = max(1, plan.workers)
	}
	parts := exchangePartitions(plan.Stages, d.ws, n, uploadBytes(co.opts.Warehouse))
	rows = append(rows, [2]string{"partitions", strconv.Itoa(parts)})
	for _, s := range plan.Stages {
		rows = append(rows, [2]string{s.String(), s.SQL})
	}
	if final, err := co.parser.print(plan.final); err == nil {
		rows = append(rows, [2]string{"the coordinator", final})
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

// gigabytes writes a size in gigabytes.
func gigabytes(n int64) string {
	return strconv.FormatFloat(float64(n)/1e9, 'f', 1, 64) + " GB"
}
