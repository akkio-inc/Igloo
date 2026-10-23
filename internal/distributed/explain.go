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

// gigabytes writes a size in gigabytes.
func gigabytes(n int64) string {
	return strconv.FormatFloat(float64(n)/1e9, 'f', 1, 64) + " GB"
}
