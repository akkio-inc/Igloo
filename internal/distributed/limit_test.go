package distributed

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestAnyRows checks LIMIT without ORDER BY on workers: each task keeps
// that many rows, the coordinator takes that many of what they sent, and
// once the tasks that ended sent as many the others are stopped, a slow
// worker's among them. The rows are as many as one machine's, and rows one
// machine's statement could return. Auto mode leaves such a scan of a table
// to one machine, which stops at the first rows it reads, unless its
// filters keep few of the table's rows.
func TestAnyRows(t *testing.T) {
	h := newHarness(t, 0.02, 6, 3)
	ctx := context.Background()
	// One worker answers its tasks only after the statement could be done.
	var slowed atomic.Int32
	h.workers[2].beforeTask = func(rw http.ResponseWriter, r *http.Request) bool {
		slowed.Add(1)
		select {
		case <-r.Context().Done():
		case <-time.After(20 * time.Second):
		}
		return false
	}
	var stopped atomic.Int32
	hook := func(r slog.Record) {
		if strings.Contains(r.Message, "stopping the others") {
			stopped.Add(1)
		}
	}
	h.hook.Store(&hook)
	for _, q := range []string{
		`SELECT l_orderkey, l_linenumber, l_comment FROM lineitem WHERE l_quantity > 10 LIMIT 25`,
		`SELECT l_orderkey, l_linenumber FROM lineitem LIMIT 7 OFFSET 3`,
		`SELECT o_orderkey, o_custkey FROM orders JOIN customer ON c_custkey = o_custkey WHERE c_mktsegment = 'BUILDING' LIMIT 40`,
	} {
		t.Run(short(q), func(t *testing.T) {
			stopped.Store(0)
			start := time.Now()
			got, _, plan, _, err := h.run(ctx, q, ModeOn)
			if err != nil {
				t.Fatal(err)
			}
			if took := time.Since(start); took > 15*time.Second {
				t.Errorf("took %s: the slow worker's tasks were waited for", took)
			}
			all, _, err := h.query(ctx, q[:strings.LastIndex(q, " LIMIT")])
			if err != nil {
				t.Fatal(err)
			}
			want, _, err := h.query(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(want) {
				t.Errorf("%d rows, want %d\n%s", len(got), len(want), plan.describe())
			}
			key := func(r []any) string { return fmt.Sprint(r...) }
			for _, r := range got {
				if !slices.ContainsFunc(all, func(a []any) bool { return key(a) == key(r) }) {
					t.Errorf("a row one machine's statement would not return: %v", r)
				}
			}
			if stopped.Load() == 0 {
				t.Error("no stage stopped its other tasks once it had the rows")
			}
		})
	}
	if slowed.Load() == 0 {
		t.Error("the slow worker got no task")
	}
	// Auto mode: any rows of a scan stay on one machine, unless a filter
	// keeps few of them.
	h.workers[2].beforeTask = nil
	ws := h.co.pool.known(ctx)
	for q, want := range map[string]bool{
		`SELECT l_orderkey, l_comment FROM lineitem WHERE l_quantity > 10 LIMIT 25`: false,
		`SELECT l_orderkey, l_comment FROM lineitem WHERE l_suppkey = 7 LIMIT 25`:   true,
	} {
		stmt, err := h.co.parser.parse(q)
		if err != nil {
			t.Fatal(err)
		}
		opts := planOptions{now: time.Now(), maxBroadcast: defaultBroadcast, minBytes: 1, taskMemory: taskMemory(ws), workers: len(ws)}
		_, err = newPlanner(ctx, harnessEnv{h}, h.co.parser, h.co.aggregates(ctx, harnessEnv{h}), opts).plan(stmt)
		if got := err == nil; got != want {
			t.Errorf("%s: planned unforced %v (%v), want %v", q, got, err, want)
		}
	}
}
