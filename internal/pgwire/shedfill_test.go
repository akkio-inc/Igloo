package pgwire

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestShedFillRunsInPasses has the memory watch stop a CREATE TABLE ... AS
// of an aggregate while it fills its table, as it stopped a copy's
// count(DISTINCT) of a billion ids: the fill runs again, its groups a share
// at a time, and the table holds what one run gives. It failed with
// SQLSTATE 53200.
func TestShedFillRunsInPasses(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	metrics := NewMetrics(prometheus.NewRegistry())
	r := vc.replica(t, func(o *Options) {
		// Room enough that DuckDB's estimate runs the fill as one.
		o.QueryMemory = 64 << 30
		o.Metrics = metrics
	})
	admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE SCHEMA IF NOT EXISTS lake.public")
	mustExec(t, admin, "CREATE TABLE lake.public.visits AS SELECT range AS id, range % 40 AS grp FROM range(40000000)")
	passes := testutil.ToFloat64(metrics.aggPasses)
	shed := make(chan bool, 1)
	r.srv.beforeFill = func(ctx context.Context) {
		go func() {
			time.Sleep(200 * time.Millisecond)
			shed <- ctx.Err() == nil && r.srv.ShedMemory(0)
		}()
	}
	start := time.Now()
	if _, err := admin.Exec(t.Context(), "CREATE TABLE lake.public.reach AS SELECT grp, count(DISTINCT id) AS ids, count(*) AS n FROM lake.public.visits GROUP BY grp"); err != nil {
		t.Fatalf("the create, stopped for memory: %v", err)
	}
	if !<-shed {
		t.Skipf("the fill ended before it was stopped, in %s", time.Since(start))
	}
	if got := testutil.ToFloat64(metrics.aggPasses); got != passes+1 {
		t.Errorf("%v fills in passes, want 1", got-passes)
	}
	got := texts(t, admin, "SELECT grp, ids, n FROM lake.public.reach ORDER BY grp")
	want := texts(t, admin, "SELECT grp, count(DISTINCT id), count(*) FROM lake.public.visits GROUP BY grp ORDER BY grp")
	if len(got) != 40 || len(got) != len(want) {
		t.Fatalf("%d groups, want 40", len(got))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("group %d: %s, want %s", i, got[i], want[i])
		}
	}
}
