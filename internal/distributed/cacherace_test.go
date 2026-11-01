package distributed

import (
	"context"
	"errors"
	"testing"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// blockRace is how DuckDB words cache_httpfs's failed removal of a block of
// the read cache that another read removed first.
const blockRace = `IO Error: Could not remove file "/var/lib/igloo/cache/blocks/` +
	`5185916048c6aa68b0867467d6512d995b2ced9f1303c43323349e8c434303a4-f.parquet-1048576-524288": No such file or directory`

// A task whose read of a worker's read cache raced another over a block
// runs again at once, on the next worker, and the statement succeeds,
// also with one worker, as the large worker alone; the coordinator counts
// each such run. One that races every time fails the statement after
// maxCacheRaces runs again.
func TestCacheRaceTasks(t *testing.T) {
	for _, workers := range []int{1, 2} {
		h := newHarness(t, 0.01, 3, workers)
		h.co.maxBroadcast = 1
		h.co.opts.Metrics = NewMetrics(prometheus.NewRegistry())
		var failures int
		taskFault = func(*taskRequest) error {
			if failures < 3 {
				failures++
				return &duckdb.Error{Type: duckdb.ErrorTypeIO, Msg: blockRace}
			}
			return nil
		}
		q := `SELECT o_orderpriority, count(*) FROM orders JOIN lineitem ON l_orderkey = o_orderkey GROUP BY 1 ORDER BY 1`
		if !h.check(q) {
			t.Fatalf("%d workers: did not run distributed", workers)
		}
		if n := testutil.ToFloat64(h.co.opts.Metrics.tasks.WithLabelValues("cache_race")); n != 3 {
			t.Errorf("%d workers: %v tasks run again after a race over a block, want 3", workers, n)
		}
		taskFault = func(*taskRequest) error { return &duckdb.Error{Type: duckdb.ErrorTypeIO, Msg: blockRace} }
		_, _, _, _, err := h.run(context.Background(), q, ModeOn)
		var cr *cacheRaceError
		if !errors.As(err, &cr) {
			t.Errorf("%d workers: racing every time: %v", workers, err)
		}
		taskFault = nil
	}
}
