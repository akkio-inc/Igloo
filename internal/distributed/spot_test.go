package distributed

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/spot"
)

// A worker on a Spot Instance that AWS takes back. The instance metadata
// service announces it, two seconds ahead here, while the worker runs a
// task of a statement: the worker's watch hears it, the worker takes no
// more tasks and finishes what it runs until half a second before the time
// given, as serveWorker has it, and then the instance goes, with the task
// it was still running. That task runs on another worker, the statement's
// later tasks go to the other workers, and the statement finishes with
// the rows one machine computes.
func TestSpotInterruption(t *testing.T) {
	defer func(every, timeout time.Duration) { taskCheckEvery, taskCheckTimeout = every, timeout }(taskCheckEvery, taskCheckTimeout)
	taskCheckEvery, taskCheckTimeout = 50*time.Millisecond, 200*time.Millisecond
	h := newHarness(t, 0.02, 6, 3)
	// Partitioned joins: stages after the first, whose tasks go out after
	// the notice.
	h.co.maxBroadcast = 1

	// The instance metadata service, IMDSv2: nothing scheduled until AWS
	// announces the interruption.
	var at atomic.Pointer[time.Time]
	imds := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/latest/api/token":
			_, _ = rw.Write([]byte("token"))
		case r.URL.Path == "/latest/meta-data/spot/instance-action" && r.Header.Get("X-aws-ec2-metadata-token") == "token":
			if when := at.Load(); when != nil {
				_ = json.NewEncoder(rw).Encode(map[string]string{"action": "terminate", "time": when.UTC().Format(time.RFC3339)})
				return
			}
			http.NotFound(rw, r)
		default:
			http.Error(rw, "unauthorized", http.StatusUnauthorized)
		}
	}))
	defer imds.Close()

	// Worker 1's watch, and what it does on a notice (see serveWorker):
	// shut down within the grace, and then the instance is gone.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var shutdownAt atomic.Pointer[time.Time]
	gone := make(chan struct{})
	watched := make(chan error, 1)
	go func() {
		n, err := spot.WatchEC2(ctx, imds.URL, 20*time.Millisecond)
		if err != nil {
			watched <- err
			close(gone)
			return
		}
		watched <- nil
		now := time.Now()
		shutdownAt.Store(&now)
		sctx, stop := context.WithTimeout(context.Background(), n.Grace(500*time.Millisecond, 0))
		_ = h.workers[1].Shutdown(sctx)
		stop()
		time.Sleep(time.Until(n.Time))
		close(gone)
	}()

	// Worker 1's first task is running when AWS announces the interruption,
	// and outlives the instance; tasks reaching worker 1 after its shutdown
	// would be taken where none may be.
	var first, late atomic.Int32
	h.workers[1].beforeTask = func(rw http.ResponseWriter, r *http.Request) bool {
		if s := shutdownAt.Load(); s != nil && time.Since(*s) > 250*time.Millisecond {
			late.Add(1)
		}
		if first.Add(1) != 1 {
			return true
		}
		when := time.Now().Add(2 * time.Second)
		at.Store(&when)
		<-gone
		// The machine is gone: the connection breaks.
		if conn, _, err := rw.(http.Hijacker).Hijack(); err == nil {
			conn.Close()
		}
		return false
	}
	var moved atomic.Int32
	hook := func(r slog.Record) {
		if strings.Contains(r.Message, "trying another") || strings.Contains(r.Message, "running the task elsewhere") {
			moved.Add(1)
		}
	}
	h.hook.Store(&hook)

	q := `SELECT o_orderpriority, count(*) AS n, sum(l_quantity) AS q
		FROM orders JOIN lineitem ON l_orderkey = o_orderkey GROUP BY 1 ORDER BY 1`
	start := time.Now()
	if !h.check(q) {
		t.Fatal("did not run distributed")
	}
	if err := <-watched; err != nil {
		t.Fatalf("the watch did not hear the notice: %v", err)
	}
	if first.Load() == 0 || moved.Load() == 0 || late.Load() != 0 {
		t.Errorf("worker 1 ran %d tasks, %d after its shutdown; %d tasks moved", first.Load(), late.Load(), moved.Load())
	}
	if took := time.Since(start); took > 30*time.Second {
		t.Errorf("the statement took %s", took)
	}
}
