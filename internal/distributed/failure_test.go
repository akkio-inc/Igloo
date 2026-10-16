package distributed

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

const failureQuery = `SELECT l_returnflag, l_linestatus, sum(l_quantity), count(*), avg(l_discount)
	FROM lineitem GROUP BY ALL ORDER BY ALL`

// A worker that dies in the middle of a task: its share runs on another
// worker and the result is complete.
func TestWorkerDiesMidTask(t *testing.T) {
	h := newHarness(t, 0.02, 6, 3)
	var killed atomic.Int32
	h.workers[1].beforeTask = func(rw http.ResponseWriter, r *http.Request) bool {
		killed.Add(1)
		// Drop the connection as a crashed process would.
		conn, _, err := rw.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
		return false
	}
	if !h.check(failureQuery) {
		t.Fatal("did not run distributed")
	}
	if killed.Load() == 0 {
		t.Fatal("the failing worker got no task")
	}
}

// A worker whose address no longer answers is skipped.
func TestWorkerGone(t *testing.T) {
	h := newHarness(t, 0.02, 6, 3)
	// Refresh the pool's view while the worker is up, then stop it.
	h.co.pool.refresh(t.Context())
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := h.workers[2].Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	h.lns[2].Close()
	if !h.check(failureQuery) {
		t.Fatal("did not run distributed")
	}
}

// With every worker failing, the statement fails with an error that sends
// it to the coordinator, not a data error.
func TestAllWorkersFail(t *testing.T) {
	h := newHarness(t, 0.01, 3, 2)
	for _, w := range h.workers {
		w.beforeTask = func(rw http.ResponseWriter, r *http.Request) bool {
			http.Error(rw, "boom", http.StatusInternalServerError)
			return false
		}
	}
	_, _, _, _, err := h.run(t.Context(), failureQuery, ModeOn)
	if err == nil || IsUnsupported(err) || dataError(err) {
		t.Fatalf("want a worker failure, got %v", err)
	}
}

// Canceling a statement stops its tasks on the workers.
func TestCancel(t *testing.T) {
	h := newHarness(t, 0.01, 3, 2)
	stopped := make(chan struct{}, 2)
	for _, w := range h.workers {
		w.beforeTask = func(rw http.ResponseWriter, r *http.Request) bool {
			// The server notices a client going away once it has read the
			// request.
			_, _ = io.Copy(io.Discard, r.Body)
			<-r.Context().Done()
			stopped <- struct{}{}
			return false
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, _, _, err := h.run(ctx, failureQuery, ModeOn)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want the deadline, got %v", err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatalf("took %s to stop", time.Since(start))
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the worker's task was not canceled")
	}
}

// An error in the data, such as a failed cast, is the statement's error,
// as it would be on one machine.
func TestDataError(t *testing.T) {
	h := newHarness(t, 0.01, 3, 2)
	_, _, _, _, err := h.run(t.Context(), `SELECT sum(CAST(l_comment AS INTEGER)) FROM lineitem`, ModeOn)
	var de *duckdb.Error
	if !errors.As(err, &de) || de.Type != duckdb.ErrorTypeConversion || !dataError(err) {
		t.Fatalf("want a conversion error, got %v", err)
	}
	if !strings.Contains(de.Msg, "INT32") && !strings.Contains(strings.ToLower(de.Msg), "convert") {
		t.Logf("message: %s", de.Msg)
	}
}

// Another secret cannot talk to the workers, and a task's files must lie
// in the warehouse.
func TestWorkerRefusesStrangers(t *testing.T) {
	h := newHarness(t, 0.01, 2, 1)
	other, err := newTransport(strings.Repeat("x", MinSecretLength))
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: other.client}}
	if _, err := client.Get("https://" + h.lns[0].Addr().String() + statusPath); err == nil {
		t.Fatal("a client with another secret was accepted")
	}
	if resp, err := http.Get("http://" + h.lns[0].Addr().String() + statusPath); err == nil {
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatal("a plain HTTP client was accepted")
		}
	}

	w := h.workers[0]
	for _, path := range []string{"/etc/passwd", h.dir + "/warehouse/../secret.parquet", "s3://elsewhere/x.parquet"} {
		_, err := w.viewStatement(viewDef{Name: "__igloo_t0", Columns: []Column{{Name: "a", Type: "INTEGER", ID: 1}}, Files: []string{path}})
		if err == nil {
			t.Errorf("view over %s was accepted", path)
		}
	}
	if _, err := w.viewStatement(viewDef{Name: "x; DROP TABLE y", Columns: []Column{{Name: "a", Type: "INTEGER", ID: 1}}}); err == nil {
		t.Error("a view with a bad name was accepted")
	}
}
