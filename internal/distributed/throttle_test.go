package distributed

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// slowDown is how DuckDB words a request S3 turned away with 503 SlowDown,
// past its retries.
const slowDown = "HTTP Error: Request returned HTTP 503 for HTTP HEAD to " +
	"'https://bucket.s3.us-east-1.amazonaws.com/warehouse/_igloo_shuffle/q/s1/0/a0/__b%3D1/data_0.parquet'"

// A task that object storage turned away runs again in a while, on the
// next worker, and the statement succeeds; the coordinator counts each
// such run. A task turned away every time fails the statement, after
// maxThrottled runs again, rather than run it forever.
func TestThrottledTasks(t *testing.T) {
	h := newHarness(t, 0.01, 3, 2)
	h.co.maxBroadcast = 1
	h.co.opts.Metrics = NewMetrics(prometheus.NewRegistry())
	defer func(d time.Duration) { throttleWait = d }(throttleWait)
	throttleWait = time.Millisecond
	var failures atomic.Int32
	failures.Store(3)
	taskFault = func(*taskRequest) error {
		if failures.Add(-1) >= 0 {
			return &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: slowDown}
		}
		return nil
	}
	defer func() { taskFault = nil }()
	q := `SELECT o_orderpriority, count(*) FROM orders JOIN lineitem ON l_orderkey = o_orderkey GROUP BY 1 ORDER BY 1`
	if !h.check(q) {
		t.Fatal("did not run distributed")
	}
	if n := testutil.ToFloat64(h.co.opts.Metrics.tasks.WithLabelValues("throttled")); n != 3 {
		t.Errorf("%v tasks run again after object storage turned them away, want 3", n)
	}

	taskFault = func(*taskRequest) error { return &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: slowDown} }
	_, _, _, _, err := h.run(context.Background(), q, ModeOn)
	var th *throttledError
	if !errors.As(err, &th) || IsUnsupported(err) {
		t.Errorf("turned away every time: %v", err)
	}

	// Errors of the task's own, or of storage that will not change its
	// mind, are not throttles.
	for _, msg := range []string{"HTTP Error: Request returned HTTP 404 for HTTP HEAD to 'x'", "Conversion Error: Could not convert"} {
		if throttled(errors.New(msg)) {
			t.Errorf("%q taken for a throttle", msg)
		}
	}
	if !throttled(errors.New("operation error S3: PutObject, api error SlowDown: Please reduce your request rate.")) {
		t.Error("S3's SlowDown not taken for a throttle")
	}
}

// DuckDB, with a task's retries of object storage's requests, reads a
// file through five 503s in a row, where its defaults give up after three
// more tries.
func TestStorageRetries(t *testing.T) {
	ext := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if ext == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	local := filepath.Join(t.TempDir(), "data.parquet")
	if _, err := db.Exec("COPY (SELECT range AS i FROM range(1000)) TO '" + local + "' (FORMAT parquet)"); err != nil {
		t.Fatal(err)
	}
	// The first five requests under each prefix are turned away, as S3
	// turns away a prefix that gets more than it serves at once.
	var mu sync.Mutex
	turnedAway := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prefix, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/bucket/"), "/")
		mu.Lock()
		n := turnedAway[prefix]
		if n < 5 {
			turnedAway[prefix] = n + 1
		}
		mu.Unlock()
		if n < 5 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>`))
			return
		}
		http.ServeFile(w, r, local)
	}))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")
	read := func(prefix string, setup []string) error {
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		for _, q := range append([]string{
			"SET extension_directory = '" + ext + "'", "LOAD httpfs",
			"SET s3_endpoint = '" + host + "'", "SET s3_use_ssl = false", "SET s3_url_style = 'path'", "SET s3_region = 'us-east-1'",
			"SET s3_access_key_id = 'x'", "SET s3_secret_access_key = 'y'",
		}, setup...) {
			if _, err := conn.ExecContext(context.Background(), q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		var n int64
		return conn.QueryRowContext(context.Background(), "SELECT count(*) FROM read_parquet('s3://bucket/"+prefix+"/data.parquet')").Scan(&n)
	}
	if err := read("defaults", nil); err == nil || !throttled(err) {
		t.Errorf("with DuckDB's own retries: %v, want it turned away", err)
	}
	if err := read("task", storageRetries); err != nil {
		t.Errorf("with a task's retries: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if turnedAway["task"] != 5 {
		t.Errorf("turned away %d requests of the task's read, want 5", turnedAway["task"])
	}
}
