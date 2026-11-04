package pgwire

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/workload"
)

// TestOutOfMemoryCapture checks that a statement that runs out of DuckDB's
// memory has Igloo log what held it: DuckDB's limit and memory by tag, what
// the workload controller reserved, and the statements running, the one
// that failed among them; once for runs out of memory close together.
func TestOutOfMemoryCapture(t *testing.T) {
	t.Parallel()
	eng, err := engine.Open(context.Background(), config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	logs := &lockedBuffer{}
	ts := startServerLogged(t, eng, slog.New(slog.NewTextHandler(logs, nil)), func(o *Options) { o.Workload = workload.NewController(1 << 30) })
	admin := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE TABLE held AS SELECT range AS i, md5(range::VARCHAR) AS s FROM range(1000000)")
	var fails atomic.Int64
	ts.srv.failLocal = func(_ context.Context, st *statement) error {
		if strings.Contains(st.query, "oom_capture_marker") && fails.Add(1) <= 2 {
			return &duckdb.Error{Type: duckdb.ErrorTypeOutOfMemory, Msg: "Out of Memory Error: failed to allocate data of size 900.0 MiB (1.0 GiB/1.0 GiB used)"}
		}
		return nil
	}
	if n := count(t, admin, "SELECT count(*) AS oom_capture_marker FROM held"); n != 1_000_000 {
		t.Fatalf("got %d", n)
	}
	if fails.Load() < 2 {
		t.Fatalf("it ran out of memory %d times, want 2", fails.Load())
	}
	out := logs.String()
	if got := strings.Count(out, "what held it"); got != 1 {
		t.Fatalf("%d captures, want 1:\n%s", got, out)
	}
	line := out[strings.Index(out, "what held it"):]
	line = line[:strings.IndexByte(line, '\n')]
	for _, want := range []string{"limit=", "used=", "by_tag=", "budget=", "reserved=", "held_at_last_sample=", "running=", "oom_capture_marker"} {
		if !strings.Contains(line, want) {
			t.Errorf("the capture has no %s: %s", want, line)
		}
	}
	if strings.Contains(line, "by_tag=\"\"") || strings.Contains(line, "memory_err=") {
		t.Errorf("the capture has no memory by tag: %s", line)
	}
}
