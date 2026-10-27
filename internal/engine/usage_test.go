package engine

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/config"
)

// rowsOf runs query on conn and returns its rows as strings.
func rowsOf(t *testing.T, ctx context.Context, conn *duckdb.Conn, query string) []string {
	t.Helper()
	rows, err := conn.QueryContext(ctx, query, nil)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer rows.Close() //nolint:errcheck
	var out []string
	dest := make([]driver.Value, len(rows.Columns()))
	for {
		if err := rows.Next(dest); err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("%s: %v", query, err)
			}
			return out
		}
		var parts []string
		for _, v := range dest {
			parts = append(parts, toString(v))
		}
		out = append(out, strings.Join(parts, "|"))
	}
}

func toString(v any) string {
	switch v := v.(type) {
	case nil:
		return "NULL"
	case time.Time:
		return v.Format("2006-01-02")
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	}
	return fmt.Sprint(v)
}

func TestUsage(t *testing.T) {
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	e, err := Open(ctx, config.Config{}, log)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close() //nolint:errcheck
	conn, err := e.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck

	store := audit.NewMemoryStore(100, 100)
	l := audit.New(store, audit.Options{}, log)
	day := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	add := func(user, pool, tag string, run, queued time.Duration, cpu time.Duration) {
		l.RecordQuery(audit.Query{User: user, Role: "reader", Start: day, Duration: run + queued, Queued: queued,
			Outcome: audit.OutcomeOK, Text: "SELECT 1", Pool: pool, QueryTag: tag,
			Resources: &audit.Resources{CPUTime: cpu, BytesRead: 100, MemoryAllocated: 1 << 20}})
	}
	add("ana", "acme", "client:acme", 3*time.Second, time.Second, 6*time.Second)
	add("ana", "acme", "client:acme", time.Second, 0, 2*time.Second)
	add("bob", "acme", "", 4*time.Second, 0, time.Second)
	add("bob", "default", "", time.Second, 0, time.Second)
	// acme ran two hours that day, on machines at 1.50 an hour; the
	// default pool's price is unknown.
	if err := store.WriteCompute(ctx, []audit.Compute{
		{Day: day.Truncate(24 * time.Hour), Pool: "acme", Replica: "a", Seconds: 3600, PricePerHour: 1.5},
		{Day: day.Truncate(24 * time.Hour), Pool: "acme", Replica: "b", Seconds: 3600, PricePerHour: 1.5},
		{Day: day.Truncate(24 * time.Hour), Pool: "default", Replica: "c", Seconds: 7200},
	}); err != nil {
		t.Fatal(err)
	}
	e.SetHistory(l, testViewer)

	admin := context.WithValue(ctx, viewerKey{}, audit.Viewer{User: "root", All: true})
	got := rowsOf(t, admin, conn, `SELECT day, pool, user_name, query_tag, statements, run_ms, queued_ms, cpu_time_ms,
		bytes_read, round(time_share, 3), compute_hours, round(estimated_cost, 3) FROM memory.igloo.usage ORDER BY ALL`)
	want := []string{
		"2026-10-14|acme|ana|client:acme|2|4000|1000|8000|200|0.5|2|1.5",
		"2026-10-14|acme|bob|NULL|1|4000|0|1000|100|0.5|2|1.5",
		"2026-10-14|default|bob|NULL|1|1000|0|1000|100|1|2|NULL",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("usage:\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	// Others see their own groups, with shares of the whole pool.
	ana := context.WithValue(ctx, viewerKey{}, audit.Viewer{User: "ana"})
	if got := rowsOf(t, ana, conn, "SELECT user_name, round(time_share, 3) FROM memory.igloo.usage"); strings.Join(got, ",") != "ana|0.5" {
		t.Errorf("ana sees %v", got)
	}
	if got := rowsOf(t, ctx, conn, "SELECT count(*) FROM memory.igloo.usage"); got[0] != "0" {
		t.Errorf("no viewer sees %v", got)
	}
	// The history has the same attribution, statement by statement.
	got = rowsOf(t, admin, conn, `SELECT pool, query_tag, queued_ms, cpu_time_ms, bytes_read, memory_allocated_bytes
		FROM memory.igloo.query_history WHERE user_name = 'ana' ORDER BY queued_ms`)
	if strings.Join(got, ",") != "acme|client:acme|0|2000|100|1048576,acme|client:acme|1000|6000|100|1048576" {
		t.Errorf("history: %v", got)
	}
}

func TestMeter(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close() //nolint:errcheck
	conn, meter, err := e.ConnectMetered(ctx)
	if err != nil {
		t.Fatal(err)
	}
	query := "SELECT count(DISTINCT i % 1000) FROM range(2000000) t(i)"
	rowsOf(t, ctx, conn, query)
	used := meter.Last(query + ";\n")
	if used == nil || used.CPUTime <= 0 || used.MemoryAllocated <= 0 {
		t.Errorf("measured %+v", used)
	}
	if meter.Last("SELECT 2") != nil {
		t.Error("another statement's measurements were returned")
	}
	if _, err := conn.ExecContext(ctx, "SELECT * FROM no_such_table", nil); err == nil {
		t.Fatal("expected an error")
	}
	if meter.Last("SELECT * FROM no_such_table") != nil {
		t.Error("a failed statement was measured")
	}
	// What DuckDB holds for running statements, over a connection of its
	// own (see workload.Controller.SampleMemory).
	read, closeRead, err := e.QueryMemory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if held, err := read(ctx); err != nil || held < 0 {
		t.Errorf("query memory %d, %v", held, err)
	}
	if err := closeRead(); err != nil {
		t.Fatal(err)
	}
	if _, err := read(ctx); err == nil {
		t.Error("read query memory after closing its connection")
	}
	limit := e.MemoryLimit(ctx)
	if limit <= 0 {
		t.Error("no memory limit")
	}
	// Changed after lockdown, as memlimit.Adapt does, and back.
	if err := e.SetMemoryLimit(ctx, 3<<30); err != nil {
		t.Fatal(err)
	}
	if got := e.MemoryLimit(ctx); got != 3<<30 {
		t.Errorf("memory limit %d after setting 3 GiB", got)
	}
	if err := e.SetMemoryLimit(ctx, limit); err != nil {
		t.Fatal(err)
	}
	if err := meter.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "SELECT 1", nil); err == nil {
		t.Error("closing the meter left the connection open")
	}
}

// TestMeterExplainAnalyze runs EXPLAIN ANALYZE as a session does: prepared,
// then a query of Igloo's checks (measured with Igloo's metrics, as every
// statement on the connection is), then run with DuckDB's own metrics. The
// query is large enough for DuckDB's worker threads to run parts of it.
// DuckDB 1.5.6 segfaulted here when the metrics changed while the profiler
// covered the statement changing them, and as the container's first process
// Igloo then hung instead of exiting.
func TestMeterExplainAnalyze(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close() //nolint:errcheck
	conn, meter, err := e.ConnectMetered(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer meter.Close() //nolint:errcheck
	const query = "SELECT count(*) FROM range(10000000) t(i) WHERE i % 7 = 0"
	for range 5 {
		st, err := conn.PrepareContext(ctx, "EXPLAIN ANALYZE "+query)
		if err != nil {
			t.Fatal(err)
		}
		rowsOf(t, ctx, conn, "SELECT count(*) FROM duckdb_functions()")
		var plan string
		err = meter.ExplainAnalyze(ctx, func() error {
			rows, err := st.(*duckdb.Stmt).QueryContext(ctx, nil)
			if err != nil {
				return err
			}
			defer rows.Close() //nolint:errcheck
			dest := make([]driver.Value, 2)
			if err := rows.Next(dest); err != nil {
				return err
			}
			plan, _ = dest[1].(string)
			return nil
		})
		if err := errors.Join(err, st.Close()); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(plan, query) || !strings.Contains(plan, "FILTER") {
			t.Fatalf("the plan is not the statement's:\n%s", plan)
		}
		// Igloo's settings are back: statements are measured again, with
		// Igloo's metrics only.
		if got := rowsOf(t, ctx, conn, "SELECT current_setting('profiling_coverage')"); got[0] != "ALL" {
			t.Errorf("profiling coverage %s", got[0])
		}
		rowsOf(t, ctx, conn, query)
		if used := meter.Last(query); used == nil || used.CPUTime <= 0 {
			t.Errorf("measured %+v", used)
		}
		info, err := duckdb.GetProfilingInfo(meter.conn)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := info.Metrics["LATENCY"]; ok || info.Metrics["QUERY_NAME"] != query {
			t.Errorf("the profiler measured %v", info.Metrics)
		}
	}
}
