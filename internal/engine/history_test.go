package engine

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/config"
)

type viewerKey struct{}

// testViewer reads the viewer a test puts in the statement's context.
func testViewer(ctx context.Context) func() (audit.Viewer, bool) {
	v, ok := ctx.Value(viewerKey{}).(audit.Viewer)
	if !ok {
		return nil
	}
	return func() (audit.Viewer, bool) { return v, true }
}

func TestHistory(t *testing.T) {
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

	count := func(ctx context.Context, query string) int64 {
		t.Helper()
		rows, err := conn.QueryContext(ctx, query, nil)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		defer rows.Close() //nolint:errcheck
		dest := make([]driver.Value, 1)
		if err := rows.Next(dest); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return dest[0].(int64)
	}

	// Empty until a history is set.
	if n := count(ctx, "SELECT count(*) FROM memory.igloo.query_history"); n != 0 {
		t.Errorf("history before SetHistory: %d rows", n)
	}

	store := audit.NewMemoryStore(5000, 100)
	l := audit.New(store, audit.Options{}, log)
	now := time.Now()
	for i := range 2500 {
		user := "ana"
		if i%2 == 0 {
			user = "bob"
		}
		l.RecordQuery(audit.Query{User: user, Role: "reader", Start: now.Add(time.Duration(i) * time.Millisecond),
			Duration: time.Millisecond, Outcome: audit.OutcomeOK, Rows: 1, Text: "SELECT 1", TablesRead: []string{"lake.public.t"}})
	}
	l.RecordLogin(audit.Login{User: "ana", Time: now, Result: audit.LoginFailure, Reason: "wrong password"})
	e.SetHistory(l, testViewer)

	ana := context.WithValue(ctx, viewerKey{}, audit.Viewer{User: "ana"})
	admin := context.WithValue(ctx, viewerKey{}, audit.Viewer{User: "root", All: true})
	if n := count(ana, "SELECT count(*) FROM memory.igloo.query_history"); n != 1250 {
		t.Errorf("ana sees %d queries, want 1250", n)
	}
	if n := count(ana, "SELECT count(*) FROM igloo_query_history() WHERE user_name <> 'ana'"); n != 0 {
		t.Errorf("ana sees %d queries of others", n)
	}
	if n := count(admin, "SELECT count(*) FROM memory.igloo.query_history"); n != 2500 {
		t.Errorf("an admin sees %d queries, want 2500", n)
	}
	if n := count(admin, "SELECT count(*) FROM memory.igloo.query_history WHERE duration_ms = 1 AND tables_read = ['lake.public.t'] AND end_time > start_time"); n != 2500 {
		t.Errorf("columns: %d rows match", n)
	}
	// Statements from outside a session see nothing.
	if n := count(ctx, "SELECT count(*) FROM memory.igloo.query_history"); n != 0 {
		t.Errorf("no viewer sees %d queries", n)
	}
	if n := count(ana, "SELECT count(*) FROM memory.igloo.login_history WHERE reason = 'wrong password'"); n != 1 {
		t.Errorf("ana's failed logins: %d", n)
	}
	if n := count(admin, "SELECT count(*) FROM (SELECT * FROM memory.igloo.query_history LIMIT 10)"); n != 10 {
		t.Errorf("limit: %d", n)
	}
}

// failingHistory fails every read.
type failingHistory struct{}

func (failingHistory) Queries(context.Context, audit.Viewer, audit.Page) ([]audit.Query, error) {
	return nil, errors.New("database down")
}

func (failingHistory) Logins(context.Context, audit.Viewer, audit.Page) ([]audit.Login, error) {
	return nil, errors.New("database down")
}

func (failingHistory) Usage(context.Context, audit.Viewer, time.Time, time.Time) ([]audit.Usage, error) {
	return nil, errors.New("database down")
}

func TestHistoryErrors(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close() //nolint:errcheck
	conn, err := e.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	e.SetHistory(failingHistory{}, testViewer)
	ctx = context.WithValue(ctx, viewerKey{}, audit.Viewer{User: "ana"})
	if _, err := conn.ExecContext(ctx, "SELECT * FROM memory.igloo.query_history", nil); err == nil {
		t.Error("a failing history read succeeded")
	}
}
