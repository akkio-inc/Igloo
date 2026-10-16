package audit

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/akkio-inc/igloo/internal/testdb"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestNewID(t *testing.T) {
	now := time.Now()
	a, b := NewID(now), NewID(now.Add(time.Second))
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(a) {
		t.Errorf("not a version 7 UUID: %s", a)
	}
	if a == b || a > b {
		t.Errorf("IDs do not sort by time: %s %s", a, b)
	}
}

// testStore checks a Store that starts empty.
func testStore(t *testing.T, s Store) {
	t.Helper()
	ctx := context.Background()
	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	var queries []Query
	for i := range 25 {
		user := "ana"
		if i%5 == 0 {
			user = "bob"
		}
		start := base.Add(time.Duration(i) * time.Second)
		queries = append(queries, Query{ID: NewID(start), SessionID: "s1", User: user, Role: "none", Start: start,
			Duration: 1500 * time.Microsecond, Outcome: OutcomeOK, Rows: int64(i), StatementType: "SELECT",
			Text: "SELECT 1", TablesRead: []string{"lake.public.t"}, ClientAddress: "10.0.0.1:5000"})
	}
	queries[3].Outcome, queries[3].SQLState, queries[3].Rows, queries[3].TablesRead = OutcomeDenied, "42501", -1, nil
	if err := s.WriteQueries(ctx, queries); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteLogins(ctx, []Login{
		{ID: NewID(base), Time: base, User: "ana", Result: LoginSuccess, ClientAddress: "10.0.0.1:5000", SessionID: "s1"},
		{ID: NewID(base.Add(time.Second)), Time: base.Add(time.Second), User: "bob", Result: LoginFailure, Reason: "wrong password"},
	}); err != nil {
		t.Fatal(err)
	}

	// Pages continue where the last ended, newest first.
	var seen []Query
	p := Page{Limit: 7}
	for {
		page, err := s.Queries(ctx, Viewer{User: "ana"}, p)
		if err != nil {
			t.Fatal(err)
		}
		seen = append(seen, page...)
		if len(page) < p.Limit {
			break
		}
		last := page[len(page)-1]
		p.AfterTime, p.AfterID = last.Start, last.ID
	}
	if len(seen) != 20 {
		t.Fatalf("ana sees %d queries, want 20", len(seen))
	}
	for i, q := range seen {
		if q.User != "ana" || i > 0 && !q.Start.Before(seen[i-1].Start) {
			t.Fatalf("page order or filter wrong at %d: %+v", i, q)
		}
	}
	denied := seen[slices.IndexFunc(seen, func(q Query) bool { return q.Outcome == OutcomeDenied })]
	if denied.SQLState != "42501" || denied.Rows != -1 || denied.TablesRead != nil {
		t.Errorf("denied query: %+v", denied)
	}
	if q := seen[0]; q.Duration != 1500*time.Microsecond || q.Rows != 24 || !slices.Equal(q.TablesRead, []string{"lake.public.t"}) ||
		q.ClientAddress != "10.0.0.1:5000" {
		t.Errorf("newest query: %+v", q)
	}

	all, err := s.Queries(ctx, Viewer{All: true}, Page{})
	if err != nil || len(all) != 25 {
		t.Errorf("an admin sees %d queries, %v", len(all), err)
	}
	since, err := s.Queries(ctx, Viewer{All: true}, Page{Since: base.Add(20 * time.Second)})
	if err != nil || len(since) != 5 {
		t.Errorf("since: %d %v", len(since), err)
	}

	logins, err := s.Logins(ctx, Viewer{User: "bob"}, Page{})
	if err != nil || len(logins) != 1 || logins[0].Reason != "wrong password" || logins[0].Result != LoginFailure {
		t.Errorf("bob's logins: %+v %v", logins, err)
	}

	if err := s.Prune(ctx, base.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	all, _ = s.Queries(ctx, Viewer{All: true}, Page{})
	if len(all) != 15 {
		t.Errorf("after pruning: %d queries", len(all))
	}
	if logins, _ := s.Logins(ctx, Viewer{All: true}, Page{}); len(logins) != 0 {
		t.Errorf("after pruning: %d logins", len(logins))
	}
}

func TestMemoryStore(t *testing.T) {
	testStore(t, NewMemoryStore(100, 100))

	s := NewMemoryStore(3, 3)
	for i := range 5 {
		_ = s.WriteQueries(context.Background(), []Query{{ID: NewID(time.Now()), User: "ana", Start: time.Now(), Rows: int64(i)}})
	}
	if qs, _ := s.Queries(context.Background(), Viewer{All: true}, Page{}); len(qs) != 3 || qs[0].Rows != 4 {
		t.Errorf("a full store keeps the newest: %+v", qs)
	}
}

// failingStore fails every write.
type failingStore struct{ *MemoryStore }

func (failingStore) WriteQueries(context.Context, []Query) error { return errors.New("database down") }

func TestLogDropsWhenTheStoreFails(t *testing.T) {
	reg := prometheus.NewRegistry()
	l := New(failingStore{NewMemoryStore(10, 10)}, Options{Metrics: NewMetrics(reg)}, discard)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		l.Run(ctx)
	}()
	l.RecordQuery(Query{User: "ana", Start: time.Now()})
	l.RecordLogin(Login{User: "ana", Time: time.Now()})
	cancel()
	<-done

	if n := testutil.ToFloat64(l.metrics.droppedTotal.WithLabelValues("query")); n != 1 {
		t.Errorf("dropped queries: %v", n)
	}
	if n := testutil.ToFloat64(l.metrics.writtenTotal.WithLabelValues("login")); n != 1 {
		t.Errorf("written logins: %v", n)
	}
}

func TestLogNeverBlocks(t *testing.T) {
	reg := prometheus.NewRegistry()
	// Not running: the queue fills and the rest is dropped.
	l := New(failingStore{NewMemoryStore(10, 10)}, Options{Metrics: NewMetrics(reg)}, discard)
	start := time.Now()
	for range queueSize + 10 {
		l.RecordQuery(Query{User: "ana", Start: time.Now()})
	}
	if time.Since(start) > 5*time.Second {
		t.Error("recording blocked")
	}
	if n := testutil.ToFloat64(l.metrics.droppedTotal.WithLabelValues("query")); n != 10 {
		t.Errorf("dropped: %v", n)
	}
	var nilLog *Log
	nilLog.RecordQuery(Query{})
	if qs, err := nilLog.Queries(context.Background(), Viewer{}, Page{}); qs != nil || err != nil {
		t.Error("a nil log has no history")
	}
}

func TestMemoryLogIsImmediate(t *testing.T) {
	s := NewMemoryStore(10, 10)
	l := New(s, Options{}, discard)
	l.RecordQuery(Query{User: "ana", Start: time.Now(), Text: "SELECT 1"})
	if qs, _ := l.Queries(context.Background(), Viewer{User: "ana"}, Page{}); len(qs) != 1 || qs[0].ID == "" {
		t.Errorf("memory history: %+v", qs)
	}
}

// TestPostgresStore runs against a real database when
// IGLOO_TEST_DATABASE_URL is set.
func TestPostgresStore(t *testing.T) {
	url := testdb.URL()
	if url == "" {
		t.Skip("IGLOO_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := OpenPostgres(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.pool.Exec(ctx, "TRUNCATE igloo_audit.query_history, igloo_audit.login_history"); err != nil {
		t.Fatal(err)
	}
	// Migrations are idempotent, as several replicas run them.
	if s2, err := OpenPostgres(ctx, url); err != nil {
		t.Fatal(err)
	} else {
		s2.Close()
	}
	testStore(t, s)

	// Only one replica prunes at a time; the others skip.
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", pruneKey); err != nil {
			return err
		}
		return s.Prune(ctx, time.Now())
	})
	if err != nil {
		t.Fatal(err)
	}
	if qs, _ := s.Queries(ctx, Viewer{All: true}, Page{}); len(qs) != 15 {
		t.Errorf("a prune ran while another replica held the lock: %d left", len(qs))
	}

	// A batch written again, as after a timeout that came once it had
	// committed, is not an error and adds nothing twice.
	again := []Query{{ID: "retried-1", User: "ana", Start: time.Now(), Outcome: "ok", StatementType: "SELECT", Text: "SELECT 1"},
		{ID: "retried-2", User: "ana", Start: time.Now(), Outcome: "ok", StatementType: "SELECT", Text: "SELECT 2"}}
	if err := s.WriteQueries(ctx, again); err != nil {
		t.Fatalf("write 1: %v", err)
	}
	// Written again with a statement more, through the staging table,
	// which must carry every column.
	full := Query{ID: "retried-3", User: "ana", Start: time.Now().Truncate(time.Millisecond), Outcome: "ok", StatementType: "COPY",
		Text: "COPY t FROM 's3://landing/a.csv'", TablesWritten: []string{"lake.public.t"}, LocationsRead: []string{"s3://landing/a.csv"},
		Job: "pipe drops", QueryTag: "etl", Pool: "default", Queued: time.Second,
		Resources: &Resources{CPUTime: 2 * time.Millisecond, BytesRead: 10, MemoryAllocated: 20}}
	if err := s.WriteQueries(ctx, append(again, full)); err != nil {
		t.Fatalf("write 2: %v", err)
	}
	var n int
	if err := s.pool.QueryRow(ctx, "SELECT count(*) FROM igloo_audit.query_history WHERE query_id LIKE 'retried-%'").Scan(&n); err != nil || n != 3 {
		t.Fatalf("retried batch: %d rows, %v; want 3", n, err)
	}
	qs, err := s.Queries(ctx, Viewer{All: true}, Page{Limit: 100})
	i := slices.IndexFunc(qs, func(q Query) bool { return q.ID == full.ID })
	if err != nil || i < 0 {
		t.Fatalf("read back: %v %v", qs, err)
	}
	if got := qs[i]; !slices.Equal(got.LocationsRead, full.LocationsRead) || got.Job != full.Job ||
		!slices.Equal(got.TablesWritten, full.TablesWritten) || got.QueryTag != "etl" || got.Pool != "default" ||
		got.Queued != time.Second || got.Resources == nil || *got.Resources != *full.Resources {
		t.Errorf("a statement written through the staging table read back as %+v", got)
	}
	if usage, err := s.TableUsage(ctx, time.Now().Add(-time.Hour)); err != nil ||
		!slices.ContainsFunc(usage, func(u TableUsage) bool { return u.Table == "lake.public.t" && u.Writes == 1 }) {
		t.Errorf("usage after a retried write: %+v %v", usage, err)
	}
	logins := []Login{{ID: "retried-login", User: "ana", Time: time.Now(), Result: "success"}}
	for i := range 2 {
		if err := s.WriteLogins(ctx, logins); err != nil {
			t.Fatalf("login write %d: %v", i+1, err)
		}
	}
}

// flakyStore fails its first fails writes.
type flakyStore struct {
	*MemoryStore
	fails int
}

func (f *flakyStore) WriteQueries(ctx context.Context, qs []Query) error {
	if f.fails > 0 {
		f.fails--
		return errors.New("timeout: context deadline exceeded")
	}
	return f.MemoryStore.WriteQueries(ctx, qs)
}

func TestFailedWritesAreRetried(t *testing.T) {
	store := &flakyStore{MemoryStore: NewMemoryStore(100, 100), fails: writeAttempts - 1}
	l := New(store, Options{}, slog.New(slog.DiscardHandler))
	l.direct = false // the flaky wrapper hides the memory store

	now := time.Now()
	l.write(context.Background(), []any{Query{ID: "q1", User: "ana", Start: now}})
	for i := 0; len(l.retries) > 0 && i < 2*writeAttempts; i++ {
		// Not due yet: nothing is written.
		l.retryDue(context.Background(), now, false)
		now = now.Add(time.Hour)
		l.retryDue(context.Background(), now, false)
	}
	if len(l.retries) != 0 || l.dropped != 0 {
		t.Fatalf("retries %d, dropped %d", len(l.retries), l.dropped)
	}
	got, err := l.Queries(context.Background(), Viewer{All: true}, Page{})
	if err != nil || len(got) != 1 {
		t.Fatalf("history %v, %v; want the one query", got, err)
	}
}

func TestWritesThatKeepFailingAreDropped(t *testing.T) {
	store := &flakyStore{MemoryStore: NewMemoryStore(100, 100), fails: 100}
	l := New(store, Options{}, slog.New(slog.DiscardHandler))
	l.direct = false

	now := time.Now()
	l.write(context.Background(), []any{Query{ID: "q1", User: "ana", Start: now}})
	for i := 0; i < 2*writeAttempts; i++ {
		now = now.Add(time.Hour)
		l.retryDue(context.Background(), now, false)
	}
	if attempts := 100 - store.fails; len(l.retries) != 0 || attempts != writeAttempts {
		t.Fatalf("retries %d after %d attempts; want the query dropped after %d", len(l.retries), attempts, writeAttempts)
	}
}
