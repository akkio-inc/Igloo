package audit

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/testdb"
)

// testLineage checks a Store's Lineage and TableUsage, from an empty store.
func testLineage(t *testing.T, s Store) {
	t.Helper()
	ctx := context.Background()
	t0 := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	q := func(id, user string, at time.Duration, outcome, kind string, read, locations, written []string, job string) Query {
		start := t0.Add(at)
		return Query{ID: NewID(start) + id, SessionID: "s", User: user, Role: "writer", Start: start, Outcome: outcome,
			StatementType: kind, Text: kind, TablesRead: read, LocationsRead: locations, TablesWritten: written, Job: job}
	}
	orders, report := []string{"lake.public.orders"}, []string{"lake.public.report"}
	// What Delta Sharing recipients read is not usage.
	recipient := q("8", "recipient:acme", 6*time.Minute, OutcomeOK, "DELTA SHARING", orders, nil, nil, "")
	recipient.Role = RecipientRole
	if err := s.WriteQueries(ctx, []Query{
		recipient,
		q("1", "ana", 0, OutcomeOK, "INSERT", orders, nil, report, ""),
		q("2", "bob", time.Minute, OutcomeOK, "INSERT", []string{"lake.public.orders", "lake.public.report"}, nil, report, "task nightly"),
		q("3", "ana", 2*time.Minute, OutcomeError, "INSERT", orders, nil, report, ""),
		q("4", "bob", 3*time.Minute, OutcomeOK, "COPY", nil, []string{"s3://landing/sales/"}, []string{"lake.public.Report"}, "pipe drops"),
		q("5", "cy", 4*time.Minute, OutcomeOK, "SELECT", orders, nil, nil, ""),
		q("6", "ana", -48*time.Hour, OutcomeOK, "INSERT", []string{"lake.public.old"}, nil, report, ""),
		// A comment changes no data.
		q("7", "ana", 5*time.Minute, OutcomeOK, "COMMENT", nil, nil, report, ""),
	}); err != nil {
		t.Fatal(err)
	}

	edges, err := s.Lineage(ctx, t0.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(edges) != 2 {
		t.Fatalf("edges: %+v", edges)
	}
	if e := edges[0]; e.From != "lake.public.orders" || e.FromLocation || e.To != "lake.public.report" || e.Statements != 2 ||
		!e.FirstSeen.Equal(t0) || !e.LastSeen.Equal(t0.Add(time.Minute)) || e.LastUser != "bob" || e.LastJob != "task nightly" ||
		e.LastStatementType != "INSERT" || e.LastQueryID == "" {
		t.Errorf("table edge: %+v", e)
	}
	if e := edges[1]; e.From != "s3://landing/sales/" || !e.FromLocation || e.To != "lake.public.Report" || e.Statements != 1 ||
		e.LastJob != "pipe drops" {
		t.Errorf("location edge: %+v", e)
	}
	if all, _ := s.Lineage(ctx, time.Time{}); len(all) != 3 {
		t.Errorf("all the history's edges: %+v", all)
	}

	usage, err := s.TableUsage(ctx, t0.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(usage) != 2 {
		t.Fatalf("usage: %+v", usage)
	}
	if u := usage[0]; u.Table != "lake.public.orders" || u.Reads != 3 || u.Writes != 0 || u.Users != 3 ||
		!u.LastRead.Equal(t0.Add(4*time.Minute)) || !u.LastWrite.IsZero() {
		t.Errorf("orders' usage: %+v", u)
	}
	if u := usage[1]; !strings.EqualFold(u.Table, "lake.public.report") || u.Reads != 1 || u.Writes != 3 || u.Users != 2 ||
		!u.LastWrite.Equal(t0.Add(3*time.Minute)) {
		t.Errorf("report's usage: %+v", u)
	}
}

func TestMemoryLineage(t *testing.T) {
	testLineage(t, NewMemoryStore(100, 100))
}

// TestPostgresLineage runs against a real database when
// IGLOO_TEST_DATABASE_URL is set.
func TestPostgresLineage(t *testing.T) {
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
	if _, err := s.pool.Exec(ctx, "TRUNCATE igloo_audit.query_history"); err != nil {
		t.Fatal(err)
	}
	testLineage(t, s)
	qs, err := s.Queries(ctx, Viewer{All: true}, Page{Limit: 10})
	if err != nil || len(qs) != 8 {
		t.Fatalf("queries: %d %v", len(qs), err)
	}
	for _, q := range qs {
		if q.StatementType == "COPY" && (q.Job != "pipe drops" || len(q.LocationsRead) != 1) {
			t.Errorf("a COPY read back: %+v", q)
		}
	}
}

func TestLogCachesTableUsage(t *testing.T) {
	store := &countingStore{MemoryStore: NewMemoryStore(10, 10)}
	l := New(store, Options{}, discard)
	l.direct = false
	for range 3 {
		if _, err := l.TableUsage(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if store.usage != 1 {
		t.Errorf("usage read %d times, want once", store.usage)
	}
}

type countingStore struct {
	*MemoryStore
	usage int
}

func (c *countingStore) TableUsage(ctx context.Context, since time.Time) ([]TableUsage, error) {
	c.usage++
	return c.MemoryStore.TableUsage(ctx, since)
}
