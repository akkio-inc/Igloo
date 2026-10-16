package audit

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/testdb"
)

func TestComputeClockSplitsDays(t *testing.T) {
	started := time.Date(2026, 10, 14, 23, 30, 0, 0, time.UTC)
	c := computeClock{pool: "acme", replica: "r1", price: 2, started: started, written: started.Add(10 * time.Minute)}
	cs := c.totals(time.Date(2026, 10, 15, 0, 15, 0, 0, time.UTC))
	if len(cs) != 2 || cs[0].Seconds != 1800 || cs[1].Seconds != 900 || !cs[1].Day.Equal(time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)) ||
		!cs[1].Started.Equal(started) {
		t.Errorf("got %+v", cs)
	}
	// Once the first day is written, only today's total is.
	c.written = time.Date(2026, 10, 15, 0, 15, 0, 0, time.UTC)
	if cs := c.totals(time.Date(2026, 10, 15, 1, 0, 0, 0, time.UTC)); len(cs) != 1 || cs[0].Seconds != 3600 {
		t.Errorf("got %+v", cs)
	}
}

// testUsage checks Usage and WriteCompute on a store that starts empty.
func testUsage(t *testing.T, s Store) {
	t.Helper()
	ctx := context.Background()
	day := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	q := func(user, pool, tag string, at time.Duration, run, queued time.Duration, outcome string, cpu time.Duration) Query {
		start := day.Add(at)
		return Query{ID: NewID(start), SessionID: "s", User: user, Role: "reader", Start: start, Duration: run + queued,
			Queued: queued, Outcome: outcome, Rows: 1, StatementType: "SELECT", Text: "SELECT 1", Pool: pool, QueryTag: tag,
			Resources: &Resources{CPUTime: cpu, BytesRead: 10, MemoryAllocated: 100}}
	}
	if err := s.WriteQueries(ctx, []Query{
		q("ana", "acme", "client:acme", time.Hour, 3*time.Second, time.Second, OutcomeOK, 4*time.Second),
		q("ana", "acme", "client:acme", 2*time.Hour, time.Second, 0, OutcomeError, time.Second),
		q("bob", "acme", "", 3*time.Hour, 4*time.Second, 0, OutcomeOK, time.Second),
		q("bob", "", "", 4*time.Hour, 2*time.Second, 0, OutcomeOK, 0),
		q("bob", "acme", "", 30*time.Hour, time.Second, 0, OutcomeOK, 0), // the next day
	}); err != nil {
		t.Fatal(err)
	}
	// Two replicas of acme ran that day, one twice (it restarted under the
	// same name), at 1.50 an hour; the default pool's price is unknown.
	first, second := day.Add(time.Hour), day.Add(5*time.Hour)
	if err := s.WriteCompute(ctx, []Compute{
		{Day: day, Pool: "acme", Replica: "a", Started: first, Seconds: 600, PricePerHour: 1.5},
		{Day: day, Pool: "acme", Replica: "b", Started: first, Seconds: 3600, PricePerHour: 1.5},
		{Day: day, Pool: "default", Replica: "c", Started: first, Seconds: 3600},
	}); err != nil {
		t.Fatal(err)
	}
	// A process writes its growing total; a total written twice, as after a
	// retry, counts once.
	for _, c := range []Compute{
		{Day: day, Pool: "acme", Replica: "a", Started: first, Seconds: 1800, PricePerHour: 1.5},
		{Day: day, Pool: "acme", Replica: "a", Started: second, Seconds: 1800, PricePerHour: 1.5},
		{Day: day, Pool: "acme", Replica: "a", Started: second, Seconds: 1800, PricePerHour: 1.5},
		{Day: day, Pool: "acme", Replica: "a", Started: first, Seconds: 1800, PricePerHour: 1.5},
	} {
		if err := s.WriteCompute(ctx, []Compute{c}); err != nil {
			t.Fatal(err)
		}
	}

	str := func(us []Usage) string {
		var lines []string
		for _, u := range us {
			cost := "NULL"
			if u.Cost != nil {
				cost = fmt.Sprintf("%.2f", *u.Cost)
			}
			lines = append(lines, fmt.Sprintf("%s %s %s %s %q %d %d %s %s %s %d %.2f %.1f %s", u.Day.Format("01-02"), u.Pool, u.User, u.Role,
				u.QueryTag, u.Statements, u.Failed, u.Run, u.Queued, u.CPUTime, u.BytesRead, u.Share, u.ComputeHours, cost))
		}
		return strings.Join(lines, "\n")
	}
	all, err := s.Usage(ctx, Viewer{All: true}, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Join([]string{
		`10-15 acme bob reader "" 1 0 1s 0s 0s 10 1.00 -1.0 NULL`,
		`10-14 acme ana reader "client:acme" 2 1 4s 1s 5s 20 0.50 2.0 1.50`,
		`10-14 acme bob reader "" 1 0 4s 0s 1s 10 0.50 2.0 1.50`,
		`10-14 default bob reader "" 1 0 2s 0s 0s 10 1.00 1.0 NULL`,
	}, "\n")
	if str(all) != want {
		t.Errorf("usage:\n%s\nwant\n%s", str(all), want)
	}
	mine, err := s.Usage(ctx, Viewer{User: "ana"}, day, day.Add(24*time.Hour))
	if err != nil || len(mine) != 1 || mine[0].User != "ana" || mine[0].Share != 0.5 {
		t.Errorf("ana's usage: %s, %v", str(mine), err)
	}

	// Pruning drops compute time with the history.
	if err := s.Prune(ctx, day.Add(48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if all, _ := s.Usage(ctx, Viewer{All: true}, time.Time{}, time.Time{}); len(all) != 0 {
		t.Errorf("after pruning: %s", str(all))
	}
}

func TestMemoryUsage(t *testing.T) {
	testUsage(t, NewMemoryStore(100, 100))
}

// TestPostgresUsage runs against a real database when
// IGLOO_TEST_DATABASE_URL is set.
func TestPostgresUsage(t *testing.T) {
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
	if _, err := s.pool.Exec(ctx, "TRUNCATE igloo_audit.query_history, igloo_audit.login_history, igloo_audit.compute_time"); err != nil {
		t.Fatal(err)
	}
	testUsage(t, s)

	// The resources and attribution of a statement come back as written.
	start := time.Now().Truncate(time.Millisecond)
	if err := s.WriteQueries(ctx, []Query{
		{ID: NewID(start), SessionID: "s", User: "cy", Role: "writer", Start: start, Duration: 2 * time.Second, Queued: time.Second,
			Outcome: OutcomeOK, Rows: 3, StatementType: "SELECT", Text: "SELECT 3", Pool: "acme", QueryTag: "client:acme",
			Resources: &Resources{CPUTime: 1500 * time.Millisecond, BytesRead: 1 << 20, MemoryAllocated: 1 << 30}},
		{ID: NewID(start.Add(time.Millisecond)), SessionID: "s", User: "cy", Role: "writer", Start: start.Add(time.Millisecond),
			Outcome: OutcomeOK, Rows: -1, StatementType: "SET", Text: "SET query_tag = 'x'"},
	}); err != nil {
		t.Fatal(err)
	}
	qs, err := s.Queries(ctx, Viewer{User: "cy"}, Page{})
	if err != nil || len(qs) != 2 {
		t.Fatalf("%+v %v", qs, err)
	}
	got, unmeasured := qs[1], qs[0]
	if got.Pool != "acme" || got.QueryTag != "client:acme" || got.Queued != time.Second || got.Resources == nil ||
		*got.Resources != (Resources{CPUTime: 1500 * time.Millisecond, BytesRead: 1 << 20, MemoryAllocated: 1 << 30}) {
		t.Errorf("read back %+v %+v", got, got.Resources)
	}
	if unmeasured.Resources != nil || unmeasured.QueryTag != "" {
		t.Errorf("a statement DuckDB did not measure: %+v", unmeasured)
	}
}
