package main

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/distributed"
)

type seeds struct{ runs []distributed.Run }

func (s *seeds) Seed(rs []distributed.Run) { s.runs = append(s.runs, rs...) }

// The past week's runs of statements that could run on workers seed the
// coordinator, oldest first, without failed ones or those of other
// statements.
func TestSeedRuns(t *testing.T) {
	store := audit.NewMemoryStore(audit.MemoryQueries, audit.MemoryLogins)
	now := time.Now()
	qs := []audit.Query{
		{ID: "1", Start: now.Add(-8 * 24 * time.Hour), PlanKey: "k", Outcome: audit.OutcomeOK, Duration: time.Hour},
		{ID: "2", Start: now.Add(-3 * time.Hour), PlanKey: "k", Outcome: audit.OutcomeOK, Duration: 90 * time.Second},
		{ID: "3", Start: now.Add(-2 * time.Hour), PlanKey: "k", Outcome: audit.OutcomeOK, Workers: 4, Duration: 30 * time.Second},
		{ID: "4", Start: now.Add(-time.Hour), PlanKey: "k", Outcome: audit.OutcomeError, Duration: time.Second},
		{ID: "5", Start: now.Add(-time.Minute), Outcome: audit.OutcomeOK, Duration: time.Second},
		{ID: "6", Start: now.Add(-30 * time.Second), PlanKey: "m", Outcome: audit.OutcomeError, SQLState: "53200", Duration: time.Hour},
	}
	if err := store.WriteQueries(context.Background(), qs); err != nil {
		t.Fatal(err)
	}
	var got seeds
	seedRuns(context.Background(), &got, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	want := []distributed.Run{{Key: "k", Duration: 90 * time.Second}, {Key: "k", Workers: 4, Duration: 30 * time.Second},
		{Key: "m", OutOfMemory: true}}
	if !slices.Equal(got.runs, want) {
		t.Errorf("seeded %+v, want %+v", got.runs, want)
	}
}
