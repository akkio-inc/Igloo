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
	"github.com/akkio-inc/igloo/internal/workload"
)

// The past week's statements seed the coordinator's runs, oldest first,
// with the memory they ran with, without failed ones or those of other
// statements; and the workload controller's footprints, of the statements
// that ran on one machine of this replica's memory, by shape: what they
// held, the smaller of their peak and what they allocated, and at least
// what runs that ended early held, or all their memory when those ran a
// while without a peak.
func TestSeedHistory(t *testing.T) {
	store := audit.NewMemoryStore(audit.MemoryQueries, audit.MemoryLogins)
	now := time.Now()
	const gb = int64(1) << 30
	used := func(n int64) *audit.Resources { return &audit.Resources{MemoryAllocated: n} }
	qs := []audit.Query{
		{ID: "1", Start: now.Add(-8 * 24 * time.Hour), PlanKey: "k", Outcome: audit.OutcomeOK, Duration: time.Hour, Memory: 22 * gb},
		{ID: "2", Start: now.Add(-3 * time.Hour), PlanKey: "k", Outcome: audit.OutcomeOK, Duration: 90 * time.Second, Memory: 22 * gb,
			Shape: "a", Resources: used(3 * gb)},
		{ID: "3", Start: now.Add(-2 * time.Hour), PlanKey: "k", Outcome: audit.OutcomeOK, Workers: 4, Duration: 30 * time.Second,
			Memory: 6 * gb, Shape: "a", Resources: used(gb)},
		{ID: "4", Start: now.Add(-time.Hour), PlanKey: "k", Outcome: audit.OutcomeError, Duration: time.Second, Shape: "b",
			Resources: used(gb), Memory: 22 * gb},
		{ID: "5", Start: now.Add(-time.Minute), Outcome: audit.OutcomeOK, Duration: time.Second, Shape: "a", Resources: used(2 * gb),
			Memory: 21 * gb},
		{ID: "6", Start: now.Add(-50 * time.Second), Outcome: audit.OutcomeOK, Duration: time.Second, Shape: "c", Resources: used(gb),
			Memory: 14 * gb},
		{ID: "7", Start: now.Add(-40 * time.Second), Outcome: audit.OutcomeOK, Duration: time.Second, Shape: "d", Resources: used(gb)},
		{ID: "8", Start: now.Add(-30 * time.Second), PlanKey: "m", Outcome: audit.OutcomeError, SQLState: "53200", Duration: time.Hour},
		// e peaked under what it allocated; f's second run was cancelled
		// holding less than its first, and g's held more; h was cancelled
		// after a minute, before peaks were kept, and i after a second.
		{ID: "9", Start: now.Add(-29 * time.Second), Outcome: audit.OutcomeOK, Duration: time.Minute, Shape: "e", Resources: used(84 * gb),
			Peak: 12 * gb, Memory: 22 * gb},
		{ID: "10", Start: now.Add(-28 * time.Second), Outcome: audit.OutcomeOK, Duration: time.Minute, Shape: "f", Resources: used(9 * gb),
			Peak: 8 * gb, Memory: 22 * gb},
		{ID: "11", Start: now.Add(-27 * time.Second), Outcome: audit.OutcomeCanceled, Duration: time.Minute, Shape: "f", Peak: 5 * gb,
			Memory: 22 * gb},
		{ID: "12", Start: now.Add(-26 * time.Second), Outcome: audit.OutcomeOK, Duration: time.Minute, Shape: "g", Resources: used(4 * gb),
			Memory: 22 * gb},
		{ID: "13", Start: now.Add(-25 * time.Second), Outcome: audit.OutcomeCanceled, Duration: time.Minute, Shape: "g", Peak: 10 * gb,
			Memory: 22 * gb},
		{ID: "14", Start: now.Add(-24 * time.Second), Outcome: audit.OutcomeCanceled, Duration: 15 * time.Minute, Queued: 5 * time.Minute,
			Shape: "h", Memory: 22 * gb},
		{ID: "15", Start: now.Add(-23 * time.Second), Outcome: audit.OutcomeCanceled, Duration: time.Second, Shape: "i", Memory: 22 * gb},
	}
	if err := store.WriteQueries(context.Background(), qs); err != nil {
		t.Fatal(err)
	}
	var runs []distributed.Run
	footprints := workload.NewController(22 * gb)
	seedHistory(context.Background(), func(rs []distributed.Run) { runs = append(runs, rs...) }, footprints, 22*gb, store,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	want := []distributed.Run{{Key: "k", Duration: 90 * time.Second, Memory: 22 * gb},
		{Key: "k", Workers: 4, Duration: 30 * time.Second, Memory: 6 * gb}, {Key: "m", OutOfMemory: true}}
	if !slices.Equal(runs, want) {
		t.Errorf("seeded runs %+v, want %+v", runs, want)
	}
	// a's latest run on this replica's memory, 21 GB within a quarter of
	// 22; not b's, which failed, c's, on 14 GB, d's, which does not say,
	// nor i's, cancelled after a second.
	wantHeld := map[string]int64{"a": 2 * gb, "e": 12 * gb, "f": 8 * gb, "g": 10 * gb, "h": 22 * gb}
	for _, shape := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"} {
		got, ok := footprints.Footprint(shape)
		if w, learned := wantHeld[shape]; ok != learned || got != w {
			t.Errorf("seeded footprint of %s %d (%v), want %d", shape, got, ok, w)
		}
	}
	// Proof for auto mode that a shape fits only from completed runs: not
	// g's, whose latest run was cancelled holding more, nor h's, all the
	// memory of a run cancelled after ten minutes.
	for shape, completed := range map[string]bool{"a": true, "e": true, "f": true, "g": false, "h": false} {
		if _, ok := footprints.Completed(shape); ok != completed {
			t.Errorf("seeded footprint of %s completed %v", shape, ok)
		}
	}
	// Without a coordinator, footprints all the same.
	footprints = workload.NewController(22 * gb)
	seedHistory(context.Background(), nil, footprints, 22*gb, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if got, _ := footprints.Footprint("a"); got != 2*gb {
		t.Errorf("without a coordinator, a's seeded footprint %d", got)
	}
}

func TestPrefetchScans(t *testing.T) {
	got := prefetchScans([]distributed.Scan{{Table: "v", Filtered: true, FieldIDs: []int{1, 4},
		Files: []distributed.File{{Path: "s3://b/t/data/a.parquet", Size: 10}, {Path: "s3://b/t/data/b.parquet", Size: 20}}}})
	if len(got) != 1 || !got[0].Filtered || len(got[0].FieldIDs) != 2 || got[0].FieldIDs[1] != 4 ||
		len(got[0].Files) != 2 || got[0].Files[1].Path != "s3://b/t/data/b.parquet" || got[0].Files[1].Size != 20 {
		t.Errorf("prefetchScans: %+v", got)
	}
}
