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

// The past week's statements seed the coordinator's runs, oldest first,
// with the memory they ran with, without failed ones or those of other
// statements; and the workload controller's footprints, of the statements
// that completed on one machine of this replica's memory, by shape.
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
	}
	if err := store.WriteQueries(context.Background(), qs); err != nil {
		t.Fatal(err)
	}
	var runs []distributed.Run
	footprints := map[string]int64{}
	seedHistory(context.Background(), func(rs []distributed.Run) { runs = append(runs, rs...) },
		func(shape string, allocated int64) { footprints[shape] = allocated }, 22*gb, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	want := []distributed.Run{{Key: "k", Duration: 90 * time.Second, Memory: 22 * gb},
		{Key: "k", Workers: 4, Duration: 30 * time.Second, Memory: 6 * gb}, {Key: "m", OutOfMemory: true}}
	if !slices.Equal(runs, want) {
		t.Errorf("seeded runs %+v, want %+v", runs, want)
	}
	// a's latest run on this replica's memory, 21 GB within a quarter of
	// 22; not b's, which failed, c's, on 14 GB, nor d's, which does not say.
	if len(footprints) != 1 || footprints["a"] != 2*gb {
		t.Errorf("seeded footprints %v, want a's 2 GB", footprints)
	}
	// Without a coordinator, footprints all the same.
	clear(footprints)
	seedHistory(context.Background(), nil, func(shape string, allocated int64) { footprints[shape] = allocated }, 22*gb, store,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if footprints["a"] != 2*gb {
		t.Errorf("without a coordinator, seeded footprints %v", footprints)
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
