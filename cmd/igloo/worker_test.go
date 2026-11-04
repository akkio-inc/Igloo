package main

import (
	"context"
	"fmt"
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
// held, from what they allocated or their peak if more, and at least what
// runs that ended early held, or all their memory when those ran a while
// without a peak.
func TestSeedHistory(t *testing.T) {
	store := audit.NewMemoryStore(audit.MemoryQueries, audit.MemoryLogins)
	now := time.Now()
	const gb = int64(1) << 30
	used := func(n int64) *audit.Resources { return &audit.Resources{BytesRead: 1, MemoryAllocated: n} }
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
	// a's runs on this replica's memory, 21 GB within a quarter of 22, the
	// lower of two; not b's, which failed, c's, on 14 GB, d's, which does not say,
	// nor i's, cancelled after a second.
	wantHeld := map[string]int64{"a": workload.FootprintOf(2*gb, workload.Seen{}), "e": workload.FootprintOf(84*gb, workload.Seen{Peak: 12 * gb, Sampled: true}), "f": 8 * gb,
		"g": 10 * gb, "h": 22 * gb}
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
	// What a shape's completed runs took here, past what they waited, whatever
	// ran them: a's lower of two, of which one has no plan key; e's, not h's or
	// i's, cancelled, nor c's, on another memory, or d's, which does not say.
	wantTook := map[string]time.Duration{"a": time.Second, "e": time.Minute, "g": time.Minute, "f": time.Minute}
	for _, shape := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"} {
		got, _, ok := footprints.Took(shape)
		if w, learned := wantTook[shape]; ok != learned || got != w {
			t.Errorf("seeded time of %s %s (%v), want %s", shape, got, ok, w)
		}
	}
	queued := workload.NewController(22 * gb)
	queuedStore := audit.NewMemoryStore(audit.MemoryQueries, audit.MemoryLogins)
	if err := queuedStore.WriteQueries(context.Background(), []audit.Query{{ID: "1", Start: now.Add(-time.Minute), Outcome: audit.OutcomeOK,
		Duration: 714 * time.Second, Queued: 14 * time.Second, Shape: "j", Resources: used(gb), Memory: 22 * gb}}); err != nil {
		t.Fatal(err)
	}
	seedHistory(context.Background(), nil, queued, 22*gb, queuedStore, slog.New(slog.DiscardHandler))
	if got, _, ok := queued.Took("j"); !ok || got != 700*time.Second {
		t.Errorf("seeded time past its wait: %s (%v)", got, ok)
	}
	// What the guard learned of a run it moved, by the placement of that run on
	// workers: not of one the history does not say so of.
	guarded := audit.NewMemoryStore(audit.MemoryQueries, audit.MemoryLogins)
	placement := (&distributed.Coordinator{}).GuardPlacement(88*time.Second, 47*time.Second, "8 workers: x")
	if err := guarded.WriteQueries(context.Background(), []audit.Query{
		{ID: "1", Start: now.Add(-time.Minute), PlanKey: "g", Outcome: audit.OutcomeOK, Workers: 8, Duration: 2 * time.Minute, Memory: 6 * gb, Placement: placement},
		{ID: "2", Start: now.Add(-time.Second), PlanKey: "w", Outcome: audit.OutcomeOK, Workers: 8, Duration: time.Minute, Memory: 6 * gb, Placement: "8 workers: x"},
	}); err != nil {
		t.Fatal(err)
	}
	var seeded []distributed.Run
	seedHistory(context.Background(), func(rs []distributed.Run) { seeded = rs }, workload.NewController(22*gb), 22*gb, guarded, slog.New(slog.DiscardHandler))
	if want := []distributed.Run{{Key: "g", Guarded: true, Duration: 88 * time.Second}, {Key: "g", Workers: 8, Duration: 2 * time.Minute, Memory: 6 * gb},
		{Key: "w", Workers: 8, Duration: time.Minute, Memory: 6 * gb}}; !slices.Equal(seeded, want) {
		t.Errorf("seeded a guarded run: %+v, want %+v", seeded, want)
	}
	// A shape's FLATTEN forms, both seen, take the faster.
	forms := workload.NewController(22 * gb)
	formStore := audit.NewMemoryStore(audit.MemoryQueries, audit.MemoryLogins)
	if err := formStore.WriteQueries(context.Background(), []audit.Query{
		{ID: "1", Start: now.Add(-2 * time.Minute), Outcome: audit.OutcomeOK, FlattenKey: "k",
			Resources: &audit.Resources{BytesRead: 1, CPUTime: 5 * time.Second}},
		{ID: "2", Start: now.Add(-time.Minute), Outcome: audit.OutcomeOK, FlattenKey: "k", FlattenLists: true,
			Resources: &audit.Resources{BytesRead: 1, CPUTime: 2 * time.Second}},
		{ID: "3", Start: now.Add(-time.Second), Outcome: audit.OutcomeError, FlattenKey: "k",
			Resources: &audit.Resources{BytesRead: 1, CPUTime: time.Second}},
		{ID: "4", Start: now.Add(-time.Second), Outcome: audit.OutcomeOK, FlattenKey: "k", Workers: 2,
			Resources: &audit.Resources{BytesRead: 1, CPUTime: time.Second}},
		// Runs on workers, judged by the workers' time: 16 for 10 minutes, and for 8.
		{ID: "5", Start: now.Add(-3 * time.Minute), Outcome: audit.OutcomeOK, FlattenKey: "w", Workers: 16, Duration: 10 * time.Minute},
		{ID: "6", Start: now.Add(-2 * time.Minute), Outcome: audit.OutcomeOK, FlattenKey: "w", FlattenLists: true, Workers: 16,
			Duration: 9 * time.Minute, Queued: time.Minute},
	}); err != nil {
		t.Fatal(err)
	}
	seedHistory(context.Background(), nil, forms, 22*gb, formStore, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if !forms.ChooseForm("k", func() bool { t.Error("asked DuckDB of a shape in the history"); return false }) {
		t.Error("seeded forms: not the faster")
	}
	if !forms.ChooseForm("w", func() bool { t.Error("asked DuckDB of a shape in the history"); return false }) {
		t.Error("seeded forms of runs on workers: not the faster")
	}
	// Without a coordinator, footprints all the same.
	footprints = workload.NewController(22 * gb)
	seedHistory(context.Background(), nil, footprints, 22*gb, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if got, _ := footprints.Footprint("a"); got != workload.FootprintOf(2*gb, workload.Seen{}) {
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

// TestSeedHistoryRunsThatReadNothing checks that a run that read nothing says
// nothing of a shape with one that read something, whichever came first.
func TestSeedHistoryRunsThatReadNothing(t *testing.T) {
	store := audit.NewMemoryStore(audit.MemoryQueries, audit.MemoryLogins)
	now := time.Now()
	const gb = int64(1) << 30
	limits := workload.NewController(22 * gb)
	limits.EstimateFromHistory(1)
	limits.WeighCosts(8, workload.LightCPU)
	var qs []audit.Query
	for i, r := range []audit.Resources{
		{BytesRead: 0, CPUTime: 100 * time.Millisecond, MemoryAllocated: 12 << 20},
		{BytesRead: gb, CPUTime: 80 * time.Second, MemoryAllocated: 6 * gb},
		{BytesRead: 0, CPUTime: 100 * time.Millisecond, MemoryAllocated: 12 << 20},
	} {
		qs = append(qs, audit.Query{ID: fmt.Sprint(i), Start: now.Add(time.Duration(i-10) * time.Minute),
			Outcome: audit.OutcomeOK, Duration: time.Second, Shape: "s", Resources: &r, Memory: 22 * gb})
	}
	if err := store.WriteQueries(context.Background(), qs); err != nil {
		t.Fatal(err)
	}
	seedHistory(context.Background(), nil, limits, 22*gb, store, slog.New(slog.DiscardHandler))
	if got := limits.Estimate("s"); got != 4*gb {
		t.Errorf("reserves %d, want %d", got, 4*gb)
	}
	if limits.Light("s") {
		t.Error("light, for runs that read nothing")
	}
	if got := limits.Expect("s"); got != 10*time.Second {
		t.Errorf("expected to take %s", got)
	}
}

// TestSeedHistoryExact checks that a restarted server has what ran of the same text
// with the same literals, apart from what ran of others alike.
func TestSeedHistoryExact(t *testing.T) {
	store := audit.NewMemoryStore(audit.MemoryQueries, audit.MemoryLogins)
	now := time.Now()
	const gb = int64(1) << 30
	limits := workload.NewController(22 * gb)
	limits.EstimateFromHistory(1)
	var qs []audit.Query
	for i, r := range []struct {
		exact     string
		allocated int64
	}{{"small", 3 * gb}, {"large", 12 * gb}, {"small", 3 * gb}} {
		qs = append(qs, audit.Query{ID: fmt.Sprint(i), Start: now.Add(time.Duration(i-10) * time.Minute), Outcome: audit.OutcomeOK,
			Duration: time.Second, Shape: "s", Exact: r.exact, Memory: 22 * gb,
			Resources: &audit.Resources{BytesRead: 1, CPUTime: time.Minute, MemoryAllocated: r.allocated}})
	}
	if err := store.WriteQueries(context.Background(), qs); err != nil {
		t.Fatal(err)
	}
	seedHistory(context.Background(), nil, limits, 22*gb, store, slog.New(slog.DiscardHandler))
	if got := limits.Estimate("large"); got != 8*gb {
		t.Errorf("large reserves %d, want %d", got, 8*gb)
	}
	if got := limits.Estimate("small"); got != 2*gb {
		t.Errorf("small reserves %d, want %d", got, 2*gb)
	}
}

// TestSeedHistoryOnWorkers checks that a restarted server knows which statements ran on
// workers last, whatever their memory, and takes a later run here over one there.
func TestSeedHistoryOnWorkers(t *testing.T) {
	store := audit.NewMemoryStore(audit.MemoryQueries, audit.MemoryLogins)
	now := time.Now()
	const gb = int64(1) << 30
	limits := workload.NewController(22 * gb)
	qs := []audit.Query{
		{ID: "1", Start: now.Add(-3 * time.Minute), Outcome: audit.OutcomeOK, Duration: time.Second, Shape: "there", Exact: "there1", Workers: 4, Memory: 8 * gb},
		{ID: "2", Start: now.Add(-3 * time.Minute), Outcome: audit.OutcomeOK, Duration: time.Second, Shape: "back", Exact: "back1", Workers: 4, Memory: 8 * gb},
		{ID: "3", Start: now.Add(-2 * time.Minute), Outcome: audit.OutcomeOK, Duration: time.Second, Shape: "back", Exact: "back1", Memory: 22 * gb,
			Resources: &audit.Resources{BytesRead: 1, CPUTime: time.Minute, MemoryAllocated: 3 * gb}},
		{ID: "4", Start: now.Add(-time.Minute), Outcome: audit.OutcomeError, Duration: time.Second, Shape: "failed", Workers: 4, Memory: 8 * gb},
	}
	if err := store.WriteQueries(context.Background(), qs); err != nil {
		t.Fatal(err)
	}
	seedHistory(context.Background(), nil, limits, 22*gb, store, slog.New(slog.DiscardHandler))
	if !limits.RunsOnWorkers("there1", "there") {
		t.Error("a run on workers was not seen")
	}
	if limits.RunsOnWorkers("back1", "back") {
		t.Error("a later run here was not taken over one on workers")
	}
	if limits.RunsOnWorkers("", "failed") {
		t.Error("a run that failed on workers was taken for one that ran there")
	}
}
