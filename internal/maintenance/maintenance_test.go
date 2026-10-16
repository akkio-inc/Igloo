package maintenance

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/testdb"
)

type fakeSchedule map[string]time.Time

func (f fakeSchedule) LastRuns(_ context.Context, prefix string) (map[string]time.Time, error) {
	runs := map[string]time.Time{}
	for job, at := range f {
		if len(job) >= len(prefix) && job[:len(prefix)] == prefix {
			runs[job] = at
		}
	}
	return runs, nil
}

func (f fakeSchedule) Ran(_ context.Context, job string, at time.Time) error {
	f[job] = at
	return nil
}

func TestFirstDelay(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	opts := Options{Interval: time.Hour}
	ctx := context.Background()

	// No schedule: somewhere in the first quarter of the interval.
	if d := New(nil, "", opts, log).firstDelay(ctx); d < 0 || d > 15*time.Minute {
		t.Fatalf("without a schedule: %v", d)
	}

	// Never ran, or ran long ago: within seconds, even after a restart.
	for name, sched := range map[string]fakeSchedule{
		"never":   {},
		"overdue": {passJob: time.Now().Add(-3 * time.Hour)},
	} {
		opts.Schedule = sched
		if d := New(nil, "", opts, log).firstDelay(ctx); d < resumeDelay || d > 2*resumeDelay {
			t.Errorf("%s: %v, want about %v", name, d, resumeDelay)
		}
	}

	// Ran recently: wait for the rest of the interval.
	opts.Schedule = fakeSchedule{passJob: time.Now().Add(-20 * time.Minute)}
	if d := New(nil, "", opts, log).firstDelay(ctx); d < 39*time.Minute || d > 40*time.Minute {
		t.Fatalf("recent pass: %v, want about 40m", d)
	}
}

func TestPostgresSchedule(t *testing.T) {
	url := testdb.URL()
	if url == "" {
		t.Skip("IGLOO_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	lock, err := NewPostgresLock(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	// A second replica starting up must not fail on the existing table.
	again, err := NewPostgresLock(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	again.Close()

	now := time.Now().Truncate(time.Microsecond)
	for job, at := range map[string]time.Time{"pass": now, "orphans/public.a": now.Add(-time.Hour), "orphans/public.b": now} {
		if err := lock.Ran(ctx, job, at); err != nil {
			t.Fatal(err)
		}
	}
	// An older time does not move a job backwards.
	if err := lock.Ran(ctx, "pass", now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}

	// Ran and LastRuns work while the lock is held, as during a pass.
	release, ok, err := lock.TryLock(ctx)
	if err != nil || !ok {
		t.Fatalf("TryLock: %v, %v", ok, err)
	}
	defer release()
	runs, err := lock.LastRuns(ctx, "orphans/")
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 || !runs["orphans/public.b"].Equal(now) {
		t.Fatalf("orphan runs: %v", runs)
	}
	runs, err = lock.LastRuns(ctx, "pass")
	if err != nil || !runs["pass"].Equal(now) {
		t.Fatalf("pass run: %v, %v", runs, err)
	}
}
