package erasure

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	iceio "github.com/apache/iceberg-go/io"

	"github.com/akkio-inc/igloo/internal/policy"
	"github.com/akkio-inc/igloo/internal/uploads"
)

// fakeUploads is a store's unfinished uploads.
type fakeUploads struct {
	mu      sync.Mutex
	list    []uploads.Upload
	listErr error
	stuck   bool
	aborted []string
}

func (f *fakeUploads) List(context.Context, string) ([]uploads.Upload, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.list), f.listErr
}

func (f *fakeUploads) Abort(_ context.Context, u uploads.Upload) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stuck {
		return nil
	}
	f.aborted = append(f.aborted, u.Path)
	f.list = slices.DeleteFunc(f.list, func(o uploads.Upload) bool { return o.Path == u.Path })
	return nil
}

// withUploads gives the fixture's request a store with uploads under the tagged
// table and elsewhere, of which the two named last began long after any deletion.
func withUploads(t *testing.T, f *fixture) (*fakeUploads, map[string]string) {
	t.Helper()
	f.people("crm.people", [][]any{ann, subject, carl})
	other := f.people("crm.other", [][]any{carl})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	long := time.Now().Add(-time.Hour)
	later := time.Now().Add(30 * 24 * time.Hour)
	paths := map[string]string{
		"ours":     f.load("crm.people").Location() + "/data/igloo-write-0123456789abcdef/q/s0-0/a0/f/part_0.parquet",
		"exchange": f.warehouse() + "_igloo_shuffle/q/s0-0/a0/part_0.parquet",
		"dropped":  f.warehouse() + "gone/data/igloo-write-0123456789abcdef/q/s0-0/a0/f/part_0.parquet",
		"theirs":   other.Location() + "/data/igloo-write-0123456789abcdef/q/s0-0/a0/f/part_0.parquet",
		"later":    f.load("crm.people").Location() + "/data/igloo-write-fedcba9876543210/q/s0-0/a0/f/part_0.parquet",
	}
	store := &fakeUploads{list: []uploads.Upload{
		{Path: paths["ours"], Initiated: long},
		{Path: paths["exchange"], Initiated: long},
		{Path: paths["dropped"], Initiated: long},
		{Path: paths["theirs"], Initiated: long},
		{Path: paths["later"], Initiated: later},
	}}
	f.svc.opts.Uploads = func(iceio.IO, string) (uploads.Store, error) { return store, nil }
	return store, paths
}

// A request aborts the unfinished uploads that could hold its rows, and completes
// with none of them left, leaving those of other tables and those that began after
// its deletions.
func TestRequestAbortsUploads(t *testing.T) {
	f := newFixture(t)
	store, paths := withUploads(t, f)
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	got := settle(f, r.ID, 10)
	f.completed(got)
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, name := range []string{"ours", "exchange", "dropped"} {
		if !slices.Contains(store.aborted, paths[name]) {
			t.Errorf("the upload %s is still there: %q", name, store.aborted)
		}
	}
	for _, name := range []string{"theirs", "later"} {
		if slices.Contains(store.aborted, paths[name]) {
			t.Errorf("aborted the upload %s", name)
		}
	}
}

// A request does not complete while it finds such an upload.
func TestRequestWaitsForUploads(t *testing.T) {
	f := newFixture(t)
	store, _ := withUploads(t, f)
	store.stuck = true
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	got := settle(f, r.ID, maxRounds+10)
	if got.Status == StatusCompleted {
		t.Fatal("it completed with uploads that could hold its rows")
	}
	if !strings.Contains(got.Error, "unfinished uploads") {
		t.Errorf("error %q, status %s", got.Error, got.Status)
	}
}

// Nor while it cannot tell.
func TestRequestCannotListUploads(t *testing.T) {
	f := newFixture(t)
	store, _ := withUploads(t, f)
	store.listErr = errors.New("AccessDenied: s3:ListBucketMultipartUploads")
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	got := settle(f, r.ID, 10)
	if got.Status == StatusCompleted {
		t.Fatal("it completed without seeing the store's uploads")
	}
	if !strings.Contains(got.Error, "ListBucketMultipartUploads") {
		t.Errorf("error %q, status %s", got.Error, got.Status)
	}
	// It goes on once the store answers.
	store.mu.Lock()
	store.listErr = nil
	store.mu.Unlock()
	got = settle(f, r.ID, 10)
	f.completed(got)
}

// TestOddKeys checks that a file with a key no path could be counts as old whatever its
// age: DuckDB cannot read it to tell what it holds, and a request would otherwise wait on
// it, or fail for it.
func TestOddKeys(t *testing.T) {
	for p, want := range map[string]bool{
		"s3://b/w/t/data/igloo-write-a/x/../evil.parquet": true,
		"s3://b/w/t/data/./evil.parquet":                  true,
		"s3://b/w/t/data//evil.parquet":                   true,
		"s3://b/w/t/data/..":                              true,
		"s3://b/w/t/data/a..b.parquet":                    false,
		"s3://b/w/t/data/x=1/part_0.parquet":              false,
		"abfss://c@a.host/w/t/data/part_0.parquet":        false,
		"/local/w/t/data/part_0.parquet":                  false,
	} {
		if got := oddKey(p); got != want {
			t.Errorf("%s: %v", p, got)
		}
	}
	s := &Service{opts: Options{}}
	s.now = time.Now
	o := s.sortOrphans(map[string]time.Time{"s3://b/w/x/../evil.parquet": time.Now(), "s3://b/w/new.parquet": time.Now()}, pathSet{})
	if !slices.Equal(o.old, []string{"s3://b/w/x/../evil.parquet"}) || len(o.young) != 1 {
		t.Errorf("old %q, young %v", o.old, o.young)
	}
}
