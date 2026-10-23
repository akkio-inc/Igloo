package distributed

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"gocloud.dev/blob/memblob"

	"github.com/akkio-inc/igloo/internal/config"
)

// TestBucketStorage checks the storage of a blob store's bucket: a flat
// listing of every file under a directory, by URL, and their removal.
func TestBucketStorage(t *testing.T) {
	ctx := t.Context()
	b := memblob.OpenBucket(nil)
	defer b.Close() //nolint:errcheck
	s := bucketStorage{b}
	root := "s3://bucket/wh/_igloo_shuffle/q1"
	want := []string{root + "/s0/0/__b=0/data_0.parquet", root + "/s0/1/__b=7/data_0.parquet", root + "/" + heartbeatFile}
	for _, p := range append(slices.Clone(want), "s3://bucket/wh/_igloo_shuffle/q10/s0/0/__b=0/data_0.parquet") {
		if err := s.Write(ctx, p, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	list := func() []string {
		var got []string
		if err := s.List(ctx, root, func(path string, written time.Time) error {
			if written.IsZero() {
				t.Errorf("%s has no time", path)
			}
			got = append(got, path)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		slices.Sort(got)
		return got
	}
	slices.Sort(want)
	if got := list(); !slices.Equal(got, want) {
		t.Fatalf("listed %q, want %q", got, want)
	}
	// A file removed already is no error.
	if err := s.Remove(ctx, append(want, root+"/gone")); err != nil {
		t.Fatal(err)
	}
	if got := list(); len(got) != 0 {
		t.Errorf("left %q", got)
	}
	if ok, _ := b.Exists(ctx, "wh/_igloo_shuffle/q10/s0/0/__b=0/data_0.parquet"); !ok {
		t.Error("removed another statement's file")
	}
}

// TestSweepExchanges checks which statements' exchanges a sweep removes:
// those under which nothing was written for staleExchanges, unless a
// statement of the replica holds them; a recent heartbeat keeps old ones.
func TestSweepExchanges(t *testing.T) {
	h := newHarness(t, 0.01, 1, 1)
	root := h.co.opts.Warehouse + config.ShuffleDir
	now := time.Now()
	write := func(path string, age time.Duration) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, now.Add(-age), now.Add(-age)); err != nil {
			t.Fatal(err)
		}
	}
	old := staleExchanges + time.Hour
	for _, q := range []string{"left", "held", "beating"} {
		write(root+"/"+q+"/s0/0/__b=0/data_0.parquet", old)
		write(root+"/"+q+"/s1/2/__b=3/data_0.parquet", old)
	}
	write(root+"/beating/"+heartbeatFile, heartbeatEvery/2)
	write(root+"/recent/s0/0/__b=0/data_0.parquet", time.Hour)
	release := h.co.hold("held", root+"/held")
	defer release()

	if err := h.co.sweep(t.Context(), now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root + "/left"); !os.IsNotExist(err) {
		t.Errorf("left the exchanges of a statement that no longer runs: %v", err)
	}
	for _, q := range []string{"held", "beating", "recent"} {
		if _, err := os.Stat(root + "/" + q + "/s0/0/__b=0/data_0.parquet"); err != nil {
			t.Errorf("removed the exchanges of %s: %v", q, err)
		}
	}
}

// TestHeartbeat checks that a statement holding its exchanges writes a
// heartbeat under them while it runs.
func TestHeartbeat(t *testing.T) {
	h := newHarness(t, 0.01, 1, 1)
	h.co.heartbeat = 10 * time.Millisecond
	dir := h.co.opts.Warehouse + config.ShuffleDir + "/q"
	release := h.co.hold("q", dir)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(dir + "/" + heartbeatFile); err == nil {
			break
		}
		if time.Now().After(deadline) {
			release()
			t.Fatal("no heartbeat")
		}
		time.Sleep(10 * time.Millisecond)
	}
	release()
	h.co.removeLater("q", dir)
	h.co.removals.Wait()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("the exchanges are still there: %v", err)
	}
	h.co.runningMu.Lock()
	defer h.co.runningMu.Unlock()
	if h.co.running["q"] {
		t.Error("removed exchanges are still held")
	}
}
