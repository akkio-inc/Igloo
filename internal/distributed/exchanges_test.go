package distributed

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
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
	// A statement whose removal began, and stopped with its coordinator:
	// recent, but marked as being removed.
	write(root+"/stopped/s0/0/__b=0/data_0.parquet", time.Minute)
	write(root+"/stopped/"+removingFile, time.Minute)
	for _, q := range []string{"left", "held", "beating"} {
		write(root+"/"+q+"/s0/0/__b=0/data_0.parquet", old)
		write(root+"/"+q+"/s1/2/__b=3/data_0.parquet", old)
	}
	write(root+"/beating/"+heartbeatFile, heartbeatEvery/2)
	write(root+"/recent/s0/0/__b=0/data_0.parquet", time.Hour)
	release := h.co.hold("held", root+"/held")
	defer release()
	// A statement's spread directories go together: old files in two, of
	// which a recent heartbeat in a third keeps both; and a held one.
	write(root+"/3.spread/s0/0/__b=0/data_0.parquet", old)
	write(root+"/a.spread/s1/1/__b=0/data_0.parquet", old)
	write(root+"/3.kept/s0/0/__b=0/data_0.parquet", old)
	write(root+"/0.kept/"+heartbeatFile, heartbeatEvery/2)
	write(root+"/7.spreadheld/s0/0/__b=0/data_0.parquet", old)
	releaseSpread := h.co.hold("spreadheld", spreadRoots(h.co.opts.Warehouse, "spreadheld")...)
	defer releaseSpread()

	if err := h.co.sweep(t.Context(), now); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"left", "stopped", "3.spread/s0", "a.spread/s1"} {
		if _, err := os.Stat(root + "/" + q); !os.IsNotExist(err) {
			t.Errorf("left the exchanges of %s, which no longer runs: %v", q, err)
		}
	}
	for _, q := range []string{"held", "beating", "recent", "3.kept", "7.spreadheld"} {
		if _, err := os.Stat(root + "/" + q + "/s0/0/__b=0/data_0.parquet"); err != nil {
			t.Errorf("removed the exchanges of %s: %v", q, err)
		}
	}
	for name, want := range map[string]string{"3.q": "q", "f.0123-1": "0123-1", "0123": "0123", "g.q": "g.q", "ab": "ab"} {
		if got := exchangeQID(name); got != want {
			t.Errorf("exchangeQID(%q) = %q, want %q", name, got, want)
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

// deleter is S3's DeleteObjects, recording the keys of each request.
type deleter struct {
	mu       sync.Mutex
	requests [][]string
	fail     bool
	errors   []s3types.Error
}

func (d *deleter) DeleteObjects(_ context.Context, in *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	var keys []string
	for _, o := range in.Delete.Objects {
		keys = append(keys, aws.ToString(in.Bucket)+"/"+aws.ToString(o.Key))
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.requests = append(d.requests, keys)
	if d.fail {
		return nil, errors.New("NotImplemented: DeleteObjects")
	}
	return &s3.DeleteObjectsOutput{Errors: d.errors}, nil
}

// TestRemoveInBatches checks that S3 deletes exchanges a thousand keys a
// request, by bucket: a statement's 55,000 files, deleted one a request,
// were still being deleted minutes later when its coordinator restarted.
// A store without DeleteObjects deletes them one a request; a key S3 could
// not delete is an error, one already gone is not.
func TestRemoveInBatches(t *testing.T) {
	var paths []string
	for i := range 2500 {
		paths = append(paths, fmt.Sprintf("s3://b1/wh/_igloo_shuffle/q/s0/%d/__b=0/data_0.parquet", i))
	}
	paths = append(paths, "s3://b2/wh/_igloo_shuffle/q/running")
	var mu sync.Mutex
	var each []string
	one := func(_ context.Context, ps []string) error {
		mu.Lock()
		defer mu.Unlock()
		each = append(each, ps...)
		return nil
	}
	d := &deleter{}
	if err := removeInBatches(t.Context(), d, paths, one); err != nil {
		t.Fatal(err)
	}
	var sizes []int
	seen := map[string]bool{}
	for _, r := range d.requests {
		sizes = append(sizes, len(r))
		for _, k := range r {
			seen[k] = true
		}
	}
	slices.Sort(sizes)
	if !slices.Equal(sizes, []int{1, 500, 1000, 1000}) || len(seen) != 2501 || len(each) > 0 ||
		!seen["b1/wh/_igloo_shuffle/q/s0/7/__b=0/data_0.parquet"] || !seen["b2/wh/_igloo_shuffle/q/running"] {
		t.Errorf("requests of %v keys, %d keys in all, %d deleted alone", sizes, len(seen), len(each))
	}

	d = &deleter{fail: true}
	if err := removeInBatches(t.Context(), d, paths[:1500], one); err != nil || len(each) != 1500 {
		t.Errorf("without DeleteObjects: %v, %d deleted alone", err, len(each))
	}

	d = &deleter{errors: []s3types.Error{{Key: aws.String("gone"), Code: aws.String("NoSuchKey")},
		{Key: aws.String("kept"), Code: aws.String("AccessDenied"), Message: aws.String("denied")}}}
	err := removeInBatches(t.Context(), d, paths[:10], one)
	if err == nil || !strings.Contains(err.Error(), "kept") || strings.Contains(err.Error(), "gone") {
		t.Errorf("errors of keys: %v", err)
	}
}

// opStorage records what is written and removed, in order.
type opStorage struct {
	Storage
	mu  sync.Mutex
	ops []string
}

func (s *opStorage) Write(_ context.Context, path string, _ []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops = append(s.ops, "write "+path)
	return nil
}

func (s *opStorage) Remove(_ context.Context, paths []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sorted := slices.Sorted(slices.Values(paths))
	s.ops = append(s.ops, "remove "+strings.Join(sorted, " "))
	return nil
}

// TestRemoveMarked checks that a statement's exchanges are marked as being
// removed before their files are, and the mark removed last, so that a
// removal the coordinator stops is finished by the next sweep.
func TestRemoveMarked(t *testing.T) {
	s := &opStorage{}
	co := &Coordinator{opts: Options{Storage: s}, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	root := "s3://b/wh/_igloo_shuffle/q"
	if err := co.removeMarked(t.Context(), root, []string{root + "/s0/0/__b=0/data_0.parquet", root + "/running"}); err != nil {
		t.Fatal(err)
	}
	want := []string{"write " + root + "/removing", "remove " + root + "/running " + root + "/s0/0/__b=0/data_0.parquet",
		"remove " + root + "/removing"}
	if !slices.Equal(s.ops, want) {
		t.Errorf("did %q, want %q", s.ops, want)
	}
	// Marked already, as a sweep finds it: not written again.
	s.ops = nil
	if err := co.removeMarked(t.Context(), root, []string{root + "/removing", root + "/x"}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(s.ops, []string{"remove " + root + "/x", "remove " + root + "/removing"}) {
		t.Errorf("resuming did %q", s.ops)
	}
}

// A statement's tasks write their exchange files under directories of
// every digit, which a sweep or a removal takes together.
func TestSpreadRoots(t *testing.T) {
	e := &execution{roots: spreadRoots("s3://bucket/warehouse/", "0123abcd")}
	if e.roots[10] != "s3://bucket/warehouse/"+config.ShuffleDir+"/a.0123abcd" {
		t.Errorf("roots %v", e.roots)
	}
	used := map[string]int{}
	for stage := range 3 {
		for task := range 64 {
			used[e.rootOf(stage, task)]++
		}
	}
	if len(used) != spread {
		t.Errorf("192 tasks wrote under %d directories of %d", len(used), spread)
	}
	for root, n := range used {
		if exchangeQID(path.Base(root)) != "0123abcd" || n > 24 {
			t.Errorf("%s: %d tasks", root, n)
		}
	}
}
