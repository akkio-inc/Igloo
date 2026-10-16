package engine

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/akkio-inc/igloo/internal/config"
)

const gib = 1 << 30

func TestCacheLimits(t *testing.T) {
	cases := []struct {
		name                        string
		total, avail, used, maxSize uint64
		wantLimit, wantReserve      uint64
	}{
		{"half of the free space", 100 * gib, 60 * gib, 0, 0, 30 * gib, 30 * gib},
		{"the same after the cache filled up", 100 * gib, 30 * gib, 30 * gib, 0, 30 * gib, 30 * gib},
		{"less once other files grow", 100 * gib, 10 * gib, 30 * gib, 0, 20 * gib, 20 * gib},
		{"at most 100 GiB", 2048 * gib, 1500 * gib, 0, 0, 100 * gib, 1400 * gib},
		{"nothing on a nearly full disk", 100 * gib, 4 * gib, 0, 0, 0, 5 * gib},
		{"IGLOO_CACHE_SIZE", 100 * gib, 60 * gib, 0, 10 * gib, 10 * gib, 50 * gib},
		{"IGLOO_CACHE_SIZE above the 100 GiB cap", 2048 * gib, 1500 * gib, 0, 500 * gib, 500 * gib, 1000 * gib},
		{"IGLOO_CACHE_SIZE never fills the disk", 100 * gib, 60 * gib, 0, 1024 * gib, 55 * gib, 5 * gib},
	}
	for _, c := range cases {
		limit, reserve := cacheLimits(c.total, c.avail, c.used, c.maxSize)
		if limit != c.wantLimit || reserve != c.wantReserve {
			t.Errorf("%s: got limit %d GiB, reserve %d GiB; want %d GiB, %d GiB",
				c.name, limit/gib, reserve/gib, c.wantLimit/gib, c.wantReserve/gib)
		}
	}
}

func TestCacheStatements(t *testing.T) {
	stmts := cacheStatements("/var/lib/igloo/cache/blocks", "s3://my.bucket/ware'house/", 42, true)
	got := joined(stmts)
	for _, want := range []string{
		"SET GLOBAL cache_httpfs_cache_directory = '/var/lib/igloo/cache/blocks'",
		"SET GLOBAL cache_httpfs_min_disk_bytes_for_cache = 42",
		"SET GLOBAL cache_httpfs_enable_cache_validation = true",
		"SET GLOBAL cache_httpfs_evict_policy = 'lru_sp'",
		"SET GLOBAL enable_external_file_cache = true",
		`SELECT cache_httpfs_add_exclusion_regex('^(?!s3://my\.bucket/ware''house/)')`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in\n%s", want, got)
		}
	}
	// Whatever fails later, listings, handles and sizes are never cached.
	for i, stmt := range cacheOffStatements()[1:] {
		if stmts[i] != stmt {
			t.Errorf("statement %d is %q, want %q", i, stmts[i], stmt)
		}
	}
	if last := stmts[len(stmts)-1]; last != "SET GLOBAL cache_httpfs_type = 'on_disk'" {
		t.Errorf("the cache should be switched on last, not %q", last)
	}
	// On a full disk the extension is configured but left passing reads
	// through.
	stmts = cacheStatements("/c", "s3://b/w/", 42, false)
	if last := stmts[len(stmts)-1]; last != "SET GLOBAL cache_httpfs_type = 'noop'" {
		t.Errorf("a paused cache should end in noop, not %q", last)
	}
}

func TestCacheAdjustments(t *testing.T) {
	c := &readCache{reserve: 10, on: true}
	if got := c.adjustments(10, true, 5); len(got) != 0 {
		t.Errorf("nothing changed, yet %q", got)
	}
	if got := c.adjustments(20, true, 5); !slices.Equal(got, []string{"SET GLOBAL cache_httpfs_min_disk_bytes_for_cache = 20"}) {
		t.Errorf("new reserve: %q", got)
	}
	pause := []string{"SET GLOBAL cache_httpfs_type = 'noop'", "SELECT cache_httpfs_clear_cache()"}
	if got := c.adjustments(10, false, 5); !slices.Equal(got, pause) {
		t.Errorf("full disk: %q", got)
	}

	c.on = false
	if got := c.adjustments(10, false, 0); len(got) != 0 {
		t.Errorf("paused and empty, yet %q", got)
	}
	if got := c.adjustments(10, false, 5); !slices.Equal(got, pause) {
		t.Errorf("paused at start with blocks left from before: %q", got)
	}
	if got := c.adjustments(10, true, 0); !slices.Equal(got, []string{"SET GLOBAL cache_httpfs_type = 'on_disk'"}) {
		t.Errorf("room again: %q", got)
	}
}

// recorder stands in for DuckDB, failing the statements in fail.
type recorder struct {
	ran  []string
	fail []string
}

func (r *recorder) exec(stmt string) error {
	r.ran = append(r.ran, stmt)
	for _, f := range r.fail {
		if strings.HasPrefix(stmt, f) {
			return errors.New("refused")
		}
	}
	return nil
}

func s3Config(dir string) config.Config {
	return config.Config{
		Cache:   config.Cache{Dir: dir},
		Storage: config.Storage{Type: config.StorageS3, Location: "s3://bucket/warehouse"},
	}
}

func TestSetupCache(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()

	e, r := &Engine{}, &recorder{}
	if err := e.setupCache(r.exec, s3Config(dir), log); err != nil {
		t.Fatal(err)
	}
	if e.cache == nil || e.cache.dir != filepath.Join(dir, "blocks") {
		t.Fatalf("cache not set up in %s: %+v", dir, e.cache)
	}
	if r.ran[0] != "LOAD cache_httpfs" || !slices.Equal(r.ran[1:], cacheStatements(e.cache.dir, "s3://bucket/warehouse/", e.cache.reserve, e.cache.on)) {
		t.Errorf("ran %q", r.ran)
	}
	if entries, _ := os.ReadDir(e.cache.dir); len(entries) != 0 {
		t.Errorf("the writability probe was left behind: %v", entries)
	}

	off := []struct {
		name string
		cfg  config.Config
	}{
		{"no directory", s3Config("")},
		{"Azure", func() config.Config { c := s3Config(dir); c.Storage.Type = config.StorageAzure; return c }()},
		{"unwritable directory", s3Config(filepath.Join(dir, "blocks", "not-a-dir", "x"))},
	}
	if err := os.WriteFile(filepath.Join(dir, "blocks", "not-a-dir"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, c := range off {
		e, r := &Engine{}, &recorder{}
		if err := e.setupCache(r.exec, c.cfg, log); err != nil || e.cache != nil || len(r.ran) != 0 {
			t.Errorf("%s: cache %+v, ran %q, err %v", c.name, e.cache, r.ran, err)
		}
	}

	// Without the extension, Igloo runs uncached.
	e, r = &Engine{}, &recorder{fail: []string{"LOAD"}}
	if err := e.setupCache(r.exec, s3Config(dir), log); err != nil || e.cache != nil || len(r.ran) != 1 {
		t.Errorf("failed load: cache %+v, ran %q, err %v", e.cache, r.ran, err)
	}

	// A setting the extension rejects turns it into plain httpfs.
	e, r = &Engine{}, &recorder{fail: []string{"SET GLOBAL cache_httpfs_evict_policy"}}
	if err := e.setupCache(r.exec, s3Config(dir), log); err != nil || e.cache != nil {
		t.Errorf("failed setting: cache %+v, err %v", e.cache, err)
	}
	if tail := r.ran[len(r.ran)-len(cacheOffStatements()):]; !slices.Equal(tail, cacheOffStatements()) {
		t.Errorf("cache not turned off after a failed setting: %q", tail)
	}

	// If it cannot even be turned off, Igloo must not start with it.
	e, r = &Engine{}, &recorder{fail: []string{"SET GLOBAL cache_httpfs_enable_glob_cache"}}
	if err := e.setupCache(r.exec, s3Config(dir), log); err == nil {
		t.Error("an extension stuck caching listings must stop startup")
	}
}

func TestDirBytes(t *testing.T) {
	dir := t.TempDir()
	for name, size := range map[string]int{"a": 100, "b": 23} {
		if err := os.WriteFile(filepath.Join(dir, name), make([]byte, size), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "lost+found"), 0o700); err != nil {
		t.Fatal(err)
	}
	if got := dirBytes(dir); got != 123 {
		t.Errorf("got %d bytes, want 123", got)
	}
	if got := dirBytes(filepath.Join(dir, "missing")); got != 0 {
		t.Errorf("missing directory: got %d", got)
	}
}

func TestDiskSpace(t *testing.T) {
	total, avail, err := diskSpace(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if total == 0 || avail > total {
		t.Errorf("total %d, available %d", total, avail)
	}
}

func TestCacheMetrics(t *testing.T) {
	e := &Engine{}
	reg := prometheus.NewRegistry()
	reg.MustRegister(e.Metrics()...)
	if n, err := testutil.GatherAndCount(reg, "igloo_cache_size_bytes", "igloo_cache_limit_bytes"); err != nil || n != 2 {
		t.Fatalf("got %d metrics, %v", n, err)
	}

	e.cache = &readCache{}
	e.cache.size.Store(7)
	e.cache.limit.Store(9)
	want := `
# HELP igloo_cache_size_bytes Bytes of object storage data cached on local disk, measured every 5 minutes.
# TYPE igloo_cache_size_bytes gauge
igloo_cache_size_bytes 7
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "igloo_cache_size_bytes"); err != nil {
		t.Error(err)
	}
}
