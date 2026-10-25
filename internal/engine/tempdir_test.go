//go:build linux || darwin

package engine

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/config"
)

// spillChildEnv, when set to a directory, makes
// TestOpenDeletesAKilledProcessesSpillFiles spill there and kill itself
// rather than spawn a subprocess to.
const spillChildEnv = "IGLOO_SPILL_CHILD_DIR"

func TestClaimTempDir(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, size int) {
		t.Helper()
		b := make([]byte, size)
		_, _ = rand.Read(b)
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	spill := map[string]int{
		"duckdb_temp_storage_DEFAULT-0.tmp":           256 << 10,
		"duckdb_temp_storage_S128K-3.tmp":             128 << 10,
		"duckdb_temp_block-4611686018427388160.block": 300 << 10,
	}
	var spilled int
	for name, size := range spill {
		write(name, size)
		spilled += size
	}
	// A sorted write's rows, and its directory, go too.
	sortedRows := filepath.Join(SortedWriteDirPrefix+"0a1b", "__igloo_range=0", "data_0.parquet")
	if err := os.MkdirAll(filepath.Join(dir, filepath.Dir(sortedRows)), 0o700); err != nil {
		t.Fatal(err)
	}
	write(sortedRows, 200<<10)
	spilled += 200 << 10
	// Everything else stays: other files, directories (the distributed
	// executor's among them) and what is in them, and links.
	for _, name := range []string{"keep.txt", "duckdb.db", "igloo-worker/duckdb_temp_storage_DEFAULT-0.tmp"} {
		if err := os.MkdirAll(filepath.Join(dir, filepath.Dir(name)), 0o700); err != nil {
			t.Fatal(err)
		}
		write(name, 10)
	}
	if err := os.Mkdir(filepath.Join(dir, "duckdb_temp_block-dir"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(dir, "keep.txt"), filepath.Join(dir, "duckdb_temp_block-link.block")); err != nil {
		t.Fatal(err)
	}
	kept := []string{"duckdb.db", "duckdb_temp_block-dir", "duckdb_temp_block-link.block", "igloo-worker", tempLockName, "keep.txt"}

	var logged bytes.Buffer
	release := claimTempDir(dir, slog.New(slog.NewTextHandler(&logged, nil)))
	if got := names(t, dir); !slices.Equal(got, kept) {
		t.Errorf("left %q, want %q", got, kept)
	}
	if _, err := os.Stat(filepath.Join(dir, "igloo-worker", "duckdb_temp_storage_DEFAULT-0.tmp")); err != nil {
		t.Errorf("a file in a subdirectory went: %v", err)
	}
	m := regexp.MustCompile(`files=4 bytes_freed=(\d+)`).FindStringSubmatch(logged.String())
	if m == nil {
		t.Fatalf("logged %q", logged.String())
	}
	if freed, _ := strconv.Atoi(m[1]); freed < spilled {
		t.Errorf("logged %d bytes freed, want at least %d", freed, spilled)
	}

	// While this process uses the directory, another leaves its files.
	write("duckdb_temp_storage_DEFAULT-0.tmp", 4096)
	logged.Reset()
	other := claimTempDir(dir, slog.New(slog.NewTextHandler(&logged, nil)))
	if _, err := os.Stat(filepath.Join(dir, "duckdb_temp_storage_DEFAULT-0.tmp")); err != nil {
		t.Errorf("deleted a live process's file: %v", err)
	}
	if !strings.Contains(logged.String(), "another process is using the spill directory") {
		t.Errorf("logged %q", logged.String())
	}

	// Once neither holds it, the next one deletes them.
	other()
	release()
	logged.Reset()
	claimTempDir(dir, slog.New(slog.NewTextHandler(&logged, nil)))()
	if _, err := os.Stat(filepath.Join(dir, "duckdb_temp_storage_DEFAULT-0.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("file of an ended process left: %v", err)
	}
	if !strings.Contains(logged.String(), "files=1") {
		t.Errorf("logged %q", logged.String())
	}

	// A directory that does not exist yet is created, with the lock in it.
	missing := filepath.Join(dir, "a", "tmp")
	claimTempDir(missing, slog.New(slog.NewTextHandler(&logged, nil)))()
	if _, err := os.Stat(filepath.Join(missing, tempLockName)); err != nil {
		t.Error(err)
	}

	// Without a directory, or one that cannot be made, nothing happens.
	claimTempDir("", nil)()
	logged.Reset()
	claimTempDir(filepath.Join(dir, "keep.txt", "tmp"), slog.New(slog.NewTextHandler(&logged, nil)))()
	if !strings.Contains(logged.String(), "cannot lock the spill directory") {
		t.Errorf("logged %q", logged.String())
	}
}

// TestOpenDeletesAKilledProcessesSpillFiles has a subprocess spill and kill
// itself, as the kernel or a container runtime kills Igloo, and checks that
// the next engine to open deletes the files it left, and only those.
func TestOpenDeletesAKilledProcessesSpillFiles(t *testing.T) {
	if dir := os.Getenv(spillChildEnv); dir != "" {
		e := spillingEngine(t, dir)
		spillTable(t, e)
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		time.Sleep(time.Minute)
		return
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestOpenDeletesAKilledProcessesSpillFiles$", "-test.timeout=2m")
	cmd.Env = append(os.Environ(), spillChildEnv+"="+dir)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("the subprocess was not killed: %v\n%s", err, out)
	}
	left := names(t, dir)
	if !slices.ContainsFunc(left, isTempFile) {
		t.Fatalf("the subprocess left no spill files: %q\n%s", left, out)
	}
	// Every file DuckDB 1.5 spills to has a name the cleanup knows.
	for _, name := range left {
		if name != tempLockName && !isTempFile(name) {
			t.Errorf("DuckDB left %s, a file the cleanup does not know", name)
		}
	}

	var logged bytes.Buffer
	e, err := Open(context.Background(), config.Config{DuckDB: config.DuckDB{TempDir: dir}}, slog.New(slog.NewTextHandler(&logged, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close() //nolint:errcheck
	if got := names(t, dir); !slices.Equal(got, []string{tempLockName}) {
		t.Errorf("left %q", got)
	}
	if !strings.Contains(logged.String(), "deleted DuckDB temporary files an earlier run left") {
		t.Errorf("logged %q", logged.String())
	}
}

// TestOpenLeavesALiveEnginesSpillFiles opens a second engine on the spill
// directory of one that has spilled, and checks that the first one's data
// is still there to read.
func TestOpenLeavesALiveEnginesSpillFiles(t *testing.T) {
	dir := t.TempDir()
	first := spillingEngine(t, dir)
	spillTable(t, first)
	spilled := names(t, dir)
	if !slices.ContainsFunc(spilled, isTempFile) {
		t.Fatalf("DuckDB did not spill: %q", spilled)
	}

	var logged bytes.Buffer
	second, err := Open(context.Background(), config.Config{DuckDB: config.DuckDB{TempDir: dir}}, slog.New(slog.NewTextHandler(&logged, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if got := names(t, dir); !slices.Equal(got, spilled) {
		t.Errorf("opening a second engine changed the spill directory from %q to %q", spilled, got)
	}
	if !strings.Contains(logged.String(), "another process is using the spill directory") {
		t.Errorf("logged %q", logged.String())
	}
	ctx := context.Background()
	conn, err := first.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, err := queryString(ctx, conn, "SELECT sum(i)::VARCHAR || ' ' || sum(length(h))::VARCHAR FROM spilled")
	if want := "7999998000000 128000000"; err != nil || got != want {
		t.Errorf("read back %q from the spilled table, want %q: %v", got, want, err)
	}

	if err := errors.Join(conn.Close(), second.Close(), first.Close()); err != nil {
		t.Fatal(err)
	}
}

// TestCloseDeletesSpillFiles checks that closing an engine closes DuckDB's
// database, which deletes its spill files, so a replica that stops cleanly
// leaves none. Connections the engine opened for itself and kept, such as
// those it registers its table functions through, would keep the database
// open.
func TestCloseDeletesSpillFiles(t *testing.T) {
	dir := t.TempDir()
	e := spillingEngine(t, dir)
	spillTable(t, e)
	if spilled := names(t, dir); !slices.ContainsFunc(spilled, isTempFile) {
		t.Fatalf("DuckDB did not spill: %q", spilled)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if got := names(t, dir); !slices.Equal(got, []string{tempLockName}) {
		t.Errorf("closing the engine left %q", got)
	}
}

// spillingEngine opens an engine with too little memory for spillTable's
// table.
func spillingEngine(t *testing.T, dir string) *Engine {
	t.Helper()
	cfg := config.Config{DuckDB: config.DuckDB{TempDir: dir, MemoryLimit: "64MB", Threads: 2}}
	e, err := Open(context.Background(), cfg, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// spillTable creates a table of about 200 MB in DuckDB's own, in-memory
// database, which DuckDB keeps in its spill directory past its memory
// limit.
func spillTable(t *testing.T, e *Engine) {
	t.Helper()
	ctx := context.Background()
	conn, err := e.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	if _, err := conn.ExecContext(ctx, "CREATE TABLE spilled AS SELECT i, md5(i::VARCHAR) AS h FROM range(4000000) t(i)", nil); err != nil {
		t.Fatal(err)
	}
}

func names(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}
