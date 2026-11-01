package engine

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/memlimit"
)

func TestOpenWithoutCatalog(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close() //nolint:errcheck

	if err := e.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	conn, err := e.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck

	rows, err := conn.QueryContext(ctx, "SELECT 40 + 2", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close() //nolint:errcheck

	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatal(err)
	}
	if dest[0] != int32(42) {
		t.Fatalf("got %v (%T), want 42", dest[0], dest[0])
	}
}

func joined(stmts []string) string { return strings.Join(stmts, ";\n") }

func TestS3WithKeys(t *testing.T) {
	got := joined(storageStatements(config.Storage{
		Type: config.StorageS3,
		S3: config.S3{
			Region: "us-east-1", URLStyle: "path", Endpoint: "storage:9000",
			AccessKeyID: "key", SecretAccessKey: "it's",
		},
	}))
	for _, want := range []string{"TYPE s3", "ENDPOINT 'storage:9000'", "SECRET 'it''s'", "USE_SSL false"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in\n%s", want, got)
		}
	}
	if strings.Contains(got, "credential_chain") {
		t.Error("static keys should not use the credential chain")
	}
}

func TestS3WithoutKeysUsesCredentialChain(t *testing.T) {
	got := joined(storageStatements(config.Storage{
		Type: config.StorageS3,
		S3:   config.S3{Region: "us-west-2", URLStyle: "vhost", UseSSL: true},
	}))
	for _, want := range []string{"LOAD aws", "PROVIDER credential_chain", "REFRESH auto", "USE_SSL true"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in\n%s", want, got)
		}
	}
}

func TestGCSUsesHMACKey(t *testing.T) {
	got := joined(storageStatements(config.Storage{
		Type: config.StorageGCS,
		GCS:  config.GCS{HMACKeyID: "id", HMACSecret: "secret"},
	}))
	for _, want := range []string{"TYPE gcs", "KEY_ID 'id'", "SECRET 'secret'"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in\n%s", want, got)
		}
	}
}

func TestAzure(t *testing.T) {
	az := config.Azure{Account: "acct", Host: "acct.dfs.core.windows.net"}

	got := joined(storageStatements(config.Storage{Type: config.StorageAzure, Azure: az}))
	for _, want := range []string{"LOAD azure", "PROVIDER credential_chain", "workload_identity", "ACCOUNT_NAME 'acct'"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in\n%s", want, got)
		}
	}

	az.AccountKey = "k=="
	got = joined(storageStatements(config.Storage{Type: config.StorageAzure, Azure: az}))
	want := "AccountName=acct;AccountKey=k==;EndpointSuffix=core.windows.net"
	if !strings.Contains(got, want) {
		t.Errorf("missing %q in\n%s", want, got)
	}
}

func TestEveryExtensionLoadsBeforeLockdown(t *testing.T) {
	// LOAD reads extension files, which lockdown forbids, so every
	// extension a query might need has to be loaded up front.
	got := joined(storageStatements(config.Storage{Type: config.StorageS3}))
	for _, ext := range []string{"httpfs", "iceberg", "avro", "tpch", "spatial", "h3", "datasketches"} {
		if !strings.Contains(got, "LOAD "+ext) {
			t.Errorf("%s is not loaded", ext)
		}
	}
}

func TestStorageCredentialsCoverOnlyIgloosLocations(t *testing.T) {
	// Tables of Iceberg REST catalogs attached as connections are never
	// read with Igloo's own credentials, wherever they are.
	for _, s := range []config.Storage{
		{Type: config.StorageS3, Location: "s3://bucket/warehouse", AllowedLocations: []string{"s3://landing/"}, S3: config.S3{AccessKeyID: "k"}},
		{Type: config.StorageS3, Location: "s3://bucket/warehouse", AllowedLocations: []string{"s3://landing"}},
		{Type: config.StorageGCS, Location: "s3://bucket/warehouse", AllowedLocations: []string{"s3://landing/"}},
		{Type: config.StorageAzure, Location: "s3://bucket/warehouse", AllowedLocations: []string{"s3://landing/"}, Azure: config.Azure{Account: "a", AccountKey: "k"}},
	} {
		got := joined(storageStatements(s))
		if want := "SCOPE ['s3://bucket/warehouse/', 's3://landing/'])"; !strings.Contains(got, want) {
			t.Errorf("%s: missing %q in\n%s", s.Type, want, got)
		}
	}
}

func TestLockdown(t *testing.T) {
	got := joined(lockdownStatements(config.Config{
		DuckDB: config.DuckDB{TempDir: "/var/lib/igloo/tmp"},
		Storage: config.Storage{
			Location:         "s3://bucket/warehouse",
			AllowedLocations: []string{"s3://landing/"},
		},
	}, "/var/lib/igloo/cache/blocks", "s3://remote/tables/t1/"))
	want := "SET allowed_directories = ['s3://bucket/warehouse/', 's3://landing/', '/var/lib/igloo/tmp/', '/var/lib/igloo/cache/blocks/', 's3://remote/tables/t1/']"
	if !strings.Contains(got, want) {
		t.Errorf("missing %q in\n%s", want, got)
	}
	if !strings.HasSuffix(got, "SET enable_external_access = false") {
		t.Errorf("external access must be disabled last:\n%s", got)
	}
}

func TestAttachStatementQuotesNames(t *testing.T) {
	got := attachStatement(config.Catalog{URI: "http://catalog:8181/catalog", Warehouse: "wh", Name: `my"lake`}, "http://127.0.0.1:4321/abc")
	want := `ATTACH 'wh' AS "my""lake" (TYPE iceberg, ENDPOINT 'http://127.0.0.1:4321/abc', AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none', PURGE_REQUESTED false, REMOVE_FILES_ON_DELETE false)`
	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestCountStatements(t *testing.T) {
	e, err := Open(context.Background(), config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close() //nolint:errcheck

	cases := map[string]int{
		"SELECT 1":                            1,
		"SELECT 1;":                           1,
		"SELECT 1; SELECT 2":                  2,
		"SELECT 'a;b'; -- c;d\nSELECT 2":      2,
		"CREATE SECRET s (TYPE s3); SELECT 1": 2,
	}
	for q, want := range cases {
		got, err := e.CountStatements(q)
		if err != nil || got != want {
			t.Errorf("%q: got %d, %v; want %d", q, got, err, want)
		}
	}
	if _, err := e.CountStatements("SELEC 1"); err == nil {
		t.Error("syntax error not reported")
	}

	// Counting must never run anything.
	if _, err := e.CountStatements("CREATE TABLE should_not_exist (i INTEGER)"); err != nil {
		t.Fatal(err)
	}
	conn, err := e.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	if _, err := conn.ExecContext(context.Background(), "SELECT * FROM should_not_exist", nil); err == nil {
		t.Error("counting statements created a table")
	}
}

func TestLockdownWithoutCatalog(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close() //nolint:errcheck
	conn, err := e.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck

	for _, q := range []string{
		"SELECT * FROM read_text('/etc/passwd')",
		"SELECT * FROM read_csv('http://example.com/x.csv')",
		"SET enable_external_access = true",
	} {
		if _, err := conn.ExecContext(ctx, q, nil); err == nil {
			t.Errorf("%s: should be blocked", q)
		}
	}
}

func TestLockdownNeverAllowsRoot(t *testing.T) {
	got := joined(lockdownStatements(config.Config{}, ""))
	if strings.Contains(got, "'/'") {
		t.Errorf("an empty warehouse location must not allow /:\n%s", got)
	}
}

func TestSettingStatements(t *testing.T) {
	got := joined(settingStatements(config.DuckDB{MemoryLimit: "6GB", MaxTempSize: "45GiB", Threads: 4}))
	for _, want := range []string{"SET memory_limit = '6GB'", "SET max_temp_directory_size = '45GiB'", "SET threads = 4"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in\n%s", want, got)
		}
	}
}

// TestThreadsFor checks DuckDB's threads: several a core when the
// warehouse is in object storage, as long as each thread past the cores
// keeps minThreadMemory, never fewer than the cores, unless
// IGLOO_DUCKDB_THREADS says otherwise; and DuckDB's own default without a
// warehouse.
func TestThreadsFor(t *testing.T) {
	s3 := config.Storage{Type: config.StorageS3}
	cores := runtime.GOMAXPROCS(0)
	for _, c := range []struct {
		name   string
		cfg    config.Config
		memory int64
		want   int
	}{
		{"memory unknown", config.Config{Storage: s3}, 0, 4 * cores},
		{"plenty of memory", config.Config{Storage: s3}, int64(4*cores) * minThreadMemory * 2, 4 * cores},
		{"memory for twice the cores", config.Config{Storage: s3}, int64(2*cores) * minThreadMemory, 2 * cores},
		{"memory for a thread and a half a core", config.Config{Storage: s3}, int64(3*cores) * minThreadMemory / 2, 3 * cores / 2},
		{"little memory", config.Config{Storage: s3}, minThreadMemory, cores},
		{"set", config.Config{Storage: s3, DuckDB: config.DuckDB{Threads: 3}}, minThreadMemory, 3},
		{"no warehouse", config.Config{}, minThreadMemory, 0},
	} {
		if got := threadsFor(c.cfg, 4, c.memory); got != c.want {
			t.Errorf("%s: %d threads, want %d", c.name, got, c.want)
		}
	}
}

// TestConfigureThreads checks that DuckDB's threads are set after its other
// settings, from the memory limit they leave.
func TestConfigureThreads(t *testing.T) {
	cores := runtime.GOMAXPROCS(0)
	cfg := config.Config{Storage: config.Storage{Type: config.StorageS3}, DuckDB: config.DuckDB{MemoryLimit: "6GB"}}
	var stmts []string
	exec := func(s string) error { stmts = append(stmts, s); return nil }
	// Three GiB a core: a thread and a half a core at two GiB a thread.
	query := func(string) (string, error) { return fmt.Sprintf("%d GiB", 3*cores), nil }
	if err := configure(cfg, serverThreadsPerCore, exec, query); err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("SET threads = %d", 3*cores/2)
	if len(stmts) < 2 || !strings.HasPrefix(stmts[0], "SET memory_limit") || stmts[len(stmts)-1] != want {
		t.Errorf("statements %q, want the memory limit first and %q last", stmts, want)
	}
}

// TestConfigureMemory checks DuckDB's memory limit where none is
// configured: memlimit.DuckDBLimit of the process's memory, not DuckDB's
// 80% of the machine; DuckDB's own where the memory is not known; and the
// configured one where there is one.
func TestConfigureMemory(t *testing.T) {
	defer func(f func() int64) { processMemory = f }(processMemory)
	limit := func(cfg config.Config) string {
		t.Helper()
		var set string
		exec := func(s string) error {
			if v, ok := strings.CutPrefix(s, "SET memory_limit = "); ok {
				set = v
			}
			return nil
		}
		if err := configure(cfg, serverThreadsPerCore, exec, func(string) (string, error) { return "", errors.New("none") }); err != nil {
			t.Fatal(err)
		}
		return set
	}
	processMemory = func() int64 { return 66_169_339_904 }
	if got, want := limit(config.Config{}), fmt.Sprintf("'%dMiB'", memlimit.DuckDBLimit(66_169_339_904)>>20); got != want || want != "'46276MiB'" {
		t.Errorf("61.6 GiB: memory_limit %s, want %s", got, want)
	}
	if got := limit(config.Config{DuckDB: config.DuckDB{MemoryLimit: "6GB"}}); got != "'6GB'" {
		t.Errorf("configured: memory_limit %s", got)
	}
	processMemory = func() int64 { return 0 }
	if got := limit(config.Config{}); got != "" {
		t.Errorf("memory not known: memory_limit %s", got)
	}
}

func TestCatalogShareContext(t *testing.T) {
	var e Engine
	if e.CatalogLockedFor() != 0 {
		t.Error("an unlocked catalog counts as locked")
	}
	unlock := e.CatalogLock()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := e.CatalogShareContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shared while held exclusively: %v", err)
	}
	if held := e.CatalogLockedFor(); held < 50*time.Millisecond {
		t.Errorf("held for %s", held)
	}
	// A login waiting for it gets it once it is released.
	got := make(chan error, 1)
	go func() {
		release, err := e.CatalogShareContext(context.Background())
		if err == nil {
			release()
		}
		got <- err
	}()
	time.Sleep(20 * time.Millisecond)
	unlock()
	select {
	case err := <-got:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("still waiting after the lock was released")
	}
	if e.CatalogLockedFor() != 0 {
		t.Error("a released catalog lock counts as held")
	}
}
