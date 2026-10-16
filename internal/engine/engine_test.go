package engine

import (
	"context"
	"database/sql/driver"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/akkio-inc/igloo/internal/config"
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

func TestLockdown(t *testing.T) {
	got := joined(lockdownStatements(config.Config{
		DuckDB: config.DuckDB{TempDir: "/var/lib/igloo/tmp"},
		Storage: config.Storage{
			Location:         "s3://bucket/warehouse",
			AllowedLocations: []string{"s3://landing/"},
		},
	}, "/var/lib/igloo/cache/blocks"))
	want := "SET allowed_directories = ['s3://bucket/warehouse/', 's3://landing/', '/var/lib/igloo/tmp/', '/var/lib/igloo/cache/blocks/']"
	if !strings.Contains(got, want) {
		t.Errorf("missing %q in\n%s", want, got)
	}
	if !strings.HasSuffix(got, "SET enable_external_access = false") {
		t.Errorf("external access must be disabled last:\n%s", got)
	}
}

func TestAttachStatementQuotesNames(t *testing.T) {
	got := attachStatement(config.Catalog{URI: "http://catalog:8181/catalog", Warehouse: "wh", Name: `my"lake`})
	want := `ATTACH 'wh' AS "my""lake" (TYPE iceberg, ENDPOINT 'http://catalog:8181/catalog', AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none', PURGE_REQUESTED false)`
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
