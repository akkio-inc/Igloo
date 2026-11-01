package engine

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/config"
)

// fakeToken answers with a token that expires in life, or err.
type fakeToken struct {
	life  time.Duration
	err   error
	asked atomic.Int64
	scope atomic.Value
}

func (f *fakeToken) GetToken(_ context.Context, o policy.TokenRequestOptions) (azcore.AccessToken, error) {
	f.asked.Add(1)
	f.scope.Store(strings.Join(o.Scopes, " "))
	if f.err != nil {
		return azcore.AccessToken{}, f.err
	}
	return azcore.AccessToken{Token: "the-token", ExpiresOn: time.Now().Add(f.life)}, nil
}

// TestAzureTokenWait checks that a token is replaced with long to go, and never in a
// hurry.
func TestAzureTokenWait(t *testing.T) {
	now := time.Now()
	for _, c := range []struct {
		left, want time.Duration
	}{
		{24 * time.Hour, 12 * time.Hour},
		{time.Hour, 30 * time.Minute},
		{30 * time.Second, azureTokenRetry},
		{-time.Hour, azureTokenRetry},
	} {
		if got := azureTokenWait(now, now.Add(c.left)); got != c.want {
			t.Errorf("%v left: waits %v, want %v", c.left, got, c.want)
		}
	}
}

// TestAzureTokenSecret checks what DuckDB is given: the token, quoted, for the
// warehouse's account.
func TestAzureTokenSecret(t *testing.T) {
	s := config.Storage{Type: config.StorageAzure, Location: "abfss://warehouse@acct.dfs.core.windows.net/warehouse", Azure: config.Azure{Account: "acct"}}
	got := azureTokenSecret(s, "a'token")
	for _, want := range []string{"TYPE azure", "PROVIDER access_token", "ACCESS_TOKEN 'a''token'", "ACCOUNT_NAME 'acct'", "CREATE OR REPLACE SECRET igloo_storage"} {
		if !strings.Contains(got, want) {
			t.Errorf("%s: without %s", got, want)
		}
	}
}

// TestAzureTokenAfterLockdown checks that DuckDB's secret can be replaced once DuckDB
// is locked down, as it has to be for as long as Igloo runs, and that DuckDB does not
// show the token.
func TestAzureTokenAfterLockdown(t *testing.T) {
	ext := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if ext == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	ctx := t.Context()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close() //nolint:errcheck
	c, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close() //nolint:errcheck
	conn := c.(*duckdb.Conn)
	exec := func(q string) error {
		_, err := conn.ExecContext(ctx, q, nil)
		return err
	}
	for _, q := range []string{"SET extension_directory = " + literal(ext), "SET autoinstall_known_extensions = false", "LOAD azure"} {
		if err := exec(q); err != nil {
			t.Fatal(err)
		}
	}
	e := &Engine{connector: connector}
	cfg := config.Config{Storage: config.Storage{Type: config.StorageAzure, Location: "abfss://warehouse@acct.dfs.core.windows.net/warehouse", Azure: config.Azure{Account: "acct"}}}
	if err := e.lockDown(exec, cfg, nil); err != nil {
		t.Fatal(err)
	}
	for _, token := range []string{"first-token", "second-token"} {
		if err := exec(azureTokenSecret(cfg.Storage, token)); err != nil {
			t.Fatal(err)
		}
		got, err := queryString(ctx, conn, "SELECT secret_string FROM duckdb_secrets() WHERE name = 'igloo_storage'")
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(got, token) || !strings.Contains(got, "acct") {
			t.Errorf("DuckDB's secret shows: %s", got)
		}
	}
}

// TestStartAzureTokens checks when Igloo asks for a token and that it stops asking,
// that a warehouse with an account key or elsewhere never does, and that not getting
// one is no failure, and is tried again.
func TestStartAzureTokens(t *testing.T) {
	old := newAzureCredential
	t.Cleanup(func() { newAzureCredential = old })
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close() //nolint:errcheck
	log := slog.New(slog.DiscardHandler)

	fake := &fakeToken{err: errors.New("no identity here")}
	newAzureCredential = func() (azcore.TokenCredential, error) { return fake, nil }
	e := &Engine{connector: connector}
	azure := config.Config{Storage: config.Storage{Type: config.StorageAzure, Azure: config.Azure{Account: "acct"}}}
	e.startAzureTokens(t.Context(), azure, log)
	if n := fake.asked.Load(); n != 1 {
		t.Errorf("asked for %d tokens", n)
	}
	if got := fake.scope.Load(); got != azureStorageScope {
		t.Errorf("asked for a token of %v", got)
	}
	if e.stopAzureTokens == nil {
		t.Fatal("it does not go on asking")
	}
	e.stopAzureTokens()

	fake = &fakeToken{}
	for name, cfg := range map[string]config.Config{
		"an account key": {Storage: config.Storage{Type: config.StorageAzure, Azure: config.Azure{Account: "acct", AccountKey: "key"}}},
		"S3":             {Storage: config.Storage{Type: config.StorageS3}},
	} {
		e := &Engine{connector: connector}
		e.startAzureTokens(t.Context(), cfg, log)
		if fake.asked.Load() != 0 || e.stopAzureTokens != nil {
			t.Errorf("%s: asked for a token", name)
		}
	}

	newAzureCredential = func() (azcore.TokenCredential, error) { return nil, errors.New("none") }
	e = &Engine{connector: connector}
	e.startAzureTokens(t.Context(), azure, log)
	if e.stopAzureTokens != nil {
		t.Error("goes on without a credential")
	}
}
