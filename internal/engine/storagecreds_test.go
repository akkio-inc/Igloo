package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/config"
)

// fakeS3 serves objects, path-style, to requests signed with an access key
// it accepts, and answers others 403, as S3 answers credentials that have
// expired.
type fakeS3 struct {
	mu      sync.Mutex
	accept  map[string]bool
	objects map[string][]byte
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Authorization: AWS4-HMAC-SHA256 Credential=KEY/date/region/s3/aws4_request, ...
	key, _, _ := strings.Cut(strings.TrimPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential="), "/")
	f.mu.Lock()
	accepted := f.accept[key]
	body, found := f.objects[strings.TrimPrefix(r.URL.Path, "/")]
	f.mu.Unlock()
	switch {
	case !accepted:
		w.WriteHeader(http.StatusForbidden)
		if r.Method != http.MethodHead {
			_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>ExpiredToken</Code>`+
				`<Message>The provided token has expired.</Message></Error>`)
		}
	case !found:
		w.WriteHeader(http.StatusNotFound)
	default:
		w.Header().Set("ETag", `"1"`)
		http.ServeContent(w, r, r.URL.Path, time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC), bytes.NewReader(body))
	}
}

func (f *fakeS3) accepting(keys ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.accept = map[string]bool{}
	for _, k := range keys {
		f.accept[k] = true
	}
}

// useAWSKey sets the credentials of the default chain's first link, the
// environment, as a rotation (IRSA's, say) hands out new ones.
func useAWSKey(t *testing.T, key string) {
	t.Setenv("AWS_ACCESS_KEY_ID", key)
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret-of-"+key)
	t.Setenv("AWS_SESSION_TOKEN", "token-of-"+key)
}

// TestStorageCredentialsRefreshOutsideTransactions rotates the credentials of
// the storage secret's chain, as IRSA does every hour, while sessions keep
// transactions open. DuckDB refreshed the secret itself, when a request
// failed, inside the transaction of the statement that made it, and a
// secret is a catalog entry: of two sessions whose transactions met
// expired credentials, the second failed, as "Catalog write-write conflict
// on alter with "igloo_storage"" (and a commit with "Failed to commit
// Iceberg transaction"). Igloo now replaces the secret ahead of expiry on a
// connection of its own, and DuckDB never does.
func TestStorageCredentialsRefreshOutsideTransactions(t *testing.T) {
	ext := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if ext == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	refreshEvery := storageCredentialsEvery
	storageCredentialsEvery = 20 * time.Millisecond
	t.Cleanup(func() { storageCredentialsEvery = refreshEvery })

	s3 := &fakeS3{objects: map[string][]byte{}}
	for _, name := range []string{"a", "b", "c", "d"} {
		s3.objects["landing/"+name+".csv"] = []byte("n\n1\n2\n3\n")
	}
	srv := httptest.NewServer(s3)
	t.Cleanup(srv.Close)
	t.Setenv("AWS_REGION", "us-east-1")
	useAWSKey(t, "OLDKEY")
	s3.accepting("OLDKEY")

	ctx := context.Background()
	cfg := config.Config{
		Storage: config.Storage{
			Type: config.StorageS3, Location: "s3://warehouse/igloo", AllowedLocations: []string{"s3://landing/"},
			S3: config.S3{Region: "us-east-1", URLStyle: "path", Endpoint: strings.TrimPrefix(srv.URL, "http://")},
		},
		DuckDB: config.DuckDB{ExtensionDir: ext, TempDir: t.TempDir()},
	}
	e, err := OpenWorker(ctx, cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	connect := func() *duckdb.Conn {
		c, err := e.connector.Connect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c.(*duckdb.Conn)
	}
	exec := func(c *duckdb.Conn, q string) error {
		_, err := c.ExecContext(ctx, q, nil)
		return err
	}
	read := func(c *duckdb.Conn, name string) error {
		got, err := queryString(ctx, c, fmt.Sprintf("SELECT count(*)::VARCHAR FROM read_csv('s3://landing/%s.csv')", name))
		if err == nil && got != "3" {
			err = fmt.Errorf("read %s rows, want 3", got)
		}
		return err
	}
	if err := read(connect(), "a"); err != nil {
		t.Fatal(err)
	}

	// New credentials come out; the old ones work until they expire.
	useAWSKey(t, "NEWKEY")
	s3.accepting("OLDKEY", "NEWKEY")
	probe := connect()
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		secret, err := queryString(ctx, probe, "SELECT secret_string FROM duckdb_secrets() WHERE name = 'igloo_storage'")
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(secret, "NEWKEY") {
			break
		}
	}
	s3.accepting("NEWKEY")

	// Two sessions with transactions open meet the expiry.
	first, second := connect(), connect()
	for _, c := range []*duckdb.Conn{first, second} {
		if err := exec(c, "BEGIN"); err != nil {
			t.Fatal(err)
		}
	}
	if err := read(first, "b"); err != nil {
		t.Errorf("the first session: %v", err)
	}
	if err := read(second, "c"); err != nil {
		t.Errorf("the second session: %v", err)
	}
	for _, c := range []*duckdb.Conn{first, second} {
		if err := exec(c, "COMMIT"); err != nil {
			t.Error(err)
		}
	}
	if err := read(connect(), "d"); err != nil {
		t.Errorf("a new session: %v", err)
	}
}
