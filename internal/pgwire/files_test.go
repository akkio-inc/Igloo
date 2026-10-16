package pgwire

import (
	"context"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
)

// fileHTTP starts a local HTTP server that records the raw request paths it
// is asked for and serves a body to any of them.
type fileHTTP struct {
	base  string
	mu    sync.Mutex
	paths []string
}

func startFileHTTP(t *testing.T) *fileHTTP {
	t.Helper()
	f := &fileHTTP{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.paths = append(f.paths, r.URL.RequestURI())
		f.mu.Unlock()
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("leaked-body\n"))
	})}
	go srv.Serve(ln) //nolint:errcheck
	t.Cleanup(func() { _ = srv.Close() })
	f.base = "http://" + ln.Addr().String()
	return f
}

func (f *fileHTTP) got() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.paths...)
}

func (f *fileHTTP) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.paths = nil
}

// TestFileTraversalNeverReachesTheServer is the heart of the fix: a
// non-admin, with no table under any policy, cannot make DuckDB fetch a URL
// that climbs out of an allowed location. DuckDB 1.5.6 would, because it
// prefix-matches the raw path; Igloo refuses the statement first, so the
// server never sees the climbed-to path.
func TestFileTraversalNeverReachesTheServer(t *testing.T) {
	ext := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if ext == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set; run igloo install-extensions into it")
	}
	h := startFileHTTP(t)
	allowed := h.base + "/allowed/"
	cs := startCatalogServerWithConfig(t,
		config.Config{Storage: config.Storage{AllowedLocations: []string{allowed}}, DuckDB: config.DuckDB{ExtensionDir: ext}},
		func(o *Options) { o.AllowedLocations = []string{allowed} })
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)

	refusedNoFetch := func(sql, mustNotFetch string) {
		t.Helper()
		h.reset()
		_, err := rita.Exec(context.Background(), sql)
		if pgCode(err) != "42501" {
			t.Errorf("%s: want 42501, got %v", sql, err)
		}
		for _, p := range h.got() {
			if strings.Contains(p, mustNotFetch) {
				t.Errorf("%s: the server was asked for %q (fetched %v)", sql, p, h.got())
			}
		}
	}
	// Each of these would reach /other.txt or another host on DuckDB alone.
	refusedNoFetch("SELECT * FROM read_text('"+h.base+"/allowed/../other.txt')", "other.txt")
	refusedNoFetch("SELECT * FROM read_text('"+h.base+"/allowed/x/../../other.txt')", "other.txt")
	refusedNoFetch("SELECT * FROM read_text('"+h.base+"/allowed/%2e%2e/other.txt')", "other")
	refusedNoFetch("SELECT * FROM '"+h.base+"/allowed/../other.csv'", "other.csv")
	refusedNoFetch(`SELECT * FROM "read_text"('`+h.base+`/allowed/../q.txt')`, "q.txt")
	refusedNoFetch("SELECT * FROM read_text('"+h.base+"/allowed/' || '..' || '/c.txt')", "c.txt")

	// A legitimate read of a file inside the allowed location is served, so
	// the check does not block ordinary use. The file does not exist, so the
	// read fails with a 404, not a permission error.
	h.reset()
	_, err := rita.Exec(context.Background(), "SELECT * FROM read_text('"+h.base+"/allowed/fine.txt')")
	if pgCode(err) == "42501" {
		t.Errorf("a file in the allowed location was refused: %v", err)
	}
	if fetched := strings.Join(h.got(), ","); !strings.Contains(fetched, "/allowed/fine.txt") {
		t.Errorf("the allowed file was not fetched: %v", h.got())
	}

	// An s3:// path's query string can redirect the read to another host;
	// the server must not be asked for it.
	refusedNoFetch("SELECT * FROM read_text('s3://b/land/x.txt?s3_endpoint="+strings.TrimPrefix(h.base, "http://")+"&s3_use_ssl=false&s3_url_style=path')", "land/x.txt")
}

// TestReaderFileFunctionsWithoutPolicies checks, function by function, that a
// reader with no table under any policy may name a file only as a plain
// string literal in an allowed location, and that a dot segment or a path
// built as the statement runs is refused. The warehouse's own files stay
// readable, since no policy is in force.
func TestReaderFileFunctionsWithoutPolicies(t *testing.T) {
	dir := t.TempDir()
	landing, warehouse := dir+"/landing/", dir+"/warehouse/"
	for _, d := range []string{landing, warehouse} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cs := startCatalogServerWithConfig(t,
		config.Config{Storage: config.Storage{AllowedLocations: []string{dir + "/"}}},
		func(o *Options) { o.AllowedLocations, o.Warehouse = []string{landing}, warehouse })
	mustExec(t, cs.admin, "COPY (SELECT 42 AS n) TO '"+landing+"in.parquet' (FORMAT parquet)")
	mustExec(t, cs.admin, "COPY (SELECT 1 AS n) TO '"+landing+"in.csv' (FORMAT csv, HEADER true)")
	mustExec(t, cs.admin, `COPY (SELECT '{"n":1}' AS j) TO '`+landing+`in.json' (FORMAT csv, HEADER false, QUOTE '')`)
	mustExec(t, cs.admin, "COPY (SELECT 'ann@acme.com' AS email) TO '"+warehouse+"secret.parquet' (FORMAT parquet)")
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	walt := cs.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)

	good := landing + "in.parquet"
	bad := landing + "../landing/in.parquet" // climbs, resolves to the same file
	// A good literal in an allowed location is accepted; a climbing path and a
	// path built as the statement runs are refused.
	for fn, path := range map[string]string{
		"read_parquet": good, "parquet_scan": good, "parquet_metadata": good,
		"read_csv": landing + "in.csv", "read_csv_auto": landing + "in.csv",
		"read_text": landing + "in.csv", "read_blob": landing + "in.csv",
		"read_json_objects": landing + "in.json",
	} {
		if _, err := rita.Exec(context.Background(), "SELECT * FROM "+fn+"('"+path+"')"); pgCode(err) == "42501" {
			t.Errorf("%s of an allowed file was refused: %v", fn, err)
		}
		expectRefused(t, rita, "SELECT * FROM "+fn+"('"+bad+"')", "42501")
		expectRefused(t, rita, "SELECT * FROM "+fn+"('"+landing+"' || 'in.csv')", "42501")
	}
	// glob over the allowed directory works; a climbing glob is refused.
	if _, err := rita.Exec(context.Background(), "SELECT count(*) FROM glob('"+landing+"*')"); pgCode(err) == "42501" {
		t.Errorf("glob of the allowed directory was refused: %v", err)
	}
	expectRefused(t, rita, "SELECT * FROM glob('"+landing+"../*')", "42501")

	// A replacement scan of a path-like name: good allowed, climbing refused.
	if _, err := rita.Exec(context.Background(), "SELECT * FROM '"+good+"'"); pgCode(err) == "42501" {
		t.Errorf("a replacement scan of an allowed file was refused: %v", err)
	}
	expectRefused(t, rita, "SELECT * FROM '"+bad+"'", "42501")

	// Without a policy, the warehouse's own files are still readable, but not
	// by climbing out of it.
	if _, err := rita.Exec(context.Background(), "SELECT * FROM read_parquet('"+warehouse+"secret.parquet')"); pgCode(err) == "42501" {
		t.Errorf("the warehouse's file was refused without a policy: %v", err)
	}
	expectRefused(t, rita, "SELECT * FROM read_parquet('"+warehouse+"../warehouse/secret.parquet')", "42501")

	// COPY FROM and COPY TO: a literal allowed path works; a climb or an
	// expression is refused.
	mustExec(t, walt, "CREATE TABLE lake.public.loaded (n INTEGER)")
	if _, err := walt.Exec(context.Background(), "COPY lake.public.loaded FROM '"+landing+"in.csv'"); pgCode(err) == "42501" {
		t.Errorf("COPY FROM an allowed file was refused: %v", err)
	}
	expectRefused(t, walt, "COPY lake.public.loaded FROM '"+landing+"../landing/in.csv'", "42501")
	if _, err := walt.Exec(context.Background(), "COPY (SELECT 1) TO '"+landing+"out.csv'"); pgCode(err) == "42501" {
		t.Errorf("COPY TO an allowed file was refused: %v", err)
	}
	expectRefused(t, walt, "COPY (SELECT 1) TO '"+landing+"../landing/out.csv'", "42501")
	expectRefused(t, walt, "COPY (SELECT 1) TO ('"+landing+"' || 'out.csv')", "42501")
	expectRefused(t, walt, "COPY lake.public.loaded TO '"+landing+"../landing/t.csv'", "42501")

	// A view or table macro that reads a file is checked by its body.
	expectRefused(t, walt, "CREATE VIEW lake.public.v AS SELECT * FROM read_csv('"+bad+"')", "42501")
	if _, err := walt.Exec(context.Background(), "CREATE VIEW lake.public.okv AS SELECT * FROM read_csv('"+landing+"in.csv')"); pgCode(err) == "42501" {
		t.Errorf("a view over an allowed file was refused: %v", err)
	}
	expectRefused(t, walt, "CREATE TABLE lake.public.t2 AS SELECT * FROM read_csv('"+bad+"')", "42501")

	// An admin is not held to the literal rule; the sandbox is their only
	// limit. A path built as the statement runs is allowed to reach DuckDB.
	if _, err := cs.admin.Exec(context.Background(), "SELECT * FROM read_csv('"+landing+"' || 'in.csv')"); pgCode(err) == "42501" {
		t.Errorf("an admin's COPY was refused by the file guard: %v", err)
	}
}
