package sharing

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/audit"
)

// testStore opens a store in a database of its own, so that packages
// testing in parallel against IGLOO_TEST_DATABASE_URL do not meet.
func testStore(t *testing.T) *Store {
	t.Helper()
	base := os.Getenv("IGLOO_TEST_DATABASE_URL")
	if base == "" {
		t.Skip("IGLOO_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	name := "sharing_test_" + hex.EncodeToString(b)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	store, err := OpenStore(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		store.Close()
		_, _ = admin.Exec(ctx, "DROP DATABASE "+name+" WITH (FORCE)")
		_ = admin.Close(ctx)
	})
	return store
}

// fakeSource serves fixed snapshots.
type fakeSource struct {
	tables map[string]*snapshot
	err    error
	asked  []version
}

func (f *fakeSource) snapshot(_ context.Context, schema, name string, v version, files bool) (*snapshot, error) {
	f.asked = append(f.asked, v)
	if f.err != nil {
		return nil, f.err
	}
	s, ok := f.tables[schema+"."+name]
	if !ok {
		return nil, ErrNotFound
	}
	out := *s
	if !files {
		out.files = nil
	}
	return &out, nil
}

func (f *fakeSource) startingVersion(context.Context, string, string, time.Time) (int64, error) {
	return 3, nil
}

func (f *fakeSource) gone(string) {}

type fixture struct {
	t      *testing.T
	store  *Store
	server *Server
	http   *httptest.Server
	src    *fakeSource
	audit  *audit.Log
	data   []byte
	// token is the recipient acme's bearer token.
	token string
}

func newFixture(t *testing.T) *fixture {
	store := testStore(t)
	ctx := context.Background()
	dir := t.TempDir()
	warehouse := "file://" + dir + "/"
	data := []byte("PAR1 not really a parquet file, but bytes to serve PAR1")
	path := filepath.Join(dir, "public", "campaigns", "data", "f1.parquet")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	src := &fakeSource{tables: map[string]*snapshot{
		"public.campaigns": {
			tableID: "11111111-2222-3333-4444-555555555555", comment: "Campaign spend", version: 7, timestamp: 1_800_000_000_000,
			schema: `{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}`,
			files: []dataFile{{path: "file://" + path, size: int64(len(data)), count: 10, stats: `{"numRecords":10}`}},
			size:  int64(len(data)),
		},
		"public.secret": {tableID: "x", version: 1, schema: `{"type":"struct","fields":[]}`},
	}}
	history := audit.New(audit.NewMemoryStore(100, 100), audit.Options{}, slog.New(slog.DiscardHandler))
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	server, err := NewServer(ServerOptions{
		Store: store, Key: key, Files: iceio.LocalFS{}, Warehouse: warehouse, Catalog: "lake", Audit: history,
		Tables: TableSource{source: src},
	})
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, store: store, server: server, http: httptest.NewServer(server), src: src, audit: history, data: data}
	t.Cleanup(f.http.Close)

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(store.CreateShare(ctx, Share{Name: "clients", CreatedBy: "igloo"}))
	must(store.CreateShare(ctx, Share{Name: "internal", CreatedBy: "igloo"}))
	must(store.AddTable(ctx, SharedTable{Share: "clients", Schema: "public", Name: "campaigns", SourceSchema: "public", SourceTable: "campaigns", History: true}))
	must(store.AddTable(ctx, SharedTable{Share: "clients", Schema: "reports", Name: "secret", SourceSchema: "public", SourceTable: "secret"}))
	must(store.AddTable(ctx, SharedTable{Share: "internal", Schema: "public", Name: "campaigns", SourceSchema: "public", SourceTable: "campaigns"}))
	code := f.createRecipient("acme")
	must(store.GrantShare(ctx, "clients", "acme", "igloo"))
	f.token = f.activate(code).BearerToken
	return f
}

func (f *fixture) createRecipient(name string) string {
	f.t.Helper()
	code, err := newToken(activationPrefix)
	if err != nil {
		f.t.Fatal(err)
	}
	if err := f.store.CreateRecipient(context.Background(), Recipient{Name: name, TokenLifetime: time.Hour}, code, time.Now().Add(time.Hour)); err != nil {
		f.t.Fatal(err)
	}
	return code.Text
}

func (f *fixture) activate(code string) profile {
	f.t.Helper()
	resp, err := http.Post(f.http.URL+Prefix+"/activate/"+code, "", nil)
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck
	var p profile
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&p) != nil {
		f.t.Fatalf("activate: %s", resp.Status)
	}
	return p
}

// call makes a request with the given bearer token ("" for none) and
// returns the status, headers and body.
func (f *fixture) call(method, path, token, body string, header ...string) (int, http.Header, string) {
	f.t.Helper()
	target := path
	if !strings.HasPrefix(path, "http") {
		target = f.http.URL + Prefix + path
	}
	req, err := http.NewRequest(method, target, strings.NewReader(body))
	if err != nil {
		f.t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for i := 0; i+1 < len(header); i += 2 {
		if header[i] == "Host" {
			req.Host = header[i+1]
		}
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, string(b)
}

func (f *fixture) expect(method, path, token, body string, wantStatus int, wantBody string) string {
	f.t.Helper()
	status, _, got := f.call(method, path, token, body)
	if status != wantStatus || !strings.Contains(got, wantBody) {
		f.t.Errorf("%s %s: %d %s, want %d containing %q", method, path, status, got, wantStatus, wantBody)
	}
	return got
}

// lines splits an ndjson response into decoded objects.
func ndjson(t *testing.T, body string) []map[string]any {
	t.Helper()
	var out []map[string]any
	sc := bufio.NewScanner(strings.NewReader(body))
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("line %q: %v", sc.Text(), err)
		}
		out = append(out, m)
	}
	return out
}

func TestServerProtocol(t *testing.T) {
	f := newFixture(t)
	tok := f.token

	f.expect("GET", "/shares", tok, "", 200, `{"items":[{"name":"clients","id":`)
	f.expect("GET", "/shares/CLIENTS", tok, "", 200, `{"share":{"name":"clients"`)
	f.expect("GET", "/shares/clients/schemas", tok, "", 200, `"items":[{"name":"public","share":"clients"},{"name":"reports","share":"clients"}]`)
	f.expect("GET", "/shares/clients/schemas/public/tables", tok, "", 200, `"items":[{"name":"campaigns","schema":"public","share":"clients"`)
	page1 := f.expect("GET", "/shares/clients/all-tables?maxResults=1", tok, "", 200, `"nextPageToken"`)
	var p struct{ NextPageToken string }
	_ = json.Unmarshal([]byte(page1), &p)
	page2 := f.expect("GET", "/shares/clients/all-tables?maxResults=1&pageToken="+p.NextPageToken, tok, "", 200, `"name":"secret"`)
	if strings.Contains(page2, "nextPageToken") {
		t.Errorf("the last page has a next page: %s", page2)
	}
	f.expect("GET", "/shares/clients/all-tables?pageToken=bogus", tok, "", 400, "pageToken")

	// Shares that are not granted look like shares that do not exist.
	f.expect("GET", "/shares/internal", tok, "", 404, "does not exist or is not shared with you")
	f.expect("GET", "/shares/internal/schemas/public/tables/campaigns/metadata", tok, "", 404, "")
	f.expect("GET", "/shares/clients/schemas/nope/tables", tok, "", 404, "")

	status, h, _ := f.call("GET", "/shares/clients/schemas/public/tables/campaigns/version", tok, "")
	if status != 200 || h.Get("Delta-Table-Version") != "7" {
		t.Errorf("version: %d %v", status, h)
	}
	status, h, _ = f.call("HEAD", "/shares/clients/schemas/public/tables/campaigns", tok, "")
	if status != 200 || h.Get("Delta-Table-Version") != "7" {
		t.Errorf("version through HEAD: %d %v", status, h)
	}

	status, h, body := f.call("GET", "/shares/clients/schemas/public/tables/campaigns/metadata", tok, "")
	ls := ndjson(t, body)
	if status != 200 || h.Get("Content-Type") != "application/x-ndjson; charset=utf-8" || len(ls) != 2 {
		t.Fatalf("metadata: %d %s", status, body)
	}
	if got, _ := json.Marshal(ls[0]); string(got) != `{"protocol":{"minReaderVersion":1}}` {
		t.Errorf("protocol: %s", got)
	}
	meta := ls[1]["metaData"].(map[string]any)
	if meta["id"] != "11111111-2222-3333-4444-555555555555" || meta["description"] != "Campaign spend" ||
		meta["format"].(map[string]any)["provider"] != "parquet" || len(meta["partitionColumns"].([]any)) != 0 {
		t.Errorf("metadata: %v", meta)
	}

	status, h, body = f.call("POST", "/shares/clients/schemas/public/tables/campaigns/query", tok, `{"predicateHints": ["id > 1"]}`)
	ls = ndjson(t, body)
	if status != 200 || len(ls) != 3 || h.Get("Delta-Table-Version") != "7" {
		t.Fatalf("query: %d %s", status, body)
	}
	file := ls[2]["file"].(map[string]any)
	fileURL := file["url"].(string)
	if file["size"] != float64(len(f.data)) || file["stats"] != `{"numRecords":10}` || len(file["id"].(string)) != 32 ||
		!strings.HasPrefix(fileURL, f.http.URL+Prefix+"/files/") || strings.Contains(fileURL, "campaigns") {
		t.Errorf("file: %v", file)
	}
	if exp := int64(file["expirationTimestamp"].(float64)); exp < time.Now().Add(50*time.Minute).UnixMilli() || exp > time.Now().Add(61*time.Minute).UnixMilli() {
		t.Errorf("the URL expires at %d, not in an hour", exp)
	}

	// The data file itself, by its signed URL, needs no bearer token.
	status, h, got := f.call("GET", fileURL, "", "")
	if status != 200 || got != string(f.data) || h.Get("Content-Type") != "application/octet-stream" {
		t.Errorf("file: %d %q", status, got)
	}
	status, h, _ = f.call("HEAD", fileURL, "", "")
	if status != 200 || h.Get("Content-Length") != strconv.Itoa(len(f.data)) {
		t.Errorf("file HEAD: %d %v", status, h)
	}
	status, _, got = f.call("GET", fileURL, "", "", "Range", "bytes=-4")
	if status != http.StatusPartialContent || got != "PAR1" {
		t.Errorf("file range: %d %q", status, got)
	}
	f.expect("GET", fileURL[:len(fileURL)-3]+"abc", "", "", 403, "")

	// The delta response format, for clients that ask only for it.
	status, h, body = f.call("POST", "/shares/clients/schemas/public/tables/campaigns/query", tok, "{}",
		"delta-sharing-capabilities", "responseformat=delta;readerfeatures=deletionvectors")
	ls = ndjson(t, body)
	if status != 200 || h.Get("delta-sharing-capabilities") != "responseformat=delta" || len(ls) != 3 {
		t.Fatalf("delta format: %d %v %s", status, h, body)
	}
	if ls[0]["protocol"].(map[string]any)["deltaProtocol"] == nil || ls[1]["metaData"].(map[string]any)["deltaMetadata"] == nil {
		t.Errorf("delta format actions: %s", body)
	}
	add := ls[2]["file"].(map[string]any)["deltaSingleAction"].(map[string]any)["add"].(map[string]any)
	if !strings.HasPrefix(add["path"].(string), f.http.URL) || add["dataChange"] != true {
		t.Errorf("delta add action: %v", add)
	}
	_, h, _ = f.call("POST", "/shares/clients/schemas/public/tables/campaigns/query", tok, "{}",
		"delta-sharing-capabilities", "responseformat=delta,parquet")
	if h.Get("delta-sharing-capabilities") != "responseformat=parquet" {
		t.Errorf("a client reading both formats gets %q", h.Get("delta-sharing-capabilities"))
	}

	// Time travel needs the table's history to be shared.
	f.src.asked = nil
	f.expect("POST", "/shares/clients/schemas/public/tables/campaigns/query", tok, `{"version": 3}`, 200, `"version":7`)
	if len(f.src.asked) != 1 || f.src.asked[0].number == nil || *f.src.asked[0].number != 3 {
		t.Errorf("asked for %+v", f.src.asked)
	}
	f.expect("GET", "/shares/clients/schemas/public/tables/campaigns/metadata?timestamp=2026-10-01T00:00:00Z", tok, "", 200, `"version":7`)
	f.expect("POST", "/shares/clients/schemas/reports/tables/secret/query", tok, `{"version": 1}`, 403, "history of reports.secret is not shared")
	f.expect("GET", "/shares/clients/schemas/reports/tables/secret/version?startingTimestamp=2026-01-01T00:00:00Z", tok, "", 403, "")
	f.expect("POST", "/shares/clients/schemas/public/tables/campaigns/query", tok, `{"startingVersion": 1}`, 400, "change data feeds")
	f.expect("GET", "/shares/clients/schemas/public/tables/campaigns/changes?startingVersion=0", tok, "", 400, "")
	f.expect("POST", "/shares/clients/schemas/public/tables/campaigns/query", tok, `{"version": 1, "timestamp": "2026-01-01T00:00:00Z"}`, 400, "")
	f.expect("POST", "/shares/clients/schemas/public/tables/campaigns/query", tok, `not json`, 400, "malformed")

	// Tables Igloo cannot serve say why.
	f.src.err = notShareable("public.campaigns has deleted rows that are not yet rewritten")
	f.expect("POST", "/shares/clients/schemas/public/tables/campaigns/query", tok, "{}", 400, "deleted rows")
	f.src.err = errors.New("catalog down")
	body = f.expect("POST", "/shares/clients/schemas/public/tables/campaigns/query", tok, "{}", 500, "INTERNAL_ERROR")
	if strings.Contains(body, "catalog down") {
		t.Error("an internal error reached the recipient")
	}
	f.src.err = nil

	// Every request is in the query history, as the recipient.
	qs, err := f.audit.Queries(context.Background(), audit.Viewer{All: true}, audit.Page{})
	if err != nil {
		t.Fatal(err)
	}
	var sawQuery bool
	for _, q := range qs {
		if q.User != "recipient:acme" || q.Role != "recipient" || q.StatementType != "DELTA SHARING" {
			t.Errorf("history record %+v", q)
		}
		if q.Text == "POST /shares/clients/schemas/public/tables/campaigns/query version 3" {
			sawQuery = q.Rows == 1 && slices.Equal(q.TablesRead, []string{"lake.public.campaigns"})
		}
	}
	if !sawQuery {
		t.Errorf("the time travel query is not in the history: %+v", qs)
	}
}

func TestServerAccess(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	f.expect("GET", "/shares", "", "", 401, "missing or invalid bearer token")
	f.expect("GET", "/shares", "Basic abc", "", 401, "")
	f.expect("GET", "/shares", f.token+"x", "", 401, "")
	wrong := f.token[:len(f.token)-10] + "AAAAAAAAAA"
	f.expect("GET", "/shares", wrong, "", 401, "missing or invalid bearer token")
	other, _ := newToken(tokenPrefix)
	f.expect("GET", "/shares", other.Text, "", 401, "")

	logins, _ := f.audit.Logins(ctx, audit.Viewer{All: true}, audit.Page{})
	var failures int
	for _, l := range logins {
		if l.Result == audit.LoginFailure {
			failures++
		}
	}
	if failures < 3 {
		t.Errorf("%d failed logins recorded, want the bad tokens", failures)
	}

	// Plain HTTP is for localhost only.
	status, _, body := f.call("GET", "/shares", f.token, "", "Host", "igloo.example.com")
	if status != 403 || !strings.Contains(body, "TLS") {
		t.Errorf("a request to another host without TLS: %d %s", status, body)
	}

	// A file URL stops working when the table leaves the share, the grant
	// is revoked or the token is rotated.
	_, _, body = f.call("POST", "/shares/clients/schemas/public/tables/campaigns/query", f.token, "{}")
	fileURL := ndjson(t, body)[2]["file"].(map[string]any)["url"].(string)
	f.expect("GET", fileURL, "", "", 200, "PAR1")
	if err := f.store.RevokeShare(ctx, "clients", "acme"); err != nil {
		t.Fatal(err)
	}
	f.server.Forget()
	f.expect("GET", fileURL, "", "", 403, "no longer shared")
	f.expect("GET", "/shares", f.token, "", 200, `"items":[]`)
	f.expect("GET", "/shares/clients/schemas", f.token, "", 404, "")
	if err := f.store.GrantShare(ctx, "clients", "acme", "igloo"); err != nil {
		t.Fatal(err)
	}
	f.server.Forget()
	f.expect("GET", fileURL, "", "", 200, "PAR1")

	// Rotation: the old token keeps working for the grace period, then
	// stops; the new link gives a new token.
	code, _ := newToken(activationPrefix)
	if err := f.store.RotateToken(ctx, "acme", time.Hour, code, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	f.server.Forget()
	f.expect("GET", "/shares", f.token, "", 200, "clients")
	code2, _ := newToken(activationPrefix)
	if err := f.store.RotateToken(ctx, "acme", 0, code2, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	f.server.Forget()
	f.expect("GET", "/shares", f.token, "", 401, "expired or was replaced")
	f.expect("GET", fileURL, "", "", 403, "access has ended")
	status, _, _ = f.call("POST", "/activate/"+code.Text, "", "")
	if status != 404 {
		t.Errorf("a replaced activation link worked: %d", status)
	}
	p := f.activate(code2.Text)
	f.expect("GET", "/shares", p.BearerToken, "", 200, "clients")
	if exp, err := time.Parse(time.RFC3339, p.ExpirationTime); err != nil || exp.Before(time.Now().Add(50*time.Minute)) || exp.After(time.Now().Add(2*time.Hour)) {
		t.Errorf("the token expires at %q, want in an hour (the recipient's token lifetime)", p.ExpirationTime)
	}

	// Activation links work once, and the page itself changes nothing.
	code3 := f.createRecipient("globex")
	status, h, page := f.call("GET", "/activate/"+code3, "", "")
	if status != 200 || !strings.Contains(page, `<form method="post">`) || !strings.Contains(h.Get("Content-Security-Policy"), "default-src 'none'") {
		t.Errorf("activation page: %d %s", status, page)
	}
	f.activate(code3)
	status, _, _ = f.call("POST", "/activate/"+code3, "", "")
	if status != 404 {
		t.Errorf("an activation link worked twice: %d", status)
	}
	f.expect("GET", "/activate/garbage", "", "", 404, "not valid")

	// Dropping the recipient ends its token.
	if err := f.store.DropRecipient(ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	f.server.Forget()
	f.expect("GET", "/shares", p.BearerToken, "", 401, "")
}

// TestClient reads Igloo's own endpoint with the consumer's client.
func TestClient(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	g := newGuardedDialer()
	g.allow(endpointHostPort(f.http.URL))
	c := &client{endpoint: f.http.URL + Prefix, token: f.token, http: newHTTPClient(g)}

	shares, err := c.shares(ctx)
	if err != nil || !slices.Equal(shares, []string{"clients"}) {
		t.Fatalf("shares = %v, %v", shares, err)
	}
	tables, err := c.tables(ctx, "clients")
	if err != nil || len(tables) != 2 || tables[0] != (remoteTable{Share: "clients", Schema: "public", Name: "campaigns"}) {
		t.Fatalf("tables = %v, %v", tables, err)
	}
	v, err := c.version(ctx, "clients", "public", "campaigns")
	if err != nil || v != 7 {
		t.Fatalf("version = %d, %v", v, err)
	}
	snap, err := c.query(ctx, "clients", "public", "campaigns", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if snap.version != 7 || len(snap.files) != 1 || snap.files[0].Size != int64(len(f.data)) ||
		snap.files[0].Expires.Before(time.Now().Add(50*time.Minute)) || !strings.Contains(snap.schema, `"name":"id"`) {
		t.Errorf("snapshot = %+v", snap)
	}

	_, err = c.query(ctx, "clients", "public", "nope", time.Now())
	var pe *providerError
	if !errors.As(err, &pe) || pe.status != 404 || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("a missing table: %v", err)
	}
	bad := &client{endpoint: c.endpoint, token: "dst_nope", http: c.http}
	if _, err := bad.shares(ctx); !errors.As(err, &pe) || pe.status != 401 {
		t.Errorf("a bad token: %v", err)
	}
}
