package federation

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestIcebergConnectionOptions(t *testing.T) {
	c := New("snow", Iceberg)
	if c.QueryTimeout != defaultCatalogTimeout || c.TLS != TLSVerifyFull {
		t.Errorf("defaults: %+v", c)
	}
	for k, v := range map[string]string{
		"uri": "https://acct.snowflakecomputing.com/polaris/api/catalog/", "warehouse": "ANALYTICS",
		"credential": "a-programmatic-access-token", "scope": "session:role:IGLOO", "namespace": "public, sales",
		"region": "us-west-2",
	} {
		if err := c.Set(k, v); err != nil {
			t.Fatalf("%s: %v", k, err)
		}
	}
	s := c.Iceberg
	if s.Endpoint != "https://acct.snowflakecomputing.com/polaris/api/catalog" || c.Host != "acct.snowflakecomputing.com" || c.Port != 443 ||
		s.Warehouse != "ANALYTICS" || s.Credential == "" || strings.Join(c.Schemas, "|") != "public|sales" {
		t.Errorf("%+v", c)
	}
	if err := c.Validate(nil); err != nil {
		t.Error(err)
	}
	if got := c.PasswordSource(); got != "credential stored" {
		t.Errorf("source: %s", got)
	}
	if err := c.Set("token_secret", "snow-token"); err != nil || c.Iceberg.Credential != "" || c.PasswordSource() != "token secret snow-token" {
		t.Errorf("token_secret: %v %+v", err, c.Iceberg)
	}
	_ = c.Set("storage_credential", "AKIA:secret")
	if got := c.PasswordSource(); got != "token secret snow-token, storage_credential stored" {
		t.Errorf("source: %s", got)
	}
	if !IsLiveOption("token") || !IsLiveOption("credential") || IsLiveOption("endpoint") || IsLiveOption("storage_credential") {
		t.Error("live options")
	}

	// Options of other types are refused, and the other way round.
	for _, name := range []string{"host", "password", "database", "user"} {
		if err := c.Set(name, "x"); err == nil {
			t.Errorf("%s accepted for TYPE iceberg", name)
		}
	}
	pg := New("crm", Postgres)
	for _, name := range []string{"endpoint", "token", "warehouse", "storage_credential"} {
		if err := pg.Set(name, "x"); err == nil {
			t.Errorf("%s accepted for TYPE postgres", name)
		}
	}
	for _, bad := range [][2]string{
		{"endpoint", "ftp://x"}, {"endpoint", "https://user:pw@host/x"}, {"endpoint", "https://host/x?a=b"},
		{"storage_url_style", "virtual"}, {"read_only", "false"},
	} {
		x := New("x", Iceberg)
		if err := x.Set(bad[0], bad[1]); err == nil {
			t.Errorf("%s = %s accepted", bad[0], bad[1])
		}
	}

	valid := func() Connection {
		c := New("snow", Iceberg)
		_ = c.Set("endpoint", "https://catalog.example.com/api")
		return c
	}
	for name, change := range map[string]func(*Connection){
		"no endpoint":              func(c *Connection) { c.Iceberg.Endpoint = "" },
		"http without tls disable": func(c *Connection) { _ = c.Set("endpoint", "http://catalog:8181/catalog") },
		"https with tls disable":   func(c *Connection) { c.TLS = TLSDisable },
		"scope without credential": func(c *Connection) { c.Iceberg.Scope = "x" },
		"bad storage credential":   func(c *Connection) { c.Iceberg.StorageCredential = "nocolon" },
		"bad storage endpoint":     func(c *Connection) { c.Iceberg.StorageEndpoint = "http://h/path" },
		"url style alone":          func(c *Connection) { c.Iceberg.StorageURLStyle = "path" },
		"secret file with a slash": func(c *Connection) { c.Iceberg.TokenSecret = "../etc/passwd" },
		"http token endpoint": func(c *Connection) {
			c.Iceberg.Credential, c.Iceberg.OAuth2ServerURI = "x", "http://idp.example.com/token"
		},
	} {
		c := valid()
		change(&c)
		if err := c.Validate(nil); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	plain := valid()
	_ = plain.Set("endpoint", "http://lakekeeper:8181/catalog")
	plain.TLS = TLSDisable
	if err := plain.Validate(nil); err != nil {
		t.Errorf("http with tls disable: %v", err)
	}
	if !strings.Contains(plain.TLSWarning(), "http") {
		t.Errorf("warning: %s", plain.TLSWarning())
	}

	// What a replica attached changes with the endpoint, not the token.
	a, b := valid(), valid()
	_ = b.Set("token", "t")
	if a.attachedFields() != b.attachedFields() {
		t.Error("a token change needs a restart")
	}
	_ = b.Set("endpoint", "https://other.example.com/api")
	if a.attachedFields() == b.attachedFields() {
		t.Error("an endpoint change needs no restart")
	}
}

func TestLocationRules(t *testing.T) {
	l := newLocationRules([]string{"s3://igloo/wh", "abfss://data@acct.dfs.core.windows.net/wh/", "https://files.example.com/in/"}, false)
	for _, ok := range []string{
		"s3://remote/tables/t1", "s3a://remote/t/", "gs://bucket/t", "s3://igloo/whx/t", "abfss://other@acct.dfs.core.windows.net/t",
		"r2://bucket/t",
	} {
		if _, err := l.check(ok, false); err != nil {
			t.Errorf("%s: %v", ok, err)
		}
	}
	for _, bad := range []string{
		"", "/var/lib/igloo/x", "file:///etc", "http://host/t", "https://files.example.com/in/t", "s3://", "s3:///t",
		"s3://bucket", "s3://bucket/", "s3://b/x/../y", "s3://b/./y", "s3://b//y", "s3://b/x?y", "s3://b/x#y", "s3://b/%2e%2e/x",
		"s3://igloo/wh/t1", "s3://igloo/wh", "S3A://IGLOO/WH/t", "s3://igloo/", "s3n://igloo/wh/x",
		"az://data/wh/t", "abfss://data@other.dfs.core.windows.net/wh/t", "abfss://c@169.254.169.254/x", "abfss://c@evil.example.com/x",
		"s3://user@host/x", "s3://b:80/x", "gs://b c/x",
	} {
		if _, err := l.check(bad, false); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if got, _ := l.check("s3://remote/t", false); got != "s3://remote/t/" {
		t.Errorf("no trailing slash: %s", got)
	}
	if err := l.checkPrefix("s3://remote"); err != nil {
		t.Errorf("a bucket as a credential's prefix: %v", err)
	}
	if err := l.checkPrefix("s3://igloo"); err == nil {
		t.Error("credentials for Igloo's bucket")
	}
	roots, err := l.tableRoots("s3://remote/t", map[string]string{"write.data.path": "s3://remote/data/t", "write.metadata.path": "s3://remote/t/metadata"})
	if err != nil || strings.Join(roots, ",") != "s3://remote/t/,s3://remote/data/t/" {
		t.Errorf("roots: %v %v", roots, err)
	}
	if _, err := l.tableRoots("s3://remote/t", map[string]string{"write.data.path": "s3://igloo/wh/data"}); err == nil {
		t.Error("data in the warehouse")
	}
	local := newLocationRules([]string{"/tmp/lake"}, true)
	if _, err := local.check("/tmp/remote/t", false); err != nil {
		t.Error(err)
	}
	if _, err := local.check("/tmp/lake/t", false); err == nil {
		t.Error("a local location in the warehouse")
	}
}

// fakeRemote is an Iceberg REST catalog whose answers a test sets.
type fakeRemote struct {
	t     *testing.T
	srv   *httptest.Server
	mu    sync.Mutex
	token string
	// tables maps namespace.table to LoadTableResult bodies; nil means 404.
	tables   map[string]map[string]any
	requests []string
	// delegated counts the loads that asked for storage credentials.
	delegated int
	tokens    int
}

func newFakeRemote(t *testing.T) *fakeRemote {
	f := &fakeRemote{t: t, token: "tok-1", tables: map[string]map[string]any{}}
	f.srv = httptest.NewServer(f)
	t.Cleanup(f.srv.Close)
	return f
}

func loadResult(location string, config map[string]string) map[string]any {
	return map[string]any{
		"metadata-location": location + "/metadata/00001.metadata.json",
		"metadata":          map[string]any{"format-version": 2, "location": location, "properties": map[string]string{}},
		"config":            config,
	}
}

func (f *fakeRemote) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, r.Method+" "+r.URL.EscapedPath())
	path := strings.TrimPrefix(r.URL.EscapedPath(), "/cat/v1/")
	if path == "oauth/tokens" {
		_ = r.ParseForm()
		if r.Form.Get("client_secret") != "the-secret" || r.Form.Get("scope") != "catalog" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"invalid_client","error_description":"bad secret"}`))
			return
		}
		f.tokens++
		_, _ = w.Write([]byte(`{"access_token":"` + f.token + `","expires_in":3600}`))
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+f.token {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"message":"who are you","code":401}}`))
		return
	}
	switch {
	case path == "config":
		if r.URL.Query().Get("warehouse") != "wh" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{"defaults":{},"overrides":{"prefix":"shares/wh","uri":"https://evil.example.com"}}`))
	case path == "shares/wh/namespaces":
		_, _ = w.Write([]byte(`{"namespaces":[["sales"],["other"]]}`))
	case path == "shares/wh/namespaces/sales/tables":
		ids := []map[string]any{}
		var names []string
		for k := range f.tables {
			if ns, name, _ := strings.Cut(k, "."); ns == "sales" {
				names = append(names, name)
			}
		}
		sort.Strings(names)
		for _, name := range names {
			ids = append(ids, map[string]any{"namespace": []string{"sales"}, "name": name})
		}
		if r.URL.Query().Get("pageToken") == "" {
			_ = json.NewEncoder(w).Encode(map[string]any{"identifiers": ids[:1], "next-page-token": "p2"})
		} else {
			_ = json.NewEncoder(w).Encode(map[string]any{"identifiers": ids[1:], "next-page-token": nil})
		}
	case strings.HasPrefix(path, "shares/wh/namespaces/sales/tables/") && r.Method == http.MethodGet:
		name := strings.TrimPrefix(path, "shares/wh/namespaces/sales/tables/")
		if r.Header.Get("X-Iceberg-Access-Delegation") == "vended-credentials" {
			f.delegated++
		}
		body, ok := f.tables["sales."+name]
		if !ok || body == nil {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"message":"not an Iceberg table","code":404}}`))
			return
		}
		_ = json.NewEncoder(w).Encode(body)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeRemote) connection(t *testing.T) Connection {
	c := New("ext", Iceberg)
	for k, v := range map[string]string{
		"endpoint": f.srv.URL + "/cat", "tls": "disable", "warehouse": "wh", "credential": "the-secret", "schema": "sales",
	} {
		if err := c.Set(k, v); err != nil {
			t.Fatal(err)
		}
	}
	return c
}

func serve(p *icebergProxy, method, path string) (int, string) {
	r := httptest.NewRequest(method, "http://127.0.0.1"+p.path+path, nil)
	w := httptest.NewRecorder()
	p.ServeHTTP(w, r)
	return w.Code, w.Body.String()
}

func TestIcebergProxy(t *testing.T) {
	f := newFakeRemote(t)
	creds := map[string]string{
		"s3.access-key-id": "AKIAVENDED", "s3.secret-access-key": "vended-secret", "s3.session-token": "vended-session",
		"s3.endpoint": "http://evil.example.com", "s3.path-style-access": "true", "client.region": "us-east-1",
		"s3.remote-signing-enabled": "true", "adls.sas-token.acct.dfs.evil.example.com": "sas", "s3.region": "us-east-1.evil.example.com/",
	}
	f.tables["sales.good"] = loadResult("s3://remote/good", creds)
	f.tables["sales.moves"] = loadResult("s3://remote/moves", creds)
	f.tables["sales.inwarehouse"] = loadResult("s3://igloo/wh/stolen", creds)
	f.tables["sales.noticeberg"] = nil
	broad := loadResult("s3://remote/broad", nil)
	broad["storage-credentials"] = []map[string]any{{"prefix": "s3://igloo/", "config": creds}, {"prefix": "s3://remote/", "config": creds}}
	f.tables["sales.badprefix"] = broad
	scoped := loadResult("s3://remote/scoped", nil)
	scoped["storage-credentials"] = []map[string]any{{"prefix": "s3://remote/scoped/", "config": creds}}
	f.tables["sales.scoped"] = scoped

	log := slog.New(slog.DiscardHandler)
	p, err := newIcebergProxy(f.connection(t), "", newLocationRules([]string{"s3://igloo/wh/"}, false), log)
	if err != nil {
		t.Fatal(err)
	}
	p.path = "/secret"
	if err := p.discover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(p.roots(), ","); got != "s3://remote/good/,s3://remote/moves/,s3://remote/scoped/" {
		t.Errorf("roots: %s", got)
	}
	sum := p.summary()
	for _, want := range []string{"3 tables attached", "3 left out", "sales.inwarehouse: s3://igloo/wh/stolen/ overlaps",
		"sales.noticeberg: the catalog answered 404: not an Iceberg table", "sales.badprefix: the catalog hands out credentials for s3://igloo/"} {
		if !strings.Contains(sum, want) {
			t.Errorf("summary lacks %q: %s", want, sum)
		}
	}

	// DuckDB is told of no prefix and of reads only, and nothing else of
	// the catalog's configuration.
	code, body := serve(p, "GET", "/v1/config?warehouse=wh")
	if code != 200 || strings.Contains(body, "evil") || strings.Contains(body, "prefix\":\"") || strings.Contains(body, "POST") {
		t.Errorf("config: %d %s", code, body)
	}
	if _, body := serve(p, "GET", "/v1/namespaces"); body != `{"namespaces":[["sales"]]}` {
		t.Errorf("namespaces: %s", body)
	}
	if _, body := serve(p, "GET", "/v1/namespaces/sales/tables"); !strings.Contains(body, `"good"`) || strings.Contains(body, "inwarehouse") {
		t.Errorf("tables: %s", body)
	}
	if code, _ := serve(p, "GET", "/v1/namespaces/other"); code != 404 {
		t.Errorf("a namespace left out: %d", code)
	}

	check := func(body string) {
		t.Helper()
		var res struct {
			Config map[string]string `json:"config"`
		}
		if err := json.Unmarshal([]byte(body), &res); err != nil {
			t.Fatal(err)
		}
		if res.Config["s3.access-key-id"] != "AKIAVENDED" || res.Config["s3.session-token"] != "vended-session" ||
			res.Config["client.region"] != "us-east-1" {
			t.Errorf("credentials lost: %v", res.Config)
		}
		for _, k := range []string{"s3.endpoint", "s3.path-style-access", "s3.remote-signing-enabled", "adls.sas-token.acct.dfs.evil.example.com", "s3.region"} {
			if _, ok := res.Config[k]; ok {
				t.Errorf("%s passed on: %v", k, res.Config)
			}
		}
	}
	// The first load is served from what startup found, the next from the
	// catalog, with storage credentials asked for.
	code, body = serve(p, "GET", "/v1/namespaces/sales/tables/good")
	if code != 200 {
		t.Fatalf("load: %d %s", code, body)
	}
	check(body)
	f.mu.Lock()
	before := f.delegated
	f.mu.Unlock()
	code, body = serve(p, "GET", "/v1/namespaces/sales/tables/good")
	if code != 200 {
		t.Fatalf("load: %d %s", code, body)
	}
	check(body)
	if f.delegated != before+1 {
		t.Error("storage credentials were not asked for")
	}
	// A table whose files moved since startup is refused.
	f.mu.Lock()
	f.tables["sales.moves"] = loadResult("s3://remote/elsewhere", creds)
	f.mu.Unlock()
	serve(p, "GET", "/v1/namespaces/sales/tables/moves") // the answer from startup
	if code, body := serve(p, "GET", "/v1/namespaces/sales/tables/moves"); code != 403 || !strings.Contains(body, "restart") {
		t.Errorf("moved: %d %s", code, body)
	}
	if code, body := serve(p, "GET", "/v1/namespaces/sales/tables/inwarehouse"); code != 404 || !strings.Contains(body, "overlaps") {
		t.Errorf("left out: %d %s", code, body)
	}
	if code, _ := serve(p, "GET", "/v1/namespaces/sales/tables/created_later"); code != 404 {
		t.Errorf("a table not found at startup: %d", code)
	}

	// Nothing changes the catalog.
	for _, req := range [][2]string{
		{"POST", "/v1/namespaces"}, {"DELETE", "/v1/namespaces/sales"}, {"POST", "/v1/namespaces/sales/tables"},
		{"POST", "/v1/namespaces/sales/tables/good"}, {"DELETE", "/v1/namespaces/sales/tables/good"}, {"POST", "/v1/tables/rename"},
		{"POST", "/v1/transactions/commit"}, {"POST", "/v1/namespaces/sales/register"}, {"PUT", "/v1/namespaces/sales/tables/good"},
	} {
		if code, _ := serve(p, req[0], req[1]); code != 403 {
			t.Errorf("%s %s: %d", req[0], req[1], code)
		}
	}
	if code, _ := serve(p, "POST", "/v1/namespaces/sales/tables/good/metrics"); code != 204 {
		t.Errorf("metrics: %d", code)
	}
	if code, _ := serve(p, "GET", "/v1/oauth/tokens"); code != 404 {
		t.Errorf("the token endpoint: %d", code)
	}
	r := httptest.NewRequest("GET", "http://127.0.0.1/other/v1/config", nil)
	w := httptest.NewRecorder()
	p.ServeHTTP(w, r)
	if w.Code != 404 {
		t.Errorf("outside the proxy's path: %d", w.Code)
	}
	f.mu.Lock()
	for _, req := range f.requests {
		if !strings.HasPrefix(req, "GET ") && req != "POST /cat/v1/oauth/tokens" {
			t.Errorf("the catalog got %s", req)
		}
	}
	tokens := f.tokens
	f.mu.Unlock()
	if tokens != 1 {
		t.Errorf("%d tokens fetched, want 1, kept for every request", tokens)
	}

	// The connection's storage endpoint and region, never the catalog's.
	c := p.settings()
	_ = c.Set("storage_endpoint", "http://minio:9000")
	_ = c.Set("region", "eu-west-1")
	got := storageConfig(c, map[string]string{"s3.endpoint": "https://evil", "s3.access-key-id": "k"})
	if got["s3.endpoint"] != "http://minio:9000" || got["s3.path-style-access"] != "true" || got["client.region"] != "eu-west-1" {
		t.Errorf("storage config: %v", got)
	}
	// With a storage credential of the connection's own, the catalog's are
	// dropped and not asked for.
	_ = c.Set("storage_credential", "KEY:SECRET")
	p.update(c)
	f.mu.Lock()
	before = f.delegated
	f.mu.Unlock()
	code, body = serve(p, "GET", "/v1/namespaces/sales/tables/scoped")
	if code != 200 || strings.Contains(body, "AKIAVENDED") || f.delegated != before {
		t.Errorf("static storage: %d %s", code, body)
	}
}

func TestIcebergProxyAuth(t *testing.T) {
	f := newFakeRemote(t)
	f.tables["sales.good"] = loadResult("s3://remote/good", nil)
	log := slog.New(slog.DiscardHandler)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "cred"), []byte("the-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := f.connection(t)
	_ = c.Set("credential_secret", "cred")
	p, err := newIcebergProxy(c, dir, newLocationRules(nil, false), log)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.check(context.Background()); err != nil {
		t.Fatal(err)
	}
	// A token the catalog stops taking is replaced.
	f.mu.Lock()
	f.token = "tok-2"
	f.mu.Unlock()
	if err := p.check(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.tokens != 2 {
		t.Errorf("tokens: %d", f.tokens)
	}
	// A wrong credential says why, without the credential.
	_ = c.Set("credential", "id:wrong-secret-value")
	p.update(c)
	err = p.check(context.Background())
	if err == nil || !strings.Contains(err.Error(), "invalid_client: bad secret") || strings.Contains(err.Error(), "wrong-secret-value") {
		t.Errorf("wrong credential: %v", err)
	}
	// A bearer token goes as it is.
	_ = c.Set("token", "tok-2")
	p.update(c)
	if err := p.check(context.Background()); err != nil {
		t.Errorf("token: %v", err)
	}
	// A catalog that does not answer in time is given up on.
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
	}))
	defer slow.Close()
	s := New("slow", Iceberg)
	_ = s.Set("endpoint", slow.URL)
	s.TLS, s.QueryTimeout = TLSDisable, 200*time.Millisecond
	sp, _ := newIcebergProxy(s, "", newLocationRules(nil, false), log)
	start := time.Now()
	if err := sp.check(context.Background()); err == nil || time.Since(start) > time.Second {
		t.Errorf("slow catalog: %v after %s", err, time.Since(start))
	}
}

func TestIcebergStoreSeals(t *testing.T) {
	c := New("ext", Iceberg)
	for k, v := range map[string]string{"endpoint": "https://c.example.com/api", "token": "tok-secret-value", "storage_credential": "AKIA:storage-secret-value",
		"storage_endpoint": "minio:9000", "region": "r1"} {
		if err := c.Set(k, v); err != nil {
			t.Fatal(err)
		}
	}
	s := &PostgresStore{}
	var err error
	if s.sealer, err = newSealer(bytes.Repeat([]byte{7}, 32)); err != nil {
		t.Fatal(err)
	}
	sealed, settings := s.sealPassword(c)
	for _, secret := range []string{"tok-secret-value", "storage-secret-value"} {
		if bytes.Contains(sealed, []byte(secret)) || bytes.Contains(settings, []byte(secret)) {
			t.Errorf("%s stored in the clear", secret)
		}
	}
	plain, err := s.sealer.open("ext", sealed)
	if err != nil {
		t.Fatal(err)
	}
	var v secretValues
	var st storedSettings
	if json.Unmarshal([]byte(plain), &v) != nil || json.Unmarshal(settings, &st) != nil {
		t.Fatal("not JSON")
	}
	if got := st.settings(v); got != c.Iceberg {
		t.Errorf("round trip: %+v != %+v", got, c.Iceberg)
	}
	if _, err := s.sealer.open("other", sealed); err == nil {
		t.Error("credentials moved to another connection decrypt")
	}
}

// TestIcebergCatalogSlow checks that an Iceberg REST catalog whose tables
// are not found by shortly before startup's deadline is left out, with the
// reason, so that Igloo still starts.
func TestIcebergCatalogSlow(t *testing.T) {
	if os.Getenv("IGLOO_TEST_EXTENSION_DIR") == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(slow.Close)
	store := NewMemoryStore()
	c := New("slowcat", Iceberg)
	for k, v := range map[string]string{"endpoint": slow.URL, "tls": "disable", "warehouse": "wh", "token": "tok-slow-catalog"} {
		if err := c.Set(k, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Create(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), attachMargin+2*time.Second)
	defer cancel()
	start := time.Now()
	m, eng := startEngineWithin(t, ctx, store)
	if took := time.Since(start); took > attachMargin {
		t.Errorf("startup took %s", took)
	}
	if _, ok := m.Catalogs()["slowcat"]; ok {
		t.Fatal("attached")
	}
	st, err := m.List(context.Background())
	if err != nil || len(st) != 1 || st[0].State != "failed" || !strings.Contains(st[0].Detail, "time Igloo gives connections at startup") {
		t.Errorf("%+v %v", st, err)
	}
	expect(t, eng, "SELECT 42", "42")
}

// TestSweepVended checks that the storage credentials DuckDB keeps for
// Iceberg REST catalogs' tables are dropped once old, and no others.
func TestSweepVended(t *testing.T) {
	if os.Getenv("IGLOO_TEST_EXTENSION_DIR") == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	m, eng := startEngine(t, NewMemoryStore())
	if err := m.sweep(context.Background(), time.Now()); err != nil {
		t.Errorf("nothing to sweep: %v", err)
	}
	// Stand in for an attached connection of TYPE iceberg.
	p, err := newIcebergProxy(New("ext", Iceberg), "", newLocationRules(nil, false), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.attached["ext"] = &attachment{conn: p.attached, ep: p}
	m.mu.Unlock()
	for _, name := range []string{"__internal_ic_a__s__t__1", "__internal_ic_b__s__t__2", "igloo_keep"} {
		if _, err := query(t, eng, "CREATE TEMPORARY SECRET "+ident(name)+" (TYPE s3, KEY_ID 'k', SECRET 's', SCOPE 's3://remote/"+name+"/')"); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	if err := m.sweep(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	expect(t, eng, "SELECT count(*) FROM duckdb_secrets() WHERE name LIKE '%internal_ic%'", "2")
	if _, err := query(t, eng, "CREATE TEMPORARY SECRET \"__internal_ic_c__s__t__3\" (TYPE s3, KEY_ID 'k', SECRET 's', SCOPE 's3://remote/c/')"); err != nil {
		t.Fatal(err)
	}
	if err := m.sweep(context.Background(), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := m.sweep(context.Background(), now.Add(vendedLifetime)); err != nil {
		t.Fatal(err)
	}
	expect(t, eng, "SELECT string_agg(name, ',' ORDER BY name) FROM duckdb_secrets() WHERE NOT persistent", "__internal_ic_c__s__t__3,igloo_keep")
	if len(m.vended) != 1 {
		t.Errorf("tracked: %v", m.vended)
	}
}
