package icebergrest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
)

func init() { failedTokenDelay = 0 }

// users is a writable in-memory user store that also knows passwords.
type users struct {
	mu        sync.Mutex
	users     map[string]auth.User
	passwords map[string]string
}

func (s *users) Get(_ context.Context, name string) (auth.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[name]
	if !ok {
		return auth.User{}, auth.ErrNotFound
	}
	return u, nil
}

func (s *users) List(context.Context) ([]auth.User, error)                 { return nil, nil }
func (s *users) Create(context.Context, auth.User) error                   { return nil }
func (s *users) SetVerifier(context.Context, string, string) error         { return nil }
func (s *users) SetRole(context.Context, string, auth.Role) error          { return nil }
func (s *users) EnsureAdmin(context.Context, string, string) (bool, error) { return false, nil }
func (s *users) MockSecret(context.Context) ([]byte, error)                { return []byte("mock"), nil }
func (s *users) Ping(context.Context) error                                { return nil }
func (s *users) Close()                                                    {}

func (s *users) Delete(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.users, name)
	return nil
}

func (s *users) add(t *testing.T, name, password string, role auth.Role) {
	t.Helper()
	v, err := auth.NewVerifier(password)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.users[name] = auth.User{Name: name, Role: role, Verifier: v}
	s.passwords[name] = password
}

func (s *users) authenticate(_ context.Context, name, password string, _ net.Addr) (auth.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if u, ok := s.users[name]; ok && s.passwords[name] == password {
		return u, nil
	}
	return auth.User{}, ErrLoginFailed
}

// lakekeeper fakes the catalog's Iceberg REST API for one warehouse.
type lakekeeper struct {
	mu sync.Mutex
	// namespaces and tables are keyed by namespace levels joined by \x1f;
	// tables map "ns\x1ftable" to its location.
	namespaces map[string]bool
	tables     map[string]string
	// staged tables answer commits but, as in Lakekeeper, not HEAD.
	staged map[string]string
	// requests are what reached the catalog: "METHOD path?query".
	requests []string
	headers  []http.Header
	bodies   []string
	// fail makes every request fail with this status.
	fail int
}

const prefix = "4b1c-warehouse"

func (l *lakekeeper) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l.mu.Lock()
	defer l.mu.Unlock()
	path := strings.TrimPrefix(r.URL.EscapedPath(), "/catalog/v1/")
	l.requests = append(l.requests, r.Method+" "+path+"?"+r.URL.RawQuery)
	l.headers = append(l.headers, r.Header.Clone())
	if l.fail != 0 {
		w.WriteHeader(l.fail)
		_, _ = w.Write([]byte(`{"error":{"message":"database password is hunter2","type":"Internal","code":500}}`))
		return
	}
	if path == "config" {
		writeJSON(w, 200, map[string]any{"overrides": map[string]string{"prefix": prefix}, "defaults": map[string]string{}})
		return
	}
	rest, ok := strings.CutPrefix(path, prefix+"/")
	if !ok {
		http.NotFound(w, r)
		return
	}
	parts := strings.Split(rest, "/")
	for i := range parts {
		parts[i], _ = url.PathUnescape(parts[i])
	}
	notFound := func(kind string) {
		writeJSON(w, 404, map[string]any{"error": map[string]any{"message": kind + " does not exist", "type": "NoSuch" + kind + "Exception", "code": 404,
			"stack": []string{"internal detail"}}})
	}
	body, _ := io.ReadAll(r.Body)
	l.bodies = append(l.bodies, string(body))
	switch {
	case len(parts) == 1 && parts[0] == "namespaces" && r.Method == "GET":
		var out [][]string
		for ns := range l.namespaces {
			out = append(out, strings.Split(ns, "\x1f"))
		}
		slices.SortFunc(out, func(a, b []string) int { return strings.Compare(strings.Join(a, "."), strings.Join(b, ".")) })
		writeJSON(w, 200, map[string]any{"namespaces": out, "next-page-token": nil})
	case len(parts) == 1 && parts[0] == "namespaces" && r.Method == "POST":
		var req struct {
			Namespace []string `json:"namespace"`
		}
		_ = json.Unmarshal(body, &req)
		l.namespaces[strings.Join(req.Namespace, "\x1f")] = true
		writeJSON(w, 200, map[string]any{"namespace": req.Namespace, "properties": map[string]string{}})
	case len(parts) == 2 && parts[0] == "tables" && parts[1] == "rename":
		var req struct{ Source, Destination identifier }
		_ = json.Unmarshal(body, &req)
		from := strings.Join(append(slices.Clone(req.Source.Namespace), req.Source.Name), "\x1f")
		loc, ok := l.tables[from]
		if !ok {
			notFound("Table")
			return
		}
		delete(l.tables, from)
		l.tables[strings.Join(append(slices.Clone(req.Destination.Namespace), req.Destination.Name), "\x1f")] = loc
		w.WriteHeader(204)
	case len(parts) >= 2 && parts[0] == "namespaces":
		ns := parts[1]
		if !l.namespaces[ns] {
			notFound("Namespace")
			return
		}
		switch {
		case len(parts) == 2 && r.Method == "GET":
			writeJSON(w, 200, map[string]any{"namespace": strings.Split(ns, "\x1f"), "properties": map[string]string{"owner": "x"}})
		case len(parts) == 2 && r.Method == "HEAD":
			w.WriteHeader(204)
		case len(parts) == 2 && r.Method == "DELETE":
			delete(l.namespaces, ns)
			w.WriteHeader(204)
		case len(parts) == 3 && parts[2] == "properties":
			writeJSON(w, 200, map[string]any{"updated": []string{}, "removed": []string{}, "missing": []string{}})
		case len(parts) == 3 && parts[2] == "tables" && r.Method == "GET":
			var ids []identifier
			for key := range l.tables {
				levels := strings.Split(key, "\x1f")
				if strings.Join(levels[:len(levels)-1], "\x1f") == ns {
					ids = append(ids, identifier{Namespace: levels[:len(levels)-1], Name: levels[len(levels)-1]})
				}
			}
			slices.SortFunc(ids, func(a, b identifier) int { return strings.Compare(a.Name, b.Name) })
			writeJSON(w, 200, map[string]any{"identifiers": ids, "next-page-token": "page-2"})
		case len(parts) == 3 && parts[2] == "tables" && r.Method == "POST":
			var req struct {
				Name     string `json:"name"`
				Location string `json:"location"`
			}
			_ = json.Unmarshal(body, &req)
			loc := req.Location
			if loc == "" {
				loc = "s3://bucket/wh/" + strings.ReplaceAll(ns, "\x1f", "-") + "/" + req.Name
			}
			l.tables[ns+"\x1f"+req.Name] = loc
			l.writeTable(w, loc)
		case len(parts) == 4 && parts[2] == "tables":
			key := ns + "\x1f" + parts[3]
			if loc, ok := l.staged[key]; ok && r.Method == "POST" {
				delete(l.staged, key)
				l.tables[key] = loc
			}
			loc, ok := l.tables[key]
			if !ok {
				notFound("Table")
				return
			}
			switch r.Method {
			case "GET":
				l.writeTable(w, loc)
			case "HEAD":
				w.WriteHeader(204)
			case "POST":
				writeJSON(w, 200, map[string]any{"metadata-location": loc + "/metadata/2.json", "metadata": map[string]any{"location": loc}})
			case "DELETE":
				delete(l.tables, key)
				w.WriteHeader(204)
			}
		default:
			http.NotFound(w, r)
		}
	default:
		http.NotFound(w, r)
	}
}

// writeTable answers with a LoadTableResult carrying Lakekeeper's own
// client settings, which Igloo must drop.
func (l *lakekeeper) writeTable(w http.ResponseWriter, loc string) {
	writeJSON(w, 200, map[string]any{
		"metadata-location":   loc + "/metadata/1.json",
		"metadata":            map[string]any{"format-version": 2, "location": loc},
		"config":              map[string]string{"s3.remote-signing-enabled": "true", "s3.signer.uri": "http://lakekeeper:8181/catalog/"},
		"storage-credentials": []any{map[string]any{"prefix": loc, "config": map[string]string{"s3.secret-access-key": "lakekeeper's"}}},
	})
}

func (l *lakekeeper) lastBody() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.bodies[len(l.bodies)-1]
}

func (l *lakekeeper) last() (string, http.Header) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.requests[len(l.requests)-1], l.headers[len(l.headers)-1]
}

// vendor fakes credential vending.
type vendor struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (v *vendor) Vend(_ context.Context, location string, write bool, user string) (Credentials, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	access := "read"
	if write {
		access = "write"
	}
	v.calls = append(v.calls, user+" "+access+" "+location)
	if v.err != nil {
		return Credentials{}, v.err
	}
	return Credentials{Prefix: location, Config: map[string]string{"s3.access-key-id": "scoped-" + access}, Expires: time.Now().Add(time.Hour)}, nil
}

type harness struct {
	t       *testing.T
	srv     *Server
	cat     *lakekeeper
	users   *users
	grants  *auth.MemoryGrants
	vendor  *vendor
	history *audit.Log
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{
		t:      t,
		cat:    &lakekeeper{namespaces: map[string]bool{}, tables: map[string]string{}, staged: map[string]string{}},
		users:  &users{users: map[string]auth.User{}, passwords: map[string]string{}},
		vendor: &vendor{},
	}
	h.grants = auth.NewMemoryGrants(h.users)
	h.history = audit.New(audit.NewMemoryStore(1000, 1000), audit.Options{}, slog.New(slog.DiscardHandler))
	catalog := httptest.NewServer(h.cat)
	t.Cleanup(catalog.Close)
	srv, err := New(Options{
		CatalogURI:   catalog.URL + "/catalog",
		Warehouse:    "igloo",
		CatalogName:  "lake",
		Users:        h.users,
		Grants:       h.grants,
		Authenticate: h.users.authenticate,
		Key:          bytes.Repeat([]byte("k"), 32),
		Credentials:  h.vendor,
		Audit:        h.history,
	})
	if err != nil {
		t.Fatal(err)
	}
	h.srv = srv

	h.users.add(t, "ada", "ada-password", auth.RoleAdmin)
	h.users.add(t, "walt", "walt-password", auth.RoleWriter)
	h.users.add(t, "rita", "rita-password", auth.RoleReader)
	h.users.add(t, "nora", "nora-password", auth.RoleNone)
	h.cat.namespaces["public"] = true
	h.cat.namespaces["secret"] = true
	h.cat.namespaces["team"] = true
	h.cat.tables["public\x1fevents"] = "s3://bucket/wh/public/events"
	h.cat.tables["public\x1fusers"] = "s3://bucket/wh/public/users"
	h.cat.tables["secret\x1fpay"] = "s3://bucket/wh/secret/pay"
	return h
}

func (h *harness) grant(grantee string, priv auth.Privilege, schema, table string) {
	h.t.Helper()
	err := h.grants.Grant(context.Background(), []auth.Grant{{Grantee: grantee, GranteeType: auth.GranteeUser,
		Privilege: priv, Object: auth.Object{Schema: schema, Table: table}}})
	if err != nil {
		h.t.Fatal(err)
	}
}

// request sends a request from a private address with token, if any.
func (h *harness) request(method, path, token string, body any, header ...string) *httptest.ResponseRecorder {
	h.t.Helper()
	var reader io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		reader = strings.NewReader(b)
	default:
		data, err := json.Marshal(b)
		if err != nil {
			h.t.Fatal(err)
		}
		reader = bytes.NewReader(data)
	}
	r := httptest.NewRequest(method, path, reader)
	r.RemoteAddr = "10.1.2.3:41000"
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	for i := 0; i+1 < len(header); i += 2 {
		r.Header.Set(header[i], header[i+1])
	}
	w := httptest.NewRecorder()
	h.srv.ServeHTTP(w, r)
	return w
}

// login gets a token for user with client credentials.
func (h *harness) login(user, password string) string {
	h.t.Helper()
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {user}, "client_secret": {password}, "scope": {"catalog"}}
	w := h.request("POST", "/v1/oauth/tokens", "", form.Encode(), "Content-Type", "application/x-www-form-urlencoded")
	if w.Code != 200 {
		h.t.Fatalf("login %s: %d %s", user, w.Code, w.Body)
	}
	var resp struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || resp.TokenType != "bearer" || resp.ExpiresIn != 3600 {
		h.t.Fatalf("token response: %s", w.Body)
	}
	return resp.AccessToken
}

// queries returns the query history, oldest first.
func (h *harness) queries() []audit.Query {
	h.t.Helper()
	qs, err := h.history.Queries(context.Background(), audit.Viewer{All: true}, audit.Page{})
	if err != nil {
		h.t.Fatal(err)
	}
	slices.Reverse(qs)
	return qs
}

func errorType(t *testing.T, w *httptest.ResponseRecorder) (string, string) {
	t.Helper()
	var e struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    int    `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil || e.Error.Code != w.Code {
		t.Fatalf("not an Iceberg error response: %d %s", w.Code, w.Body)
	}
	return e.Error.Type, e.Error.Message
}

func TestTokens(t *testing.T) {
	h := newHarness(t)

	w := h.request("GET", "/v1/config", "", nil)
	if w.Code != 401 || w.Header().Get("WWW-Authenticate") == "" {
		t.Fatalf("config without a token: %d %s", w.Code, w.Body)
	}

	token := h.login("walt", "walt-password")
	if w := h.request("GET", "/v1/config?warehouse=igloo", token, nil); w.Code != 200 {
		t.Fatalf("config: %d %s", w.Code, w.Body)
	}
	if w := h.request("GET", "/v1/config", "", nil, "Authorization", "bearer "+token); w.Code != 200 {
		t.Errorf("lower-case scheme: %d %s", w.Code, w.Body)
	}
	if w := h.request("GET", "/v1/config", "", nil, "Authorization", "Basic "+token); w.Code != 401 {
		t.Errorf("Basic scheme: %d %s", w.Code, w.Body)
	}

	form := func(kv ...string) string {
		v := url.Values{}
		for i := 0; i+1 < len(kv); i += 2 {
			v.Set(kv[i], kv[i+1])
		}
		return v.Encode()
	}
	formType := []string{"Content-Type", "application/x-www-form-urlencoded"}
	for _, c := range []struct {
		name   string
		body   string
		header []string
		status int
		code   string
	}{
		{"wrong password", form("grant_type", "client_credentials", "client_id", "walt", "client_secret", "nope"), nil, 401, "invalid_client"},
		{"unknown user", form("grant_type", "client_credentials", "client_id", "eve", "client_secret", "x"), nil, 401, "invalid_client"},
		{"no user name", form("grant_type", "client_credentials", "client_secret", "walt-password"), nil, 401, "invalid_client"},
		{"other grants", form("grant_type", "password", "username", "walt", "password", "walt-password"), nil, 400, "unsupported_grant_type"},
		{"a forged subject token", form("grant_type", grantTokenExchange, "subject_token", "forged", "subject_token_type", accessTokenType), nil, 401, "invalid_grant"},
	} {
		w := h.request("POST", "/v1/oauth/tokens", "", c.body, append(formType, c.header...)...)
		var resp struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(w.Body.Bytes(), &resp)
		if w.Code != c.status || resp.Error != c.code {
			t.Errorf("%s: got %d %s, want %d %s", c.name, w.Code, w.Body, c.status, c.code)
		}
	}

	// Client credentials in a Basic header, as RFC 6749 prefers.
	r := httptest.NewRequest("POST", "/v1/oauth/tokens", strings.NewReader(form("grant_type", "client_credentials")))
	r.RemoteAddr = "127.0.0.1:5000"
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.SetBasicAuth("rita", "rita-password")
	rec := httptest.NewRecorder()
	h.srv.ServeHTTP(rec, r)
	if rec.Code != 200 {
		t.Errorf("Basic client credentials: %d %s", rec.Code, rec.Body)
	}

	// Exchanging a token renews it within the same session.
	w = h.request("POST", "/v1/oauth/tokens", token, form("grant_type", grantTokenExchange, "subject_token", token,
		"subject_token_type", accessTokenType), formType...)
	var renewed struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &renewed); w.Code != 200 || err != nil || renewed.AccessToken == token {
		t.Fatalf("token exchange: %d %s", w.Code, w.Body)
	}
	a, _ := h.srv.tokens.open(token)
	b, _ := h.srv.tokens.open(renewed.AccessToken)
	if a.Session == "" || a.Session != b.Session || b.User != "walt" {
		t.Errorf("renewed token %+v does not continue %+v", b, a)
	}

	// An expired token is refused, and renewing one needs the credentials.
	expired, err := h.srv.tokens.seal(tokenOf(walt(h), a.Session, time.Now().Add(-time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	if w := h.request("GET", "/v1/config", expired, nil); w.Code != 401 {
		t.Errorf("expired token: %d", w.Code)
	}
	if w := h.request("POST", "/v1/oauth/tokens", "", form("grant_type", grantTokenExchange, "subject_token", expired), formType...); w.Code != 401 {
		t.Errorf("exchanging an expired token: %d %s", w.Code, w.Body)
	}
	r = httptest.NewRequest("POST", "/v1/oauth/tokens", strings.NewReader(form("grant_type", grantTokenExchange, "subject_token", expired)))
	r.RemoteAddr = "127.0.0.1:5000"
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.SetBasicAuth("walt", "walt-password")
	rec = httptest.NewRecorder()
	h.srv.ServeHTTP(rec, r)
	if rec.Code != 200 {
		t.Errorf("renewing an expired token with credentials: %d %s", rec.Code, rec.Body)
	}

	// A new password, or dropping the user, ends their tokens.
	h.users.add(t, "walt", "walt-new-password", auth.RoleWriter)
	if w := h.request("GET", "/v1/config", token, nil); w.Code != 401 || !strings.Contains(w.Body.String(), "password") {
		t.Errorf("token after a password change: %d %s", w.Code, w.Body)
	}
	ritaToken := h.login("rita", "rita-password")
	_ = h.users.Delete(context.Background(), "rita")
	if w := h.request("GET", "/v1/config", ritaToken, nil); w.Code != 401 || !strings.Contains(w.Body.String(), "no longer exists") {
		t.Errorf("token of a dropped user: %d %s", w.Code, w.Body)
	}

	// Tokens sealed with another key are not accepted.
	other, err := newSealer(bytes.Repeat([]byte("x"), 32))
	if err != nil {
		t.Fatal(err)
	}
	foreign, _ := other.seal(tokenOf(walt(h), "s", time.Now().Add(time.Hour)))
	if w := h.request("GET", "/v1/config", foreign, nil); w.Code != 401 {
		t.Errorf("token from another key: %d", w.Code)
	}
}

func walt(h *harness) auth.User {
	u, _ := h.users.Get(context.Background(), "walt")
	return u
}

// tokenOf is the token the endpoint would issue to user.
func tokenOf(user auth.User, session string, expires time.Time) token {
	s, _ := newSealer(bytes.Repeat([]byte("k"), 32))
	return token{User: user.Name, Session: session, Expires: expires.Unix(), Password: s.fingerprint(user.Verifier)}
}

func TestPrivateNetworksOnlyWithoutTLS(t *testing.T) {
	h := newHarness(t)
	token := h.login("walt", "walt-password")
	for addr, want := range map[string]int{
		"127.0.0.1:1":     200,
		"[::1]:1":         200,
		"10.0.0.1:1":      200,
		"172.18.0.4:1":    200,
		"192.168.1.9:1":   200,
		"100.64.3.2:1":    200,
		"[fd00::1]:1":     200,
		"203.0.113.10:1":  403,
		"[2001:db8::1]:1": 403,
		"8.8.8.8:1":       403,
	} {
		r := httptest.NewRequest("GET", "/v1/config", nil)
		r.RemoteAddr = addr
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		h.srv.ServeHTTP(w, r)
		if w.Code != want {
			t.Errorf("%s: got %d, want %d", addr, w.Code, want)
		}
	}
	h.srv.opts.TLS = true
	r := httptest.NewRequest("GET", "/v1/config", nil)
	r.RemoteAddr = "203.0.113.10:1"
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	h.srv.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Errorf("public client over TLS: %d", w.Code)
	}
}

func TestConfig(t *testing.T) {
	h := newHarness(t)
	token := h.login("nora", "nora-password")
	w := h.request("GET", "/v1/config?warehouse=igloo", token, nil)
	var cfg struct {
		Overrides map[string]string `json:"overrides"`
		Endpoints []string          `json:"endpoints"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &cfg); err != nil || w.Code != 200 {
		t.Fatalf("config: %d %s", w.Code, w.Body)
	}
	if cfg.Overrides["prefix"] != "" || !slices.Contains(cfg.Endpoints, "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}") ||
		slices.ContainsFunc(cfg.Endpoints, func(e string) bool { return strings.Contains(e, "views") }) {
		t.Errorf("config: %s", w.Body)
	}
	if w := h.request("GET", "/v1/config?warehouse=other", token, nil); w.Code != 404 {
		t.Errorf("another warehouse: %d", w.Code)
	}
	if w := h.request("GET", "/v1/namespaces/public/views", token, nil); w.Code != 501 {
		t.Errorf("views: %d %s", w.Code, w.Body)
	}
	if w := h.request("POST", "/v1/namespaces/public/register", token, `{}`); w.Code != 501 {
		t.Errorf("register: %d %s", w.Code, w.Body)
	}
}

func TestListingsShowWhatGrantsReach(t *testing.T) {
	h := newHarness(t)
	h.grant("nora", auth.PrivilegeSelect, "public", "events")
	h.grant("nora", auth.PrivilegeCreate, "team", "")
	nora := h.login("nora", "nora-password")

	namespaces := func(token string) string {
		w := h.request("GET", "/v1/namespaces", token, nil)
		var list struct {
			Namespaces [][]string `json:"namespaces"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil || w.Code != 200 {
			t.Fatalf("list namespaces: %d %s", w.Code, w.Body)
		}
		var names []string
		for _, ns := range list.Namespaces {
			names = append(names, schemaOf(ns))
		}
		return strings.Join(names, ",")
	}
	if got := namespaces(nora); got != "public,team" {
		t.Errorf("nora's namespaces: %s", got)
	}
	if got := namespaces(h.login("rita", "rita-password")); got != "public,secret,team" {
		t.Errorf("a reader's namespaces: %s", got)
	}

	w := h.request("GET", "/v1/namespaces/public/tables?pageToken=p1&recursive=true", nora, nil)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"events"`) || strings.Contains(w.Body.String(), `"users"`) ||
		!strings.Contains(w.Body.String(), `"next-page-token":"page-2"`) {
		t.Errorf("nora's tables: %d %s", w.Code, w.Body)
	}
	if req, _ := h.cat.last(); req != "GET "+prefix+"/namespaces/public/tables?pageToken=p1" {
		t.Errorf("forwarded %q; only paging passes on", req)
	}
	if w := h.request("GET", "/v1/namespaces/secret/tables", nora, nil); w.Code != 403 {
		t.Errorf("a schema without grants: %d", w.Code)
	}
	if w := h.request("GET", "/v1/namespaces/secret", nora, nil); w.Code != 403 {
		t.Errorf("loading a schema without grants: %d", w.Code)
	}
	if w := h.request("HEAD", "/v1/namespaces/public/tables/users", nora, nil); w.Code != 403 || w.Body.Len() != 0 {
		t.Errorf("HEAD of a table without grants: %d %q", w.Code, w.Body)
	}
	if w := h.request("HEAD", "/v1/namespaces/public/tables/events", nora, nil); w.Code != 204 {
		t.Errorf("HEAD of a granted table: %d", w.Code)
	}
	if w := h.request("HEAD", "/v1/namespaces/public/tables/nope", h.login("walt", "walt-password"), nil); w.Code != 404 {
		t.Errorf("HEAD of a missing table: %d", w.Code)
	}
}

func TestLoadTable(t *testing.T) {
	h := newHarness(t)
	h.grant("nora", auth.PrivilegeSelect, "public", "events")
	nora := h.login("nora", "nora-password")
	vended := []string{accessDelegation, "vended-credentials,remote-signing"}

	w := h.request("GET", "/v1/namespaces/public/tables/users", nora, nil, vended...)
	if typ, msg := errorType(t, w); w.Code != 403 || typ != errForbidden ||
		msg != "permission denied for nora: SELECT on table public.users needs the reader role or a SELECT grant" {
		t.Errorf("load without SELECT: %d %s", w.Code, w.Body)
	}
	// A table that does not exist is reported so, as engines load a table
	// to learn whether to create it.
	w = h.request("GET", "/v1/namespaces/public/tables/missing", nora, nil)
	if typ, _ := errorType(t, w); w.Code != 404 || typ != "NoSuchTableException" {
		t.Errorf("load of a missing table: %d %s", w.Code, w.Body)
	}
	w = h.request("GET", "/v1/namespaces/nowhere", nora, nil)
	if typ, _ := errorType(t, w); w.Code != 404 || typ != "NoSuchNamespaceException" {
		t.Errorf("load of a missing namespace: %d %s", w.Code, w.Body)
	}

	w = h.request("GET", "/v1/namespaces/public/tables/events?snapshots=refs&purgeRequested=true", nora, nil, vended...)
	if w.Code != 200 {
		t.Fatalf("load: %d %s", w.Code, w.Body)
	}
	req, header := h.cat.last()
	if req != "GET "+prefix+"/namespaces/public/tables/events?snapshots=refs" || header.Get(accessDelegation) != "" ||
		header.Get("Authorization") != "" {
		t.Errorf("forwarded %q with %v", req, header)
	}
	var result struct {
		Metadata           map[string]any    `json:"metadata"`
		MetadataLocation   string            `json:"metadata-location"`
		Config             map[string]string `json:"config"`
		StorageCredentials []map[string]any  `json:"storage-credentials"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.MetadataLocation == "" || result.Metadata["location"] != "s3://bucket/wh/public/events" {
		t.Errorf("metadata did not pass through: %s", w.Body)
	}
	if strings.Contains(w.Body.String(), "lakekeeper") || result.Config["s3.access-key-id"] != "scoped-read" ||
		result.Config["client.refresh-credentials-endpoint"] != "v1/namespaces/public/tables/events/credentials" ||
		len(result.StorageCredentials) != 1 || result.StorageCredentials[0]["prefix"] != "s3://bucket/wh/public/events" {
		t.Errorf("credentials: %s", w.Body)
	}

	// Write credentials take every row-changing privilege.
	h.grant("nora", auth.PrivilegeInsert, "public", "events")
	h.grant("nora", auth.PrivilegeUpdate, "public", "events")
	h.request("GET", "/v1/namespaces/public/tables/events", nora, nil, vended...)
	h.grant("nora", auth.PrivilegeDelete, "public", "")
	h.request("GET", "/v1/namespaces/public/tables/events", nora, nil, vended...)
	h.request("GET", "/v1/namespaces/public/tables/users", h.login("rita", "rita-password"), nil, vended...)
	h.request("GET", "/v1/namespaces/public/tables/users", h.login("walt", "walt-password"), nil, vended...)
	// Without the header, no credentials.
	w = h.request("GET", "/v1/namespaces/public/tables/events", nora, nil)
	if strings.Contains(w.Body.String(), "scoped") || strings.Contains(w.Body.String(), "storage-credentials") {
		t.Errorf("credentials nobody asked for: %s", w.Body)
	}
	want := []string{
		"nora read s3://bucket/wh/public/events",
		"nora read s3://bucket/wh/public/events",
		"nora write s3://bucket/wh/public/events",
		"rita read s3://bucket/wh/public/users",
		"walt write s3://bucket/wh/public/users",
	}
	if !slices.Equal(h.vendor.calls, want) {
		t.Errorf("vended:\n%s\nwant:\n%s", strings.Join(h.vendor.calls, "\n"), strings.Join(want, "\n"))
	}

	// When no credentials can be limited to the table, none are given; when
	// the store fails, the request does.
	h.vendor.err = ErrNoCredentials
	w = h.request("GET", "/v1/namespaces/public/tables/events", nora, nil, vended...)
	if w.Code != 200 || strings.Contains(w.Body.String(), "storage-credentials") {
		t.Errorf("without credentials: %d %s", w.Code, w.Body)
	}
	h.vendor.err = errors.New("STS is down")
	if w := h.request("GET", "/v1/namespaces/public/tables/events", nora, nil, vended...); w.Code != 503 {
		t.Errorf("vending failure: %d %s", w.Code, w.Body)
	}

	// Refreshing credentials needs what loading the table does.
	h.vendor.err = nil
	w = h.request("GET", "/v1/namespaces/public/tables/events/credentials", nora, nil)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"scoped-write"`) {
		t.Errorf("credentials endpoint: %d %s", w.Code, w.Body)
	}
	if w := h.request("GET", "/v1/namespaces/secret/tables/pay/credentials", nora, nil); w.Code != 403 {
		t.Errorf("credentials without SELECT: %d", w.Code)
	}
}

func TestCreateTable(t *testing.T) {
	h := newHarness(t)
	h.grant("nora", auth.PrivilegeCreate, "team", "")
	nora := h.login("nora", "nora-password")
	vended := []string{accessDelegation, vendedCredentials}

	if w := h.request("POST", "/v1/namespaces/public/tables", nora, map[string]any{"name": "mine"}); w.Code != 403 {
		t.Errorf("create without CREATE: %d %s", w.Code, w.Body)
	}
	if w := h.request("POST", "/v1/namespaces/team/tables", h.login("rita", "rita-password"), map[string]any{"name": "mine"}); w.Code != 403 {
		t.Errorf("create as a reader: %d %s", w.Code, w.Body)
	}
	w := h.request("POST", "/v1/namespaces/team/tables", nora, map[string]any{"name": "mine", "stage-create": true}, vended...)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"scoped-write"`) {
		t.Errorf("create with CREATE: %d %s", w.Code, w.Body)
	}
	w = h.request("POST", "/v1/namespaces/team/tables", nora, map[string]any{"name": "elsewhere", "location": "s3://bucket/wh/secret/pay"})
	if _, msg := errorType(t, w); w.Code != 403 || !strings.Contains(msg, "location") {
		t.Errorf("create with a location: %d %s", w.Code, w.Body)
	}
	w = h.request("POST", "/v1/namespaces/team/tables", h.login("walt", "walt-password"),
		map[string]any{"name": "spread", "properties": map[string]string{"write.data.path": "s3://bucket/wh/secret/pay"}})
	if w.Code != 400 {
		t.Errorf("create with a data path: %d %s", w.Code, w.Body)
	}
	ada := h.login("ada", "ada-password")
	if w := h.request("POST", "/v1/namespaces/team/tables", ada, map[string]any{"name": "placed", "location": "s3://bucket/wh/team/placed"}); w.Code != 200 {
		t.Errorf("admin creates with a location: %d %s", w.Code, w.Body)
	}
}

func TestCommits(t *testing.T) {
	h := newHarness(t)
	h.grant("nora", auth.PrivilegeInsert, "public", "events")
	nora := h.login("nora", "nora-password")
	commit := func(token string, updates ...map[string]any) *httptest.ResponseRecorder {
		return h.request("POST", "/v1/namespaces/public/tables/events", token, map[string]any{
			"requirements": []any{map[string]any{"type": "assert-table-uuid", "uuid": "x"}},
			"updates":      updates,
		})
	}
	snapshot := func(id int64, op string) map[string]any {
		return map[string]any{"action": "add-snapshot", "snapshot": map[string]any{"snapshot-id": id, "summary": map[string]string{"operation": op}}}
	}
	ref := func(id int64) map[string]any {
		return map[string]any{"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": id}
	}

	// A declared append could name an older snapshot's files, so it needs
	// what any change of rows does.
	w := commit(nora, snapshot(7, "append"), ref(7))
	if _, msg := errorType(t, w); w.Code != 403 || !strings.Contains(msg, "UPDATE on table public.events") {
		t.Errorf("append with INSERT only: %d %s", w.Code, w.Body)
	}
	if w := commit(nora, map[string]any{"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 3}); w.Code != 403 {
		t.Errorf("moving main to an older snapshot: %d %s", w.Code, w.Body)
	}
	h.grant("nora", auth.PrivilegeUpdate, "public", "events")
	h.grant("nora", auth.PrivilegeDelete, "public", "events")
	for _, op := range []string{"append", "overwrite", "delete"} {
		if w := commit(nora, snapshot(8, op), ref(8)); w.Code != 200 {
			t.Errorf("%s with INSERT, UPDATE and DELETE: %d %s", op, w.Code, w.Body)
		}
	}
	if w := commit(nora, map[string]any{"action": "add-schema", "schema": map[string]any{}}); w.Code != 403 {
		t.Errorf("schema change without CREATE: %d %s", w.Code, w.Body)
	}
	h.grant("nora", auth.PrivilegeCreate, "public", "")
	if w := commit(nora, map[string]any{"action": "add-schema", "schema": map[string]any{}}); w.Code != 200 {
		t.Errorf("schema change with CREATE: %d %s", w.Code, w.Body)
	}
	if w := commit(nora, map[string]any{"action": "set-properties", "updates": map[string]string{"history.expire.max-snapshot-age-ms": "0"}}); w.Code != 403 {
		t.Errorf("table properties without the writer role: %d %s", w.Code, w.Body)
	}
	walt := h.login("walt", "walt-password")
	if w := commit(walt, snapshot(10, "overwrite"), ref(10), map[string]any{"action": "set-properties", "updates": map[string]string{"a": "b"}}); w.Code != 200 {
		t.Errorf("writer: %d %s", w.Code, w.Body)
	}
	if w := commit(walt, map[string]any{"action": "set-properties", "updates": map[string]string{"write.object-storage.path": "s3://x"}}); w.Code != 400 {
		t.Errorf("data path property: %d %s", w.Code, w.Body)
	}
	if w := commit(walt, map[string]any{"action": "add-encryption-key"}); w.Code != 403 {
		t.Errorf("unknown update: %d %s", w.Code, w.Body)
	}
	if w := commit(h.login("ada", "ada-password"), map[string]any{"action": "add-encryption-key"}); w.Code != 200 {
		t.Errorf("unknown update as admin: %d %s", w.Code, w.Body)
	}

	// The commit that creates a staged table needs CREATE only, as CREATE
	// TABLE AS does.
	h.grant("nora", auth.PrivilegeCreate, "team", "")
	h.cat.staged["team\x1fstaged"] = "s3://bucket/wh/team/staged"
	w = h.request("POST", "/v1/namespaces/team/tables/staged", nora, map[string]any{
		"requirements": []any{map[string]any{"type": "assert-create"}},
		"updates":      []any{snapshot(1, "append"), ref(1), map[string]any{"action": "set-properties", "updates": map[string]string{"x": "y"}}},
	})
	if w.Code != 200 {
		t.Errorf("create commit: %d %s", w.Code, w.Body)
	}
	// It cannot rewrite a table that exists, which would need INSERT,
	// UPDATE and DELETE.
	h.cat.tables["team\x1fexisting"] = "s3://bucket/wh/team/existing"
	w = h.request("POST", "/v1/namespaces/team/tables/existing", nora, map[string]any{
		"requirements": []any{map[string]any{"type": "assert-create"}},
		"updates":      []any{snapshot(2, "overwrite"), ref(2)},
	})
	if typ, _ := errorType(t, w); w.Code != 409 || typ != "CommitFailedException" {
		t.Errorf("create commit on an existing table: %d %s", w.Code, w.Body)
	}
}

func TestDropAndRename(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.grant("nora", auth.PrivilegeSelect, "public", "events")
	h.grant("rita", auth.PrivilegeInsert, "public", "users")
	nora := h.login("nora", "nora-password")
	walt := h.login("walt", "walt-password")

	if w := h.request("DELETE", "/v1/namespaces/public/tables/events", nora, nil); w.Code != 403 {
		t.Errorf("drop without CREATE: %d", w.Code)
	}
	w := h.request("POST", "/v1/tables/rename", walt, map[string]any{
		"source":      map[string]any{"namespace": []string{"public"}, "name": "events"},
		"destination": map[string]any{"namespace": []string{"public"}, "name": "events_v2"},
	})
	if w.Code != 204 {
		t.Fatalf("rename: %d %s", w.Code, w.Body)
	}
	p, _ := h.grants.UserPrivileges(ctx, "nora")
	if !p.Allows(auth.PrivilegeSelect, "public", "events_v2") || p.Allows(auth.PrivilegeSelect, "public", "events") {
		t.Errorf("grants did not follow the rename: %+v", p.Grants)
	}
	if w := h.request("POST", "/v1/tables/rename", nora, map[string]any{
		"source":      map[string]any{"namespace": []string{"public"}, "name": "events_v2"},
		"destination": map[string]any{"namespace": []string{"public"}, "name": "mine"},
	}); w.Code != 403 {
		t.Errorf("rename without CREATE: %d", w.Code)
	}

	if w := h.request("DELETE", "/v1/namespaces/public/tables/users?purgeRequested=true&force=true", walt, nil); w.Code != 204 {
		t.Fatalf("drop: %d %s", w.Code, w.Body)
	}
	if req, _ := h.cat.last(); req != "DELETE "+prefix+"/namespaces/public/tables/users?purgeRequested=true" {
		t.Errorf("forwarded %q", req)
	}
	if p, _ := h.grants.UserPrivileges(ctx, "rita"); !p.Empty() {
		t.Errorf("grants outlived the dropped table: %+v", p.Grants)
	}

	if w := h.request("DELETE", "/v1/namespaces/team", nora, nil); w.Code != 403 {
		t.Errorf("drop schema as none: %d", w.Code)
	}
	h.grant("nora", auth.PrivilegeSelect, "team", "")
	if w := h.request("DELETE", "/v1/namespaces/team?recursive=true", walt, nil); w.Code != 204 {
		t.Errorf("drop schema as writer: %d %s", w.Code, w.Body)
	}
	if req, _ := h.cat.last(); req != "DELETE "+prefix+"/namespaces/team?" {
		t.Errorf("forwarded %q", req)
	}
	if p, _ := h.grants.UserPrivileges(ctx, "nora"); p.AllowsSchema(auth.PrivilegeSelect, "team") {
		t.Errorf("grants outlived the dropped schema")
	}
}

func TestNamespaces(t *testing.T) {
	h := newHarness(t)
	h.grant("nora", auth.PrivilegeCreate, "team", "")
	nora := h.login("nora", "nora-password")
	walt := h.login("walt", "walt-password")
	if w := h.request("POST", "/v1/namespaces", nora, map[string]any{"namespace": []string{"mine"}}); w.Code != 403 {
		t.Errorf("create schema without the writer role: %d", w.Code)
	}
	if w := h.request("POST", "/v1/namespaces", walt, map[string]any{"namespace": []string{"sales"}}); w.Code != 200 {
		t.Errorf("create schema: %d %s", w.Code, w.Body)
	}
	if w := h.request("POST", "/v1/namespaces", walt, map[string]any{"namespace": []string{"sales", "emea"}}); w.Code != 403 {
		t.Errorf("nested namespace as a writer: %d %s", w.Code, w.Body)
	}
	if w := h.request("POST", "/v1/namespaces", h.login("ada", "ada-password"), map[string]any{"namespace": []string{"sales", "emea"}}); w.Code != 200 {
		t.Errorf("nested namespace as an admin: %d %s", w.Code, w.Body)
	}
	if w := h.request("POST", "/v1/namespaces", walt, map[string]any{"namespace": []string{"elsewhere"},
		"properties": map[string]string{"location": "s3://bucket/wh/secret"}}); w.Code != 403 {
		t.Errorf("schema location as a writer: %d %s", w.Code, w.Body)
	}
	if w := h.request("POST", "/v1/namespaces/sales/properties", walt, map[string]any{"updates": map[string]string{"location": "s3://x"}}); w.Code != 403 {
		t.Errorf("moving a schema as a writer: %d %s", w.Code, w.Body)
	}
	if w := h.request("POST", "/v1/namespaces/sales/properties", walt, map[string]any{"updates": map[string]string{"owner": "sales"}}); w.Code != 200 {
		t.Errorf("schema properties as a writer: %d %s", w.Code, w.Body)
	}
	if w := h.request("POST", "/v1/namespaces/team/properties", nora, map[string]any{"updates": map[string]string{"owner": "me"}}); w.Code != 403 {
		t.Errorf("schema properties as none: %d %s", w.Code, w.Body)
	}
	if w := h.request("GET", "/v1/namespaces/team", nora, nil); w.Code != 200 {
		t.Errorf("load a schema with CREATE on it: %d", w.Code)
	}
	if w := h.request("HEAD", "/v1/namespaces/nope", walt, nil); w.Code != 404 {
		t.Errorf("HEAD of a missing schema: %d", w.Code)
	}
	// Nested namespaces are named with dots, as grants name them.
	h.cat.namespaces["team\x1fsub"] = true
	h.cat.tables["team\x1fsub\x1fdeep"] = "s3://bucket/wh/team-sub/deep"
	h.grant("nora", auth.PrivilegeSelect, "team.sub", "deep")
	if w := h.request("GET", "/v1/namespaces/team%1Fsub/tables/deep", nora, nil); w.Code != 200 {
		t.Errorf("load from a nested namespace: %d %s", w.Code, w.Body)
	}
}

func TestHistory(t *testing.T) {
	h := newHarness(t)
	h.grant("nora", auth.PrivilegeSelect, "public", "events")
	nora := h.login("nora", "nora-password")
	vended := []string{accessDelegation, vendedCredentials, "X-Client-Version", "Apache Iceberg 1.10.0"}
	h.request("GET", "/v1/config", nora, nil)
	h.request("GET", "/v1/namespaces/public/tables/events", nora, nil, vended...)
	h.request("GET", "/v1/namespaces/public/tables/users", nora, nil, vended...)
	h.request("POST", "/v1/namespaces/public/tables/events/metrics", nora, `{}`)
	h.request("POST", "/v1/namespaces/public/tables/events", h.login("walt", "walt-password"), map[string]any{
		"updates": []any{map[string]any{"action": "add-snapshot", "snapshot": map[string]any{"snapshot-id": 1, "summary": map[string]string{"operation": "append"}}},
			map[string]any{"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 1}},
	})

	qs := h.queries()
	if len(qs) != 3 {
		t.Fatalf("got %d records, want 3: %+v", len(qs), qs)
	}
	load, denied, commit := qs[0], qs[1], qs[2]
	if load.User != "nora" || load.Role != "none" || load.StatementType != opLoadTable || load.Outcome != audit.OutcomeOK ||
		load.Text != "GET /v1/namespaces/public/tables/events (read credentials)" ||
		!slices.Equal(load.TablesRead, []string{"lake.public.events"}) || load.ApplicationName != "iceberg-rest: Apache Iceberg 1.10.0" ||
		load.SessionID == "" || load.ClientAddress != "10.1.2.3:41000" || load.Rows != -1 {
		t.Errorf("load: %+v", load)
	}
	if denied.Outcome != audit.OutcomeDenied || denied.SQLState != "42501" || !slices.Equal(denied.TablesRead, []string{"lake.public.users"}) {
		t.Errorf("denied: %+v", denied)
	}
	if got := clientName(&http.Request{Header: http.Header{"User-Agent": {strings.Repeat("é", 99) + "\xff\xfe" + "é"}}}); !utf8.ValidString(got) ||
		utf8.RuneCountInString(got) != len("iceberg-rest: ")+100+3 {
		t.Errorf("client name %q", got)
	}
	if commit.User != "walt" || commit.StatementType != opUpdateTable || commit.Text != "POST /v1/namespaces/public/tables/events (add-snapshot (append), set-snapshot-ref)" ||
		!slices.Equal(commit.TablesWritten, []string{"lake.public.events"}) || commit.SessionID == load.SessionID {
		t.Errorf("commit: %+v", commit)
	}
}

func TestCatalogErrors(t *testing.T) {
	h := newHarness(t)
	walt := h.login("walt", "walt-password")
	w := h.request("GET", "/v1/namespaces/public/tables/nope", walt, nil)
	if typ, msg := errorType(t, w); w.Code != 404 || typ != "NoSuchTableException" || msg != "Table does not exist" || strings.Contains(w.Body.String(), "internal") {
		t.Errorf("missing table: %d %s", w.Code, w.Body)
	}
	if qs := h.queries(); len(qs) != 1 || qs[0].Outcome != audit.OutcomeError || qs[0].SQLState != "42704" {
		t.Errorf("history: %+v", qs)
	}
	h.cat.fail = 500
	w = h.request("GET", "/v1/namespaces", walt, nil)
	if _, msg := errorType(t, w); w.Code != 500 || strings.Contains(msg, "hunter2") {
		t.Errorf("catalog failure: %d %s", w.Code, w.Body)
	}
}

func TestRequestsMeanTheSameToLakekeeper(t *testing.T) {
	h := newHarness(t)
	for _, p := range rowChanges {
		h.grant("nora", p, "public", "events")
	}
	h.grant("nora", auth.PrivilegeCreate, "team", "")
	nora := h.login("nora", "nora-password")

	// Keys that differ only in case could be read one way here and another
	// in Lakekeeper.
	for _, body := range []string{
		`{"updates":[{"action":"set-properties","ACTION":"add-snapshot","updates":{"a":"b"},
			"snapshot":{"snapshot-id":1,"summary":{"operation":"append"}}}]}`,
		`{"updates":[{"action":"add-snapshot","snapshot":{"snapshot-id":1,"summary":{"operation":"append","OPERATION":"overwrite"}}}]}`,
		`{"updates":[{"action":"add-snapshot","snapshot":{"snapshot-id":1,"summary":{"operation":"append","\u212aey":"x","key":"y"}}}]}`,
		`{"updates":[]} {"updates":[]}`,
	} {
		w := h.request("POST", "/v1/namespaces/public/tables/events", nora, body)
		if typ, _ := errorType(t, w); w.Code != 400 || typ != errBadRequest {
			t.Errorf("%s: %d %s", body, w.Code, w.Body)
		}
	}
	w := h.request("POST", "/v1/namespaces/team/tables", nora, `{"name":"x","Location":"s3://bucket/wh/secret/pay"}`)
	if w.Code != 403 {
		t.Errorf("a location in another case: %d %s", w.Code, w.Body)
	}

	// What reaches Lakekeeper is what was checked, with repeated keys
	// resolved and 64-bit numbers exact.
	w = h.request("POST", "/v1/namespaces/public/tables/events", nora,
		`{"updates":[{"action":"add-snapshot","action":"add-snapshot","snapshot":{"snapshot-id":9007199254740993,
			"summary":{"operation":"append"}}},{"action":"set-snapshot-ref","ref-name":"main","snapshot-id":9007199254740993}]}`)
	if w.Code != 200 {
		t.Fatalf("append: %d %s", w.Code, w.Body)
	}
	if got := h.cat.lastBody(); strings.Count(got, `"action"`) != 2 || strings.Count(got, "9007199254740993") != 2 {
		t.Errorf("forwarded %s", got)
	}
}

func TestDotSegmentsAreRefused(t *testing.T) {
	h := newHarness(t)
	walt := h.login("walt", "walt-password")
	for _, path := range []string{
		"/v1/namespaces/%2E%2E/tables/rename",
		"/v1/namespaces/public/tables/%2E%2E",
		"/v1/namespaces/public%1F%2E%2E/tables/x",
	} {
		w := h.request("POST", path, walt, `{"updates":[]}`)
		if w.Code == 200 || w.Code == 204 {
			t.Errorf("%s: %d", path, w.Code)
		}
	}
	for _, req := range h.cat.requests {
		if strings.Contains(req, "..") || strings.Contains(req, "%2E") {
			t.Errorf("reached the catalog: %s", req)
		}
	}
	if w := h.request("POST", "/v1/namespaces", walt, map[string]any{"namespace": []string{".."}}); w.Code != 400 {
		t.Errorf("namespace ..: %d", w.Code)
	}
	if w := h.request("POST", "/v1/namespaces/public/tables", walt, map[string]any{"name": "."}); w.Code != 400 {
		t.Errorf("table .: %d", w.Code)
	}
}

func TestMovingTablesBetweenSchemas(t *testing.T) {
	h := newHarness(t)
	move := func(token string) int {
		return h.request("POST", "/v1/tables/rename", token, map[string]any{
			"source":      map[string]any{"namespace": []string{"secret"}, "name": "pay"},
			"destination": map[string]any{"namespace": []string{"public"}, "name": "pay"},
		}).Code
	}
	if code := move(h.login("walt", "walt-password")); code != 403 {
		t.Errorf("writer moves a table to another schema: %d", code)
	}
	if code := move(h.login("ada", "ada-password")); code != 204 {
		t.Errorf("admin moves a table to another schema: %d", code)
	}
}
