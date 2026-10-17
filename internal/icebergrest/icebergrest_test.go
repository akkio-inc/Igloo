package icebergrest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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

	"github.com/xdg-go/scram"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
)

func init() { failedTokenDelay = 0 }

// users is a writable in-memory user store that also knows passwords.
type users struct {
	mu        sync.Mutex
	users     map[string]auth.User
	passwords map[string]string
	// blocked users' password sign-ins are refused for this long, as after
	// too many failures.
	blocked map[string]time.Duration
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
	if wait := s.blocked[name]; wait > 0 {
		return auth.User{}, &LoginBlockedError{Wait: wait}
	}
	u, ok := s.users[name]
	switch {
	case !ok || password == "" || s.passwords[name] != password:
		return auth.User{}, ErrLoginFailed
	case u.Disabled:
		return auth.User{}, ErrUserDisabled
	}
	return u, nil
}

// change applies fn to a user, as ALTER USER does.
func (s *users) change(name string, fn func(*auth.User)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u := s.users[name]
	fn(&u)
	s.users[name] = u
}

// tokens fakes the access token store, and sign-ins with tokens as
// pgwire.Server.AuthenticateToken makes them.
type tokens struct {
	mu      sync.Mutex
	users   *users
	byID    map[string]auth.Token
	secrets map[string]string
}

func (s *tokens) mint(t *testing.T, user string, lifetime time.Duration) (string, auth.Token) {
	t.Helper()
	secret, tok, _, err := auth.NewToken(user, "job", "ada", time.Now().Add(lifetime), scram.KeyFactors{Salt: "salt", Iters: 4096})
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byID[tok.ID] = tok
	s.secrets[secret] = tok.ID
	return secret, tok
}

func (s *tokens) authenticate(ctx context.Context, name, secret string, _ net.Addr) (auth.User, auth.Token, error) {
	s.mu.Lock()
	tok, ok := s.byID[s.secrets[secret]]
	s.mu.Unlock()
	if !ok || tok.Expired(time.Now()) || name != "" && name != tok.User {
		return auth.User{}, auth.Token{}, ErrLoginFailed
	}
	u, err := s.users.Get(ctx, tok.User)
	if err != nil || u.Disabled {
		return auth.User{}, auth.Token{}, ErrLoginFailed
	}
	return u, tok, nil
}

// expire makes a token expire now.
func (s *tokens) expire(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.byID[id]
	t.ExpiresAt = time.Now().Add(-time.Second)
	s.byID[id] = t
}

func (s *tokens) TokenByID(_ context.Context, id string) (auth.Token, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.byID[id]
	if !ok {
		return auth.Token{}, nil, auth.ErrTokenNotFound
	}
	return t, nil, nil
}

func (s *tokens) DropToken(_ context.Context, _, name string) (auth.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.byID[name]
	delete(s.byID, name)
	if !ok {
		return auth.Token{}, auth.ErrTokenNotFound
	}
	return t, nil
}

func (s *tokens) CreateToken(context.Context, auth.Token, auth.TokenSecret) error { return nil }
func (s *tokens) Tokens(context.Context, string) ([]auth.Token, error)            { return nil, nil }
func (s *tokens) TokenCredentials(context.Context, string) ([]auth.TokenCredential, error) {
	return nil, nil
}
func (s *tokens) TokenUsed(context.Context, string, time.Time) error { return nil }

// lakekeeper fakes the catalog's Iceberg REST API for one warehouse.
type lakekeeper struct {
	mu sync.Mutex
	// namespaces and tables are keyed by namespace levels joined by \x1f;
	// tables map "ns\x1ftable" to its location.
	// As in Lakekeeper, tables are at the namespaces' location, warehouse,
	// in a directory named for their UUID, unless uuids says otherwise.
	namespaces map[string]bool
	tables     map[string]string
	uuids      map[string]string
	// props are tables' properties, by the same keys.
	props map[string]map[string]string
	// staged tables answer commits but, as in Lakekeeper, not HEAD.
	staged map[string]string
	// metadata are more fields of tables' metadata, such as schemas.
	metadata map[string]map[string]any
	// requests are what reached the catalog: "METHOD path?query".
	requests []string
	headers  []http.Header
	bodies   []string
	// fail makes every request fail with this status.
	fail int
}

const (
	prefix    = "4b1c-warehouse"
	warehouse = "s3://bucket/wh"
)

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
		// Lakekeeper reads "+" in names as a space.
		parts[i], _ = url.PathUnescape(parts[i])
		parts[i] = strings.ReplaceAll(parts[i], "+", " ")
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
			writeJSON(w, 200, map[string]any{"namespace": strings.Split(ns, "\x1f"), "properties": map[string]string{"owner": "x", "location": warehouse}})
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
				loc = warehouse + "/u-" + req.Name
			}
			l.tables[ns+"\x1f"+req.Name] = loc
			l.writeTable(w, ns+"\x1f"+req.Name, loc)
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
				l.writeTable(w, key, loc)
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
func (l *lakekeeper) writeTable(w http.ResponseWriter, key, loc string) {
	uuid, ok := l.uuids[key]
	if !ok {
		uuid = loc[strings.LastIndex(loc, "/")+1:]
	}
	meta := map[string]any{"format-version": 2, "location": loc, "table-uuid": uuid, "properties": l.props[key]}
	for k, v := range l.metadata[key] {
		meta[k] = v
	}
	writeJSON(w, 200, map[string]any{
		"metadata-location":   loc + "/metadata/1.json",
		"metadata":            meta,
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

func (v *vendor) Vend(_ context.Context, location string, a Access, user string) (Credentials, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	access := map[Access]string{ReadFiles: "read", WriteFiles: "write", AddFiles: "add"}[a]
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
	tokens  *tokens
	history *audit.Log
	// files are the files in the warehouse's storage (Options.Exists).
	files sync.Map
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{
		t:      t,
		cat:    &lakekeeper{namespaces: map[string]bool{}, tables: map[string]string{}, uuids: map[string]string{}, staged: map[string]string{}},
		users:  &users{users: map[string]auth.User{}, passwords: map[string]string{}, blocked: map[string]time.Duration{}},
		vendor: &vendor{},
	}
	h.tokens = &tokens{users: h.users, byID: map[string]auth.Token{}, secrets: map[string]string{}}
	h.grants = auth.NewMemoryGrants(h.users)
	h.history = audit.New(audit.NewMemoryStore(1000, 1000), audit.Options{}, slog.New(slog.DiscardHandler))
	catalog := httptest.NewServer(h.cat)
	t.Cleanup(catalog.Close)
	srv, err := New(Options{
		CatalogURI:        catalog.URL + "/catalog",
		Warehouse:         "igloo",
		CatalogName:       "lake",
		Users:             h.users,
		Grants:            h.grants,
		Owners:            h.grants,
		Authenticate:      h.users.authenticate,
		AuthenticateToken: h.tokens.authenticate,
		Tokens:            h.tokens,
		Key:               bytes.Repeat([]byte("k"), 32),
		Credentials:       h.vendor,
		Audit:             h.history,
		StorageRoot:       warehouse,
		Exists: func(_ context.Context, path string) (bool, error) {
			_, ok := h.files.Load(path)
			return ok, nil
		},
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
	h.cat.tables["public\x1fevents"] = warehouse + "/u-events"
	h.cat.tables["public\x1fusers"] = warehouse + "/u-users"
	h.cat.tables["secret\x1fpay"] = warehouse + "/u-pay"
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
	// The table's load, then its namespace's, whose location tells where
	// the table should be.
	h.cat.mu.Lock()
	n := len(h.cat.requests)
	req, header, after := h.cat.requests[n-2], h.cat.headers[n-2], h.cat.requests[n-1]
	h.cat.mu.Unlock()
	if req != "GET "+prefix+"/namespaces/public/tables/events?snapshots=refs" || header.Get(accessDelegation) != "" ||
		header.Get("Authorization") != "" || after != "GET "+prefix+"/namespaces/public?" {
		t.Errorf("forwarded %q with %v, then %q", req, header, after)
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
	if result.MetadataLocation == "" || result.Metadata["location"] != warehouse+"/u-events" {
		t.Errorf("metadata did not pass through: %s", w.Body)
	}
	if strings.Contains(w.Body.String(), "lakekeeper") || result.Config["s3.access-key-id"] != "scoped-read" ||
		result.Config["client.refresh-credentials-endpoint"] != "v1/namespaces/public/tables/events/credentials" ||
		len(result.StorageCredentials) != 1 || result.StorageCredentials[0]["prefix"] != warehouse+"/u-events" {
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
		"nora read s3://bucket/wh/u-events",
		"nora read s3://bucket/wh/u-events",
		"nora write s3://bucket/wh/u-events",
		"rita read s3://bucket/wh/u-users",
		"walt write s3://bucket/wh/u-users",
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

// A table that is not where Lakekeeper put it, in its namespace's location
// under its UUID, gets no credentials: they could reach another table's
// files, or a directory above or below another table's.
func TestNoCredentialsForMovedTables(t *testing.T) {
	h := newHarness(t)
	h.grant("nora", auth.PrivilegeSelect, "public", "")
	nora := h.login("nora", "nora-password")
	vended := []string{accessDelegation, vendedCredentials}
	for name, loc := range map[string]string{
		"onto":   warehouse + "/u-pay",           // another table's directory
		"below":  warehouse + "/u-pay/u-below",   // inside another table's
		"above":  warehouse,                      // above every table's
		"nested": warehouse + "/team/u-nested",   // not in its namespace's location
		"slash":  warehouse + "/u-slash/",        // its own, spelled with a slash
		"placed": "s3://bucket/elsewhere/placed", // chosen by an admin
	} {
		h.cat.tables["public\x1f"+name] = loc
		h.cat.uuids["public\x1f"+name] = "u-" + name
	}
	h.cat.uuids["public\x1fplaced"] = "placed"
	for _, name := range []string{"onto", "below", "above", "nested", "placed"} {
		w := h.request("GET", "/v1/namespaces/public/tables/"+name, nora, nil, vended...)
		if w.Code != 200 || strings.Contains(w.Body.String(), "storage-credentials") {
			t.Errorf("%s: %d %s", name, w.Code, w.Body)
		}
		w = h.request("GET", "/v1/namespaces/public/tables/"+name+"/credentials", nora, nil)
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"storage-credentials":[]`) {
			t.Errorf("%s credentials: %d %s", name, w.Code, w.Body)
		}
	}
	if w := h.request("GET", "/v1/namespaces/public/tables/slash", nora, nil, vended...); !strings.Contains(w.Body.String(), "scoped-read") {
		t.Errorf("a table's own location with a slash: %d %s", w.Code, w.Body)
	}
	// Admins, who may place tables, get credentials for wherever they are.
	if w := h.request("GET", "/v1/namespaces/public/tables/placed", h.login("ada", "ada-password"), nil, vended...); !strings.Contains(w.Body.String(), "scoped-write") {
		t.Errorf("admin: %d %s", w.Code, w.Body)
	}
	if want := []string{"nora read " + warehouse + "/u-slash/", "ada write s3://bucket/elsewhere/placed"}; !slices.Equal(h.vendor.calls, want) {
		t.Errorf("vended %q", h.vendor.calls)
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

// TestCreatorsOwn checks that engines' creators own what they create, as
// in SQL, and that owners follow renames and go with drops.
func TestCreatorsOwn(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.grant("nora", auth.PrivilegeCreate, "team", "")
	nora := h.login("nora", "nora-password")
	walt := h.login("walt", "walt-password")
	owners := func() []string {
		t.Helper()
		ws, err := h.grants.ObjectOwners(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, w := range ws {
			out = append(out, w.Object.String()+"="+w.Owner)
		}
		slices.Sort(out)
		return out
	}

	if w := h.request("POST", "/v1/namespaces/team/tables", nora, map[string]any{"name": "mine"}); w.Code != 200 {
		t.Fatalf("create: %d %s", w.Code, w.Body)
	}
	if w := h.request("POST", "/v1/namespaces/public/tables", nora, map[string]any{"name": "refused"}); w.Code != 403 {
		t.Fatalf("create without CREATE: %d %s", w.Code, w.Body)
	}
	if w := h.request("POST", "/v1/namespaces", walt, map[string]any{"namespace": []string{"marts"}}); w.Code != 200 {
		t.Fatalf("create namespace: %d %s", w.Code, w.Body)
	}
	if got, want := owners(), []string{"marts=walt", "team.mine=nora"}; !slices.Equal(got, want) {
		t.Errorf("owners: %q, want %q", got, want)
	}
	if w := h.request("POST", "/v1/tables/rename", walt, map[string]any{
		"source":      map[string]any{"namespace": []string{"team"}, "name": "mine"},
		"destination": map[string]any{"namespace": []string{"team"}, "name": "ours"},
	}); w.Code != 204 {
		t.Fatalf("rename: %d %s", w.Code, w.Body)
	}
	if w := h.request("DELETE", "/v1/namespaces/marts", walt, nil); w.Code != 204 {
		t.Fatalf("drop namespace: %d %s", w.Code, w.Body)
	}
	if got, want := owners(), []string{"team.ours=nora"}; !slices.Equal(got, want) {
		t.Errorf("owners after a rename and a drop: %q, want %q", got, want)
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

	// Moving a table or changing its UUID could point it at another
	// table's files.
	for _, u := range []map[string]any{{"action": "set-location", "location": warehouse + "/u-pay"}, {"action": "assign-uuid", "uuid": "u-pay"}} {
		if w := commit(walt, u); w.Code != 403 {
			t.Errorf("%s as a writer: %d %s", u["action"], w.Code, w.Body)
		}
	}

	// The commit that creates a staged table needs CREATE only, as CREATE
	// TABLE AS does. Its UUID and location are the staged table's, so the
	// ones it restates are left out, whatever they say.
	h.grant("nora", auth.PrivilegeCreate, "team", "")
	create := func(token, name string) *httptest.ResponseRecorder {
		h.cat.staged["team\x1f"+name] = warehouse + "/u-" + name
		return h.request("POST", "/v1/namespaces/team/tables/"+name, token, map[string]any{
			"requirements": []any{map[string]any{"type": "assert-create"}},
			"updates": []any{map[string]any{"action": "assign-uuid", "uuid": "u-pay"}, map[string]any{"action": "set-location", "location": warehouse + "/u-pay"},
				snapshot(1, "append"), ref(1), map[string]any{"action": "set-properties", "updates": map[string]string{"x": "y"}}},
		})
	}
	if w := create(nora, "staged"); w.Code != 200 {
		t.Errorf("create commit: %d %s", w.Code, w.Body)
	}
	if got := h.cat.lastBody(); strings.Contains(got, "assign-uuid") || strings.Contains(got, "set-location") ||
		!strings.Contains(got, "add-snapshot") || !strings.Contains(got, "set-properties") {
		t.Errorf("forwarded %s", got)
	}
	if w := create(h.login("ada", "ada-password"), "by_admin"); w.Code != 200 || !strings.Contains(h.cat.lastBody(), "set-location") {
		t.Errorf("an admin's create commit: %d %s", w.Code, h.cat.lastBody())
	}
	// Its other updates are checked as in any commit.
	h.cat.staged["team\x1fodd"] = warehouse + "/u-odd"
	w = h.request("POST", "/v1/namespaces/team/tables/odd", nora, map[string]any{
		"requirements": []any{map[string]any{"type": "assert-create"}},
		"updates":      []any{map[string]any{"action": "add-encryption-key"}},
	})
	if w.Code != 403 {
		t.Errorf("unknown update in a create commit: %d %s", w.Code, w.Body)
	}
	w = h.request("POST", "/v1/namespaces/team/tables/odd", nora, map[string]any{
		"requirements": []any{map[string]any{"type": "assert-create"}},
		"updates":      []any{map[string]any{"action": "set-properties", "updates": map[string]string{"write.data.path": warehouse + "/u-pay"}}},
	})
	if w.Code != 400 {
		t.Errorf("data path in a create commit: %d %s", w.Code, w.Body)
	}
	// It cannot rewrite a table that exists, which would need INSERT,
	// UPDATE and DELETE.
	h.cat.tables["team\x1fexisting"] = warehouse + "/u-existing"
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
	// Only admins use nested namespaces, which Igloo's SQL does not show,
	// so no grant names them: joining levels with dots, a grant on
	// schema "team.sub" could otherwise name a nested one.
	h.cat.namespaces["team\x1fsub"] = true
	h.cat.tables["team\x1fsub\x1fdeep"] = warehouse + "/u-deep"
	h.grant("nora", auth.PrivilegeSelect, "team.sub", "deep")
	for _, path := range []string{"/v1/namespaces/team%1Fsub/tables/deep", "/v1/namespaces/team%1Fsub", "/v1/namespaces/team%1Fsub/tables"} {
		if w := h.request("GET", path, nora, nil); w.Code != 403 {
			t.Errorf("%s as none: %d %s", path, w.Code, w.Body)
		}
	}
	if w := h.request("GET", "/v1/namespaces/team%1Fsub/tables/deep", h.login("rita", "rita-password"), nil); w.Code != 403 {
		t.Errorf("load from a nested namespace as a reader: %d %s", w.Code, w.Body)
	}
	ada := h.login("ada", "ada-password")
	if w := h.request("GET", "/v1/namespaces/team%1Fsub/tables/deep", ada, nil); w.Code != 200 {
		t.Errorf("load from a nested namespace as an admin: %d %s", w.Code, w.Body)
	}
	listed := func(token string) string {
		return h.request("GET", "/v1/namespaces?parent=team", token, nil).Body.String()
	}
	if strings.Contains(listed(walt), "sub") || !strings.Contains(listed(ada), "sub") {
		t.Errorf("listing nested namespaces: writer %s, admin %s", listed(walt), listed(ada))
	}
	// A level with a dot would make ["a", "b"] and ["a.b"] one schema.
	for _, body := range []any{map[string]any{"namespace": []string{"team.sub"}}, map[string]any{"namespace": []string{"a\x1fb"}}} {
		if w := h.request("POST", "/v1/namespaces", ada, body); w.Code != 400 {
			t.Errorf("namespace %v: %d %s", body, w.Code, w.Body)
		}
	}
	if w := h.request("GET", "/v1/namespaces/team.sub/tables/deep", ada, nil); w.Code != 400 {
		t.Errorf("a path level with a dot: %d %s", w.Code, w.Body)
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
	if typ, msg := errorType(t, w); w.Code != 404 || typ != "NoSuchTableException" || msg != "the table does not exist" || strings.Contains(w.Body.String(), "internal") {
		t.Errorf("missing table: %d %s", w.Code, w.Body)
	}
	if qs := h.queries(); len(qs) != 1 || qs[0].Outcome != audit.OutcomeError || qs[0].SQLState != "42704" {
		t.Errorf("history: %+v", qs)
	}
	// Lakekeeper's messages can name its internal IDs and locations, so
	// clients get the status, the type when they act on it, and Igloo's
	// words.
	for status, want := range map[int]struct {
		status      int
		kind, words string
	}{
		500: {500, errInternal, "the catalog failed"},
		400: {400, errBadRequest, "the catalog refused the request: Internal"},
		409: {409, "CommitFailedException", "the catalog refused the request: Internal"},
		404: {404, "NotFoundException", "the catalog refused the request: Internal"},
		403: {500, errInternal, "the catalog refused Igloo's request"},
	} {
		h.cat.fail = status
		w = h.request("GET", "/v1/namespaces", walt, nil)
		if typ, msg := errorType(t, w); w.Code != want.status || typ != want.kind || !strings.HasPrefix(msg, want.words) || strings.Contains(msg, "hunter2") {
			t.Errorf("catalog answering %d: %d %s", status, w.Code, w.Body)
		}
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

// posts counts the commits and other POSTs that reached the catalog.
func (l *lakekeeper) posts() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, r := range l.requests {
		if strings.HasPrefix(r, "POST ") {
			n++
		}
	}
	return n
}

// Lakekeeper commits to the body's table when it names the path's
// namespace as namespace and table (a Spark branch commit), so a commit
// whose body names another table than its path is refused.
func TestCommitsNameOneTable(t *testing.T) {
	h := newHarness(t)
	for _, p := range rowChanges {
		h.grant("nora", p, "public", "users")
	}
	nora := h.login("nora", "nora-password")
	ada := h.login("ada", "ada-password")
	commit := func(token, path string, id any) *httptest.ResponseRecorder {
		body := map[string]any{"updates": []any{map[string]any{"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 1}}}
		if id != nil {
			body["identifier"] = id
		}
		return h.request("POST", path, token, body)
	}
	before := h.cat.posts()
	for _, c := range []struct {
		token, path string
		id          any
	}{
		{nora, "/v1/namespaces/public/tables/users", map[string]any{"namespace": []string{"public"}, "name": "events"}},
		{nora, "/v1/namespaces/public/tables/users", map[string]any{"namespace": []string{"PUBLIC"}, "name": "users"}},
		{nora, "/v1/namespaces/public/tables/users", map[string]any{}},
		{ada, "/v1/namespaces/public%1Fevents/tables/branch_x", map[string]any{"namespace": []string{"public"}, "name": "events"}},
	} {
		if w := commit(c.token, c.path, c.id); w.Code != 400 {
			t.Errorf("%s with %v: %d %s", c.path, c.id, w.Code, w.Body)
		}
	}
	if h.cat.posts() != before {
		t.Error("a mismatched commit reached the catalog")
	}
	if w := commit(nora, "/v1/namespaces/public/tables/users", map[string]any{"namespace": []string{"public"}, "name": "users"}); w.Code != 200 {
		t.Errorf("matching identifier: %d %s", w.Code, w.Body)
	}
	if w := commit(nora, "/v1/namespaces/public/tables/users", nil); w.Code != 200 {
		t.Errorf("no identifier: %d %s", w.Code, w.Body)
	}
}

// Lakekeeper reads "+" in a path's names as a space, so Igloo must too, or
// it would check one table and serve another.
func TestPlusInPathsIsASpace(t *testing.T) {
	h := newHarness(t)
	h.cat.tables["public\x1fa b"] = warehouse + "/u-ab"
	h.grant("nora", auth.PrivilegeSelect, "public", "a+b")
	nora := h.login("nora", "nora-password")
	for _, path := range []string{"/v1/namespaces/public/tables/a+b", "/v1/namespaces/public/tables/a%2Bb"} {
		if w := h.request("GET", path, nora, nil); w.Code != 403 {
			t.Errorf("%s with SELECT on \"a+b\" only: %d %s", path, w.Code, w.Body)
		}
	}
	h.grant("nora", auth.PrivilegeSelect, "public", "a b")
	if w := h.request("GET", "/v1/namespaces/public/tables/a+b", nora, nil); w.Code != 200 {
		t.Fatalf("with SELECT on \"a b\": %d %s", w.Code, w.Body)
	}
	if req, _ := h.cat.last(); req != "GET "+prefix+"/namespaces/public/tables/a%20b?" {
		t.Errorf("forwarded %q", req)
	}
	if qs := h.queries(); !slices.Equal(qs[len(qs)-1].TablesRead, []string{"lake.public.a b"}) {
		t.Errorf("history: %+v", qs[len(qs)-1])
	}
}

// Lakekeeper renames a table to its own name; that must not drop its
// grants, whose new name would be the old one.
func TestRenameToItsOwnName(t *testing.T) {
	h := newHarness(t)
	h.grant("nora", auth.PrivilegeSelect, "public", "events")
	walt := h.login("walt", "walt-password")
	for _, name := range []string{"events", "EVENTS"} {
		w := h.request("POST", "/v1/tables/rename", walt, map[string]any{
			"source":      map[string]any{"namespace": []string{"public"}, "name": "events"},
			"destination": map[string]any{"namespace": []string{"public"}, "name": name},
		})
		if w.Code != 204 {
			t.Fatalf("rename to %s: %d %s", name, w.Code, w.Body)
		}
		if p, _ := h.grants.UserPrivileges(context.Background(), "nora"); !p.Allows(auth.PrivilegeSelect, "public", "events") {
			t.Errorf("rename to %s dropped the grants", name)
		}
	}
	// Nested namespaces are admins' alone, as source or destination.
	w := h.request("POST", "/v1/tables/rename", walt, map[string]any{
		"source":      map[string]any{"namespace": []string{"public"}, "name": "events"},
		"destination": map[string]any{"namespace": []string{"public", "x"}, "name": "events"},
	})
	if w.Code != 403 {
		t.Errorf("rename into a nested namespace: %d %s", w.Code, w.Body)
	}
}

// Tables with masking or row access policies are read through Igloo's SQL:
// an engine with their metadata and files would bypass the policies.
func TestTablesWithDataPolicies(t *testing.T) {
	h := newHarness(t)
	var (
		mu        sync.Mutex
		protected = map[string]bool{"public.events": true, "team.masked": true}
		failure   error
	)
	h.srv.opts.Protected = func(_ context.Context, o auth.Object) (bool, error) {
		mu.Lock()
		defer mu.Unlock()
		return protected[strings.ToLower(o.String())], failure
	}
	for _, p := range append(slices.Clone(rowChanges), auth.PrivilegeSelect) {
		h.grant("nora", p, "public", "events")
	}
	h.grant("nora", auth.PrivilegeCreate, "team", "")
	nora := h.login("nora", "nora-password")
	rita := h.login("rita", "rita-password")
	walt := h.login("walt", "walt-password")
	ada := h.login("ada", "ada-password")
	vended := []string{accessDelegation, vendedCredentials}
	refused := func(what string, w *httptest.ResponseRecorder) {
		t.Helper()
		if typ, msg := errorType(t, w); w.Code != 403 || typ != errForbidden || !strings.Contains(msg, "data policies") ||
			!strings.Contains(msg, "Igloo's SQL or Flight SQL") {
			t.Errorf("%s: %d %s", what, w.Code, w.Body)
		}
	}
	for name, token := range map[string]string{"a user with SELECT": nora, "a reader": rita, "a writer": walt} {
		refused(name+" loads", h.request("GET", "/v1/namespaces/public/tables/events", token, nil, vended...))
		refused(name+" refreshes credentials", h.request("GET", "/v1/namespaces/public/tables/events/credentials", token, nil))
	}
	refused("a commit", h.request("POST", "/v1/namespaces/public/tables/events", nora, map[string]any{
		"updates": []any{map[string]any{"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 1}}}))
	refused("creating a protected name", h.request("POST", "/v1/namespaces/team/tables", nora, map[string]any{"name": "masked"}))
	h.cat.staged["team\x1fmasked"] = warehouse + "/u-masked"
	refused("creating a protected name by commit", h.request("POST", "/v1/namespaces/team/tables/masked", nora, map[string]any{
		"requirements": []any{map[string]any{"type": "assert-create"}}, "updates": []any{}}))
	rename := func(from, to string) *httptest.ResponseRecorder {
		return h.request("POST", "/v1/tables/rename", walt, map[string]any{
			"source":      map[string]any{"namespace": []string{"public"}, "name": from},
			"destination": map[string]any{"namespace": []string{"public"}, "name": to},
		})
	}
	refused("renaming a protected table away", rename("events", "events_raw"))
	refused("renaming a table to a protected name", rename("users", "EVENTS"))
	// Dropping a protected table (which would purge its files) is refused too,
	// even for a user with CREATE on the schema, and never reaches the catalog.
	h.grant("nora", auth.PrivilegeCreate, "public", "")
	refused("dropping a protected table", h.request("DELETE", "/v1/namespaces/public/tables/events?purgeRequested=true", nora, nil))
	if h.reached("DELETE " + prefix + "/namespaces/public/tables/events?purgeRequested=true") {
		t.Errorf("a protected table's drop reached the catalog: %v", h.cat.requests)
	}
	if strings.Contains(strings.Join(h.vendor.calls, ","), "u-events") {
		t.Errorf("credentials for a protected table: %v", h.vendor.calls)
	}

	// Listings still show it; admins, whom policies do not bind, load it.
	if w := h.request("GET", "/v1/namespaces/public/tables", nora, nil); !strings.Contains(w.Body.String(), `"events"`) {
		t.Errorf("listing: %d %s", w.Code, w.Body)
	}
	if w := h.request("HEAD", "/v1/namespaces/public/tables/events", nora, nil); w.Code != 204 {
		t.Errorf("HEAD: %d", w.Code)
	}
	if w := h.request("GET", "/v1/namespaces/public/tables/events", ada, nil, vended...); w.Code != 200 || !strings.Contains(w.Body.String(), "scoped-write") {
		t.Errorf("admin: %d %s", w.Code, w.Body)
	}
	// Other tables are unaffected.
	if w := h.request("GET", "/v1/namespaces/public/tables/users", rita, nil, vended...); w.Code != 200 || !strings.Contains(w.Body.String(), "scoped-read") {
		t.Errorf("an unprotected table: %d %s", w.Code, w.Body)
	}
	// Policies are checked on every request.
	mu.Lock()
	protected["public.events"] = false
	mu.Unlock()
	if w := h.request("GET", "/v1/namespaces/public/tables/events", nora, nil); w.Code != 200 {
		t.Errorf("after the policy went: %d %s", w.Code, w.Body)
	}
	mu.Lock()
	failure = errors.New("database down")
	mu.Unlock()
	if w := h.request("GET", "/v1/namespaces/public/tables/events", nora, nil); w.Code != 503 {
		t.Errorf("when policies cannot be checked: %d %s", w.Code, w.Body)
	}
}

// A table with tags or a data retention keeps its name, and its tagged
// columns theirs, against other engines too, admins' included: renaming
// the table, renaming or dropping such a column, adding another under its
// name or changing its type is refused, as in SQL.
func TestTablesWithTags(t *testing.T) {
	h := newHarness(t)
	h.srv.opts.Classified = func(_ context.Context, o auth.Object) ([]string, bool, error) {
		if strings.EqualFold(o.String(), "public.users") {
			return []string{"email"}, true, nil
		}
		return nil, false, nil
	}
	field := func(id int, name, typ string) map[string]any {
		return map[string]any{"id": id, "name": name, "type": typ, "required": false}
	}
	h.cat.metadata = map[string]map[string]any{"public\x1fusers": {
		"current-schema-id": 0,
		"schemas":           []any{map[string]any{"type": "struct", "schema-id": 0, "fields": []any{field(1, "id", "long"), field(2, "email", "string")}}},
	}}
	ada := h.login("ada", "ada-password")
	commit := func(fields ...map[string]any) *httptest.ResponseRecorder {
		return h.request("POST", "/v1/namespaces/public/tables/users", ada, map[string]any{"updates": []any{
			map[string]any{"action": "add-schema", "schema": map[string]any{"type": "struct", "schema-id": 1, "fields": fields}},
			map[string]any{"action": "set-current-schema", "schema-id": -1},
		}})
	}
	refused := func(what string, w *httptest.ResponseRecorder) {
		t.Helper()
		if w.Code != 403 || !strings.Contains(w.Body.String(), "stay with its name") {
			t.Errorf("%s: %d %s", what, w.Code, w.Body)
		}
	}
	refused("renaming the column", commit(field(1, "id", "long"), field(2, "mail", "string")))
	refused("dropping it", commit(field(1, "id", "long")))
	refused("dropping it and adding it again", commit(field(1, "id", "long"), field(3, "email", "string")))
	refused("changing its type", commit(field(1, "id", "long"), field(2, "email", "binary")))
	if w := commit(field(1, "id", "long"), field(2, "email", "string"), field(3, "note", "string")); w.Code != 200 {
		t.Errorf("adding another column: %d %s", w.Code, w.Body)
	}
	// Catalogs give an added schema the next ID, whatever add-schema says:
	// set-current-schema can name it by an ID the commit never wrote.
	// Every added schema is checked, and an ID no schema has is refused
	// unless one was added.
	updates := func(updates ...map[string]any) *httptest.ResponseRecorder {
		list := make([]any, len(updates))
		for i, u := range updates {
			list[i] = u
		}
		return h.request("POST", "/v1/namespaces/public/tables/users", ada, map[string]any{"updates": list})
	}
	for _, sent := range []int{0, 7} {
		refused(fmt.Sprintf("renaming it in a schema sent as %d, made current as 1", sent), updates(
			map[string]any{"action": "add-schema", "schema": map[string]any{"type": "struct", "schema-id": sent,
				"fields": []any{field(1, "id", "long"), field(2, "mail", "string")}}},
			map[string]any{"action": "set-current-schema", "schema-id": 1}))
	}
	refused("making an unknown schema current", updates(map[string]any{"action": "set-current-schema", "schema-id": 5}))
	if w := updates(map[string]any{"action": "remove-schemas", "schema-ids": []int{0}}); w.Code != 403 || !strings.Contains(w.Body.String(), "schemas are not removed") {
		t.Errorf("removing old schemas, which name the columns as they were: %d %s", w.Code, w.Body)
	}
	// Erasure requests cannot rewrite tables of format version 3 yet.
	if w := updates(map[string]any{"action": "upgrade-format-version", "format-version": 3}); w.Code != 403 || !strings.Contains(w.Body.String(), "format version") {
		t.Errorf("upgrading to format version 3: %d %s", w.Code, w.Body)
	}
	refused("renaming the table", h.request("POST", "/v1/tables/rename", ada, map[string]any{
		"source":      map[string]any{"namespace": []string{"public"}, "name": "users"},
		"destination": map[string]any{"namespace": []string{"public"}, "name": "people"},
	}))
	if w := h.request("POST", "/v1/tables/rename", ada, map[string]any{
		"source":      map[string]any{"namespace": []string{"public"}, "name": "events"},
		"destination": map[string]any{"namespace": []string{"public"}, "name": "events_raw"},
	}); w.Code != 204 {
		t.Errorf("renaming a table without tags: %d %s", w.Code, w.Body)
	}
}

// While an erasure request erases from a table, no engine may upgrade it to
// Iceberg format version 3, by upgrade-format-version or by the property,
// admins included, whether its columns have tags or not (as a clone of a
// tagged table): the request could not rewrite it, and one that has begun
// deleting from it could not go on.
func TestFormatVersionWhileErasing(t *testing.T) {
	h := newHarness(t)
	var (
		mu      sync.Mutex
		erasing bool
		failure error
	)
	h.srv.opts.Erasing = func(_ context.Context, o auth.Object) (bool, error) {
		mu.Lock()
		defer mu.Unlock()
		return erasing && strings.EqualFold(o.String(), "public.events"), failure
	}
	set := func(e bool, err error) {
		mu.Lock()
		defer mu.Unlock()
		erasing, failure = e, err
	}
	ada := h.login("ada", "ada-password")
	commit := func(u map[string]any) *httptest.ResponseRecorder {
		return h.request("POST", "/v1/namespaces/public/tables/events", ada, map[string]any{"updates": []any{u}})
	}
	upgrade := map[string]any{"action": "upgrade-format-version", "format-version": 3}
	byProperty := map[string]any{"action": "set-properties", "updates": map[string]string{"Format-Version": "3"}}
	if w := commit(upgrade); w.Code != 200 {
		t.Errorf("upgrading a table no request erases from: %d %s", w.Code, w.Body)
	}
	set(true, nil)
	for what, u := range map[string]map[string]any{"upgrade-format-version": upgrade, "the property": byProperty} {
		if w := commit(u); w.Code != 403 || !strings.Contains(w.Body.String(), "an erasure request erases from public.events") {
			t.Errorf("upgrading by %s while a request erases from the table: %d %s", what, w.Code, w.Body)
		}
	}
	for what, u := range map[string]map[string]any{
		"another property":   {"action": "set-properties", "updates": map[string]string{"comment": "events"}},
		"format version 2":   {"action": "upgrade-format-version", "format-version": 2},
		"the property, to 2": {"action": "set-properties", "updates": map[string]string{"format-version": "2"}},
	} {
		if w := commit(u); w.Code != 200 {
			t.Errorf("%s while a request erases from the table: %d %s", what, w.Code, w.Body)
		}
	}
	// Nor dropped, renamed, or given a table of version 3 under its name,
	// which the request would take for it; a tagged table's name neither.
	h.srv.opts.Classified = func(_ context.Context, o auth.Object) ([]string, bool, error) {
		return nil, strings.EqualFold(o.String(), "public.users"), nil
	}
	rename := func(from, to string) *httptest.ResponseRecorder {
		return h.request("POST", "/v1/tables/rename", ada, map[string]any{
			"source":      map[string]any{"namespace": []string{"public"}, "name": from},
			"destination": map[string]any{"namespace": []string{"public"}, "name": to},
		})
	}
	create := func(name, version string) *httptest.ResponseRecorder {
		props := map[string]string{}
		if version != "" {
			props["format-version"] = version
		}
		return h.request("POST", "/v1/namespaces/public/tables", ada,
			map[string]any{"name": name, "schema": map[string]any{"type": "struct", "fields": []any{}}, "properties": props})
	}
	for what, w := range map[string]*httptest.ResponseRecorder{
		"dropping it":                  h.request("DELETE", "/v1/namespaces/public/tables/events", ada, nil),
		"renaming it":                  rename("events", "events_old"),
		"renaming another to its name": rename("users_new", "events"),
		"making a table of version 3 under its name": create("events", "3"),
		"making one under a tagged name":             create("users", "v3"),
	} {
		if w.Code != 403 {
			t.Errorf("%s while a request erases from the table: %d %s", what, w.Code, w.Body)
		}
	}
	for what, w := range map[string]*httptest.ResponseRecorder{
		"making a table of version 2 under its name": create("events", "2"),
		"making a table of version 3 elsewhere":      create("clicks", "3"),
		"renaming another table":                     rename("clicks", "taps"),
	} {
		if w.Code != 200 && w.Code != 204 {
			t.Errorf("%s: %d %s", what, w.Code, w.Body)
		}
	}
	set(false, errors.New("database down"))
	if w := commit(upgrade); w.Code != 503 {
		t.Errorf("when requests cannot be checked: %d %s", w.Code, w.Body)
	}
}

// Schema changes name the schema in the history.
func TestSchemaChangesInHistory(t *testing.T) {
	h := newHarness(t)
	walt := h.login("walt", "walt-password")
	h.request("POST", "/v1/namespaces", walt, map[string]any{"namespace": []string{"sales"}})
	h.request("POST", "/v1/namespaces/sales/properties", walt, map[string]any{"updates": map[string]string{"owner": "x"}})
	h.request("DELETE", "/v1/namespaces/sales", walt, nil)
	h.request("DELETE", "/v1/namespaces/secret", h.login("nora", "nora-password"), nil)
	qs := h.queries()
	if len(qs) != 4 {
		t.Fatalf("got %d records: %+v", len(qs), qs)
	}
	for i, op := range []string{opCreateNamespace, opUpdateNamespace, opDropNamespace, opDropNamespace} {
		want := "lake.sales"
		if i == 3 {
			want = "lake.secret"
		}
		if qs[i].StatementType != op || !slices.Equal(qs[i].TablesWritten, []string{want}) {
			t.Errorf("record %d: %+v", i, qs[i])
		}
	}
	if qs[3].Outcome != audit.OutcomeDenied {
		t.Errorf("refused drop: %+v", qs[3])
	}
}

// Sign-ins and failed requests that wait out a delay are limited, so
// guessing cannot tie up the server.
func TestBusySignIns(t *testing.T) {
	h := newHarness(t)
	token := h.login("walt", "walt-password")
	for range cap(h.srv.signIns) {
		h.srv.signIns <- struct{}{}
	}
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {"walt"}, "client_secret": {"walt-password"}}
	w := h.request("POST", "/v1/oauth/tokens", "", form.Encode(), "Content-Type", "application/x-www-form-urlencoded")
	if w.Code != 503 || w.Header().Get("Retry-After") == "" || !strings.Contains(w.Body.String(), "temporarily_unavailable") {
		t.Errorf("sign-in when busy: %d %s", w.Code, w.Body)
	}
	for range cap(h.srv.signIns) {
		<-h.srv.signIns
	}
	for range cap(h.srv.delayed) {
		h.srv.delayed <- struct{}{}
	}
	if w := h.request("GET", "/v1/config", "not-a-token", nil); w.Code != 503 {
		t.Errorf("a bad token when busy: %d %s", w.Code, w.Body)
	}
	if w := h.request("GET", "/v1/config", token, nil); w.Code != 200 {
		t.Errorf("a good token when busy: %d %s", w.Code, w.Body)
	}
	for range cap(h.srv.delayed) {
		<-h.srv.delayed
	}
	if w := h.request("GET", "/v1/config", "not-a-token", nil); w.Code != 401 {
		t.Errorf("a bad token: %d %s", w.Code, w.Body)
	}
}

// tokenRequest asks the token endpoint for a token with form.
func (h *harness) tokenRequest(form url.Values, header ...string) (*httptest.ResponseRecorder, string, int) {
	h.t.Helper()
	w := h.request("POST", "/v1/oauth/tokens", "", form.Encode(), append([]string{"Content-Type", "application/x-www-form-urlencoded"}, header...)...)
	var resp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	return w, resp.AccessToken, resp.ExpiresIn
}

func clientCredentials(id, secret string) url.Values {
	form := url.Values{"grant_type": {"client_credentials"}, "client_secret": {secret}}
	if id != "" {
		form.Set("client_id", id)
	}
	return form
}

// Service accounts have no password: jobs sign in with an Igloo access
// token, whose session lasts no longer than the token and ends when it is
// dropped.
func TestAccessTokens(t *testing.T) {
	h := newHarness(t)
	h.users.change("svc", func(u *auth.User) { *u = auth.User{Name: "svc", Role: auth.RoleNone, Kind: auth.KindService} })
	h.grant("svc", auth.PrivilegeSelect, "public", "events")
	secret, tok := h.tokens.mint(t, "svc", 10*time.Minute)

	// With the user name or without, as credential=igloo_... sends it.
	for _, id := range []string{"svc", "SVC", ""} {
		w, token, expires := h.tokenRequest(clientCredentials(id, secret))
		if w.Code != 200 || token == "" || expires > 600 || expires < 590 {
			t.Fatalf("sign in as %q: %d %s", id, w.Code, w.Body)
		}
		if w := h.request("GET", "/v1/namespaces/public/tables/events", token, nil); w.Code != 200 {
			t.Errorf("load with a token session: %d %s", w.Code, w.Body)
		}
	}
	for name, form := range map[string]url.Values{
		"another user's":          clientCredentials("nora", secret),
		"a malformed token":       clientCredentials("svc", secret[:len(secret)-1]),
		"a service's no password": clientCredentials("svc", ""),
	} {
		if w, _, _ := h.tokenRequest(form); w.Code != 401 || !strings.Contains(w.Body.String(), "invalid_client") {
			t.Errorf("%s: %d %s", name, w.Code, w.Body)
		}
	}
	// In an Authorization: Basic header, too.
	if w, _, _ := h.tokenRequest(url.Values{"grant_type": {"client_credentials"}}, "Authorization",
		"Basic "+base64.StdEncoding.EncodeToString([]byte("svc:"+secret))); w.Code != 200 {
		t.Errorf("Basic: %d %s", w.Code, w.Body)
	}

	// Renewing keeps the session bound to the token.
	_, token, _ := h.tokenRequest(clientCredentials("", secret))
	exchange := url.Values{"grant_type": {grantTokenExchange}, "subject_token": {token}, "subject_token_type": {accessTokenType}}
	w, renewed, expires := h.tokenRequest(exchange)
	if w.Code != 200 || expires > 600 {
		t.Fatalf("exchange: %d %s", w.Code, w.Body)
	}

	// Dropping the token ends its sessions, renewed or not, at once.
	if _, err := h.tokens.DropToken(context.Background(), "svc", tok.ID); err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{token, renewed} {
		w := h.request("GET", "/v1/config", s, nil)
		if _, msg := errorType(t, w); w.Code != 401 || !strings.Contains(msg, "dropped") {
			t.Errorf("after DROP TOKEN: %d %s", w.Code, w.Body)
		}
	}
	if w, _, _ := h.tokenRequest(exchange); w.Code != 401 || !strings.Contains(w.Body.String(), "invalid_grant") {
		t.Errorf("exchange after DROP TOKEN: %d %s", w.Code, w.Body)
	}
	if w, _, _ := h.tokenRequest(clientCredentials("", secret)); w.Code != 401 {
		t.Errorf("sign in with a dropped token: %d %s", w.Code, w.Body)
	}

	// So does its expiry.
	secret, tok = h.tokens.mint(t, "svc", time.Hour)
	_, token, _ = h.tokenRequest(clientCredentials("svc", secret))
	h.tokens.expire(tok.ID)
	if w := h.request("GET", "/v1/config", token, nil); w.Code != 401 || !strings.Contains(w.Body.String(), "expired") {
		t.Errorf("after the token expired: %d %s", w.Code, w.Body)
	}
	// A token that outlives the hour gives an hour.
	secret, _ = h.tokens.mint(t, "svc", 90*24*time.Hour)
	if _, _, expires := h.tokenRequest(clientCredentials("svc", secret)); expires != 3600 {
		t.Errorf("a long-lived token's session lasts %d seconds", expires)
	}

	// Without access tokens, a token is refused, not tried as a password.
	h.srv.opts.AuthenticateToken = nil
	if w, _, _ := h.tokenRequest(clientCredentials("svc", secret)); w.Code != 401 || !strings.Contains(w.Body.String(), "takes no access tokens") {
		t.Errorf("tokens off: %d %s", w.Code, w.Body)
	}
}

// Disabled users are refused, whether they sign in with a password or a
// token, and sessions they already have end.
func TestDisabledUsers(t *testing.T) {
	h := newHarness(t)
	secret, _ := h.tokens.mint(t, "nora", time.Hour)
	byPassword := h.login("nora", "nora-password")
	_, byToken, _ := h.tokenRequest(clientCredentials("nora", secret))
	h.users.change("nora", func(u *auth.User) { u.Disabled = true })
	for name, token := range map[string]string{"password": byPassword, "token": byToken} {
		w := h.request("GET", "/v1/config", token, nil)
		if _, msg := errorType(t, w); w.Code != 401 || !strings.Contains(msg, "not permitted") {
			t.Errorf("%s session of a disabled user: %d %s", name, w.Code, w.Body)
		}
	}
	if w, _, _ := h.tokenRequest(clientCredentials("nora", "nora-password")); w.Code != 401 || !strings.Contains(w.Body.String(), "not permitted") {
		t.Errorf("password sign-in of a disabled user: %d %s", w.Code, w.Body)
	}
	if w, _, _ := h.tokenRequest(clientCredentials("nora", secret)); w.Code != 401 {
		t.Errorf("token sign-in of a disabled user: %d %s", w.Code, w.Body)
	}
	exchange := url.Values{"grant_type": {grantTokenExchange}, "subject_token": {byPassword}}
	if w, _, _ := h.tokenRequest(exchange); w.Code != 401 || !strings.Contains(w.Body.String(), "not permitted") {
		t.Errorf("renewing a disabled user's session: %d %s", w.Code, w.Body)
	}
	h.users.change("nora", func(u *auth.User) { u.Disabled = false })
	if w := h.request("GET", "/v1/config", byPassword, nil); w.Code != 200 {
		t.Errorf("enabled again: %d %s", w.Code, w.Body)
	}

	// A token session does not end when the password changes; a password
	// session does.
	h.users.add(t, "nora", "a-new-password", auth.RoleNone)
	if w := h.request("GET", "/v1/config", byToken, nil); w.Code != 200 {
		t.Errorf("token session after a password change: %d %s", w.Code, w.Body)
	}
	if w := h.request("GET", "/v1/config", byPassword, nil); w.Code != 401 {
		t.Errorf("password session after a password change: %d %s", w.Code, w.Body)
	}

	// While failed logins block a user's passwords, the endpoint says when
	// to try again.
	h.users.mu.Lock()
	h.users.blocked["nora"] = 90 * time.Second
	h.users.mu.Unlock()
	if w, _, _ := h.tokenRequest(clientCredentials("nora", "a-new-password")); w.Code != 429 || w.Header().Get("Retry-After") != "90" {
		t.Errorf("blocked: %d %v %s", w.Code, w.Header(), w.Body)
	}
}
