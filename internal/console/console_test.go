package console

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/pgwire"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// store is a writable in-memory user store.
type store struct {
	mu    sync.Mutex
	users map[string]auth.User
}

func (s *store) Get(_ context.Context, name string) (auth.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[name]
	if !ok {
		return auth.User{}, auth.ErrNotFound
	}
	return u, nil
}

func (s *store) List(context.Context) ([]auth.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []auth.User
	for _, u := range s.users {
		out = append(out, u)
	}
	return out, nil
}

func (s *store) Create(_ context.Context, u auth.User) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[u.Name]; ok {
		return auth.ErrExists
	}
	u.CreatedAt = time.Now()
	s.users[u.Name] = u
	return nil
}

func (s *store) update(name string, f func(*auth.User)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[name]
	if !ok {
		return auth.ErrNotFound
	}
	f(&u)
	s.users[name] = u
	return nil
}

func (s *store) SetVerifier(_ context.Context, name, v string) error {
	return s.update(name, func(u *auth.User) { u.Verifier = v })
}

func (s *store) SetRole(_ context.Context, name string, r auth.Role) error {
	return s.update(name, func(u *auth.User) { u.Role = r })
}

func (s *store) Delete(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[name]; !ok {
		return auth.ErrNotFound
	}
	delete(s.users, name)
	return nil
}

func (s *store) EnsureAdmin(context.Context, string, string) (bool, error) { return false, nil }
func (s *store) MockSecret(context.Context) ([]byte, error)                { return []byte("mock"), nil }
func (s *store) Ping(context.Context) error                                { return nil }
func (s *store) Close()                                                    {}

func (s *store) add(t *testing.T, name, password string, role auth.Role) {
	t.Helper()
	v, err := auth.NewVerifier(password)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.users[name] = auth.User{Name: name, Role: role, Verifier: v, CreatedAt: time.Now()}
}

type testConsole struct {
	*Console
	sql   *pgwire.Server
	users *store
	srv   *httptest.Server
}

// newTestConsole serves the console over plain HTTP, or HTTPS with tls,
// with users igloo (admin), walt (writer) and rita (reader), each with the
// password "<name>-password".
func newTestConsole(t *testing.T, tls bool) *testConsole {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := engine.Open(context.Background(), config.Config{}, log)
	if err != nil {
		t.Fatal(err)
	}
	users := &store{users: map[string]auth.User{}}
	users.add(t, "igloo", "igloo-password", auth.RoleAdmin)
	users.add(t, "walt", "walt-password", auth.RoleWriter)
	users.add(t, "rita", "rita-password", auth.RoleReader)
	jobs := tasks.NewScheduler(tasks.NewMemoryStore(), tasks.Options{Log: log})
	eng.SetJobHistory(jobs.Store(), pgwire.HistoryViewer)
	sql, err := pgwire.New(eng, pgwire.Options{Users: users, Tasks: jobs}, log)
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(Options{SQL: sql, Users: users, Key: bytes.Repeat([]byte{7}, 32), Log: log,
		Status: func(context.Context) Status { return Status{Version: "test"} }})
	if err != nil {
		t.Fatal(err)
	}
	var srv *httptest.Server
	if tls {
		srv = httptest.NewTLSServer(c)
	} else {
		srv = httptest.NewServer(c)
	}
	t.Cleanup(func() {
		srv.Close()
		_ = eng.Close()
	})
	return &testConsole{Console: c, sql: sql, users: users, srv: srv}
}

// browser is a signed-in client with its own cookies and CSRF token.
type browser struct {
	t    *testing.T
	tc   *testConsole
	http *http.Client
	csrf string
}

func (tc *testConsole) browser(t *testing.T) *browser {
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Transport: tc.srv.Client().Transport, Jar: jar}
	return &browser{t: t, tc: tc, http: client}
}

func (b *browser) do(method, path string, body any, header map[string]string) *http.Response {
	b.t.Helper()
	var r io.Reader
	if body != nil {
		buf, _ := json.Marshal(body)
		r = bytes.NewReader(buf)
	}
	req, err := http.NewRequest(method, b.tc.srv.URL+path, r)
	if err != nil {
		b.t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if b.csrf != "" {
		req.Header.Set(csrfHeader, b.csrf)
	}
	for k, v := range header {
		if k == "Host" {
			req.Host = v
			continue
		}
		req.Header.Set(k, v)
	}
	resp, err := b.http.Do(req)
	if err != nil {
		b.t.Fatal(err)
	}
	b.t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func decode(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
}

func (b *browser) login(user, password string) *http.Response {
	b.t.Helper()
	resp := b.do("POST", "/api/login", map[string]string{"user": user, "password": password}, nil)
	if resp.StatusCode == http.StatusOK {
		var info sessionInfo
		decode(b.t, resp, &info)
		b.csrf = info.CSRF
	}
	return resp
}

func (tc *testConsole) signedIn(t *testing.T, user string) *browser {
	t.Helper()
	b := tc.browser(t)
	if resp := b.login(user, user+"-password"); resp.StatusCode != http.StatusOK {
		t.Fatalf("sign in as %s: %s", user, resp.Status)
	}
	return b
}

// event is one line of a /api/query stream.
type event struct {
	Statement *struct {
		Index  int    `json:"index"`
		Offset int    `json:"offset"`
		Text   string `json:"text"`
	} `json:"statement"`
	Columns  []struct{ Name, Type string } `json:"columns"`
	Rows     [][]*string                   `json:"rows"`
	Complete *struct {
		Tag       string `json:"tag"`
		Rows      int    `json:"rows"`
		Truncated bool   `json:"truncated"`
	} `json:"complete"`
	Error *sqlError `json:"error"`
	Done  *struct {
		MS float64 `json:"ms"`
	} `json:"done"`
}

func (b *browser) query(sql string) (*http.Response, []event) {
	b.t.Helper()
	resp := b.do("POST", "/api/query", map[string]string{"sql": sql}, nil)
	if resp.StatusCode != http.StatusOK {
		return resp, nil
	}
	var events []event
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(nil, 16<<20)
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) == "" {
			continue
		}
		var ev event
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			b.t.Fatalf("bad event %q: %v", sc.Text(), err)
		}
		events = append(events, ev)
	}
	return resp, events
}

func cookieNamed(resp *http.Response, name string) *http.Cookie {
	for _, c := range resp.Cookies() {
		if c.Name == name {
			return c
		}
	}
	return nil
}

func TestSignIn(t *testing.T) {
	tc := newTestConsole(t, false)
	b := tc.browser(t)

	resp := b.login("igloo", "igloo-password")
	if resp.StatusCode != http.StatusOK || b.csrf == "" {
		t.Fatalf("good password: %s", resp.Status)
	}
	c := cookieNamed(resp, cookieName)
	if c == nil || !c.HttpOnly || c.SameSite != http.SameSiteStrictMode || c.Path != "/" || c.Secure || c.MaxAge <= 0 {
		t.Fatalf("session cookie %+v", c)
	}
	var info sessionInfo
	decode(t, b.do("GET", "/api/session", nil, nil), &info)
	if info.User != "igloo" || info.Role != auth.RoleAdmin || info.CSRF != b.csrf {
		t.Errorf("session %+v", info)
	}

	// Wrong passwords and unknown users fail alike, after the delay SQL
	// logins have.
	for _, cred := range [][2]string{{"igloo", "not-the-password"}, {"nobody", "whatever-it-is"}} {
		start := time.Now()
		resp := tc.browser(t).login(cred[0], cred[1])
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s: %s", cred[0], resp.Status)
		}
		if took := time.Since(start); took < 500*time.Millisecond {
			t.Errorf("%s failed after only %s", cred[0], took)
		}
		if cookieNamed(resp, cookieName) != nil {
			t.Errorf("%s: a failed sign-in set a cookie", cred[0])
		}
	}

	// Unencrypted sign-ins from other machines are refused before the
	// password is checked.
	resp = tc.browser(t).do("POST", "/api/login", map[string]string{"user": "igloo", "password": "igloo-password"},
		map[string]string{"Host": "igloo.example.com:8443"})
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("plain HTTP sign-in from afar: %s", resp.Status)
	}
	// Forms from other sites are refused too.
	resp = tc.browser(t).do("POST", "/api/login", map[string]string{"user": "igloo", "password": "igloo-password"},
		map[string]string{"Origin": "https://evil.example"})
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("cross-origin sign-in: %s", resp.Status)
	}
	req, _ := http.NewRequest("POST", tc.srv.URL+"/api/login", strings.NewReader("user=igloo&password=igloo-password"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if resp, err := tc.srv.Client().Do(req); err != nil {
		t.Error(err)
	} else if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Errorf("form-encoded sign-in: %s", resp.Status)
	}

	// Signing out clears the cookie.
	resp = b.do("POST", "/api/logout", map[string]string{}, nil)
	if c := cookieNamed(resp, cookieName); resp.StatusCode != http.StatusOK || c == nil || c.MaxAge >= 0 {
		t.Errorf("sign out: %s %+v", resp.Status, c)
	}
	if resp := b.do("GET", "/api/session", nil, nil); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("session after signing out: %s", resp.Status)
	}
}

func TestSignInOverTLS(t *testing.T) {
	tc := newTestConsole(t, true)
	b := tc.browser(t)
	resp := b.do("POST", "/api/login", map[string]string{"user": "rita", "password": "rita-password"},
		map[string]string{"Host": "igloo.example.com"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("sign in over TLS: %s", resp.Status)
	}
	c := cookieNamed(resp, secureCookieName)
	if c == nil || !c.Secure || !c.HttpOnly || c.SameSite != http.SameSiteStrictMode || c.Path != "/" || c.Domain != "" {
		t.Fatalf("session cookie over TLS %+v", c)
	}
}

func TestCSRF(t *testing.T) {
	tc := newTestConsole(t, false)
	b := tc.signedIn(t, "walt")
	token := b.csrf

	b.csrf = ""
	if resp, _ := b.query("SELECT 1"); resp.StatusCode != http.StatusForbidden {
		t.Errorf("no token: %s", resp.Status)
	}
	b.csrf = "forged"
	if resp, _ := b.query("SELECT 1"); resp.StatusCode != http.StatusForbidden {
		t.Errorf("wrong token: %s", resp.Status)
	}
	b.csrf = token
	for _, h := range []map[string]string{{"Origin": "https://evil.example"}, {"Sec-Fetch-Site": "cross-site"}} {
		resp := b.do("POST", "/api/query", map[string]string{"sql": "SELECT 1"}, h)
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("%v: %s", h, resp.Status)
		}
	}
	for _, path := range []string{"/api/logout", "/api/download", "/api/admin/users"} {
		b.csrf = ""
		if resp := b.do("POST", path, map[string]string{}, nil); resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s without a token: %s", path, resp.Status)
		}
	}
	b.csrf = token
	if resp, events := b.query("SELECT 1"); resp.StatusCode != http.StatusOK || len(events) == 0 {
		t.Errorf("with the token: %s", resp.Status)
	}
}

func TestSessionEnds(t *testing.T) {
	tc := newTestConsole(t, false)
	user, _ := tc.users.Get(context.Background(), "rita")
	now := time.Now()
	valid := session{User: "rita", Issued: now.Unix(), Seen: now.Unix(), CSRF: "x", Password: tc.cookies.fingerprint(user.Verifier)}

	get := func(sess session, tamper bool) int {
		value, err := tc.cookies.seal(sess)
		if err != nil {
			t.Fatal(err)
		}
		if tamper {
			// Flip a character in the middle of the ciphertext.
			i := len(value) / 2
			flipped := "A"
			if value[i] == 'A' {
				flipped = "B"
			}
			value = value[:i] + flipped + value[i+1:]
		}
		req, _ := http.NewRequest("GET", tc.srv.URL+"/api/session", nil)
		req.AddCookie(&http.Cookie{Name: cookieName, Value: value})
		resp, err := tc.srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}

	if code := get(valid, false); code != http.StatusOK {
		t.Fatalf("valid session: %d", code)
	}
	if code := get(valid, true); code != http.StatusUnauthorized {
		t.Errorf("tampered cookie: %d", code)
	}
	old := valid
	old.Issued = now.Add(-sessionLifetime - time.Minute).Unix()
	if code := get(old, false); code != http.StatusUnauthorized {
		t.Errorf("session past its lifetime: %d", code)
	}
	idle := valid
	idle.Seen = now.Add(-sessionIdle - time.Minute).Unix()
	if code := get(idle, false); code != http.StatusUnauthorized {
		t.Errorf("idle session: %d", code)
	}
	other, _ := newSealer(bytes.Repeat([]byte{8}, 32))
	value, _ := other.seal(valid)
	req, _ := http.NewRequest("GET", tc.srv.URL+"/api/session", nil)
	req.AddCookie(&http.Cookie{Name: cookieName, Value: value})
	if resp, _ := tc.srv.Client().Do(req); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("cookie sealed with another key: %d", resp.StatusCode)
	}

	// A changed password or a dropped user ends the session.
	v, _ := auth.NewVerifier("a-new-password")
	_ = tc.users.SetVerifier(context.Background(), "rita", v)
	if code := get(valid, false); code != http.StatusUnauthorized {
		t.Errorf("after a password change: %d", code)
	}
	_ = tc.users.Delete(context.Background(), "rita")
	valid.Password = tc.cookies.fingerprint(v)
	if code := get(valid, false); code != http.StatusUnauthorized {
		t.Errorf("after the user was dropped: %d", code)
	}

	// Role changes apply to the next request.
	b := tc.signedIn(t, "walt")
	if resp := b.do("GET", "/api/admin/users", nil, nil); resp.StatusCode != http.StatusForbidden {
		t.Errorf("writer reads users: %s", resp.Status)
	}
	_ = tc.users.SetRole(context.Background(), "walt", auth.RoleAdmin)
	if resp := b.do("GET", "/api/admin/users", nil, nil); resp.StatusCode != http.StatusOK {
		t.Errorf("after promotion to admin: %s", resp.Status)
	}
}

func TestAdminOnly(t *testing.T) {
	tc := newTestConsole(t, false)
	for _, name := range []string{"rita", "walt"} {
		b := tc.signedIn(t, name)
		for _, r := range []struct {
			method, path string
			body         any
		}{
			{"GET", "/api/admin/users", nil},
			{"POST", "/api/admin/users", map[string]string{"action": "create", "name": "eve", "password": "eve-password", "role": "admin"}},
			{"GET", "/api/admin/grants", nil},
			{"POST", "/api/admin/grants", map[string]string{"sql": "GRANT SELECT ON TABLE t TO ROLE r"}},
			{"GET", "/api/admin/compute", nil},
			{"POST", "/api/admin/cancel", map[string]any{"pid": 1, "replica": tc.replica}},
		} {
			if resp := b.do(r.method, r.path, r.body, nil); resp.StatusCode != http.StatusForbidden {
				t.Errorf("%s %s %s: %s", name, r.method, r.path, resp.Status)
			}
		}
	}
	if _, err := tc.users.Get(context.Background(), "eve"); err == nil {
		t.Error("a non-admin created a user")
	}
	// The SQL server refuses the same thing over SQL.
	b := tc.signedIn(t, "walt")
	if _, events := b.query("CREATE USER eve PASSWORD 'eve-password'"); len(events) < 2 || events[1].Error == nil || events[1].Error.Code != "42501" {
		t.Errorf("writer CREATE USER through a worksheet: %+v", events)
	}
}

func TestUsersPage(t *testing.T) {
	tc := newTestConsole(t, false)
	b := tc.signedIn(t, "igloo")
	post := func(body map[string]string) (int, map[string]string) {
		resp := b.do("POST", "/api/admin/users", body, nil)
		var out map[string]string
		decode(t, resp, &out)
		return resp.StatusCode, out
	}
	if code, out := post(map[string]string{"action": "create", "name": "ana", "password": "it's-a-secret", "role": "writer"}); code != http.StatusOK || out["tag"] != "CREATE ROLE" {
		t.Fatalf("create: %d %v", code, out)
	}
	if resp := tc.browser(t).login("ana", "it's-a-secret"); resp.StatusCode != http.StatusOK {
		t.Errorf("new user signs in: %s", resp.Status)
	}
	if code, _ := post(map[string]string{"action": "role", "name": "ana", "role": "reader"}); code != http.StatusOK {
		t.Errorf("change role: %d", code)
	}
	if u, _ := tc.users.Get(context.Background(), "ana"); u.Role != auth.RoleReader {
		t.Errorf("role is %s", u.Role)
	}
	if code, _ := post(map[string]string{"action": "password", "name": "ana", "password": "short"}); code != http.StatusBadRequest {
		t.Errorf("short password: %d", code)
	}
	if code, out := post(map[string]string{"action": "create", "name": "ana", "password": "another-password", "role": "reader"}); code != http.StatusBadRequest || out["code"] != "42710" {
		t.Errorf("duplicate: %d %v", code, out)
	}
	if code, _ := post(map[string]string{"action": "create", "name": "Robert'); DROP", "password": "password1", "role": "reader"}); code != http.StatusBadRequest {
		t.Errorf("bad name: %d", code)
	}
	if code, _ := post(map[string]string{"action": "drop", "name": "ana"}); code != http.StatusOK {
		t.Errorf("drop: %d", code)
	}
	var users result
	decode(t, b.do("GET", "/api/admin/users", nil, nil), &users)
	if len(users.Rows) != 3 || strings.Join(users.Columns, ",") != "name,role,created_at,type,owner,can_login,has_password,email,sso" {
		t.Errorf("users %+v", users)
	}

	// Roles and grants go through the page as SQL.
	if resp := b.do("POST", "/api/admin/grants", map[string]string{"sql": "CREATE ROLE analyst"}, nil); resp.StatusCode != http.StatusOK {
		t.Errorf("CREATE ROLE on the grants page: %s", resp.Status)
	}
	var grants struct {
		Available bool `json:"available"`
		Roles     struct {
			Rows [][]any `json:"rows"`
		} `json:"roles"`
	}
	decode(t, b.do("GET", "/api/admin/grants", nil, nil), &grants)
	if !grants.Available || len(grants.Roles.Rows) != 1 {
		t.Errorf("roles and grants: %+v", grants)
	}
	if resp := b.do("POST", "/api/admin/grants", map[string]string{"sql": "DROP TABLE t"}, nil); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("non-grant statement on the grants page: %s", resp.Status)
	}
}

func TestQuery(t *testing.T) {
	tc := newTestConsole(t, false)
	admin := tc.signedIn(t, "igloo")
	_, events := admin.query("CREATE TABLE t (id INTEGER, name VARCHAR);\nINSERT INTO t VALUES (1, 'a'), (2, NULL);\n-- just a comment;\nSELECT * FROM t ORDER BY id")
	var tags []string
	for _, ev := range events {
		if ev.Complete != nil {
			tags = append(tags, ev.Complete.Tag)
		}
		if ev.Error != nil {
			t.Fatalf("error: %+v", ev.Error)
		}
	}
	if strings.Join(tags, ",") != "CREATE TABLE,INSERT 0 2,SELECT 2" {
		t.Fatalf("tags %v (events %+v)", tags, events)
	}
	var rows [][]*string
	var columns []string
	for _, ev := range events {
		rows = append(rows, ev.Rows...)
		for _, c := range ev.Columns {
			columns = append(columns, c.Name+":"+c.Type)
		}
	}
	if strings.Join(columns, ",") != "id:integer,name:text" || len(rows) != 2 || *rows[0][1] != "a" || rows[1][1] != nil {
		t.Errorf("columns %v rows %v", columns, rows)
	}
	if last := events[len(events)-1]; last.Done == nil {
		t.Errorf("no done event: %+v", last)
	}
	var texts []string
	for _, ev := range events {
		if ev.Statement != nil {
			texts = append(texts, ev.Statement.Text)
		}
	}
	if len(texts) != 3 || texts[2] != "-- just a comment;\nSELECT * FROM t ORDER BY id" {
		t.Errorf("statements %q", texts)
	}

	// A reader's write is refused by the SQL server, with its SQLSTATE, and
	// the script stops there.
	reader := tc.signedIn(t, "rita")
	_, events = reader.query("SELECT 1; INSERT INTO t VALUES (3, 'c'); SELECT 2")
	var failed *sqlError
	statements := 0
	for _, ev := range events {
		if ev.Error != nil {
			failed = ev.Error
		}
		if ev.Statement != nil {
			statements++
		}
	}
	if failed == nil || failed.Code != "42501" || !strings.Contains(failed.Message, "requires the writer role") {
		t.Errorf("reader insert: %+v", failed)
	}
	if statements != 2 {
		t.Errorf("ran %d statements, want 2 (stop at the error)", statements)
	}

	// Large results are cut at maxRows.
	_, events = reader.query("SELECT * FROM range(25000)")
	n := 0
	for _, ev := range events {
		n += len(ev.Rows)
		if ev.Complete != nil && (!ev.Complete.Truncated || ev.Complete.Rows != maxRows) {
			t.Errorf("complete %+v", ev.Complete)
		}
	}
	if n != maxRows {
		t.Errorf("sent %d rows, want %d", n, maxRows)
	}

	if resp, _ := reader.query("  -- nothing\n"); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("empty script: %s", resp.Status)
	}
}

// TestDataBrowserReadsPolicies runs the data browser's queries of a
// table's policies and tags, as data.js sends them.
func TestDataBrowserReadsPolicies(t *testing.T) {
	tc := newTestConsole(t, false)
	reader := tc.signedIn(t, "rita")
	_, events := reader.query(`SELECT policy_name, policy_kind, ref_column_name, tag_name FROM igloo.policy_references
WHERE ref_database_name = 'memory' AND lower(ref_schema_name) = lower('main') AND lower(ref_entity_name) = lower('t');
SELECT column_name, tag_name, tag_value FROM igloo.tag_references
WHERE object_database = 'memory' AND lower(object_schema) = lower('main') AND lower(object_name) = lower('t');`)
	completed := 0
	for _, ev := range events {
		if ev.Error != nil {
			t.Fatalf("error: %+v", ev.Error)
		}
		if ev.Complete != nil {
			completed++
		}
	}
	if completed != 2 {
		t.Errorf("%d of 2 queries completed", completed)
	}
}

func TestCancelByClosingTheRequest(t *testing.T) {
	tc := newTestConsole(t, false)
	b := tc.signedIn(t, "rita")
	ctx, cancel := context.WithCancel(context.Background())
	body, _ := json.Marshal(map[string]string{"sql": "SELECT count(*) FROM range(10000000000) a, range(10) b WHERE a.range % 7 = b.range"})
	req, _ := http.NewRequestWithContext(ctx, "POST", tc.srv.URL+"/api/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(csrfHeader, b.csrf)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, err := b.http.Do(req); err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}()
	waitFor(t, "the statement to run", func() bool {
		s := tc.sql.Sessions()
		return len(s) == 1 && !s[0].Started.IsZero()
	})
	cancel()
	<-done
	waitFor(t, "the session to end", func() bool { return len(tc.sql.Sessions()) == 0 })
}

func TestComputeCancel(t *testing.T) {
	tc := newTestConsole(t, false)
	reader := tc.signedIn(t, "rita")
	result := make(chan []event, 1)
	go func() {
		_, events := reader.query("SELECT count(*) FROM range(10000000000) a, range(10) b WHERE a.range % 7 = b.range")
		result <- events
	}()

	admin := tc.signedIn(t, "igloo")
	var compute struct {
		Replica  string        `json:"replica"`
		Status   Status        `json:"status"`
		Sessions []sessionView `json:"sessions"`
	}
	waitFor(t, "a running statement", func() bool {
		decode(t, admin.do("GET", "/api/admin/compute", nil, nil), &compute)
		return len(compute.Sessions) == 1 && compute.Sessions[0].Started != nil
	})
	s := compute.Sessions[0]
	if s.User != "rita" || !strings.HasPrefix(s.Client, "console ") || !strings.HasPrefix(s.Statement, "SELECT count(*)") || compute.Status.Version != "test" {
		t.Errorf("compute %+v", compute)
	}
	resp := admin.do("POST", "/api/admin/cancel", map[string]any{"pid": s.PID, "replica": "elsewhere"}, nil)
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("cancel on another replica: %s", resp.Status)
	}
	var out map[string]bool
	decode(t, admin.do("POST", "/api/admin/cancel", map[string]any{"pid": s.PID, "replica": compute.Replica}, nil), &out)
	if !out["canceled"] {
		t.Error("not canceled")
	}
	select {
	case events := <-result:
		var code string
		for _, ev := range events {
			if ev.Error != nil {
				code = ev.Error.Code
			}
		}
		if code != "57014" {
			t.Errorf("canceled statement: %+v", events)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the canceled statement kept running")
	}
}

func TestDownload(t *testing.T) {
	tc := newTestConsole(t, false)
	b := tc.signedIn(t, "rita")
	resp := b.do("POST", "/api/download", map[string]string{"sql": "SELECT range AS n, 'x,y' AS s, NULL AS z FROM range(25000)"}, nil)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "text/csv; charset=utf-8" ||
		!strings.HasPrefix(resp.Header.Get("Content-Disposition"), "attachment; filename=") {
		t.Fatalf("download: %s %v", resp.Status, resp.Header)
	}
	records, err := csv.NewReader(resp.Body).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	// Every row, beyond what the grid shows.
	if len(records) != 25001 || strings.Join(records[0], "|") != "n|s|z" || strings.Join(records[25000], "|") != "24999|x,y|" {
		t.Errorf("%d records, first %v, last %v", len(records), records[0], records[len(records)-1])
	}

	// Only queries run again.
	resp = tc.signedIn(t, "igloo").do("POST", "/api/download", map[string]string{"sql": "CREATE TABLE t AS SELECT 1 AS i"}, nil)
	var out map[string]string
	decode(t, resp, &out)
	if resp.StatusCode != http.StatusBadRequest || out["code"] != "0A000" {
		t.Errorf("download of a CREATE: %s %v", resp.Status, out)
	}
	if resp := b.do("POST", "/api/download", map[string]string{"sql": "SELECT 1; SELECT 2"}, nil); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("two statements: %s", resp.Status)
	}
	if resp := b.do("POST", "/api/download", map[string]string{"sql": "SELECT * FROM nowhere"}, nil); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("failing query: %s", resp.Status)
	}
}

// TestDownloadRepeatsTheRunsSettings downloads a query with the statements
// that ran before it in the worksheet's run, as worksheets.js sends them.
func TestDownloadRepeatsTheRunsSettings(t *testing.T) {
	tc := newTestConsole(t, false)
	admin := tc.signedIn(t, "igloo")
	_, events := admin.query(`CREATE SCHEMA tenant_a;
		CREATE TABLE orders AS SELECT 'main' AS what;
		CREATE TABLE tenant_a.orders AS SELECT 'tenant_a' AS what`)
	for _, ev := range events {
		if ev.Error != nil {
			t.Fatalf("setup: %+v", ev.Error)
		}
	}
	download := func(before ...string) (int, string) {
		t.Helper()
		resp := admin.do("POST", "/api/download", map[string]any{"sql": "SELECT what FROM orders", "before": before}, nil)
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, string(body)
	}

	// The worksheet's query read tenant_a.orders, and so does its download.
	if status, body := download("SET search_path = tenant_a", "SELECT 1"); status != http.StatusOK || body != "what\ntenant_a\n" {
		t.Errorf("after SET search_path: %d %q", status, body)
	}
	if status, body := download("BEGIN", "COMMIT", "USE tenant_a"); status != http.StatusOK || body != "what\ntenant_a\n" {
		t.Errorf("after USE: %d %q", status, body)
	}
	// What changes data is never run again, nor what changed every
	// session's settings: the download reads the table as it is.
	if status, body := download("INSERT INTO orders VALUES ('again')", "SET GLOBAL search_path = tenant_a"); status != http.StatusOK || body != "what\nmain\n" {
		t.Errorf("after INSERT: %d %q", status, body)
	}
	// Nor what follows a setting in the same entry.
	if status, body := download("SET search_path = tenant_a; DELETE FROM orders"); status != http.StatusOK || body != "what\ntenant_a\n" {
		t.Errorf("after SET and DELETE in one entry: %d %q", status, body)
	}
	// What a new session cannot have without running more, the download
	// refuses rather than read another table.
	for _, before := range [][]string{
		{"CREATE TEMP TABLE orders AS SELECT 'temp' AS what"},
		{"CREATE VIEW temp.main.orders AS SELECT 'temp' AS what"},
		{"BEGIN", "INSERT INTO orders VALUES ('uncommitted')"},
		{"SET VARIABLE s = (SELECT 'tenant_a')"},
	} {
		if status, body := download(before...); status != http.StatusBadRequest || !strings.Contains(body, `"0A000"`) {
			t.Errorf("after %q: %d %s", before, status, body)
		}
	}
}

func TestPagesAndHeaders(t *testing.T) {
	tc := newTestConsole(t, false)
	client := tc.srv.Client()
	resp, err := client.Get(tc.srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	page, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !bytes.Contains(page, []byte(`<script type="module" src="/js/app.js">`)) {
		t.Fatalf("index: %s", resp.Status)
	}
	h := resp.Header
	if csp := h.Get("Content-Security-Policy"); !strings.Contains(csp, "script-src 'self'") || !strings.Contains(csp, "frame-ancestors 'none'") || strings.Contains(csp, "unsafe") {
		t.Errorf("CSP %q", csp)
	}
	if h.Get("X-Content-Type-Options") != "nosniff" || h.Get("X-Frame-Options") != "DENY" || h.Get("Referrer-Policy") != "no-referrer" {
		t.Errorf("headers %v", h)
	}

	// Modules are served as JavaScript and revalidated with ETags.
	resp, _ = client.Get(tc.srv.URL + "/js/app.js")
	_ = resp.Body.Close()
	etag := resp.Header.Get("ETag")
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/javascript") || etag == "" {
		t.Errorf("app.js: %v", resp.Header)
	}
	req, _ := http.NewRequest("GET", tc.srv.URL+"/js/app.js", nil)
	req.Header.Set("If-None-Match", etag)
	if resp, _ := client.Do(req); resp.StatusCode != http.StatusNotModified {
		t.Errorf("revalidation: %s", resp.Status)
	}
	for _, path := range []string{"/js/", "/index.html", "/nope.js", "/api/nope"} {
		if resp, _ := client.Get(tc.srv.URL + path); resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s: %s", path, resp.Status)
		}
	}
	if resp, _ := client.Get(tc.srv.URL + "/api/session"); resp.Header.Get("Cache-Control") != "no-store" {
		t.Errorf("API responses may be cached: %v", resp.Header)
	}
}

// TestPagesHaveNoInlineCode keeps the pages within the Content Security
// Policy and away from building markup from strings.
func TestPagesHaveNoInlineCode(t *testing.T) {
	inline := regexp.MustCompile(`(?i)<script(?:\s[^>]*)?>\s*[^<\s]|<style|\sstyle=|\son[a-z]+=|javascript:`)
	unsafe := regexp.MustCompile(`innerHTML|outerHTML|insertAdjacentHTML|document\.write|\beval\(|new Function|setAttribute\('style'|setAttribute\("on`)
	err := fs.WalkDir(staticFiles, "static", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, _ := fs.ReadFile(staticFiles, path)
		switch {
		case strings.HasSuffix(path, ".html") && inline.Match(b):
			t.Errorf("%s has inline code: %q", path, inline.Find(b))
		case strings.HasSuffix(path, ".js") && unsafe.Match(b):
			t.Errorf("%s builds markup from strings: %q", path, unsafe.Find(b))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestLoopback(t *testing.T) {
	for host, want := range map[string]bool{
		"localhost": true, "localhost:8443": true, "127.0.0.1:8443": true, "[::1]:8443": true, "console.localhost": true,
		"127.0.0.2": true, "igloo.example.com": false, "10.0.0.5:8443": false, "localhost.example.com": false, "": false,
	} {
		if got := loopback(host); got != want {
			t.Errorf("loopback(%q) = %v", host, got)
		}
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
