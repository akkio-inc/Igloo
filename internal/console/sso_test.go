package console

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/pgwire"
	"github.com/akkio-inc/igloo/internal/sso"
	"github.com/akkio-inc/igloo/internal/sso/ssotest"
)

// ssoConsole is a console over HTTPS with single sign-on to a test
// identity provider, and a SQL port, with users in a MemoryDirectory: igloo
// (admin, "igloo-password").
type ssoConsole struct {
	srv     *httptest.Server
	idp     *ssotest.Provider
	dir     *auth.MemoryDirectory
	sql     *pgwire.Server
	sqlAddr string
	history *audit.MemoryStore
}

func newSSOConsole(t *testing.T, tls bool, configure ...func(*config.OIDC, *pgwire.Options)) *ssoConsole {
	t.Helper()
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	sc := &ssoConsole{idp: ssotest.New(t, "igloo-console", "console-secret"), dir: auth.NewMemoryDirectory(),
		history: audit.NewMemoryStore(1000, 1000)}
	v, _ := auth.NewVerifier("igloo-password")
	if _, err := sc.dir.EnsureAdmin(ctx, "igloo", v); err != nil {
		t.Fatal(err)
	}
	cfg := config.OIDC{Issuer: sc.idp.Issuer(), ClientID: "igloo-console", ClientSecret: "console-secret",
		Scopes: []string{"openid", "email"}, UsernameClaim: "email", GroupsClaim: "groups", CreateUsers: true,
		PasswordSignIn: true, DisplayName: "Test IdP"}
	opts := pgwire.Options{Users: sc.dir, Audit: audit.New(sc.history, audit.Options{}, log)}
	for _, c := range configure {
		c(&cfg, &opts)
	}
	eng, err := engine.Open(ctx, config.Config{}, log)
	if err != nil {
		t.Fatal(err)
	}
	eng.SetHistory(opts.Audit, pgwire.HistoryViewer)
	var sql *pgwire.Server
	single, err := sso.New(sso.Options{Config: cfg, Users: sc.dir, Accounts: sc.dir, SSO: sc.dir,
		Changed: func(u string) { sql.UserChanged(u) }})
	if err != nil {
		t.Fatal(err)
	}
	if sql, err = pgwire.New(eng, opts, log); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go sql.Serve(ln) //nolint:errcheck
	c, err := New(Options{SQL: sql, Users: sc.dir, Key: bytes.Repeat([]byte{9}, 32), SSO: single, Log: log})
	if err != nil {
		t.Fatal(err)
	}
	if tls {
		sc.srv = httptest.NewTLSServer(c)
	} else {
		sc.srv = httptest.NewServer(c)
	}
	t.Cleanup(func() {
		sc.srv.Close()
		_ = sql.Shutdown(context.Background())
		_ = eng.Close()
	})
	sc.sql, sc.sqlAddr = sql, ln.Addr().String()
	return sc
}

// client is a browser that does not follow redirects, so each step shows.
func (sc *ssoConsole) client() *http.Client {
	jar, _ := cookiejar.New(nil)
	return &http.Client{Transport: sc.srv.Client().Transport, Jar: jar,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func get(t *testing.T, c *http.Client, u string) *http.Response {
	t.Helper()
	resp, err := c.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// signIn runs a single sign-on as someone with claims in browser c, and
// returns where the console finally sends the browser.
func (sc *ssoConsole) signIn(t *testing.T, c *http.Client, claims map[string]any) string {
	t.Helper()
	start := get(t, c, sc.srv.URL+"/sso/start")
	if start.StatusCode != http.StatusFound {
		t.Fatalf("/sso/start: %s", start.Status)
	}
	sc.idp.SignInAs(claims)
	back, err := sc.idp.Authorize(start.Header.Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	done := get(t, c, back.String())
	if done.StatusCode != http.StatusFound {
		t.Fatalf("callback: %s", done.Status)
	}
	loc, _ := done.Location()
	return loc.RequestURI()
}

func ssoPerson(sub, email string, groups ...string) map[string]any {
	return map[string]any{"sub": sub, "email": email, "email_verified": true, "groups": groups}
}

func TestSingleSignOn(t *testing.T) {
	sc := newSSOConsole(t, true)
	c := sc.client()

	// The sign-in page learns that single sign-on is there.
	resp := get(t, c, sc.srv.URL+"/api/session")
	var info struct {
		SSO *struct{ Name string } `json:"sso"`
	}
	decode(t, resp, &info)
	if resp.StatusCode != http.StatusUnauthorized || info.SSO == nil || info.SSO.Name != "Test IdP" {
		t.Fatalf("signed out: %s %+v", resp.Status, info)
	}

	start := get(t, c, sc.srv.URL+"/sso/start")
	authURL, _ := url.Parse(start.Header.Get("Location"))
	pending := cookieNamed(start, secureSSOCookieName)
	if start.StatusCode != http.StatusFound || !strings.HasPrefix(authURL.String(), sc.idp.Issuer()) ||
		authURL.Query().Get("redirect_uri") != sc.srv.URL+"/sso/callback" || authURL.Query().Get("code_challenge") == "" {
		t.Fatalf("start: %s %s", start.Status, authURL)
	}
	if pending == nil || !pending.HttpOnly || !pending.Secure || pending.SameSite != http.SameSiteLaxMode || pending.Path != "/" ||
		strings.Contains(pending.Value, authURL.Query().Get("state")) {
		t.Fatalf("pending sign-in cookie %+v", pending)
	}
	sc.idp.SignInAs(ssoPerson("s-ana", "ana@agency.com"))
	back, err := sc.idp.Authorize(authURL.String())
	if err != nil {
		t.Fatal(err)
	}
	done := get(t, c, back.String())
	session := cookieNamed(done, secureCookieName)
	if loc, _ := done.Location(); done.StatusCode != http.StatusFound || loc.RequestURI() != "/" || session == nil ||
		session.SameSite != http.SameSiteStrictMode {
		t.Fatalf("callback: %s %v %+v", done.Status, loc, session)
	}
	if cleared := cookieNamed(done, secureSSOCookieName); cleared == nil || cleared.MaxAge >= 0 {
		t.Errorf("the pending sign-in was not cleared: %+v", cleared)
	}

	var me sessionInfo
	decode(t, get(t, c, sc.srv.URL+"/api/session"), &me)
	if me.User != "ana" || me.Role != auth.RoleNone || me.Method != "sso" || me.CSRF == "" {
		t.Fatalf("session %+v", me)
	}
	logins, _ := sc.history.Logins(context.Background(), audit.Viewer{All: true}, audit.Page{})
	if len(logins) == 0 || logins[0].User != "ana" || logins[0].Method != audit.MethodSSO || logins[0].Result != audit.LoginSuccess {
		t.Errorf("history: %+v", logins)
	}

	// The callback works once.
	if loc := get(t, c, back.String()).Header.Get("Location"); loc != "/?sso_error=expired" {
		t.Errorf("replayed callback went to %q", loc)
	}

	// An SSO user makes an access token in the console, for SQL clients.
	b := &browser{t: t, tc: &testConsole{srv: sc.srv}, http: c, csrf: me.CSRF}
	_, events := b.query("CREATE TOKEN dbt EXPIRES '30d'")
	if len(events) < 4 || events[2].Rows == nil {
		t.Fatalf("CREATE TOKEN in the console: %+v", events)
	}
	token := *events[2].Rows[0][2]
	u := url.URL{Scheme: "postgres", User: url.UserPassword("ana", token), Host: sc.sqlAddr, Path: "/igloo", RawQuery: "sslmode=disable"}
	conn, err := pgx.Connect(context.Background(), u.String())
	if err != nil {
		t.Fatalf("SQL login with the token: %v", err)
	}
	_ = conn.Close(context.Background())
}

func TestSingleSignOnRefusals(t *testing.T) {
	sc := newSSOConsole(t, true, func(c *config.OIDC, o *pgwire.Options) {
		c.AllowedDomains = []string{"agency.com"}
		o.DisableSSOPasswords = true
	})
	ctx := context.Background()
	refused := func(name string, claims map[string]any, code string) {
		t.Helper()
		if loc := sc.signIn(t, sc.client(), claims); loc != "/?sso_error="+code {
			t.Errorf("%s: sent to %s, want error %s", name, loc, code)
		}
	}
	refused("another domain", ssoPerson("s1", "x@elsewhere.com"), sso.RefusedDomain)
	refused("declined at the provider", nil, "denied")

	// A tampered state, or none, is refused.
	c := sc.client()
	start := get(t, c, sc.srv.URL+"/sso/start")
	sc.idp.SignInAs(ssoPerson("s1", "ana@agency.com"))
	back, _ := sc.idp.Authorize(start.Header.Get("Location"))
	q := back.Query()
	q.Set("state", "forged")
	back.RawQuery = q.Encode()
	if loc := get(t, c, back.String()).Header.Get("Location"); loc != "/?sso_error=expired" {
		t.Errorf("forged state: %q", loc)
	}
	if loc := get(t, sc.client(), sc.srv.URL+"/sso/callback?code=x&state=y").Header.Get("Location"); loc != "/?sso_error=expired" {
		t.Errorf("no pending sign-in: %q", loc)
	}

	// Disabling a user ends their console sessions; enabling them again
	// does not bring those back.
	c = sc.client()
	if loc := sc.signIn(t, c, ssoPerson("s-ana", "ana@agency.com")); loc != "/" {
		t.Fatalf("sign-in: %s", loc)
	}
	if err := sc.dir.SetLogin(ctx, "ana", false); err != nil {
		t.Fatal(err)
	}
	if resp := get(t, c, sc.srv.URL+"/api/session"); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("a disabled user's session: %s", resp.Status)
	}
	refused("a disabled user", ssoPerson("s-ana", "ana@agency.com"), sso.RefusedDisabled)
	c = sc.client()
	if err := sc.dir.SetLogin(ctx, "ana", true); err != nil {
		t.Fatal(err)
	}
	if loc := sc.signIn(t, c, ssoPerson("s-ana", "ana@agency.com")); loc != "/" {
		t.Fatalf("sign-in after enabling: %s", loc)
	}
	if err := sc.dir.SetLogin(ctx, "ana", false); err != nil {
		t.Fatal(err)
	}
	if err := sc.dir.SetLogin(ctx, "ana", true); err != nil {
		t.Fatal(err)
	}
	if resp := get(t, c, sc.srv.URL+"/api/session"); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("a session from before the user was disabled: %s", resp.Status)
	}
	// A user dropped and created again under the same name does not get
	// the old one's sessions.
	c = sc.client()
	if loc := sc.signIn(t, c, ssoPerson("s-dan", "dan@agency.com")); loc != "/" {
		t.Fatalf("dan's sign-in: %s", loc)
	}
	if err := sc.dir.Delete(ctx, "dan"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	if err := sc.dir.Create(ctx, auth.User{Name: "dan", Role: auth.RoleAdmin}); err != nil {
		t.Fatal(err)
	}
	if resp := get(t, c, sc.srv.URL+"/api/session"); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("a new dan got the old one's session: %s", resp.Status)
	}

	// With passwords off for SSO users, a password set for one does not
	// sign them in.
	v, _ := auth.NewVerifier("ana-password")
	if err := sc.dir.SetVerifier(ctx, "ana", v); err != nil {
		t.Fatal(err)
	}
	b := &browser{t: t, tc: &testConsole{srv: sc.srv}, http: sc.client()}
	if resp := b.login("ana", "ana-password"); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("an SSO user's password: %s", resp.Status)
	}
	if resp := b.login("igloo", "igloo-password"); resp.StatusCode != http.StatusOK {
		t.Errorf("an admin without SSO signs in with a password: %s", resp.Status)
	}
}

func TestSingleSignOnNeedsTLSFromAfar(t *testing.T) {
	sc := newSSOConsole(t, false)
	req, _ := http.NewRequest("GET", sc.srv.URL+"/sso/start", nil)
	req.Host = "igloo.example.com:8443"
	resp, err := sc.client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if loc := resp.Header.Get("Location"); loc != "/?sso_error=insecure" {
		t.Errorf("plain HTTP from afar: %q", loc)
	}
	// At localhost it works, and the callback is this console's.
	if loc := sc.signIn(t, sc.client(), ssoPerson("s-ana", "ana@agency.com")); loc != "/" {
		t.Errorf("at localhost: %s", loc)
	}
	// Without single sign-on configured, its pages do not exist.
	tc := newTestConsole(t, false)
	if resp := get(t, tc.srv.Client(), tc.srv.URL+"/sso/start"); resp.StatusCode != http.StatusNotFound {
		t.Errorf("/sso/start without single sign-on: %s", resp.Status)
	}
}

func TestSCIMRoute(t *testing.T) {
	// Without provisioning, the route says so.
	tc := newTestConsole(t, false)
	if resp := get(t, tc.srv.Client(), tc.srv.URL+"/scim/v2/Users"); resp.StatusCode != http.StatusNotFound {
		t.Errorf("SCIM without provisioning: %s", resp.Status)
	}

	// With it, every method reaches the handler, at localhost or over TLS.
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := engine.Open(context.Background(), config.Config{}, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	dir := auth.NewMemoryDirectory()
	sql, err := pgwire.New(eng, pgwire.Options{Users: dir}, log)
	if err != nil {
		t.Fatal(err)
	}
	reached := 0
	c, err := New(Options{SQL: sql, Users: dir, Key: bytes.Repeat([]byte{3}, 32), Log: log,
		SCIM: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reached++
			w.WriteHeader(http.StatusTeapot)
		})})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(c)
	t.Cleanup(srv.Close)
	for _, method := range []string{"GET", "POST", "PUT", "PATCH", "DELETE"} {
		req, _ := http.NewRequest(method, srv.URL+"/scim/v2/Users/x", nil)
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusTeapot || resp.Header.Get("Cache-Control") != "no-store" {
			t.Errorf("%s: %s %q", method, resp.Status, resp.Header.Get("Cache-Control"))
		}
	}
	// A bearer token may not cross a network in the clear.
	req, _ := http.NewRequest("GET", srv.URL+"/scim/v2/Users", nil)
	req.Host = "igloo.example.com"
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden || reached != 5 {
		t.Errorf("plain HTTP from afar: %s, handler reached %d times", resp.Status, reached)
	}
}

func TestSSOAdminPage(t *testing.T) {
	sc := newSSOConsole(t, true)
	if loc := sc.signIn(t, sc.client(), ssoPerson("s-ana", "ana@agency.com", "Data Team")); loc != "/" {
		t.Fatalf("sign-in: %s", loc)
	}
	b := &browser{t: t, tc: &testConsole{srv: sc.srv}, http: sc.client()}
	if resp := b.login("igloo", "igloo-password"); resp.StatusCode != http.StatusOK {
		t.Fatalf("admin sign-in: %s", resp.Status)
	}
	if resp := b.do("POST", "/api/admin/grants", map[string]string{"sql": "CREATE ROLE analysts"}, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("CREATE ROLE: %s", resp.Status)
	}
	if resp := b.do("POST", "/api/admin/grants", map[string]string{"sql": "GRANT analysts TO SSO GROUP 'Data Team'"}, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("mapping a group: %s", resp.Status)
	}
	var page struct {
		Enabled  bool
		Callback string
		Status   sso.Status
		Users    []struct {
			User   string
			Groups []string
		}
		Mappings struct {
			Rows [][]any
		}
	}
	decode(t, b.do("GET", "/api/admin/sso", nil, nil), &page)
	if !page.Enabled || !page.Status.Reachable || page.Callback != sc.srv.URL+"/sso/callback" || len(page.Users) != 1 ||
		page.Users[0].User != "ana" || len(page.Mappings.Rows) != 1 {
		t.Errorf("admin page: %+v", page)
	}
	// The mapping applies at ana's next sign-in.
	if loc := sc.signIn(t, sc.client(), ssoPerson("s-ana", "ana@agency.com", "Data Team")); loc != "/" {
		t.Fatal(loc)
	}
	if p, _ := sc.dir.UserPrivileges(context.Background(), "ana"); len(p.Roles) != 1 || p.Roles[0] != "analysts" {
		t.Errorf("ana's roles: %v", p.Roles)
	}
	// Non-admins do not see the page.
	ana := sc.client()
	sc.signIn(t, ana, ssoPerson("s-ana", "ana@agency.com"))
	if resp := get(t, ana, sc.srv.URL+"/api/admin/sso"); resp.StatusCode != http.StatusForbidden {
		t.Errorf("a non-admin: %s", resp.Status)
	}
}
