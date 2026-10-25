package pgwire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/xdg-go/scram"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// startDirectoryServer runs Igloo with users in an auth.MemoryDirectory,
// which also keeps access tokens, accounts and single sign-on, and records
// the history in memory.
func startDirectoryServer(t *testing.T, configure ...func(*Options)) (*testServer, *auth.MemoryDirectory, *audit.MemoryStore) {
	t.Helper()
	dir := auth.NewMemoryDirectory()
	v, err := auth.NewVerifier(adminPassword)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dir.EnsureAdmin(context.Background(), "igloo", v); err != nil {
		t.Fatal(err)
	}
	history := audit.NewMemoryStore(1000, 1000)
	setup := func(o *Options) {
		o.Users = dir
		o.Audit = audit.New(history, audit.Options{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}
	return startServer(t, append([]func(*Options){setup}, configure...)...), dir, history
}

// newToken runs CREATE TOKEN on conn and returns the token.
func newToken(t *testing.T, conn *pgx.Conn, sql string) string {
	t.Helper()
	var user, name, secret string
	var expires time.Time
	if err := conn.QueryRow(context.Background(), sql).Scan(&user, &name, &secret, &expires); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if !auth.LooksLikeToken(secret) || time.Until(expires) <= 0 {
		t.Fatalf("%s returned %q expiring %s", sql, secret, expires)
	}
	return secret
}

// tokenSession has conn make a token with create, a CREATE TOKEN statement
// whose lifetime in seconds is its %d, signs in as user with the token and
// runs a statement, and returns the token, the session and when the token
// expires. A token expires at the start of a second, so one of 2 s can
// last little more than one, and on a busy machine signing in can take
// longer: then the token is dropped with drop and made again with twice
// the lifetime.
func tokenSession(t *testing.T, ts *testServer, conn *pgx.Conn, user, create, drop string) (string, *pgx.Conn, time.Time) {
	t.Helper()
	ctx := context.Background()
	for life := 2; ; life *= 2 {
		sql := fmt.Sprintf(create, life)
		var owner, name, token string
		var expires time.Time
		if err := conn.QueryRow(ctx, sql).Scan(&owner, &name, &token, &expires); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		session, err := tryConnect(ts.url(user, token), pgx.QueryExecModeSimpleProtocol)
		if err == nil {
			t.Cleanup(func() { _ = session.Close(context.Background()) })
			_, err = session.Exec(ctx, "SELECT 1")
		}
		if err == nil {
			return token, session, expires
		}
		if time.Now().Before(expires) || life >= 16 {
			t.Fatalf("a session of a token with %d s to live: %v", life, err)
		}
		mustExec(t, conn, drop)
	}
}

func loginFails(t *testing.T, ts *testServer, user, password, code string) {
	t.Helper()
	conn, err := tryConnect(ts.url(user, password), pgx.QueryExecModeSimpleProtocol)
	if err == nil {
		_ = conn.Close(context.Background())
		t.Errorf("%s signed in with %.12q…", user, password)
		return
	}
	if got := pgCode(err); got != code {
		t.Errorf("%s: got %s (%v), want %s", user, got, err, code)
	}
}

func TestServiceAccountTokens(t *testing.T) {
	t.Parallel()
	ts, dir, history := startDirectoryServer(t)
	admin := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()

	mustExec(t, admin, "CREATE USER dbt TYPE SERVICE ROLE writer")
	if _, err := admin.Exec(ctx, "CREATE USER etl TYPE SERVICE PASSWORD 'some-password'"); pgCode(err) != "22023" {
		t.Errorf("a service account with a password: %v", err)
	}
	if _, err := admin.Exec(ctx, "ALTER USER dbt PASSWORD 'some-password'"); pgCode(err) != "22023" {
		t.Errorf("giving a service account a password: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE USER ana OWNER igloo PASSWORD NULL"); pgCode(err) != "42601" {
		t.Errorf("an owner for a person: %v", err)
	}
	rows, _ := admin.Query(ctx, "SHOW USERS")
	users, err := pgx.CollectRows(rows, pgx.RowToMap)
	if err != nil || len(users) != 2 || users[0]["name"] != "dbt" || users[0]["type"] != "service" ||
		users[0]["owner"] != "igloo" || users[0]["has_password"] != false || users[0]["can_login"] != true {
		t.Errorf("SHOW USERS of a service account: %v %v", users, err)
	}

	token := newToken(t, admin, "CREATE TOKEN ci FOR USER dbt EXPIRES '30d'")
	dbt := connect(t, ts.url("dbt", token), pgx.QueryExecModeCacheStatement)
	mustExec(t, dbt, "CREATE TABLE made_by_dbt AS SELECT 1 AS x")
	var user string
	if err := dbt.QueryRow(ctx, "SELECT current_user").Scan(&user); err != nil || user != "dbt" {
		t.Errorf("current_user: %q %v", user, err)
	}
	loginFails(t, ts, "dbt", flip(token), "28P01")
	loginFails(t, ts, "igloo", token, "28P01")

	// Tokens are recorded in the login history by name.
	logins, _ := history.Logins(ctx, audit.Viewer{All: true}, audit.Page{})
	var used bool
	for _, l := range logins {
		if l.User == "dbt" && l.Result == audit.LoginSuccess && l.Method == audit.MethodToken && l.Token == "ci" {
			used = true
		}
	}
	if !used {
		t.Errorf("token login not in the history: %+v", logins)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		list, _ := dir.Tokens(ctx, "dbt")
		if len(list) == 1 && !list[0].LastUsedAt.IsZero() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("last use not recorded: %+v", list)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Service accounts get their tokens from admins.
	if _, err := dbt.Exec(ctx, "CREATE TOKEN more"); pgCode(err) != "42501" {
		t.Errorf("a service account made itself a token: %v", err)
	}
	if _, err := dbt.Exec(ctx, "SHOW TOKENS FOR USER igloo"); pgCode(err) != "42501" {
		t.Errorf("a service account listed an admin's tokens: %v", err)
	}
	mustExec(t, admin, "CREATE USER ana PASSWORD NULL ROLE reader")
	if _, err := admin.Exec(ctx, "CREATE TOKEN x FOR USER ana"); pgCode(err) != "42501" {
		t.Errorf("an admin made a token for a person: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE TOKEN ci FOR USER dbt"); pgCode(err) != "42710" {
		t.Errorf("a second token of the same name: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE TOKEN long FOR USER dbt EXPIRES '400d'"); pgCode(err) != "22023" {
		t.Errorf("a token beyond the longest lifetime: %v", err)
	}

	// Dropping the token ends the sessions it opened.
	mustExec(t, admin, "DROP TOKEN ci FOR USER dbt")
	if _, err := dbt.Exec(ctx, "SELECT 1"); pgCode(err) != "28000" || !strings.Contains(err.Error(), "revoked") {
		t.Errorf("a session of a dropped token: %v", err)
	}
	loginFails(t, ts, "dbt", token, "28P01")
	if _, err := admin.Exec(ctx, "DROP TOKEN ci FOR USER dbt"); pgCode(err) != "42704" {
		t.Errorf("dropping a dropped token: %v", err)
	}
	mustExec(t, admin, "DROP TOKEN IF EXISTS ci FOR USER dbt")

	// A session ends once the token it signed in with has expired.
	brief, short, expires := tokenSession(t, ts, admin, "dbt", "CREATE TOKEN brief FOR USER dbt EXPIRES '%ds'", "DROP TOKEN brief FOR USER dbt")
	time.Sleep(time.Until(expires) + 100*time.Millisecond)
	if _, err := short.Exec(ctx, "SELECT 1"); pgCode(err) != "28000" || !strings.Contains(err.Error(), "expired") {
		t.Errorf("a session of an expired token: %v", err)
	}
	loginFails(t, ts, "dbt", brief, "28P01")
	mustExec(t, admin, "DROP TOKEN brief FOR USER dbt")

	// A service account always has an admin who owns it.
	mustExec(t, admin, "CREATE USER boss PASSWORD 'boss-password' ROLE admin")
	if _, err := admin.Exec(ctx, "ALTER USER dbt OWNER ana"); pgCode(err) != "22023" {
		t.Errorf("a non-admin owner: %v", err)
	}
	// A refused change changes nothing.
	if _, err := admin.Exec(ctx, "ALTER USER dbt ROLE reader OWNER ana"); pgCode(err) != "22023" {
		t.Errorf("a non-admin owner with a role: %v", err)
	}
	if u, _ := dir.Get(ctx, "dbt"); u.Role != auth.RoleWriter {
		t.Errorf("a refused change set the role to %q", u.Role)
	}
	mustExec(t, admin, "ALTER USER dbt OWNER boss")
	if u, _ := dir.Get(ctx, "dbt"); u.Owner != "boss" {
		t.Errorf("owner %q", u.Owner)
	}
	if _, err := admin.Exec(ctx, "DROP USER boss"); pgCode(err) != "2BP01" {
		t.Errorf("dropping an owner: %v", err)
	}
	mustExec(t, admin, "ALTER USER dbt OWNER igloo")
	mustExec(t, admin, "DROP USER boss")
}

func TestPersonalTokens(t *testing.T) {
	t.Parallel()
	ts, _, _ := startDirectoryServer(t)
	admin := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()
	mustExec(t, admin, "CREATE USER rita PASSWORD 'rita-password' ROLE reader")
	rita := connect(t, ts.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)

	token := newToken(t, rita, "CREATE TOKEN tableau EXPIRES '7d'")
	viaToken := connect(t, ts.url("rita", token), pgx.QueryExecModeCacheStatement)
	mustExec(t, viaToken, "SELECT 1")
	// A token session may not mint more credentials.
	if _, err := viaToken.Exec(ctx, "CREATE TOKEN more"); pgCode(err) != "42501" {
		t.Errorf("a token made another: %v", err)
	}
	if _, err := viaToken.Exec(ctx, "ALTER USER CURRENT_USER PASSWORD 'stolen-password'"); pgCode(err) != "42501" {
		t.Errorf("a token session set the password: %v", err)
	}

	// Changing the password keeps the tokens working.
	mustExec(t, rita, "ALTER USER CURRENT_USER PASSWORD 'rita-password-2'")
	connect(t, ts.url("rita", "rita-password-2"), pgx.QueryExecModeSimpleProtocol)
	connect(t, ts.url("rita", token), pgx.QueryExecModeSimpleProtocol)
	loginFails(t, ts, "rita", "rita-password", "28P01")
	// A verifier psql computed has a salt of its own, which would orphan
	// the tokens.
	other, _ := auth.NewVerifier("rita-password-3")
	if _, err := rita.Exec(ctx, "ALTER USER rita PASSWORD '"+other+"'"); pgCode(err) != "22023" {
		t.Errorf("a precomputed verifier with tokens: %v", err)
	}
	// Without a password, the tokens still sign in.
	mustExec(t, admin, "ALTER USER rita PASSWORD NULL")
	loginFails(t, ts, "rita", "rita-password-2", "28P01")
	connect(t, ts.url("rita", token), pgx.QueryExecModeSimpleProtocol)

	// Tokens of a user without a password use PostgreSQL's iteration count,
	// so a password with another one stays while its tokens exist.
	slow, _ := auth.NewVerifierWith("paul-password", scram.KeyFactors{Salt: "paul-salt-16byte", Iters: 8192})
	mustExec(t, admin, "CREATE USER paul PASSWORD '"+slow+"'")
	paul := connect(t, ts.url("paul", "paul-password"), pgx.QueryExecModeSimpleProtocol)
	newToken(t, paul, "CREATE TOKEN cli")
	if _, err := admin.Exec(ctx, "ALTER USER paul PASSWORD NULL"); pgCode(err) != "55000" {
		t.Errorf("removing a password whose tokens would stop: %v", err)
	}
	mustExec(t, paul, "DROP TOKEN cli")
	mustExec(t, admin, "ALTER USER paul PASSWORD NULL")

	second := newToken(t, rita, "CREATE TOKEN 'dbt-prod' EXPIRES IN '2w'")
	connect(t, ts.url("rita", second), pgx.QueryExecModeSimpleProtocol)

	rows, _ := rita.Query(ctx, "SHOW TOKENS")
	mine, err := pgx.CollectRows(rows, pgx.RowToMap)
	if err != nil || len(mine) != 2 || mine[0]["name"] != "dbt-prod" || mine[1]["name"] != "tableau" ||
		!strings.HasPrefix(mine[1]["prefix"].(string), "igloo_") || strings.Contains(token, mine[1]["prefix"].(string)) == false {
		t.Errorf("SHOW TOKENS: %v %v", mine, err)
	}
	mustExec(t, admin, "CREATE USER svc TYPE SERVICE")
	newToken(t, admin, "CREATE TOKEN t FOR USER svc")
	rows, _ = admin.Query(ctx, "SHOW TOKENS")
	if all, _ := pgx.CollectRows(rows, pgx.RowToMap); len(all) != 3 {
		t.Errorf("an admin's SHOW TOKENS: %v", all)
	}
	rows, _ = rita.Query(ctx, "SHOW TOKENS")
	if mine, _ := pgx.CollectRows(rows, pgx.RowToMap); len(mine) != 2 {
		t.Errorf("rita sees others' tokens: %v", mine)
	}
	if _, err := rita.Exec(ctx, "DROP TOKEN t FOR USER svc"); pgCode(err) != "42501" {
		t.Errorf("rita dropped another's token: %v", err)
	}
	mustExec(t, admin, "DROP TOKEN tableau FOR USER rita")
	loginFails(t, ts, "rita", token, "28P01")

	// Tokens expire.
	short, _, expires := tokenSession(t, ts, rita, "rita", "CREATE TOKEN short EXPIRES '%ds'", "DROP TOKEN short")
	time.Sleep(time.Until(expires) + 500*time.Millisecond)
	loginFails(t, ts, "rita", short, "28P01")

	for _, bad := range []string{"CREATE TOKEN 'has space'", "CREATE TOKEN x EXPIRES 'soon'", "CREATE TOKEN x EXPIRES '-1d'", "DROP TOKEN", "SHOW TOKENS FOR igloo"} {
		if _, err := rita.Exec(ctx, bad); err == nil || pgCode(err) == "" {
			t.Errorf("%s: %v", bad, err)
		}
	}
}

func TestTokensNeedAStore(t *testing.T) {
	t.Parallel()
	ts := startServer(t)
	admin := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	if _, err := admin.Exec(context.Background(), "CREATE TOKEN x"); pgCode(err) != "0A000" {
		t.Errorf("CREATE TOKEN without a token store: %v", err)
	}
}

func TestDisabledUsers(t *testing.T) {
	t.Parallel()
	ts, _, history := startDirectoryServer(t)
	admin := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()
	mustExec(t, admin, "CREATE USER walt PASSWORD 'walt-password' ROLE writer EMAIL 'Walt@Example.com'")
	walt := connect(t, ts.url("walt", "walt-password"), pgx.QueryExecModeSimpleProtocol)

	mustExec(t, admin, "ALTER USER walt NOLOGIN")
	if _, err := walt.Exec(ctx, "SELECT 1"); pgCode(err) != "28000" {
		t.Errorf("an open session of a disabled user: %v", err)
	}
	// Only a client that knows the password learns that the user may not
	// log in.
	loginFails(t, ts, "walt", "walt-password", "28000")
	loginFails(t, ts, "walt", "wrong-password", "28P01")
	logins, _ := history.Logins(ctx, audit.Viewer{All: true}, audit.Page{})
	if len(logins) == 0 || logins[0].User != "walt" || logins[0].Reason != "wrong password" ||
		logins[1].Reason != "user is disabled" {
		t.Errorf("history: %+v", logins[:2])
	}
	rows, _ := admin.Query(ctx, "SHOW USERS")
	users, _ := pgx.CollectRows(rows, pgx.RowToMap)
	for _, u := range users {
		if u["name"] == "walt" && (u["can_login"] != false || u["email"] != "walt@example.com") {
			t.Errorf("SHOW USERS of walt: %v", u)
		}
	}
	mustExec(t, admin, "ALTER USER walt LOGIN")
	connect(t, ts.url("walt", "walt-password"), pgx.QueryExecModeSimpleProtocol)

	if _, err := admin.Exec(ctx, "ALTER USER igloo NOLOGIN"); pgCode(err) != "0A000" {
		t.Errorf("disabling the last admin: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE USER wanda PASSWORD NULL EMAIL 'walt@example.com'"); pgCode(err) != "23505" {
		t.Errorf("a second user with walt's email: %v", err)
	}
	if _, err := admin.Exec(ctx, "ALTER USER walt EMAIL 'not an email'"); pgCode(err) != "22023" {
		t.Errorf("a malformed email: %v", err)
	}
	// Users change only their own password.
	again := connect(t, ts.url("walt", "walt-password"), pgx.QueryExecModeSimpleProtocol)
	for _, q := range []string{"ALTER USER CURRENT_USER EMAIL NULL", "ALTER USER CURRENT_USER NOLOGIN",
		"ALTER USER walt PASSWORD 'walt-password-2' EMAIL NULL"} {
		if _, err := again.Exec(ctx, q); pgCode(err) != "42501" {
			t.Errorf("%s: %v", q, err)
		}
	}
}

func TestDisabledOwnersJobsFail(t *testing.T) {
	t.Parallel()
	jobs := tasks.NewScheduler(tasks.NewMemoryStore(), tasks.Options{})
	ts, _, _ := startDirectoryServer(t, func(o *Options) { o.Tasks = jobs })
	admin := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()
	mustExec(t, admin, "CREATE USER walt PASSWORD 'walt-password' ROLE writer")
	walt := connect(t, ts.url("walt", "walt-password"), pgx.QueryExecModeSimpleProtocol)
	mustExec(t, walt, "CREATE TASK tick SCHEDULE '1 hour' AS SELECT 1")
	mustExec(t, admin, "EXECUTE TASK tick")

	mustExec(t, admin, "ALTER USER walt NOLOGIN")
	if _, err := admin.Exec(ctx, "EXECUTE TASK tick"); pgCode(err) != "55000" || !strings.Contains(err.Error(), "disabled") {
		t.Errorf("a disabled owner's task ran: %v", err)
	}
	// It is not suspended: it runs again once the owner may sign in.
	mustExec(t, admin, "ALTER USER walt LOGIN")
	mustExec(t, admin, "EXECUTE TASK tick")

	// A task, which a token's session may have made, cannot give its
	// owner a credential.
	token := newToken(t, walt, "CREATE TOKEN cli")
	viaToken := connect(t, ts.url("walt", token), pgx.QueryExecModeSimpleProtocol)
	mustExec(t, viaToken, "CREATE TASK keep SCHEDULE '1 hour' AS ALTER USER CURRENT_USER PASSWORD 'stolen-password'")
	mustExec(t, viaToken, "CREATE TASK mint SCHEDULE '1 hour' AS CREATE TOKEN more")
	for _, task := range []string{"keep", "mint"} {
		if _, err := viaToken.Exec(ctx, "EXECUTE TASK "+task); pgCode(err) != "42501" {
			t.Errorf("task %s: %v", task, err)
		}
	}
	loginFails(t, ts, "walt", "stolen-password", "28P01")
}

func TestLoginLimits(t *testing.T) {
	t.Parallel()
	ts, _, history := startDirectoryServer(t)
	admin := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()
	mustExec(t, admin, "CREATE USER rita PASSWORD 'rita-password' ROLE reader")
	rita := connect(t, ts.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)
	token := newToken(t, rita, "CREATE TOKEN etl")

	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}
	for range userFailures {
		ts.srv.limiter.failed("rita", &net.TCPAddr{IP: net.IPv4(192, 0, 2, 9)})
	}
	// Even the right password is refused for a while, without being checked.
	_, err := tryConnect(ts.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "28000" || !strings.Contains(pgErr.Message, "too many failed logins") {
		t.Errorf("a blocked user: %v", err)
	}
	connect(t, ts.url("igloo", adminPassword), pgx.QueryExecModeSimpleProtocol)
	logins, _ := history.Logins(ctx, audit.Viewer{All: true}, audit.Page{})
	if len(logins) < 2 || logins[1].Reason != "too many failed logins" {
		t.Errorf("history: %+v", logins)
	}
	// Access tokens, which cannot be guessed, still sign in.
	connect(t, ts.url("rita", token), pgx.QueryExecModeSimpleProtocol)

	// Many failures from one address block it for every user.
	ts.srv.limiter.succeeded("rita")
	for range addrFailures {
		ts.srv.limiter.failed("someone", addr)
	}
	if _, err := tryConnect(ts.url("igloo", adminPassword), pgx.QueryExecModeSimpleProtocol); pgCode(err) != "28000" {
		t.Errorf("a blocked address: %v", err)
	}
}

func TestLoginLimiter(t *testing.T) {
	t.Parallel()
	l := newLoginLimiter()
	now := time.Now()
	l.now = func() time.Time { return now }
	a := &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
	b := &net.TCPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	blocked := func(user string, addr net.Addr) time.Duration {
		done, wait := l.begin(user, addr)
		done()
		return wait
	}
	for i := range userFailures {
		if blocked("ana", a) != 0 {
			t.Fatalf("blocked after %d failures", i)
		}
		l.failed("ana", &net.TCPAddr{IP: a.IP, Port: i})
		now = now.Add(time.Second)
	}
	if w := blocked("ana", b); w <= 0 || w > failureWindow {
		t.Errorf("ana from another address: %s", w)
	}
	if blocked("bob", b) != 0 {
		t.Error("another user from another address is blocked")
	}
	now = now.Add(failureWindow - 10*time.Second)
	if blocked("ana", b) != 0 {
		t.Error("still blocked once the first failure aged out")
	}
	l.failed("ana", b)
	if blocked("ana", b) == 0 {
		t.Error("not blocked after another failure")
	}
	l.succeeded("ana")
	if blocked("ana", b) != 0 {
		t.Error("blocked after a success")
	}

	// Addresses, by host alone.
	for i := range addrFailures {
		l.failed("user"+string(rune('a'+i%26)), &net.TCPAddr{IP: b.IP, Port: i})
	}
	if blocked("zed", &net.TCPAddr{IP: b.IP, Port: 9999}) == 0 {
		t.Error("an address with many failures is not blocked")
	}
	if addrKey(nil) != "" || addrKey((*net.TCPAddr)(nil)) != "" {
		t.Error("addrKey of nothing")
	}

	// Logins still running count, so parallel connections get no more
	// guesses than the limit.
	l = newLoginLimiter()
	var dones []func()
	for range userFailures {
		done, wait := l.begin("bo", a)
		if wait != 0 {
			t.Fatal("blocked before the limit")
		}
		dones = append(dones, done)
	}
	if _, wait := l.begin("bo", b); wait == 0 {
		t.Error("a login beyond the limit started while the others ran")
	}
	for _, done := range dones {
		done()
		done()
	}
	if _, wait := l.begin("bo", b); wait != 0 {
		t.Error("finished logins still count")
	}

	// A flood of names does not wipe out a blocked user's count.
	l = newLoginLimiter()
	for range userFailures {
		l.failed("victim", nil)
	}
	for i := range maxTracked + 10 {
		l.failed(fmt.Sprintf("flood%d", i), nil)
	}
	if blocked("victim", nil) == 0 {
		t.Error("the flood unblocked the user")
	}
}

func TestParseTokenAndGroupCommands(t *testing.T) {
	t.Parallel()
	for q, want := range map[string]tokenCommand{
		"CREATE TOKEN":                                        {kind: tokenCreate},
		"create token DBT expires '90d'":                      {kind: tokenCreate, name: "dbt", lifetime: 90 * 24 * time.Hour},
		`CREATE TOKEN "Tableau" FOR USER svc EXPIRES IN '1y'`: {kind: tokenCreate, name: "Tableau", user: "svc", lifetime: 365 * 24 * time.Hour},
		"CREATE TOKEN 'airflow.prod' EXPIRES '12h'":           {kind: tokenCreate, name: "airflow.prod", lifetime: 12 * time.Hour},
		"CREATE TOKEN FOR USER svc":                           {kind: tokenCreate, user: "svc"},
		"SHOW TOKENS":                                         {kind: tokenShow},
		"SHOW TOKENS FOR USER ana":                            {kind: tokenShow, user: "ana"},
		"DROP TOKEN IF EXISTS dbt FOR USER svc":               {kind: tokenDrop, name: "dbt", user: "svc", ifExists: true},
	} {
		cmds, ok, err := parseUserCommands(q)
		if !ok || err != nil || len(cmds) != 1 || cmds[0].token == nil || *cmds[0].token != want {
			t.Errorf("%s: %v %v %+v", q, ok, err, cmds)
		}
	}
	for _, q := range []string{"CREATE TOKEN a b", "CREATE TOKEN x EXPIRES 90", "SHOW TOKENS ana", "DROP TOKEN",
		"CREATE TOKEN x FOR USER", "CREATE TOKEN x EXPIRES '1d' EXPIRES '2d'", "DROP TOKEN x EXPIRES '1d'"} {
		if _, ok, err := parseUserCommands(q); !ok || err == nil {
			t.Errorf("%s: %v %v", q, ok, err)
		}
	}

	for q, want := range map[string]groupCommand{
		"GRANT analysts TO SSO GROUP 'Data Team'": {kind: groupGrant, role: "analysts", group: "Data Team"},
		"revoke WRITER from sso group \"g-1\"":    {kind: groupRevoke, role: "writer", group: "g-1"},
		"SHOW SSO GROUPS":                         {kind: groupShow},
	} {
		cmds, ok, err := parseUserCommands(q)
		if !ok || err != nil || len(cmds) != 1 || cmds[0].group == nil || *cmds[0].group != want {
			t.Errorf("%s: %v %v %+v", q, ok, err, cmds)
		}
	}
	for _, q := range []string{"GRANT analysts TO SSO GROUP", "GRANT analysts TO SSO GROUP data", "GRANT x TO SSO GROUP ''"} {
		if _, ok, err := parseUserCommands(q); !ok || err == nil {
			t.Errorf("%s: %v %v", q, ok, err)
		}
	}
	// A GRANT to a user named sso is a role grant, as before.
	if cmds, ok, err := parseUserCommands("GRANT analysts TO sso"); !ok || err != nil || cmds[0].acl == nil {
		t.Errorf("GRANT analysts TO sso: %v %v", ok, err)
	}
}

func TestParseLifetime(t *testing.T) {
	t.Parallel()
	century := 100 * 365 * 24 * time.Hour
	for in, want := range map[string]time.Duration{
		"90d": 90 * 24 * time.Hour, " 2W ": 14 * 24 * time.Hour, "1y": 365 * 24 * time.Hour, "36h": 36 * time.Hour,
		// Huge numbers stop at a century rather than overflowing.
		"99999999999y": century, "9999999999w": century - century%(7*24*time.Hour), "9999999999999d": century,
	} {
		if got, err := parseLifetime(in); err != nil || got != want {
			t.Errorf("%q: %s %v, want %s", in, got, err, want)
		}
	}
	for _, in := range []string{"", "d", "0d", "-1d", "1.5d", "forever", "99999999999999999999y"} {
		if got, err := parseLifetime(in); err == nil {
			t.Errorf("%q: %s", in, got)
		}
	}
}

func TestSSOGroupMappings(t *testing.T) {
	t.Parallel()
	ts, dir, _ := startDirectoryServer(t)
	admin := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()
	if _, err := admin.Exec(ctx, "GRANT analysts TO SSO GROUP 'Data Team'"); pgCode(err) != "42704" {
		t.Errorf("mapping to a missing role: %v", err)
	}
	mustExec(t, admin, "CREATE ROLE analysts")
	mustExec(t, admin, "GRANT analysts TO SSO GROUP 'Data Team'")
	mustExec(t, admin, "GRANT writer TO SSO GROUP 'Data Team'")
	mustExec(t, admin, "GRANT admin TO SSO GROUP 'igloo-admins'")
	rows, _ := admin.Query(ctx, "SHOW SSO GROUPS")
	got, err := pgx.CollectRows(rows, pgx.RowToMap)
	if err != nil || len(got) != 3 || got[0]["group_name"] != "Data Team" || got[0]["role_type"] != "custom" ||
		got[2]["role_type"] != "built-in" || got[0]["granted_by"] != "igloo" {
		t.Errorf("SHOW SSO GROUPS: %v %v", got, err)
	}
	mustExec(t, admin, "REVOKE admin FROM SSO GROUP 'igloo-admins'")
	if m, _ := dir.GroupRoles(ctx); len(m) != 2 {
		t.Errorf("after REVOKE: %+v", m)
	}
	mustExec(t, admin, "CREATE USER rita PASSWORD 'rita-password' ROLE reader")
	rita := connect(t, ts.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)
	for _, q := range []string{"SHOW SSO GROUPS", "GRANT analysts TO SSO GROUP 'x'", "ALTER USER rita SSO NULL"} {
		if _, err := rita.Exec(ctx, q); pgCode(err) != "42501" {
			t.Errorf("rita: %s: %v", q, err)
		}
	}

	// An admin unlinks a user from their identity provider account.
	if err := dir.LinkIdentity(ctx, auth.Identity{Issuer: "https://idp", Subject: "s-rita", User: "rita"}); err != nil {
		t.Fatal(err)
	}
	ssoOf := func() any {
		rows, _ := admin.Query(ctx, "SHOW USERS")
		users, _ := pgx.CollectRows(rows, pgx.RowToMap)
		for _, u := range users {
			if u["name"] == "rita" {
				return u["sso"]
			}
		}
		return nil
	}
	if ssoOf() != true {
		t.Error("SHOW USERS does not show rita's link")
	}
	mustExec(t, admin, "ALTER USER rita SSO NULL")
	if _, err := dir.IdentityOf(ctx, "rita"); !errors.Is(err, auth.ErrIdentityNotFound) || ssoOf() != false {
		t.Errorf("after SSO NULL: %v", err)
	}
	for _, q := range []string{"ALTER USER rita SSO 'x'", "CREATE USER x PASSWORD NULL SSO NULL"} {
		if _, err := admin.Exec(ctx, q); pgCode(err) != "42601" {
			t.Errorf("%s: %v", q, err)
		}
	}
}

// fakeAccessTokens accepts "idp-token-for-<user>".
type fakeAccessTokens struct{}

func (fakeAccessTokens) VerifyAccessToken(_ context.Context, user auth.User, token string) error {
	if token != "idp-token-for-"+user.Name {
		return errors.New("bad token")
	}
	return nil
}

func TestAccessTokenLogins(t *testing.T) {
	t.Parallel()
	ts, dir, history := startDirectoryServer(t, func(o *Options) {
		o.TLS = selfSignedTLS(t)
		o.AccessTokens = fakeAccessTokens{}
	})
	admin := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()
	mustExec(t, admin, "CREATE USER nora PASSWORD NULL ROLE reader")
	tlsURL := func(user, password string) string {
		return strings.Replace(ts.url(user, password), "sslmode=disable", "sslmode=require", 1)
	}

	// Inside TLS, a user without a password may send the identity
	// provider's access token, in the clear. Like Igloo's own tokens, it
	// cannot mint longer-lived credentials.
	nora := connect(t, tlsURL("nora", "idp-token-for-nora"), pgx.QueryExecModeSimpleProtocol)
	for _, q := range []string{"CREATE TOKEN dbt", "ALTER USER CURRENT_USER PASSWORD 'nora-password-1'"} {
		if _, err := nora.Exec(ctx, q); pgCode(err) != "42501" {
			t.Errorf("an access token session: %s: %v", q, err)
		}
	}
	if _, err := tryConnect(tlsURL("nora", "idp-token-for-rita"), pgx.QueryExecModeSimpleProtocol); pgCode(err) != "28P01" {
		t.Errorf("another user's access token: %v", err)
	}
	// nora makes her tokens in the console, as single sign-on users do.
	u, _ := dir.Get(ctx, "nora")
	factors, _ := auth.TokenFactors(u)
	token, tok, hashes, err := auth.NewToken("nora", "dbt", "nora", time.Now().Add(time.Hour), factors)
	if err != nil {
		t.Fatal(err)
	}
	if err := dir.CreateToken(ctx, tok, hashes); err != nil {
		t.Fatal(err)
	}
	// Igloo's own tokens work both ways.
	connect(t, tlsURL("nora", token), pgx.QueryExecModeSimpleProtocol)
	connect(t, ts.url("nora", token), pgx.QueryExecModeSimpleProtocol)
	// Without TLS the login is SCRAM, which an access token cannot pass.
	if _, err := tryConnect(ts.url("nora", "idp-token-for-nora"), pgx.QueryExecModeSimpleProtocol); pgCode(err) != "28P01" {
		t.Errorf("an access token without TLS: %v", err)
	}
	// Users with a password keep SCRAM.
	connect(t, tlsURL("igloo", adminPassword), pgx.QueryExecModeSimpleProtocol)

	logins, _ := history.Logins(ctx, audit.Viewer{All: true}, audit.Page{})
	methods := map[string]int{}
	for _, l := range logins {
		if l.User == "nora" && l.Result == audit.LoginSuccess {
			methods[l.Method+"/"+l.Token]++
		}
	}
	if methods["access_token/"] != 1 || methods["token/dbt"] != 2 {
		t.Errorf("nora's logins: %v", methods)
	}
	mustExec(t, admin, "ALTER USER nora NOLOGIN")
	if _, err := tryConnect(tlsURL("nora", "idp-token-for-nora"), pgx.QueryExecModeSimpleProtocol); pgCode(err) != "28000" {
		t.Errorf("a disabled user's access token: %v", err)
	}
}

// flip changes a token's last character.
func flip(s string) string {
	if strings.HasSuffix(s, "a") {
		return s[:len(s)-1] + "b"
	}
	return s[:len(s)-1] + "a"
}
