package pgwire

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/notify"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// hooks is a webhook receiver that keeps what it is sent.
type hooks struct {
	*httptest.Server
	mu    sync.Mutex
	posts []hookPost
}

type hookPost struct {
	Path    string
	Type    string
	Name    string
	User    string
	Title   string
	Message string
	Details map[string]string
	Result  *struct {
		Columns       []string
		Rows          [][]*string
		More          bool
		HiddenColumns int `json:"hidden_columns"`
	}
	Body string `json:"-"`
}

func newHooks(t *testing.T) *hooks {
	t.Helper()
	h := &hooks{}
	h.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var p hookPost
		_ = json.Unmarshal(body, &p)
		p.Path, p.Body = r.URL.Path, string(body)
		h.mu.Lock()
		h.posts = append(h.posts, p)
		h.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(h.Close)
	return h
}

func (h *hooks) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.posts)
}

// wait waits for the nth post and returns it.
func (h *hooks) wait(t *testing.T, n int) hookPost {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for h.count() < n {
		if time.Now().After(deadline) {
			t.Fatalf("waited for webhook post %d; got %d", n, h.count())
		}
		time.Sleep(20 * time.Millisecond)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.posts[n-1]
}

// alertServer is a catalog server whose notifications may reach
// 127.0.0.1, with a webhook receiver there.
type alertServer struct {
	*catalogServer
	notify *notify.Service
	sched  *tasks.Scheduler
	hooks  *hooks
	dir    string
}

func startAlertServer(t *testing.T, fixture ...string) *alertServer {
	t.Helper()
	as := &alertServer{hooks: newHooks(t), dir: t.TempDir()}
	as.notify = notify.New(notify.Options{AllowHosts: []string{"127.0.0.1"}})
	as.sched = tasks.NewScheduler(tasks.NewMemoryStore(), tasks.Options{Ended: as.notify.RunEnded})
	as.catalogServer = startCatalogServerWithConfig(t, config.Config{Storage: config.Storage{AllowedLocations: []string{as.dir + "/"}}},
		func(o *Options) { o.Tasks, o.Notify = as.sched, as.notify })
	for _, sql := range fixture {
		mustExec(t, as.admin, sql)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		as.notify.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return as
}

func (as *alertServer) runScheduler(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		as.sched.Run(ctx, as.srv)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		as.sched.Wait(ctx)
	})
}

func TestNotificationIntegrations(t *testing.T) {
	t.Parallel()
	as := startAlertServer(t)
	admin := as.admin
	const token = "T000/B000/s3cr3t-hook-token"
	hookURL := as.hooks.URL + "/services/" + token
	mustExec(t, admin, "CREATE NOTIFICATION INTEGRATION ops TYPE slack URL '"+hookURL+"' COMMENT 'the ops channel'")

	shown := texts(t, admin, "SHOW INTEGRATIONS")
	if len(shown) != 1 || !strings.HasPrefix(shown[0], "ops|slack|true|"+as.hooks.URL+"|NULL|NULL|the ops channel|igloo|") {
		t.Errorf("SHOW INTEGRATIONS: %q", shown)
	}
	if n := count(t, admin, "SELECT count(*) FROM igloo.query_history WHERE query_text LIKE '%s3cr3t%'"); n != 0 {
		t.Errorf("the webhook's token is in the query history %d times", n)
	}
	if n := count(t, admin, "SELECT count(*) FROM igloo.query_history WHERE statement_type = 'CREATE NOTIFICATION INTEGRATION'"); n != 1 {
		t.Errorf("CREATE NOTIFICATION INTEGRATION in the history: %d", n)
	}

	walt := as.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	expectDenied(t, walt, "CREATE NOTIFICATION INTEGRATION mine TYPE webhook URL 'https://example.com/hook'", "admin")
	expectDenied(t, walt, "GRANT USAGE ON INTEGRATION ops TO walt", "admin")
	expectDenied(t, walt, "DROP INTEGRATION ops", "admin")

	// Integrations reach public addresses only, unless allowed.
	for sql, code := range map[string]string{
		"CREATE NOTIFICATION INTEGRATION x TYPE webhook URL 'https://169.254.169.254/latest/meta-data/'":     "42501",
		"CREATE NOTIFICATION INTEGRATION x TYPE webhook URL 'https://10.0.0.1/hook'":                         "42501",
		"CREATE NOTIFICATION INTEGRATION x TYPE webhook URL 'https://localhost/hook'":                        "42501",
		"CREATE NOTIFICATION INTEGRATION x TYPE webhook URL 'https://[::1]:8443/hook'":                       "42501",
		"CREATE NOTIFICATION INTEGRATION x TYPE webhook URL 'http://example.com/hook'":                       "22023",
		"CREATE NOTIFICATION INTEGRATION x TYPE webhook URL 'https://user:pw@example.com/hook'":              "22023",
		"CREATE NOTIFICATION INTEGRATION x TYPE pager URL 'https://example.com/hook'":                        "22023",
		"CREATE NOTIFICATION INTEGRATION x URL 'https://example.com/hook'":                                   "42601",
		"CREATE NOTIFICATION INTEGRATION x TYPE slack URL 'https://example.com/' SECRET 'a-long-secret-123'": "22023",
		"CREATE NOTIFICATION INTEGRATION x TYPE email RECIPIENTS ('ana@agency.example')":                     "22023",
		"CREATE NOTIFICATION INTEGRATION x TYPE webhook URL 'https://example.com/' FLAVOR 'mint'":            "42601",
		"CREATE NOTIFICATION INTEGRATION x TYPE webhook URL 'a' URL 'b'":                                     "42601",
		"CREATE NOTIFICATION INTEGRATION ops TYPE slack URL '" + hookURL + "'":                               "42710",
		"CREATE NOTIFICATION INTEGRATION \"Bad Name\" TYPE slack URL '" + hookURL + "'":                      "42602",
		"CREATE NOTIFICATION INTEGRATION x TYPE slack URL '" + hookURL + "'; SELECT 1":                       "0A000",
		"ALTER NOTIFICATION INTEGRATION ops SET TYPE = webhook":                                              "0A000",
		"ALTER NOTIFICATION INTEGRATION ops SET OPERATIONAL_ALERTS = (BOGUS)":                                "22023",
		"ALTER NOTIFICATION INTEGRATION nope SET ENABLED = FALSE":                                            "42704",
		"GRANT USAGE ON INTEGRATION nope TO walt":                                                            "42704",
		"GRANT USAGE ON INTEGRATION ops TO nobody":                                                           "42704",
		"DROP INTEGRATION nope": "42704",
	} {
		if _, err := admin.Exec(context.Background(), sql); pgCode(err) != code {
			t.Errorf("%s: %v, want %s", sql, err, code)
		} else if strings.Contains(err.Error(), "s3cr3t") {
			t.Errorf("%s: the error shows the token: %v", sql, err)
		}
	}
	mustExec(t, admin, "CREATE NOTIFICATION INTEGRATION IF NOT EXISTS ops TYPE slack URL 'https://example.com/other'")
	mustExec(t, admin, "DROP INTEGRATION IF EXISTS nope")

	// Others see the integrations they may use, without who else may.
	mustExec(t, admin, "CREATE ROLE analysts")
	mustExec(t, admin, "GRANT analysts TO walt")
	if got := texts(t, walt, "SHOW INTEGRATIONS"); len(got) != 0 {
		t.Errorf("walt sees %q before a grant", got)
	}
	mustExec(t, admin, "GRANT USAGE ON INTEGRATION ops TO ROLE analysts")
	if got := texts(t, walt, "SHOW INTEGRATIONS"); len(got) != 1 || !strings.HasPrefix(got[0], "ops|slack|true|"+as.hooks.URL+"|NULL|NULL|") {
		t.Errorf("walt's integrations: %q", got)
	}
	if got := texts(t, admin, "SHOW NOTIFICATION INTEGRATIONS"); len(got) != 1 || !strings.Contains(got[0], "|role analysts|") {
		t.Errorf("the admin's view: %q", got)
	}

	mustExec(t, admin, "ALTER NOTIFICATION INTEGRATION ops SET ENABLED = FALSE, OPERATIONAL_ALERTS = (FAILED_RUNS, IDLE_PIPES = 6)")
	if got := texts(t, admin, "SHOW INTEGRATIONS"); len(got) != 1 || !strings.HasPrefix(got[0], "ops|slack|false|"+as.hooks.URL+"|failed_runs, idle_pipes = 6|") {
		t.Errorf("after ALTER: %q", got)
	}
	mustExec(t, admin, "ALTER NOTIFICATION INTEGRATION ops UNSET OPERATIONAL_ALERTS, COMMENT")
	if got := texts(t, admin, "SHOW INTEGRATIONS"); len(got) != 1 || !strings.HasPrefix(got[0], "ops|slack|false|"+as.hooks.URL+"|NULL|role analysts|NULL|") {
		t.Errorf("after UNSET: %q", got)
	}

	// Dropping a role takes its grants with it, so a new role of the
	// same name does not inherit them.
	mustExec(t, admin, "DROP ROLE analysts")
	mustExec(t, admin, "CREATE ROLE analysts")
	mustExec(t, admin, "GRANT analysts TO walt")
	if got := texts(t, walt, "SHOW INTEGRATIONS"); len(got) != 0 {
		t.Errorf("a dropped role's grant survives: %q", got)
	}
	mustExec(t, admin, "DROP NOTIFICATION INTEGRATION ops")
	if got := texts(t, admin, "SHOW INTEGRATIONS"); len(got) != 0 {
		t.Errorf("after DROP: %q", got)
	}
}

func TestAlerts(t *testing.T) {
	as := startAlertServer(t, policyFixture...)
	admin := as.admin
	mustExec(t, admin, "CREATE NOTIFICATION INTEGRATION hook TYPE webhook URL '"+as.hooks.URL+"/hook'")
	walt := as.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	rita := as.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "GRANT acme_team TO walt")

	const create = `CREATE ALERT contacts SCHEDULE '1 hour' IF (EXISTS (
		SELECT id, email FROM lake.crm.contacts ORDER BY id)) THEN NOTIFY hook MESSAGE 'New contact: {{email}}' WITH RESULT`
	expectDenied(t, walt, create, "USAGE")
	mustExec(t, admin, "GRANT USAGE ON INTEGRATION hook TO walt")
	mustExec(t, admin, "GRANT USAGE ON INTEGRATION hook TO rita")
	expectDenied(t, rita, strings.Replace(create, "contacts SCHEDULE", "r SCHEDULE", 1), "writer")
	mustExec(t, walt, create)

	a := showJobs(t, walt, "ALERTS")["contacts"]
	if a.owner != "walt" || a.state != "suspended" || a.nextRun != nil ||
		!strings.HasPrefix(a.definition, "IF (EXISTS (\nSELECT id, email FROM lake.crm.contacts ORDER BY id\n)) THEN NOTIFY hook") {
		t.Fatalf("new alert: %+v", a)
	}

	// It runs as walt, through walt's policies: acme's rows, emails
	// masked, in the message and the result alike.
	mustExec(t, walt, "EXECUTE ALERT contacts")
	p := as.hooks.wait(t, 1)
	if p.Type != "alert" || p.Name != "contacts" || p.User != "walt" || p.Message != "New contact: ***" || p.Result == nil ||
		len(p.Result.Rows) != 2 || *p.Result.Rows[0][0] != "1" || *p.Result.Rows[0][1] != "***" || p.Result.More {
		t.Errorf("payload: %s", p.Body)
	}
	for _, s := range secrets {
		if strings.Contains(p.Body, s) {
			t.Errorf("the notification shows %q: %s", s, p.Body)
		}
	}
	expectRows(t, walt, "SELECT alert_type, alert_name, user_name, trigger, state, row_count, integration, attempts FROM igloo.alert_history",
		"alert|contacts|walt|manual|triggered|2|hook|1")
	expectRows(t, rita, "SELECT count(*) FROM igloo.alert_history", "0")
	expectRows(t, admin, "SELECT count(*) FROM igloo.alert_history", "1")
	expectRows(t, walt, "SELECT outcome, row_count FROM igloo.task_history WHERE kind = 'alert' AND name = 'contacts'", "succeeded|2")
	if n := count(t, admin, "SELECT count(*) FROM igloo.query_history WHERE user_name = 'walt' AND client_address = 'alert contacts'"); n != 1 {
		t.Errorf("the condition is in walt's query history %d times", n)
	}

	// A condition that finds nothing sends nothing; a big result is cut.
	mustExec(t, walt, "CREATE ALERT quiet SCHEDULE '1 hour' IF (EXISTS (SELECT 1 WHERE false)) THEN NOTIFY hook")
	mustExec(t, walt, "EXECUTE ALERT quiet")
	mustExec(t, walt, "CREATE ALERT big SCHEDULE '1 hour' IF (EXISTS (SELECT range AS n FROM range(100))) THEN NOTIFY hook WITH RESULT")
	mustExec(t, walt, "EXECUTE ALERT big")
	if p := as.hooks.wait(t, 2); p.Name != "big" || p.Result == nil || len(p.Result.Rows) != notify.MaxResultRows || !p.Result.More ||
		p.Message != "The condition of the alert big found more than 20 rows." {
		t.Errorf("a big result: %s", p.Body)
	}
	expectRows(t, walt, "SELECT alert_name, state, row_count FROM igloo.alert_history ORDER BY start_time",
		"contacts|triggered|2", "quiet|condition_false|0", "big|triggered|21")

	for sql, code := range map[string]string{
		"CREATE ALERT b SCHEDULE '1 hour' IF (EXISTS (DELETE FROM lake.crm.contacts)) THEN NOTIFY hook":          "42601",
		"CREATE ALERT b SCHEDULE '1 hour' IF (EXISTS (SELECT 1; DROP TABLE lake.crm.contacts)) THEN NOTIFY hook": "0A000",
		"CREATE ALERT b SCHEDULE '1 hour' IF (EXISTS (SHOW TASKS)) THEN NOTIFY hook":                             "",
		"CREATE ALERT b IF (EXISTS (SELECT 1)) THEN NOTIFY hook":                                                 "42601",
		"CREATE ALERT b SCHEDULE '1 hour' AS SELECT 1":                                                           "42601",
		"CREATE ALERT b SCHEDULE '1 hour' IF (EXISTS (SELECT 1)) THEN DROP TABLE lake.crm.contacts":              "42601",
		"CREATE ALERT b SCHEDULE '1 hour' IF (EXISTS (SELECT 1 AS x)) THEN NOTIFY hook MESSAGE '{{y}}'":          "42703",
		"CREATE ALERT b SCHEDULE '1 hour' IF (EXISTS (SELECT 1)) THEN NOTIFY nope":                               "42704",
		"CREATE ALERT b SCHEDULE '1 hour' IF (EXISTS (SELECT 1)) THEN NOTIFY hook MESSAGE 'x' MESSAGE 'y'":       "42601",
		"CREATE ALERT b SCHEDULE '1 hour' IF (EXISTS (SELECT 1)) THEN NOTIFY hook WITH RESULT junk":              "42601",
		"CREATE ALERT b SCHEDULE '1 hour' IF (EXISTS (SELECT * FROM lake.public.nope)) THEN NOTIFY hook":         "42P01",
		"CREATE ALERT contacts SCHEDULE '1 hour' IF (EXISTS (SELECT 1)) THEN NOTIFY hook":                        "42710",
		"CREATE TASK t SCHEDULE '1 hour' AS EXECUTE ALERT contacts":                                              "0A000",
		"ALTER ALERT nope RESUME": "42704",
		"EXECUTE ALERT nope":      "42704",
	} {
		_, err := walt.Exec(context.Background(), sql)
		if code == "" && err == nil || code != "" && pgCode(err) != code {
			t.Errorf("%s: %v, want %s", sql, err, code)
		}
	}
	expectDenied(t, rita, "EXECUTE ALERT contacts", "belongs to walt")

	// The owner needs USAGE at every run, not only when creating it.
	mustExec(t, admin, "REVOKE USAGE ON INTEGRATION hook FROM walt")
	expectDenied(t, walt, "EXECUTE ALERT contacts", "USAGE")
	expectRows(t, walt, "SELECT state, integration, attempts FROM igloo.alert_history WHERE alert_name = 'contacts' ORDER BY start_time DESC LIMIT 1",
		"action_failed|hook|0")
	mustExec(t, admin, "GRANT USAGE ON INTEGRATION hook TO walt")

	// On its schedule; the gateway learns when it is due. Alerts run at
	// most once a minute, which this test shortens.
	expectRefused(t, walt, "ALTER ALERT contacts SET SCHEDULE '1 second'", "22023")
	shortAlertInterval(t)
	mustExec(t, walt, "ALTER ALERT contacts SET SCHEDULE '1 second'")
	mustExec(t, walt, "ALTER ALERT contacts RESUME")
	as.sched.Kick()
	if due, ok, err := as.sched.NextDue(context.Background()); !ok || err != nil || time.Until(due) > 2*time.Second {
		t.Errorf("next due: %v %v %v", due, ok, err)
	}
	before := as.hooks.count()
	as.runScheduler(t)
	if p := as.hooks.wait(t, before+1); p.Name != "contacts" {
		t.Errorf("a scheduled run sent %s", p.Body)
	}
	mustExec(t, walt, "ALTER ALERT contacts SUSPEND")
	eventually(t, walt, "SELECT count(*) FROM igloo.alert_history WHERE alert_name = 'contacts' AND trigger = 'schedule' "+
		"AND state = 'triggered' LIMIT 1", 1)

	// While its owner is disabled (as ALTER USER ... NOLOGIN does with a
	// metadata database), its runs are skipped.
	as.store.mu.Lock()
	u := as.store.users["walt"]
	u.Disabled = true
	as.store.users["walt"] = u
	as.store.mu.Unlock()
	if _, err := admin.Exec(context.Background(), "EXECUTE ALERT contacts"); err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Errorf("the alert of a disabled owner: %v", err)
	}
	expectRows(t, admin, "SELECT state FROM igloo.alert_history WHERE alert_name = 'contacts' ORDER BY start_time DESC LIMIT 1", "skipped")
	expectRows(t, admin, "SELECT outcome FROM igloo.task_history WHERE name = 'contacts' ORDER BY start_time DESC LIMIT 1", "skipped")
	mustExec(t, admin, "DROP ALERT contacts")
	if _, ok := showJobs(t, admin, "ALERTS")["contacts"]; ok {
		t.Error("a dropped alert is listed")
	}
}

// shortAlertInterval lets alerts run every second until the test ends. It
// sets a package variable, so tests that call it do not run in parallel.
func shortAlertInterval(t *testing.T) {
	saved := minAlertInterval
	minAlertInterval = time.Second
	t.Cleanup(func() { minAlertInterval = saved })
}

// TestAlertDefinitionIsWhatRuns creates an alert whose condition ends in a
// comment and whose MESSAGE holds SQL: what runs and what is notified are
// what was checked, and SHOW ALERTS shows it so that it reads back the same.
func TestAlertDefinitionIsWhatRuns(t *testing.T) {
	t.Parallel()
	as := startAlertServer(t)
	admin := as.admin
	walt := as.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	for _, name := range []string{"a", "b"} {
		mustExec(t, admin, "CREATE NOTIFICATION INTEGRATION "+name+" TYPE webhook URL '"+as.hooks.URL+"/"+name+"'")
	}
	mustExec(t, admin, "GRANT USAGE ON INTEGRATION a TO walt")
	mustExec(t, walt, "CREATE ALERT tricky SCHEDULE '1 hour' IF (EXISTS (SELECT 1 AS n --\n)) THEN NOTIFY a "+
		"MESSAGE 'x\nUNION ALL SELECT 2)) THEN NOTIFY b' WITH RESULT")
	mustExec(t, walt, "EXECUTE ALERT tricky")
	p := as.hooks.wait(t, 1)
	if p.Path != "/a" || p.Result == nil || len(p.Result.Rows) != 1 || p.Message != "x\nUNION ALL SELECT 2)) THEN NOTIFY b" {
		t.Errorf("the alert sent to %s: %s", p.Path, p.Body)
	}
	def := showJobs(t, walt, "ALERTS")["tricky"].definition
	back, err := parseAlert(def)
	if err != nil || back.Condition != "SELECT 1 AS n --" || back.Integration != "a" || back.Message == nil ||
		*back.Message != "x\nUNION ALL SELECT 2)) THEN NOTIFY b" || !back.WithResult {
		t.Errorf("SHOW ALERTS shows %q, which reads back as %+v (%v)", def, back, err)
	}
}

// TestAlertLimits checks the minimum interval and the alerts a user may
// own.
func TestAlertLimits(t *testing.T) {
	t.Parallel()
	as := startAlertServer(t)
	mustExec(t, as.admin, "CREATE NOTIFICATION INTEGRATION hook TYPE webhook URL '"+as.hooks.URL+"/hook'")
	walt := as.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, as.admin, "GRANT USAGE ON INTEGRATION hook TO walt")
	const alert = " IF (EXISTS (SELECT 1)) THEN NOTIFY hook"
	for _, sched := range []string{"'1 second'", "'30 seconds'", "'59 SECOND'"} {
		expectRefused(t, walt, "CREATE ALERT fast SCHEDULE "+sched+alert, "22023")
	}
	for i := range maxAlertsPerOwner {
		mustExec(t, walt, fmt.Sprintf("CREATE ALERT a%d SCHEDULE '1 minute'", i)+alert)
	}
	expectRefused(t, walt, "CREATE ALERT one_more SCHEDULE '1 hour'"+alert, "53400")
	mustExec(t, walt, "CREATE OR REPLACE ALERT a0 SCHEDULE '* * * * *'"+alert)
	mustExec(t, walt, "DROP ALERT a1")
	mustExec(t, walt, "CREATE ALERT one_more SCHEDULE '1 hour'"+alert)
}

// TestAlertCaps checks that a wide result and a MESSAGE that names a long
// value many times are cut to the caps.
func TestAlertCaps(t *testing.T) {
	t.Parallel()
	as := startAlertServer(t)
	mustExec(t, as.admin, "CREATE NOTIFICATION INTEGRATION hook TYPE webhook URL '"+as.hooks.URL+"/hook'")
	var cols []string
	for i := range 200 {
		cols = append(cols, fmt.Sprintf("repeat('x', 1000) AS c%d", i))
	}
	mustExec(t, as.admin, "CREATE ALERT wide SCHEDULE '1 hour' IF (EXISTS (SELECT "+strings.Join(cols, ", ")+
		" FROM range(5))) THEN NOTIFY hook MESSAGE '"+strings.Repeat("{{c0}}", 600)+"' WITH RESULT")
	mustExec(t, as.admin, "EXECUTE ALERT wide")
	p := as.hooks.wait(t, 1)
	if len(p.Body) > 64<<10 || len(p.Message) > notify.MaxMessageBytes || p.Result == nil ||
		len(p.Result.Columns) != notify.MaxResultColumns || p.Result.HiddenColumns != 200-notify.MaxResultColumns || !p.Result.More {
		t.Errorf("a wide alert sent %d bytes: message %d bytes, result %+v", len(p.Body), len(p.Message), p.Result != nil)
	}
}

// TestScheduledAlertsLeaveTheScheduler checks that a scheduled alert's run
// ends once its condition has run, whatever its integration is like: the
// notification goes out from the integration's outbox, and the alert
// history records it once sent.
func TestScheduledAlertsLeaveTheScheduler(t *testing.T) {
	as := startAlertServer(t)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(slow.Close)
	mustExec(t, as.admin, "CREATE NOTIFICATION INTEGRATION slow TYPE webhook URL '"+slow.URL+"/hook'")
	shortAlertInterval(t)
	mustExec(t, as.admin, "CREATE ALERT ticking SCHEDULE '1 second' IF (EXISTS (SELECT 1)) THEN NOTIFY slow")
	mustExec(t, as.admin, "ALTER ALERT ticking RESUME")
	as.runScheduler(t)
	eventually(t, as.admin, "SELECT count(*) FROM igloo.alert_history WHERE alert_name = 'ticking' AND state = 'triggered' LIMIT 1", 1)
	mustExec(t, as.admin, "ALTER ALERT ticking SUSPEND")
	if n := count(t, as.admin, "SELECT count(*) FROM igloo.task_history WHERE name = 'ticking' AND outcome = 'succeeded' "+
		"AND duration_ms > 1500"); n != 0 {
		t.Errorf("%d scheduled runs waited for the slow integration", n)
	}
}

// TestDroppedOwnersWork checks that dropping a user disowns their alerts
// and tasks: a new user of the same name does not get them.
func TestDroppedOwnersWork(t *testing.T) {
	t.Parallel()
	as := startAlertServer(t)
	mustExec(t, as.admin, "CREATE NOTIFICATION INTEGRATION hook TYPE webhook URL '"+as.hooks.URL+"/hook'")
	walt := as.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, as.admin, "GRANT USAGE ON INTEGRATION hook TO walt")
	mustExec(t, walt, "CREATE ALERT watch SCHEDULE '1 hour' IF (EXISTS (SELECT 1)) THEN NOTIFY hook")
	mustExec(t, walt, "ALTER ALERT watch RESUME")
	mustExec(t, walt, "CREATE TASK tick SCHEDULE '1 hour' AS SELECT 1")
	mustExec(t, walt, "ALTER TASK tick RESUME")
	_ = walt.Close(context.Background())
	mustExec(t, as.admin, "DROP USER walt")
	for kind, name := range map[string]string{"ALERTS": "watch", "TASKS": "tick"} {
		if j := showJobs(t, as.admin, kind)[name]; j.owner != "walt (dropped)" || j.state != "suspended" || j.nextRun != nil {
			t.Errorf("%s %s after its owner was dropped: %+v", kind, name, j)
		}
	}
	walt2 := as.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	if n := len(showJobs(t, walt2, "ALERTS")) + len(showJobs(t, walt2, "TASKS")); n != 0 {
		t.Errorf("the new walt sees %d of the old one's jobs", n)
	}
	expectDenied(t, walt2, "ALTER ALERT watch RESUME", "walt (dropped)")
	expectDenied(t, walt2, "EXECUTE TASK tick", "walt (dropped)")
	if _, err := as.admin.Exec(context.Background(), "EXECUTE ALERT watch"); err == nil {
		t.Error("a dropped owner's alert ran")
	}
	mustExec(t, as.admin, "DROP ALERT watch")
}

// TestFailedLoginsCannotFlood checks the failed_logins alert's inputs: a
// client blocked from using passwords adds no failures, however malformed
// its logins; and names that are no user's raise no notice.
func TestFailedLoginsCannotFlood(t *testing.T) {
	t.Parallel()
	as := startAlertServer(t)
	mustExec(t, as.admin, "CREATE NOTIFICATION INTEGRATION ops TYPE webhook URL '"+as.hooks.URL+"/ops' "+
		"OPERATIONAL_ALERTS = (FAILED_LOGINS)")
	as.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	here := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}
	for i := range addrFailures {
		as.srv.limiter.failed(fmt.Sprintf("probe%d", i), here)
	}
	for _, mechanism := range []string{"BOGUS", "SCRAM-SHA-256"} {
		conn := rawStartup(t, as.addr, "please-call-555-0100")
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 512)
		if _, err := conn.Read(buf); err != nil { // AuthenticationSASL
			t.Fatal(err)
		}
		body := append([]byte(mechanism+"\x00"), 0xFF, 0xFF, 0xFF, 0xFE)
		msg := append([]byte{'p'}, binary.BigEndian.AppendUint32(nil, uint32(4+len(body)))...)
		_, _ = conn.Write(append(msg, body...))
		n, _ := conn.Read(buf)
		if !strings.Contains(string(buf[:n]), "too many failed logins") {
			t.Errorf("a malformed %s login while blocked: %q", mechanism, buf[:n])
		}
		_ = conn.Close()
	}
	as.srv.limiter.mu.Lock()
	failures := len(as.srv.limiter.users["please-call-555-0100"])
	as.srv.limiter.mu.Unlock()
	if failures != 0 {
		t.Errorf("a blocked client's malformed logins counted %d failures", failures)
	}

	// An address's limit raises a notice; a name's, only a user's.
	as.srv.notifyLoginsBlocked("please-call-555-0100", here, true, false)
	as.srv.notifyLoginsBlocked("walt", here, true, true)
	p := as.hooks.wait(t, 1)
	p2 := as.hooks.wait(t, 2)
	time.Sleep(200 * time.Millisecond)
	if as.hooks.count() != 2 || p.Details["address"] != "127.0.0.1" || p2.Details["user"] != "walt" {
		t.Errorf("notices: %s, %s (%d in all)", p.Body, p2.Body, as.hooks.count())
	}
}

func TestSendNotification(t *testing.T) {
	t.Parallel()
	as := startAlertServer(t)
	admin := as.admin
	mustExec(t, admin, "CREATE NOTIFICATION INTEGRATION hook TYPE webhook URL '"+as.hooks.URL+"/hook'")
	walt := as.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)

	expectDenied(t, walt, "CALL SYSTEM$SEND_NOTIFICATION('hook', 'hello')", "USAGE")
	mustExec(t, admin, "GRANT USAGE ON INTEGRATION hook TO walt")
	mustExec(t, walt, "CALL SYSTEM$SEND_NOTIFICATION('hook', 'hello from walt')")
	if p := as.hooks.wait(t, 1); p.Type != "call" || p.User != "walt" || p.Message != "hello from walt" {
		t.Errorf("payload: %s", p.Body)
	}

	// From a task, as its owner.
	mustExec(t, walt, "CREATE TASK ping SCHEDULE '1 hour' AS CALL SYSTEM$SEND_NOTIFICATION(hook, 'the nightly load is done')")
	mustExec(t, walt, "EXECUTE TASK ping")
	if p := as.hooks.wait(t, 2); p.Type != "call" || p.Name != "ping" || p.Message != "the nightly load is done" ||
		!strings.Contains(p.Title, "task ping") {
		t.Errorf("from a task: %s", p.Body)
	}
	// Refused messages are recorded too.
	expectRows(t, walt, "SELECT alert_type, coalesce(alert_name, ''), user_name, trigger, state FROM igloo.alert_history ORDER BY start_time",
		"call||walt|manual|action_failed", "call||walt|manual|triggered", "call|ping|walt|manual|triggered")

	// The message is what its sender wrote: never computed from data.
	for sql, code := range map[string]string{
		"CALL SYSTEM$SEND_NOTIFICATION('hook', (SELECT email FROM lake.public.orders))": "42601",
		"CALL SYSTEM$SEND_NOTIFICATION('hook', 'a' || 'b')":                             "42601",
		"CALL SYSTEM$SEND_NOTIFICATION('hook')":                                         "42601",
		"CALL SYSTEM$SEND_NOTIFICATION('nope', 'x')":                                    "42704",
	} {
		if _, err := walt.Exec(context.Background(), sql); pgCode(err) != code {
			t.Errorf("%s: %v, want %s", sql, err, code)
		}
	}
	mustExec(t, admin, "ALTER NOTIFICATION INTEGRATION hook SET ENABLED = FALSE")
	if _, err := walt.Exec(context.Background(), "CALL SYSTEM$SEND_NOTIFICATION('hook', 'x')"); pgCode(err) != "55000" {
		t.Errorf("a disabled integration: %v", err)
	}
}

// TestBuiltinAlertsFromSQL switches on built-in alerts for an integration
// and checks that a failed scheduled task and a failed load reach it.
func TestBuiltinAlertsFromSQL(t *testing.T) {
	t.Parallel()
	as := startAlertServer(t)
	admin := as.admin
	mustExec(t, admin, "CREATE NOTIFICATION INTEGRATION ops TYPE webhook URL '"+as.hooks.URL+"/ops' "+
		"OPERATIONAL_ALERTS = (FAILED_RUNS, LOAD_ERRORS)")
	walt := as.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)

	mustExec(t, walt, "CREATE TASK broken SCHEDULE '1 second' AS INSERT INTO lake.public.missing VALUES (1)")
	mustExec(t, walt, "ALTER TASK broken RESUME")
	as.runScheduler(t)
	p := as.hooks.wait(t, 1)
	if p.Type != "builtin" || p.Name != "failed_runs" || p.Details["name"] != "broken" || p.Details["owner"] != "walt" {
		t.Errorf("failed run: %s", p.Body)
	}
	mustExec(t, walt, "ALTER TASK broken SUSPEND")

	if err := os.WriteFile(filepath.Join(as.dir, "bad.parquet"), []byte("not parquet"), 0o644); err != nil {
		t.Fatal(err)
	}
	mustExec(t, walt, "CREATE TABLE lake.public.drops (id INTEGER)")
	mustExec(t, walt, "COPY INTO lake.public.drops FROM '"+as.dir+"/' (ON_ERROR skip_file)")
	deadline := time.Now().Add(10 * time.Second)
	for {
		var found bool
		as.hooks.mu.Lock()
		for _, p := range as.hooks.posts {
			found = found || p.Name == "load_errors" && p.Details["table"] == "lake.public.drops" && strings.HasSuffix(p.Details["file"], "bad.parquet")
		}
		as.hooks.mu.Unlock()
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no load_errors notification")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The history records a delivery once it has completed, just after the
	// receiver has the post.
	history := "SELECT DISTINCT alert_type, alert_name, state FROM igloo.alert_history ORDER BY alert_name"
	want := []string{"builtin|failed_runs|triggered", "builtin|load_errors|triggered"}
	for deadline := time.Now().Add(10 * time.Second); !slices.Equal(texts(t, admin, history), want) && time.Now().Before(deadline); {
		time.Sleep(20 * time.Millisecond)
	}
	expectRows(t, admin, history, want...)
	expectRows(t, walt, "SELECT count(*) FROM igloo.alert_history", "0")
}

func TestLoginLimiterReportsBursts(t *testing.T) {
	t.Parallel()
	l := newLoginLimiter()
	addr := &net.TCPAddr{IP: net.IPv4(192, 0, 2, 7)}
	reached := 0
	for range userFailures + 3 {
		if user, _ := l.failed("eve", addr); user {
			reached++
		}
	}
	if reached != 1 {
		t.Errorf("the user's limit was reached %d times", reached)
	}
	reached = 0
	for i := range addrFailures + 3 {
		if _, address := l.failed("u"+string(rune('a'+i%20)), addr); address {
			reached++
		}
	}
	if reached != 1 {
		t.Errorf("the address's limit was reached %d times", reached)
	}
}
