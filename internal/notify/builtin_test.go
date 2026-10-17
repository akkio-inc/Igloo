package notify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// drain delivers the built-in alerts waiting, as Run does, and waits for
// them to be sent or to fail.
func drain(ctx context.Context, s *Service) {
	for _, q := range []chan event{s.events, s.logins} {
	more:
		for {
			select {
			case e := <-q:
				s.deliver(ctx, e)
			default:
				break more
			}
		}
	}
	s.pending.Wait()
}

func payloads(t *testing.T, rc *receiver) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, r := range rc.got() {
		var p map[string]any
		if err := json.Unmarshal(r.body, &p); err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	return out
}

func TestBuiltinAlerts(t *testing.T) {
	ctx := context.Background()
	rc := newReceiver(t, false)
	quiet := newReceiver(t, false)
	jobs := tasks.NewMemoryStore()
	var running []Statement
	s := newTestService(t, Options{Jobs: jobs, Statements: func() []Statement { return running }})
	store := s.Store()
	ops := Integration{Name: "ops", Type: TypeWebhook, Enabled: true, URL: rc.URL,
		Builtin: Builtin{FailedRuns: true, LoadErrors: true, IdlePipes: 24 * time.Hour, LongQueries: time.Hour,
			FailedLogins: true, MaintenanceFailures: true}}
	for _, in := range []Integration{ops,
		{Name: "quiet", Type: TypeWebhook, Enabled: true, URL: quiet.URL},
		{Name: "off", Type: TypeWebhook, Enabled: false, URL: quiet.URL, Builtin: Builtin{FailedRuns: true}},
	} {
		if err := store.CreateIntegration(ctx, in, false); err != nil {
			t.Fatal(err)
		}
	}

	failed := tasks.Run{ID: "r1", Kind: tasks.KindTask, Name: "nightly", Outcome: tasks.OutcomeFailed, Trigger: tasks.TriggerSchedule,
		Start: time.Now(), Error: "Conversion Error: <!here> could not convert"}
	job := tasks.Job{Kind: tasks.KindTask, Name: "nightly", Owner: "walt"}
	s.RunEnded(job, failed)
	s.RunEnded(job, failed) // the same problem, again: once an hour
	manual := failed
	manual.Trigger = tasks.TriggerManual
	s.RunEnded(job, manual)
	ok := failed
	ok.Outcome = tasks.OutcomeSucceeded
	s.RunEnded(job, ok)
	s.RunEnded(tasks.Job{Kind: tasks.KindAlert, Name: "a", Owner: "walt"}, failed)
	s.LoadFailed("lake.raw.dsp", "walt", "dsp_drops", 2, 5, "s3://landing/dsp/bad.csv", "unterminated quote")
	s.LoginsBlocked("eve", true, 10, 15*time.Minute)
	s.MaintenanceFailed("raw.dsp", errors.New("compact: 503 Slow Down"))
	s.ErasureFailed("01a14588", "rewrite", "matching rows keep arriving in crm.events")
	drain(ctx, s)

	got := payloads(t, rc)
	var names []string
	for _, p := range got {
		names = append(names, p["name"].(string))
		if p["type"] != RecordBuiltin {
			t.Errorf("type %v", p["type"])
		}
	}
	if want := "failed_runs,load_errors,maintenance_failures,failed_runs,failed_logins"; strings.Join(names, ",") != want {
		t.Errorf("sent %v, want %s", names, want)
	}
	if len(got) > 0 {
		if d := got[0]["details"].(map[string]any); d["name"] != "nightly" || d["owner"] != "walt" {
			t.Errorf("details %v", d)
		}
	}
	if n := len(quiet.got()); n != 0 {
		t.Errorf("integrations without built-in alerts got %d", n)
	}

	// A pipe that has loaded nothing for longer than IDLE_PIPES, and a
	// statement running longer than LONG_QUERIES, are found by the checks.
	pipe := tasks.Job{Kind: tasks.KindPipe, Name: "partner_drops", Owner: "walt", State: tasks.StateStarted,
		Statement: "COPY INTO t FROM 's3://x/'", CreatedAt: time.Now().Add(-30 * time.Hour)}
	fresh := tasks.Job{Kind: tasks.KindPipe, Name: "fresh", Owner: "walt", State: tasks.StateStarted,
		Statement: "COPY INTO t FROM 's3://y/'", CreatedAt: time.Now().Add(-30 * time.Hour)}
	for _, j := range []tasks.Job{pipe, fresh} {
		if err := jobs.CreateJob(ctx, j, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := jobs.WriteRun(ctx, tasks.Run{ID: "p1", Kind: tasks.KindPipe, Name: "fresh", Outcome: tasks.OutcomeSucceeded,
		Start: time.Now().Add(-time.Hour), Rows: 3}); err != nil {
		t.Fatal(err)
	}
	running = []Statement{{PID: 42, User: "ana", Text: "SELECT * FROM big", Started: time.Now().Add(-2 * time.Hour)},
		{PID: 43, User: "bo", Text: "SELECT 1", Started: time.Now()}}
	s.check(ctx)
	s.pending.Wait()
	s.check(ctx)
	s.pending.Wait()
	got = payloads(t, rc)[5:]
	names = nil
	for _, p := range got {
		names = append(names, p["name"].(string))
	}
	if strings.Join(names, ",") != "idle_pipes,long_queries" {
		t.Fatalf("checks sent %v", names)
	}
	if d := got[0]["details"].(map[string]any); d["pipe"] != "partner_drops" {
		t.Errorf("idle pipe: %v", d)
	}
	if d := got[1]["details"].(map[string]any); d["pid"] != "42" || d["statement"] != "SELECT * FROM big" {
		t.Errorf("long query: %v", d)
	}

	// Each notification is in the alert history, for admins.
	recs, err := store.Records(ctx, audit.Viewer{All: true}, audit.Page{})
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 7 || recs[0].Type != RecordBuiltin || recs[0].State != StateTriggered || recs[0].Integration != "ops" {
		t.Errorf("history: %+v", recs)
	}
	if recs, _ := store.Records(ctx, audit.Viewer{User: "walt"}, audit.Page{}); len(recs) != 0 {
		t.Errorf("walt sees %d built-in alerts", len(recs))
	}
}

// An erasure request that is blocked raises failed_runs, once an hour
// however often it tries again.
func TestErasureBlocked(t *testing.T) {
	ctx := context.Background()
	rc := newReceiver(t, false)
	s := newTestService(t, Options{Jobs: tasks.NewMemoryStore()})
	if err := s.Store().CreateIntegration(ctx, Integration{Name: "ops", Type: TypeWebhook, Enabled: true, URL: rc.URL,
		Builtin: Builtin{FailedRuns: true}}, false); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		s.ErasureBlocked("01a14589", "delete", "crm.events is an Iceberg table of format version 3")
	}
	drain(ctx, s)
	got := payloads(t, rc)
	if len(got) != 1 || got[0]["name"] != BuiltinFailedRuns {
		t.Fatalf("sent %v", got)
	}
	if d := got[0]["details"].(map[string]any); d["kind"] != "erasure" || d["name"] != "01a14589" || d["step"] != "delete" {
		t.Errorf("details %v", d)
	}
}

// TestLoginNoticesCannotFlood checks that failed-login notices, which
// clients drive, are few per window, show names quoted, and leave the
// integration's queue and rate limit to the other built-in alerts.
func TestLoginNoticesCannotFlood(t *testing.T) {
	ctx := context.Background()
	rc := newReceiver(t, false)
	s := newTestService(t, Options{})
	in := Integration{Name: "ops", Type: TypeWebhook, Enabled: true, URL: rc.URL,
		Builtin: Builtin{FailedRuns: true, FailedLogins: true}}
	if err := s.Store().CreateIntegration(ctx, in, false); err != nil {
		t.Fatal(err)
	}
	for i := range 40 {
		s.LoginsBlocked(fmt.Sprintf("198.51.100.%d", i), false, 50, 15*time.Minute)
	}
	if n := len(s.logins); n != maxLoginNotices {
		t.Errorf("%d failed-login notices queued, want %d", n, maxLoginNotices)
	}
	if n := len(s.events); n != 0 {
		t.Errorf("failed-login notices took %d of the other alerts' queue", n)
	}
	s.LoginsBlocked("please-call-555-0100", true, 10, 15*time.Minute)
	if n := len(s.logins); n != maxLoginNotices {
		t.Errorf("a notice past the cap was queued: %d", n)
	}
	drain(ctx, s)
	// The other alerts still go out at once, from the reserve.
	s.RunEnded(tasks.Job{Kind: tasks.KindTask, Name: "nightly", Owner: "walt"},
		tasks.Run{Outcome: tasks.OutcomeFailed, Trigger: tasks.TriggerSchedule, Start: time.Now(), Error: "boom"})
	drain(ctx, s)
	var logins []map[string]any
	failedRuns := 0
	for _, p := range payloads(t, rc) {
		switch p["name"] {
		case BuiltinFailedLogins:
			logins = append(logins, p)
		case BuiltinFailedRuns:
			failedRuns++
		}
	}
	if len(logins) != maxLoginNotices || failedRuns != 1 {
		t.Errorf("sent %d failed-login notices and %d failed runs", len(logins), failedRuns)
	}
	if last := logins[len(logins)-1]["message"].(string); !strings.Contains(last, "sends no more") {
		t.Errorf("the last notice does not say more are held back: %q", last)
	}

	// Names are shown as the quoted data they are.
	s2 := newTestService(t, Options{Store: s.Store()})
	s2.LoginsBlocked("please-call-555-0100", true, 10, 15*time.Minute)
	e := <-s2.logins
	if text := slackText(e.msg); !strings.Contains(text, "`\"please-call-555-0100\"`") {
		t.Errorf("Slack shows the name as %q", text)
	}

	// A failed-login notice takes from the rate limit only what the other
	// alerts leave, and never waits for it.
	s3 := newTestService(t, Options{})
	for i := range sendBurst {
		err := s3.take(ctx, "ops", true)
		if i < sendBurst-reserveTokens && err != nil || i >= sendBurst-reserveTokens && !errors.Is(err, ErrRateLimited) {
			t.Errorf("low-priority take %d: %v", i, err)
		}
	}
	for i := range reserveTokens {
		if err := s3.take(ctx, "ops", false); err != nil {
			t.Errorf("take %d of the reserve: %v", i, err)
		}
	}
}

// TestFailedNotificationsAreTriedAgain checks that a built-in alert whose
// notification failed holds its problem back only briefly, not for its
// whole repeat window.
func TestFailedNotificationsAreTriedAgain(t *testing.T) {
	ctx := context.Background()
	rc := newReceiver(t, false)
	rc.handle = func(w http.ResponseWriter, _ *http.Request) bool { w.WriteHeader(http.StatusBadRequest); return true }
	store := NewMemoryStore()
	started := time.Now().Add(-3 * time.Hour)
	s := newTestService(t, Options{Store: store, Statements: func() []Statement {
		return []Statement{{PID: 7, User: "ana", Text: "SELECT 1", Started: started}}
	}})
	in := Integration{Name: "ops", Type: TypeWebhook, Enabled: true, URL: rc.URL, Builtin: Builtin{LongQueries: time.Hour}}
	if err := store.CreateIntegration(ctx, in, false); err != nil {
		t.Fatal(err)
	}
	s.check(ctx)
	s.pending.Wait()
	recs, _ := store.Records(ctx, audit.Viewer{All: true}, audit.Page{})
	if len(recs) != 1 || recs[0].State != StateActionFailed {
		t.Fatalf("history: %+v", recs)
	}
	store.mu.Lock()
	var until time.Time
	for _, u := range store.claims {
		until = u
	}
	store.mu.Unlock()
	if left := time.Until(until); left > retryFailed+time.Second || left <= 0 {
		t.Errorf("a failed notification holds its problem back for %v, want about %v", left, retryFailed)
	}
	// Once the hold ends, the next check tries again, and succeeds.
	store.mu.Lock()
	for k := range store.claims {
		store.claims[k] = time.Now()
	}
	store.mu.Unlock()
	rc.handle = nil
	s.check(ctx)
	s.pending.Wait()
	if recs, _ := store.Records(ctx, audit.Viewer{All: true}, audit.Page{}); len(recs) != 2 || recs[0].State != StateTriggered {
		t.Errorf("after the hold: %+v", recs)
	}
	if left := func() time.Duration {
		store.mu.Lock()
		defer store.mu.Unlock()
		for _, u := range store.claims {
			return time.Until(u)
		}
		return 0
	}(); left < repeatLongQuery-time.Minute {
		t.Errorf("a sent notification holds its problem back for %v, want %v", left, repeatLongQuery)
	}
}

// TestCaps checks that whatever a message carries, every destination gets
// at most the caps: wide rows, many columns, long values, a MESSAGE that
// names a column many times, a long error.
func TestCaps(t *testing.T) {
	long := strings.Repeat("x", 100_000)
	cols := make([]string, 2000)
	row := make([]*string, 2000)
	for i := range cols {
		cols[i], row[i] = fmt.Sprintf("c%d", i), &long
	}
	expanded := Template(strings.Repeat("{{c0}}", 650), cols, row)
	if n := len(plain(expanded)); n > MaxMessageBytes {
		t.Errorf("the MESSAGE expanded to %d bytes", n)
	}
	m := Message{Type: RecordBuiltin, Name: BuiltinFailedRuns, Title: strings.Repeat("t", 5000),
		Body:    append(expanded, Text(" and then "), Value(long)),
		Details: map[string]string{"error": long, "name": "x"},
		Result:  &Result{Columns: cols, Rows: [][]*string{row, row, row}}}
	if n := len(plain(capped(m).Body)); n > MaxMessageBytes {
		t.Errorf("the message keeps %d bytes", n)
	}
	c := capped(m)
	if len(c.Title) > MaxTitleBytes || len(c.Result.Columns) != MaxResultColumns || c.Result.HiddenColumns != 2000-MaxResultColumns ||
		!c.Result.More {
		t.Errorf("capped: title %d, %d columns, %d hidden, more %v", len(c.Title), len(c.Result.Columns), c.Result.HiddenColumns, c.Result.More)
	}
	total := 0
	for _, r := range c.Result.Rows {
		for _, v := range r {
			if v != nil {
				total += len(*v)
			}
		}
	}
	if total > MaxResultBytes+MaxResultColumns*len("…") {
		t.Errorf("the result keeps %d bytes", total)
	}
	// Slack takes 40,000 characters of text; Teams cards of about 28 KB.
	in := Integration{Name: "ops"}
	for typ, limit := range map[string]int{TypeWebhook: 64 << 10, TypeSlack: 40_000, TypeTeams: 28_000} {
		var b []byte
		var err error
		switch typ {
		case TypeWebhook:
			b, err = webhookPayload(in, c)
		case TypeSlack:
			b, err = slackPayload(c)
		case TypeTeams:
			b, err = teamsPayload(c)
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(b) > limit {
			t.Errorf("%s payload: %d bytes, over %d", typ, len(b), limit)
		}
	}
	_, body := emailContent(c)
	if len(body) > 48<<10 {
		t.Errorf("email: %d bytes", len(body))
	}
	var p struct {
		Message string
		Details map[string]string
	}
	b, _ := webhookPayload(in, capped(Message{Body: []Part{Text("a\nb "), Value("line one\nline two")},
		Details: map[string]string{"error": "x\x00y"}}))
	if err := json.Unmarshal(b, &p); err != nil {
		t.Fatal(err)
	}
	if p.Message != `a`+"\n"+`b line one\nline two` || strings.ContainsRune(p.Details["error"], 0) {
		t.Errorf("webhook values are not one line: %q %q", p.Message, p.Details["error"])
	}
}

func TestTemplate(t *testing.T) {
	a, b := "acme", "<!here>"
	parts := Template("{{client}} spent {{ spend }} of {{plan}}{{nope}}", []string{"client", "SPEND", "plan"}, []*string{&a, &b, nil})
	want := []Part{Value("acme"), Text(" spent "), Value("<!here>"), Text(" of "), Value("NULL"), Text("{{nope}}")}
	if len(parts) != len(want) {
		t.Fatalf("%+v", parts)
	}
	for i := range want {
		if parts[i] != want[i] {
			t.Errorf("part %d: %+v, want %+v", i, parts[i], want[i])
		}
	}
	if got := Placeholders("a {{x}} b {{ y }} {{"); strings.Join(got, ",") != "x,y" {
		t.Errorf("placeholders %v", got)
	}
}

func TestBuiltinSettings(t *testing.T) {
	var b Builtin
	for _, set := range []struct {
		name  string
		n     int
		given bool
	}{{"FAILED_RUNS", 0, false}, {"idle_pipes", 6, true}, {"long_queries", 0, false}} {
		if err := b.Set(set.name, set.n, set.given); err != nil {
			t.Fatal(err)
		}
	}
	if s := b.String(); s != "failed_runs, idle_pipes = 6, long_queries = 60" {
		t.Errorf("String: %q", s)
	}
	back, err := ParseBuiltin(b.String())
	if err != nil || back != b {
		t.Errorf("ParseBuiltin: %+v, %v", back, err)
	}
	for _, bad := range []struct {
		name  string
		n     int
		given bool
	}{{"bogus", 0, false}, {"failed_runs", 3, true}, {"idle_pipes", 0, true}, {"long_queries", 99999, true}} {
		if err := b.Set(bad.name, bad.n, bad.given); !errors.Is(err, ErrInvalid) {
			t.Errorf("%+v: %v", bad, err)
		}
	}
}

func TestTable(t *testing.T) {
	long := strings.Repeat("é", MaxValueBytes)
	multi := "line one\nline two\x00"
	r := &Result{Columns: []string{"name", "v"}, Rows: [][]*string{{&long, &multi}}, More: true}
	lines := table(r, func(v string) string { return v })
	if len(lines) != 4 || !strings.HasPrefix(lines[0], "name") || !strings.Contains(lines[2], `line one\nline two`+"�") ||
		lines[3] != "(1 row shown; there is more)" {
		t.Errorf("%q", lines)
	}
	if !strings.Contains(lines[2], "…") || len(lines[2]) > MaxValueBytes+100 {
		t.Errorf("a long value is not cut: %d bytes", len(lines[2]))
	}
}
