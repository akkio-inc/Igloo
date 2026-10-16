package notify

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// drain delivers the built-in alerts waiting, as Run does.
func drain(ctx context.Context, s *Service) {
	for {
		select {
		case e := <-s.events:
			s.deliver(ctx, e)
		default:
			return
		}
	}
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
	drain(ctx, s)

	got := payloads(t, rc)
	var names []string
	for _, p := range got {
		names = append(names, p["name"].(string))
		if p["type"] != RecordBuiltin {
			t.Errorf("type %v", p["type"])
		}
	}
	if want := "failed_runs,load_errors,failed_logins,maintenance_failures"; strings.Join(names, ",") != want {
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
	s.check(ctx)
	got = payloads(t, rc)[4:]
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
	if len(recs) != 6 || recs[0].Type != RecordBuiltin || recs[0].State != StateTriggered || recs[0].Integration != "ops" {
		t.Errorf("history: %+v", recs)
	}
	if recs, _ := store.Records(ctx, audit.Viewer{User: "walt"}, audit.Page{}); len(recs) != 0 {
		t.Errorf("walt sees %d built-in alerts", len(recs))
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
		lines[3] != "(1 row shown; there are more)" {
		t.Errorf("%q", lines)
	}
	if !strings.Contains(lines[2], "…") || len(lines[2]) > MaxValueBytes+100 {
		t.Errorf("a long value is not cut: %d bytes", len(lines[2]))
	}
}
