package pgwire

import (
	"context"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
)

// TestPreparePhasesLogged checks that, with the log at debug, preparing a
// statement and readying it to run each log how long their steps took.
func TestPreparePhasesLogged(t *testing.T) {
	t.Parallel()
	eng, err := engine.Open(context.Background(), config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	logs := &lockedBuffer{}
	ts := startServerLogged(t, eng, slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	ctx := context.Background()
	conn := ts.admin(t, pgx.QueryExecModeExec)
	var n int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM range(10)").Scan(&n); err != nil || n != 10 {
		t.Fatalf("count: %d, %v", n, err)
	}
	out := logs.String()
	prepared := regexp.MustCompile(`msg="statement prepared" took=\S+ phases="[^"]*duckdb_prepare=\d`)
	if !prepared.MatchString(out) {
		t.Errorf("no prepare phases logged:\n%s", out)
	}
	if !strings.Contains(out, `msg="statement readied to run"`) || !regexp.MustCompile(`phases="[^"]*recheck=\d`).MatchString(out) {
		t.Errorf("no phases of readying the statement logged:\n%s", out)
	}
}

// TestPreparePhasesAddUp checks that steps of the same name add up, and
// that nothing is timed with no statement timed.
func TestPreparePhasesAddUp(t *testing.T) {
	t.Parallel()
	sess := &session{}
	sess.phase("a")() // not timed
	p := &prepPhases{}
	sess.phases = p
	sess.phase("a")()
	outer := sess.phase("b")
	sess.phase("a")()
	outer()
	if len(p.names) != 2 || p.names[0] != "a" || p.names[1] != "b" {
		t.Fatalf("phases: %v", p.names)
	}
	if got := p.String(); !regexp.MustCompile(`^a=[^ /]+/2 b=[^ /]+$`).MatchString(got) {
		t.Fatalf("String: %q", got)
	}
}

// TestSlowPhasesNameTheStatement checks that the log line of steps that took
// slowPhases or more names the session and the statement, redacted and cut
// short, and that of quicker ones does not.
func TestSlowPhasesNameTheStatement(t *testing.T) {
	t.Parallel()
	p := &prepPhases{}
	if attrs := phaseAttrs(slowPhases/2, p, 7, "SELECT 1"); len(attrs) != 4 {
		t.Errorf("quick steps: %v", attrs)
	}
	attrs := phaseAttrs(slowPhases, p, 7, "SELECT  count(*)\n FROM t WHERE x = '"+strings.Repeat("a", 300)+"'")
	if len(attrs) != 8 || attrs[5] != int32(7) {
		t.Fatalf("slow steps: %v", attrs)
	}
	if q := attrs[7].(string); !strings.HasPrefix(q, "SELECT count(*) FROM t WHERE x = 'aaa") || len(q) != 203 {
		t.Errorf("statement %q", q)
	}
	if q := phaseAttrs(slowPhases, p, 7, "CREATE SECRET s (TYPE s3, KEY_ID 'k', SECRET 'hidden')")[7].(string); strings.Contains(q, "hidden") {
		t.Errorf("a secret logged: %q", q)
	}
}
