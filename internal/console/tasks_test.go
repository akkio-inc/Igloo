package console

import (
	"strings"
	"testing"
)

// rows runs sql as b and returns the rows of its last statement, failing
// on an error.
func (b *browser) rows(sql string) [][]*string {
	b.t.Helper()
	_, events := b.query(sql)
	var rows [][]*string
	for _, ev := range events {
		if ev.Error != nil {
			b.t.Fatalf("%s: %+v", sql, ev.Error)
		}
		if ev.Statement != nil {
			rows = nil
		}
		rows = append(rows, ev.Rows...)
	}
	return rows
}

// TestTaskPages runs the statements the tasks and load history pages run:
// each user sees their own tasks and runs, admins everyone's.
func TestTaskPages(t *testing.T) {
	tc := newTestConsole(t, false)
	walt := tc.signedIn(t, "walt")
	rita := tc.signedIn(t, "rita")
	admin := tc.signedIn(t, "igloo")

	walt.rows("CREATE TASK tick SCHEDULE '1 hour' AS SELECT 42")
	if rows := walt.rows("SHOW TASKS"); len(rows) != 1 || *rows[0][0] != "tick" || *rows[0][2] != "suspended" {
		t.Fatalf("walt's tasks: %v", rows)
	}
	if rows := rita.rows("SHOW TASKS"); len(rows) != 0 {
		t.Errorf("rita sees %d tasks", len(rows))
	}
	if rows := admin.rows("SHOW TASKS"); len(rows) != 1 {
		t.Errorf("the admin sees %d tasks", len(rows))
	}

	// The page's buttons.
	walt.rows(`EXECUTE TASK "tick"`)
	walt.rows(`ALTER TASK "tick" RESUME`)
	if rows := walt.rows("SHOW TASKS"); *rows[0][2] != "started" || rows[0][7] == nil || *rows[0][7] != "succeeded" {
		t.Errorf("after running and resuming: %v", rows)
	}
	_, events := rita.query(`ALTER TASK "tick" SUSPEND`)
	if len(events) < 2 || events[1].Error == nil || !strings.Contains(events[1].Error.Message, "belongs to walt") {
		t.Errorf("rita suspends walt's task: %+v", events)
	}

	runs := "SELECT outcome FROM igloo.task_history WHERE kind = 'task' AND name = 'tick' ORDER BY start_time DESC LIMIT 50"
	if rows := walt.rows(runs); len(rows) != 1 || *rows[0][0] != "succeeded" {
		t.Errorf("walt's runs: %v", rows)
	}
	if rows := rita.rows(runs); len(rows) != 0 {
		t.Errorf("rita sees walt's runs: %v", rows)
	}
	if rows := admin.rows("SELECT * FROM igloo.load_history ORDER BY start_time DESC LIMIT 500"); len(rows) != 0 {
		t.Errorf("loads: %v", rows)
	}
}
