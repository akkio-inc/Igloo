package engine

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// JobHistory serves igloo_task_history() and igloo_load_history(): the
// runs of tasks, pipes and materialized views, and the files COPY INTO
// loaded.
type JobHistory interface {
	Runs(ctx context.Context, v audit.Viewer, p audit.Page) ([]tasks.Run, error)
	Loads(ctx context.Context, v audit.Viewer, p audit.Page) ([]tasks.Load, error)
}

type jobHistoryConfig struct {
	source JobHistory
	viewer HistoryViewer
}

// SetJobHistory makes igloo.task_history and igloo.load_history list what
// source returns, to the viewer that viewer works out for each statement:
// admins see everything, others the runs of their own jobs and the files
// they loaded. Until it is set, they are empty.
func (e *Engine) SetJobHistory(source JobHistory, viewer HistoryViewer) {
	e.jobHistory.Store(&jobHistoryConfig{source: source, viewer: viewer})
}

// registerJobHistory defines igloo_task_history() and
// igloo_load_history(), and the views igloo.task_history and
// igloo.load_history over them, as registerHistory does for the query
// history.
func (e *Engine) registerJobHistory(ctx context.Context) error {
	// Not closed: closing a DB closes its connector.
	db := sql.OpenDB(e.connector)
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck

	t, err := newHistoryTypes()
	if err != nil {
		return err
	}
	viewer := func(ctx context.Context) func() (audit.Viewer, bool) {
		c := e.jobHistory.Load()
		if c == nil || c.viewer == nil {
			return func() (audit.Viewer, bool) { return audit.Viewer{}, false }
		}
		if v := c.viewer(ctx); v != nil {
			return v
		}
		return func() (audit.Viewer, bool) { return audit.Viewer{}, false }
	}
	source := func() JobHistory {
		if c := e.jobHistory.Load(); c != nil {
			return c.source
		}
		return nil
	}
	err = duckdb.RegisterTableUDF(conn, "igloo_task_history", duckdb.RowTableFunction{
		BindArgumentsContext: func(ctx context.Context, _ map[string]any, _ ...any) (duckdb.RowTableSource, error) {
			src := source()
			return &historySource[tasks.Run]{
				columns: []duckdb.ColumnInfo{
					{Name: "run_id", T: t.varchar}, {Name: "kind", T: t.varchar}, {Name: "name", T: t.varchar},
					{Name: "user_name", T: t.varchar}, {Name: "trigger", T: t.varchar},
					{Name: "scheduled_time", T: t.timestamp}, {Name: "start_time", T: t.timestamp},
					{Name: "end_time", T: t.timestamp}, {Name: "duration_ms", T: t.double},
					{Name: "outcome", T: t.varchar}, {Name: "error", T: t.varchar}, {Name: "row_count", T: t.bigint},
				},
				viewer: viewer(ctx),
				fetch: func(_ History, v audit.Viewer, p audit.Page) ([]tasks.Run, error) {
					return src.Runs(context.Background(), v, p)
				},
				history: present(src),
				key:     func(r tasks.Run) (time.Time, string) { return r.Start, r.ID },
				values:  runValues,
			}, nil
		},
	})
	if err != nil {
		return err
	}
	err = duckdb.RegisterTableUDF(conn, "igloo_load_history", duckdb.RowTableFunction{
		BindArgumentsContext: func(ctx context.Context, _ map[string]any, _ ...any) (duckdb.RowTableSource, error) {
			src := source()
			return &historySource[tasks.Load]{
				columns: []duckdb.ColumnInfo{
					{Name: "load_id", T: t.varchar}, {Name: "table_name", T: t.varchar}, {Name: "file", T: t.varchar},
					{Name: "size", T: t.bigint}, {Name: "last_modified", T: t.timestamp}, {Name: "status", T: t.varchar},
					{Name: "row_count", T: t.bigint}, {Name: "error_count", T: t.bigint}, {Name: "first_error", T: t.varchar},
					{Name: "start_time", T: t.timestamp}, {Name: "end_time", T: t.timestamp},
					{Name: "user_name", T: t.varchar}, {Name: "pipe_name", T: t.varchar},
				},
				viewer: viewer(ctx),
				fetch: func(_ History, v audit.Viewer, p audit.Page) ([]tasks.Load, error) {
					return src.Loads(context.Background(), v, p)
				},
				history: present(src),
				key:     func(l tasks.Load) (time.Time, string) { return l.Start, l.ID },
				values:  loadValues,
			}, nil
		},
	})
	if err != nil {
		return err
	}

	schema := ident(e.local) + "." + ident(HistorySchema)
	for _, stmt := range []string{
		"CREATE SCHEMA IF NOT EXISTS " + schema,
		"CREATE OR REPLACE VIEW " + schema + ".task_history AS SELECT * FROM system.main.igloo_task_history()",
		"CREATE OR REPLACE VIEW " + schema + ".load_history AS SELECT * FROM system.main.igloo_load_history()",
	} {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return nil
}

// present stands in for a History in historySource, which reads nil as
// no history: nil when source is nil, and otherwise a History that is
// never asked for anything (fetch reads source itself).
func present(source JobHistory) History {
	if source == nil {
		return nil
	}
	return noHistory{}
}

type noHistory struct{}

func (noHistory) Queries(context.Context, audit.Viewer, audit.Page) ([]audit.Query, error) {
	return nil, nil
}

func (noHistory) Logins(context.Context, audit.Viewer, audit.Page) ([]audit.Login, error) {
	return nil, nil
}

func (noHistory) Usage(context.Context, audit.Viewer, time.Time, time.Time) ([]audit.Usage, error) {
	return nil, nil
}

func runValues(r tasks.Run) []any {
	var rows, end, duration any
	if r.Rows >= 0 {
		rows = r.Rows
	}
	if !r.End.IsZero() {
		end, duration = r.End, float64(r.End.Sub(r.Start).Microseconds())/1000
	}
	var scheduled any
	if !r.Scheduled.IsZero() {
		scheduled = r.Scheduled
	}
	return []any{r.ID, r.Kind, r.Name, r.User, r.Trigger, scheduled, r.Start, end, duration, r.Outcome,
		nullString(r.Error), rows}
}

func loadValues(l tasks.Load) []any {
	var modified, end any
	if !l.Modified.IsZero() {
		modified = l.Modified
	}
	if !l.End.IsZero() {
		end = l.End
	}
	return []any{l.ID, l.Table, l.File, l.Size, modified, l.Status, l.Rows, l.Errors, nullString(l.FirstError),
		l.Start, end, l.User, nullString(l.Pipe)}
}
