package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/notify"
)

// AlertHistory serves igloo_alert_history(): alerts' runs, built-in
// alerts' notifications and messages sent with SYSTEM$SEND_NOTIFICATION.
type AlertHistory interface {
	Records(ctx context.Context, v audit.Viewer, p audit.Page) ([]notify.Record, error)
}

type alertHistoryConfig struct {
	source AlertHistory
	viewer HistoryViewer
}

// SetAlertHistory makes igloo.alert_history list what source returns, to
// the viewer that viewer works out for each statement: admins see
// everything, others the runs of their own alerts and their own messages.
// Until it is set, it is empty.
func (e *Engine) SetAlertHistory(source AlertHistory, viewer HistoryViewer) {
	e.alertHistory.Store(&alertHistoryConfig{source: source, viewer: viewer})
}

// registerAlertHistory defines igloo_alert_history() and the view
// igloo.alert_history over it, as registerHistory does for the query
// history.
func (e *Engine) registerAlertHistory(ctx context.Context) error {
	conn, release, err := e.sqlConn(ctx)
	if err != nil {
		return err
	}
	defer release()

	t, err := newHistoryTypes()
	if err != nil {
		return err
	}
	none := func() (audit.Viewer, bool) { return audit.Viewer{}, false }
	err = duckdb.RegisterTableUDF(conn, "igloo_alert_history", duckdb.RowTableFunction{
		BindArgumentsContext: func(ctx context.Context, _ map[string]any, _ ...any) (duckdb.RowTableSource, error) {
			c := e.alertHistory.Load()
			viewer := none
			var src AlertHistory
			if c != nil {
				src = c.source
				if c.viewer != nil {
					if v := c.viewer(ctx); v != nil {
						viewer = v
					}
				}
			}
			var present History
			if src != nil {
				present = noHistory{}
			}
			return &historySource[notify.Record]{
				columns: []duckdb.ColumnInfo{
					{Name: "alert_id", T: t.varchar}, {Name: "alert_type", T: t.varchar}, {Name: "alert_name", T: t.varchar},
					{Name: "user_name", T: t.varchar}, {Name: "trigger", T: t.varchar},
					{Name: "scheduled_time", T: t.timestamp}, {Name: "start_time", T: t.timestamp},
					{Name: "end_time", T: t.timestamp}, {Name: "duration_ms", T: t.double},
					{Name: "state", T: t.varchar}, {Name: "row_count", T: t.bigint}, {Name: "integration", T: t.varchar},
					{Name: "attempts", T: t.bigint}, {Name: "error", T: t.varchar},
				},
				viewer: viewer,
				fetch: func(_ History, v audit.Viewer, p audit.Page) ([]notify.Record, error) {
					return src.Records(context.Background(), v, p)
				},
				history: present,
				key:     func(r notify.Record) (time.Time, string) { return r.Start, r.ID },
				values:  alertValues,
			}, nil
		},
	})
	if err != nil {
		return err
	}
	schema := ident(e.local) + "." + ident(HistorySchema)
	for _, stmt := range []string{
		"CREATE SCHEMA IF NOT EXISTS " + schema,
		"CREATE OR REPLACE VIEW " + schema + ".alert_history AS SELECT * FROM system.main.igloo_alert_history()",
	} {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return nil
}

func alertValues(r notify.Record) []any {
	var scheduled, end, duration, rows any
	if !r.Scheduled.IsZero() {
		scheduled = r.Scheduled
	}
	if !r.End.IsZero() {
		end, duration = r.End, float64(r.End.Sub(r.Start).Microseconds())/1000
	}
	if r.Rows >= 0 {
		rows = r.Rows
	}
	return []any{r.ID, r.Type, nullString(r.Name), nullString(r.User), r.Trigger, scheduled, r.Start, end, duration, r.State,
		rows, nullString(r.Integration), int64(r.Attempts), nullString(r.Error)}
}
