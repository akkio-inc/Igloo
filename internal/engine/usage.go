package engine

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/audit"
)

// registerUsage defines igloo_usage(), behind the view igloo.usage: the
// query history summed by day, compute pool, user, role and query tag, with
// each group's estimated compute cost (see package audit). Like the history
// functions, it shows admins every group and everyone else their own.
func (e *Engine) registerUsage(conn *sql.Conn, types historyTypes) error {
	return duckdb.RegisterTableUDF(conn, "igloo_usage", duckdb.RowTableFunction{
		BindArgumentsContext: func(ctx context.Context, _ map[string]any, _ ...any) (duckdb.RowTableSource, error) {
			return &usageSource{columns: types.usageColumns(), viewer: e.historyViewer(ctx), history: e.historySource()}, nil
		},
	})
}

// usageSource reads every group at once: there are far fewer groups than
// statements, and the store sums them.
type usageSource struct {
	columns []duckdb.ColumnInfo
	viewer  func() (audit.Viewer, bool)
	history History

	rows   []audit.Usage
	loaded bool
	pos    int
}

func (s *usageSource) ColumnInfos() []duckdb.ColumnInfo     { return s.columns }
func (s *usageSource) Cardinality() *duckdb.CardinalityInfo { return nil }
func (s *usageSource) Init()                                {}

func (s *usageSource) FillRow(row duckdb.Row) (bool, error) {
	if !s.loaded {
		s.loaded = true
		v, ok := s.viewer()
		if !ok || s.history == nil {
			return false, nil
		}
		rows, err := s.history.Usage(context.Background(), v, time.Time{}, time.Time{})
		if err != nil {
			return false, fmt.Errorf("read usage: %w", err)
		}
		s.rows = rows
	}
	if s.pos >= len(s.rows) {
		return false, nil
	}
	u := s.rows[s.pos]
	s.pos++
	for i, v := range usageValues(u) {
		if err := row.SetRowValue(i, v); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (t historyTypes) usageColumns() []duckdb.ColumnInfo {
	return []duckdb.ColumnInfo{
		{Name: "day", T: t.date},
		{Name: "pool", T: t.varchar},
		{Name: "user_name", T: t.varchar},
		{Name: "role", T: t.varchar},
		{Name: "query_tag", T: t.varchar},
		{Name: "statements", T: t.bigint},
		{Name: "failed", T: t.bigint},
		{Name: "run_ms", T: t.double},
		{Name: "queued_ms", T: t.double},
		{Name: "cpu_time_ms", T: t.double},
		{Name: "bytes_read", T: t.bigint},
		{Name: "time_share", T: t.double},
		{Name: "compute_hours", T: t.double},
		{Name: "estimated_cost", T: t.double},
	}
}

func usageValues(u audit.Usage) []any {
	var hours, cost any
	if u.ComputeHours >= 0 {
		hours = u.ComputeHours
	}
	if u.Cost != nil {
		cost = *u.Cost
	}
	return []any{u.Day, u.Pool, u.User, u.Role, nullString(u.QueryTag), u.Statements, u.Failed,
		millis(u.Run), millis(u.Queued), millis(u.CPUTime), u.BytesRead, u.Share, hours, cost}
}
