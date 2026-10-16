package tasks

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/audit"
)

// Redact removes r's identifiers from the errors in the load history and
// the runs of jobs, which can quote rows a load could not read (see
// package erasure). It returns how many records it changed.
func (s *PostgresStore) Redact(ctx context.Context, r audit.Redactor) (int64, error) {
	delims, salt, hashes := r.Prefilter()
	var changed int64
	for _, t := range []struct{ table, key, column string }{
		{"loads", "load_id", "first_error"},
		{"runs", "run_id", "error"},
	} {
		rows, err := s.pool.Query(ctx, `SELECT `+t.key+`, `+t.column+` FROM igloo_tasks.`+t.table+`
			WHERE `+audit.CandidateSQL(t.column), delims, salt, hashes)
		if err != nil {
			return changed, err
		}
		type record struct{ id, text string }
		found, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (record, error) {
			var r record
			return r, row.Scan(&r.id, &r.text)
		})
		if err != nil {
			return changed, err
		}
		for _, rec := range found {
			text, ok := r.RedactText(rec.text)
			if !ok {
				continue
			}
			if _, err := s.pool.Exec(ctx, `UPDATE igloo_tasks.`+t.table+` SET `+t.column+` = $2 WHERE `+t.key+` = $1`,
				rec.id, text); err != nil {
				return changed, err
			}
			changed++
		}
	}
	return changed, nil
}

// Redact removes r's identifiers from the errors of loads and runs.
func (m *MemoryStore) Redact(_ context.Context, r audit.Redactor) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var changed int64
	for i, l := range m.loads {
		if text, ok := r.RedactText(l.FirstError); ok {
			m.loads[i].FirstError = text
			changed++
		}
	}
	for i, run := range m.runs {
		if text, ok := r.RedactText(run.Error); ok {
			m.runs[i].Error = text
			changed++
		}
	}
	return changed, nil
}
