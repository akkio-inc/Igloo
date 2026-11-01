package tasks

import (
	"context"

	"github.com/akkio-inc/igloo/internal/audit"
)

// Redact removes r's identifiers from the errors in the load history and
// the errors and details of the runs of jobs, which can quote rows a load
// could not read (see package erasure). It returns how many records it changed.
func (s *PostgresStore) Redact(ctx context.Context, r audit.Redactor) (int64, error) {
	n, err := audit.RedactColumns(ctx, s.pool, r, "igloo_tasks.loads", "load_id", "first_error")
	if err != nil {
		return n, err
	}
	m, err := audit.RedactColumns(ctx, s.pool, r, "igloo_tasks.runs", "run_id", "error", "detail")
	return n + m, err
}

// Redact removes r's identifiers from the errors of loads and runs.
func (m *MemoryStore) Redact(_ context.Context, r audit.Redactor) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var changed int64
	for i, l := range m.loads {
		if text, ok := r.Redact(l.FirstError); ok {
			m.loads[i].FirstError = text
			changed++
		}
	}
	for i, run := range m.runs {
		errText, e := r.Redact(run.Error)
		detail, d := r.Redact(run.Detail)
		if e {
			m.runs[i].Error = errText
		}
		if d {
			m.runs[i].Detail = detail
		}
		if e || d {
			changed++
		}
	}
	return changed, nil
}
