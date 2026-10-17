package audit

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Redactor finds in history the identifiers an erasure request erases,
// and removes them (see package erasure). Stores read every record to it:
// nothing about the identifiers reaches the metadata database, not even
// hashes.
type Redactor interface {
	// Redact removes the identifiers from text, reporting whether there
	// were any.
	Redact(text string) (string, bool)
}

// redactBatch is how many records are read at a time.
const redactBatch = 2000

// RedactColumns redacts the text columns of every row of table, read in
// order of the text key column key, a batch at a time, returning how many
// rows it changed. Stores that keep history elsewhere in the metadata
// database use it too.
func RedactColumns(ctx context.Context, pool *pgxpool.Pool, r Redactor, table, key string, columns ...string) (int64, error) {
	var changed int64
	after := ""
	for {
		rows, err := pool.Query(ctx, `SELECT `+key+`, `+strings.Join(columns, ", ")+` FROM `+table+`
			WHERE `+key+` > $1 ORDER BY `+key+` LIMIT `+fmt.Sprint(redactBatch), after)
		if err != nil {
			return changed, err
		}
		type record struct {
			key    string
			values []*string
		}
		batch, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (record, error) {
			rec := record{values: make([]*string, len(columns))}
			dest := []any{&rec.key}
			for i := range rec.values {
				dest = append(dest, &rec.values[i])
			}
			return rec, row.Scan(dest...)
		})
		if err != nil {
			return changed, err
		}
		for _, rec := range batch {
			var sets []string
			args := []any{rec.key}
			for i, v := range rec.values {
				if v == nil {
					continue
				}
				if text, ok := r.Redact(*v); ok {
					args = append(args, text)
					sets = append(sets, fmt.Sprintf("%s = $%d", columns[i], len(args)))
				}
			}
			if len(sets) == 0 {
				continue
			}
			if _, err := pool.Exec(ctx, `UPDATE `+table+` SET `+strings.Join(sets, ", ")+` WHERE `+key+` = $1`, args...); err != nil {
				return changed, err
			}
			changed++
		}
		if len(batch) < redactBatch {
			return changed, nil
		}
		after = batch[len(batch)-1].key
	}
}

// Redact removes r's identifiers from the history: from statements, the
// query tags they ran under and the applications that sent them, and from
// the applications that logged in.
func (s *PostgresStore) Redact(ctx context.Context, r Redactor) (int64, error) {
	n, err := RedactColumns(ctx, s.pool, r, "igloo_audit.query_history", "query_id", "query_text", "query_tag", "application_name")
	if err != nil {
		return n, err
	}
	m, err := RedactColumns(ctx, s.pool, r, "igloo_audit.login_history", "event_id", "application_name")
	return n + m, err
}

// Writes returns successful statements that wrote catalog tables and that
// either read one of tables (catalog.schema.table), as Igloo recorded it,
// or, where it recorded nothing, name one of names in their text: what
// lineage from history can tell about where data went (see package
// erasure).
func (s *PostgresStore) Writes(ctx context.Context, tables, names []string) ([]Query, error) {
	rows, err := s.pool.Query(ctx, `SELECT query_id, statement_type, query_text, tables_read, tables_written
		FROM igloo_audit.query_history
		WHERE outcome = 'ok' AND (tables_read && $1::text[] AND tables_written IS NOT NULL
			OR tables_written IS NULL AND statement_type IN ('INSERT', 'MERGE', 'CREATE TABLE')
				AND $2::text IS NOT NULL AND query_text ~* $2)
		ORDER BY start_time DESC LIMIT 10000`, tables, namesPattern(names))
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Query, error) {
		var q Query
		return q, row.Scan(&q.ID, &q.StatementType, &q.Text, &q.TablesRead, &q.TablesWritten)
	})
}

// namesPattern matches any of names as a word, in PostgreSQL's regular
// expressions, or is NULL when there are none.
func namesPattern(names []string) any {
	if len(names) == 0 {
		return nil
	}
	return `\m(` + strings.Join(quoteAll(names), "|") + `)\M`
}

// Redact removes r's identifiers from the history, as
// PostgresStore.Redact does.
func (m *MemoryStore) Redact(_ context.Context, r Redactor) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var changed int64
	redact := func(fields ...*string) {
		hit := false
		for _, f := range fields {
			if text, ok := r.Redact(*f); ok {
				*f, hit = text, true
			}
		}
		if hit {
			changed++
		}
	}
	for i := range m.queries {
		q := &m.queries[i]
		redact(&q.Text, &q.QueryTag, &q.ApplicationName)
	}
	for i := range m.logins {
		redact(&m.logins[i].ApplicationName)
	}
	return changed, nil
}

// Writes returns what PostgresStore.Writes does.
func (m *MemoryStore) Writes(_ context.Context, tables, names []string) ([]Query, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mentions := regexp.MustCompile(`(?i)\b(` + strings.Join(quoteAll(names), "|") + `)\b`)
	var out []Query
	for i := len(m.queries) - 1; i >= 0; i-- {
		q := m.queries[i]
		if q.Outcome != OutcomeOK {
			continue
		}
		read := q.TablesWritten != nil && overlaps(q.TablesRead, tables)
		named := q.TablesWritten == nil && len(names) > 0 && mentions.MatchString(q.Text) &&
			(q.StatementType == "INSERT" || q.StatementType == "MERGE" || q.StatementType == "CREATE TABLE")
		if read || named {
			out = append(out, q)
		}
	}
	return out, nil
}

func quoteAll(names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = regexp.QuoteMeta(n)
	}
	return out
}

func overlaps(a, b []string) bool {
	for _, x := range a {
		for _, y := range b {
			if strings.EqualFold(x, y) {
				return true
			}
		}
	}
	return false
}
