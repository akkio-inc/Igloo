package audit

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Redactor finds in history the identifiers an erasure request erases,
// and removes them (see package erasure). It knows them only as salted
// hashes.
type Redactor interface {
	// Prefilter describes the records a store can find by itself: those
	// with a word, split off by the regular expression delimiters (valid
	// in Go and PostgreSQL), or a '...' string literal, whose SHA-256
	// after salt is in hashes, once lower-cased and trimmed of spaces.
	Prefilter() (delimiters, salt string, hashes [][]byte)
	// RedactSQL removes the identifiers from a statement, reporting
	// whether there were any.
	RedactSQL(text string) (string, bool)
	// RedactText removes them from other text, such as error messages.
	RedactText(text string) (string, bool)
}

// CandidateSQL is the condition on a text column, named by column, that
// finds candidates for a Redactor's Prefilter given as $1, $2 and $3.
// Stores that keep history elsewhere in the metadata database use it too.
func CandidateSQL(column string) string {
	return fmt.Sprintf(`(EXISTS (SELECT 1 FROM regexp_split_to_table(lower(%[1]s), $1) AS w(word)
			WHERE word <> '' AND sha256(convert_to($2 || word, 'UTF8')) = ANY ($3))
		OR EXISTS (SELECT 1 FROM regexp_matches(%[1]s, '''((?:[^'']|'''')*)''', 'g') AS m(lit)
			WHERE sha256(convert_to($2 || lower(btrim(replace(lit[1], '''''', ''''), ' ')), 'UTF8')) = ANY ($3)))`, column)
}

// redactBatch is how many candidates are redacted at a time.
const redactBatch = 500

// Redact removes r's identifiers from the statements in the history,
// returning how many it changed.
func (s *PostgresStore) Redact(ctx context.Context, r Redactor) (int64, error) {
	delims, salt, hashes := r.Prefilter()
	var changed int64
	after := ""
	for {
		rows, err := s.pool.Query(ctx, `SELECT query_id, query_text FROM igloo_audit.query_history
			WHERE query_id > $4 AND `+CandidateSQL("query_text")+` ORDER BY query_id LIMIT `+fmt.Sprint(redactBatch),
			delims, salt, hashes, after)
		if err != nil {
			return changed, err
		}
		type record struct{ id, text string }
		batch, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (record, error) {
			var r record
			return r, row.Scan(&r.id, &r.text)
		})
		if err != nil {
			return changed, err
		}
		for _, rec := range batch {
			text, ok := r.RedactSQL(rec.text)
			if !ok {
				continue
			}
			if _, err := s.pool.Exec(ctx, `UPDATE igloo_audit.query_history SET query_text = $2 WHERE query_id = $1`,
				rec.id, text); err != nil {
				return changed, err
			}
			changed++
		}
		if len(batch) < redactBatch {
			return changed, nil
		}
		after = batch[len(batch)-1].id
	}
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

// Redact removes r's identifiers from the statements in the history.
func (m *MemoryStore) Redact(_ context.Context, r Redactor) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var changed int64
	for i, q := range m.queries {
		if text, ok := r.RedactSQL(q.Text); ok {
			m.queries[i].Text = text
			changed++
		}
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
