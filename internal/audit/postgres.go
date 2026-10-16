package audit

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// migrationKey serializes creating the history tables across replicas
	// starting at the same time.
	migrationKey int64 = 0x49676c6f6f417564 // "IglooAud"
	// pruneKey lets one replica at a time delete old history.
	pruneKey int64 = 0x49676c6f6f507275 // "IglooPru"
)

var migrations = []string{
	`CREATE SCHEMA IF NOT EXISTS igloo_audit`,
	`CREATE TABLE IF NOT EXISTS igloo_audit.query_history (
		query_id         text PRIMARY KEY,
		session_id       text NOT NULL,
		user_name        text NOT NULL,
		role             text NOT NULL,
		start_time       timestamptz NOT NULL,
		duration_us      bigint NOT NULL,
		outcome          text NOT NULL,
		sqlstate         text,
		row_count        bigint,
		statement_type   text NOT NULL,
		query_text       text NOT NULL,
		tables_read      text[],
		tables_written   text[],
		client_address   text,
		application_name text
	)`,
	`CREATE INDEX IF NOT EXISTS query_history_time ON igloo_audit.query_history (start_time DESC, query_id DESC)`,
	`CREATE INDEX IF NOT EXISTS query_history_user ON igloo_audit.query_history (user_name, start_time DESC, query_id DESC)`,
	`CREATE TABLE IF NOT EXISTS igloo_audit.login_history (
		event_id         text PRIMARY KEY,
		event_time       timestamptz NOT NULL,
		user_name        text NOT NULL,
		result           text NOT NULL,
		reason           text,
		client_address   text,
		application_name text,
		session_id       text
	)`,
	`CREATE INDEX IF NOT EXISTS login_history_time ON igloo_audit.login_history (event_time DESC, event_id DESC)`,
	`CREATE INDEX IF NOT EXISTS login_history_user ON igloo_audit.login_history (user_name, event_time DESC, event_id DESC)`,
}

// PostgresStore keeps history in the "igloo_audit" schema of the metadata
// database, shared by every replica.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// OpenPostgres connects to url with a small pool of its own, so history
// never holds a connection the user store needs, and creates the history
// tables if needed.
func OpenPostgres(ctx context.Context, url string) (*PostgresStore, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", migrationKey); err != nil {
			return err
		}
		for _, m := range migrations {
			if _, err := tx.Exec(ctx, m); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("create history tables: %w", err)
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) WriteQueries(ctx context.Context, qs []Query) error {
	rows := make([][]any, len(qs))
	for i, q := range qs {
		var rowCount any
		if q.Rows >= 0 {
			rowCount = q.Rows
		}
		rows[i] = []any{q.ID, q.SessionID, q.User, q.Role, q.Start, q.Duration.Microseconds(), q.Outcome,
			nullable(q.SQLState), rowCount, q.StatementType, q.Text, nullSlice(q.TablesRead), nullSlice(q.TablesWritten),
			nullable(q.ClientAddress), nullable(q.ApplicationName)}
	}
	_, err := s.pool.CopyFrom(ctx, pgx.Identifier{"igloo_audit", "query_history"}, []string{
		"query_id", "session_id", "user_name", "role", "start_time", "duration_us", "outcome", "sqlstate",
		"row_count", "statement_type", "query_text", "tables_read", "tables_written", "client_address", "application_name",
	}, pgx.CopyFromRows(rows))
	return err
}

func (s *PostgresStore) WriteLogins(ctx context.Context, ls []Login) error {
	rows := make([][]any, len(ls))
	for i, x := range ls {
		rows[i] = []any{x.ID, x.Time, x.User, x.Result, nullable(x.Reason), nullable(x.ClientAddress),
			nullable(x.ApplicationName), nullable(x.SessionID)}
	}
	_, err := s.pool.CopyFrom(ctx, pgx.Identifier{"igloo_audit", "login_history"}, []string{
		"event_id", "event_time", "user_name", "result", "reason", "client_address", "application_name", "session_id",
	}, pgx.CopyFromRows(rows))
	return err
}

func (s *PostgresStore) Queries(ctx context.Context, v Viewer, p Page) ([]Query, error) {
	rows, err := s.pool.Query(ctx, `SELECT query_id, session_id, user_name, role, start_time, duration_us, outcome,
			coalesce(sqlstate, ''), coalesce(row_count, -1), statement_type, query_text, tables_read, tables_written,
			coalesce(client_address, ''), coalesce(application_name, '')
		FROM igloo_audit.query_history
		WHERE ($1::text IS NULL OR user_name = $1)
		  AND ($2::timestamptz IS NULL OR start_time >= $2)
		  AND ($3::timestamptz IS NULL OR start_time < $3)
		  AND ($4::timestamptz IS NULL OR (start_time, query_id) < ($4, $5))
		ORDER BY start_time DESC, query_id DESC
		LIMIT $6`, p.args(v)...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Query, error) {
		var (
			q  Query
			us int64
		)
		err := row.Scan(&q.ID, &q.SessionID, &q.User, &q.Role, &q.Start, &us, &q.Outcome, &q.SQLState, &q.Rows,
			&q.StatementType, &q.Text, &q.TablesRead, &q.TablesWritten, &q.ClientAddress, &q.ApplicationName)
		q.Duration = time.Duration(us) * time.Microsecond
		return q, err
	})
}

func (s *PostgresStore) Logins(ctx context.Context, v Viewer, p Page) ([]Login, error) {
	rows, err := s.pool.Query(ctx, `SELECT event_id, event_time, user_name, result, coalesce(reason, ''),
			coalesce(client_address, ''), coalesce(application_name, ''), coalesce(session_id, '')
		FROM igloo_audit.login_history
		WHERE ($1::text IS NULL OR user_name = $1)
		  AND ($2::timestamptz IS NULL OR event_time >= $2)
		  AND ($3::timestamptz IS NULL OR event_time < $3)
		  AND ($4::timestamptz IS NULL OR (event_time, event_id) < ($4, $5))
		ORDER BY event_time DESC, event_id DESC
		LIMIT $6`, p.args(v)...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Login, error) {
		var x Login
		err := row.Scan(&x.ID, &x.Time, &x.User, &x.Result, &x.Reason, &x.ClientAddress, &x.ApplicationName, &x.SessionID)
		return x, err
	})
}

// args are the query parameters for v and p: user, since, until, after
// time, after ID and limit.
func (p Page) args(v Viewer) []any {
	var user any
	if !v.All {
		user = v.User
	}
	limit := p.Limit
	if limit <= 0 {
		limit = 1000
	}
	return []any{user, nullTime(p.Since), nullTime(p.Until), nullTime(p.AfterTime), p.AfterID, limit}
}

// Prune deletes old history if no other replica is doing so.
func (s *PostgresStore) Prune(ctx context.Context, before time.Time) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var locked bool
		if err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock($1)", pruneKey).Scan(&locked); err != nil || !locked {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM igloo_audit.query_history WHERE start_time < $1`, before); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DELETE FROM igloo_audit.login_history WHERE event_time < $1`, before)
		return err
	})
}

func (s *PostgresStore) Close() { s.pool.Close() }

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullSlice(s []string) any {
	if s == nil {
		return nil
	}
	return s
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
