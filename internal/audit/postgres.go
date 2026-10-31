package audit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
	`ALTER TABLE igloo_audit.query_history ADD COLUMN IF NOT EXISTS policies text[]`,
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
	// How each login signed in, and with which access token. Added once,
	// so later starts take no lock on the table.
	`DO $$
	BEGIN
		IF NOT EXISTS (SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'igloo_audit' AND table_name = 'login_history' AND column_name = 'token_name') THEN
			ALTER TABLE igloo_audit.login_history
				ADD COLUMN IF NOT EXISTS auth_method text,
				ADD COLUMN IF NOT EXISTS token_name text;
		END IF;
	END $$`,
	// Workload attribution: the query tag, the compute pool, the time spent
	// waiting for limits, and what DuckDB measured. Added once, too.
	`DO $$
	BEGIN
		IF NOT EXISTS (SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'igloo_audit' AND table_name = 'query_history' AND column_name = 'memory_allocated') THEN
			ALTER TABLE igloo_audit.query_history
				ADD COLUMN IF NOT EXISTS query_tag text,
				ADD COLUMN IF NOT EXISTS pool text,
				ADD COLUMN IF NOT EXISTS queued_us bigint,
				ADD COLUMN IF NOT EXISTS cpu_time_us bigint,
				ADD COLUMN IF NOT EXISTS bytes_read bigint,
				ADD COLUMN IF NOT EXISTS memory_allocated bigint;
		END IF;
	END $$`,
	// How many workers ran parts of each statement. Added once, too.
	`DO $$
	BEGIN
		IF NOT EXISTS (SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'igloo_audit' AND table_name = 'query_history' AND column_name = 'workers') THEN
			ALTER TABLE igloo_audit.query_history ADD COLUMN IF NOT EXISTS workers integer NOT NULL DEFAULT 0;
		END IF;
	END $$`,
	// How long each replica process ran on each day. A process writes its
	// running total, so writing it again after a failure counts nothing
	// twice.
	`CREATE TABLE IF NOT EXISTS igloo_audit.compute_time (
		day            date NOT NULL,
		pool           text NOT NULL,
		replica        text NOT NULL,
		started        timestamptz NOT NULL,
		seconds        double precision NOT NULL,
		price_per_hour double precision NOT NULL,
		PRIMARY KEY (day, pool, replica, started)
	)`,
	// The locations a statement read files from, and the job it ran for,
	// for lineage. Added once, as above.
	`DO $$
	BEGIN
		IF NOT EXISTS (SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'igloo_audit' AND table_name = 'query_history' AND column_name = 'job') THEN
			ALTER TABLE igloo_audit.query_history
				ADD COLUMN IF NOT EXISTS locations_read text[],
				ADD COLUMN IF NOT EXISTS job text;
		END IF;
	END $$`,
	// Lineage reads only the statements that wrote tables.
	`CREATE INDEX IF NOT EXISTS query_history_writes ON igloo_audit.query_history (start_time)
		WHERE tables_written IS NOT NULL`,
	// How many stages and exchanges a distributed statement's parts took.
	// Added once, as above.
	`DO $$
	BEGIN
		IF NOT EXISTS (SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'igloo_audit' AND table_name = 'query_history' AND column_name = 'exchanges') THEN
			ALTER TABLE igloo_audit.query_history
				ADD COLUMN IF NOT EXISTS stages integer NOT NULL DEFAULT 0,
				ADD COLUMN IF NOT EXISTS exchanges integer NOT NULL DEFAULT 0;
		END IF;
	END $$`,
	// Which statements could run on workers, across runs. Added once, as
	// above.
	`DO $$
	BEGIN
		IF NOT EXISTS (SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'igloo_audit' AND table_name = 'query_history' AND column_name = 'plan_key') THEN
			ALTER TABLE igloo_audit.query_history ADD COLUMN IF NOT EXISTS plan_key text;
		END IF;
	END $$`,
	// Where a statement that could run on workers ran, and why. Added once,
	// as above.
	`DO $$
	BEGIN
		IF NOT EXISTS (SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'igloo_audit' AND table_name = 'query_history' AND column_name = 'placement') THEN
			ALTER TABLE igloo_audit.query_history ADD COLUMN IF NOT EXISTS placement text;
		END IF;
	END $$`,
	// Statements' shapes, and the memory they ran with, for reservations
	// and auto mode after a restart. Added once, as above.
	`DO $$
	BEGIN
		IF NOT EXISTS (SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'igloo_audit' AND table_name = 'query_history' AND column_name = 'run_memory') THEN
			ALTER TABLE igloo_audit.query_history
				ADD COLUMN IF NOT EXISTS shape text,
				ADD COLUMN IF NOT EXISTS run_memory bigint;
		END IF;
	END $$`,
	// The most memory statements were seen holding, for reservations after
	// a restart. Added once, as above.
	`DO $$
	BEGIN
		IF NOT EXISTS (SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'igloo_audit' AND table_name = 'query_history' AND column_name = 'memory_peak') THEN
			ALTER TABLE igloo_audit.query_history ADD COLUMN IF NOT EXISTS memory_peak bigint;
		END IF;
	END $$`,
	// What statements' AI functions did. Added once, as above.
	`DO $$
	BEGIN
		IF NOT EXISTS (SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'igloo_audit' AND table_name = 'query_history' AND column_name = 'ai_cost_usd') THEN
			ALTER TABLE igloo_audit.query_history
				ADD COLUMN IF NOT EXISTS ai_calls bigint,
				ADD COLUMN IF NOT EXISTS ai_cache_hits bigint,
				ADD COLUMN IF NOT EXISTS ai_input_tokens bigint,
				ADD COLUMN IF NOT EXISTS ai_output_tokens bigint,
				ADD COLUMN IF NOT EXISTS ai_cost_usd double precision,
				ADD COLUMN IF NOT EXISTS ai_models text[];
		END IF;
	END $$`,
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
		var cpu, read, memory any
		if r := q.Resources; r != nil {
			cpu, read, memory = r.CPUTime.Microseconds(), r.BytesRead, r.MemoryAllocated
		}
		var aiCalls, aiHits, aiIn, aiOut, aiCost, aiModels any
		if a := q.AI; a != nil {
			aiCalls, aiHits, aiIn, aiOut, aiCost, aiModels = a.Calls, a.CacheHits, a.InputTokens, a.OutputTokens, a.CostUSD, nullSlice(a.Models)
		}
		rows[i] = []any{q.ID, q.SessionID, q.User, q.Role, q.Start, q.Duration.Microseconds(), q.Outcome,
			nullable(q.SQLState), rowCount, q.StatementType, q.Text, nullSlice(q.TablesRead), nullSlice(q.TablesWritten),
			nullable(q.ClientAddress), nullable(q.ApplicationName), nullSlice(q.Policies), nullable(q.QueryTag),
			nullable(q.Pool), q.Queued.Microseconds(), cpu, read, memory, nullSlice(q.LocationsRead), nullable(q.Job), q.Workers,
			q.Stages, q.Exchanges, nullable(q.PlanKey), aiCalls, aiHits, aiIn, aiOut, aiCost, aiModels,
			nullable(q.Shape), nullableInt(q.Memory), nullableInt(q.Peak), nullable(q.Placement)}
	}
	return s.copy(ctx, "query_history", "query_id", []string{
		"query_id", "session_id", "user_name", "role", "start_time", "duration_us", "outcome", "sqlstate",
		"row_count", "statement_type", "query_text", "tables_read", "tables_written", "client_address", "application_name",
		"policies", "query_tag", "pool", "queued_us", "cpu_time_us", "bytes_read", "memory_allocated", "locations_read", "job", "workers",
		"stages", "exchanges", "plan_key", "ai_calls", "ai_cache_hits", "ai_input_tokens", "ai_output_tokens", "ai_cost_usd", "ai_models",
		"shape", "run_memory", "memory_peak", "placement",
	}, rows)
}

func (s *PostgresStore) WriteLogins(ctx context.Context, ls []Login) error {
	rows := make([][]any, len(ls))
	for i, x := range ls {
		rows[i] = []any{x.ID, x.Time, x.User, x.Result, nullable(x.Reason), nullable(x.ClientAddress),
			nullable(x.ApplicationName), nullable(x.SessionID), nullable(x.Method), nullable(x.Token)}
	}
	return s.copy(ctx, "login_history", "event_id", []string{
		"event_id", "event_time", "user_name", "result", "reason", "client_address", "application_name", "session_id",
		"auth_method", "token_name",
	}, rows)
}

// copy writes rows into table with COPY. A batch is written again after a
// failure (see Log.retry), and a failure such as a timeout can come after
// the batch committed: COPY then hits rows already there, and the batch is
// written through a staging table that skips them.
func (s *PostgresStore) copy(ctx context.Context, table, key string, cols []string, rows [][]any) error {
	_, err := s.pool.CopyFrom(ctx, pgx.Identifier{"igloo_audit", table}, cols, pgx.CopyFromRows(rows))
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" { // unique_violation
		return err
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"CREATE TEMP TABLE staged (LIKE igloo_audit.%s) ON COMMIT DROP", table)); err != nil {
			return err
		}
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"staged"}, cols, pgx.CopyFromRows(rows)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, fmt.Sprintf(
			"INSERT INTO igloo_audit.%s SELECT * FROM staged ON CONFLICT (%s) DO NOTHING", table, key))
		return err
	})
}

func (s *PostgresStore) Queries(ctx context.Context, v Viewer, p Page) ([]Query, error) {
	rows, err := s.pool.Query(ctx, `SELECT query_id, session_id, user_name, role, start_time, duration_us, outcome,
			coalesce(sqlstate, ''), coalesce(row_count, -1), statement_type, query_text, tables_read, tables_written,
			coalesce(client_address, ''), coalesce(application_name, ''), policies, coalesce(query_tag, ''),
			coalesce(pool, ''), coalesce(queued_us, 0), cpu_time_us, coalesce(bytes_read, 0), coalesce(memory_allocated, 0),
			locations_read, coalesce(job, ''), workers, stages, exchanges, coalesce(plan_key, ''),
			ai_calls, coalesce(ai_cache_hits, 0), coalesce(ai_input_tokens, 0), coalesce(ai_output_tokens, 0),
			coalesce(ai_cost_usd, 0), ai_models, coalesce(shape, ''), coalesce(run_memory, 0), coalesce(memory_peak, 0),
			coalesce(placement, '')
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
			q            Query
			us, queuedUS int64
			cpuUS        *int64
			r            Resources
			aiCalls      *int64
			a            AIUsage
		)
		err := row.Scan(&q.ID, &q.SessionID, &q.User, &q.Role, &q.Start, &us, &q.Outcome, &q.SQLState, &q.Rows,
			&q.StatementType, &q.Text, &q.TablesRead, &q.TablesWritten, &q.ClientAddress, &q.ApplicationName, &q.Policies,
			&q.QueryTag, &q.Pool, &queuedUS, &cpuUS, &r.BytesRead, &r.MemoryAllocated, &q.LocationsRead, &q.Job, &q.Workers,
			&q.Stages, &q.Exchanges, &q.PlanKey, &aiCalls, &a.CacheHits, &a.InputTokens, &a.OutputTokens, &a.CostUSD, &a.Models,
			&q.Shape, &q.Memory, &q.Peak, &q.Placement)
		q.Duration, q.Queued = micros(us), micros(queuedUS)
		if cpuUS != nil {
			r.CPUTime = micros(*cpuUS)
			q.Resources = &r
		}
		if aiCalls != nil {
			a.Calls = *aiCalls
			q.AI = &a
		}
		return q, err
	})
}

func (s *PostgresStore) Logins(ctx context.Context, v Viewer, p Page) ([]Login, error) {
	rows, err := s.pool.Query(ctx, `SELECT event_id, event_time, user_name, result, coalesce(reason, ''),
			coalesce(client_address, ''), coalesce(application_name, ''), coalesce(session_id, ''),
			coalesce(auth_method, ''), coalesce(token_name, '')
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
		err := row.Scan(&x.ID, &x.Time, &x.User, &x.Result, &x.Reason, &x.ClientAddress, &x.ApplicationName, &x.SessionID,
			&x.Method, &x.Token)
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

// Lineage pairs what each successful statement read with what it wrote,
// in the database: one row per edge, with its latest statement. Names
// that differ only in case are one table, shown as last written.
func (s *PostgresStore) Lineage(ctx context.Context, since time.Time) ([]Edge, error) {
	rows, err := s.pool.Query(ctx, `WITH writes AS (
			SELECT query_id, user_name, start_time, statement_type, job, tables_read, locations_read, tables_written
			FROM igloo_audit.query_history
			WHERE tables_written IS NOT NULL AND outcome = 'ok' AND ($1::timestamptz IS NULL OR start_time >= $1)
		), edges AS (
			SELECT q.*, r.name AS src, false AS loc, w.name AS dst
			FROM writes q, unnest(q.tables_read) AS r(name), unnest(q.tables_written) AS w(name)
			WHERE lower(r.name) <> lower(w.name)
			UNION ALL
			SELECT q.*, r.name, true, w.name
			FROM writes q, unnest(q.locations_read) AS r(name), unnest(q.tables_written) AS w(name)
		)
		SELECT DISTINCT ON (lower(src), loc, lower(dst)) src, loc, dst,
			min(start_time) OVER e, max(start_time) OVER e, count(*) OVER e,
			query_id, user_name, statement_type, coalesce(job, '')
		FROM edges
		WINDOW e AS (PARTITION BY lower(src), loc, lower(dst))
		ORDER BY lower(src), loc, lower(dst), start_time DESC, query_id DESC`, nullTime(since))
	if err != nil {
		return nil, err
	}
	edges, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Edge, error) {
		var e Edge
		err := row.Scan(&e.From, &e.FromLocation, &e.To, &e.FirstSeen, &e.LastSeen, &e.Statements,
			&e.LastQueryID, &e.LastUser, &e.LastStatementType, &e.LastJob)
		return e, err
	})
	sortEdges(edges)
	return edges, err
}

// TableUsage counts, in the database, the successful statements since
// since that read each table and that changed its data.
func (s *PostgresStore) TableUsage(ctx context.Context, since time.Time) ([]TableUsage, error) {
	rows, err := s.pool.Query(ctx, `SELECT max(t), count(*) FILTER (WHERE NOT w), count(*) FILTER (WHERE w),
			count(DISTINCT user_name), max(start_time) FILTER (WHERE NOT w), max(start_time) FILTER (WHERE w)
		FROM (
			SELECT q.user_name, q.start_time, t, false AS w
			FROM igloo_audit.query_history q, unnest(q.tables_read) AS t
			WHERE q.tables_read IS NOT NULL AND q.outcome = 'ok' AND q.start_time >= $1 AND q.role IS DISTINCT FROM $3
			UNION ALL
			SELECT q.user_name, q.start_time, t, true
			FROM igloo_audit.query_history q, unnest(q.tables_written) AS t
			WHERE q.tables_written IS NOT NULL AND q.outcome = 'ok' AND q.start_time >= $1 AND q.statement_type = ANY ($2)
		) used
		GROUP BY lower(t) ORDER BY lower(t)`, since, DataStatements, RecipientRole)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (TableUsage, error) {
		var (
			u           TableUsage
			read, write *time.Time
		)
		err := row.Scan(&u.Table, &u.Reads, &u.Writes, &u.Users, &read, &write)
		if read != nil {
			u.LastRead = *read
		}
		if write != nil {
			u.LastWrite = *write
		}
		return u, err
	})
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
		if _, err := tx.Exec(ctx, `DELETE FROM igloo_audit.login_history WHERE event_time < $1`, before); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DELETE FROM igloo_audit.compute_time WHERE day < ($1::timestamptz AT TIME ZONE 'UTC')::date`, before)
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

func nullableInt(n int64) any {
	if n == 0 {
		return nil
	}
	return n
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
