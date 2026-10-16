package maintenance

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// lockKey identifies Igloo's maintenance lock among Postgres advisory
	// locks.
	lockKey int64 = 0x49676c6f6f4d6e74 // "IglooMnt"
	// migrationKey serializes creating the schedule table across replicas
	// starting at the same time.
	migrationKey int64 = 0x49676c6f6f4d6967 // "IglooMig"
)

// PostgresLock is a session-level advisory lock in the metadata database.
// Postgres releases it if the replica holding it dies, so a crash never
// leaves tables unmaintained. It is also the Schedule, in the table
// igloo_maintenance.runs.
type PostgresLock struct {
	pool *pgxpool.Pool
}

// NewPostgresLock uses a small connection pool of its own, so maintenance
// never holds a connection the user store needs: one connection holds the
// lock during a pass, the other records the schedule.
func NewPostgresLock(ctx context.Context, url string) (*PostgresLock, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		for _, stmt := range []string{
			"SELECT pg_advisory_xact_lock(" + fmt.Sprint(migrationKey) + ")",
			"CREATE SCHEMA IF NOT EXISTS igloo_maintenance",
			"CREATE TABLE IF NOT EXISTS igloo_maintenance.runs (job text PRIMARY KEY, at timestamptz NOT NULL)",
		} {
			if _, err := tx.Exec(ctx, stmt); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("create maintenance schedule: %w", err)
	}
	return &PostgresLock{pool: pool}, nil
}

// LastRuns implements Schedule.
func (l *PostgresLock) LastRuns(ctx context.Context, prefix string) (map[string]time.Time, error) {
	rows, err := l.pool.Query(ctx, "SELECT job, at FROM igloo_maintenance.runs WHERE starts_with(job, $1)", prefix)
	if err != nil {
		return nil, err
	}
	runs := map[string]time.Time{}
	var (
		job string
		at  time.Time
	)
	_, err = pgx.ForEachRow(rows, []any{&job, &at}, func() error {
		runs[job] = at
		return nil
	})
	return runs, err
}

// Ran implements Schedule. A replica with a slow clock never moves a job's
// time backwards.
func (l *PostgresLock) Ran(ctx context.Context, job string, at time.Time) error {
	_, err := l.pool.Exec(ctx, `INSERT INTO igloo_maintenance.runs (job, at) VALUES ($1, $2)
		ON CONFLICT (job) DO UPDATE SET at = greatest(igloo_maintenance.runs.at, excluded.at)`, job, at)
	return err
}

// TryLock implements Lock.
func (l *PostgresLock) TryLock(ctx context.Context) (func(), bool, error) {
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	var ok bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockKey).Scan(&ok); err != nil {
		conn.Release()
		return nil, false, err
	}
	if !ok {
		conn.Release()
		return nil, false, nil
	}
	return func() {
		// Unlock even when the pass was canceled. If this fails the
		// connection is closed instead, which also releases the lock.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", lockKey); err != nil {
			_ = conn.Conn().Close(ctx)
		}
		conn.Release()
	}, true, nil
}

// Close closes the pool.
func (l *PostgresLock) Close() { l.pool.Close() }
