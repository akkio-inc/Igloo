package tasks

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/akkio-inc/igloo/internal/audit"
)

const (
	// migrationKey serializes creating the tables across replicas starting
	// at the same time.
	migrationKey int64 = 0x49676c6f6f54736b // "IglooTsk"
	// pruneKey lets one replica at a time delete old runs.
	pruneKey int64 = 0x49676c6f6f545072 // "IglooTPr"

	// leaseTTL is how long a lease (TryLease) lasts in the database
	// unless its holder renews it, which it does every renewEvery. Its
	// holder takes it for lost leaseMargin before then, by its own clock:
	// the time a run canceled for it has to stop before another may
	// start.
	leaseTTL    = 30 * time.Second
	renewEvery  = 5 * time.Second
	leaseMargin = 10 * time.Second
)

var migrations = []string{
	`CREATE SCHEMA IF NOT EXISTS igloo_tasks`,
	`CREATE TABLE IF NOT EXISTS igloo_tasks.jobs (
		kind       text NOT NULL,
		name       text NOT NULL,
		owner      text NOT NULL,
		statement  text NOT NULL,
		schedule   text,
		timezone   text,
		state      text NOT NULL,
		next_run   timestamptz,
		created_at timestamptz NOT NULL DEFAULT now()
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS jobs_name ON igloo_tasks.jobs (kind, lower(name))`,
	`CREATE TABLE IF NOT EXISTS igloo_tasks.runs (
		run_id         text PRIMARY KEY,
		kind           text NOT NULL,
		name           text NOT NULL,
		user_name      text NOT NULL,
		trigger        text NOT NULL,
		scheduled_time timestamptz,
		start_time     timestamptz NOT NULL,
		end_time       timestamptz,
		outcome        text NOT NULL,
		error          text,
		row_count      bigint
	)`,
	`CREATE INDEX IF NOT EXISTS runs_time ON igloo_tasks.runs (start_time DESC, run_id DESC)`,
	`CREATE INDEX IF NOT EXISTS runs_user ON igloo_tasks.runs (user_name, start_time DESC, run_id DESC)`,
	`CREATE INDEX IF NOT EXISTS runs_job ON igloo_tasks.runs (kind, lower(name), start_time DESC)`,
	`CREATE TABLE IF NOT EXISTS igloo_tasks.loads (
		load_id       text PRIMARY KEY,
		table_name    text NOT NULL,
		file          text NOT NULL,
		size          bigint NOT NULL,
		last_modified timestamptz,
		status        text NOT NULL,
		row_count     bigint NOT NULL,
		error_count   bigint NOT NULL,
		first_error   text,
		start_time    timestamptz NOT NULL,
		end_time      timestamptz,
		user_name     text NOT NULL,
		pipe_name     text
	)`,
	`CREATE INDEX IF NOT EXISTS loads_file ON igloo_tasks.loads (lower(table_name), file, start_time DESC)`,
	`CREATE INDEX IF NOT EXISTS loads_time ON igloo_tasks.loads (start_time DESC, load_id DESC)`,
	`CREATE INDEX IF NOT EXISTS loads_user ON igloo_tasks.loads (user_name, start_time DESC, load_id DESC)`,
	// The leases of locks taken with TryLease, until they expire or their
	// holders let them go.
	`CREATE TABLE IF NOT EXISTS igloo_tasks.leases (
		key     text PRIMARY KEY,
		holder  text NOT NULL,
		expires timestamptz NOT NULL
	)`,
}

// PostgresStore keeps jobs and history in the "igloo_tasks" schema of the
// metadata database, shared by every replica. Locks are session advisory
// locks, each on a connection of its own: Postgres releases them if the
// replica holding them dies. A lease (TryLease) is a lease row as well.
type PostgresStore struct {
	pool *pgxpool.Pool
	url  string
	// leaseTTL, renewEvery and leaseMargin are the constants of those
	// names; tests shorten them.
	leaseTTL, renewEvery, leaseMargin time.Duration
}

// OpenPostgres connects to url and creates the tables if needed.
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
		return nil, fmt.Errorf("create task tables: %w", err)
	}
	return &PostgresStore{pool: pool, url: url, leaseTTL: leaseTTL, renewEvery: renewEvery, leaseMargin: leaseMargin}, nil
}

const jobColumns = `j.kind, j.name, j.owner, j.statement, coalesce(j.schedule, ''), coalesce(j.timezone, ''), j.state,
	j.next_run, j.created_at`

func scanJob(row pgx.Row, withRun bool) (Job, error) {
	var (
		j    Job
		next *time.Time
	)
	dest := []any{&j.Kind, &j.Name, &j.Owner, &j.Statement, &j.Schedule, &j.Timezone, &j.State, &next, &j.CreatedAt}
	var (
		runID               *string
		r                   Run
		scheduled, end      *time.Time
		start               *time.Time
		outcome, errText    *string
		rows                *int64
		user, trigger, kind *string
	)
	if withRun {
		dest = append(dest, &runID, &kind, &user, &trigger, &scheduled, &start, &end, &outcome, &errText, &rows)
	}
	if err := row.Scan(dest...); err != nil {
		return j, err
	}
	if next != nil {
		j.NextRun = *next
	}
	if runID != nil {
		r = Run{ID: *runID, Kind: *kind, Name: j.Name, User: *user, Trigger: *trigger, Outcome: *outcome, Rows: -1}
		if scheduled != nil {
			r.Scheduled = *scheduled
		}
		if start != nil {
			r.Start = *start
		}
		if end != nil {
			r.End = *end
		}
		if errText != nil {
			r.Error = *errText
		}
		if rows != nil {
			r.Rows = *rows
		}
		j.LastRun = &r
	}
	return j, nil
}

// lastRunJoin adds each job's latest run.
const lastRunJoin = ` LEFT JOIN LATERAL (
		SELECT run_id, kind, user_name, trigger, scheduled_time, start_time, end_time, outcome, error, row_count
		FROM igloo_tasks.runs r WHERE r.kind = j.kind AND lower(r.name) = lower(j.name)
		ORDER BY start_time DESC LIMIT 1) r ON true`

func (s *PostgresStore) CreateJob(ctx context.Context, j Job, replace bool) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if replace {
			if _, err := tx.Exec(ctx, `DELETE FROM igloo_tasks.jobs WHERE kind = $1 AND lower(name) = lower($2)`, j.Kind, j.Name); err != nil {
				return err
			}
		}
		_, err := tx.Exec(ctx, `INSERT INTO igloo_tasks.jobs (kind, name, owner, statement, schedule, timezone, state, next_run)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			j.Kind, j.Name, j.Owner, j.Statement, nullable(j.Schedule), nullable(j.Timezone), j.State, nullTime(j.NextRun))
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrExists
		}
		return err
	})
}

func (s *PostgresStore) Job(ctx context.Context, kind, name string) (Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, `SELECT `+jobColumns+`, r.* FROM igloo_tasks.jobs j`+lastRunJoin+`
		WHERE j.kind = $1 AND lower(j.name) = lower($2)`, kind, name), true)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return j, err
}

func (s *PostgresStore) Jobs(ctx context.Context, kind string) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+jobColumns+`, r.* FROM igloo_tasks.jobs j`+lastRunJoin+`
		WHERE j.kind = $1 ORDER BY j.name`, kind)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Job, error) { return scanJob(row, true) })
}

func (s *PostgresStore) exec(ctx context.Context, sql string, args ...any) error {
	tag, err := s.pool.Exec(ctx, sql, args...)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

func (s *PostgresStore) SetState(ctx context.Context, kind, name, state string, next time.Time) error {
	return s.exec(ctx, `UPDATE igloo_tasks.jobs SET state = $3, next_run = $4 WHERE kind = $1 AND lower(name) = lower($2)`,
		kind, name, state, nullTime(next))
}

func (s *PostgresStore) SetSchedule(ctx context.Context, kind, name, schedule, timezone string, next time.Time) error {
	return s.exec(ctx, `UPDATE igloo_tasks.jobs SET schedule = $3, timezone = $4, next_run = $5
		WHERE kind = $1 AND lower(name) = lower($2)`, kind, name, nullable(schedule), nullable(timezone), nullTime(next))
}

func (s *PostgresStore) DropJob(ctx context.Context, kind, name string) error {
	return s.exec(ctx, `DELETE FROM igloo_tasks.jobs WHERE kind = $1 AND lower(name) = lower($2)`, kind, name)
}

func (s *PostgresStore) DisownJobs(ctx context.Context, owner, tombstone string) error {
	_, err := s.pool.Exec(ctx, `UPDATE igloo_tasks.jobs SET owner = $2, state = 'suspended', next_run = NULL WHERE owner = $1`,
		owner, tombstone)
	return err
}

func (s *PostgresStore) Scheduled(ctx context.Context) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+jobColumns+` FROM igloo_tasks.jobs j
		WHERE j.state = 'started' AND j.next_run IS NOT NULL ORDER BY j.next_run`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Job, error) { return scanJob(row, false) })
}

func (s *PostgresStore) Claim(ctx context.Context, kind, name string, due, next time.Time) (bool, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE igloo_tasks.jobs SET next_run = $4
		WHERE kind = $1 AND lower(name) = lower($2) AND state = 'started' AND next_run = $3`, kind, name, due, nullTime(next))
	return err == nil && tag.RowsAffected() == 1, err
}

// lockConn opens a connection of its own for a lock, so that closing it
// releases the lock whatever happened.
func (s *PostgresStore) lockConn(ctx context.Context) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(s.url)
	if err != nil {
		return nil, err
	}
	return pgx.ConnectConfig(ctx, cfg)
}

func unlocker(conn *pgx.Conn) func() {
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Closing the connection ends its session and so its locks, but the
		// server ends the session after Close returns, and a lock taken again
		// right away could find it still held: unlock first.
		_, _ = conn.Exec(ctx, "SELECT pg_advisory_unlock_all()")
		_ = conn.Close(ctx)
	}
}

func (s *PostgresStore) TryLock(ctx context.Context, key string) (func(), bool, error) {
	conn, err := s.lockConn(ctx)
	if err != nil {
		return nil, false, err
	}
	var ok bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockID(key)).Scan(&ok); err != nil || !ok {
		unlocker(conn)()
		return nil, false, err
	}
	return unlocker(conn), true, nil
}

// TryLease takes the advisory lock, as TryLock does, and the key's lease
// row. Postgres releases the lock the moment its connection drops, while
// the run that held it may go on until it notices: the lease row stays
// until its holder lets it go or it expires, leaseTTL after its holder
// last renewed it, and the lock is not taken again before then. Its
// holder renews it every renewEvery, through the pool, whether or not it
// still has the lock, and takes it for lost once it could not for leaseTTL
// less leaseMargin: by then a run that holds it is canceled (see Held),
// before anyone else can take the lease. Expiry is by the database's
// clock; the holder counts by its own, from before it sent each renewal.
func (s *PostgresStore) TryLease(ctx context.Context, key string) (Lease, bool, error) {
	conn, err := s.lockConn(ctx)
	if err != nil {
		return nil, false, err
	}
	var ok bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockID(key)).Scan(&ok); err != nil || !ok {
		unlocker(conn)()
		return nil, false, err
	}
	l := &pgLease{s: s, conn: conn, id: lockID(key), key: key, holder: uuid.NewString(),
		stop: make(chan struct{}), stopped: make(chan struct{})}
	sent := time.Now()
	tag, err := s.pool.Exec(ctx, `INSERT INTO igloo_tasks.leases AS l (key, holder, expires)
		VALUES ($1, $2, now() + $3 * interval '1 millisecond')
		ON CONFLICT (key) DO UPDATE SET holder = excluded.holder, expires = excluded.expires WHERE l.expires <= now()`,
		key, l.holder, s.leaseTTL.Milliseconds())
	if err != nil || tag.RowsAffected() == 0 {
		// The last holder lost the lock, and may still be running.
		unlocker(conn)()
		return nil, false, err
	}
	l.until = sent.Add(s.leaseTTL - s.leaseMargin)
	go l.renew()
	return l, true, nil
}

// pgLease is an advisory lock on a connection of its own, and a lease row.
type pgLease struct {
	s           *PostgresStore
	conn        *pgx.Conn
	id          int64
	key, holder string

	mu sync.Mutex
	// until is when the holder takes the lease for lost unless renewed,
	// and lost why it is.
	until time.Time
	lost  error

	stop, stopped chan struct{}
	release       sync.Once
}

// renew renews the lease every renewEvery until Release.
func (l *pgLease) renew() {
	defer close(l.stopped)
	tick := time.NewTicker(l.s.renewEvery)
	defer tick.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-tick.C:
		}
		sent := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), l.s.renewEvery)
		tag, err := l.s.pool.Exec(ctx, `UPDATE igloo_tasks.leases SET expires = now() + $3 * interval '1 millisecond'
			WHERE key = $1 AND holder = $2 AND expires > now()`, l.key, l.holder, l.s.leaseTTL.Milliseconds())
		cancel()
		l.mu.Lock()
		switch {
		case l.lost != nil || !sent.Before(l.until):
			// Taken for lost already: it stays lost.
		case err != nil:
			// Tried again next time, until until.
		case tag.RowsAffected() == 0:
			l.lost = errors.New("its lease expired")
		default:
			l.until = sent.Add(l.s.leaseTTL - l.s.leaseMargin)
		}
		l.mu.Unlock()
	}
}

// Held reports the lease lost once it could not be renewed in time, and
// asks the lock's own connection whether it holds the lock: one that
// dropped has none, and another session may have taken it since.
func (l *pgLease) Held(ctx context.Context) error {
	l.mu.Lock()
	if l.lost == nil && !time.Now().Before(l.until) {
		l.lost = errors.New("its lease could not be renewed in time")
	}
	lost, until := l.lost, l.until
	l.mu.Unlock()
	if lost != nil {
		return fmt.Errorf("the lock is lost: %w", lost)
	}
	ctx, cancel := context.WithDeadline(ctx, until)
	defer cancel()
	var n int
	err := l.conn.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND pid = pg_backend_pid()
		AND classid::bigint = ($1::bigint >> 32) & 4294967295 AND objid::bigint = $1::bigint & 4294967295 AND objsubid = 1`, l.id).Scan(&n)
	switch {
	case err != nil:
		return fmt.Errorf("the lock is lost: %w", err)
	case n == 0:
		return errors.New("the lock is lost")
	}
	return nil
}

// Release lets the lease row go, and then the lock, so that the next to
// take the lock finds the row gone. A row left behind expires.
func (l *pgLease) Release() {
	l.release.Do(func() {
		close(l.stop)
		<-l.stopped
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = l.s.pool.Exec(ctx, `DELETE FROM igloo_tasks.leases WHERE key = $1 AND holder = $2`, l.key, l.holder)
		unlocker(l.conn)()
	})
}

func (s *PostgresStore) Lock(ctx context.Context, key string) (func(), error) {
	conn, err := s.lockConn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockID(key)); err != nil {
		unlocker(conn)()
		return nil, err
	}
	return unlocker(conn), nil
}

func (s *PostgresStore) WriteRun(ctx context.Context, r Run) error {
	var rows any
	if r.Rows >= 0 {
		rows = r.Rows
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO igloo_tasks.runs
			(run_id, kind, name, user_name, trigger, scheduled_time, start_time, end_time, outcome, error, row_count)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (run_id) DO UPDATE SET end_time = excluded.end_time, outcome = excluded.outcome,
			error = excluded.error, row_count = excluded.row_count`,
		r.ID, r.Kind, r.Name, r.User, r.Trigger, nullTime(r.Scheduled), r.Start, nullTime(r.End), r.Outcome,
		nullable(r.Error), rows)
	return err
}

func (s *PostgresStore) AbandonRuns(ctx context.Context, kind, name string) error {
	_, err := s.pool.Exec(ctx, `UPDATE igloo_tasks.runs SET outcome = 'failed', error = $3, end_time = now()
		WHERE kind = $1 AND lower(name) = lower($2) AND outcome = 'running'`, kind, name, abandoned)
	return err
}

func (s *PostgresStore) Runs(ctx context.Context, v audit.Viewer, p audit.Page) ([]Run, error) {
	rows, err := s.pool.Query(ctx, `SELECT run_id, kind, name, user_name, trigger, scheduled_time, start_time,
			end_time, outcome, coalesce(error, ''), coalesce(row_count, -1)
		FROM igloo_tasks.runs
		WHERE ($1::text IS NULL OR user_name = $1)
		  AND ($2::timestamptz IS NULL OR start_time >= $2)
		  AND ($3::timestamptz IS NULL OR start_time < $3)
		  AND ($4::timestamptz IS NULL OR (start_time, run_id) < ($4, $5))
		ORDER BY start_time DESC, run_id DESC
		LIMIT $6`, pageArgs(v, p)...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Run, error) {
		var (
			r              Run
			scheduled, end *time.Time
		)
		err := row.Scan(&r.ID, &r.Kind, &r.Name, &r.User, &r.Trigger, &scheduled, &r.Start, &end, &r.Outcome, &r.Error, &r.Rows)
		if scheduled != nil {
			r.Scheduled = *scheduled
		}
		if end != nil {
			r.End = *end
		}
		return r, err
	})
}

func (s *PostgresStore) Prune(ctx context.Context, before time.Time) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var locked bool
		if err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock($1)", pruneKey).Scan(&locked); err != nil || !locked {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM igloo_tasks.runs WHERE start_time < $1`, before); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DELETE FROM igloo_tasks.loads WHERE status = 'failed' AND start_time < $1`, before)
		return err
	})
}

// LastLoad reads the pipe's runs: a pipe's check is recorded only when it
// loaded files or failed, and a failed one may have loaded some.
func (s *PostgresStore) LastLoad(ctx context.Context, pipe string) (time.Time, error) {
	var at *time.Time
	err := s.pool.QueryRow(ctx, `SELECT max(start_time) FROM igloo_tasks.runs
		WHERE kind = $1 AND lower(name) = lower($2) AND (outcome = 'succeeded' OR row_count > 0)`, KindPipe, pipe).Scan(&at)
	if err != nil || at == nil {
		return time.Time{}, err
	}
	return *at, nil
}

const loadColumns = `load_id, table_name, file, size, last_modified, status, row_count, error_count,
	coalesce(first_error, ''), start_time, end_time, user_name, coalesce(pipe_name, '')`

func scanLoad(row pgx.CollectableRow) (Load, error) {
	var (
		l             Load
		modified, end *time.Time
	)
	err := row.Scan(&l.ID, &l.Table, &l.File, &l.Size, &modified, &l.Status, &l.Rows, &l.Errors, &l.FirstError,
		&l.Start, &end, &l.User, &l.Pipe)
	if modified != nil {
		l.Modified = *modified
	}
	if end != nil {
		l.End = *end
	}
	return l, err
}

func (s *PostgresStore) PreviousLoads(ctx context.Context, table string, files []string) (map[string]Load, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT ON (file) `+loadColumns+` FROM igloo_tasks.loads
		WHERE lower(table_name) = lower($1) AND file = ANY($2)
		ORDER BY file, start_time DESC, load_id DESC`, table, files)
	if err != nil {
		return nil, err
	}
	loads, err := pgx.CollectRows(rows, scanLoad)
	if err != nil {
		return nil, err
	}
	out := make(map[string]Load, len(loads))
	for _, l := range loads {
		out[l.File] = l
	}
	return out, nil
}

func (s *PostgresStore) WriteLoads(ctx context.Context, ls []Load) error {
	batch := &pgx.Batch{}
	for _, l := range ls {
		batch.Queue(`INSERT INTO igloo_tasks.loads (load_id, table_name, file, size, last_modified, status, row_count,
				error_count, first_error, start_time, end_time, user_name, pipe_name)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
			ON CONFLICT (load_id) DO UPDATE SET status = excluded.status, row_count = excluded.row_count,
				error_count = excluded.error_count, first_error = excluded.first_error, end_time = excluded.end_time`,
			l.ID, l.Table, l.File, l.Size, nullTime(l.Modified), l.Status, l.Rows, l.Errors, nullable(l.FirstError),
			l.Start, nullTime(l.End), l.User, nullable(l.Pipe))
	}
	return s.pool.SendBatch(ctx, batch).Close()
}

func (s *PostgresStore) DeleteLoads(ctx context.Context, ids []string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM igloo_tasks.loads WHERE load_id = ANY($1)`, ids)
	return err
}

func (s *PostgresStore) Loads(ctx context.Context, v audit.Viewer, p audit.Page) ([]Load, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+loadColumns+` FROM igloo_tasks.loads
		WHERE ($1::text IS NULL OR user_name = $1)
		  AND ($2::timestamptz IS NULL OR start_time >= $2)
		  AND ($3::timestamptz IS NULL OR start_time < $3)
		  AND ($4::timestamptz IS NULL OR (start_time, load_id) < ($4, $5))
		ORDER BY start_time DESC, load_id DESC
		LIMIT $6`, pageArgs(v, p)...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanLoad)
}

func (s *PostgresStore) ForgetTable(ctx context.Context, table string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM igloo_tasks.loads WHERE lower(table_name) = lower($1)`, table); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DELETE FROM igloo_tasks.jobs WHERE kind = $1 AND lower(name) = lower($2)`,
			KindMaterializedView, mvName(table))
		return err
	})
}

func (s *PostgresStore) RenameTable(ctx context.Context, from, to string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE igloo_tasks.loads SET table_name = $2 WHERE lower(table_name) = lower($1)`, from, to); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE igloo_tasks.jobs SET name = $3 WHERE kind = $1 AND lower(name) = lower($2)`,
			KindMaterializedView, mvName(from), mvName(to))
		return err
	})
}

func (s *PostgresStore) Close() { s.pool.Close() }

// pageArgs are the query parameters for v and p: user, since, until,
// after time, after ID and limit.
func pageArgs(v audit.Viewer, p audit.Page) []any {
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

func nullable(s string) any {
	if s == "" {
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
