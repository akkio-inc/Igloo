package cleanroom

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store keeps rooms, collaborators, datasets, templates, approvals and runs
// in the "igloo_cleanroom" schema of the metadata database, so every
// replica sees the same ones.
type Store struct {
	pool *pgxpool.Pool
}

// migrationKey serializes creating the tables across replicas that start
// together.
const migrationKey int64 = 0x49676c6f6f436c52 // "IglooClR"

var migrations = []string{
	`CREATE SCHEMA IF NOT EXISTS igloo_cleanroom`,
	`CREATE TABLE IF NOT EXISTS igloo_cleanroom.rooms (
		name       text PRIMARY KEY,
		comment    text NOT NULL DEFAULT '',
		created_by text NOT NULL DEFAULT '',
		created_at timestamptz NOT NULL DEFAULT now()
	)`,
	// A principal is one collaborator of a room at most.
	`CREATE TABLE IF NOT EXISTS igloo_cleanroom.collaborators (
		room      text NOT NULL REFERENCES igloo_cleanroom.rooms (name) ON DELETE CASCADE,
		name      text NOT NULL,
		kind      text NOT NULL CHECK (kind IN ('user', 'role', 'partner')),
		principal text NOT NULL,
		provider  text NOT NULL DEFAULT '',
		added_by  text NOT NULL DEFAULT '',
		added_at  timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (room, name),
		UNIQUE (room, kind, principal)
	)`,
	`CREATE TABLE IF NOT EXISTS igloo_cleanroom.datasets (
		room          text NOT NULL,
		name          text NOT NULL,
		collaborator  text NOT NULL,
		source_schema text NOT NULL,
		source_table  text NOT NULL,
		kind          text NOT NULL,
		identity      text NOT NULL DEFAULT '',
		policy        jsonb NOT NULL,
		added_by      text NOT NULL DEFAULT '',
		added_at      timestamptz NOT NULL DEFAULT now(),
		updated_at    timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (room, name),
		FOREIGN KEY (room, collaborator) REFERENCES igloo_cleanroom.collaborators (room, name) ON DELETE CASCADE
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS datasets_source
		ON igloo_cleanroom.datasets (room, lower(source_schema), lower(source_table))`,
	`CREATE TABLE IF NOT EXISTS igloo_cleanroom.templates (
		room       text NOT NULL REFERENCES igloo_cleanroom.rooms (name) ON DELETE CASCADE,
		name       text NOT NULL,
		version    integer NOT NULL CHECK (version > 0),
		sql        text NOT NULL,
		parameters jsonb NOT NULL,
		run_by     text[] NOT NULL,
		builtin    text NOT NULL DEFAULT '',
		digest     text NOT NULL,
		created_by text NOT NULL DEFAULT '',
		created_at timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (room, name, version)
	)`,
	// The last version of each template a room has had, removed ones too:
	// a version number is never used twice, so an approval of one is of
	// what its approver reviewed.
	`CREATE TABLE IF NOT EXISTS igloo_cleanroom.template_versions (
		room    text NOT NULL REFERENCES igloo_cleanroom.rooms (name) ON DELETE CASCADE,
		name    text NOT NULL,
		version integer NOT NULL,
		PRIMARY KEY (room, name)
	)`,
	`CREATE TABLE IF NOT EXISTS igloo_cleanroom.approvals (
		room         text NOT NULL,
		template     text NOT NULL,
		version      integer NOT NULL,
		digest       text NOT NULL,
		collaborator text NOT NULL,
		approved_by  text NOT NULL DEFAULT '',
		approved_at  timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (room, template, version, collaborator),
		FOREIGN KEY (room, template, version) REFERENCES igloo_cleanroom.templates (room, name, version) ON DELETE CASCADE,
		FOREIGN KEY (room, collaborator) REFERENCES igloo_cleanroom.collaborators (room, name) ON DELETE CASCADE
	)`,
	`CREATE TABLE IF NOT EXISTS igloo_cleanroom.runs (
		id           bigserial PRIMARY KEY,
		room         text NOT NULL REFERENCES igloo_cleanroom.rooms (name) ON DELETE CASCADE,
		template     text NOT NULL,
		version      integer NOT NULL,
		requester    text NOT NULL,
		requested_by text NOT NULL,
		parameters   jsonb NOT NULL,
		approvals    text[] NOT NULL,
		datasets     text[] NOT NULL,
		status       text NOT NULL,
		error        text NOT NULL DEFAULT '',
		rows         bigint NOT NULL DEFAULT 0,
		truncated    boolean NOT NULL DEFAULT false,
		noise        double precision NOT NULL DEFAULT 0,
		cost         integer NOT NULL DEFAULT 1,
		result_table text NOT NULL DEFAULT '',
		columns      jsonb,
		result       jsonb,
		started_at   timestamptz NOT NULL DEFAULT now(),
		finished_at  timestamptz
	)`,
	`CREATE INDEX IF NOT EXISTS runs_room ON igloo_cleanroom.runs (room, started_at)`,
	// Stores made before these columns: a source of unknown identity, and
	// approvals of unknown digest, approve nothing until made again.
	`ALTER TABLE igloo_cleanroom.datasets ADD COLUMN IF NOT EXISTS identity text NOT NULL DEFAULT ''`,
	`ALTER TABLE igloo_cleanroom.templates ADD COLUMN IF NOT EXISTS digest text NOT NULL DEFAULT ''`,
	`ALTER TABLE igloo_cleanroom.approvals ADD COLUMN IF NOT EXISTS digest text NOT NULL DEFAULT ''`,
	`ALTER TABLE igloo_cleanroom.runs ADD COLUMN IF NOT EXISTS cost integer NOT NULL DEFAULT 1`,
	// Changes to stored data, each applied once.
	`CREATE TABLE IF NOT EXISTS igloo_cleanroom.applied (name text PRIMARY KEY, at timestamptz NOT NULL DEFAULT now())`,
	// Datasets with noise allow 10 runs a day unless they say otherwise;
	// those stored with the old default of 100 get the new one.
	`WITH once AS (INSERT INTO igloo_cleanroom.applied (name) VALUES ('noisy_runs_per_day_10') ON CONFLICT DO NOTHING RETURNING name)
	UPDATE igloo_cleanroom.datasets SET policy = jsonb_set(policy, '{runs_per_day}', '10')
	WHERE EXISTS (SELECT FROM once) AND coalesce((policy->>'noise')::float8, 0) > 0 AND (policy->>'runs_per_day')::int = 100`,
}

// OpenStore connects to the metadata database at url with a small pool of
// its own and creates the clean room tables if needed.
func OpenStore(ctx context.Context, url string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 8
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
		return nil, fmt.Errorf("create clean room tables: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the store's connections.
func (s *Store) Close() { s.pool.Close() }

func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// notFound turns a missing row into ErrNotFound.
func notFound(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

// --- rooms -------------------------------------------------------------------

func (s *Store) CreateRoom(ctx context.Context, r Room) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO igloo_cleanroom.rooms (name, comment, created_by) VALUES ($1, $2, $3)`,
		r.Name, r.Comment, r.CreatedBy)
	if pgCode(err) == "23505" {
		return ErrExists
	}
	return err
}

// DropRoom removes a room and everything in it.
func (s *Store) DropRoom(ctx context.Context, name string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM igloo_cleanroom.rooms WHERE name = $1`, name)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

func (s *Store) Rooms(ctx context.Context) ([]Room, error) {
	rows, err := s.pool.Query(ctx, `SELECT name, comment, created_by, created_at FROM igloo_cleanroom.rooms ORDER BY name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Room, error) {
		var r Room
		err := row.Scan(&r.Name, &r.Comment, &r.CreatedBy, &r.CreatedAt)
		return r, err
	})
}

func (s *Store) Room(ctx context.Context, name string) (Room, error) {
	r := Room{Name: name}
	err := s.pool.QueryRow(ctx, `SELECT comment, created_by, created_at FROM igloo_cleanroom.rooms WHERE name = $1`, name).
		Scan(&r.Comment, &r.CreatedBy, &r.CreatedAt)
	return r, notFound(err)
}

// --- collaborators -----------------------------------------------------------

// AddCollaborator fails with ErrExists when the room has a collaborator of
// the name or for the principal, and ErrNotFound without the room.
func (s *Store) AddCollaborator(ctx context.Context, c Collaborator) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO igloo_cleanroom.collaborators (room, name, kind, principal, provider, added_by)
		VALUES ($1, $2, $3, $4, $5, $6)`, c.Room, c.Name, c.Kind, c.Principal, c.Provider, c.AddedBy)
	switch pgCode(err) {
	case "23505":
		return ErrExists
	case "23503":
		return ErrNotFound
	}
	return err
}

// RemoveCollaborator removes a collaborator with its approvals.
func (s *Store) RemoveCollaborator(ctx context.Context, room, name string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM igloo_cleanroom.collaborators WHERE room = $1 AND name = $2`, room, name)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

// Collaborators lists a room's collaborators, or every room's when room is
// empty.
func (s *Store) Collaborators(ctx context.Context, room string) ([]Collaborator, error) {
	rows, err := s.pool.Query(ctx, `SELECT room, name, kind, principal, provider, added_by, added_at
		FROM igloo_cleanroom.collaborators WHERE $1 = '' OR room = $1 ORDER BY room, name`, room)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Collaborator, error) {
		var c Collaborator
		err := row.Scan(&c.Room, &c.Name, &c.Kind, &c.Principal, &c.Provider, &c.AddedBy, &c.AddedAt)
		return c, err
	})
}

// --- datasets ------------------------------------------------------------------

// PutDataset adds a dataset or, with replace, replaces one of the same
// collaborator. It fails with ErrExists when the room has the dataset (or
// one of the same source) and replace is false, and with ErrNotFound when
// replace is true and it has not.
func (s *Store) PutDataset(ctx context.Context, d Dataset, replace bool) error {
	policy, err := json.Marshal(d.Policy)
	if err != nil {
		return err
	}
	var tag pgconn.CommandTag
	if replace {
		tag, err = s.pool.Exec(ctx, `UPDATE igloo_cleanroom.datasets SET source_schema = $4, source_table = $5, kind = $6,
			policy = $7, identity = $8, updated_at = now() WHERE room = $1 AND name = $2 AND collaborator = $3`,
			d.Room, d.Name, d.Collaborator, d.Schema, d.Table, d.Kind, policy, d.Identity)
		if err == nil && tag.RowsAffected() == 0 {
			return ErrNotFound
		}
	} else {
		_, err = s.pool.Exec(ctx, `INSERT INTO igloo_cleanroom.datasets
			(room, name, collaborator, source_schema, source_table, kind, policy, identity, added_by)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			d.Room, d.Name, d.Collaborator, d.Schema, d.Table, d.Kind, policy, d.Identity, d.AddedBy)
	}
	switch pgCode(err) {
	case "23505":
		return ErrExists
	case "23503":
		return ErrNotFound
	}
	return err
}

func (s *Store) RemoveDataset(ctx context.Context, room, name string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM igloo_cleanroom.datasets WHERE room = $1 AND name = $2`, room, name)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

// Datasets lists a room's datasets, or every room's when room is empty.
func (s *Store) Datasets(ctx context.Context, room string) ([]*Dataset, error) {
	rows, err := s.pool.Query(ctx, `SELECT room, name, collaborator, source_schema, source_table, kind, identity, policy,
		added_by, added_at, updated_at FROM igloo_cleanroom.datasets WHERE $1 = '' OR room = $1 ORDER BY room, name`, room)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*Dataset, error) {
		var (
			d      Dataset
			policy []byte
		)
		if err := row.Scan(&d.Room, &d.Name, &d.Collaborator, &d.Schema, &d.Table, &d.Kind, &d.Identity, &policy,
			&d.AddedBy, &d.AddedAt, &d.UpdatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(policy, &d.Policy); err != nil {
			return nil, err
		}
		return &d, nil
	})
}

// --- templates and approvals ---------------------------------------------------

// PutTemplate stores a new version of a template: with replace the
// version after the current one, and otherwise the one after any the room
// had of that name, removed or not, so no version number is used twice. It
// returns the version.
func (s *Store) PutTemplate(ctx context.Context, t Template, replace bool) (int, error) {
	params, err := json.Marshal(t.Parameters)
	if err != nil {
		return 0, err
	}
	var version int
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT 1 FROM igloo_cleanroom.rooms WHERE name = $1 FOR UPDATE`, t.Room); err != nil {
			return err
		}
		var current, last int
		if err := tx.QueryRow(ctx, `SELECT coalesce(max(version), 0) FROM igloo_cleanroom.templates WHERE room = $1 AND name = $2`,
			t.Room, t.Name).Scan(&current); err != nil {
			return err
		}
		switch {
		case current > 0 && !replace:
			return ErrExists
		case current == 0 && replace:
			return ErrNotFound
		}
		err := tx.QueryRow(ctx, `SELECT version FROM igloo_cleanroom.template_versions WHERE room = $1 AND name = $2`, t.Room, t.Name).Scan(&last)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		version = max(current, last) + 1
		_, err = tx.Exec(ctx, `INSERT INTO igloo_cleanroom.templates (room, name, version, sql, parameters, run_by, builtin, digest, created_by)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`, t.Room, t.Name, version, t.SQL, params, t.RunBy, t.Builtin, t.digest(), t.CreatedBy)
		if pgCode(err) == "23503" {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO igloo_cleanroom.template_versions (room, name, version) VALUES ($1, $2, $3)
			ON CONFLICT (room, name) DO UPDATE SET version = excluded.version`, t.Room, t.Name, version)
		return err
	})
	return version, err
}

// RemoveTemplate removes every version of a template, with their
// approvals.
func (s *Store) RemoveTemplate(ctx context.Context, room, name string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM igloo_cleanroom.templates WHERE room = $1 AND name = $2`, room, name)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

// Templates lists the current version of a room's templates.
func (s *Store) Templates(ctx context.Context, room string) ([]Template, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT ON (name) room, name, version, sql, parameters, run_by, builtin, digest, created_by, created_at
		FROM igloo_cleanroom.templates WHERE room = $1 ORDER BY name, version DESC`, room)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Template, error) {
		var (
			t      Template
			params []byte
		)
		if err := row.Scan(&t.Room, &t.Name, &t.Version, &t.SQL, &params, &t.RunBy, &t.Builtin, &t.Digest, &t.CreatedBy, &t.CreatedAt); err != nil {
			return t, err
		}
		return t, json.Unmarshal(params, &t.Parameters)
	})
}

// Approve records an approval of a template's version, which must be its
// current one, with its digest (see approves); an approval of the version
// before, of datasets since changed, is replaced.
func (s *Store) Approve(ctx context.Context, a Approval) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var current int
		err := tx.QueryRow(ctx, `SELECT version FROM igloo_cleanroom.templates WHERE room = $1 AND name = $2
			ORDER BY version DESC LIMIT 1 FOR SHARE`, a.Room, a.Template).Scan(&current)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return ErrNotFound
		case err != nil:
			return err
		case current != a.Version:
			return invalid("template %s is at version %d, not %d: approve the version you reviewed, after reviewing the current one", a.Template, current, a.Version)
		}
		_, err = tx.Exec(ctx, `INSERT INTO igloo_cleanroom.approvals (room, template, version, digest, collaborator, approved_by)
			VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (room, template, version, collaborator)
			DO UPDATE SET digest = excluded.digest, approved_by = excluded.approved_by, approved_at = now()`,
			a.Room, a.Template, a.Version, a.Digest, a.Collaborator, a.ApprovedBy)
		return err
	})
}

// Unapprove withdraws a collaborator's approvals of every version of a
// template.
func (s *Store) Unapprove(ctx context.Context, room, template, collaborator string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM igloo_cleanroom.approvals WHERE room = $1 AND template = $2 AND collaborator = $3`,
		room, template, collaborator)
	return err
}

// Approvals lists a room's approvals, of every version.
func (s *Store) Approvals(ctx context.Context, room string) ([]Approval, error) {
	rows, err := s.pool.Query(ctx, `SELECT room, template, version, digest, collaborator, approved_by, approved_at
		FROM igloo_cleanroom.approvals WHERE room = $1 ORDER BY template, version, collaborator`, room)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Approval, error) {
		var a Approval
		err := row.Scan(&a.Room, &a.Template, &a.Version, &a.Digest, &a.Collaborator, &a.ApprovedBy, &a.ApprovedAt)
		return a, err
	})
}

// --- runs ----------------------------------------------------------------------

// budgetError refuses a run over a dataset's runs per day.
type budgetError struct{ msg string }

func (e *budgetError) Error() string { return e.msg }

// IsBudget reports whether err refuses a run for its requester's budget.
func IsBudget(err error) bool {
	var b *budgetError
	return errors.As(err, &b)
}

// BeginRun records a run as running, unless with it its requester would
// run templates reading one of its datasets more often in the last 24
// hours than the dataset allows, a run counting as often as its cost (one,
// or one per count it noises); runs of a room begin one at a time, so two
// cannot both take the last run of a budget. Runs older than retention are
// deleted.
func (s *Store) BeginRun(ctx context.Context, r *Run, budgets map[string]int, retention time.Duration) error {
	params, err := json.Marshal(r.Parameters)
	if err != nil {
		return err
	}
	r.Cost = max(1, r.Cost)
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT 1 FROM igloo_cleanroom.rooms WHERE name = $1 FOR UPDATE`, r.Room); err != nil {
			return err
		}
		for _, d := range r.Datasets {
			var n int
			if err := tx.QueryRow(ctx, `SELECT coalesce(sum(cost), 0) FROM igloo_cleanroom.runs WHERE room = $1 AND requester = $2
				AND $3 = ANY (datasets) AND started_at > now() - interval '24 hours'`, r.Room, r.Requester, d).Scan(&n); err != nil {
				return err
			}
			if limit := budgets[d]; n+r.Cost > limit {
				return &budgetError{msg: fmt.Sprintf("collaborator %s ran templates that read dataset %s %d times in the last 24 hours "+
					"(a run with noise counting once per count it noises), which with this run's %d is more than its policy allows (RUNS PER DAY %d); try again later",
					r.Requester, d, n, r.Cost, limit)}
			}
		}
		if retention > 0 {
			if _, err := tx.Exec(ctx, `DELETE FROM igloo_cleanroom.runs WHERE room = $1 AND started_at < now() - make_interval(secs => $2)`,
				r.Room, retention.Seconds()); err != nil {
				return err
			}
		}
		return tx.QueryRow(ctx, `INSERT INTO igloo_cleanroom.runs (room, template, version, requester, requested_by, parameters,
			approvals, datasets, status, noise, cost, result_table) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) RETURNING id, started_at`,
			r.Room, r.Template, r.Version, r.Requester, r.RequestedBy, params, r.Approvals, r.Datasets, RunRunning, r.Noise, r.Cost, r.ResultTable,
		).Scan(&r.ID, &r.Started)
	})
}

// FinishRun records how a run ended.
func (s *Store) FinishRun(ctx context.Context, r *Run) error {
	var columns, result []byte
	if r.Columns != nil {
		var err error
		if columns, err = json.Marshal(r.Columns); err != nil {
			return err
		}
		if result, err = json.Marshal(r.Result); err != nil {
			return err
		}
	}
	return s.pool.QueryRow(ctx, `UPDATE igloo_cleanroom.runs SET status = $2, error = $3, rows = $4, truncated = $5,
		columns = $6, result = $7, finished_at = now() WHERE id = $1 RETURNING finished_at`,
		r.ID, r.Status, r.Error, r.Rows, r.Truncated, columns, result).Scan(&r.Finished)
}

const runColumns = `id, room, template, version, requester, requested_by, parameters, approvals, datasets, status, error,
	rows, truncated, noise, cost, result_table, started_at, finished_at`

func scanRun(row pgx.Row, extra ...any) (Run, error) {
	var (
		r      Run
		params []byte
	)
	dest := append([]any{&r.ID, &r.Room, &r.Template, &r.Version, &r.Requester, &r.RequestedBy, &params, &r.Approvals,
		&r.Datasets, &r.Status, &r.Error, &r.Rows, &r.Truncated, &r.Noise, &r.Cost, &r.ResultTable, &r.Started, &r.Finished}, extra...)
	if err := row.Scan(dest...); err != nil {
		return r, err
	}
	dec := json.NewDecoder(bytes.NewReader(params))
	dec.UseNumber()
	return r, dec.Decode(&r.Parameters)
}

// Runs lists a room's runs, newest first, without their results.
func (s *Store) Runs(ctx context.Context, room string, limit int) ([]Run, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+runColumns+` FROM igloo_cleanroom.runs WHERE room = $1 ORDER BY id DESC LIMIT $2`, room, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Run returns a run of room with its result.
func (s *Store) Run(ctx context.Context, room string, id int64) (Run, error) {
	var columns, result []byte
	r, err := scanRun(s.pool.QueryRow(ctx, `SELECT `+runColumns+`, columns, result FROM igloo_cleanroom.runs WHERE room = $1 AND id = $2`,
		room, id), &columns, &result)
	if err != nil {
		return r, notFound(err)
	}
	if columns != nil {
		if err := json.Unmarshal(columns, &r.Columns); err != nil {
			return r, err
		}
		if err := json.Unmarshal(result, &r.Result); err != nil {
			return r, err
		}
	}
	return r, nil
}
