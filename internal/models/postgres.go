package models

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// migrationKey serializes creating the tables across replicas.
const migrationKey int64 = 0x49676c6f6f4d6c4d // "IglooMlM"

var migrations = []string{
	`CREATE SCHEMA IF NOT EXISTS igloo_ml`,
	// A model's row; its blob is in the warehouse, named by blob_sha256.
	// Names are kept in lower case.
	`CREATE TABLE IF NOT EXISTS igloo_ml.models (
		schema_name  text NOT NULL,
		name         text NOT NULL,
		class        text NOT NULL,
		owner        text NOT NULL,
		comment      text NOT NULL DEFAULT '',
		training_sql text NOT NULL,
		config       jsonb NOT NULL DEFAULT '{}',
		metrics      jsonb NOT NULL DEFAULT '{}',
		blob_sha256  text NOT NULL,
		blob_size    bigint NOT NULL,
		created_at   timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (schema_name, name)
	)`,
	`CREATE INDEX IF NOT EXISTS models_blob ON igloo_ml.models (blob_sha256)`,
	// What a model's training read, for erasure requests (see Model):
	// tables NULL for a model that did not record them.
	`ALTER TABLE igloo_ml.models ADD COLUMN IF NOT EXISTS tables text[]`,
	`ALTER TABLE igloo_ml.models ADD COLUMN IF NOT EXISTS epoch bigint NOT NULL DEFAULT 0`,
	`ALTER TABLE igloo_ml.models ADD COLUMN IF NOT EXISTS search_path text NOT NULL DEFAULT ''`,
	`ALTER TABLE igloo_ml.models ADD COLUMN IF NOT EXISTS sql_dialect text NOT NULL DEFAULT ''`,
	// The erasure epoch: a Put holds its row FOR SHARE, and BumpEpoch
	// updates it, so the two never interleave.
	`CREATE TABLE IF NOT EXISTS igloo_ml.state (id int PRIMARY KEY CHECK (id = 1), epoch bigint NOT NULL)`,
	`INSERT INTO igloo_ml.state (id, epoch) VALUES (1, 1) ON CONFLICT DO NOTHING`,
	`CREATE TABLE IF NOT EXISTS igloo_ml.grants (
		schema_name  text NOT NULL,
		name         text NOT NULL,
		grantee      text NOT NULL,
		grantee_type text NOT NULL CHECK (grantee_type IN ('user', 'role')),
		privilege    text NOT NULL CHECK (privilege IN ('USAGE', 'PREDICT')),
		granted_by   text NOT NULL DEFAULT '',
		granted_at   timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (schema_name, name, grantee_type, grantee, privilege),
		FOREIGN KEY (schema_name, name) REFERENCES igloo_ml.models (schema_name, name) ON DELETE CASCADE
	)`,
}

// PostgresStore keeps models in the "igloo_ml" schema of the metadata
// database, and locks blobs with session-level advisory locks, which
// Postgres releases if the replica holding them dies.
type PostgresStore struct {
	pool *pgxpool.Pool
	// locks holds a connection for each blob lock held. It is a pool of
	// its own, so callers holding locks always find a connection in pool
	// for the models.
	locks *pgxpool.Pool
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
	lockCfg := cfg.Copy()
	lockCfg.MaxConns = 16
	lockCfg.MaxConnIdleTime = time.Minute
	locks, err := pgxpool.NewWithConfig(ctx, lockCfg)
	if err != nil {
		pool.Close()
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
		locks.Close()
		return nil, fmt.Errorf("create model tables: %w", err)
	}
	return &PostgresStore{pool: pool, locks: locks}, nil
}

const modelColumns = `schema_name, name, class, owner, comment, training_sql, config::text, metrics::text, blob_sha256, blob_size, created_at,
	tables, epoch, search_path, sql_dialect`

func scanModel(row pgx.Row) (Model, error) {
	var m Model
	err := row.Scan(&m.Schema, &m.Name, &m.Class, &m.Owner, &m.Comment, &m.TrainingSQL, &m.Config, &m.Metrics, &m.Blob, &m.BlobSize, &m.CreatedAt,
		&m.Tables, &m.Epoch, &m.SearchPath, &m.Dialect)
	if errors.Is(err, pgx.ErrNoRows) {
		return m, ErrNotFound
	}
	return m, err
}

func (s *PostgresStore) Get(ctx context.Context, schema, name string) (Model, error) {
	return scanModel(s.pool.QueryRow(ctx, `SELECT `+modelColumns+` FROM igloo_ml.models WHERE schema_name = $1 AND name = $2`,
		strings.ToLower(schema), strings.ToLower(name)))
}

func (s *PostgresStore) List(ctx context.Context) ([]Model, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+modelColumns+` FROM igloo_ml.models ORDER BY schema_name, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Model
	for rows.Next() {
		m, err := scanModel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func jsonOrEmpty(s string) string {
	if s == "" {
		return "{}"
	}
	return s
}

func (s *PostgresStore) Put(ctx context.Context, m Model, replace bool) (*Model, error) {
	m.Schema, m.Name = strings.ToLower(m.Schema), strings.ToLower(m.Name)
	var old *Model
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var epoch int64
		if err := tx.QueryRow(ctx, `SELECT epoch FROM igloo_ml.state WHERE id = 1 FOR SHARE`).Scan(&epoch); err != nil {
			return err
		}
		if m.Epoch != 0 && m.Epoch != epoch {
			return ErrStale
		}
		prev, err := scanModel(tx.QueryRow(ctx, `SELECT `+modelColumns+` FROM igloo_ml.models WHERE schema_name = $1 AND name = $2 FOR UPDATE`,
			m.Schema, m.Name))
		switch {
		case err == nil && !replace:
			return ErrExists
		case err == nil && m.IfBlob != "" && prev.Blob != m.IfBlob:
			return ErrChanged
		case err == nil:
			old = &prev
			_, err = tx.Exec(ctx, `UPDATE igloo_ml.models SET class = $3, owner = $4, comment = $5, training_sql = $6, config = $7::jsonb,
				metrics = $8::jsonb, blob_sha256 = $9, blob_size = $10, created_at = now(), tables = $11, epoch = $12, search_path = $13,
				sql_dialect = $14 WHERE schema_name = $1 AND name = $2`,
				m.Schema, m.Name, m.Class, m.Owner, m.Comment, m.TrainingSQL, jsonOrEmpty(m.Config), jsonOrEmpty(m.Metrics), m.Blob, m.BlobSize,
				m.Tables, m.Epoch, m.SearchPath, m.Dialect)
			return err
		case errors.Is(err, ErrNotFound):
			if m.IfBlob != "" {
				return ErrChanged
			}
			_, err = tx.Exec(ctx, `INSERT INTO igloo_ml.models (schema_name, name, class, owner, comment, training_sql, config, metrics,
				blob_sha256, blob_size, tables, epoch, search_path, sql_dialect)
				VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10, $11, $12, $13, $14)`,
				m.Schema, m.Name, m.Class, m.Owner, m.Comment, m.TrainingSQL, jsonOrEmpty(m.Config), jsonOrEmpty(m.Metrics), m.Blob, m.BlobSize,
				m.Tables, m.Epoch, m.SearchPath, m.Dialect)
			return err
		}
		return err
	})
	return old, err
}

func (s *PostgresStore) Epoch(ctx context.Context) (int64, error) {
	var epoch int64
	err := s.pool.QueryRow(ctx, `SELECT epoch FROM igloo_ml.state WHERE id = 1`).Scan(&epoch)
	return epoch, err
}

func (s *PostgresStore) BumpEpoch(ctx context.Context) (int64, error) {
	var epoch int64
	err := s.pool.QueryRow(ctx, `UPDATE igloo_ml.state SET epoch = epoch + 1 WHERE id = 1 RETURNING epoch`).Scan(&epoch)
	return epoch, err
}

func (s *PostgresStore) Delete(ctx context.Context, schema, name string) (Model, error) {
	return scanModel(s.pool.QueryRow(ctx, `DELETE FROM igloo_ml.models WHERE schema_name = $1 AND name = $2 RETURNING `+modelColumns,
		strings.ToLower(schema), strings.ToLower(name)))
}

func (s *PostgresStore) BlobUsed(ctx context.Context, sha string) (bool, error) {
	var used bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM igloo_ml.models WHERE blob_sha256 = $1)`, sha).Scan(&used)
	return used, err
}

// blobLockKey is the advisory lock key of blob sha.
func blobLockKey(sha string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("igloo/model-blob/" + sha))
	return int64(h.Sum64())
}

func (s *PostgresStore) LockBlob(ctx context.Context, sha string) (func(), error) {
	conn, err := s.locks.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	key := blobLockKey(sha)
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", key); err != nil {
		// A wait cut short may still be granted before the server sees
		// the cancellation; closing the connection ends its session, and
		// with it any lock it got.
		_ = conn.Conn().Close(context.Background())
		conn.Release()
		return nil, err
	}
	return func() {
		// Unlock even when ctx was canceled. If this fails the connection
		// is closed instead, which also releases the lock.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", key); err != nil {
			_ = conn.Conn().Close(ctx)
		}
		conn.Release()
	}, nil
}

func (s *PostgresStore) Grants(ctx context.Context) ([]Grant, error) {
	rows, err := s.pool.Query(ctx, `SELECT schema_name, name, grantee, grantee_type, privilege, granted_by, granted_at
		FROM igloo_ml.grants ORDER BY schema_name, name, grantee_type, grantee, privilege`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Grant
	for rows.Next() {
		var g Grant
		if err := rows.Scan(&g.Schema, &g.Name, &g.Grantee, &g.GranteeType, &g.Privilege, &g.GrantedBy, &g.GrantedAt); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

func (s *PostgresStore) AddGrant(ctx context.Context, g Grant) error {
	tag, err := s.pool.Exec(ctx, `INSERT INTO igloo_ml.grants (schema_name, name, grantee, grantee_type, privilege, granted_by)
		SELECT schema_name, name, $3, $4, $5, $6 FROM igloo_ml.models WHERE schema_name = $1 AND name = $2
		ON CONFLICT DO NOTHING`,
		strings.ToLower(g.Schema), strings.ToLower(g.Name), g.Grantee, g.GranteeType, g.Privilege, g.GrantedBy)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		if _, err := s.Get(ctx, g.Schema, g.Name); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresStore) RevokeGrant(ctx context.Context, g Grant) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM igloo_ml.grants WHERE schema_name = $1 AND name = $2 AND grantee = $3 AND grantee_type = $4
		AND privilege = $5`, strings.ToLower(g.Schema), strings.ToLower(g.Name), g.Grantee, g.GranteeType, g.Privilege)
	return err
}

func (s *PostgresStore) ForgetGrantee(ctx context.Context, granteeType, grantee string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM igloo_ml.grants WHERE grantee_type = $1 AND grantee = $2`, granteeType, grantee)
	return err
}

func (s *PostgresStore) DisownModels(ctx context.Context, owner, tombstone string) error {
	_, err := s.pool.Exec(ctx, `UPDATE igloo_ml.models SET owner = $2 WHERE owner = $1`, owner, tombstone)
	return err
}

func (s *PostgresStore) ForgetDropped(ctx context.Context, principals func(context.Context) (Principals, error)) (int, int, error) {
	// The database's clock, which stamps models, grants, users and roles.
	var asked time.Time
	if err := s.pool.QueryRow(ctx, `SELECT now()`).Scan(&asked); err != nil {
		return 0, 0, err
	}
	p, err := principals(ctx)
	if err != nil {
		return 0, 0, err
	}
	users, usersCreated := namesAndTimes(p.Users)
	roles, rolesCreated := namesAndTimes(p.Roles)
	tag, err := s.pool.Exec(ctx, `UPDATE igloo_ml.models m SET owner = owner || $4
		WHERE created_at < $1 AND owner NOT LIKE ('%' || $4) AND NOT EXISTS (
			SELECT 1 FROM unnest($2::text[], $3::timestamptz[]) u (name, created_at) WHERE u.name = m.owner AND u.created_at <= m.created_at)`,
		asked, users, usersCreated, droppedSuffix)
	if err != nil {
		return 0, 0, err
	}
	disowned := int(tag.RowsAffected())
	tag, err = s.pool.Exec(ctx, `WITH p (type, name, created_at) AS (
			SELECT 'user', * FROM unnest($2::text[], $3::timestamptz[])
			UNION ALL SELECT 'role', * FROM unnest($4::text[], $5::timestamptz[]))
		DELETE FROM igloo_ml.grants g WHERE granted_at < $1 AND NOT EXISTS (
			SELECT 1 FROM p WHERE p.type = g.grantee_type AND p.name = g.grantee AND p.created_at <= g.granted_at)`,
		asked, users, usersCreated, roles, rolesCreated)
	if err != nil {
		return disowned, 0, err
	}
	return disowned, int(tag.RowsAffected()), nil
}

func namesAndTimes(m map[string]time.Time) ([]string, []time.Time) {
	names, times := make([]string, 0, len(m)), make([]time.Time, 0, len(m))
	for name, t := range m {
		names, times = append(names, name), append(times, t)
	}
	return names, times
}

func (s *PostgresStore) Close() {
	s.pool.Close()
	s.locks.Close()
}
