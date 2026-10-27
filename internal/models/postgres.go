package models

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
// database.
type PostgresStore struct {
	pool *pgxpool.Pool
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
		return nil, fmt.Errorf("create model tables: %w", err)
	}
	return &PostgresStore{pool: pool}, nil
}

const modelColumns = `schema_name, name, class, owner, comment, training_sql, config::text, metrics::text, blob_sha256, blob_size, created_at`

func scanModel(row pgx.Row) (Model, error) {
	var m Model
	err := row.Scan(&m.Schema, &m.Name, &m.Class, &m.Owner, &m.Comment, &m.TrainingSQL, &m.Config, &m.Metrics, &m.Blob, &m.BlobSize, &m.CreatedAt)
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
		prev, err := scanModel(tx.QueryRow(ctx, `SELECT `+modelColumns+` FROM igloo_ml.models WHERE schema_name = $1 AND name = $2 FOR UPDATE`,
			m.Schema, m.Name))
		switch {
		case err == nil && !replace:
			return ErrExists
		case err == nil:
			old = &prev
			_, err = tx.Exec(ctx, `UPDATE igloo_ml.models SET class = $3, owner = $4, comment = $5, training_sql = $6, config = $7::jsonb,
				metrics = $8::jsonb, blob_sha256 = $9, blob_size = $10, created_at = now() WHERE schema_name = $1 AND name = $2`,
				m.Schema, m.Name, m.Class, m.Owner, m.Comment, m.TrainingSQL, jsonOrEmpty(m.Config), jsonOrEmpty(m.Metrics), m.Blob, m.BlobSize)
			return err
		case errors.Is(err, ErrNotFound):
			_, err = tx.Exec(ctx, `INSERT INTO igloo_ml.models (schema_name, name, class, owner, comment, training_sql, config, metrics,
				blob_sha256, blob_size) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10)`,
				m.Schema, m.Name, m.Class, m.Owner, m.Comment, m.TrainingSQL, jsonOrEmpty(m.Config), jsonOrEmpty(m.Metrics), m.Blob, m.BlobSize)
			return err
		}
		return err
	})
	return old, err
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

func (s *PostgresStore) Close() { s.pool.Close() }
