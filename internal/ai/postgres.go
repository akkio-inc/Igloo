package ai

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// KeyName names the key in igloo_auth.secrets that encrypts AI
// integrations' keys. Every replica shares it.
const KeyName = "ai-secrets"

// migrationKey serializes creating the tables across replicas.
const migrationKey int64 = 0x49676c6f6f414921 // "IglooAI!"

var migrations = []string{
	`CREATE SCHEMA IF NOT EXISTS igloo_ai`,
	// sealed holds the keys, encrypted; settings the rest, as JSON.
	`CREATE TABLE IF NOT EXISTS igloo_ai.integrations (
		name       text PRIMARY KEY,
		provider   text NOT NULL,
		enabled    boolean NOT NULL,
		sealed     bytea,
		settings   jsonb NOT NULL DEFAULT '{}',
		comment    text NOT NULL DEFAULT '',
		created_by text NOT NULL DEFAULT '',
		created_at timestamptz NOT NULL DEFAULT now()
	)`,
	`CREATE TABLE IF NOT EXISTS igloo_ai.usage (
		integration  text NOT NULL REFERENCES igloo_ai.integrations (name) ON DELETE CASCADE,
		grantee      text NOT NULL,
		grantee_type text NOT NULL CHECK (grantee_type IN ('user', 'role')),
		granted_by   text NOT NULL DEFAULT '',
		granted_at   timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (integration, grantee_type, grantee)
	)`,
	// One row: the cache's erasure epoch, and the version of the
	// integrations and grants, which replicas reload when it changes.
	`CREATE TABLE IF NOT EXISTS igloo_ai.state (
		id      integer PRIMARY KEY CHECK (id = 1),
		epoch   bigint NOT NULL,
		version bigint NOT NULL
	)`,
	`INSERT INTO igloo_ai.state (id, epoch, version) VALUES (1, 1, 1) ON CONFLICT (id) DO NOTHING`,
	`CREATE TABLE IF NOT EXISTS igloo_ai.cache (
		key        text PRIMARY KEY,
		function   text NOT NULL,
		model      text NOT NULL,
		value      bytea NOT NULL,
		tables     text[] NOT NULL,
		epoch      bigint NOT NULL,
		created_at timestamptz NOT NULL,
		expires_at timestamptz NOT NULL
	)`,
	`CREATE INDEX IF NOT EXISTS cache_tables ON igloo_ai.cache USING gin (tables)`,
	`CREATE INDEX IF NOT EXISTS cache_expires ON igloo_ai.cache (expires_at)`,
	// Each integration's ID (Integration.ID), which CREATE OR REPLACE
	// keeps, as it updates the row. Added once, so later starts take no
	// lock on the table; each existing integration gets one of its own.
	`DO $$
	BEGIN
		IF NOT EXISTS (SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'igloo_ai' AND table_name = 'integrations' AND column_name = 'id') THEN
			ALTER TABLE igloo_ai.integrations ADD COLUMN IF NOT EXISTS id text NOT NULL DEFAULT gen_random_uuid()::text;
		END IF;
	END $$`,
}

// PostgresStore keeps everything in the "igloo_ai" schema of the metadata
// database, shared by every replica.
type PostgresStore struct {
	pool   *pgxpool.Pool
	sealer *sealer
}

// OpenPostgres connects to url, creates the tables if needed, and encrypts
// keys with key, at least 32 random bytes.
func OpenPostgres(ctx context.Context, url string, key []byte) (*PostgresStore, error) {
	sl, err := newSealer(key)
	if err != nil {
		return nil, err
	}
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
		return nil, fmt.Errorf("create AI tables: %w", err)
	}
	return &PostgresStore{pool: pool, sealer: sl}, nil
}

// sealedParts are what is encrypted.
type sealedParts struct {
	APIKey             string            `json:"api_key,omitempty"`
	Headers            map[string]string `json:"headers,omitempty"`
	AWSAccessKeyID     string            `json:"aws_access_key_id,omitempty"`
	AWSSecretAccessKey string            `json:"aws_secret_access_key,omitempty"`
	AWSSessionToken    string            `json:"aws_session_token,omitempty"`
	ServiceAccount     string            `json:"service_account,omitempty"`
}

// settings are the rest.
type settings struct {
	BaseURL        string            `json:"base_url,omitempty"`
	APIVersion     string            `json:"api_version,omitempty"`
	Region         string            `json:"region,omitempty"`
	Project        string            `json:"project,omitempty"`
	Models         map[string]string `json:"models,omitempty"`
	DefaultModel   string            `json:"default_model,omitempty"`
	EmbeddingModel string            `json:"embedding_model,omitempty"`
	DecisionModel  string            `json:"decision_model,omitempty"`
	Prices         map[string]Price  `json:"prices,omitempty"`
	MaxConcurrency int               `json:"max_concurrency,omitempty"`
}

func (s *PostgresStore) encode(in Integration) (sealed []byte, set []byte, err error) {
	plain, err := json.Marshal(sealedParts{APIKey: in.APIKey, Headers: in.Headers, AWSAccessKeyID: in.AWSAccessKeyID,
		AWSSecretAccessKey: in.AWSSecretAccessKey, AWSSessionToken: in.AWSSessionToken, ServiceAccount: in.ServiceAccount})
	if err != nil {
		return nil, nil, err
	}
	set, err = json.Marshal(settings{BaseURL: in.BaseURL, APIVersion: in.APIVersion, Region: in.Region, Project: in.Project,
		Models: in.Models, DefaultModel: in.DefaultModel, EmbeddingModel: in.EmbeddingModel, DecisionModel: in.DecisionModel,
		Prices: in.Prices, MaxConcurrency: in.MaxConcurrency})
	return s.sealer.seal(in.Name, plain), set, err
}

const integrationColumns = `name, id, provider, enabled, sealed, settings, comment, created_by, created_at`

func (s *PostgresStore) scan(row pgx.Row) (Integration, error) {
	var (
		in     Integration
		sealed []byte
		set    []byte
	)
	if err := row.Scan(&in.Name, &in.ID, &in.Provider, &in.Enabled, &sealed, &set, &in.Comment, &in.CreatedBy, &in.CreatedAt); err != nil {
		return in, err
	}
	var st settings
	if err := json.Unmarshal(set, &st); err != nil {
		return in, fmt.Errorf("AI integration %s: %w", in.Name, err)
	}
	in.BaseURL, in.APIVersion, in.Region, in.Project = st.BaseURL, st.APIVersion, st.Region, st.Project
	in.Models, in.DefaultModel, in.EmbeddingModel, in.DecisionModel = st.Models, st.DefaultModel, st.EmbeddingModel, st.DecisionModel
	in.Prices, in.MaxConcurrency = st.Prices, st.MaxConcurrency
	if len(sealed) > 0 {
		plain, err := s.sealer.open(in.Name, sealed)
		if err != nil {
			return in, fmt.Errorf("AI integration %s: %w", in.Name, err)
		}
		var p sealedParts
		if err := json.Unmarshal(plain, &p); err != nil {
			return in, fmt.Errorf("AI integration %s: %w", in.Name, err)
		}
		in.APIKey, in.Headers, in.AWSAccessKeyID, in.AWSSecretAccessKey = p.APIKey, p.Headers, p.AWSAccessKeyID, p.AWSSecretAccessKey
		in.AWSSessionToken, in.ServiceAccount = p.AWSSessionToken, p.ServiceAccount
	}
	return in, nil
}

// changed moves the version on, in tx.
func changed(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `UPDATE igloo_ai.state SET version = version + 1 WHERE id = 1`)
	return err
}

func (s *PostgresStore) CreateIntegration(ctx context.Context, in Integration, replace bool) error {
	sealed, set, err := s.encode(in)
	if err != nil {
		return err
	}
	q := `INSERT INTO igloo_ai.integrations (name, provider, enabled, sealed, settings, comment, created_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`
	if replace {
		q += ` ON CONFLICT (name) DO UPDATE SET provider = excluded.provider, enabled = excluded.enabled, sealed = excluded.sealed,
			settings = excluded.settings, comment = excluded.comment, created_by = excluded.created_by, created_at = now()`
	}
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, q, in.Name, in.Provider, in.Enabled, sealed, set, in.Comment, in.CreatedBy); err != nil {
			return err
		}
		return changed(ctx, tx)
	})
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return ErrExists
	}
	return err
}

func (s *PostgresStore) Integration(ctx context.Context, name string) (Integration, error) {
	in, err := s.scan(s.pool.QueryRow(ctx, `SELECT `+integrationColumns+` FROM igloo_ai.integrations WHERE name = $1`, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return Integration{}, ErrNotFound
	}
	return in, err
}

func (s *PostgresStore) Integrations(ctx context.Context) ([]Integration, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+integrationColumns+` FROM igloo_ai.integrations ORDER BY name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Integration, error) { return s.scan(row) })
}

func (s *PostgresStore) UpdateIntegration(ctx context.Context, in Integration) error {
	sealed, set, err := s.encode(in)
	if err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE igloo_ai.integrations SET enabled = $2, sealed = $3, settings = $4, comment = $5
			WHERE name = $1`, in.Name, in.Enabled, sealed, set, in.Comment)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return changed(ctx, tx)
	})
}

func (s *PostgresStore) DropIntegration(ctx context.Context, name string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `DELETE FROM igloo_ai.integrations WHERE name = $1`, name)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return changed(ctx, tx)
	})
}

func (s *PostgresStore) GrantUsage(ctx context.Context, u Grant) error {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO igloo_ai.usage (integration, grantee, grantee_type, granted_by)
			VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`, u.Integration, u.Grantee, u.GranteeType, u.GrantedBy); err != nil {
			return err
		}
		return changed(ctx, tx)
	})
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23503" {
		return ErrNotFound
	}
	return err
}

func (s *PostgresStore) RevokeUsage(ctx context.Context, integration, granteeType, grantee string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM igloo_ai.usage WHERE integration = $1 AND grantee_type = $2 AND grantee = $3`,
			integration, granteeType, grantee); err != nil {
			return err
		}
		return changed(ctx, tx)
	})
}

func (s *PostgresStore) Grants(ctx context.Context) ([]Grant, error) {
	rows, err := s.pool.Query(ctx, `SELECT integration, grantee, grantee_type, granted_by, granted_at
		FROM igloo_ai.usage ORDER BY integration, grantee_type, grantee`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Grant, error) {
		var u Grant
		err := row.Scan(&u.Integration, &u.Grantee, &u.GranteeType, &u.GrantedBy, &u.GrantedAt)
		return u, err
	})
}

func (s *PostgresStore) ForgetGrantee(ctx context.Context, granteeType, grantee string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM igloo_ai.usage WHERE grantee_type = $1 AND grantee = $2`, granteeType, grantee); err != nil {
			return err
		}
		return changed(ctx, tx)
	})
}

func (s *PostgresStore) State(ctx context.Context) (epoch, version int64, err error) {
	err = s.pool.QueryRow(ctx, `SELECT epoch, version FROM igloo_ai.state WHERE id = 1`).Scan(&epoch, &version)
	return epoch, version, err
}

func (s *PostgresStore) Lookup(ctx context.Context, keys []string, now time.Time) (map[string]CacheEntry, error) {
	out := map[string]CacheEntry{}
	if len(keys) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, `SELECT key, function, model, value, tables, epoch, created_at, expires_at
		FROM igloo_ai.cache WHERE key = ANY($1) AND expires_at > $2`, keys, now)
	if err != nil {
		return nil, err
	}
	entries, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (CacheEntry, error) {
		var e CacheEntry
		err := row.Scan(&e.Key, &e.Function, &e.Model, &e.Value, &e.Tables, &e.Epoch, &e.Created, &e.Expires)
		return e, err
	})
	for _, e := range entries {
		out[e.Key] = e
	}
	return out, err
}

// Put holds the state's row FOR SHARE while it inserts, and Purge updates
// it: one waits for the other, so a purge never misses what a statement
// begun before it puts, and a statement never puts after a purge.
func (s *PostgresStore) Put(ctx context.Context, epoch int64, entries []CacheEntry) (bool, error) {
	if len(entries) == 0 {
		return true, nil
	}
	kept := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var current int64
		if err := tx.QueryRow(ctx, `SELECT epoch FROM igloo_ai.state WHERE id = 1 FOR SHARE`).Scan(&current); err != nil {
			return err
		}
		if current != epoch {
			return nil
		}
		batch := &pgx.Batch{}
		for _, e := range entries {
			batch.Queue(`INSERT INTO igloo_ai.cache (key, function, model, value, tables, epoch, created_at, expires_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
				ON CONFLICT (key) DO UPDATE SET value = excluded.value, created_at = excluded.created_at,
					expires_at = excluded.expires_at, epoch = least(cache.epoch, excluded.epoch),
					tables = ARRAY(SELECT DISTINCT unnest(cache.tables || excluded.tables))`,
				e.Key, e.Function, e.Model, e.Value, nonNil(e.Tables), e.Epoch, e.Created, e.Expires)
		}
		if err := tx.SendBatch(ctx, batch).Close(); err != nil {
			return err
		}
		kept = true
		return nil
	})
	return kept, err
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func lowered(tables []string) []string {
	out := []string{AnyTable}
	for _, t := range tables {
		out = append(out, strings.ToLower(t))
	}
	return out
}

func (s *PostgresStore) Purge(ctx context.Context, tables []string) (deleted int64, err error) {
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE igloo_ai.state SET epoch = epoch + 1 WHERE id = 1`); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `DELETE FROM igloo_ai.cache WHERE tables && $1::text[]`, lowered(tables))
		deleted = tag.RowsAffected()
		return err
	})
	return deleted, err
}

func (s *PostgresStore) Remaining(ctx context.Context, tables []string) (n int64, err error) {
	err = s.pool.QueryRow(ctx, `SELECT count(*) FROM igloo_ai.cache
		WHERE tables && $1::text[] AND epoch < (SELECT epoch FROM igloo_ai.state WHERE id = 1)`,
		lowered(tables)).Scan(&n)
	return n, err
}

func (s *PostgresStore) Expire(ctx context.Context, before time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM igloo_ai.cache WHERE expires_at <= $1`, before)
	return tag.RowsAffected(), err
}

func (s *PostgresStore) Close() { s.pool.Close() }

// sealer encrypts integrations' keys with AES-256-GCM, the integration's
// name bound in as associated data, so keys copied to another
// integration's row do not decrypt there.
type sealer struct{ aead cipher.AEAD }

func newSealer(secret []byte) (*sealer, error) {
	if len(secret) < 32 {
		return nil, errors.New("ai: the secrets key must be at least 32 bytes")
	}
	key, err := hkdf.Key(sha256.New, secret, nil, "igloo ai integration v1", 32)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &sealer{aead: aead}, nil
}

func (s *sealer) seal(name string, plain []byte) []byte {
	nonce := make([]byte, s.aead.NonceSize(), s.aead.NonceSize()+len(plain)+s.aead.Overhead())
	_, _ = rand.Read(nonce)
	return s.aead.Seal(nonce, nonce, plain, []byte(name))
}

func (s *sealer) open(name string, sealed []byte) ([]byte, error) {
	n := s.aead.NonceSize()
	if len(sealed) < n {
		return nil, errors.New("its stored keys are damaged")
	}
	plain, err := s.aead.Open(nil, sealed[:n], sealed[n:], []byte(name))
	if err != nil {
		return nil, errors.New("its stored keys cannot be decrypted with this deployment's key")
	}
	return plain, nil
}
