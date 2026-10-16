package notify

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
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/akkio-inc/igloo/internal/audit"
)

// KeyName names the key in igloo_auth.secrets that encrypts integrations'
// URLs, headers and signing secrets. Every replica shares it, like the
// console's session key.
const KeyName = "notification-secrets"

const (
	// migrationKey serializes creating the tables across replicas.
	migrationKey int64 = 0x49676c6f6f4e7466 // "IglooNtf"
	// pruneKey lets one replica at a time delete old history.
	pruneKey int64 = 0x49676c6f6f4e5072 // "IglooNPr"
)

var migrations = []string{
	`CREATE SCHEMA IF NOT EXISTS igloo_notify`,
	// sealed holds the URL, headers and signing secret, encrypted; target
	// is what SHOW INTEGRATIONS shows of where it sends.
	`CREATE TABLE IF NOT EXISTS igloo_notify.integrations (
		name               text PRIMARY KEY,
		type               text NOT NULL,
		enabled            boolean NOT NULL,
		sealed             bytea,
		target             text NOT NULL DEFAULT '',
		recipients         text[] NOT NULL DEFAULT '{}',
		operational_alerts text NOT NULL DEFAULT '',
		comment            text NOT NULL DEFAULT '',
		created_by         text NOT NULL DEFAULT '',
		created_at         timestamptz NOT NULL DEFAULT now()
	)`,
	`CREATE TABLE IF NOT EXISTS igloo_notify.usage (
		integration  text NOT NULL REFERENCES igloo_notify.integrations (name) ON DELETE CASCADE,
		grantee      text NOT NULL,
		grantee_type text NOT NULL CHECK (grantee_type IN ('user', 'role')),
		granted_by   text NOT NULL DEFAULT '',
		granted_at   timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (integration, grantee_type, grantee)
	)`,
	`CREATE INDEX IF NOT EXISTS usage_grantee ON igloo_notify.usage (grantee_type, grantee)`,
	`CREATE TABLE IF NOT EXISTS igloo_notify.alert_history (
		alert_id       text PRIMARY KEY,
		alert_type     text NOT NULL,
		alert_name     text NOT NULL DEFAULT '',
		user_name      text NOT NULL DEFAULT '',
		trigger        text NOT NULL,
		scheduled_time timestamptz,
		start_time     timestamptz NOT NULL,
		end_time       timestamptz,
		state          text NOT NULL,
		row_count      bigint,
		integration    text NOT NULL DEFAULT '',
		attempts       integer NOT NULL DEFAULT 0,
		error          text
	)`,
	`CREATE INDEX IF NOT EXISTS alert_history_time ON igloo_notify.alert_history (start_time DESC, alert_id DESC)`,
	`CREATE INDEX IF NOT EXISTS alert_history_user ON igloo_notify.alert_history (user_name, start_time DESC, alert_id DESC)`,
	// A built-in alert's problem is held, not sent again, until held_until.
	`CREATE TABLE IF NOT EXISTS igloo_notify.claims (
		key        text PRIMARY KEY,
		held_until timestamptz NOT NULL
	)`,
}

// PostgresStore keeps everything in the "igloo_notify" schema of the
// metadata database, shared by every replica.
type PostgresStore struct {
	pool   *pgxpool.Pool
	sealer *sealer
}

// OpenPostgres connects to url, creates the tables if needed, and encrypts
// secrets with key, at least 32 random bytes.
func OpenPostgres(ctx context.Context, url string, key []byte) (*PostgresStore, error) {
	sl, err := newSealer(key)
	if err != nil {
		return nil, err
	}
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
		return nil, fmt.Errorf("create notification tables: %w", err)
	}
	return &PostgresStore{pool: pool, sealer: sl}, nil
}

// secretParts are what is sealed.
type secretParts struct {
	URL     string            `json:"url,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Secret  string            `json:"secret,omitempty"`
}

func (s *PostgresStore) seal(in Integration) ([]byte, error) {
	plain, err := json.Marshal(secretParts{URL: in.URL, Headers: in.Headers, Secret: in.Secret})
	if err != nil {
		return nil, err
	}
	return s.sealer.seal(in.Name, plain), nil
}

const integrationColumns = `name, type, enabled, sealed, recipients, operational_alerts, comment, created_by, created_at`

func (s *PostgresStore) scan(row pgx.Row) (Integration, error) {
	var (
		in      Integration
		sealed  []byte
		builtin string
	)
	if err := row.Scan(&in.Name, &in.Type, &in.Enabled, &sealed, &in.Recipients, &builtin, &in.Comment, &in.CreatedBy,
		&in.CreatedAt); err != nil {
		return in, err
	}
	var err error
	if in.Builtin, err = ParseBuiltin(builtin); err != nil {
		return in, fmt.Errorf("integration %s: %w", in.Name, err)
	}
	if len(sealed) > 0 {
		plain, err := s.sealer.open(in.Name, sealed)
		if err != nil {
			return in, fmt.Errorf("integration %s: %w", in.Name, err)
		}
		var p secretParts
		if err := json.Unmarshal(plain, &p); err != nil {
			return in, fmt.Errorf("integration %s: %w", in.Name, err)
		}
		in.URL, in.Headers, in.Secret = p.URL, p.Headers, p.Secret
	}
	return in, nil
}

func (s *PostgresStore) CreateIntegration(ctx context.Context, in Integration, replace bool) error {
	sealed, err := s.seal(in)
	if err != nil {
		return err
	}
	q := `INSERT INTO igloo_notify.integrations (name, type, enabled, sealed, target, recipients, operational_alerts,
			comment, created_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	if replace {
		q += ` ON CONFLICT (name) DO UPDATE SET type = excluded.type, enabled = excluded.enabled, sealed = excluded.sealed,
			target = excluded.target, recipients = excluded.recipients, operational_alerts = excluded.operational_alerts,
			comment = excluded.comment, created_by = excluded.created_by, created_at = now()`
	}
	_, err = s.pool.Exec(ctx, q, in.Name, in.Type, in.Enabled, sealed, in.Target(), recipients(in), in.Builtin.String(),
		in.Comment, in.CreatedBy)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return ErrExists
	}
	return err
}

func recipients(in Integration) []string {
	if in.Recipients == nil {
		return []string{}
	}
	return in.Recipients
}

func (s *PostgresStore) Integration(ctx context.Context, name string) (Integration, error) {
	in, err := s.scan(s.pool.QueryRow(ctx, `SELECT `+integrationColumns+` FROM igloo_notify.integrations WHERE name = $1`, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return Integration{}, ErrNotFound
	}
	return in, err
}

func (s *PostgresStore) Integrations(ctx context.Context) ([]Integration, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+integrationColumns+` FROM igloo_notify.integrations ORDER BY name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Integration, error) { return s.scan(row) })
}

func (s *PostgresStore) UpdateIntegration(ctx context.Context, in Integration) error {
	sealed, err := s.seal(in)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE igloo_notify.integrations SET enabled = $2, sealed = $3, target = $4,
			recipients = $5, operational_alerts = $6, comment = $7
		WHERE name = $1`, in.Name, in.Enabled, sealed, in.Target(), recipients(in), in.Builtin.String(), in.Comment)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

func (s *PostgresStore) DropIntegration(ctx context.Context, name string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM igloo_notify.integrations WHERE name = $1`, name)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

func (s *PostgresStore) GrantUsage(ctx context.Context, u Usage) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO igloo_notify.usage (integration, grantee, grantee_type, granted_by)
		VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`, u.Integration, u.Grantee, u.GranteeType, u.GrantedBy)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23503" {
		return ErrNotFound
	}
	return err
}

func (s *PostgresStore) RevokeUsage(ctx context.Context, integration, granteeType, grantee string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM igloo_notify.usage WHERE integration = $1 AND grantee_type = $2 AND grantee = $3`,
		integration, granteeType, grantee)
	return err
}

func (s *PostgresStore) Usage(ctx context.Context) ([]Usage, error) {
	rows, err := s.pool.Query(ctx, `SELECT integration, grantee, grantee_type, granted_by, granted_at
		FROM igloo_notify.usage ORDER BY integration, grantee_type, grantee`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Usage, error) {
		var u Usage
		err := row.Scan(&u.Integration, &u.Grantee, &u.GranteeType, &u.GrantedBy, &u.GrantedAt)
		return u, err
	})
}

func (s *PostgresStore) ForgetGrantee(ctx context.Context, granteeType, grantee string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM igloo_notify.usage WHERE grantee_type = $1 AND grantee = $2`, granteeType, grantee)
	return err
}

func (s *PostgresStore) WriteRecord(ctx context.Context, r Record) error {
	var rows any
	if r.Rows >= 0 {
		rows = r.Rows
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO igloo_notify.alert_history (alert_id, alert_type, alert_name, user_name, trigger,
			scheduled_time, start_time, end_time, state, row_count, integration, attempts, error)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (alert_id) DO UPDATE SET end_time = excluded.end_time, state = excluded.state,
			row_count = excluded.row_count, integration = excluded.integration, attempts = excluded.attempts,
			error = excluded.error`,
		r.ID, r.Type, r.Name, r.User, r.Trigger, nullTime(r.Scheduled), r.Start, nullTime(r.End), r.State, rows,
		r.Integration, r.Attempts, nullable(r.Error))
	return err
}

func (s *PostgresStore) Records(ctx context.Context, v audit.Viewer, p audit.Page) ([]Record, error) {
	var user any
	if !v.All {
		user = v.User
	}
	limit := p.Limit
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.pool.Query(ctx, `SELECT alert_id, alert_type, alert_name, user_name, trigger, scheduled_time, start_time,
			end_time, state, coalesce(row_count, -1), integration, attempts, coalesce(error, '')
		FROM igloo_notify.alert_history
		WHERE ($1::text IS NULL OR user_name = $1)
		  AND ($2::timestamptz IS NULL OR start_time >= $2)
		  AND ($3::timestamptz IS NULL OR start_time < $3)
		  AND ($4::timestamptz IS NULL OR (start_time, alert_id) < ($4, $5))
		ORDER BY start_time DESC, alert_id DESC
		LIMIT $6`, user, nullTime(p.Since), nullTime(p.Until), nullTime(p.AfterTime), p.AfterID, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Record, error) {
		var (
			r              Record
			scheduled, end *time.Time
		)
		err := row.Scan(&r.ID, &r.Type, &r.Name, &r.User, &r.Trigger, &scheduled, &r.Start, &end, &r.State, &r.Rows,
			&r.Integration, &r.Attempts, &r.Error)
		if scheduled != nil {
			r.Scheduled = *scheduled
		}
		if end != nil {
			r.End = *end
		}
		return r, err
	})
}

func (s *PostgresStore) Claim(ctx context.Context, key string, hold time.Duration) (bool, error) {
	tag, err := s.pool.Exec(ctx, `INSERT INTO igloo_notify.claims (key, held_until) VALUES ($1, now() + $2::interval)
		ON CONFLICT (key) DO UPDATE SET held_until = excluded.held_until
		WHERE igloo_notify.claims.held_until <= now()`, key, hold)
	return err == nil && tag.RowsAffected() == 1, err
}

func (s *PostgresStore) Hold(ctx context.Context, key string, d time.Duration) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO igloo_notify.claims (key, held_until) VALUES ($1, now() + $2::interval)
		ON CONFLICT (key) DO UPDATE SET held_until = excluded.held_until`, key, d)
	return err
}

func (s *PostgresStore) Prune(ctx context.Context, records, claims time.Time) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var locked bool
		if err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock($1)", pruneKey).Scan(&locked); err != nil || !locked {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM igloo_notify.alert_history WHERE start_time < $1`, records); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DELETE FROM igloo_notify.claims WHERE held_until < $1`, claims)
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

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// sealer encrypts integrations' secrets with AES-256-GCM. The
// integration's name is bound in as associated data, so secrets copied to
// another integration's row do not decrypt there.
type sealer struct{ aead cipher.AEAD }

func newSealer(secret []byte) (*sealer, error) {
	if len(secret) < 32 {
		return nil, errors.New("notify: the secrets key must be at least 32 bytes")
	}
	key, err := hkdf.Key(sha256.New, secret, nil, "igloo notification integration v1", 32)
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

// seal returns the nonce followed by the ciphertext.
func (s *sealer) seal(name string, plain []byte) []byte {
	nonce := make([]byte, s.aead.NonceSize(), s.aead.NonceSize()+len(plain)+s.aead.Overhead())
	_, _ = rand.Read(nonce)
	return s.aead.Seal(nonce, nonce, plain, []byte(name))
}

func (s *sealer) open(name string, sealed []byte) ([]byte, error) {
	n := s.aead.NonceSize()
	if len(sealed) < n {
		return nil, errors.New("its stored secrets are damaged")
	}
	plain, err := s.aead.Open(nil, sealed[:n], sealed[n:], []byte(name))
	if err != nil {
		return nil, errors.New("its stored secrets cannot be decrypted with this deployment's key")
	}
	return plain, nil
}
