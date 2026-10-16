package sharing

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store keeps shares, recipients and their tokens, providers and external
// tables in the "igloo_sharing" schema of the metadata database, so every
// replica sees the same ones.
type Store struct {
	pool *pgxpool.Pool
}

// migrationKey serializes creating the sharing tables across replicas that
// start together.
const migrationKey int64 = 0x49676c6f6f536872 // "IglooShr"

var migrations = []string{
	`CREATE SCHEMA IF NOT EXISTS igloo_sharing`,
	`CREATE TABLE IF NOT EXISTS igloo_sharing.shares (
		name       text PRIMARY KEY,
		id         uuid NOT NULL,
		comment    text NOT NULL DEFAULT '',
		created_by text NOT NULL DEFAULT '',
		created_at timestamptz NOT NULL DEFAULT now()
	)`,
	// Shared names are case-insensitive in the protocol.
	`CREATE TABLE IF NOT EXISTS igloo_sharing.share_tables (
		share         text NOT NULL REFERENCES igloo_sharing.shares (name) ON DELETE CASCADE,
		schema_name   text NOT NULL,
		table_name    text NOT NULL,
		source_schema text NOT NULL,
		source_table  text NOT NULL,
		history       boolean NOT NULL DEFAULT false,
		added_by      text NOT NULL DEFAULT '',
		added_at      timestamptz NOT NULL DEFAULT now()
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS share_tables_name
		ON igloo_sharing.share_tables (share, lower(schema_name), lower(table_name))`,
	`CREATE TABLE IF NOT EXISTS igloo_sharing.recipients (
		name             text PRIMARY KEY,
		id               uuid NOT NULL,
		comment          text NOT NULL DEFAULT '',
		token_lifetime_s bigint NOT NULL CHECK (token_lifetime_s > 0),
		created_by       text NOT NULL DEFAULT '',
		created_at       timestamptz NOT NULL DEFAULT now()
	)`,
	// Bearer tokens and activation codes, by the ID in their text. Only the
	// hash of the secret part is kept.
	`CREATE TABLE IF NOT EXISTS igloo_sharing.tokens (
		id          text PRIMARY KEY,
		recipient   text NOT NULL REFERENCES igloo_sharing.recipients (name) ON DELETE CASCADE,
		kind        text NOT NULL CHECK (kind IN ('bearer', 'activation')),
		secret_hash bytea NOT NULL,
		created_at  timestamptz NOT NULL DEFAULT now(),
		expires_at  timestamptz NOT NULL,
		used_at     timestamptz
	)`,
	`CREATE INDEX IF NOT EXISTS tokens_recipient ON igloo_sharing.tokens (recipient)`,
	`CREATE TABLE IF NOT EXISTS igloo_sharing.share_grants (
		share      text NOT NULL REFERENCES igloo_sharing.shares (name) ON DELETE CASCADE,
		recipient  text NOT NULL REFERENCES igloo_sharing.recipients (name) ON DELETE CASCADE,
		granted_by text NOT NULL DEFAULT '',
		granted_at timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (share, recipient)
	)`,
	// A provider's bearer token is encrypted (see Service.sealToken).
	`CREATE TABLE IF NOT EXISTS igloo_sharing.providers (
		name             text PRIMARY KEY,
		endpoint         text NOT NULL,
		token            bytea NOT NULL,
		token_expires_at timestamptz,
		created_by       text NOT NULL DEFAULT '',
		created_at       timestamptz NOT NULL DEFAULT now()
	)`,
	// A provider cannot be dropped while external tables read from it.
	`CREATE TABLE IF NOT EXISTS igloo_sharing.external_tables (
		schema_name   text NOT NULL,
		table_name    text NOT NULL,
		provider      text NOT NULL REFERENCES igloo_sharing.providers (name),
		share         text NOT NULL,
		remote_schema text NOT NULL,
		remote_table  text NOT NULL,
		created_by    text NOT NULL DEFAULT '',
		created_at    timestamptz NOT NULL DEFAULT now()
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS external_tables_name
		ON igloo_sharing.external_tables (lower(schema_name), lower(table_name))`,
}

// OpenStore connects to the metadata database at url with a small pool of
// its own and creates the sharing tables if needed.
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
		return nil, fmt.Errorf("create sharing tables: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the store's connections.
func (s *Store) Close() { s.pool.Close() }

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func isForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23503"
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = 0x40 | b[6]&0x0f
	b[8] = 0x80 | b[8]&0x3f
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// --- shares -------------------------------------------------------------------

// CreateShare adds a share. It fails with ErrExists if one has the name.
func (s *Store) CreateShare(ctx context.Context, sh Share) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO igloo_sharing.shares (name, id, comment, created_by) VALUES ($1, $2, $3, $4)`,
		sh.Name, newUUID(), sh.Comment, sh.CreatedBy)
	if isUniqueViolation(err) {
		return ErrExists
	}
	return err
}

// DropShare removes a share, its tables and its grants.
func (s *Store) DropShare(ctx context.Context, name string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM igloo_sharing.shares WHERE name = $1`, name)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

const shareColumns = `name, id::text, comment, created_by, created_at`

func scanShare(row pgx.CollectableRow) (Share, error) {
	var sh Share
	err := row.Scan(&sh.Name, &sh.ID, &sh.Comment, &sh.CreatedBy, &sh.CreatedAt)
	return sh, err
}

// Shares lists every share by name.
func (s *Store) Shares(ctx context.Context) ([]Share, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+shareColumns+` FROM igloo_sharing.shares ORDER BY name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanShare)
}

// RecipientShares lists the shares granted to recipient, by name.
func (s *Store) RecipientShares(ctx context.Context, recipient string) ([]Share, error) {
	rows, err := s.pool.Query(ctx, `SELECT s.name, s.id::text, s.comment, s.created_by, s.created_at
		FROM igloo_sharing.shares s JOIN igloo_sharing.share_grants g ON g.share = s.name
		WHERE g.recipient = $1 ORDER BY s.name`, recipient)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanShare)
}

// AddTable adds a table to a share. It fails with ErrNotFound when the share
// does not exist and ErrExists when the share has a table of that name.
func (s *Store) AddTable(ctx context.Context, t SharedTable) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO igloo_sharing.share_tables
		(share, schema_name, table_name, source_schema, source_table, history, added_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		t.Share, t.Schema, t.Name, t.SourceSchema, t.SourceTable, t.History, t.AddedBy)
	switch {
	case isUniqueViolation(err):
		return ErrExists
	case isForeignKeyViolation(err):
		return ErrNotFound
	}
	return err
}

// RemoveTable removes the table recipients know as schema.name from share.
func (s *Store) RemoveTable(ctx context.Context, share, schema, name string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM igloo_sharing.share_tables
		WHERE share = $1 AND lower(schema_name) = lower($2) AND lower(table_name) = lower($3)`, share, schema, name)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

// ShareTables lists a share's tables by shared name. It fails with
// ErrNotFound when the share does not exist.
func (s *Store) ShareTables(ctx context.Context, share string) ([]SharedTable, error) {
	var tables []SharedTable
	err := pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM igloo_sharing.shares WHERE name = $1)`, share).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return ErrNotFound
		}
		rows, err := tx.Query(ctx, `SELECT share, schema_name, table_name, source_schema, source_table, history, added_by, added_at
			FROM igloo_sharing.share_tables WHERE share = $1 ORDER BY lower(schema_name), lower(table_name)`, share)
		if err != nil {
			return err
		}
		tables, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (SharedTable, error) {
			var t SharedTable
			err := row.Scan(&t.Share, &t.Schema, &t.Name, &t.SourceSchema, &t.SourceTable, &t.History, &t.AddedBy, &t.AddedAt)
			return t, err
		})
		return err
	})
	return tables, err
}

// GrantShare lets recipient read share.
func (s *Store) GrantShare(ctx context.Context, share, recipient, by string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var shareExists, recipientExists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM igloo_sharing.shares WHERE name = $1),
				EXISTS (SELECT 1 FROM igloo_sharing.recipients WHERE name = $2)`, share, recipient,
		).Scan(&shareExists, &recipientExists); err != nil {
			return err
		}
		if !shareExists {
			return fmt.Errorf("share %q %w", share, ErrNotFound)
		}
		if !recipientExists {
			return fmt.Errorf("recipient %q %w", recipient, ErrNotFound)
		}
		_, err := tx.Exec(ctx, `INSERT INTO igloo_sharing.share_grants (share, recipient, granted_by) VALUES ($1, $2, $3)
			ON CONFLICT (share, recipient) DO NOTHING`, share, recipient, by)
		if isForeignKeyViolation(err) {
			return ErrNotFound
		}
		return err
	})
}

// RevokeShare stops recipient reading share. Revoking a grant that does not
// exist is not an error.
func (s *Store) RevokeShare(ctx context.Context, share, recipient string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM igloo_sharing.share_grants WHERE share = $1 AND recipient = $2`, share, recipient)
	return err
}

// ShareGrants lists the recipients of share, or of every share when share is
// empty.
func (s *Store) ShareGrants(ctx context.Context, share string) ([]ShareGrant, error) {
	rows, err := s.pool.Query(ctx, `SELECT share, recipient, granted_by, granted_at FROM igloo_sharing.share_grants
		WHERE $1 = '' OR share = $1 ORDER BY share, recipient`, share)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (ShareGrant, error) {
		var g ShareGrant
		err := row.Scan(&g.Share, &g.Recipient, &g.GrantedBy, &g.GrantedAt)
		return g, err
	})
}

// --- recipients and tokens ---------------------------------------------------

// CreateRecipient adds a recipient with an activation code that is valid
// until activationExpires.
func (s *Store) CreateRecipient(ctx context.Context, r Recipient, activation secretToken, activationExpires time.Time) error {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO igloo_sharing.recipients (name, id, comment, token_lifetime_s, created_by)
			VALUES ($1, $2, $3, $4, $5)`, r.Name, newUUID(), r.Comment, int64(r.TokenLifetime/time.Second), r.CreatedBy); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO igloo_sharing.tokens (id, recipient, kind, secret_hash, expires_at)
			VALUES ($1, $2, 'activation', $3, $4)`, activation.ID, r.Name, activation.Hash, activationExpires)
		return err
	})
	if isUniqueViolation(err) {
		return ErrExists
	}
	return err
}

// RotateToken replaces a recipient's activation code with a new one, and
// makes the recipient's bearer tokens expire after keep (at once when keep
// is zero), unless they expire sooner.
func (s *Store) RotateToken(ctx context.Context, recipient string, keep time.Duration, activation secretToken, activationExpires time.Time) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var name string
		err := tx.QueryRow(ctx, `SELECT name FROM igloo_sharing.recipients WHERE name = $1 FOR UPDATE`, recipient).Scan(&name)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE igloo_sharing.tokens SET expires_at = least(expires_at, now() + make_interval(secs => $2))
			WHERE recipient = $1 AND kind = 'bearer'`, recipient, keep.Seconds()); err != nil {
			return err
		}
		// Expired tokens are of no further use; keep the table small.
		if _, err := tx.Exec(ctx, `DELETE FROM igloo_sharing.tokens WHERE recipient = $1
			AND (kind = 'activation' OR expires_at < now() - interval '30 days')`, recipient); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO igloo_sharing.tokens (id, recipient, kind, secret_hash, expires_at)
			VALUES ($1, $2, 'activation', $3, $4)`, activation.ID, recipient, activation.Hash, activationExpires)
		return err
	})
}

// DropRecipient removes a recipient with its tokens and grants.
func (s *Store) DropRecipient(ctx context.Context, name string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM igloo_sharing.recipients WHERE name = $1`, name)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

// Recipients lists every recipient with its shares and token state.
func (s *Store) Recipients(ctx context.Context) ([]Recipient, error) {
	rows, err := s.pool.Query(ctx, `SELECT r.name, r.id::text, r.comment, r.token_lifetime_s, r.created_by, r.created_at,
			coalesce((SELECT array_agg(g.share ORDER BY g.share) FROM igloo_sharing.share_grants g WHERE g.recipient = r.name), '{}'),
			(SELECT max(t.expires_at) FROM igloo_sharing.tokens t
				WHERE t.recipient = r.name AND t.kind = 'bearer' AND t.expires_at > now()),
			EXISTS (SELECT 1 FROM igloo_sharing.tokens t
				WHERE t.recipient = r.name AND t.kind = 'activation' AND t.used_at IS NULL AND t.expires_at > now())
		FROM igloo_sharing.recipients r ORDER BY r.name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Recipient, error) {
		var (
			r        Recipient
			lifetime int64
		)
		err := row.Scan(&r.Name, &r.ID, &r.Comment, &lifetime, &r.CreatedBy, &r.CreatedAt, &r.Shares, &r.TokenExpires, &r.Pending)
		r.TokenLifetime = time.Duration(lifetime) * time.Second
		return r, err
	})
}

// activated is the outcome of redeeming an activation code.
type activated struct {
	Recipient string
	Expires   time.Time
}

// Activate redeems an activation code once: it checks the code's hash in
// constant time, marks the code used and stores bearer as the recipient's
// new token, valid for the recipient's token lifetime. Unknown, used and
// expired codes all fail with ErrNotFound.
func (s *Store) Activate(ctx context.Context, id string, hash []byte, bearer secretToken) (activated, error) {
	var a activated
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var (
			stored []byte
			usable bool
		)
		err := tx.QueryRow(ctx, `SELECT recipient, secret_hash, used_at IS NULL AND expires_at > now()
			FROM igloo_sharing.tokens WHERE id = $1 AND kind = 'activation' FOR UPDATE`, id).Scan(&a.Recipient, &stored, &usable)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !sameHash(stored, hash) || !usable {
			return ErrNotFound
		}
		if _, err := tx.Exec(ctx, `UPDATE igloo_sharing.tokens SET used_at = now() WHERE id = $1`, id); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `INSERT INTO igloo_sharing.tokens (id, recipient, kind, secret_hash, expires_at)
			SELECT $1, name, 'bearer', $2, now() + make_interval(secs => token_lifetime_s)
			FROM igloo_sharing.recipients WHERE name = $3
			RETURNING expires_at`, bearer.ID, bearer.Hash, a.Recipient).Scan(&a.Expires)
	})
	return a, err
}

// bearerToken is a stored bearer token.
type bearerToken struct {
	ID        string
	Recipient string
	Hash      []byte
	// Valid is false once the token has expired or been rotated out.
	Valid   bool
	Expires time.Time
}

// BearerToken looks up the bearer token with the given ID.
func (s *Store) BearerToken(ctx context.Context, id string) (bearerToken, error) {
	t := bearerToken{ID: id}
	err := s.pool.QueryRow(ctx, `SELECT recipient, secret_hash, expires_at > now(), expires_at
		FROM igloo_sharing.tokens WHERE id = $1 AND kind = 'bearer'`, id).Scan(&t.Recipient, &t.Hash, &t.Valid, &t.Expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return t, ErrNotFound
	}
	return t, err
}

// --- providers and external tables ------------------------------------------

// PutProvider adds a provider, or with replace changes an existing one. The
// token must already be sealed.
func (s *Store) PutProvider(ctx context.Context, p Provider, sealedToken []byte, replace bool) error {
	q := `INSERT INTO igloo_sharing.providers (name, endpoint, token, token_expires_at, created_by) VALUES ($1, $2, $3, $4, $5)`
	if replace {
		q += ` ON CONFLICT (name) DO UPDATE SET endpoint = excluded.endpoint, token = excluded.token,
			token_expires_at = excluded.token_expires_at`
	}
	_, err := s.pool.Exec(ctx, q, p.Name, p.Endpoint, sealedToken, p.TokenExpires, p.CreatedBy)
	if isUniqueViolation(err) {
		return ErrExists
	}
	return err
}

// DropProvider removes a provider. It fails with ErrInUse while external
// tables read from it.
func (s *Store) DropProvider(ctx context.Context, name string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM igloo_sharing.providers WHERE name = $1`, name)
	switch {
	case isForeignKeyViolation(err):
		return ErrInUse
	case err == nil && tag.RowsAffected() == 0:
		return ErrNotFound
	}
	return err
}

// Providers lists every provider, with its sealed token.
func (s *Store) Providers(ctx context.Context) ([]Provider, [][]byte, error) {
	rows, err := s.pool.Query(ctx, `SELECT name, endpoint, token, token_expires_at, created_by, created_at
		FROM igloo_sharing.providers ORDER BY name`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var (
		out    []Provider
		sealed [][]byte
	)
	for rows.Next() {
		var (
			p     Provider
			token []byte
		)
		if err := rows.Scan(&p.Name, &p.Endpoint, &token, &p.TokenExpires, &p.CreatedBy, &p.CreatedAt); err != nil {
			return nil, nil, err
		}
		out, sealed = append(out, p), append(sealed, token)
	}
	return out, sealed, rows.Err()
}

// CreateExternalTable records an external table. It fails with ErrExists if
// one has the name, and ErrNotFound if the provider does not exist.
func (s *Store) CreateExternalTable(ctx context.Context, t ExternalTable) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO igloo_sharing.external_tables
		(schema_name, table_name, provider, share, remote_schema, remote_table, created_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		t.Schema, t.Name, t.Provider, t.Share, t.RemoteSchema, t.RemoteTable, t.CreatedBy)
	switch {
	case isUniqueViolation(err):
		return ErrExists
	case isForeignKeyViolation(err):
		return ErrNotFound
	}
	return err
}

// DropExternalTable forgets the external table schema.name.
func (s *Store) DropExternalTable(ctx context.Context, schema, name string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM igloo_sharing.external_tables
		WHERE lower(schema_name) = lower($1) AND lower(table_name) = lower($2)`, schema, name)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

// ExternalTables lists every external table.
func (s *Store) ExternalTables(ctx context.Context) ([]ExternalTable, error) {
	rows, err := s.pool.Query(ctx, `SELECT schema_name, table_name, provider, share, remote_schema, remote_table, created_by, created_at
		FROM igloo_sharing.external_tables ORDER BY lower(schema_name), lower(table_name)`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (ExternalTable, error) {
		var t ExternalTable
		err := row.Scan(&t.Schema, &t.Name, &t.Provider, &t.Share, &t.RemoteSchema, &t.RemoteTable, &t.CreatedBy, &t.CreatedAt)
		return t, err
	})
}
