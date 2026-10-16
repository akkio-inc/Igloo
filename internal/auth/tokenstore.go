package auth

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// PostgresStore keeps access tokens in igloo_auth.tokens, which holds only
// their hashes.

const tokenColumns = `id, user_name, name, created_by, created_at, expires_at, last_used_at`

func scanToken(row pgx.Row, extra ...any) (Token, error) {
	var (
		t    Token
		used pgtype.Timestamptz
	)
	err := row.Scan(append([]any{&t.ID, &t.User, &t.Name, &t.CreatedBy, &t.CreatedAt, &t.ExpiresAt, &used}, extra...)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return Token{}, ErrTokenNotFound
	}
	if used.Valid {
		t.LastUsedAt = used.Time
	}
	return t, err
}

func (s *PostgresStore) CreateToken(ctx context.Context, t Token, secret TokenSecret) error {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Locking the user's row orders concurrent creations, so the limit
		// holds, and fails if the user is gone.
		var one int
		err := tx.QueryRow(ctx, `SELECT 1 FROM igloo_auth.users WHERE name = $1 FOR UPDATE`, t.User).Scan(&one)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		// An expired token's name may be used again.
		if _, err := tx.Exec(ctx, `DELETE FROM igloo_auth.tokens WHERE user_name = $1 AND name = $2 AND expires_at <= now()`,
			t.User, t.Name); err != nil {
			return err
		}
		var n int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM igloo_auth.tokens WHERE user_name = $1 AND expires_at > now()`,
			t.User).Scan(&n); err != nil {
			return err
		}
		if n >= MaxTokensPerUser {
			return ErrTooManyTokens
		}
		_, err = tx.Exec(ctx, `INSERT INTO igloo_auth.tokens (id, user_name, name, secret_hash, scram_salt,
				scram_iterations, stored_key, server_key, created_by, created_at, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			t.ID, t.User, t.Name, secret.Hash, []byte(secret.SCRAM.Salt), secret.SCRAM.Iters,
			secret.SCRAM.StoredKey, secret.SCRAM.ServerKey, t.CreatedBy, t.CreatedAt, t.ExpiresAt)
		return err
	})
	return uniqueError(err, ErrTokenExists)
}

func (s *PostgresStore) Tokens(ctx context.Context, user string) ([]Token, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+tokenColumns+` FROM igloo_auth.tokens
		WHERE $1 = '' OR user_name = $1 ORDER BY user_name, name`, user)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Token, error) { return scanToken(row) })
}

func (s *PostgresStore) DropToken(ctx context.Context, user, name string) (Token, error) {
	return scanToken(s.pool.QueryRow(ctx, `DELETE FROM igloo_auth.tokens WHERE user_name = $1 AND name = $2
		RETURNING `+tokenColumns, user, name))
}

func (s *PostgresStore) TokenCredentials(ctx context.Context, user string) ([]TokenCredential, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+tokenColumns+`, scram_salt, scram_iterations, stored_key, server_key
		FROM igloo_auth.tokens WHERE user_name = $1 AND expires_at > now() ORDER BY name`, user)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (TokenCredential, error) {
		var (
			c    TokenCredential
			salt []byte
		)
		t, err := scanToken(row, &salt, &c.SCRAM.Iters, &c.SCRAM.StoredKey, &c.SCRAM.ServerKey)
		c.Token, c.SCRAM.Salt = t, string(salt)
		return c, err
	})
}

func (s *PostgresStore) TokenByID(ctx context.Context, id string) (Token, []byte, error) {
	var hash []byte
	t, err := scanToken(s.pool.QueryRow(ctx, `SELECT `+tokenColumns+`, secret_hash FROM igloo_auth.tokens WHERE id = $1`, id), &hash)
	return t, hash, err
}

func (s *PostgresStore) TokenUsed(ctx context.Context, id string, at time.Time) error {
	_, err := s.pool.Exec(ctx, `UPDATE igloo_auth.tokens SET last_used_at = $2
		WHERE id = $1 AND (last_used_at IS NULL OR last_used_at < $2)`, id, at)
	return err
}

var _ TokenStore = (*PostgresStore)(nil)
