package auth

import (
	"context"
	"crypto/rand"
)

// Secret returns the random 32-byte secret called name from
// igloo_auth.secrets, creating it the first time any replica asks, so every
// replica gets the same one. The web console seals its session cookies with
// one, so a cookie from one replica works on all of them.
func (s *PostgresStore) Secret(ctx context.Context, name string) ([]byte, error) {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, err
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO igloo_auth.secrets (name, value) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING`, name, secret)
	if err != nil {
		return nil, err
	}
	err = s.pool.QueryRow(ctx, `SELECT value FROM igloo_auth.secrets WHERE name = $1`, name).Scan(&secret)
	return secret, err
}
