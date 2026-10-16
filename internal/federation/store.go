package federation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound = errors.New("connection does not exist")
	ErrExists   = errors.New("connection already exists")
)

// Store keeps connections. Implementations must be safe for concurrent use.
type Store interface {
	// List returns every connection, by name, with passwords decrypted.
	List(ctx context.Context) ([]Connection, error)
	// Create fails with ErrExists.
	Create(ctx context.Context, c Connection) error
	// Update replaces the settings of the connection called c.Name, keeping
	// its ID and creation. It fails with ErrNotFound.
	Update(ctx context.Context, c Connection) error
	// Delete removes a connection and the grants on it, failing with
	// ErrNotFound.
	Delete(ctx context.Context, name string) error
}

// newID returns a random connection ID.
func newID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// MemoryStore keeps connections in memory, for tests.
type MemoryStore struct {
	mu    sync.Mutex
	conns map[string]Connection
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{conns: map[string]Connection{}} }

func (m *MemoryStore) List(context.Context) ([]Connection, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Connection, 0, len(m.conns))
	for _, c := range m.conns {
		c.Schemas = slices.Clone(c.Schemas)
		out = append(out, c)
	}
	slices.SortFunc(out, func(a, b Connection) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

func (m *MemoryStore) Create(_ context.Context, c Connection) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.conns[c.Name]; ok {
		return ErrExists
	}
	c.ID, c.CreatedAt, c.UpdatedAt = newID(), time.Now(), time.Now()
	m.conns[c.Name] = c
	return nil
}

func (m *MemoryStore) Update(_ context.Context, c Connection) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	old, ok := m.conns[c.Name]
	if !ok {
		return ErrNotFound
	}
	c.ID, c.CreatedBy, c.CreatedAt, c.UpdatedAt = old.ID, old.CreatedBy, old.CreatedAt, time.Now()
	m.conns[c.Name] = c
	return nil
}

func (m *MemoryStore) Delete(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.conns[name]; !ok {
		return ErrNotFound
	}
	delete(m.conns, name)
	return nil
}

// migrationKey serializes creating the connections table across replicas
// starting at the same time.
const migrationKey int64 = 0x49676c6f6f466564 // "IglooFed"

// The settings of each connection, in igloo_auth next to the users and
// grants. A stored password is sealed (see sealer); one named by
// password_secret is never stored at all.
var migrations = []string{
	`CREATE SCHEMA IF NOT EXISTS igloo_auth`,
	`CREATE TABLE IF NOT EXISTS igloo_auth.connections (
		name               text PRIMARY KEY,
		id                 text NOT NULL,
		type               text NOT NULL CHECK (type IN ('postgres', 'mysql')),
		host               text NOT NULL,
		port               integer NOT NULL,
		database_name      text NOT NULL,
		user_name          text NOT NULL,
		password           bytea,
		password_secret    text NOT NULL DEFAULT '',
		tls                text NOT NULL,
		tls_ca             text NOT NULL DEFAULT '',
		schemas            text[] NOT NULL DEFAULT '{}',
		connect_timeout_ms bigint NOT NULL,
		query_timeout_ms   bigint NOT NULL,
		created_by         text NOT NULL DEFAULT '',
		created_at         timestamptz NOT NULL DEFAULT now(),
		updated_at         timestamptz NOT NULL DEFAULT now()
	)`,
}

// PostgresStore keeps connections in igloo_auth.connections of the
// metadata database, shared by every replica.
type PostgresStore struct {
	pool   *pgxpool.Pool
	sealer *sealer
}

// OpenPostgresStore connects to url with a small pool of its own and
// creates the connections table if needed. key encrypts stored passwords;
// every replica must use the same one (see KeyName).
func OpenPostgresStore(ctx context.Context, url string, key []byte) (*PostgresStore, error) {
	s, err := newSealer(key)
	if err != nil {
		return nil, err
	}
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
		return nil, fmt.Errorf("create the connections table: %w", err)
	}
	return &PostgresStore{pool: pool, sealer: s}, nil
}

func (s *PostgresStore) Close() { s.pool.Close() }

const columns = `name, id, type, host, port, database_name, user_name, password, password_secret, tls, tls_ca,
	schemas, connect_timeout_ms, query_timeout_ms, created_by, created_at, updated_at`

func (s *PostgresStore) List(ctx context.Context) ([]Connection, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+columns+` FROM igloo_auth.connections ORDER BY name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Connection, error) {
		var (
			c                  Connection
			typ, tls           string
			sealed             []byte
			connectMS, queryMS int64
		)
		err := row.Scan(&c.Name, &c.ID, &typ, &c.Host, &c.Port, &c.Database, &c.User, &sealed, &c.PasswordSecret,
			&tls, &c.TLSCA, &c.Schemas, &connectMS, &queryMS, &c.CreatedBy, &c.CreatedAt, &c.UpdatedAt)
		if err != nil {
			return c, err
		}
		c.Type, c.TLS = Type(typ), TLSMode(tls)
		c.ConnectTimeout, c.QueryTimeout = time.Duration(connectMS)*time.Millisecond, time.Duration(queryMS)*time.Millisecond
		if sealed != nil {
			// A password that does not decrypt leaves the connection
			// unable to sign in, rather than hiding every connection.
			if c.Password, err = s.sealer.open(c.Name, sealed); err != nil {
				c.Password, c.Unusable = "", err.Error()
			}
		}
		return c, nil
	})
}

// sealPassword returns the stored form of c's password, or nil.
func (s *PostgresStore) sealPassword(c Connection) []byte {
	if c.Password == "" {
		return nil
	}
	return s.sealer.seal(c.Name, c.Password)
}

func (s *PostgresStore) Create(ctx context.Context, c Connection) error {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO igloo_auth.connections (`+columns+`)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, now(), now())`,
			c.Name, newID(), string(c.Type), c.Host, c.Port, c.Database, c.User, s.sealPassword(c), c.PasswordSecret,
			string(c.TLS), c.TLSCA, nonNil(c.Schemas), c.ConnectTimeout.Milliseconds(), c.QueryTimeout.Milliseconds(), c.CreatedBy)
		if err != nil {
			return err
		}
		// Grants left by a connection of this name dropped outside Igloo
		// must not apply to this one.
		return forgetGrants(ctx, tx, c.Name)
	})
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return ErrExists
	}
	return err
}

func (s *PostgresStore) Update(ctx context.Context, c Connection) error {
	tag, err := s.pool.Exec(ctx, `UPDATE igloo_auth.connections SET type = $2, host = $3, port = $4,
			database_name = $5, user_name = $6, password = $7, password_secret = $8, tls = $9, tls_ca = $10,
			schemas = $11, connect_timeout_ms = $12, query_timeout_ms = $13, updated_at = now()
		WHERE name = $1`,
		c.Name, string(c.Type), c.Host, c.Port, c.Database, c.User, s.sealPassword(c), c.PasswordSecret,
		string(c.TLS), c.TLSCA, nonNil(c.Schemas), c.ConnectTimeout.Milliseconds(), c.QueryTimeout.Milliseconds())
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

func (s *PostgresStore) Delete(ctx context.Context, name string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `DELETE FROM igloo_auth.connections WHERE name = $1`, name)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return forgetGrants(ctx, tx, name)
	})
}

// forgetGrants removes the grants on a connection, from the table package
// auth keeps them in (it creates it before this package's table).
func forgetGrants(ctx context.Context, tx pgx.Tx, name string) error {
	_, err := tx.Exec(ctx, `DELETE FROM igloo_auth.connection_grants WHERE lower(connection_name) = lower($1)`, name)
	return err
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
