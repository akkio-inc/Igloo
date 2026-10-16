package auth

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store keeps users. Implementations must be safe for concurrent use.
type Store interface {
	Get(ctx context.Context, name string) (User, error)
	List(ctx context.Context) ([]User, error)
	Create(ctx context.Context, u User) error
	SetVerifier(ctx context.Context, name, verifier string) error
	SetRole(ctx context.Context, name string, role Role) error
	Delete(ctx context.Context, name string) error
	// EnsureAdmin creates the bootstrap administrator if no user of that
	// name exists. It never changes an existing user.
	EnsureAdmin(ctx context.Context, name, verifier string) (created bool, err error)
	// MockSecret is a random secret, stable across restarts and replicas
	// where possible, for deriving mock credentials for unknown users.
	MockSecret(ctx context.Context) ([]byte, error)
	Ping(ctx context.Context) error
	Close()
}

// MemoryStore holds only the bootstrap administrator. It is used when no
// metadata database is configured, and refuses changes.
type MemoryStore struct {
	mu     sync.RWMutex
	admin  User
	secret []byte
}

// NewMemoryStore returns an empty store; EnsureAdmin fills it.
func NewMemoryStore() *MemoryStore {
	secret := make([]byte, 32)
	_, _ = rand.Read(secret)
	return &MemoryStore{secret: secret}
}

// MockSecret is random per process: a memory store serves one replica.
func (m *MemoryStore) MockSecret(context.Context) ([]byte, error) { return m.secret, nil }

func (m *MemoryStore) Get(_ context.Context, name string) (User, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.admin.Name == "" || name != m.admin.Name {
		return User{}, ErrNotFound
	}
	return m.admin, nil
}

func (m *MemoryStore) List(ctx context.Context) ([]User, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.admin.Name == "" {
		return nil, nil
	}
	return []User{m.admin}, nil
}

func (m *MemoryStore) EnsureAdmin(_ context.Context, name, verifier string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.admin.Name != "" {
		return false, nil
	}
	m.admin = User{Name: name, Role: RoleAdmin, Verifier: verifier, CreatedAt: time.Now()}
	return true, nil
}

func (m *MemoryStore) Create(context.Context, User) error                { return ErrReadOnly }
func (m *MemoryStore) SetVerifier(context.Context, string, string) error { return ErrReadOnly }
func (m *MemoryStore) SetRole(context.Context, string, Role) error       { return ErrReadOnly }
func (m *MemoryStore) Delete(context.Context, string) error              { return ErrReadOnly }
func (m *MemoryStore) Ping(context.Context) error                        { return nil }
func (m *MemoryStore) Close()                                            {}

// PostgresStore keeps users in the "igloo_auth" schema of a PostgreSQL
// database, so every replica sees the same accounts. The database is usually
// shared with the catalog. The schema must not share a name with the
// database role: PostgreSQL's default search_path ("$user", public) would
// then resolve the catalog's unqualified table names into it.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// migrationLock serializes schema migrations across replicas starting at
// the same time. The value is arbitrary but fixed.
const migrationLock = 7_440_011_001

// namesLock serializes changes that create or drop users and custom roles,
// which share one namespace, with grants that name them, so a grant never
// outlives its grantee.
const namesLock = 7_440_011_002

var migrations = []string{
	`CREATE SCHEMA IF NOT EXISTS igloo_auth`,
	`CREATE TABLE IF NOT EXISTS igloo_auth.users (
		name       text PRIMARY KEY,
		role       text NOT NULL CHECK (role IN ('admin', 'writer', 'reader')),
		verifier   text NOT NULL,
		created_at timestamptz NOT NULL DEFAULT now(),
		updated_at timestamptz NOT NULL DEFAULT now()
	)`,
	`CREATE TABLE IF NOT EXISTS igloo_auth.secrets (
		name  text PRIMARY KEY,
		value bytea NOT NULL
	)`,
	// Allow the built-in role none. Older versions created the role check
	// without it; replace that check once, so later starts change nothing.
	// Older replicas still running read a user with role none as having no
	// role at all, and refuse their statements.
	`DO $$
	DECLARE c record;
	BEGIN
		IF NOT EXISTS (SELECT 1 FROM pg_constraint
				WHERE conrelid = 'igloo_auth.users'::regclass AND conname = 'users_role_check'
				AND pg_get_constraintdef(oid) LIKE '%''none''%') THEN
			FOR c IN SELECT conname FROM pg_constraint
					WHERE conrelid = 'igloo_auth.users'::regclass AND contype = 'c'
					AND pg_get_constraintdef(oid) LIKE '%role%' LOOP
				EXECUTE format('ALTER TABLE igloo_auth.users DROP CONSTRAINT %I', c.conname);
			END LOOP;
			ALTER TABLE igloo_auth.users ADD CONSTRAINT users_role_check
				CHECK (role IN ('admin', 'writer', 'reader', 'none'));
		END IF;
	END $$`,
	`CREATE TABLE IF NOT EXISTS igloo_auth.roles (
		name       text PRIMARY KEY,
		created_at timestamptz NOT NULL DEFAULT now()
	)`,
	`CREATE TABLE IF NOT EXISTS igloo_auth.role_members (
		role       text NOT NULL REFERENCES igloo_auth.roles (name) ON DELETE CASCADE,
		member     text NOT NULL REFERENCES igloo_auth.users (name) ON DELETE CASCADE,
		granted_by text NOT NULL DEFAULT '',
		granted_at timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (role, member)
	)`,
	`CREATE INDEX IF NOT EXISTS role_members_member ON igloo_auth.role_members (member)`,
	// A grant to a user or custom role on a schema (table_name '') or on a
	// table or view. Grantees are deleted with the user or role.
	`CREATE TABLE IF NOT EXISTS igloo_auth.grants (
		grantee      text NOT NULL,
		grantee_type text NOT NULL CHECK (grantee_type IN ('user', 'role')),
		privilege    text NOT NULL CHECK (privilege IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE')),
		schema_name  text NOT NULL,
		table_name   text NOT NULL DEFAULT '',
		granted_by   text NOT NULL DEFAULT '',
		granted_at   timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (grantee_type, grantee, privilege, schema_name, table_name)
	)`,
	`CREATE INDEX IF NOT EXISTS grants_object ON igloo_auth.grants (lower(schema_name), lower(table_name))`,
}

// OpenPostgresStore connects to url and creates Igloo's tables if needed.
func OpenPostgresStore(ctx context.Context, url string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("metadata database: %w", err)
	}
	s := &PostgresStore{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("metadata database: %w", err)
	}
	return s, nil
}

func (s *PostgresStore) migrate(ctx context.Context) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", migrationLock); err != nil {
			return err
		}
		for _, m := range migrations {
			if _, err := tx.Exec(ctx, m); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *PostgresStore) Get(ctx context.Context, name string) (User, error) {
	var u User
	err := s.pool.QueryRow(ctx,
		`SELECT name, role, verifier, created_at FROM igloo_auth.users WHERE name = $1`, name,
	).Scan(&u.Name, &u.Role, &u.Verifier, &u.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrNotFound
	}
	return u, err
}

func (s *PostgresStore) List(ctx context.Context) ([]User, error) {
	rows, err := s.pool.Query(ctx, `SELECT name, role, verifier, created_at FROM igloo_auth.users ORDER BY name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (User, error) {
		var u User
		err := row.Scan(&u.Name, &u.Role, &u.Verifier, &u.CreatedAt)
		return u, err
	})
}

func (s *PostgresStore) Create(ctx context.Context, u User) error {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockNames(ctx, tx); err != nil {
			return err
		}
		var isRole bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM igloo_auth.roles WHERE name = $1)`, u.Name).Scan(&isRole); err != nil {
			return err
		}
		if isRole {
			return ErrNameTaken
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO igloo_auth.users (name, role, verifier) VALUES ($1, $2, $3)`, u.Name, u.Role, u.Verifier)
		return err
	})
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return ErrExists
	}
	return err
}

func (s *PostgresStore) EnsureAdmin(ctx context.Context, name, verifier string) (bool, error) {
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO igloo_auth.users (name, role, verifier) VALUES ($1, 'admin', $2) ON CONFLICT (name) DO NOTHING`,
		name, verifier)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (s *PostgresStore) SetVerifier(ctx context.Context, name, verifier string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE igloo_auth.users SET verifier = $2, updated_at = now() WHERE name = $1`, name, verifier)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

func (s *PostgresStore) SetRole(ctx context.Context, name string, role Role) error {
	return s.changeAdmins(ctx, name, func(tx pgx.Tx) (pgconn.CommandTag, error) {
		return tx.Exec(ctx, `UPDATE igloo_auth.users SET role = $2, updated_at = now() WHERE name = $1`, name, role)
	})
}

// Delete removes the user with their memberships (by cascade) and grants.
func (s *PostgresStore) Delete(ctx context.Context, name string) error {
	return s.changeAdmins(ctx, name, func(tx pgx.Tx) (pgconn.CommandTag, error) {
		tag, err := tx.Exec(ctx, `DELETE FROM igloo_auth.users WHERE name = $1`, name)
		if err != nil || tag.RowsAffected() == 0 {
			return tag, err
		}
		_, err = tx.Exec(ctx, `DELETE FROM igloo_auth.grants WHERE grantee_type = 'user' AND grantee = $1`, name)
		return tag, err
	})
}

// changeAdmins applies change and rolls it back if no admin would remain.
// The table lock stops two concurrent demotions from both passing the check.
// The names lock comes first, as in every transaction that takes both.
func (s *PostgresStore) changeAdmins(ctx context.Context, name string, change func(pgx.Tx) (pgconn.CommandTag, error)) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockNames(ctx, tx); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `LOCK TABLE igloo_auth.users IN SHARE ROW EXCLUSIVE MODE`); err != nil {
			return err
		}
		tag, err := change(tx)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		var admins int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM igloo_auth.users WHERE role = 'admin'`).Scan(&admins); err != nil {
			return err
		}
		if admins == 0 {
			return ErrLastAdmin
		}
		return nil
	})
}

// MockSecret is created once, by whichever replica gets there first.
func (s *PostgresStore) MockSecret(ctx context.Context) ([]byte, error) {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, err
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO igloo_auth.secrets (name, value) VALUES ('mock-auth', $1) ON CONFLICT (name) DO NOTHING`, secret)
	if err != nil {
		return nil, err
	}
	err = s.pool.QueryRow(ctx, `SELECT value FROM igloo_auth.secrets WHERE name = 'mock-auth'`).Scan(&secret)
	return secret, err
}

func (s *PostgresStore) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *PostgresStore) Close() { s.pool.Close() }
