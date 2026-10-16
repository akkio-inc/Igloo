package policy

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MemoryStore keeps the catalog in memory, for tests and for servers
// without a metadata database.
type MemoryStore struct {
	mu      sync.Mutex
	version int64
	catalog Catalog
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

func (m *MemoryStore) Load(context.Context) (*Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return NewSnapshot(m.version, m.catalog.clone()), nil
}

func (m *MemoryStore) Version(context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.version, nil
}

func (m *MemoryStore) Update(_ context.Context, change func(*Catalog) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.catalog.clone()
	if err := change(&c); err != nil {
		return err
	}
	m.catalog = c
	m.version++
	return nil
}

func (m *MemoryStore) Close() {}

// PostgresStore keeps the catalog in the metadata database, in
// igloo_policy.catalog, so every replica shares it.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// migrationKey serializes creating the policy tables across replicas
// starting at the same time.
const migrationKey int64 = 0x49676c6f6f506f6c // "IglooPol"

var migrations = []string{
	`CREATE SCHEMA IF NOT EXISTS igloo_policy`,
	// One row: the catalog as JSON and its version, which every change
	// increments.
	`CREATE TABLE IF NOT EXISTS igloo_policy.catalog (
		id         smallint PRIMARY KEY CHECK (id = 1),
		version    bigint NOT NULL,
		doc        jsonb NOT NULL,
		updated_at timestamptz NOT NULL DEFAULT now()
	)`,
	`INSERT INTO igloo_policy.catalog (id, version, doc) VALUES (1, 0, '{}') ON CONFLICT (id) DO NOTHING`,
}

// OpenPostgres connects to url with a small pool of its own and creates
// the policy table if needed.
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
		return nil, fmt.Errorf("create policy tables: %w", err)
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) Load(ctx context.Context) (*Snapshot, error) {
	var (
		version int64
		doc     []byte
	)
	if err := s.pool.QueryRow(ctx, `SELECT version, doc FROM igloo_policy.catalog WHERE id = 1`).Scan(&version, &doc); err != nil {
		return nil, err
	}
	var c Catalog
	if err := json.Unmarshal(doc, &c); err != nil {
		return nil, fmt.Errorf("read policy catalog: %w", err)
	}
	return NewSnapshot(version, c), nil
}

func (s *PostgresStore) Version(ctx context.Context) (int64, error) {
	var version int64
	err := s.pool.QueryRow(ctx, `SELECT version FROM igloo_policy.catalog WHERE id = 1`).Scan(&version)
	return version, err
}

// Update locks the row, so changes from several replicas apply one after
// the other.
func (s *PostgresStore) Update(ctx context.Context, change func(*Catalog) error) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var doc []byte
		if err := tx.QueryRow(ctx, `SELECT doc FROM igloo_policy.catalog WHERE id = 1 FOR UPDATE`).Scan(&doc); err != nil {
			return err
		}
		var c Catalog
		if err := json.Unmarshal(doc, &c); err != nil {
			return fmt.Errorf("read policy catalog: %w", err)
		}
		if err := change(&c); err != nil {
			return err
		}
		doc, err := json.Marshal(&c)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE igloo_policy.catalog SET version = version + 1, doc = $1, updated_at = now() WHERE id = 1`, doc)
		return err
	})
}

func (s *PostgresStore) Close() { s.pool.Close() }

var (
	_ Store = (*MemoryStore)(nil)
	_ Store = (*PostgresStore)(nil)
)
