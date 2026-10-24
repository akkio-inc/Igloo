package iddomain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MemoryStore keeps everything in memory, for tests and for servers
// without a metadata database.
type MemoryStore struct {
	mu       sync.Mutex
	version  int64
	catalog  Catalog
	counters map[string]counter
	leases   map[string]lease
}

type counter struct {
	generation int
	next       int64
}

type lease struct {
	holder  string
	expires time.Time
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{counters: map[string]counter{}, leases: map[string]lease{}}
}

func (m *MemoryStore) Load(context.Context) (*Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &Snapshot{Version: m.version, Catalog: m.catalog.clone()}, nil
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

func (m *MemoryStore) Allocate(_ context.Context, domain string, generation int, n int64) (int64, error) {
	if n <= 0 {
		return 0, fmt.Errorf("%w: allocate %d ids", ErrInvalid, n)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.counters[domain]
	switch {
	case c.generation > generation:
		return 0, fmt.Errorf("%w: ID domain %s is at generation %d, not %d", ErrInvalid, domain, c.generation, generation)
	case c.generation < generation:
		c = counter{generation: generation}
	}
	if c.next+n > MaxIDs {
		return 0, fmt.Errorf("ID domain %s %w", domain, ErrExhausted)
	}
	first := c.next
	c.next += n
	m.counters[domain] = c
	return first, nil
}

func (m *MemoryStore) Next(_ context.Context, domain string) (int64, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.counters[domain]
	return c.next, c.generation, nil
}

func (m *MemoryStore) Lock(ctx context.Context, domain, holder string, ttl, wait time.Duration) (func(), error) {
	err := waitLease(ctx, wait, func() (bool, error) {
		m.mu.Lock()
		defer m.mu.Unlock()
		now := time.Now()
		if l, ok := m.leases[domain]; ok && l.holder != holder && l.expires.After(now) {
			return false, nil
		}
		m.leases[domain] = lease{holder: holder, expires: now.Add(ttl)}
		return true, nil
	})
	if err != nil {
		return nil, wrapBusy(domain, err)
	}
	return func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if l, ok := m.leases[domain]; ok && l.holder == holder {
			delete(m.leases, domain)
		}
	}, nil
}

func (m *MemoryStore) Close() {}

func wrapBusy(domain string, err error) error {
	if errors.Is(err, ErrBusy) {
		return fmt.Errorf("ID domain %s %w", domain, ErrBusy)
	}
	return err
}

// PostgresStore keeps everything in the metadata database, in the schema
// igloo_iddomain, so every replica shares it.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// migrationKey serializes creating the tables across replicas starting at
// the same time.
const migrationKey int64 = 0x49676c6f6f494464 // "IglooIDd"

var migrations = []string{
	`CREATE SCHEMA IF NOT EXISTS igloo_iddomain`,
	// One row: the domains and members as JSON, and their version, which
	// every change increments.
	`CREATE TABLE IF NOT EXISTS igloo_iddomain.catalog (
		id         smallint PRIMARY KEY CHECK (id = 1),
		version    bigint NOT NULL,
		doc        jsonb NOT NULL,
		updated_at timestamptz NOT NULL DEFAULT now()
	)`,
	`INSERT INTO igloo_iddomain.catalog (id, version, doc) VALUES (1, 0, '{}') ON CONFLICT (id) DO NOTHING`,
	// The next id each domain hands out, which only goes up within a
	// generation.
	`CREATE TABLE IF NOT EXISTS igloo_iddomain.counters (
		domain     text PRIMARY KEY,
		generation integer NOT NULL,
		next_id    bigint NOT NULL CHECK (next_id >= 0 AND next_id <= 4294967296)
	)`,
	// Each domain's assignment lease, while someone holds it.
	`CREATE TABLE IF NOT EXISTS igloo_iddomain.leases (
		domain     text PRIMARY KEY,
		holder     text NOT NULL,
		expires_at timestamptz NOT NULL
	)`,
}

// OpenPostgres connects to url with a small pool of its own and creates the
// tables if needed.
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
		return nil, fmt.Errorf("create ID domain tables: %w", err)
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) Load(ctx context.Context) (*Snapshot, error) {
	var (
		version int64
		doc     []byte
	)
	if err := s.pool.QueryRow(ctx, `SELECT version, doc FROM igloo_iddomain.catalog WHERE id = 1`).Scan(&version, &doc); err != nil {
		return nil, err
	}
	var c Catalog
	if err := json.Unmarshal(doc, &c); err != nil {
		return nil, fmt.Errorf("read ID domains: %w", err)
	}
	return &Snapshot{Version: version, Catalog: c}, nil
}

func (s *PostgresStore) Version(ctx context.Context) (int64, error) {
	var version int64
	err := s.pool.QueryRow(ctx, `SELECT version FROM igloo_iddomain.catalog WHERE id = 1`).Scan(&version)
	return version, err
}

// Update locks the row, so changes from several replicas apply one after
// the other.
func (s *PostgresStore) Update(ctx context.Context, change func(*Catalog) error) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var doc []byte
		if err := tx.QueryRow(ctx, `SELECT doc FROM igloo_iddomain.catalog WHERE id = 1 FOR UPDATE`).Scan(&doc); err != nil {
			return err
		}
		var c Catalog
		if err := json.Unmarshal(doc, &c); err != nil {
			return fmt.Errorf("read ID domains: %w", err)
		}
		if err := change(&c); err != nil {
			return err
		}
		doc, err := json.Marshal(&c)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE igloo_iddomain.catalog SET version = version + 1, doc = $1, updated_at = now() WHERE id = 1`, doc)
		return err
	})
}

func (s *PostgresStore) Allocate(ctx context.Context, domain string, generation int, n int64) (int64, error) {
	if n <= 0 {
		return 0, fmt.Errorf("%w: allocate %d ids", ErrInvalid, n)
	}
	var first int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO igloo_iddomain.counters (domain, generation, next_id) VALUES ($1, $2, 0)
			ON CONFLICT (domain) DO NOTHING`, domain, generation); err != nil {
			return err
		}
		var gen int
		var next int64
		if err := tx.QueryRow(ctx, `SELECT generation, next_id FROM igloo_iddomain.counters WHERE domain = $1 FOR UPDATE`, domain).Scan(&gen, &next); err != nil {
			return err
		}
		switch {
		case gen > generation:
			return fmt.Errorf("%w: ID domain %s is at generation %d, not %d", ErrInvalid, domain, gen, generation)
		case gen < generation:
			next = 0
		}
		if next+n > MaxIDs {
			return fmt.Errorf("ID domain %s %w", domain, ErrExhausted)
		}
		first = next
		_, err := tx.Exec(ctx, `UPDATE igloo_iddomain.counters SET generation = $2, next_id = $3 WHERE domain = $1`, domain, generation, next+n)
		return err
	})
	return first, err
}

func (s *PostgresStore) Next(ctx context.Context, domain string) (int64, int, error) {
	var next int64
	var gen int
	err := s.pool.QueryRow(ctx, `SELECT next_id, generation FROM igloo_iddomain.counters WHERE domain = $1`, domain).Scan(&next, &gen)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, nil
	}
	return next, gen, err
}

func (s *PostgresStore) Lock(ctx context.Context, domain, holder string, ttl, wait time.Duration) (func(), error) {
	err := waitLease(ctx, wait, func() (bool, error) {
		var got string
		err := s.pool.QueryRow(ctx, `INSERT INTO igloo_iddomain.leases (domain, holder, expires_at)
			VALUES ($1, $2, now() + make_interval(secs => $3))
			ON CONFLICT (domain) DO UPDATE SET holder = EXCLUDED.holder, expires_at = EXCLUDED.expires_at
			WHERE igloo_iddomain.leases.expires_at < now() OR igloo_iddomain.leases.holder = EXCLUDED.holder
			RETURNING holder`, domain, holder, ttl.Seconds()).Scan(&got)
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return err == nil && got == holder, err
	})
	if err != nil {
		return nil, wrapBusy(domain, err)
	}
	return func() {
		// A lease that cannot be let go now runs out at its expiry.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = s.pool.Exec(ctx, `DELETE FROM igloo_iddomain.leases WHERE domain = $1 AND holder = $2`, domain, holder)
	}, nil
}

func (s *PostgresStore) Close() { s.pool.Close() }

var (
	_ Store = (*MemoryStore)(nil)
	_ Store = (*PostgresStore)(nil)
)
