package clone

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Share records that a clone's metadata may reference files under the
// location of another table: the table it was cloned from, or one that
// table shared files with in turn.
type Share struct {
	Clone uuid.UUID
	// Location is the other table's location, without a trailing slash.
	Location string
	// Schema and Table name the clone where it was last seen.
	Schema, Table string
	// Created is when the share was first recorded.
	Created time.Time
}

// Store keeps the shares, and the locks that keep cloning a table and
// deleting its files apart. A Postgres store serves every replica; a
// memory store serves one, and maintenance rebuilds it from the clones'
// own metadata (SharesProperty) before it deletes anything.
type Store interface {
	// Add records shares. One recorded already keeps its Created time and
	// takes the new name.
	Add(ctx context.Context, shares []Share) error
	// Sharers lists the shares of the files under location.
	Sharers(ctx context.Context, location string) ([]Share, error)
	// Of lists the shares of a clone.
	Of(ctx context.Context, clone uuid.UUID) ([]Share, error)
	// All lists every share.
	All(ctx context.Context) ([]Share, error)
	// Remove forgets that clone shares the files under location.
	Remove(ctx context.Context, clone uuid.UUID, location string) error
	// Lock takes the lock of every location: waiting for each, or with
	// wait false, giving up (ok false) if another holds one. release must
	// be called once ok.
	Lock(ctx context.Context, locations []string, wait bool) (release func(), ok bool, err error)
	// LockLease is Lock, the locks held as a Lease, which says whether
	// they still are.
	LockLease(ctx context.Context, locations []string, wait bool) (lease Lease, ok bool, err error)
	// Authoritative reports whether the Store has every clone the catalog
	// has, so that what clones read can be told from it alone, without
	// reading every table's properties. Clones are recorded as they are
	// made, and only Igloo sets the properties that make a table one, so
	// a Postgres store always has; a memory store has once maintenance
	// has rebuilt it since it started (MemoryStore.Rebuilt).
	Authoritative() bool
	Close()
}

// Lease is locks that can tell whether they are still held.
type Lease interface {
	// Held returns an error once a lock is lost: a Postgres store's are
	// when their connection drops, and another replica may take them then.
	Held(ctx context.Context) error
	Release()
}

// lockKeys are the advisory lock keys of locations, sorted, so two callers
// taking several always take them in the same order.
func lockKeys(locations []string) []int64 {
	var keys []int64
	for _, loc := range locations {
		h := fnv.New64a()
		_, _ = h.Write([]byte("igloo/clone/" + trimSlash(loc)))
		keys = append(keys, int64(h.Sum64()))
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

func trimSlash(loc string) string { return strings.TrimRight(loc, "/") }

// MemoryStore is a Store for one replica.
type MemoryStore struct {
	mu     sync.Mutex
	shares map[shareKey]Share
	locks  map[int64]chan struct{}
	// rebuilt says a maintenance pass has recorded what every table
	// declares (see Rebuilt).
	rebuilt atomic.Bool
}

type shareKey struct {
	clone    uuid.UUID
	location string
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{shares: map[shareKey]Share{}, locks: map[int64]chan struct{}{}}
}

// Rebuilt notes that a maintenance pass has recorded the shares every
// table of the catalog declares: from then on the store has every clone,
// as those made since are recorded as they are made.
func (m *MemoryStore) Rebuilt() { m.rebuilt.Store(true) }

// Authoritative implements Store: once Rebuilt.
func (m *MemoryStore) Authoritative() bool { return m.rebuilt.Load() }

func (m *MemoryStore) Add(_ context.Context, shares []Share) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range shares {
		s.Location = trimSlash(s.Location)
		k := shareKey{s.Clone, s.Location}
		if old, ok := m.shares[k]; ok {
			s.Created = old.Created
		} else if s.Created.IsZero() {
			s.Created = time.Now()
		}
		m.shares[k] = s
	}
	return nil
}

func (m *MemoryStore) list(keep func(Share) bool) []Share {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Share
	for _, s := range m.shares {
		if keep(s) {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Location != out[j].Location {
			return out[i].Location < out[j].Location
		}
		return out[i].Clone.String() < out[j].Clone.String()
	})
	return out
}

func (m *MemoryStore) Sharers(_ context.Context, location string) ([]Share, error) {
	location = trimSlash(location)
	return m.list(func(s Share) bool { return s.Location == location }), nil
}

func (m *MemoryStore) Of(_ context.Context, clone uuid.UUID) ([]Share, error) {
	return m.list(func(s Share) bool { return s.Clone == clone }), nil
}

func (m *MemoryStore) All(context.Context) ([]Share, error) {
	return m.list(func(Share) bool { return true }), nil
}

func (m *MemoryStore) Remove(_ context.Context, clone uuid.UUID, location string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.shares, shareKey{clone, trimSlash(location)})
	return nil
}

func (m *MemoryStore) LockLease(ctx context.Context, locations []string, wait bool) (Lease, bool, error) {
	release, ok, err := m.Lock(ctx, locations, wait)
	if !ok || err != nil {
		return nil, ok, err
	}
	return memLease(release), true, nil
}

// memLease is locks of one process, which it cannot lose.
type memLease func()

func (memLease) Held(context.Context) error { return nil }
func (l memLease) Release()                 { l() }

func (m *MemoryStore) Lock(ctx context.Context, locations []string, wait bool) (func(), bool, error) {
	var held []chan struct{}
	release := func() {
		for _, c := range held {
			<-c
		}
	}
	for _, k := range lockKeys(locations) {
		m.mu.Lock()
		c := m.locks[k]
		if c == nil {
			c = make(chan struct{}, 1)
			m.locks[k] = c
		}
		m.mu.Unlock()
		if !wait {
			select {
			case c <- struct{}{}:
				held = append(held, c)
				continue
			default:
				release()
				return nil, false, nil
			}
		}
		select {
		case c <- struct{}{}:
			held = append(held, c)
		case <-ctx.Done():
			release()
			return nil, false, ctx.Err()
		}
	}
	return release, true, nil
}

func (m *MemoryStore) Close() {}

// PostgresStore keeps the shares in the metadata database, in
// igloo_clones.shares, and locks with session-level advisory locks, which
// Postgres releases if the replica holding them dies.
type PostgresStore struct {
	pool *pgxpool.Pool
	// locks holds a connection for each lock held. It is a pool of its
	// own, so callers holding locks always find a connection in pool for
	// the shares.
	locks *pgxpool.Pool
}

// migrationKey serializes creating the table across replicas starting at
// the same time.
const migrationKey int64 = 0x49676c6f6f436c6e // "IglooCln"

var migrations = []string{
	`CREATE SCHEMA IF NOT EXISTS igloo_clones`,
	`CREATE TABLE IF NOT EXISTS igloo_clones.shares (
		clone_uuid   uuid NOT NULL,
		location     text NOT NULL,
		clone_schema text NOT NULL,
		clone_name   text NOT NULL,
		created_at   timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (clone_uuid, location)
	)`,
	`CREATE INDEX IF NOT EXISTS shares_location ON igloo_clones.shares (location)`,
}

// OpenPostgres connects to url with small pools of its own and creates the
// table if needed.
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
	lockCfg := cfg.Copy()
	lockCfg.MaxConns = 16
	lockCfg.MaxConnIdleTime = time.Minute
	locks, err := pgxpool.NewWithConfig(ctx, lockCfg)
	if err != nil {
		pool.Close()
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
		locks.Close()
		return nil, fmt.Errorf("create clone tables: %w", err)
	}
	return &PostgresStore{pool: pool, locks: locks}, nil
}

func (s *PostgresStore) Add(ctx context.Context, shares []Share) error {
	if len(shares) == 0 {
		return nil
	}
	b := &pgx.Batch{}
	for _, sh := range shares {
		created := sh.Created
		if created.IsZero() {
			created = time.Now()
		}
		b.Queue(`INSERT INTO igloo_clones.shares (clone_uuid, location, clone_schema, clone_name, created_at)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (clone_uuid, location) DO UPDATE SET clone_schema = excluded.clone_schema, clone_name = excluded.clone_name`,
			sh.Clone, trimSlash(sh.Location), sh.Schema, sh.Table, created)
	}
	return s.pool.SendBatch(ctx, b).Close()
}

func (s *PostgresStore) query(ctx context.Context, where string, args ...any) ([]Share, error) {
	rows, err := s.pool.Query(ctx, `SELECT clone_uuid, location, clone_schema, clone_name, created_at
		FROM igloo_clones.shares `+where+` ORDER BY location, clone_uuid`, args...)
	if err != nil {
		return nil, err
	}
	var (
		out []Share
		sh  Share
	)
	_, err = pgx.ForEachRow(rows, []any{&sh.Clone, &sh.Location, &sh.Schema, &sh.Table, &sh.Created}, func() error {
		out = append(out, sh)
		return nil
	})
	return out, err
}

func (s *PostgresStore) Sharers(ctx context.Context, location string) ([]Share, error) {
	return s.query(ctx, "WHERE location = $1", trimSlash(location))
}

func (s *PostgresStore) Of(ctx context.Context, clone uuid.UUID) ([]Share, error) {
	return s.query(ctx, "WHERE clone_uuid = $1", clone)
}

func (s *PostgresStore) All(ctx context.Context) ([]Share, error) {
	return s.query(ctx, "")
}

func (s *PostgresStore) Remove(ctx context.Context, clone uuid.UUID, location string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM igloo_clones.shares WHERE clone_uuid = $1 AND location = $2`, clone, trimSlash(location))
	return err
}

func (s *PostgresStore) Lock(ctx context.Context, locations []string, wait bool) (func(), bool, error) {
	lease, ok, err := s.LockLease(ctx, locations, wait)
	if !ok || err != nil {
		return nil, ok, err
	}
	return lease.Release, true, nil
}

// pgLease is advisory locks held by one connection of the locks pool.
type pgLease struct {
	conn *pgxpool.Conn
	held []int64
}

// Held asks the locks' connection whether it holds every one: one that
// dropped has none, and another session may have taken them since.
func (l *pgLease) Held(ctx context.Context) error {
	var n int
	err := l.conn.QueryRow(ctx, `SELECT count(*) FROM unnest($1::bigint[]) k WHERE EXISTS (SELECT FROM pg_locks
		WHERE locktype = 'advisory' AND granted AND pid = pg_backend_pid()
			AND classid::bigint = (k >> 32) & 4294967295 AND objid::bigint = k & 4294967295 AND objsubid = 1)`, l.held).Scan(&n)
	switch {
	case err != nil:
		return fmt.Errorf("the lock is lost: %w", err)
	case n != len(l.held):
		return errors.New("the lock is lost")
	}
	return nil
}

func (l *pgLease) Release() {
	// Unlock even when ctx was canceled. If this fails the connection is
	// closed instead, which also releases the locks.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, k := range l.held {
		if _, err := l.conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", k); err != nil {
			_ = l.conn.Conn().Close(ctx)
			break
		}
	}
	l.conn.Release()
}

func (s *PostgresStore) LockLease(ctx context.Context, locations []string, wait bool) (Lease, bool, error) {
	conn, err := s.locks.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	l := &pgLease{conn: conn}
	for _, k := range lockKeys(locations) {
		ok := true
		if wait {
			_, err = conn.Exec(ctx, "SELECT pg_advisory_lock($1)", k)
		} else {
			err = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", k).Scan(&ok)
		}
		if err != nil {
			// A wait cut short may still be granted before the server sees
			// the cancellation; closing the connection ends its session,
			// and with it any lock it got.
			_ = conn.Conn().Close(context.Background())
			conn.Release()
			return nil, false, err
		}
		if !ok {
			l.Release()
			return nil, false, nil
		}
		l.held = append(l.held, k)
	}
	return l, true, nil
}

// Authoritative implements Store: always.
func (s *PostgresStore) Authoritative() bool { return true }

func (s *PostgresStore) Close() {
	s.pool.Close()
	s.locks.Close()
}
