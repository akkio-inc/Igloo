package erasure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore keeps requests in the "igloo_erasure" schema of the
// metadata database, shared by every replica, so they survive restarts.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// migrationKey serializes creating the tables across replicas starting at
// the same time.
const migrationKey int64 = 0x49676c6f6f457273 // "IglooErs"

var migrations = []string{
	`CREATE SCHEMA IF NOT EXISTS igloo_erasure`,
	// A request is one document: its progress changes shape as it goes.
	`CREATE TABLE IF NOT EXISTS igloo_erasure.requests (
		request_id text PRIMARY KEY,
		created_at timestamptz NOT NULL,
		doc        jsonb NOT NULL
	)`,
	// Files a request deletes once nothing references them, while it is
	// open: their paths can hold identifiers, as partition values.
	`CREATE TABLE IF NOT EXISTS igloo_erasure.files (
		request_id text NOT NULL,
		path       text NOT NULL,
		table_name text NOT NULL,
		kind       text NOT NULL,
		deleted_at timestamptz,
		PRIMARY KEY (request_id, path)
	)`,
	`CREATE INDEX IF NOT EXISTS files_deleted ON igloo_erasure.files (deleted_at) WHERE deleted_at IS NOT NULL`,
	// The replicas' read caches, and up to when each was emptied of what
	// requests deleted.
	`CREATE TABLE IF NOT EXISTS igloo_erasure.caches (
		cache_id text PRIMARY KEY,
		host     text NOT NULL,
		seen_at  timestamptz NOT NULL
	)`,
	`ALTER TABLE igloo_erasure.caches ADD COLUMN IF NOT EXISTS emptied_through timestamptz`,
	`DROP TABLE IF EXISTS igloo_erasure.purges`,
}

// OpenPostgres connects to url with a small pool of its own and creates
// the tables if needed.
func OpenPostgres(ctx context.Context, url string) (*PostgresStore, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 3
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
		return nil, fmt.Errorf("create erasure tables: %w", err)
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) Create(ctx context.Context, r *Request) error {
	doc, err := json.Marshal(r)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO igloo_erasure.requests (request_id, created_at, doc) VALUES ($1, $2, $3)`,
		r.ID, r.Created, doc)
	return err
}

func (s *PostgresStore) Get(ctx context.Context, id string) (*Request, error) {
	var doc []byte
	err := s.pool.QueryRow(ctx, `SELECT doc FROM igloo_erasure.requests WHERE request_id = $1`, id).Scan(&doc)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var r Request
	return &r, json.Unmarshal(doc, &r)
}

func (s *PostgresStore) List(ctx context.Context) ([]*Request, error) {
	rows, err := s.pool.Query(ctx, `SELECT doc FROM igloo_erasure.requests ORDER BY created_at DESC, request_id DESC`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*Request, error) {
		var doc []byte
		if err := row.Scan(&doc); err != nil {
			return nil, err
		}
		var r Request
		return &r, json.Unmarshal(doc, &r)
	})
}

func (s *PostgresStore) Save(ctx context.Context, r *Request) error {
	doc, err := json.Marshal(r)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE igloo_erasure.requests SET doc = $2 WHERE request_id = $1`, r.ID, doc)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

func (s *PostgresStore) AddFiles(ctx context.Context, id string, files []File) error {
	if len(files) == 0 {
		return nil
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		for _, f := range files {
			if _, err := tx.Exec(ctx, `INSERT INTO igloo_erasure.files (request_id, path, table_name, kind)
				VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`, id, f.Path, f.Table, f.Kind); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *PostgresStore) Files(ctx context.Context, id string) ([]File, error) {
	rows, err := s.pool.Query(ctx, `SELECT path, table_name, kind, deleted_at FROM igloo_erasure.files
		WHERE request_id = $1 ORDER BY path`, id)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (File, error) {
		var (
			f       File
			deleted *time.Time
		)
		err := row.Scan(&f.Path, &f.Table, &f.Kind, &deleted)
		if deleted != nil {
			f.Deleted = *deleted
		}
		return f, err
	})
}

func (s *PostgresStore) Deleted(ctx context.Context, id string, paths []string, at time.Time) error {
	if len(paths) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `UPDATE igloo_erasure.files SET deleted_at = $3
		WHERE request_id = $1 AND path = ANY ($2) AND deleted_at IS NULL`, id, paths, at)
	return err
}

func (s *PostgresStore) Forget(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM igloo_erasure.files WHERE request_id = $1`, id)
	return err
}

func (s *PostgresStore) Seen(ctx context.Context, cache, host string, at time.Time) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO igloo_erasure.caches (cache_id, host, seen_at) VALUES ($1, $2, $3)
		ON CONFLICT (cache_id) DO UPDATE SET host = excluded.host, seen_at = greatest(igloo_erasure.caches.seen_at, excluded.seen_at)`,
		cache, host, at)
	return err
}

func (s *PostgresStore) Caches(ctx context.Context) ([]Cache, error) {
	rows, err := s.pool.Query(ctx, `SELECT cache_id, host, seen_at, emptied_through FROM igloo_erasure.caches ORDER BY cache_id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Cache, error) {
		var (
			c       Cache
			emptied *time.Time
		)
		err := row.Scan(&c.ID, &c.Host, &c.Seen, &emptied)
		if emptied != nil {
			c.Emptied = *emptied
		}
		return c, err
	})
}

func (s *PostgresStore) Emptied(ctx context.Context, cache string, through time.Time) error {
	_, err := s.pool.Exec(ctx, `UPDATE igloo_erasure.caches SET emptied_through = greatest(emptied_through, $2) WHERE cache_id = $1`,
		cache, through)
	return err
}

func (s *PostgresStore) LatestDeletion(ctx context.Context) (time.Time, error) {
	var at *time.Time
	err := s.pool.QueryRow(ctx, `SELECT max((doc->>'deleted_through')::timestamptz) FROM igloo_erasure.requests`).Scan(&at)
	if at == nil {
		return time.Time{}, err
	}
	return *at, err
}

func (s *PostgresStore) Close() { s.pool.Close() }

// MemoryStore keeps requests in memory, for one replica without a
// metadata database. They are lost on restart.
type MemoryStore struct {
	mu       sync.Mutex
	requests map[string][]byte
	files    map[string][]File
	caches   map[string]Cache
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{requests: map[string][]byte{}, files: map[string][]File{}, caches: map[string]Cache{}}
}

func (m *MemoryStore) Create(_ context.Context, r *Request) error {
	doc, err := json.Marshal(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.requests[r.ID]; ok {
		return fmt.Errorf("request %s already exists", r.ID)
	}
	m.requests[r.ID] = doc
	return nil
}

func (m *MemoryStore) Get(_ context.Context, id string) (*Request, error) {
	m.mu.Lock()
	doc, ok := m.requests[id]
	m.mu.Unlock()
	if !ok {
		return nil, ErrNotFound
	}
	var r Request
	return &r, json.Unmarshal(doc, &r)
}

func (m *MemoryStore) List(ctx context.Context) ([]*Request, error) {
	m.mu.Lock()
	ids := make([]string, 0, len(m.requests))
	for id := range m.requests {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	var out []*Request
	for _, id := range ids {
		r, err := m.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Created.Equal(out[j].Created) {
			return out[i].Created.After(out[j].Created)
		}
		return out[i].ID > out[j].ID
	})
	return out, nil
}

func (m *MemoryStore) Save(_ context.Context, r *Request) error {
	doc, err := json.Marshal(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.requests[r.ID]; !ok {
		return ErrNotFound
	}
	m.requests[r.ID] = doc
	return nil
}

func (m *MemoryStore) AddFiles(_ context.Context, id string, files []File) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, f := range files {
		if !slices.ContainsFunc(m.files[id], func(g File) bool { return g.Path == f.Path }) {
			f.Deleted = time.Time{}
			m.files[id] = append(m.files[id], f)
		}
	}
	return nil
}

func (m *MemoryStore) Files(_ context.Context, id string) ([]File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := slices.Clone(m.files[id])
	slices.SortFunc(out, func(a, b File) int { return strings.Compare(a.Path, b.Path) })
	return out, nil
}

func (m *MemoryStore) Deleted(_ context.Context, id string, paths []string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, f := range m.files[id] {
		if f.Deleted.IsZero() && slices.Contains(paths, f.Path) {
			m.files[id][i].Deleted = at
		}
	}
	return nil
}

func (m *MemoryStore) Forget(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.files, id)
	return nil
}

func (m *MemoryStore) Seen(_ context.Context, cache, host string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.caches[cache]
	if !ok || at.After(c.Seen) {
		c.ID, c.Host, c.Seen = cache, host, at
	}
	m.caches[cache] = c
	return nil
}

func (m *MemoryStore) Caches(context.Context) ([]Cache, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Cache, 0, len(m.caches))
	for _, c := range m.caches {
		out = append(out, c)
	}
	slices.SortFunc(out, func(a, b Cache) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

func (m *MemoryStore) Emptied(_ context.Context, cache string, through time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.caches[cache]; ok && through.After(c.Emptied) {
		c.Emptied = through
		m.caches[cache] = c
	}
	return nil
}

func (m *MemoryStore) LatestDeletion(context.Context) (time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var latest time.Time
	for _, doc := range m.requests {
		var r Request
		if err := json.Unmarshal(doc, &r); err != nil {
			return time.Time{}, err
		}
		if r.DeletedThrough.After(latest) {
			latest = r.DeletedThrough
		}
	}
	return latest, nil
}

func (m *MemoryStore) Close() {}

var (
	_ Store = (*PostgresStore)(nil)
	_ Store = (*MemoryStore)(nil)
)
