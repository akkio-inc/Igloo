package ai

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"
)

// Store keeps AI integrations, who may use them, and the cache of results.
// PostgresStore keeps them in the metadata database for every replica;
// MemoryStore in one replica's memory.
type Store interface {
	CreateIntegration(ctx context.Context, in Integration, replace bool) error
	Integration(ctx context.Context, name string) (Integration, error)
	Integrations(ctx context.Context) ([]Integration, error)
	UpdateIntegration(ctx context.Context, in Integration) error
	DropIntegration(ctx context.Context, name string) error
	GrantUsage(ctx context.Context, u Grant) error
	RevokeUsage(ctx context.Context, integration, granteeType, grantee string) error
	Grants(ctx context.Context) ([]Grant, error)
	// ForgetGrantee drops the grants of a user or role that is gone.
	ForgetGrantee(ctx context.Context, granteeType, grantee string) error

	// State returns the cache's erasure epoch and a version that changes
	// whenever integrations or grants do. A statement reads the epoch as
	// it starts, before it reads any table, and tags what it caches with
	// it (see Purge).
	State(ctx context.Context) (epoch, version int64, err error)
	// Lookup returns the live entries of keys, by key.
	Lookup(ctx context.Context, keys []string, now time.Time) (map[string]CacheEntry, error)
	// Put keeps entries, all of one statement's, which began at epoch, and
	// reports whether it did: once the epoch has moved on, as a purge
	// moves it, a statement that began before keeps nothing more, since it
	// may have read rows the purge was for. A purge and a Put never
	// interleave. An entry put again keeps the smaller epoch and the
	// tables of both.
	Put(ctx context.Context, epoch int64, entries []CacheEntry) (bool, error)
	// Purge moves the epoch on, so that no statement begun before it keeps
	// anything more, then deletes the entries derived from any of tables,
	// or from what Igloo could not name (AnyTable), and returns how many it
	// deleted.
	Purge(ctx context.Context, tables []string) (int64, error)
	// Remaining counts the entries derived from any of tables, or from
	// AnyTable, by statements begun before the last purge: none once a
	// purge of the tables has run.
	Remaining(ctx context.Context, tables []string) (int64, error)
	// Expire deletes the entries that expired before t.
	Expire(ctx context.Context, before time.Time) (int64, error)
	Close()
}

// AnyTable tags a cache entry whose statement read something Igloo could
// not name, such as a file: every erasure request purges it.
const AnyTable = "*"

// CacheEntry is a result kept for a function's input.
type CacheEntry struct {
	// Key is the hash of the function, the provider and model, the
	// template's version, the options and the input (see cacheKey).
	Key string
	// Function and Model say what made it, for operators.
	Function, Model string
	// Value is the result, as JSON.
	Value []byte
	// Tables are the catalog tables, namespace.name in lower case, that
	// the statement which made it read: what it may be derived from.
	Tables []string
	// Epoch is the erasure epoch when that statement started.
	Epoch            int64
	Created, Expires time.Time
}

// tagged reports whether e is derived from any of tables.
func (e CacheEntry) tagged(tables map[string]bool) bool {
	for _, t := range e.Tables {
		if t == AnyTable || tables[t] {
			return true
		}
	}
	return false
}

func tableSet(tables []string) map[string]bool {
	m := map[string]bool{}
	for _, t := range tables {
		m[strings.ToLower(t)] = true
	}
	return m
}

// MemoryStore keeps everything in memory, for one replica without a
// metadata database.
type MemoryStore struct {
	mu           sync.Mutex
	integrations map[string]Integration
	usage        []Grant
	cache        map[string]CacheEntry
	epoch        int64
	version      int64
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{integrations: map[string]Integration{}, cache: map[string]CacheEntry{}, epoch: 1}
}

func (m *MemoryStore) CreateIntegration(_ context.Context, in Integration, replace bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.integrations[in.Name]; ok && !replace {
		return ErrExists
	}
	if in.CreatedAt.IsZero() {
		in.CreatedAt = time.Now()
	}
	m.integrations[in.Name] = in.Clone()
	m.version++
	return nil
}

func (m *MemoryStore) Integration(_ context.Context, name string) (Integration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	in, ok := m.integrations[name]
	if !ok {
		return Integration{}, ErrNotFound
	}
	return in.Clone(), nil
}

func (m *MemoryStore) Integrations(context.Context) ([]Integration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Integration
	for _, in := range m.integrations {
		out = append(out, in.Clone())
	}
	slices.SortFunc(out, func(a, b Integration) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

func (m *MemoryStore) UpdateIntegration(_ context.Context, in Integration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	old, ok := m.integrations[in.Name]
	if !ok {
		return ErrNotFound
	}
	in.CreatedAt, in.CreatedBy = old.CreatedAt, old.CreatedBy
	m.integrations[in.Name] = in.Clone()
	m.version++
	return nil
}

func (m *MemoryStore) DropIntegration(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.integrations[name]; !ok {
		return ErrNotFound
	}
	delete(m.integrations, name)
	m.usage = slices.DeleteFunc(m.usage, func(u Grant) bool { return u.Integration == name })
	m.version++
	return nil
}

func (m *MemoryStore) GrantUsage(_ context.Context, u Grant) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.integrations[u.Integration]; !ok {
		return ErrNotFound
	}
	for _, x := range m.usage {
		if x.Integration == u.Integration && x.GranteeType == u.GranteeType && x.Grantee == u.Grantee {
			return nil
		}
	}
	if u.GrantedAt.IsZero() {
		u.GrantedAt = time.Now()
	}
	m.usage = append(m.usage, u)
	m.version++
	return nil
}

func (m *MemoryStore) RevokeUsage(_ context.Context, integration, granteeType, grantee string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.usage = slices.DeleteFunc(m.usage, func(u Grant) bool {
		return u.Integration == integration && u.GranteeType == granteeType && u.Grantee == grantee
	})
	m.version++
	return nil
}

func (m *MemoryStore) Grants(context.Context) ([]Grant, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.usage), nil
}

func (m *MemoryStore) ForgetGrantee(_ context.Context, granteeType, grantee string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.usage = slices.DeleteFunc(m.usage, func(u Grant) bool { return u.GranteeType == granteeType && u.Grantee == grantee })
	m.version++
	return nil
}

func (m *MemoryStore) State(context.Context) (int64, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.epoch, m.version, nil
}

func (m *MemoryStore) Lookup(_ context.Context, keys []string, now time.Time) (map[string]CacheEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]CacheEntry{}
	for _, k := range keys {
		if e, ok := m.cache[k]; ok && now.Before(e.Expires) {
			out[k] = e
		}
	}
	return out, nil
}

func (m *MemoryStore) Put(_ context.Context, epoch int64, entries []CacheEntry) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if epoch != m.epoch {
		return false, nil
	}
	for _, e := range entries {
		e.Tables = slices.Clone(e.Tables)
		if old, ok := m.cache[e.Key]; ok {
			e.Epoch = min(e.Epoch, old.Epoch)
			for _, t := range old.Tables {
				if !slices.Contains(e.Tables, t) {
					e.Tables = append(e.Tables, t)
				}
			}
		}
		m.cache[e.Key] = e
	}
	return true, nil
}

func (m *MemoryStore) Purge(_ context.Context, tables []string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.epoch++
	set := tableSet(tables)
	var n int64
	for k, e := range m.cache {
		if e.tagged(set) {
			delete(m.cache, k)
			n++
		}
	}
	return n, nil
}

func (m *MemoryStore) Remaining(_ context.Context, tables []string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	set := tableSet(tables)
	var n int64
	for _, e := range m.cache {
		if e.Epoch < m.epoch && e.tagged(set) {
			n++
		}
	}
	return n, nil
}

func (m *MemoryStore) Expire(_ context.Context, before time.Time) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for k, e := range m.cache {
		if !e.Expires.After(before) {
			delete(m.cache, k)
			n++
		}
	}
	return n, nil
}

func (m *MemoryStore) Close() {}
