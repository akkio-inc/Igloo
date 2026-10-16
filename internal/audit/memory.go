package audit

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"
)

// MemoryStore keeps the most recent records of one replica in memory, for
// servers without a metadata database.
type MemoryStore struct {
	mu         sync.Mutex
	queries    []Query
	logins     []Login
	compute    []Compute
	maxQueries int
	maxLogins  int
}

// Default bounds of a MemoryStore.
const (
	MemoryQueries = 10_000
	MemoryLogins  = 10_000
)

// NewMemoryStore keeps up to maxQueries queries and maxLogins logins,
// dropping the oldest.
func NewMemoryStore(maxQueries, maxLogins int) *MemoryStore {
	return &MemoryStore{maxQueries: maxQueries, maxLogins: maxLogins}
}

func (m *MemoryStore) WriteQueries(_ context.Context, qs []Query) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queries = keepLast(append(m.queries, qs...), m.maxQueries)
	return nil
}

func (m *MemoryStore) WriteLogins(_ context.Context, ls []Login) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logins = keepLast(append(m.logins, ls...), m.maxLogins)
	return nil
}

func (m *MemoryStore) Queries(_ context.Context, v Viewer, p Page) ([]Query, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Query
	for i := len(m.queries) - 1; i >= 0; i-- {
		q := m.queries[i]
		if visible(v, q.User) && p.includes(q.Start, q.ID) {
			out = append(out, q)
		}
	}
	return limit(sortNewest(out, func(q Query) (time.Time, string) { return q.Start, q.ID }), p.Limit), nil
}

func (m *MemoryStore) Logins(_ context.Context, v Viewer, p Page) ([]Login, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Login
	for i := len(m.logins) - 1; i >= 0; i-- {
		x := m.logins[i]
		if visible(v, x.User) && p.includes(x.Time, x.ID) {
			out = append(out, x)
		}
	}
	return limit(sortNewest(out, func(x Login) (time.Time, string) { return x.Time, x.ID }), p.Limit), nil
}

func (m *MemoryStore) Prune(_ context.Context, before time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queries = dropOlder(m.queries, func(q Query) bool { return q.Start.Before(before) })
	m.logins = dropOlder(m.logins, func(x Login) bool { return x.Time.Before(before) })
	m.compute = dropOlder(m.compute, func(c Compute) bool { return c.Day.Add(24 * time.Hour).Before(before) })
	return nil
}

func (m *MemoryStore) Close() {}

func visible(v Viewer, user string) bool { return v.All || user == v.User }

// includes reports whether a record at t with id belongs on the page.
func (p Page) includes(t time.Time, id string) bool {
	if !p.Since.IsZero() && t.Before(p.Since) {
		return false
	}
	if !p.Until.IsZero() && !t.Before(p.Until) {
		return false
	}
	if !p.AfterTime.IsZero() && !(t.Before(p.AfterTime) || t.Equal(p.AfterTime) && id < p.AfterID) {
		return false
	}
	return true
}

func keepLast[T any](s []T, n int) []T {
	if len(s) > n {
		s = append(s[:0:0], s[len(s)-n:]...)
	}
	return s
}

func dropOlder[T any](s []T, old func(T) bool) []T {
	out := s[:0]
	for _, r := range s {
		if !old(r) {
			out = append(out, r)
		}
	}
	return out
}

// sortNewest orders records newest first, then by descending ID, the order
// pages continue in.
func sortNewest[T any](s []T, key func(T) (time.Time, string)) []T {
	slices.SortFunc(s, func(a, b T) int {
		ta, ia := key(a)
		tb, ib := key(b)
		if c := tb.Compare(ta); c != 0 {
			return c
		}
		return strings.Compare(ib, ia)
	})
	return s
}

func limit[T any](s []T, n int) []T {
	if n > 0 && len(s) > n {
		return s[:n]
	}
	return s
}
