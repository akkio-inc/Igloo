package notify

import (
	"context"
	"maps"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/akkio-inc/igloo/internal/audit"
)

// Store keeps integrations, who may use them, the alert history, and what
// built-in alerts sent when. Implementations must be safe for concurrent
// use.
type Store interface {
	// CreateIntegration adds in, or with replace replaces the integration
	// of its name, keeping who may use it. ErrExists if it exists and
	// replace is false.
	CreateIntegration(ctx context.Context, in Integration, replace bool) error
	// Integration returns the integration called name, or ErrNotFound.
	Integration(ctx context.Context, name string) (Integration, error)
	// Integrations lists every integration, by name.
	Integrations(ctx context.Context) ([]Integration, error)
	// UpdateIntegration changes an integration's settings.
	UpdateIntegration(ctx context.Context, in Integration) error
	// DropIntegration removes an integration and who may use it.
	DropIntegration(ctx context.Context, name string) error

	// GrantUsage lets a user or role use an integration; RevokeUsage takes
	// that back. Usage lists every grant.
	GrantUsage(ctx context.Context, u Usage) error
	RevokeUsage(ctx context.Context, integration, granteeType, grantee string) error
	Usage(ctx context.Context) ([]Usage, error)
	// ForgetGrantee removes a dropped user's or role's grants.
	ForgetGrantee(ctx context.Context, granteeType, grantee string) error

	// WriteRecord adds r to the alert history.
	WriteRecord(ctx context.Context, r Record) error
	// Records returns the alert history v may see, newest first.
	Records(ctx context.Context, v audit.Viewer, p audit.Page) ([]Record, error)
	// Claim reports whether a notification named key may go out now: it
	// may, and Claim remembers so, unless one did within every. Replicas
	// share claims, so only one of them sends.
	Claim(ctx context.Context, key string, every time.Duration) (bool, error)
	// Prune deletes history older than records, and claims older than
	// claims.
	Prune(ctx context.Context, records, claims time.Time) error
	Close()
}

// MemoryRecords bounds the alert history a MemoryStore keeps.
const MemoryRecords = 10_000

// MemoryStore keeps everything in memory, for one replica without a
// metadata database. It is lost on restart.
type MemoryStore struct {
	mu           sync.Mutex
	integrations map[string]Integration
	usage        []Usage
	records      []Record
	claims       map[string]time.Time
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{integrations: map[string]Integration{}, claims: map[string]time.Time{}}
}

// clone copies in's maps and slices, so callers cannot change what the
// store holds.
func clone(in Integration) Integration {
	in.Headers = maps.Clone(in.Headers)
	in.Recipients = slices.Clone(in.Recipients)
	return in
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
	m.integrations[in.Name] = clone(in)
	return nil
}

func (m *MemoryStore) Integration(_ context.Context, name string) (Integration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	in, ok := m.integrations[name]
	if !ok {
		return Integration{}, ErrNotFound
	}
	return clone(in), nil
}

func (m *MemoryStore) Integrations(context.Context) ([]Integration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Integration, 0, len(m.integrations))
	for _, in := range m.integrations {
		out = append(out, clone(in))
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Name < out[b].Name })
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
	m.integrations[in.Name] = clone(in)
	return nil
}

func (m *MemoryStore) DropIntegration(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.integrations[name]; !ok {
		return ErrNotFound
	}
	delete(m.integrations, name)
	m.usage = slices.DeleteFunc(m.usage, func(u Usage) bool { return u.Integration == name })
	return nil
}

func (m *MemoryStore) GrantUsage(_ context.Context, u Usage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.integrations[u.Integration]; !ok {
		return ErrNotFound
	}
	if slices.ContainsFunc(m.usage, func(x Usage) bool {
		return x.Integration == u.Integration && x.GranteeType == u.GranteeType && x.Grantee == u.Grantee
	}) {
		return nil
	}
	if u.GrantedAt.IsZero() {
		u.GrantedAt = time.Now()
	}
	m.usage = append(m.usage, u)
	return nil
}

func (m *MemoryStore) RevokeUsage(_ context.Context, integration, granteeType, grantee string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.usage = slices.DeleteFunc(m.usage, func(x Usage) bool {
		return x.Integration == integration && x.GranteeType == granteeType && x.Grantee == grantee
	})
	return nil
}

func (m *MemoryStore) Usage(context.Context) ([]Usage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.usage), nil
}

func (m *MemoryStore) ForgetGrantee(_ context.Context, granteeType, grantee string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.usage = slices.DeleteFunc(m.usage, func(x Usage) bool { return x.GranteeType == granteeType && x.Grantee == grantee })
	return nil
}

func (m *MemoryStore) WriteRecord(_ context.Context, r Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if i := slices.IndexFunc(m.records, func(x Record) bool { return x.ID == r.ID }); i >= 0 {
		m.records[i] = r
		return nil
	}
	m.records = append(m.records, r)
	if len(m.records) > MemoryRecords {
		m.records = slices.Delete(m.records, 0, len(m.records)-MemoryRecords)
	}
	return nil
}

func (m *MemoryStore) Records(_ context.Context, v audit.Viewer, p audit.Page) ([]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Record
	for _, r := range m.records {
		switch {
		case !v.All && r.User != v.User,
			!p.Since.IsZero() && r.Start.Before(p.Since),
			!p.Until.IsZero() && !r.Start.Before(p.Until),
			!p.AfterTime.IsZero() && !(r.Start.Before(p.AfterTime) || r.Start.Equal(p.AfterTime) && r.ID < p.AfterID):
			continue
		}
		out = append(out, r)
	}
	sort.SliceStable(out, func(a, b int) bool {
		if !out[a].Start.Equal(out[b].Start) {
			return out[a].Start.After(out[b].Start)
		}
		return out[a].ID > out[b].ID
	})
	limit := p.Limit
	if limit <= 0 {
		limit = 1000
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *MemoryStore) Claim(_ context.Context, key string, every time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	if at, ok := m.claims[key]; ok && now.Sub(at) < every {
		return false, nil
	}
	m.claims[key] = now
	return true, nil
}

func (m *MemoryStore) Prune(_ context.Context, records, claims time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = slices.DeleteFunc(m.records, func(r Record) bool { return r.Start.Before(records) })
	maps.DeleteFunc(m.claims, func(_ string, at time.Time) bool { return at.Before(claims) })
	return nil
}

func (m *MemoryStore) Close() {}
