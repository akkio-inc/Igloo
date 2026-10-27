package models

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"
)

// Model is a model's row: what it is, who owns it, how it was trained,
// and its blob. Models never change: CREATE OR REPLACE trains a new one.
type Model struct {
	Schema, Name string
	Class        string
	// Owner is the user who created it.
	Owner   string
	Comment string
	// TrainingSQL is the CREATE statement as written.
	TrainingSQL string
	// Config is CREATE's CONFIG_OBJECT and Metrics a summary of the
	// model's metrics, as JSON objects.
	Config, Metrics string
	// Blob is the SHA-256 of the model's blob, BlobSize its size.
	Blob      string
	BlobSize  int64
	CreatedAt time.Time
}

// Privileges on a model: USAGE allows every method; PREDICT the methods
// that predict (Snowflake's MLCONSUMER instance role).
const (
	PrivilegeUsage   = "USAGE"
	PrivilegePredict = "PREDICT"
)

// Grant is a privilege on a model, given to a user or custom role.
type Grant struct {
	Schema, Name string
	Grantee      string
	// GranteeType is "user" or "role".
	GranteeType string
	Privilege   string
	GrantedBy   string
	GrantedAt   time.Time
}

var (
	ErrNotFound = errors.New("model not found")
	ErrExists   = errors.New("model exists")
)

// Store keeps models' rows and grants: the metadata database, shared by
// every replica, or memory.
type Store interface {
	// Get returns the model schema.name; names compare in lower case.
	Get(ctx context.Context, schema, name string) (Model, error)
	// List returns every model.
	List(ctx context.Context) ([]Model, error)
	// Put stores m, replacing a model of its name when replace is set
	// (and returning the one replaced); otherwise one there is ErrExists.
	// A replaced model keeps its grants.
	Put(ctx context.Context, m Model, replace bool) (*Model, error)
	// Delete removes a model and its grants, and returns it.
	Delete(ctx context.Context, schema, name string) (Model, error)
	// BlobUsed reports whether a model's blob is sha.
	BlobUsed(ctx context.Context, sha string) (bool, error)
	// Grants returns every grant on models.
	Grants(ctx context.Context) ([]Grant, error)
	// AddGrant and RevokeGrant give and take back a privilege; giving one
	// already given changes nothing. The model must exist.
	AddGrant(ctx context.Context, g Grant) error
	RevokeGrant(ctx context.Context, g Grant) error
	Close()
}

func key(schema, name string) string { return strings.ToLower(schema) + "." + strings.ToLower(name) }

// MemoryStore keeps models in this process.
type MemoryStore struct {
	mu     sync.Mutex
	models map[string]Model
	grants []Grant
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{models: map[string]Model{}} }

func (s *MemoryStore) Get(_ context.Context, schema, name string) (Model, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.models[key(schema, name)]
	if !ok {
		return Model{}, ErrNotFound
	}
	return m, nil
}

func (s *MemoryStore) List(context.Context) ([]Model, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Model, 0, len(s.models))
	for _, m := range s.models {
		out = append(out, m)
	}
	slices.SortFunc(out, func(a, b Model) int { return strings.Compare(key(a.Schema, a.Name), key(b.Schema, b.Name)) })
	return out, nil
}

func (s *MemoryStore) Put(_ context.Context, m Model, replace bool) (*Model, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m.Schema, m.Name = strings.ToLower(m.Schema), strings.ToLower(m.Name)
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now()
	}
	k := key(m.Schema, m.Name)
	old, ok := s.models[k]
	if ok && !replace {
		return nil, ErrExists
	}
	s.models[k] = m
	if ok {
		return &old, nil
	}
	return nil, nil
}

func (s *MemoryStore) Delete(_ context.Context, schema, name string) (Model, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(schema, name)
	m, ok := s.models[k]
	if !ok {
		return Model{}, ErrNotFound
	}
	delete(s.models, k)
	s.grants = slices.DeleteFunc(s.grants, func(g Grant) bool { return key(g.Schema, g.Name) == k })
	return m, nil
}

func (s *MemoryStore) BlobUsed(_ context.Context, sha string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.models {
		if m.Blob == sha {
			return true, nil
		}
	}
	return false, nil
}

func (s *MemoryStore) Grants(context.Context) ([]Grant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.grants), nil
}

func sameGrant(a, b Grant) bool {
	return key(a.Schema, a.Name) == key(b.Schema, b.Name) && a.Grantee == b.Grantee && a.GranteeType == b.GranteeType &&
		a.Privilege == b.Privilege
}

func (s *MemoryStore) AddGrant(_ context.Context, g Grant) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.models[key(g.Schema, g.Name)]; !ok {
		return ErrNotFound
	}
	if slices.ContainsFunc(s.grants, func(o Grant) bool { return sameGrant(o, g) }) {
		return nil
	}
	if g.GrantedAt.IsZero() {
		g.GrantedAt = time.Now()
	}
	g.Schema, g.Name = strings.ToLower(g.Schema), strings.ToLower(g.Name)
	s.grants = append(s.grants, g)
	return nil
}

func (s *MemoryStore) RevokeGrant(_ context.Context, g Grant) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.grants = slices.DeleteFunc(s.grants, func(o Grant) bool { return sameGrant(o, g) })
	return nil
}

func (s *MemoryStore) Close() {}
