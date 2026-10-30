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
	// Owner is the user who created it, or "name (dropped)" once that
	// user was dropped (see Store.DisownModels).
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
	// Tables are the catalog's tables its training read, namespace.name in
	// lower case, with those the models it read had read, and AnyTable for
	// what Igloo could not name (files, say): what an erasure request
	// trains it again for (see package pgwire's ModelRetrainer). Nil means
	// they were not recorded, as for a model created before Igloo recorded
	// them, which every erasure request trains again; empty means none, as
	// TOP_INSIGHTS reads nothing.
	Tables []string
	// Epoch is the store's erasure epoch when its training began (see
	// Store.Put). SearchPath and Dialect are the creating session's search
	// path and SQL dialect, which training it again uses.
	Epoch      int64
	SearchPath string
	Dialect    string
	// IfBlob, when Put replaces the model, is the blob the model it
	// replaces must have, or Put returns ErrChanged: an erasure request
	// training it again replaces only the model it read. It is not kept.
	IfBlob string
}

// AnyTable stands for what a model's training read that Igloo could not
// name, such as files: every erasure request trains such a model again.
const AnyTable = "*"

// TrainedOn reports whether the model may hold what it learned from any of
// tables (lower case): its training read one of them, or what Igloo could
// not name, or it did not record what it read.
func (m Model) TrainedOn(tables []string) bool {
	if m.Tables == nil {
		return true
	}
	for _, t := range m.Tables {
		if t == AnyTable || slices.Contains(tables, t) {
			return true
		}
	}
	return false
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
	// ErrStale is a model whose training began before an erasure request
	// moved the epoch on: it may have read rows the request erased.
	ErrStale = errors.New("an erasure request ran while the model trained, and the rows it read may be among those it erased: create it again")
	// ErrChanged is a model Put was to replace (see Model.IfBlob) that was
	// replaced or dropped since.
	ErrChanged = errors.New("the model was replaced or dropped meanwhile")
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
	// A replaced model keeps its grants. A model whose Epoch is not the
	// store's is ErrStale: an erasure request moved the epoch on while it
	// trained. A Put and a BumpEpoch never interleave.
	Put(ctx context.Context, m Model, replace bool) (*Model, error)
	// Epoch returns the erasure epoch, which a CREATE reads before it reads
	// its training data; BumpEpoch moves it on and returns the new one.
	Epoch(ctx context.Context) (int64, error)
	BumpEpoch(ctx context.Context) (int64, error)
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
	// ForgetGrantee removes the grants to a user or role ("user" or
	// "role") that was dropped.
	ForgetGrantee(ctx context.Context, granteeType, grantee string) error
	// DisownModels gives the models of owner, a user who was dropped, to
	// tombstone, a name no user can have, so that a user created later
	// with the same name does not own them.
	DisownModels(ctx context.Context, owner, tombstone string) error
	Close()
}

func key(schema, name string) string { return strings.ToLower(schema) + "." + strings.ToLower(name) }

// MemoryStore keeps models in this process.
type MemoryStore struct {
	mu     sync.Mutex
	models map[string]Model
	grants []Grant
	epoch  int64
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{models: map[string]Model{}, epoch: 1} }

func (s *MemoryStore) Epoch(context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.epoch, nil
}

func (s *MemoryStore) BumpEpoch(context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epoch++
	return s.epoch, nil
}

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
	if m.Epoch != 0 && m.Epoch != s.epoch {
		return nil, ErrStale
	}
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now()
	}
	m.Tables = slices.Clone(m.Tables)
	k := key(m.Schema, m.Name)
	old, ok := s.models[k]
	if ok && !replace {
		return nil, ErrExists
	}
	if m.IfBlob != "" && (!ok || old.Blob != m.IfBlob) {
		return nil, ErrChanged
	}
	m.IfBlob = ""
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

func (s *MemoryStore) ForgetGrantee(_ context.Context, granteeType, grantee string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.grants = slices.DeleteFunc(s.grants, func(g Grant) bool { return g.GranteeType == granteeType && g.Grantee == grantee })
	return nil
}

func (s *MemoryStore) DisownModels(_ context.Context, owner, tombstone string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, m := range s.models {
		if m.Owner == owner {
			m.Owner = tombstone
			s.models[k] = m
		}
	}
	return nil
}

func (s *MemoryStore) Close() {}
