package ai

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// Options configure a Runtime.
type Options struct {
	Store Store
	// Dial connects to providers: the egress guard's DialContext. Nil
	// means a plain dialer (tests).
	Dial func(ctx context.Context, network, address string) (net.Conn, error)
	// CheckURL vets an integration's address: the egress guard's
	// CheckURL. Nil allows any.
	CheckURL func(string) error
	// Cache keeps results across statements for CacheRetention.
	Cache          bool
	CacheRetention time.Duration
	// MaxCalls and MaxCostUSD are the default caps of a statement; 0 is
	// none.
	MaxCalls   int64
	MaxCostUSD float64
	// RequestTimeout bounds each request to a provider.
	RequestTimeout time.Duration
	Log            *slog.Logger
}

// Runtime runs the AI functions of every statement on a replica: it keeps
// the integrations as last read, a client and a bound on requests in
// flight for each, and the cache.
type Runtime struct {
	opts   Options
	client *client
	log    *slog.Logger
	now    func() time.Time

	mu       sync.Mutex
	version  int64
	bindings map[string]*binding
	usage    []Grant
}

// binding is an integration ready to call: its provider, and the slots
// that bound its requests in flight.
type binding struct {
	in       Integration
	provider Provider
	slots    chan struct{}
}

// NewRuntime makes a runtime. Without a store it keeps everything in
// memory.
func NewRuntime(opts Options) *Runtime {
	if opts.Store == nil {
		opts.Store = NewMemoryStore()
	}
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	if opts.CacheRetention <= 0 {
		opts.CacheRetention = 30 * 24 * time.Hour
	}
	dial := opts.Dial
	if dial == nil {
		dial = (&net.Dialer{Timeout: 30 * time.Second}).DialContext
	}
	return &Runtime{opts: opts, client: newClient(dial, opts.RequestTimeout), log: opts.Log.With("component", "ai"),
		now: time.Now, version: -1, bindings: map[string]*binding{}}
}

// Store is where integrations, grants and the cache are.
func (rt *Runtime) Store() Store { return rt.opts.Store }

// Caps are the default caps of a statement on calls and cost; 0 is none.
func (rt *Runtime) Caps() (int64, float64) { return rt.opts.MaxCalls, rt.opts.MaxCostUSD }

// CheckURL vets an integration's address, as integrations are created.
func (rt *Runtime) CheckURL(raw string) error {
	if rt.opts.CheckURL == nil {
		return nil
	}
	return rt.opts.CheckURL(raw)
}

// Changed makes the next statement read the integrations again, after this
// replica changed them.
func (rt *Runtime) Changed() {
	rt.mu.Lock()
	rt.version = -1
	rt.mu.Unlock()
}

// load reads the integrations and grants again if they changed since
// version, and returns them.
func (rt *Runtime) load(ctx context.Context, version int64) (map[string]*binding, []Grant, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.version == version {
		return rt.bindings, rt.usage, nil
	}
	all, err := rt.opts.Store.Integrations(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("read AI integrations: %w", err)
	}
	usage, err := rt.opts.Store.Grants(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("read AI integrations' grants: %w", err)
	}
	bindings := map[string]*binding{}
	for _, in := range all {
		if old, ok := rt.bindings[in.Name]; ok && sameIntegration(old.in, in) {
			bindings[in.Name] = old
			continue
		}
		p, err := newProvider(in, rt.client)
		if err != nil {
			rt.log.Warn("an AI integration cannot be used", "integration", in.Name, "err", in.Scrub(err.Error()))
			continue
		}
		bindings[in.Name] = &binding{in: in, provider: p, slots: make(chan struct{}, in.concurrency())}
	}
	rt.bindings, rt.usage, rt.version = bindings, usage, version
	return bindings, usage, nil
}

// sameIntegration reports whether a and b are the same in all that a
// binding depends on.
func sameIntegration(a, b Integration) bool {
	return fmt.Sprintf("%#v", a) == fmt.Sprintf("%#v", b)
}

// StatementOptions describe the statement whose AI functions a Statement
// runs.
type StatementOptions struct {
	// User and Roles are who runs it, with their custom roles; Admin says
	// the user is an admin, who may use every integration.
	User  string
	Roles []string
	Admin bool
	// Integration is the session's ai_integration, or empty.
	Integration string
	// Tables are the catalog tables the statement reads (namespace.name),
	// with AnyTable if it may read what Igloo cannot name: the tags of
	// what it caches.
	Tables []string
	// MaxCalls and MaxCostUSD are the statement's caps, from its role's
	// settings or the server's; 0 is none. CapSource names where they come
	// from, for the error.
	MaxCalls   int64
	MaxCostUSD float64
	CapSource  string
	// Threshold is the probability from which a decision model's yes
	// counts, for AI_FILTER and multi-label AI_CLASSIFY (the session's
	// ai_decision_threshold); 0 means 0.5.
	Threshold float64
	// DryRun answers from the cache, and counts what the statement would
	// send for the rest without calling providers, for EXPLAIN.
	DryRun bool
}

// Begin starts the AI functions of a statement that is about to run, with
// ctx, which ends with it. It reads the cache's epoch now, before the
// statement reads any table: what it caches is tagged with it. It fails
// when no integration exists, or the user may use none.
func (rt *Runtime) Begin(ctx context.Context, o StatementOptions) (*Statement, error) {
	epoch, version, err := rt.opts.Store.State(ctx)
	if err != nil {
		return nil, fmt.Errorf("read the AI cache's state: %w", err)
	}
	bindings, usage, err := rt.load(ctx, version)
	if err != nil {
		return nil, err
	}
	if len(bindings) == 0 {
		return nil, ErrOff
	}
	s := &Statement{rt: rt, ctx: ctx, opts: o, epoch: epoch, flights: map[string]*flight{}, byModel: map[string]*ModelSpend{}}
	for _, name := range slices.Sorted(maps.Keys(bindings)) {
		b := bindings[name]
		if b.in.Enabled && (o.Admin || mayUse(usage, name, o.User, o.Roles)) {
			s.usable = append(s.usable, b)
		}
	}
	if o.Integration != "" {
		b, ok := bindings[o.Integration]
		switch {
		case !ok:
			return nil, fmt.Errorf("%w: the AI integration %s (ai_integration) does not exist", ErrNotFound, o.Integration)
		case !slices.Contains(s.usable, b) && !b.in.Enabled:
			return nil, fmt.Errorf("%w: the AI integration %s is disabled", ErrInvalid, o.Integration)
		case !slices.Contains(s.usable, b):
			return nil, fmt.Errorf("%w: %s has no USAGE on the AI integration %s; an admin can grant it with "+
				"GRANT USAGE ON INTEGRATION %s TO ...", ErrDenied, o.User, o.Integration, o.Integration)
		}
		s.usable = []*binding{b}
	}
	if len(s.usable) == 0 {
		return nil, fmt.Errorf("%w: %s has no USAGE on an AI integration; an admin can grant it with "+
			"GRANT USAGE ON INTEGRATION name TO ...", ErrDenied, o.User)
	}
	s.ctx = withMeter(ctx, &s.sent)
	return s, nil
}

// mayUse reports whether a grant in usage lets user, with custom roles,
// use the integration called name.
func mayUse(usage []Grant, name, user string, roles []string) bool {
	return slices.ContainsFunc(usage, func(u Grant) bool {
		return u.Integration == name && (u.GranteeType == "user" && u.Grantee == user ||
			u.GranteeType == "role" && slices.Contains(roles, u.Grantee))
	})
}

// Prune deletes the cache's expired entries; one replica calls it now and
// then.
func (rt *Runtime) Prune(ctx context.Context) {
	n, err := rt.opts.Store.Expire(ctx, rt.now())
	if err != nil {
		rt.log.Warn("could not delete the AI cache's expired entries", "err", err)
		return
	}
	if n > 0 {
		rt.log.Info("deleted the AI cache's expired entries", "entries", n)
	}
}

// Run prunes the cache every interval until ctx ends.
func (rt *Runtime) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		rt.Prune(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// cacheKey hashes parts, each length-prefixed so no two lists of parts
// hash alike.
func cacheKey(parts ...string) string {
	h := sha256.New()
	var n [8]byte
	for _, p := range parts {
		binary.BigEndian.PutUint64(n[:], uint64(len(p)))
		h.Write(n[:])
		h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Tables normalizes a statement's tables: lower case, sorted, once each.
func Tables(tables []string) []string {
	var out []string
	for _, t := range tables {
		t = strings.ToLower(strings.TrimSpace(t))
		if t != "" && !slices.Contains(out, t) {
			out = append(out, t)
		}
	}
	sort.Strings(out)
	return out
}

// ErrorCode classifies err for SQL: "cap", "denied", "off", "model",
// "invalid", or "" for others.
func ErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrCap):
		return "cap"
	case errors.Is(err, ErrDenied):
		return "denied"
	case errors.Is(err, ErrOff):
		return "off"
	case errors.Is(err, ErrModel):
		return "model"
	case errors.Is(err, ErrInvalid), errors.Is(err, ErrNotFound), errors.Is(err, ErrUnsupported):
		return "invalid"
	}
	return ""
}
