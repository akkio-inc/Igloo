package ai

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
)

// Statement runs the AI functions of one statement. DuckDB calls them on
// several threads at once, a chunk of rows at a time; a Statement sends
// each distinct input once (a flight), reads the cache first, keeps to the
// statement's caps, and puts what providers answer in the cache.
type Statement struct {
	rt   *Runtime
	ctx  context.Context
	opts StatementOptions
	// epoch is the cache's erasure epoch when the statement began.
	epoch int64
	// usable are the integrations the user may use, by name.
	usable []*binding
	sent   atomic.Int64

	mu      sync.Mutex
	flights map[string]*flight
	spend   Spend
	// reserved is the estimated cost of the calls in flight.
	reserved float64
	// byModel is what was sent to each model, as integration/model.
	byModel map[string]*ModelSpend
	// failed is the first error that fails the statement; no call starts
	// after it.
	failed error
}

// flight is one distinct input's call, which every row that asks for it
// waits for.
type flight struct {
	done  chan struct{}
	value []byte
	err   error
}

// call is a call a function makes for one distinct input.
type call struct {
	key string
	fn  string
	b   *binding
	// model is the provider's model; keyModel what the key and the cache
	// name it by.
	model string
	// refresh bypasses the cache: the provider is called, and its answer
	// kept.
	refresh bool
	// estimate is the call's estimated input and output tokens, for the
	// cost cap.
	estimate Usage
	// run calls the provider, and returns the result as JSON.
	run func(ctx context.Context) ([]byte, Usage, error)
	// group, when set, sends calls with the same group together: run
	// then takes the group's calls (see batch).
	group string
}

// batchRunner sends several calls of a group in one request, and returns
// each one's result.
type batchRunner func(ctx context.Context, calls []*call) ([][]byte, Usage, error)

// Context is the statement's context, which ends when it does.
func (s *Statement) Context() context.Context { return s.ctx }

// Run sets the context the statement's calls are made under, from now:
// its run's, which a cancel request or its timeout ends. Call it before
// the statement runs.
func (s *Statement) Run(ctx context.Context) { s.ctx = withMeter(ctx, &s.sent) }

// Options are the statement's options.
func (s *Statement) Options() StatementOptions { return s.opts }

// fail records err as the statement's failure, unless one is recorded.
func (s *Statement) fail(err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed == nil {
		s.failed = err
	}
	return s.failed
}

// Failed returns the error that failed the statement, if any.
func (s *Statement) Failed() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failed
}

// End returns what the statement's AI functions did.
func (s *Statement) End() Spend {
	s.mu.Lock()
	defer s.mu.Unlock()
	sp := s.spend
	sp.BytesSent = s.sent.Load()
	sp.Models = slices.Sorted(maps.Keys(s.byModel))
	sp.ByModel = map[string]ModelSpend{}
	for k, m := range s.byModel {
		sp.ByModel[k] = *m
	}
	return sp
}

// count adds a call to model, which used u, to what the statement sent to
// it, and its cost, at the integration's price, to the statement's. s.mu
// must be held.
func (s *Statement) count(b *binding, model string, u Usage) {
	key := b.in.Name + "/" + model
	m := s.byModel[key]
	if m == nil {
		m = &ModelSpend{}
		s.byModel[key] = m
	}
	m.Calls++
	m.InputTokens += u.InputTokens
	m.OutputTokens += u.OutputTokens
	if _, ok := b.in.Price(model); ok {
		c := cost(b.in, model, u)
		m.CostUSD += c
		m.Priced = true
		s.spend.CostUSD += c
	} else {
		s.spend.Unpriced++
	}
}

// RowError counts a row that got NULL or an error for its own input.
func (s *Statement) rowError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spend.RowErrors++
	if s.spend.FirstError == "" {
		s.spend.FirstError = err.Error()
	}
}

// do runs calls, each distinct key once across the statement, and returns
// their results in order. batch, if set, sends the calls of a group
// together. It fails, for every call, with the error that fails the
// statement.
func (s *Statement) do(calls []*call, batch batchRunner, batchSize int) ([][]byte, []error, error) {
	if err := s.Failed(); err != nil {
		return nil, nil, err
	}
	if err := s.ctx.Err(); err != nil {
		return nil, nil, s.fail(err)
	}
	waits := make([]*flight, len(calls))
	var owned []*call
	var ownedFlights []*flight
	s.mu.Lock()
	for i, c := range calls {
		f, ok := s.flights[c.key]
		if !ok {
			f = &flight{done: make(chan struct{})}
			s.flights[c.key] = f
			owned = append(owned, c)
			ownedFlights = append(ownedFlights, f)
		}
		waits[i] = f
	}
	s.mu.Unlock()

	if len(owned) > 0 {
		s.resolve(owned, ownedFlights, batch, batchSize)
	}

	values := make([][]byte, len(calls))
	errs := make([]error, len(calls))
	for i, f := range waits {
		select {
		case <-f.done:
		case <-s.ctx.Done():
			return nil, nil, s.fail(s.ctx.Err())
		}
		values[i], errs[i] = f.value, f.err
	}
	if err := s.Failed(); err != nil {
		return nil, nil, err
	}
	return values, errs, nil
}

// resolve answers the owned calls: from the cache, then from providers,
// and puts what providers answered in the cache.
func (s *Statement) resolve(calls []*call, flights []*flight, batch batchRunner, batchSize int) {
	finish := func(f *flight, value []byte, err error) {
		f.value, f.err = value, err
		close(f.done)
	}
	// The cache first.
	var misses []int
	if s.rt.opts.Cache {
		var keys []string
		for _, c := range calls {
			if !c.refresh {
				keys = append(keys, c.key)
			}
		}
		hits, err := s.rt.opts.Store.Lookup(s.ctx, keys, s.rt.now())
		if err != nil {
			// The cache is an optimization; providers answer without it.
			s.rt.log.Warn("could not read the AI cache", "err", err)
		}
		for i, c := range calls {
			if e, ok := hits[c.key]; ok && !c.refresh {
				s.mu.Lock()
				s.spend.CacheHits++
				s.mu.Unlock()
				finish(flights[i], e.Value, nil)
				continue
			}
			misses = append(misses, i)
		}
	} else {
		for i := range calls {
			misses = append(misses, i)
		}
	}
	if len(misses) == 0 {
		return
	}
	if s.opts.DryRun {
		// Counted, as their calls would be, and answered with nothing.
		s.mu.Lock()
		for _, i := range misses {
			c := calls[i]
			s.spend.Calls++
			s.spend.add(c.estimate)
			s.count(c.b, c.model, c.estimate)
		}
		s.mu.Unlock()
		for _, i := range misses {
			finish(flights[i], nil, errDryRun)
		}
		return
	}

	// Then providers, in groups of batchSize where batch sends them
	// together, each request in a slot of its integration's.
	type job struct{ idx []int }
	var jobs []job
	if batch != nil {
		groups := map[string][]int{}
		var order []string
		for _, i := range misses {
			g := calls[i].group
			if _, ok := groups[g]; !ok {
				order = append(order, g)
			}
			groups[g] = append(groups[g], i)
		}
		for _, g := range order {
			idx := groups[g]
			for start := 0; start < len(idx); start += max(batchSize, 1) {
				jobs = append(jobs, job{idx[start:min(start+max(batchSize, 1), len(idx))]})
			}
		}
	} else {
		for _, i := range misses {
			jobs = append(jobs, job{[]int{i}})
		}
	}

	var (
		wg      sync.WaitGroup
		entMu   sync.Mutex
		entries []CacheEntry
	)
	now := s.rt.now()
	for _, j := range jobs {
		first := calls[j.idx[0]]
		var est Usage
		for _, i := range j.idx {
			est.add(calls[i].estimate)
		}
		if err := s.reserve(first.b, first.model, est); err != nil {
			for _, i := range j.idx {
				finish(flights[i], nil, err)
			}
			continue
		}
		wg.Add(1)
		go func(j job, est Usage) {
			defer wg.Done()
			var (
				values [][]byte
				used   Usage
				err    error
			)
			select {
			case first.b.slots <- struct{}{}:
				if err = s.Failed(); err == nil {
					if batch != nil {
						cs := make([]*call, len(j.idx))
						for k, i := range j.idx {
							cs[k] = calls[i]
						}
						values, used, err = batch(s.ctx, cs)
					} else {
						var v []byte
						v, used, err = first.run(s.ctx)
						values = [][]byte{v}
					}
				}
				<-first.b.slots
			case <-s.ctx.Done():
				err = s.ctx.Err()
			}
			s.settle(first.b, first.model, est, used, err)
			if err != nil && !IsRowError(err) {
				err = s.fail(s.providerError(first.b, err))
			}
			for k, i := range j.idx {
				c := calls[i]
				if err != nil {
					finish(flights[i], nil, err)
					continue
				}
				var v []byte
				if k < len(values) {
					v = values[k]
				}
				finish(flights[i], v, nil)
				if s.rt.opts.Cache && v != nil {
					entMu.Lock()
					entries = append(entries, CacheEntry{Key: c.key, Function: c.fn, Model: c.b.in.Name + "/" + c.model, Value: v,
						Tables: s.opts.Tables, Epoch: s.epoch, Created: now, Expires: now.Add(s.rt.opts.CacheRetention)})
					entMu.Unlock()
				}
			}
		}(j, est)
	}
	wg.Wait()
	if len(entries) > 0 {
		if _, err := s.rt.opts.Store.Put(context.WithoutCancel(s.ctx), s.epoch, entries); err != nil {
			s.rt.log.Warn("could not keep AI results in the cache", "err", err)
		}
	}
}

// errDryRun answers a dry run's calls.
var errDryRun = errors.New("dry run")

// providerError says which integration failed, without its secrets.
func (s *Statement) providerError(b *binding, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrCap) ||
		errors.Is(err, ErrUnsupported) || errors.Is(err, ErrInvalid) {
		return err
	}
	return fmt.Errorf("the AI integration %s: %s", b.in.Name, b.in.Scrub(err.Error()))
}

// reserve counts a call against the statement's caps before it is sent,
// with its estimated cost, or fails the statement if it would go over.
func (s *Statement) reserve(b *binding, model string, est Usage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return s.failed
	}
	o := s.opts
	if o.MaxCalls > 0 && s.spend.Calls+1 > o.MaxCalls {
		s.failed = fmt.Errorf("%w: it would make more than %d calls to providers (%s); "+
			"an admin can raise it with ALTER ROLE ... SET ai_max_calls = n, or filter and deduplicate the inputs first",
			ErrCap, o.MaxCalls, capSource(o))
		return s.failed
	}
	c := cost(b.in, model, est)
	if o.MaxCostUSD > 0 && s.spend.CostUSD+s.reserved+c > o.MaxCostUSD {
		s.failed = fmt.Errorf("%w: its calls would cost more than $%.2f (%s); "+
			"an admin can raise it with ALTER ROLE ... SET ai_max_cost_usd = n", ErrCap, o.MaxCostUSD, capSource(o))
		return s.failed
	}
	s.spend.Calls++
	s.reserved += c
	return nil
}

func capSource(o StatementOptions) string {
	if o.CapSource != "" {
		return o.CapSource
	}
	return "the server's default"
}

// settle replaces a call's estimate with what it used.
func (s *Statement) settle(b *binding, model string, est, used Usage, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reserved -= cost(b.in, model, est)
	if s.reserved < 0 {
		s.reserved = 0
	}
	s.spend.add(used)
	s.count(b, model, used)
}

// cost is what u costs at model's price, 0 when it has none. Tokens a
// provider read from its prompt cache cost a tenth.
func cost(in Integration, model string, u Usage) float64 {
	p, ok := in.Price(model)
	if !ok {
		return 0
	}
	fresh := float64(u.InputTokens - u.CachedInputTokens)
	return (fresh*p.Input + float64(u.CachedInputTokens)*p.Input/10 + float64(u.OutputTokens)*p.Output) / 1e6
}

// binding returns the integration that serves fn with model, as SQL names
// it: the one the session chose, or the only usable one that maps it.
// model empty means the function's default model, for which want says
// which setting serves it ("default", "embedding" or "decision").
func (s *Statement) binding(fn, model, want string) (*binding, string, error) {
	if model != "" {
		var found []*binding
		var provider string
		for _, b := range s.usable {
			if m, ok := b.in.Model(model); ok {
				found = append(found, b)
				provider = m
			}
		}
		switch len(found) {
		case 1:
			return found[0], provider, nil
		case 0:
			names := make([]string, len(s.usable))
			for i, b := range s.usable {
				names[i] = b.in.Name
			}
			return nil, "", fmt.Errorf("%w %s: no AI integration %s may use maps it; an admin maps it with ALTER AI INTEGRATION %s SET "+
				"MODELS = (%s = 'provider-model', ...)", ErrModel, quote(model), s.opts.User, names[0], quote(strings.ToLower(model)))
		}
		return nil, "", fmt.Errorf("%w: several AI integrations map the model %s; choose one with SET ai_integration = 'name'",
			ErrInvalid, quote(model))
	}
	setting := func(b *binding) string {
		switch want {
		case "embedding":
			return b.in.EmbeddingModel
		case "decision":
			return b.in.DecisionModel
		}
		return b.in.DefaultModel
	}
	var found []*binding
	for _, b := range s.usable {
		if setting(b) != "" {
			found = append(found, b)
		}
	}
	switch len(found) {
	case 1:
		return found[0], found[0].in.resolve(setting(found[0])), nil
	case 0:
		key := map[string]string{"embedding": "EMBEDDING_MODEL", "decision": "DECISION_MODEL"}[want]
		if key == "" {
			key = "DEFAULT_MODEL"
		}
		return nil, "", fmt.Errorf("%w: %s needs a model chosen for it, and no AI integration %s may use sets %s; "+
			"an admin sets it with ALTER AI INTEGRATION name SET %s = 'provider-model'", ErrModel, strings.ToUpper(fn), s.opts.User, key, key)
	}
	return nil, "", fmt.Errorf("%w: several AI integrations can serve %s; choose one with SET ai_integration = 'name'",
		ErrInvalid, strings.ToUpper(fn))
}

// decisionBinding returns the integration whose decision model the user
// may use, and that model, if one has: ok is false where none does, and
// err says several do and the session must choose one.
func (s *Statement) decisionBinding() (b *binding, model string, ok bool, err error) {
	var found []*binding
	for _, b := range s.usable {
		if b.in.DecisionModel != "" {
			found = append(found, b)
		}
	}
	switch len(found) {
	case 0:
		return nil, "", false, nil
	case 1:
		return found[0], found[0].in.resolve(found[0].in.DecisionModel), true, nil
	}
	return nil, "", false, fmt.Errorf("%w: several AI integrations have a decision model; choose one with SET ai_integration = 'name'",
		ErrInvalid)
}
