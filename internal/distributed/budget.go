package distributed

import (
	"context"
	"strings"
	"sync"
	"time"
)

// Planning budgets in auto mode. Planning a statement for workers costs
// time that a statement auto mode runs on one machine pays for nothing: a
// report of a few seconds on one machine could wait out a budget of 10 s
// before it fell back. So auto mode plans a statement within a quarter of
// the time it is expected to take on one machine, no less than
// autoBudgetFloor and no more than planBudget: what it took here before,
// by the history of the plan it got last, or else what one machine reads
// of its tables cold, as auto mode's estimate of one machine reads them.
// TPC-H's queries plan in under 10 ms at test scale, and reports of a
// dozen common table expressions in 70 to 300 ms. A statement planned past
// its budget is remembered, by its shape, so that its next runs within an
// hour, budgeted no more, go to one machine without planning. Forced runs
// on workers and EXPLAIN DISTRIBUTED keep planBudget.

// autoBudgetFloor is the least auto mode plans a statement for, and
// autoBudgetShare the share of its expected time on one machine it may
// spend planning.
const (
	autoBudgetFloor = 500 * time.Millisecond
	autoBudgetShare = 0.25
)

// gaveUpFor is how long a shape whose planning ran past its budget goes to
// one machine without planning.
const gaveUpFor = time.Hour

// autoBudget is the planning budget of a statement expected to take
// seconds on one machine (see the top of this file).
func autoBudget(seconds float64) time.Duration {
	b := time.Duration(autoBudgetShare * seconds * float64(time.Second))
	return min(max(b, autoBudgetFloor), planBudget)
}

// planShapes remembers, by statement shape (planKey without its tables'
// sizes), the plan key a statement got last, whose history says how long
// it took on one machine, and the shapes whose planning ran past their
// budget.
type planShapes struct {
	mu     sync.Mutex
	keys   map[string]string
	gaveUp map[string]gaveUp
}

// gaveUp is when planning a shape ran past budget.
type gaveUp struct {
	budget time.Duration
	at     time.Time
}

// maxShapes bounds the shapes planShapes keeps of each kind.
const maxShapes = 4096

func (s *planShapes) key(shape string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, ok := s.keys[shape]
	return k, ok
}

func (s *planShapes) planned(shape, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.keys == nil || len(s.keys) >= maxShapes {
		s.keys = map[string]string{}
	}
	s.keys[shape] = key
	delete(s.gaveUp, shape)
}

func (s *planShapes) gaveUpOn(shape string, budget time.Duration, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gaveUp == nil || len(s.gaveUp) >= maxShapes {
		s.gaveUp = map[string]gaveUp{}
	}
	s.gaveUp[shape] = gaveUp{budget: budget, at: now}
}

// gaveUpLately reports whether planning shape ran past a budget of at
// least budget within gaveUpFor before now.
func (s *planShapes) gaveUpLately(shape string, budget time.Duration, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.gaveUp[shape]
	return ok && now.Sub(g.at) < gaveUpFor && budget <= g.budget
}

// expectedLocal is how many seconds the statement of shape is expected to
// take on one machine: what it took here, by the history of the plan its
// shape got last, or what one machine reads of its tables cold. ok is
// false when neither is known.
func (co *Coordinator) expectedLocal(pl *planner, stmt node, shape string) (float64, bool) {
	if key, ok := co.shapes.key(shape); ok {
		if r, ok := co.history.get(key); ok && r.local > 0 {
			return r.local, true
		}
	}
	bytes, ok := pl.statementReads(stmt)
	if !ok {
		return 0, false
	}
	cores := float64(max(co.opts.Cores, 1))
	return float64(bytes) / (cores * coldRateAt(float64(max(co.opts.Threads, co.opts.Cores))/cores)), true
}

// statementReads estimates what one machine reads of the catalog tables
// stmt reads, as auto mode's estimate weighs them: each table's size times
// the share of its columns the statement names (see columnsRead), and the
// tables views read in full. ok is false when a table cannot be resolved.
func (pl *planner) statementReads(stmt node) (int64, bool) {
	ctes := map[string]bool{}
	walk(stmt, func(n node) bool {
		for _, name := range cteNames(n) {
			ctes[name] = true
		}
		return true
	})
	var total int64
	ok := true
	walk(stmt, func(n node) bool {
		if !ok || str(n["type"]) != "BASE_TABLE" {
			return ok
		}
		parts := tableParts(n)
		if len(parts) == 1 && ctes[strings.ToLower(parts[0])] {
			return false
		}
		t, err := pl.table(parts)
		if IsUnsupported(err) {
			if b, viewOK, verr := pl.viewBytes(parts, 0); verr == nil && viewOK {
				total += b
				return false
			}
		}
		if err != nil || t == nil {
			ok = err == nil || IsUnsupported(err)
			return false
		}
		total += columnsRead(stmt, t).bytes(t)
		return false
	})
	return total, ok && pl.ctx.Err() == nil
}

// budgetContext is the context planning runs in: ctx, ended once budget
// has passed (errPlanBudget).
func budgetContext(ctx context.Context, budget time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeoutCause(ctx, budget, errPlanBudget)
}
