package distributed

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strings"
	"sync"
	"time"
)

// Planning limits. Planning a statement for workers costs time that a
// statement auto mode then runs on one machine pays for nothing, so auto
// mode stops planning a statement that planning does not get on with: once
// it has made no progress, planning no stage and describing no part it had
// not described, for a tenth of the time the statement is expected to take
// on one machine (at least autoStallFloor, at most maxAutoStall), or once
// planning has taken that long in all, as long as one machine would take
// it. A report expected to take minutes plans as long as it gets on: a
// report of many stages, whose parts each take a while to describe in the
// session, took longer to plan than a cap of 10 s, which sent it to one
// machine for many times as long as workers take. A cheap statement whose
// planning stalls gives up within half a second. The
// expected time is what the statement took here before, by the history of
// the plan its shape got last, or else what one machine reads of its
// tables cold, as auto mode's estimate of one machine reads them. Forced
// runs on workers and EXPLAIN DISTRIBUTED plan for as long as planning
// takes, within the statement's timeout; the planner's guards (a relation
// is never its own input, and relations nest at most maxRelationDepth
// deep) end plans that would recurse.

// autoStallShare is the share of a statement's expected time on one machine
// auto mode waits for planning to progress; autoStallFloor and
// maxAutoStall bound the wait, and unknownStall is it when the expected
// time is not known.
const (
	autoStallShare = 0.1
	autoStallFloor = 500 * time.Millisecond
	maxAutoStall   = time.Minute
	unknownStall   = 10 * time.Second
)

// planLimits are when planning stops: when it has made no progress for
// stall, or it has taken total in all; zero means never.
type planLimits struct {
	stall, total time.Duration
}

// autoLimits are auto mode's limits of planning a statement expected to
// take seconds on one machine, if known (see the top of this file).
func autoLimits(seconds float64, known bool) planLimits {
	if !known {
		return planLimits{stall: unknownStall}
	}
	expected := time.Duration(seconds * float64(time.Second))
	stall := time.Duration(autoStallShare * float64(expected))
	return planLimits{stall: min(max(stall, autoStallFloor), maxAutoStall), total: max(expected, autoStallFloor)}
}

// describeCache holds the session's descriptions of a statement's parts,
// by their text. The planner describes a part again as it tries one plan
// after another, and plans a statement more than once (again for the large
// worker, and with relations' sizes once measured), and each description
// binds the part in the session, which loads its tables from the catalog
// and their metadata from object storage. A nil cache holds nothing.
type describeCache struct {
	mu   sync.Mutex
	cols map[string][]Column
}

func (c *describeCache) get(text string) ([]Column, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cols, ok := c.cols[text]
	return slices.Clone(cols), ok
}

func (c *describeCache) put(text string, cols []Column) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cols[text] = slices.Clone(cols)
}

// describesKey keys a statement's describeCache in its planning context.
type describesKey struct{}

// withDescribes returns ctx with a describeCache for the statement, unless
// it has one, which the plans of the statement under ctx share.
func withDescribes(ctx context.Context) context.Context {
	if ctx.Value(describesKey{}) != nil {
		return ctx
	}
	return context.WithValue(ctx, describesKey{}, &describeCache{cols: map[string][]Column{}})
}

// describesOf returns ctx's describeCache, or nil.
func describesOf(ctx context.Context) *describeCache {
	c, _ := ctx.Value(describesKey{}).(*describeCache)
	return c
}

// planProfile is what planning a statement spent, for the log: how many
// parts it described in the session, how long that took, and how many
// descriptions it had already; and how many names and functions it looked
// up in the session (tables, views, their definitions), and how long that
// took.
type planProfile struct {
	describes, cached, lookups int
	describing, looking        time.Duration
}

// progressed records that planning progressed: it planned a stage or
// described a part (see limitPlanning).
func (pl *planner) progressed() {
	pl.waited.Store(int64(pl.catalogWaited()))
	pl.progress.Store(time.Now().UnixNano())
}

// CatalogWaits is an Env whose session says how long it has waited for the
// engine's catalog lock, in all, so far, the wait under way included. It may be
// asked from any goroutine.
type CatalogWaits interface {
	CatalogWaited() time.Duration
}

// catalogWaited is how long pl's session has waited for the catalog lock so
// far; zero for a session that cannot say.
func (pl *planner) catalogWaited() time.Duration {
	if w, ok := pl.env.(CatalogWaits); ok {
		return w.CatalogWaited()
	}
	return 0
}

// looked records a lookup in the session begun at start.
func (pl *planner) looked(start time.Time) {
	pl.profile.lookups++
	pl.profile.looking += time.Since(start)
}

// planShapes remembers, by statement shape (planKey without its tables'
// sizes), the plan key a statement got last, whose history says how long
// it took on one machine.
type planShapes struct {
	mu   sync.Mutex
	keys map[string]string
}

// maxShapes bounds the shapes planShapes keeps.
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
}

// staysHere says why a statement of shape runs on this replica without planning
// it for workers: planning a report of 28 stages took 15 s, where the report ran in
// 10 s here, and auto mode then chose here all the same. So where its last plan's
// history already decides that (see chooseShared), as its run here under the
// threshold or runs on workers no better than here, nothing more is asked of the
// session. Its first run pays for planning, and a run that goes far past what it
// took is guarded as any other, after which it plans again. nil where planning has
// a say: a fill, which times its write too; a statement that ran short of memory or
// was guarded; one that may yet do better on many more workers than it ran on.
func (co *Coordinator) staysHere(shape string, env Env, workers int) *errUnsupported {
	key, ok := co.shapes.key(shape)
	if !ok || strings.HasSuffix(key, "-fill") {
		return nil
	}
	r, ok := co.history.get(key)
	if !ok {
		return nil
	}
	r = comparable(r, co.opts.MemoryBytes, co.pool.lastMemory(), co.largeMemory())
	if r.local <= 0 || r.outOfMemory || r.guarded || r.large > 0 {
		return nil
	}
	others := 0
	if le, ok := env.(Loads); ok {
		others = le.Others()
	}
	share := float64(1 + min(max(others, 0), shareCap))
	var reason string
	switch {
	case r.dist > 0 && r.distWorkers > 0:
		if r.dist < riskyWithin*r.local*share || 2*r.distWorkers <= max(workers, co.opts.Pool.MaxWorkers) {
			return nil
		}
		reason = fmt.Sprintf("it ran in %.0f s on one machine and %.0f s on workers", r.local, r.dist)
	case r.local*share >= cmp.Or(co.opts.Threshold, DefaultThreshold).Seconds():
		return nil
	default:
		reason = fmt.Sprintf("it ran in %.0f s on one machine, under %.0f s", r.local, cmp.Or(co.opts.Threshold, DefaultThreshold).Seconds()/share)
	}
	co.log.Info("chose where a statement runs", "on", "one machine", "reason", reason, "key", key, "one_machine_seconds", math.Round(r.local), "planned", false)
	u := &errUnsupported{reason: "auto mode chose one machine: " + reason, key: key, oneMachine: r.local}
	if r.dist > 0 {
		u.onWorkers = r.dist
	}
	return u
}

// expectedLocal is how many seconds the statement of shape is expected to
// take on one machine: what it took here, by the history of the plan its
// shape got last or by what ran it here (see LocalRuns), or what one machine
// reads of its tables cold. ok is false when none is known.
func (co *Coordinator) expectedLocal(pl *planner, stmt node, shape string) (float64, bool) {
	if key, ok := co.shapes.key(shape); ok {
		if r, ok := co.history.get(key); ok && r.local > 0 {
			return r.local, true
		}
	}
	if took, ok := localRun(pl.env); ok {
		return took.Seconds(), true
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

// logPlanning logs what planning a statement spent, at INFO when it took a
// second or more or ran past its limits: how long, the stages it planned,
// the parts it described in the session and how long that took, those it
// had described already, and its other lookups in the session and how long
// they took; the rest is the planner's own work.
func (co *Coordinator) logPlanning(pl *planner, plan *Plan, err error, limits planLimits) {
	took := time.Since(pl.started)
	why, _ := pl.stalled.Load().(string)
	level := slog.LevelDebug
	if took >= time.Second || why != "" {
		level = slog.LevelInfo
	}
	stages := len(pl.stages)
	if plan != nil {
		stages = len(plan.Stages)
	}
	co.log.Log(context.Background(), level, "planned a statement", "seconds", math.Round(took.Seconds()*1000)/1000, "stages", stages,
		"describes", pl.profile.describes, "describes_cached", pl.profile.cached,
		"describe_seconds", math.Round(pl.profile.describing.Seconds()*1000)/1000,
		"lookups", pl.profile.lookups, "lookup_seconds", math.Round(pl.profile.looking.Seconds()*1000)/1000,
		"catalog_wait_seconds", math.Round((pl.catalogWaited()-pl.waitedFrom).Seconds()*1000)/1000,
		"stall_limit", limits.stall, "limit", limits.total, "gave_up", why, "err", err)
}

// limitPlanning returns the context planner pl plans in: ctx, ended
// (errPlanBudget) once pl has made no progress for limits.stall, or
// planned for limits.total, when they are set; pl.stalled then says which.
// stop ends the watch. Time the session spends waiting for the catalog lock
// counts for neither: the statement waits for it to run wherever it runs,
// and a writer holding it is no sign that planning does not get on. Beside a
// refresh that took the lock alone 12 times for up to 13 s, 275 of a mixed
// load's dashboard statements gave up planning for it and ran on one machine,
// a tile that took 10 to 14 s on workers taking 75 to 88 s there.
func limitPlanning(ctx context.Context, pl *planner, limits planLimits) (context.Context, func()) {
	pl.started = time.Now()
	pl.waitedFrom = pl.catalogWaited()
	pl.waited.Store(int64(pl.waitedFrom))
	pl.progress.Store(pl.started.UnixNano())
	if limits.stall <= 0 && limits.total <= 0 {
		return ctx, func() {}
	}
	pctx, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	tick := 50 * time.Millisecond
	if limits.stall > 0 {
		tick = min(tick, limits.stall/4)
	}
	go func() {
		t := time.NewTicker(tick)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-pctx.Done():
				return
			case now := <-t.C:
				waited := pl.catalogWaited()
				idle := now.Sub(time.Unix(0, pl.progress.Load())) - (waited - time.Duration(pl.waited.Load()))
				switch {
				case limits.stall > 0 && idle >= limits.stall:
					pl.stalled.Store("planning planned no stage and described no part for " + limits.stall.String())
				case limits.total > 0 && now.Sub(pl.started)-(waited-pl.waitedFrom) >= limits.total:
					pl.stalled.Store("planning took " + limits.total.String() + ", as long as one machine should take the statement")
				default:
					continue
				}
				cancel(errPlanBudget)
				return
			}
		}
	}()
	return pctx, func() {
		close(done)
		cancel(nil)
	}
}
