package distributed

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"
)

// Relations of uncertain size. A relation is estimated at the size of the
// tables it reads (see estimate), which a filter on a constant may cut a
// thousandfold: an audience, the members of one version of one audience
// in a lookup of all of them, is estimated at the lookup's 757 GB and
// holds a couple of million IDs. Estimated larger than every worker may
// read, it is partitioned, and so is every table joined to it: at a
// hundred times the data, a report shuffled 465 GB of tables to join them
// to its audience, in 925 s, where one machine filters each table by the
// audience as it reads it.

// uncertain is a relation of uncertain size that a stage reads by
// partition, with other large inputs partitioned to meet it.
type uncertain struct {
	// name is the relation's name, for messages; key identifies it across
	// plans of the statement (relationKey); out is the exchange its rows
	// are written to; estimate what it was estimated at.
	name     string
	key      string
	out      *Exchange
	estimate int64
}

// outcome is how a relation of uncertain size came out: measured at size,
// when it was estimated at estimate, and read by partition all the same
// (large) or whole.
type outcome struct {
	size     knownSize
	estimate int64
	large    bool
	at       time.Time
}

// outcomes remember how relations of uncertain size came out, by key. A
// relation measured large is planned as large, without measuring it again,
// for exploreEvery, while the tables it reads stay within a factor of two
// of their size then: reports run again, and measuring makes the stages
// that meet it wait. One measured small is measured every time, as the
// stages that read it whole wait for it anyway.
type outcomes struct {
	mu    sync.Mutex
	byKey map[string]outcome
}

// maxOutcomes bounds the relations outcomes remember.
const maxOutcomes = 4096

// record remembers how the relation of key came out.
func (o *outcomes) record(key string, out outcome) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.byKey) >= maxOutcomes {
		clear(o.byKey)
	}
	o.byKey[key] = out
}

// large returns the relations measured large since exploreEvery before now.
func (o *outcomes) large(now time.Time) map[string]outcome {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := map[string]outcome{}
	for k, v := range o.byKey {
		if v.large && now.Sub(v.at) < exploreEvery {
			out[k] = v
		}
	}
	return out
}

// knownSize is a relation's size as measured: its exchange's bytes and
// rows.
type knownSize struct{ bytes, rows int64 }

// relationKey identifies the relation of query node q, as scope sees it,
// across plans of one statement: its query with the common table
// expressions it reads, as SQL. It is empty if q does not print.
func (pl *planner) relationKey(q node, scope cteScope) string {
	n, err := standalone(q, scope)
	if err != nil {
		return ""
	}
	sql, err := pl.p.print(n)
	if err != nil {
		return ""
	}
	return sql
}

// measured returns the size to plan the relation of key with, of columns
// cols, and whether it was measured: estimated at est, or as measured
// before (opts.sizes), but past what every worker may read in full if
// its rows would not fit a task's memory in a hash table (rowMemory), so
// that a relation of few bytes and many rows is not read whole.
func (pl *planner) measured(key string, est int64, cols []Column) (int64, bool) {
	k, ok := pl.opts.sizes[key]
	if o, large := pl.opts.large[key]; !ok && large && est <= 2*o.estimate && 2*est >= o.estimate {
		// Measured large lately, over tables of about the same size: no
		// need to measure it again before partitioning.
		k, ok = o.size, true
		k.bytes = max(k.bytes, pl.opts.maxBroadcast+1)
	}
	if !ok || key == "" {
		return est, false
	}
	bytes := k.bytes
	if pl.opts.taskMemory > 0 && k.rows*rowMemory(cols) > pl.opts.taskMemory {
		bytes = max(bytes, pl.opts.maxBroadcast+1)
	}
	return bytes, true
}

// uncertainSize reports whether a relation's estimate may be far too
// large: its query, or a common table expression it reads, filters rows by
// comparing a column to a constant or to a subquery's value, or by IN.
func uncertainSize(r *relUse) bool {
	parts := []any{r.q}
	if ctes, err := closure(r.q, r.scope); err == nil {
		for _, d := range ctes {
			parts = append(parts, d.query())
		}
	}
	found := false
	for _, part := range parts {
		walk(part, func(m node) bool {
			if found {
				return false
			}
			for _, c := range conjuncts(child(m, "where_clause")) {
				found = found || selectiveFilter(c)
			}
			return !found
		})
	}
	return found
}

// selectiveFilter reports whether a conjunct compares a column to a
// constant or to a subquery's value, with = or IN.
func selectiveFilter(c node) bool {
	switch str(c["type"]) {
	case "COMPARE_EQUAL":
		l, r := child(c, "left"), child(c, "right")
		value := func(n node) bool { return str(n["class"]) == "CONSTANT" || str(n["class"]) == "SUBQUERY" }
		return str(l["class"]) == "COLUMN_REF" && value(r) || str(r["class"]) == "COLUMN_REF" && value(l)
	case "COMPARE_IN":
		kids := list(c, "children")
		first, _ := kids[0].(node)
		return len(kids) > 1 && str(first["class"]) == "COLUMN_REF"
	}
	return str(c["class"]) == "SUBQUERY" && strings.EqualFold(str(c["subquery_type"]), "ANY")
}

// uncertainOf returns the relations of uncertain size among a candidate's
// inputs read by partition, when other large inputs are partitioned to
// meet them.
func uncertainOf(c *candidate) []uncertain {
	if len(c.keyed) < 2 {
		return nil
	}
	var out []uncertain
	for _, k := range c.keyed {
		if k.rel != nil && !k.rel.known && k.rel.key != "" && k.rel.out != nil && uncertainSize(k.rel) {
			out = append(out, uncertain{name: k.item.name, key: k.rel.key, out: k.rel.out, estimate: k.rel.bytes})
		}
	}
	return out
}

// runProbing runs plan's stages in run, and returns the execution and the
// plan whose results the statement reads. A plan that partitions large
// tables to meet relations of uncertain size runs the stages that write
// those relations first, and is planned again with their sizes: a relation
// small enough for every task to read is read whole beside the tables'
// shares, which are then not partitioned at all, in a new execution from
// newRun that reads what the first wrote of it. A plan that comes out the
// same runs on in run, so that measuring costs only the wait for those
// stages. probes says how each relation came out.
func (co *Coordinator) runProbing(ctx context.Context, plan *Plan, run *execution, newRun func(id string) *execution, probes *[]string) (*execution, *Plan, error) {
	if len(plan.uncertain) == 0 || plan.replan == nil {
		return run, plan, run.run(ctx, plan.Stages)
	}
	outs := map[int]bool{}
	for _, u := range plan.uncertain {
		outs[u.out.ID] = true
	}
	first := producers(plan.Stages, outs)
	if err := run.run(ctx, first); err != nil {
		return run, plan, err
	}
	sizes := map[string]knownSize{}
	byKey := map[string]*exchangeRun{}
	written := map[string]*Exchange{}
	for _, u := range plan.uncertain {
		x := run.exchanges[u.out.ID]
		sizes[u.key] = knownSize{bytes: x.bytes, rows: x.rows}
		byKey[u.key], written[u.key] = x, u.out
	}
	next, err := plan.replan(sizes)
	if err != nil || sameStages(next, plan) {
		if err != nil {
			co.log.Warn("could not plan a statement again with the sizes of its relations; it runs as planned", "query", run.qid, "err", err)
		}
		for _, u := range plan.uncertain {
			*probes = append(*probes, probeNote(u.name, sizes[u.key], "read by partition, as planned"))
			co.outcomes.record(u.key, outcome{size: sizes[u.key], estimate: u.estimate, large: true, at: time.Now()})
		}
		co.log.Info("measured relations of uncertain size", "query", run.qid, "relations", strings.Join(*probes, "; "))
		return run, plan, run.run(ctx, stagesExcept(plan.Stages, first))
	}

	// Planned again: what the first stages wrote of each relation stands
	// for it where the new plan reads it whole, or partitioned alike.
	again := newRun(run.qid + "-m")
	again.parts = run.parts
	exchanges := map[int]*Exchange{}
	readWhole := map[int]bool{}
	for _, s := range next.Stages {
		if s.Out != nil {
			exchanges[s.Out.ID] = s.Out
		}
		for _, in := range s.Inputs {
			if in.Exchange != nil {
				if _, seen := readWhole[in.Exchange.ID]; !seen {
					readWhole[in.Exchange.ID] = true
				}
				readWhole[in.Exchange.ID] = readWhole[in.Exchange.ID] && !in.Partitioned
			}
		}
	}
	reused := map[int]bool{}
	for id, key := range next.relKeys {
		x, w := byKey[key], written[key]
		if x == nil || exchanges[id] == nil {
			continue
		}
		if readWhole[id] || slices.Equal(exchanges[id].Keys, w.Keys) && slices.Equal(exchanges[id].Casts, w.Casts) {
			again.exchanges[id] = x
			reused[id] = true
		}
	}
	for _, u := range plan.uncertain {
		how, whole := "read by partition", false
		for id, key := range next.relKeys {
			if key == u.key && readWhole[id] {
				how, whole = "read whole by every task", true
			}
		}
		*probes = append(*probes, probeNote(u.name, sizes[u.key], how+", planned again"))
		co.outcomes.record(u.key, outcome{size: sizes[u.key], estimate: u.estimate, large: !whole, at: time.Now()})
	}
	co.log.Info("measured relations of uncertain size", "query", run.qid, "relations", strings.Join(*probes, "; "),
		"stages", len(next.Stages), "before", len(plan.Stages))
	return again, next, again.run(ctx, needed(next.Stages, reused))
}

// probeNote says how a relation of uncertain size came out.
func probeNote(name string, k knownSize, how string) string {
	return fmt.Sprintf("%s: %d rows, %s, %s", name, k.rows, megabytes(k.bytes), how)
}

// megabytes writes a size in megabytes.
func megabytes(n int64) string {
	return fmt.Sprintf("%.1f MB", float64(n)/1e6)
}

// producers returns the stages, in order, that write exchanges outs and
// the exchanges those read, every writer of each.
func producers(stages []*Stage, outs map[int]bool) []*Stage {
	need := maps.Clone(outs)
	keep := make([]bool, len(stages))
	// Writers come before readers: walk back from the last.
	for changed := true; changed; {
		changed = false
		for i := len(stages) - 1; i >= 0; i-- {
			s := stages[i]
			if keep[i] || s.Out == nil || !need[s.Out.ID] {
				continue
			}
			keep[i], changed = true, true
			for _, in := range s.Inputs {
				if in.Exchange != nil {
					need[in.Exchange.ID] = true
				}
			}
		}
	}
	var out []*Stage
	for i, s := range stages {
		if keep[i] {
			out = append(out, s)
		}
	}
	return out
}

// needed returns the stages, in order, that the statement's results need,
// but those that only write exchanges in done, which are written already,
// and what only those read.
func needed(stages []*Stage, done map[int]bool) []*Stage {
	need := map[int]bool{}
	keep := make([]bool, len(stages))
	for i := len(stages) - 1; i >= 0; i-- {
		s := stages[i]
		if s.Out != nil && (!need[s.Out.ID] || done[s.Out.ID]) {
			continue
		}
		keep[i] = true
		for _, in := range s.Inputs {
			if in.Exchange != nil {
				need[in.Exchange.ID] = true
			}
		}
	}
	var out []*Stage
	for i, s := range stages {
		if keep[i] {
			out = append(out, s)
		}
	}
	return out
}

// stagesExcept returns stages but those in some, in order.
func stagesExcept(stages, some []*Stage) []*Stage {
	return slices.DeleteFunc(slices.Clone(stages), func(s *Stage) bool { return slices.Contains(some, s) })
}

// sameStages reports whether two plans run the same stages.
func sameStages(a, b *Plan) bool {
	return slices.EqualFunc(a.Stages, b.Stages, func(x, y *Stage) bool { return x.String() == y.String() && x.SQL == y.SQL })
}
