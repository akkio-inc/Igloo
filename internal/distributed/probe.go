package distributed

import (
	"context"
	"fmt"
	"maps"
	"math"
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
	// small says the statistics of the tables it reads bound it under what
	// every worker may read (see sizeBound): it is measured with the
	// workers to itself, as one measured small lately is.
	small bool
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
// relation some stage read by partition is planned with the size measured
// then, without measuring it again, for exploreEvery, while the tables it
// reads stay within a factor of two of their size then: reports run again,
// and measuring makes the stages that meet it wait. Planned with that size,
// a statement gets the plan it got once measured, which may read the
// relation whole in some places and by partition in others. One every
// stage read whole is measured every time, as the stages that read it
// whole wait for it anyway.
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

// smallLately reports whether every relation of u was measured and read
// whole since exploreEvery before now, over tables within a factor of two
// of their size then.
func (o *outcomes) smallLately(u []uncertain, now time.Time) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, r := range u {
		v, ok := o.byKey[r.key]
		if !ok || v.large || now.Sub(v.at) >= exploreEvery || r.estimate > 2*v.estimate || 2*r.estimate < v.estimate {
			return false
		}
	}
	return len(u) > 0
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

// written returns the relations of u whose exchanges stages write: a stage
// that writes one may have been fused into the stage that reads it (see
// fusePartitions), which then computes its rows itself, and there is
// nothing to measure.
func written(u []uncertain, stages []*Stage) []uncertain {
	return slices.DeleteFunc(slices.Clone(u), func(r uncertain) bool {
		return !slices.ContainsFunc(stages, func(s *Stage) bool { return s.Out == r.out })
	})
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
	return pl.withoutMark(sql)
}

// measured returns the size to plan the relation of key with, of columns
// cols, the rows it was measured at, and whether it was measured: estimated
// at est, or as measured before (opts.sizes), but past what every worker
// may read in full if its hash table would not fit comfortably beside a
// task's own work, in wholeShare of the task's memory. Every task that
// reads it whole builds that table, and stages that read it may run side
// by side: at ten times the data, an audience of 45.9 million IDs, 453 MB,
// read whole by every task of two stages that counted people distinct by
// segment, took a report from 84 s to 608 s, its workers spilling 28.5 GB;
// a cohort of 10 million, 316 MB, from 176 s to 260 s.
func (pl *planner) measured(key string, est int64, cols []Column) (int64, int64, bool) {
	k, ok := pl.opts.sizes[key]
	if o, large := pl.opts.large[key]; !ok && large && est <= 2*o.estimate && 2*est >= o.estimate {
		// Measured lately and read by partition, over tables of about the
		// same size: planned with the size measured then, as the statement
		// was planned again then. Planned past what every worker may read,
		// a report whose audience the plan with its size read whole in some
		// branches ran its first plan, 34 stages for 24, in 1,698 s for
		// 248 s.
		k, ok = o.size, true
	}
	if !ok || key == "" {
		return est, 0, false
	}
	bytes := k.bytes
	if pl.opts.taskMemory > 0 && k.rows*measuredRowMemory(k, cols) > pl.opts.taskMemory/wholeShare {
		bytes = max(bytes, pl.opts.maxBroadcast+1)
	}
	return bytes, k.rows, true
}

// wholeShare is the share of a task's memory, one in wholeShare, that the
// hash table of a measured relation every task reads whole may take.
const wholeShare = 4

// wholeCheaper reports whether every task of c's stage reading the measured
// relation r whole costs less than partitioning to meet it: each task,
// about one a worker, builds a hash table of all its rows, where
// partitioning writes the rows of the table the stage would divide to an
// exchange and reads them back, and builds r's table once. Rows stand for
// both costs. Without a table to divide, or a count of its rows, there is
// nothing to weigh.
func (pl *planner) wholeCheaper(r *relUse, c *candidate) bool {
	var split *tableUse
	for _, u := range c.uses {
		if u.splittable && (split == nil || u.table.Bytes > split.table.Bytes) {
			split = u
		}
	}
	if split == nil || split.table.Rows <= 0 {
		return true
	}
	rows := rowsRead(c.sel, split.table)
	return float64(max(pl.opts.workers, 1))*float64(r.rows) <= 2*rows+float64(r.rows)
}

// measuredRowMemory is the memory a row of a measured relation of columns
// cols takes in a hash table: as rowMemory guesses from their types, or
// from four times its compressed bytes a row if that is more, as for long
// strings, which the types' guess counts at 32 bytes whatever their length.
func measuredRowMemory(k knownSize, cols []Column) int64 {
	m := rowMemory(cols)
	if k.rows > 0 {
		m = max(m, 2*4*(k.bytes/k.rows)+64)
	}
	return m
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
// meet them, and whether statistics bound each under what every worker
// may read.
func (pl *planner) uncertainOf(c *candidate) []uncertain {
	if len(c.keyed) < 2 {
		return nil
	}
	var out []uncertain
	for _, k := range c.keyed {
		if k.rel != nil && !k.rel.known && k.rel.key != "" && k.rel.out != nil && uncertainSize(k.rel) {
			u := uncertain{name: k.item.name, key: k.rel.key, out: k.rel.out, estimate: k.rel.bytes}
			if bound, ok := pl.sizeBound(k.rel.q, k.rel.scope, 0); ok && bound <= pl.opts.maxBroadcast {
				u.small = true
			}
			out = append(out, u)
		}
	}
	return out
}

// smallByStatistics reports whether statistics bound every relation of u
// under what every worker may read.
func smallByStatistics(u []uncertain) bool {
	for _, r := range u {
		if !r.small {
			return false
		}
	}
	return len(u) > 0
}

// sizeBound bounds the size of the rows of query node q by the statistics
// of the tables it reads (the distinct counts of ANALYZE, or of Iceberg's
// theta sketches): a grouping, by GROUP BY or DISTINCT, of their columns
// makes no more rows than the product of the columns' distinct counts; a
// conjunct of its WHERE clause comparing a column to a value with = keeps
// no more of a table's rows than the rows over the column's distinct
// count; a query of one relation, as of a common table expression, makes
// no more rows than it. Its size is its rows at the memory its result's
// columns take (memoryBytes), which for 1.9 million IDs of 32 characters,
// an audience, came to 61.9 MB measured. ok is false where the statistics
// bound nothing. An audience, the distinct members of one version of a
// lookup of every audience, is estimated at the lookup's size, which may
// be a thousand times its own: partitioning 16 workers' tables to meet it
// beside it, at a hundred times the data, took a report 305 s where,
// measured first, it took 50 s.
func (pl *planner) sizeBound(q node, scope cteScope, depth int) (int64, bool) {
	if depth > 8 || str(q["type"]) != "SELECT_NODE" {
		return 0, false
	}
	sc := scope.with(q)
	var tables []*Table
	var inner []node
	var innerScopes []cteScope
	var visit func(ref node)
	visit = func(ref node) {
		switch str(ref["type"]) {
		case "JOIN":
			visit(child(ref, "left"))
			visit(child(ref, "right"))
		case "SUBQUERY":
			inner, innerScopes = append(inner, subqueryNode(ref)), append(innerScopes, sc)
		case "BASE_TABLE":
			parts := tableParts(ref)
			if len(parts) == 1 {
				if d := sc.find(parts[0]); d != nil {
					inner, innerScopes = append(inner, d.query()), append(innerScopes, d.scope)
					return
				}
			}
			if t, err := pl.table(parts); err == nil {
				tables = append(tables, t)
			}
		}
	}
	visit(child(q, "from_table"))
	// The tables of the relations it reads, for their columns' counts.
	all := slices.Clone(tables)
	for i, n := range inner {
		all = append(all, pl.tablesOf(n, innerScopes[i], depth+1)...)
	}
	column := func(e any) (Column, int64, bool) {
		n, _ := e.(node)
		if str(n["class"]) != "COLUMN_REF" {
			return Column{}, 0, false
		}
		names := list(n, "column_names")
		if len(names) == 0 {
			return Column{}, 0, false
		}
		name := strings.ToLower(str(names[len(names)-1]))
		var col Column
		var ndv int64
		for _, t := range all {
			for _, c := range t.Columns {
				if strings.EqualFold(c.Name, name) && t.Distinct[name] > ndv {
					col, ndv = c, t.Distinct[name]
				}
			}
		}
		return col, ndv, ndv > 0
	}
	width := func() float64 {
		var w float64
		for _, item := range list(q, "select_list") {
			if c, _, ok := column(item); ok {
				w += memoryBytes(c.Type)
			} else {
				w += planExprWidth
			}
		}
		return max(w, planExprWidth)
	}
	rows := math.Inf(1)
	keys := list(q, "group_expressions")
	if len(keys) == 0 && hasModifier(q, "DISTINCT_MODIFIER") {
		keys = list(q, "select_list")
	}
	if len(keys) > 0 {
		product := 1.0
		for _, k := range keys {
			_, ndv, ok := column(k)
			if !ok {
				product = math.Inf(1)
				break
			}
			product *= float64(ndv)
		}
		rows = min(rows, product)
	}
	if len(tables) == 1 && len(inner) == 0 && tables[0].Rows > 0 {
		kept := float64(tables[0].Rows)
		for _, c := range conjuncts(child(q, "where_clause")) {
			if str(c["type"]) != "COMPARE_EQUAL" || !selectiveFilter(c) {
				continue
			}
			for _, side := range []string{"left", "right"} {
				if _, ndv, ok := column(child(c, side)); ok {
					kept = min(kept, float64(tables[0].Rows)/float64(ndv))
				}
			}
		}
		rows = min(rows, kept)
	}
	if len(tables) == 0 && len(inner) == 1 {
		if b, ok := pl.sizeBound(inner[0], innerScopes[0], depth+1); ok {
			// No more rows than the relation it reads.
			if !math.IsInf(rows, 1) {
				b = min(b, int64(math.Ceil(rows*width())))
			}
			return b, true
		}
	}
	if math.IsInf(rows, 1) {
		return 0, false
	}
	return int64(math.Ceil(rows * width())), true
}

// tablesOf returns the catalog tables query node q reads, through its
// subqueries and common table expressions.
func (pl *planner) tablesOf(q node, scope cteScope, depth int) []*Table {
	if depth > 8 || str(q["type"]) != "SELECT_NODE" {
		return nil
	}
	sc := scope.with(q)
	var out []*Table
	var visit func(ref node)
	visit = func(ref node) {
		switch str(ref["type"]) {
		case "JOIN":
			visit(child(ref, "left"))
			visit(child(ref, "right"))
		case "SUBQUERY":
			out = append(out, pl.tablesOf(subqueryNode(ref), sc, depth+1)...)
		case "BASE_TABLE":
			parts := tableParts(ref)
			if len(parts) == 1 {
				if d := sc.find(parts[0]); d != nil {
					out = append(out, pl.tablesOf(d.query(), d.scope, depth+1)...)
					return
				}
			}
			if t, err := pl.table(parts); err == nil {
				out = append(out, t)
			}
		}
	}
	visit(child(q, "from_table"))
	return out
}

// runProbing runs plan's stages in run, and returns the execution and the
// plan whose results the statement reads. A plan that partitions large
// tables to meet relations of uncertain size is planned again with their
// sizes once the stages that write them are done: a relation small enough
// for every task to read is read whole beside the tables' shares, which
// are then not partitioned at all, in a new execution from newRun that
// reads what the first wrote of the relations. A plan that comes out the
// same runs on in run. probes says how each relation came out.
//
// Meanwhile the rest of the plan runs too, but for the stages that read
// the relations, which wait for the decision, and stages under way are
// canceled if the plan changes: waiting for the relations before starting
// the stages that partition the tables added the relations' 9 s to a
// report of 44 s, and 25 s to one of 125 s, at ten times the data, whose
// relations came out large. Relations measured small lately are measured
// with the workers to themselves, as they will be read whole: at a hundred
// times the data, an audience took 377 s beside the stages partitioning
// the tables to meet it.
func (co *Coordinator) runProbing(ctx context.Context, plan *Plan, run *execution, newRun func(id string) *execution, load func([]*Stage) error,
	probes *[]string) (*execution, *Plan, error) {
	if len(plan.uncertain) == 0 || plan.replan == nil {
		return run, plan, run.run(ctx, plan.Stages)
	}
	outs := map[int]bool{}
	for _, u := range plan.uncertain {
		outs[u.out.ID] = true
	}
	first := producers(plan.Stages, outs)
	// Measured small lately, or bound small by statistics, the relations
	// are measured with the workers to themselves: partitioning tables to
	// meet them beside them is work thrown away once they are read whole.
	speculate := !co.outcomes.smallLately(plan.uncertain, time.Now()) && !smallByStatistics(plan.uncertain)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	decided := make(chan struct{})
	finished := make(chan error, 1)
	if speculate {
		// What reads the relations, but for what writes them, waits to
		// know which plan runs.
		run.held = map[int]<-chan struct{}{}
		for _, s := range plan.Stages {
			if !slices.Contains(first, s) && slices.ContainsFunc(s.Inputs, func(in Input) bool { return in.Exchange != nil && outs[in.Exchange.ID] }) {
				run.held[s.ID] = decided
			}
		}
		wait := run.start(runCtx, plan.Stages)
		go func() { finished <- wait() }()
	} else if err := run.run(ctx, first); err != nil {
		return run, plan, err
	}
	sizes := map[string]knownSize{}
	for _, u := range plan.uncertain {
		x := run.exchanges[u.out.ID]
		if x == nil {
			cancel()
			if speculate {
				<-finished
			}
			return run, plan, fmt.Errorf("no stage writes relation %s, of uncertain size", u.name)
		}
		select {
		case <-x.done:
		case err := <-finished:
			// Failed before the relation was written, or done.
			return run, plan, err
		}
		sizes[u.key] = knownSize{bytes: x.bytes, rows: x.rows}
	}
	next, err := plan.replan(sizes)
	if err != nil || sameStages(next, plan) {
		if err != nil {
			co.log.Warn("could not plan a statement again with the sizes of its relations; it runs as planned", "query", run.qid, "err", err)
		}
		for _, u := range distinctKeys(plan.uncertain) {
			*probes = append(*probes, probeNote(u.name, sizes[u.key], "read by partition, as planned"))
			co.outcomes.record(u.key, outcome{size: sizes[u.key], estimate: u.estimate, large: true, at: time.Now()})
		}
		co.log.Info("measured relations of uncertain size", "query", run.qid, "relations", strings.Join(*probes, "; "))
		// Run again after a failure, it runs as it is, without measuring.
		as := *plan
		as.uncertain = nil
		if !speculate {
			return run, &as, run.run(ctx, stagesExcept(plan.Stages, first))
		}
		close(decided)
		return run, &as, <-finished
	}

	// Planned again: stop the first plan, and read what its stages wrote of
	// each relation where the new plan reads it whole, or partitioned alike.
	if speculate {
		cancel()
		if err := <-finished; err != nil && ctx.Err() != nil {
			return run, plan, ctx.Err()
		}
	}
	// The tables only the new plan reads, at their current snapshots, now
	// that no stage of the first reads the list of them.
	if err := load(next.Stages); err != nil {
		return run, plan, err
	}
	byKey := map[string]*exchangeRun{}
	written := map[string]*Exchange{}
	for _, s := range plan.Stages {
		if s.Out == nil || plan.relKeys[s.Out.ID] == "" || run.exchanges[s.Out.ID] == nil {
			continue
		}
		x := run.exchanges[s.Out.ID]
		select {
		case <-x.done:
			byKey[plan.relKeys[s.Out.ID]], written[plan.relKeys[s.Out.ID]] = x, s.Out
		default:
			// Canceled before it was complete.
		}
	}
	again := newRun(run.qid + "-m")
	again.parts = run.parts
	// The workers the relations' stages ran on count as the statement's
	// too, not those of stages canceled.
	for _, s := range first {
		maps.Copy(again.used, run.usedBy[s.ID])
	}
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
		if readWhole[id] || slices.Equal(exchanges[id].Keys, w.Keys) && slices.Equal(exchanges[id].Casts, w.Casts) && exchanges[id].Buckets == w.Buckets && exchanges[id].UUIDHash == w.UUIDHash {
			again.exchanges[id] = x
			reused[id] = true
		}
	}
	for _, u := range distinctKeys(plan.uncertain) {
		// Read whole by every stage that reads it, it is measured again
		// next time; read by partition anywhere, the next plan is made with
		// its size (see outcomes), as this one was.
		reads, whole := 0, 0
		for id, key := range next.relKeys {
			if _, read := readWhole[id]; key == u.key && read {
				reads++
				if readWhole[id] {
					whole++
				}
			}
		}
		how := "read by partition"
		switch {
		case reads > 0 && whole == reads:
			how = "read whole by every task"
		case whole > 0:
			how = "read whole in places and by partition in others"
		}
		*probes = append(*probes, probeNote(u.name, sizes[u.key], how+", planned again"))
		co.outcomes.record(u.key, outcome{size: sizes[u.key], estimate: u.estimate, large: reads == 0 || whole < reads, at: time.Now()})
	}
	co.log.Info("measured relations of uncertain size", "query", run.qid, "relations", strings.Join(*probes, "; "),
		"stages", len(next.Stages), "before", len(plan.Stages))
	return again, next, again.run(ctx, needed(next.Stages, reused))
}

// distinctKeys returns the relations of u, the first of each key: a
// common table expression two others read is one relation.
func distinctKeys(u []uncertain) []uncertain {
	seen := map[string]bool{}
	return slices.DeleteFunc(slices.Clone(u), func(r uncertain) bool {
		dup := seen[r.key]
		seen[r.key] = true
		return dup
	})
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
