package distributed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/akkio-inc/igloo/internal/ai"
)

// Planning splits a statement into jobs. A job is one part of the statement
// that workers run in parallel: a SELECT whose FROM clause reads one large
// catalog table (the split table) together with smaller ones, typically
// with an aggregation on top. Each worker runs the job's fragment over a
// share of the split table's data files and all of the smaller tables, and
// returns partial results; the coordinator then runs the rest of the
// statement with each job's SELECT replaced by a combine step over the
// partial results.
//
// That is correct when every row of the split table is processed by
// exactly one worker, and what the job's FROM and WHERE produce for a row
// depends only on that row and on the other tables, which every worker
// reads in full: inner joins and cross joins, outer, semi and anti joins
// that keep the split table's rows, and filters, including subqueries over
// the other tables. Aggregates are computed in two steps: partial
// aggregates per worker (sum, count, min, max, ...), combined by the
// coordinator (sum of sums, sum of counts, min of mins, ...). Aggregates
// over DISTINCT values are computed by the coordinator over the distinct
// values the workers found. Anything else stays on one machine.

// Env is the session a statement runs in, as the planner sees it.
type Env interface {
	// Table resolves a table name as a statement writes it (one to three
	// parts) the way DuckDB would in this session. ok is false when the
	// name means anything but a catalog table: a view, a temporary or
	// local table, or a table of another database.
	Table(ctx context.Context, parts []string) (t *Table, ok bool, err error)
	// Describe binds a query in the session, without running it, and
	// returns its result columns.
	Describe(ctx context.Context, query string) ([]Column, error)
	// UserFunction reports whether name is a function or macro that a user
	// defined rather than one built into DuckDB. Only built-in functions
	// behave the same on every machine.
	UserFunction(ctx context.Context, name string) (bool, error)
	// Setting returns the session's value of a DuckDB setting.
	Setting(ctx context.Context, name string) (string, error)
	// Aggregates returns the names of the session's aggregate functions,
	// those of the extensions it loaded included.
	Aggregates(ctx context.Context) ([]string, error)
	// View reports whether a name, as a statement writes it, may mean a
	// view in this session: a view, or a name that does not resolve to
	// exactly one relation.
	View(ctx context.Context, parts []string) (bool, error)
	// ViewDefinition returns the definition of the view a name, as a
	// statement writes it, means in this session. ok is false when the name
	// does not mean exactly one view whose query the planner may read in
	// its place: one of Igloo's catalog, of its local copies of those or of
	// the session's temporary ones.
	ViewDefinition(ctx context.Context, parts []string) (def *ViewDefinition, ok bool, err error)
}

// GroupFunctions is an Env whose session has functions worth calling where
// a group is whole, on workers, rather than on the coordinator once every
// group reached it: costly ones over a group's aggregates, such as one
// that forecasts a series from list() of its points. Workers must define
// them as the coordinator does, and UserFunction must not count them as a
// user's. An aggregation whose select list calls one completes its groups
// by group, in a second stage, which calls it too.
type GroupFunctions interface {
	GroupFunction(name string) bool
}

// ViewDefinition is a view's query and how to read the names in it.
type ViewDefinition struct {
	// Query is the view's SELECT.
	Query string
	// Resolve returns the database, schema and name of the one relation a
	// name written in Query means there, as DuckDB resolves names in the
	// body of the view; ok is false when that is not one relation.
	Resolve func(ctx context.Context, parts []string) (qualified []string, ok bool, err error)
}

// Column is a column of a table or a result.
type Column struct {
	Name string `json:"name"`
	// Type is the DuckDB type, as DESCRIBE prints it.
	Type string `json:"type"`
	// ID is the Iceberg field ID of a table column.
	ID int `json:"id,omitempty"`
	// Nested says how a nested column of a table nests its fields, so that
	// workers can check that a data file nests them as the table does.
	Nested *Nesting `json:"nested,omitempty"`
}

// Nesting is how a list, map or struct column nests its fields.
type Nesting struct {
	// Kind is list, map or struct.
	Kind string `json:"kind"`
	// Fields are a list's element, a map's key and value, or a struct's
	// fields.
	Fields []Field `json:"fields"`
}

// Field is a field of a nested column.
type Field struct {
	Name   string   `json:"name"`
	ID     int      `json:"id"`
	Nested *Nesting `json:"nested,omitempty"`
}

// Table is a catalog table at one of its snapshots.
type Table struct {
	Schema, Name string
	// Columns are the table's columns as DuckDB sees them, with their
	// Iceberg field IDs.
	Columns []Column
	// Bytes is the size of the table's data files, and Rows its records,
	// zero if unknown.
	Bytes, Rows int64
	// Distinct are the approximate distinct counts of its columns, by
	// lower-case name, as ANALYZE keeps them; a column without one is
	// missing.
	Distinct map[string]int64
	// Buckets are the bucket transforms of its partition spec that every
	// data file of its snapshot was written under (see bucket.go); each
	// File's Buckets says the file's bucket of each.
	Buckets []Bucketing
	// Unsupported, if set, says why workers cannot read the table, such as
	// delete files they cannot apply.
	Unsupported string
	// Snapshot is the table's snapshot ID. Execute reads every table at its
	// current snapshot, and the coordinator's own reads of the table in the
	// same statement at that snapshot too.
	Snapshot int64
	// Versioned says the table has snapshots, as Iceberg tables do, which
	// the coordinator's reads can be pinned to (AT (VERSION => ...)).
	Versioned bool
	// source is the Source's own handle on the snapshot.
	source any
}

func (t *Table) key() string { return strings.ToLower(t.Schema + "." + t.Name) }

func (t *Table) String() string { return t.Schema + "." + t.Name }

// Plan is a statement split into stages that workers run (see stage.go).
type Plan struct {
	// Stages are in an order in which each comes after the stages whose
	// exchanges it reads.
	Stages []*Stage
	// final is the statement with each distributed part replaced by its
	// combine step, which reads a placeholder table until the stage whose
	// rows it combines has run.
	final node
	// force distributes whatever the tables' sizes; with mayStart it uses
	// every worker, starting them if need be, and without it only workers
	// running, as many as the sizes call for.
	force    bool
	mayStart bool
	// pins are the coordinator's own reads, in final, of tables that stages
	// read: by the name as written (lower case, parts joined by NUL), the
	// table's key. Execute points them at the snapshot the workers read.
	pins map[string]string
	// local says why parts of the statement stay on the coordinator, for
	// logs: the first few reasons.
	local []string
	// Key identifies the statement in the history of its runs, for
	// Coordinator.Observe (see planKey).
	Key string
	// workers is how many workers auto mode chose (see pick), and large
	// says the large worker, alone; placement says where and why, for the
	// query history.
	workers   int
	large     bool
	placement string
	// shortOfMemory says the statement would not fit in the coordinator's
	// memory: its working set is over its share (estimateTimes), or it ran
	// out of memory there before. farBeyond says its working set by
	// DuckDB's plan, whatever a run of its shape allocated here, is more
	// than farBeyondRatio times the coordinator's room for it.
	shortOfMemory bool
	farBeyond     bool
	// uncertain are relations of uncertain size its stages read by
	// partition, with large tables partitioned to meet them; relKeys are
	// the keys of the relations its exchanges hold, by exchange ID.
	uncertain []uncertain
	relKeys   map[int]string
	// replan plans the statement again with the sizes of relations
	// measured, as probing them does (see Execute).
	replan func(sizes map[string]knownSize) (*Plan, error)
	// values are the scalar subqueries the coordinator computes, with
	// valueEnv, before the stages run, which read them from variables (see
	// values.go).
	values   []*value
	valueEnv ValueEnv
	// writes says its tasks can write the table it fills (WorkersWrite), and
	// target is how, once the caller said (Write).
	writes bool
	target *WriteTarget
	// versions are the snapshots the statement reads tables at, AT
	// (VERSION => n), by key: Execute runs it on workers only if each is
	// the snapshot it loads (see pinVersion).
	versions map[string]int64
	// buckets say where bucketed tables are read by bucket, without an
	// exchange, and why others are exchanged all the same, for EXPLAIN
	// DISTRIBUTED (see bucket.go).
	buckets []string
	// streamMerged says the session reads a merge of sorted rows as a
	// stream (see StreamMerged).
	streamMerged bool
}

// StreamMerged has Execute leave a merge of the sorted rows a statement
// reads to be made as the statement reads them, as a view the session
// registers before it runs the execution's SQL (see Execution.Merged),
// rather than merge them into a file first.
func (p *Plan) StreamMerged() { p.streamMerged = true }

// ShortOfMemory reports whether the statement would not fit in the
// coordinator's memory, so that running it there instead of on workers
// would fail, slowly.
func (p *Plan) ShortOfMemory() bool { return p.shortOfMemory }

// FitsHere reports whether the coordinator could run the statement itself,
// should the workers chosen fail: not when its working set by DuckDB's
// plan is far beyond the coordinator's memory (farBeyondRatio times its
// room), whatever a run of its shape allocated there once, as at a tenth
// of the data. At a hundred times the data, a report estimated at 250 GB
// ran on the coordinator after the large worker failed, spilled 167 GB and
// was killed after 9 minutes; 16 workers ran it in 729 s.
func (p *Plan) FitsHere() bool { return !p.shortOfMemory && !p.farBeyond }

// Workers is how many workers the statement was planned to run on, 1 for
// the large worker; Large says it runs on the large worker.
func (p *Plan) Workers() int { return p.workers }

// Large reports whether the statement runs on the large worker alone.
func (p *Plan) Large() bool { return p.large }

// Placement says where auto mode, or the session's mode, runs the
// statement and why: how many workers or the large worker, and the reason,
// as the query history records it (see OnLarge).
func (p *Plan) Placement() string { return p.placement }

// job is a candidate on its way to stages: the tables its fragment reads,
// by the names it reads them as.
type job struct {
	// split is the table whose files are divided among the workers; the
	// fragment reads it as splitView. A job whose inputs are keyed has none.
	split     *Table
	splitView string
	// views are the tables every task reads in full, and the split table.
	views []view
	// keyed are the inputs each task reads a partition of.
	keyed []*keyed
	// whole are the relations of one row every task reads.
	whole []wholeRel
}

// wholeRel is a relation every task reads in full, under a view name.
type wholeRel struct {
	view string
	rel  *relUse
}

// view is a table a fragment reads under a generated name.
type view struct {
	name  string
	table *Table
}

// planOptions decide when a statement is worth distributing.
type planOptions struct {
	// force distributes whatever the tables' sizes.
	force bool
	// minBytes is the smallest split table worth distributing.
	minBytes int64
	// maxBroadcast caps the total size of the tables every worker reads in
	// full.
	maxBroadcast int64
	// now is what the statement's now() and current_date stand for, so
	// every machine sees the same time.
	now time.Time
	// aggregation, if set, overrides how aggregations complete their groups
	// where there is a choice (aggGather or aggByGroup), for tests.
	aggregation string
	// sizes are the sizes of relations measured before (see probe.go), by
	// relation key; taskMemory is what a worker's task may plan on, to
	// check that a measured relation every task reads fits it.
	sizes      map[string]knownSize
	taskMemory int64
	// workers are the workers the statement may run on, for weighing
	// what every task reads whole.
	workers int
	// writes says the statement fills a table whose files workers may write
	// (see writes.go): every row of a table may go to workers, which then
	// need not cross to the coordinator (see rowsJob), and the stage whose
	// rows are the result is made ready to write them (markWrites).
	writes bool
	// bitmaps says count(DISTINCT) of 32-bit unsigned IDs combines roaring
	// bitmaps (combineBitmap): the rb extension is loaded, as on every
	// machine, and the session turned them on (see bitmapsOn).
	bitmaps bool
	// large are relations measured large lately, by key, with what they
	// were estimated at then (see outcomes).
	large map[string]outcome
	// values are the variables of the values the coordinator computes for
	// the stages (see computedValues): the only variables fragments may
	// read.
	values map[string]bool
}

// planner plans one statement.
type planner struct {
	ctx  context.Context
	env  Env
	p    *parser
	opts planOptions
	// aggs are the names of DuckDB's aggregate functions.
	aggs map[string]bool
	// producing are the relations being produced, by relationKey, and
	// depth how many are nested: a relation never reads itself, and
	// relations nest no deeper than maxRelationDepth (see relationExchange).
	producing map[string]bool
	depth     int
	// started is when planning began, progress when it last planned a
	// stage or described a part (Unix nanoseconds), and stalled why its
	// limits ended it (see limitPlanning).
	started  time.Time
	progress atomic.Int64
	stalled  atomic.Value
	// describes are the session's descriptions of parts, by their text,
	// for the statement (see describe), and profile what planning spent.
	describes *describeCache
	profile   planProfile

	stages []*Stage
	// exchanges counts the exchanges planned.
	exchanges int
	// memo records the exchanges remembered for common table expressions,
	// for taking them back (see savepoint).
	memo []memoEntry
	// unread are the common table expressions nothing on the coordinator
	// reads any more.
	unread map[*cteDef]bool
	// overs numbers the relations rewritten for window functions over all
	// rows (overAll).
	overs int
	// forced are subqueries in FROM to read as relations, though they pass
	// their rows through: those IN subqueries become (semiJoins).
	forced map[uintptr]bool
	// ins numbers them.
	ins int
	// inlined are the entries of common table expressions read as their
	// queries in their one reader's place (inline); inlines counts those
	// reads, and noInline turns them off. grouped counts the subqueries
	// planned to group where their tables' buckets are read, and noGrouped
	// turns that off (see groupsByBucket).
	inlined   map[uintptr]bool
	inlines   int
	noInline  bool
	grouped   int
	noGrouped bool
	// gathers are the SELECTs that read a set operation workers compute,
	// whose rows the coordinator reads as they are (see setOperationJob).
	gathers map[uintptr]bool
	// fused numbers the views of stages fusePartitions took out.
	fused int
	// reasons say why candidates were not distributed, for logs.
	reasons []string
	// tables caches resolved names.
	tables map[string]tableResult
	// versions are the snapshots the statement reads tables at, by key (see
	// pinVersion).
	versions map[string]int64
	// defaultCollation caches the session's default_collation; nil until
	// read.
	defaultCollation *string
	// undo puts back the views a candidate that did not qualify read as
	// their queries, so that the rest of the statement reads them as views.
	undo []func()
	// expanded counts the views read as their queries.
	expanded int
	// uncertain are the relations of uncertain size that stages read by
	// partition, with other large inputs partitioned to meet them.
	uncertain []uncertain
	// relKeys are the keys of the relations exchanges hold, by exchange.
	relKeys map[*Exchange]string
	// bucketNotes say where bucketed tables were read by bucket, without
	// an exchange, or why they were exchanged all the same (see bucket.go).
	bucketNotes []string
}

type tableResult struct {
	t  *Table
	ok bool
}

func newPlanner(ctx context.Context, env Env, p *parser, aggs map[string]bool, opts planOptions) *planner {
	return &planner{ctx: ctx, env: env, p: p, opts: opts, aggs: aggs, tables: map[string]tableResult{}}
}

// plan splits stmt, a parsed SELECT, into jobs. It returns an
// errUnsupported error when no part of it can run distributed.
func (pl *planner) plan(stmt node) (*Plan, error) {
	q := clone(stmt).(node)
	pl.dropOrders(q)
	pl.markTopK(q)
	out, err := pl.query(q, true, nil)
	if err != nil {
		return nil, err
	}
	prune(out)
	if len(pl.stages) == 0 {
		reason := "no part reads a large enough table"
		if len(pl.reasons) > 0 {
			reason = strings.Join(pl.reasons, "; ")
		}
		return nil, unsupported("%s", reason)
	}
	if err := pl.fuseRows(); err != nil {
		return nil, err
	}
	if err := pl.fusePartitions(); err != nil {
		return nil, err
	}

	// Every machine must see the same now(): the time the statement was
	// planned, as DuckDB fixes it for a whole statement.
	if err := pl.checkTimeColumns(stmt); err != nil {
		return nil, err
	}
	if err := pl.fixTime(out); err != nil {
		return nil, err
	}
	for _, s := range pl.stages {
		if err := pl.fixTime(s.fragment); err != nil {
			return nil, err
		}
		if s.SQL, err = pl.p.print(s.fragment); err != nil {
			return nil, unsupported("%v", err)
		}
	}
	if pl.opts.writes {
		pl.markWrites(out)
	}
	pins, err := pl.pinReads(out)
	if err != nil {
		return nil, err
	}
	keys := map[int]string{}
	for x, k := range pl.relKeys {
		keys[x.ID] = k
	}
	return &Plan{Stages: pl.stages, final: out, pins: pins, local: pl.reasons, uncertain: written(pl.uncertain, pl.stages), relKeys: keys,
		versions: pl.versions, buckets: slices.Compact(slices.Clone(pl.bucketNotes))}, nil
}

// pinReads finds the tables the coordinator reads itself, in final, that
// stages read too: Execute points those reads at the snapshot the workers
// read, so the statement sees one snapshot of each table, as on one
// machine. A view there reads as its query (see expandView), whose reads
// are pinned in turn; a view that cannot could read such a table at
// another snapshot, so a statement whose coordinator part reads one is not
// distributed.
func (pl *planner) pinReads(final node) (map[string]string, error) {
	jobTables := map[string]bool{}
	for _, s := range pl.stages {
		for _, in := range s.Inputs {
			if in.Table != nil {
				jobTables[in.Table.key()] = true
			}
		}
	}
	ctes := map[string]bool{}
	walk(final, func(n node) bool {
		for _, name := range cteNames(n) {
			ctes[name] = true
		}
		return true
	})
	pins := map[string]string{}
	var err error
	walk(final, func(n node) bool {
		if err != nil || str(n["type"]) != "BASE_TABLE" || strings.HasPrefix(str(n["table_name"]), "__igloo_partials_") {
			return err == nil
		}
		parts := tableParts(n)
		key := strings.ToLower(strings.Join(parts, "\x00"))
		cte := len(parts) == 1 && ctes[key]
		if cte {
			// A common table expression, unless it is named like a table
			// the jobs read, which the name might mean here instead.
			t, ok, terr := pl.lookupTable(parts)
			switch {
			case terr != nil:
				err = terr
			case ok && t != nil && jobTables[t.key()]:
				err = unsupported("a common table expression is named like %s", t)
			}
			return err == nil
		}
		start := time.Now()
		view, verr := pl.env.View(pl.ctx, parts)
		pl.looked(start)
		if verr != nil {
			err = verr
			return false
		}
		if view {
			// Read as its query, it reads its tables at the workers'
			// snapshots like any other read here.
			sub, ok, xerr := pl.expandView(n, parts)
			switch {
			case xerr != nil && !IsUnsupported(xerr):
				err = xerr
			case !ok:
				err = unsupported("the coordinator's part reads %s, which may be a view", strings.Join(parts, "."))
			default:
				clear(n)
				maps.Copy(n, sub)
				return true
			}
			return false
		}
		t, ok, terr := pl.lookupTable(parts)
		if terr != nil {
			err = terr
			return false
		}
		if !ok || t == nil || !jobTables[t.key()] {
			return true
		}
		if child(n, "at_clause") != nil {
			// Pinned to the snapshot workers read, as Execute checks.
			version, pinned := pinnedVersion(n)
			if !pinned {
				err = unsupported("time travel")
				return false
			}
			if err = pl.pinVersion(t, version); err != nil {
				return false
			}
		}
		pins[key] = t.key()
		return true
	})
	if err != nil {
		return nil, err
	}
	return pins, nil
}

// collation returns the session's default_collation.
func (pl *planner) collation() (string, error) {
	if pl.defaultCollation == nil {
		c, err := pl.env.Setting(pl.ctx, "default_collation")
		if err != nil {
			return "", err
		}
		pl.defaultCollation = &c
	}
	return *pl.defaultCollation, nil
}

// gather plans a stage that sends the coordinator the rows a common table
// expression's exchange holds, and returns the query that reads them.
func (pl *planner) gather(d *cteDef) (node, error) {
	f, err := pl.p.parse("SELECT * FROM " + quoteIdent("__igloo_g"))
	if err != nil {
		return nil, err
	}
	g := pl.stage(&Stage{fragment: f, Inputs: []Input{{View: "__igloo_g", Exchange: d.first, Partitioned: true}}})
	g.placeholder = fmt.Sprintf("__igloo_partials_%d", g.ID)
	q, err := pl.p.template("SELECT * FROM t")
	if err != nil {
		return nil, err
	}
	q["from_table"] = placeholderRef(g.placeholder)
	return q, nil
}

// read reports whether the statement reads a common table expression, as
// decided when its readers were planned.
func (pl *planner) read(d *cteDef) bool { return pl.unread == nil || !pl.unread[d] }

func (pl *planner) setRead(d *cteDef, read bool) {
	if pl.unread == nil {
		pl.unread = map[*cteDef]bool{}
	}
	pl.unread[d] = !read
}

// skip records why a candidate was not distributed.
func (pl *planner) skip(err error) {
	if len(pl.reasons) < 8 {
		pl.reasons = append(pl.reasons, strings.TrimPrefix(err.Error(), "cannot distribute: "))
	}
}

// query looks for jobs in a query node and returns the node with each job
// replaced. ctes are the common table expressions in scope.
func (pl *planner) query(n node, top bool, ctes cteScope) (node, error) {
	switch str(n["type"]) {
	case "SELECT_NODE":
		if work := pl.distinctGroups(n); work != nil {
			replaced, err := pl.job(work, top, ctes)
			if err != nil || replaced != nil {
				return replaced, err
			}
		}
		replaced, err := pl.job(n, top, ctes)
		if err != nil || replaced != nil {
			return replaced, err
		}
		// Not a job itself; look inside the subqueries of its FROM clause
		// and its common table expressions, readers before what they read,
		// so that a common table expression that distributed parts read is
		// computed once, for them.
		inner := ctes.with(n)
		if err := pl.fromSubqueries(child(n, "from_table"), inner); err != nil {
			return nil, err
		}
		if err := pl.withClause(n, ctes, inner); err != nil {
			return nil, err
		}
	case "SET_OPERATION_NODE":
		replaced, err := pl.setOperationJob(n, top, ctes)
		if err != nil || replaced != nil {
			return replaced, err
		}
		// Its branches, which read its common table expressions, then them.
		inner := ctes.with(n)
		for _, side := range []string{"left", "right"} {
			if c := child(n, side); c != nil {
				r, err := pl.query(c, false, inner)
				if err != nil {
					return nil, err
				}
				n[side] = r
			}
		}
		if err := pl.withClause(n, ctes, inner); err != nil {
			return nil, err
		}
	}
	return n, nil
}

// withClause looks for jobs in the common table expressions of query node
// n, whose readers in n were planned already, last first: inner is the
// scope inside n, ctes the one around it.
func (pl *planner) withClause(n node, ctes, inner cteScope) error {
	own := inner[len(ctes):]
	for i := len(own) - 1; i >= 0; i-- {
		d := own[i]
		// Read by the statement, or by a common table expression after it
		// that is.
		body := clone(n).(node)
		body["cte_map"] = node{"map": []any{}}
		read := references(body, d.name) > 0
		for _, later := range own[i+1:] {
			if references(later.entry, d.name) > 0 && pl.read(later) {
				read = true
			}
		}
		pl.setRead(d, read)
		if !read {
			continue
		}
		q := child(d.entry, "query")
		if d.first != nil {
			// Distributed parts write its rows to an exchange: the
			// coordinator reads them from there.
			r, err := pl.gather(d)
			if err != nil {
				return err
			}
			q["node"] = r
			continue
		}
		if sub := child(q, "node"); sub != nil {
			r, err := pl.query(sub, false, d.scope)
			if err != nil {
				return err
			}
			q["node"] = r
		}
	}
	return nil
}

func (pl *planner) fromSubqueries(ref node, ctes cteScope) error {
	switch str(ref["type"]) {
	case "JOIN":
		for _, side := range []string{"left", "right"} {
			if err := pl.fromSubqueries(child(ref, side), ctes); err != nil {
				return err
			}
		}
	case "SUBQUERY":
		q := child(ref, "subquery")
		if sub := child(q, "node"); sub != nil {
			r, err := pl.query(sub, false, ctes)
			if err != nil {
				return err
			}
			q["node"] = r
		}
	}
	return nil
}

func cteNames(n node) []string {
	var names []string
	for _, e := range list(child(n, "cte_map"), "map") {
		names = append(names, strings.ToLower(str(e.(node)["key"])))
	}
	return names
}

// candidate is a SELECT node considered as a job.
type candidate struct {
	sel   node
	scope *scope
	reads
	split *tableUse
	// keyed, if set, are the inputs its stage reads by partition, with no
	// split table, all items of keyScope; whole are relations of one row
	// every task reads.
	keyed    []*keyed
	keyScope *scope
	whole    []*relUse
	// wholeBytes is the estimated size of the relations in whole.
	wholeBytes int64
	// tooDeep is a table that must be partitioned but lies in a subquery,
	// which then becomes a relation.
	tooDeep *tableUse
	// noThrough says no table in a subquery is read by bucket: the
	// candidate's inputs did not all meet by bucket with one that was.
	noThrough bool
	// columns are its result columns.
	columns []Column
	// ctes are the common table expressions its FROM sees, its own among
	// them.
	ctes cteScope
	// relation says the candidate computes a relation for other stages to
	// read, which its consumer decided to distribute; want are the result
	// columns the relation is to be written by.
	relation bool
	want     []string
	// top says the candidate is the statement itself, whose result the
	// coordinator sends the client.
	top bool
	// window, if set, are the columns that the window functions of a
	// candidate that does not aggregate all partition their rows by: its
	// stage reads its inputs by partition, keyed so that each task holds
	// whole partitions of the windows (see windowColumns).
	window []itemColumn
}

// job turns sel into a job if it qualifies and returns the combine step
// that replaces it, or nil if it does not qualify.
func (pl *planner) job(sel node, top bool, ctes cteScope) (node, error) {
	if err := pl.budget(); err != nil {
		return nil, err
	}
	sp := pl.save()
	inlines, grouped := pl.inlines, pl.grouped
	c := &candidate{sel: sel, top: top}
	j, b, err := pl.build(c, top, ctes)
	if pl.ctx.Err() != nil {
		// Out of time: the statement, not this part, runs on one machine.
		pl.restore(sp)
		return nil, pl.budget()
	}
	var combine node
	if err == nil {
		combine, err = pl.finish(j, b)
	}
	if err == nil {
		pl.uncertain = append(pl.uncertain, pl.uncertainOf(c)...)
	}
	if err != nil && IsUnsupported(err) && pl.grouped > grouped && !pl.noGrouped {
		// Its subqueries that group could not be read by bucket: they are
		// relations.
		pl.restore(sp)
		pl.noGrouped = true
		defer func() { pl.noGrouped = false }()
		return pl.job(sel, top, ctes)
	}
	if err != nil && IsUnsupported(err) && pl.inlines > inlines && !pl.noInline {
		// Some parts need a common table expression's rows in an exchange:
		// median() computes over a relation keyed by its groups, not over
		// a divided table. Plan it again reading each as a relation.
		pl.restore(sp)
		pl.noInline = true
		defer func() { pl.noInline = false }()
		return pl.job(sel, top, ctes)
	}
	if err != nil {
		pl.restore(sp)
		if IsUnsupported(err) {
			pl.skip(err)
			return nil, nil
		}
		return nil, err
	}
	return combine, nil
}

func (pl *planner) build(c *candidate, top bool, ctes cteScope) (*job, *jobBuild, error) {
	if err := pl.analyze(c, ctes); err != nil {
		return nil, nil, err
	}
	if err := pl.windowColumns(c); err != nil {
		return nil, nil, err
	}
	if err := pl.chooseInputs(c); err != nil {
		return nil, nil, err
	}
	if err := c.checkWindows(); err != nil {
		return nil, nil, err
	}
	if err := c.checkSamples(); err != nil {
		return nil, nil, err
	}
	var err error
	if c.columns, err = pl.describe(c.sel, ctes); err != nil {
		return nil, nil, err
	}

	// Mark each table reference with the view the fragment will read it
	// as; the fragment is built from copies of the references, which keep
	// the mark.
	defer unmark(c)
	j, err := pl.views(c)
	if err != nil {
		return nil, nil, err
	}
	if err := pl.sides(c); err != nil {
		return nil, nil, err
	}
	var b *jobBuild
	switch {
	case c.aggregating(pl):
		b, err = pl.aggregateJob(c)
	default:
		// The statement, or a subquery or common table expression the
		// coordinator reads: workers filter, project and compute windows,
		// and the coordinator reads the rows they keep, where that cuts
		// them (see rowsJob).
		b, err = pl.rowsJob(c)
		if err != nil && !top {
			err = unsupported("a subquery without aggregation reads %s: %v", c.largest(), strings.TrimPrefix(err.Error(), "cannot distribute: "))
		}
	}
	if err != nil {
		return nil, nil, err
	}
	if err := pl.checkFragment(b.fragment); err != nil {
		return nil, nil, err
	}
	return j, b, nil
}

// checkFragment refuses a fragment that workers could not run as the
// coordinator would, and points its table references at their views.
func (pl *planner) checkFragment(f node) error {
	if hasClass(f, "PARAMETER") {
		return unsupported("a parameter in a distributed part")
	}
	if err := pl.checkFunctions(f); err != nil {
		return err
	}
	pointAtViews(f)
	return nil
}

// unmark removes the view marks of a candidate's references.
func unmark(c *candidate) {
	for _, u := range c.uses {
		delete(u.ref, viewMark)
	}
	for _, r := range c.rels {
		delete(r.ref, viewMark)
		delete(r.ref, viewQuery)
		delete(r.ref, viewColumns)
	}
}

// largest names the largest table or relation a candidate reads, for
// messages.
func (c *candidate) largest() string {
	name, size := "", int64(-1)
	for _, u := range c.uses {
		if u.table.Bytes > size {
			name, size = u.table.String(), u.table.Bytes
		}
	}
	for _, r := range c.rels {
		if r.bytes > size {
			name, size = r.item.name, r.bytes
			if name == "" {
				name = "a subquery"
			}
		}
	}
	return name
}

// --- the FROM clause --------------------------------------------------------

// scope is the FROM clause of a SELECT: what its expressions can name.
type scope struct {
	items []*fromItem
	// conds hold for every row the SELECT's FROM and WHERE produce: the
	// conjuncts of its WHERE clause and of its inner joins' conditions.
	conds []node
	// semiConds are the conjuncts of its semi joins' conditions, which hold
	// for every pair of rows a semi join matches.
	semiConds []node
	// outerJoins are its LEFT, RIGHT and FULL joins of one item to another,
	// whose sides may be read by partition by a key its condition makes
	// equal (see outer.go).
	outerJoins []*outerJoin
	// For the FROM clause of a subquery that passes its rows through: the
	// subquery's reference, the FROM clause it is in, its item there, and
	// whether its rows may be divided among workers.
	ref        node
	outer      *scope
	item       *fromItem
	splittable bool
	// outerSide says the subquery is a side of an outer join that may be
	// read by partition: as a relation, for that (see outer.go).
	outerSide bool
}

// fromItem is a table or subquery in a FROM clause.
type fromItem struct {
	// name is what columns are qualified with: the alias, else the table's
	// name, lower case.
	name string
	// columns are its columns, lower case; nil when not all are known.
	columns []string
}

// tableUse is a catalog table a candidate reads.
type tableUse struct {
	ref   node
	table *Table
	// splittable says the table's rows may be divided among workers: it is
	// reached from the candidate's FROM through joins that keep its rows
	// whole, and not from a subquery expression.
	splittable bool
	// scope and item are the FROM clause the table is an item of and its
	// item there; nil for tables read by subquery expressions.
	scope *scope
	item  *fromItem
	// sub, for a table a subquery expression in a filter reads, is that
	// subquery's number in the candidate's subs, plus one.
	sub int
	// semi says it is on the right side of a semi join that keeps the
	// candidate's rows whole: it may be partitioned by the join's key.
	semi bool
	// outer says it is the side of an outer join that supplies NULLs, or a
	// side of a FULL join: it may be partitioned by the join's key, with
	// the other side partitioned alike (see outer.go). outerSub says it is
	// in a subquery that is such a side, which is then a relation.
	outer, outerSub bool
	// grouped says it is in a subquery that groups by its bucket column:
	// read by bucket beside what the subquery joins, or a relation, but
	// never divided by shares (see groupsByBucket).
	grouped bool
}

// hasColumn reports whether some item of sc has a column called name
// (lower case). certain is false when that cannot be ruled out because an
// item's columns are not all known.
func (sc *scope) hasColumn(name string) (has, certain bool) {
	certain = true
	for _, it := range sc.items {
		if it.columns == nil {
			certain = false
			continue
		}
		if slices.Contains(it.columns, name) {
			return true, true
		}
	}
	return false, certain
}

// owner returns the one item with a column called name (lower case), or
// nil when there is none or more than one.
func (sc *scope) owner(name string) *fromItem {
	var found *fromItem
	for _, it := range sc.items {
		if it.columns == nil {
			return nil
		}
		if slices.Contains(it.columns, name) {
			if found != nil {
				return nil
			}
			found = it
		}
	}
	return found
}

// analyze checks the shape of a candidate SELECT and collects what it
// reads.
func (pl *planner) analyze(c *candidate, ctes cteScope) error {
	sel := c.sel
	// Its own common table expressions are in scope; the fragments read
	// what workers compute of them through exchanges.
	ctes = ctes.with(sel)
	c.ctes = ctes
	if err := pl.semiJoins(sel, ctes); err != nil {
		return err
	}
	if err := pl.scalarJoins(sel, ctes); err != nil {
		return err
	}
	if child(sel, "sample") != nil {
		return unsupported("USING SAMPLE")
	}
	switch h := str(sel["aggregate_handling"]); h {
	case "STANDARD_HANDLING", "FORCE_AGGREGATES":
	default:
		return unsupported("aggregate handling %s", h)
	}
	for _, m := range list(sel, "modifiers") {
		mod := m.(node)
		switch str(mod["type"]) {
		case "ORDER_MODIFIER", "LIMIT_MODIFIER":
		case "DISTINCT_MODIFIER":
			if len(list(mod, "distinct_on_targets")) > 0 {
				return unsupported("DISTINCT ON")
			}
		default:
			return unsupported("%s", strings.ToLower(str(mod["type"])))
		}
	}

	from := child(sel, "from_table")
	if from == nil || str(from["type"]) == "EMPTY" {
		return unsupported("no FROM clause")
	}
	c.scope = &scope{}
	if err := pl.from(from, true, ctes, c.scope, &c.reads); err != nil {
		return err
	}
	if err := pl.filterSubqueries(child(sel, "where_clause"), ctes, &c.reads); err != nil {
		return err
	}
	c.scope.conds = append(c.scope.conds, conjuncts(child(sel, "where_clause"))...)
	// Subqueries in the select list, HAVING, QUALIFY or ORDER BY would run
	// on the coordinator over the combined result, where the columns they
	// may refer to no longer exist. A relation that groups nothing computes
	// its select list in its own stage, where a scalar subquery that stands
	// on its own is a relation of one row.
	parts := []any{sel["having"], sel["qualify"], sel["modifiers"], sel["group_expressions"]}
	if c.relation && len(list(sel, "group_expressions")) == 0 && !pl.hasAggregate(sel["select_list"]) {
		if err := pl.scalarRelations(sel["select_list"], ctes, &c.reads); err != nil {
			return err
		}
	} else {
		parts = append(parts, sel["select_list"])
	}
	if !c.relation && c.aggregating(pl) {
		// An aggregation's combine step runs on the coordinator, which
		// computes there a scalar subquery that stands on its own, over the
		// combined groups (see aggregation.rewrite).
		if err := pl.combinedSubqueries([]any{sel["select_list"], sel["having"], sel["qualify"], sel["modifiers"]}, ctes); err != nil {
			return err
		}
		parts = []any{sel["group_expressions"]}
	}
	for _, part := range parts {
		if hasClass(part, "SUBQUERY") {
			return unsupported("a subquery outside WHERE")
		}
	}
	return checkLambdas(sel)
}

// combinedSubqueries checks that the subqueries in v, parts of an
// aggregation the coordinator computes over the combined groups, are scalar
// subqueries that stand on their own, which bind alone: one that reads the
// aggregation's columns could not be computed over its groups. The
// coordinator computes them from scratch, so they may read no more than it
// reads in the time a statement must take to be worth workers.
func (pl *planner) combinedSubqueries(v any, ctes cteScope) error {
	limit := pl.opts.minBytes
	if limit <= 0 {
		limit = pl.opts.maxBroadcast
	}
	var err error
	walk(v, func(n node) bool {
		if err != nil || str(n["class"]) != "SUBQUERY" {
			return err == nil
		}
		if str(n["subquery_type"]) != "SCALAR" {
			err = unsupported("a subquery outside WHERE")
			return false
		}
		q := subqueryNode(n)
		if cols, cerr := pl.relationColumns(q, ctes, nil); cerr != nil || len(cols) != 1 {
			err = unsupported("a scalar subquery outside WHERE that does not stand on its own")
			return false
		}
		switch b, berr := pl.readBytes(q, ctes, 0); {
		case berr != nil:
			err = berr
		case b > limit:
			err = unsupported("a scalar subquery outside WHERE reads %s, more than the coordinator should read itself", gigabytes(b))
		}
		return false
	})
	return err
}

// scalarRelations makes the scalar subqueries in v, a select list computed
// where its rows are, relations of one row that every task reads, when
// they stand on their own: pgwire writes min(m.day) OVER () in a SELECT
// that groups nothing as (SELECT min(m.day) FROM <its FROM and WHERE>),
// which DuckDB computes once (see pgwire's wholecount.go), and such a
// subquery reads the large tables and common table expressions its SELECT
// does. A correlated one, or another kind, keeps the part off the workers.
func (pl *planner) scalarRelations(v any, ctes cteScope, rd *reads) error {
	var err error
	walk(v, func(n node) bool {
		if err != nil {
			return false
		}
		if str(n["class"]) != "SUBQUERY" {
			return true
		}
		q := subqueryNode(n)
		cols, cerr := pl.relationColumns(q, ctes, nil)
		switch {
		case str(n["subquery_type"]) != "SCALAR":
			err = unsupported("a subquery outside WHERE")
		case cerr != nil || len(cols) != 1:
			err = unsupported("a scalar subquery in the select list that does not stand on its own")
		}
		if err != nil {
			return false
		}
		bytes, eerr := pl.estimate(q, ctes)
		if eerr != nil {
			err = eerr
			return false
		}
		rd.rels = append(rd.rels, &relUse{ref: q, q: q, scope: ctes, columns: cols, bytes: bytes, query: true,
			item: &fromItem{name: "subquery"}})
		return false
	})
	return err
}

// checkLambdas refuses lambda functions whose parameters planning could
// take for something else: a parameter named like an alias of the select
// list, which planning puts in place of bare names in WHERE and GROUP BY,
// as DuckDB resolves them, or like now() and its relatives (see fixTime).
// Otherwise a lambda is an expression like any, which workers compute as
// the coordinator would; translations of Snowflake's array functions and
// FLATTENs of objects call them.
func checkLambdas(sel node) error {
	aliases := map[string]bool{}
	for _, item := range list(sel, "select_list") {
		if a := strings.ToLower(str(item.(node)["alias"])); a != "" {
			aliases[a] = true
		}
	}
	var err error
	walk(sel, func(n node) bool {
		if err != nil || str(n["class"]) != "LAMBDA" {
			return err == nil
		}
		params := 0
		walk(n["lhs"], func(p node) bool {
			if str(p["class"]) != "COLUMN_REF" {
				return true
			}
			for _, name := range columnNames(p) {
				params++
				l := strings.ToLower(name)
				if _, t := timeFunctions[l]; aliases[l] || t {
					err = unsupported("a lambda parameter called %s", name)
				}
			}
			return err == nil
		})
		if err == nil && params == 0 {
			err = unsupported("a lambda function without parameters it names")
		}
		return err == nil
	})
	return err
}

// clauses returns a SELECT's clauses, leaving out its common table
// expressions.
func clauses(sel node) []any {
	return []any{sel["select_list"], sel["from_table"], sel["where_clause"], sel["group_expressions"], sel["having"],
		sel["qualify"], sel["modifiers"]}
}

// from analyzes a table reference in FROM, adding its items to sc and the
// catalog tables it reads to uses. splittable says whether rows of the
// tables reached here may be divided among workers.
func (pl *planner) from(ref node, splittable bool, ctes cteScope, sc *scope, rd *reads) error {
	switch t := str(ref["type"]); t {
	case "BASE_TABLE":
		if err := sampleOK(child(ref, "sample")); err != nil {
			return err
		}
		if child(ref, "sample") != nil && len(tableParts(ref)) == 1 && ctes.find(tableParts(ref)[0]) != nil {
			return unsupported("TABLESAMPLE of a common table expression")
		}
		version, pinned := pinnedVersion(ref)
		if child(ref, "at_clause") != nil && !pinned {
			return unsupported("time travel")
		}
		parts := tableParts(ref)
		if len(parts) == 1 {
			if d := ctes.find(parts[0]); d != nil {
				// One every task computes itself, in place of its name.
				if ok, err := pl.inlineLocal(ref, d); err != nil || ok {
					if err != nil {
						return err
					}
					return pl.from(ref, splittable, ctes, sc, rd)
				}
				ok, err := pl.inline(ref, d, ctes)
				if err != nil {
					return err
				}
				if ok {
					// Its query, in place of its name.
					return pl.from(ref, splittable, ctes, sc, rd)
				}
				// A relation workers compute.
				return pl.relation(ref, d.query(), d.scope, d, splittable, sc, rd)
			}
		}
		tbl, err := pl.table(parts)
		if IsUnsupported(err) && child(ref, "sample") == nil {
			// A view reads as its query, in place of its name.
			sub, ok, verr := pl.expandView(ref, parts)
			if verr != nil {
				return verr
			}
			if ok {
				clear(ref)
				maps.Copy(ref, sub)
				return pl.from(ref, splittable, ctes, sc, rd)
			}
		}
		if err != nil {
			return err
		}
		if pinned {
			if err := pl.pinVersion(tbl, version); err != nil {
				return err
			}
		}
		item := &fromItem{name: strings.ToLower(tbl.Name)}
		if a := str(ref["alias"]); a != "" {
			item.name = strings.ToLower(a)
		}
		for _, col := range tbl.Columns {
			item.columns = append(item.columns, strings.ToLower(col.Name))
		}
		renameColumns(item, list(ref, "column_name_alias"))
		sc.items = append(sc.items, item)
		rd.uses = append(rd.uses, &tableUse{ref: ref, table: tbl, splittable: splittable, scope: sc, item: item})
		return nil

	case "JOIN":
		left, right, err := joinSides(ref)
		if err != nil {
			return err
		}
		uses0, rels0, items0 := len(rd.uses), len(rd.rels), len(sc.items)
		if err := pl.from(child(ref, "left"), splittable && left, ctes, sc, rd); err != nil {
			return err
		}
		uses, rels, items := len(rd.uses), len(rd.rels), len(sc.items)
		if err := pl.from(child(ref, "right"), splittable && right, ctes, sc, rd); err != nil {
			return err
		}
		conds := append(conjuncts(child(ref, "condition")), usingConds(ref, sc.items[items0:items], sc.items[items:])...)
		if splittable {
			sc.addOuterJoin(ref, rd, conds, [3]int{uses0, uses, len(rd.uses)}, [3]int{rels0, rels, len(rd.rels)})
		}
		jt := str(ref["join_type"])
		switch {
		case jt == "INNER" && str(ref["ref_type"]) != "ASOF":
			sc.conds = append(sc.conds, conds...)
		case (jt == "SEMI" || jt == "ANTI") && str(ref["ref_type"]) == "REGULAR" && splittable:
			// Its right side's rows may be partitioned by the join's key: a
			// row on the left meets all of its matches in one partition, and
			// is kept, or not, by them.
			sc.semiConds = append(sc.semiConds, conds...)
			for _, u := range rd.uses[uses:] {
				u.semi = true
			}
			for _, r := range rd.rels[rels:] {
				r.semi = true
			}
		}
		return pl.filterSubqueries(child(ref, "condition"), ctes, rd)

	case "TABLE_FUNCTION":
		// unnest(t.list) and the like: rows of the row's own values.
		if err := rowFunction(ref); err != nil {
			return err
		}
		f := child(ref, "function")
		item := &fromItem{name: strings.ToLower(str(ref["alias"]))}
		if item.name == "" {
			item.name = strings.ToLower(str(f["function_name"]))
		}
		// unnest of one list is one column, named by the alias list, and
		// so is a series, else named after its function.
		aliases := list(ref, "column_name_alias")
		name := strings.ToLower(str(f["function_name"]))
		switch {
		case str(ref["with_ordinality"]) == "WITH_ORDINALITY":
		case name == "unnest" && len(list(f, "children")) == 1 && len(aliases) == 1:
			item.columns = []string{strings.ToLower(str(aliases[0]))}
		case constantSeries[name] && len(aliases) <= 1:
			item.columns = []string{name}
			if len(aliases) == 1 {
				item.columns = []string{strings.ToLower(str(aliases[0]))}
			}
		}
		sc.items = append(sc.items, item)
		return nil

	case "SUBQUERY":
		sub := subqueryNode(ref)
		item := &fromItem{name: strings.ToLower(str(ref["alias"]))}
		if sample := child(ref, "sample"); sample != nil {
			// Sampled where its rows are made, only from a divided table's
			// (see checkSamples).
			if err := sampleOK(sample); err != nil {
				return err
			}
			if rowQuery(sub) || perTaskQuery(sub) || str(sub["type"]) != "SELECT_NODE" || pl.opaque(sub) != "" || pl.forced[identity(ref)] {
				return unsupported("TABLESAMPLE of a subquery workers would not divide")
			}
		}
		if rowQuery(sub) {
			// It reads no table: for each row of the items before it, which a
			// lateral subquery may read, its rows depend on that row alone,
			// as FLATTEN's do. Its columns are known if it names them all.
			if str(sub["type"]) == "SELECT_NODE" && !hasClass(sub["select_list"], "STAR") {
				item.columns = outputColumns(sub, &scope{})
			}
			renameColumns(item, list(ref, "column_name_alias"))
			sc.items = append(sc.items, item)
			return nil
		}
		if perTaskQuery(sub) && !pl.forced[identity(ref)] {
			// It reads tables only in subquery expressions, which every task
			// reads in full: each task computes its rows itself.
			if str(sub["type"]) == "SELECT_NODE" && !hasClass(sub["select_list"], "STAR") {
				item.columns = outputColumns(sub, &scope{})
			}
			renameColumns(item, list(ref, "column_name_alias"))
			sc.items = append(sc.items, item)
			return pl.filterSubqueries(sub, ctes, rd)
		}
		if !pl.forced[identity(ref)] && pl.groupsByBucket(sub, ctes) {
			return pl.groupedFrom(ref, sub, item, splittable, ctes, sc, rd)
		}
		if str(sub["type"]) != "SELECT_NODE" || pl.opaque(sub) != "" || pl.forced[identity(ref)] {
			// A relation workers compute.
			return pl.relation(ref, sub, ctes, nil, splittable, sc, rd)
		}
		inner := &scope{ref: ref, outer: sc, splittable: splittable}
		if err := pl.transparent(sub, splittable, ctes, inner, rd); err != nil {
			return err
		}
		item.columns = outputColumns(sub, inner)
		renameColumns(item, list(ref, "column_name_alias"))
		sc.items = append(sc.items, item)
		inner.item = item
		return nil
	}
	return unsupported("%s in FROM", strings.ToLower(str(ref["type"])))
}

// maxViewDepth bounds views read through other views.
const maxViewDepth = 16

// maxRelationDepth bounds how deeply relations workers compute nest, each
// produced for the one that reads it: a report's common table expressions
// nest a dozen deep at most.
const maxRelationDepth = 64

// errPlanBudget ends a planner's context once auto mode's limits of
// planning a statement passed (see limitPlanning).
var errPlanBudget = errors.New("planning ran past its limits")

// budget fails once the planner's context ended, which auto mode's limits
// end (see limitPlanning), as do the statement's timeout and a cancel
// request: the planner checks it as it recurses, besides the calls into
// the session that read the context.
func (pl *planner) budget() error {
	if err := pl.ctx.Err(); err != nil {
		if errors.Is(context.Cause(pl.ctx), errPlanBudget) {
			why, _ := pl.stalled.Load().(string)
			return unsupported("%s", why)
		}
		return err
	}
	return nil
}

// expandView returns the subquery a reference to a view reads as: the
// view's query, every table name in it qualified as DuckDB resolves it in
// the view's body, aliased as the reference was (or by the view's name),
// with the view's column names. It is used only when it describes as the
// view itself does in the session, column for column. Masking and row
// access policies, which views may hide, kept the statement from the
// planner if they concern anything it reads (see pgwire's policyReason),
// and the tables of the query are checked as any are.
func (pl *planner) expandView(ref node, parts []string) (node, bool, error) {
	if child(ref, "sample") != nil || child(ref, "at_clause") != nil {
		return nil, false, nil
	}
	start := time.Now()
	def, ok, err := pl.env.ViewDefinition(pl.ctx, parts)
	pl.looked(start)
	if err != nil || !ok {
		return nil, false, err
	}
	name := strings.Join(parts, ".")
	if pl.expanded++; pl.expanded > maxViewDepth*4 {
		return nil, false, unsupported("views nest too deep")
	}
	body, err := pl.p.parse(def.Query)
	if err != nil {
		return nil, false, unsupported("view %s: %v", name, err)
	}
	if len(pl.opts.values) > 0 && callsFunction(body, "getvariable") {
		// It could read a variable of the values computed for the stages,
		// which workers would set and the coordinator not.
		return nil, false, unsupported("view %s calls getvariable()", name)
	}
	var qerr error
	walk(body, func(n node) bool {
		if qerr != nil {
			return false
		}
		if len(cteNames(n)) > 0 {
			// A name could mean a common table expression or a table.
			qerr = unsupported("view %s has common table expressions", name)
			return false
		}
		if str(n["type"]) != "BASE_TABLE" {
			return true
		}
		q, ok, err := def.Resolve(pl.ctx, tableParts(n))
		switch {
		case err != nil:
			qerr = err
		case !ok || len(q) != 3:
			qerr = unsupported("view %s reads %s, which may mean more than one relation", name, strings.Join(tableParts(n), "."))
		default:
			if str(n["alias"]) == "" {
				n["alias"] = str(n["table_name"])
			}
			n["catalog_name"], n["schema_name"], n["table_name"] = q[0], q[1], q[2]
		}
		return qerr == nil
	})
	if qerr != nil {
		return nil, false, qerr
	}

	// The view's columns, as the statement reads them, name the subquery's.
	probe, err := pl.p.fromRef("SELECT * FROM t")
	if err != nil {
		return nil, false, err
	}
	for _, k := range []string{"catalog_name", "schema_name", "table_name", "alias", "column_name_alias"} {
		probe[k] = clone(ref[k])
	}
	asView, err := pl.describeFrom(probe)
	if err != nil {
		return nil, false, err
	}
	sub, err := pl.p.fromRef("SELECT * FROM (SELECT 1) AS v")
	if err != nil {
		return nil, false, err
	}
	child(sub, "subquery")["node"] = body
	sub["alias"] = str(ref["alias"])
	if sub["alias"] == "" {
		sub["alias"] = parts[len(parts)-1]
	}
	var aliases []any
	for _, c := range asView {
		aliases = append(aliases, c.Name)
	}
	sub["column_name_alias"] = aliases
	asQuery, err := pl.describeFrom(sub)
	if err != nil {
		return nil, false, err
	}
	if !slices.Equal(asView, asQuery) {
		return nil, false, unsupported("view %s does not read as its query", name)
	}
	original := clone(ref).(node)
	pl.undo = append(pl.undo, func() {
		clear(ref)
		maps.Copy(ref, original)
	})
	return sub, true, nil
}

// describeFrom returns the columns of SELECT * FROM ref in the session.
func (pl *planner) describeFrom(ref node) ([]Column, error) {
	sel, err := pl.p.template("SELECT * FROM t")
	if err != nil {
		return nil, err
	}
	sel["from_table"] = clone(ref)
	text, err := pl.p.print(sel)
	if err != nil {
		return nil, unsupported("%v", err)
	}
	return pl.describeText(text)
}

// rowFunctions are the table functions whose rows depend on their
// arguments alone: unnest of a list, json_each and json_tree of a JSON
// value. Snowflake's FLATTEN becomes one of them, in a lateral subquery
// (see internal/snowflake). Over the columns of a row, they are computed
// for each row, as a filter is, so they may run on workers.
var rowFunctions = map[string]bool{"unnest": true, "json_each": true, "json_tree": true}

// constantSeries are the table functions whose rows, of integer literals,
// are the same wherever they are computed: range and generate_series.
// Joined to a split table, each worker joins its share of the table to all
// of them, as one machine joins the whole table: Snowflake's FLATTEN of an
// object of a few keys becomes CROSS JOIN range(n) with a CASE per key,
// which DuckDB estimates at n times the table's rows, where it estimates
// an unnest at the table's rows alone.
var constantSeries = map[string]bool{"range": true, "generate_series": true}

// maxSeriesRows bounds the rows of such a series: a large one multiplies
// every worker's rows, and is no FLATTEN.
const maxSeriesRows = 1000

// rowFunction checks that a table function in FROM is one of rowFunctions,
// over expressions that read no table, or a series of integer literals
// (constantSeries).
func rowFunction(ref node) error {
	f := child(ref, "function")
	name := strings.ToLower(str(f["function_name"]))
	switch {
	case str(f["schema"]) != "" || str(f["catalog"]) != "":
		return unsupported("table function %s in FROM", name)
	case child(ref, "sample") != nil:
		return unsupported("TABLESAMPLE")
	case constantSeries[name]:
		return seriesRows(name, list(f, "children"))
	case !rowFunctions[name]:
		return unsupported("table function %s in FROM", name)
	case hasClass(f["children"], "SUBQUERY"):
		return unsupported("a subquery in %s()", name)
	}
	return nil
}

// seriesRows checks that range or generate_series of args yields at most
// maxSeriesRows rows, from integer literals: range(stop), range(start,
// stop) and range(start, stop, step), and generate_series likewise but
// including stop.
func seriesRows(name string, args []any) error {
	vals := make([]int64, len(args))
	for i, a := range args {
		n, ok := a.(node)
		v, isInt := intConstant(n)
		if !ok || !isInt || v < -1<<40 || v > 1<<40 {
			return unsupported("%s() of something other than small integer literals", name)
		}
		vals[i] = v
	}
	start, stop, step := int64(0), int64(0), int64(1)
	switch len(vals) {
	case 1:
		stop = vals[0]
	case 2:
		start, stop = vals[0], vals[1]
	case 3:
		start, stop, step = vals[0], vals[1], vals[2]
	default:
		return unsupported("%s() of %d arguments", name, len(vals))
	}
	if step <= 0 {
		return unsupported("%s() with a step of %d", name, step)
	}
	if name == "generate_series" {
		stop++
	}
	if rows := (stop - start + step - 1) / step; stop > start && rows > maxSeriesRows {
		return unsupported("%s() of %d rows, more than %d", name, rows, maxSeriesRows)
	}
	return nil
}

// volatileFunctions return something else each time they are called, so
// a query that calls them computes something else on each worker. The AI
// functions are among them (see coordinatorFunctions).
var volatileFunctions = func() map[string]bool {
	m := map[string]bool{"random": true, "uuid": true, "gen_random_uuid": true, "uuidv4": true, "uuidv7": true}
	for k := range coordinatorFunctions {
		m[k] = true
	}
	return m
}()

// coordinatorFunctions are the AI functions (package ai), which call model
// providers with keys only the coordinator holds: workers scan, filter and
// aggregate, and the coordinator applies them to what workers return.
var coordinatorFunctions = func() map[string]bool {
	m := map[string]bool{}
	for _, name := range ai.Names() {
		m[name] = true
	}
	return m
}()

// rowQuery reports whether query node n reads no table, and so computes,
// in FROM, rows that depend on the row it may be lateral to alone: its
// FROM clauses hold only rowFunctions, other such queries, VALUES and
// nothing, and it calls no volatile function, which one machine might
// call once for every row.
func rowQuery(n node) bool {
	ok := true
	walk(n, func(m node) bool {
		switch t := str(m["type"]); {
		case t == "BASE_TABLE", t == "PIVOT", t == "SHOW_REF", t == "COLUMN_DATA":
			ok = false
		case t == "TABLE_FUNCTION" && rowFunction(m) != nil:
			ok = false
		case str(m["class"]) == "FUNCTION" && volatileFunctions[strings.ToLower(str(m["function_name"]))]:
			ok = false
		}
		// Once false, it stays so: the walk goes on to the siblings of the
		// node that made it false, in no set order. A row function there
		// set it back to true, so a subquery joining a table to unnest()
		// sometimes read as reading no table, and its table went to the
		// workers unplanned.
		return ok
	})
	return ok
}

// perTaskQuery reports whether query node n computes its rows from nothing
// a task divides: its FROM clauses hold what a rowQuery's may (row
// functions, VALUES, nothing, other such queries and set operations of
// them), with tables only in subquery expressions, which every task reads
// in full; it calls no volatile function and defines no common table
// expression of its own. Every task can compute its rows itself, as each
// computes a rowQuery's, and reads them beside its share of a divided
// table, as it would a small table read in full: a report's cohort, its
// members listed from a bitmap the coordinator computed first (see
// computedValues), under a check that reads a small table.
func perTaskQuery(n node) bool {
	ok := true
	var visit func(v any, inExpr bool)
	visit = func(v any, inExpr bool) {
		switch m := v.(type) {
		case node:
			t := str(m["type"])
			switch {
			case !ok:
				return
			case !inExpr && (t == "BASE_TABLE" || t == "PIVOT" || t == "SHOW_REF" || t == "COLUMN_DATA"):
				ok = false
			case !inExpr && t == "TABLE_FUNCTION" && rowFunction(m) != nil:
				ok = false
			case str(m["class"]) == "FUNCTION" && volatileFunctions[strings.ToLower(str(m["function_name"]))]:
				ok = false
			case len(list(child(m, "cte_map"), "map")) > 0:
				ok = false
			}
			in := inExpr || str(m["class"]) == "SUBQUERY"
			for _, c := range m {
				visit(c, in)
			}
		case []any:
			for _, c := range m {
				visit(c, inExpr)
			}
		}
	}
	visit(n, false)
	return ok
}

// joinSides says which sides of a join keep their rows whole: the sides
// each of whose rows produces its share of the join's result regardless of
// the other rows on its side. Only those may be divided among workers.
func joinSides(ref node) (left, right bool, err error) {
	switch rt := str(ref["ref_type"]); rt {
	case "REGULAR", "CROSS", "NATURAL":
	case "ASOF":
		// Each row on the left finds its match among all rows on the right.
		switch str(ref["join_type"]) {
		case "INNER", "LEFT":
			return true, false, nil
		}
		return false, false, nil
	default:
		return false, false, unsupported("%s join", strings.ToLower(rt))
	}
	switch jt := str(ref["join_type"]); jt {
	case "INNER":
		return true, true, nil
	case "LEFT", "SEMI", "ANTI":
		return true, false, nil
	case "RIGHT":
		return false, true, nil
	case "OUTER", "FULL":
		// Neither side's rows may be divided by shares; both may be read
		// by partition, by a key the condition makes equal (see outer.go).
		return false, false, nil
	default:
		return false, false, unsupported("%s join", strings.ToLower(jt))
	}
}

// transparent analyzes a subquery in FROM that passes rows through: no
// aggregation, DISTINCT, LIMIT, window functions or ordering, so each of
// its rows comes from one row of its own FROM clause.
func (pl *planner) transparent(sel node, splittable bool, ctes cteScope, sc *scope, rd *reads) error {
	if reason := pl.opaque(sel); reason != "" {
		return unsupported("%s", reason)
	}
	if err := pl.from(child(sel, "from_table"), splittable, ctes, sc, rd); err != nil {
		return err
	}
	if err := pl.filterSubqueries(sel["select_list"], ctes, rd); err != nil {
		return err
	}
	sc.conds = append(sc.conds, conjuncts(child(sel, "where_clause"))...)
	return pl.filterSubqueries(child(sel, "where_clause"), ctes, rd)
}

// conjuncts splits a condition at its top-level ANDs.
func conjuncts(e node) []node {
	if e == nil {
		return nil
	}
	if str(e["class"]) == "CONJUNCTION" && str(e["type"]) == "CONJUNCTION_AND" {
		var out []node
		for _, c := range list(e, "children") {
			out = append(out, conjuncts(c.(node))...)
		}
		return out
	}
	return []node{e}
}

// filterSubqueries adds the tables that subquery expressions in v read to
// uses, as tables every worker reads in full.
func (pl *planner) filterSubqueries(v any, ctes cteScope, rd *reads) error {
	var err error
	walk(v, func(n node) bool {
		if err != nil {
			return false
		}
		if str(n["class"]) == "SUBQUERY" {
			before := len(rd.uses)
			err = pl.fullRead(subqueryNode(n), ctes.names(), nil, rd)
			rd.subs = append(rd.subs, n)
			for _, u := range rd.uses[before:] {
				u.sub = len(rd.subs)
			}
			return false
		}
		return true
	})
	return err
}

// fullRead adds every catalog table that query node n reads to uses, as
// tables every worker reads in full. Everything n reads must be a catalog
// table or a common table expression of its own (local); the common table
// expressions of the statement around the job (outer) are not available
// to workers.
func (pl *planner) fullRead(n node, outer, local []string, rd *reads) error {
	if n == nil {
		return nil
	}
	local = append(slices.Clone(local), cteNames(n)...)
	var err error
	visit := func(m node) bool {
		if err != nil {
			return false
		}
		if strings.HasSuffix(str(m["type"]), "_NODE") {
			err = pl.fullRead(m, outer, local, rd)
			return false
		}
		switch str(m["type"]) {
		case "BASE_TABLE":
			version, pinned := pinnedVersion(m)
			if child(m, "at_clause") != nil && !pinned || child(m, "sample") != nil {
				err = unsupported("time travel or sampling")
				return false
			}
			parts := tableParts(m)
			if len(parts) == 1 {
				name := strings.ToLower(parts[0])
				if slices.Contains(local, name) {
					if pinned {
						err = unsupported("time travel")
					}
					return false
				}
				if slices.Contains(outer, name) {
					err = unsupported("a common table expression in a distributed part")
					return false
				}
			}
			var t *Table
			if t, err = pl.table(parts); err == nil {
				if pinned {
					err = pl.pinVersion(t, version)
				}
				rd.uses = append(rd.uses, &tableUse{ref: m, table: t})
			} else if IsUnsupported(err) {
				// A view reads as its query here too.
				sub, ok, verr := pl.expandView(m, parts)
				switch {
				case verr != nil:
					err = verr
				case ok:
					clear(m)
					maps.Copy(m, sub)
					err = pl.fullRead(subqueryNode(m), outer, local, rd)
				}
			}
			return false
		case "TABLE_FUNCTION":
			// unnest of a list of the row, as in EXISTS (SELECT 1 FROM
			// unnest(t.tags) ...), reads no table.
			if rowFunction(m) == nil {
				return true
			}
			err = unsupported("table_function in a subquery")
			return false
		case "PIVOT", "SHOW_REF", "EXPRESSION_LIST", "COLUMN_DATA":
			err = unsupported("%s in a subquery", strings.ToLower(str(m["type"])))
			return false
		}
		return true
	}
	for _, c := range n {
		walk(c, visit)
	}
	return err
}

// pinnedVersion returns the snapshot a table reference reads, AT (VERSION
// => n) with n an integer: the only time travel the planner takes. Igloo
// pins a statement it answers from bitmap indexes to the snapshots the
// bitmaps are of, and before each run prepares it again if a table has
// moved on (see pgwire's refreshIndexes), so n is the table's current
// snapshot nearly always.
func pinnedVersion(ref node) (int64, bool) {
	at := child(ref, "at_clause")
	if at == nil || !strings.EqualFold(str(at["unit"]), "VERSION") {
		return 0, false
	}
	return intConstant(child(at, "expr"))
}

// pinVersion records that the statement reads table t at snapshot n.
// Workers read the snapshot Execute loads, and the coordinator's own reads
// of a table workers read are pinned to it: Execute runs the statement on
// workers only if that snapshot is n. A statement that reads two
// snapshots of a table stays on one machine.
func (pl *planner) pinVersion(t *Table, n int64) error {
	if v, ok := pl.versions[t.key()]; ok && v != n {
		return unsupported("time travel to two snapshots of %s", t)
	}
	if pl.versions == nil {
		pl.versions = map[string]int64{}
	}
	pl.versions[t.key()] = n
	return nil
}

func tableParts(ref node) []string {
	var parts []string
	for _, k := range []string{"catalog_name", "schema_name", "table_name"} {
		if p := str(ref[k]); p != "" {
			parts = append(parts, p)
		}
	}
	return parts
}

// renameColumns applies a column alias list, as in FROM t AS x(a, b),
// which renames the first columns.
func renameColumns(item *fromItem, aliases []any) {
	if item.columns == nil {
		return
	}
	for i, a := range aliases {
		if i < len(item.columns) {
			item.columns[i] = strings.ToLower(str(a))
		}
	}
}

// outputColumns returns the names of a SELECT's result columns, lower
// case, or nil when some are not known: an expression without an alias,
// whose name DuckDB makes up.
func outputColumns(sel node, sc *scope) []string {
	var cols []string
	for _, item := range list(sel, "select_list") {
		e := item.(node)
		if a := str(e["alias"]); a != "" {
			cols = append(cols, strings.ToLower(a))
			continue
		}
		switch str(e["class"]) {
		case "COLUMN_REF":
			parts := columnNames(e)
			cols = append(cols, strings.ToLower(parts[len(parts)-1]))
		case "STAR":
			if e["columns"] == true || len(list(e, "exclude_list")) > 0 || len(list(e, "replace_list")) > 0 ||
				len(list(e, "rename_list")) > 0 || child(e, "expr") != nil {
				return nil
			}
			rel := strings.ToLower(str(e["relation_name"]))
			for _, it := range sc.items {
				if rel != "" && it.name != rel {
					continue
				}
				if it.columns == nil {
					return nil
				}
				cols = append(cols, it.columns...)
			}
		default:
			return nil
		}
	}
	return cols
}

// lookupTable resolves a table name as the session does (see Env.Table),
// once per planner.
func (pl *planner) lookupTable(parts []string) (*Table, bool, error) {
	key := strings.ToLower(strings.Join(parts, "\x00"))
	if r, ok := pl.tables[key]; ok {
		return r.t, r.ok, nil
	}
	start := time.Now()
	t, found, err := pl.env.Table(pl.ctx, parts)
	pl.looked(start)
	if err != nil {
		return nil, false, err
	}
	pl.tables[key] = tableResult{t: t, ok: found}
	return t, found, nil
}

// table resolves a table name to a catalog table that workers can read.
func (pl *planner) table(parts []string) (*Table, error) {
	t, found, err := pl.lookupTable(parts)
	if err != nil {
		return nil, err
	}
	r := tableResult{t: t, ok: found}
	if !r.ok {
		return nil, unsupported("%s is not a catalog table", strings.Join(parts, "."))
	}
	if r.t.Unsupported != "" {
		return nil, unsupported("table %s: %s", r.t, r.t.Unsupported)
	}
	for _, c := range r.t.Columns {
		if c.Nested == nil && nestedType(c.Type) {
			// A task's view of the table has every column, and a worker
			// refuses one whose nesting it is not told (see checkNested): the
			// statement would run here only once it ran, and EXPLAIN
			// DISTRIBUTED would say it runs on workers.
			return nil, unsupported("column %s of %s is nested, and its nesting is not known", c.Name, r.t)
		}
	}
	return r.t, nil
}

// chooseSplit picks the largest table the candidate may divide among
// workers, and checks that the rest are small enough for every worker to
// read in full.
// chooseInputs decides how the candidate's stage reads its inputs: one
// table divided among the workers by file and the others in full, or its
// large inputs by partition.
func (pl *planner) chooseInputs(c *candidate) error {
	pl.subqueryRelations(c)
	for _, r := range c.rels {
		if r.known && r.bytes <= pl.opts.maxBroadcast && !pl.wholeCheaper(r, c) {
			r.bytes = pl.opts.maxBroadcast + 1
		}
	}
	// Relations small enough for every worker to read, beside a larger
	// table the workers divide, every task reads in full, as tables: their
	// rows cross once per task, where partitioning would send the table's
	// through object storage. Relations of one row are always read so.
	c.whole, c.wholeBytes = c.rels, 0
	for _, r := range c.rels {
		c.wholeBytes += r.bytes
	}
	if c.window != nil {
		// Each task holds whole partitions of the windows: its inputs are
		// read by partition, keyed by the windows' columns.
		return pl.chooseKeyed(c)
	}
	if c.wholeBytes == 0 || c.wholeBytes <= pl.opts.maxBroadcast {
		err := pl.chooseSplit(c)
		if err == nil && c.split != nil && len(c.keyed) == 0 {
			pl.bucketSplit(c)
		}
		// A side of an outer join too large to read whole, which shares
		// cannot divide, may be read by partition (see outer.go).
		if err == nil || c.wholeBytes == 0 && !c.hasOuter() || len(c.keyed) > 0 {
			return err
		}
	}
	return pl.chooseKeyed(c)
}

func (pl *planner) chooseSplit(c *candidate) error {
	var largest *tableUse
	for _, u := range c.uses {
		if u.splittable && (c.split == nil || u.table.Bytes > c.split.table.Bytes) {
			c.split = u
		}
		if largest == nil || u.table.Bytes > largest.table.Bytes {
			largest = u
		}
	}
	if (c.split == nil || largest.table.Bytes > c.split.table.Bytes) && largest != nil && largest.grouped {
		// Read by bucket, or the subquery around it is a relation.
		c.split = nil
		return pl.chooseKeyed(c)
	}
	if c.split == nil {
		return unsupported("no table whose rows can be divided among workers")
	}
	if largest.table.Bytes > c.split.table.Bytes {
		return unsupported("the largest table, %s, is read where its rows cannot be divided", largest.table)
	}
	var broadcast int64
	var second *tableUse
	for _, u := range c.uses {
		if u == c.split {
			continue
		}
		if u.table.key() == c.split.table.key() {
			if u.splittable && u.scope == c.scope {
				// Joined to itself: both sides by partition.
				return pl.chooseKeyed(c)
			}
			return unsupported("%s is read more than once", c.split.table)
		}
		broadcast += u.table.Bytes
		if second == nil || u.table.Bytes > second.table.Bytes {
			second = u
		}
	}
	if !pl.opts.force && !c.relation && c.split.table.Bytes < pl.opts.minBytes {
		return unsupported("%s is small (%d bytes)", c.split.table, c.split.table.Bytes)
	}
	if broadcast+c.wholeBytes <= pl.opts.maxBroadcast {
		for _, r := range c.whole {
			if r.bytes > c.split.table.Bytes {
				// The relation is the larger: partition instead.
				return unsupported("relation %s is larger than %s", r.item.name, c.split.table)
			}
		}
		return nil
	}
	// Too much for every worker to read: partition the largest tables by
	// their join key, if the rest fits.
	return pl.chooseKeyed(c)
}

// --- finishing a job --------------------------------------------------------

// jobBuild is a job's fragment and combine step before they are tied
// together.
type jobBuild struct {
	// fragment is the SELECT workers run. Its table references are still
	// the candidate's.
	fragment node
	// combine is the SELECT that replaces the candidate. Its FROM is filled
	// in by finish.
	combine node
	// columns rename the fragment's result columns positionally.
	columns []string
	// second, if set, completes an aggregation's groups partition by
	// partition, before the combine step; third, if set, completes them
	// from second's rows, partitioned by group (see aggregation.regroup).
	second, third *secondStage
	// merge are the columns the last stage's tasks sort their rows by, for
	// a statement's result ordered by ORDER BY without LIMIT (see merge.go).
	merge []mergeKey
	// enough is the rows of any that its last stage needs (see
	// Stage.enough), or 0.
	enough int64
}

// viewMark is the key that marks a table reference with the view a
// fragment reads it as, while the fragment is built; viewQuery marks a
// subquery expression's query, which then reads its view.
const (
	viewMark  = "__igloo_view"
	viewQuery = "__igloo_view_query"
	// viewColumns, on a subquery read as a view, replaces its column
	// aliases: its relation's stages wrote only some of its columns, named
	// as the candidate reads them (see asRelation).
	viewColumns = "__igloo_view_columns"
)

// views names the views a job's fragment reads its inputs as, and marks
// the candidate's references with them. Every use of a table read in full
// reads the same view; the split table's view each worker defines over its
// own share of the files, and each input read by partition has one of its
// own.
func (pl *planner) views(c *candidate) (*job, error) {
	j := &job{splitView: "__igloo_split", keyed: c.keyed}
	if c.split != nil {
		j.split = c.split.table
	}
	for i, k := range c.keyed {
		k.view = fmt.Sprintf("__igloo_k%d", i)
		k.ref()[viewMark] = k.view
	}
	for i, r := range c.whole {
		v := fmt.Sprintf("__igloo_w%d", i)
		j.whole = append(j.whole, wholeRel{view: v, rel: r})
		if r.query {
			// A subquery expression's query: it reads the view instead.
			q, err := pl.p.parse("SELECT * FROM " + quoteIdent(v))
			if err != nil {
				return nil, err
			}
			r.ref[viewQuery] = q
		} else {
			r.ref[viewMark] = v
		}
	}
	names := map[string]string{}
	for _, u := range c.uses {
		if slices.ContainsFunc(c.keyed, func(k *keyed) bool { return k.use == u }) {
			continue
		}
		name := j.splitView
		if u != c.split {
			if name = names[u.table.key()]; name == "" {
				name = fmt.Sprintf("__igloo_t%d", len(names))
				names[u.table.key()] = name
			}
		}
		if !slices.ContainsFunc(j.views, func(v view) bool { return v.name == name }) {
			j.views = append(j.views, view{name: name, table: u.table})
		}
		u.ref[viewMark] = name
	}
	return j, nil
}

// pointAtViews makes the marked references of a fragment read their
// views. A reference keeps the name its columns are qualified with: its
// alias, else the table's name; a subquery becomes a reference to its
// view.
func pointAtViews(fragment node) {
	rewrite(fragment, func(n node) (node, bool) {
		if q, ok := n[viewQuery].(node); ok {
			return clone(q).(node), true
		}
		v := str(n[viewMark])
		if v == "" {
			return nil, false
		}
		if str(n["type"]) == "SUBQUERY" {
			alias := str(n["alias"])
			if alias == "" {
				alias = v
			}
			aliases := n["column_name_alias"]
			if cols, ok := n[viewColumns]; ok {
				aliases = cols
			}
			return node{
				"type": "BASE_TABLE", "alias": alias, "sample": nil, "query_location": json.Number(queryLocationNone), "schema_name": "",
				"table_name": v, "column_name_alias": aliases, "catalog_name": "", "at_clause": nil,
			}, true
		}
		if str(n["alias"]) == "" {
			n["alias"] = str(n["table_name"])
		}
		// A view of the snapshot a pin names (see pinVersion).
		n["table_name"], n["schema_name"], n["catalog_name"], n["at_clause"] = v, "", "", nil
		delete(n, viewMark)
		return nil, false
	})
}

// unmarked returns a copy of v without view marks.
func unmarked(v any) any {
	c := clone(v)
	walk(c, func(n node) bool {
		delete(n, viewMark)
		delete(n, viewQuery)
		delete(n, viewColumns)
		return true
	})
	return c
}

// finish turns a job into stages: the combine step reads the partial
// results of the last.
func (pl *planner) finish(j *job, b *jobBuild) (node, error) {
	last, err := pl.emit(j, b, nil)
	if err != nil {
		return nil, err
	}
	last.placeholder = fmt.Sprintf("__igloo_partials_%d", last.ID)
	last.merge, last.enough = b.merge, b.enough
	b.combine["from_table"] = placeholderRef(last.placeholder)
	return b.combine, nil
}

// placeholderRef is a reference to a coordinator-bound stage's partial
// results, which Execute replaces with a read of them.
func placeholderRef(name string) node {
	return node{
		"type": "BASE_TABLE", "alias": "__igloo_p", "sample": nil, "query_location": json.Number(queryLocationNone),
		"schema_name": "", "table_name": name, "column_name_alias": []any{}, "catalog_name": "", "at_clause": nil,
	}
}

// emit adds a job's stages to the plan: a stage per partitioned table, the
// stages of the relations it reads, its own stage and, for an aggregation
// completed on workers, a second; the last writes to out, or to the
// coordinator without one.
func (pl *planner) emit(j *job, b *jobBuild, out *Exchange) (*Stage, error) {
	last := &Stage{fragment: b.fragment, Columns: b.columns}
	for _, k := range j.keyed {
		var x *Exchange
		if k.use != nil && k.bucketed {
			// Read by bucket, as the table's files keep its rows: no stage
			// and no exchange of its own (see bucket.go).
			last.Inputs = append(last.Inputs, Input{View: k.view, Table: k.use.table, Bucketed: true, Bucket: k.bucket})
			continue
		}
		if k.use != nil {
			// A stage per partitioned table, writing its rows to an exchange
			// keyed by its side of the join key.
			x = pl.exchange("join", k.keys, k.casts)
			x.Buckets, x.UUIDHash = k.into, k.intoUUID
			pl.stage(&Stage{fragment: k.stage, Inputs: []Input{{View: k.view, Table: k.use.table, Split: true}}, Out: x})
		} else {
			var err error
			if x, err = pl.relationExchange(k.rel, k.keys, k.casts, k.into, k.intoUUID); err != nil {
				return nil, err
			}
		}
		last.Inputs = append(last.Inputs, Input{View: k.view, Exchange: x, Partitioned: true})
	}
	for _, w := range j.whole {
		x, err := pl.relationExchange(w.rel, nil, nil, 0, "")
		if err != nil {
			return nil, err
		}
		last.Inputs = append(last.Inputs, Input{View: w.view, Exchange: x})
	}
	for _, v := range j.views {
		last.Inputs = append(last.Inputs, Input{View: v.name, Table: v.table, Split: v.name == j.splitView})
	}
	pl.stage(last)
	for _, next := range []*secondStage{b.second, b.third} {
		if next == nil {
			continue
		}
		// The partial results go to an exchange, which a second stage reads
		// partition by partition (or, with no groups, whole).
		last.Out = pl.exchange("aggregate", next.keys, make([]string, len(next.keys)))
		last = pl.stage(&Stage{fragment: next.fragment, Columns: next.columns, completes: true,
			Inputs: []Input{{View: partialsView, Exchange: last.Out, Partitioned: !next.whole}}})
	}
	last.Out = out
	return last, nil
}

// fuseRows takes out the stages that only combine the partial results of
// an aggregation without GROUP BY into its one row, which later stages
// read in full, as a filter's subquery's (ver = (SELECT max(ver) ...)) is:
// each task of a stage that reads the row combines the partial results,
// a row per task that wrote them, itself, rather than the plan waiting for
// a round of tasks that writes the row to an exchange first.
func (pl *planner) fuseRows() error {
	writers, readers := map[*Exchange]int{}, map[*Exchange]int{}
	partitioned := map[*Exchange]bool{}
	for _, s := range pl.stages {
		if s.Out != nil {
			writers[s.Out]++
		}
		for _, in := range s.Inputs {
			if in.Exchange != nil {
				readers[in.Exchange]++
				partitioned[in.Exchange] = partitioned[in.Exchange] || in.Partitioned
			}
		}
	}
	// Every reader must read the row by its view's name.
	named := func(x *Exchange) bool {
		for _, c := range pl.stages {
			for _, in := range c.Inputs {
				if in.Exchange != x {
					continue
				}
				found := false
				walk(c.fragment, func(n node) bool {
					found = found || isView(n, in.View)
					return !found
				})
				if !found {
					return false
				}
			}
		}
		return true
	}
	var rows []*Stage
	for _, s := range pl.stages {
		if x := s.Out; x != nil && writers[x] == 1 && readers[x] > 0 && !partitioned[x] && len(s.Inputs) == 1 && named(x) {
			in := s.Inputs[0]
			if in.Exchange != nil && !in.Partitioned && in.Exchange.purpose == "aggregate" && len(in.Exchange.Keys) == 0 {
				rows = append(rows, s)
			}
		}
	}
	if len(rows) == 0 {
		return nil
	}
	ref, err := pl.p.fromRef("SELECT * FROM (SELECT 1) AS v")
	if err != nil {
		return err
	}
	wrap, err := pl.p.template("SELECT * FROM (SELECT 1) AS __igloo_f")
	if err != nil {
		return err
	}
	gone := map[*Stage]bool{}
	for _, p := range rows {
		row, partials := p.fragment, p.Inputs[0]
		if len(p.Columns) > 0 {
			// The names the exchange's rows have.
			w := clone(wrap).(node)
			from := child(w, "from_table")
			child(from, "subquery")["node"] = row
			names := make([]any, len(p.Columns))
			for i, c := range p.Columns {
				names[i] = c
			}
			from["column_name_alias"] = names
			row = w
		}
		for _, c := range pl.stages {
			for i := range c.Inputs {
				in := &c.Inputs[i]
				if in.Exchange != p.Out {
					continue
				}
				// The row's query over the partial results, which the reader
				// reads under the row's view name, in place of the row.
				q := clone(row).(node)
				renameViews(q, map[string]string{partials.View: in.View})
				c.fragment = rewrite(c.fragment, func(n node) (node, bool) {
					if !isView(n, in.View) {
						return nil, false
					}
					sub := clone(ref).(node)
					child(sub, "subquery")["node"] = clone(q)
					sub["alias"] = str(n["alias"])
					if sub["alias"] == "" {
						sub["alias"] = in.View
					}
					if aliases := list(n, "column_name_alias"); aliases != nil {
						sub["column_name_alias"] = aliases
					}
					return sub, true
				}).(node)
				in.Exchange = partials.Exchange
			}
		}
		gone[p] = true
	}
	var stages []*Stage
	for _, s := range pl.stages {
		if !gone[s] {
			s.ID = len(stages)
			stages = append(stages, s)
		}
	}
	pl.stages = stages
	return nil
}

// fusePartitions takes out the stages that write, partition by partition,
// an exchange that one later stage alone reads partition by partition,
// when each row they write lands in the partition it was computed from:
// that stage then computes their rows itself, from the same partition of
// what they read, rather than waiting for a round of tasks that writes
// them first, as an aggregation by person over a relation keyed by person
// does before the stage that reads its groups. See passesKey for when rows
// stay in their partition; any other stage stays.
func (pl *planner) fusePartitions() error {
	ref, err := pl.p.fromRef("SELECT * FROM (SELECT 1) AS v")
	if err != nil {
		return err
	}
	wrap, err := pl.p.template("SELECT * FROM (SELECT 1) AS __igloo_f")
	if err != nil {
		return err
	}
	for {
		p, c, at := pl.passThrough()
		if p == nil {
			return nil
		}
		// p's views, under names of their own in c.
		names := map[string]string{}
		for _, in := range p.Inputs {
			pl.fused++
			names[in.View] = fmt.Sprintf("__igloo_x%d", pl.fused)
		}
		row := clone(p.fragment).(node)
		renameViews(row, names)
		if len(p.Columns) > 0 {
			w := clone(wrap).(node)
			from := child(w, "from_table")
			child(from, "subquery")["node"] = row
			cols := make([]any, len(p.Columns))
			for i, col := range p.Columns {
				cols[i] = col
			}
			from["column_name_alias"] = cols
			row = w
		}
		view := c.Inputs[at].View
		c.fragment = rewrite(c.fragment, func(n node) (node, bool) {
			if !isView(n, view) {
				return nil, false
			}
			sub := clone(ref).(node)
			child(sub, "subquery")["node"] = clone(row)
			sub["alias"] = str(n["alias"])
			if sub["alias"] == "" {
				sub["alias"] = view
			}
			if aliases := list(n, "column_name_alias"); aliases != nil {
				sub["column_name_alias"] = aliases
			}
			return sub, true
		}).(node)
		inputs := slices.Delete(slices.Clone(c.Inputs), at, at+1)
		for _, in := range p.Inputs {
			in.View = names[in.View]
			inputs = append(inputs, in)
		}
		c.Inputs = inputs
		var stages []*Stage
		for _, s := range pl.stages {
			if s != p {
				s.ID = len(stages)
				stages = append(stages, s)
			}
		}
		pl.stages = stages
	}
}

// passThrough finds a stage p that fusePartitions takes out: one that
// reads exchanges partition by partition and no divided table, writes an
// exchange that only c reads, as its input at, partition by partition,
// and passes its key through (passesKey).
func (pl *planner) passThrough() (p, c *Stage, at int) {
	writers := map[*Exchange]int{}
	type use struct {
		s  *Stage
		at int
	}
	readers := map[*Exchange][]use{}
	for _, s := range pl.stages {
		if s.Out != nil {
			writers[s.Out]++
		}
		for i, in := range s.Inputs {
			if in.Exchange != nil {
				readers[in.Exchange] = append(readers[in.Exchange], use{s, i})
			}
		}
	}
	for _, s := range pl.stages {
		x := s.Out
		if x == nil || writers[x] != 1 || len(readers[x]) != 1 || s.split() != nil || !s.partitioned() {
			continue
		}
		r := readers[x][0]
		if r.s == s || !r.s.Inputs[r.at].Partitioned || !passesKey(s, x) {
			continue
		}
		named := false
		walk(r.s.fragment, func(n node) bool {
			named = named || isView(n, r.s.Inputs[r.at].View)
			return !named
		})
		if named {
			return s, r.s, r.at
		}
	}
	return nil, nil, 0
}

// passesKey reports whether stage s writes each row to the partition of x
// that the row was computed from. x's key columns must be, in order, the
// key columns of one exchange s reads partition by partition, each a
// select item that refers to that exchange's view and column and nothing
// else (and so of the column's type, as the view casts it to the type its
// writers wrote), and neither exchange may hash a key through a cast: the
// hash of each row's key is then the hash it was read by. A key computed
// in any way, a cast included, is not, nor an integer key, which
// exchanges hash as BIGINT.
func passesKey(s *Stage, x *Exchange) bool {
	sel := s.fragment
	if str(sel["type"]) != "SELECT_NODE" || len(x.Keys) == 0 || len(x.Casts) != len(x.Keys) {
		return false
	}
	refs := map[string]node{}
	if !viewRefs(child(sel, "from_table"), refs) {
		return false
	}
	items := list(sel, "select_list")
	var keyed *Input
	for i, key := range x.Keys {
		// The select item that writes the key column.
		item := -1
		for j, it := range items {
			e := it.(node)
			name := str(e["alias"])
			switch {
			case len(s.Columns) > 0:
				if j < len(s.Columns) {
					name = s.Columns[j]
				}
			case str(e["class"]) == "STAR":
				return false
			case name == "" && str(e["class"]) == "COLUMN_REF":
				parts := columnNames(e)
				name = parts[len(parts)-1]
			}
			if strings.EqualFold(name, key) {
				if item >= 0 {
					return false
				}
				item = j
			}
		}
		if item < 0 {
			return false
		}
		e := items[item].(node)
		if str(e["class"]) != "COLUMN_REF" {
			return false
		}
		parts := columnNames(e)
		var ref node
		switch {
		case len(parts) == 2:
			ref = refs[strings.ToLower(parts[0])]
		case len(parts) == 1 && len(refs) == 1:
			for _, r := range refs {
				ref = r
			}
		}
		if ref == nil || len(list(ref, "column_name_alias")) > 0 {
			return false
		}
		var in *Input
		for k := range s.Inputs {
			if s.Inputs[k].View == str(ref["table_name"]) && s.Inputs[k].Exchange != nil && s.Inputs[k].Partitioned {
				in = &s.Inputs[k]
			}
		}
		if in == nil || keyed != nil && keyed != in {
			return false
		}
		keyed = in
		ix := in.Exchange
		if len(ix.Keys) != len(x.Keys) || len(ix.Casts) != len(ix.Keys) || !strings.EqualFold(ix.Keys[i], parts[len(parts)-1]) ||
			ix.Casts[i] != "" || x.Casts[i] != "" || ix.Buckets != x.Buckets || ix.UUIDHash != x.UUIDHash {
			return false
		}
	}
	return true
}

// viewRefs adds the view references of a FROM clause to refs, by the name
// their columns are qualified with, and reports whether it holds nothing
// else: only views and joins of them, under distinct names.
func viewRefs(ref node, refs map[string]node) bool {
	switch str(ref["type"]) {
	case "BASE_TABLE":
		if str(ref["schema_name"]) != "" || str(ref["catalog_name"]) != "" || !strings.HasPrefix(str(ref["table_name"]), "__igloo_") {
			return false
		}
		name := str(ref["alias"])
		if name == "" {
			name = str(ref["table_name"])
		}
		name = strings.ToLower(name)
		if refs[name] != nil {
			return false
		}
		refs[name] = ref
		return true
	case "JOIN":
		return viewRefs(child(ref, "left"), refs) && viewRefs(child(ref, "right"), refs)
	}
	return false
}

// renameViews makes the references to the views names maps in q read the
// views they map to, under the names their columns are qualified with.
func renameViews(q node, names map[string]string) {
	walk(q, func(n node) bool {
		if str(n["type"]) == "BASE_TABLE" && str(n["schema_name"]) == "" && str(n["catalog_name"]) == "" {
			if to, ok := names[str(n["table_name"])]; ok {
				if str(n["alias"]) == "" {
					n["alias"] = str(n["table_name"])
				}
				n["table_name"] = to
			}
		}
		return true
	})
}

// isView reports whether n is a reference to the view called name.
func isView(n node, name string) bool {
	return str(n["type"]) == "BASE_TABLE" && str(n["table_name"]) == name && str(n["schema_name"]) == "" && str(n["catalog_name"]) == ""
}

// stage adds a stage to the plan.
func (pl *planner) stage(s *Stage) *Stage {
	s.ID = len(pl.stages)
	pl.stages = append(pl.stages, s)
	pl.progressed()
	return s
}

// exchange plans an exchange keyed by keys, hashed as casts say.
func (pl *planner) exchange(purpose string, keys, casts []string) *Exchange {
	pl.exchanges++
	return &Exchange{ID: pl.exchanges - 1, Keys: keys, Casts: casts, purpose: purpose}
}

// queryLocationNone is DuckDB's position for a node with no place in the
// statement's text.
const queryLocationNone = "18446744073709551615"

// --- time and functions -----------------------------------------------------

// timeFunctions are DuckDB's functions that return the statement's time,
// and the type each returns.
var timeFunctions = map[string]string{
	"now": "TIMESTAMP WITH TIME ZONE", "get_current_timestamp": "TIMESTAMP WITH TIME ZONE",
	"current_timestamp": "TIMESTAMP WITH TIME ZONE", "transaction_timestamp": "TIMESTAMP WITH TIME ZONE",
	"today": "DATE", "current_date": "DATE",
	"current_time": "TIME WITH TIME ZONE", "get_current_time": "TIME WITH TIME ZONE",
	"localtime": "TIME", "localtimestamp": "TIMESTAMP",
	"current_localtime": "TIME", "current_localtimestamp": "TIMESTAMP",
}

// fixTime replaces the statement's calls of now() and its relatives with
// the planner's fixed time, converted in the session's time zone as DuckDB
// would: workers run with the session's TimeZone.
func (pl *planner) fixTime(root node) error {
	stamp := pl.opts.now.UTC().Truncate(time.Microsecond).Format("2006-01-02 15:04:05.999999") + "+00"
	var err error
	rewrite(root, func(n node) (node, bool) {
		if err != nil {
			return nil, false
		}
		var name string
		switch str(n["class"]) {
		case "FUNCTION":
			if n["is_operator"] != true && len(list(n, "children")) == 0 {
				name = strings.ToLower(str(n["function_name"]))
			}
		case "COLUMN_REF":
			// checkTimeColumns made sure no column has such a name.
			if parts := columnNames(n); len(parts) == 1 {
				name = strings.ToLower(parts[0])
			}
		}
		typ, ok := timeFunctions[name]
		if !ok {
			return nil, false
		}
		text := "CAST(" + quoteLiteral(stamp) + " AS TIMESTAMP WITH TIME ZONE)"
		if typ != "TIMESTAMP WITH TIME ZONE" {
			text = "CAST(" + text + " AS " + typ + ")"
		}
		e, perr := pl.p.expr(text)
		if perr != nil {
			err = perr
			return nil, false
		}
		e["alias"] = n["alias"]
		return e, true
	})
	return err
}

// checkTimeColumns makes sure that a bare name such as current_date means
// the function, as it does in DuckDB unless something the statement reads
// has a column of that name: everything it reads must be a catalog table
// without one, and no alias may have such a name.
func (pl *planner) checkTimeColumns(stmt node) error {
	named := false
	walk(stmt, func(n node) bool {
		if str(n["class"]) == "COLUMN_REF" {
			if p := columnNames(n); len(p) == 1 {
				_, named = timeFunctions[strings.ToLower(p[0])]
			}
		}
		return !named
	})
	if !named {
		return nil
	}
	var ctes []string
	walk(stmt, func(n node) bool {
		ctes = append(ctes, cteNames(n)...)
		return true
	})
	var err error
	walk(stmt, func(n node) bool {
		if err != nil {
			return false
		}
		if _, ok := timeFunctions[strings.ToLower(str(n["alias"]))]; ok {
			err = unsupported("something called %s", str(n["alias"]))
			return false
		}
		for _, a := range list(n, "column_name_alias") {
			if _, ok := timeFunctions[strings.ToLower(str(a))]; ok {
				err = unsupported("a column called %s", str(a))
				return false
			}
		}
		switch str(n["type"]) {
		case "BASE_TABLE":
			parts := tableParts(n)
			if len(parts) == 1 && slices.Contains(ctes, strings.ToLower(parts[0])) {
				return false
			}
			t, ok, terr := pl.lookupTable(parts)
			switch {
			case terr != nil:
				err = terr
			case !ok:
				err = unsupported("%s, whose columns may include current_date and the like", strings.Join(parts, "."))
			default:
				for _, c := range t.Columns {
					if _, clash := timeFunctions[strings.ToLower(c.Name)]; clash {
						err = unsupported("%s has a column called %s", t, c.Name)
					}
				}
			}
			return false
		case "TABLE_FUNCTION", "PIVOT", "SHOW_REF", "COLUMN_DATA":
			err = unsupported("%s, whose columns may include current_date and the like", strings.ToLower(str(n["type"])))
			return false
		}
		return true
	})
	return err
}

// sessionFunctions depend on the session or the machine they run on, so
// workers would compute something else. igloo_ml_score scores a model
// that only the server that trained it for the statement has.
var sessionFunctions = map[string]bool{
	"getvariable": true, "current_setting": true, "current_schema": true, "current_schemas": true,
	"current_database": true, "current_catalog": true, "current_query": true, "current_role": true,
	"current_user": true, "session_user": true, "user": true, "nextval": true, "currval": true,
	"setseed": true, "txid_current": true, "pg_backend_pid": true, "in_search_path": true,
	"has_any_column_privilege": true, "has_column_privilege": true, "has_database_privilege": true,
	"has_function_privilege": true, "has_language_privilege": true, "has_schema_privilege": true,
	"has_sequence_privilege": true, "has_server_privilege": true, "has_table_privilege": true,
	"has_tablespace_privilege": true, "pg_has_role": true, "version": true, "igloo_ml_score": true,
	// The rest of what DuckDB says is not consistent (duckdb_functions()'s
	// stability), or reads the session's connection or catalog.
	"ago": true, "current_connection_id": true, "current_query_id": true, "current_transaction_id": true,
	"get_block_size": true, "stats": true, "write_log": true, "has_foreign_data_wrapper_privilege": true,
	"inet_client_addr": true, "inet_client_port": true, "inet_server_addr": true, "inet_server_port": true,
	"pg_conf_load_time": true, "pg_postmaster_start_time": true, "pg_is_other_temp_schema": true,
	"pg_my_temp_schema": true, "pg_collation_is_visible": true, "pg_conversion_is_visible": true,
	"pg_function_is_visible": true, "pg_opclass_is_visible": true, "pg_operator_is_visible": true,
	"pg_opfamily_is_visible": true, "pg_table_is_visible": true, "pg_ts_config_is_visible": true,
	"pg_ts_dict_is_visible": true, "pg_ts_parser_is_visible": true, "pg_ts_template_is_visible": true,
	"pg_type_is_visible": true, "pg_get_constraintdef": true, "pg_get_expr": true, "pg_get_viewdef": true,
	"col_description": true, "obj_description": true, "shobj_description": true,
}

// SessionFunction says a function of that name, in lower case, depends on
// the session or the machine it runs on.
func SessionFunction(name string) bool { return sessionFunctions[name] }

// checkFunctions refuses a fragment that calls a function workers would
// evaluate differently from the coordinator: one tied to the session, or
// one a user defined, which only the coordinator has.
func (pl *planner) checkFunctions(fragment node) error {
	var err error
	walk(fragment, func(n node) bool {
		if err != nil {
			return false
		}
		if (str(n["class"]) == "FUNCTION" && n["is_operator"] != true) || str(n["class"]) == "WINDOW" {
			name := strings.ToLower(str(n["function_name"]))
			if sessionFunctions[name] && (name != "getvariable" || !pl.opts.values[variableName(n)]) {
				err = unsupported("%s()", name)
				return false
			}
			if coordinatorFunctions[name] {
				err = unsupported("%s(), which calls a model provider with keys only the coordinator holds", name)
				return false
			}
			if name == "rb_count_distinct" && len(list(n, "children")) == 2 {
				// It has no partials: workers would complete its groups from
				// every row, exchanged whole, several times slower than one
				// machine on a report's unnested rows. A bitmap of each
				// group's IDs a key (rb_agg), then the sum of their
				// cardinalities, crosses as partials.
				err = unsupported("rb_count_distinct(id, key), which has no partials")
				return false
			}
			start := time.Now()
			user, uerr := pl.env.UserFunction(pl.ctx, name)
			pl.looked(start)
			switch {
			case uerr != nil:
				err = uerr
			case user:
				err = unsupported("%s is a user-defined function", name)
			}
		}
		return err == nil
	})
	return err
}

// --- expression helpers -----------------------------------------------------

// hasClass reports whether v holds an expression of the given class.
func hasClass(v any, class string) bool {
	found := false
	walk(v, func(n node) bool {
		if str(n["class"]) == class {
			found = true
		}
		return !found
	})
	return found
}

// hasAggregate reports whether v calls an aggregate function outside
// subqueries.
func (pl *planner) hasAggregate(v any) bool {
	found := false
	walk(v, func(n node) bool {
		switch {
		case found, str(n["class"]) == "SUBQUERY":
			return false
		case pl.isAggregate(n):
			found = true
			return false
		}
		return true
	})
	return found
}

// isAggregate reports whether e is a call of an aggregate function.
func (pl *planner) isAggregate(e node) bool {
	return str(e["class"]) == "FUNCTION" && e["is_operator"] != true && pl.aggs[strings.ToLower(str(e["function_name"]))]
}
