package distributed

import (
	"context"
	"slices"
	"strings"

	"golang.org/x/sync/errgroup"
)

// Scan is what a statement reads of one catalog table, for reading its
// data files into a cache ahead of DuckDB (see Scanner).
type Scan struct {
	Schema, Table string
	// Snapshot is the snapshot Files are of: the table's current one when
	// it was loaded, at most a few seconds ago (see Source.Table). A commit
	// since then is not seen.
	Snapshot int64
	// Files are the snapshot's data files in the order DuckDB reads them:
	// the manifest list's order, then each manifest's. Path is as the
	// manifest writes it, the string DuckDB opens. Every data file is
	// listed; the statement's filters prune none (see Filtered). On a
	// worker, they are a task's files, in the order its view lists them,
	// and Rows is 0.
	Files []File
	// Columns are the columns of the table the statement may read, and
	// FieldIDs their Iceberg field IDs, with the IDs of their nested
	// fields. They err on the side of more: a column named anywhere in
	// the statement counts for every table with a column of that name.
	// Both are empty when it reads no column, as COUNT(*) does.
	Columns  []string
	FieldIDs []int
	// Filtered says a WHERE clause of the statement names one of the
	// table's columns, so that DuckDB may skip files and row groups of
	// Files.
	Filtered bool
}

// ScanEnv is the session a statement is scanned in.
type ScanEnv interface {
	// Resolve returns the schema and name of the catalog table a table
	// name, as a statement writes it (one to three parts), means in this
	// session. ok is false when it means anything else, or a table whose
	// files the session's user may not read.
	Resolve(ctx context.Context, parts []string) (schema, name string, ok bool, err error)
	// ViewDefinition is as Env's.
	ViewDefinition(ctx context.Context, parts []string) (def *ViewDefinition, ok bool, err error)
}

// Scanner lists the catalog tables a statement reads and their data files,
// before it runs, for prefetching them. It reads the tables' metadata
// through a Source, which keeps it a few seconds and each snapshot's files
// for as long as it is current, so a statement over tables read lately
// costs no catalog round trip.
type Scanner struct {
	p   *parser
	src Source
}

// NewScanner returns a Scanner that loads tables through src. Close it
// when done.
func NewScanner(src Source) (*Scanner, error) {
	p, err := newParser()
	if err != nil {
		return nil, err
	}
	return &Scanner{p: p, src: src}, nil
}

// Close releases the Scanner's parser.
func (s *Scanner) Close() error { return s.p.close() }

// maxScanLoads bounds the tables one statement's scan loads at once.
const maxScanLoads = 8

// Scans returns what query, one SELECT, reads of each catalog table: one
// Scan per table, in the order the statement first names them. Views of
// the catalog, and the session's, are read through. What it cannot list
// is left out: tables read at another snapshot (AT), table functions,
// tables of other databases, and tables whose metadata or files cannot be
// loaded or listed.
func (s *Scanner) Scans(ctx context.Context, query string, env ScanEnv) ([]Scan, error) {
	w, err := walkScans(ctx, s.p, query, env)
	if err != nil {
		return nil, err
	}
	scans := make([]Scan, len(w.order))
	ok := make([]bool, len(w.order))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxScanLoads)
	for i, key := range w.order {
		u := w.uses[key]
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}
			t, err := s.src.Table(gctx, u.schema, u.name)
			if err != nil || t == nil {
				return gctx.Err()
			}
			files, err := s.src.Files(gctx, t)
			if err != nil {
				return gctx.Err()
			}
			// The Source keeps the list, for the statements workers run too.
			scans[i] = Scan{Schema: t.Schema, Table: t.Name, Snapshot: t.Snapshot, Files: slices.Clone(files)}
			w.read(&scans[i], t.Columns, u)
			ok[i] = true
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	out := scans[:0]
	for i, sc := range scans {
		if ok[i] {
			out = append(out, sc)
		}
	}
	return out, nil
}

// walkScans walks query, one SELECT, for the catalog tables it reads and
// the columns it names.
func walkScans(ctx context.Context, p *parser, query string, env ScanEnv) (*scanWalk, error) {
	stmt, err := p.parse(query)
	if err != nil {
		return nil, err
	}
	w := &scanWalk{ctx: ctx, p: p, env: env, ctes: map[string]bool{}, wholeCTE: map[string]bool{},
		uses: map[string]*scanUse{}, names: map[string]bool{}, filters: map[string]bool{}}
	walk(stmt, func(n node) bool {
		for _, name := range cteNames(n) {
			w.ctes[name] = true
		}
		return true
	})
	if err := w.query(stmt, true, nil); err != nil {
		return nil, err
	}
	return w, nil
}

// scanWalk walks a statement for Scanner.Scans.
type scanWalk struct {
	ctx context.Context
	p   *parser
	env ScanEnv
	// ctes are the names of every common table expression of the
	// statement and of the views it reads through, lower case: a one-part
	// name among them may mean one rather than a table.
	ctes map[string]bool
	// wholeCTE are those of them read by a star whose columns are all
	// needed.
	wholeCTE map[string]bool
	uses     map[string]*scanUse
	order    []string
	// names are every name a column reference of the statement has, in
	// any part, lower case; filters those of references in WHERE clauses.
	// The walk is done before the loads that read them start.
	names, filters map[string]bool
	views          int
}

// scanUse is a catalog table a statement reads.
type scanUse struct {
	schema, name string
	// all says the statement may read every column, through a star whose
	// columns are all needed.
	all bool
	// renames are the alias lists it is read with, as in FROM t AS x(a, b),
	// which rename its first columns.
	renames [][]string
}

// qualifier returns the database, schema and name a table name written in
// a view's body means, as ViewDefinition.Resolve does; nil outside views.
type qualifier func(ctx context.Context, parts []string) ([]string, bool, error)

// query walks a query node. whole says its result's columns are all
// needed, so that a star in its SELECT list reads every column it covers:
// the statement's own result, or a subquery read through such a star.
func (w *scanWalk) query(q node, whole bool, qualify qualifier) error {
	if q == nil {
		return nil
	}
	switch str(q["type"]) {
	case "SELECT_NODE":
		if err := w.selectNode(q, whole, qualify); err != nil {
			return err
		}
	case "SET_OPERATION_NODE", "RECURSIVE_CTE_NODE":
		for _, k := range []string{"left", "right"} {
			if err := w.query(child(q, k), whole, qualify); err != nil {
				return err
			}
		}
		for _, c := range list(q, "children") {
			if n, ok := c.(node); ok {
				if err := w.query(n, whole, qualify); err != nil {
					return err
				}
			}
		}
		if err := w.exprs(q["modifiers"], qualify, false); err != nil {
			return err
		}
	default:
		// Read every table under it whole.
		return w.anything(q, qualify)
	}
	// Common table expressions, last first: one is read by the query or by
	// those defined after it.
	m := list(child(q, "cte_map"), "map")
	for i := len(m) - 1; i >= 0; i-- {
		e := m[i].(node)
		body := child(child(child(e, "value"), "query"), "node")
		if err := w.query(body, w.wholeCTE[strings.ToLower(str(e["key"]))], qualify); err != nil {
			return err
		}
	}
	return nil
}

// selectNode walks a SELECT.
func (w *scanWalk) selectNode(q node, whole bool, qualify qualifier) error {
	// The relations whose every column its stars read: "" for all of them.
	// A star of the SELECT list reads them if the result needs all its
	// columns; a star anywhere else, as in a function's arguments, does
	// whatever the result needs.
	stars := map[string]bool{}
	nested := func(v any) {
		walk(v, func(n node) bool {
			if n["subquery"] != nil {
				return false
			}
			if str(n["class"]) == "STAR" {
				stars[strings.ToLower(str(n["relation_name"]))] = true
			}
			return true
		})
	}
	for k, v := range q {
		switch k {
		case "from_table", "cte_map":
		case "select_list":
			for _, item := range list(q, k) {
				e, _ := item.(node)
				if str(e["class"]) != "STAR" {
					nested(e)
					continue
				}
				if whole {
					stars[strings.ToLower(str(e["relation_name"]))] = true
				}
				for k, v := range e {
					if k != "class" && k != "type" {
						nested(v)
					}
				}
			}
		default:
			nested(v)
		}
	}
	covered := func(alias string) bool { return stars[""] || stars[strings.ToLower(alias)] }
	if err := w.from(child(q, "from_table"), covered, qualify); err != nil {
		return err
	}
	for k, v := range q {
		switch k {
		case "from_table", "cte_map":
		default:
			if err := w.exprs(v, qualify, k == "where_clause"); err != nil {
				return err
			}
		}
	}
	return nil
}

// from walks a FROM clause's table reference.
func (w *scanWalk) from(ref node, covered func(alias string) bool, qualify qualifier) error {
	if ref == nil {
		return nil
	}
	alias := str(ref["alias"])
	switch str(ref["type"]) {
	case "BASE_TABLE":
		if alias == "" {
			alias = str(ref["table_name"])
		}
		return w.table(ref, covered(alias), qualify)
	case "SUBQUERY":
		whole := covered(alias) || len(list(ref, "column_name_alias")) > 0
		return w.query(subqueryNode(ref), whole, qualify)
	case "JOIN":
		if str(ref["ref_type"]) == "NATURAL" {
			// The join compares the columns of both sides with the same
			// names, whichever they are.
			covered = func(string) bool { return true }
		}
		if err := w.from(child(ref, "left"), covered, qualify); err != nil {
			return err
		}
		if err := w.from(child(ref, "right"), covered, qualify); err != nil {
			return err
		}
		for _, u := range list(ref, "using_columns") {
			w.names[strings.ToLower(str(u))] = true
		}
		return w.exprs(ref["condition"], qualify, false)
	case "TABLE_FUNCTION":
		return w.exprs(ref["function"], qualify, false)
	case "EMPTY":
		return nil
	}
	// PIVOT and the like.
	return w.anything(ref, qualify)
}

// table walks a table name in FROM: a catalog table, a common table
// expression or a view.
func (w *scanWalk) table(ref node, whole bool, qualify qualifier) error {
	if child(ref, "at_clause") != nil {
		return nil
	}
	parts := tableParts(ref)
	if len(parts) == 1 && w.ctes[strings.ToLower(parts[0])] {
		if whole {
			w.wholeCTE[strings.ToLower(parts[0])] = true
		}
		return nil
	}
	if qualify != nil {
		q, ok, err := qualify(w.ctx, parts)
		if err != nil || !ok {
			return w.ctxErr(err)
		}
		parts = q
	}
	schema, name, ok, err := w.env.Resolve(w.ctx, parts)
	if err != nil {
		return w.ctxErr(err)
	}
	if ok {
		key := strings.ToLower(schema + "." + name)
		u := w.uses[key]
		if u == nil {
			u = &scanUse{schema: schema, name: name}
			w.uses[key] = u
			w.order = append(w.order, key)
		}
		u.all = u.all || whole
		if aliases := list(ref, "column_name_alias"); len(aliases) > 0 {
			names := make([]string, len(aliases))
			for i, a := range aliases {
				names[i] = strings.ToLower(str(a))
			}
			u.renames = append(u.renames, names)
		}
		return nil
	}
	def, ok, err := w.env.ViewDefinition(w.ctx, parts)
	if err != nil || !ok {
		return w.ctxErr(err)
	}
	if w.views++; w.views > maxViewDepth*4 {
		return nil
	}
	body, err := w.p.parse(def.Query)
	if err != nil {
		return nil
	}
	walk(body, func(n node) bool {
		for _, name := range cteNames(n) {
			w.ctes[name] = true
		}
		return true
	})
	// A view renamed by an alias list reads its columns under other names.
	return w.query(body, whole || len(list(ref, "column_name_alias")) > 0, def.Resolve)
}

// exprs notes the column names expressions under v reference, and walks
// their subqueries. where says they are a WHERE clause's.
func (w *scanWalk) exprs(v any, qualify qualifier, where bool) error {
	var err error
	walk(v, func(n node) bool {
		if err != nil {
			return false
		}
		switch {
		case str(n["class"]) == "COLUMN_REF":
			for _, part := range columnNames(n) {
				w.names[strings.ToLower(part)] = true
				if where {
					w.filters[strings.ToLower(part)] = true
				}
			}
		case str(n["class"]) == "SUBQUERY":
			// EXISTS needs no column of its subquery's result; IN and
			// scalar subqueries need all of them.
			t := str(n["subquery_type"])
			err = w.query(subqueryNode(n), t != "EXISTS" && t != "NOT_EXISTS", qualify)
			if err == nil {
				err = w.exprs(n["child"], qualify, where)
			}
			return false
		}
		return true
	})
	return err
}

// anything walks a node the walk does not otherwise know, reading every
// table under it whole.
func (w *scanWalk) anything(v any, qualify qualifier) error {
	var err error
	walk(v, func(n node) bool {
		if err != nil {
			return false
		}
		switch {
		case str(n["type"]) == "BASE_TABLE":
			err = w.table(n, true, qualify)
			return false
		case str(n["class"]) == "COLUMN_REF":
			for _, part := range columnNames(n) {
				w.names[strings.ToLower(part)] = true
			}
		}
		return true
	})
	return err
}

// ctxErr returns the context's error if err came with it done: other
// errors resolving a name leave the name out.
func (w *scanWalk) ctxErr(err error) error {
	if err != nil && w.ctx.Err() != nil {
		return w.ctx.Err()
	}
	return nil
}

// read sets what sc reads of a table of columns, which u is a use of: all
// of them, or those the statement names, by their names or the names an
// alias list gives them.
func (w *scanWalk) read(sc *Scan, columns []Column, u *scanUse) {
	for i, c := range columns {
		named := func(names map[string]bool) bool {
			if names[strings.ToLower(c.Name)] {
				return true
			}
			for _, r := range u.renames {
				if i < len(r) && names[r[i]] {
					return true
				}
			}
			return false
		}
		if named(w.filters) {
			sc.Filtered = true
		}
		if !u.all && !named(w.names) {
			continue
		}
		sc.Columns = append(sc.Columns, c.Name)
		sc.FieldIDs = append(sc.FieldIDs, c.ID)
		sc.FieldIDs = appendFieldIDs(sc.FieldIDs, c.Nested)
	}
}

// appendFieldIDs appends the field IDs of a nested column's fields, at
// every depth.
func appendFieldIDs(ids []int, n *Nesting) []int {
	if n == nil {
		return ids
	}
	for _, f := range n.Fields {
		ids = append(ids, f.ID)
		ids = appendFieldIDs(ids, f.Nested)
	}
	return ids
}
