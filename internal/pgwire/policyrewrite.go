package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/policy"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// The policy rewrite. Every reference to a protected table, wherever it is
// in a statement (FROM, joins, subqueries, common table expressions, set
// operations, the source of INSERT, CREATE TABLE AS and COPY, cursors), is
// replaced by a subquery that reads the table through its policies:
//
//	(SELECT CASE WHEN t.ok THEN CAST((<mask of email>) AS VARCHAR) END AS email,
//	        CASE WHEN t.ok THEN t.client_id END AS client_id, ...
//	 FROM (SELECT t.email, t.client_id, ..., (<row access policy>) AS ok
//	       FROM "lake"."crm"."contacts" AS t [AT (...)]) AS t
//	 WHERE t.ok) AS contacts
//
// The rest of the statement sees only what the subquery returns, so masking
// applies in WHERE, JOIN, GROUP BY and window clauses alike. Each column is
// also guarded by the row access policy: DuckDB may evaluate a condition of
// the statement before the policy's, and a condition that fails (a cast,
// error()) would otherwise show a hidden row's value in its message. Views
// that read protected tables are expanded in place, with their table names
// qualified so they mean what they meant in the view. The target of UPDATE,
// DELETE and MERGE cannot be a subquery; its conditions and expressions are
// evaluated against a guarded, masked copy of the target row instead (see
// targetEdits).
//
// Policies that are no-ops for the user (see policyProbe) are left out, so
// users a policy exempts run their statements as written.

// rewriter rewrites one statement for one session.
type rewriter struct {
	s     *Server
	ctx   context.Context
	sess  *session
	r     *resolver
	snap  *policy.Snapshot
	sens  *sensitivity
	pc    policyContext
	admin bool
	// filesOnly limits the rewriter to checking the files a statement names,
	// for non-admins whether or not any table has a policy; it makes no
	// table edits. allowWarehouse lets those file checks read the
	// warehouse's own files, which readers and writers may until a table has
	// a policy.
	filesOnly      bool
	allowWarehouse bool
	// nonce makes the aliases of this rewrite, which the checks use to
	// recognise its subqueries; a statement cannot contain it.
	nonce string
	n     int
	plans map[string]*tablePlan
	// touched says the statement reads a protected table; applied names
	// the policies that applied, in order.
	touched bool
	applied []string
}

func (rw *rewriter) alias(kind string) string {
	rw.n++
	return fmt.Sprintf("__igloo_%s_%s_%d", kind, rw.nonce, rw.n)
}

func (rw *rewriter) prefix(kind string) string { return "__igloo_" + kind + "_" + rw.nonce + "_" }

// ours reports whether alias is one of this rewrite's: the reference it
// names was made by the rewrite, which the statement's text cannot fake.
func (rw *rewriter) ours(alias string) bool {
	return strings.HasPrefix(alias, "__igloo_") && strings.Contains(alias, "_"+rw.nonce+"_")
}

func (rw *rewriter) okColumn() string { return quoteIdent("__igloo_ok_" + rw.nonce) }

// markTable is how a table a policy body reads is written into the
// statement: a subquery whose alias marks it as the policy's.
func (rw *rewriter) markTable(table string) string {
	return "(SELECT * FROM " + table + " AS " + quoteIdent(rw.alias("p")) + ")"
}

func (rw *rewriter) apply(names ...string) {
	for _, n := range names {
		if !slices.Contains(rw.applied, n) {
			rw.applied = append(rw.applied, n)
		}
	}
}

// --- protected tables -------------------------------------------------------

type columnType struct{ name, typ string }

// maskPlan is how one column is masked for the session.
type maskPlan struct {
	tmpl *bodyTemplate
	pc   policyContext
	// null masks the column to NULL: it has tags with masking policies,
	// none of them for its type.
	null bool
}

// tablePlan is how a protected table is read for the session.
type tablePlan struct {
	obj     object
	columns []columnType
	// filter is the row access policy, unless it is a no-op; filterArgs
	// are the columns it takes, as indexes into columns.
	filter     *bodyTemplate
	filterArgs []int
	// masks are by column index; nil where nothing masks the column.
	masks []*maskPlan
}

func (p *tablePlan) noop() bool {
	return p.filter == nil && !slices.ContainsFunc(p.masks, func(m *maskPlan) bool { return m != nil })
}

// plan works out how the session reads the protected table o.
func (rw *rewriter) plan(o object, rules *policy.TableRules) (*tablePlan, error) {
	if p, ok := rw.plans[o.key()]; ok {
		return p, nil
	}
	cols, err := describeTable(rw.ctx, rw.sess, quoteName([]string{o.db, o.schema, o.name}))
	if err != nil {
		return nil, err
	}
	plan := &tablePlan{obj: o, columns: cols, masks: make([]*maskPlan, len(cols))}
	index := func(name string) int {
		return slices.IndexFunc(cols, func(c columnType) bool { return strings.EqualFold(c.name, name) })
	}
	table := o.schema + "." + o.name

	if rules.Filter != nil {
		p := *rules.FilterPolicy
		rw.apply(p.Name)
		for i, name := range rules.Filter.Columns {
			j := index(name)
			if j < 0 {
				return nil, denied("row access policy %s of %s reads column %s, which the table does not have", p.Name, table, name)
			}
			if i >= len(p.Args) || !strings.EqualFold(cols[j].typ, p.Args[i].Type) {
				return nil, denied("row access policy %s takes %s, but column %s of %s is %s", p.Name, p.Signature(), name, table, cols[j].typ)
			}
			plan.filterArgs = append(plan.filterArgs, j)
		}
		tmpl, noop, err := rw.prepareBody(p, rw.pc)
		if err != nil {
			return nil, err
		}
		if !noop {
			plan.filter = tmpl
		}
	}

	for i, col := range cols {
		cr := rules.Column(col.name)
		if !cr.Masked() {
			continue
		}
		pc := rw.pc
		pc.tags = map[string]string{}
		for _, ct := range cr.Tags {
			pc.tags[ct.Tag] = ct.Value
		}
		var p policy.Policy
		switch {
		case cr.Mask != nil:
			p = *cr.Mask
			if !strings.EqualFold(p.Args[0].Type, col.typ) {
				return nil, denied("masking policy %s takes %s, but column %s of %s is %s", p.Name, p.Args[0].Type, col.name, table, col.typ)
			}
		default:
			var found []policy.TagMask
			for _, tm := range cr.TagMasks {
				if strings.EqualFold(tm.Policy.Args[0].Type, col.typ) {
					found = append(found, tm)
				}
			}
			switch len(found) {
			case 0:
				// Tagged for masking, with no policy for its type: hide it.
				for _, tm := range cr.TagMasks {
					rw.apply(tm.Policy.Name)
				}
				plan.masks[i] = &maskPlan{null: true}
				continue
			case 1:
				p = found[0].Policy
			default:
				return nil, denied("column %s of %s has tags %s and %s, both with masking policies for %s; unset one of them",
					col.name, table, found[0].Tag, found[1].Tag, col.typ)
			}
		}
		rw.apply(p.Name)
		tmpl, noop, err := rw.prepareBody(p, pc)
		if err != nil {
			return nil, err
		}
		if !noop {
			plan.masks[i] = &maskPlan{tmpl: tmpl, pc: pc}
		}
	}
	if rw.plans == nil {
		rw.plans = map[string]*tablePlan{}
	}
	rw.plans[o.key()] = plan
	return plan, nil
}

// prepareBody returns p's template and whether p is a no-op for pc. It
// refuses a body that calls a function a macro of the session's shadows:
// DuckDB would call the macro instead.
func (rw *rewriter) prepareBody(p policy.Policy, pc policyContext) (*bodyTemplate, bool, error) {
	tmpl, err := rw.s.gov.template(rw.r, p)
	if err != nil {
		return nil, false, denied("%v", err)
	}
	for _, f := range tmpl.functions {
		if rw.sens.macros[f] {
			return nil, false, denied("policy %s calls %s, which a macro of this session replaces; drop the macro", p.Name, f)
		}
	}
	args := probeArgs(p)
	noop, err := rw.s.gov.probe.noop(rw.ctx, p.Kind, args[0], tmpl.render(args, pc, func(t string) string { return t }))
	if err != nil {
		return nil, false, denied("policy %s cannot run: %v", p.Name, err)
	}
	return tmpl, noop, nil
}

// describeTable lists a table's columns and their types.
func describeTable(ctx context.Context, sess *session, name string) ([]columnType, error) {
	rows, err := sess.conn.QueryContext(ctx, "SELECT column_name, column_type FROM (DESCRIBE "+name+")", nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var out []columnType
	dest := make([]driver.Value, 2)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}
		n, _ := dest[0].(string)
		t, _ := dest[1].(string)
		out = append(out, columnType{n, t})
	}
}

// value is column i as the session sees it, read through col, the raw
// column's expression.
func (rw *rewriter) value(p *tablePlan, i int, col string) string {
	m := p.masks[i]
	switch {
	case m == nil:
		return col
	case m.null:
		return "CAST(NULL AS " + p.columns[i].typ + ")"
	}
	return "CAST((" + m.tmpl.render([]string{col}, m.pc, rw.markTable) + ") AS " + p.columns[i].typ + ")"
}

// filterExpr is the row access policy over the columns of alias.
func (rw *rewriter) filterExpr(p *tablePlan, alias string) string {
	args := make([]string, len(p.filterArgs))
	for i, j := range p.filterArgs {
		args[i] = alias + "." + quoteIdent(p.columns[j].name)
	}
	return p.filter.render(args, rw.pc, rw.markTable)
}

// subquery reads the table through its policies; at is its AT clause.
func (rw *rewriter) subquery(p *tablePlan, at string) string {
	a := quoteIdent(rw.alias("t"))
	table := quoteName([]string{p.obj.db, p.obj.schema, p.obj.name})
	if at != "" {
		at = " " + at
	}
	items := make([]string, len(p.columns))
	if p.filter == nil {
		for i, c := range p.columns {
			items[i] = rw.value(p, i, a+"."+quoteIdent(c.name)) + " AS " + quoteIdent(c.name)
		}
		return "(SELECT " + strings.Join(items, ", ") + " FROM " + table + " AS " + a + at + ")"
	}
	ok := rw.okColumn()
	raw := make([]string, len(p.columns))
	for i, c := range p.columns {
		raw[i] = a + "." + quoteIdent(c.name)
		items[i] = "CASE WHEN " + a + "." + ok + " THEN " + rw.value(p, i, raw[i]) + " END AS " + quoteIdent(c.name)
	}
	inner := "SELECT " + strings.Join(raw, ", ") + ", (" + rw.filterExpr(p, a) + ") AS " + ok +
		" FROM " + table + " AS " + a + at
	return "(SELECT " + strings.Join(items, ", ") + " FROM (" + inner + ") AS " + a + " WHERE " + a + "." + ok + ")"
}

// --- queries ----------------------------------------------------------------

// sensitive reports whether o is protected or a view that reads a
// protected table.
func (rw *rewriter) sensitive(o object) bool {
	if o.view {
		return rw.sens.viewProtected[o.key()]
	}
	return strings.EqualFold(o.db, rw.r.catalog) && rw.snap.Rules(o.schema, o.name) != nil
}

// queryEdits returns the edits that make text, a query written in the
// body of view (or at the top level when view is nil), read protected
// tables through their policies.
func (rw *rewriter) queryEdits(text string, view *object, depth int) ([]edit, error) {
	if depth > maxViewDepth {
		return nil, denied("views nest more than %d deep", maxViewDepth)
	}
	ast, err := rw.r.serialize(text)
	if err != nil {
		return nil, denied("cannot apply policies to this statement: %v", err)
	}
	return rw.parseEdits(text, scanParse(ast), view, depth, func(loc int) (int, bool) { return loc, loc >= 0 && loc < len(text) }, nil)
}

// parseEdits returns the edits to text for a parse of it, or of a text
// composed from its pieces: toText maps the parse's positions to text's.
// skip leaves references out, such as the target of UPDATE.
func (rw *rewriter) parseEdits(text string, ps *parseScan, view *object, depth int, toText func(int) (int, bool), skip func(*refNode) bool) ([]edit, error) {
	if err := rw.checkFunctions(ps); err != nil {
		return nil, err
	}
	var edits []edit
	for _, ref := range ps.refs {
		if ref.cte || skip != nil && skip(ref) || rw.ours(ref.alias) {
			continue
		}
		e, err := rw.refEdit(text, ref, view, depth, toText)
		if err != nil {
			return nil, err
		}
		if e != nil {
			edits = append(edits, *e)
		}
	}
	return edits, nil
}

// refEdit returns the edit, if any, for one table reference.
func (rw *rewriter) refEdit(text string, ref *refNode, view *object, depth int, toText func(int) (int, bool)) (*edit, error) {
	if rw.filesOnly {
		// Only a reference that names a file concerns the file checks: a
		// path-like string or quoted identifier DuckDB reads as a file
		// (a replacement scan). Real tables are left alone.
		last := ref.parts[len(ref.parts)-1]
		if !rw.admin && (pathLike(last) || strings.ContainsAny(last, "/\\")) {
			if err := rw.s.checkPath(last, rw.allowWarehouse); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}
	name := strings.Join(ref.parts, ".")
	res, err := rw.r.resolve(tableRef{parts: ref.parts}, view)
	if err != nil {
		return nil, err
	}
	if len(res.all) == 0 {
		// Not a table or view Igloo knows: a file, or a table DuckDB will
		// report missing, or one this replica has not listed yet.
		last := ref.parts[len(ref.parts)-1]
		switch {
		case pathLike(last) || strings.ContainsAny(last, "/\\"):
			if !rw.admin {
				if err := rw.s.checkPath(last, rw.allowWarehouse); err != nil {
					return nil, err
				}
			}
			return nil, nil
		case rw.snap.ProtectedNames()[strings.ToLower(last)]:
			return nil, denied("cannot tell which table %s is; a table of that name has policies", name)
		}
		return nil, nil
	}
	if len(res.first) != 1 {
		if slices.ContainsFunc(res.all, rw.sensitive) {
			return nil, denied("%s is ambiguous; qualify it", name)
		}
		return nil, nil
	}
	o := res.first[0]
	for _, other := range res.all {
		if other != o && rw.sensitive(other) {
			return nil, denied("%s could mean %s.%s.%s, which has policies; qualify it", name, other.db, other.schema, other.name)
		}
	}
	loc, ok := toText(ref.loc)
	if !ok {
		return nil, denied("cannot place %s in the statement", name)
	}
	at := *ref
	at.loc = loc
	switch {
	case o.external:
		// Igloo's own view of an external table (Delta Sharing), over
		// files only its proxy serves; policies cannot be attached to
		// one (see policyTable).
	case !o.view && strings.EqualFold(o.db, rw.r.catalog) && rw.snap.Rules(o.schema, o.name) != nil:
		rw.touched = true
		plan, err := rw.plan(o, rw.snap.Rules(o.schema, o.name))
		if err != nil {
			return nil, err
		}
		if plan.noop() {
			return rw.qualify(text, &at, o, view)
		}
		sp, err := scanTableRef(text, &at)
		if err != nil {
			return nil, denied("cannot apply policies to this statement: %v", err)
		}
		return rw.replaceRef(text, &at, sp, rw.subquery(plan, sp.at))

	case o.view && rw.sens.viewProtected[o.key()]:
		sp, err := scanTableRef(text, &at)
		if err != nil {
			return nil, denied("cannot apply policies to this statement: %v", err)
		}
		cols, body, err := rw.inline(o, depth)
		if err != nil {
			return nil, err
		}
		alias := sp.alias
		if alias == "" {
			alias = quoteIdent(ref.parts[len(ref.parts)-1])
		}
		if len(cols) > 0 && len(ref.columnAliases) == 0 {
			// The view's own column names, unless the reference renames
			// them.
			quoted := make([]string, len(cols))
			for i, c := range cols {
				quoted[i] = quoteIdent(c)
			}
			alias += "(" + strings.Join(quoted, ", ") + ")"
		}
		sp.alias = alias
		return rw.replaceRef(text, &at, sp, body)

	case o.view && !rw.admin && rw.sens.viewRisky[o.key()]:
		if err := rw.checkView(o, depth); err != nil {
			return nil, err
		}
	}
	return rw.qualify(text, &at, o, view)
}

// qualify writes a reference in the body of a view that is being
// expanded with its full name, so it means in the statement what it meant
// in the view.
func (rw *rewriter) qualify(text string, ref *refNode, o object, view *object) (*edit, error) {
	if view == nil {
		return nil, nil
	}
	name, ok := scanName(text, ref.loc)
	if !ok || !sameParts(name.parts, ref.parts) {
		return nil, denied("cannot place %s in view %s.%s", strings.Join(ref.parts, "."), view.schema, view.name)
	}
	full := quoteName([]string{o.db, o.schema, o.name})
	if ref.alias == "" {
		// The table's own name stays usable as a qualifier.
		full += " AS " + quoteIdent(ref.parts[len(ref.parts)-1])
	}
	return &edit{name.start, name.end, full}, nil
}

// replaceRef replaces a table reference with a subquery, keeping its
// alias, or the table's name as one.
func (rw *rewriter) replaceRef(text string, ref *refNode, sp tableRefSpan, subquery string) (*edit, error) {
	alias := sp.alias
	if alias == "" {
		alias = quoteIdent(ref.parts[len(ref.parts)-1])
	}
	repl := subquery + " AS " + alias
	start := ref.loc
	switch word, at := prevWord(text, ref.loc); {
	case ref.summarized:
		// SUMMARIZE name: SUMMARIZE takes a query.
		repl = "SELECT * FROM " + repl
	case word == "TABLE":
		// TABLE name is SELECT * FROM name.
		start, repl = at, "(SELECT * FROM "+repl+")"
	}
	return &edit{start, sp.end, repl}, nil
}

// inline returns the body of view o, rewritten, as a subquery, and the
// view's column names if it has any.
func (rw *rewriter) inline(o object, depth int) ([]string, string, error) {
	cols, body, ok := viewParts(o.sql)
	if !ok {
		return nil, "", denied("cannot analyze view %s.%s", o.schema, o.name)
	}
	edits, err := rw.queryEdits(body, &o, depth+1)
	if err != nil {
		return nil, "", err
	}
	body, err = applyEdits(body, edits)
	if err != nil {
		return nil, "", denied("cannot expand view %s.%s: %v", o.schema, o.name, err)
	}
	return cols, "(" + body + ")", nil
}

// checkView checks the body of a view that reads files or calls table
// functions, for a user who may not read files outside the allowed
// locations, without expanding it.
func (rw *rewriter) checkView(o object, depth int) error {
	if depth > maxViewDepth {
		return denied("views nest more than %d deep", maxViewDepth)
	}
	_, body, ok := viewParts(o.sql)
	if !ok {
		return denied("cannot analyze view %s.%s", o.schema, o.name)
	}
	edits, err := rw.queryEdits(body, &o, depth+1)
	if err == nil && slices.ContainsFunc(edits, func(e edit) bool { return strings.Contains(e.text, rw.prefix("t")) }) {
		// It reads a protected table after all.
		err = denied("cannot tell what view %s.%s reads", o.schema, o.name)
	}
	return err
}

// checkFunctions refuses calls that would read what policies cannot see:
// macros that read protected tables, and for users other than admins,
// macros and table functions that read files or tables by name.
func (rw *rewriter) checkFunctions(ps *parseScan) error {
	if rw.filesOnly {
		if rw.admin {
			return nil
		}
		for _, f := range ps.functions {
			if f.tableValue {
				if err := rw.checkFileFunction(f); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for _, f := range ps.functions {
		name := strings.ToLower(f.name)
		switch {
		case rw.sens.macroProtected[name]:
			return denied("macro %s reads tables with policies, which policies cannot apply to inside a macro; query the tables instead", f.name)
		case name == "stats" && !f.tableValue:
			// The statistics of a masked or guarded column describe the
			// values behind it: their minimum and maximum.
			return denied("stats() shows statistics of values that policies hide")
		case rw.admin:
		case rw.sens.macroRisky[name]:
			return denied("macro %s reads files or tables by name, which policies cannot check", f.name)
		case f.tableValue:
			if err := rw.checkTableFunction(f); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkTableFunction allows a table function while policies exist: one
// that reads no table data, or one that reads files only from the allowed
// locations, outside the warehouse.
func (rw *rewriter) checkTableFunction(f *fnNode) error {
	name := strings.ToLower(f.name)
	switch {
	case f.schema != "" && !strings.EqualFold(f.schema, "main"):
		return denied("%s.%s reads data that policies cannot check", f.schema, f.name)
	case name == "iceberg_snapshots" || name == "iceberg_metadata":
		// They list a table's snapshots and files, and count its rows,
		// hidden ones too.
		if len(f.args) == 0 {
			return denied("%s takes a table", f.name)
		}
		arg, _ := f.args[0].(map[string]any)
		var parts []string
		for _, p := range asList(arg["column_names"]) {
			parts = append(parts, str(p))
		}
		if v, ok := constantStrings(f.args[0]); ok && len(v) == 1 {
			parts = strings.Split(v[0], ".")
		}
		if len(parts) == 0 || pathLike(parts[len(parts)-1]) || strings.Contains(parts[0], "/") ||
			rw.snap.ProtectedNames()[strings.ToLower(strings.Trim(parts[len(parts)-1], `"`))] {
			return denied("%s may count the rows of a table with policies, hidden ones too", f.name)
		}
		return nil
	case safeTableFunctions[name]:
		return nil
	case fileFunctions[name]:
		if len(f.args) == 0 {
			return denied("%s needs a file name", f.name)
		}
		paths, ok := constantStrings(f.args[0])
		if !ok {
			return denied("%s may read the warehouse's files directly while tables have policies; name the files as string literals", f.name)
		}
		for _, p := range paths {
			if err := rw.s.checkPath(p, rw.allowWarehouse); err != nil {
				return err
			}
		}
		return nil
	}
	return denied("%s may read data that policies cannot check, which is refused while tables have policies", f.name)
}

// extraFileFunctions read the file their first argument names, like
// fileFunctions, but are refused outright while a table has a policy (they
// are not in safeTableFunctions). Without policies, a non-admin may call
// them, naming the file as a plain string literal in an allowed location.
// iceberg_metadata, iceberg_snapshots, iceberg_scan and read_duckdb may
// instead name a catalog table or database, which is not a file.
var extraFileFunctions = map[string]bool{
	"iceberg_scan": true, "iceberg_metadata": true, "iceberg_snapshots": true,
	"delta_scan": true, "read_xlsx": true, "read_duckdb": true,
	"st_readshp": true, "st_readosm": true, "shapefile_meta": true,
}

// tableArgFunctions may take a catalog table or database (a plain
// identifier) as their first argument instead of a file path.
var tableArgFunctions = map[string]bool{
	"iceberg_scan": true, "iceberg_metadata": true, "iceberg_snapshots": true, "read_duckdb": true,
}

// checkFileFunction refuses a file-reading table function, for a non-admin,
// that names its file any way but as a plain string literal (or a list of
// them) that checkPath allows. It is the files-only counterpart of
// checkTableFunction, used whether or not a table has a policy.
func (rw *rewriter) checkFileFunction(f *fnNode) error {
	name := strings.ToLower(f.name)
	if f.schema != "" && !strings.EqualFold(f.schema, "main") {
		return nil
	}
	if !fileFunctions[name] && !extraFileFunctions[name] {
		return nil
	}
	if len(f.args) == 0 {
		return nil
	}
	if paths, ok := constantStrings(f.args[0]); ok {
		for _, p := range paths {
			if err := rw.s.checkPath(p, rw.allowWarehouse); err != nil {
				return err
			}
		}
		return nil
	}
	if tableArgFunctions[name] {
		if n, _ := f.args[0].(map[string]any); str(n["class"]) == "COLUMN_REF" {
			// A catalog table or database, not a file.
			return nil
		}
	}
	return denied("%s needs its file as a plain string literal, not an expression built as the statement runs", f.name)
}

// constantStrings returns the strings a table function's argument names:
// a string literal, or a list of them.
func constantStrings(arg any) ([]string, bool) {
	n, _ := arg.(map[string]any)
	switch str(n["class"]) {
	case "CONSTANT":
		v, _ := n["value"].(map[string]any)
		s, ok := v["value"].(string)
		return []string{s}, ok
	case "FUNCTION":
		if str(n["function_name"]) != "list_value" {
			return nil, false
		}
		var out []string
		for _, c := range asList(n["children"]) {
			s, ok := constantStrings(c)
			if !ok {
				return nil, false
			}
			out = append(out, s...)
		}
		return out, len(out) > 0
	}
	return nil, false
}

// viewParts splits a view's definition, as DuckDB prints it, into its
// column names, if it has any, and its query.
func viewParts(sql string) ([]string, string, bool) {
	sc := &scanner{s: sql}
	if !sc.keywords("CREATE") {
		return nil, "", false
	}
	sc.keywords("OR", "REPLACE")
	_ = sc.keywords("TEMP") || sc.keywords("TEMPORARY")
	if !sc.keywords("VIEW") {
		return nil, "", false
	}
	sc.keywords("IF", "NOT", "EXISTS")
	if _, ok := sc.name(); !ok {
		return nil, "", false
	}
	var cols []string
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
		sc.i++
		for {
			col, ok := sc.name()
			if !ok || len(col) != 1 {
				return nil, "", false
			}
			cols = append(cols, col[0])
			sc.skip()
			if sc.i < len(sc.s) && sc.s[sc.i] == ',' {
				sc.i++
				continue
			}
			if sc.i < len(sc.s) && sc.s[sc.i] == ')' {
				sc.i++
				break
			}
			return nil, "", false
		}
	}
	if !sc.keywords("AS") {
		return nil, "", false
	}
	body := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sc.rest()), ";"))
	return cols, body, body != ""
}

// --- statements ---------------------------------------------------------------

// statementEdits returns the edits that make query, a statement of type t
// that names something sensitive, read protected tables through their
// policies. Statements it cannot rewrite with certainty are refused.
func (rw *rewriter) statementEdits(t duckdb.StmtType, query string) ([]edit, error) {
	switch t {
	case duckdb.STATEMENT_TYPE_SELECT, duckdb.STATEMENT_TYPE_RELATION:
		return rw.queryEdits(query, nil, 0)

	case duckdb.STATEMENT_TYPE_EXPLAIN:
		inner, analyze := explainSpan(query)
		if inner.empty() {
			return nil, denied("cannot apply policies to this EXPLAIN")
		}
		text := query[inner.start:inner.end]
		if !selectLike(text) {
			if analyze {
				return nil, denied("EXPLAIN ANALYZE runs the statement, which policies can be applied to only when it is a query")
			}
			return nil, nil
		}
		return rw.partEdits(query, inner, "")

	case duckdb.STATEMENT_TYPE_INSERT:
		return rw.insertEdits(query)

	case duckdb.STATEMENT_TYPE_UPDATE, duckdb.STATEMENT_TYPE_DELETE, statementTypeMergeInto:
		return rw.dmlEdits(t, query)

	case duckdb.STATEMENT_TYPE_CREATE:
		if rw.filesOnly {
			return nil, rw.createFileEdits(query)
		}
		c, ok := parseCreateSpans(query)
		switch {
		case ok && !c.query.empty():
			edits, err := rw.partEdits(query, c.query, "")
			if err != nil {
				return nil, err
			}
			return edits, rw.residual(query, c.name, c.query)
		case ok:
			return nil, nil
		}
		// Views, macros, schemas and the like read nothing when created;
		// a type's values may come from a query.
		sc := &scanner{s: query}
		sc.keywords("CREATE")
		sc.keywords("OR", "REPLACE")
		_ = sc.keywords("TEMP") || sc.keywords("TEMPORARY") || sc.keywords("PERSISTENT")
		switch sc.word() {
		case "VIEW", "MACRO", "FUNCTION", "SCHEMA", "SEQUENCE", "INDEX", "UNIQUE":
			return nil, nil
		}
		return nil, denied("cannot apply policies to this CREATE statement")

	case duckdb.STATEMENT_TYPE_COPY:
		return rw.copyEdits(query)

	case duckdb.STATEMENT_TYPE_SET, duckdb.STATEMENT_TYPE_VARIABLE_SET:
		expr := setVariableSpan(query)
		if expr.empty() {
			return nil, nil
		}
		edits, err := rw.partEdits(query, expr, "SELECT ")
		if err != nil {
			return nil, err
		}
		return edits, rw.residual(query, expr)

	case duckdb.STATEMENT_TYPE_DROP, duckdb.STATEMENT_TYPE_ALTER, duckdb.STATEMENT_TYPE_TRANSACTION,
		duckdb.STATEMENT_TYPE_CALL, duckdb.STATEMENT_TYPE_PRAGMA:
		// They read no rows. CALL and PRAGMA are limited in policyRules.
		return nil, nil
	}
	if rw.filesOnly {
		// The file checks do not refuse statement shapes they do not parse;
		// the residual check catches a file named where they cannot look.
		return nil, nil
	}
	return nil, denied("cannot apply policies to this statement, which names a table with policies")
}

// partEdits returns the edits for a query that is text[s] of a statement,
// read with prefix before it.
func (rw *rewriter) partEdits(text string, s span, prefix string) ([]edit, error) {
	part := prefix + text[s.start:s.end]
	edits, err := rw.queryEdits(part, nil, 0)
	if err != nil {
		return nil, err
	}
	for _, e := range edits {
		if e.start < len(prefix) {
			return nil, denied("cannot apply policies to this statement")
		}
	}
	return shiftEdits(edits, s.start-len(prefix)), nil
}

// residual refuses a statement whose text outside the spans that were
// analyzed names something sensitive: the rewrite would not have seen it.
func (rw *rewriter) residual(text string, analyzed ...span) error {
	b := blank(text, analyzed...)
	if rw.filesOnly {
		if toks := tokensOf(b); toks.paths || toks.opaque {
			return denied("this statement names a file where Igloo cannot check it; name files as plain string literals")
		}
		return nil
	}
	if rw.sens.triggers(b, rw.admin) {
		return denied("cannot apply policies to this statement: part of it names a table with policies where Igloo cannot rewrite it")
	}
	return nil
}

// explainSpan returns the statement EXPLAIN explains, and whether it is
// EXPLAIN ANALYZE.
func explainSpan(query string) (span, bool) {
	sc := &scanner{s: query}
	if !sc.keywords("EXPLAIN") {
		return span{}, false
	}
	analyze := sc.keywords("ANALYZE") || sc.keywords("ANALYSE")
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
		end := matchParen(sc.s, sc.i)
		if end < 0 {
			return span{}, analyze
		}
		if slices.Contains(sqllex.CodeWords(sc.s[sc.i:end]), "ANALYZE") {
			analyze = true
		}
		sc.i = end + 1
	}
	return trimSpan(query, span{sc.i, len(query)}), analyze
}

// setVariableSpan returns the expression of SET VARIABLE x = expr.
func setVariableSpan(query string) span {
	sc := &scanner{s: query}
	if !sc.keywords("SET", "VARIABLE") && !sc.keywords("SET", "SESSION", "VARIABLE") {
		return span{}
	}
	if _, ok := sc.name(); !ok {
		return span{}
	}
	sc.skip()
	switch {
	case strings.HasPrefix(sc.rest(), "="):
		sc.i++
	case sc.keywords("TO"):
	default:
		return span{}
	}
	return trimSpan(query, span{sc.i, len(query)})
}

// createSpans are the parts of CREATE TABLE [AS query].
type createSpans struct {
	name, query span
}

func parseCreateSpans(query string) (createSpans, bool) {
	var c createSpans
	sc := &scanner{s: query}
	if !sc.keywords("CREATE") {
		return c, false
	}
	sc.keywords("OR", "REPLACE")
	_ = sc.keywords("TEMP") || sc.keywords("TEMPORARY") || sc.keywords("PERSISTENT")
	if !sc.keywords("TABLE") {
		return c, false
	}
	sc.keywords("IF", "NOT", "EXISTS")
	sc.skip()
	start := sc.i
	if _, ok := sc.name(); !ok {
		return c, false
	}
	c.name = span{start, sc.i}
	rest := span{sc.i, len(query)}
	if i := keywordAt(query, rest, "AS"); i >= 0 {
		q := trimSpan(query, span{i + 2, len(query)})
		if j := keywordAt(query, q, "WITH", "NO", "DATA"); j >= 0 {
			q.end = j
		} else if j := keywordAt(query, q, "WITH", "DATA"); j >= 0 {
			q.end = j
		}
		c.query = trimSpan(query, q)
	}
	return c, true
}

// --- INSERT ------------------------------------------------------------------

func (rw *rewriter) insertEdits(query string) ([]edit, error) {
	sc := &scanner{s: query}
	if !sc.keywords("INSERT") {
		return nil, denied("cannot apply policies to this INSERT")
	}
	replaces := sc.keywords("OR", "REPLACE") || sc.keywords("OR", "IGNORE")
	if !sc.keywords("INTO") {
		return nil, denied("cannot apply policies to this INSERT")
	}
	sc.skip()
	start := sc.i
	target, ok := sc.name()
	if !ok {
		return nil, denied("cannot apply policies to this INSERT")
	}
	name := span{start, sc.i}
	if sc.keywords("AS") {
		sc.name()
	}
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
		end := matchParen(sc.s, sc.i)
		if end > 0 && !selectLike(sc.s[sc.i+1:end]) {
			sc.i = end + 1
		}
	}
	_ = sc.keywords("BY", "NAME") || sc.keywords("BY", "POSITION")
	var source span
	if !sc.keywords("DEFAULT", "VALUES") {
		source = span{sc.i, len(query)}
		for _, end := range [][]string{{"ON", "CONFLICT"}, {"RETURNING"}} {
			if i := keywordAt(query, source, end...); i >= 0 {
				source.end = i
			}
		}
		source = trimSpan(query, source)
	}
	upsert := replaces || keywordAt(query, span{name.end, len(query)}, "ON", "CONFLICT") >= 0
	if upsert {
		if o, ok, err := rw.target(target); err != nil {
			return nil, err
		} else if ok {
			plan, err := rw.plan(o, rw.snap.Rules(o.schema, o.name))
			if err != nil {
				return nil, err
			}
			if !plan.noop() {
				return nil, denied("INSERT OR REPLACE and ON CONFLICT read the rows of %s.%s, which has policies", o.schema, o.name)
			}
		}
	}
	var edits []edit
	if !source.empty() {
		var err error
		if edits, err = rw.partEdits(query, source, ""); err != nil {
			return nil, err
		}
	}
	return edits, rw.residual(query, name, source)
}

// target resolves the table a statement writes, reporting whether it is a
// protected catalog table.
func (rw *rewriter) target(name []string) (object, bool, error) {
	res, err := rw.r.resolve(tableRef{parts: name}, nil)
	if err != nil {
		return object{}, false, err
	}
	if len(res.first) != 1 {
		if slices.ContainsFunc(res.all, rw.sensitive) {
			return object{}, false, denied("%s is ambiguous; qualify it", strings.Join(name, "."))
		}
		return object{}, false, nil
	}
	o := res.first[0]
	for _, other := range res.all {
		if other != o && rw.sensitive(other) {
			return object{}, false, denied("%s is ambiguous; qualify it", strings.Join(name, "."))
		}
	}
	if o.view || !strings.EqualFold(o.db, rw.r.catalog) || rw.snap.Rules(o.schema, o.name) == nil {
		return o, false, nil
	}
	rw.touched = true
	return o, true, nil
}

// --- COPY --------------------------------------------------------------------

func (rw *rewriter) copyEdits(query string) ([]edit, error) {
	sc := &scanner{s: query}
	if !sc.keywords("COPY") {
		return nil, denied("cannot apply policies to this COPY")
	}
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
		// COPY (query) TO 'file': the query reads, the file is written.
		end := matchParen(sc.s, sc.i)
		if end < 0 {
			return nil, denied("cannot apply policies to this COPY")
		}
		inner := trimSpan(query, span{sc.i + 1, end})
		edits, err := rw.partEdits(query, inner, "")
		if err != nil {
			return nil, err
		}
		sc.i = end + 1
		if !sc.keywords("TO") {
			return nil, denied("cannot apply policies to this COPY")
		}
		toStart := sc.i
		if err := rw.copyTo(sc); err != nil {
			return nil, err
		}
		return edits, rw.residual(query, inner, span{toStart, sc.i})
	}
	start := sc.i
	if _, ok := sc.name(); !ok {
		return nil, denied("cannot apply policies to this COPY")
	}
	nameEnd, end := sc.i, sc.i
	sc.skip()
	cols := "*"
	if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
		close := matchParen(sc.s, sc.i)
		if close < 0 {
			return nil, denied("cannot apply policies to this COPY")
		}
		cols, end = sc.s[sc.i+1:close], close+1
		sc.i = close + 1
	}
	switch {
	case sc.keywords("FROM"):
		// Writes the table. Others than admins may read only files they are
		// allowed, as plain string literals.
		sc.skip()
		lit := sc.i
		path, ok := sc.literal()
		if !rw.admin {
			if !ok || path == nil {
				return nil, denied("COPY FROM needs a file name as a plain string literal")
			}
			if err := rw.s.checkPath(*path, rw.allowWarehouse); err != nil {
				return nil, err
			}
		}
		return nil, rw.residual(query, span{start, end}, span{lit, sc.i})
	case sc.keywords("TO"):
		toStart := sc.i
		if err := rw.copyTo(sc); err != nil {
			return nil, err
		}
		if rw.filesOnly {
			// The source table is read, but with no policies there is
			// nothing to mask; checking the destination is enough.
			synth := "SELECT " + cols + " FROM " + query[start:nameEnd]
			if _, err := rw.queryEdits(synth, nil, 0); err != nil {
				return nil, err
			}
			return nil, rw.residual(query, span{start, end}, span{toStart, sc.i})
		}
		synth := "SELECT " + cols + " FROM " + query[start:nameEnd]
		edits, err := rw.queryEdits(synth, nil, 0)
		if err != nil {
			return nil, err
		}
		if len(edits) == 0 {
			return nil, rw.residual(query, span{start, end}, span{toStart, sc.i})
		}
		rewritten, err := applyEdits(synth, edits)
		if err != nil {
			return nil, denied("cannot apply policies to this COPY: %v", err)
		}
		return []edit{{start, end, "(" + rewritten + ")"}}, rw.residual(query, span{start, end}, span{toStart, sc.i})
	}
	return nil, denied("cannot apply policies to this COPY")
}

// copyTo checks the destination of COPY ... TO, with sc positioned just
// after the TO keyword. A user other than an admin may write only to a
// plain string path they are allowed, so the destination cannot be an
// expression that builds a path at run time, nor climb out of a prefix.
func (rw *rewriter) copyTo(sc *scanner) error {
	sc.skip()
	if rw.admin {
		return nil
	}
	path, ok := sc.literal()
	if !ok || path == nil {
		return denied("COPY ... TO needs a file name as a plain string literal")
	}
	return rw.s.checkPath(*path, rw.allowWarehouse)
}

// --- UPDATE, DELETE, MERGE ---------------------------------------------------

// dmlParts are the parts of UPDATE, DELETE and MERGE.
type dmlParts struct {
	// target is the table written, and alias its alias as written
	// (without AS), if any; targetEnd is where both end.
	target    span
	alias     string
	targetEnd int
	// list is DELETE's USING or UPDATE's FROM; source is MERGE's.
	list, source span
	// cond is WHERE's or MERGE's ON condition.
	cond      span
	where     bool
	returning span
	// exprs are expressions over the target row: UPDATE's assignments and
	// MERGE's WHEN conditions and UPDATE SET values.
	exprs []span
	// sourceExprs are MERGE's INSERT values, which read only the source.
	sourceExprs []span
	// bySource says MERGE has WHEN NOT MATCHED BY SOURCE, notMatched that
	// it has WHEN NOT MATCHED [BY TARGET].
	bySource, notMatched bool
	truncate             bool
	// insertAt is where WHERE goes in a DELETE or UPDATE without one.
	insertAt int
}

// stop words that end a target's alias.
var aliasStops = map[string]bool{"USING": true, "WHERE": true, "RETURNING": true, "SET": true, "FROM": true, "ON": true}

func parseDML(t duckdb.StmtType, query string) (dmlParts, bool) {
	var d dmlParts
	end := trimSpan(query, span{0, len(query)}).end
	sc := &scanner{s: query}
	switch t {
	case duckdb.STATEMENT_TYPE_DELETE:
		if sc.keywords("TRUNCATE") {
			sc.keywords("TABLE")
			d.truncate = true
		} else if !sc.keywords("DELETE", "FROM") {
			return d, false
		}
	case duckdb.STATEMENT_TYPE_UPDATE:
		if !sc.keywords("UPDATE") {
			return d, false
		}
	case statementTypeMergeInto:
		if !sc.keywords("MERGE", "INTO") {
			return d, false
		}
	default:
		return d, false
	}
	sc.skip()
	start := sc.i
	if _, ok := sc.name(); !ok {
		return d, false
	}
	d.target = span{start, sc.i}
	d.targetEnd = sc.i
	if d.truncate {
		return d, trimSpan(query, span{sc.i, end}).empty()
	}
	// An alias: AS name, or a name that is not the next clause's keyword.
	save := sc.i
	hasAS := sc.keywords("AS")
	sc.skip()
	aliasStart := sc.i
	if w := sc.word(); hasAS || (w != "" && !aliasStops[w]) || (w == "" && sc.i < len(sc.s) && sc.s[sc.i] == '"') {
		if a, ok := sc.name(); ok && len(a) == 1 {
			d.alias, d.targetEnd = query[aliasStart:sc.i], sc.i
		} else {
			return d, false
		}
	} else {
		sc.i = save
	}
	rest := span{d.targetEnd, end}

	switch t {
	case duckdb.STATEMENT_TYPE_DELETE:
		ret := keywordAt(query, rest, "RETURNING")
		body := rest
		if ret >= 0 {
			d.returning, body.end = trimSpan(query, span{afterKeywords(query, ret, "RETURNING"), end}), ret
		}
		d.insertAt = body.end
		where := keywordAt(query, body, "WHERE")
		using := keywordAt(query, body, "USING")
		if using >= 0 {
			listEnd := body.end
			if where >= 0 {
				listEnd = where
			}
			d.list = trimSpan(query, span{afterKeywords(query, using, "USING"), listEnd})
		}
		if where >= 0 {
			d.cond, d.where = trimSpan(query, span{afterKeywords(query, where, "WHERE"), body.end}), true
		}
		if !trimSpan(query, span{d.targetEnd, firstOf(using, where, body.end)}).empty() {
			return d, false
		}

	case duckdb.STATEMENT_TYPE_UPDATE:
		set := keywordAt(query, rest, "SET")
		if set < 0 || !trimSpan(query, span{d.targetEnd, set}).empty() {
			return d, false
		}
		body := span{afterKeywords(query, set, "SET"), end}
		if ret := keywordAt(query, body, "RETURNING"); ret >= 0 {
			d.returning, body.end = trimSpan(query, span{afterKeywords(query, ret, "RETURNING"), end}), ret
		}
		d.insertAt = body.end
		assignEnd := body.end
		if where := keywordAt(query, body, "WHERE"); where >= 0 {
			d.cond, d.where = trimSpan(query, span{afterKeywords(query, where, "WHERE"), body.end}), true
			assignEnd = where
		}
		if from := keywordAt(query, span{body.start, assignEnd}, "FROM"); from >= 0 {
			d.list = trimSpan(query, span{afterKeywords(query, from, "FROM"), assignEnd})
			assignEnd = from
		}
		exprs, ok := assignmentValues(query, span{body.start, assignEnd})
		if !ok {
			return d, false
		}
		d.exprs = exprs

	case statementTypeMergeInto:
		using := keywordAt(query, rest, "USING")
		if using < 0 || !trimSpan(query, span{d.targetEnd, using}).empty() {
			return d, false
		}
		afterUsing := span{afterKeywords(query, using, "USING"), end}
		on := keywordAt(query, afterUsing, "ON")
		if on < 0 {
			return d, false
		}
		d.source = trimSpan(query, span{afterUsing.start, on})
		clauses := whenClauses(query, span{afterKeywords(query, on, "ON"), end})
		if len(clauses) == 0 {
			return d, false
		}
		d.cond = trimSpan(query, span{afterKeywords(query, on, "ON"), clauses[0]})
		for i, at := range clauses {
			cEnd := end
			if i+1 < len(clauses) {
				cEnd = clauses[i+1]
			} else if ret := keywordAt(query, span{at, end}, "RETURNING"); ret >= 0 {
				d.returning, cEnd = trimSpan(query, span{afterKeywords(query, ret, "RETURNING"), end}), ret
			}
			if !d.mergeClause(query, span{at, cEnd}) {
				return d, false
			}
		}
	}
	return d, true
}

func firstOf(positions ...int) int {
	for _, p := range positions {
		if p >= 0 {
			return p
		}
	}
	return -1
}

// assignmentValues returns the values of a = x, b = y, ....
func assignmentValues(query string, s span) ([]span, bool) {
	var out []span
	pos := s.start
	for _, part := range splitTopLevel(query[s.start:s.end], ',') {
		eq := topLevelIndex(part, '=')
		if eq < 0 {
			return nil, false
		}
		v := trimSpan(query, span{pos + eq + 1, pos + len(part)})
		if v.empty() {
			return nil, false
		}
		out = append(out, v)
		pos += len(part) + 1
	}
	return out, len(out) > 0
}

// whenClauses returns where MERGE's WHEN [NOT] MATCHED clauses start.
func whenClauses(query string, s span) []int {
	var out []int
	for pos := s.start; pos < s.end; {
		a := keywordAt(query, span{pos, s.end}, "WHEN", "MATCHED")
		b := keywordAt(query, span{pos, s.end}, "WHEN", "NOT", "MATCHED")
		next := a
		if next < 0 || b >= 0 && b < next {
			next = b
		}
		if next < 0 {
			break
		}
		out = append(out, next)
		pos = next + 4
	}
	return out
}

// mergeClause reads one WHEN clause of MERGE.
func (d *dmlParts) mergeClause(query string, s span) bool {
	sc := &scanner{s: query[:s.end], i: s.start}
	if !sc.keywords("WHEN") {
		return false
	}
	matched := !sc.keywords("NOT")
	if !sc.keywords("MATCHED") {
		return false
	}
	bySource := false
	if !matched {
		switch {
		case sc.keywords("BY", "SOURCE"):
			bySource, d.bySource = true, true
		default:
			sc.keywords("BY", "TARGET")
			d.notMatched = true
		}
	}
	// The clause sees the target row when it matched, or when it is the
	// target's row that the source did not match.
	target := matched || bySource
	rest := span{sc.i, s.end}
	then := keywordAtOutsideCase(query, rest, "THEN")
	if then < 0 {
		return false
	}
	if cond := trimSpan(query, span{rest.start, then}); !cond.empty() {
		k := &scanner{s: query[:then], i: cond.start}
		if !k.keywords("AND") {
			return false
		}
		c := trimSpan(query, span{k.i, then})
		if target {
			d.exprs = append(d.exprs, c)
		} else {
			d.sourceExprs = append(d.sourceExprs, c)
		}
	}
	action := trimSpan(query, span{afterKeywords(query, then, "THEN"), s.end})
	ac := &scanner{s: query[:action.end], i: action.start}
	switch {
	case ac.keywords("UPDATE", "SET"):
		vals, ok := assignmentValues(query, span{ac.i, action.end})
		if !ok {
			if trimSpan(query, span{ac.i, action.end}).empty() {
				return false
			}
			// UPDATE SET * takes every column from the source.
			if strings.TrimSpace(query[ac.i:action.end]) != "*" {
				return false
			}
		}
		d.exprs = append(d.exprs, vals...)
	case ac.keywords("UPDATE"), ac.keywords("DELETE"), ac.keywords("DO", "NOTHING"):
		ac.keywords("BY", "NAME")
		ac.keywords("BY", "POSITION")
		return ac.done()
	case ac.keywords("INSERT"):
		if !target {
			rest := span{ac.i, action.end}
			if v := keywordAt(query, rest, "VALUES"); v >= 0 {
				k := &scanner{s: query[:action.end], i: afterKeywords(query, v, "VALUES")}
				k.skip()
				if k.i >= action.end || query[k.i] != '(' {
					return false
				}
				close := matchParen(query, k.i)
				if close < 0 || close >= action.end {
					return false
				}
				pos := k.i + 1
				for _, part := range splitTopLevel(query[k.i+1:close], ',') {
					d.sourceExprs = append(d.sourceExprs, trimSpan(query, span{pos, pos + len(part)}))
					pos += len(part) + 1
				}
			}
			return true
		}
		return false
	case ac.keywords("ERROR"):
		if e := trimSpan(query, span{ac.i, action.end}); !e.empty() {
			d.exprs = append(d.exprs, e)
		}
	default:
		return false
	}
	return true
}

// keywordAtOutsideCase is keywordAt that skips CASE ... END.
func keywordAtOutsideCase(query string, s span, word string) int {
	depth := 0
	for pos := s.start; pos < s.end; {
		next := keywordAt(query, span{pos, s.end}, word)
		c := keywordAt(query, span{pos, s.end}, "CASE")
		e := keywordAt(query, span{pos, s.end}, "END")
		switch {
		case c >= 0 && (next < 0 || c < next) && (e < 0 || c < e):
			depth++
			pos = c + 4
		case e >= 0 && depth > 0 && (next < 0 || e < next):
			depth--
			pos = e + 3
		case next >= 0 && depth == 0:
			return next
		case next >= 0:
			pos = next + len(word)
		default:
			return -1
		}
	}
	return -1
}

// composeDML returns what UPDATE, DELETE or MERGE reads, as one query:
// SELECT <expressions> FROM <target>, <list> | JOIN <source> ON <cond>
// WHERE <cond>.
func composeDML(query string, d dmlParts) *composer {
	c := &composer{}
	c.lit("SELECT ")
	first := true
	for _, e := range slices.Concat(d.exprs, d.sourceExprs, []span{d.returning}) {
		if e.empty() {
			continue
		}
		if !first {
			c.lit(", ")
		}
		c.orig(query, e.start, e.end)
		first = false
	}
	if first {
		c.lit("1")
	}
	c.lit(" FROM ")
	c.orig(query, d.target.start, d.targetEnd)
	switch {
	case !d.list.empty():
		c.lit(", ")
		c.orig(query, d.list.start, d.list.end)
	case !d.source.empty():
		c.lit(" JOIN ")
		c.orig(query, d.source.start, d.source.end)
		c.lit(" ON ")
		c.orig(query, d.cond.start, d.cond.end)
	}
	if d.where {
		c.lit(" WHERE ")
		c.orig(query, d.cond.start, d.cond.end)
	}
	return c
}

// dmlEdits rewrites UPDATE, DELETE and MERGE: the tables they read, like a
// query's, and their target when it has policies (see targetEdits).
func (rw *rewriter) dmlEdits(t duckdb.StmtType, query string) ([]edit, error) {
	d, ok := parseDML(t, query)
	if !ok {
		return nil, denied("cannot apply policies to this statement; write it without a WITH clause, and with its clauses in the usual order")
	}
	name, _ := scanName(query, d.target.start)
	o, protected, err := rw.target(name.parts)
	if err != nil {
		return nil, err
	}
	var plan *tablePlan
	// A target with an alias of this rewrite's was rewritten already.
	ours := strings.HasPrefix(strings.Trim(d.alias, `"`), rw.prefix("g"))
	if protected && !ours {
		if plan, err = rw.plan(o, rw.snap.Rules(o.schema, o.name)); err != nil {
			return nil, err
		}
		if plan.noop() {
			plan = nil
		}
	}
	if d.truncate {
		if plan != nil && plan.filter != nil {
			return nil, denied("TRUNCATE would delete rows of %s.%s that its row access policy hides; use DELETE", o.schema, o.name)
		}
		return nil, rw.residual(query, d.target)
	}

	c := composeDML(query, d)
	ast, err := rw.r.serialize(c.String())
	if err != nil {
		return nil, denied("cannot apply policies to this statement: %v", err)
	}
	edits, err := rw.parseEdits(query, scanParse(ast), nil, 0, c.toOrig, func(ref *refNode) bool {
		at, ok := c.toOrig(ref.loc)
		return ok && at == d.target.start
	})
	if err != nil {
		return nil, err
	}
	analyzed := slices.Concat([]span{d.target, {d.target.end, d.targetEnd}, d.list, d.source, d.cond, d.returning}, d.exprs, d.sourceExprs)
	if err := rw.residual(query, analyzed...); err != nil {
		return nil, err
	}
	if plan == nil {
		return edits, nil
	}
	return rw.targetEdits(query, d, name, plan, edits)
}

// targetEdits rewrites the target of UPDATE, DELETE or MERGE, which has
// policies the user is not exempt from. The target keeps its name but gets
// an alias of Igloo's; every condition and expression of the statement
// that reads the target row reads instead a one-row subquery under the
// target's own alias, which holds the row's values as the user sees them:
// masked, and NULL when the row access policy hides the row. The row
// access policy is added to WHERE (and MERGE's ON), so hidden rows are
// neither changed nor matched.
func (rw *rewriter) targetEdits(query string, d dmlParts, name nameSpan, plan *tablePlan, inner []edit) ([]edit, error) {
	if !d.returning.empty() && slices.ContainsFunc(plan.masks, func(m *maskPlan) bool { return m != nil }) {
		return nil, denied("RETURNING would show masked columns of %s.%s unmasked", plan.obj.schema, plan.obj.name)
	}
	if d.bySource && plan.filter != nil {
		return nil, denied("WHEN NOT MATCHED BY SOURCE would change rows of %s.%s that its row access policy hides", plan.obj.schema, plan.obj.name)
	}
	if d.notMatched {
		// The condition becomes a subquery, which DuckDB cannot join on
		// in an outer join.
		return nil, denied("MERGE into %s.%s, which has policies for you, takes only WHEN MATCHED clauses; add new rows with INSERT",
			plan.obj.schema, plan.obj.name)
	}
	g := quoteIdent(rw.alias("g"))
	visible := d.alias
	if visible == "" {
		visible = quoteIdent(name.parts[len(name.parts)-1])
	}

	// The row as the user sees it.
	items := make([]string, len(plan.columns))
	var row string
	if plan.filter != nil {
		r, ok := quoteIdent(rw.alias("r")), rw.okColumn()
		for i, col := range plan.columns {
			items[i] = "CASE WHEN " + r + "." + ok + " THEN " + rw.value(plan, i, g+"."+quoteIdent(col.name)) + " END AS " + quoteIdent(col.name)
		}
		row = "(SELECT " + strings.Join(items, ", ") + " FROM (SELECT (" + rw.filterExpr(plan, g) + ") AS " + ok + ") AS " + r + ") AS " + visible
	} else {
		for i, col := range plan.columns {
			items[i] = rw.value(plan, i, g+"."+quoteIdent(col.name)) + " AS " + quoteIdent(col.name)
		}
		row = "(SELECT " + strings.Join(items, ", ") + ") AS " + visible
	}
	guarded := func(text string) string { return "(SELECT (" + text + ") FROM " + row + ")" }

	edits := slices.Clone(inner)
	// wrap replaces s with what f makes of its text, edits inside it
	// applied.
	wrap := func(s span, f func(string) string) error {
		var within, outside []edit
		for _, e := range edits {
			if e.start >= s.start && e.end <= s.end {
				within = append(within, edit{e.start - s.start, e.end - s.start, e.text})
			} else {
				outside = append(outside, e)
			}
		}
		text, err := applyEdits(query[s.start:s.end], within)
		if err != nil {
			return err
		}
		edits = append(outside, edit{s.start, s.end, f(text)})
		return nil
	}

	edits = append(edits, edit{d.target.start, d.targetEnd, quoteName([]string{plan.obj.db, plan.obj.schema, plan.obj.name}) + " AS " + g})
	var filter string
	if plan.filter != nil {
		filter = "(" + rw.filterExpr(plan, g) + ")"
	}
	cond := func(text string) string {
		if filter == "" {
			return guarded(text)
		}
		return filter + " AND " + guarded(text)
	}
	for _, e := range d.exprs {
		if constantExpr(query[e.start:e.end]) {
			continue
		}
		if err := wrap(e, guarded); err != nil {
			return nil, denied("cannot apply policies to this statement: %v", err)
		}
	}
	switch {
	case !d.cond.empty():
		if err := wrap(d.cond, cond); err != nil {
			return nil, denied("cannot apply policies to this statement: %v", err)
		}
	case filter != "":
		edits = append(edits, edit{d.insertAt, d.insertAt, " WHERE " + filter + " "})
	}
	return edits, nil
}

// constantExpr reports whether an expression is made of literals and
// parameters only, so it reads nothing of the row.
func constantExpr(text string) bool {
	for _, t := range sqlTokens(text) {
		switch {
		case t.punct != 0:
			// A string literal, a sign, a decimal point or parentheses.
			if strings.IndexByte("'-+.()", t.punct) < 0 {
				return false
			}
		case t.keyword == "NULL", t.keyword == "TRUE", t.keyword == "FALSE", t.keyword == "DEFAULT":
		case t.keyword != "" && strings.Trim(t.keyword, "0123456789") == "":
			// A number.
		default:
			return false
		}
	}
	return true
}
