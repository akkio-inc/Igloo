package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// What a statement read and wrote goes into the query history, which the
// lineage and the usage counts are worked out from. For users with the
// role none it is what their grants were checked for (see access.go). For
// everyone else it comes from DuckDB's parse of the statement, with names
// resolved and views expanded as for grants, but unchecked: it never
// refuses a statement, and reads it cannot see, through macros or table
// functions that read tables by name, are not listed. Files read directly,
// with COPY or a function such as read_parquet, are listed by location.
//
// So that no statement waits for it, the analysis runs in the background,
// after the statement, against a snapshot of the catalog's objects and the
// session's search path as it was; the history record waits for it.

const (
	// analysisQueue bounds the statements waiting for analysis. Beyond
	// it, statements are recorded without what they read and wrote.
	analysisQueue = 10_000
	// snapshotTTL is how old the snapshot of the catalog's objects may be,
	// for objects other replicas create; this replica's DDL refreshes it.
	snapshotTTL = 30 * time.Second
)

// statementObjects is what a statement reads and writes.
type statementObjects struct {
	read, written []auth.Object
	// locations are files and locations outside the catalog that it
	// reads, as named.
	locations []string
}

// sessionNames is what decides what the names in a session's statements
// mean: its search path, current schema and temporary objects.
type sessionNames struct {
	path       searchPath
	db, schema string
	temp       map[string]bool
	// settingGen and ddl are the session's settingGen and ddlCount when
	// they were read.
	settingGen, ddl uint64
}

// setupTempViews are the temporary views every session starts with (see
// sessionCatalogSetup).
var setupTempViews = func() map[string]bool {
	m := map[string]bool{}
	for _, stmt := range sessionCatalogSetup("", false, "") {
		if g := tempView.FindStringSubmatch(stmt); g != nil {
			m[strings.ToLower(g[1])] = true
		}
	}
	return m
}()

var tempView = regexp.MustCompile(`(?i)^CREATE OR REPLACE TEMP VIEW (\w+)`)

// names returns what decides the meaning of names in the session's
// statements, reading it again after the session changed a setting or ran
// DDL, which may have made temporary objects.
func (s *Server) names(ctx context.Context, sess *session) (*sessionNames, error) {
	if n := sess.names; n != nil && n.settingGen == sess.settingGen && n.ddl == sess.ddlCount {
		return n, nil
	}
	if sess.inTx && sess.txFailed {
		return nil, errors.New("the transaction failed")
	}
	rows, err := queryRows(ctx, sess.conn, `SELECT system.main.current_setting('search_path')::VARCHAR,
		system.main.current_database(), system.main.current_schema()`)
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, errors.New("no search path")
	}
	setting, _ := rows[0][0].(string)
	n := &sessionNames{temp: setupTempViews, settingGen: sess.settingGen, ddl: sess.ddlCount}
	n.db, _ = rows[0][1].(string)
	n.schema, _ = rows[0][2].(string)
	n.path = parseSearchPath(setting, n.db)
	if sess.ddlCount > 0 {
		// duckdb_tables()/duckdb_views() list every attached database,
		// walking the catalog's in-memory set; hold the exclusive side so
		// this does not run while a table is renamed or dropped on this
		// instance. Reentrant: when this is reached while preparing a
		// statement the hold is already held (see holdCatalog); when reached
		// afterwards, recording what a statement read, it takes the hold
		// itself.
		release := s.holdCatalog(sess)
		temp, err := queryStrings(ctx, sess, `SELECT system.main.lower(table_name) FROM system.main.duckdb_tables() WHERE temporary
			UNION ALL SELECT system.main.lower(view_name) FROM system.main.duckdb_views() WHERE temporary`)
		release()
		if err != nil {
			return nil, err
		}
		n.temp = map[string]bool{}
		for _, t := range temp {
			n.temp[t] = true
		}
	}
	sess.names = n
	return n, nil
}

// analysis is a statement whose history record waits for what it read
// and wrote to be worked out.
type analysis struct {
	q *audit.Query
	// text is the statement as the session prepared it, before policies,
	// and kind is its type.
	text  string
	kind  duckdb.StmtType
	names *sessionNames
	// known is what was known already: what it wrote, and with readKnown
	// what it read.
	known     statementObjects
	readKnown bool
}

// analyzer works out what statements read and wrote, for the history.
type analyzer struct {
	s *Server
	// direct analyzes on the statement's own goroutine, for a history
	// whose readers see a statement as soon as it ends.
	direct bool
	queue  chan analysis
	done   chan struct{}

	sendMu sync.Mutex
	closed bool

	// mu guards the connections and the snapshot. conn lists the
	// catalog's objects; parser, on a DuckDB of its own, parses
	// statements, so analyzing contends with no query.
	mu     sync.Mutex
	conn   *duckdb.Conn
	parser *duckdb.Conn
	snap   *objectSnapshot
	// known are the lower-cased names of the catalog's tables and views,
	// as of the last snapshot: a query that mentions none reads none.
	known atomic.Pointer[map[string]bool]

	dropMu   sync.Mutex
	dropped  int
	lastWarn time.Time
}

// objectSnapshot is every table and view DuckDB knows of, by lower-cased
// name, and its databases' types, by lower-cased name.
type objectSnapshot struct {
	gen       uint64
	at        time.Time
	databases map[string]string
	byName    map[string][]object
}

func newAnalyzer(s *Server, direct bool) *analyzer {
	a := &analyzer{s: s, direct: direct, done: make(chan struct{})}
	if direct {
		close(a.done)
		return a
	}
	a.queue = make(chan analysis, analysisQueue)
	go a.run()
	return a
}

func (a *analyzer) run() {
	defer close(a.done)
	for j := range a.queue {
		a.finish(j)
	}
}

// worthwhile reports whether a statement of type kind may read or write
// catalog tables: any write may, and a query that mentions a table's or
// view's name.
func (a *analyzer) worthwhile(kind duckdb.StmtType, text string) bool {
	known := a.known.Load()
	return kind != duckdb.STATEMENT_TYPE_SELECT || known == nil || tokensOf(text).mentions(*known)
}

// add analyzes j, now or in the background. It reports false when it
// cannot, and the record must be written without.
func (a *analyzer) add(j analysis) bool {
	if a.direct {
		a.finish(j)
		return true
	}
	a.sendMu.Lock()
	defer a.sendMu.Unlock()
	if a.closed {
		return false
	}
	select {
	case a.queue <- j:
		return true
	default:
		a.drop()
		return false
	}
}

// close analyzes what is queued, until ctx ends, and stops.
func (a *analyzer) close(ctx context.Context) {
	a.sendMu.Lock()
	if !a.closed && a.queue != nil {
		close(a.queue)
	}
	a.closed = true
	a.sendMu.Unlock()
	select {
	case <-a.done:
	case <-ctx.Done():
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, c := range []**duckdb.Conn{&a.conn, &a.parser} {
		if *c != nil {
			_ = (*c).Close()
			*c = nil
		}
	}
}

func (a *analyzer) drop() {
	a.dropMu.Lock()
	defer a.dropMu.Unlock()
	a.dropped++
	if time.Since(a.lastWarn) < time.Minute {
		return
	}
	a.s.log.Warn("recorded statements without what they read and wrote: too many waited for analysis", "count", a.dropped)
	a.dropped, a.lastWarn = 0, time.Now()
}

// finish works out what j's statement read and wrote and writes its
// record.
func (a *analyzer) finish(j analysis) {
	found := a.objects(j)
	read, written, locations := j.known.read, j.known.written, j.known.locations
	if found != nil {
		if !j.readKnown {
			read = found.read
		}
		if written == nil {
			written = found.written
		}
		locations = append(slices.Clone(locations), found.locations...)
	}
	a.s.finishRecord(j.q, read, written, locations)
}

// objects works out what j's statement reads and writes, or returns nil.
func (a *analyzer) objects(j analysis) *statementObjects {
	a.mu.Lock()
	defer a.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.connect(ctx); err != nil {
		a.s.log.Warn("could not analyze statements for the history", "err", err)
		return nil
	}
	snap := a.snapshot(ctx)
	parsed := map[string]any{}
	r := &resolver{
		s: a.s, ctx: ctx, path: j.names.path, databases: snap.databases,
		catalog: a.s.catalogName(), local: a.s.engine.LocalDatabase(),
		objects: map[string][]object{}, readsOf: map[string]readSet{}, lenient: true,
		lookup: func(name string) []object {
			out := snap.byName[name]
			if j.names.temp[name] {
				out = append(slices.Clone(out), object{db: "temp", schema: "main", name: name})
			}
			return out
		},
		parse: func(text string) (any, error) {
			if ast, ok := parsed[text]; ok {
				return ast, nil
			}
			ast, err := parseStatement(ctx, a.parser, text)
			if err == nil {
				parsed[text] = ast
			}
			return ast, err
		},
	}
	return analyzeStatement(r, j.text, j.kind, j.names)
}

// connect opens the connections, if they are not open. a.mu must be held.
func (a *analyzer) connect(ctx context.Context) error {
	if a.conn == nil {
		conn, err := a.s.engine.Connect(ctx)
		if err != nil {
			return err
		}
		a.conn = conn
	}
	if a.parser == nil {
		connector, err := duckdb.NewConnector("", nil)
		if err != nil {
			return err
		}
		c, err := connector.Connect(ctx)
		if err != nil {
			return errors.Join(err, connector.Close())
		}
		parser := c.(*duckdb.Conn)
		// It only parses, and never reads anything.
		if _, err := parser.ExecContext(ctx, "SET enable_external_access = false", nil); err != nil {
			return errors.Join(err, parser.Close())
		}
		a.parser = parser
	}
	return nil
}

// snapshot returns the catalog's objects, read again after this replica's
// DDL and once snapshotTTL old. a.mu must be held.
func (a *analyzer) snapshot(ctx context.Context) *objectSnapshot {
	gen := a.s.catalogGeneration()
	if a.snap != nil && a.snap.gen == gen && time.Since(a.snap.at) < snapshotTTL {
		return a.snap
	}
	snap := &objectSnapshot{gen: gen, at: time.Now(), databases: map[string]string{}, byName: map[string][]object{}}
	// This runs on the analyzer's own connection, off the statement that
	// queued the work, so it takes the catalog lock itself: listing the
	// catalog's tables and views walks its in-memory set, which must not
	// happen while a table is renamed or dropped on this instance (see
	// engine.Engine.CatalogLock).
	releaseCatalog := a.s.engine.CatalogLock()
	rows, err := queryRows(ctx, a.conn, `SELECT database_name, schema_name, table_name, false, '', false
			FROM system.main.duckdb_tables() WHERE NOT temporary
		UNION ALL SELECT database_name, schema_name, view_name, true, sql, internal
			FROM system.main.duckdb_views() WHERE NOT temporary`)
	if err == nil {
		var dbs [][]driver.Value
		if dbs, err = queryRows(ctx, a.conn, "SELECT system.main.lower(database_name), type FROM system.main.duckdb_databases()"); err == nil {
			for _, d := range dbs {
				name, _ := d[0].(string)
				snap.databases[name], _ = d[1].(string)
			}
		}
	}
	releaseCatalog()
	if err != nil {
		a.s.log.Warn("could not list the catalog's objects for the history", "err", err)
		if a.snap != nil {
			return a.snap
		}
		return snap
	}
	known := map[string]bool{}
	for _, row := range rows {
		o := object{}
		o.db, _ = row[0].(string)
		o.schema, _ = row[1].(string)
		o.name, _ = row[2].(string)
		o.view, _ = row[3].(bool)
		o.sql, _ = row[4].(string)
		o.internal, _ = row[5].(bool)
		o.catalogView = o.view && a.s.ddl != nil && strings.EqualFold(o.db, a.s.engine.LocalDatabase()) && a.s.ddl.IsView(o.schema, o.name)
		if o.view && a.s.external != nil && strings.EqualFold(o.db, a.s.engine.LocalDatabase()) {
			o.externalScans, o.external = a.s.external.IsExternal(o.schema, o.name)
		}
		k := strings.ToLower(o.name)
		snap.byName[k] = append(snap.byName[k], o)
		if strings.EqualFold(o.db, a.s.catalogName()) || o.catalogView || o.external || isConnectionType(snap.databases[strings.ToLower(o.db)]) {
			known[k] = true
		}
	}
	a.snap = snap
	a.known.Store(&known)
	return snap
}

// parseStatement returns DuckDB's parse of one statement, as JSON.
func parseStatement(ctx context.Context, conn *duckdb.Conn, text string) (any, error) {
	rows, err := conn.QueryContext(ctx, "SELECT system.main.json_serialize_sql($1::VARCHAR, skip_null := true, skip_empty := true)::VARCHAR",
		[]driver.NamedValue{{Ordinal: 1, Value: text}})
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		if errors.Is(err, io.EOF) {
			err = errors.New("no parse")
		}
		return nil, err
	}
	raw, _ := dest[0].(string)
	return decodeParse(raw)
}

// analyzeStatement works out what text, a statement of type kind, reads
// and writes in the catalog, with r resolving names as the session does;
// nil when it cannot tell.
func analyzeStatement(r *resolver, text string, kind duckdb.StmtType, names *sessionNames) *statementObjects {
	if len(sqllex.Split(text)) != 1 {
		return nil
	}
	o := &statementObjects{}
	var readText string
	sc := &scanner{s: text}
	word := sc.word()
	switch {
	case kind == duckdb.STATEMENT_TYPE_SELECT && word != "SHOW" && word != "DESCRIBE":
		readText = text
	case kind == duckdb.STATEMENT_TYPE_INSERT && word == "INSERT":
		sc.keywords("INSERT")
		_ = sc.keywords("OR", "REPLACE") || sc.keywords("OR", "IGNORE")
		if !sc.keywords("INTO") {
			return nil
		}
		name, ok := sc.name()
		if !ok {
			return nil
		}
		readText, o.written = insertSource(text), writeTargets(r, name, names)
	case kind == duckdb.STATEMENT_TYPE_UPDATE && sc.keywords("UPDATE"),
		kind == duckdb.STATEMENT_TYPE_DELETE && sc.keywords("DELETE", "FROM"),
		kind == statementTypeMergeInto && sc.keywords("MERGE", "INTO"):
		name, ok := sc.name()
		if !ok {
			return nil
		}
		readText, o.written = dmlAsSelect(kind, text), writeTargets(r, name, names)
	case kind == duckdb.STATEMENT_TYPE_CREATE:
		c, ok := parseCreateTable(text)
		if !ok || c.temporary {
			return nil
		}
		target, ok := newTarget(r, c.name, names)
		if !ok {
			return nil
		}
		readText, o.written = c.query, []auth.Object{target}
	case kind == duckdb.STATEMENT_TYPE_COPY && sc.keywords("COPY"):
		// COPY table FROM 'file' loads a file; COPY ... TO writes one.
		name, ok := sc.name()
		if !ok || !sc.keywords("FROM") {
			return nil
		}
		file, ok := sc.literal()
		if !ok || file == nil {
			return nil
		}
		o.locations, o.written = []string{*file}, writeTargets(r, name, names)
	default:
		return nil
	}
	if readText == "" {
		return o
	}
	ast, err := r.serialize(readText)
	if err != nil {
		return nil
	}
	reads, err := r.analyze(readText, nil, 0)
	if err != nil {
		return nil
	}
	o.read, o.locations = append(reads.tables, reads.connObjects...), append(o.locations, fileReads(ast)...)
	return o
}

// writeTargets is the catalog table or tables a write's name may mean,
// or, when it means none, the one it would be.
func writeTargets(r *resolver, name []string, names *sessionNames) []auth.Object {
	res, err := r.resolve(tableRef{parts: name}, nil)
	if err != nil {
		return nil
	}
	var out []auth.Object
	for _, o := range res.all {
		if strings.EqualFold(o.db, r.catalog) || o.catalogView {
			out = append(out, auth.Object{Schema: o.schema, Table: o.name})
		}
	}
	if len(out) == 0 {
		if o, ok := newTarget(r, name, names); ok {
			out = append(out, o)
		}
	}
	return out
}

// newTarget is the catalog table a CREATE of name makes: in the schema
// named, or the session's current one (see newTableTarget).
func newTarget(r *resolver, name []string, names *sessionNames) (auth.Object, bool) {
	switch len(name) {
	case 3:
		return auth.Object{Schema: name[1], Table: name[2]}, strings.EqualFold(name[0], r.catalog)
	case 2:
		_, isDB := r.databases[strings.ToLower(name[0])]
		return auth.Object{Schema: name[0], Table: name[1]}, !isDB
	case 1:
		return auth.Object{Schema: names.schema, Table: name[0]}, strings.EqualFold(names.db, r.catalog)
	}
	return auth.Object{}, false
}

// fileReads lists the files a parsed query reads directly: string
// literals passed first to a function that reads files, and names used as
// tables that DuckDB reads as files.
func fileReads(ast any) []string {
	var out []string
	add := func(f string) {
		if f != "" && !slices.Contains(out, f) {
			out = append(out, f)
		}
	}
	var walk func(any)
	walk = func(n any) {
		switch n := n.(type) {
		case []any:
			for _, c := range n {
				walk(c)
			}
		case map[string]any:
			switch str(n["type"]) {
			case "BASE_TABLE":
				if name := str(n["table_name"]); str(n["schema_name"]) == "" && pathLike(name) {
					add(name)
				}
			case "TABLE_FUNCTION":
				if f, ok := n["function"].(map[string]any); ok && fileFunctions[strings.ToLower(str(f["function_name"]))] {
					if args, _ := f["children"].([]any); len(args) > 0 {
						add(constantString(args[0]))
					}
				}
			}
			for _, v := range n {
				walk(v)
			}
		}
	}
	walk(ast)
	return out
}

// constantString is the value of a parsed string constant, or "".
func constantString(n any) string {
	m, _ := n.(map[string]any)
	if str(m["class"]) != "CONSTANT" {
		return ""
	}
	v, _ := m["value"].(map[string]any)
	if t, _ := v["type"].(map[string]any); str(t["id"]) != "VARCHAR" {
		return ""
	}
	return str(v["value"])
}
