package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"sync"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/distributed"
	"github.com/akkio-inc/igloo/internal/snowflake"
)

// Distributed execution (docs/distributed-execution.md): a SELECT that
// reads a large table may run in parts on workers. The statement is
// authorized as always before it gets here, and the parts read only the
// tables the statement reads, resolved the way DuckDB resolves them in
// this session; the rest of the statement, which combines the parts'
// results, runs on the session's own connection. Anything that cannot run
// distributed, and any failure of the workers, runs here as usual.

var (
	setDistributed   = regexp.MustCompile(`(?is)^\s*SET\s+(?:SESSION\s+)?distributed_execution\s*(?:=|\bTO\b)\s*(.*?)\s*;?\s*$`)
	resetDistributed = regexp.MustCompile(`(?is)^\s*RESET\s+distributed_execution\s*;?\s*$`)
	showDistributed  = regexp.MustCompile(`(?is)^\s*SHOW\s+distributed_execution\s*;?\s*$`)
)

// distributedCommand handles SET, RESET and SHOW distributed_execution,
// which says whether the session's statements may run on workers: auto
// (the default), on (whenever they can, for testing and benchmarks) or off.
func (s *Server) distributedCommand(sess *session, query string) (wire.PreparedStatements, bool) {
	respond := func(fn wire.PreparedStatementFn, opts ...wire.PreparedOptionFn) (wire.PreparedStatements, bool) {
		return wire.Prepared(wire.NewStatement(s.audited(sess, query, fn), opts...)), true
	}
	if m := setDistributed.FindStringSubmatch(query); m != nil {
		mode, ok := distributed.ParseMode(strings.Trim(m[1], `'"`))
		if strings.EqualFold(strings.Trim(m[1], `'"`), "default") {
			mode, ok = distributed.ModeAuto, true
		}
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			if !ok {
				return pgerror.WithCode(fmt.Errorf("distributed_execution must be auto, on or off, not %s", m[1]), codes.InvalidParameterValue)
			}
			sess.distMode = mode
			return w.Complete("SET")
		})
	}
	if resetDistributed.MatchString(query) {
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			sess.distMode = distributed.ModeAuto
			return w.Complete("RESET")
		})
	}
	if showDistributed.MatchString(query) {
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			if err := w.Row([]any{string(sess.mode())}); err != nil {
				return err
			}
			return w.Complete("SHOW")
		}, wire.WithColumns(wire.Columns{{Name: "distributed_execution", Oid: pgtype.TextOID, Width: -1}}))
	}
	return nil, false
}

func (sess *session) mode() distributed.Mode {
	if sess.distMode == "" {
		return distributed.ModeAuto
	}
	return sess.distMode
}

// errWorkerMemory is what a statement fails with when a worker, about to
// run out of memory, canceled its part (distributed.ErrWorkerMemory).
var errWorkerMemory = pgerror.WithCode(errors.New("canceling statement: a worker was running out of memory, "+
	"and this statement's part had run the longest there; run it again, or on less data at a time"), codes.OutOfMemory)

// distParts is what running a statement in parts took, for the history:
// how many workers ran them, how many stages, and how many exchanges the
// stages wrote.
type distParts struct{ workers, stages, exchanges int }

// runDistributed runs st on workers if it qualifies and writes its rows to
// w. done is false when st did not run, and should run on this replica;
// parts is what running it took.
func (s *Server) runDistributed(ctx context.Context, sess *session, user auth.User, st *statement, args []driver.NamedValue, w *tagWriter) (parts distParts, done bool, err error) {
	if s.dist == nil || st.stmtType != duckdb.STATEMENT_TYPE_SELECT || st.kind != returnsRows || sess.inTx || sess.mode() == distributed.ModeOff {
		return parts, false, nil
	}
	if reason := policyReason(st); reason != "" {
		s.log.Debug("statement runs on this replica", "user", user.Name, "reason", reason)
		return parts, false, nil
	}
	env := &sessionEnv{s: s, sess: sess, user: user, st: st}
	mode := sess.mode()
	if mode == distributed.ModeOn && user.Role != auth.RoleAdmin {
		// Only admins start workers at will, or take every running one.
		mode = distributed.ModeOnRunning
	}
	plan, err := s.dist.Plan(ctx, st.query, env, mode)
	if err != nil {
		if ctx.Err() != nil {
			return parts, true, ctx.Err()
		}
		if !distributed.IsUnsupported(err) {
			s.log.Warn("could not plan a distributed statement; running it here", "user", user.Name, "err", err)
		} else {
			s.log.Debug("statement runs on this replica", "user", user.Name, "reason", err)
		}
		return parts, false, nil
	}
	settings, err := s.distributedSettings(ctx, sess)
	if err != nil {
		s.log.Debug("statement runs on this replica", "user", user.Name, "reason", err)
		return parts, false, nil
	}
	ex, err := s.dist.Execute(ctx, plan, settings)
	if err != nil {
		switch {
		case ctx.Err() != nil:
			return parts, true, ctx.Err()
		case distributed.DataError(err):
			return parts, true, err
		case distributed.OutOfMemory(err):
			// Another worker, or this replica, would run out the same way.
			s.log.Warn("a worker ran out of memory running part of a statement; the statement fails rather than run again",
				"user", user.Name, "err", err)
			return parts, true, errWorkerMemory
		case distributed.IsUnsupported(err):
			// The workers found what planning could not, such as data
			// files without field IDs.
			s.log.Debug("statement runs on this replica", "user", user.Name, "reason", err)
			return parts, false, nil
		}
		reason := "worker_error"
		if errors.Is(err, distributed.ErrNoWorkers) {
			reason = "no_workers"
		}
		s.dist.Metrics().Fallback(reason)
		s.log.Warn("workers could not run a statement; running it here", "user", user.Name, "err", err)
		return parts, false, nil
	}
	defer ex.Close()
	parts = distParts{workers: ex.Workers, stages: ex.Stages, exchanges: ex.Exchanges}
	if err := st.runFinal(ctx, sess.conn, ex.SQL, args, w); err != nil {
		if w.Written() > 0 || ctx.Err() != nil || distributed.DataError(err) {
			return parts, true, err
		}
		s.dist.Metrics().Fallback("combine_error")
		s.log.Warn("could not combine a distributed statement's results; running it here", "user", user.Name, "err", err)
		return distParts{}, false, nil
	}
	return parts, true, nil
}

// policyReason says why masking and row access policies keep st on this
// replica, or is empty. Workers read table files directly and know nothing
// of the requesting user, so they cannot apply policies: a statement that
// policies rewrote, that reads a protected table or that names anything
// policies concern (a protected table, or a view or macro that reads one)
// is not distributed. sessionEnv.Table refuses protected tables as well,
// should a statement reach the planner without having triggered the
// rewriter.
func policyReason(st *statement) string {
	g := st.guard
	switch {
	case st.original != "":
		return "masking or row access policies rewrote it"
	case g != nil && (g.touched || len(g.applied) > 0):
		return "masking or row access policies apply to what it reads"
	case g != nil && g.nonce != "":
		return "it names something masking or row access policies concern"
	}
	return ""
}

// runFinal runs the part of a distributed statement that combines its
// parts' results, in st's place: it must have st's columns and parameters.
func (st *statement) runFinal(ctx context.Context, conn *duckdb.Conn, query string, args []driver.NamedValue, w wire.DataWriter) error {
	fin, err := prepare(ctx, conn, query, st.clientOIDs)
	if err != nil {
		return err
	}
	defer fin.close() //nolint:errcheck
	if fin.kind != returnsRows || !slices.Equal(fin.params, st.params) ||
		!slices.EqualFunc(fin.columns, st.columns, func(a, b wire.Column) bool { return a.Name == b.Name && a.Oid == b.Oid }) {
		return errors.New("the combined result does not match the statement's columns")
	}
	fin.stmtType, fin.query = st.stmtType, st.query
	return fin.run(ctx, args, w)
}

// distributedHarmless are settings a session may change that do not change
// what a statement computes.
var distributedHarmless = []string{
	"search_path", "schema", "explain_output", "errors_as_json", "enable_progress_bar",
	"enable_progress_bar_print", "progress_bar_time", "enable_profiling", "profiling_mode",
	"profiling_output", "profile_output", "custom_profiling_settings", "profiling_coverage",
}

// distributedSettings returns the session's values of the settings workers
// must share, and fails if the session changed a setting that may change
// what statements compute but that workers do not take over.
func (s *Server) distributedSettings(ctx context.Context, sess *session) ([]distributed.Setting, error) {
	mine, err := settingValues(ctx, sess.conn)
	if err != nil {
		return nil, err
	}
	fresh, err := s.engine.Connect(ctx)
	if err != nil {
		return nil, err
	}
	base, err := settingValues(ctx, fresh)
	fresh.Close() //nolint:errcheck
	if err != nil {
		return nil, err
	}
	var out []distributed.Setting
	for name, value := range mine {
		known := slices.ContainsFunc(distributed.SettingNames, func(n string) bool { return strings.EqualFold(n, name) })
		switch {
		case known:
			out = append(out, distributed.Setting{Name: name, Value: value})
		case base[name] != value && !slices.Contains(distributedHarmless, strings.ToLower(name)):
			return nil, fmt.Errorf("the session changed %s", name)
		}
	}
	slices.SortFunc(out, func(a, b distributed.Setting) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

func settingValues(ctx context.Context, conn *duckdb.Conn) (map[string]string, error) {
	rows, err := conn.QueryContext(ctx, "SELECT name, value::VARCHAR FROM duckdb_settings()", nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	out := map[string]string{}
	dest := make([]driver.Value, 2)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}
		name, _ := dest[0].(string)
		value, _ := dest[1].(string)
		out[name] = value
	}
}

// sessionEnv is the session a statement runs in, for the planner.
type sessionEnv struct {
	s    *Server
	sess *session
	user auth.User
	st   *statement

	resolver *resolver
	userFns  map[string]bool
}

// Table resolves a name to a catalog table as DuckDB would in this
// session. Only a name that can mean exactly one catalog table qualifies.
// For users who read only what grants allow, the table must be one the
// statement was checked to read.
func (e *sessionEnv) Table(ctx context.Context, parts []string) (*distributed.Table, bool, error) {
	catalogName := e.s.catalogName()
	if catalogName == "" {
		return nil, false, nil
	}
	if e.resolver == nil {
		r, err := e.s.newResolver(ctx, e.sess)
		if err != nil {
			return nil, false, err
		}
		e.resolver = r
	}
	res, err := e.resolver.resolve(tableRef{parts: parts}, nil)
	if err != nil {
		return nil, false, err
	}
	if len(res.all) != 1 || len(res.first) != 1 {
		return nil, false, nil
	}
	o := res.first[0]
	if o.view || !strings.EqualFold(o.db, catalogName) {
		return nil, false, nil
	}
	obj := auth.Object{Schema: o.schema, Table: o.name}
	if rank[e.user.Role] < rank[auth.RoleReader] {
		if e.st.access == nil || !slices.ContainsFunc(e.st.access.read, obj.Equal) {
			return nil, false, nil
		}
	}
	if e.s.Protected(ctx, o.schema, o.name) {
		return &distributed.Table{Schema: o.schema, Name: o.name, Unsupported: "masking or row access policies, or a clean room, apply to it"}, true, nil
	}
	t, err := e.s.dist.Table(ctx, o.schema, o.name)
	if err != nil {
		return nil, false, err
	}
	// Workers must read the columns as DuckDB does here. The statement was
	// bound, so DuckDB has loaded them. Listing them with duckdb_columns()
	// walks the catalog's in-memory set; hold the exclusive side for that one
	// scan so it does not run while a table is renamed or dropped on this
	// instance (the read's run holds nothing; see catalogRunLock, holdCatalog).
	release := e.s.holdCatalog(e.sess)
	cols, err := tableColumns(ctx, e.sess, catalogName, o.schema, o.name)
	release()
	if err != nil {
		return nil, false, err
	}
	if len(cols) != len(t.Columns) && t.Unsupported == "" {
		t.Unsupported = "its columns differ between DuckDB and the catalog"
	}
	for i, c := range cols {
		if i >= len(t.Columns) {
			break
		}
		switch {
		case t.Unsupported != "":
		case !strings.EqualFold(t.Columns[i].Name, c.Name):
			t.Unsupported = "its columns differ between DuckDB and the catalog"
		case t.Columns[i].Type != c.Type:
			t.Unsupported = fmt.Sprintf("DuckDB reads column %s as %s, not %s", c.Name, c.Type, t.Columns[i].Type)
		}
	}
	return t, true, nil
}

// View reports whether a name may mean a view in this session: a view of
// any database, or a name that does not resolve to exactly one relation.
func (e *sessionEnv) View(ctx context.Context, parts []string) (bool, error) {
	if e.resolver == nil {
		r, err := e.s.newResolver(ctx, e.sess)
		if err != nil {
			return false, err
		}
		e.resolver = r
	}
	res, err := e.resolver.resolve(tableRef{parts: parts}, nil)
	if err != nil {
		return false, err
	}
	if len(res.all) != 1 || len(res.first) != 1 {
		return true, nil
	}
	return res.first[0].view, nil
}

// ViewDefinition returns the query of the view a name means in this
// session, if it means exactly one: a view of the catalog's (a local copy
// of one), or a temporary view of the session's. Names in the query are
// resolved as DuckDB resolves them in the view's body. The grant check
// expanded the view the same way for users who read only what grants
// allow, so the tables it reads are among those it allowed (see Table).
func (e *sessionEnv) ViewDefinition(ctx context.Context, parts []string) (*distributed.ViewDefinition, bool, error) {
	if e.resolver == nil {
		r, err := e.s.newResolver(ctx, e.sess)
		if err != nil {
			return nil, false, err
		}
		e.resolver = r
	}
	res, err := e.resolver.resolve(tableRef{parts: parts}, nil)
	if err != nil {
		return nil, false, err
	}
	if len(res.all) != 1 || len(res.first) != 1 {
		return nil, false, nil
	}
	o := res.first[0]
	if !o.view || o.internal || o.external || !e.resolver.ownDatabase(o.db) {
		return nil, false, nil
	}
	body := viewBody(o.sql)
	if body == "" {
		return nil, false, nil
	}
	return &distributed.ViewDefinition{
		Query: body,
		Resolve: func(_ context.Context, parts []string) ([]string, bool, error) {
			res, err := e.resolver.resolve(tableRef{parts: parts}, &o)
			if err != nil || len(res.all) != 1 || len(res.first) != 1 {
				return nil, false, err
			}
			t := res.first[0]
			return []string{t.db, t.schema, t.name}, true, nil
		},
	}, true, nil
}

// tableColumns lists a table's columns as DuckDB has them. Names match as
// DuckDB matches them, whatever their case: a statement in Snowflake's
// dialect names lower-case tables in upper case.
func tableColumns(ctx context.Context, sess *session, db, schema, table string) ([]distributed.Column, error) {
	rows, err := sess.conn.QueryContext(ctx, `SELECT column_name, data_type FROM duckdb_columns()
		WHERE lower(database_name) = lower($1) AND lower(schema_name) = lower($2) AND lower(table_name) = lower($3)
		ORDER BY column_index`,
		[]driver.NamedValue{{Ordinal: 1, Value: db}, {Ordinal: 2, Value: schema}, {Ordinal: 3, Value: table}})
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var cols []distributed.Column
	dest := make([]driver.Value, 2)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return cols, nil
			}
			return nil, err
		}
		name, _ := dest[0].(string)
		typ, _ := dest[1].(string)
		cols = append(cols, distributed.Column{Name: name, Type: typ})
	}
}

// Setting returns the session's value of a DuckDB setting.
func (e *sessionEnv) Setting(ctx context.Context, name string) (string, error) {
	return settingValue(ctx, e.sess.conn, name)
}

// Describe binds query on the session's connection.
// Parameters are bound to NULL, as for the grant check's plans: the result
// columns must match the statement's, which runFinal checks.
func (e *sessionEnv) Describe(ctx context.Context, query string) ([]distributed.Column, error) {
	ds, err := e.sess.conn.PrepareContext(ctx, "DESCRIBE "+query)
	if err != nil {
		return nil, err
	}
	stmt := ds.(*duckdb.Stmt)
	defer stmt.Close() //nolint:errcheck
	args := make([]driver.NamedValue, stmt.NumInput())
	for i := range args {
		args[i] = driver.NamedValue{Ordinal: i + 1}
	}
	rows, err := stmt.QueryContext(ctx, args)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var cols []distributed.Column
	dest := make([]driver.Value, len(rows.Columns()))
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return cols, nil
			}
			return nil, err
		}
		name, _ := dest[0].(string)
		typ, _ := dest[1].(string)
		cols = append(cols, distributed.Column{Name: name, Type: typ})
	}
}

// UserFunction reports whether name is a function or macro someone
// defined, rather than one built into DuckDB or its extensions. Igloo's
// own macros for Snowflake's functions (snowflake.Macros), which workers
// define too, count as built in while the session's are exactly those:
// redefining any of them makes them all the session's own.
func (e *sessionEnv) UserFunction(ctx context.Context, name string) (bool, error) {
	if e.userFns == nil {
		defs, err := macroDefinitions(ctx, e.sess.conn, `NOT internal`)
		if err != nil {
			return false, err
		}
		standard, err := e.s.snowflakeMacros(ctx)
		if err != nil {
			return false, err
		}
		var sf []string
		e.userFns = map[string]bool{}
		for _, d := range defs {
			e.userFns[d.name] = true
			if strings.HasPrefix(d.name, "sf_") {
				sf = append(sf, d.key)
			}
		}
		slices.Sort(sf)
		if slices.Equal(sf, standard) {
			for n := range e.userFns {
				if strings.HasPrefix(n, "sf_") {
					delete(e.userFns, n)
				}
			}
		}
	}
	return e.userFns[strings.ToLower(name)], nil
}

// macroDefinition is a function or macro overload as duckdb_functions()
// lists it.
type macroDefinition struct {
	// name is lower case; key is everything that defines the overload.
	name, key string
}

// macroDefinitions lists the functions and macros duckdb_functions() has
// where cond holds.
func macroDefinitions(ctx context.Context, conn *duckdb.Conn, cond string) ([]macroDefinition, error) {
	rows, err := conn.QueryContext(ctx, `SELECT lower(function_name), function_type || chr(31) || database_name || chr(31) ||
		coalesce(schema_name, '') || chr(31) || function_name || chr(31) || coalesce(parameters::VARCHAR, '') || chr(31) ||
		coalesce(macro_definition, '') FROM duckdb_functions() WHERE `+cond, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var out []macroDefinition
	dest := make([]driver.Value, 2)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}
		name, _ := dest[0].(string)
		key, _ := dest[1].(string)
		out = append(out, macroDefinition{name: name, key: key})
	}
}

var (
	sfMacrosMu sync.Mutex
	// sfMacros are the keys of Igloo's macros for Snowflake's functions as
	// a session defines them, sorted.
	sfMacros []string
)

// snowflakeMacros returns the keys (see macroDefinitions) of the macros
// snowflake.Macros defines, as a fresh connection lists them.
func (s *Server) snowflakeMacros(ctx context.Context) ([]string, error) {
	sfMacrosMu.Lock()
	defer sfMacrosMu.Unlock()
	if sfMacros != nil {
		return sfMacros, nil
	}
	conn, err := s.engine.Connect(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close() //nolint:errcheck
	for _, stmt := range snowflake.Macros() {
		if _, err := conn.ExecContext(ctx, stmt, nil); err != nil {
			return nil, err
		}
	}
	defs, err := macroDefinitions(ctx, conn, `NOT internal AND lower(function_name) LIKE 'sf\_%' ESCAPE '\'`)
	if err != nil {
		return nil, err
	}
	keys := []string{}
	for _, d := range defs {
		keys = append(keys, d.key)
	}
	slices.Sort(keys)
	sfMacros = keys
	return keys, nil
}
