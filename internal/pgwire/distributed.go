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

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/distributed"
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

// runDistributed runs st on workers if it qualifies and writes its rows to
// w. done is false when st did not run, and should run on this replica;
// workers is how many workers ran parts of it.
func (s *Server) runDistributed(ctx context.Context, sess *session, user auth.User, st *statement, args []driver.NamedValue, w *tagWriter) (workers int, done bool, err error) {
	if s.dist == nil || st.stmtType != duckdb.STATEMENT_TYPE_SELECT || st.kind != returnsRows || sess.inTx || sess.mode() == distributed.ModeOff {
		return 0, false, nil
	}
	if reason := policyReason(st); reason != "" {
		s.log.Debug("statement runs on this replica", "user", user.Name, "reason", reason)
		return 0, false, nil
	}
	env := &sessionEnv{s: s, sess: sess, user: user, st: st}
	plan, err := s.dist.Plan(ctx, st.query, env, sess.mode())
	if err != nil {
		if ctx.Err() != nil {
			return 0, true, ctx.Err()
		}
		if !distributed.IsUnsupported(err) {
			s.log.Warn("could not plan a distributed statement; running it here", "user", user.Name, "err", err)
		} else {
			s.log.Debug("statement runs on this replica", "user", user.Name, "reason", err)
		}
		return 0, false, nil
	}
	settings, err := s.distributedSettings(ctx, sess)
	if err != nil {
		s.log.Debug("statement runs on this replica", "user", user.Name, "reason", err)
		return 0, false, nil
	}
	ex, err := s.dist.Execute(ctx, plan, settings)
	if err != nil {
		switch {
		case ctx.Err() != nil:
			return 0, true, ctx.Err()
		case distributed.DataError(err):
			return 0, true, err
		case distributed.IsUnsupported(err):
			// The workers found what planning could not, such as data
			// files without field IDs.
			s.log.Debug("statement runs on this replica", "user", user.Name, "reason", err)
			return 0, false, nil
		}
		reason := "worker_error"
		if errors.Is(err, distributed.ErrNoWorkers) {
			reason = "no_workers"
		}
		s.dist.Metrics().Fallback(reason)
		s.log.Warn("workers could not run a statement; running it here", "user", user.Name, "err", err)
		return 0, false, nil
	}
	defer ex.Close()
	if err := st.runFinal(ctx, sess.conn, ex.SQL, args, w); err != nil {
		if w.Written() > 0 || ctx.Err() != nil || distributed.DataError(err) {
			return ex.Workers, true, err
		}
		s.dist.Metrics().Fallback("combine_error")
		s.log.Warn("could not combine a distributed statement's results; running it here", "user", user.Name, "err", err)
		return 0, false, nil
	}
	return ex.Workers, true, nil
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
		return &distributed.Table{Schema: o.schema, Name: o.name, Unsupported: "masking or row access policies apply to it"}, true, nil
	}
	t, err := e.s.dist.Table(ctx, o.schema, o.name)
	if err != nil {
		return nil, false, err
	}
	// Workers must read the columns as DuckDB does here. The statement was
	// bound, so DuckDB has loaded them.
	cols, err := tableColumns(ctx, e.sess, catalogName, o.schema, o.name)
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

// tableColumns lists a table's columns as DuckDB has them.
func tableColumns(ctx context.Context, sess *session, db, schema, table string) ([]distributed.Column, error) {
	rows, err := sess.conn.QueryContext(ctx, `SELECT column_name, data_type FROM duckdb_columns()
		WHERE database_name = $1 AND schema_name = $2 AND table_name = $3 ORDER BY column_index`,
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
// defined, rather than one built into DuckDB or its extensions.
func (e *sessionEnv) UserFunction(ctx context.Context, name string) (bool, error) {
	if e.userFns == nil {
		names, err := queryStrings(ctx, e.sess, `SELECT DISTINCT lower(function_name) FROM duckdb_functions() WHERE NOT internal`)
		if err != nil {
			return false, err
		}
		e.userFns = map[string]bool{}
		for _, n := range names {
			e.userFns[n] = true
		}
	}
	return e.userFns[strings.ToLower(name)], nil
}
