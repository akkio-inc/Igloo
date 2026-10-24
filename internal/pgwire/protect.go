package pgwire

import (
	"context"
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/cleanroom"
	"github.com/akkio-inc/igloo/internal/iddomain"
	"github.com/akkio-inc/igloo/internal/policy"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Masking policies, row access policies and tags (package policy) apply to
// every statement of every user, admins included: a statement that reads a
// protected table is rewritten so it reads the table through its policies
// (see policyrewrite.go), and one Igloo cannot rewrite with certainty is
// refused. Statements that name nothing protected run as written, after a
// check of their words against the names of protected tables and of the
// views and macros that read them.
//
// While any table has policies, users other than admins also may not read
// the warehouse's files directly (read_parquet and the like), nor call table
// functions that read tables by name, since policies cannot see into those.
// Admins can: they manage policies and can read any file, so policies are
// not a boundary for them, only a default.

// governance holds the policies and what is derived from them.
type governance struct {
	store   policy.Store
	refresh time.Duration
	log     *slog.Logger

	mu      sync.Mutex
	snap    atomic.Pointer[policy.Snapshot]
	checked atomic.Int64
	// gen changes whenever the snapshot does.
	gen atomic.Uint64

	probe policyProbe

	tmplMu    sync.Mutex
	templates map[string]*bodyTemplate

	// allowed are the locations users other than admins may read files
	// from while policies exist, and warehouse the warehouse's, which they
	// may not; nor external, where external tables' files are (Delta
	// Sharing's proxy), which they read through the external tables. The
	// warehouse holds the copies of shared files with deletes applied, in
	// its .igloo/sharing/ directory.
	allowed   []string
	warehouse string
	external  string
}

func newGovernance(ctx context.Context, store policy.Store, refresh time.Duration, log *slog.Logger,
	connect func(context.Context) (*duckdb.Conn, error), allowed []string, warehouse, external string) (*governance, error) {
	g := &governance{store: store, refresh: refresh, log: log, templates: map[string]*bodyTemplate{}}
	if external != "" {
		g.external = strings.ToLower(withSlash(external))
	}
	g.probe.connect = connect
	for _, a := range allowed {
		if a != "" {
			g.allowed = append(g.allowed, withSlash(a))
		}
	}
	if warehouse != "" {
		g.warehouse = withSlash(warehouse)
	}
	snap, err := store.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("load policies: %w", err)
	}
	g.snap.Store(snap)
	g.checked.Store(time.Now().UnixNano())
	return g, nil
}

func withSlash(s string) string {
	if strings.HasSuffix(s, "/") {
		return s
	}
	return s + "/"
}

// snapshot returns the policies, checking the store for changes once they
// are as old as the refresh interval. If the store fails, the last ones
// loaded stay in force.
func (g *governance) snapshot(ctx context.Context) *policy.Snapshot {
	fresh := func() bool { return time.Since(time.Unix(0, g.checked.Load())) < g.refresh }
	if fresh() {
		return g.snap.Load()
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	snap := g.snap.Load()
	if fresh() {
		return snap
	}
	defer g.checked.Store(time.Now().UnixNano())
	v, err := g.store.Version(ctx)
	if err != nil {
		g.log.Warn("could not check for policy changes", "err", err)
		return snap
	}
	if v == snap.Version {
		return snap
	}
	next, err := g.store.Load(ctx)
	if err != nil {
		g.log.Warn("could not load policies", "err", err)
		return snap
	}
	g.snap.Store(next)
	g.gen.Add(1)
	return next
}

// reload loads the policies now, after a change this replica made.
func (g *governance) reload(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	next, err := g.store.Load(ctx)
	if err != nil {
		// Check again before the next statement.
		g.checked.Store(0)
		return err
	}
	g.snap.Store(next)
	g.gen.Add(1)
	g.checked.Store(time.Now().UnixNano())
	return nil
}

// template parses a policy's body, once per definition.
func (g *governance) template(r *resolver, p policy.Policy) (*bodyTemplate, error) {
	key := p.Name + "\x00" + p.Signature() + "\x00" + p.Body
	g.tmplMu.Lock()
	t, ok := g.templates[key]
	g.tmplMu.Unlock()
	if ok {
		return t, nil
	}
	t, err := r.parseBody(p)
	if err != nil {
		return nil, err
	}
	g.tmplMu.Lock()
	if len(g.templates) > 1024 {
		g.templates = map[string]*bodyTemplate{}
	}
	g.templates[key] = t
	g.tmplMu.Unlock()
	return t, nil
}

func (g *governance) close() { g.probe.close() }

// Policies returns the policies in force on this replica, for the
// information views and for other front ends to the same tables.
func (s *Server) Policies(ctx context.Context) *policy.Snapshot {
	return s.gov.snapshot(ctx)
}

// Protected reports whether a policy applies to the catalog table
// schema.table, which must then be read through Igloo only. Other engines
// cannot apply policies, so anything that hands tables to them (an Iceberg
// REST endpoint, say, or credentials vended for a table's files) must
// refuse a table for which Protected reports true. Igloo has no such
// endpoint yet; this is the check for the one that comes.
func (s *Server) Protected(ctx context.Context, schema, table string) bool {
	return s.gov.snapshot(ctx).Rules(schema, table) != nil
}

// Classified reports whether the catalog table schema.table has policies,
// tags, a data retention or a clean room's confinement, which stay with
// its name, and names its columns with policies or tags, which stay with
// theirs, as checkAlter guards them.
func (s *Server) Classified(ctx context.Context, schema, table string) ([]string, bool) {
	rules := s.gov.snapshot(ctx).Tagged(schema, table)
	if rules == nil {
		return nil, false
	}
	var columns []string
	for name := range rules.Columns {
		columns = append(columns, name)
	}
	if rules.Filter != nil {
		columns = append(columns, rules.Filter.Columns...)
	}
	slices.Sort(columns)
	return slices.Compact(columns), true
}

// Erasing reports whether an erasure request that is not over erases from
// the catalog table schema.table (see erasure.Service.Erasing).
func (s *Server) Erasing(ctx context.Context, schema, table string) (bool, error) {
	if s.erasure == nil {
		return false, nil
	}
	return s.erasure.Erasing(ctx, schema, table)
}

// ProtectedFor reports whether the catalog table schema.table must be read
// through Igloo only, for user: policies protect it, or a clean room keeps
// it to others than user (see policy.Confinement). The Iceberg REST
// catalog refuses such a table to everyone but admins.
func (s *Server) ProtectedFor(ctx context.Context, user auth.User, schema, table string) (bool, error) {
	if len(s.memberDomains(ctx, schema, table)) > 0 {
		// Its files hold the columns of ids, which are Igloo's own.
		return true, nil
	}
	r := s.gov.snapshot(ctx).Rules(schema, table)
	if r == nil {
		return false, nil
	}
	if r.Policies() {
		return true, nil
	}
	privs, err := s.grants.UserPrivileges(ctx, user.Name)
	if err != nil {
		return true, err
	}
	return !r.Admits(user.Name, append(privs.Roles, string(user.Role))), nil
}

// keptFrom refuses user the first of tables, of the catalog, that clean
// rooms keep to another party, as reading it would be refused. Statements
// that reach a table other than through DuckDB, such as CLONE, check it
// with this.
func (s *Server) keptFrom(ctx context.Context, sess *session, user auth.User, tables []auth.Object) error {
	snap := s.gov.snapshot(ctx)
	var roles []string
	for _, o := range tables {
		r := snap.Rules(o.Schema, o.Table)
		if o.Catalog != "" || r == nil || len(r.Confined) == 0 {
			continue
		}
		if roles == nil {
			privs, err := s.privileges(ctx, sess, user)
			if err != nil {
				return denied("could not load roles: %v", err)
			}
			roles = append(slices.Clone(privs.Roles), string(user.Role))
		}
		if !r.Admits(user.Name, roles) {
			s.log.Info("statement denied", "user", user.Name, "role", user.Role, "table", o.String(), "rooms", r.Rooms())
			return keptError(o.Schema, o.Table, r)
		}
	}
	return nil
}

// mayWrite reports whether a session of user, with roles, may change a
// table that clean rooms keep (r): a dataset, by its own party; a result,
// by its room's runs alone.
func mayWrite(r *policy.TableRules, user string, roles []string) bool {
	if strings.EqualFold(r.Schema, cleanroom.ResultSchema) {
		return slices.ContainsFunc(r.Rooms(), func(room string) bool { return user == policy.RoomUser(room) })
	}
	return r.Admits(user, roles)
}

// writeGuard says which tables clean rooms keep from user's changes (see
// mayWrite), and refuses them; nil means none.
type writeGuard struct {
	s     *Server
	snap  *policy.Snapshot
	user  auth.User
	roles []string
}

func (s *Server) writeGuard(ctx context.Context, sess *session, user auth.User) (*writeGuard, error) {
	snap := s.gov.snapshot(ctx)
	if !snap.Confines() || s.catalogName() == "" {
		return nil, nil
	}
	privs, err := s.privileges(ctx, sess, user)
	if err != nil {
		return nil, denied("could not load roles: %v", err)
	}
	return &writeGuard{s: s, snap: snap, user: user, roles: append(slices.Clone(privs.Roles), string(user.Role))}, nil
}

func (g *writeGuard) kept(r *policy.TableRules) bool {
	return r != nil && r.Catalog == "" && len(r.Confined) > 0 && !mayWrite(r, g.user.Name, g.roles)
}

func (g *writeGuard) refuse(r *policy.TableRules) error {
	g.s.log.Info("statement denied", "user", g.user.Name, "role", g.user.Role, "table", r.Schema+"."+r.Table, "rooms", r.Rooms())
	return denied("%s.%s is in clean room %s, which keeps it from changes but by its own party, and a result's but by its runs",
		r.Schema, r.Table, strings.Join(r.Rooms(), ", "))
}

// tables refuses changes to any of tables, of the catalog, that are kept.
func (g *writeGuard) tables(tables []auth.Object) error {
	for _, o := range tables {
		if r := g.snap.Rules(o.Schema, o.Table); o.Catalog == "" && g.kept(r) {
			return g.refuse(r)
		}
	}
	return nil
}

// keptChanges refuses user changes to any of tables that clean rooms keep
// from them, as CLONE onto them would make.
func (s *Server) keptChanges(ctx context.Context, sess *session, user auth.User, tables []auth.Object) error {
	g, err := s.writeGuard(ctx, sess, user)
	if g == nil || err != nil {
		return err
	}
	return g.tables(tables)
}

// tableWrites are the kinds of statement that may change a table's rows or
// the table itself.
var tableWrites = []duckdb.StmtType{duckdb.STATEMENT_TYPE_INSERT, duckdb.STATEMENT_TYPE_UPDATE, duckdb.STATEMENT_TYPE_DELETE,
	statementTypeMergeInto, duckdb.STATEMENT_TYPE_COPY, duckdb.STATEMENT_TYPE_CREATE, duckdb.STATEMENT_TYPE_DROP, duckdb.STATEMENT_TYPE_ALTER}

// keptWrites refuses a statement, of type t, that writes to, replaces,
// drops, alters or renames a table that clean rooms keep from user (see
// mayWrite): otherwise someone else could change what a party contributes,
// which its runs then read, or a result. A write Igloo cannot place is
// refused when it names such a table at all, and dropping a schema when the
// schema holds one. It reports whether it ran statements on the session's
// connection, which the statement must then be prepared again after.
func (s *Server) keptWrites(ctx context.Context, sess *session, user auth.User, t duckdb.StmtType, query string) (bool, error) {
	if !slices.Contains(tableWrites, t) {
		return false, nil
	}
	g, err := s.writeGuard(ctx, sess, user)
	if g == nil || err != nil {
		return false, err
	}
	_, written, err := s.writeNeeds(ctx, sess, t, query)
	switch {
	case errors.Is(err, errNoGrantPath):
		if schema, ok := droppedSchema(query); ok {
			for _, r := range g.snap.Tables() {
				if g.kept(r) && strings.EqualFold(r.Schema, schema) {
					return true, g.refuse(r)
				}
			}
			return true, nil
		}
		if t == duckdb.STATEMENT_TYPE_CREATE {
			// Not a table of the catalog: a view, macro, schema, sequence
			// or temporary table, none of which changes another table.
			return true, nil
		}
		tokens := tokensOf(query)
		for _, r := range g.snap.Tables() {
			if g.kept(r) && tokens.mentions(map[string]bool{strings.ToLower(r.Table): true}) {
				return true, g.refuse(r)
			}
		}
		return true, nil
	case err != nil:
		return true, err
	}
	return true, g.tables(written)
}

// droppedSchema is the schema DROP SCHEMA drops.
func droppedSchema(query string) (string, bool) {
	sc := &scanner{s: query}
	if !sc.keywords("DROP", "SCHEMA") {
		return "", false
	}
	sc.keywords("IF", "EXISTS")
	name, ok := sc.name()
	if !ok || len(name) == 0 {
		return "", false
	}
	return name[len(name)-1], true
}

// ProtectedBy names, sorted, the policies that protect the catalog table
// schema.table: its row access policy, and the masking policies of its
// columns, set on them or through their tags, and the clean rooms that
// keep it, as "clean room name", and the ID domains its columns are
// members of, as "ID domain name", whose ids are in its files. Delta
// Sharing refuses to share or serve such a table (package sharing).
func (s *Server) ProtectedBy(ctx context.Context, schema, table string) []string {
	// Columns that hold ID domains' ids are in the table's files, which
	// Delta Sharing serves whole.
	var names []string
	for _, d := range s.memberDomains(ctx, schema, table) {
		names = append(names, "ID domain "+d)
	}
	r := s.gov.snapshot(ctx).Rules(schema, table)
	if r == nil {
		slices.Sort(names)
		return names
	}
	for _, room := range r.Rooms() {
		names = append(names, "clean room "+room)
	}
	if r.FilterPolicy != nil {
		names = append(names, r.FilterPolicy.Name)
	}
	for _, c := range r.Columns {
		switch {
		case c.Mask != nil:
			names = append(names, c.Mask.Name)
		default:
			for _, m := range c.TagMasks {
				names = append(names, m.Policy.Name)
			}
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// externalFiles reports whether reading prefix, the start of a path
// before any wildcard, could read the external tables' files.
func (g *governance) externalFiles(prefix string) bool {
	p := strings.ToLower(prefix)
	return g.external != "" && (strings.HasPrefix(p, g.external) || strings.HasPrefix(g.external, p))
}

// --- what statements may name --------------------------------------------------

// sensitivity is what a session's statements may name that policies
// concern, worked out from the protected tables and the definitions of the
// session's views and macros. Names are lower-cased.
type sensitivity struct {
	policyGen, catalogGen uint64
	// names are the protected tables', and those of views and macros that
	// read them, directly or through others.
	names map[string]bool
	// risky are views and macros that read files or call table functions
	// that read tables by name, directly or through others.
	risky map[string]bool
	// viewProtected and viewRisky are such views, by object key.
	viewProtected, viewRisky map[string]bool
	// macroProtected and macroRisky are such macros.
	macroProtected, macroRisky map[string]bool
	// macros are every macro that is not built in, which could replace a
	// function a policy calls.
	macros map[string]bool
	// unsafeFns are the table functions that are not safeTableFunctions.
	unsafeFns map[string]bool
}

// safeTableFunctions are the table functions users other than admins may
// call while policies exist: those the none role may, which read no table
// data and no files, and a few more of the same kind. iceberg_snapshots
// and iceberg_metadata are allowed on tables without policies (see
// checkTableFunction).
var safeTableFunctions = func() map[string]bool {
	m := map[string]bool{
		"duckdb_memory": true, "duckdb_temporary_files": true,
		"duckdb_connection_count": true, "duckdb_secret_types": true, "duckdb_profiling_settings": true,
		"duckdb_coordinate_systems": true, "sql_auto_complete": true, "test_all_types": true,
		"tpch_queries": true, "tpch_answers": true, "tpcds_queries": true, "tpcds_answers": true,
		"igloo_policy_references": true, "igloo_tag_references": true,
		// They change and list table properties, and read no rows.
		"iceberg_table_properties": true,
	}
	for f := range allowedFunctions {
		m[f] = true
	}
	for _, f := range propertyFunctions {
		m[f] = true
	}
	return m
}()

// fileFunctions read the files their first argument names. While policies
// exist, users other than admins may name only files in the allowed
// locations, as string literals.
var fileFunctions = map[string]bool{
	"read_parquet": true, "parquet_scan": true, "read_csv": true, "read_csv_auto": true, "sniff_csv": true,
	"read_json": true, "read_json_auto": true, "read_json_objects": true, "read_json_objects_auto": true,
	"read_ndjson": true, "read_ndjson_auto": true, "read_ndjson_objects": true, "read_text": true,
	"read_blob": true, "glob": true, "parquet_metadata": true, "parquet_schema": true, "parquet_kv_metadata": true,
	"parquet_file_metadata": true, "parquet_full_metadata": true, "parquet_bloom_probe": true, "read_avro": true,
}

// fileExtensions are those DuckDB reads a table name ending in as a file.
var fileExtensions = []string{".parquet", ".parq", ".csv", ".tsv", ".txt", ".json", ".jsonl", ".ndjson",
	".avro", ".arrow", ".ipc", ".feather", ".xlsx", ".db", ".duckdb", ".sqlite", ".orc"}

var compressionExtensions = []string{".gz", ".gzip", ".zst", ".zstd", ".lz4", ".bz2", ".xz", ".snappy", ".br"}

// pathLike reports whether DuckDB could read name, used as a table, as a
// file: a URL, or a name with a file extension.
func pathLike(name string) bool {
	n := strings.ToLower(name)
	if strings.Contains(n, "://") {
		return true
	}
	for {
		trimmed := false
		for _, ext := range compressionExtensions {
			if strings.HasSuffix(n, ext) {
				n, trimmed = strings.TrimSuffix(n, ext), true
			}
		}
		if !trimmed {
			break
		}
	}
	return slices.ContainsFunc(fileExtensions, func(ext string) bool { return strings.HasSuffix(n, ext) })
}

// checkPath decides whether a user other than an admin may name the file
// path, which must be a plain string with no way to climb out of a prefix:
// DuckDB 1.5.6 does not canonicalise remote paths before matching them
// against allowed_directories, so a path such as "s3://b/land/../secret" or
// "https://h/land/%2e%2e/x" would prefix-match an allowed location yet reach
// elsewhere. allowWarehouse lets it read the warehouse's own files, which is
// so for readers and writers until a table has a policy.
func (s *Server) checkPath(path string, allowWarehouse bool) error {
	g := s.gov
	switch {
	case path == "":
		return denied("an empty file name is not allowed")
	case strings.Contains(path, "\\"):
		return denied("cannot read %q: backslashes are not allowed in file names", path)
	case strings.ContainsAny(path, "?#"):
		// '?' and '#' start a URL's query and fragment, which can redirect a
		// remote read (an s3:// path's ?s3_endpoint= names another host); '?'
		// is also a glob wildcard, so globbing uses '*', '[' and '{' instead.
		return denied("cannot read %q: '?' and '#' are not allowed in file names", path)
	case strings.Contains(path, "%"):
		// No percent-encoding, so %2e (.), %2f (/), %5c (\\) and double
		// encodings like %252e cannot smuggle a separator past this check.
		return denied("cannot read %q: percent-encoding is not allowed in file names", path)
	}
	// The part after an optional scheme://authority, split into segments.
	rest := path
	if i := strings.Index(rest, "://"); i >= 0 {
		rest = rest[i+3:]
		if j := strings.IndexByte(rest, '/'); j >= 0 {
			rest = rest[j+1:]
		} else {
			rest = ""
		}
	}
	segs := strings.Split(rest, "/")
	for i, seg := range segs {
		// A leading empty segment is a local path's root slash and a trailing
		// one a directory's own slash; an interior empty segment is "//",
		// which can normalise to a climb, so only that is refused.
		if seg == "." || seg == ".." || (seg == "" && i != 0 && i != len(segs)-1) {
			return denied("cannot read %q: '.', '..' and empty path segments are not allowed", path)
		}
	}
	prefix := path
	if i := strings.IndexAny(path, "*[{"); i >= 0 {
		prefix = path[:i]
	}
	// The allowed set is the warehouse (when permitted) and the allowed
	// locations, each ending in a slash, as DuckDB's sandbox has them, so
	// this check and the sandbox stay in step. The spill and cache
	// directories are the server's own and not on it.
	allowed := g.allowed
	if allowWarehouse && g.warehouse != "" {
		allowed = append(append([]string(nil), g.allowed...), g.warehouse)
	}
	if !slices.ContainsFunc(allowed, func(loc string) bool { return strings.HasPrefix(prefix, loc) }) {
		if allowWarehouse {
			return denied("reading %q: only files in the warehouse and IGLOO_ALLOWED_LOCATIONS may be read directly", path)
		}
		return denied("reading %q: while tables have policies, only files in IGLOO_ALLOWED_LOCATIONS may be read directly", path)
	}
	if !allowWarehouse && g.warehouse != "" && (strings.HasPrefix(prefix, g.warehouse) || strings.HasPrefix(g.warehouse, prefix)) {
		return denied("reading %q: the warehouse's files are read through its tables, which policies apply to", path)
	}
	if g.externalFiles(prefix) {
		return denied("reading %q: external tables' files are read through the external tables", path)
	}
	return nil
}

// sensitivity returns the session's, working it out again when the
// policies or the catalog changed.
func (s *Server) sensitivity(ctx context.Context, sess *session, snap *policy.Snapshot) (*sensitivity, bool, error) {
	pg, cg := s.gov.gen.Load(), s.catalogGeneration()
	if c := sess.sens; c != nil && c.policyGen == pg && c.catalogGen == cg {
		return c, false, nil
	}
	rows, err := sess.conn.QueryContext(ctx, `SELECT 'view', database_name, schema_name, view_name, sql
			FROM system.main.duckdb_views() WHERE NOT internal
		UNION ALL SELECT 'macro', database_name, schema_name, function_name, coalesce(macro_definition, '')
			FROM system.main.duckdb_functions() WHERE NOT internal AND function_type IN ('macro', 'table_macro')
		UNION ALL SELECT DISTINCT 'table function', '', '', function_name, ''
			FROM system.main.duckdb_functions() WHERE function_type IN ('table', 'table_macro')`, nil)
	if err != nil {
		return nil, true, err
	}
	type def struct {
		kind, key, name string
		toks            defTokens
	}
	var defs []def
	sens := &sensitivity{policyGen: pg, catalogGen: cg, names: map[string]bool{}, risky: map[string]bool{},
		viewProtected: map[string]bool{}, viewRisky: map[string]bool{}, macroProtected: map[string]bool{},
		macroRisky: map[string]bool{}, macros: map[string]bool{}, unsafeFns: map[string]bool{}}
	dest := make([]driver.Value, 5)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			rows.Close() //nolint:errcheck
			return nil, true, err
		}
		kind, _ := dest[0].(string)
		db, _ := dest[1].(string)
		schema, _ := dest[2].(string)
		name, _ := dest[3].(string)
		text, _ := dest[4].(string)
		lower := strings.ToLower(name)
		if kind == "view" && s.isExternal(db, schema, name) {
			// Igloo's own view over files only its proxy serves, of a
			// table that policies cannot be attached to: like a table
			// without policies.
			continue
		}
		switch kind {
		case "table function":
			if !safeTableFunctions[lower] {
				sens.unsafeFns[lower] = true
			}
			continue
		case "macro":
			sens.macros[lower] = true
		case "view":
			if _, body, ok := viewParts(text); ok {
				text = body
			}
		}
		key := lower
		if kind == "view" {
			key = object{db: db, schema: schema, name: name}.key()
		}
		defs = append(defs, def{kind: kind, key: key, name: lower, toks: tokensOf(text)})
	}
	if err := rows.Close(); err != nil {
		return nil, true, err
	}
	for n := range snap.ProtectedNames() {
		sens.names[n] = true
	}
	// And those with columns that hold ids, which statements read without
	// them (see tablePlan.hidden).
	_, members := s.memberTables(ctx)
	for n := range members {
		sens.names[n] = true
	}
	mark := func(protected bool, d def) {
		switch {
		case protected && d.kind == "view":
			sens.viewProtected[d.key] = true
		case protected:
			sens.macroProtected[d.key] = true
		case d.kind == "view":
			sens.viewRisky[d.key] = true
		default:
			sens.macroRisky[d.key] = true
		}
	}
	// Views and macros that read protected tables, directly or through
	// other views and macros; then those that read files or call unsafe
	// table functions. Names are compared without their schemas, which
	// can only make more of them count, and one with names in escapes that
	// are not decoded counts as reading protected tables.
	protected, risky := map[int]bool{}, map[int]bool{}
	for changed := true; changed; {
		changed = false
		for i, d := range defs {
			if !protected[i] && (d.toks.opaque || d.toks.mentions(sens.names)) {
				protected[i], sens.names[d.name], changed = true, true, true
				mark(true, d)
			}
		}
	}
	for changed := true; changed; {
		changed = false
		for i, d := range defs {
			if !risky[i] && (d.toks.paths || d.toks.mentions(sens.unsafeFns) || d.toks.mentions(sens.risky)) {
				risky[i], sens.risky[d.name], changed = true, true, true
				mark(false, d)
			}
		}
	}
	sess.sens = sens
	return sens, true, nil
}

// quotedText is the value of a string literal or quoted name.
type quotedText struct {
	value string
	ident bool
	// escaped says it is an E'...' string, whose escapes are decoded.
	escaped bool
	// opaque says it is a U&'...' string or U&"..." name, whose escapes
	// are not decoded: it could be any name.
	opaque bool
}

// textValues returns the values of query's string literals and quoted
// names, as DuckDB reads them.
func textValues(query string) []quotedText {
	var out []quotedText
	prev := ""
	// prefixed reports whether the code before a quote ends in the prefix p
	// of a word of its own, as E'...' and U&'...' are written.
	prefixed := func(p string) bool {
		n := len(prev) - len(p)
		return n >= 0 && strings.EqualFold(prev[n:], p) && (n == 0 || !sqllex.IdentByte(prev[n-1]))
	}
	for _, seg := range sqllex.Segments(query) {
		switch {
		case (seg.Kind == sqllex.Ident || seg.Kind == sqllex.String) && len(seg.Text) >= 2 && prefixed("U&"):
			out = append(out, quotedText{value: seg.Text[1 : len(seg.Text)-1], ident: seg.Kind == sqllex.Ident, opaque: true})
		case seg.Kind == sqllex.Ident && len(seg.Text) >= 2:
			out = append(out, quotedText{value: strings.ReplaceAll(seg.Text[1:len(seg.Text)-1], `""`, `"`), ident: true})
		case seg.Kind == sqllex.String && strings.HasPrefix(seg.Text, "$"):
			if i := strings.Index(seg.Text[1:], "$"); i >= 0 {
				out = append(out, quotedText{value: strings.TrimSuffix(seg.Text[i+2:], seg.Text[:i+2])})
			}
		case seg.Kind == sqllex.String:
			body := strings.TrimSuffix(strings.TrimPrefix(seg.Text, "'"), "'")
			if prefixed("E") {
				out = append(out, quotedText{value: unescape(body), escaped: true})
			} else {
				out = append(out, quotedText{value: strings.ReplaceAll(body, "''", "'")})
			}
		}
		if seg.Kind != sqllex.Comment {
			prev = seg.Text
		}
	}
	return out
}

// unescape decodes the escapes of an E'...' string, as PostgreSQL does.
func unescape(s string) string {
	var b strings.Builder
	digits := func(i, n int, ok func(byte) bool) int {
		j := i
		for j < len(s) && j-i < n && ok(s[j]) {
			j++
		}
		return j
	}
	hex := func(c byte) bool { return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' }
	octal := func(c byte) bool { return c >= '0' && c <= '7' }
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\'' && i+1 < len(s) && s[i+1] == '\'':
			b.WriteByte('\'')
			i++
			continue
		case c != '\\' || i+1 >= len(s):
			b.WriteByte(c)
			continue
		}
		i++
		switch d := s[i]; {
		case d == 'b':
			b.WriteByte('\b')
		case d == 'f':
			b.WriteByte('\f')
		case d == 'n':
			b.WriteByte('\n')
		case d == 'r':
			b.WriteByte('\r')
		case d == 't':
			b.WriteByte('\t')
		case d == 'x' || d == 'u' || d == 'U':
			n := map[byte]int{'x': 2, 'u': 4, 'U': 8}[d]
			end := digits(i+1, n, hex)
			if end == i+1 {
				b.WriteByte(d)
				continue
			}
			v, _ := strconv.ParseUint(s[i+1:end], 16, 32)
			if d == 'x' {
				b.WriteByte(byte(v))
			} else {
				b.WriteRune(rune(v))
			}
			i = end - 1
		case octal(d):
			end := digits(i, 3, octal)
			v, _ := strconv.ParseUint(s[i:end], 8, 32)
			b.WriteByte(byte(v))
			i = end - 1
		default:
			b.WriteByte(d)
		}
	}
	return b.String()
}

// literalsOf returns the values of the strings and quoted names in query.
func literalsOf(query string) []string {
	var out []string
	for _, v := range textValues(query) {
		out = append(out, v.value)
	}
	return out
}

// defTokens are the names a text mentions: its words, quoted identifiers
// and the parts of its string literals, lower-cased, and whether a string
// or quoted name in it could name a file. escaped and opaque say it has a
// string or quoted name written with escapes (see quotedText), which can
// spell any name.
type defTokens struct {
	names           []string
	paths           bool
	escaped, opaque bool
}

func tokensOf(text string) defTokens {
	var t defTokens
	for _, w := range sqllex.CodeWords(text) {
		t.names = append(t.names, strings.ToLower(w))
	}
	for _, v := range textValues(text) {
		t.names = append(t.names, strings.ToLower(v.value))
		for _, p := range strings.Split(v.value, ".") {
			t.names = append(t.names, strings.ToLower(strings.Trim(p, `" `)))
		}
		if pathLike(v.value) || v.ident && strings.ContainsAny(v.value, "/\\") {
			t.paths = true
		}
		t.escaped = t.escaped || v.escaped
		t.opaque = t.opaque || v.opaque
	}
	return t
}

func (t defTokens) mentions(set map[string]bool) bool {
	return slices.ContainsFunc(t.names, func(n string) bool { return set[n] || set[policy.BaseName(n)] })
}

// triggers reports whether a statement may read something policies
// concern, and so must be analyzed: it names a protected table or a view
// or macro that reads one, or, for users other than admins, a table
// function or file that policies cannot see into.
func (sens *sensitivity) triggers(query string, admin bool) bool {
	t := tokensOf(query)
	if t.escaped || t.opaque || t.mentions(sens.names) {
		return true
	}
	return !admin && (t.paths || t.mentions(sens.risky) || t.mentions(sens.unsafeFns))
}

// --- statements -----------------------------------------------------------------

// guard records how policies were applied to a statement, so it is
// rewritten again before it runs once that may have changed.
type guard struct {
	policyGen, catalogGen, settingGen uint64
	// privs are the grants and roles it was rewritten for; touched says it
	// reads a protected table, which makes them matter.
	privs   *auth.Privileges
	touched bool
	// applied names the policies that applied.
	applied []string
	// nonce marks the subqueries of the rewrite.
	nonce string
}

func newNonce() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// policyRules are what users other than admins may not do at all while
// policies exist: PRAGMA and CALL of what could read data that policies
// cannot see into (PRAGMA tpch runs queries that name no table).
func policyRules(t duckdb.StmtType, query string) error {
	sc := &scanner{s: query}
	switch t {
	case duckdb.STATEMENT_TYPE_PRAGMA:
		sc.keywords("PRAGMA")
		switch name := strings.ToLower(sc.word()); name {
		case "table_info", "show", "show_tables", "show_tables_expanded", "show_databases", "database_list",
			"database_size", "version", "platform", "functions", "collations", "user_agent":
			return nil
		default:
			return denied("PRAGMA %s may read data that policies cannot check, which is refused while tables have policies", name)
		}
	case duckdb.STATEMENT_TYPE_CALL:
		sc.keywords("CALL")
		if name := strings.ToLower(sc.word()); !safeTableFunctions[name] {
			return denied("CALL %s may read data that policies cannot check, which is refused while tables have policies", name)
		}
	}
	return nil
}

// policyRewrite returns query, a statement of type t, as it must run for
// user under the policies in force, and how they applied. ran says it ran
// statements on the session's connection, after which statements prepared
// earlier must be prepared again (see reprepare).
func (s *Server) policyRewrite(ctx context.Context, sess *session, user auth.User, t duckdb.StmtType, query string, oids []uint32) (string, *guard, bool, error) {
	// Applying policies resolves names and runs DESCRIBE and sensitivity
	// checks, all of which walk the catalog's in-memory set. Most callers
	// already hold the catalog (preparing or rechecking a statement); a
	// materialized view's refresh calls this on its own. Hold the exclusive
	// side so none of it runs while a table is renamed or dropped on this
	// instance (reentrant; see holdCatalog).
	defer s.holdCatalog(sess)()
	snap := s.gov.snapshot(ctx)
	g := &guard{policyGen: s.gov.gen.Load(), catalogGen: s.catalogGeneration(), settingGen: sess.settingGen}
	// Tags and data retentions follow names too, policies or not: a
	// renamed table would escape erasure (see package erasure).
	checked := false
	ddl := t == duckdb.STATEMENT_TYPE_DROP || t == duckdb.STATEMENT_TYPE_ALTER || t == duckdb.STATEMENT_TYPE_CREATE
	if (s.erasure != nil || snap.Classified()) && ddl {
		ran, err := s.checkErasingDDL(ctx, sess, snap, t, query)
		checked = checked || ran
		if err != nil {
			return "", nil, checked, err
		}
	}
	if t == duckdb.STATEMENT_TYPE_ALTER && snap.Classified() {
		checked = true
		if err := s.checkAlterNames(ctx, sess, snap, query); err != nil {
			return "", nil, checked, err
		}
	}
	if t == duckdb.STATEMENT_TYPE_CREATE && snap.Classified() {
		if r, ok := parseTableReplace(query); ok {
			checked = true
			if err := s.checkReplaceKeeps(ctx, sess, r, nil); err != nil {
				return "", nil, checked, err
			}
		}
	}
	if snap.Empty() {
		return query, g, checked, nil
	}
	admin := user.Role == auth.RoleAdmin
	if !admin {
		if err := policyRules(t, query); err != nil {
			return "", nil, checked, err
		}
	}
	sens, ran, err := s.sensitivity(ctx, sess, snap)
	ran = ran || checked
	if err != nil {
		return "", nil, ran, err
	}
	if t == duckdb.STATEMENT_TYPE_ALTER {
		ran = true
		if err := s.checkAlter(ctx, sess, snap, sens, query, admin); err != nil {
			return "", nil, ran, err
		}
	}
	if t == duckdb.STATEMENT_TYPE_CREATE && isCreateIndex(query) {
		ran = true
		if err := s.checkCreateIndex(ctx, sess, snap, sens, query, admin); err != nil {
			return "", nil, ran, err
		}
	}
	if !sens.triggers(query, admin) {
		return query, g, ran, nil
	}
	ran = true
	privs, err := s.privileges(ctx, sess, user)
	if err != nil {
		return "", nil, ran, denied("could not load roles: %v", err)
	}
	rw, err := s.newRewriter(ctx, sess, user, privs, snap, sens)
	if err != nil {
		return "", nil, ran, err
	}
	edits, err := rw.statementEdits(t, query)
	if err != nil {
		return "", nil, ran, err
	}
	text, err := applyEdits(query, edits)
	if err != nil {
		return "", nil, ran, denied("cannot apply policies to this statement: %v", err)
	}
	if err := rw.verify(t, query, text, oids); err != nil {
		s.log.Warn("a statement rewritten for policies failed its check", "user", user.Name, "err", err)
		return "", nil, ran, err
	}
	if len(rw.applied) > 0 {
		if err := s.checkSettings(ctx, sess); err != nil {
			return "", nil, ran, err
		}
	}
	g.touched, g.applied, g.nonce, g.privs = rw.touched, rw.applied, rw.nonce, privs
	return text, g, ran, nil
}

// policySettings are the settings any user may change that change what an
// expression means: with integer_division, / divides integers as integers;
// ORDER BY and list_sort without a direction follow default_order and
// default_null_order; date parts follow the calendar. Policies apply only
// while a session keeps the values it started with. The time zone is the
// session's own, as in Snowflake (see docs/security-policies.md).
var policySettings = []string{"integer_division", "default_order", "default_null_order", "calendar"}

// settingsCheck is a session's checkSettings result, for its settings at
// generation gen.
type settingsCheck struct {
	gen uint64
	err error
}

// checkSettings refuses a statement that policies apply to while the
// session has changed one of policySettings.
func (s *Server) checkSettings(ctx context.Context, sess *session) error {
	if c := sess.settings; c != nil && c.gen == sess.settingGen {
		return c.err
	}
	defaults, err := s.gov.probe.settings(ctx)
	if err != nil {
		return denied("cannot apply policies: %v", err)
	}
	vals, err := currentSettings(ctx, sess.conn, policySettings)
	if err != nil {
		return denied("cannot apply policies: %v", err)
	}
	var changed []string
	for i, name := range policySettings {
		if vals[i] != defaults[i] {
			changed = append(changed, name)
		}
	}
	if len(changed) > 0 {
		err = denied("policies apply only with the default %s: RESET %s to read tables with policies",
			strings.Join(changed, " and "), strings.Join(changed, ", "))
	}
	sess.settings = &settingsCheck{gen: sess.settingGen, err: err}
	return err
}

// newRewriter makes the rewriter of one statement of user's.
func (s *Server) newRewriter(ctx context.Context, sess *session, user auth.User, privs *auth.Privileges,
	snap *policy.Snapshot, sens *sensitivity) (*rewriter, error) {
	r, err := s.newResolver(ctx, sess)
	if err != nil {
		return nil, err
	}
	return &rewriter{s: s, ctx: ctx, sess: sess, r: r, snap: snap, sens: sens, admin: user.Role == auth.RoleAdmin,
		nonce: newNonce(), pc: policyContext{user: user.Name, roles: append(slices.Clone(privs.Roles), string(user.Role))}}, nil
}

// verify checks a rewritten statement, text, made of query: rewriting it
// again changes nothing, and DuckDB's plan of it reads as many catalog
// tables as its text names, so it reads nothing the rewrite did not see.
func (rw *rewriter) verify(t duckdb.StmtType, query, text string, oids []uint32) error {
	if text != query {
		again, err := rw.statementEdits(t, text)
		if err != nil {
			return err
		}
		if len(again) > 0 {
			return denied("cannot apply policies to this statement with certainty")
		}
	}
	planText, parseText := policyPlanTexts(t, text)
	if planText == "" {
		return nil
	}
	typed, _ := typeArrayParams(planText, oids)
	plan, err := explainLogical(rw.ctx, rw.sess.conn, typed)
	if err != nil {
		return denied("cannot verify which tables this statement reads: %v", trimContext(err))
	}
	scans, err := rw.s.countCatalogScans(plan)
	if err != nil {
		return err
	}
	lenient := *rw.r
	lenient.lenient = true
	lenient.readsOf = map[string]readSet{}
	reads, err := lenient.analyze(parseText, nil, 0)
	if err != nil {
		return err
	}
	// The plan shows the scans of Iceberg REST catalogs' tables as it shows
	// the catalog's, which countCatalogScans counts.
	connScans, iceberg := splitScans(reads.connScans)
	if reads.scans+iceberg != scans {
		return denied("cannot verify which tables this statement reads (the plan scans %d catalog tables, the statement names %d)",
			scans, reads.scans+iceberg)
	}
	if rw.snap.ProtectsConnections() && len(rw.s.connectionCatalogs()) > 0 {
		// Tables of connections, which policies protect too: the plan
		// scans each as POSTGRES_SCAN or MYSQL_SCAN, which countCatalogScans
		// leaves out.
		ps, err := rw.s.scansOf(rw.ctx, rw.sess, plan, false)
		if err != nil {
			return err
		}
		if !sameScans(ps.connections, connScans) {
			return denied("cannot verify which tables of connections this statement reads (the plan scans %d, the statement names %d)",
				len(ps.connections), len(connScans))
		}
	}
	return nil
}

// policyPlanTexts are what to EXPLAIN of a rewritten statement and what to
// count its tables in: the statement and the query it reads with, or for
// EXPLAIN and COPY, their query. They are empty when it reads nothing.
func policyPlanTexts(t duckdb.StmtType, text string) (string, string) {
	switch t {
	case duckdb.STATEMENT_TYPE_SELECT, duckdb.STATEMENT_TYPE_RELATION:
		return text, text
	case duckdb.STATEMENT_TYPE_EXPLAIN:
		if inner, _ := explainSpan(text); !inner.empty() && selectLike(text[inner.start:inner.end]) {
			return text[inner.start:inner.end], text[inner.start:inner.end]
		}
	case duckdb.STATEMENT_TYPE_INSERT:
		if src := insertSource(text); src != "" {
			return text, src
		}
	case duckdb.STATEMENT_TYPE_UPDATE, duckdb.STATEMENT_TYPE_DELETE, statementTypeMergeInto:
		if d, ok := parseDML(t, text); ok && !d.truncate {
			return text, composeDML(text, d).String()
		}
	case duckdb.STATEMENT_TYPE_CREATE:
		if c, ok := parseCreateSpans(text); ok && !c.query.empty() {
			return text, text[c.query.start:c.query.end]
		}
	case duckdb.STATEMENT_TYPE_SET, duckdb.STATEMENT_TYPE_VARIABLE_SET:
		if e := setVariableSpan(text); !e.empty() {
			return text, "SELECT " + text[e.start:e.end]
		}
	case duckdb.STATEMENT_TYPE_COPY:
		sc := &scanner{s: text}
		if sc.keywords("COPY") {
			sc.skip()
			if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
				if end := matchParen(sc.s, sc.i); end > 0 {
					q := strings.TrimSpace(sc.s[sc.i+1 : end])
					return q, q
				}
			}
		}
	}
	return "", ""
}

// countCatalogScans counts the scans of catalog tables in an unoptimized
// plan. Scans of connections' tables are not among them; verify checks
// those with scansOf.
func (s *Server) countCatalogScans(plan any) (int, error) {
	n := 0
	catalogName := s.catalogName()
	var visit func(any) error
	visit = func(v any) error {
		switch v := v.(type) {
		case []any:
			for _, c := range v {
				if err := visit(c); err != nil {
					return err
				}
			}
		case map[string]any:
			name, _ := v["name"].(string)
			switch strings.ToLower(strings.TrimSpace(name)) {
			case "iceberg_scan":
				n++
			case "seq_scan":
				extra, _ := v["extra_info"].(map[string]any)
				table, _ := extra["Table"].(string)
				parts := splitQualified(table)
				if len(parts) != 3 {
					return denied("cannot tell which table a scan reads (%q)", table)
				}
				if strings.EqualFold(parts[0], catalogName) {
					n++
				}
			}
			return visit(v["children"])
		}
		return nil
	}
	return n, visit(plan)
}

// checkAlter refuses an ALTER TABLE that would read a policied table's rows
// or move a policy away from the name it is attached to (see
// checkAlterNames), and any form that evaluates an expression over the
// existing rows (ALTER COLUMN ... [SET DATA] TYPE ... USING, ADD/SET a
// non-constant DEFAULT, a generated column, a CHECK constraint), which runs
// over every raw row, hidden ones included, so it can read masked values
// and the rows a row access policy hides. These are refused whether or not
// the user is exempt, like a rename: unset the policies first.
func (s *Server) checkAlter(ctx context.Context, sess *session, snap *policy.Snapshot, sens *sensitivity, query string, admin bool) error {
	if err := s.checkAlterNames(ctx, sess, snap, query); err != nil {
		return err
	}
	sc := &scanner{s: query}
	if !sc.keywords("ALTER", "TABLE") {
		return nil
	}
	name, ok := sc.alterTarget()
	if !ok {
		// The target cannot be read: refuse for non-admins rather than run an
		// ALTER whose policies could not be checked.
		if admin {
			return nil
		}
		return denied("cannot determine which table this ALTER changes; it is refused while policies exist")
	}
	action := sc.rest()
	if !alterEvaluatesRows(&scanner{s: action}) {
		return nil
	}
	objects, err := s.catalogCandidates(ctx, sess, name, false)
	if err != nil {
		return err
	}
	// An expression over the rows is refused when the table is policied, or
	// when the expression names another policied table (a subquery).
	policied := slices.ContainsFunc(objects, func(o auth.Object) bool { return snap.Tagged(o.Schema, o.Table) != nil })
	if policied || sens.triggers(action, admin) {
		return denied("this ALTER TABLE evaluates an expression over the table's rows, which a policy or tag protects; unset the policies before altering it this way")
	}
	return nil
}

// checkAlterNames refuses renaming a table with policies, tags or a data
// retention, or renaming or dropping one of its protected or tagged
// columns, or changing such a column's type: they follow names, and a
// renamed table would be readable unprotected under its new name until
// every replica learned of it, and out of reach of erasure requests and
// its retention, while a column dropped and added again under its name
// would leave its old values, in older files, to a field the tags no
// longer mean.
func (s *Server) checkAlterNames(ctx context.Context, sess *session, snap *policy.Snapshot, query string) error {
	sc := &scanner{s: query}
	if !sc.keywords("ALTER", "TABLE") {
		return nil
	}
	name, ok := sc.alterTarget()
	if !ok {
		return nil
	}
	var table, column, version bool
	var col string
	switch {
	case sc.keywords("RENAME", "TO"), sc.keywords("SET", "SCHEMA"):
		table = true
	case formatVersion.MatchString(sc.rest()):
		version = true
	case sc.keywords("RENAME", "COLUMN"), sc.keywords("RENAME"),
		sc.keywords("ALTER", "COLUMN"), sc.keywords("ALTER"),
		sc.keywords("DROP", "COLUMN"), sc.keywords("DROP"):
		sc.keywords("IF", "EXISTS")
		c, ok := sc.name()
		if !ok || len(c) != 1 {
			return nil
		}
		col, column = c[0], true
	default:
		return nil
	}
	objects, err := s.catalogCandidates(ctx, sess, name, false)
	if err != nil {
		return err
	}
	for _, o := range objects {
		rules := snap.Tagged(o.Schema, o.Table)
		if rules == nil {
			continue
		}
		if table {
			return denied("table %s has policies, tags or a data retention, which stay with its name; unset them before renaming it", o)
		}
		if version {
			return denied("table %s has policies or tags; it stays at Iceberg format version 2, as erasure requests cannot rewrite tables of version 3 yet", o)
		}
		inFilter := rules.Filter != nil && slices.ContainsFunc(rules.Filter.Columns, func(c string) bool { return strings.EqualFold(c, col) })
		if column && (rules.Column(col) != nil || inFilter) {
			return denied("column %s of %s has a policy or tags, which stay with its name; unset them before renaming or dropping it or changing its type", col, o)
		}
	}
	return nil
}

// formatVersion finds a change of a table's Iceberg format version in
// what follows ALTER TABLE's name, or in CREATE TABLE's options.
var formatVersion = regexp.MustCompile(`(?i)format[-_]version`)

// catalogTarget is the catalog table a name written in a DDL statement
// creates or drops, existing or not: as DuckDB resolves such a name, in
// the session's current schema when it is not qualified. ok is false for a
// name in another database, or a temporary table.
func (s *Server) catalogTarget(ctx context.Context, sess *session, parts []string) (auth.Object, bool, error) {
	catalogName := s.catalogName()
	switch len(parts) {
	case 3:
		return auth.Object{Schema: parts[1], Table: parts[2]}, strings.EqualFold(parts[0], catalogName), nil
	case 2:
		switch strings.ToLower(parts[0]) {
		case "temp", "system", strings.ToLower(s.engine.LocalDatabase()):
			return auth.Object{}, false, nil
		}
		return auth.Object{Schema: parts[0], Table: parts[1]}, true, nil
	case 1:
		// isTemporary lists with duckdb_tables()/duckdb_views(), which walk
		// the catalog's in-memory set; hold the exclusive side (reentrant;
		// see holdCatalog).
		release := s.holdCatalog(sess)
		temp, err := isTemporary(ctx, sess.conn, parts[0])
		release()
		if err != nil || temp {
			return auth.Object{}, false, err
		}
		db, current, err := currentSchema(ctx, sess.conn)
		if err != nil {
			return auth.Object{}, false, err
		}
		return auth.Object{Schema: current, Table: parts[0]}, strings.EqualFold(db, catalogName), nil
	}
	return auth.Object{}, false, nil
}

// targets are the catalog tables a name in a DDL statement may mean: the
// one DuckDB creates or drops under it, and those it resolves to along the
// session's path.
func (s *Server) targets(ctx context.Context, sess *session, name []string) ([]auth.Object, error) {
	objects, err := s.catalogCandidates(ctx, sess, name, false)
	if err != nil {
		return nil, err
	}
	o, ok, err := s.catalogTarget(ctx, sess, name)
	if err != nil {
		return nil, err
	}
	if ok && !slices.ContainsFunc(objects, func(c auth.Object) bool { return strings.EqualFold(c.String(), o.String()) }) {
		objects = append(objects, o)
	}
	return objects, nil
}

// erasing reports whether an erasure request that is not over erases from
// table o (see erasure.Service.Erasing).
func (s *Server) erasing(ctx context.Context, o auth.Object) (bool, error) {
	if s.erasure == nil {
		return false, nil
	}
	erasing, err := s.erasure.Erasing(ctx, o.Schema, o.Table)
	if err != nil {
		return false, fmt.Errorf("check whether an erasure request erases from %s: %w", o, err)
	}
	return erasing, nil
}

// checkErasingDDL refuses what would take a table out of reach of an
// erasure request that is not over, or make a table one that requests
// cannot rewrite, admins included: dropping a table the request erases
// from, or the schema that holds it; renaming it, or another table to its
// name, which the request would take for it; upgrading it to Iceberg format
// version 3 (ALTER TABLE, CREATE OR REPLACE TABLE), or creating a table of
// version 3 under its name or a tagged table's; and replacing it (CREATE
// OR REPLACE TABLE), which drops it. Whatever a statement's text, the same
// rules hold for every change it makes to the catalog, which Check sees
// (see catalogguard.go), table properties set by set_iceberg_table_properties
// included; the catalog refuses version 3 too (see package catalog). It
// reports whether it ran statements on the session's connection.
func (s *Server) checkErasingDDL(ctx context.Context, sess *session, snap *policy.Snapshot, t duckdb.StmtType, query string) (bool, error) {
	ran := false
	resolve := func(name []string) ([]auth.Object, error) {
		ran = true
		return s.targets(ctx, sess, name)
	}
	// refuse refuses o with reason if an open request erases from it.
	refuse := func(name []string, reason string) error {
		objects, err := resolve(name)
		if err != nil {
			return err
		}
		for _, o := range objects {
			if erasing, err := s.erasing(ctx, o); err != nil {
				return err
			} else if erasing {
				return denied("an erasure request erases from %s; "+reason, o)
			}
		}
		return nil
	}
	sc := &scanner{s: query}
	switch {
	case t == duckdb.STATEMENT_TYPE_DROP && sc.keywords("DROP", "TABLE"):
		sc.keywords("IF", "EXISTS")
		for {
			name, ok := sc.name()
			if !ok {
				break
			}
			if err := refuse(name, "it cannot be dropped until the request is over"); err != nil {
				return ran, err
			}
			if !sc.punct(',') {
				break
			}
		}
	case t == duckdb.STATEMENT_TYPE_DROP && sc.keywords("DROP", "SCHEMA"):
		sc.keywords("IF", "EXISTS")
		name, ok := sc.name()
		if ok && s.erasure != nil && (len(name) == 1 || len(name) == 2 && strings.EqualFold(name[0], s.catalogName())) {
			erasing, err := s.erasure.ErasingIn(ctx, name[len(name)-1])
			if err != nil {
				return ran, fmt.Errorf("check whether an erasure request erases from a table of %s: %w", name[len(name)-1], err)
			}
			if erasing {
				return ran, denied("an erasure request erases from a table of schema %s, which cannot be dropped until the request is over", name[len(name)-1])
			}
		}
	case t == duckdb.STATEMENT_TYPE_ALTER && sc.keywords("ALTER", "TABLE"):
		name, ok := sc.alterTarget()
		if !ok {
			break
		}
		var to func(o auth.Object) auth.Object
		switch {
		case sc.keywords("RENAME", "TO"):
			if n, ok := sc.name(); ok {
				to = func(o auth.Object) auth.Object { return auth.Object{Schema: o.Schema, Table: n[len(n)-1]} }
			}
		case sc.keywords("SET", "SCHEMA"):
			if n, ok := sc.name(); ok {
				to = func(o auth.Object) auth.Object { return auth.Object{Schema: n[len(n)-1], Table: o.Table} }
			}
		case formatVersion.MatchString(sc.rest()):
			return ran, refuse(name, "it stays at its Iceberg format version until the request is over, as erasure requests "+
				"cannot rewrite tables of version 3 yet")
		default:
			return ran, nil
		}
		if err := refuse(name, "it cannot be renamed or moved until the request is over"); err != nil {
			return ran, err
		}
		if to == nil {
			break
		}
		objects, err := resolve(name)
		if err != nil {
			return ran, err
		}
		for _, o := range objects {
			dest := to(o)
			if erasing, err := s.erasing(ctx, dest); err != nil {
				return ran, err
			} else if erasing {
				return ran, denied("an erasure request erases from %s; no other table can take its name until the request is over", dest)
			}
		}
	case t == duckdb.STATEMENT_TYPE_CREATE:
		replace := sc.keywords("CREATE", "OR", "REPLACE", "TABLE")
		if !replace && !sc.keywords("CREATE", "TABLE") {
			break
		}
		sc.keywords("IF", "NOT", "EXISTS")
		name, ok := sc.name()
		if !ok {
			break
		}
		if replace {
			// Replacing drops the table, as DROP TABLE does.
			if err := refuse(name, "it cannot be replaced until the request is over"); err != nil {
				return ran, err
			}
		}
		if !formatVersion.MatchString(sc.rest()) {
			break
		}
		objects, err := resolve(name)
		if err != nil {
			return ran, err
		}
		for _, o := range objects {
			erasing, err := s.erasing(ctx, o)
			if err != nil {
				return ran, err
			}
			if erasing || snap.Tagged(o.Schema, o.Table) != nil {
				return ran, denied("%s has tags, or an erasure request erases from it; a table under its name stays at Iceberg format version 2, "+
					"as erasure requests cannot rewrite tables of version 3 yet", o)
			}
		}
	}
	return ran, nil
}

// checkReplaceKeeps refuses a CREATE OR REPLACE TABLE whose new table
// lacks a column of the old one that has a policy or tags, or that its row
// access policy reads, or gives one another kind of values (text, a UUID,
// bytes, an integer, a date...): they follow names, so the column's values
// would be left unmasked, or out of reach of erasure requests, under
// another name, and identifiers would compare with them otherwise, or not
// at all. Keep the column as it is, or unset them first. Nor may it make a
// table with tags one of Iceberg format version 3, which erasure requests
// cannot rewrite yet. have are the new table's columns, or nil to work
// them out from the statement: its query's, or its column definitions'.
func (s *Server) checkReplaceKeeps(ctx context.Context, sess *session, r tableReplace, have []columnType) error {
	snap := s.gov.snapshot(ctx)
	if !snap.Classified() {
		return nil
	}
	objects, err := s.catalogCandidates(ctx, sess, r.target, false)
	if err != nil {
		return err
	}
	for _, o := range objects {
		rules := snap.Tagged(o.Schema, o.Table)
		if rules == nil {
			continue
		}
		if formatVersion.MatchString(r.rest) {
			return denied("table %s has policies or tags; the table replacing it stays at Iceberg format version 2, "+
				"as erasure requests cannot rewrite tables of version 3 yet", o)
		}
		if have == nil {
			if have, err = s.replacementColumns(ctx, sess, r); err != nil {
				return denied("%s has policies or tags, whose columns a table replacing it must keep, and Igloo cannot tell the new table's columns: %v", o, err)
			}
		}
		old, err := describeTable(ctx, sess, quoteName(r.target))
		if err != nil {
			return err
		}
		keep := slices.Collect(maps.Keys(rules.Columns))
		if rules.Filter != nil {
			keep = append(keep, rules.Filter.Columns...)
		}
		slices.Sort(keep)
		find := func(cols []columnType, name string) (columnType, bool) {
			i := slices.IndexFunc(cols, func(c columnType) bool { return strings.EqualFold(c.name, name) })
			if i < 0 {
				return columnType{}, false
			}
			return cols[i], true
		}
		for _, col := range keep {
			now, ok := find(have, col)
			if !ok {
				return denied("column %s of %s has a policy or tags, which stay with its name, and the table replacing it has no column %[1]s: "+
					"keep the column, or unset them first", col, o)
			}
			if was, ok := find(old, col); ok && valueKind(was.typ) != valueKind(now.typ) {
				return denied("column %s of %s has a policy or tags, and the table replacing it makes it %s, from %s: "+
					"keep its kind of values, or unset them first", col, o, now.typ, was.typ)
			}
		}
	}
	return nil
}

// valueKind is the kind of values a DuckDB type holds, as identifiers and
// policies compare with them: text, a UUID, bytes, an integer, or the
// type's own name.
func valueKind(typ string) string {
	word := strings.ToUpper(strings.TrimSpace(typ))
	if i := strings.IndexAny(word, "( "); i >= 0 {
		word = word[:i]
	}
	switch word {
	case "VARCHAR", "TEXT", "STRING", "CHAR", "BPCHAR", "NVARCHAR":
		return "text"
	case "UUID":
		return "uuid"
	case "BLOB", "BYTEA", "BINARY", "VARBINARY":
		return "bytes"
	case "TINYINT", "SMALLINT", "INTEGER", "INT", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT",
		"INT1", "INT2", "INT4", "INT8", "LONG", "SHORT", "SIGNED":
		return "integer"
	}
	return word
}

// replacementColumns are the columns of the table a CREATE OR REPLACE TABLE
// makes, with their types: those its query returns, as DuckDB describes
// it, or those its column definitions name.
func (s *Server) replacementColumns(ctx context.Context, sess *session, r tableReplace) ([]columnType, error) {
	sc := &scanner{s: r.rest}
	if sc.keywords("AS") {
		return describeTable(ctx, sess, strings.TrimRight(strings.TrimSpace(sc.rest()), ";"))
	}
	if !sc.punct('(') {
		return nil, errors.New("no column definitions or query")
	}
	var cols []columnType
	for {
		switch {
		case sc.keywords("CONSTRAINT"), sc.keywords("PRIMARY"), sc.keywords("UNIQUE"), sc.keywords("CHECK"), sc.keywords("FOREIGN"):
		default:
			name, ok := sc.name()
			if !ok || len(name) != 1 {
				return nil, errors.New("cannot read the column definitions")
			}
			cols = append(cols, columnType{name[0], sc.word()})
		}
		last, err := skipDefinition(sc)
		if err != nil || last {
			return cols, err
		}
	}
}

// skipDefinition moves sc past the rest of a column or constraint
// definition, nested parentheses and quoted text included, and its comma,
// reporting whether it was the last, before the closing parenthesis.
func skipDefinition(sc *scanner) (last bool, err error) {
	depth := 0
	for {
		sc.skip()
		if sc.i >= len(sc.s) {
			return false, errors.New("unterminated column definitions")
		}
		switch c := sc.s[sc.i]; {
		case c == '\'' || c == '"':
			end := strings.IndexByte(sc.s[sc.i+1:], c)
			if end < 0 {
				return false, errors.New("unterminated quoted text")
			}
			sc.i += end + 1
		case c == '(':
			depth++
		case c == ')' && depth == 0:
			return true, nil
		case c == ')':
			depth--
		case c == ',' && depth == 0:
			sc.i++
			return false, nil
		}
		sc.i++
	}
}

// alterEvaluatesRows reports whether an ALTER TABLE action (the text after
// the table name) evaluates an expression over the table's existing rows: a
// type change (which casts every value), a non-constant DEFAULT, a generated
// column, or a CHECK constraint. A constant DEFAULT reads no column, so it is
// allowed; DROP COLUMN, DROP DEFAULT and NOT NULL changes read no values.
func alterEvaluatesRows(sc *scanner) bool {
	switch {
	case sc.keywords("ADD"):
		sc.keywords("COLUMN")
		sc.keywords("IF", "NOT", "EXISTS")
		if sc.keywords("CONSTRAINT") {
			sc.name() //nolint:errcheck
		}
		switch {
		case sc.keywords("CHECK"):
			return true
		case sc.keywords("PRIMARY", "KEY"), sc.keywords("UNIQUE"), sc.keywords("FOREIGN", "KEY"):
			return false
		}
		return columnDefEvaluates(sc.rest())
	case sc.keywords("ALTER"):
		sc.keywords("COLUMN")
		if _, ok := sc.name(); !ok {
			return false
		}
		switch {
		case sc.keywords("SET", "DATA", "TYPE"), sc.keywords("SET", "TYPE"), sc.keywords("TYPE"):
			return true
		case sc.keywords("SET", "DEFAULT"):
			return !constantExpr(sc.rest())
		}
		return false
	}
	return false
}

// columnDefEvaluates reports whether a column definition (the text after
// ADD [COLUMN] name) carries an expression evaluated over existing rows: a
// generated column (AS / GENERATED), a CHECK constraint, or a DEFAULT that is
// not a constant. The text after DEFAULT includes any trailing clause, so a
// trailing CHECK makes it non-constant too.
func columnDefEvaluates(def string) bool {
	sc := &scanner{s: def}
	for {
		sc.skip()
		if sc.i >= len(sc.s) {
			return false
		}
		switch w := sc.word(); w {
		case "":
			sc.i++
		case "AS", "GENERATED", "CHECK":
			return true
		case "DEFAULT":
			sc.i += len(w)
			return !constantExpr(sc.rest())
		default:
			sc.i += len(w)
		}
	}
}

// isCreateIndex reports whether query is CREATE [OR REPLACE] [UNIQUE] INDEX.
func isCreateIndex(query string) bool {
	sc := &scanner{s: query}
	if !sc.keywords("CREATE") {
		return false
	}
	sc.keywords("OR", "REPLACE")
	sc.keywords("UNIQUE")
	return sc.keywords("INDEX")
}

// checkCreateIndex refuses CREATE INDEX on a table with a policy or tag, or
// on an expression that names one: building the index reads every row, and an
// expression index evaluates over them, so it can read protected values and
// hidden rows. Unset the policies first.
func (s *Server) checkCreateIndex(ctx context.Context, sess *session, snap *policy.Snapshot, sens *sensitivity, query string, admin bool) error {
	sc := &scanner{s: query}
	if !sc.keywords("CREATE") {
		return nil
	}
	sc.keywords("OR", "REPLACE")
	sc.keywords("UNIQUE")
	if !sc.keywords("INDEX") {
		return nil
	}
	sc.keywords("IF", "NOT", "EXISTS")
	if _, ok := sc.name(); !ok {
		return nil
	}
	if !sc.keywords("ON") {
		return nil
	}
	sc.keywords("ONLY")
	sc.skip()
	paren := sc.i < len(sc.s) && sc.s[sc.i] == '('
	if paren {
		sc.i++
	}
	name, ok := sc.name()
	if !ok {
		if admin {
			return nil
		}
		return denied("cannot determine which table this index is on; it is refused while policies exist")
	}
	if paren && sc.i < len(sc.s) && sc.s[sc.i] == ')' {
		sc.i++
	}
	objects, err := s.catalogCandidates(ctx, sess, name, false)
	if err != nil {
		return err
	}
	for _, o := range objects {
		if snap.Tagged(o.Schema, o.Table) != nil {
			return denied("cannot create an index on %s, which a policy or tag protects; unset the policies first", o)
		}
	}
	if sens.triggers(sc.rest(), admin) {
		return denied("this index evaluates an expression over a table that a policy or tag protects; unset the policies first")
	}
	return nil
}

// checkWarehouseFiles refuses, for users other than admins while
// policies exist, a statement that names a file of the warehouse in a
// string: DuckDB opens the files a function reads as soon as it binds the
// statement, before any check that needs the bound statement.
func (s *Server) checkWarehouseFiles(ctx context.Context, user auth.User, query string) error {
	g := s.gov
	if user.Role == auth.RoleAdmin || g.warehouse == "" && g.external == "" || g.snapshot(ctx).Empty() {
		return nil
	}
	for _, lit := range literalsOf(query) {
		wild := strings.ContainsAny(lit, "*?[{")
		if g.warehouse != "" && (strings.HasPrefix(lit, g.warehouse) || strings.HasPrefix(g.warehouse, lit) && wild) {
			return denied("reading %q: the warehouse's files are read through its tables, which policies apply to", lit)
		}
		l := strings.ToLower(lit)
		if g.external != "" && (strings.HasPrefix(l, g.external) || strings.HasPrefix(g.external, l) && wild) {
			return denied("reading %q: external tables' files are read through the external tables", lit)
		}
	}
	return nil
}

// checkLoadLocation confines where a non-admin COPY INTO or pipe may load
// from: a plain path in an allowed location that cannot climb out of it
// (see checkPath), and, while a table has a policy, not the warehouse, whose
// files are the tables' own and read through them. The INSERT of each file
// is checked as the user's own statement besides.
func (s *Server) checkLoadLocation(ctx context.Context, user auth.User, loc string) error {
	if user.Role == auth.RoleAdmin {
		return nil
	}
	return s.checkPath(loc, s.gov.snapshot(ctx).Empty())
}

// isExternal reports whether the view db.schema.name is an external
// table's (Delta Sharing).
func (s *Server) isExternal(db, schema, name string) bool {
	if s.external == nil || !strings.EqualFold(db, s.engine.LocalDatabase()) {
		return false
	}
	_, ok := s.external.IsExternal(schema, name)
	return ok
}

// protectBeforePrepare refuses a query DuckDB runs as several statements,
// all but the last while preparing it (or a PIVOT's query that finds its
// columns), when it names something policies concern: only a single
// statement can be rewritten before it runs.
func (s *Server) protectBeforePrepare(ctx context.Context, sess *session, user auth.User, query string, pivot bool) error {
	snap := s.gov.snapshot(ctx)
	if snap.Empty() {
		return nil
	}
	admin := user.Role == auth.RoleAdmin
	if !pivot {
		if !admin || !strings.Contains(sqllex.CodeOnly(strings.TrimRight(strings.TrimSpace(query), "; \t\r\n")), ";") {
			return nil
		}
		if n, err := s.engine.CountStatements(query); err != nil || n < 2 {
			return nil
		}
	}
	sens, _, err := s.sensitivity(ctx, sess, snap)
	if err != nil {
		return err
	}
	if sens.triggers(query, admin) {
		if pivot {
			return denied("a PIVOT that takes its columns from the data runs a query before Igloo can apply policies to it; list the columns instead, as in PIVOT ... ON col IN ('a', 'b')")
		}
		return denied("send statements that read tables with policies one at a time")
	}
	return nil
}

// adopt replaces the statement with text, its rewrite, which must have the
// same result columns and parameters.
func (st *statement) adopt(ctx context.Context, conn *duckdb.Conn, text string) error {
	fresh, err := prepare(ctx, conn, text, st.clientOIDs)
	if err != nil {
		return pgError(trimContext(err))
	}
	if !sameShape(fresh, st, false) && !sameShape(fresh, withoutHidden(st), true) {
		_ = fresh.close()
		return pgerror.WithCode(errors.New("cannot apply policies to this statement: they change its result's columns"),
			codes.FeatureNotSupported)
	}
	old := st.stmt
	st.stmt, st.query, st.prepared, st.encoders, st.columns = fresh.stmt, text, fresh.prepared, fresh.encoders, fresh.columns
	return old.Close()
}

// trimContext drops the statement text DuckDB quotes after an error,
// which for a rewritten statement would show the policies' SQL.
func trimContext(err error) error {
	var de *duckdb.Error
	if errors.As(err, &de) {
		if i := strings.Index(de.Msg, "\n\nLINE "); i >= 0 {
			return &duckdb.Error{Type: de.Type, Msg: de.Msg[:i]}
		}
	}
	return err
}

// recheckPolicies rewrites a prepared statement again before it runs when
// the policies, the catalog, the session's settings or, for a statement
// that reads protected tables, the user's roles changed since it was
// rewritten. It reports whether the statement changed.
func (s *Server) recheckPolicies(ctx context.Context, sess *session, st *statement) (bool, error) {
	g := st.guard
	if g == nil {
		return false, nil
	}
	s.gov.snapshot(ctx)
	user, err := s.refreshUser(ctx, sess)
	if err != nil {
		return false, err
	}
	stale := g.policyGen != s.gov.gen.Load() || g.catalogGen != s.catalogGeneration() || g.settingGen != sess.settingGen
	if !stale && g.touched {
		privs, err := s.privileges(ctx, sess, user)
		stale = err != nil || privs != g.privs
	}
	if !stale {
		return false, nil
	}
	original := st.original
	if original == "" {
		original = st.query
	}
	text, ng, ran, err := s.policyRewrite(ctx, sess, user, st.stmtType, original, st.clientOIDs)
	if err == nil {
		var ranWrites bool
		ranWrites, err = s.keptWrites(ctx, sess, user, st.stmtType, original)
		ran = ran || ranWrites
	}
	if err != nil {
		s.log.Info("statement denied", "user", user.Name, "role", user.Role, "err", err)
		return false, err
	}
	// The guard changes only once the statement does: a statement that
	// failed to change stays stale, and is checked again before it runs.
	if text == st.query {
		if ran {
			if err := st.reprepare(ctx, sess.conn); err != nil {
				return false, pgError(err)
			}
		}
		st.guard = ng
		return false, nil
	}
	if err := st.adopt(ctx, sess.conn, text); err != nil {
		return false, err
	}
	st.guard, st.original = ng, ""
	if text != original {
		st.original = original
	}
	return true, nil
}

// recheck checks a prepared statement again before it runs: its policies,
// then its grants.
func (s *Server) recheck(ctx context.Context, sess *session, st *statement) error {
	changed, err := s.recheckPolicies(ctx, sess, st)
	if err != nil {
		return err
	}
	return s.recheckGrants(ctx, sess, st, changed)
}

// sourceText is the statement as the client sent it, before policies.
func (st *statement) sourceText() string {
	if st.original != "" {
		return st.original
	}
	return st.query
}

// policies names the policies that applied to the statement.
func (st *statement) policies() []string {
	if st.guard == nil {
		return nil
	}
	return st.guard.applied
}

// policyMarker is the alias prefix of the tables policies read in the
// statement's rewrite.
func (st *statement) policyMarker() string {
	if st.guard == nil || st.guard.nonce == "" {
		return ""
	}
	return "__igloo_p_" + st.guard.nonce + "_"
}

// withoutHidden is st's shape without the columns that hold ID domains'
// ids, which a star expanded as written and the rewrite leaves out (see
// tablePlan.hidden).
func withoutHidden(st *statement) *statement {
	out := *st
	out.columns = nil
	for _, c := range st.columns {
		if !iddomain.IsHidden(c.Name) {
			out.columns = append(out.columns, c)
		}
	}
	return &out
}
