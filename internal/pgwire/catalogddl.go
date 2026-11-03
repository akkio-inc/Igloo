package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/ddl"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// viewCommand is CREATE [OR REPLACE] VIEW or DROP VIEW on a view in the
// Iceberg catalog, which Igloo carries out itself (see package ddl).
type viewCommand struct {
	drop, replace, ifExists, ifNotExists bool
	name                                 []string
	columns                              []string
	query                                string
}

// parseViewCommand recognizes CREATE [OR REPLACE] VIEW [IF NOT EXISTS] name
// [(columns)] AS query, and DROP VIEW [IF EXISTS] name. Temporary views are
// left to DuckDB.
func parseViewCommand(query string) (viewCommand, bool) {
	sc := &scanner{s: query}
	var c viewCommand
	switch {
	case sc.keywords("CREATE", "OR", "REPLACE", "VIEW"):
		c.replace = true
	case sc.keywords("CREATE", "VIEW"):
	case sc.keywords("DROP", "VIEW"):
		c.drop = true
		c.ifExists = sc.keywords("IF", "EXISTS")
		name, ok := sc.name()
		if !ok {
			return c, false
		}
		c.name = name
		// Iceberg views record no dependents: a view that reads a dropped
		// one fails when queried, whichever is given.
		_ = sc.keywords("RESTRICT") || sc.keywords("CASCADE")
		return c, sc.done()
	default:
		return c, false
	}
	c.ifNotExists = sc.keywords("IF", "NOT", "EXISTS")
	name, ok := sc.name()
	if !ok {
		return c, false
	}
	c.name = name
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
		sc.i++
		for {
			col, ok := sc.name()
			if !ok || len(col) != 1 {
				return c, false
			}
			c.columns = append(c.columns, col[0])
			sc.skip()
			if sc.i < len(sc.s) && sc.s[sc.i] == ',' {
				sc.i++
				continue
			}
			if sc.i < len(sc.s) && sc.s[sc.i] == ')' {
				sc.i++
				break
			}
			return c, false
		}
	}
	if !sc.keywords("AS") {
		return c, false
	}
	c.query = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sc.rest()), ";"))
	return c, c.query != ""
}

// alterRelation is ALTER TABLE or ALTER VIEW. PostgreSQL lets ALTER TABLE
// rename views too, and dbt does so.
type alterRelation struct {
	view, ifExists bool
	name           []string
	// renameTo is the new name of ALTER ... RENAME TO, or empty for any
	// other change.
	renameTo string
}

func parseAlterRelation(query string) (alterRelation, bool) {
	sc := &scanner{s: query}
	var a alterRelation
	switch {
	case sc.keywords("ALTER", "VIEW"):
		a.view = true
	case sc.keywords("ALTER", "TABLE"):
	default:
		return a, false
	}
	a.ifExists = sc.keywords("IF", "EXISTS")
	name, ok := sc.name()
	if !ok {
		return a, false
	}
	a.name = name
	if sc.keywords("RENAME", "TO") {
		if to, ok := sc.name(); ok && len(to) == 1 && sc.done() {
			a.renameTo = to[0]
		}
	}
	return a, true
}

// commentCommand is COMMENT ON SCHEMA, TABLE, VIEW or COLUMN.
type commentCommand struct {
	kind string
	// name is the schema, or the table or view, then the column for
	// COLUMN.
	name []string
	text *string
}

func parseCommentCommand(query string) (commentCommand, bool) {
	sc := &scanner{s: query}
	if !sc.keywords("COMMENT", "ON") {
		return commentCommand{}, false
	}
	var c commentCommand
	switch {
	case sc.keywords("SCHEMA"):
		c.kind = "SCHEMA"
	case sc.keywords("TABLE"):
		c.kind = "TABLE"
	case sc.keywords("VIEW"):
		c.kind = "VIEW"
	case sc.keywords("COLUMN"):
		c.kind = "COLUMN"
	default:
		return c, false
	}
	name, ok := sc.name()
	if !ok || (c.kind == "COLUMN" && len(name) < 2) || (c.kind == "SCHEMA" && len(name) > 2) {
		return c, false
	}
	c.name = name
	if !sc.keywords("IS") {
		return c, false
	}
	if c.text, ok = sc.literal(); !ok {
		return c, false
	}
	return c, sc.done()
}

// catalogDDL carries out view and comment statements that target the
// Iceberg catalog, and renames of its views. ok is false for anything
// else, including statements on temporary or local objects and ALTER TABLE
// on the catalog's tables, which DuckDB handles itself.
func (s *Server) catalogDDL(ctx context.Context, sess *session, user auth.User, query string) (wire.PreparedStatements, bool, error) {
	if s.ddl == nil {
		return nil, false, nil
	}
	view, isView := parseViewCommand(query)
	comment, isComment := parseCommentCommand(query)
	alter, isAlter := parseAlterRelation(query)
	if !isView && !isComment && !isAlter {
		return nil, false, nil
	}
	// DuckDB's parser refuses COMMENT ON SCHEMA, which it cannot carry
	// out; parseCommentCommand saw it end after its comment.
	if n, err := s.engine.CountStatements(query); (err != nil || n != 1) && !(isComment && comment.kind == "SCHEMA") {
		return nil, false, nil
	}

	var nameParts []string
	switch {
	case isView:
		nameParts = view.name
	case isAlter:
		nameParts = alter.name
	default:
		nameParts = comment.name
		if comment.kind == "COLUMN" {
			nameParts = nameParts[:len(nameParts)-1]
		}
	}
	var (
		schema, name string
		err          error
	)
	if isComment && comment.kind == "SCHEMA" {
		var ok bool
		if schema, ok, err = s.resolveCatalogSchema(ctx, sess, nameParts); err != nil || !ok {
			return nil, false, err
		}
	} else {
		var ok bool
		schema, name, ok, err = s.resolveCatalogName(ctx, sess, nameParts)
		if err != nil || !ok {
			return nil, false, err
		}
	}

	if isView && view.drop && view.ifExists && !s.ddl.IsView(schema, name) {
		// Nothing in the catalog to drop: DuckDB makes it a no-op.
		return nil, false, nil
	}
	if isAlter {
		switch {
		case !s.ddl.IsView(schema, name) && !alter.view:
			// The catalog's tables are DuckDB's to alter.
			return nil, false, nil
		case alter.renameTo == "":
			return nil, true, pgerror.WithCode(errors.New("ALTER VIEW on the Iceberg catalog supports only RENAME TO; use CREATE OR REPLACE VIEW, or DROP VIEW and CREATE VIEW"),
				codes.FeatureNotSupported)
		}
	}

	start := time.Now()
	target := []auth.Object{{Schema: schema, Table: name}}
	if name == "" {
		// A schema's comment changes no table.
		target = nil
	}
	reserved := func(name string) bool { return strings.EqualFold(schema, engine.HistorySchema) && isHistoryView(name) }
	if isView && !view.drop && reserved(name) || isAlter && (reserved(name) || reserved(alter.renameTo)) {
		err := pgerror.WithCode(fmt.Errorf("%s.%s is reserved for Igloo's own views", schema, name), codes.ReservedName)
		s.recordQuery(sess, queryRecord{text: query, start: start, err: err, rows: -1})
		return nil, true, err
	}

	if err := forbidden(query); err != nil {
		// Checked apart from authorize: grants must not allow it either.
		s.metrics.query(time.Now(), err)
		s.recordQuery(sess, queryRecord{text: query, start: start, err: err, rows: -1})
		return nil, true, err
	}
	stmtType := duckdb.STATEMENT_TYPE_CREATE
	switch {
	case view.drop:
		stmtType = duckdb.STATEMENT_TYPE_DROP
	case isAlter:
		stmtType = duckdb.STATEMENT_TYPE_ALTER
	}
	if err := authorize(user.Role, stmtType, query); err != nil {
		// Creating, replacing, renaming, dropping and commenting on the
		// catalog's views and tables is CREATE on their schema. Their
		// owners may comment on them too.
		if isComment && s.mayComment(ctx, sess, user, auth.Object{Schema: schema, Table: name}) {
			err = nil
		} else if comment.kind == "SCHEMA" {
			err = needError(user, need{auth.PrivilegeCreate, auth.Object{Schema: schema}})
			if user.Role != auth.RoleNone {
				err = pgerror.WithCode(fmt.Errorf("permission denied for %s: commenting on schema %s needs the writer role or owning it",
					user.Name, schema), codes.InsufficientPrivilege)
			}
		} else {
			err = s.authorizeSchemaCreate(ctx, sess, user, schema, err)
		}
		if err != nil {
			s.log.Info("statement denied", "user", user.Name, "role", user.Role, "err", err)
			s.metrics.query(time.Now(), err)
			s.recordQuery(sess, queryRecord{text: query, start: start, err: err, rows: -1})
			return nil, true, err
		}
	}
	if isView && !view.drop {
		// Authorizing a view's reads rewrites and analyzes its body, which
		// looks the catalog's tables up (see holdCatalog).
		relist := s.shareCatalog(sess)
		err := s.syncingViews(ctx, func() error {
			return s.authorizeViewReads(ctx, sess, user, s.rewrite(ctx, sess, user, view.query))
		})
		relist()
		if err != nil {
			s.log.Info("statement denied", "user", user.Name, "role", user.Role, "err", err)
			s.metrics.query(time.Now(), err)
			s.recordQuery(sess, queryRecord{text: query, start: start, err: err, rows: -1})
			return nil, true, err
		}
	}

	var run func(context.Context) error
	var tag string
	switch {
	case isComment:
		target := ddl.CommentTarget{Kind: comment.kind, Schema: schema, Name: name, Text: comment.text}
		if comment.kind == "COLUMN" {
			target.Column = comment.name[len(comment.name)-1]
		}
		run = func(ctx context.Context) error { return s.ddl.Comment(ctx, target) }
		tag = "COMMENT"
	case isAlter:
		run = func(ctx context.Context) error {
			if alter.ifExists && !s.ddl.IsView(schema, name) {
				return nil
			}
			return s.ddl.RenameView(ctx, schema, name, alter.renameTo)
		}
		tag = "ALTER TABLE"
		if alter.view {
			tag = "ALTER VIEW"
		}
	case view.drop:
		run = func(ctx context.Context) error { return s.ddl.DropView(ctx, schema, name, view.ifExists) }
		tag = "DROP VIEW"
	default:
		run = func(ctx context.Context) error {
			if view.ifNotExists && s.ddl.IsView(schema, name) {
				return nil
			}
			return s.syncingViews(ctx, func() error {
				return s.ddl.CreateView(ctx, sess.conn, schema, name, view.columns, view.query, view.replace)
			})
		}
		tag = "CREATE VIEW"
	}

	return wire.Prepared(wire.NewStatement(func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		start := time.Now()
		record := func(err error) error {
			s.recordQuery(sess, queryRecord{text: query, start: start, err: err, rows: -1, written: target})
			return err
		}
		if err := sess.checkAborted(query); err != nil {
			return record(err)
		}
		// Creating or renaming a view changes the catalog (see holdCatalog).
		releaseCatalog := s.holdCatalog(sess)
		defer releaseCatalog()
		err := sess.runCancelable(ctx, query, func(ctx context.Context) error {
			return sess.outsideTransaction(ctx, func() error { return run(ctx) })
		})
		s.metrics.query(start, err)
		sess.observe(nil, err)
		if err != nil {
			return record(catalogDDLError(err))
		}
		var gerr error
		switch {
		case view.drop:
			gerr = s.grants.ForgetObject(ctx, target[0])
		case isAlter:
			// Grants and the owner follow the view to its new name.
			gerr = s.grants.RenameObject(ctx, target[0], auth.Object{Schema: schema, Table: alter.renameTo})
		}
		if gerr != nil {
			s.log.Warn("could not update a view's grants and owner", "view", target[0].String(), "err", gerr)
		}
		switch {
		case view.drop || isAlter:
			s.ownersChanged()
			s.sessions.expireAll()
		case isView:
			s.recordOwner(ctx, user, &createdObject{object: target[0], keep: view.replace || view.ifNotExists})
		}
		return record(w.Complete(tag))
	})), true, nil
}

// authorizeSchemaCreate allows a user whose built-in role refused a change
// to the catalog's tables or views in schema (roleErr) if they have CREATE
// on the schema.
func (s *Server) authorizeSchemaCreate(ctx context.Context, sess *session, user auth.User, schema string, roleErr error) error {
	if s.catalogName() == "" {
		return roleErr
	}
	privs, err := s.privileges(ctx, sess, user)
	if err != nil {
		s.log.Warn("could not load grants", "user", user.Name, "err", err)
		return roleErr
	}
	if user.Role != auth.RoleNone && privs.Empty() {
		return roleErr
	}
	n := need{auth.PrivilegeCreate, auth.Object{Schema: schema}}
	if allows(user, privs, n) {
		return nil
	}
	return needError(user, n)
}

// namesDatabase reports whether query's code uses db as a qualifier, as in
// db.schema.name, quoted or not.
func namesDatabase(query, db string) bool {
	for _, n := range qualifiers(query) {
		if strings.EqualFold(n, db) {
			return true
		}
	}
	return false
}

// qualifiers returns every name in query that a dot follows.
func qualifiers(query string) []string {
	var out []string
	segs := sqllex.Segments(query)
	for i, seg := range segs {
		var name, after string
		switch seg.Kind {
		case sqllex.Ident:
			if len(seg.Text) < 2 {
				continue
			}
			name = strings.ReplaceAll(seg.Text[1:len(seg.Text)-1], `""`, `"`)
			if i+1 < len(segs) && segs[i+1].Kind == sqllex.Code {
				after = segs[i+1].Text
			}
			if strings.HasPrefix(strings.TrimLeft(after, " \t\r\n"), ".") {
				out = append(out, name)
			}
		case sqllex.Code:
			for _, m := range qualifierWord.FindAllStringSubmatch(seg.Text, -1) {
				out = append(out, m[1])
			}
		}
	}
	return out
}

var qualifierWord = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_$]*)\s*\.`)

// catalogDDLError gives view and comment errors their PostgreSQL codes.
func catalogDDLError(err error) error {
	switch {
	case errors.Is(err, ddl.ErrExists):
		return pgerror.WithCode(err, codes.DuplicateRelation)
	case errors.Is(err, ddl.ErrNotFound):
		return pgerror.WithCode(err, codes.UndefinedTable)
	}
	return pgError(err)
}

// resolveCatalogSchema works out which schema of the Iceberg catalog a
// schema name written in a statement refers to: schema or catalog.schema.
// ok is false for another database's.
func (s *Server) resolveCatalogSchema(ctx context.Context, sess *session, parts []string) (schema string, ok bool, err error) {
	if len(parts) == 2 && !strings.EqualFold(parts[0], s.ddl.Catalog()) {
		return "", false, nil
	}
	if len(parts) == 1 {
		db, _, err := currentSchema(ctx, sess.conn)
		if err != nil || !strings.EqualFold(db, s.ddl.Catalog()) {
			return "", false, err
		}
	}
	return parts[len(parts)-1], true, nil
}

// resolveCatalogName works out which schema and table or view a name written
// in a statement refers to in the Iceberg catalog. ok is false when it names
// something in another database: temporary objects, or DuckDB's own.
func (s *Server) resolveCatalogName(ctx context.Context, sess *session, parts []string) (schema, name string, ok bool, err error) {
	catalogName := s.ddl.Catalog()
	switch len(parts) {
	case 3:
		return parts[1], parts[2], strings.EqualFold(parts[0], catalogName), nil
	case 2:
		switch strings.ToLower(parts[0]) {
		case "temp", "system", strings.ToLower(s.engine.LocalDatabase()):
			return "", "", false, nil
		}
		return parts[0], parts[1], true, nil
	case 1:
		// As in DuckDB, an unqualified name means a temporary object first.
		// isTemporary lists with duckdb_tables()/duckdb_views(), which look
		// the catalog's tables up (see holdCatalog).
		release := s.shareCatalog(sess)
		temp, err := isTemporary(ctx, sess.conn, parts[0])
		release()
		if err != nil || temp {
			return "", "", false, err
		}
		db, current, err := currentSchema(ctx, sess.conn)
		if err != nil {
			return "", "", false, err
		}
		return current, parts[0], strings.EqualFold(db, catalogName), nil
	}
	return "", "", false, nil
}

// isTemporary reports whether the session has a temporary table or view
// named name.
func isTemporary(ctx context.Context, conn *duckdb.Conn, name string) (bool, error) {
	rows, err := conn.QueryContext(ctx, `SELECT system.main.count_star() FROM (
			SELECT 1 FROM system.main.duckdb_tables() WHERE temporary AND system.main.lower(table_name) = system.main.lower($1)
			UNION ALL SELECT 1 FROM system.main.duckdb_views() WHERE temporary AND system.main.lower(view_name) = system.main.lower($1))`,
		[]driver.NamedValue{{Ordinal: 1, Value: name}})
	if err != nil {
		return false, err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		return false, err
	}
	n, _ := dest[0].(int64)
	return n > 0, nil
}

// currentSchema returns the session's current database and schema.
func currentSchema(ctx context.Context, conn *duckdb.Conn) (db, schema string, err error) {
	rows, err := conn.QueryContext(ctx, "SELECT system.main.current_database(), system.main.current_schema()", nil)
	if err != nil {
		return "", "", err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 2)
	if err := rows.Next(dest); err != nil {
		if errors.Is(err, io.EOF) {
			err = errors.New("no current schema")
		}
		return "", "", err
	}
	db, _ = dest[0].(string)
	schema, _ = dest[1].(string)
	return db, schema, nil
}

// missingRelation matches DuckDB's error for an unknown table or view: one
// created on another replica, perhaps, that this one has not synced yet.
var missingRelation = regexp.MustCompile(`Catalog Error: (Table|View) with name \S+ does not exist`)

// syncingViews runs fn, which binds a view's body, and if that fails for
// want of a relation, syncs the catalog's views and runs fn again, as
// preparing a statement does (see prepareCheckedText). A view that still
// cannot be made fails it with why.
func (s *Server) syncingViews(ctx context.Context, fn func() error) error {
	gen := s.ddl.Generation()
	err := fn()
	if err != nil && missingRelation.MatchString(err.Error()) {
		if changed, _ := s.ddl.SyncViews(ctx); changed || s.ddl.Generation() != gen {
			err = fn()
		}
	}
	return s.ddl.Explain(err)
}

// missingSchema matches DuckDB's errors for a schema it did not find, its
// name in group 1 or 2.
var missingSchema = regexp.MustCompile(`[Ss]chema (?:with name )?"([^"]+)" (?:not found|does not exist)|` +
	`namespace by the name of '([^']+)' does not exist`)

// missingSchemaOf returns the schema an error of DuckDB's says it did not
// find, or "".
func missingSchemaOf(msg string) string {
	m := missingSchema.FindStringSubmatch(msg)
	if m == nil {
		return ""
	}
	return m[1] + m[2]
}
