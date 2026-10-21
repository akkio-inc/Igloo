package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	icecat "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/duckdb/duckdb-go/v2"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/policy"
)

// Zero-copy clones (package clone), with Snowflake's syntax:
//
//	CREATE [OR REPLACE] TABLE [IF NOT EXISTS] name CLONE source [AT (...)]
//	CREATE [OR REPLACE] SCHEMA [IF NOT EXISTS] name CLONE source [AT (TIMESTAMP => ...)]
//
// AT takes TIMESTAMP => expression, SNAPSHOT => id (or VERSION => id, as
// in time travel), BRANCH => 'name' or TAG => 'name'.
//
// Cloning a table needs SELECT on it, which is what reading it all through
// CREATE TABLE AS would need, and CREATE on the clone's schema; cloning a
// schema needs the writer role, as creating one does. The clone gets every
// masking and row access policy and tag of its source, before it exists,
// so no statement on any replica reads it unprotected; otherwise cloning a
// table would be a way around them. It gets none of its source's grants: a
// table's grants follow its name. A schema's clone keeps the source
// schema's own grants, which cover tables created in it later. A table a
// clean room keeps to a party is cloned by that party alone (keptFrom).

// cloneCommand is a CREATE TABLE or CREATE SCHEMA ... CLONE.
type cloneCommand struct {
	schema               bool
	replace, ifNotExists bool
	target, source       []string
	at                   *atClause
}

// atClause is AT (unit => value) as written.
type atClause struct {
	// unit is TIMESTAMP, SNAPSHOT, BRANCH or TAG.
	unit string
	// value is an expression for TIMESTAMP, a snapshot ID, or a branch's
	// or tag's name.
	value string
}

func cloneSyntax(format string, args ...any) error {
	return pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
}

// parseCloneCommand recognizes CREATE ... TABLE or SCHEMA name CLONE ....
// ok is false for any other statement.
func parseCloneCommand(query string) (c cloneCommand, ok bool, err error) {
	sc := &scanner{s: query}
	if !sc.keywords("CREATE") {
		return c, false, nil
	}
	c.replace = sc.keywords("OR", "REPLACE")
	switch {
	case sc.keywords("TABLE"):
	case sc.keywords("SCHEMA"):
		c.schema = true
	default:
		return c, false, nil
	}
	c.ifNotExists = sc.keywords("IF", "NOT", "EXISTS")
	if c.target, ok = sc.name(); !ok || !sc.keywords("CLONE") {
		return c, false, nil
	}
	kind := "table"
	if c.schema {
		kind = "schema"
	}
	if c.replace && c.ifNotExists {
		return c, true, cloneSyntax("OR REPLACE and IF NOT EXISTS cannot both be used")
	}
	if c.source, ok = sc.name(); !ok {
		return c, true, cloneSyntax("expected the name of the %s to clone after CLONE", kind)
	}
	switch {
	case sc.keywords("AT"):
		at, err := parseAt(sc)
		if err != nil {
			return c, true, err
		}
		c.at = &at
	case sc.keywords("BEFORE"):
		return c, true, pgerror.WithCode(errors.New("CLONE ... BEFORE is not supported; use AT"), codes.FeatureNotSupported)
	}
	if !sc.done() {
		return c, true, cloneSyntax("unexpected %q after the %s to clone", strings.TrimSpace(sc.rest()), kind)
	}
	if c.schema && c.at != nil && c.at.unit != "TIMESTAMP" {
		return c, true, cloneSyntax("a schema is cloned as of a time, AT (TIMESTAMP => ...), as its tables' snapshots differ")
	}
	return c, true, nil
}

// parseAt reads (unit => value), after AT.
func parseAt(sc *scanner) (atClause, error) {
	usage := cloneSyntax("expected AT (TIMESTAMP => ...), AT (SNAPSHOT => id), AT (BRANCH => 'name') or AT (TAG => 'name')")
	sc.skip()
	if sc.i >= len(sc.s) || sc.s[sc.i] != '(' {
		return atClause{}, usage
	}
	end := matchParen(sc.s, sc.i)
	if end < 0 {
		return atClause{}, cloneSyntax("unbalanced parentheses after AT")
	}
	in := &scanner{s: sc.s[sc.i+1 : end]}
	sc.i = end + 1
	at := atClause{unit: in.word()}
	if at.unit == "VERSION" {
		at.unit = "SNAPSHOT"
	}
	if !in.keywords(in.word()) {
		return atClause{}, usage
	}
	in.skip()
	if !strings.HasPrefix(in.rest(), "=>") {
		return atClause{}, usage
	}
	in.i += 2
	switch at.unit {
	case "TIMESTAMP":
		at.value = strings.TrimSpace(in.rest())
		if at.value == "" {
			return atClause{}, usage
		}
	case "SNAPSHOT":
		at.value = strings.TrimSpace(in.rest())
		if _, err := strconv.ParseInt(at.value, 10, 64); err != nil {
			return atClause{}, cloneSyntax("a snapshot is named by its ID, a number, not %q", at.value)
		}
	case "BRANCH", "TAG":
		v, ok := in.literal()
		if !ok || v == nil || !in.done() {
			return atClause{}, cloneSyntax("a %s is named in quotes, as in AT (%[1]s => 'name')", at.unit)
		}
		at.value = *v
	default:
		return atClause{}, usage
	}
	return at, nil
}

// cloneStatement carries out a CLONE statement. ok is false for any other
// statement.
func (s *Server) cloneStatement(sess *session, q wire.Query) (wire.PreparedStatements, bool, error) {
	c, ok, err := parseCloneCommand(q.Query)
	if !ok {
		return nil, false, nil
	}
	if err == nil && s.clones == nil {
		err = pgerror.WithCode(errors.New("CLONE needs the Iceberg catalog (IGLOO_CATALOG_URI)"), codes.FeatureNotSupported)
	}
	if err != nil {
		s.recordQuery(sess, queryRecord{text: q.Query, start: time.Now(), err: err, rows: -1})
		return nil, true, err
	}
	tag := "CREATE TABLE"
	if c.schema {
		tag = "CREATE SCHEMA"
	}
	return wire.Prepared(wire.NewStatement(func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		start := time.Now()
		var acc *access
		// A clone creates and may replace catalog tables, and binds the
		// views it copies; hold the exclusive side so listings do not run
		// against it on this instance (see holdCatalog).
		releaseCatalog := s.holdCatalog(sess)
		defer releaseCatalog()
		err := sess.runCancelable(ctx, q.Query, func(ctx context.Context) error {
			var err error
			acc, err = s.runClone(ctx, sess, c)
			return err
		})
		s.metrics.query(start, err)
		s.recordQuery(sess, queryRecord{text: q.Query, start: start, err: err, rows: -1, acc: acc})
		if err != nil {
			return pgError(err)
		}
		return w.Complete(tag)
	})), true, nil
}

// runClone clones a table or schema. It returns what the clone read and
// wrote, for the history.
func (s *Server) runClone(ctx context.Context, sess *session, c cloneCommand) (*access, error) {
	user, err := s.refreshUser(ctx, sess)
	if err != nil {
		return nil, err
	}
	if sess.inTx {
		return nil, pgerror.WithCode(errors.New("CLONE cannot run inside a transaction block"), codes.ActiveSQLTransaction)
	}
	if c.schema {
		return s.cloneSchema(ctx, sess, user, c)
	}
	return s.cloneTable(ctx, sess, user, c)
}

func (s *Server) cloneTable(ctx context.Context, sess *session, user auth.User, c cloneCommand) (*access, error) {
	src, err := s.cloneSource(ctx, sess, c.source)
	if err != nil {
		return nil, err
	}
	dst, err := s.newTableTarget(ctx, sess, c.target)
	if errors.Is(err, errNoGrantPath) {
		err = pgerror.WithCode(fmt.Errorf("clones are tables of the %s catalog", s.catalogName()), codes.FeatureNotSupported)
	}
	if err != nil {
		return nil, err
	}
	acc := &access{read: []auth.Object{src}, written: []auth.Object{dst}}
	if err := s.authorizeClone(ctx, sess, user, acc.read, dst.Schema, false); err != nil {
		return acc, err
	}
	if dst.Equal(src) {
		return acc, pgerror.WithCode(fmt.Errorf("%s cannot be cloned onto itself", src), codes.DuplicateRelation)
	}
	if err := s.keptChanges(ctx, sess, user, acc.written); err != nil {
		return acc, err
	}
	if s.ddl != nil && s.ddl.IsView(dst.Schema, dst.Table) {
		return acc, pgerror.WithCode(fmt.Errorf("a view named %s exists", dst), codes.DuplicateRelation)
	}
	exists, err := s.clones.Exists(ctx, identOf(dst))
	switch {
	case err != nil:
		return acc, err
	case exists && c.ifNotExists:
		return acc, nil
	case exists && !c.replace:
		return acc, pgerror.WithCode(fmt.Errorf("table %s already exists", dst), codes.DuplicateRelation)
	case exists:
		// Replacing drops the table (see catalogguard.go).
		if err := s.refuseErasingReplace(ctx, dst); err != nil {
			return acc, err
		}
	}
	at, err := s.cloneAt(ctx, sess, user, c.at)
	if err != nil {
		return acc, err
	}
	if err := s.carryPolicies(ctx, user, []clonePair{{source: src, target: dst, exists: exists}}); err != nil {
		return acc, err
	}
	res, err := s.clones.Clone(ctx, clone.Request{Source: identOf(src), Target: identOf(dst), At: at, Replace: c.replace})
	s.catalogChanged()
	if err != nil {
		return acc, cloneError(err)
	}
	s.log.Info("table cloned", "source", src.String(), "target", dst.String(), "snapshot", snapshotID(res.Snapshot), "by", user.Name)
	// The table replaced goes with its files, as a drop does.
	s.clones.Purge(ctx, res.Replaced)
	// The cloner owns the clone; one that replaced a table keeps its owner.
	s.recordOwner(ctx, user, &createdObject{object: dst, keep: exists})
	return acc, nil
}

func (s *Server) cloneSchema(ctx context.Context, sess *session, user auth.User, c cloneCommand) (*access, error) {
	srcName, err := s.schemaPart(c.source)
	if err != nil {
		return nil, err
	}
	dstName, err := s.schemaPart(c.target)
	if err != nil {
		return nil, err
	}
	acc := &access{}
	if err := s.authorizeClone(ctx, sess, user, nil, dstName, true); err != nil {
		return acc, err
	}
	src, ok, err := s.catalogSchema(ctx, sess, srcName)
	if err != nil {
		return acc, err
	}
	if !ok {
		return acc, pgerror.WithCode(fmt.Errorf("schema %s does not exist", srcName), codes.InvalidSchemaName)
	}
	dst, exists, err := s.catalogSchema(ctx, sess, dstName)
	switch {
	case err != nil:
		return acc, err
	case strings.EqualFold(src, dstName):
		return acc, pgerror.WithCode(fmt.Errorf("schema %s cannot be cloned onto itself", src), codes.DuplicateSchema)
	case exists && c.ifNotExists:
		return acc, nil
	case exists && !c.replace:
		return acc, pgerror.WithCode(fmt.Errorf("schema %s already exists", dst), codes.DuplicateSchema)
	case !exists:
		dst = dstName
	}

	tables, err := s.clones.Tables(ctx, src)
	if err != nil {
		return acc, err
	}
	var had []string
	if exists {
		if had, err = s.clones.Tables(ctx, dst); err != nil {
			return acc, err
		}
	}
	pairs := make([]clonePair, len(tables))
	for i, t := range tables {
		pairs[i] = clonePair{source: auth.Object{Schema: src, Table: t}, target: auth.Object{Schema: dst, Table: t},
			exists: slices.ContainsFunc(had, func(h string) bool { return strings.EqualFold(h, t) })}
		acc.read = append(acc.read, pairs[i].source)
		acc.written = append(acc.written, pairs[i].target)
		if pairs[i].exists {
			if err := s.refuseErasingReplace(ctx, pairs[i].target); err != nil {
				return acc, err
			}
		}
	}
	// Writers read every table but those clean rooms keep to other
	// parties; the check is for those, the history, and in case that ever
	// changes.
	if err := s.authorizeClone(ctx, sess, user, acc.read, dst, true); err != nil {
		return acc, err
	}
	// What it replaces, and what it drops of the schema it replaces.
	changed := slices.Clone(acc.written)
	for _, h := range had {
		changed = append(changed, auth.Object{Schema: dst, Table: h})
	}
	if err := s.keptChanges(ctx, sess, user, changed); err != nil {
		return acc, err
	}
	at, err := s.cloneAt(ctx, sess, user, c.at)
	if err != nil {
		return acc, err
	}

	if !exists {
		if err := s.execDDL(ctx, sess, user, "CREATE SCHEMA "+quoteName([]string{s.catalogName(), dst})); err != nil {
			return acc, err
		}
		s.recordOwner(ctx, user, &createdObject{object: auth.Object{Schema: dst}})
	}
	if err := s.carryPolicies(ctx, user, pairs); err != nil {
		return acc, err
	}
	if err := s.copySchemaGrants(ctx, user, src, dst); err != nil {
		return acc, err
	}
	defer s.catalogChanged()
	// The tables replaced go with their files, as drops do, once the
	// clones are made.
	var replaced []*table.Table
	defer func() { s.clones.Purge(ctx, replaced...) }()
	for _, p := range pairs {
		res, err := s.clones.Clone(ctx, clone.Request{Source: identOf(p.source), Target: identOf(p.target), At: at, Replace: p.exists})
		if err != nil {
			return acc, fmt.Errorf("clone %s: %w", p.source, cloneError(err))
		}
		replaced = append(replaced, res.Replaced)
		s.log.Info("table cloned", "source", p.source.String(), "target", p.target.String(), "snapshot", snapshotID(res.Snapshot), "by", user.Name)
		s.recordOwner(ctx, user, &createdObject{object: p.target, keep: p.exists})
	}
	if s.ddl != nil {
		if _, err := s.ddl.SyncViews(ctx); err != nil {
			return acc, err
		}
		for _, v := range s.ddl.Views(src) {
			cols := make([]string, len(v.Columns))
			for i, col := range v.Columns {
				cols[i] = col.Name
			}
			if err := s.ddl.CreateView(ctx, sess.conn, dst, v.Name, cols, v.SQL, true); err != nil {
				return acc, fmt.Errorf("clone view %s.%s: %w", src, v.Name, catalogDDLError(err))
			}
			acc.written = append(acc.written, auth.Object{Schema: dst, Table: v.Name})
			s.recordOwner(ctx, user, &createdObject{object: auth.Object{Schema: dst, Table: v.Name}, keep: true})
		}
	}
	if exists {
		return acc, s.dropOthers(ctx, sess, user, src, dst, had)
	}
	return acc, nil
}

// dropOthers drops what the schema dst had that its new source src does
// not, completing CREATE OR REPLACE SCHEMA ... CLONE.
func (s *Server) dropOthers(ctx context.Context, sess *session, user auth.User, src, dst string, tables []string) error {
	for _, t := range tables {
		if exists, err := s.clones.Exists(ctx, table.Identifier{src, t}); err != nil || exists {
			if err != nil {
				return err
			}
			continue
		}
		if err := s.execDDL(ctx, sess, user, "DROP TABLE "+quoteName([]string{s.catalogName(), dst, t})); err != nil {
			return err
		}
	}
	if s.ddl == nil {
		return nil
	}
	for _, v := range s.ddl.Views(dst) {
		if s.ddl.IsView(src, v.Name) {
			continue
		}
		if err := s.ddl.DropView(ctx, dst, v.Name, true); err != nil {
			return catalogDDLError(err)
		}
		if err := s.grants.ForgetObject(ctx, auth.Object{Schema: dst, Table: v.Name}); err != nil {
			s.log.Warn("could not remove a dropped view's grants", "view", dst+"."+v.Name, "err", err)
		}
	}
	s.sessions.expireAll()
	return nil
}

// schemaPart is the schema a CREATE SCHEMA names: schema, or catalog.schema
// in the Iceberg catalog.
func (s *Server) schemaPart(name []string) (string, error) {
	switch {
	case len(name) == 1:
		return name[0], nil
	case len(name) == 2 && strings.EqualFold(name[0], s.catalogName()):
		return name[1], nil
	}
	return "", pgerror.WithCode(fmt.Errorf("clones are schemas of the %s catalog, not %s", s.catalogName(), strings.Join(name, ".")),
		codes.FeatureNotSupported)
}

// cloneSource resolves the table a clone is made from.
func (s *Server) cloneSource(ctx context.Context, sess *session, name []string) (auth.Object, error) {
	objects, err := s.catalogCandidates(ctx, sess, name, false)
	if err != nil {
		return auth.Object{}, err
	}
	var tables []auth.Object
	for _, o := range objects {
		if s.ddl == nil || !s.ddl.IsView(o.Schema, o.Table) {
			tables = append(tables, o)
		}
	}
	switch {
	case len(tables) == 1:
		return tables[0], nil
	case len(tables) > 1:
		return auth.Object{}, pgerror.WithCode(fmt.Errorf("%s is ambiguous; qualify it with its schema", strings.Join(name, ".")),
			codes.AmbiguousAlias)
	case len(objects) > 0:
		return auth.Object{}, pgerror.WithCode(fmt.Errorf("%s is a view; clone the tables it reads, or the schema", objects[0]),
			codes.WrongObjectType)
	}
	return auth.Object{}, pgerror.WithCode(fmt.Errorf("table %s does not exist", strings.Join(name, ".")), codes.UndefinedTable)
}

// authorizeClone decides whether user may clone the tables reads into
// schema: SELECT on each, and CREATE on schema, or for a schema's clone,
// the writer role.
func (s *Server) authorizeClone(ctx context.Context, sess *session, user auth.User, reads []auth.Object, schema string, newSchema bool) error {
	deny := func(err error) error {
		s.log.Info("statement denied", "user", user.Name, "role", user.Role, "err", err)
		return err
	}
	if newSchema {
		if err := authorize(user.Role, duckdb.STATEMENT_TYPE_CREATE, "CREATE SCHEMA"); err != nil {
			return deny(err)
		}
	} else if err := authorize(user.Role, duckdb.STATEMENT_TYPE_CREATE, "CREATE TABLE"); err != nil {
		if err = s.authorizeSchemaCreate(ctx, sess, user, schema, err); err != nil {
			return deny(err)
		}
	}
	if err := s.authorizeReads(ctx, sess, user, reads); err != nil {
		return err
	}
	// A clone carries no confinement, which belongs to its room: only the
	// party a clean room keeps a table to may copy it, as it may read it.
	return s.keptFrom(ctx, sess, user, reads)
}

// cloneAt works out the snapshot AT picks. A TIMESTAMP's expression runs as
// the user's own query.
func (s *Server) cloneAt(ctx context.Context, sess *session, user auth.User, a *atClause) (clone.At, error) {
	if a == nil {
		return clone.At{}, nil
	}
	switch a.unit {
	case "SNAPSHOT":
		id, _ := strconv.ParseInt(a.value, 10, 64)
		return clone.At{Unit: clone.Snapshot, ID: id}, nil
	case "BRANCH":
		return clone.At{Unit: clone.Branch, Ref: a.value}, nil
	case "TAG":
		return clone.At{Unit: clone.Tag, Ref: a.value}, nil
	}
	st, err := s.prepareChecked(ctx, sess, user, "SELECT epoch_ms(CAST(("+a.value+") AS TIMESTAMP WITH TIME ZONE))", nil)
	if err != nil {
		return clone.At{}, err
	}
	defer st.close() //nolint:errcheck
	rows, err := st.stmt.QueryContext(ctx, nil)
	if err != nil {
		return clone.At{}, pgError(err)
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		return clone.At{}, pgError(err)
	}
	ms, ok := dest[0].(int64)
	if !ok {
		return clone.At{}, pgerror.WithCode(errors.New("AT (TIMESTAMP => ...) needs a time"), codes.InvalidParameterValue)
	}
	return clone.At{Unit: clone.Timestamp, Time: time.UnixMilli(ms)}, nil
}

// clonePair is a table to clone and its clone.
type clonePair struct {
	source, target auth.Object
	// exists says whether a table called target exists already.
	exists bool
}

// errPoliciesUnchanged leaves the policy store as it was.
var errPoliciesUnchanged = errors.New("policies unchanged")

// carryPolicies attaches the policies and tags of each source to its clone,
// before the clone exists. Other replicas take up to the refresh interval
// to notice a change to the policies, and until then would not protect the
// clone, so when the clone's name gains a policy, this waits that long. A
// clone made again under the same name, as a development copy is every
// day, has its policies already and does not wait.
func (s *Server) carryPolicies(ctx context.Context, user auth.User, pairs []clonePair) error {
	changed := false
	err := s.gov.store.Update(ctx, func(cat *policy.Catalog) error {
		var err error
		if changed, err = attachClonePolicies(cat, pairs, user.Name, time.Now()); err == nil && !changed {
			return errPoliciesUnchanged
		}
		return err
	})
	if errors.Is(err, errPoliciesUnchanged) {
		return nil
	}
	if err != nil {
		return policyStoreError(err)
	}
	if err := s.gov.reload(ctx); err != nil {
		s.log.Warn("could not reload policies", "err", err)
	}
	s.log.Info("policies changed", "statement", "CLONE", "by", user.Name)
	if _, local := s.gov.store.(*policy.MemoryStore); local {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(s.gov.refresh + time.Second):
		return nil
	}
}

// attachClonePolicies attaches to each pair's target the masking policies,
// row access policy and tags of its source, and reports whether cat
// changed. A target that exists and has a different policy or tag value of
// its own is refused: the clone must not leave a table less protected if it
// fails, and an admin decides which applies.
func attachClonePolicies(cat *policy.Catalog, pairs []clonePair, by string, now time.Time) (bool, error) {
	changed := false
	// Clones are of tables of the Iceberg catalog, whose attachments name
	// no catalog.
	is := func(catalog, schema, table string, o auth.Object) bool {
		return catalog == "" && strings.EqualFold(schema, o.Schema) && strings.EqualFold(table, o.Table)
	}
	conflict := func(p clonePair, what string) error {
		return pgerror.WithCode(fmt.Errorf("table %s has %s that its source %s has not; an admin must change one first", p.target, what, p.source),
			codes.ObjectNotInPrerequisiteState)
	}
	for _, p := range pairs {
		for _, m := range slices.Clone(cat.Masks) {
			if !is(m.Catalog, m.Schema, m.Table, p.source) {
				continue
			}
			i := slices.IndexFunc(cat.Masks, func(o policy.ColumnMask) bool {
				return is(o.Catalog, o.Schema, o.Table, p.target) && strings.EqualFold(o.Column, m.Column)
			})
			if i >= 0 && cat.Masks[i].Policy == m.Policy {
				continue
			}
			if i >= 0 && p.exists {
				return false, conflict(p, fmt.Sprintf("masking policy %s on column %s", cat.Masks[i].Policy, m.Column))
			}
			if err := cat.SetMask(policy.ColumnMask{Schema: p.target.Schema, Table: p.target.Table, Column: m.Column, Policy: m.Policy, SetBy: by}, true, now); err != nil {
				return false, err
			}
			changed = true
		}
		if i := slices.IndexFunc(cat.RowFilters, func(f policy.RowFilter) bool { return is(f.Catalog, f.Schema, f.Table, p.source) }); i >= 0 {
			f := cat.RowFilters[i]
			j := slices.IndexFunc(cat.RowFilters, func(o policy.RowFilter) bool { return is(o.Catalog, o.Schema, o.Table, p.target) })
			same := j >= 0 && cat.RowFilters[j].Policy == f.Policy &&
				slices.EqualFunc(cat.RowFilters[j].Columns, f.Columns, strings.EqualFold)
			if !same {
				if j >= 0 {
					if p.exists {
						return false, conflict(p, "row access policy "+cat.RowFilters[j].Policy)
					}
					if err := cat.DropRowFilter("", p.target.Schema, p.target.Table, ""); err != nil {
						return false, err
					}
				}
				nf := policy.RowFilter{Schema: p.target.Schema, Table: p.target.Table, Policy: f.Policy, Columns: f.Columns, SetBy: by}
				if err := cat.AddRowFilter(nf, now); err != nil {
					return false, err
				}
				changed = true
			}
		}
		for _, t := range slices.Clone(cat.ColumnTags) {
			if !is(t.Catalog, t.Schema, t.Table, p.source) {
				continue
			}
			i := slices.IndexFunc(cat.ColumnTags, func(o policy.ColumnTag) bool {
				return is(o.Catalog, o.Schema, o.Table, p.target) && strings.EqualFold(o.Column, t.Column) && o.Tag == t.Tag
			})
			if i >= 0 && cat.ColumnTags[i].Value == t.Value {
				continue
			}
			if i >= 0 && p.exists {
				return false, conflict(p, fmt.Sprintf("tag %s = '%s' on column %s", t.Tag, cat.ColumnTags[i].Value, t.Column))
			}
			nt := policy.ColumnTag{Schema: p.target.Schema, Table: p.target.Table, Column: t.Column, Tag: t.Tag, Value: t.Value, SetBy: by}
			if err := cat.SetColumnTag(nt, now); err != nil {
				return false, err
			}
			changed = true
		}
	}
	return changed, nil
}

// copySchemaGrants gives the schema dst the grants on the schema src, which
// cover every table in it, now and later.
func (s *Server) copySchemaGrants(ctx context.Context, user auth.User, src, dst string) error {
	all, err := s.grants.Grants(ctx)
	if err != nil {
		return err
	}
	var add []auth.Grant
	for _, g := range all {
		if g.Object.IsSchema() && strings.EqualFold(g.Object.Schema, src) {
			g.Object.Schema, g.GrantedBy, g.GrantedAt = dst, user.Name, time.Now()
			add = append(add, g)
		}
	}
	if len(add) == 0 {
		return nil
	}
	if err := s.grants.Grant(ctx, add); err != nil {
		return err
	}
	s.sessions.expireAll()
	s.log.Info("grants changed", "action", "clone", "object", dst, "from", src, "grants", len(add), "by", user.Name)
	return nil
}

// cloneError gives clone errors their PostgreSQL codes.
func cloneError(err error) error {
	switch {
	case pgerror.GetCode(err) != codes.Uncategorized:
		return err
	case errors.Is(err, clone.ErrNotFound):
		return pgerror.WithCode(err, codes.UndefinedObject)
	case errors.Is(err, clone.ErrExists):
		return pgerror.WithCode(err, codes.DuplicateObject)
	case errors.Is(err, icecat.ErrTableAlreadyExists):
		return pgerror.WithCode(err, codes.DuplicateRelation)
	case errors.Is(err, clone.ErrInvalid):
		return pgerror.WithCode(err, codes.ObjectNotInPrerequisiteState)
	case errors.Is(err, icecat.ErrNoSuchTable):
		return pgerror.WithCode(err, codes.UndefinedTable)
	case errors.Is(err, icecat.ErrNoSuchNamespace):
		return pgerror.WithCode(err, codes.InvalidSchemaName)
	}
	return err
}

func identOf(o auth.Object) table.Identifier { return table.Identifier{o.Schema, o.Table} }

func snapshotID(s *table.Snapshot) int64 {
	if s == nil {
		return 0
	}
	return s.SnapshotID
}
