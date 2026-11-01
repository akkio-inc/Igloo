package pgwire

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/iddomain"
	"github.com/akkio-inc/igloo/internal/policy"
)

// Bitmap indexes, which Igloo keeps itself:
//
//	ALTER TABLE [IF EXISTS] t ADD BITMAP INDEX [IF NOT EXISTS] name ON (c [, ...]) FOR column
//	ALTER TABLE [IF EXISTS] t DROP BITMAP INDEX [IF EXISTS] name
//	SHOW BITMAP INDEXES
//
// An index keeps, for column, a member of an ID domain, a roaring bitmap of
// its values' ids for each combination of the values of the columns in ON
// (one bitmap for the whole table with ON ()), in the table
// iddomain.IndexSchema.name of the Iceberg catalog: its columns are
// source_snapshot_id, the snapshot of t the bitmaps are of, the group
// columns, generation, the domain's, and bm, the bitmap in CRoaring's
// portable format. Igloo builds and keeps it up to date (see
// bitmapbuild.go), and no user's statement reads it: like the
// dictionaries, it carries Igloo's deny-all row access policy
// (iddomain.IndexPolicy), and nothing writes, creates, drops or alters
// anything in its schema but Igloo (iglooSchemaWrites). Indexes are
// governance, as domains are: only admins change them, anyone may list
// them, and they exist only with IGLOO_BITMAP_INDEXES on, which needs
// IGLOO_ID_DOMAINS.

type indexCmdKind int

const (
	icAdd indexCmdKind = iota
	icDrop
	icShow
)

type indexCmd struct {
	kind                  indexCmdKind
	tag                   string
	table                 []string
	name                  string
	groups                []string
	elements              bool
	column                string
	ifExists, ifNotExists bool
}

var indexColumns = wire.Columns{
	{Name: "name", Oid: pgtype.TextOID, Width: -1},
	{Name: "table", Oid: pgtype.TextOID, Width: -1},
	{Name: "column", Oid: pgtype.TextOID, Width: -1},
	{Name: "groups", Oid: pgtype.TextOID, Width: -1},
	{Name: "domain", Oid: pgtype.TextOID, Width: -1},
	{Name: "snapshot", Oid: pgtype.Int8OID, Width: 8},
	{Name: "current", Oid: pgtype.BoolOID, Width: 1},
	{Name: "bitmaps", Oid: pgtype.Int8OID, Width: 8},
	{Name: "created_by", Oid: pgtype.TextOID, Width: -1},
	{Name: "created_at", Oid: pgtype.TimestamptzOID, Width: 8},
}

// indexReserved are the index table's own columns, which no group column
// may be called.
var indexReserved = []string{"source_snapshot_id", "generation", "bm", "row_count", "members"}

// parseIndexCommand recognizes the statements above. ok is false for any
// other statement; err is set for one of them written wrong.
func parseIndexCommand(query string) (c *indexCmd, ok bool, err error) {
	sc := &scanner{s: query}
	c = &indexCmd{}
	switch {
	case sc.keywords("SHOW", "BITMAP", "INDEXES"):
		c.kind, c.tag = icShow, "SHOW BITMAP INDEXES"
	case sc.keywords("ALTER", "TABLE"):
		c.ifExists = sc.keywords("IF", "EXISTS")
		if c.table, ok = sc.name(); !ok {
			return nil, false, nil
		}
		c.tag = "ALTER TABLE"
		switch {
		case sc.keywords("ADD", "BITMAP", "INDEX"):
			c.kind = icAdd
			c.ifNotExists = sc.keywords("IF", "NOT", "EXISTS")
			if c.name, err = indexName(sc); err != nil {
				return nil, true, err
			}
			if !sc.keywords("ON") || !sc.punct('(') {
				return nil, true, domainSyntax("expected ADD BITMAP INDEX name ON (columns) FOR column")
			}
			at := sc.i
			if sc.keywords("ELEMENTS") && sc.punct('(') {
				// ON (ELEMENTS(column)): a bitmap for each element of a list.
				g, ok := sc.name()
				if !ok || len(g) != 1 || !sc.punct(')') || !sc.punct(')') {
					return nil, true, domainSyntax("expected ON (ELEMENTS(column)), a list column alone")
				}
				c.groups, c.elements = g, true
			} else if sc.i = at; !sc.punct(')') {
				for {
					g, ok := sc.name()
					if !ok || len(g) != 1 {
						return nil, true, domainSyntax("expected the columns a bitmap index groups by, as in ON (segment, day)")
					}
					c.groups = append(c.groups, g[0])
					if sc.punct(')') {
						break
					}
					if !sc.punct(',') {
						return nil, true, domainSyntax("expected , or ) after a bitmap index's column")
					}
				}
			}
			if !sc.keywords("FOR") {
				return nil, true, domainSyntax("expected FOR column, the member column whose ids the bitmaps hold")
			}
			col, ok := sc.name()
			if !ok || len(col) != 1 {
				return nil, true, domainSyntax("expected FOR column, the member column whose ids the bitmaps hold")
			}
			c.column = col[0]
		case sc.keywords("DROP", "BITMAP", "INDEX"):
			c.kind = icDrop
			c.ifExists = sc.keywords("IF", "EXISTS") || c.ifExists
			if c.name, err = indexName(sc); err != nil {
				return nil, true, err
			}
		default:
			return nil, false, nil
		}
	default:
		return nil, false, nil
	}
	if !sc.done() {
		return nil, true, domainSyntax("unexpected %q in %s", strings.TrimSpace(sc.rest()), c.tag)
	}
	return c, true, nil
}

// indexName reads a bitmap index's name, which names its table.
func indexName(sc *scanner) (string, error) {
	name, ok := sc.name()
	if !ok || len(name) != 1 {
		return "", domainSyntax("expected a bitmap index's name")
	}
	if err := iddomain.CheckName(strings.ToLower(name[0])); err != nil {
		return "", pgerror.WithCode(fmt.Errorf("a bitmap index's name is lower-case letters, digits and _, not starting with a digit (at most 63): %q", name[0]),
			codes.InvalidName)
	}
	return strings.ToLower(name[0]), nil
}

// indexStatement handles a bitmap index statement. ok is false for anything
// else.
func (s *Server) indexStatement(sess *session, q wire.Query) (wire.PreparedStatements, bool, error) {
	c, ok, err := parseIndexCommand(q.Query)
	if !ok {
		return nil, false, nil
	}
	if err != nil {
		s.recordQuery(sess, queryRecord{text: q.Query, start: time.Now(), err: err, rows: -1})
		return nil, true, err
	}
	var opts []wire.PreparedOptionFn
	if c.kind == icShow {
		opts = append(opts, wire.WithColumns(indexColumns))
	}
	return wire.Prepared(wire.NewStatement(s.audited(sess, q.Query, func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		start := time.Now()
		err := s.runIndexCommand(ctx, sess, c, w)
		s.metrics.query(start, err)
		return err
	}), opts...)), true, nil
}

func (s *Server) runIndexCommand(ctx context.Context, sess *session, c *indexCmd, w wire.DataWriter) error {
	user, err := s.refreshUser(ctx, sess)
	if err != nil {
		return err
	}
	if c.kind == icShow {
		return s.showIndexes(ctx, w)
	}
	if user.Role != auth.RoleAdmin {
		return pgerror.WithCode(fmt.Errorf("permission denied: %s ... BITMAP INDEX requires the admin role", c.tag), codes.InsufficientPrivilege)
	}
	if s.catalogName() == "" || s.ddl == nil {
		return pgerror.WithCode(errors.New("bitmap indexes need an Iceberg catalog (IGLOO_CATALOG_URI)"), codes.FeatureNotSupported)
	}
	if sess.inTx {
		return pgerror.WithCode(errors.New("bitmap index statements cannot run inside a transaction block"), codes.ActiveSQLTransaction)
	}
	switch c.kind {
	case icAdd:
		err = s.addIndex(ctx, sess, user, c)
	case icDrop:
		err = s.dropIndex(ctx, sess, c)
	}
	if err != nil {
		return domainError(err)
	}
	s.log.Info("bitmap indexes changed", "index", c.name, "table", strings.Join(c.table, "."), "by", user.Name)
	return w.Complete(c.tag)
}

// indexTable is the quoted name of bitmap index name's table.
func (s *Server) indexTable(name string) string {
	return quoteName([]string{s.catalogName(), iddomain.IndexSchema, name})
}

func (s *Server) addIndex(ctx context.Context, sess *session, user auth.User, c *indexCmd) error {
	if !s.bitmapIndexesOn {
		return pgerror.WithCode(errors.New("bitmap indexes need IGLOO_BITMAP_INDEXES=true (and IGLOO_ID_DOMAINS=true)"), codes.FeatureNotSupported)
	}
	schema, table, cols, err := s.domainTable(ctx, sess, &domainCmd{table: c.table}, true)
	if err != nil {
		if c.ifExists && pgerror.GetCode(err) == codes.UndefinedTable {
			return nil
		}
		return err
	}
	cur, err := s.idDomains.Load(ctx)
	if err != nil {
		return err
	}
	if cur.Index(c.name) != nil {
		if c.ifNotExists {
			return nil
		}
		return fmt.Errorf("bitmap index %s %w", c.name, iddomain.ErrExists)
	}
	column := func(name string) (columnType, error) {
		i := slices.IndexFunc(cols, func(col columnType) bool { return strings.EqualFold(col.name, name) })
		if i < 0 || iddomain.IsHidden(cols[i].name) {
			return columnType{}, pgerror.WithCode(fmt.Errorf("column %q of table %s.%s does not exist", name, schema, table), codes.UndefinedColumn)
		}
		return cols[i], nil
	}
	member, err := column(c.column)
	if err != nil {
		return err
	}
	m := cur.MemberOf(schema, table, member.name)
	if m == nil {
		return pgerror.WithCode(fmt.Errorf("column %s of table %s.%s is not a member of an ID domain: a bitmap index holds a member's ids "+
			"(ALTER TABLE ... ALTER COLUMN %s SET ID DOMAIN first)", member.name, schema, table, member.name), codes.ObjectNotInPrerequisiteState)
	}
	x := iddomain.BitmapIndex{Name: c.name, Schema: schema, Table: table, Column: member.name, Added: time.Now().UTC(), AddedBy: user.Name}
	var defs []string
	for _, g := range c.groups {
		col, err := column(g)
		if err != nil {
			return err
		}
		if slices.ContainsFunc(indexReserved, func(r string) bool { return strings.EqualFold(r, col.name) }) {
			return pgerror.WithCode(fmt.Errorf("a bitmap index cannot group by a column called %s, a name of its own table's", col.name), codes.InvalidColumnReference)
		}
		typ := col.typ
		if c.elements {
			// The index's column holds the list's elements.
			if !strings.HasSuffix(typ, "[]") {
				return pgerror.WithCode(fmt.Errorf("ELEMENTS takes a list column, not %s %s", col.name, col.typ), codes.DatatypeMismatch)
			}
			typ = strings.TrimSuffix(typ, "[]")
			x.Elements = true
		}
		if t := strings.ToUpper(typ); strings.Contains(t, "[") || strings.HasPrefix(t, "STRUCT") || strings.HasPrefix(t, "MAP") || strings.HasPrefix(t, "UNION") {
			return pgerror.WithCode(fmt.Errorf("a bitmap index groups by columns of plain types, not %s %s", col.name, typ), codes.DatatypeMismatch)
		}
		x.Groups = append(x.Groups, col.name)
		defs = append(defs, quoteIdent(col.name)+" "+typ)
	}
	// Checked again in the store's update; checked here first so that no
	// table is made for an index that cannot be.
	check := cur.Catalog
	if err := check.AddIndex(x); err != nil {
		return err
	}
	// Each bitmap with the rows of its group that have ids and its
	// members, as many as its ids: equal, each member has one row in the
	// group, which the report API counts by (see reportindex.go).
	ddl := "CREATE TABLE IF NOT EXISTS " + s.indexTable(c.name) + " (" +
		strings.Join(slices.Concat([]string{"source_snapshot_id BIGINT"}, defs, []string{"generation INTEGER", "bm BLOB", "row_count BIGINT", "members BIGINT"}), ", ") + ")"
	if err := s.execHeld(ctx, sess, "CREATE SCHEMA IF NOT EXISTS "+quoteName([]string{s.catalogName(), iddomain.IndexSchema}), ddl); err != nil {
		return pgError(err)
	}
	// A table left by an ADD that stopped half way is taken over only
	// while empty.
	rows, err := queryRows(ctx, sess.conn, "SELECT count(*) FROM "+s.indexTable(c.name))
	if err != nil {
		return pgError(err)
	}
	if len(rows) == 1 && len(rows[0]) == 1 {
		if n, _ := rows[0][0].(int64); n > 0 {
			return pgerror.WithCode(fmt.Errorf("the table %s.%s already holds %d rows; drop it to add bitmap index %s", iddomain.IndexSchema, c.name, n, c.name),
				codes.DuplicateObject)
		}
	}
	now := time.Now().UTC()
	err = s.gov.store.Update(ctx, func(cat *policy.Catalog) error {
		if _, ok := cat.Policy(iddomain.IndexPolicy); !ok {
			p := policy.Policy{Kind: policy.RowAccess, Name: iddomain.IndexPolicy, Args: []policy.Arg{{Name: "source_snapshot_id", Type: "BIGINT"}},
				Returns: "BOOLEAN", Body: "false", CreatedBy: "igloo",
				Comment: "Igloo's own: keeps bitmap indexes (igloo_bitmaps) from every user's statements"}
			if err := cat.PutPolicy(p, false, now); err != nil {
				return err
			}
		}
		err := cat.AddRowFilter(policy.RowFilter{Schema: iddomain.IndexSchema, Table: c.name, Policy: iddomain.IndexPolicy,
			Columns: []string{"source_snapshot_id"}, SetBy: "igloo"}, now)
		if err != nil && !errors.Is(err, policy.ErrExists) {
			return err
		}
		return nil
	})
	if err != nil {
		return policyStoreError(err)
	}
	if err := s.gov.reload(ctx); err != nil {
		s.log.Warn("could not reload policies", "err", err)
	}
	if err := s.idDomains.Update(ctx, func(cat *iddomain.Catalog) error { return cat.AddIndex(x) }); err != nil {
		return err
	}
	s.ddl.TableChanged(iddomain.IndexSchema, c.name)
	// Built in the background from now on (see bitmapbuild.go).
	s.indexesChanged(schema, table)
	return nil
}

func (s *Server) dropIndex(ctx context.Context, sess *session, c *indexCmd) error {
	cur, err := s.idDomains.Load(ctx)
	if err != nil {
		return err
	}
	x := cur.Index(c.name)
	if x == nil {
		if c.ifExists {
			return nil
		}
		return fmt.Errorf("bitmap index %s %w", c.name, iddomain.ErrNotFound)
	}
	if len(c.table) > 0 {
		schema, table, _, err := s.domainTable(ctx, sess, &domainCmd{table: c.table}, false)
		if err != nil {
			return err
		}
		if !x.OfTable(schema, table) {
			return pgerror.WithCode(fmt.Errorf("bitmap index %s is an index of %s.%s, not of %s.%s", x.Name, x.Schema, x.Table, schema, table), codes.UndefinedObject)
		}
	}
	// The index goes from the store first, so that no statement reads its
	// table once it is going, and then its bitmaps.
	if err := s.idDomains.Update(ctx, func(cat *iddomain.Catalog) error {
		cat.DropIndex(c.name)
		return nil
	}); err != nil {
		return err
	}
	if err := s.execHeld(ctx, sess, "DROP TABLE IF EXISTS "+s.indexTable(c.name)); err != nil {
		return pgError(err)
	}
	err = s.gov.store.Update(ctx, func(cat *policy.Catalog) error {
		if err := cat.DropRowFilter("", iddomain.IndexSchema, c.name, iddomain.IndexPolicy); err != nil && !errors.Is(err, policy.ErrNotFound) {
			return err
		}
		return nil
	})
	if err != nil {
		return policyStoreError(err)
	}
	if err := s.gov.reload(ctx); err != nil {
		s.log.Warn("could not reload policies", "err", err)
	}
	s.ddl.TableChanged(iddomain.IndexSchema, c.name)
	return nil
}

func (s *Server) showIndexes(ctx context.Context, w wire.DataWriter) error {
	cur, err := s.idDomains.Load(ctx)
	if err != nil {
		return err
	}
	indexes := slices.Clone(cur.Indexes)
	slices.SortFunc(indexes, func(a, b iddomain.BitmapIndex) int { return strings.Compare(a.Name, b.Name) })
	for _, x := range indexes {
		domain := ""
		if m := cur.MemberOf(x.Schema, x.Table, x.Column); m != nil {
			domain = m.Domain
		}
		var snapshot, bitmaps any
		current := false
		if st, err := s.indexState(ctx, x); err == nil {
			if st.built != 0 {
				snapshot = st.built
				bitmaps = st.bitmaps
			}
			current = st.built != 0 && st.built == st.current
		}
		groups := strings.Join(x.Groups, ", ")
		if x.Elements {
			groups = "ELEMENTS(" + groups + ")"
		}
		row := []any{x.Name, x.Schema + "." + x.Table, x.Column, groups, domain, snapshot, current, bitmaps, x.AddedBy, x.Added}
		if err := w.Row(row); err != nil {
			return err
		}
	}
	return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
}

// iglooSchemas are the catalog schemas whose tables are Igloo's own: ID
// domains' dictionaries, bitmap indexes, and materialized views' and
// dynamic tables' state. iglooPolicies guard them.
var (
	iglooSchemas  = []string{iddomain.DictionarySchema, iddomain.IndexSchema, refreshSchema}
	iglooPolicies = []string{iddomain.DictionaryPolicy, iddomain.IndexPolicy, refreshPolicy}
)

// isIglooSchema reports whether schema is one of iglooSchemas.
func isIglooSchema(schema string) bool {
	return slices.ContainsFunc(iglooSchemas, func(s string) bool { return strings.EqualFold(s, schema) })
}
