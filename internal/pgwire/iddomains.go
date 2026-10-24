package pgwire

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/ddl"
	"github.com/akkio-inc/igloo/internal/iddomain"
	"github.com/akkio-inc/igloo/internal/policy"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// ID domains (package iddomain), which Igloo carries out itself:
//
//	CREATE ID DOMAIN [IF NOT EXISTS] name (type) [TAG tag = 'value' [, ...]] [COMMENT = 'text']
//	DROP ID DOMAIN [IF EXISTS] name
//	SHOW ID DOMAINS
//	ALTER TABLE [IF EXISTS] t {ALTER | MODIFY} [COLUMN] c SET ID DOMAIN name
//	ALTER TABLE [IF EXISTS] t {ALTER | MODIFY} [COLUMN] c UNSET ID DOMAIN
//
// CREATE ID DOMAIN makes the domain's dictionary, the table
// iddomain.DictionarySchema.name of the Iceberg catalog, with the domain's
// tags on its value column, so that erasure finds the values it keeps. SET
// ID DOMAIN adds the column that holds c's ids (iddomain.HiddenColumn) and
// records the membership in a table property; c must carry the domain's
// tags. Like policies, domains are governance: only admins change them,
// and anyone may list them. Columns join domains only with
// IGLOO_ID_DOMAINS on.

type domainCmdKind int

const (
	dcCreate domainCmdKind = iota
	dcDrop
	dcShow
	dcSet
	dcUnset
)

type domainCmd struct {
	kind                  domainCmdKind
	tag                   string
	name                  string
	typ                   string
	tags                  map[string]string
	comment               string
	ifExists, ifNotExists bool
	table                 []string
	column                string
}

var domainColumns = wire.Columns{
	{Name: "name", Oid: pgtype.TextOID, Width: -1},
	{Name: "type", Oid: pgtype.TextOID, Width: -1},
	{Name: "tags", Oid: pgtype.TextOID, Width: -1},
	{Name: "generation", Oid: pgtype.Int8OID, Width: 8},
	{Name: "members", Oid: pgtype.TextOID, Width: -1},
	{Name: "next_id", Oid: pgtype.Int8OID, Width: 8},
	{Name: "comment", Oid: pgtype.TextOID, Width: -1},
	{Name: "created_by", Oid: pgtype.TextOID, Width: -1},
	{Name: "created_at", Oid: pgtype.TimestamptzOID, Width: 8},
}

func domainSyntax(format string, args ...any) error {
	return pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
}

// parseDomainCommand recognizes the statements above. ok is false for any
// other statement; err is set for one of them written wrong.
func parseDomainCommand(query string) (c *domainCmd, ok bool, err error) {
	sc := &scanner{s: query}
	c = &domainCmd{}
	switch {
	case sc.keywords("CREATE", "ID", "DOMAIN"):
		c.kind, c.tag = dcCreate, "CREATE ID DOMAIN"
		c.ifNotExists = sc.keywords("IF", "NOT", "EXISTS")
		if c.name, err = domainName(sc); err != nil {
			return nil, true, err
		}
		if !sc.punct('(') {
			return nil, true, domainSyntax("expected CREATE ID DOMAIN name (type)")
		}
		typ, ok := sc.name()
		if !ok || len(typ) != 1 || !sc.punct(')') {
			return nil, true, domainSyntax("expected the type of the domain's values, as in CREATE ID DOMAIN people (VARCHAR)")
		}
		c.typ = typ[0]
		if sc.keywords("TAG") {
			c.tags = map[string]string{}
			for {
				name, err := objectName(sc)
				if err != nil {
					return nil, true, err
				}
				sc.skip()
				if !strings.HasPrefix(sc.rest(), "=") {
					return nil, true, domainSyntax("expected TAG name = 'value'")
				}
				sc.i++
				v, ok := sc.literal()
				if !ok || v == nil {
					return nil, true, domainSyntax("a tag's value is a string, as in TAG pii = 'person_id'")
				}
				c.tags[name] = *v
				if !sc.punct(',') {
					break
				}
			}
		}
		if sc.keywords("COMMENT") {
			sc.skip()
			if strings.HasPrefix(sc.rest(), "=") {
				sc.i++
			}
			v, ok := sc.literal()
			if !ok {
				return nil, true, domainSyntax("expected COMMENT = 'text'")
			}
			if v != nil {
				c.comment = *v
			}
		}
	case sc.keywords("DROP", "ID", "DOMAIN"):
		c.kind, c.tag = dcDrop, "DROP ID DOMAIN"
		c.ifExists = sc.keywords("IF", "EXISTS")
		if c.name, err = domainName(sc); err != nil {
			return nil, true, err
		}
	case sc.keywords("SHOW", "ID", "DOMAINS"):
		c.kind, c.tag = dcShow, "SHOW ID DOMAINS"
	case sc.keywords("ALTER", "TABLE"):
		c.ifExists = sc.keywords("IF", "EXISTS")
		if c.table, ok = sc.name(); !ok {
			return nil, false, nil
		}
		if !sc.keywords("ALTER") && !sc.keywords("MODIFY") {
			return nil, false, nil
		}
		sc.keywords("COLUMN")
		col, ok := sc.name()
		if !ok || len(col) != 1 {
			return nil, false, nil
		}
		c.column, c.tag = col[0], "ALTER TABLE"
		switch {
		case sc.keywords("SET", "ID", "DOMAIN"):
			c.kind = dcSet
			if c.name, err = domainName(sc); err != nil {
				return nil, true, err
			}
		case sc.keywords("UNSET", "ID", "DOMAIN"):
			c.kind = dcUnset
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

// domainName reads a domain's name, which names its dictionary table.
func domainName(sc *scanner) (string, error) {
	name, ok := sc.name()
	if !ok || len(name) != 1 {
		return "", domainSyntax("expected an ID domain's name")
	}
	if err := iddomain.CheckName(name[0]); err != nil {
		return "", pgerror.WithCode(err, codes.InvalidName)
	}
	return name[0], nil
}

// domainStatement handles an ID domain statement. ok is false for anything
// else.
func (s *Server) domainStatement(sess *session, q wire.Query) (wire.PreparedStatements, bool, error) {
	c, ok, err := parseDomainCommand(q.Query)
	if !ok {
		return nil, false, nil
	}
	if err != nil {
		s.recordQuery(sess, queryRecord{text: q.Query, start: time.Now(), err: err, rows: -1})
		return nil, true, err
	}
	var opts []wire.PreparedOptionFn
	if c.kind == dcShow {
		opts = append(opts, wire.WithColumns(domainColumns))
	}
	return wire.Prepared(wire.NewStatement(s.audited(sess, q.Query, func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		start := time.Now()
		err := s.runDomainCommand(ctx, sess, c, w)
		s.metrics.query(start, err)
		return err
	}), opts...)), true, nil
}

func (s *Server) runDomainCommand(ctx context.Context, sess *session, c *domainCmd, w wire.DataWriter) error {
	user, err := s.refreshUser(ctx, sess)
	if err != nil {
		return err
	}
	if c.kind == dcShow {
		return s.showDomains(ctx, w)
	}
	if user.Role != auth.RoleAdmin {
		return pgerror.WithCode(fmt.Errorf("permission denied: %s requires the admin role", c.tag), codes.InsufficientPrivilege)
	}
	if s.catalogName() == "" || s.ddl == nil {
		return pgerror.WithCode(errors.New("ID domains need an Iceberg catalog (IGLOO_CATALOG_URI)"), codes.FeatureNotSupported)
	}
	if sess.inTx {
		return pgerror.WithCode(fmt.Errorf("%s cannot run inside a transaction block", c.tag), codes.ActiveSQLTransaction)
	}
	switch c.kind {
	case dcCreate:
		err = s.createDomain(ctx, sess, user, c)
	case dcDrop:
		err = s.dropDomain(ctx, sess, c)
	case dcSet:
		err = s.joinDomain(ctx, sess, user, c)
	case dcUnset:
		err = s.leaveDomain(ctx, sess, c)
	}
	if err != nil {
		return domainError(err)
	}
	s.log.Info("ID domains changed", "statement", c.tag, "domain", c.name, "table", strings.Join(c.table, "."), "column", c.column, "by", user.Name)
	return w.Complete(c.tag)
}

// dictionary is the quoted name of domain's dictionary table.
func (s *Server) dictionary(domain string) string {
	return quoteName([]string{s.catalogName(), iddomain.DictionarySchema, domain})
}

func (s *Server) createDomain(ctx context.Context, sess *session, user auth.User, c *domainCmd) error {
	typ, err := iddomain.CheckType(c.typ)
	if err != nil {
		return err
	}
	snap := s.gov.snapshot(ctx)
	for name, value := range c.tags {
		tag, ok := snap.Catalog.Tag(name)
		if !ok {
			return pgerror.WithCode(fmt.Errorf("tag %q does not exist", name), codes.UndefinedObject)
		}
		if len(tag.AllowedValues) > 0 && !slices.Contains(tag.AllowedValues, value) {
			return pgerror.WithCode(fmt.Errorf("tag %s allows %s, not %q", name, strings.Join(tag.AllowedValues, ", "), value), codes.InvalidParameterValue)
		}
	}
	cur, err := s.idDomains.Load(ctx)
	if err != nil {
		return err
	}
	if cur.Domain(c.name) != nil {
		if c.ifNotExists {
			return nil
		}
		return fmt.Errorf("ID domain %s %w", c.name, iddomain.ErrExists)
	}
	// A domain made again under a name counts ids anew, in a generation of
	// its own.
	_, gen, err := s.idDomains.Next(ctx, c.name)
	if err != nil {
		return err
	}
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS " + quoteName([]string{s.catalogName(), iddomain.DictionarySchema}),
		"CREATE TABLE IF NOT EXISTS " + s.dictionary(c.name) + " (value " + typ + ", id BIGINT, assigned_at TIMESTAMP WITH TIME ZONE)",
	} {
		if _, err := sess.conn.ExecContext(ctx, q, nil); err != nil {
			return pgError(err)
		}
	}
	// A dictionary left by a CREATE that stopped half way is taken over
	// only while empty.
	rows, err := queryRows(ctx, sess.conn, "SELECT count(*) FROM "+s.dictionary(c.name))
	if err != nil {
		return pgError(err)
	}
	var n int64
	if len(rows) == 1 && len(rows[0]) == 1 {
		n, _ = rows[0][0].(int64)
	}
	if n > 0 {
		return pgerror.WithCode(fmt.Errorf("the table %s.%s already holds %d rows; drop it to create ID domain %s", iddomain.DictionarySchema, c.name, n, c.name),
			codes.DuplicateObject)
	}
	now := time.Now().UTC()
	err = s.gov.store.Update(ctx, func(cat *policy.Catalog) error {
		if _, ok := cat.Policy(iddomain.DictionaryPolicy); !ok {
			p := policy.Policy{Kind: policy.RowAccess, Name: iddomain.DictionaryPolicy, Args: []policy.Arg{{Name: "id", Type: "BIGINT"}},
				Returns: "BOOLEAN", Body: "false", CreatedBy: "igloo",
				Comment: "Igloo's own: keeps ID domains' dictionaries (igloo_domains) from every user's statements"}
			if err := cat.PutPolicy(p, false, now); err != nil {
				return err
			}
		}
		err := cat.AddRowFilter(policy.RowFilter{Schema: iddomain.DictionarySchema, Table: c.name, Policy: iddomain.DictionaryPolicy,
			Columns: []string{"id"}, SetBy: "igloo"}, now)
		if err != nil && !errors.Is(err, policy.ErrExists) {
			return err
		}
		for name, value := range c.tags {
			ct := policy.ColumnTag{Schema: iddomain.DictionarySchema, Table: c.name, Column: "value", Tag: name, Value: value, SetBy: user.Name}
			if err := cat.SetColumnTag(ct, now); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return policyStoreError(err)
	}
	if err := s.gov.reload(ctx); err != nil {
		s.log.Warn("could not reload policies", "err", err)
	}
	d := iddomain.Domain{Name: c.name, Type: typ, Tags: c.tags, Generation: gen + 1, Created: now, CreatedBy: user.Name, Comment: c.comment}
	if err := s.idDomains.Update(ctx, func(cat *iddomain.Catalog) error { return cat.AddDomain(d) }); err != nil {
		return err
	}
	s.ddl.TableChanged(iddomain.DictionarySchema, c.name)
	return nil
}

func (s *Server) dropDomain(ctx context.Context, sess *session, c *domainCmd) error {
	cur, err := s.idDomains.Load(ctx)
	if err != nil {
		return err
	}
	d := cur.Domain(c.name)
	if d == nil {
		if c.ifExists {
			return nil
		}
		return fmt.Errorf("ID domain %s %w", c.name, iddomain.ErrNotFound)
	}
	if ms := cur.DomainMembers(c.name); len(ms) > 0 {
		return fmt.Errorf("ID domain %s %w: %s is a member (ALTER TABLE ... UNSET ID DOMAIN first)", c.name, iddomain.ErrInUse, ms[0])
	}
	// The values go first: a domain whose dictionary is gone can still be
	// dropped, while a dictionary no domain owns would keep them unseen.
	if _, err := sess.conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+s.dictionary(c.name), nil); err != nil {
		return pgError(err)
	}
	err = s.gov.store.Update(ctx, func(cat *policy.Catalog) error {
		if err := cat.DropRowFilter("", iddomain.DictionarySchema, c.name, iddomain.DictionaryPolicy); err != nil && !errors.Is(err, policy.ErrNotFound) {
			return err
		}
		for name := range d.Tags {
			if err := cat.UnsetColumnTag("", iddomain.DictionarySchema, c.name, "value", name); err != nil && !errors.Is(err, policy.ErrNotFound) {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return policyStoreError(err)
	}
	if err := s.gov.reload(ctx); err != nil {
		s.log.Warn("could not reload policies", "err", err)
	}
	if err := s.idDomains.Update(ctx, func(cat *iddomain.Catalog) error { return cat.DropDomain(c.name) }); err != nil {
		return err
	}
	s.ddl.TableChanged(iddomain.DictionarySchema, c.name)
	return nil
}

// domainTable resolves the table of an ALTER TABLE ... ID DOMAIN, which
// must be a table of the Iceberg catalog outside the dictionaries' schema,
// and its columns.
func (s *Server) domainTable(ctx context.Context, sess *session, c *domainCmd, exists bool) (schema, table string, cols []columnType, err error) {
	catalog, schema, table, cols, err := s.policyTable(ctx, sess, c.table, exists)
	if err != nil {
		return "", "", nil, err
	}
	if catalog != "" {
		return "", "", nil, pgerror.WithCode(fmt.Errorf("%s is a table of connection %s: only the %s catalog's tables' columns join ID domains",
			strings.Join(c.table, "."), catalog, s.catalogName()), codes.WrongObjectType)
	}
	if strings.EqualFold(schema, iddomain.DictionarySchema) {
		return "", "", nil, pgerror.WithCode(fmt.Errorf("the schema %s is Igloo's own", iddomain.DictionarySchema), codes.InsufficientPrivilege)
	}
	return schema, table, cols, nil
}

func (s *Server) joinDomain(ctx context.Context, sess *session, user auth.User, c *domainCmd) error {
	if !s.idDomainsOn {
		return pgerror.WithCode(errors.New("columns join ID domains only with IGLOO_ID_DOMAINS=true"), codes.FeatureNotSupported)
	}
	schema, table, cols, err := s.domainTable(ctx, sess, c, true)
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
	d := cur.Domain(c.name)
	if d == nil {
		return fmt.Errorf("ID domain %s %w", c.name, iddomain.ErrNotFound)
	}
	i := slices.IndexFunc(cols, func(col columnType) bool { return strings.EqualFold(col.name, c.column) })
	if i < 0 {
		return pgerror.WithCode(fmt.Errorf("column %q of table %s.%s does not exist", c.column, schema, table), codes.UndefinedColumn)
	}
	col := cols[i]
	if iddomain.IsHidden(col.name) {
		return pgerror.WithCode(fmt.Errorf("column %s holds Igloo's ids itself", col.name), codes.InvalidColumnReference)
	}
	if !strings.EqualFold(col.typ, d.Type) {
		return pgerror.WithCode(fmt.Errorf("ID domain %s holds %s values, but column %s is %s", d.Name, d.Type, col.name, col.typ), codes.DatatypeMismatch)
	}
	snap := s.gov.snapshot(ctx)
	for name, value := range d.Tags {
		if !slices.ContainsFunc(snap.Catalog.ColumnTags, func(t policy.ColumnTag) bool {
			return t.Catalog == "" && strings.EqualFold(t.Schema, schema) && strings.EqualFold(t.Table, table) &&
				strings.EqualFold(t.Column, col.name) && t.Tag == name && t.Value == value
		}) {
			return pgerror.WithCode(fmt.Errorf("column %s must carry ID domain %s's tag %s = '%s', so that erasure finds its values with the dictionary's: "+
				"ALTER TABLE %s.%s ALTER COLUMN %s SET TAG %s = '%s' first", col.name, d.Name, name, value, schema, table, col.name, name, value),
				codes.ObjectNotInPrerequisiteState)
		}
	}
	if old := cur.MemberOf(schema, table, col.name); old != nil && old.Domain != d.Name {
		return fmt.Errorf("%w: %s is a member of ID domain %s (UNSET ID DOMAIN first)", iddomain.ErrInvalid, old, old.Domain)
	}
	if _, err := s.ddl.JoinIDDomain(ctx, schema, table, col.name, iddomain.HiddenColumn(col.name), d.Name, d.Generation); err != nil {
		switch {
		case errors.Is(err, ddl.ErrNotFound):
			return pgerror.WithCode(err, codes.UndefinedTable)
		case errors.Is(err, catalog.ErrHiddenColumnTaken):
			return pgerror.WithCode(err, codes.DuplicateColumn)
		}
		return err
	}
	m := iddomain.Member{Domain: d.Name, Schema: schema, Table: table, Column: col.name, Added: time.Now().UTC(), AddedBy: user.Name}
	defer s.forgetMembers()
	return s.idDomains.Update(ctx, func(cat *iddomain.Catalog) error { return cat.AddMember(m) })
}

func (s *Server) leaveDomain(ctx context.Context, sess *session, c *domainCmd) error {
	schema, table, _, err := s.domainTable(ctx, sess, c, false)
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
	m := cur.MemberOf(schema, table, c.column)
	if m == nil {
		return pgerror.WithCode(fmt.Errorf("column %s of table %s.%s is not a member of an ID domain", c.column, schema, table), codes.UndefinedObject)
	}
	if err := s.ddl.LeaveIDDomain(ctx, schema, table, m.Column, iddomain.HiddenColumn(m.Column)); err != nil {
		return err
	}
	defer s.forgetMembers()
	return s.idDomains.Update(ctx, func(cat *iddomain.Catalog) error {
		cat.RemoveMember(schema, table, m.Column)
		return nil
	})
}

func (s *Server) showDomains(ctx context.Context, w wire.DataWriter) error {
	cur, err := s.idDomains.Load(ctx)
	if err != nil {
		return err
	}
	domains := slices.Clone(cur.Domains)
	slices.SortFunc(domains, func(a, b iddomain.Domain) int { return strings.Compare(a.Name, b.Name) })
	for _, d := range domains {
		var tags []string
		for name, value := range d.Tags {
			tags = append(tags, name+" = '"+strings.ReplaceAll(value, "'", "''")+"'")
		}
		slices.Sort(tags)
		var members []string
		for _, m := range cur.DomainMembers(d.Name) {
			members = append(members, m.String())
		}
		slices.Sort(members)
		next, gen, err := s.idDomains.Next(ctx, d.Name)
		if err != nil {
			return err
		}
		if gen != d.Generation {
			next = 0
		}
		row := []any{d.Name, d.Type, strings.Join(tags, ", "), int64(d.Generation), strings.Join(members, ", "), next, d.Comment, d.CreatedBy, d.Created}
		if err := w.Row(row); err != nil {
			return err
		}
	}
	return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
}

// domainError gives ID domain errors PostgreSQL codes.
func domainError(err error) error {
	switch {
	case pgerror.GetCode(err) != codes.Uncategorized:
		return err
	case errors.Is(err, iddomain.ErrNotFound):
		return pgerror.WithCode(err, codes.UndefinedObject)
	case errors.Is(err, iddomain.ErrExists):
		return pgerror.WithCode(err, codes.DuplicateObject)
	case errors.Is(err, iddomain.ErrInUse):
		return pgerror.WithCode(err, codes.DependentObjectsStillExist)
	case errors.Is(err, iddomain.ErrInvalid):
		return pgerror.WithCode(err, codes.InvalidParameterValue)
	}
	return err
}

// dictionaryDenied is the refusal of a change to what Igloo keeps for ID
// domains: their dictionaries' schema and the policy that guards them.
func dictionaryDenied() error {
	return pgerror.WithCode(fmt.Errorf("permission denied: the schema %s and the row access policy %s are Igloo's own, which keep ID domains' "+
		"dictionaries; use CREATE and DROP ID DOMAIN", iddomain.DictionarySchema, iddomain.DictionaryPolicy), codes.InsufficientPrivilege)
}

// dictionaryWrites refuses, whoever the user, a statement of type t that
// would write, create, drop or alter anything in the dictionaries' schema,
// which only Igloo changes. Any write that names the schema counts.
func dictionaryWrites(t duckdb.StmtType, query string) error {
	if !slices.Contains(tableWrites, t) {
		return nil
	}
	if tokensOf(query).mentions(map[string]bool{iddomain.DictionarySchema: true}) {
		return dictionaryDenied()
	}
	return nil
}

// dictionaryPolicyChange refuses a policy or tag statement about the
// dictionaries' policy, or about a table of their schema.
func dictionaryPolicyChange(c *policyCmd) error {
	for _, name := range []string{c.name, c.policy.Name} {
		if strings.EqualFold(name, iddomain.DictionaryPolicy) {
			return dictionaryDenied()
		}
	}
	if len(c.table) >= 2 && strings.EqualFold(c.table[len(c.table)-2], iddomain.DictionarySchema) {
		return dictionaryDenied()
	}
	return nil
}

// memberCheck is how often a replica asks the store whether ID domains'
// members changed, which other replicas' statements may have changed.
const memberCheck = 5 * time.Second

// memberCache is which tables have columns that hold ids, as of the store's
// version when last checked.
type memberCache struct {
	mu      sync.Mutex
	checked time.Time
	version int64
	loaded  bool
	// tables are schema.table and names the tables' names, lower case.
	tables, names map[string]bool
	// domains are the domains each table's columns are members of, by
	// lower-case schema.table.
	domains map[string][]string
	// snap is the store's catalog as last loaded.
	snap *iddomain.Snapshot
}

// memberTables returns the tables with columns that hold ids, by lower-case
// schema.table, and their names. A change since the last check counts as a
// change of the catalog, so statements' policy checks are worked out again
// (see sensitivity).
func (s *Server) memberTables(ctx context.Context) (tables, names map[string]bool) {
	c := &s.members
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.loaded && time.Since(c.checked) < memberCheck {
		return c.tables, c.names
	}
	c.checked = time.Now()
	v, err := s.idDomains.Version(ctx)
	if err != nil {
		s.log.Warn("could not check ID domains' members", "err", err)
		return c.tables, c.names
	}
	if c.loaded && v == c.version {
		return c.tables, c.names
	}
	snap, err := s.idDomains.Load(ctx)
	if err != nil {
		s.log.Warn("could not load ID domains' members", "err", err)
		return c.tables, c.names
	}
	tables, names = map[string]bool{}, map[string]bool{}
	domains := map[string][]string{}
	for _, m := range snap.Members {
		key := strings.ToLower(m.Schema + "." + m.Table)
		tables[key] = true
		names[strings.ToLower(m.Table)] = true
		if !slices.Contains(domains[key], m.Domain) {
			domains[key] = append(domains[key], m.Domain)
		}
	}
	changed := c.loaded
	c.tables, c.names, c.domains, c.version, c.loaded, c.snap = tables, names, domains, v, true, snap
	if changed {
		s.catalogChanged()
	}
	return tables, names
}

// memberSnapshot returns the domains and members as memberTables last
// loaded them, or nil if it could not.
func (s *Server) memberSnapshot(ctx context.Context) *iddomain.Snapshot {
	s.memberTables(ctx)
	s.members.mu.Lock()
	defer s.members.mu.Unlock()
	return s.members.snap
}

// forgetMembers makes the next memberTables ask the store, after this
// replica changed members.
func (s *Server) forgetMembers() {
	s.members.mu.Lock()
	s.members.checked = time.Time{}
	s.members.mu.Unlock()
	s.catalogChanged()
}

// hiddenReferences refuses a statement a user wrote that names a column
// that holds ids, or would make one: Igloo's alone, which statements read
// as if the tables did not have them.
func hiddenReferences(text string) error {
	names := sqllex.CodeWords(text)
	for _, v := range textValues(text) {
		if v.ident {
			names = append(names, v.value)
		}
	}
	for _, n := range names {
		if iddomain.IsHidden(n) {
			return pgerror.WithCode(fmt.Errorf("permission denied: names beginning with _igloo_id_ are Igloo's own, the columns that hold ID domains' ids"),
				codes.InsufficientPrivilege)
		}
	}
	return nil
}

// memberDomains returns the domains columns of the catalog table
// schema.table are members of, sorted, or none.
func (s *Server) memberDomains(ctx context.Context, schema, table string) []string {
	s.memberTables(ctx)
	s.members.mu.Lock()
	defer s.members.mu.Unlock()
	out := slices.Clone(s.members.domains[strings.ToLower(schema+"."+table)])
	slices.Sort(out)
	return out
}
