package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/policy"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Policy and tag statements, after Snowflake's. DuckDB has none of them,
// so Igloo carries them out itself, like user management:
//
//	CREATE [OR REPLACE] MASKING POLICY [IF NOT EXISTS] name
//	    AS (arg type) RETURNS type -> expression [COMMENT = '...']
//	CREATE [OR REPLACE] ROW ACCESS POLICY [IF NOT EXISTS] name
//	    AS (arg type, ...) RETURNS BOOLEAN -> expression [COMMENT = '...']
//	DROP MASKING POLICY | ROW ACCESS POLICY [IF EXISTS] name
//	ALTER TABLE t {ALTER | MODIFY} [COLUMN] c SET MASKING POLICY name [FORCE]
//	ALTER TABLE t {ALTER | MODIFY} [COLUMN] c UNSET MASKING POLICY
//	ALTER TABLE t ADD ROW ACCESS POLICY name ON (column, ...)
//	ALTER TABLE t DROP ROW ACCESS POLICY name | DROP ALL ROW ACCESS POLICIES
//	CREATE [OR REPLACE] TAG [IF NOT EXISTS] name [ALLOWED_VALUES 'v', ...] [COMMENT = '...']
//	DROP TAG [IF EXISTS] name
//	ALTER TAG name SET | UNSET MASKING POLICY name [, MASKING POLICY name]
//	ALTER TABLE t {ALTER | MODIFY} [COLUMN] c SET TAG name = 'value' [, ...]
//	ALTER TABLE t {ALTER | MODIFY} [COLUMN] c UNSET TAG name [, ...]
//	SHOW [MASKING | ROW ACCESS] POLICIES
//	SHOW TAGS
//
// Only admins change policies and tags; anyone may list them, and only
// admins see policies' expressions.

type policyCmdKind int

const (
	pcCreatePolicy policyCmdKind = iota
	pcDropPolicy
	pcSetMask
	pcUnsetMask
	pcAddRowPolicy
	pcDropRowPolicy
	pcCreateTag
	pcDropTag
	pcSetTagPolicy
	pcUnsetTagPolicy
	pcSetColumnTag
	pcUnsetColumnTag
	pcShowPolicies
	pcShowTags
)

// policyCmd is a parsed policy or tag statement.
type policyCmd struct {
	kind policyCmdKind
	// tag is the command tag the statement completes with.
	tag string
	// policy is CREATE POLICY's, with its argument and return types as
	// written.
	policy               policy.Policy
	replace, ifNotExists bool
	ifExists             bool
	// name is the policy or tag dropped, set or unset; policyKind the
	// kind of policy.
	name       string
	policyKind policy.Kind
	// table and column are what ALTER TABLE changes.
	table  []string
	column string
	force  bool
	// columns are ADD ROW ACCESS POLICY's.
	columns []string
	// tags are SET TAG's names and values, or UNSET TAG's names.
	tags     []policy.ColumnTag
	tagDef   policy.Tag
	policies []string
}

var (
	policyColumns = wire.Columns{
		{Name: "name", Oid: pgtype.TextOID, Width: -1},
		{Name: "kind", Oid: pgtype.TextOID, Width: -1},
		{Name: "signature", Oid: pgtype.TextOID, Width: -1},
		{Name: "body", Oid: pgtype.TextOID, Width: -1},
		{Name: "comment", Oid: pgtype.TextOID, Width: -1},
		{Name: "created_by", Oid: pgtype.TextOID, Width: -1},
		{Name: "created_at", Oid: pgtype.TimestamptzOID, Width: 8},
		{Name: "updated_at", Oid: pgtype.TimestamptzOID, Width: 8},
	}
	tagColumns = wire.Columns{
		{Name: "name", Oid: pgtype.TextOID, Width: -1},
		{Name: "allowed_values", Oid: pgtype.TextOID, Width: -1},
		{Name: "masking_policies", Oid: pgtype.TextOID, Width: -1},
		{Name: "comment", Oid: pgtype.TextOID, Width: -1},
		{Name: "created_by", Oid: pgtype.TextOID, Width: -1},
		{Name: "created_at", Oid: pgtype.TimestamptzOID, Width: 8},
	}
)

func policySyntax(format string, args ...any) error {
	return pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
}

// parsePolicyCommand parses one statement. ok is false when it is not a
// policy or tag statement.
func parsePolicyCommand(query string) (c *policyCmd, ok bool, err error) {
	sc := &scanner{s: query}
	c = &policyCmd{}
	switch {
	case sc.keywords("SHOW", "POLICIES"):
		c.kind, c.tag = pcShowPolicies, "SHOW"
	case sc.keywords("SHOW", "MASKING", "POLICIES"):
		c.kind, c.tag, c.policyKind = pcShowPolicies, "SHOW", policy.Masking
	case sc.keywords("SHOW", "ROW", "ACCESS", "POLICIES"):
		c.kind, c.tag, c.policyKind = pcShowPolicies, "SHOW", policy.RowAccess
	case sc.keywords("SHOW", "TAGS"):
		c.kind, c.tag = pcShowTags, "SHOW"
	case sc.keywords("DROP", "MASKING", "POLICY"):
		c.kind, c.tag, c.policyKind = pcDropPolicy, "DROP MASKING POLICY", policy.Masking
	case sc.keywords("DROP", "ROW", "ACCESS", "POLICY"):
		c.kind, c.tag, c.policyKind = pcDropPolicy, "DROP ROW ACCESS POLICY", policy.RowAccess
	case sc.keywords("DROP", "TAG"):
		c.kind, c.tag = pcDropTag, "DROP TAG"
	case sc.keywords("ALTER", "TAG"):
		return c, true, c.parseAlterTag(sc)
	case sc.keywords("ALTER", "TABLE"):
		return c.parseAlterTable(sc)
	default:
		start := *sc
		if !sc.keywords("CREATE") {
			return nil, false, nil
		}
		c.replace = sc.keywords("OR", "REPLACE")
		switch {
		case sc.keywords("MASKING", "POLICY"):
			c.kind, c.tag, c.policy.Kind = pcCreatePolicy, "CREATE MASKING POLICY", policy.Masking
		case sc.keywords("ROW", "ACCESS", "POLICY"):
			c.kind, c.tag, c.policy.Kind = pcCreatePolicy, "CREATE ROW ACCESS POLICY", policy.RowAccess
		case sc.keywords("TAG"):
			c.kind, c.tag = pcCreateTag, "CREATE TAG"
		default:
			*sc = start
			return nil, false, nil
		}
		c.ifNotExists = sc.keywords("IF", "NOT", "EXISTS")
		if c.replace && c.ifNotExists {
			return nil, true, policySyntax("OR REPLACE and IF NOT EXISTS cannot both be used")
		}
		if c.kind == pcCreateTag {
			return c, true, c.parseCreateTag(sc)
		}
		return c, true, c.parseCreatePolicy(sc)
	}
	switch c.kind {
	case pcShowPolicies, pcShowTags:
		if !sc.done() {
			return nil, true, policySyntax("unexpected %q after SHOW", strings.TrimSpace(sc.rest()))
		}
	case pcDropPolicy, pcDropTag:
		c.ifExists = sc.keywords("IF", "EXISTS")
		if c.name, err = objectName(sc); err != nil {
			return nil, true, err
		}
		if !sc.done() {
			return nil, true, policySyntax("unexpected %q after the name", strings.TrimSpace(sc.rest()))
		}
	}
	return c, true, nil
}

// objectName reads the name of a policy or tag: lowercase letters, digits
// and underscores, like user and role names.
func objectName(sc *scanner) (string, error) {
	name, ok := sc.name()
	if !ok || len(name) != 1 {
		return "", policySyntax("expected a name")
	}
	if !config.ValidName(name[0]) {
		return "", policySyntax("invalid name %q: use lowercase letters, digits and underscores", name[0])
	}
	return name[0], nil
}

func (c *policyCmd) parseCreatePolicy(sc *scanner) error {
	var err error
	if c.policy.Name, err = objectName(sc); err != nil {
		return err
	}
	if !sc.keywords("AS") {
		return policySyntax("expected AS (argument type, ...) after the policy's name")
	}
	sc.skip()
	if sc.i >= len(sc.s) || sc.s[sc.i] != '(' {
		return policySyntax("expected the policy's arguments in parentheses")
	}
	end := matchParen(sc.s, sc.i)
	if end < 0 {
		return policySyntax("unbalanced parentheses")
	}
	for _, part := range splitTopLevel(sc.s[sc.i+1:end], ',') {
		a := &scanner{s: part}
		name, ok := a.name()
		if !ok || len(name) != 1 {
			return policySyntax("expected an argument name and type, as in (val VARCHAR)")
		}
		typ := strings.TrimSpace(a.rest())
		if typ == "" {
			return policySyntax("argument %s needs a type", name[0])
		}
		c.policy.Args = append(c.policy.Args, policy.Arg{Name: name[0], Type: typ})
	}
	sc.i = end + 1
	if !sc.keywords("RETURNS") {
		return policySyntax("expected RETURNS type after the arguments")
	}
	arrow := strings.Index(sqllex.CodeOnly(sc.rest()), "->")
	if arrow < 0 {
		return policySyntax("expected -> and the policy's expression after RETURNS")
	}
	c.policy.Returns = strings.TrimSpace(sc.rest()[:arrow])
	body := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sc.rest()[arrow+2:]), ";"))
	if i := topLevelKeyword(body, "COMMENT"); i >= 0 {
		k := &scanner{s: body, i: i + len("COMMENT")}
		k.skip()
		if strings.HasPrefix(k.rest(), "=") {
			k.i++
		}
		text, ok := k.literal()
		if !ok || !k.done() {
			return policySyntax("expected COMMENT = 'text' at the end")
		}
		if text != nil {
			c.policy.Comment = *text
		}
		body = strings.TrimSpace(body[:i])
	}
	if body == "" {
		return policySyntax("the policy needs an expression after ->")
	}
	c.policy.Body = body
	switch {
	case len(c.policy.Args) == 0:
		return policySyntax("a policy takes at least one argument")
	case c.policy.Kind == policy.Masking && len(c.policy.Args) != 1:
		return policySyntax("a masking policy takes one argument: the column's value")
	case len(c.policy.Args) > maxPolicyArgs:
		return policySyntax("a row access policy takes at most %d arguments", maxPolicyArgs)
	case c.policy.Kind == policy.RowAccess && !strings.EqualFold(c.policy.Returns, "BOOLEAN") && !strings.EqualFold(c.policy.Returns, "BOOL"):
		return policySyntax("a row access policy RETURNS BOOLEAN")
	}
	return nil
}

func (c *policyCmd) parseCreateTag(sc *scanner) error {
	var err error
	if c.tagDef.Name, err = objectName(sc); err != nil {
		return err
	}
	if sc.keywords("ALLOWED_VALUES") {
		for {
			v, ok := sc.literal()
			if !ok || v == nil {
				return policySyntax("ALLOWED_VALUES takes strings, as in ALLOWED_VALUES 'email', 'ip'")
			}
			c.tagDef.AllowedValues = append(c.tagDef.AllowedValues, *v)
			sc.skip()
			if sc.i < len(sc.s) && sc.s[sc.i] == ',' {
				sc.i++
				continue
			}
			break
		}
	}
	if sc.keywords("COMMENT") {
		sc.skip()
		if strings.HasPrefix(sc.rest(), "=") {
			sc.i++
		}
		v, ok := sc.literal()
		if !ok {
			return policySyntax("expected COMMENT = 'text'")
		}
		if v != nil {
			c.tagDef.Comment = *v
		}
	}
	if !sc.done() {
		return policySyntax("unexpected %q in CREATE TAG", strings.TrimSpace(sc.rest()))
	}
	return nil
}

func (c *policyCmd) parseAlterTag(sc *scanner) error {
	var err error
	if c.name, err = objectName(sc); err != nil {
		return err
	}
	switch {
	case sc.keywords("SET"):
		c.kind, c.tag = pcSetTagPolicy, "ALTER TAG"
	case sc.keywords("UNSET"):
		c.kind, c.tag = pcUnsetTagPolicy, "ALTER TAG"
	default:
		return policySyntax("expected ALTER TAG name SET | UNSET MASKING POLICY name")
	}
	for {
		if !sc.keywords("MASKING", "POLICY") && len(c.policies) == 0 {
			return policySyntax("expected MASKING POLICY name")
		}
		name, err := objectName(sc)
		if err != nil {
			return err
		}
		c.policies = append(c.policies, name)
		sc.skip()
		if sc.i < len(sc.s) && sc.s[sc.i] == ',' {
			sc.i++
			continue
		}
		break
	}
	if !sc.done() {
		return policySyntax("unexpected %q in ALTER TAG", strings.TrimSpace(sc.rest()))
	}
	return nil
}

// parseAlterTable claims the ALTER TABLE statements about policies and
// tags; others are DuckDB's.
func (c *policyCmd) parseAlterTable(sc *scanner) (*policyCmd, bool, error) {
	c.ifExists = sc.keywords("IF", "EXISTS")
	name, ok := sc.name()
	if !ok {
		return nil, false, nil
	}
	c.table, c.tag = name, "ALTER TABLE"
	switch {
	case sc.keywords("ADD", "ROW", "ACCESS", "POLICY"):
		c.kind = pcAddRowPolicy
		var err error
		if c.name, err = objectName(sc); err != nil {
			return nil, true, err
		}
		if !sc.keywords("ON") {
			return nil, true, policySyntax("expected ON (column, ...) after the policy's name")
		}
		sc.skip()
		if sc.i >= len(sc.s) || sc.s[sc.i] != '(' {
			return nil, true, policySyntax("expected ON (column, ...) after the policy's name")
		}
		end := matchParen(sc.s, sc.i)
		if end < 0 {
			return nil, true, policySyntax("unbalanced parentheses")
		}
		for _, part := range splitTopLevel(sc.s[sc.i+1:end], ',') {
			k := &scanner{s: part}
			col, ok := k.name()
			if !ok || len(col) != 1 || !k.done() {
				return nil, true, policySyntax("expected column names in ON (...)")
			}
			c.columns = append(c.columns, col[0])
		}
		sc.i = end + 1
	case sc.keywords("DROP", "ALL", "ROW", "ACCESS", "POLICIES"):
		c.kind = pcDropRowPolicy
	case sc.keywords("DROP", "ROW", "ACCESS", "POLICY"):
		c.kind = pcDropRowPolicy
		var err error
		if c.name, err = objectName(sc); err != nil {
			return nil, true, err
		}
	case sc.keywords("ALTER"), sc.keywords("MODIFY"):
		sc.keywords("COLUMN")
		col, ok := sc.name()
		if !ok || len(col) != 1 {
			return nil, false, nil
		}
		c.column = col[0]
		switch {
		case sc.keywords("SET", "MASKING", "POLICY"):
			c.kind = pcSetMask
			var err error
			if c.name, err = objectName(sc); err != nil {
				return nil, true, err
			}
			if sc.keywords("USING") {
				return nil, true, pgerror.WithCode(errors.New("masking policies take only the masked column; USING is not supported"),
					codes.FeatureNotSupported)
			}
			c.force = sc.keywords("FORCE")
		case sc.keywords("UNSET", "MASKING", "POLICY"):
			c.kind = pcUnsetMask
		case sc.keywords("SET", "TAG"):
			c.kind = pcSetColumnTag
			for {
				name, err := objectName(sc)
				if err != nil {
					return nil, true, err
				}
				sc.skip()
				if !strings.HasPrefix(sc.rest(), "=") {
					return nil, true, policySyntax("expected SET TAG name = 'value'")
				}
				sc.i++
				v, ok := sc.literal()
				if !ok || v == nil {
					return nil, true, policySyntax("a tag's value is a string, as in SET TAG pii = 'email'")
				}
				c.tags = append(c.tags, policy.ColumnTag{Tag: name, Value: *v})
				sc.skip()
				if sc.i < len(sc.s) && sc.s[sc.i] == ',' {
					sc.i++
					continue
				}
				break
			}
		case sc.keywords("UNSET", "TAG"):
			c.kind = pcUnsetColumnTag
			for {
				name, err := objectName(sc)
				if err != nil {
					return nil, true, err
				}
				c.tags = append(c.tags, policy.ColumnTag{Tag: name})
				sc.skip()
				if sc.i < len(sc.s) && sc.s[sc.i] == ',' {
					sc.i++
					continue
				}
				break
			}
		default:
			return nil, false, nil
		}
	default:
		return nil, false, nil
	}
	if !sc.done() {
		return nil, true, policySyntax("unexpected %q in ALTER TABLE", strings.TrimSpace(sc.rest()))
	}
	return c, true, nil
}

func (c *policyCmd) columnsOut() wire.Columns {
	switch c.kind {
	case pcShowPolicies:
		return policyColumns
	case pcShowTags:
		return tagColumns
	}
	return nil
}

// policyCommand handles a policy or tag statement. ok is false for
// anything else.
func (s *Server) policyCommand(ctx context.Context, sess *session, q wire.Query) (wire.PreparedStatements, bool, error) {
	stmts := sqllex.Split(q.Query)
	var cmd *policyCmd
	for _, st := range stmts {
		c, ok, err := parsePolicyCommand(st.Text)
		if !ok {
			continue
		}
		if err == nil && len(stmts) > 1 {
			err = pgerror.WithCode(errors.New("send policy and tag statements separately from other SQL"), codes.FeatureNotSupported)
		}
		if err != nil {
			s.recordQuery(sess, queryRecord{text: q.Query, start: time.Now(), err: err, rows: -1})
			return nil, true, err
		}
		cmd = c
	}
	if cmd == nil {
		return nil, false, nil
	}
	return wire.Prepared(wire.NewStatement(s.audited(sess, q.Query, func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		start := time.Now()
		err := s.runPolicyCommand(ctx, sess, cmd, w)
		s.metrics.query(start, err)
		return err
	}), wire.WithColumns(cmd.columnsOut()))), true, nil
}

func (s *Server) runPolicyCommand(ctx context.Context, sess *session, c *policyCmd, w wire.DataWriter) error {
	user, err := s.refreshUser(ctx, sess)
	if err != nil {
		return err
	}
	switch c.kind {
	case pcShowPolicies:
		return s.showPolicies(ctx, user, c, w)
	case pcShowTags:
		return s.showTags(ctx, w)
	}
	if user.Role != auth.RoleAdmin {
		return pgerror.WithCode(fmt.Errorf("permission denied: %s requires the admin role", c.tag), codes.InsufficientPrivilege)
	}
	if s.catalogName() == "" {
		return pgerror.WithCode(errors.New("policies need an Iceberg catalog (IGLOO_CATALOG_URI)"), codes.FeatureNotSupported)
	}
	if sess.inTx {
		return pgerror.WithCode(fmt.Errorf("%s cannot run inside a transaction block", c.tag), codes.ActiveSQLTransaction)
	}
	now := time.Now()
	var change func(*policy.Catalog) error
	switch c.kind {
	case pcCreatePolicy:
		p, err := s.checkPolicy(ctx, sess, user, c.policy)
		if err != nil {
			return err
		}
		p.CreatedBy = user.Name
		change = func(cat *policy.Catalog) error {
			if _, exists := cat.Policy(p.Name); exists && c.ifNotExists {
				return nil
			}
			return cat.PutPolicy(p, c.replace, now)
		}
	case pcDropPolicy:
		change = func(cat *policy.Catalog) error {
			err := cat.DropPolicy(c.policyKind, c.name)
			if errors.Is(err, policy.ErrNotFound) && c.ifExists {
				return nil
			}
			return err
		}
	case pcCreateTag:
		t := c.tagDef
		t.CreatedBy = user.Name
		change = func(cat *policy.Catalog) error {
			if _, exists := cat.Tag(t.Name); exists && c.ifNotExists {
				return nil
			}
			return cat.PutTag(t, c.replace, now)
		}
	case pcDropTag:
		change = func(cat *policy.Catalog) error {
			err := cat.DropTag(c.name)
			if errors.Is(err, policy.ErrNotFound) && c.ifExists {
				return nil
			}
			return err
		}
	case pcSetTagPolicy, pcUnsetTagPolicy:
		change = func(cat *policy.Catalog) error {
			for _, p := range c.policies {
				var err error
				if c.kind == pcSetTagPolicy {
					err = cat.SetTagPolicy(c.name, p)
				} else {
					err = cat.UnsetTagPolicy(c.name, p)
				}
				if err != nil {
					return err
				}
			}
			return nil
		}
	default:
		if change, err = s.tableChange(ctx, sess, user, c, now); err != nil || change == nil {
			return err
		}
	}
	if err := s.gov.store.Update(ctx, change); err != nil {
		return policyStoreError(err)
	}
	if err := s.gov.reload(ctx); err != nil {
		s.log.Warn("could not reload policies", "err", err)
	}
	s.log.Info("policies changed", "statement", c.tag, "name", firstNonEmpty(c.name, c.policy.Name, c.tagDef.Name),
		"table", strings.Join(c.table, "."), "column", c.column, "by", user.Name)
	return w.Complete(c.tag)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// tableChange works out an ALTER TABLE about policies and tags. A table
// that does not exist (any more) can still lose its policies and tags,
// named with its schema, as they stay with its name when it is dropped.
func (s *Server) tableChange(ctx context.Context, sess *session, user auth.User, c *policyCmd, now time.Time) (func(*policy.Catalog) error, error) {
	attach := c.kind == pcSetMask || c.kind == pcAddRowPolicy || c.kind == pcSetColumnTag
	catalog, schema, table, cols, err := s.policyTable(ctx, sess, c.table, attach)
	if err != nil {
		if c.ifExists && pgerror.GetCode(err) == codes.UndefinedTable {
			return nil, nil
		}
		return nil, err
	}
	column := func(name string) (columnType, error) {
		i := slices.IndexFunc(cols, func(col columnType) bool { return strings.EqualFold(col.name, name) })
		if i < 0 {
			return columnType{}, pgerror.WithCode(fmt.Errorf("column %q of table %s does not exist", name, policy.TableName(catalog, schema, table)),
				codes.UndefinedColumn)
		}
		return cols[i], nil
	}
	snap := s.gov.snapshot(ctx)
	// The column's name as the table has it, or as written for a table
	// that is gone.
	columnName := c.column
	if attach {
		col, err := column(c.column)
		if c.column != "" && err != nil {
			return nil, err
		}
		if c.column != "" {
			columnName = col.name
		}
	}
	switch c.kind {
	case pcSetMask:
		p, ok := snap.Catalog.Policy(c.name)
		if !ok || p.Kind != policy.Masking {
			return nil, pgerror.WithCode(fmt.Errorf("masking policy %q does not exist", c.name), codes.UndefinedObject)
		}
		col, _ := column(c.column)
		if !strings.EqualFold(p.Args[0].Type, col.typ) {
			return nil, pgerror.WithCode(fmt.Errorf("masking policy %s takes %s, but column %s is %s", p.Name, p.Args[0].Type, col.name, col.typ),
				codes.DatatypeMismatch)
		}
		m := policy.ColumnMask{Catalog: catalog, Schema: schema, Table: table, Column: columnName, Policy: c.name, SetBy: user.Name}
		return func(cat *policy.Catalog) error { return cat.SetMask(m, c.force, now) }, nil
	case pcUnsetMask:
		return func(cat *policy.Catalog) error { return cat.UnsetMask(catalog, schema, table, columnName) }, nil
	case pcAddRowPolicy:
		p, ok := snap.Catalog.Policy(c.name)
		if !ok || p.Kind != policy.RowAccess {
			return nil, pgerror.WithCode(fmt.Errorf("row access policy %q does not exist", c.name), codes.UndefinedObject)
		}
		if len(c.columns) != len(p.Args) {
			return nil, pgerror.WithCode(fmt.Errorf("row access policy %s takes %d columns, not %d", p.Name, len(p.Args), len(c.columns)),
				codes.InvalidParameterValue)
		}
		names := make([]string, len(c.columns))
		for i, name := range c.columns {
			col, err := column(name)
			if err != nil {
				return nil, err
			}
			if !strings.EqualFold(p.Args[i].Type, col.typ) {
				return nil, pgerror.WithCode(fmt.Errorf("row access policy %s takes %s, but column %s is %s", p.Name, p.Signature(), col.name, col.typ),
					codes.DatatypeMismatch)
			}
			names[i] = col.name
		}
		f := policy.RowFilter{Catalog: catalog, Schema: schema, Table: table, Policy: c.name, Columns: names, SetBy: user.Name}
		return func(cat *policy.Catalog) error { return cat.AddRowFilter(f, now) }, nil
	case pcDropRowPolicy:
		return func(cat *policy.Catalog) error { return cat.DropRowFilter(catalog, schema, table, c.name) }, nil
	case pcSetColumnTag:
		return func(cat *policy.Catalog) error {
			for _, t := range c.tags {
				ct := policy.ColumnTag{Catalog: catalog, Schema: schema, Table: table, Column: columnName, Tag: t.Tag, Value: t.Value, SetBy: user.Name}
				if err := cat.SetColumnTag(ct, now); err != nil {
					return err
				}
			}
			return nil
		}, nil
	case pcUnsetColumnTag:
		return func(cat *policy.Catalog) error {
			for _, t := range c.tags {
				if err := cat.UnsetColumnTag(catalog, schema, table, columnName, t.Tag); err != nil {
					return err
				}
			}
			return nil
		}, nil
	}
	return nil, errors.New("unknown policy statement")
}

// policyTable resolves the table an ALTER TABLE about policies names to a
// table of the catalog, or of a connection named in full (see
// connectionPolicyTable), and its columns. catalog is empty for the
// catalog's tables and the connection's name for a connection's. With
// exists false, a schema-qualified name of a table that no longer exists
// is accepted, with no columns.
func (s *Server) policyTable(ctx context.Context, sess *session, name []string, exists bool) (catalog, schema, table string, cols []columnType, err error) {
	catalogName := s.catalogName()
	if schema, table, ok := s.externalNamed(ctx, sess, name); ok && exists {
		return "", "", "", nil, pgerror.WithCode(fmt.Errorf("%s.%s is an external table (Delta Sharing): policies and tags attach only to the %s catalog's "+
			"tables and connections' tables, which Igloo reads through them. To protect its rows, copy what users may read into a table, such as "+
			"CREATE TABLE ... AS SELECT ... FROM %s.%s, and attach policies to that", schema, table, catalogName, schema, table), codes.WrongObjectType)
	}
	if len(name) == 3 && !strings.EqualFold(name[0], catalogName) {
		return s.connectionPolicyTable(ctx, sess, name, exists)
	}
	missing := pgerror.WithCode(fmt.Errorf("table %q does not exist", strings.Join(name, ".")), codes.UndefinedTable)
	objects, err := s.catalogCandidates(ctx, sess, name, false)
	if err != nil {
		return "", "", "", nil, err
	}
	for _, o := range objects {
		if s.ddl != nil && s.ddl.IsView(o.Schema, o.Table) {
			return "", "", "", nil, pgerror.WithCode(fmt.Errorf("%s is a view; policies attach to the tables it reads", o),
				codes.WrongObjectType)
		}
	}
	if len(objects) > 1 {
		return "", "", "", nil, pgerror.WithCode(fmt.Errorf("%s is ambiguous; qualify it with its schema", strings.Join(name, ".")),
			codes.AmbiguousAlias)
	}
	if len(objects) == 0 {
		if exists || len(name) < 2 {
			return "", "", "", nil, missing
		}
		return "", name[len(name)-2], name[len(name)-1], nil, nil
	}
	o := objects[0]
	cols, err = describeTable(ctx, sess, quoteName([]string{catalogName, o.Schema, o.Table}))
	if err != nil {
		return "", "", "", nil, pgError(err)
	}
	return "", o.Schema, o.Table, cols, nil
}

// connectionPolicyTable resolves connection.schema.table, a table of a
// connection to another database that policies are to protect. Only its
// full name counts, which nothing else can mean. The connection must be
// attached on this replica to have policies set on its tables, so their
// columns are known, but its tables' policies can be unset at any time,
// even once the connection is dropped.
func (s *Server) connectionPolicyTable(ctx context.Context, sess *session, name []string, exists bool) (string, string, string, []columnType, error) {
	conn := strings.ToLower(name[0])
	asWritten := func() (string, string, string, []columnType, error) {
		if exists {
			return "", "", "", nil, pgerror.WithCode(fmt.Errorf("table %q does not exist", strings.Join(name, ".")), codes.UndefinedTable)
		}
		return conn, name[1], name[2], nil, nil
	}
	if _, attached := s.connectionCatalogs()[conn]; !attached {
		known := false
		if s.fed != nil {
			var err error
			if known, err = s.fed.Exists(ctx, conn); err != nil {
				return "", "", "", nil, err
			}
		}
		// A connection dropped since can still lose its tables' policies.
		dropped := !known && !exists && slices.ContainsFunc(s.gov.snapshot(ctx).Tables(), func(t *policy.TableRules) bool { return t.Catalog == conn })
		switch {
		case !known && !dropped:
			return "", "", "", nil, pgerror.WithCode(fmt.Errorf("policies apply to tables of the %s catalog and of connections, and %q is neither",
				s.catalogName(), name[0]), codes.FeatureNotSupported)
		case known && exists:
			return "", "", "", nil, pgerror.WithCode(fmt.Errorf("connection %s is not attached on this replica yet, so its tables' columns are not known; "+
				"set policies on them once it is", conn), codes.ObjectNotInPrerequisiteState)
		}
		return asWritten()
	}
	r, err := s.newResolver(ctx, sess)
	if err != nil {
		return "", "", "", nil, err
	}
	res, err := r.resolve(tableRef{parts: name}, nil)
	if err != nil {
		return "", "", "", nil, err
	}
	var objects []object
	for _, o := range res.all {
		if strings.EqualFold(o.db, conn) {
			objects = append(objects, o)
		}
	}
	if len(objects) == 0 {
		return asWritten()
	}
	o := objects[0]
	if err := r.distinctInCase(o); err != nil {
		return "", "", "", nil, pgerror.WithCode(errors.New(strings.TrimPrefix(err.Error(), "permission denied: ")), codes.AmbiguousAlias)
	}
	cols, err := describeTable(ctx, sess, quoteName([]string{o.db, o.schema, o.name}))
	if err != nil {
		return "", "", "", nil, pgError(err)
	}
	return conn, o.schema, o.name, cols, nil
}

// checkPolicy validates a new policy and returns it as stored: types as
// DuckDB names them, and every table its expression reads fully
// qualified. The expression may read only tables of the catalog, call no
// macros and nothing that reads files, and depend on the session only
// through the context functions.
func (s *Server) checkPolicy(ctx context.Context, sess *session, user auth.User, p policy.Policy) (policy.Policy, error) {
	invalid := func(format string, args ...any) (policy.Policy, error) {
		return p, pgerror.WithCode(fmt.Errorf("policy %s: "+format, append([]any{p.Name}, args...)...), codes.InvalidParameterValue)
	}
	for i := range p.Args {
		if !config.ValidName(strings.ToLower(p.Args[i].Name)) {
			return invalid("invalid argument name %q", p.Args[i].Name)
		}
		p.Args[i].Name = strings.ToLower(p.Args[i].Name)
		t, err := normalizeType(ctx, sess, p.Args[i].Type)
		if err != nil {
			return invalid("unknown type %q: %v", p.Args[i].Type, trimContext(err))
		}
		p.Args[i].Type = t
	}
	ret, err := normalizeType(ctx, sess, p.Returns)
	if err != nil {
		return invalid("unknown type %q: %v", p.Returns, trimContext(err))
	}
	p.Returns = ret
	if p.Kind == policy.Masking && p.Returns != p.Args[0].Type {
		return invalid("a masking policy returns the type it takes, %s", p.Args[0].Type)
	}
	// Comments go: a line comment would swallow the SQL that follows the
	// body where it is put.
	var b strings.Builder
	for _, seg := range sqllex.Segments(p.Body) {
		if seg.Kind == sqllex.Comment {
			b.WriteByte(' ')
		} else {
			b.WriteString(seg.Text)
		}
	}
	p.Body = strings.TrimSpace(b.String())
	if err := forbidden(p.Body); err != nil {
		return p, err
	}
	if err := authorize(auth.RoleReader, duckdb.STATEMENT_TYPE_SELECT, "SELECT "+p.Body); err != nil {
		return invalid("it uses what only admins may: %v", err)
	}

	// Qualify every table the expression reads.
	r, err := s.newResolver(ctx, sess)
	if err != nil {
		return p, err
	}
	text := bodyPrefix + p.Body + ")"
	ast, err := r.serialize(text)
	if err != nil {
		return p, pgerror.WithCode(fmt.Errorf("policy %s: %v", p.Name, err), codes.Syntax)
	}
	ps := scanParse(ast)
	if ps.params {
		return invalid("parameters are not allowed")
	}
	macros, err := queryStrings(ctx, sess, `SELECT DISTINCT system.main.lower(function_name) FROM system.main.duckdb_functions()
		WHERE NOT internal AND function_type IN ('macro', 'table_macro')`)
	if err != nil {
		return p, err
	}
	for _, f := range ps.functions {
		name := strings.ToLower(f.name)
		switch {
		case slices.Contains(macros, name):
			return invalid("it calls macro %s, which could change after the policy is checked", f.name)
		case f.tableValue && !allowedFunctions[name]:
			return invalid("%s reads data that policies cannot check", f.name)
		case sessionStateFunctions[name]:
			return invalid("%s depends on the session, which the user controls", f.name)
		}
	}
	var edits []edit
	for _, ref := range ps.refs {
		if ref.cte {
			continue
		}
		res, err := r.resolve(tableRef{parts: ref.parts}, nil)
		if err != nil {
			return p, err
		}
		if len(res.first) != 1 || len(res.all) != 1 {
			return invalid("cannot tell which table %s is; qualify it", strings.Join(ref.parts, "."))
		}
		o := res.first[0]
		if o.view || !strings.EqualFold(o.db, s.catalogName()) {
			return invalid("%s is not a table of the %s catalog; policies read only its tables", strings.Join(ref.parts, "."), s.catalogName())
		}
		// An argument named like a column of the table would be ambiguous
		// where the expression names it without a qualifier.
		cols, err := describeTable(ctx, sess, quoteName([]string{o.db, o.schema, o.name}))
		if err != nil {
			return p, pgError(err)
		}
		for _, a := range p.Args {
			if slices.ContainsFunc(cols, func(c columnType) bool { return strings.EqualFold(c.name, a.Name) }) {
				return invalid("argument %s has the name of a column of %s.%s; name it otherwise, as in AS (arg_%s %s)",
					a.Name, o.schema, o.name, a.Name, a.Type)
			}
		}
		name, ok := scanName(text, ref.loc)
		if !ok || !sameParts(name.parts, ref.parts) {
			return invalid("cannot place table %s", strings.Join(ref.parts, "."))
		}
		full := quoteName([]string{o.db, o.schema, o.name})
		if ref.alias == "" {
			full += " AS " + quoteIdent(ref.parts[len(ref.parts)-1])
		}
		edits = append(edits, edit{name.start, name.end, full})
	}
	qualified, err := applyEdits(text, edits)
	if err != nil {
		return p, err
	}
	p.Body = strings.TrimSuffix(strings.TrimPrefix(qualified, bodyPrefix), ")")

	// It must parse as a template, and bind by itself to the right type.
	tmpl, err := r.parseBody(p)
	if err != nil {
		return p, pgerror.WithCode(err, codes.InvalidParameterValue)
	}
	privs, err := s.privileges(ctx, sess, user)
	if err != nil {
		return p, err
	}
	pc := policyContext{user: user.Name, roles: append(slices.Clone(privs.Roles), string(user.Role))}
	body := tmpl.render(probeArgs(p), pc, func(t string) string { return t })
	typ, err := s.gov.probe.describe(ctx, body)
	if err != nil {
		return invalid("%v", trimContext(err))
	}
	switch {
	case p.Kind == policy.RowAccess && typ != "BOOLEAN":
		return invalid("its expression is %s, not BOOLEAN", typ)
	case p.Kind == policy.Masking && typ != p.Returns && typ != `"NULL"` && typ != "NULL":
		return invalid("its expression is %s, not %s; cast it", typ, p.Returns)
	}
	return p, nil
}

// normalizeType returns a type as DuckDB names it, such as VARCHAR for
// TEXT.
func normalizeType(ctx context.Context, sess *session, typ string) (string, error) {
	if strings.ContainsAny(sqllex.CodeOnly(typ), ";") || strings.Contains(typ, "--") || strings.Contains(typ, "/*") {
		return "", errors.New("not a type")
	}
	rows, err := sess.conn.QueryContext(ctx, "SELECT system.main.typeof(CAST(NULL AS "+typ+"))", nil)
	if err != nil {
		return "", err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		return "", err
	}
	t, _ := dest[0].(string)
	if t == "" {
		return "", errors.New("not a type")
	}
	return t, nil
}

func (s *Server) showPolicies(ctx context.Context, user auth.User, c *policyCmd, w wire.DataWriter) error {
	snap := s.gov.snapshot(ctx)
	policies := slices.Clone(snap.Catalog.Policies)
	slices.SortFunc(policies, func(a, b policy.Policy) int { return strings.Compare(a.Name, b.Name) })
	for _, p := range policies {
		if c.policyKind != "" && p.Kind != c.policyKind {
			continue
		}
		var body any
		if user.Role == auth.RoleAdmin {
			body = p.Body
		}
		row := []any{p.Name, string(p.Kind), p.Signature(), body, p.Comment, p.CreatedBy, p.CreatedAt, p.UpdatedAt}
		if err := w.Row(row); err != nil {
			return err
		}
	}
	return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
}

func (s *Server) showTags(ctx context.Context, w wire.DataWriter) error {
	snap := s.gov.snapshot(ctx)
	tags := slices.Clone(snap.Catalog.Tags)
	slices.SortFunc(tags, func(a, b policy.Tag) int { return strings.Compare(a.Name, b.Name) })
	for _, t := range tags {
		row := []any{t.Name, strings.Join(t.AllowedValues, ", "), strings.Join(t.Policies, ", "), t.Comment, t.CreatedBy, t.CreatedAt}
		if err := w.Row(row); err != nil {
			return err
		}
	}
	return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
}

// policyStoreError gives policy errors PostgreSQL codes.
func policyStoreError(err error) error {
	switch {
	case pgerror.GetCode(err) != codes.Uncategorized:
		return err
	case errors.Is(err, policy.ErrNotFound):
		return pgerror.WithCode(err, codes.UndefinedObject)
	case errors.Is(err, policy.ErrExists):
		return pgerror.WithCode(err, codes.DuplicateObject)
	case errors.Is(err, policy.ErrInUse):
		return pgerror.WithCode(err, codes.DependentObjectsStillExist)
	case errors.Is(err, policy.ErrInvalid):
		return pgerror.WithCode(err, codes.InvalidParameterValue)
	}
	return err
}
