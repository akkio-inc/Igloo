package pgwire

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/ddl"
)

// Sort orders of the catalog's tables, as DuckDB writes them:
//
//	ALTER TABLE [IF EXISTS] t SET SORTED BY (a [ASC | DESC] [NULLS FIRST | LAST], ...)
//	ALTER TABLE [IF EXISTS] t RESET SORTED BY
//
// The order is the table's default Iceberg sort order, which maintenance
// clusters its files by (see package maintenance) and which other engines
// read; Snowflake's CLUSTER BY and DROP CLUSTERING KEY translate to these.
// DuckDB has the syntax but cannot carry it out for Iceberg tables. Changing
// a table's order needs what ALTER TABLE needs.

// sortCommand is ALTER TABLE ... SET SORTED BY or RESET SORTED BY.
type sortCommand struct {
	table    []string
	ifExists bool
	// columns is empty for RESET SORTED BY.
	columns []catalog.SortColumn
}

// parseSortCommand recognizes the statements above. ok is false for any
// other statement; err is set for one of them written wrong.
func parseSortCommand(query string) (c sortCommand, ok bool, err error) {
	sc := &scanner{s: query}
	if !sc.keywords("ALTER", "TABLE") {
		return c, false, nil
	}
	c.ifExists = sc.keywords("IF", "EXISTS")
	if c.table, ok = sc.name(); !ok {
		return c, false, nil
	}
	switch {
	case sc.keywords("RESET", "SORTED", "BY"):
		if !sc.done() {
			return c, true, sortSyntax("unexpected %q after RESET SORTED BY", strings.TrimSpace(sc.rest()))
		}
		return c, true, nil
	case sc.keywords("SET", "SORTED", "BY"):
	default:
		return c, false, nil
	}
	if !sc.punct('(') {
		return c, true, sortSyntax("expected SET SORTED BY (column, ...)")
	}
	for {
		name, ok := sc.name()
		if !ok || len(name) != 1 {
			return c, true, sortSyntax("expected a column name in SET SORTED BY")
		}
		col := catalog.SortColumn{Name: name[0]}
		switch {
		case sc.keywords("DESC"):
			col.Descending = true
		case sc.keywords("ASC"):
		}
		switch {
		case sc.keywords("NULLS", "FIRST"):
			first := true
			col.NullsFirst = &first
		case sc.keywords("NULLS", "LAST"):
			first := false
			col.NullsFirst = &first
		}
		c.columns = append(c.columns, col)
		if sc.punct(',') {
			continue
		}
		if sc.punct(')') {
			break
		}
		return c, true, sortSyntax("expected , or ) in SET SORTED BY")
	}
	if !sc.done() {
		return c, true, sortSyntax("unexpected %q after SET SORTED BY (...)", strings.TrimSpace(sc.rest()))
	}
	return c, true, nil
}

func sortSyntax(format string, args ...any) error {
	return pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
}

// sortStatement carries out a sort order statement. ok is false for any
// other statement.
func (s *Server) sortStatement(sess *session, q wire.Query) (wire.PreparedStatements, bool, error) {
	c, ok, err := parseSortCommand(q.Query)
	if !ok {
		return nil, false, nil
	}
	if err == nil && s.ddl == nil {
		err = pgerror.WithCode(errors.New("sort orders are kept in the Iceberg catalog (IGLOO_CATALOG_URI)"), codes.FeatureNotSupported)
	}
	if err != nil {
		s.recordQuery(sess, queryRecord{text: q.Query, start: time.Now(), err: err, rows: -1})
		return nil, true, err
	}
	return wire.Prepared(wire.NewStatement(func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		start := time.Now()
		var acc *access
		err := sess.runCancelable(ctx, q.Query, func(ctx context.Context) error {
			var err error
			acc, err = s.runSortCommand(ctx, sess, c)
			return err
		})
		s.metrics.query(start, err)
		s.recordQuery(sess, queryRecord{text: q.Query, start: start, err: err, rows: -1, acc: acc})
		if err != nil {
			return pgError(err)
		}
		return w.Complete("ALTER TABLE")
	})), true, nil
}

// runSortCommand checks and carries out c.
func (s *Server) runSortCommand(ctx context.Context, sess *session, c sortCommand) (*access, error) {
	if s.ddl == nil {
		return nil, pgerror.WithCode(errors.New("sort orders are kept in the Iceberg catalog (IGLOO_CATALOG_URI)"), codes.FeatureNotSupported)
	}
	user, err := s.refreshUser(ctx, sess)
	if err != nil {
		return nil, err
	}
	if schema, name, ok, err := s.resolveCatalogName(ctx, sess, c.table); err == nil && ok && s.ddl.IsView(schema, name) {
		return nil, pgerror.WithCode(fmt.Errorf("%s is a view; only tables have a sort order", strings.Join(c.table, ".")), codes.WrongObjectType)
	}
	o, err := s.cloneSource(ctx, sess, c.table)
	switch code := pgerror.GetCode(err); {
	case err == nil:
	case c.ifExists && code == codes.UndefinedTable:
		return nil, nil
	case code == codes.WrongObjectType:
		return nil, pgerror.WithCode(fmt.Errorf("%s is a view; only tables have a sort order", strings.Join(c.table, ".")), codes.WrongObjectType)
	default:
		return nil, err
	}
	acc := &access{written: []auth.Object{o}}
	if err := authorize(user.Role, duckdb.STATEMENT_TYPE_ALTER, "ALTER TABLE"); err != nil {
		if err = s.authorizeSchemaCreate(ctx, sess, user, o.Schema, err); err != nil {
			s.log.Info("statement denied", "user", user.Name, "role", user.Role, "err", err)
			return acc, err
		}
	}
	if sess.inTx {
		return acc, pgerror.WithCode(errors.New("ALTER TABLE ... SORTED BY cannot run inside a transaction block"), codes.ActiveSQLTransaction)
	}
	err = s.ddl.SetSortOrder(ctx, o.Schema, o.Table, c.columns)
	switch {
	case errors.Is(err, ddl.ErrNotFound):
		if c.ifExists {
			return acc, nil
		}
		return acc, pgerror.WithCode(err, codes.UndefinedTable)
	case errors.Is(err, catalog.ErrNotSortable):
		return acc, pgerror.WithCode(err, codes.DatatypeMismatch)
	case errors.Is(err, catalog.ErrNoSuchColumn):
		return acc, pgerror.WithCode(err, codes.UndefinedColumn)
	case err != nil:
		return acc, err
	}
	statement, order := "SET SORTED BY", make([]string, len(c.columns))
	for i, col := range c.columns {
		order[i] = col.Name
		if col.Descending {
			order[i] += " DESC"
		}
	}
	if len(c.columns) == 0 {
		statement = "RESET SORTED BY"
	}
	s.log.Info("table changed", "statement", "ALTER TABLE ... "+statement, "table", o.String(), "sorted_by", strings.Join(order, ", "), "by", user.Name)
	return acc, nil
}

// runAfter carries out the statements a translation from Snowflake's dialect
// left for after its statement (see snowflake.Result.After).
func (s *Server) runAfter(ctx context.Context, sess *session, after []string) error {
	for _, stmt := range after {
		c, ok, err := parseSortCommand(stmt)
		if !ok && err == nil {
			err = fmt.Errorf("cannot run %q after the statement", stmt)
		}
		if err != nil {
			return err
		}
		if _, err := s.runSortCommand(ctx, sess, c); err != nil {
			return fmt.Errorf("the table was created, but its clustering key was not set: %w", err)
		}
	}
	return nil
}
