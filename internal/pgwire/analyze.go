package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/ddl"
	"github.com/akkio-inc/igloo/internal/iddomain"
)

// Distinct counts for DuckDB's planner. DuckDB estimates the rows a filter
// such as audience_id = 'x' leaves of a table, and so how it joins them, by
// the column's distinct count, which it has for its own tables and not for
// Iceberg tables: there it takes a fifth of the table for any such filter,
// one audience of a few thousand rows in a lookup table of billions
// for hundreds of millions, and built its join's hash table of the facts rather than
// of the audience. ANALYZE counts the distinct values of a table's
// columns and keeps the counts in the table (catalog.DistinctCountsProperty);
// the catalog proxy takes them from the loads of the tables DuckDB binds,
// and they are handed to DuckDB's planner before each statement is prepared
// and run (registerDistinctCounts; the rb extension's rb_stats.cpp).

// analyzeCommand is ANALYZE table [(column, ...)].
type analyzeCommand struct {
	table   []string
	columns []string
}

// parseAnalyzeCommand recognizes ANALYZE of a named table; bare ANALYZE,
// DuckDB's, is not one.
func parseAnalyzeCommand(query string) (c analyzeCommand, ok bool, err error) {
	sc := &scanner{s: query}
	if !sc.keywords("ANALYZE") && !sc.keywords("ANALYSE") {
		return c, false, nil
	}
	if c.table, ok = sc.name(); !ok {
		return c, false, nil
	}
	if sc.punct('(') {
		for {
			name, ok := sc.name()
			if !ok || len(name) != 1 {
				return c, true, pgerror.WithCode(errors.New("expected a column name in ANALYZE table (column, ...)"), codes.Syntax)
			}
			c.columns = append(c.columns, name[0])
			if sc.punct(')') {
				break
			}
			if !sc.punct(',') {
				return c, true, pgerror.WithCode(errors.New("expected , or ) in ANALYZE table (column, ...)"), codes.Syntax)
			}
		}
	}
	if !sc.done() {
		return c, true, pgerror.WithCode(fmt.Errorf("unexpected %q after ANALYZE %s", strings.TrimSpace(sc.rest()), strings.Join(c.table, ".")),
			codes.Syntax)
	}
	return c, true, nil
}

// analyzeStatement carries out ANALYZE of a catalog table.
func (s *Server) analyzeStatement(sess *session, q wire.Query) (wire.PreparedStatements, bool, error) {
	c, ok, err := parseAnalyzeCommand(q.Query)
	if !ok {
		return nil, false, nil
	}
	if err == nil && s.ddl == nil {
		err = pgerror.WithCode(errors.New("distinct counts are kept in the Iceberg catalog (IGLOO_CATALOG_URI)"), codes.FeatureNotSupported)
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
			acc, err = s.runAnalyze(ctx, sess, c)
			return err
		})
		s.metrics.query(start, err)
		s.recordQuery(sess, queryRecord{text: q.Query, start: start, err: err, rows: -1, acc: acc})
		if err != nil {
			return pgError(err)
		}
		return w.Complete("ANALYZE")
	})), true, nil
}

// runAnalyze counts the distinct values of c's columns, every column of a
// type DuckDB compares by value when it names none, and keeps the counts in
// the table, beside the counts of its other columns.
func (s *Server) runAnalyze(ctx context.Context, sess *session, c analyzeCommand) (*access, error) {
	user, err := s.refreshUser(ctx, sess)
	if err != nil {
		return nil, err
	}
	if user.Role != auth.RoleAdmin {
		return nil, pgerror.WithCode(errors.New("permission denied: only admins may ANALYZE a table"), codes.InsufficientPrivilege)
	}
	if sess.inTx {
		return nil, pgerror.WithCode(errors.New("ANALYZE cannot run inside a transaction block"), codes.ActiveSQLTransaction)
	}
	o, err := s.cloneSource(ctx, sess, c.table)
	if err != nil {
		return nil, err
	}
	acc := &access{read: []auth.Object{o}, written: []auth.Object{o}}
	name := quoteName([]string{s.catalogName(), o.Schema, o.Table})
	release := s.shareCatalog(sess)
	cols, err := describeTable(ctx, sess, name)
	release()
	if err != nil {
		return acc, err
	}
	var counted []string
	for _, col := range cols {
		if iddomain.IsHidden(col.name) || !distinctCountable(col.typ) {
			continue
		}
		if len(c.columns) == 0 || slices.ContainsFunc(c.columns, func(n string) bool { return strings.EqualFold(n, col.name) }) {
			counted = append(counted, col.name)
		}
	}
	for _, n := range c.columns {
		if !slices.ContainsFunc(counted, func(m string) bool { return strings.EqualFold(n, m) }) {
			return acc, pgerror.WithCode(fmt.Errorf("column %q of %s does not exist or has no distinct count", n, o), codes.UndefinedColumn)
		}
	}
	if len(counted) == 0 {
		return acc, nil
	}
	exprs := make([]string, len(counted))
	for i, col := range counted {
		exprs[i] = "approx_count_distinct(" + quoteIdent(col) + ")::BIGINT"
	}
	rows, err := sess.conn.QueryContext(ctx, "SELECT "+strings.Join(exprs, ", ")+" FROM "+name, nil)
	if err != nil {
		return acc, err
	}
	values := make([]driver.Value, len(counted))
	err = rows.Next(values)
	_ = rows.Close()
	if err != nil {
		return acc, err
	}
	// The scan loaded the table, so the proxy has the counts it keeps now.
	_, tables := s.engine.DistinctCounts()
	counts := maps.Clone(tables[strings.ToLower(s.catalogName()+"."+o.Schema+"."+o.Table)])
	if counts == nil || len(c.columns) == 0 {
		counts = map[string]int64{}
	}
	for i, col := range counted {
		n, _ := values[i].(int64)
		counts[col] = max(n, 1)
	}
	err = s.ddl.SetDistinctCounts(ctx, o.Schema, o.Table, counts)
	if errors.Is(err, ddl.ErrNotFound) {
		return acc, pgerror.WithCode(err, codes.UndefinedTable)
	}
	if err == nil {
		s.log.Info("table analyzed", "table", o.String(), "columns", len(counted), "by", user.Name)
	}
	return acc, err
}

// distinctCountable reports whether DuckDB compares values of type typ by
// value, as filters and joins do: not lists, structs, maps or unions.
func distinctCountable(typ string) bool {
	return !strings.Contains(typ, "[") && !strings.HasPrefix(typ, "STRUCT") && !strings.HasPrefix(typ, "MAP") &&
		!strings.HasPrefix(typ, "UNION")
}

// registerDistinctCounts hands DuckDB's planner the distinct counts the
// catalog's tables keep, as their latest loads had them, when they changed
// since it last did (igloo_set_distinct_counts, which the rb extension
// defines); they are the database's, for every session. A session inside a
// transaction block leaves it to the next one outside.
//
// It takes the catalog lock before countsMu, as prepareChecked does, which
// holds the lock's exclusive side when it calls this. A session that took
// countsMu first, and then waited for the lock's shared side as DuckDB bound
// the call, deadlocked with one preparing: 64 sessions that sent their
// first statements together after a restart waited for good.
func (s *Server) registerDistinctCounts(ctx context.Context, sess *session) {
	gen, tables := s.engine.DistinctCounts()
	if sess.inTx || gen == s.countsGen.Load() {
		return
	}
	defer s.shareCatalog(sess)()
	s.countsMu.Lock()
	defer s.countsMu.Unlock()
	if gen == s.countsGen.Load() {
		return
	}
	var b strings.Builder
	for _, table := range slices.Sorted(maps.Keys(tables)) {
		for _, col := range slices.Sorted(maps.Keys(tables[table])) {
			fmt.Fprintf(&b, "%s\t%s\t%d\n", table, strings.ToLower(col), tables[table][col])
		}
	}
	_, err := sess.conn.ExecContext(ctx, "SELECT system.main.igloo_set_distinct_counts($1)",
		[]driver.NamedValue{{Ordinal: 1, Value: b.String()}})
	if err != nil {
		// Without the rb extension, DuckDB estimates as it always has.
		s.log.Debug("could not hand DuckDB the tables' distinct counts", "err", err)
	}
	s.countsGen.Store(gen)
}
