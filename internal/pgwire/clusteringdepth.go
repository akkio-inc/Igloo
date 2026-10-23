package pgwire

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/ddl"
	"github.com/akkio-inc/igloo/internal/maintenance"
)

// Clustering depth. Snowflake's
//
//	SELECT SYSTEM$CLUSTERING_DEPTH('table' [, '(column, ...)']) [AS name]
//
// returns how deep a table's files stack by its clustering key, or by the
// first of the columns given: 1 when no two files' ranges overlap (see
// maintenance.ClusteringDepth). Igloo works it out from the table's
// manifests, in either dialect, for a user who may read the table.

// depthCall is a call of SYSTEM$CLUSTERING_DEPTH.
type depthCall struct {
	table  []string
	column string
	alias  string
}

// depthName is SYSTEM$CLUSTERING_DEPTH's name, and its column's when the
// call has no alias.
const depthName = "SYSTEM$CLUSTERING_DEPTH"

// parseDepthCall recognizes a statement that is a call of
// SYSTEM$CLUSTERING_DEPTH. ok is false for any other statement.
func parseDepthCall(query string) (c depthCall, ok bool, err error) {
	sc := &scanner{s: query}
	if !sc.keywords("SELECT", depthName) {
		return c, false, nil
	}
	usage := pgerror.WithCode(errors.New("expected SELECT SYSTEM$CLUSTERING_DEPTH('table' [, '(column, ...)'])"), codes.Syntax)
	if !sc.punct('(') {
		return c, true, usage
	}
	name, ok := sc.literal()
	if !ok || name == nil {
		return c, true, usage
	}
	nsc := &scanner{s: *name}
	if c.table, ok = nsc.name(); !ok || !nsc.done() || len(c.table) > 3 {
		return c, true, pgerror.WithCode(fmt.Errorf("invalid table name %q", *name), codes.InvalidName)
	}
	if sc.punct(',') {
		cols, ok := sc.literal()
		if !ok || cols == nil {
			return c, true, usage
		}
		csc := &scanner{s: *cols}
		paren := csc.punct('(')
		col, ok := csc.name()
		if !ok || len(col) != 1 {
			return c, true, pgerror.WithCode(fmt.Errorf("expected columns as '(column, ...)', not %q", *cols), codes.Syntax)
		}
		c.column = col[0]
		// Only the first column counts: Igloo clusters by a sort order's
		// first column.
		for csc.punct(',') {
			if _, ok := csc.name(); !ok {
				return c, true, pgerror.WithCode(fmt.Errorf("expected columns as '(column, ...)', not %q", *cols), codes.Syntax)
			}
		}
		if paren && !csc.punct(')') || !csc.done() {
			return c, true, pgerror.WithCode(fmt.Errorf("expected columns as '(column, ...)', not %q", *cols), codes.Syntax)
		}
		if sc.punct(',') {
			return c, true, pgerror.WithCode(errors.New("SYSTEM$CLUSTERING_DEPTH's predicate is not supported"), codes.FeatureNotSupported)
		}
	}
	if !sc.punct(')') {
		return c, true, usage
	}
	c.alias = depthName
	if sc.keywords("AS") {
		alias, ok := sc.name()
		if !ok || len(alias) != 1 {
			return c, true, usage
		}
		c.alias = alias[0]
	}
	if !sc.done() {
		return c, true, usage
	}
	return c, true, nil
}

// depthStatement carries out a call of SYSTEM$CLUSTERING_DEPTH. ok is false
// for any other statement.
func (s *Server) depthStatement(sess *session, q wire.Query) (wire.PreparedStatements, bool, error) {
	c, ok, err := parseDepthCall(q.Query)
	if !ok {
		return nil, false, nil
	}
	if err == nil && s.ddl == nil {
		err = pgerror.WithCode(errors.New("clustering depth needs the Iceberg catalog (IGLOO_CATALOG_URI)"), codes.FeatureNotSupported)
	}
	if err != nil {
		s.recordQuery(sess, queryRecord{text: q.Query, start: time.Now(), err: err, rows: -1})
		return nil, true, err
	}
	columns := wire.Columns{{Name: c.alias, Oid: pgtype.Float8OID, Width: 8}}
	return wire.Prepared(wire.NewStatement(func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		start := time.Now()
		var acc *access
		var depth maintenance.Depth
		err := sess.runCancelable(ctx, q.Query, func(ctx context.Context) error {
			var err error
			acc, depth, err = s.clusteringDepth(ctx, sess, c)
			return err
		})
		s.metrics.query(start, err)
		rows := int64(-1)
		if err == nil {
			rows = 1
		}
		s.recordQuery(sess, queryRecord{text: q.Query, start: start, err: err, rows: rows, acc: acc})
		if err != nil {
			return pgError(err)
		}
		if err := w.Row([]any{depth.Average}); err != nil {
			return err
		}
		return w.Complete("SELECT 1")
	}, wire.WithColumns(columns))), true, nil
}

// clusteringDepth checks and works out c.
func (s *Server) clusteringDepth(ctx context.Context, sess *session, c depthCall) (*access, maintenance.Depth, error) {
	user, err := s.refreshUser(ctx, sess)
	if err != nil {
		return nil, maintenance.Depth{}, err
	}
	if schema, name, ok, err := s.resolveCatalogName(ctx, sess, c.table); err == nil && ok && s.ddl.IsView(schema, name) {
		return nil, maintenance.Depth{}, pgerror.WithCode(fmt.Errorf("%s is a view; only tables have a clustering depth", strings.Join(c.table, ".")), codes.WrongObjectType)
	}
	o, err := s.cloneSource(ctx, sess, c.table)
	if err != nil {
		if pgerror.GetCode(err) == codes.WrongObjectType {
			err = pgerror.WithCode(fmt.Errorf("%s is a view; only tables have a clustering depth", strings.Join(c.table, ".")), codes.WrongObjectType)
		}
		return nil, maintenance.Depth{}, err
	}
	acc := &access{read: []auth.Object{o}}
	if err := s.authorizeReads(ctx, sess, user, []auth.Object{o}); err != nil {
		s.log.Info("statement denied", "user", user.Name, "role", user.Role, "err", err)
		return acc, maintenance.Depth{}, err
	}
	tbl, err := s.ddl.LoadTable(ctx, o.Schema, o.Table)
	if errors.Is(err, ddl.ErrNotFound) {
		return acc, maintenance.Depth{}, pgerror.WithCode(err, codes.UndefinedTable)
	}
	if err != nil {
		return acc, maintenance.Depth{}, err
	}
	depth, err := maintenance.ClusteringDepth(ctx, tbl, c.column)
	switch {
	case errors.Is(err, maintenance.ErrNotClustered):
		return acc, depth, pgerror.WithCode(fmt.Errorf("%s has no clustering key: give the column, as SYSTEM$CLUSTERING_DEPTH('%s', '(column)')",
			o, strings.Join(c.table, ".")), codes.InvalidParameterValue)
	case errors.Is(err, maintenance.ErrNoSuchColumn):
		return acc, depth, pgerror.WithCode(err, codes.UndefinedColumn)
	}
	return acc, depth, err
}
