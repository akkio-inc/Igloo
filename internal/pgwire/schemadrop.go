package pgwire

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// DROP SCHEMA ... CASCADE of a schema of the Iceberg catalog, which the
// catalog cannot do, drops what is in it first, one statement at a time, each
// carried out as the client's own: its materialized views and dynamic tables,
// with their tables and state, then its views, then its tables, whose files go
// as those of DROP TABLE do; then the schema. Before it drops anything it
// checks that the user may drop each of them, so that a refusal leaves the
// schema as it was; one that comes later, as the catalog refuses to drop a
// table an erasure request is erasing from, stops it there, and names what
// stopped it.

// schemaCascade is DROP SCHEMA [IF EXISTS] name CASCADE.
type schemaCascade struct {
	schema, plain string
	ifExists      bool
}

// parseSchemaCascade recognizes DROP SCHEMA ... CASCADE of a schema of the
// Iceberg catalog.
func (s *Server) parseSchemaCascade(ctx context.Context, sess *session, query string) (schemaCascade, bool) {
	if s.ddl == nil || s.tasks == nil {
		return schemaCascade{}, false
	}
	sc := &scanner{s: query}
	if !sc.keywords("DROP", "SCHEMA") {
		return schemaCascade{}, false
	}
	c := schemaCascade{ifExists: sc.keywords("IF", "EXISTS")}
	at := sc.i
	name, ok := sc.name()
	if !ok {
		return c, false
	}
	end := sc.i
	if !sc.keywords("CASCADE") || !sc.done() || !s.namesCatalogSchema(ctx, sess, &scanner{s: query, i: at}) {
		return c, false
	}
	c.schema, c.plain = name[len(name)-1], strings.TrimSpace(query[:end])
	return c, true
}

// schemaCascadeStatement carries out c.
func (s *Server) schemaCascadeStatement(sess *session, query string, c schemaCascade) wire.PreparedStatements {
	return wire.Prepared(wire.NewStatement(func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		if err := s.dropSchemaCascade(ctx, sess, c, w); err != nil {
			return err
		}
		return w.Complete("DROP SCHEMA")
	}))
}

// dropSchemaCascade drops what is in c's schema, then the schema.
func (s *Server) dropSchemaCascade(ctx context.Context, sess *session, c schemaCascade, w wire.DataWriter) error {
	start := time.Now()
	fail := func(err error) error {
		s.metrics.query(start, err)
		s.recordQuery(sess, queryRecord{text: c.plain + " CASCADE", start: start, err: err, rows: -1})
		return err
	}
	if err := sess.checkAborted(c.plain); err != nil {
		return fail(err)
	}
	user := sess.currentUser()
	// The user may drop the schema: as DROP SCHEMA itself is checked.
	st, err := s.prepareChecked(ctx, sess, user, c.plain, nil)
	if err != nil {
		return fail(err)
	}
	_ = st.close()

	tables, err := s.ddl.TableNames(ctx, c.schema)
	if err != nil {
		// No such schema, say: DROP SCHEMA says so, or with IF EXISTS
		// that there is nothing to drop.
		return s.runScriptStatement(ctx, c.plain, &innerWriter{DataWriter: w})
	}
	var jobs []tasks.Job
	for _, kind := range []string{tasks.KindMaterializedView, tasks.KindDynamicTable} {
		js, err := s.tasks.Store().Jobs(ctx, kind)
		if err != nil {
			return fail(err)
		}
		for _, j := range js {
			if schema, _, _ := strings.Cut(j.Name, "."); strings.EqualFold(schema, c.schema) {
				jobs = append(jobs, j)
			}
		}
	}
	var views []string
	for _, v := range s.ddl.Views(c.schema) {
		views = append(views, v.Name)
	}
	// A materialized view's table goes with it.
	tables = slices.DeleteFunc(tables, func(t string) bool {
		return slices.ContainsFunc(jobs, func(j tasks.Job) bool {
			_, name, _ := strings.Cut(j.Name, ".")
			return strings.EqualFold(name, t)
		})
	})
	qualified := func(name string) string { return quoteName([]string{s.catalogName(), c.schema, name}) }

	// Before anything is dropped, the user may drop all of it.
	for _, j := range jobs {
		if j.Owner != user.Name && user.Role != auth.RoleAdmin {
			return fail(pgerror.WithCode(fmt.Errorf("permission denied: cannot drop schema %s because %s %s belongs to %s; only its owner or an admin may drop it",
				c.schema, j.Kind, j.Name, j.Owner), codes.InsufficientPrivilege))
		}
	}
	for _, t := range tables {
		st, err := s.prepareChecked(ctx, sess, user, "DROP TABLE "+qualified(t), nil)
		if err != nil {
			return fail(fmt.Errorf("cannot drop schema %s: %w", c.schema, err))
		}
		_ = st.close()
	}

	stopped := func(what string, err error) error {
		return pgerror.WithCode(fmt.Errorf("DROP SCHEMA %s CASCADE stopped at %s, having dropped what came before it: %w", c.schema, what,
			pgError(err)), pgerror.GetCode(pgError(err)))
	}
	for _, j := range jobs {
		start := time.Now()
		text := "DROP " + strings.ToUpper(j.Kind) + " " + qualified(strings.SplitN(j.Name, ".", 2)[1])
		err := sess.outsideTransaction(ctx, func() error { return s.dropJob(ctx, sess, user, j) })
		_, name, _ := strings.Cut(j.Name, ".")
		s.recordQuery(sess, queryRecord{text: text, start: start, err: err, rows: -1, written: []auth.Object{{Schema: c.schema, Table: name}}})
		if err != nil {
			return stopped(j.Kind+" "+j.Name, err)
		}
	}
	for _, v := range views {
		if err := s.runScriptStatement(ctx, "DROP VIEW "+qualified(v), &innerWriter{DataWriter: w}); err != nil {
			return stopped("view "+c.schema+"."+v, err)
		}
	}
	for _, t := range tables {
		if err := s.runScriptStatement(ctx, "DROP TABLE "+qualified(t), &innerWriter{DataWriter: w}); err != nil {
			return stopped("table "+c.schema+"."+t, err)
		}
	}
	err = s.runScriptStatement(ctx, c.plain, &innerWriter{DataWriter: w})
	if err != nil && !errors.Is(err, context.Canceled) {
		// Something made in it meanwhile, say.
		return stopped("the schema itself", err)
	}
	return err
}
