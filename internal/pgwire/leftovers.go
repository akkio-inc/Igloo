package pgwire

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/policy"
)

// Leftovers of stopped creates. CREATE TABLE ... AS in steps fills a table
// under a staging name and renames it into place (see stagedCreate). An
// Igloo stopped part way, or before that an older Igloo stopped in the
// middle of a CREATE TABLE ... AS that DuckDB ran as one statement, leaves
// behind a half-filled staging table, or a staged create Lakekeeper keeps
// under the name, unseen: DROP TABLE IF EXISTS does not see it, and the
// rename into place, after the whole fill, failed with 409 "Tabular with
// the same name already exists". Igloo notes in the namespace's properties
// each table it is making (see catalog.Pending): a note of a process that
// has stopped proves what is under its name is Igloo's own, and the next
// CREATE TABLE ... AS of the name, or DROP TABLE IF EXISTS of it, clears it.
// Before it fills anything, a create checks that nothing else holds the
// name unseen, and fails at once if something does; an admin's DROP TABLE
// of the name, without IF EXISTS, releases such a hold whoever left it.

// errHeldUnseen is the error for a name held by a create nobody committed.
func errHeldUnseen(schema, name string) error {
	return fmt.Errorf("%s.%s is held in the catalog by a table that was being created and never was, by another engine or by an "+
		"Igloo that was stopped before it noted its creates: no table of the name can be read, dropped or renamed onto it until "+
		"that create ends; if nothing is creating it, an admin's DROP TABLE %s releases the name", schema, name, quoteName([]string{schema, name}))
}

// clearLeftovers clears what stopped processes of Igloo left under name in
// schema, as their notes prove: half-filled staging tables to be renamed
// to it, and staged creates of it, or of those staging tables, that were
// never committed. check also makes sure, when no table of the name
// exists, that nothing else holds it unseen. The caller holds the catalog
// lock exclusively.
func (s *Server) clearLeftovers(ctx context.Context, sess *session, schema, name string, check bool) error {
	if s.ddl == nil {
		return nil
	}
	notes, err := s.ddl.PendingNotes(ctx, schema)
	if err != nil {
		s.log.Debug("could not read the notes of tables being made", "schema", schema, "err", err)
		notes = nil
	}
	now := time.Now()
	staging := strings.ToLower(name) + policy.StagingMark
	var done []string
	released := false
	for _, n := range notes {
		if !n.Dead(now) {
			continue
		}
		switch {
		case n.Kind == catalog.PendingFill && strings.EqualFold(n.Target, name) && strings.HasPrefix(strings.ToLower(n.Name), staging):
			if _, err := sess.conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteName([]string{s.catalogName(), schema, n.Name}), nil); err != nil {
				return fmt.Errorf("drop %s.%s, which a stopped CREATE TABLE ... AS left: %w", schema, n.Name, err)
			}
			s.log.Info("dropped a table a stopped CREATE TABLE ... AS left", "table", schema+"."+n.Name, "since", n.At)
		case n.Kind == catalog.PendingStaged && (strings.EqualFold(n.Name, name) || strings.HasPrefix(strings.ToLower(n.Name), staging)):
			there, err := s.ddl.TableExists(ctx, schema, n.Name)
			if err != nil {
				return err
			}
			if !there {
				if err := s.ddl.ReleaseHold(ctx, schema, n.Name); err != nil {
					return err
				}
				released = true
				s.log.Info("released a name a stopped create held", "table", schema+"."+n.Name, "since", n.At)
			}
		default:
			continue
		}
		done = append(done, n.Key)
	}
	if err := s.ddl.ForgetPending(ctx, schema, done...); err != nil {
		s.log.Debug("could not forget notes of tables being made", "schema", schema, "err", err)
	}
	if released {
		// Changed through the catalog, not DuckDB.
		s.catalogChanged()
	}
	if !check {
		return nil
	}
	held, err := s.ddl.HeldUnseen(ctx, schema, name)
	if err != nil {
		return err
	}
	if held {
		return errHeldUnseen(schema, name)
	}
	return nil
}

// notePending writes note in schema's properties, and returns the func
// that forgets it. Without the note a stop leaves what it would have
// proved for an admin to clear.
func (s *Server) notePending(ctx context.Context, schema string, note catalog.Pending) func() {
	if s.ddl == nil {
		return func() {}
	}
	if err := s.ddl.NotePending(ctx, schema, note); err != nil {
		s.log.Debug("could not note a table being made", "schema", schema, "table", note.Name, "err", err)
		return func() {}
	}
	return func() {
		if err := s.ddl.ForgetPending(context.WithoutCancel(ctx), schema, note.Key); err != nil {
			s.log.Debug("could not forget a note of a table being made", "schema", schema, "table", note.Name, "err", err)
		}
	}
}

// afterDropTable follows DROP TABLE [IF EXISTS] name, st, once it went
// through: what stopped creates of Igloo's left under the name goes too.
func (s *Server) afterDropTable(ctx context.Context, sess *session, st *statement) {
	sc := &scanner{s: st.query}
	if !sc.keywords("DROP", "TABLE") {
		return
	}
	sc.keywords("IF", "EXISTS")
	name, ok := sc.name()
	if !ok || !sc.done() {
		return
	}
	target, err := s.newTableTarget(ctx, sess, name)
	if err != nil {
		return
	}
	if err := s.clearLeftovers(ctx, sess, target.Schema, target.Table, false); err != nil {
		s.log.Warn("could not clear what a stopped create left", "table", target.String(), "err", err)
	}
}

// dropUnseen is, for an admin's DROP TABLE name of a name no table has,
// which preparing failed with err, the statement to prepare in its place
// once a create nobody committed that held the name was released: DROP
// TABLE IF EXISTS name, which succeeds. It is "" otherwise.
func (s *Server) dropUnseen(ctx context.Context, sess *session, user auth.User, text string, err error) string {
	sc := &scanner{s: text}
	if user.Role != auth.RoleAdmin || !strings.Contains(err.Error(), "does not exist") || !sc.keywords("DROP", "TABLE") ||
		sc.keywords("IF", "EXISTS") {
		return ""
	}
	at := sc.i
	name, ok := sc.name()
	if !ok || !sc.done() {
		return ""
	}
	target, terr := s.newTableTarget(ctx, sess, name)
	if terr != nil {
		return ""
	}
	if released, rerr := s.releaseUnseen(ctx, target.Schema, target.Table); rerr != nil || !released {
		return ""
	}
	return "DROP TABLE IF EXISTS " + strings.TrimSpace(text[at:])
}

// releaseUnseen releases, for an admin's DROP TABLE of schema.name that
// DuckDB found no table for, a hold of the name by a create nobody
// committed, and reports whether there was one.
func (s *Server) releaseUnseen(ctx context.Context, schema, name string) (bool, error) {
	if s.ddl == nil {
		return false, nil
	}
	held, err := s.ddl.HeldUnseen(ctx, schema, name)
	if err != nil || !held {
		return false, err
	}
	if err := s.ddl.ReleaseHold(ctx, schema, name); err != nil {
		return false, err
	}
	s.catalogChanged()
	s.log.Info("released a name a create that was never committed held", "table", schema+"."+name)
	return true, nil
}
