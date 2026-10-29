package pgwire

import (
	"context"
	"errors"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
)

// The resets PostgreSQL clients and connection poolers send to clean a
// session for its next use, in either dialect:
//
//	RESET ALL           -- every setting back to what the session started with
//	DISCARD TEMP        -- drop the session's temporary objects (also TEMPORARY)
//	DISCARD PLANS       -- nothing to do: DuckDB keeps no plans but prepared
//	                    -- statements, which it binds again when what they
//	                    -- read changes
//	DISCARD SEQUENCES   -- nothing to do: DuckDB keeps no sequence values
//	                    -- per session; currval is the sequence's own
//	DISCARD ALL         -- all of these, and close cursors and drop prepared
//	                    -- statements; not in a transaction block
//
// DuckDB has neither statement: it refuses RESET ALL and does not parse
// DISCARD.
var (
	resetAll         = regexp.MustCompile(`(?is)^\s*RESET\s+ALL\s*;?\s*$`)
	discardStatement = regexp.MustCompile(`(?is)^\s*DISCARD\s+(ALL|PLANS|SEQUENCES|TEMP|TEMPORARY)\s*;?\s*$`)
)

// sessionResetCommand handles RESET ALL and DISCARD. They come before the
// Snowflake dialect's translation: poolers send them whatever the session
// speaks.
func (s *Server) sessionResetCommand(sess *session, text string) (wire.PreparedStatements, bool) {
	query := stripComments(text)
	var what string
	if resetAll.MatchString(query) {
		what = "RESET ALL"
	} else if m := discardStatement.FindStringSubmatch(query); m != nil {
		what = strings.ToUpper(m[1])
		if what == "TEMPORARY" {
			what = "TEMP"
		}
	} else {
		return nil, false
	}
	return wire.Prepared(wire.NewStatement(s.audited(sess, text, func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		var err error
		switch what {
		case "RESET ALL":
			err = s.resetSettings(ctx, sess)
		case "TEMP":
			err = s.discardTemp(ctx, sess)
		case "ALL":
			if sess.inTx {
				err = pgerror.WithCode(errors.New("DISCARD ALL cannot run inside a transaction block"), codes.ActiveSQLTransaction)
				break
			}
			sess.closeCursors(ctx, true)
			if err = sess.deallocateAll(ctx); err == nil {
				err = s.resetSettings(ctx, sess)
			}
			if err == nil {
				err = s.discardTemp(ctx, sess)
			}
		}
		switch {
		case err != nil && sess.inTx:
			sess.txFailed = true
		case sess.inTx && what != "PLANS" && what != "SEQUENCES":
			// Rolling back to a savepoint cannot undo it (see rollbackTo).
			sess.changes++
		}
		if err != nil {
			return err
		}
		if what == "RESET ALL" {
			return w.Complete("RESET")
		}
		return w.Complete("DISCARD " + what)
	}))), true
}

// resetSettings puts every setting a session may change back to what it
// started with: DuckDB's session settings (sessionSettings), with the
// search path the engine starts connections on, Igloo's own settings,
// application_name as the connection gave it, and SQL variables. The
// settings of the user and their roles (ALTER ROLE ... SET) apply again,
// read afresh as at login. What an admin set for the whole server (SET
// GLOBAL, or an unscoped SET that DuckDB applies globally) stays.
func (s *Server) resetSettings(ctx context.Context, sess *session) error {
	for _, name := range slices.Sorted(maps.Keys(sessionSettings)) {
		if err := sess.exec(ctx, "RESET SESSION "+name); err != nil {
			return pgError(err)
		}
	}
	if err := s.engine.UseDefaultSchema(ctx, sess.conn); err != nil {
		return pgError(err)
	}
	vars, err := queryStrings(ctx, sess, "SELECT name FROM system.main.duckdb_variables()")
	if err != nil {
		return pgError(err)
	}
	for _, name := range vars {
		if err := sess.exec(ctx, "RESET VARIABLE "+quoteIdent(name)); err != nil {
			return pgError(err)
		}
	}
	for _, v := range sess.bitmapVars {
		v.set = false
	}

	sess.timeout, sess.queueTimeout = s.timeout, 0
	sess.wl.tag, sess.wl.tagSet = "", false
	sess.dialect, sess.distMode = "", ""
	sess.prefetchOff = false
	for _, sw := range sessionSwitches {
		*sw.off(sess) = false
	}
	for _, name := range []string{"ai_integration", "ai_max_calls", "ai_max_cost_usd", "ai_decision_threshold"} {
		apply, _ := parseAISetting(sess, name, "default")
		apply()
	}
	sess.setApplicationName(wire.ClientParameters(ctx)[wire.ParamApplicationName])
	sess.settingGen++
	sess.mu.Lock()
	sess.wl.loaded = time.Time{}
	sess.mu.Unlock()
	return nil
}

// tempObjects lists the session's temporary objects, in the order they
// can be dropped: views, then tables, newest first, ahead of any they
// reference, then macros and sequences.
const tempObjects = `SELECT kind, name FROM (
	SELECT 1 AS k, 'VIEW' AS kind, view_name AS name, view_oid AS oid
		FROM system.main.duckdb_views() WHERE database_name = 'temp'
	UNION ALL SELECT 2, 'TABLE', table_name, table_oid
		FROM system.main.duckdb_tables() WHERE database_name = 'temp'
	UNION ALL SELECT DISTINCT 3, CASE function_type WHEN 'table_macro' THEN 'MACRO TABLE' ELSE 'MACRO' END, function_name, 0
		FROM system.main.duckdb_functions() WHERE database_name = 'temp'
	UNION ALL SELECT 4, 'SEQUENCE', sequence_name, sequence_oid
		FROM system.main.duckdb_sequences() WHERE database_name = 'temp'
) ORDER BY k, oid DESC, name`

// discardTemp drops the session's temporary objects, and makes again the
// ones every session starts with (see sessionCatalogSetup), as a new
// session has them.
func (s *Server) discardTemp(ctx context.Context, sess *session) error {
	// These walk every attached database (see names).
	release := s.holdCatalog(sess)
	objects, err := queryRows(ctx, sess.conn, tempObjects)
	release()
	if err != nil {
		return pgError(err)
	}
	for _, o := range objects {
		kind, _ := o[0].(string)
		name, _ := o[1].(string)
		if err := sess.exec(ctx, "DROP "+kind+" IF EXISTS temp.main."+quoteIdent(name)); err != nil {
			return pgError(err)
		}
	}
	user := sess.currentUser()
	for _, stmt := range sessionCatalogSetup(user.Name, user.Role == auth.RoleAdmin, s.version, s.engine.BitmapsLoaded()) {
		if err := sess.exec(ctx, stmt); err != nil {
			return pgError(err)
		}
	}
	sess.sfTemps = nil
	sess.ddlCount++
	return nil
}

// deallocateAll drops the client's named prepared statements.
func (sess *session) deallocateAll(ctx context.Context) error {
	if c := sess.statements; c != nil {
		for name := range c.names {
			if err := c.Delete(ctx, name); err != nil {
				return err
			}
		}
	}
	return nil
}
