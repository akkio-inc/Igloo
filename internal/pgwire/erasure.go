package pgwire

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/erasure"
	"github.com/akkio-inc/igloo/internal/policy"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Erasure of people's data (package erasure), and data retention:
//
//	ERASE SUBJECT WHERE tag.value = 'identifier' [OR tag.value IN ('identifier', ...)] ...
//	SHOW ERASURE REQUESTS
//	ALTER TABLE [IF EXISTS] name SET (data_retention_days = n)
//	ALTER TABLE [IF EXISTS] name RESET (data_retention_days)
//
// Admins may run them, and users granted ERASE ON ACCOUNT, directly or
// through a role: privacy officers. The identifiers in ERASE SUBJECT never
// reach the history, the logs or the list of running statements.

// erasureCmd is a parsed erasure or retention statement.
type erasureCmd struct {
	tag    string
	wanted []erasure.Wanted
	// table, ifExists and days are ALTER TABLE's; days is nil for RESET.
	table    []string
	ifExists bool
	days     *int
}

const (
	tagErase     = "ERASE SUBJECT"
	tagShowErase = "SHOW ERASURE REQUESTS"
	tagRetention = "ALTER TABLE"
)

// maxRetentionDays bounds data_retention_days: a hundred years.
const maxRetentionDays = 36500

// parseErasureCommand recognizes the statements above. ok is false for
// any other; err reports a malformed one. Anything that starts with ERASE
// is taken, so a malformed request is refused here rather than sent on.
func parseErasureCommand(query string) (c erasureCmd, ok bool, err error) {
	sc := &scanner{s: query}
	syntax := func(format string, args ...any) (erasureCmd, bool, error) {
		return c, true, pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
	}
	switch {
	case sc.keywords("ERASE"):
		c.tag = tagErase
		if !sc.keywords("SUBJECT", "WHERE") {
			return syntax("expected ERASE SUBJECT WHERE tag.value = 'identifier'")
		}
		for {
			name, ok := sc.name()
			if !ok || len(name) != 2 {
				return syntax("expected a tag and one of its values, such as pii.email")
			}
			w := erasure.Wanted{Tag: name[0], Value: name[1]}
			switch {
			case sc.punct('='):
				lit, ok := sc.literal()
				if !ok || lit == nil {
					return syntax("expected a string after %s.%s =", w.Tag, w.Value)
				}
				w.Identifiers = []string{*lit}
			case sc.keywords("IN"):
				if !sc.punct('(') {
					return syntax("expected ( after %s.%s IN", w.Tag, w.Value)
				}
				for {
					lit, ok := sc.literal()
					if !ok || lit == nil {
						return syntax("expected strings in %s.%s IN (...)", w.Tag, w.Value)
					}
					w.Identifiers = append(w.Identifiers, *lit)
					if sc.punct(')') {
						break
					}
					if !sc.punct(',') {
						return syntax("expected , or ) in %s.%s IN (...)", w.Tag, w.Value)
					}
				}
			default:
				return syntax("expected = or IN after %s.%s", w.Tag, w.Value)
			}
			c.wanted = append(c.wanted, w)
			if sc.keywords("OR") {
				continue
			}
			if sc.done() {
				return c, true, nil
			}
			return syntax("expected OR or the end of ERASE SUBJECT")
		}
	case sc.keywords("SHOW", "ERASURE", "REQUESTS"):
		c.tag = tagShowErase
		if !sc.done() {
			return syntax("unexpected text after SHOW ERASURE REQUESTS")
		}
		return c, true, nil
	case sc.keywords("ALTER", "TABLE"):
		c.tag = tagRetention
		c.ifExists = sc.keywords("IF", "EXISTS")
		if c.table, ok = sc.name(); !ok {
			return c, false, nil
		}
		set := sc.keywords("SET")
		if !set && !sc.keywords("RESET") || !sc.punct('(') || !sc.keywords("DATA_RETENTION_DAYS") {
			return c, false, nil
		}
		if set {
			sc.punct('=')
			sc.skip()
			start := sc.i
			for sc.i < len(sc.s) && sc.s[sc.i] >= '0' && sc.s[sc.i] <= '9' {
				sc.i++
			}
			n, err := strconv.Atoi(sc.s[start:sc.i])
			if err != nil || n > maxRetentionDays {
				return syntax("data_retention_days takes a number of days from 0 to %d", maxRetentionDays)
			}
			c.days = &n
		}
		if !sc.punct(')') || !sc.done() {
			return syntax("expected ALTER TABLE name SET (data_retention_days = n) or RESET (data_retention_days)")
		}
		return c, true, nil
	}
	return c, false, nil
}

// erasureText is what the history, the logs and the list of running
// statements show of an erasure statement: the identifiers of ERASE
// SUBJECT become '***', and a malformed one is not shown at all, as it
// may hold them anywhere.
func erasureText(query string) string {
	c, ok, err := parseErasureCommand(query)
	switch {
	case !ok || c.tag != tagErase:
		return query
	case err != nil:
		return "ERASE ... (not shown: it may hold identifiers)"
	}
	var parts []string
	for _, w := range c.wanted {
		stars := strings.TrimSuffix(strings.Repeat("'***', ", len(w.Identifiers)), ", ")
		parts = append(parts, quoteIdent(w.Tag)+"."+quoteIdent(w.Value)+" IN ("+stars+")")
	}
	return "ERASE SUBJECT WHERE " + strings.Join(parts, " OR ")
}

// erasureColumns describe SHOW ERASURE REQUESTS, as igloo.erasure_requests.
func erasureColumns() wire.Columns {
	cols := make(wire.Columns, len(erasure.Columns))
	for i, name := range erasure.Columns {
		cols[i] = wire.Column{Name: name, Oid: pgtype.TextOID, Width: -1}
		switch {
		case strings.HasSuffix(name, "_at"):
			cols[i].Oid, cols[i].Width = pgtype.TimestamptzOID, 8
		case name == "history_redacted":
			cols[i].Oid, cols[i].Width = pgtype.Int8OID, 8
		}
	}
	return cols
}

// erasureCommand carries out the statements above. ok is false for every
// other statement.
func (s *Server) erasureCommand(sess *session, query string) (wire.PreparedStatements, bool, error) {
	c, ok, err := parseErasureCommand(query)
	if !ok {
		return nil, false, nil
	}
	text := erasureText(query)
	if err == nil && len(sqllex.Split(query)) != 1 {
		err = pgerror.WithCode(fmt.Errorf("send %s on its own", c.tag), codes.FeatureNotSupported)
	}
	if err != nil {
		s.metrics.query(time.Now(), err)
		s.recordQuery(sess, queryRecord{text: text, start: time.Now(), err: err, rows: -1})
		return nil, true, err
	}
	var opts []wire.PreparedOptionFn
	switch c.tag {
	case tagErase:
		opts = append(opts, wire.WithColumns(wire.Columns{
			{Name: "request_id", Oid: pgtype.TextOID, Width: -1},
			{Name: "status", Oid: pgtype.TextOID, Width: -1},
		}))
	case tagShowErase:
		opts = append(opts, wire.WithColumns(erasureColumns()))
	}
	return wire.Prepared(wire.NewStatement(s.audited(sess, text, func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		start := time.Now()
		user, err := s.refreshUser(ctx, sess)
		if err == nil {
			err = sess.runCancelable(ctx, text, func(ctx context.Context) error {
				return s.runErasureCommand(ctx, sess, user, c, w)
			})
		}
		s.metrics.query(start, err)
		if err != nil {
			s.log.Debug("statement failed", "user", user.Name, "err", err)
		}
		return pgError(err)
	}), opts...)), true, nil
}

func (s *Server) runErasureCommand(ctx context.Context, sess *session, user auth.User, c erasureCmd, w wire.DataWriter) error {
	if s.catalogName() == "" {
		return pgerror.WithCode(fmt.Errorf("%s needs an Iceberg catalog (IGLOO_CATALOG_URI)", c.tag), codes.FeatureNotSupported)
	}
	if !s.mayErase(ctx, sess, user) {
		return pgerror.WithCode(fmt.Errorf("permission denied: %s requires the admin role or ERASE ON ACCOUNT", c.tag),
			codes.InsufficientPrivilege)
	}
	switch c.tag {
	case tagShowErase:
		return s.showErasureRequests(ctx, w)
	case tagRetention:
		return s.setRetention(ctx, sess, user, c, w)
	}
	if s.erasure == nil {
		return pgerror.WithCode(errors.New("ERASE SUBJECT needs an Iceberg catalog (IGLOO_CATALOG_URI)"), codes.FeatureNotSupported)
	}
	if err := s.inNoTransaction(sess, tagErase); err != nil {
		return err
	}
	r, err := s.erasure.Submit(ctx, user.Name, c.wanted)
	if errors.Is(err, erasure.ErrInvalid) {
		return pgerror.WithCode(err, codes.InvalidParameterValue)
	}
	if err != nil {
		return err
	}
	if err := w.Row([]any{r.ID, r.Status}); err != nil {
		return err
	}
	return w.Complete(tagErase)
}

// mayErase reports whether user may erase people's data and see the
// requests to: admins, and holders of ERASE ON ACCOUNT.
func (s *Server) mayErase(ctx context.Context, sess *session, user auth.User) bool {
	if user.Role == auth.RoleAdmin {
		return true
	}
	privs, err := s.privileges(ctx, sess, user)
	if err != nil {
		s.log.Warn("could not load grants", "user", user.Name, "err", err)
		return false
	}
	return privs.AllowsAccount(auth.PrivilegeErase)
}

// ErasureViewer is the engine.ErasureViewer for Igloo's sessions: a
// statement prepared by a session lists erasure requests if its user may
// erase when it runs. Statements from outside a session see none.
func (s *Server) ErasureViewer(ctx context.Context) func() bool {
	sess := sessionFrom(ctx)
	if sess == nil {
		return nil
	}
	return func() bool { return s.mayErase(context.Background(), sess, sess.currentUser()) }
}

func (s *Server) showErasureRequests(ctx context.Context, w wire.DataWriter) error {
	if s.erasure == nil {
		return w.Complete("SELECT 0")
	}
	requests, err := s.erasure.List(ctx)
	if err != nil {
		return err
	}
	for _, r := range requests {
		if err := w.Row(erasure.Row(r)); err != nil {
			return err
		}
	}
	return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
}

// setRetention sets or removes a table's data retention, which is kept
// with policies and tags and, like them, stays with the table's name.
func (s *Server) setRetention(ctx context.Context, sess *session, user auth.User, c erasureCmd, w wire.DataWriter) error {
	if err := s.inNoTransaction(sess, "ALTER TABLE ... data_retention_days"); err != nil {
		return err
	}
	catalog, schema, table, _, err := s.policyTable(ctx, sess, c.table, c.days != nil)
	if err != nil {
		if c.ifExists && pgerror.GetCode(err) == codes.UndefinedTable {
			return w.Complete(tagRetention)
		}
		return err
	}
	if catalog != "" {
		return pgerror.WithCode(fmt.Errorf("%s is a table of connection %s: its database keeps its rows, not Igloo", table, catalog),
			codes.WrongObjectType)
	}
	now := time.Now()
	err = s.gov.store.Update(ctx, func(cat *policy.Catalog) error {
		if c.days != nil {
			return cat.SetRetention(policy.DataRetention{Schema: schema, Table: table, Days: *c.days, SetBy: user.Name}, now)
		}
		if err := cat.UnsetRetention(schema, table); !errors.Is(err, policy.ErrNotFound) {
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
	days := "none"
	if c.days != nil {
		days = strconv.Itoa(*c.days)
	}
	s.log.Info("data retention changed", "table", schema+"."+table, "days", days, "by", user.Name)
	return w.Complete(tagRetention)
}
