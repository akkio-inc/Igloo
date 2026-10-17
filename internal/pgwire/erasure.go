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
//	ERASE SUBJECT WHERE tag.value = 'identifier' [OR tag.value IN ('identifier', ...)] ... [DRY RUN]
//	SHOW ERASURE REQUESTS
//	CANCEL ERASURE REQUEST 'id'
//	CONFIRM ERASURE REQUEST 'id'
//	CLOSE ERASURE REQUEST 'id'
//	ALTER TABLE [IF EXISTS] name SET (data_retention_days = n)
//	ALTER TABLE [IF EXISTS] name RESET (data_retention_days)
//
// Admins may run them, and users granted ERASE ON ACCOUNT, directly or
// through a role: privacy officers, who may set data retentions only of
// tables with tags. CLOSE ERASURE REQUEST, the last resort for a blocked
// request, is for admins only. The identifiers in ERASE SUBJECT never
// reach the history, the logs or the list of running statements.

// erasureCmd is a parsed erasure or retention statement.
type erasureCmd struct {
	tag    string
	wanted []erasure.Wanted
	// dryRun is ERASE SUBJECT ... DRY RUN's; id names the request CANCEL
	// and CONFIRM change.
	dryRun bool
	id     string
	// table, ifExists and days are ALTER TABLE's; days is nil for RESET.
	table    []string
	ifExists bool
	days     *int
}

const (
	tagErase        = "ERASE SUBJECT"
	tagShowErase    = "SHOW ERASURE REQUESTS"
	tagCancelErase  = "CANCEL ERASURE REQUEST"
	tagConfirmErase = "CONFIRM ERASURE REQUEST"
	tagCloseErase   = "CLOSE ERASURE REQUEST"
	tagRetention    = "ALTER TABLE"
)

// data_retention_days is from a day, as maintenance keeps at least the
// current snapshot anyway, to a hundred years.
const minRetentionDays, maxRetentionDays = 1, 36500

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
			c.dryRun = sc.keywords("DRY", "RUN")
			if sc.done() {
				return c, true, nil
			}
			return syntax("expected OR, DRY RUN or the end of ERASE SUBJECT")
		}
	case sc.keywords("SHOW", "ERASURE", "REQUESTS"):
		c.tag = tagShowErase
		if !sc.done() {
			return syntax("unexpected text after SHOW ERASURE REQUESTS")
		}
		return c, true, nil
	case sc.keywords("CANCEL", "ERASURE", "REQUEST"), sc.keywords("CONFIRM", "ERASURE", "REQUEST"), sc.keywords("CLOSE", "ERASURE", "REQUEST"):
		// The keyword the scanner read, after any comments, says which.
		switch strings.ToUpper(firstWord(query)) {
		case "CONFIRM":
			c.tag = tagConfirmErase
		case "CLOSE":
			c.tag = tagCloseErase
		default:
			c.tag = tagCancelErase
		}
		lit, ok := sc.literal()
		if !ok || lit == nil || !sc.done() {
			return syntax("expected %s 'request id'", c.tag)
		}
		c.id = *lit
		return c, true, nil
	case sc.keywords("ALTER", "TABLE"):
		c.tag = tagRetention
		c.ifExists = sc.keywords("IF", "EXISTS")
		sc.keywords("ONLY")
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
			if err != nil || n < minRetentionDays || n > maxRetentionDays {
				return syntax("data_retention_days takes a number of days from %d to %d", minRetentionDays, maxRetentionDays)
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

// firstWord is the first word of query, after spaces and comments, as the
// scanner reads it.
func firstWord(query string) string {
	sc := &scanner{s: query}
	return sc.word()
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
		return erasureHidden
	}
	var parts []string
	for _, w := range c.wanted {
		stars := strings.TrimSuffix(strings.Repeat("'***', ", len(w.Identifiers)), ", ")
		parts = append(parts, quoteIdent(w.Tag)+"."+quoteIdent(w.Value)+" IN ("+stars+")")
	}
	text := "ERASE SUBJECT WHERE " + strings.Join(parts, " OR ")
	if c.dryRun {
		text += " DRY RUN"
	}
	return text
}

// erasureHidden is what the history shows of a statement that mentions
// ERASE SUBJECT in any other form, as EXPLAIN or after another statement,
// whose identifiers could be anywhere in it.
const erasureHidden = "ERASE ... (not shown: it may hold identifiers)"

// mentionsErasure reports whether a statement's code has ERASE SUBJECT.
func mentionsErasure(words []string) bool {
	for i := 0; i+1 < len(words); i++ {
		if words[i] == "ERASE" && words[i+1] == "SUBJECT" {
			return true
		}
	}
	return false
}

// previewColumns describe ERASE SUBJECT ... DRY RUN.
var previewColumns = wire.Columns{
	{Name: "table_name", Oid: pgtype.TextOID, Width: -1},
	{Name: "reason", Oid: pgtype.TextOID, Width: -1},
	{Name: "columns", Oid: pgtype.TextOID, Width: -1},
	{Name: "rows_matched", Oid: pgtype.Int8OID, Width: 8},
	{Name: "rows", Oid: pgtype.Int8OID, Width: 8},
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
	case tagErase, tagCancelErase, tagConfirmErase, tagCloseErase:
		cols := wire.Columns{
			{Name: "request_id", Oid: pgtype.TextOID, Width: -1},
			{Name: "status", Oid: pgtype.TextOID, Width: -1},
		}
		if c.dryRun {
			cols = previewColumns
		}
		opts = append(opts, wire.WithColumns(cols))
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
		return pgerror.WithCode(fmt.Errorf("%s needs an Iceberg catalog (IGLOO_CATALOG_URI)", c.tag), codes.FeatureNotSupported)
	}
	if err := s.inNoTransaction(sess, c.tag); err != nil {
		return err
	}
	var (
		r   *erasure.Request
		err error
	)
	switch {
	case c.dryRun:
		if r, err = s.erasure.Preview(ctx, c.wanted); err == nil {
			for _, t := range r.Tables {
				var cols []string
				for _, col := range t.Columns {
					cols = append(cols, col.Name+" ("+col.Tag+"."+col.Value+")")
				}
				if err := w.Row([]any{t.Name, t.Reason, strings.Join(cols, ", "), t.Matched, t.Rows}); err != nil {
					return err
				}
			}
			return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
		}
	case c.tag == tagCancelErase:
		r, err = s.erasure.Cancel(ctx, c.id, user.Name)
	case c.tag == tagConfirmErase:
		r, err = s.erasure.Confirm(ctx, c.id, user.Name)
	case c.tag == tagCloseErase:
		// The last resort: it forgets whose data the request erased while
		// some is left, which only an admin decides.
		if user.Role != auth.RoleAdmin {
			return pgerror.WithCode(fmt.Errorf("permission denied: %s requires the admin role", c.tag), codes.InsufficientPrivilege)
		}
		r, err = s.erasure.Close(ctx, c.id, user.Name)
	default:
		r, err = s.erasure.Submit(ctx, user.Name, c.wanted)
	}
	switch {
	case errors.Is(err, erasure.ErrInvalid):
		return pgerror.WithCode(err, codes.InvalidParameterValue)
	case errors.Is(err, erasure.ErrNotFound):
		return pgerror.WithCode(fmt.Errorf("erasure request %s does not exist", c.id), codes.UndefinedObject)
	case errors.Is(err, erasure.ErrBusy):
		return pgerror.WithCode(err, codes.LockNotAvailable)
	case errors.Is(err, erasure.ErrNotYet):
		return pgerror.WithCode(err, codes.ObjectNotInPrerequisiteState)
	case err != nil:
		return err
	}
	if err := w.Row([]any{r.ID, r.Status}); err != nil {
		return err
	}
	return w.Complete(c.tag)
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

// hasTags reports whether a table has tagged columns.
func hasTags(r *policy.TableRules) bool {
	if r == nil {
		return false
	}
	for _, c := range r.Columns {
		if len(c.Tags) > 0 {
			return true
		}
	}
	return false
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
	// Privacy officers look after tables with personal data, which are
	// tagged; a retention elsewhere would cut other tables' history, so
	// that is up to admins.
	if user.Role != auth.RoleAdmin && !hasTags(s.gov.snapshot(ctx).Tagged(schema, table)) {
		return pgerror.WithCode(fmt.Errorf("permission denied: %s.%s has no tagged columns; only admins may set data retentions of other tables",
			schema, table), codes.InsufficientPrivilege)
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
