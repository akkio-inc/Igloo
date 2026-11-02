package pgwire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/sqllex"
	"github.com/akkio-inc/igloo/internal/workload"
)

// Workload management: query tags attribute statements to clients and
// campaigns in the history and igloo.usage, and admins limit what users
// run with settings on users and roles:
//
//	SET query_tag = 'client:acme;campaign:q4'    -- also ALTER SESSION SET
//	RESET query_tag
//	SHOW query_tag
//	ALTER ROLE name SET setting { = | TO } value | DEFAULT
//	ALTER ROLE name RESET setting | ALL
//	ALTER USER name SET ... | RESET ...
//	SHOW ROLE SETTINGS
//
// ALTER ROLE takes a custom role, a built-in role (for everyone who has
// it) or, as in PostgreSQL, a user. The settings are those of package
// workload. Limits apply on each replica, through workload.Controller.

// workloadState is what a session's statements run under.
type workloadState struct {
	// tag is the session's own query tag; tagSet says it replaces the
	// default from the user's settings, even when empty.
	tag    string
	tagSet bool
	// limits are the user's settings, as of loaded. registry.expire
	// clears loaded, under the session's lock, when settings change.
	limits workload.Limits
	loaded time.Time
	// queued is how long the latest statement waited to start.
	queued time.Duration
	// counted says the statement about to run counts against the
	// replica's limit of statements at once (see countsOnReplica).
	counted bool
	// apart says it is known to be heavy and runs in the second DuckDB (see
	// workload.Limits.Apart).
	apart bool
	// shape is the statement's text with its literals masked, by which the
	// controller estimates what it reserves (see statementShape); "" for
	// none.
	shape string
	// peak is the most memory the latest statement was seen holding while
	// it ran, and sampled says a sample fell within its run (see
	// workload.Controller.SampleMemory).
	peak    int64
	sampled bool
	// meter reads what each statement used; nil without one.
	meter *engine.Meter
	// backgroundUsed is what the latest statement used, if it ran in the
	// second DuckDB, whose connection has a meter of its own.
	backgroundUsed *audit.Resources
	// ranInBackground says the statement measure last measured ran there.
	ranInBackground bool
	// controller holds statements back for their limits.
	controller *workload.Controller
}

// queryTag is the tag the session's statements run under.
func (sess *session) queryTag() string {
	if sess.wl.tagSet {
		return sess.wl.tag
	}
	return sess.wl.limits.QueryTag
}

// statementTimeout is the session's statement_timeout, capped by its
// user's limit.
func (sess *session) statementTimeout() time.Duration {
	limit := sess.wl.limits.StatementTimeout
	if limit > 0 && (sess.timeout == 0 || sess.timeout > limit) {
		return limit
	}
	return sess.timeout
}

// queuedTimeout is the session's statement_queued_timeout, capped by its
// user's limit.
func (sess *session) queuedTimeout() time.Duration {
	limit := sess.wl.limits.QueuedTimeout
	if limit > 0 && (sess.queueTimeout == 0 || sess.queueTimeout > limit) {
		return limit
	}
	return sess.queueTimeout
}

// countsOnReplica reports whether st counts against the replica's limit of
// statements at once (IGLOO_MAX_CONCURRENT_STATEMENTS): it runs a query or
// changes data, and holds nothing while it waits to start (see
// catalogRunLock). Settings, transaction control, catalog listings and
// DDL do not count, and neither does a result the cache sends (see
// fromCache), which uses nothing, nor a query that asks for no rows.
func countsOnReplica(sess *session, st *statement) bool {
	if noRows.MatchString(stripComments(st.prepared)) {
		// It runs nothing, and waits for nothing (see limitZero).
		return false
	}
	if st.stmtType == duckdb.STATEMENT_TYPE_CREATE && st.staged != nil {
		// CREATE TABLE ... AS, carried out in steps that hold the catalog
		// only to create and rename (see runStagedCreate).
		return !sess.inTx
	}
	if st.listsCatalog || st.changesIceberg {
		return false
	}
	switch st.stmtType {
	case duckdb.STATEMENT_TYPE_SELECT, duckdb.STATEMENT_TYPE_INSERT, duckdb.STATEMENT_TYPE_UPDATE,
		duckdb.STATEMENT_TYPE_DELETE, statementTypeMergeInto, duckdb.STATEMENT_TYPE_COPY:
		return true
	}
	return false
}

// admit waits until the session's limits let a statement start, and
// returns the function to call when it ends.
func (sess *session) admit(ctx context.Context) (func(), error) {
	l := sess.wl.limits
	l.Counted = sess.wl.counted
	l.Apart = sess.wl.apart
	if controller := sess.wl.controller; l.Counted {
		l.Expect = controller.Expect(sess.wl.shape)
		switch {
		case controller.Light(sess.wl.shape):
			l.Counted = false
		case l.Memory == 0:
			// What statements like it held, when its role reserves nothing.
			l.Memory = controller.Estimate(sess.wl.shape)
			_, seen := controller.Footprint(sess.wl.shape)
			l.Unseen = !seen
		}
	}
	release, waited, err := sess.wl.controller.Admit(ctx, l)
	sess.wl.queued = waited
	sess.wl.peak, sess.wl.sampled = 0, false
	if err == nil {
		return func() { sess.wl.peak, sess.wl.sampled = release() }, nil
	}
	behind := []string{}
	if n := sess.wl.controller.ReplicaLimit(); l.Counted && n > 0 {
		unit := "statements"
		if n == 1 {
			unit = "statement"
		}
		behind = append(behind, fmt.Sprintf("the replica's limit of %d %s at once", n, unit))
	}
	if l.Slots.Max > 0 {
		unit := "statements"
		if l.Slots.Max == 1 {
			unit = "statement"
		}
		behind = append(behind, fmt.Sprintf("the limit of %d %s at once for %s", l.Slots.Max, unit, l.Slots.Key))
	}
	if l.Memory > 0 {
		behind = append(behind, "the memory other statements reserved")
	}
	why := "user request"
	if errors.Is(err, context.DeadlineExceeded) {
		why = "statement queued timeout"
	}
	return nil, pgerror.WithCode(fmt.Errorf("canceling statement due to %s: it waited %s to start, behind %s",
		why, waited.Round(time.Millisecond), strings.Join(behind, " and ")), codes.QueryCanceled)
}

// numberLiteral is a number in SQL code, not part of a name.
var numberLiteral = regexp.MustCompile(`\b[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?\b`)

// limitZero is a limit of no rows, which tools send to learn a statement's
// columns: DuckDB plans it as an empty result, and runs none of the statement.
var limitZero = regexp.MustCompile(`(?i)\bLIMIT\s+0\b`)

// noRows reports whether the statement text is a query that ends in a limit of
// no rows.
var noRows = regexp.MustCompile(`(?i)\bLIMIT\s+0\s*;?\s*$`)

// shapeKey identifies the statements of text's shape (statementShape) in
// the workload controller and the query history, which keeps it.
func shapeKey(text string) string {
	sum := sha256.Sum256([]byte(statementShape(text)))
	return hex.EncodeToString(sum[:12])
}

// workloadShape is the shape the workload controller, the query history and
// the FLATTEN forms know a statement of sess by: its text's (see shapeKey)
// where its names resolve, the session's search path and database. A
// dashboard's statements name their tables bare, so the same text over
// another schema, another client's or the same data at ten times the size,
// reads other tables, and its runs say nothing of them: a burst at ten times
// the data reserved what its statements had at the data's size. The path is
// read once for each of the session's settings (see settingGen).
func (s *Server) workloadShape(ctx context.Context, sess *session, text string) string {
	if !sess.shapePathKnown || sess.shapePathGen != sess.settingGen {
		path, err := sessionPath(ctx, sess.conn)
		if err != nil {
			return shapeKey(text)
		}
		sess.shapePath = fmt.Sprintf("%q@%q", path.entries, path.defaultDB)
		sess.shapePathGen, sess.shapePathKnown = sess.settingGen, true
	}
	sum := sha256.Sum256([]byte(statementShape(text) + "\x00" + sess.shapePath))
	return hex.EncodeToString(sum[:12])
}

// statementShape is text with its string and number literals masked, its
// comments left out and its white space collapsed: a dashboard's statement
// sent again with another audience, date or limit has the same shape.
func statementShape(text string) string {
	var b strings.Builder
	for _, seg := range sqllex.Segments(text) {
		switch seg.Kind {
		case sqllex.String:
			b.WriteString("?")
		case sqllex.Code:
			// A statement with no rows is not the same work as one with some,
			// though only a number tells them apart: of a dashboard's 96
			// queries, 32 asked for their columns that way, and the shape they
			// shared with the ones that ran said they used next to nothing.
			b.WriteString(numberLiteral.ReplaceAllString(limitZero.ReplaceAllString(seg.Text, "LIMIT NONE"), "?"))
		case sqllex.Ident:
			b.WriteString(seg.Text)
		case sqllex.Comment:
			b.WriteString(" ")
		}
	}
	sum := sha256.Sum256([]byte(strings.Join(strings.Fields(b.String()), " ")))
	return string(sum[:])
}

// measure returns what DuckDB measured of st, which just ran.
func (sess *session) measure(st *statement) *audit.Resources {
	sess.wl.ranInBackground = sess.wl.backgroundUsed != nil
	if u := sess.wl.backgroundUsed; u != nil {
		// It ran in the second DuckDB (see background.go).
		sess.wl.backgroundUsed = nil
		return u
	}
	return sess.wl.meter.Last(st.prepared)
}

// profiled runs st with run. An EXPLAIN ANALYZE runs with DuckDB's own
// profiling settings, so that it prints every metric it would without
// Igloo's.
func (sess *session) profiled(ctx context.Context, st *statement, run func() error) error {
	if st.stmtType != duckdb.STATEMENT_TYPE_EXPLAIN {
		return run()
	}
	words := sqllex.CodeWords(st.query)
	if !slices.Contains(words, "ANALYZE") && !slices.Contains(words, "ANALYSE") {
		return run()
	}
	return sess.wl.meter.ExplainAnalyze(ctx, run)
}

// refreshLimits loads the workload settings of the session's user when
// they are as old as the role refresh interval. If the store fails, the
// last ones loaded stay in force, as the role does.
func (s *Server) refreshLimits(ctx context.Context, sess *session, user auth.User) {
	sess.mu.Lock()
	fresh := !sess.wl.loaded.IsZero() && time.Since(sess.wl.loaded) < s.roleRefresh
	sess.mu.Unlock()
	if fresh || s.settings == nil {
		return
	}
	settings, err := s.settings.UserSettings(ctx, user.Name, user.Role)
	var roles []string
	if err == nil {
		var privs *auth.Privileges
		if privs, err = s.privileges(ctx, sess, user); err == nil {
			roles = privs.Roles
		}
	}
	if err != nil {
		s.log.Warn("could not load workload settings", "user", user.Name, "err", err)
		return
	}
	limits := workload.Resolve(user, roles, settings)
	sess.mu.Lock()
	sess.wl.limits, sess.wl.loaded = limits, time.Now()
	sess.mu.Unlock()
}

var (
	setQueryTag   = regexp.MustCompile(`(?is)^\s*(?:SET\s+(?:SESSION\s+|LOCAL\s+)?|ALTER\s+SESSION\s+SET\s+)"?query_tag"?\s*(?:=|\bTO\b)\s*(.*?)\s*;?\s*$`)
	resetQueryTag = regexp.MustCompile(`(?is)^\s*(?:RESET|ALTER\s+SESSION\s+UNSET)\s+"?query_tag"?\s*;?\s*$`)
	showQueryTag  = regexp.MustCompile(`(?is)^\s*SHOW\s+"?query_tag"?\s*;?\s*$`)
	// tagValue is a quoted string, or a bare word as PostgreSQL allows.
	tagValue = regexp.MustCompile(`(?s)^(?:'((?:[^']|'')*)'|([A-Za-z0-9_:.\-]+))$`)
)

// queryTagCommand handles SET, RESET and SHOW query_tag, which Igloo keeps
// itself rather than DuckDB.
func (s *Server) queryTagCommand(sess *session, text string) (wire.PreparedStatements, bool) {
	query := stripComments(text)
	respond := func(fn wire.PreparedStatementFn, opts ...wire.PreparedOptionFn) (wire.PreparedStatements, bool) {
		return wire.Prepared(wire.NewStatement(s.audited(sess, text, fn), opts...)), true
	}
	if m := setQueryTag.FindStringSubmatch(query); m != nil {
		tag, isDefault, err := parseTagValue(m[1])
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			if err != nil {
				return err
			}
			sess.wl.tag, sess.wl.tagSet = tag, !isDefault
			return w.Complete("SET")
		})
	}
	if resetQueryTag.MatchString(query) {
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			sess.wl.tag, sess.wl.tagSet = "", false
			return w.Complete("RESET")
		})
	}
	if showQueryTag.MatchString(query) {
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			if err := w.Row([]any{sess.queryTag()}); err != nil {
				return err
			}
			return w.Complete("SHOW")
		}, wire.WithColumns(wire.Columns{{Name: "query_tag", Oid: pgtype.TextOID, Width: -1}}))
	}
	return nil, false
}

// parseTagValue reads the value of SET query_tag: a quoted string, a bare
// word, or DEFAULT for the user's default.
func parseTagValue(v string) (tag string, isDefault bool, err error) {
	if strings.EqualFold(v, "default") {
		return "", true, nil
	}
	m := tagValue.FindStringSubmatch(v)
	if m == nil {
		return "", false, pgerror.WithCode(errors.New("SET query_tag takes one quoted string, such as 'client:acme;campaign:q4'"), codes.Syntax)
	}
	tag = m[2]
	if m[2] == "" {
		tag = strings.ReplaceAll(m[1], "''", "'")
	}
	if err := workload.CheckQueryTag(tag); err != nil {
		return "", false, pgerror.WithCode(err, codes.InvalidParameterValue)
	}
	return tag, false, nil
}

// settingCommand is a parsed ALTER ROLE or ALTER USER ... SET or RESET, or
// SHOW ROLE SETTINGS.
type settingCommand struct {
	show bool
	// role says the statement was ALTER ROLE, whose target may be a
	// custom role, a built-in role or a user; ALTER USER takes a user.
	role   bool
	target string
	// name is the setting; reset with an empty name means RESET ALL.
	name  string
	value string
	reset bool
}

var settingColumns = wire.Columns{
	{Name: "target_type", Oid: pgtype.TextOID, Width: -1},
	{Name: "target", Oid: pgtype.TextOID, Width: -1},
	{Name: "setting", Oid: pgtype.TextOID, Width: -1},
	{Name: "value", Oid: pgtype.TextOID, Width: -1},
	{Name: "set_by", Oid: pgtype.TextOID, Width: -1},
	{Name: "set_at", Oid: pgtype.TimestamptzOID, Width: 8},
}

func (c *settingCommand) columns() wire.Columns {
	if c.show {
		return settingColumns
	}
	return nil
}

// parseSettingCommand parses one statement's tokens. ok is false when it
// is not a setting statement.
func parseSettingCommand(toks []token) (c *settingCommand, ok bool, err error) {
	word := func(i int) string {
		if i < len(toks) {
			return toks[i].upper()
		}
		return ""
	}
	syntax := func(format string, args ...any) (*settingCommand, bool, error) {
		return nil, true, pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
	}
	if word(0) == "SHOW" && word(1) == "ROLE" && word(2) == "SETTINGS" {
		if len(toks) != 3 {
			return syntax("unexpected %q after SHOW ROLE SETTINGS", toks[3].text)
		}
		return &settingCommand{show: true}, true, nil
	}
	if word(0) != "ALTER" || word(1) != "ROLE" && word(1) != "USER" || word(3) != "SET" && word(3) != "RESET" {
		return nil, false, nil
	}
	c = &settingCommand{role: word(1) == "ROLE", reset: word(3) == "RESET"}
	if c.target, err = principalName(toks[2]); err != nil {
		return nil, true, err
	}
	rest := toks[4:]
	if c.reset {
		switch {
		case len(rest) == 1 && rest[0].upper() == "ALL":
		case len(rest) == 1 && rest[0].kind == tokWord:
			c.name = rest[0].ident()
		default:
			return syntax("expected RESET setting or RESET ALL")
		}
		return c, true, validSetting(c.name)
	}
	isDigits := func(s string) bool {
		return s != "" && strings.Trim(s, "0123456789") == ""
	}
	if len(rest) == 5 && rest[3].text == "." && rest[2].kind == tokWord && rest[4].kind == tokWord && isDigits(rest[2].text) &&
		isDigits(rest[4].text) {
		// A decimal, as ai_max_cost_usd = 0.25 is.
		rest = append(rest[:2:2], token{kind: tokWord, text: rest[2].text + "." + rest[4].text})
	}
	if len(rest) != 3 || rest[0].kind != tokWord || rest[1].upper() != "TO" && rest[1].text != "=" {
		return syntax("expected SET setting = value")
	}
	c.name = rest[0].ident()
	if err := validSetting(c.name); err != nil {
		return nil, true, err
	}
	switch v := rest[2]; {
	case v.upper() == "DEFAULT":
		c.reset = true
	case v.kind == tokString, v.kind == tokWord:
		value, err := workload.Normalize(c.name, v.text)
		if err != nil {
			return nil, true, pgerror.WithCode(err, codes.InvalidParameterValue)
		}
		c.value = value
	default:
		return syntax("the value of %s must be a quoted string or a number", c.name)
	}
	return c, true, nil
}

// validSetting refuses settings that are not workload settings. An empty
// name stands for ALL.
func validSetting(name string) error {
	if name == "" {
		return nil
	}
	for _, n := range workload.Names {
		if n == name {
			return nil
		}
	}
	return pgerror.WithCode(fmt.Errorf("unknown role setting %q; use %s", name, strings.Join(workload.Names, ", ")),
		codes.UndefinedObject)
}

// runSettingCommand carries out c for sess.
func (s *Server) runSettingCommand(ctx context.Context, sess *session, c *settingCommand, w wire.DataWriter) error {
	me := sess.currentUser()
	if s.settings == nil {
		return pgerror.WithCode(errors.New("role settings need IGLOO_METADATA_DATABASE_URL"), codes.FeatureNotSupported)
	}
	if c.show {
		return s.showSettings(ctx, me, w)
	}
	if me.Role != auth.RoleAdmin {
		return pgerror.WithCode(errors.New("permission denied: ALTER ROLE ... SET requires the admin role"), codes.InsufficientPrivilege)
	}
	targetType, err := s.settingTarget(ctx, c)
	if err != nil {
		return err
	}
	if c.reset {
		var names []string
		if c.name != "" {
			names = []string{c.name}
		}
		err = s.settings.ResetSettings(ctx, targetType, c.target, names)
	} else {
		err = s.settings.SetSetting(ctx, auth.Setting{TargetType: targetType, Target: c.target, Name: c.name, Value: c.value, SetBy: me.Name})
	}
	switch {
	case errors.Is(err, auth.ErrNotFound):
		return pgerror.WithCode(fmt.Errorf("user %q does not exist", c.target), codes.UndefinedObject)
	case errors.Is(err, auth.ErrRoleNotFound):
		return pgerror.WithCode(fmt.Errorf("role %q does not exist", c.target), codes.UndefinedObject)
	case err != nil:
		return err
	}
	if targetType == auth.GranteeUser {
		s.sessions.expire(c.target)
	} else {
		s.sessions.expireAll()
	}
	s.log.Info("role settings changed", "target", c.target, "setting", c.name, "reset", c.reset, "by", me.Name)
	if c.role {
		return w.Complete("ALTER ROLE")
	}
	return w.Complete("ALTER USER")
}

// settingTarget decides what c's target is: ALTER USER takes a user, and
// ALTER ROLE a built-in role, a custom role or a user.
func (s *Server) settingTarget(ctx context.Context, c *settingCommand) (string, error) {
	if c.role {
		if auth.IsBuiltinRole(c.target) {
			return auth.TargetBuiltin, nil
		}
		if isRole, err := s.isCustomRole(ctx, c.target); err != nil || isRole {
			return auth.GranteeRole, err
		}
	}
	if _, err := s.users.Get(ctx, c.target); err != nil {
		if errors.Is(err, auth.ErrNotFound) {
			kind := "user"
			if c.role {
				kind = "role or user"
			}
			return "", pgerror.WithCode(fmt.Errorf("%s %q does not exist", kind, c.target), codes.UndefinedObject)
		}
		return "", err
	}
	return auth.GranteeUser, nil
}

// showSettings lists role settings: every one for admins, those that bear
// on the user otherwise.
func (s *Server) showSettings(ctx context.Context, me auth.User, w wire.DataWriter) error {
	var settings []auth.Setting
	var err error
	if me.Role == auth.RoleAdmin {
		settings, err = s.settings.Settings(ctx)
	} else {
		settings, err = s.settings.UserSettings(ctx, me.Name, me.Role)
	}
	if err != nil {
		return err
	}
	for _, x := range settings {
		kind := x.TargetType
		if kind == auth.TargetBuiltin {
			kind = "built-in role"
		}
		if err := w.Row([]any{kind, x.Target, x.Name, x.Value, x.SetBy, x.SetAt}); err != nil {
			return err
		}
	}
	return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
}
