package pgwire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/notify"
	"github.com/akkio-inc/igloo/internal/schedule"
	"github.com/akkio-inc/igloo/internal/sqllex"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// Notification integrations, alerts and SYSTEM$SEND_NOTIFICATION. Igloo
// carries out these statements itself, keeping integrations in the
// notification store (package notify):
//
//	CREATE [OR REPLACE] NOTIFICATION INTEGRATION [IF NOT EXISTS] name TYPE [=] webhook | slack | teams | email [option ...]
//	ALTER NOTIFICATION INTEGRATION [IF EXISTS] name SET option ... | UNSET name [, ...]
//	DROP [NOTIFICATION] INTEGRATION [IF EXISTS] name
//	SHOW [NOTIFICATION] INTEGRATIONS
//	GRANT USAGE ON INTEGRATION name TO [ROLE | USER] grantee
//	REVOKE USAGE ON INTEGRATION name FROM [ROLE | USER] grantee
//	CALL SYSTEM$SEND_NOTIFICATION('integration', 'message')
//
// with the options URL '...', HEADERS ('name' = 'value', ...), SECRET '...',
// RECIPIENTS ('address', ...), ENABLED TRUE | FALSE, OPERATIONAL_ALERTS
// (name [= n], ...) and COMMENT '...'. Only admins manage integrations and
// who may use them. Alerts are jobs (see jobs.go), whose IF ... THEN
// NOTIFY ... this file reads and runs:
//
//	CREATE [OR REPLACE] ALERT [IF NOT EXISTS] name SCHEDULE 'spec' [TIMEZONE 'tz']
//	  IF (EXISTS (query)) THEN NOTIFY integration [MESSAGE 'text'] [WITH RESULT]
//
// Sending to an integration, from an alert or with SYSTEM$SEND_NOTIFICATION,
// needs USAGE on it, checked when the alert is created and again at every
// run: what an alert sends is what its query returns as its owner, with
// the owner's grants and policies, so only integrations the owner may use
// get it.

// integrationCommand is a parsed statement on integrations.
type integrationCommand struct {
	verb string // CREATE, ALTER, DROP, SHOW, GRANT, REVOKE or CALL
	name string
	// replace, ifExists and ifNotExists are the statement's clauses.
	replace, ifExists, ifNotExists bool
	// set are the options of CREATE and ALTER ... SET; unset the names of
	// ALTER ... UNSET.
	set   integrationOptions
	unset []string
	// grantee is whom GRANT and REVOKE name.
	grantee string
	// message is what CALL sends.
	message string
}

// integrationOptions are an integration's settings as a statement gives
// them; nil means not given.
type integrationOptions struct {
	typ        *string
	url        *string
	headers    map[string]string
	secret     *string
	recipients []string
	enabled    *bool
	builtin    *notify.Builtin
	comment    *string
}

// tag is the command tag of a successful c.
func (c integrationCommand) tag() string {
	switch c.verb {
	case "CREATE", "ALTER":
		return c.verb + " NOTIFICATION INTEGRATION"
	case "DROP":
		return "DROP INTEGRATION"
	}
	return c.verb
}

func integrationSyntax(format string, args ...any) error {
	return pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
}

// parseIntegrationCommand recognizes the statements above. ok is false for
// any other; err reports a malformed one.
func parseIntegrationCommand(query string) (c integrationCommand, ok bool, err error) {
	sc := &scanner{s: query}
	switch {
	case sc.keywords("CREATE"):
		c.replace = sc.keywords("OR", "REPLACE")
		if !sc.keywords("NOTIFICATION", "INTEGRATION") {
			return c, false, nil
		}
		c.verb = "CREATE"
		c.ifNotExists = sc.keywords("IF", "NOT", "EXISTS")
	case sc.keywords("ALTER", "NOTIFICATION", "INTEGRATION"):
		c.verb = "ALTER"
		c.ifExists = sc.keywords("IF", "EXISTS")
	case sc.keywords("DROP", "INTEGRATION"), sc.keywords("DROP", "NOTIFICATION", "INTEGRATION"):
		c.verb = "DROP"
		c.ifExists = sc.keywords("IF", "EXISTS")
	case sc.keywords("SHOW", "INTEGRATIONS"), sc.keywords("SHOW", "NOTIFICATION", "INTEGRATIONS"):
		c.verb = "SHOW"
		if !sc.done() {
			return c, true, integrationSyntax("unexpected text after SHOW INTEGRATIONS")
		}
		return c, true, nil
	case sc.keywords("GRANT", "USAGE", "ON", "INTEGRATION"):
		c.verb = "GRANT"
	case sc.keywords("REVOKE", "USAGE", "ON", "INTEGRATION"):
		c.verb = "REVOKE"
	case sc.keywords("CALL", "SYSTEM$SEND_NOTIFICATION"):
		c.verb = "CALL"
		return c, true, parseSendNotification(sc, &c)
	default:
		return c, false, nil
	}

	if c.name, err = integrationName(sc); err != nil {
		return c, true, err
	}
	switch c.verb {
	case "CREATE":
		err = parseIntegrationOptions(sc, &c.set)
		if err == nil && c.set.typ == nil {
			err = integrationSyntax("CREATE NOTIFICATION INTEGRATION needs a TYPE: webhook, slack, teams or email")
		}
		return c, true, err
	case "ALTER":
		switch {
		case sc.keywords("SET"):
			err = parseIntegrationOptions(sc, &c.set)
		case sc.keywords("UNSET"):
			for {
				w := sc.word()
				if w == "" {
					return c, true, integrationSyntax("expected the setting to UNSET, such as UNSET SECRET")
				}
				sc.i += len(w)
				c.unset = append(c.unset, w)
				if !sc.punct(',') {
					break
				}
			}
		default:
			return c, true, integrationSyntax("expected SET or UNSET after ALTER NOTIFICATION INTEGRATION %s", c.name)
		}
		if err == nil && !sc.done() {
			err = integrationSyntax("unexpected text after ALTER NOTIFICATION INTEGRATION %s: %q", c.name, strings.TrimSpace(sc.rest()))
		}
		return c, true, err
	case "GRANT", "REVOKE":
		to := "TO"
		if c.verb == "REVOKE" {
			to = "FROM"
		}
		if !sc.keywords(to) {
			return c, true, integrationSyntax("expected %s USAGE ON INTEGRATION %s %s role_or_user", c.verb, c.name, to)
		}
		_ = sc.keywords("ROLE") || sc.keywords("USER")
		name, ok := sc.name()
		if !ok || len(name) != 1 || !config.ValidName(name[0]) {
			return c, true, integrationSyntax("expected the role or user %s %s", strings.ToLower(to), c.verb)
		}
		c.grantee = name[0]
	}
	if !sc.done() {
		return c, true, integrationSyntax("unexpected text after %s ... INTEGRATION %s: %q", c.verb, c.name, strings.TrimSpace(sc.rest()))
	}
	return c, true, nil
}

// integrationName reads and checks an integration's name.
func integrationName(sc *scanner) (string, error) {
	name, ok := sc.name()
	if !ok {
		return "", integrationSyntax("expected the integration's name")
	}
	if len(name) != 1 || !config.ValidName(name[0]) {
		return "", pgerror.WithCode(fmt.Errorf("invalid integration name %q: use lowercase letters, digits and underscores",
			strings.Join(name, ".")), codes.InvalidName)
	}
	return name[0], nil
}

// parseSendNotification reads SYSTEM$SEND_NOTIFICATION's arguments: an
// integration, as a name or a string, and the message, a string.
func parseSendNotification(sc *scanner, c *integrationCommand) error {
	usage := integrationSyntax("expected CALL SYSTEM$SEND_NOTIFICATION('integration', 'message')")
	if !sc.punct('(') {
		return usage
	}
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == '\'' {
		v, ok := sc.literal()
		if !ok || v == nil {
			return usage
		}
		c.name = *v
	} else {
		name, err := integrationName(sc)
		if err != nil {
			return err
		}
		c.name = name
	}
	if !sc.punct(',') {
		return usage
	}
	msg, ok := sc.literal()
	if !ok || msg == nil {
		return integrationSyntax("SYSTEM$SEND_NOTIFICATION's message is a string; it cannot be computed from a query")
	}
	c.message = *msg
	if !sc.punct(')') || !sc.done() {
		return usage
	}
	if !config.ValidName(c.name) {
		return pgerror.WithCode(fmt.Errorf("invalid integration name %q", c.name), codes.InvalidName)
	}
	return nil
}

// parseIntegrationOptions reads option [=] value ..., separated by spaces
// or commas, until the statement ends.
func parseIntegrationOptions(sc *scanner, o *integrationOptions) error {
	seen := map[string]bool{}
	for !sc.done() {
		sc.punct(',')
		key := sc.word()
		if key == "" {
			return integrationSyntax("expected an option, such as URL '...', at %q", strings.TrimSpace(sc.rest()))
		}
		sc.i += len(key)
		canonical := map[string]string{"WEBHOOK_URL": "URL", "WEBHOOK_HEADERS": "HEADERS", "ALLOWED_RECIPIENTS": "RECIPIENTS"}[key]
		if canonical == "" {
			canonical = key
		}
		if seen[canonical] {
			return integrationSyntax("conflicting or redundant option %s", key)
		}
		seen[canonical] = true
		sc.punct('=')
		str := func() (*string, error) {
			v, ok := sc.literal()
			if !ok || v == nil {
				return nil, integrationSyntax("%s takes a string", key)
			}
			return v, nil
		}
		var err error
		switch canonical {
		case "TYPE":
			w := sc.word()
			if w == "" {
				var v *string
				if v, err = str(); err == nil {
					w = *v
				}
			} else {
				sc.i += len(w)
			}
			t := strings.ToLower(w)
			o.typ = &t
		case "URL":
			o.url, err = str()
		case "SECRET":
			o.secret, err = str()
		case "COMMENT":
			o.comment, err = str()
		case "ENABLED":
			switch sc.word() {
			case "TRUE":
				v := true
				o.enabled, sc.i = &v, sc.i+4
			case "FALSE":
				v := false
				o.enabled, sc.i = &v, sc.i+5
			default:
				err = integrationSyntax("ENABLED takes TRUE or FALSE")
			}
		case "HEADERS":
			o.headers = map[string]string{}
			err = parseList(sc, key, func() error {
				k, ok := sc.literal()
				if !ok || k == nil || !sc.punct('=') {
					return integrationSyntax("HEADERS takes ('name' = 'value', ...)")
				}
				v, ok := sc.literal()
				if !ok || v == nil {
					return integrationSyntax("HEADERS takes ('name' = 'value', ...)")
				}
				if _, dup := o.headers[*k]; dup {
					return integrationSyntax("the header %s is given twice", *k)
				}
				o.headers[*k] = *v
				return nil
			})
		case "RECIPIENTS":
			o.recipients = []string{}
			err = parseList(sc, key, func() error {
				v, ok := sc.literal()
				if !ok || v == nil {
					return integrationSyntax("RECIPIENTS takes ('name@example.com', ...)")
				}
				o.recipients = append(o.recipients, *v)
				return nil
			})
		case "OPERATIONAL_ALERTS":
			b := notify.Builtin{}
			err = parseList(sc, key, func() error {
				name := sc.word()
				if name == "" {
					return integrationSyntax("OPERATIONAL_ALERTS takes names, such as (FAILED_RUNS, IDLE_PIPES = 24)")
				}
				sc.i += len(name)
				n, given := 0, sc.punct('=')
				if given {
					w := sc.word()
					var perr error
					if n, perr = strconv.Atoi(w); perr != nil {
						return integrationSyntax("%s takes a whole number", name)
					}
					sc.i += len(w)
				}
				if err := b.Set(name, n, given); err != nil {
					return pgerror.WithCode(err, codes.InvalidParameterValue)
				}
				return nil
			})
			o.builtin = &b
		default:
			return integrationSyntax("unknown option %s; the options are TYPE, URL, HEADERS, SECRET, RECIPIENTS, ENABLED, "+
				"OPERATIONAL_ALERTS and COMMENT", key)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// parseList reads (item, ...) with item reading each one; () is empty.
func parseList(sc *scanner, key string, item func() error) error {
	if !sc.punct('(') {
		return integrationSyntax("%s takes a list in parentheses", key)
	}
	if sc.punct(')') {
		return nil
	}
	for {
		if err := item(); err != nil {
			return err
		}
		if sc.punct(')') {
			return nil
		}
		if !sc.punct(',') {
			return integrationSyntax("expected , or ) in %s", key)
		}
	}
}

// apply puts o's settings into in.
func (o integrationOptions) apply(in *notify.Integration) {
	if o.url != nil {
		in.URL = *o.url
	}
	if o.headers != nil {
		in.Headers = o.headers
	}
	if o.secret != nil {
		in.Secret = *o.secret
	}
	if o.recipients != nil {
		in.Recipients = o.recipients
	}
	if o.enabled != nil {
		in.Enabled = *o.enabled
	}
	if o.builtin != nil {
		in.Builtin = *o.builtin
	}
	if o.comment != nil {
		in.Comment = *o.comment
	}
}

// notificationCommand carries out the statements above. ok is false for
// every other statement.
func (s *Server) notificationCommand(sess *session, query string) (wire.PreparedStatements, bool, error) {
	c, ok, err := parseIntegrationCommand(query)
	if !ok {
		return nil, false, nil
	}
	if len(sqllex.Split(query)) != 1 {
		err = pgerror.WithCode(errors.New("send statements on notification integrations on their own"), codes.FeatureNotSupported)
	}
	if err != nil {
		s.recordQuery(sess, queryRecord{text: query, start: time.Now(), err: err, rows: -1})
		return nil, true, err
	}
	var opts []wire.PreparedOptionFn
	if c.verb == "SHOW" {
		opts = append(opts, wire.WithColumns(integrationColumns))
	}
	return wire.Prepared(wire.NewStatement(s.audited(sess, query, func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		start := time.Now()
		user, err := s.refreshUser(ctx, sess)
		if err == nil {
			// The console lists the running statement; this one may hold a
			// webhook's token.
			err = sess.runCancelable(ctx, redactQuery(query), func(ctx context.Context) error {
				return s.runIntegrationCommand(ctx, sess, user, c, w)
			})
		}
		s.metrics.query(start, err)
		if err != nil {
			s.log.Debug("statement failed", "user", user.Name, "err", err)
		}
		return pgError(err)
	}), opts...)), true, nil
}

// integrationColumns describe SHOW INTEGRATIONS.
var integrationColumns = wire.Columns{
	{Name: "name", Oid: pgtype.TextOID, Width: -1},
	{Name: "type", Oid: pgtype.TextOID, Width: -1},
	{Name: "enabled", Oid: pgtype.BoolOID, Width: 1},
	{Name: "target", Oid: pgtype.TextOID, Width: -1},
	{Name: "operational_alerts", Oid: pgtype.TextOID, Width: -1},
	{Name: "usage", Oid: pgtype.TextOID, Width: -1},
	{Name: "comment", Oid: pgtype.TextOID, Width: -1},
	{Name: "created_by", Oid: pgtype.TextOID, Width: -1},
	{Name: "created_at", Oid: pgtype.TimestamptzOID, Width: 8},
}

func (s *Server) runIntegrationCommand(ctx context.Context, sess *session, user auth.User, c integrationCommand, w wire.DataWriter) error {
	switch c.verb {
	case "SHOW":
		return s.showIntegrations(ctx, user, w)
	case "CALL":
		if err := s.sendNotification(ctx, sess, user, c); err != nil {
			return err
		}
		return w.Complete("CALL")
	}
	if user.Role != auth.RoleAdmin {
		return pgerror.WithCode(fmt.Errorf("permission denied: %s requires the admin role", c.tag()), codes.InsufficientPrivilege)
	}
	store := s.notify.Store()
	var err error
	switch c.verb {
	case "CREATE":
		in := notify.Integration{Name: c.name, Type: *c.set.typ, Enabled: true, CreatedBy: user.Name}
		c.set.apply(&in)
		if _, gerr := store.Integration(ctx, c.name); gerr == nil && c.ifNotExists {
			return w.Complete(c.tag())
		}
		if s.aiNameTaken(ctx, c.name) {
			return pgerror.WithCode(fmt.Errorf("an AI integration is called %s; integrations of both kinds share names", c.name),
				codes.DuplicateObject)
		}
		if err = s.checkIntegration(ctx, in); err != nil {
			return err
		}
		err = store.CreateIntegration(ctx, in, c.replace)
	case "ALTER":
		var in notify.Integration
		if in, err = store.Integration(ctx, c.name); err != nil {
			break
		}
		if c.set.typ != nil && *c.set.typ != in.Type {
			return pgerror.WithCode(errors.New("an integration's TYPE cannot change; create another integration"),
				codes.FeatureNotSupported)
		}
		c.set.apply(&in)
		for _, u := range c.unset {
			switch u {
			case "HEADERS", "WEBHOOK_HEADERS":
				in.Headers = nil
			case "SECRET":
				in.Secret = ""
			case "COMMENT":
				in.Comment = ""
			case "OPERATIONAL_ALERTS":
				in.Builtin = notify.Builtin{}
			default:
				return integrationSyntax("only HEADERS, SECRET, COMMENT and OPERATIONAL_ALERTS can be unset, not %s", u)
			}
		}
		if err = s.checkIntegration(ctx, in); err != nil {
			return err
		}
		err = store.UpdateIntegration(ctx, in)
	case "DROP":
		err = store.DropIntegration(ctx, c.name)
	case "GRANT", "REVOKE":
		var granteeType string
		if granteeType, err = s.granteeType(ctx, c.grantee); err != nil {
			return err
		}
		if c.verb == "GRANT" {
			err = store.GrantUsage(ctx, notify.Usage{Integration: c.name, Grantee: c.grantee, GranteeType: granteeType,
				GrantedBy: user.Name})
		} else if _, err = store.Integration(ctx, c.name); err == nil {
			err = store.RevokeUsage(ctx, c.name, granteeType, c.grantee)
		}
	}
	switch {
	case errors.Is(err, notify.ErrNotFound) && c.ifExists:
		return w.Complete(c.tag())
	case errors.Is(err, notify.ErrNotFound):
		return integrationMissing(c.name)
	case errors.Is(err, notify.ErrExists):
		return pgerror.WithCode(fmt.Errorf("notification integration %s already exists", c.name), codes.DuplicateObject)
	case err != nil:
		return err
	}
	s.notify.Changed()
	s.log.Info("notification integration changed", "action", strings.ToLower(c.verb), "integration", c.name,
		"grantee", c.grantee, "by", user.Name)
	return w.Complete(c.tag())
}

func integrationMissing(name string) error {
	return pgerror.WithCode(fmt.Errorf("notification integration %s does not exist", name), codes.UndefinedObject)
}

// checkIntegration validates an integration's settings and that it may
// reach its URL's host, with SQLSTATEs.
func (s *Server) checkIntegration(ctx context.Context, in notify.Integration) error {
	err := s.notify.Check(ctx, in)
	var ae *notify.AddressError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &ae):
		return pgerror.WithCode(fmt.Errorf("permission denied: %w", err), codes.InsufficientPrivilege)
	case errors.Is(err, notify.ErrInvalid):
		return pgerror.WithCode(errors.New(strings.TrimPrefix(err.Error(), notify.ErrInvalid.Error()+": ")), codes.InvalidParameterValue)
	}
	return pgerror.WithCode(err, codes.InvalidParameterValue)
}

// showIntegrations lists integrations: every one, with who may use it, for
// admins; those they may use for others.
func (s *Server) showIntegrations(ctx context.Context, user auth.User, w wire.DataWriter) error {
	store := s.notify.Store()
	all, err := store.Integrations(ctx)
	if err != nil {
		return err
	}
	usage, err := store.Usage(ctx)
	if err != nil {
		return err
	}
	admin := user.Role == auth.RoleAdmin
	var roles []string
	if !admin {
		if roles, err = s.customRoles(ctx, user); err != nil {
			return err
		}
	}
	for _, in := range all {
		if !admin && !mayUse(usage, in.Name, user, roles) {
			continue
		}
		row := []any{in.Name, in.Type, in.Enabled, nullText(in.Target()), nullText(in.Builtin.String()), nil,
			nullText(in.Comment), in.CreatedBy, in.CreatedAt}
		if admin {
			var who []string
			for _, u := range usage {
				if u.Integration == in.Name {
					who = append(who, u.GranteeType+" "+u.Grantee)
				}
			}
			row[5] = nullText(strings.Join(who, ", "))
		}
		if err := w.Row(row); err != nil {
			return err
		}
	}
	return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
}

// customRoles are the custom roles user has, read now.
func (s *Server) customRoles(ctx context.Context, user auth.User) ([]string, error) {
	p, err := s.grants.UserPrivileges(ctx, user.Name)
	return p.Roles, err
}

// mayUse reports whether a grant in usage lets user, with custom roles,
// use the integration called name.
func mayUse(usage []notify.Usage, name string, user auth.User, roles []string) bool {
	return user.Role == auth.RoleAdmin || slices.ContainsFunc(usage, func(u notify.Usage) bool {
		return u.Integration == name && (u.GranteeType == auth.GranteeUser && u.Grantee == user.Name ||
			u.GranteeType == auth.GranteeRole && slices.Contains(roles, u.Grantee))
	})
}

// usableIntegration returns the integration called name if user may send
// to it now: it exists, is enabled, and user is an admin or has USAGE on
// it.
func (s *Server) usableIntegration(ctx context.Context, user auth.User, name string) (notify.Integration, error) {
	store := s.notify.Store()
	in, err := store.Integration(ctx, name)
	if errors.Is(err, notify.ErrNotFound) {
		return in, integrationMissing(name)
	}
	if err != nil {
		return in, err
	}
	if user.Role != auth.RoleAdmin {
		usage, err := store.Usage(ctx)
		if err != nil {
			return in, err
		}
		roles, err := s.customRoles(ctx, user)
		if err != nil {
			return in, err
		}
		if !mayUse(usage, name, user, roles) {
			return in, denied("%s has no USAGE on the notification integration %s; an admin can grant it with "+
				"GRANT USAGE ON INTEGRATION %s TO ...", user.Name, name, name)
		}
	}
	if !in.Enabled {
		return in, pgerror.WithCode(fmt.Errorf("the notification integration %s is disabled", name), codes.ObjectNotInPrerequisiteState)
	}
	return in, nil
}

// sendError gives a failed send its SQLSTATE.
func sendError(name string, err error) error {
	switch {
	case pgerror.GetCode(err) != codes.Uncategorized:
		return err
	case errors.Is(err, notify.ErrRateLimited):
		return pgerror.WithCode(err, codes.ConfigurationLimitExceeded)
	}
	return pgerror.WithCode(fmt.Errorf("could not notify %s: %w", name, err), codes.Io)
}

// sendNotification carries out SYSTEM$SEND_NOTIFICATION for user: a
// message they wrote, never data, to an integration they may use. In a
// task it runs as the task's owner.
func (s *Server) sendNotification(ctx context.Context, sess *session, user auth.User, c integrationCommand) error {
	rec := notify.Record{ID: audit.NewID(time.Now()), Type: notify.RecordCall, User: user.Name, Trigger: tasks.TriggerManual,
		Start: time.Now(), Rows: -1, Integration: c.name}
	title := "Igloo: a message from " + user.Name
	if task, ok := strings.CutPrefix(sess.client, tasks.KindTask+" "); ok && sess.method == methodTask {
		rec.Name, title = task, "Igloo: a message from the task "+task
		if run, ok := tasks.RunOf(ctx); ok {
			rec.Trigger = run.Trigger
		}
	}
	in, err := s.usableIntegration(ctx, user, c.name)
	if err == nil {
		msg := notify.Message{Type: notify.RecordCall, Name: rec.Name, User: user.Name, Title: title,
			Body: []notify.Part{notify.Text(c.message)}}
		rec.Attempts, err = s.notify.Send(ctx, in, msg)
		if err != nil {
			err = sendError(c.name, err)
		}
	}
	rec.State, rec.End = notify.StateTriggered, time.Now()
	if err != nil {
		rec.State, rec.Error = notify.StateActionFailed, err.Error()
	}
	s.notify.Record(ctx, rec)
	return err
}

// --- alerts -------------------------------------------------------------

// Limits on alerts, which run often and hold a session while their query
// runs: an alert's schedule is at least minAlertInterval apart (as in
// Snowflake), and a user owns at most maxAlertsPerOwner of them.
var minAlertInterval = time.Minute

const maxAlertsPerOwner = 50

// alertDef is what an alert does: CREATE ALERT's IF ... THEN ..., parsed. It
// is stored field by field, as JSON in the job's statement, so what runs
// is exactly what was checked when the alert was created; it is never read
// from SQL text again.
type alertDef struct {
	// Condition is the query in IF (EXISTS (...)).
	Condition   string  `json:"condition"`
	Integration string  `json:"integration"`
	Message     *string `json:"message,omitempty"`
	WithResult  bool    `json:"with_result,omitempty"`
}

func alertSyntax(format string, args ...any) error {
	return pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
}

// parseAlert reads IF (EXISTS (query)) THEN NOTIFY integration [MESSAGE
// 'text'] [WITH RESULT].
func parseAlert(body string) (alertDef, error) {
	var a alertDef
	usage := alertSyntax("an alert is IF (EXISTS (query)) THEN NOTIFY integration [MESSAGE '...'] [WITH RESULT]")
	sc := &scanner{s: body}
	if !sc.keywords("IF") {
		return a, usage
	}
	outer := sc.punct('(')
	if !sc.keywords("EXISTS") || !sc.punct('(') {
		return a, usage
	}
	end, ok := closingParen(body, sc.i-1)
	if !ok {
		return a, alertSyntax("the alert's condition has unbalanced parentheses")
	}
	a.Condition = strings.TrimSpace(body[sc.i:end])
	sc.i = end + 1
	if a.Condition == "" {
		return a, alertSyntax("the alert needs a query in IF (EXISTS (...))")
	}
	if outer && !sc.punct(')') {
		return a, usage
	}
	if !sc.keywords("THEN", "NOTIFY") {
		return a, alertSyntax("expected THEN NOTIFY integration after the alert's condition")
	}
	var err error
	if a.Integration, err = integrationName(sc); err != nil {
		return a, err
	}
	for !sc.done() {
		switch {
		case a.Message == nil && sc.keywords("MESSAGE"):
			sc.punct('=')
			v, ok := sc.literal()
			if !ok || v == nil {
				return a, alertSyntax("MESSAGE takes a string")
			}
			if len(*v) > maxMessageTemplate {
				return a, pgerror.WithCode(fmt.Errorf("an alert's MESSAGE is at most %d bytes", maxMessageTemplate),
					codes.InvalidParameterValue)
			}
			a.Message = v
		case !a.WithResult && sc.keywords("WITH", "RESULT"):
			a.WithResult = true
		default:
			return a, alertSyntax("unexpected text after THEN NOTIFY %s: %q; expected MESSAGE '...' or WITH RESULT",
				a.Integration, strings.TrimSpace(sc.rest()))
		}
	}
	return a, nil
}

// maxMessageTemplate caps an alert's MESSAGE as written; what it expands
// to is capped at notify.MaxMessageBytes.
const maxMessageTemplate = 4000

// closingParen returns the index of the parenthesis that closes the one at
// s[open], looking only at SQL code.
func closingParen(s string, open int) (int, bool) {
	depth, pos := 0, open
	for _, seg := range sqllex.Segments(s[open:]) {
		if seg.Kind == sqllex.Code {
			for i := 0; i < len(seg.Text); i++ {
				switch seg.Text[i] {
				case '(':
					depth++
				case ')':
					if depth--; depth == 0 {
						return pos + i, true
					}
				}
			}
		}
		pos += len(seg.Text)
	}
	return 0, false
}

// encode is how a is stored.
func (a alertDef) encode() string {
	b, _ := json.Marshal(a)
	return string(b)
}

// decodeAlert reads what encode stored.
func decodeAlert(stored string) (alertDef, error) {
	var a alertDef
	dec := json.NewDecoder(strings.NewReader(stored))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&a); err != nil || a.Condition == "" || a.Integration == "" {
		return alertDef{}, errors.New("the stored alert is damaged; CREATE OR REPLACE it")
	}
	return a, nil
}

// String writes a as SQL, as SHOW ALERTS shows it. The condition has lines
// of its own, so a comment that ends it ends there.
func (a alertDef) String() string {
	out := "IF (EXISTS (\n" + a.Condition + "\n)) THEN NOTIFY " + a.Integration
	if a.Message != nil {
		out += " MESSAGE " + quoteLiteral(*a.Message)
	}
	if a.WithResult {
		out += " WITH RESULT"
	}
	return out
}

// alertDefinition is what SHOW ALERTS shows of a stored alert.
func alertDefinition(stored string) string {
	a, err := decodeAlert(stored)
	if err != nil {
		return stored
	}
	return a.String()
}

// errNotQuery refuses a condition that is not one query.
var errNotQuery = pgerror.WithCode(errors.New("an alert's condition is one query: IF (EXISTS (SELECT ...))"), codes.Syntax)

// oneQuery reports whether condition is one statement that reads.
func (s *Server) oneQuery(condition string) bool {
	n, err := s.engine.CountStatements(condition)
	return err == nil && n == 1 && selectLike(condition)
}

// checkAlert checks a new alert as user: its condition is one query that
// user may run, its MESSAGE names only the query's columns, user may use
// its integration, and user owns no more than maxAlertsPerOwner alerts with
// it, other than the one called name it may replace.
func (s *Server) checkAlert(ctx context.Context, sess *session, user auth.User, name string, a alertDef) error {
	if !s.oneQuery(a.Condition) {
		return errNotQuery
	}
	st, err := s.prepareChecked(ctx, sess, user, a.Condition, nil)
	if err != nil {
		return err
	}
	defer st.close() //nolint:errcheck
	if !st.onlyReads() {
		return errNotQuery
	}
	if a.Message != nil {
		for _, p := range notify.Placeholders(*a.Message) {
			if !slices.ContainsFunc(st.columns, func(c wire.Column) bool { return strings.EqualFold(c.Name, p) }) {
				return pgerror.WithCode(fmt.Errorf("the MESSAGE names {{%s}}, which is not a column of the condition's query", p),
					codes.UndefinedColumn)
			}
		}
	}
	if _, err := s.usableIntegration(ctx, user, a.Integration); err != nil {
		return err
	}
	alerts, err := s.tasks.Store().Jobs(ctx, tasks.KindAlert)
	if err != nil {
		return err
	}
	owned := 0
	for _, j := range alerts {
		if j.Owner == user.Name && !strings.EqualFold(j.Name, name) {
			owned++
		}
	}
	if owned >= maxAlertsPerOwner {
		return pgerror.WithCode(fmt.Errorf("%s owns %d alerts, the most a user may; drop some first", user.Name, owned),
			codes.ConfigurationLimitExceeded)
	}
	return nil
}

// checkAlertSchedule refuses schedules that would run an alert more often
// than every minAlertInterval.
func checkAlertSchedule(sched schedule.Schedule) error {
	if sched.IsInterval() && sched.Every < minAlertInterval {
		return pgerror.WithCode(fmt.Errorf("alerts run at most once a minute, and the schedule %q is more often", sched),
			codes.InvalidParameterValue)
	}
	return nil
}

// alertResult keeps what an alert's condition returns, as much as a
// notification shows: MaxResultColumns columns, MaxValueBytes of each
// value, rows until MaxResultRows or MaxResultBytes, and the first row's
// values of the columns the MESSAGE names.
type alertResult struct {
	named   []string
	columns []string
	// hidden counts the columns not kept.
	hidden int
	// index has the position of each column, by name in lower case.
	index map[string]int
	rows  [][]*string
	// first holds the first row's values of the named columns.
	first map[string]*string
	bytes int
	more  bool
}

func (r *alertResult) Columns(cols []Column) error {
	r.index = map[string]int{}
	for i, c := range cols {
		if _, dup := r.index[strings.ToLower(c.Name)]; !dup {
			r.index[strings.ToLower(c.Name)] = i
		}
		if i < notify.MaxResultColumns {
			r.columns = append(r.columns, c.Name)
		}
	}
	r.hidden = len(cols) - len(r.columns)
	return nil
}

// alertValue renders a value, cut to MaxValueBytes.
func alertValue(v any) *string {
	if v == nil {
		return nil
	}
	text := fmt.Sprint(v)
	if len(text) > notify.MaxValueBytes {
		cut := notify.MaxValueBytes - len("…")
		for cut > 0 && !utf8.RuneStart(text[cut]) {
			cut--
		}
		text = text[:cut] + "…"
	}
	return &text
}

func (r *alertResult) Row(values []any) error {
	if len(r.rows) == notify.MaxResultRows || r.bytes >= notify.MaxResultBytes {
		r.more = true
		return ErrStopRows
	}
	if r.first == nil {
		r.first = map[string]*string{}
		for _, name := range r.named {
			if i, ok := r.index[strings.ToLower(name)]; ok && i < len(values) {
				r.first[strings.ToLower(name)] = alertValue(values[i])
			}
		}
	}
	row := make([]*string, len(r.columns))
	for i := range row {
		if row[i] = alertValue(values[i]); row[i] != nil {
			r.bytes += len(*row[i])
		}
	}
	r.rows = append(r.rows, row)
	return nil
}

func (r *alertResult) Complete(string) error { return nil }

// runAlert runs alert j as its owner: it runs the condition in a new
// session of the owner's, with their grants and policies, and when it
// returns rows sends the notification, if the owner may still use the
// integration. The session ends before the notification goes out. A
// scheduled run leaves the notification in the integration's outbox and
// ends, so a slow destination holds no scheduler slot; EXECUTE ALERT waits
// for it. Every run is recorded in the alert history, a scheduled one once
// its notification was sent or failed. While the owner is disabled, runs
// are skipped.
func (s *Server) runAlert(ctx context.Context, j tasks.Job) (int64, error) {
	run, _ := tasks.RunOf(ctx)
	rec := notify.Record{ID: run.ID, Type: notify.RecordAlert, Name: j.Name, User: j.Owner, Trigger: run.Trigger,
		Scheduled: run.Scheduled, Start: time.Now(), Rows: -1}
	if rec.ID == "" {
		rec.ID, rec.Trigger = audit.NewID(rec.Start), tasks.TriggerManual
	}
	finish := func(state string, rows int64, err error) (int64, error) {
		rec.State, rec.Rows, rec.End = state, rows, time.Now()
		if err != nil {
			rec.Error = err.Error()
		}
		s.notify.Record(ctx, rec)
		return rows, err
	}
	if err := s.stillDue(ctx, j); err != nil {
		return finish(notify.StateSkipped, -1, err)
	}

	def, err := decodeAlert(j.Statement)
	if err != nil {
		return finish(notify.StateConditionFailed, -1, err)
	}
	if !s.oneQuery(def.Condition) {
		return finish(notify.StateConditionFailed, -1, errNotQuery)
	}
	owner, err := s.users.Get(ctx, j.Owner)
	if errors.Is(err, auth.ErrNotFound) {
		return finish(notify.StateSkipped, -1, fmt.Errorf("%w: %s", tasks.ErrOwnerGone, j.Owner))
	}
	if err != nil {
		return finish(notify.StateConditionFailed, -1, err)
	}
	if owner.Disabled {
		return finish(notify.StateSkipped, -1, fmt.Errorf("%w: the owner %s is disabled (ALTER USER %s LOGIN enables them)",
			tasks.ErrSkipped, owner.Name, owner.Name))
	}

	res := &alertResult{}
	if def.Message != nil {
		res.named = notify.Placeholders(*def.Message)
	}
	es, err := s.OpenEmbedded(ctx, owner, tasks.KindAlert+" "+j.Name)
	if err != nil {
		return finish(notify.StateConditionFailed, -1, err)
	}
	es.sess.method = methodTask
	err = es.RunQuery(ctx, def.Condition, res)
	_ = es.Close()
	if err != nil {
		if pgerror.GetCode(err) == codes.FeatureNotSupported && strings.Contains(err.Error(), "run again for a download") {
			err = errors.New("an alert's condition must be a query")
		}
		return finish(notify.StateConditionFailed, -1, err)
	}
	rows := int64(len(res.rows))
	if res.more {
		rows++
	}
	if rows == 0 {
		return finish(notify.StateConditionFalse, 0, nil)
	}

	// Its owner may have lost USAGE since the alert was created.
	rec.Integration = def.Integration
	in, err := s.usableIntegration(ctx, owner, def.Integration)
	if err != nil {
		return finish(notify.StateActionFailed, rows, err)
	}
	msg := alertMessage(j.Name, owner.Name, def, res)
	if run.Trigger == tasks.TriggerSchedule {
		s.notify.Deliver(in, msg, false, func(attempts int, err error) {
			rec.Attempts = attempts
			if err != nil {
				_, _ = finish(notify.StateActionFailed, rows, sendError(def.Integration, err))
				return
			}
			_, _ = finish(notify.StateTriggered, rows, nil)
		})
		return rows, nil
	}
	rec.Attempts, err = s.notify.Send(ctx, in, msg)
	if err != nil {
		return finish(notify.StateActionFailed, rows, sendError(def.Integration, err))
	}
	return finish(notify.StateTriggered, rows, nil)
}

// alertMessage is what alert name tells its integration about res.
func alertMessage(name, owner string, def alertDef, res *alertResult) notify.Message {
	m := notify.Message{Type: notify.RecordAlert, Name: name, User: owner, Title: "Igloo alert " + name}
	if def.Message != nil {
		var columns []string
		var values []*string
		for col, v := range res.first {
			columns, values = append(columns, col), append(values, v)
		}
		m.Body = notify.Template(*def.Message, columns, values)
	} else {
		found := fmt.Sprintf("%d rows", len(res.rows))
		switch {
		case res.more:
			found = fmt.Sprintf("more than %d rows", len(res.rows))
		case len(res.rows) == 1:
			found = "1 row"
		}
		m.Body = []notify.Part{notify.Text("The condition of the alert " + name + " found " + found + ".")}
	}
	if def.WithResult {
		m.Result = &notify.Result{Columns: res.columns, Rows: res.rows, More: res.more, HiddenColumns: res.hidden}
	}
	return m
}

// --- built-in alerts ------------------------------------------------------

// notifyLoadErrors raises the load_errors alert for files that failed to
// load, or loaded without some rows.
func (s *Server) notifyLoadErrors(table, user, pipe string, loads []tasks.Load) {
	failed, first := 0, -1
	for i, l := range loads {
		if l.Status == tasks.LoadFailed || l.Status == tasks.LoadPartial {
			failed++
			if first < 0 {
				first = i
			}
		}
	}
	if failed > 0 {
		s.notify.LoadFailed(table, user, pipe, failed, len(loads), loads[first].File, loads[first].FirstError)
	}
}

// notifyLoginsBlocked raises the failed_logins alert when the limiter
// starts refusing an address's password logins, or a user's. Clients
// choose the names they fail with, so a name's limit raises it only for a
// user who exists.
func (s *Server) notifyLoginsBlocked(name string, addr net.Addr, user, address bool) {
	if address {
		s.notify.LoginsBlocked(addrKey(addr), false, addrFailures, failureWindow)
	}
	if !user {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := s.users.Get(ctx, name); err == nil {
		s.notify.LoginsBlocked(name, true, userFailures, failureWindow)
	}
}

// RunningStatements lists the statements running on this replica, without
// their secrets, for the long query alert.
func (s *Server) RunningStatements() []notify.Statement {
	var out []notify.Statement
	for _, si := range s.Sessions() {
		if si.Statement != "" {
			out = append(out, notify.Statement{PID: si.PID, User: si.User, Text: redactQuery(si.Statement), Started: si.Started})
		}
	}
	return out
}

// forgetGrantee drops the integration and model grants of a dropped user
// or role.
func (s *Server) forgetGrantee(granteeType, name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.notify.Store().ForgetGrantee(ctx, granteeType, name); err != nil {
		s.log.Warn("could not remove the integration grants of a dropped "+granteeType, granteeType, name, "err", err)
	}
	if err := s.ai.Store().ForgetGrantee(ctx, granteeType, name); err != nil {
		s.log.Warn("could not remove the AI integration grants of a dropped "+granteeType, granteeType, name, "err", err)
	}
	if err := s.models.ForgetGrantee(ctx, granteeType, name); err != nil {
		s.log.Warn("could not remove the model grants of a dropped "+granteeType, granteeType, name, "err", err)
	}
}
