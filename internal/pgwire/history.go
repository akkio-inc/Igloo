package pgwire

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	wire "github.com/jeroenrinzema/psql-wire"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Query and login history: every statement a session runs or tries to,
// and every login attempt, go to the audit log (package audit), which
// igloo.query_history and igloo.login_history read back.

// HistoryViewer is the engine.HistoryViewer for Igloo's sessions: a
// statement prepared by a session shows that session's user their own
// history, or everyone's if they are an admin when it runs. Statements from
// outside a session see nothing.
func HistoryViewer(ctx context.Context) func() (audit.Viewer, bool) {
	sess := sessionFrom(ctx)
	if sess == nil {
		return nil
	}
	return func() (audit.Viewer, bool) {
		u := sess.currentUser()
		return audit.Viewer{User: u.Name, All: u.Role == auth.RoleAdmin}, u.Name != ""
	}
}

// maxQueryText bounds the statement text kept in the history.
const maxQueryText = 100_000

// queryRecord describes one statement for the history.
type queryRecord struct {
	text  string
	start time.Time
	err   error
	// rows is how many rows the statement returned or changed, or -1.
	rows int64
	// acc is what the statement touched, when grants were checked.
	acc *access
	// written names what catalog DDL changed.
	written []auth.Object
	// policies names the policies that applied.
	policies []string
	// used is what DuckDB measured of the statement, and queued how long
	// it waited for its user's limits.
	used   *audit.Resources
	queued time.Duration
}

// recordQuery sends a statement to the history.
func (s *Server) recordQuery(sess *session, r queryRecord) {
	if s.audit == nil || sess == nil {
		return
	}
	user := sess.currentUser()
	q := audit.Query{
		SessionID:       sess.sessionID(),
		User:            user.Name,
		Role:            string(user.Role),
		Start:           r.start,
		Duration:        time.Since(r.start),
		Outcome:         queryOutcome(r.err),
		Rows:            r.rows,
		StatementType:   statementKind(r.text),
		Text:            redactQuery(r.text),
		ClientAddress:   sess.client,
		ApplicationName: sess.applicationName(),
		QueryTag:        sess.queryTag(),
		Pool:            s.pool,
		Queued:          r.queued,
		Resources:       r.used,
	}
	if r.err != nil && q.Outcome != audit.OutcomeOK {
		q.SQLState = string(pgerror.GetCode(pgError(r.err)))
		q.Rows = -1
	}
	if r.acc != nil {
		q.TablesRead = objectNames(s.catalogName(), r.acc.read)
		q.TablesWritten = objectNames(s.catalogName(), r.acc.written)
	}
	if r.written != nil {
		q.TablesWritten = objectNames(s.catalogName(), r.written)
	}
	q.Policies = r.policies
	s.audit.RecordQuery(q)
}

// recordLogin sends a login attempt from addr, which may be nil, to the
// history. A failed one has a reason; a successful one may have opened
// sess.
func (s *Server) recordLogin(user string, addr net.Addr, appName string, sess *session, reason string, how login) {
	if s.audit == nil {
		return
	}
	x := audit.Login{
		Time:            time.Now(),
		User:            user,
		Result:          audit.LoginSuccess,
		Reason:          reason,
		ApplicationName: appName,
		Method:          how.method,
		Token:           how.token,
	}
	if addr != nil {
		if a, ok := addr.(*net.TCPAddr); !ok || a != nil {
			x.ClientAddress = addr.String()
		}
	}
	if reason != "" {
		x.Result = audit.LoginFailure
	}
	if sess != nil {
		x.SessionID = sess.sessionID()
	}
	s.audit.RecordLogin(x)
}

// audited wraps a statement Igloo carries out itself so it is recorded
// when it runs.
func (s *Server) audited(sess *session, text string, fn wire.PreparedStatementFn) wire.PreparedStatementFn {
	if s.audit == nil {
		return fn
	}
	return func(ctx context.Context, w wire.DataWriter, params []wire.Parameter) error {
		tw := &tagWriter{DataWriter: w}
		start := time.Now()
		err := fn(ctx, tw, params)
		s.recordQuery(sess, queryRecord{text: text, start: start, err: err, rows: tw.rows()})
		return err
	}
}

// tagWriter remembers the command tag a statement completes with.
type tagWriter struct {
	wire.DataWriter
	tag string
}

func (w *tagWriter) Complete(tag string) error {
	w.tag = tag
	return w.DataWriter.Complete(tag)
}

// Notice passes a warning on to the client, if its writer sends them.
func (w *tagWriter) Notice(code, message string) error {
	if n, ok := w.DataWriter.(wire.Noticer); ok {
		return n.Notice(code, message)
	}
	return nil
}

// rows is the row count in the tag, such as 5 in "INSERT 0 5", or the rows
// sent before a client stopped reading, or -1.
func (w *tagWriter) rows() int64 {
	if n := tagRows(w.tag); n >= 0 || w.tag != "" || w.Written() == 0 {
		return n
	}
	return int64(w.Written())
}

func tagRows(tag string) int64 {
	fields := strings.Fields(tag)
	if len(fields) < 2 {
		return -1
	}
	switch fields[0] {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "COPY", "FETCH", "MERGE", "MOVE":
		n, err := strconv.ParseInt(fields[len(fields)-1], 10, 64)
		if err == nil {
			return n
		}
	}
	return -1
}

// statementKind names a statement for the history: its command, such as
// SELECT, CREATE TABLE or SHOW GRANTS.
func statementKind(query string) string {
	words := keywords(query, 2)
	if len(words) == 0 {
		return ""
	}
	switch words[0] {
	case "CREATE", "DROP", "ALTER":
		if c, ok, err := parsePolicyCommand(query); ok && err == nil {
			// CREATE MASKING POLICY, not CREATE MASKING.
			return c.tag
		}
		if c, ok, err := parseIntegrationCommand(query); ok && err == nil {
			return c.tag()
		}
		return ddlTag(query)
	case "SHOW":
		return strings.Join(words, " ")
	}
	return words[0]
}

// secretWords mark statements that can carry passwords or credentials.
// Their string literals are kept out of the history, as is the word after
// PASSWORD, which a malformed statement may leave unquoted. CREATE PROVIDER
// carries a Delta Sharing profile file, with its bearer token, and a
// notification integration's URL its webhook's token.
var secretWords = []string{"PASSWORD", "SECRET", "KEY_ID", "SESSION_TOKEN", "ACCOUNT_KEY",
	"CONNECTION_STRING", "CLIENT_SECRET", "TOKEN", "CREDENTIAL", "CREDENTIALS", "PROVIDER", "INTEGRATION"}

// sensitiveSetting matches settings whose values are credentials, such as
// s3_secret_access_key.
var sensitiveSetting = regexp.MustCompile(`(?i)(secret|password|key|token|credential)`)

// redactQuery returns query fit for the history: passwords and secrets
// replaced by '***', and at most maxQueryText bytes.
func redactQuery(query string) string {
	words := sqllex.CodeWords(query)
	sensitive := slices.ContainsFunc(words, func(w string) bool { return slices.Contains(secretWords, w) })
	if !sensitive && len(words) > 1 && (words[0] == "SET" || words[0] == "RESET") {
		sensitive = slices.ContainsFunc(words[1:min(len(words), 4)], func(w string) bool { return sensitiveSetting.MatchString(w) })
	}
	if sensitive {
		var b strings.Builder
		afterPassword := false
		// A profile file pasted without quotes holds its token in a
		// "quoted identifier".
		provider := slices.Contains(words, "PROVIDER")
		for _, seg := range sqllex.Segments(query) {
			switch {
			case seg.Kind == sqllex.String:
				seg.Text, afterPassword = "'***'", false
			case seg.Kind == sqllex.Ident && (afterPassword || provider):
				seg.Text, afterPassword = "***", false
			case seg.Kind == sqllex.Code:
				seg.Text, afterPassword = redactUnquoted(seg.Text, afterPassword)
			}
			b.WriteString(seg.Text)
		}
		query = b.String()
	}
	if len(query) > maxQueryText {
		cut := maxQueryText
		for cut > 0 && !utf8.RuneStart(query[cut]) {
			cut--
		}
		query = query[:cut]
	}
	return query
}

var passwordWord = regexp.MustCompile(`(?i)\bPASSWORD\b`)

// redactUnquoted replaces whatever follows PASSWORD in a stretch of code,
// which can only be an unquoted password, with ***. pending says a
// PASSWORD ended the previous stretch; it is passed on when this one ends
// with one too.
func redactUnquoted(code string, pending bool) (string, bool) {
	if pending && strings.TrimSpace(code) != "" {
		return "***", false
	}
	loc := passwordWord.FindStringIndex(code)
	if loc == nil {
		return code, pending
	}
	if strings.TrimSpace(code[loc[1]:]) == "" {
		return code, true
	}
	return code[:loc[1]] + " ***", false
}

// objectNames names objects in full: catalog.schema.table, where a
// connection's objects name their connection.
func objectNames(catalog string, objs []auth.Object) []string {
	if objs == nil {
		return nil
	}
	out := make([]string, len(objs))
	for i, o := range objs {
		if o.Catalog != "" {
			out[i] = o.String()
		} else {
			out[i] = catalog + "." + o.String()
		}
	}
	return out
}

// sessionID returns the session's ID, making one if needed.
func (sess *session) sessionID() string {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.id == "" {
		var b [8]byte
		_, _ = rand.Read(b[:])
		sess.id = hex.EncodeToString(b[:])
	}
	return sess.id
}

func (sess *session) applicationName() string {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.appName
}

func (sess *session) setApplicationName(name string) {
	sess.mu.Lock()
	sess.appName = name
	sess.mu.Unlock()
}

// setApplicationName matches SET application_name, which clients send to
// name themselves.
var setApplicationName = regexp.MustCompile(`(?is)^\s*SET\s+(?:SESSION\s+|LOCAL\s+)?"?application_name"?\s*(?:=|\bTO\b)\s*(.*?)\s*;?\s*$`)

// applicationNameValue returns the value a SET application_name sets.
func applicationNameValue(query string) (string, bool) {
	m := setApplicationName.FindStringSubmatch(stripComments(query))
	if m == nil {
		return "", false
	}
	v := m[1]
	if len(v) >= 2 && (v[0] == '\'' || v[0] == '"') && v[len(v)-1] == v[0] {
		v = strings.ReplaceAll(v[1:len(v)-1], v[:1]+v[:1], v[:1])
	}
	return v, true
}
