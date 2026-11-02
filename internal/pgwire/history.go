package pgwire

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/duckdb/duckdb-go/v2"
	wire "github.com/jeroenrinzema/psql-wire"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/ai"
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
	// read and locations name what a command Igloo carries out itself
	// read: catalog tables, and locations outside the catalog.
	read      []auth.Object
	locations []string
	// analyze is the statement, of type stmtType, as the session prepared
	// it, when what it read and wrote is to be worked out from it (see
	// objects.go).
	analyze  string
	stmtType duckdb.StmtType
	// policies names the policies that applied.
	policies []string
	// used is what DuckDB measured of the statement, and queued how long
	// it waited for its user's limits.
	used   *audit.Resources
	queued time.Duration
	// catWait is how long its steps waited for the catalog lock (see
	// relockCatalog); recordQuery takes it from the session.
	catWait time.Duration
	// parts is what running the statement in parts on workers took, and
	// shape its key for the workload controller (see shapeKey), if any.
	parts distParts
	shape string
	// exact is the same with its literals (see workload.Controller.Alike).
	exact string
	// form is what its FLATTENs took, if they could take either.
	form statementForm
	// peak is the most memory it was seen holding, if sampled (see
	// workload.Controller.SampleMemory); zero otherwise.
	peak int64
	// ai is what its AI functions did, if it called any.
	ai *ai.Spend
}

// holdRecords keeps what recordQuery is given for sess from the history
// until the returned function is called, which returns it, for a statement
// Igloo may try twice (see parseTranslated).
func (sess *session) holdRecords() func() []queryRecord {
	var held []queryRecord
	sess.held = &held
	return func() []queryRecord {
		sess.held = nil
		return held
	}
}

// recordAll records each of recs.
func (s *Server) recordAll(sess *session, recs []queryRecord) {
	for _, r := range recs {
		s.recordQuery(sess, r)
	}
}

// runMemory is the memory a statement that ran on workers workers, on the
// large worker if large, or on this replica if neither, ran with (see
// distributed.Coordinator.RunMemoryOn).
func (s *Server) runMemory(workers int, large bool) int64 {
	if s.dist != nil {
		return s.dist.RunMemoryOn(workers, large)
	}
	if workers == 0 {
		return s.queryMemory
	}
	return 0
}

// recordQuery sends a statement to the history.
func (s *Server) recordQuery(sess *session, r queryRecord) {
	if s.audit == nil || sess == nil {
		return
	}
	// What its steps waited for the catalog lock since the session's last
	// statement was recorded is this one's; a record held and recorded
	// again (see holdRecords) has it already.
	if r.catWait == 0 {
		r.catWait, sess.catWait = sess.catWait, 0
	}
	if sess.held != nil {
		*sess.held = append(*sess.held, r)
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
		CatalogWait:     r.catWait,
		Resources:       r.used,
		Workers:         r.parts.workers,
		Stages:          r.parts.stages,
		Exchanges:       r.parts.exchanges,
		PlanKey:         r.parts.key,
		Placement:       r.parts.placement,
		FlattenKey:      r.form.key,
		FlattenLists:    r.form.lists,
		Shape:           r.shape,
		Exact:           r.exact,
		Memory:          s.runMemory(r.parts.workers, r.parts.large),
		Peak:            r.peak,
	}
	if sp := r.ai; sp != nil {
		q.AI = &audit.AIUsage{Calls: sp.Calls, CacheHits: sp.CacheHits, InputTokens: sp.InputTokens, OutputTokens: sp.OutputTokens,
			CostUSD: sp.CostUSD, Models: sp.Models}
	}
	if r.err != nil && q.Outcome != audit.OutcomeOK {
		q.SQLState = string(pgerror.GetCode(pgError(r.err)))
		q.Rows = -1
	}
	q.Policies = r.policies
	q.Job = sess.job
	known := statementObjects{read: r.read, written: r.written, locations: r.locations}
	readKnown := r.read != nil
	if r.acc != nil {
		known.written = r.acc.written
		// For users with the role none, grants were checked for what the
		// statement reads.
		known.read, readKnown = r.acc.read, user.Role == auth.RoleNone
	}
	if r.analyze != "" && !readKnown && q.Outcome == audit.OutcomeOK && s.analyzer != nil && s.catalogName() != "" &&
		s.analyzer.worthwhile(r.stmtType, r.analyze) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		names, err := s.names(ctx, sess)
		cancel()
		if err == nil && s.analyzer.add(analysis{q: &q, text: r.analyze, kind: r.stmtType, names: names, known: known, readKnown: readKnown}) {
			return
		}
	}
	s.finishRecord(&q, known.read, known.written, known.locations)
}

// finishRecord writes q to the history with what it read and wrote, tells
// the lineage sink about it, and has the metadata of the tables it wrote
// read again.
func (s *Server) finishRecord(q *audit.Query, read, written []auth.Object, locations []string) {
	q.TablesRead = objectNames(s.catalogName(), read)
	q.TablesWritten = objectNames(s.catalogName(), written)
	if external := s.gov.external; external != "" {
		// External tables' files are served by Igloo's proxy under a
		// secret prefix, which the history, the lineage and lineage
		// events must not show; reads of them through their tables are
		// recorded as reads of the tables.
		locations = slices.DeleteFunc(slices.Clone(locations), func(l string) bool {
			return strings.HasPrefix(strings.ToLower(l), external)
		})
		if len(locations) == 0 {
			locations = nil
		}
	}
	q.LocationsRead = locations
	s.audit.RecordQuery(*q)
	if q.Outcome != audit.OutcomeOK || len(written) == 0 {
		return
	}
	if s.lineage != nil {
		s.lineage.Statement(*q)
	}
	if audit.ChangesData(q.StatementType) {
		s.metadataChanged(written)
		s.indexesWritten(written)
	}
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
// when it runs, and refused in a failed transaction block.
func (s *Server) audited(sess *session, text string, fn wire.PreparedStatementFn) wire.PreparedStatementFn {
	return func(ctx context.Context, w wire.DataWriter, params []wire.Parameter) error {
		tw := &tagWriter{DataWriter: w}
		start := time.Now()
		err := sess.checkAborted(text)
		if err == nil {
			err = fn(ctx, tw, params)
		}
		s.recordQuery(sess, queryRecord{text: text, start: start, err: err, rows: tw.rows()})
		return err
	}
}

// tagWriter remembers the command tag a statement completes with, and the
// rows it wrote if the tag does not count them (see rowsTaker).
type tagWriter struct {
	wire.DataWriter
	tag string
	// wrote is the rows a CREATE TABLE ... AS wrote, if took.
	wrote int64
	took  bool
	// gate, while a guard watches the statement's run (see guardLocal),
	// keeps its first output and the guard from both happening.
	gate *rowGate
	// fill, if set, takes the rows for the result cache (see
	// resultcache.go).
	fill *resultFill
}

// rowGate keeps a statement's first output to the client and its guard's
// cancel (see guardLocal) from both happening: once output has begun, the
// guard does not fire, and once it fired, no output follows.
type rowGate struct {
	mu           sync.Mutex
	begun, fired bool
}

// errGuardFired is what output fails with after the guard fired.
var errGuardFired = errors.New("the statement is being run on workers instead")

func (g *rowGate) begin() error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.fired {
		return errGuardFired
	}
	g.begun = true
	return nil
}

func (g *rowGate) fire() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.begun {
		return false
	}
	g.fired = true
	return true
}

func (w *tagWriter) Row(values []any) error {
	if err := w.gate.begin(); err != nil {
		return err
	}
	w.fill.add(values)
	return w.DataWriter.Row(values)
}

func (w *tagWriter) Empty() error {
	if err := w.gate.begin(); err != nil {
		return err
	}
	return w.DataWriter.Empty()
}

func (w *tagWriter) tookRows(n int64) {
	w.wrote, w.took = n, true
	reportRows(w.DataWriter, n)
}

func (w *tagWriter) Complete(tag string) error {
	if err := w.gate.begin(); err != nil {
		return err
	}
	w.tag = tag
	if w.fill != nil {
		w.fill.tag = tag
	}
	return w.DataWriter.Complete(tag)
}

// Notice passes a warning on to the client, if its writer sends them.
func (w *tagWriter) Notice(code, message string) error {
	if n, ok := w.DataWriter.(wire.Noticer); ok {
		return n.Notice(code, message)
	}
	return nil
}

func (w *tagWriter) Rows(rows [][]any) error {
	if err := w.gate.begin(); err != nil {
		return err
	}
	for _, r := range rows {
		w.fill.add(r)
	}
	return wire.WriteRows(w.DataWriter, rows)
}

func (w *tagWriter) DefineColumns(columns wire.Columns) error {
	return wire.DefineColumns(w.DataWriter, columns)
}

// rows is the row count in the tag, such as 5 in "INSERT 0 5", or the rows
// a CREATE TABLE ... AS wrote, or the rows sent before a client stopped
// reading, or -1.
func (w *tagWriter) rows() int64 {
	if w.took {
		return w.wrote
	}
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
// notification integration's URL its webhook's token, and CREATE or ALTER
// CONNECTION a Snowflake user's private key.
var secretWords = []string{"PASSWORD", "SECRET", "KEY_ID", "SESSION_TOKEN", "ACCOUNT_KEY",
	"CONNECTION_STRING", "CLIENT_SECRET", "TOKEN", "CREDENTIAL", "CREDENTIALS", "PROVIDER", "INTEGRATION",
	"PRIVATE_KEY"}

// sensitiveSetting matches settings whose values are credentials, such as
// s3_secret_access_key.
var sensitiveSetting = regexp.MustCompile(`(?i)(secret|password|key|token|credential)`)

// redactQuery returns query fit for the history: passwords and secrets
// replaced by '***', as are the identifiers of ERASE SUBJECT, and at most
// maxQueryText bytes.
func redactQuery(query string) string {
	words := sqllex.CodeWords(query)
	switch {
	case len(words) > 0 && words[0] == "ERASE":
		query = erasureText(query)
	case mentionsErasure(words):
		return erasureHidden
	}
	sensitive := slices.ContainsFunc(words, func(w string) bool { return slices.Contains(secretWords, w) || w == "ERASE" })
	// A connection's options can be credentials under any name, such as
	// an Iceberg REST catalog's storage_credential.
	sensitive = sensitive || len(words) > 1 && (words[0] == "CREATE" || words[0] == "ALTER") && words[1] == "CONNECTION"
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
