package federation

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/jackc/pgx/v5/pgproto3"
)

// The gateway between DuckDB and Snowflake.
//
// DuckDB attaches a Snowflake connection as a PostgreSQL database, with its
// postgres extension, at the gateway's port on the loopback interface,
// signing in with the proxies' random token. The gateway is not a
// PostgreSQL server: it answers exactly the statements the extension sends,
// and nothing else:
//
//   - transactions, the pool's health checks and resets, which need no
//     answer from Snowflake;
//   - the extension's queries of pg_catalog, from the list of schemas,
//     tables and columns the gateway read from Snowflake when the
//     connection was attached (sfcatalog.go), so listing tables never waits
//     on Snowflake and works while it is down;
//   - scans, COPY (SELECT ...) TO STDOUT, which it turns into a Snowflake
//     query with the same columns, filters and limit (sfscan.go), runs with
//     Snowflake's Go driver, and streams back as binary COPY data
//     (sfcopy.go), a batch of Snowflake's Arrow results at a time, while
//     the next few download.
//
// Doing so through the postgres extension, rather than a table function of
// Igloo's, gives Snowflake's tables everything a PostgreSQL connection's
// have: they are DuckDB tables of a catalog (information_schema, psql's \dt,
// views), DuckDB pushes filters and limits into their scans, and the plan
// names each scan's table (POSTGRES_SCAN), which the grant checks of
// package pgwire rely on.

// gatewayVersion is the server version the gateway reports.
const gatewayVersion = "17.0"

// scanPrefetch is how many result batches download ahead of the one being
// sent to DuckDB.
const scanPrefetch = 8

// sfGateway serves DuckDB's sessions with one Snowflake connection.
type sfGateway struct {
	name  string
	ln    net.Listener
	token string
	// attached is the connection as DuckDB attached it; its database and
	// schemas stay as they were until a restart.
	attached Connection
	secrets  string
	log      *slog.Logger
	// ctx ends when the gateway closes, ending its scans.
	ctx  context.Context
	stop context.CancelFunc
	// failures and lastErr are as the proxy's (see proxy.signedIn).
	failures atomic.Int32
	lastErr  atomic.Pointer[string]

	mu       sync.Mutex
	current  Connection
	client   sfBackend
	catalog  *sfCatalog
	sessions map[*sfSession]bool
	closed   bool
}

// newSnowflakeGateway listens for DuckDB's sessions for c.
func newSnowflakeGateway(c Connection, token, secrets string, log *slog.Logger) (*sfGateway, error) {
	ln, err := listenLoopback()
	if err != nil {
		return nil, fmt.Errorf("connection %s: listen for DuckDB: %w", c.Name, err)
	}
	ctx, stop := context.WithCancel(context.Background())
	g := &sfGateway{
		name: c.Name, ln: ln, token: token, attached: c, current: c, secrets: secrets,
		log: log.With("connection", c.Name), ctx: ctx, stop: stop, sessions: map[*sfSession]bool{},
	}
	go g.serve()
	return g, nil
}

func (g *sfGateway) port() int { return g.ln.Addr().(*net.TCPAddr).Port }

func (g *sfGateway) attachedConn() Connection { return g.attached }

// settings returns the connection's current settings, with its attached
// database and schemas.
func (g *sfGateway) settings() Connection {
	g.mu.Lock()
	defer g.mu.Unlock()
	c := g.current
	c.Type, c.Database, c.Schemas = g.attached.Type, g.attached.Database, g.attached.Schemas
	return c
}

// snowflake returns the pool of Snowflake sessions with the current
// settings.
func (g *sfGateway) snowflake() sfBackend {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.client == nil {
		c := g.current
		c.Type, c.Database, c.Schemas = g.attached.Type, g.attached.Database, g.attached.Schemas
		g.client = openSnowflake(c, g.secrets)
	}
	return g.client
}

// probe signs in to Snowflake and reads the database's tables, which the
// gateway then lists to DuckDB.
func (g *sfGateway) probe() error { return g.refresh(g.ctx) }

// refresh reads the database's tables again. On failure the gateway keeps
// the list it has.
func (g *sfGateway) refresh(ctx context.Context) error {
	c := g.settings()
	ctx, cancel := context.WithTimeout(ctx, c.ConnectTimeout+catalogTimeout(c))
	defer cancel()
	cl := g.snowflake()
	if err := cl.check(ctx); err != nil {
		g.signedIn(err)
		return err
	}
	g.signedIn(nil)
	cat, err := cl.loadCatalog(ctx, g.log.Warn)
	if err != nil {
		return err
	}
	g.mu.Lock()
	g.catalog = cat
	g.mu.Unlock()
	return nil
}

// catalogTimeout bounds reading the list of tables: query_timeout, or, for
// none, five minutes.
func catalogTimeout(c Connection) time.Duration {
	if c.QueryTimeout > 0 {
		return c.QueryTimeout
	}
	return defaultQueryTimeout
}

// update applies new settings: sessions with Snowflake signed in with the
// old ones finish what they are doing and are not reused.
func (g *sfGateway) update(c Connection) {
	g.mu.Lock()
	g.current = c
	old := g.client
	g.client = nil
	g.mu.Unlock()
	g.failures.Store(0)
	if old != nil {
		// Closing waits for the queries running on it.
		go old.close()
	}
}

// close stops the gateway, its scans and its sessions with Snowflake.
func (g *sfGateway) close() {
	g.mu.Lock()
	g.closed = true
	sessions := g.sessions
	g.sessions = map[*sfSession]bool{}
	client := g.client
	g.client = nil
	g.mu.Unlock()
	g.stop()
	_ = g.ln.Close()
	for s := range sessions {
		_ = s.conn.Close()
	}
	if client != nil {
		go client.close()
	}
}

func (g *sfGateway) signedIn(err error) {
	if err == nil {
		g.lastErr.Store(nil)
		if g.failures.Swap(0) > 0 {
			g.log.Info("Snowflake reachable again")
		}
		return
	}
	msg := Scrub(err.Error())
	g.lastErr.Store(&msg)
	if g.failures.Add(1) == 1 {
		g.log.Warn("cannot sign in to Snowflake", "err", msg)
	}
}

func (g *sfGateway) lastError() string {
	if e := g.lastErr.Load(); e != nil {
		return *e
	}
	return ""
}

func (g *sfGateway) currentCatalog() *sfCatalog {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.catalog
}

func (g *sfGateway) serve() {
	for {
		conn, err := g.ln.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				g.log.Warn("gateway stopped accepting connections", "err", err)
			}
			return
		}
		go g.handle(conn)
	}
}

// handle signs DuckDB in with the token and runs its session.
func (g *sfGateway) handle(conn net.Conn) {
	defer conn.Close() //nolint:errcheck
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	be := pgproto3.NewBackend(conn, conn)
	var startup *pgproto3.StartupMessage
	for startup == nil {
		msg, err := be.ReceiveStartupMessage()
		if err != nil {
			return
		}
		switch m := msg.(type) {
		case *pgproto3.SSLRequest, *pgproto3.GSSEncRequest:
			if _, err := conn.Write([]byte{'N'}); err != nil {
				return
			}
		case *pgproto3.CancelRequest:
			g.cancelRequest(m.ProcessID, m.SecretKey)
			return
		case *pgproto3.StartupMessage:
			startup = m
		default:
			return
		}
	}
	be.Send(&pgproto3.AuthenticationCleartextPassword{})
	if be.Flush() != nil || be.SetAuthType(pgproto3.AuthTypeCleartextPassword) != nil {
		return
	}
	msg, err := be.Receive()
	if err != nil {
		return
	}
	if pw, ok := msg.(*pgproto3.PasswordMessage); !ok || subtle.ConstantTimeCompare([]byte(pw.Password), []byte(g.token)) != 1 {
		be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: "28P01",
			Message: "connection " + g.name + ": wrong gateway token"})
		_ = be.Flush()
		return
	}
	var key [8]byte
	_, _ = rand.Read(key[:])
	s := &sfSession{
		g: g, conn: conn, out: bufio.NewWriterSize(conn, 256<<10),
		pid: binary.BigEndian.Uint32(key[:4]) &^ (1 << 31), key: key[4:],
	}
	if startup.ProtocolVersion != pgproto3.ProtocolVersion30 {
		be.Send(&pgproto3.NegotiateProtocolVersion{NewestMinorProtocol: 0})
	}
	be.Send(&pgproto3.AuthenticationOk{})
	for _, p := range [][2]string{
		{"server_version", gatewayVersion}, {"server_encoding", "UTF8"}, {"client_encoding", "UTF8"},
		{"DateStyle", "ISO, MDY"}, {"integer_datetimes", "on"}, {"standard_conforming_strings", "on"},
		{"TimeZone", "UTC"}, {"IntervalStyle", "postgres"}, {"is_superuser", "off"},
	} {
		be.Send(&pgproto3.ParameterStatus{Name: p[0], Value: p[1]})
	}
	be.Send(&pgproto3.BackendKeyData{ProcessID: s.pid, SecretKey: s.key})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if be.Flush() != nil {
		return
	}
	_ = conn.SetDeadline(time.Time{})

	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	g.sessions[s] = true
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		delete(g.sessions, s)
		g.mu.Unlock()
	}()
	s.run()
}

// cancelRequest cancels the scan of the session with that key, as
// PostgreSQL cancels a backend's statement.
func (g *sfGateway) cancelRequest(pid uint32, key []byte) {
	g.mu.Lock()
	var target *sfSession
	for s := range g.sessions {
		if s.pid == pid && string(s.key) == string(key) {
			target = s
		}
	}
	g.mu.Unlock()
	if target != nil {
		target.cancelScan()
	}
}

// sfSession is one DuckDB connection to the gateway.
type sfSession struct {
	g    *sfGateway
	conn net.Conn
	out  *bufio.Writer
	pid  uint32
	key  []byte

	// These belong to run.
	inTx, failed bool
	// skipping is set after a message of the extended protocol, which the
	// gateway does not serve, until DuckDB's Sync.
	skipping bool

	mu sync.Mutex
	// cancel ends the scan running, if any.
	cancel context.CancelFunc
}

// pgMessage is a message from DuckDB.
type pgMessage struct {
	typ  byte
	body []byte
}

// run serves DuckDB's messages until it goes. Messages are read by a
// goroutine of their own, so a session DuckDB closes in the middle of a
// scan ends the scan, and its Snowflake query, at once.
func (s *sfSession) run() {
	ctx, cancel := context.WithCancel(s.g.ctx)
	defer cancel()
	msgs := make(chan pgMessage)
	go func() {
		defer close(msgs)
		defer cancel()
		in := bufio.NewReaderSize(s.conn, 64<<10)
		for {
			typ, msg, err := readPGMessage(in)
			if err != nil {
				return
			}
			select {
			case msgs <- pgMessage{typ, msg[5:]}:
			case <-ctx.Done():
				return
			}
		}
	}()
	for m := range msgs {
		switch m.typ {
		case 'X':
			return
		case 'Q':
			s.simpleQuery(ctx, strings.TrimRight(string(m.body), "\x00"))
		case 'S':
			s.skipping = false
			s.send(&pgproto3.ReadyForQuery{TxStatus: s.status()})
		case 'H':
		default:
			if !s.skipping {
				s.skipping = true
				s.send(gatewayError("0A000", "the extended query protocol is not supported"))
			}
		}
		if s.flush() != nil {
			return
		}
	}
}

// status is the session's transaction status for ReadyForQuery.
func (s *sfSession) status() byte {
	switch {
	case s.failed:
		return 'E'
	case s.inTx:
		return 'T'
	}
	return 'I'
}

func (s *sfSession) send(msgs ...pgproto3.BackendMessage) {
	var buf []byte
	for _, m := range msgs {
		buf, _ = m.Encode(buf)
	}
	_, _ = s.out.Write(buf)
}

func (s *sfSession) flush() error { return s.out.Flush() }

// gatewayError is an error for DuckDB.
func gatewayError(code, msg string) *pgproto3.ErrorResponse {
	return &pgproto3.ErrorResponse{Severity: "ERROR", SeverityUnlocalized: "ERROR", Code: code, Message: Scrub(msg)}
}

// stmtError is a statement's failure, with its SQLSTATE.
type stmtError struct {
	code string
	err  error
}

func (e *stmtError) Error() string { return e.err.Error() }

func failf(code, format string, args ...any) error {
	return &stmtError{code: code, err: fmt.Errorf(format, args...)}
}

// simpleQuery runs the statements of a simple query, as PostgreSQL does:
// up to the first that fails, inside the session's transaction if one is
// open.
func (s *sfSession) simpleQuery(ctx context.Context, text string) {
	stmts := splitStatements(text)
	if len(stmts) == 0 {
		s.send(&pgproto3.EmptyQueryResponse{})
	}
	for _, st := range stmts {
		if err := s.statement(ctx, st); err != nil {
			code := "XX000"
			var se *stmtError
			if errors.As(err, &se) {
				code = se.code
			}
			s.send(gatewayError(code, "connection "+s.g.name+": "+err.Error()))
			if s.inTx {
				s.failed = true
			}
			break
		}
	}
	s.send(&pgproto3.ReadyForQuery{TxStatus: s.status()})
}

// splitStatements splits a simple query at semicolons outside quotes.
func splitStatements(text string) []string {
	var out []string
	start := 0
	var quote byte
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == ';':
			out = append(out, text[start:i])
			start = i + 1
		}
	}
	out = append(out, text[start:])
	return slices.DeleteFunc(out, func(s string) bool { return strings.TrimSpace(s) == "" })
}

var spaces = regexp.MustCompile(`\s+`)

func (s *sfSession) complete(tag string) {
	s.send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
}

// statement runs one statement of the extension's.
func (s *sfSession) statement(ctx context.Context, st string) error {
	st = strings.TrimSpace(st)
	u := strings.ToUpper(spaces.ReplaceAllString(st, " "))
	if s.failed && !strings.HasPrefix(u, "ROLLBACK") && !strings.HasPrefix(u, "ABORT") && !strings.HasPrefix(u, "COMMIT") && u != "END" {
		return failf("25P02", "current transaction is aborted, commands ignored until end of transaction block")
	}
	switch {
	case strings.HasPrefix(u, "BEGIN") || strings.HasPrefix(u, "START TRANSACTION"):
		s.inTx = true
		s.complete("BEGIN")
	case strings.HasPrefix(u, "COMMIT") || u == "END":
		tag := "COMMIT"
		if s.failed {
			tag = "ROLLBACK"
		}
		s.inTx, s.failed = false, false
		s.complete(tag)
	case strings.HasPrefix(u, "ROLLBACK") || strings.HasPrefix(u, "ABORT"):
		s.inTx, s.failed = false, false
		s.complete("ROLLBACK")
	case u == "DISCARD ALL":
		s.complete("DISCARD ALL")
	case strings.HasPrefix(u, "SET ") || strings.HasPrefix(u, "RESET "):
		// statement_timeout and the like, or a transaction's snapshot: the
		// gateway's own limits apply instead.
		s.complete(strings.Fields(u)[0])
	case strings.HasPrefix(u, "COPY ("):
		return s.copyOut(ctx, st)
	case strings.HasPrefix(u, "SELECT "):
		return s.query(st, u)
	default:
		return failf("0A000", "statement not supported by Igloo's Snowflake gateway")
	}
	return nil
}

// Columns of the gateway's answers, as text. The extension reads them by
// position, as text.
func textColumns(names ...string) *pgproto3.RowDescription {
	d := &pgproto3.RowDescription{}
	for _, n := range names {
		d.Fields = append(d.Fields, pgproto3.FieldDescription{Name: []byte(n), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1})
	}
	return d
}

// row sends one row of text values; nil is NULL.
func (s *sfSession) row(vals ...*string) {
	r := &pgproto3.DataRow{Values: make([][]byte, len(vals))}
	for i, v := range vals {
		if v != nil {
			r.Values[i] = []byte(*v)
		}
	}
	s.send(r)
}

func str(s string) *string { return &s }

// optional is s, or NULL when it is empty.
func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

var (
	pingQuery     = regexp.MustCompile(`^SELECT [0-9]+$`)
	nspnameIn     = regexp.MustCompile(`(?i)nspname\s+IN\s*\(([^)]*)\)`)
	relnameEquals = regexp.MustCompile(`(?i)relname\s*=\s*('(?:[^']|'')*')`)
	quotedString  = regexp.MustCompile(`'((?:[^']|'')*)'`)
)

// query answers the extension's SELECTs: health checks, server checks and
// catalog queries.
func (s *sfSession) query(st, u string) error {
	switch {
	case pingQuery.MatchString(u):
		s.send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{Name: []byte("?column?"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1}}})
		s.row(str(strings.TrimPrefix(u, "SELECT ")))
		s.complete("SELECT 1")
	case strings.Contains(u, "VERSION()"):
		s.send(textColumns("version", "count"))
		s.row(str("PostgreSQL "+gatewayVersion+" (Igloo's gateway to Snowflake)"), str("0"))
		s.complete("SELECT 1")
	case strings.Contains(u, "TO_REGCLASS("):
		s.send(textColumns("to_regclass"))
		s.row(nil)
		s.complete("SELECT 1")
	case strings.Contains(u, "PG_EXPORT_SNAPSHOT()"):
		var b [8]byte
		_, _ = rand.Read(b[:])
		id := hex.EncodeToString(b[:])
		if strings.Contains(u, "PG_IS_IN_RECOVERY()") {
			s.send(textColumns("pg_is_in_recovery", "pg_export_snapshot"))
			s.row(str("f"), &id)
		} else {
			s.send(textColumns("pg_export_snapshot"))
			s.row(&id)
		}
		s.complete("SELECT 1")
	case strings.Contains(u, "PG_MY_TEMP_SCHEMA()"):
		s.send(textColumns("nspname"))
		s.complete("SELECT 0")
	case strings.Contains(u, "FROM PG_ENUM"):
		s.send(textColumns("oid", "enumtypid", "typname", "enumlabel"))
		s.complete("SELECT 0")
	case strings.Contains(u, "T.TYPTYPE='C'"):
		s.send(textColumns("oid", "id", "type", "attname", "typname", "sub_type_schema"))
		s.complete("SELECT 0")
	case strings.Contains(u, "FROM PG_INDEXES"):
		s.send(textColumns("oid", "tablename", "indexname"))
		s.complete("SELECT 0")
	case strings.Contains(u, "PG_ATTRIBUTE.ATTNDIMS"):
		return s.tables(st)
	case strings.Contains(u, "PG_CLASS.XMIN"):
		return s.relations(st)
	case strings.Contains(u, "FROM PG_NAMESPACE") && strings.Contains(u, "ORDER BY OID"):
		return s.schemas(st)
	case strings.HasSuffix(u, " LIMIT 0"):
		// The extension's empty results of a given shape.
		cols := strings.Count(strings.TrimSuffix(strings.SplitN(u, " FROM ", 2)[0], " LIMIT 0"), ",") + 1
		names := make([]string, cols)
		for i := range names {
			names[i] = "?column?"
		}
		s.send(textColumns(names...))
		s.complete("SELECT 0")
	default:
		return failf("0A000", "query not supported by Igloo's Snowflake gateway")
	}
	return nil
}

// catalogFilter is the condition the extension put in a catalog query:
// which schemas, and which table, it asks about.
type catalogFilter struct {
	schemas []string
	table   *string
}

func parseCatalogFilter(st string) catalogFilter {
	var f catalogFilter
	if m := nspnameIn.FindStringSubmatch(st); m != nil {
		for _, q := range quotedString.FindAllStringSubmatch(m[1], -1) {
			f.schemas = append(f.schemas, strings.ReplaceAll(q[1], "''", "'"))
		}
		if f.schemas == nil {
			f.schemas = []string{}
		}
	}
	if m := relnameEquals.FindStringSubmatch(st); m != nil {
		t := strings.ReplaceAll(m[1][1:len(m[1])-1], "''", "'")
		f.table = &t
	}
	return f
}

func (f catalogFilter) schema(s *sfSchema) bool {
	return f.schemas == nil || slices.Contains(f.schemas, s.pgName)
}

func (f catalogFilter) relation(t *sfTable) bool {
	return f.table == nil || strings.EqualFold(*f.table, t.pgName)
}

// loadedCatalog is the gateway's list of tables, which exists once the
// connection was attached.
func (s *sfSession) loadedCatalog() (*sfCatalog, error) {
	cat := s.g.currentCatalog()
	if cat == nil {
		return nil, failf("08006", "the list of Snowflake's tables is not loaded")
	}
	return cat, nil
}

// schemas answers the extension's query of pg_namespace.
func (s *sfSession) schemas(st string) error {
	cat, err := s.loadedCatalog()
	if err != nil {
		return err
	}
	f := parseCatalogFilter(st)
	s.send(textColumns("oid", "nspname"))
	n := 0
	for _, sc := range cat.schemas {
		if f.schema(sc) {
			s.row(str(strconv.Itoa(sc.oid)), str(sc.pgName))
			n++
		}
	}
	s.complete(fmt.Sprintf("SELECT %d", n))
	return nil
}

// relations answers the extension's query of a schema's tables and their
// versions (pg_class.xmin), which it makes when checking for changes.
func (s *sfSession) relations(st string) error {
	cat, err := s.loadedCatalog()
	if err != nil {
		return err
	}
	m := quotedString.FindAllStringSubmatch(st[strings.LastIndex(strings.ToLower(st), "nspname"):], 1)
	s.send(textColumns("oid", "relname", "xmin"))
	n := 0
	if m != nil {
		name := strings.ReplaceAll(m[0][1], "''", "'")
		for _, sc := range cat.schemas {
			if sc.pgName != name {
				continue
			}
			for i, t := range sc.tables {
				s.row(str(strconv.Itoa(sc.oid*100000+i)), str(t.pgName), str("1"))
				n++
			}
		}
	}
	s.complete(fmt.Sprintf("SELECT %d", n))
	return nil
}

// tables answers the extension's query of tables and their columns: one
// row per column, ordered by schema, table and column, in the columns of
// the extension's query.
func (s *sfSession) tables(st string) error {
	cat, err := s.loadedCatalog()
	if err != nil {
		return err
	}
	f := parseCatalogFilter(st)
	s.send(textColumns("namespace_id", "relname", "relpages", "attname", "type_name", "type_modifier", "ndim",
		"attnum", "notnull", "constraint_id", "constraint_type", "constraint_key", "type_schema",
		"column_comment", "table_comment"))
	n := 0
	for _, sc := range cat.schemas {
		if !f.schema(sc) {
			continue
		}
		tables := slices.Clone(sc.tables)
		slices.SortFunc(tables, func(a, b *sfTable) int { return strings.Compare(a.pgName, b.pgName) })
		for _, t := range tables {
			if !f.relation(t) {
				continue
			}
			for i, c := range t.columns {
				typ, mod := c.typ.pgType()
				notNull := "f"
				if c.notNull {
					notNull = "t"
				}
				s.row(str(strconv.Itoa(sc.oid)), str(t.pgName), str("0"), str(c.pgName), str(typ), str(strconv.Itoa(mod)),
					str("0"), str(strconv.Itoa(i+1)), str(notNull), nil, nil, nil, str("pg_catalog"),
					optional(c.comment), optional(t.comment))
				n++
			}
		}
	}
	s.complete(fmt.Sprintf("SELECT %d", n))
	return nil
}

// setCancel sets the function that ends the scan running.
func (s *sfSession) setCancel(f context.CancelFunc) {
	s.mu.Lock()
	s.cancel = f
	s.mu.Unlock()
}

func (s *sfSession) cancelScan() {
	s.mu.Lock()
	f := s.cancel
	s.mu.Unlock()
	if f != nil {
		f()
	}
}

// errCanceled ends a scan whose statement DuckDB canceled.
var errCanceled = errors.New("canceling statement due to user request")

// copyOut runs a scan: COPY (SELECT ...) TO STDOUT (FORMAT "binary").
func (s *sfSession) copyOut(ctx context.Context, st string) error {
	q, err := parseScan(st)
	if err != nil {
		return failf("0A000", "unsupported scan: %v", err)
	}
	if q.ctid && (q.ctidFrom != 0 || q.ctidTo != fullCTID) {
		return failf("0A000", "parallel scans are not supported")
	}
	cat, err := s.loadedCatalog()
	if err != nil {
		return err
	}
	scan, err := planScan(cat, q)
	if err != nil {
		return failf("42P01", "%v", err)
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	s.setCancel(func() { cancel(errCanceled) })
	defer s.setCancel(nil)

	c := s.g.settings()
	started := time.Now()
	res, err := s.g.snowflake().query(ctx, scan.sql, c.QueryTimeout)
	var se *sfError
	if errors.As(err, &se) && se.doing == "sign in" {
		s.g.signedIn(err)
	} else if err == nil {
		s.g.signedIn(nil)
	}
	if err != nil {
		if context.Cause(ctx) == errCanceled {
			return failf("57014", "%v", errCanceled)
		}
		return failf("XX000", "%v", err)
	}
	defer res.close()
	log := s.g.log.With("query_id", res.queryID)
	log.Debug("Snowflake query", "sql", scan.sql, "seconds", time.Since(started).Seconds())

	if scan.count {
		n, err := countResult(ctx, res, c.QueryTimeout)
		if err != nil {
			return failf("XX000", "%v", err)
		}
		return s.sendNulls(n)
	}
	rows, err := s.stream(ctx, res, scan, c.QueryTimeout)
	if err != nil {
		if context.Cause(ctx) == errCanceled {
			err = errCanceled
		}
		log.Debug("scan failed", "rows", rows, "err", err)
		if errors.Is(err, errClientGone) {
			return err
		}
		code := "XX000"
		if errors.Is(err, errCanceled) {
			code = "57014"
		}
		return failf(code, "%v", err)
	}
	log.Debug("scan done", "rows", rows, "seconds", time.Since(started).Seconds())
	return nil
}

// countResult reads the one number of a COUNT(*) query.
func countResult(ctx context.Context, res *sfResult, timeout time.Duration) (int64, error) {
	for _, b := range res.batches {
		recs, err := fetchBatch(ctx, b, timeout)
		if err != nil {
			return 0, err
		}
		defer release(recs)
		for _, r := range recs {
			if r.NumRows() == 0 {
				continue
			}
			if ints, ok := intValues(r.Column(0)); ok {
				return ints(0), nil
			}
			if d, ok := r.Column(0).(*array.Decimal128); ok {
				return d.Value(0).BigInt().Int64(), nil
			}
			return 0, fmt.Errorf("unexpected count of type %s", r.Column(0).DataType())
		}
	}
	return 0, errors.New("Snowflake returned no count")
}

func release(recs []arrow.Record) {
	for _, r := range recs {
		r.Release()
	}
}

// fetchBatch downloads a batch of a result, within timeout.
func fetchBatch(ctx context.Context, b sfBatch, timeout time.Duration) ([]arrow.Record, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	recs, err := b.fetch(ctx)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("Snowflake did not deliver a part of the result within query_timeout (%s)", timeout)
		}
		return nil, snowflakeError("download the result", err)
	}
	return recs, nil
}

// copyStart begins COPY data of n columns.
func (s *sfSession) copyStart(n int) {
	formats := make([]uint16, n)
	for i := range formats {
		formats[i] = 1
	}
	s.send(&pgproto3.CopyOutResponse{OverallFormat: 1, ColumnFormatCodes: formats})
}

// copyEnd ends COPY data of n rows.
func (s *sfSession) copyEnd(rows int64, sentHeader bool) error {
	msg := copyTrailer
	if !sentHeader {
		msg = append(slices.Clone(copyHeader), copyTrailer...)
	}
	if err := s.copyData(msg); err != nil {
		return err
	}
	s.send(&pgproto3.CopyDone{})
	s.complete(fmt.Sprintf("COPY %d", rows))
	return nil
}

// copyData writes a CopyData message.
func (s *sfSession) copyData(payload []byte) error {
	var h [5]byte
	h[0] = 'd'
	binary.BigEndian.PutUint32(h[1:], uint32(len(payload)+4))
	if _, err := s.out.Write(h[:]); err != nil {
		return errClientGone
	}
	if _, err := s.out.Write(payload); err != nil {
		return errClientGone
	}
	return nil
}

// sendNulls sends n rows of one NULL, for SELECT NULL.
func (s *sfSession) sendNulls(n int64) error {
	s.copyStart(1)
	row := []byte{0, 1, 0xff, 0xff, 0xff, 0xff}
	first := append(slices.Clone(copyHeader), row...)
	for i := int64(0); i < n; i++ {
		msg := row
		if i == 0 {
			msg = first
		}
		if err := s.copyData(msg); err != nil {
			return err
		}
	}
	return s.copyEnd(n, n > 0)
}

// fetched is a downloaded batch.
type fetched struct {
	recs []arrow.Record
	err  error
}

// stream sends a result as COPY data, one row per message as PostgreSQL
// does, downloading up to scanPrefetch batches ahead. It returns the
// number of rows sent.
func (s *sfSession) stream(ctx context.Context, res *sfResult, scan *sfScan, timeout time.Duration) (int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make([]chan fetched, len(res.batches))
	for i := range results {
		results[i] = make(chan fetched, 1)
	}
	slots := make(chan struct{}, scanPrefetch)
	go func() {
		for i, b := range res.batches {
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				results[i] <- fetched{err: ctx.Err()}
				continue
			}
			go func() {
				recs, err := fetchBatch(ctx, b, timeout)
				results[i] <- fetched{recs, err}
			}()
		}
	}()
	// Drain what is still downloading if the stream ends early.
	defer func() {
		cancel()
		for _, ch := range results {
			if f, ok := <-ch; ok {
				release(f.recs)
			}
		}
	}()

	s.copyStart(len(scan.columns))
	var rows int64
	buf := slices.Clone(copyHeader)
	sentHeader := false
	for i := range res.batches {
		f := <-results[i]
		close(results[i])
		<-slots
		if f.err != nil {
			release(f.recs)
			return rows, f.err
		}
		for _, rec := range f.recs {
			encs, err := encoders(rec, scan.columns)
			if err != nil {
				release(f.recs)
				return rows, err
			}
			n := int(rec.NumRows())
			for r := 0; r < n; r++ {
				buf = binary.BigEndian.AppendUint16(buf, uint16(len(encs)))
				for _, enc := range encs {
					if buf, err = enc(buf, r); err != nil {
						release(f.recs)
						return rows, err
					}
				}
				if err := s.copyData(buf); err != nil {
					release(f.recs)
					return rows, err
				}
				sentHeader = true
				buf = buf[:0]
				rows++
			}
			if ctx.Err() != nil {
				release(f.recs)
				return rows, ctx.Err()
			}
		}
		release(f.recs)
	}
	return rows, s.copyEnd(rows, sentHeader)
}
