package console

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/pgwire"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

const (
	// maxRows is how many rows of a result the console sends to the
	// browser; the CSV download has them all.
	maxRows = 10_000
	// maxValue caps one value's length in the browser, in bytes.
	maxValue = 32 << 10
	// maxScript caps the SQL text of one request.
	maxScript = 1 << 20
	// heartbeat keeps proxies from closing a connection while a long
	// statement runs without sending anything.
	heartbeat = 15 * time.Second
)

type queryRequest struct {
	SQL string `json:"sql"`
	// Lookup marks the console's own lookups, which pages make to show
	// themselves, such as SHOW TASKS, rather than what the user ran.
	Lookup bool `json:"lookup,omitempty"`
}

// The application names of the console's SQL sessions, which the query
// history records: what users run in worksheets and the changes they make
// on its pages, and the lookups the pages make to show themselves (a
// catalog page's dozen reads of the catalog's views, the lists of tasks
// and tokens, the history page's own queries). The history page leaves
// lookups out unless asked; every statement is recorded either way. Any
// client can name itself anything with SET application_name, so this
// separates the console's noise, not who may see what.
const (
	appConsole = "Igloo console"
	appLookup  = "Igloo console lookup"
)

// appFor names the session of a request that is a lookup or not.
func appFor(lookup bool) string {
	if lookup {
		return appLookup
	}
	return appConsole
}

// open opens a session for user, named app in the history.
func (c *Console) open(ctx context.Context, user auth.User, r *http.Request, app string) (*pgwire.EmbeddedSession, error) {
	es, err := c.sql.OpenEmbedded(ctx, user, client(r))
	if err != nil {
		return nil, err
	}
	es.SetApplicationName(app)
	return es, nil
}

// sqlError is an error as SQL clients see it.
type sqlError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Detail  string `json:"detail,omitempty"`
	Hint    string `json:"hint,omitempty"`
}

func toSQLError(err error) sqlError {
	e := pgerror.Flatten(err)
	code := string(e.Code)
	if code == "" || e.Code == codes.Uncategorized {
		code = string(codes.Internal)
	}
	return sqlError{Code: code, Message: e.Message, Detail: e.Detail, Hint: e.Hint}
}

// statusFor picks the HTTP status for a SQL error returned outside a
// result stream.
func statusFor(err sqlError) int {
	switch codes.Code(err.Code) {
	case codes.InsufficientPrivilege:
		return http.StatusForbidden
	case codes.TooManyConnections:
		return http.StatusServiceUnavailable
	}
	return http.StatusBadRequest
}

// query runs a script statement by statement, as psql does, in one session
// as the signed-in user, and streams the results as JSON lines:
//
//	{"statement": {"index": 0, "offset": 0, "text": "SELECT ..."}}
//	{"columns": [{"name": "n", "type": "bigint"}]}
//	{"rows": [["1"], [null]]}
//	{"complete": {"tag": "SELECT 2", "rows": 2, "truncated": false, "ms": 3.1}}
//	{"error": {"code": "42501", "message": "...", "ms": 0.4}}
//	{"done": {"ms": 3.6}}
//
// A statement starts, sends its columns and rows if it returns any, and
// completes or fails; offset is where it starts in the script.
//
// Values are strings in PostgreSQL's text format, or null. At most maxRows
// rows of each result are sent. The first error ends the script.
// Cancelling the request (closing the connection) cancels the statement.
func (c *Console) query(w http.ResponseWriter, r *http.Request, req *request) {
	var in queryRequest
	if !readJSON(w, r, maxScript, &in) {
		return
	}
	stmts := sqllex.Split(in.SQL)
	if len(stmts) == 0 {
		writeError(w, http.StatusBadRequest, "nothing to run")
		return
	}
	es, err := c.open(r.Context(), req.user, r, appFor(in.Lookup))
	if err != nil {
		e := toSQLError(err)
		writeJSON(w, statusFor(e), map[string]any{"error": e.Message, "code": e.Code})
		return
	}
	defer es.Close() //nolint:errcheck

	out := newStream(w)
	defer out.stop()
	began := time.Now()
	ctx := r.Context()
	if in.Lookup {
		// A page's lookup is the console's own SQL, see runAs.
		ctx = pgwire.WithIglooSQL(ctx)
	}
	for i, stmt := range stmts {
		if err := out.send(map[string]any{"statement": map[string]any{"index": i, "offset": stmt.Offset, "text": stmt.Text}}); err != nil {
			return
		}
		start := time.Now()
		res := &gridWriter{out: out}
		err := es.Run(ctx, stmt.Text, res)
		if err == nil {
			err = res.flush()
		}
		ms := millis(time.Since(start))
		if err != nil {
			if r.Context().Err() != nil {
				return
			}
			e := toSQLError(err)
			_ = out.send(map[string]any{"error": map[string]any{
				"code": e.Code, "message": e.Message, "detail": e.Detail, "hint": e.Hint, "ms": ms,
			}})
			break
		}
		if res.tag == "" {
			res.tag = "OK"
		}
		if err := out.send(map[string]any{"complete": map[string]any{
			"tag": res.tag, "rows": res.count(), "truncated": res.truncated, "shortened": res.shortened, "ms": ms,
		}}); err != nil {
			return
		}
	}
	_ = out.send(map[string]any{"done": map[string]any{"ms": millis(time.Since(began))}})
}

func millis(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

// stream writes JSON lines to the response, flushing as it goes, and blank
// lines while nothing else is sent.
type stream struct {
	mu    sync.Mutex
	w     http.ResponseWriter
	rc    *http.ResponseController
	enc   *json.Encoder
	err   error
	done  chan struct{}
	ended sync.WaitGroup
}

func newStream(w http.ResponseWriter) *stream {
	w.Header().Set("Content-Type", "application/x-ndjson")
	// Proxies such as nginx otherwise hold results back.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	s := &stream{w: w, rc: http.NewResponseController(w), enc: json.NewEncoder(w), done: make(chan struct{})}
	s.ended.Add(1)
	go s.beat()
	return s
}

func (s *stream) beat() {
	defer s.ended.Done()
	t := time.NewTicker(heartbeat)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
			s.mu.Lock()
			if s.err == nil {
				if _, s.err = s.w.Write([]byte("\n")); s.err == nil {
					s.err = s.rc.Flush()
				}
			}
			s.mu.Unlock()
		}
	}
}

func (s *stream) send(v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		if s.err = s.enc.Encode(v); s.err == nil {
			s.err = s.rc.Flush()
		}
	}
	return s.err
}

func (s *stream) stop() {
	close(s.done)
	s.ended.Wait()
}

// gridWriter sends a statement's result to the stream in batches.
type gridWriter struct {
	out       *stream
	batch     [][]any
	sent      int
	last      time.Time
	tag       string
	truncated bool
	// shortened says some values were cut to maxValue.
	shortened bool
}

func (g *gridWriter) Columns(cols []pgwire.Column) error {
	type column struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	out := make([]column, len(cols))
	for i, c := range cols {
		out[i] = column{c.Name, c.Type}
	}
	g.last = time.Now()
	return g.out.send(map[string]any{"columns": out})
}

func (g *gridWriter) Row(values []any) error {
	if g.count() == maxRows {
		g.truncated = true
		return pgwire.ErrStopRows
	}
	row := make([]any, len(values))
	for i, v := range values {
		if s, ok := v.(string); ok && len(s) > maxValue {
			v, g.shortened = truncate(s, maxValue)+"…", true
		}
		row[i] = v
	}
	g.batch = append(g.batch, row)
	if len(g.batch) >= 1000 || time.Since(g.last) > 200*time.Millisecond {
		return g.flush()
	}
	return nil
}

func (g *gridWriter) Complete(tag string) error {
	g.tag = tag
	return g.flush()
}

func (g *gridWriter) count() int { return g.sent + len(g.batch) }

func (g *gridWriter) flush() error {
	if len(g.batch) == 0 {
		return nil
	}
	err := g.out.send(map[string]any{"rows": g.batch})
	g.sent += len(g.batch)
	g.batch, g.last = g.batch[:0], time.Now()
	return err
}

// truncate cuts s to at most n bytes without splitting a character.
func truncate(s string, n int) string {
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// download runs one query again and streams its whole result as CSV. Only
// queries that read are run again; anything else is refused before it runs.
func (c *Console) download(w http.ResponseWriter, r *http.Request, req *request) {
	var in queryRequest
	if !readJSON(w, r, maxScript, &in) {
		return
	}
	stmts := sqllex.Split(in.SQL)
	if len(stmts) != 1 {
		writeError(w, http.StatusBadRequest, "send exactly one query to download")
		return
	}
	es, err := c.open(r.Context(), req.user, r, appConsole)
	if err != nil {
		e := toSQLError(err)
		writeJSON(w, statusFor(e), map[string]any{"error": e.Message, "code": e.Code})
		return
	}
	defer es.Close() //nolint:errcheck

	out := &csvWriter{w: w, rc: http.NewResponseController(w)}
	err = es.RunQuery(r.Context(), stmts[0].Text, out)
	switch {
	case err != nil && !out.started:
		e := toSQLError(err)
		writeJSON(w, statusFor(e), map[string]any{"error": e.Message, "code": e.Code})
	case err != nil:
		// Too late for an error status: break the connection, so the
		// browser reports the download as failed rather than complete.
		c.log.Debug("download failed", "user", req.user.Name, "err", err)
		panic(http.ErrAbortHandler)
	}
}

// csvWriter streams a result as CSV, with a header row of column names.
// NULLs are empty fields.
type csvWriter struct {
	w       http.ResponseWriter
	rc      *http.ResponseController
	csv     *csv.Writer
	record  []string
	rows    int
	started bool
}

func (c *csvWriter) Columns(cols []pgwire.Column) error {
	if !c.started {
		c.w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		c.w.Header().Set("Content-Disposition", `attachment; filename="igloo-`+time.Now().UTC().Format("20060102-150405")+`.csv"`)
		c.w.WriteHeader(http.StatusOK)
		c.csv = csv.NewWriter(c.w)
		c.started = true
	}
	c.record = make([]string, len(cols))
	for i, col := range cols {
		c.record[i] = col.Name
	}
	return c.csv.Write(c.record)
}

func (c *csvWriter) Row(values []any) error {
	for i, v := range values {
		c.record[i], _ = v.(string)
	}
	if err := c.csv.Write(c.record); err != nil {
		return err
	}
	if c.rows++; c.rows%10_000 == 0 {
		c.csv.Flush()
		if err := c.csv.Error(); err != nil {
			return err
		}
		return c.rc.Flush()
	}
	return nil
}

func (c *csvWriter) Complete(string) error {
	if !c.started {
		return nil
	}
	c.csv.Flush()
	return c.csv.Error()
}

// result is a statement's rows, for the admin pages.
type result struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
	Tag     string   `json:"tag"`
}

// collect gathers a result in memory, up to maxRows rows.
type collect struct{ result }

func (c *collect) Columns(cols []pgwire.Column) error {
	c.result.Columns = make([]string, len(cols))
	for i, col := range cols {
		c.result.Columns[i] = col.Name
	}
	return nil
}

func (c *collect) Row(values []any) error {
	if len(c.Rows) == maxRows {
		return pgwire.ErrStopRows
	}
	c.Rows = append(c.Rows, append([]any(nil), values...))
	return nil
}

func (c *collect) Complete(tag string) error {
	c.Tag = tag
	return nil
}

// run runs one statement as user and returns its result: a change the
// user asked for on one of the console's pages.
func (c *Console) run(ctx context.Context, user auth.User, r *http.Request, sql string) (result, error) {
	return c.runAs(ctx, user, r, appConsole, sql)
}

// lookup is run for a lookup a page makes to show itself.
func (c *Console) lookup(ctx context.Context, user auth.User, r *http.Request, sql string) (result, error) {
	return c.runAs(ctx, user, r, appLookup, sql)
}

func (c *Console) runAs(ctx context.Context, user auth.User, r *http.Request, app, sql string) (result, error) {
	es, err := c.open(ctx, user, r, app)
	if err != nil {
		return result{}, err
	}
	defer es.Close() //nolint:errcheck
	// The pages' statements are the console's own, in DuckDB's dialect,
	// whatever sql_dialect the user's role gives their sessions.
	ctx = pgwire.WithIglooSQL(ctx)
	var out collect
	err = es.Run(ctx, sql, &out)
	if out.Rows == nil {
		out.Rows = [][]any{}
	}
	return out.result, err
}

// quoteLiteral quotes s as a SQL string literal.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
