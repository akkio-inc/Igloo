package sharing

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	iceio "github.com/apache/iceberg-go/io"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/catalog"
)

// Prefix is where the sharing endpoint serves the protocol. A recipient's
// profile names the endpoint with it, as in https://igloo.example.com:8444/delta-sharing.
const Prefix = "/delta-sharing"

const (
	// fileURLLifetime is how long a file URL in a query response works.
	// Each request for the file also checks the recipient's token, so
	// rotating or dropping a recipient ends its URLs at once.
	fileURLLifetime = time.Hour
	// tokenCacheTTL is how long a checked bearer token is trusted without
	// asking the metadata database again. It bounds how long a rotated
	// token keeps working on other replicas.
	tokenCacheTTL = 10 * time.Second
	// maxPageSize caps maxResults in list requests.
	maxPageSize = 1000
	// recipientRole is the role recipients' requests have in the query
	// history; their user name there is recipientUser plus their name,
	// which cannot be an Igloo user's name.
	recipientRole = "recipient"
	recipientUser = "recipient:"
)

// ServerOptions configure the sharing endpoint.
type ServerOptions struct {
	Store *Store
	// Key is a secret every replica shares, at least 32 random bytes. It
	// signs file URLs.
	Key []byte
	// Files reads the warehouse's data files.
	Files iceio.IO
	// Warehouse is the warehouse root with a trailing slash; no file
	// outside it is ever served.
	Warehouse string
	// URL is the endpoint's public URL including Prefix, for profile files
	// and file URLs. Empty means the URL a request came to.
	URL string
	// Catalog is the Iceberg catalog's name in SQL, for the history.
	Catalog string
	// Audit records every request in the query history; it may be nil.
	Audit   *audit.Log
	Log     *slog.Logger
	Metrics *Metrics
	// Tables are the catalog's tables.
	Tables TableSource
	// Protection names the policies of a catalog table. Tables with
	// policies are never served; until a Protection is set (here or with
	// SetProtection), no table is.
	Protection Protection
}

// Server serves the Delta Sharing protocol to recipients.
type Server struct {
	store     *Store
	source    source
	files     iceio.IO
	warehouse string
	signer    *sealer
	url       string
	catalog   string
	audit     *audit.Log
	log       *slog.Logger
	metrics   *Metrics
	mux       *http.ServeMux
	// protection says which tables have policies (see protect.go).
	protection protection

	// tokensMu guards the caches of bearer tokens and of recipients'
	// access to tables.
	tokensMu sync.Mutex
	tokens   map[string]cachedToken
	access   map[string]cachedAccess
	now      func() time.Time
}

type cachedToken struct {
	token  bearerToken
	err    error
	loaded time.Time
}

// NewServer builds the sharing endpoint's handler.
func NewServer(opts ServerOptions) (*Server, error) {
	if opts.Store == nil || opts.Files == nil || opts.Tables.source == nil || opts.Warehouse == "" {
		return nil, errors.New("sharing: ServerOptions.Store, Files, Warehouse and Tables are required")
	}
	signer, err := newSealer(opts.Key, "file url v1")
	if err != nil {
		return nil, err
	}
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	s := &Server{
		store: opts.Store, source: opts.Tables.source, files: opts.Files, warehouse: opts.Warehouse, signer: signer,
		url: strings.TrimSuffix(opts.URL, "/"), catalog: opts.Catalog, audit: opts.Audit,
		log: opts.Log.With("component", "sharing"), metrics: opts.Metrics,
		tokens: map[string]cachedToken{}, access: map[string]cachedAccess{}, now: time.Now,
	}
	s.protection.set(opts.Protection)
	s.routes()
	return s, nil
}

// TableSource gives the sharing endpoint the catalog's tables.
type TableSource struct{ source source }

// IcebergTables reads shared tables from the catalog; only data files under
// warehouse (the warehouse root, with a trailing slash) are served. files
// reads the warehouse, and writes copies of data files with their deletes
// applied.
func IcebergTables(cat *catalog.Client, warehouse string, files iceio.IO) TableSource {
	return TableSource{source: newIcebergSource(cat, warehouse, files)}
}

func (s *Server) routes() {
	s.mux = http.NewServeMux()
	api := func(pattern string, h func(http.ResponseWriter, *http.Request, *call) error) {
		method, path, _ := strings.Cut(pattern, " ")
		s.mux.HandleFunc(method+" "+Prefix+path, s.authenticated(h))
	}
	api("GET /shares", s.listShares)
	api("GET /shares/{share}", s.getShare)
	api("GET /shares/{share}/schemas", s.listSchemas)
	api("GET /shares/{share}/schemas/{schema}/tables", s.listTables)
	api("GET /shares/{share}/all-tables", s.listTables)
	api("GET /shares/{share}/schemas/{schema}/tables/{table}/version", s.tableVersion)
	// The original form of the version request, which the protocol keeps
	// for older clients.
	api("HEAD /shares/{share}/schemas/{schema}/tables/{table}", s.tableVersion)
	api("GET /shares/{share}/schemas/{schema}/tables/{table}/metadata", s.tableMetadata)
	api("POST /shares/{share}/schemas/{schema}/tables/{table}/query", s.queryTable)
	api("GET /shares/{share}/schemas/{schema}/tables/{table}/changes", s.notSupported)
	api("POST /shares/{share}/schemas/{schema}/tables/{table}/temporary-table-credentials", s.notSupported)
	s.mux.HandleFunc("GET "+Prefix+"/files/{grant}", s.serveFile)
	s.mux.HandleFunc("GET "+Prefix+"/activate/{code}", s.activationPage)
	s.mux.HandleFunc("POST "+Prefix+"/activate/{code}", s.activate)
	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, &apiError{status: http.StatusNotFound, code: "RESOURCE_DOES_NOT_EXIST", msg: "no such endpoint"})
	})
}

// ServeHTTP refuses requests that would send a bearer token or data
// unencrypted across a network, then serves the protocol.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Cache-Control", "no-store")
	h.Set("Referrer-Policy", "no-referrer")
	if r.TLS == nil && !loopback(r.Host) {
		writeError(w, &apiError{status: http.StatusForbidden, code: "PERMISSION_DENIED",
			msg: "the Delta Sharing endpoint accepts unencrypted requests only at localhost; " +
				"configure TLS (IGLOO_TLS_CERT_FILE and IGLOO_TLS_KEY_FILE) to share across a network"})
		return
	}
	s.mux.ServeHTTP(w, r)
}

// call is an authenticated request.
type call struct {
	recipient string
	tokenID   string
	// table is the catalog table the request reads, for the history.
	table string
	// rows is how many files a query returned, for the history.
	rows int64
	text string
	// policies name the policies that made Igloo refuse the table.
	policies []string
}

// apiError is an error response in the protocol's format.
type apiError struct {
	status    int
	code, msg string
}

func (e *apiError) Error() string { return e.msg }

func badRequest(format string, args ...any) error {
	return &apiError{status: http.StatusBadRequest, code: "INVALID_PARAMETER_VALUE", msg: fmt.Sprintf(format, args...)}
}

func notFound(format string, args ...any) error {
	return &apiError{status: http.StatusNotFound, code: "RESOURCE_DOES_NOT_EXIST", msg: fmt.Sprintf(format, args...)}
}

func writeError(w http.ResponseWriter, e *apiError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.status)
	_ = json.NewEncoder(w).Encode(map[string]string{"errorCode": e.code, "message": e.msg})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}

// authenticated checks the request's bearer token, runs h, answers h's
// error in the protocol's format and records the request in the history.
func (s *Server) authenticated(h func(http.ResponseWriter, *http.Request, *call) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := s.now()
		c, err := s.authenticate(r)
		if err != nil {
			s.metrics.request("unauthenticated")
			w.Header().Set("WWW-Authenticate", `Bearer realm="delta-sharing"`)
			writeError(w, &apiError{status: http.StatusUnauthorized, code: "UNAUTHENTICATED", msg: err.Error()})
			return
		}
		c.text = r.Method + " " + strings.TrimPrefix(r.URL.Path, Prefix)
		if r.URL.RawQuery != "" {
			c.text += "?" + r.URL.RawQuery
		}
		c.rows = -1
		err = h(w, r, c)
		status := http.StatusOK
		if err != nil {
			var ae *apiError
			switch {
			case errors.As(err, &ae):
			case errors.Is(err, ErrNotFound):
				ae = &apiError{status: http.StatusNotFound, code: "RESOURCE_DOES_NOT_EXIST", msg: err.Error()}
			case isNotShareable(err):
				ae = &apiError{status: http.StatusBadRequest, code: "INVALID_PARAMETER_VALUE", msg: err.Error()}
			default:
				s.log.Warn("sharing request failed", "recipient", c.recipient, "request", c.text, "err", err)
				ae = &apiError{status: http.StatusInternalServerError, code: "INTERNAL_ERROR", msg: "the request failed; try again or ask the provider"}
			}
			status = ae.status
			writeError(w, ae)
		}
		s.metrics.request(strconv.Itoa(status))
		s.record(r, c, start, status)
	}
}

// authenticate finds the recipient whose bearer token the request carries.
// Every failure gets the same answer, so a caller learns nothing about
// which tokens exist.
func (s *Server) authenticate(r *http.Request) (*call, error) {
	invalid := errors.New("missing or invalid bearer token")
	scheme, text, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return nil, invalid
	}
	id, hash, ok := parseToken(tokenPrefix, strings.TrimSpace(text))
	if !ok {
		s.recordFailure(r, "", "malformed bearer token")
		return nil, invalid
	}
	tok, err := s.bearer(r.Context(), id)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			s.log.Warn("could not check a bearer token", "err", err)
			return nil, errors.New("could not check the bearer token; try again")
		}
		s.recordFailure(r, "", "unknown bearer token")
		return nil, invalid
	}
	if !sameHash(tok.Hash, hash) {
		s.recordFailure(r, tok.Recipient, "wrong bearer token")
		return nil, invalid
	}
	if !tok.Valid || !s.now().Before(tok.Expires) {
		s.recordFailure(r, tok.Recipient, "expired or rotated bearer token")
		return nil, errors.New("the bearer token has expired or was replaced; ask the provider for a new one")
	}
	return &call{recipient: tok.Recipient, tokenID: tok.ID}, nil
}

// bearer looks up a bearer token, through a short-lived cache.
func (s *Server) bearer(ctx context.Context, id string) (bearerToken, error) {
	now := s.now()
	s.tokensMu.Lock()
	c, ok := s.tokens[id]
	s.tokensMu.Unlock()
	if ok && now.Sub(c.loaded) < tokenCacheTTL {
		return c.token, c.err
	}
	tok, err := s.store.BearerToken(ctx, id)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return tok, err
	}
	s.tokensMu.Lock()
	defer s.tokensMu.Unlock()
	if len(s.tokens) > 10_000 {
		clear(s.tokens)
	}
	s.tokens[id] = cachedToken{token: tok, err: err, loaded: now}
	return tok, err
}

// Forget drops cached tokens and access checks, so a change made on this
// replica, such as a token rotation or a revoked share, applies at once.
func (s *Server) Forget() {
	if s == nil {
		return
	}
	s.tokensMu.Lock()
	clear(s.tokens)
	clear(s.access)
	s.tokensMu.Unlock()
}

func (s *Server) record(r *http.Request, c *call, start time.Time, status int) {
	if s.audit == nil {
		return
	}
	q := audit.Query{
		SessionID:       c.tokenID,
		User:            recipientUser + c.recipient,
		Role:            recipientRole,
		Start:           start,
		Duration:        s.now().Sub(start),
		Outcome:         audit.OutcomeOK,
		Rows:            c.rows,
		StatementType:   "DELTA SHARING",
		Text:            c.text,
		ClientAddress:   clientAddress(r),
		ApplicationName: truncate(r.UserAgent(), 200),
	}
	if c.table != "" {
		q.TablesRead = []string{c.table}
	}
	q.Policies = c.policies
	switch {
	case status == http.StatusForbidden || status == http.StatusNotFound:
		q.Outcome, q.SQLState = audit.OutcomeDenied, "42501"
	case status >= 500:
		q.Outcome, q.SQLState = audit.OutcomeError, "XX000"
	case status >= 400:
		q.Outcome, q.SQLState = audit.OutcomeError, "22023"
	}
	s.audit.RecordQuery(q)
}

// recordFailure records a request with a bad bearer token in the login
// history.
func (s *Server) recordFailure(r *http.Request, recipient, reason string) {
	if s.audit == nil {
		return
	}
	if recipient == "" {
		recipient = "?"
	}
	s.audit.RecordLogin(audit.Login{
		Time: s.now(), User: recipientUser + recipient, Result: audit.LoginFailure, Reason: reason,
		ClientAddress: clientAddress(r), ApplicationName: truncate(r.UserAgent(), 200),
	})
}

func clientAddress(r *http.Request) string {
	return r.RemoteAddr
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// baseURL is the endpoint's URL, for file URLs and profiles.
func (s *Server) baseURL(r *http.Request) string {
	if s.url != "" {
		return s.url
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host + Prefix
}

// --- listings ---------------------------------------------------------------

// share returns the share the request names, if it is granted to the caller.
// Shares that exist but are not granted look the same as ones that do not.
func (s *Server) share(r *http.Request, c *call) (Share, error) {
	name := strings.ToLower(r.PathValue("share"))
	shares, err := s.store.RecipientShares(r.Context(), c.recipient)
	if err != nil {
		return Share{}, err
	}
	for _, sh := range shares {
		if sh.Name == name {
			return sh, nil
		}
	}
	return Share{}, notFound("share %q does not exist or is not shared with you", r.PathValue("share"))
}

// page applies maxResults and pageToken to n items, returning the range to
// send and the next page's token.
func page(r *http.Request, n int) (from, to int, next string, err error) {
	limit := maxPageSize
	if v := r.URL.Query().Get("maxResults"); v != "" {
		limit, err = strconv.Atoi(v)
		if err != nil || limit < 0 {
			return 0, 0, "", badRequest("maxResults must be a non-negative integer")
		}
		limit = min(limit, maxPageSize)
	}
	if t := r.URL.Query().Get("pageToken"); t != "" {
		raw, derr := base64.RawURLEncoding.DecodeString(t)
		from, err = strconv.Atoi(string(raw))
		if derr != nil || err != nil || from < 0 || from > n {
			return 0, 0, "", badRequest("invalid pageToken")
		}
	}
	to = min(from+limit, n)
	if to < n {
		next = base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(to)))
	}
	return from, to, next, nil
}

type listResponse struct {
	Items         []any  `json:"items"`
	NextPageToken string `json:"nextPageToken,omitempty"`
}

func respondPage(w http.ResponseWriter, r *http.Request, items []any) error {
	from, to, next, err := page(r, len(items))
	if err != nil {
		return err
	}
	writeJSON(w, listResponse{Items: items[from:to], NextPageToken: next})
	return nil
}

type shareItem struct {
	Name string `json:"name"`
	ID   string `json:"id,omitempty"`
}

func (s *Server) listShares(w http.ResponseWriter, r *http.Request, c *call) error {
	shares, err := s.store.RecipientShares(r.Context(), c.recipient)
	if err != nil {
		return err
	}
	items := make([]any, len(shares))
	for i, sh := range shares {
		items[i] = shareItem{Name: sh.Name, ID: sh.ID}
	}
	return respondPage(w, r, items)
}

func (s *Server) getShare(w http.ResponseWriter, r *http.Request, c *call) error {
	sh, err := s.share(r, c)
	if err != nil {
		return err
	}
	writeJSON(w, map[string]any{"share": shareItem{Name: sh.Name, ID: sh.ID}})
	return nil
}

type schemaItem struct {
	Name  string `json:"name"`
	Share string `json:"share"`
}

func (s *Server) listSchemas(w http.ResponseWriter, r *http.Request, c *call) error {
	sh, err := s.share(r, c)
	if err != nil {
		return err
	}
	tables, err := s.store.ShareTables(r.Context(), sh.Name)
	if err != nil {
		return err
	}
	items := []any{}
	var seen []string
	for _, t := range tables {
		if !slices.ContainsFunc(seen, func(n string) bool { return strings.EqualFold(n, t.Schema) }) {
			seen = append(seen, t.Schema)
			items = append(items, schemaItem{Name: t.Schema, Share: sh.Name})
		}
	}
	return respondPage(w, r, items)
}

type tableItem struct {
	Name    string `json:"name"`
	Schema  string `json:"schema"`
	Share   string `json:"share"`
	ShareID string `json:"shareId,omitempty"`
}

// listTables lists a schema's tables, or for all-tables the whole share's.
func (s *Server) listTables(w http.ResponseWriter, r *http.Request, c *call) error {
	sh, err := s.share(r, c)
	if err != nil {
		return err
	}
	tables, err := s.store.ShareTables(r.Context(), sh.Name)
	if err != nil {
		return err
	}
	schema := r.PathValue("schema")
	items := []any{}
	for _, t := range tables {
		if schema == "" || strings.EqualFold(t.Schema, schema) {
			items = append(items, tableItem{Name: t.Name, Schema: t.Schema, Share: sh.Name, ShareID: sh.ID})
		}
	}
	if schema != "" && len(items) == 0 {
		return notFound("schema %q does not exist in share %q", schema, sh.Name)
	}
	return respondPage(w, r, items)
}

// --- tables -----------------------------------------------------------------

// sharedTable returns the table the request names, in a share granted to the
// caller, and notes the catalog table it reads.
func (s *Server) sharedTable(r *http.Request, c *call) (SharedTable, error) {
	sh, err := s.share(r, c)
	if err != nil {
		return SharedTable{}, err
	}
	tables, err := s.store.ShareTables(r.Context(), sh.Name)
	if err != nil {
		return SharedTable{}, err
	}
	schema, name := r.PathValue("schema"), r.PathValue("table")
	for _, t := range tables {
		if strings.EqualFold(t.Schema, schema) && strings.EqualFold(t.Name, name) {
			c.table = s.catalog + "." + t.SourceSchema + "." + t.SourceTable
			if err := s.checkTable(r.Context(), c, t); err != nil {
				return SharedTable{}, err
			}
			return t, nil
		}
	}
	return SharedTable{}, notFound("table %s.%s does not exist in share %q", schema, name, sh.Name)
}

// requestedVersion reads a version or timestamp, which only tables shared
// with history may be read at.
func requestedVersion(t SharedTable, number *int64, timestamp string) (version, error) {
	var v version
	if number == nil && timestamp == "" {
		return v, nil
	}
	if !t.History {
		return v, &apiError{status: http.StatusForbidden, code: "PERMISSION_DENIED",
			msg: fmt.Sprintf("the history of %s.%s is not shared; only its current version can be read", t.Schema, t.Name)}
	}
	if number != nil && timestamp != "" {
		return v, badRequest("version and timestamp cannot both be set")
	}
	if number != nil {
		if *number < 0 {
			return v, badRequest("version must not be negative")
		}
		v.number = number
		return v, nil
	}
	at, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		return v, badRequest("timestamp must look like 2026-01-01T00:00:00Z")
	}
	v.at = &at
	return v, nil
}

func (s *Server) tableVersion(w http.ResponseWriter, r *http.Request, c *call) error {
	t, err := s.sharedTable(r, c)
	if err != nil {
		return err
	}
	var n int64
	if ts := r.URL.Query().Get("startingTimestamp"); ts != "" {
		if !t.History {
			return &apiError{status: http.StatusForbidden, code: "PERMISSION_DENIED",
				msg: fmt.Sprintf("the history of %s.%s is not shared", t.Schema, t.Name)}
		}
		at, perr := time.Parse(time.RFC3339Nano, ts)
		if perr != nil {
			return badRequest("startingTimestamp must look like 2026-01-01T00:00:00Z")
		}
		n, err = s.source.startingVersion(r.Context(), t.SourceSchema, t.SourceTable, at)
	} else {
		var snap *snapshot
		snap, err = s.source.snapshot(r.Context(), t.SourceSchema, t.SourceTable, version{}, false)
		if snap != nil {
			n = snap.version
		}
	}
	if err != nil {
		return err
	}
	w.Header().Set("Delta-Table-Version", strconv.FormatInt(n, 10))
	return nil
}

// capabilities are what the client said it can read, in the
// delta-sharing-capabilities header.
type capabilities struct {
	// delta is true when the client asked for the delta response format
	// only; otherwise Igloo answers in the parquet format.
	delta bool
	// header is the capabilities header of the response.
	header string
}

func readCapabilities(r *http.Request) capabilities {
	raw := r.Header.Get("delta-sharing-capabilities")
	if raw == "" {
		return capabilities{}
	}
	var formats []string
	for _, part := range strings.Split(raw, ";") {
		k, v, _ := strings.Cut(part, "=")
		if strings.EqualFold(strings.TrimSpace(k), "responseformat") {
			for _, f := range strings.Split(v, ",") {
				formats = append(formats, strings.ToLower(strings.TrimSpace(f)))
			}
		}
	}
	if len(formats) > 0 && !slices.Contains(formats, "parquet") && slices.Contains(formats, "delta") {
		return capabilities{delta: true, header: "responseformat=delta"}
	}
	return capabilities{header: "responseformat=parquet"}
}

func (s *Server) tableMetadata(w http.ResponseWriter, r *http.Request, c *call) error {
	t, err := s.sharedTable(r, c)
	if err != nil {
		return err
	}
	var number *int64
	if v := r.URL.Query().Get("version"); v != "" {
		n, perr := strconv.ParseInt(v, 10, 64)
		if perr != nil {
			return badRequest("version must be an integer")
		}
		number = &n
	}
	v, err := requestedVersion(t, number, r.URL.Query().Get("timestamp"))
	if err != nil {
		return err
	}
	snap, err := s.source.snapshot(r.Context(), t.SourceSchema, t.SourceTable, v, false)
	if err != nil {
		return err
	}
	caps := readCapabilities(r)
	lines := newLines(w, snap, caps)
	lines.protocol()
	lines.metadata(t, v.timeTravel())
	return lines.err
}

// queryRequest is the body of a query.
type queryRequest struct {
	PredicateHints     []string `json:"predicateHints"`
	JSONPredicateHints string   `json:"jsonPredicateHints"`
	LimitHint          *int64   `json:"limitHint"`
	Version            *int64   `json:"version"`
	Timestamp          string   `json:"timestamp"`
	StartingVersion    *int64   `json:"startingVersion"`
	EndingVersion      *int64   `json:"endingVersion"`
}

func (s *Server) queryTable(w http.ResponseWriter, r *http.Request, c *call) error {
	t, err := s.sharedTable(r, c)
	if err != nil {
		return err
	}
	var q queryRequest
	if r.ContentLength != 0 {
		body, rerr := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if rerr != nil {
			return badRequest("the request body is too large")
		}
		if len(strings.TrimSpace(string(body))) > 0 {
			if err := json.Unmarshal(body, &q); err != nil {
				return badRequest("malformed request body: %v", err)
			}
		}
	}
	if q.StartingVersion != nil || q.EndingVersion != nil {
		return badRequest("Igloo does not share change data feeds; query a snapshot instead")
	}
	v, err := requestedVersion(t, q.Version, q.Timestamp)
	if err != nil {
		return err
	}
	if v.number != nil {
		c.text += fmt.Sprintf(" version %d", *v.number)
	} else if v.at != nil {
		c.text += " timestamp " + v.at.UTC().Format(time.RFC3339)
	}
	if id := r.Header.Get("fileidhash"); id != "" {
		switch strings.ToLower(id) {
		case "parquet", "delta":
			w.Header().Set("fileidhash", strings.ToLower(id))
		default:
			return badRequest("unsupported fileidhash %q; use parquet or delta", id)
		}
	}
	snap, err := s.source.snapshot(r.Context(), t.SourceSchema, t.SourceTable, v, true)
	if err != nil {
		return err
	}

	files := snap.files
	if q.LimitHint != nil && *q.LimitHint >= 0 {
		// Best effort, as the protocol allows: enough files for the rows
		// asked for.
		var rows int64
		for i, f := range files {
			if rows >= *q.LimitHint {
				files = files[:i]
				break
			}
			rows += f.count
		}
	}
	expires := s.now().Add(fileURLLifetime)
	base := s.baseURL(r)
	urls := make([]string, len(files))
	for i, f := range files {
		grant, err := s.signer.sealFile(fileGrant{
			Token: c.tokenID, Path: f.path, Size: f.size, Expires: expires.Unix(),
			Table: t.Share + "." + t.Schema + "." + t.Name,
		})
		if err != nil {
			return err
		}
		urls[i] = base + "/files/" + grant
	}

	caps := readCapabilities(r)
	lines := newLines(w, snap, caps)
	lines.protocol()
	lines.metadata(t, v.timeTravel())
	for i, f := range files {
		lines.file(f, urls[i], expires, v.timeTravel())
	}
	c.rows = int64(len(files))
	s.metrics.filesListed(len(files))
	return lines.err
}

func (s *Server) notSupported(http.ResponseWriter, *http.Request, *call) error {
	return badRequest("Igloo does not share change data feeds or cloud credentials; query a table snapshot instead")
}

// lines writes a newline-delimited JSON response in the parquet or delta
// format.
type lines struct {
	w    http.ResponseWriter
	enc  *json.Encoder
	snap *snapshot
	caps capabilities
	err  error
}

func newLines(w http.ResponseWriter, snap *snapshot, caps capabilities) *lines {
	h := w.Header()
	h.Set("Content-Type", "application/x-ndjson; charset=utf-8")
	h.Set("Delta-Table-Version", strconv.FormatInt(snap.version, 10))
	if caps.header != "" {
		h.Set("delta-sharing-capabilities", caps.header)
	}
	return &lines{w: w, enc: json.NewEncoder(w), snap: snap, caps: caps}
}

func (l *lines) write(v any) {
	if l.err == nil {
		l.err = l.enc.Encode(v)
	}
}

func (l *lines) protocol() {
	if l.caps.delta {
		l.write(map[string]any{"protocol": map[string]any{"deltaProtocol": map[string]int{"minReaderVersion": 1, "minWriterVersion": 2}}})
		return
	}
	l.write(map[string]any{"protocol": map[string]int{"minReaderVersion": 1}})
}

func (l *lines) metadata(t SharedTable, timeTravel bool) {
	meta := map[string]any{
		"id":               l.snap.tableID,
		"name":             t.Name,
		"format":           map[string]string{"provider": "parquet"},
		"schemaString":     l.snap.schema,
		"partitionColumns": []string{},
		"configuration":    map[string]string{},
	}
	if l.snap.comment != "" {
		meta["description"] = l.snap.comment
	}
	// Size and file count are optional, and known when files were listed
	// or the snapshot's summary has them.
	extra := map[string]any{}
	if l.snap.size > 0 {
		extra["size"] = l.snap.size
	}
	if len(l.snap.files) > 0 {
		extra["numFiles"] = len(l.snap.files)
	}
	if timeTravel {
		extra["version"] = l.snap.version
	}
	if l.caps.delta {
		// Delta's own metadata action, which Delta readers decode
		// strictly.
		meta["format"] = map[string]any{"provider": "parquet", "options": map[string]string{}}
		meta["createdTime"] = l.snap.timestamp
		extra["deltaMetadata"] = meta
		l.write(map[string]any{"metaData": extra})
		return
	}
	for k, v := range extra {
		meta[k] = v
	}
	l.write(map[string]any{"metaData": meta})
}

func (l *lines) file(f dataFile, url string, expires time.Time, timeTravel bool) {
	if l.caps.delta {
		add := map[string]any{
			"path": url, "partitionValues": map[string]string{}, "size": f.size,
			"modificationTime": l.snap.timestamp, "dataChange": true,
		}
		if f.stats != "" {
			add["stats"] = f.stats
		}
		file := map[string]any{"id": f.id(), "expirationTimestamp": expires.UnixMilli(), "deltaSingleAction": map[string]any{"add": add}}
		if timeTravel {
			file["version"], file["timestamp"] = l.snap.version, l.snap.timestamp
		}
		l.write(map[string]any{"file": file})
		return
	}
	file := map[string]any{
		"url": url, "id": f.id(), "partitionValues": map[string]string{}, "size": f.size,
		"expirationTimestamp": expires.UnixMilli(),
	}
	if f.stats != "" {
		file["stats"] = f.stats
	}
	if timeTravel {
		file["version"], file["timestamp"] = l.snap.version, l.snap.timestamp
	}
	l.write(map[string]any{"file": file})
}

// loopback reports whether host, from the Host header, names this machine.
func loopback(host string) bool {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.TrimSuffix(strings.Trim(host, "[]"), ".")
	if strings.EqualFold(host, "localhost") || strings.HasSuffix(strings.ToLower(host), ".localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
