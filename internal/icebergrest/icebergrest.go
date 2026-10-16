// Package icebergrest serves Igloo's tables to other engines (Spark, Trino,
// PyIceberg, Snowflake, Databricks) through the Iceberg REST catalog
// protocol, under Igloo's users, grants and history.
//
// Igloo stands in front of its Lakekeeper catalog, which stays reachable by
// Igloo alone. Engines sign in as an Igloo user with the protocol's OAuth2
// client-credentials flow (/v1/oauth/tokens) and get a sealed token of
// Igloo's own. Every request is then checked against the user's built-in
// role and grants, as the matching SQL statement would be, recorded in the
// query history, and only then forwarded to Lakekeeper. Engines never learn
// Lakekeeper's address or any long-lived storage credentials: with a table
// they get, when they ask, credentials that last an hour and reach only
// that table's files, read-only unless the user may change all of its rows
// (see Vendor).
//
// Two other designs were considered and rejected:
//
//   - Lakekeeper's own authentication and authorization. Lakekeeper accepts
//     only JWTs from an OpenID Connect provider and keeps permissions in
//     OpenFGA, a separate service with its own database (its policy-based
//     authorizer is not open source). Igloo would have to become an
//     identity provider and copy every grant into OpenFGA, keeping two
//     sources of truth in step, engine access would be missing from Igloo's
//     history, and Igloo would need credentials for its own catalog.
//   - Lakekeeper's remote request signing instead of credentials. It needs
//     no cloud role, but Trino, Snowflake and Databricks cannot use it, and
//     without an authorizer Lakekeeper signs writes for readers too.
//
// What each request needs:
//
//	list, describe       any privilege on the table, or in the schema
//	load a table         SELECT; write credentials need INSERT, UPDATE, DELETE
//	commit               INSERT, UPDATE and DELETE to change rows (see
//	                     classify); CREATE on the schema for schema, partition
//	                     and sort changes; the writer role for table properties
//	                     and snapshot removal
//	create, drop, rename CREATE on the schema
//	schemas              the writer role to create, drop or change them
//
// Built-in roles count as in SQL: reader may read every table, writer may
// do everything here but what only admins may: choose table and schema
// locations, move a table or change its UUID, use nested namespaces, and
// move tables between schemas, where other schemas' grants would reach
// them. Tables with data policies (Options.Protected) are admins' alone
// here, since their files hold the data the policies hide.
//
// Igloo must check the request Lakekeeper will carry out, so it reads
// requests as Lakekeeper does: names in paths with "+" as a space, bodies
// with exact keys (readJSON), and a commit's body identifier only when it
// is the path's. Views, table registration and multi-table transactions
// are not served.
package icebergrest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/akkio-inc/igloo/internal/activity"
	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
)

// Options configure the endpoint.
type Options struct {
	// CatalogURI is Lakekeeper's Iceberg REST URL (IGLOO_CATALOG_URI), and
	// Warehouse the warehouse in it that Igloo serves.
	CatalogURI string
	Warehouse  string
	// CatalogName is the catalog's name in SQL, which names tables in the
	// history: lake.public.events.
	CatalogName string
	// Users is re-read on every request, so dropped users, role changes and
	// new passwords take effect at once; Grants likewise.
	Users  auth.Store
	Grants auth.GrantStore
	// Authenticate checks a password as a SQL login does: failures are
	// delayed and count towards the failed-login limits, and every outcome
	// is logged, counted and kept in the login history. It returns
	// ErrLoginFailed for an unknown user or a wrong password,
	// ErrUserDisabled for a user who may not sign in, and a
	// *LoginBlockedError while the limits refuse passwords.
	Authenticate func(ctx context.Context, name, password string, addr net.Addr) (auth.User, error)
	// AuthenticateToken checks an Igloo access token (igloo_...) as a SQL
	// login with one does, and returns its user and the token; name, when
	// not empty, must be the token's user. It returns ErrLoginFailed for a
	// token that does not sign in: malformed, unknown, dropped or expired,
	// another user's, or a disabled user's. Nil, or a nil Tokens, means
	// engines sign in with passwords only.
	AuthenticateToken func(ctx context.Context, name, secret string, addr net.Addr) (auth.User, auth.Token, error)
	// Tokens keeps access tokens. A session that began with one is checked
	// against it on every request, so dropping the token ends the session.
	Tokens auth.TokenStore
	// Key seals access tokens. Replicas behind one address need the same
	// key; it must be at least 32 random bytes.
	Key []byte
	// Credentials hands out storage credentials limited to one table. Nil
	// hands out none: engines then need their own access to the files.
	Credentials Vendor
	// Protected reports whether a table has data policies (masking or row
	// access), which Igloo applies to the SQL it runs: an engine reading the
	// table's files would bypass them, so non-admins may neither load nor
	// change such a table here. It is asked on every request, since
	// policies change. Nil means no table has any.
	Protected func(ctx context.Context, o auth.Object) (bool, error)
	// TLS says whether requests arrive over TLS. Without it, only clients
	// on private networks are served, since tokens, passwords and storage
	// credentials cross the connection.
	TLS bool
	// HTTP calls Lakekeeper. Nil means a client with a timeout.
	HTTP *http.Client
	// Audit, Activity and Metrics may be nil.
	Audit    *audit.Log
	Activity *activity.Tracker
	Metrics  *Metrics
	Log      *slog.Logger
}

// Server is the endpoint's HTTP handler.
type Server struct {
	opts   Options
	cat    *upstream
	tokens *sealer
	log    *slog.Logger
	mux    *http.ServeMux
	// delayed and signIns hold a slot per failed request waiting out its
	// delay and per token request being served.
	delayed, signIns chan struct{}
}

// New builds the endpoint.
func New(opts Options) (*Server, error) {
	if opts.CatalogURI == "" || opts.Warehouse == "" || opts.Users == nil || opts.Grants == nil || opts.Authenticate == nil {
		return nil, errors.New("icebergrest: CatalogURI, Warehouse, Users, Grants and Authenticate are required")
	}
	tokens, err := newSealer(opts.Key)
	if err != nil {
		return nil, err
	}
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	if opts.HTTP == nil {
		opts.HTTP = &http.Client{Timeout: 2 * time.Minute}
	}
	log := opts.Log.With("component", "iceberg")
	s := &Server{
		opts:    opts,
		cat:     &upstream{base: strings.TrimSuffix(opts.CatalogURI, "/"), warehouse: opts.Warehouse, http: opts.HTTP, log: log},
		tokens:  tokens,
		log:     log,
		mux:     http.NewServeMux(),
		delayed: make(chan struct{}, maxDelayed),
		signIns: make(chan struct{}, maxSignIns),
	}
	s.routes()
	return s, nil
}

// Operations, named as in the Iceberg REST specification. They are the
// statement types of requests in the query history and label metrics.
const (
	opConfig          = "getConfig"
	opToken           = "getToken"
	opListNamespaces  = "listNamespaces"
	opCreateNamespace = "createNamespace"
	opLoadNamespace   = "loadNamespaceMetadata"
	opNamespaceExists = "namespaceExists"
	opDropNamespace   = "dropNamespace"
	opUpdateNamespace = "updateProperties"
	opListTables      = "listTables"
	opCreateTable     = "createTable"
	opLoadTable       = "loadTable"
	opTableExists     = "tableExists"
	opUpdateTable     = "updateTable"
	opDropTable       = "dropTable"
	opRenameTable     = "renameTable"
	opLoadCredentials = "loadCredentials"
	opReportMetrics   = "reportMetrics"
	opUnsupported     = "unsupported"
)

// Error types of the Iceberg REST specification.
const (
	errBadRequest      = "BadRequestException"
	errNotAuthorized   = "NotAuthorizedException"
	errForbidden       = "ForbiddenException"
	errNoSuchWarehouse = "NoSuchWarehouseException"
	errUnsupported     = "UnsupportedOperationException"
	errInternal        = "InternalServerError"
	errUnavailable     = "ServiceUnavailableException"
)

const (
	namespacePath  = "/v1/namespaces/{namespace}"
	tablePath      = namespacePath + "/tables/{table}"
	maxRequestBody = 16 << 20
	// accessDelegation is the header in which clients ask for storage
	// credentials (vendedCredentials) or request signing.
	accessDelegation  = "X-Iceberg-Access-Delegation"
	vendedCredentials = "vended-credentials"
)

// endpoints are what /v1/config advertises; clients call nothing else.
var endpoints = []string{
	"GET /v1/{prefix}/namespaces",
	"POST /v1/{prefix}/namespaces",
	"GET /v1/{prefix}/namespaces/{namespace}",
	"HEAD /v1/{prefix}/namespaces/{namespace}",
	"DELETE /v1/{prefix}/namespaces/{namespace}",
	"POST /v1/{prefix}/namespaces/{namespace}/properties",
	"GET /v1/{prefix}/namespaces/{namespace}/tables",
	"POST /v1/{prefix}/namespaces/{namespace}/tables",
	"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
	"HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
	"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
	"DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
	"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
	"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
	"POST /v1/{prefix}/tables/rename",
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /v1/oauth/tokens", s.token)
	s.mux.HandleFunc("GET /v1/config", s.handle(opConfig, s.config))
	s.mux.HandleFunc("GET /v1/namespaces", s.handle(opListNamespaces, s.listNamespaces))
	s.mux.HandleFunc("POST /v1/namespaces", s.handle(opCreateNamespace, s.createNamespace))
	s.mux.HandleFunc("GET "+namespacePath, s.handle(opLoadNamespace, s.loadNamespace))
	s.mux.HandleFunc("HEAD "+namespacePath, s.handle(opNamespaceExists, s.namespaceExists))
	s.mux.HandleFunc("DELETE "+namespacePath, s.handle(opDropNamespace, s.dropNamespace))
	s.mux.HandleFunc("POST "+namespacePath+"/properties", s.handle(opUpdateNamespace, s.updateNamespace))
	s.mux.HandleFunc("GET "+namespacePath+"/tables", s.handle(opListTables, s.listTables))
	s.mux.HandleFunc("POST "+namespacePath+"/tables", s.handle(opCreateTable, s.createTable))
	s.mux.HandleFunc("GET "+tablePath, s.handle(opLoadTable, s.loadTable))
	s.mux.HandleFunc("HEAD "+tablePath, s.handle(opTableExists, s.tableExists))
	s.mux.HandleFunc("POST "+tablePath, s.handle(opUpdateTable, s.updateTable))
	s.mux.HandleFunc("DELETE "+tablePath, s.handle(opDropTable, s.dropTable))
	s.mux.HandleFunc("GET "+tablePath+"/credentials", s.handle(opLoadCredentials, s.loadCredentials))
	s.mux.HandleFunc("POST "+tablePath+"/metrics", s.handle(opReportMetrics, s.reportMetrics))
	s.mux.HandleFunc("POST /v1/tables/rename", s.handle(opRenameTable, s.renameTable))
	// Views, registering tables, multi-table commits, scan planning.
	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		s.opts.Metrics.request(opUnsupported, outcomeError)
		writeError(w, r, errorf(http.StatusNotImplemented, errUnsupported,
			"%s %s is not supported by Igloo's Iceberg REST endpoint", r.Method, r.URL.Path))
	})
}

// ServeHTTP serves a request, refusing clients outside private networks
// when the endpoint has no TLS.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	if !s.opts.TLS && !privateNetwork(r.RemoteAddr) {
		s.log.Warn("refused a client outside a private network: the endpoint has no TLS", "addr", r.RemoteAddr)
		writeError(w, r, errorf(http.StatusForbidden, errForbidden,
			"this endpoint is not encrypted, so it serves clients on private networks only; "+
				"configure TLS (IGLOO_TLS_CERT_FILE and IGLOO_TLS_KEY_FILE) to serve others"))
		return
	}
	s.mux.ServeHTTP(w, r)
}

// sharedAddressSpace is 100.64.0.0/10 (RFC 6598), which some Kubernetes
// networks give pods.
var sharedAddressSpace = netip.MustParsePrefix("100.64.0.0/10")

// privateNetwork reports whether addr, a request's remote address, is on
// this machine or a private network.
func privateNetwork(addr string) bool {
	ap, err := netip.ParseAddrPort(addr)
	if err != nil {
		return false
	}
	ip := ap.Addr().Unmap()
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || sharedAddressSpace.Contains(ip)
}

// call is one catalog request being served.
type call struct {
	w   http.ResponseWriter
	r   *http.Request
	op  string
	who *caller
	// read and written are the tables the request read or changed, for the
	// history.
	read, written []auth.Object
	// detail adds to the request in the history, such as which
	// credentials it got.
	detail string
}

func (c *call) ctx() context.Context { return c.r.Context() }

// handle wraps an operation: it authenticates the caller, runs fn, writes
// fn's error if any, and records the request.
func (s *Server) handle(op string, fn func(*call) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		defer s.opts.Activity.Begin()()
		c := &call{w: w, r: r, op: op}
		who, err := s.authenticate(r)
		if err == nil {
			c.who = who
			err = fn(c)
		}
		if err != nil {
			var api *apiError
			if !errors.As(err, &api) {
				s.log.Error("request failed", "op", op, "err", err)
				err = errorf(http.StatusInternalServerError, errInternal, "internal error; see Igloo's log")
			}
			writeError(w, r, err)
		}
		s.record(c, start, err)
	}
}

// apiError is an error response in the Iceberg REST format.
type apiError struct {
	status int
	// kind is the Iceberg error type, such as ForbiddenException.
	kind string
	msg  string
}

func (e *apiError) Error() string { return e.msg }

func errorf(status int, kind, format string, args ...any) *apiError {
	return &apiError{status: status, kind: kind, msg: fmt.Sprintf(format, args...)}
}

func writeError(w http.ResponseWriter, r *http.Request, err error) {
	var api *apiError
	if !errors.As(err, &api) {
		api = errorf(http.StatusInternalServerError, errInternal, "internal error")
	}
	if api.status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer realm="igloo"`)
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(api.status)
		return
	}
	writeJSON(w, api.status, map[string]any{"error": map[string]any{"message": api.msg, "type": api.kind, "code": api.status}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeBody sends a JSON body as is.
func writeBody(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// readJSON reads a JSON request body of at most maxRequestBody bytes into
// v, and returns the body to forward to Lakekeeper: the same document,
// encoded again. Igloo decides on what Go decodes, which matches keys to
// fields whatever their case and keeps the last of repeated keys, while
// Lakekeeper matches keys exactly; a body could otherwise mean one thing
// to Igloo and another to Lakekeeper. Encoding again leaves no repeated
// keys, and objects with keys that differ only in case are refused.
func readJSON(c *call, v any) ([]byte, error) {
	body, err := io.ReadAll(http.MaxBytesReader(c.w, c.r.Body, maxRequestBody))
	if err != nil {
		return nil, errorf(http.StatusBadRequest, errBadRequest, "could not read the request: %v", err)
	}
	malformed := func(err error) error {
		return errorf(http.StatusBadRequest, errBadRequest, "malformed request: %v", err)
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber() // keeps 64-bit snapshot IDs exact
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return nil, malformed(err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, malformed(errors.New("data after the JSON document"))
	}
	if key := caseVariant(doc); key != "" {
		return nil, malformed(fmt.Errorf("key %q appears in more than one case", key))
	}
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(doc); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(out.Bytes(), v); err != nil {
		return nil, malformed(err)
	}
	return out.Bytes(), nil
}

// withoutUpdates returns body, a commit as readJSON returned it, without
// its updates of the given actions.
func withoutUpdates(body []byte, actions ...string) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	updates, ok := doc["updates"].([]any)
	if !ok {
		return body, nil
	}
	doc["updates"] = slices.DeleteFunc(updates, func(u any) bool {
		m, _ := u.(map[string]any)
		action, _ := m["action"].(string)
		return slices.Contains(actions, action)
	})
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(doc); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// caseVariant returns a key of an object in doc that another key of the
// same object equals but for case, as Go's decoder compares them, or "".
func caseVariant(doc any) string {
	switch d := doc.(type) {
	case map[string]any:
		seen := make(map[string]bool, len(d))
		for k, v := range d {
			folded := fold(k)
			if seen[folded] {
				return k
			}
			seen[folded] = true
			if key := caseVariant(v); key != "" {
				return key
			}
		}
	case []any:
		for _, v := range d {
			if key := caseVariant(v); key != "" {
				return key
			}
		}
	}
	return ""
}

// fold maps s to one spelling per case-insensitive equivalence class, under
// Unicode simple case folding, as strings.EqualFold compares.
func fold(s string) string {
	return strings.Map(func(r rune) rune {
		least := r
		for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
			least = min(least, f)
		}
		return least
	}, s)
}
