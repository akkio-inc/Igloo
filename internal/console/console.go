// Package console serves Igloo's web console: SQL worksheets, a data
// browser, query history, and pages for administrators to manage users,
// roles and grants, and to watch this replica's sessions.
//
// The console holds no state of its own. Passwords are checked by the SQL
// server (pgwire.Server.Authenticate), as for SQL logins, single sign-on by
// package sso (see sso.go), and a signed-in session lives in a sealed
// cookie, so any replica behind a load balancer serves any request. Everything the console shows or changes it reads or
// does with SQL, run as the signed-in user through an in-process SQL session
// (pgwire.EmbeddedSession) on the same path as statements from SQL clients:
// the console never decides itself what a user may see or do.
package console

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strings"
	"time"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/pgwire"
	"github.com/akkio-inc/igloo/internal/sso"
)

// Options configure the console.
type Options struct {
	// SQL runs statements and checks passwords.
	SQL *pgwire.Server
	// Users is re-read on every request, so dropped users and role changes
	// take effect at once.
	Users auth.Store
	// Key seals session cookies. Replicas behind one address need the same
	// key; it must be at least 32 random bytes.
	Key []byte
	// Status describes this replica on the compute page. It may be nil.
	Status func(context.Context) Status
	// SSO, if set, signs users in with an OpenID Connect identity
	// provider besides passwords.
	SSO *sso.Service
	// SCIM, if set, serves SCIM provisioning under /scim/v2/ (package
	// scim), which authenticates requests itself.
	SCIM http.Handler
	Log  *slog.Logger
}

// Status describes this replica on the compute page, besides its sessions.
type Status struct {
	Version string    `json:"version"`
	Started time.Time `json:"started"`
	// Maintenance is nil when this replica does not maintain tables.
	Maintenance *Maintenance `json:"maintenance"`
	// Running counts the statements and maintenance passes running now,
	// and IdleSeconds is the time since one started or ended: what the
	// scale-to-zero gateway reads to decide whether Igloo is idle.
	Running     int64   `json:"running"`
	IdleSeconds float64 `json:"idle_seconds"`
	// GatewaySeen is when a scale-to-zero gateway last asked this replica
	// whether it was busy; nil if none has since it started.
	GatewaySeen *time.Time `json:"gateway_seen"`
	// NextDue is when the soonest task, pipe or materialized view is due,
	// which a gateway starts Igloo for; nil if none is scheduled.
	NextDue *time.Time `json:"next_due"`
}

// Maintenance describes automatic table maintenance.
type Maintenance struct {
	Interval  string `json:"interval"`
	Retention string `json:"retention"`
	// LastPass is when any replica last finished a pass; nil if unknown.
	LastPass *time.Time `json:"last_pass"`
}

// Console is the console's HTTP handler.
type Console struct {
	sql     *pgwire.Server
	users   auth.Store
	cookies *sealer
	status  func(context.Context) Status
	sso     *sso.Service
	scim    http.Handler
	log     *slog.Logger
	// replica names this process on the compute page.
	replica string
	mux     *http.ServeMux
}

// New builds the console.
func New(opts Options) (*Console, error) {
	if opts.SQL == nil || opts.Users == nil {
		return nil, errors.New("console: Options.SQL and Options.Users are required")
	}
	cookies, err := newSealer(opts.Key)
	if err != nil {
		return nil, err
	}
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	if opts.Status == nil {
		opts.Status = func(context.Context) Status { return Status{} }
	}
	replica, _ := os.Hostname()
	c := &Console{
		sql:     opts.SQL,
		users:   opts.Users,
		cookies: cookies,
		status:  opts.Status,
		sso:     opts.SSO,
		scim:    opts.SCIM,
		log:     opts.Log.With("component", "console"),
		replica: replica,
		mux:     http.NewServeMux(),
	}
	c.routes()
	return c, nil
}

func (c *Console) routes() {
	c.mux.Handle("GET /", static())
	c.mux.HandleFunc("POST /api/login", c.login)
	c.mux.HandleFunc("GET /api/session", c.signedIn(c.session))
	c.mux.HandleFunc("POST /api/logout", c.signedIn(c.logout))
	c.mux.HandleFunc("POST /api/query", c.signedIn(c.query))
	c.mux.HandleFunc("POST /api/download", c.signedIn(c.download))
	c.mux.HandleFunc("GET /api/admin/users", c.admin(c.listUsers))
	c.mux.HandleFunc("POST /api/admin/users", c.admin(c.changeUser))
	c.mux.HandleFunc("GET /api/admin/grants", c.admin(c.listGrants))
	c.mux.HandleFunc("POST /api/admin/grants", c.admin(c.changeGrants))
	c.mux.HandleFunc("GET /api/admin/compute", c.admin(c.compute))
	c.mux.HandleFunc("POST /api/admin/cancel", c.admin(c.cancel))
	c.mux.HandleFunc("GET /api/admin/sso", c.admin(c.ssoStatus))
	c.mux.HandleFunc("GET /sso/start", c.ssoStart)
	c.mux.HandleFunc("GET /sso/callback", c.ssoCallback)
	for _, method := range []string{"GET", "POST", "PUT", "PATCH", "DELETE"} {
		c.mux.HandleFunc(method+" /scim/v2/", c.scimRequest)
	}
	notFound := func(w http.ResponseWriter, _ *http.Request) { writeError(w, http.StatusNotFound, "no such endpoint") }
	c.mux.HandleFunc("GET /api/", notFound)
	c.mux.HandleFunc("POST /api/", notFound)
}

// contentSecurityPolicy allows only the console's own scripts, styles and
// requests: no inline code, no framing, no other origins.
const contentSecurityPolicy = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self'; " +
	"connect-src 'self'; form-action 'none'; frame-ancestors 'none'; base-uri 'none'"

// ServeHTTP adds security headers to every response.
func (c *Console) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Content-Security-Policy", contentSecurityPolicy)
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("Cross-Origin-Opener-Policy", "same-origin")
	h.Set("Cross-Origin-Resource-Policy", "same-origin")
	h.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=(), usb=()")
	if strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/sso/") || strings.HasPrefix(r.URL.Path, "/scim/") {
		h.Set("Cache-Control", "no-store")
	}
	c.mux.ServeHTTP(w, r)
}

// scimRequest hands SCIM provisioning requests to its handler. Like a
// password, the bearer token they carry may cross a network only inside
// TLS.
func (c *Console) scimRequest(w http.ResponseWriter, r *http.Request) {
	if c.scim == nil {
		writeError(w, http.StatusNotFound, "SCIM provisioning needs single sign-on (IGLOO_OIDC_ISSUER) and the metadata database")
		return
	}
	if err := c.signInAllowed(r); err != nil {
		writeError(w, http.StatusForbidden, "this connection is not encrypted, so SCIM is accepted only at localhost; configure TLS")
		return
	}
	c.scim.ServeHTTP(w, r)
}

// writeJSON sends v as the response body.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError sends {"error": message}.
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// readJSON decodes a JSON request body of at most limit bytes into v. Only
// application/json is accepted: browsers cannot send it across origins
// without a CORS preflight, which the console never allows.
func readJSON(w http.ResponseWriter, r *http.Request, limit int64, v any) bool {
	if ct, _, _ := strings.Cut(r.Header.Get("Content-Type"), ";"); strings.TrimSpace(ct) != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "send application/json")
		return false
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit)).Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request: "+err.Error())
		return false
	}
	return true
}

// remoteAddr is the client's address as a net.Addr, for logs.
func remoteAddr(r *http.Request) net.Addr {
	ap, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil {
		return nil
	}
	return net.TCPAddrFromAddrPort(ap)
}

// client describes a console user's SQL session in the list of sessions.
func client(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return "console " + host
}
