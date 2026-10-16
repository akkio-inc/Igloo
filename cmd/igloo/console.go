package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/akkio-inc/igloo/internal/activity"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/console"
	"github.com/akkio-inc/igloo/internal/maintenance"
	"github.com/akkio-inc/igloo/internal/pgwire"
	"github.com/akkio-inc/igloo/internal/sso"
)

// consoleSecret names the key in igloo_auth.secrets that seals console
// session cookies.
const consoleSecret = "console-session"

// listenConsole opens the web console's listener. It opens before the SQL
// port, so that once the SQL port accepts connections (which is what the
// scale-to-zero gateway waits for) the console does too.
func listenConsole(addr string) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("console: %w", err)
	}
	return ln, nil
}

// serveConsole serves the web console on ln, over HTTPS with the SQL
// port's certificate when TLS is configured, with single sign-on if single
// is not nil and SCIM provisioning if provisioning is not. Serving errors
// go to errc.
func serveConsole(ctx context.Context, ln net.Listener, srv *pgwire.Server, users auth.Store, single *sso.Service,
	provisioning http.Handler, tlsConfig *tls.Config, status func(context.Context) console.Status, errc chan<- error,
	log *slog.Logger) (*http.Server, error) {
	key, err := consoleKey(ctx, users)
	if err != nil {
		return nil, fmt.Errorf("console session key: %w", err)
	}
	handler, err := console.New(console.Options{SQL: srv, Users: users, Key: key, Status: status, SSO: single,
		SCIM: provisioning, Log: log})
	if err != nil {
		return nil, err
	}
	httpSrv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		// Bounds reading a request; a response, such as a long query's
		// result, may take as long as it needs.
		ReadTimeout:    30 * time.Second,
		IdleTimeout:    2 * time.Minute,
		MaxHeaderBytes: 64 << 10,
		// TLS handshake failures from scanners are not worth a warning.
		ErrorLog: slog.NewLogLogger(log.Handler(), slog.LevelDebug),
	}
	serve := func() error { return httpSrv.Serve(ln) }
	if tlsConfig != nil {
		httpSrv.TLSConfig = tlsConfig.Clone()
		serve = func() error { return httpSrv.ServeTLS(ln, "", "") }
	} else {
		log.Warn("the web console is served without TLS; it accepts sign-ins only from a browser at localhost until IGLOO_TLS_CERT_FILE and IGLOO_TLS_KEY_FILE are set")
	}
	go func() {
		if err := serve(); !errors.Is(err, http.ErrServerClosed) {
			errc <- fmt.Errorf("console: %w", err)
		}
	}()
	return httpSrv, nil
}

// consoleKey returns the key that seals console session cookies: shared by
// every replica through the metadata database, so a session works on any
// of them. Without a metadata database there is one replica, and a key of
// its own, which signs everyone out when it restarts, is enough.
func consoleKey(ctx context.Context, users auth.Store) ([]byte, error) {
	if pg, ok := users.(*auth.PostgresStore); ok {
		return pg.Secret(ctx, consoleSecret)
	}
	key := make([]byte, 32)
	_, err := rand.Read(key)
	return key, err
}

// shortDuration renders d without zero units: 1h rather than 1h0m0s, and
// whole days as 7d.
func shortDuration(d time.Duration) string {
	if d >= 24*time.Hour && d%(24*time.Hour) == 0 {
		return fmt.Sprintf("%dd", d/(24*time.Hour))
	}
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}

// consoleStatus describes this replica on the console's compute page.
// schedule, when not nil, records maintenance passes across replicas.
func consoleStatus(cfg config.Config, started time.Time, work *activity.Tracker, h *health,
	schedule maintenance.Schedule, maintains bool) func(context.Context) console.Status {
	return func(ctx context.Context) console.Status {
		running, idle := work.Snapshot()
		st := console.Status{Version: version, Started: started, Running: running, IdleSeconds: idle.Seconds()}
		if t := h.gatewaySeen.Load(); t != 0 {
			seen := time.Unix(0, t)
			st.GatewaySeen = &seen
		}
		if due, ok := h.nextDue(ctx); ok {
			st.NextDue = &due
		}
		if maintains {
			st.Maintenance = &console.Maintenance{
				Interval:  shortDuration(cfg.Maintenance.Interval),
				Retention: shortDuration(cfg.Maintenance.Retention),
			}
			if schedule != nil {
				if at, ok, err := maintenance.LastPass(ctx, schedule); err == nil && ok {
					st.Maintenance.LastPass = &at
				}
			}
		}
		return st
	}
}
