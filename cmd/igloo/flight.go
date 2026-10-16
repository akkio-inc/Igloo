package main

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"

	"github.com/akkio-inc/igloo/internal/flightsql"
	"github.com/akkio-inc/igloo/internal/pgwire"
)

// listenFlight opens the Arrow Flight SQL listener. Like the console's, it
// opens before the SQL port, which the scale-to-zero gateway waits for.
func listenFlight(addr string) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("flight sql: %w", err)
	}
	return ln, nil
}

// serveFlight serves Arrow Flight SQL on ln, over TLS with the SQL port's
// certificate when TLS is configured. Serving errors go to errc.
func serveFlight(ln net.Listener, srv *pgwire.Server, tlsConfig *tls.Config, errc chan<- error, log *slog.Logger) (*flightsql.Server, error) {
	fs, err := flightsql.New(flightsql.Options{SQL: srv, TLS: tlsConfig, Version: version, Log: log})
	if err != nil {
		return nil, err
	}
	if tlsConfig == nil {
		log.Warn("Arrow Flight SQL is served without TLS; it accepts clients only at localhost until IGLOO_TLS_CERT_FILE and IGLOO_TLS_KEY_FILE are set")
	}
	go func() {
		if err := fs.Serve(ln); err != nil {
			errc <- fmt.Errorf("flight sql: %w", err)
		}
	}()
	return fs, nil
}
