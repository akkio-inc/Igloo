// Package flightsql serves Igloo over Apache Arrow Flight SQL, so data
// science and BI clients (ADBC, pyarrow, the Flight SQL JDBC driver) fetch
// query results as Arrow record batches, straight from DuckDB's columnar
// results, instead of row by row in the PostgreSQL protocol's text format.
//
// Clients sign in with a Flight handshake carrying HTTP basic
// authentication and receive a bearer token for a session, which lives as
// long as their connection. Every statement runs in that session, an
// embedded session of the PostgreSQL server (pgwire.EmbeddedSession), on
// the path a PostgreSQL client's statements take: the same authorization,
// statement timeout, cancellation, metrics and query history. This package
// never decides itself what a user may do.
package flightsql

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"net/netip"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	fsql "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"github.com/akkio-inc/igloo/internal/pgwire"
)

// Options configure the server.
type Options struct {
	// SQL checks passwords and runs statements.
	SQL *pgwire.Server
	// TLS, when set, encrypts every connection. Without it the server
	// serves only clients that connect to it as localhost.
	TLS *tls.Config
	// Version is Igloo's version, which clients read in SqlInfo.
	Version string
	Log     *slog.Logger
}

// Server is an Arrow Flight SQL front end for the SQL server.
type Server struct {
	sql  *pgwire.Server
	grpc *grpc.Server
	tls  bool
	log  *slog.Logger
	// sessions counts sessions that are open or closing.
	sessions sync.WaitGroup
}

// New builds the server; call Serve to accept connections.
func New(opts Options) (*Server, error) {
	if opts.SQL == nil {
		return nil, errors.New("flightsql: Options.SQL is required")
	}
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	s := &Server{sql: opts.SQL, tls: opts.TLS != nil, log: opts.Log.With("component", "flight sql")}

	h := &handler{s: s}
	if err := h.registerSqlInfo(opts.Version); err != nil {
		return nil, err
	}
	grpcOpts := []grpc.ServerOption{
		grpc.StatsHandler(connStats{s}),
		grpc.ChainUnaryInterceptor(s.unaryAuth),
		grpc.ChainStreamInterceptor(s.streamAuth),
	}
	if opts.TLS != nil {
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(opts.TLS.Clone())))
	}
	s.grpc = grpc.NewServer(grpcOpts...)
	flight.RegisterFlightServiceServer(s.grpc, &service{FlightServer: fsql.NewFlightServer(h), s: s})
	return s, nil
}

// Serve accepts connections on ln until Shutdown is called.
func (s *Server) Serve(ln net.Listener) error {
	err := s.grpc.Serve(ln)
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

// Shutdown stops accepting connections and waits for running calls to
// finish until ctx is done, then ends them. Sessions close with their
// connections.
func (s *Server) Shutdown(ctx context.Context) error {
	stopped := make(chan struct{})
	go func() {
		s.grpc.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-ctx.Done():
		s.grpc.Stop()
		<-stopped
	}
	closed := make(chan struct{})
	go func() {
		s.sessions.Wait()
		close(closed)
	}()
	select {
	case <-closed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// service is the Flight service: Flight SQL's routing of commands to the
// handler, plus the handshake.
type service struct {
	flight.FlightServer
	s *Server
}

// handshakeMethod is the one call a client makes before it has a session.
const handshakeMethod = "/arrow.flight.protocol.FlightService/Handshake"

// errUnencrypted refuses a client on a connection without TLS that does
// not reach the server as localhost, from this machine or a private
// network. It mirrors the web console's rule for sign-ins: basic
// authentication sends the password itself, so without TLS it may only
// travel inside this machine, or through an SSH tunnel or kubectl
// port-forward that encrypts it. The :authority catches a client sent
// across a network by mistake; since a client may send any, its address
// must be nearby too (see privatePeer).
var errUnencrypted = status.Error(codes.PermissionDenied,
	"this connection is not encrypted, so Igloo accepts Flight SQL clients only at localhost; "+
		"configure TLS (IGLOO_TLS_CERT_FILE and IGLOO_TLS_KEY_FILE) to connect from other machines")

// authorize admits a call: it must come over TLS or to localhost, and
// every call but the handshake must carry a session's bearer token. It
// returns ctx with the caller's session.
func (s *Server) authorize(ctx context.Context, method string) (context.Context, error) {
	if !s.tls && !(loopback(authority(ctx)) && privatePeer(peerAddr(ctx))) {
		return ctx, errUnencrypted
	}
	if method == handshakeMethod {
		return ctx, nil
	}
	sess, err := s.session(ctx)
	if err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, sessionKey{}, sess), nil
}

func (s *Server) unaryAuth(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	ctx, err := s.authorize(ctx, info.FullMethod)
	if err != nil {
		return nil, err
	}
	return h(ctx, req)
}

func (s *Server) streamAuth(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	ctx, err := s.authorize(ss.Context(), info.FullMethod)
	if err != nil {
		return err
	}
	return h(srv, &sessionStream{ServerStream: ss, ctx: ctx})
}

// sessionStream is a stream whose context carries the caller's session.
type sessionStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *sessionStream) Context() context.Context { return s.ctx }

// authority is the host and port the client connected to, from the
// request's :authority (HTTP/2's Host header).
func authority(ctx context.Context) string {
	if v := metadata.ValueFromIncomingContext(ctx, ":authority"); len(v) > 0 {
		return v[0]
	}
	return ""
}

// loopback reports whether host, with or without a port, names this
// machine, as the web console decides for its Host header.
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

// sharedAddressSpace is 100.64.0.0/10 (RFC 6598), which some Kubernetes
// networks give pods.
var sharedAddressSpace = netip.MustParsePrefix("100.64.0.0/10")

// privatePeer reports whether addr, a client's address as its connection
// reports it, is on this machine or a private network, as the Iceberg REST
// catalog requires of clients without TLS. It cannot be this machine
// alone: Docker's published ports, which the single-machine setup binds to
// loopback, present a client on the host as the bridge network's gateway.
func privatePeer(addr net.Addr) bool {
	if addr == nil {
		return false
	}
	ap, err := netip.ParseAddrPort(addr.String())
	if err != nil {
		return false
	}
	ip := ap.Addr().Unmap()
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || sharedAddressSpace.Contains(ip)
}

// connStats ties sessions to the connection they were opened on: each
// connection gets a conn in its context, and its sessions close when it
// does, as a PostgreSQL session ends with its connection.
type connStats struct{ s *Server }

type connKey struct{}

func (connStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return context.WithValue(ctx, connKey{}, &conn{})
}

func (h connStats) HandleConn(ctx context.Context, st stats.ConnStats) {
	if _, ok := st.(*stats.ConnEnd); ok {
		if c, ok := ctx.Value(connKey{}).(*conn); ok {
			c.close()
		}
	}
}

func (connStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (connStats) HandleRPC(context.Context, stats.RPCStats)                       {}

// emptySchema is the result schema of statements that return no rows.
var emptySchema = arrow.NewSchema(nil, nil)
