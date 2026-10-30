package wire

import (
	"context"
	"net"
)

// ctxConn holds the connection the listener accepted for a client.
type ctxConn struct{}

// AcceptedConn returns the connection the server's listener accepted for the
// client serving ctx, as it was before any TLS, so that a listener that
// wraps its connections can be told when the client has signed in.
func AcceptedConn(ctx context.Context) net.Conn {
	conn, _ := ctx.Value(ctxConn{}).(net.Conn)
	return conn
}
