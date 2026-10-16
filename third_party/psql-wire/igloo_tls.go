package wire

import (
	"context"
	"crypto/tls"
	"net"
)

// ctxTLS marks the context of a connection that negotiated TLS.
type ctxTLS struct{}

func setTLS(ctx context.Context, conn net.Conn) context.Context {
	_, ok := conn.(*tls.Conn)
	return context.WithValue(ctx, ctxTLS{}, ok)
}

// IsTLS reports whether the client connection serving ctx is encrypted with
// TLS, so an authentication strategy can decide whether a secret may be sent
// in clear text inside it.
func IsTLS(ctx context.Context) bool {
	ok, _ := ctx.Value(ctxTLS{}).(bool)
	return ok
}
