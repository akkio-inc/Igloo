package pgwire

import (
	"encoding/binary"
	"io"
	"net"
	"syscall"
)

// Request codes a client may send before its startup message.
const (
	sslRequestCode      = 80877103
	cancelRequestCode   = 80877102
	gssEncRequestCode   = 80877104
	startupHeaderLength = 8
)

// requireTLS wraps l so that connections which start a session without
// first asking for TLS are refused with a PostgreSQL error. The protocol
// library can only mandate TLS together with client certificates, so the
// check happens here, on the first bytes of each connection. Cancel
// requests are let through: they carry no credentials and many clients send
// them unencrypted.
func requireTLS(l net.Listener) net.Listener { return tlsGate{l} }

type tlsGate struct{ net.Listener }

func (l tlsGate) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	// The check runs on the connection's first Read, in the connection's own
	// goroutine, so a slow client cannot stall Accept.
	return &gatedConn{Conn: c}, nil
}

type gatedConn struct {
	net.Conn
	// passed is set once the client has asked for TLS or sent a cancel
	// request; later bytes are not inspected.
	passed bool
	// replay holds peeked bytes still owed to the reader.
	replay []byte
}

func (c *gatedConn) Read(p []byte) (int, error) {
	if len(c.replay) == 0 && !c.passed {
		head := make([]byte, startupHeaderLength)
		if _, err := io.ReadFull(c.Conn, head); err != nil {
			return 0, err
		}
		switch binary.BigEndian.Uint32(head[4:]) {
		case sslRequestCode, cancelRequestCode:
			c.passed = true
		case gssEncRequestCode:
			// The server declines GSS encryption; the client then sends its
			// next request, which is checked in turn.
		default:
			_, _ = c.Conn.Write(fatalError("28000", "TLS is required; connect with sslmode=require"))
			_ = c.Conn.Close()
			return 0, io.EOF
		}
		c.replay = head
	}
	if len(c.replay) > 0 {
		n := copy(p, c.replay)
		c.replay = c.replay[n:]
		return n, nil
	}
	return c.Conn.Read(p)
}

// SyscallConn gives the connection's socket, to look at (see clientgone.go).
func (c *gatedConn) SyscallConn() (syscall.RawConn, error) { return innerSocket(c.Conn) }

// fatalError encodes a PostgreSQL ErrorResponse with severity FATAL.
func fatalError(code, message string) []byte {
	var fields []byte
	for _, f := range []struct {
		tag   byte
		value string
	}{{'S', "FATAL"}, {'V', "FATAL"}, {'C', code}, {'M', message}} {
		fields = append(fields, f.tag)
		fields = append(fields, f.value...)
		fields = append(fields, 0)
	}
	fields = append(fields, 0)

	msg := make([]byte, 5, 5+len(fields))
	msg[0] = 'E'
	binary.BigEndian.PutUint32(msg[1:], uint32(4+len(fields)))
	return append(msg, fields...)
}
