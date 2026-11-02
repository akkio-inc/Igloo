//go:build !linux

package pgwire

import (
	"errors"
	"syscall"
)

// socketClosed reports whether the peer of socket has closed it, or it failed,
// without reading it. Where there is no POLLRDHUP, a client that closed after
// sending messages nobody has read yet shows only once they have been.
func socketClosed(socket syscall.RawConn) bool {
	closed := false
	err := socket.Control(func(fd uintptr) {
		var b [1]byte
		n, _, err := syscall.Recvfrom(int(fd), b[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
		switch {
		case err == nil:
			closed = n == 0
		case !errors.Is(err, syscall.EAGAIN) && !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EINTR):
			closed = true
		}
	})
	return closed || err != nil
}
