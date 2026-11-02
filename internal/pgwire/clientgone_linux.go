package pgwire

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// socketClosed reports whether the peer of socket has closed it, or it failed,
// without reading it: messages the client sent before it closed, which reading
// would have to get through first, do not hide that it did.
func socketClosed(socket syscall.RawConn) bool {
	closed := false
	err := socket.Control(func(fd uintptr) {
		fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLRDHUP}}
		if n, err := unix.Poll(fds, 0); err == nil && n > 0 {
			closed = fds[0].Revents&(unix.POLLRDHUP|unix.POLLHUP|unix.POLLERR|unix.POLLNVAL) != 0
		}
	})
	return closed || err != nil
}
