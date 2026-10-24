//go:build linux

package prefetch

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

// openWatch tells which of the blocks the prefetcher wrote have been
// opened since, from the kernel's file events (inotify) on the cache's
// directory, which come whatever the file system does with access times:
// cache_httpfs opens a block's file to read it. The prefetcher opens only
// footer blocks itself, which it does not track.
type openWatch struct {
	f      *os.File
	mu     sync.Mutex
	blocks map[string]*watched
	done   chan struct{}
}

type watched struct{ read, gone bool }

func newOpenWatch(dir string) (*openWatch, error) {
	fd, err := unix.InotifyInit1(unix.IN_CLOEXEC | unix.IN_NONBLOCK)
	if err != nil {
		return nil, err
	}
	if _, err := unix.InotifyAddWatch(fd, dir, unix.IN_OPEN|unix.IN_DELETE|unix.IN_MOVED_FROM); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	// Non-blocking, so the runtime's poller reads it and Close stops a
	// read in progress.
	w := &openWatch{f: os.NewFile(uintptr(fd), "inotify"), blocks: map[string]*watched{}, done: make(chan struct{})}
	go w.run()
	return w, nil
}

func (w *openWatch) run() {
	defer close(w.done)
	buf := make([]byte, 256<<10)
	for {
		n, err := w.f.Read(buf)
		if err != nil {
			return
		}
		w.mu.Lock()
		for off := 0; off+unix.SizeofInotifyEvent <= n; {
			mask := binary.NativeEndian.Uint32(buf[off+4:])
			size := int(binary.NativeEndian.Uint32(buf[off+12:]))
			name := buf[off+unix.SizeofInotifyEvent : min(n, off+unix.SizeofInotifyEvent+size)]
			if i := bytes.IndexByte(name, 0); i >= 0 {
				name = name[:i]
			}
			off += unix.SizeofInotifyEvent + size
			if mask&unix.IN_Q_OVERFLOW != 0 {
				// Events were lost: take every block for read rather
				// than hold statements back for good.
				for _, b := range w.blocks {
					b.read = true
				}
				continue
			}
			b := w.blocks[string(name)]
			switch {
			case b == nil:
			case mask&unix.IN_OPEN != 0:
				b.read = true
			case mask&(unix.IN_DELETE|unix.IN_MOVED_FROM) != 0:
				b.gone = true
			}
		}
		w.mu.Unlock()
	}
}

// track starts watching the block file name for opens, before it is
// renamed into place.
func (w *openWatch) track(name string) {
	w.mu.Lock()
	w.blocks[name] = &watched{}
	w.mu.Unlock()
}

// untrack stops watching it.
func (w *openWatch) untrack(name string) {
	w.mu.Lock()
	delete(w.blocks, name)
	w.mu.Unlock()
}

// readSince reports whether the block was opened since it was tracked; ok
// is false when it is gone, or not tracked.
func (w *openWatch) readSince(name string) (read, ok bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	b := w.blocks[name]
	if b == nil {
		return false, false
	}
	return b.read, !b.gone
}

func (w *openWatch) close() {
	_ = w.f.Close()
	<-w.done
}
