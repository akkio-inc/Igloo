// Package logwriter writes the process's log without ever holding up the code
// that logs. A write to stderr blocks while whatever reads it does not keep up:
// with stderr a pipe nobody read, statements logging at debug level stopped once
// the pipe's buffer was full. Lines go to a bounded buffer instead, which a
// goroutine of its own writes out; when the buffer is full, a line is dropped
// and counted, and a warning says how many were, at most once a minute.
package logwriter

import (
	"bytes"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultMax is how many bytes of lines a Writer holds before it drops them:
// a few seconds of a busy debug log.
const DefaultMax = 4 << 20

// warnEvery is how often at most a Writer reports the lines it dropped.
const warnEvery = time.Minute

// Writer is an io.Writer whose Write never blocks (see the package's comment).
// Each Write is one line, as a slog handler writes one record.
type Writer struct {
	out io.Writer
	max int

	mu sync.Mutex
	// queue are the lines waiting to be written, and held the bytes of them
	// and of those being written.
	queue  [][]byte
	held   int
	closed bool
	wake   chan struct{}
	done   chan struct{}

	dropped atomic.Uint64
	// warn, if set, is called from the writing goroutine with how many lines
	// were dropped since it was last called.
	warn     func(dropped uint64)
	reported uint64
	warned   time.Time
}

// New returns a Writer of lines to out that holds up to max bytes of them.
func New(out io.Writer, max int) *Writer {
	w := &Writer{out: out, max: max, wake: make(chan struct{}, 1), done: make(chan struct{})}
	go w.run()
	register(w)
	return w
}

// OnDrop has warn called, at most once a minute, with the number of lines the
// Writer dropped since it last was: the caller logs a warning through the
// Writer, which has room for it then.
func (w *Writer) OnDrop(warn func(dropped uint64)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.warn = warn
}

// Dropped is how many lines the Writer has dropped.
func (w *Writer) Dropped() uint64 { return w.dropped.Load() }

// Write queues a copy of p to be written, or drops it if the Writer holds as
// many bytes as it may. It never blocks, and never fails.
func (w *Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	if w.held+len(p) > w.max {
		w.mu.Unlock()
		w.dropped.Add(1)
		return len(p), nil
	}
	w.queue = append(w.queue, bytes.Clone(p))
	w.held += len(p)
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
	return len(p), nil
}

// run writes out what is queued, all of it at once, until the Writer is
// closed and nothing is left.
func (w *Writer) run() {
	defer close(w.done)
	for {
		w.mu.Lock()
		batch, closed := w.queue, w.closed
		w.queue = nil
		w.mu.Unlock()
		if len(batch) == 0 {
			if closed {
				return
			}
			<-w.wake
			continue
		}
		buf := bytes.Join(batch, nil)
		_, _ = w.out.Write(buf)
		w.mu.Lock()
		w.held -= len(buf)
		warn := w.warn
		w.mu.Unlock()
		if d := w.dropped.Load(); warn != nil && d > w.reported && time.Since(w.warned) >= warnEvery {
			n := d - w.reported
			w.reported, w.warned = d, time.Now()
			warn(n)
		}
	}
}

// Flush waits up to timeout for what the Writer holds to be written, the
// warning of lines it dropped among it, and has it stop once it holds nothing:
// what comes later is not written.
func (w *Writer) Flush(timeout time.Duration) {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
	select {
	case <-w.done:
	case <-time.After(timeout):
	}
}

// The process's Writers, which Exit flushes.
var (
	writersMu sync.Mutex
	writers   []*Writer
)

func register(w *Writer) {
	writersMu.Lock()
	defer writersMu.Unlock()
	writers = append(writers, w)
}

// exitFlush is how long Exit waits for the log to be written.
const exitFlush = 3 * time.Second

// FlushAll flushes every Writer of the process (see Flush), for what it logged
// last before it exits.
func FlushAll() {
	writersMu.Lock()
	all := append([]*Writer(nil), writers...)
	writersMu.Unlock()
	for _, w := range all {
		w.Flush(exitFlush)
	}
}

// Exit flushes the process's Writers and exits with code.
func Exit(code int) {
	FlushAll()
	os.Exit(code)
}
