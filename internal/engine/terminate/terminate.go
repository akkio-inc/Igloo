// Package terminate ends the process at once when a C++ exception escapes
// DuckDB or one of its extensions, rather than abort it; on a thread in a call
// a statement guards (see Statement), it parks the thread and ends only that
// statement and its session.
//
// C++ calls std::terminate for an exception that nothing catches: one
// thrown on a thread DuckDB started that its code lets out, or one thrown
// while another unwinds, or out of a destructor, as when a cleanup that pins
// memory runs out of it too. The default handler prints the exception and
// calls abort(), which raises SIGABRT, and Go's signal handler takes that on a
// thread Go does not run. On two workers whose memory ran out ("failed to pin
// block of size 256.0 KiB"), the process neither died nor went on: one thread
// spun in Go's runtime and every other waited, for over half an hour, while
// Docker, seeing it run, restarted nothing and the statements whose tasks it
// held waited too.
//
// The handler here writes to standard error what escaped (its type and its
// message) and the frames of the thread it escaped on, with nothing that
// could wait on a lock or allocate: no stack unwinding, no Go code, no signal.
// On any thread but one in a call a statement guards (see Statement) it then
// calls _exit(134), the status of a process aborted by SIGABRT, and whatever
// runs the process restarts it.
//
// A thread in a statement's call into DuckDB holds what that statement does
// and nothing of anyone else's: the exception stopped in DuckDB's code there,
// and unwound what it had begun beneath, as an Out of Memory error thrown out
// of the iceberg extension's destructor of a scan's manifest tasks did, in the
// optimizer of a statement bound again at a replica's memory limit. So that
// thread waits for good instead, and says so (see Watch), and Go ends its
// statement and session: what those held of Igloo's, it lets go of, and the
// thread keeps only its DuckDB connection, and what DuckDB held for it. Past
// 64 such threads, the process ends as on any other.
package terminate

/*
#cgo CXXFLAGS: -std=c++17
void igloo_set_terminate(void);
void igloo_park_fd(int fd);
long igloo_statement_enter(void);
void igloo_statement_leave(void);
*/
import "C"

import (
	"bufio"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

var (
	// park is what the handler writes parked threads' records to, kept open
	// for as long as the process runs.
	park *os.File
	// statements are the threads statements run on, by their ids, with what
	// ends each where it parks.
	statementsMu sync.Mutex
	statements   = map[int64]func(what string){}
	// notify, if set, hears of every parked thread (see Watch).
	notifyMu sync.Mutex
	notify   func(what string, known bool)
)

// Install sets the process's C++ terminate handler. It holds for every
// thread, DuckDB's own and its extensions', which share the process's C++
// runtime.
func Install() {
	C.igloo_set_terminate()
	r, w, err := os.Pipe()
	if err != nil {
		return
	}
	park = w
	C.igloo_park_fd(C.int(w.Fd()))
	go parked(r)
}

// Watch has notify hear of every thread the handler parks, with what escaped
// on it, and whether a statement's end took it (see Statement).
func Watch(n func(what string, known bool)) {
	notifyMu.Lock()
	defer notifyMu.Unlock()
	notify = n
}

// Statement runs fn, a statement's call into DuckDB, on the thread it is
// called on, and nothing else on that thread meanwhile: an exception that
// escapes DuckDB there, which nothing can catch, parks the thread for good,
// and ends calls ended with what escaped. Calls within fn's run use the
// outer's end.
//
// fn must not switch to or from a coroutine (iter.Pull) made outside it: Go
// requires every switch to one to be on the thread, and under the thread
// locks, it was made with, and Statement locks the goroutine to its thread
// for fn. Guarding each call, not a client's whole command, keeps that: a
// portal read a few rows at a time resumes in each command the coroutine its
// first read made, and with commands guarded whole, JDBC's fetch size ended
// the process.
func Statement(ended func(what string), fn func()) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	id := int64(C.igloo_statement_enter())
	defer C.igloo_statement_leave()
	statementsMu.Lock()
	_, inner := statements[id]
	if !inner {
		statements[id] = ended
	}
	statementsMu.Unlock()
	if !inner {
		defer func() {
			statementsMu.Lock()
			delete(statements, id)
			statementsMu.Unlock()
		}()
	}
	fn()
}

// parked reads the handler's records, a thread's id and what escaped on it a
// line, and ends the statement each thread ran.
func parked(r *os.File) {
	s := bufio.NewScanner(r)
	for s.Scan() {
		tid, what, _ := strings.Cut(s.Text(), "\t")
		id, _ := strconv.ParseInt(tid, 10, 64)
		statementsMu.Lock()
		ended, known := statements[id]
		statementsMu.Unlock()
		notifyMu.Lock()
		n := notify
		notifyMu.Unlock()
		if n != nil {
			n(what, known)
		}
		if known {
			go ended(what)
		}
	}
}
