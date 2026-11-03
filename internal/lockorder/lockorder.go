// Package lockorder keeps one order between the catalog lock and the server's own
// mutexes: the catalog lock first. A goroutine that holds one of the mutexes must not
// wait for the catalog lock, as another may hold the catalog lock and wait for the
// mutex. A session preparing a statement, holding the catalog lock, waited for a
// mutex that a session waiting for the catalog lock held, and a replica froze for 16
// minutes, refusing every login.
//
// Mutex is a sync.Mutex that, once Enable is called, as tests do, records which
// goroutines hold it, and Outer records a violation when the goroutine calling it
// holds one: the engine calls it before it takes the catalog lock, either side.
// Disabled, each costs an atomic load.
package lockorder

import (
	"bytes"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
)

var enabled atomic.Bool

// Enable starts the checks; tests call it before anything takes a Mutex.
func Enable() { enabled.Store(true) }

// Mutex is a sync.Mutex whose holders Outer checks for.
type Mutex struct {
	mu sync.Mutex
}

var (
	heldMu sync.Mutex
	// held counts the Mutexes each goroutine holds, by goroutine id.
	held = map[uint64]int{}
	// violations are the first few found, with their stacks.
	violations []string
)

// Lock locks m.
func (m *Mutex) Lock() {
	m.mu.Lock()
	if enabled.Load() {
		note(1)
	}
}

// TryLock tries to lock m, and reports whether it did.
func (m *Mutex) TryLock() bool {
	if !m.mu.TryLock() {
		return false
	}
	if enabled.Load() {
		note(1)
	}
	return true
}

// Unlock unlocks m, which the goroutine that locked it must do.
func (m *Mutex) Unlock() {
	if enabled.Load() {
		note(-1)
	}
	m.mu.Unlock()
}

func note(d int) {
	id := goroutine()
	heldMu.Lock()
	defer heldMu.Unlock()
	if held[id] += d; held[id] <= 0 {
		delete(held, id)
	}
}

// Outer records a violation if the calling goroutine holds a Mutex: what, the catalog
// lock or the like, must be taken before any of them.
func Outer(what string) {
	if !enabled.Load() {
		return
	}
	id := goroutine()
	heldMu.Lock()
	n := held[id]
	heldMu.Unlock()
	if n == 0 {
		return
	}
	buf := make([]byte, 16<<10)
	buf = buf[:runtime.Stack(buf, false)]
	heldMu.Lock()
	defer heldMu.Unlock()
	if len(violations) < 10 {
		violations = append(violations, fmt.Sprintf("%s taken while holding %d of the server's mutexes:\n%s", what, n, buf))
	}
}

// Violations returns those recorded.
func Violations() []string {
	heldMu.Lock()
	defer heldMu.Unlock()
	return append([]string(nil), violations...)
}

// goroutine returns the calling goroutine's id, from its stack's first line.
func goroutine() uint64 {
	var buf [64]byte
	b := buf[:runtime.Stack(buf[:], false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	if i := bytes.IndexByte(b, ' '); i > 0 {
		b = b[:i]
	}
	id, _ := strconv.ParseUint(string(b), 10, 64)
	return id
}
