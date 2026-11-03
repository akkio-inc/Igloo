package engine

import (
	"sync"
	"time"
)

// catalogMutex is the catalog lock (see Engine.CatalogLock): a readers-writer lock,
// as sync.RWMutex, held shared by binds and exclusively by DDL and listings, whose
// writers take turns together.
//
// With sync.RWMutex, a writer that waits keeps new readers out, and when it lets go,
// the readers waiting go first: each writer queued behind it then waits for those
// readers to let go before it has its turn, and keeps the readers that came
// meanwhile waiting for that too. Under DDL and listings beside dashboards, binds
// waited for one such drain after another. So writers waiting when one lets go take
// the lock one after another, before the readers that came meanwhile, for up to
// writerTurn from the first; then those readers go first, and the writers after them.
// A queue of writers costs the readers one drain, not one each.
//
// Readers never hold it with a writer, nor writers with each other.
type catalogMutex struct {
	mu sync.Mutex
	// changed is closed, and replaced, when the lock is let go of, for those
	// waiting for it.
	changed chan struct{}
	// readers hold it shared, counting those let in that have yet to wake.
	readers int
	writing bool
	// writers wait for it, and keep new readers out.
	writers int
	// handoff says a writer let go of it to the next one waiting, which new
	// readers wait for too.
	handoff bool
	// waiting readers wait behind a writer: arrived numbers them, and those
	// numbered below admitted are let in (and counted in readers).
	waiting           int
	arrived, admitted uint64
	// turn is when writers began to take it one after another.
	turn time.Time
}

// writerTurn is how long writers take the lock one after another before the readers
// that came meanwhile; a variable for tests.
var writerTurn = 100 * time.Millisecond

// wait lets go of m.mu until the lock changes, and takes it back.
func (m *catalogMutex) wait() {
	if m.changed == nil {
		m.changed = make(chan struct{})
	}
	ch := m.changed
	m.mu.Unlock()
	<-ch
	m.mu.Lock()
}

// notify wakes those waiting for the lock, under m.mu.
func (m *catalogMutex) notify() {
	if m.changed != nil {
		close(m.changed)
		m.changed = nil
	}
}

// RLock takes the lock shared.
func (m *catalogMutex) RLock() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.open() {
		m.readers++
		return
	}
	ticket := m.arrived
	m.arrived++
	m.waiting++
	for ticket >= m.admitted {
		m.wait()
	}
}

// TryRLock takes the lock shared if it can at once.
func (m *catalogMutex) TryRLock() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.open() {
		return false
	}
	m.readers++
	return true
}

// open reports whether a reader may come in at once, under m.mu: nobody holds it
// exclusively or is about to, and no writer waits.
func (m *catalogMutex) open() bool { return !m.writing && !m.handoff && m.writers == 0 }

// RUnlock lets go of a shared hold.
func (m *catalogMutex) RUnlock() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.readers--; m.readers < 0 {
		panic("engine: catalog lock let go of shared more often than taken")
	}
	if m.readers == 0 {
		m.notify()
	}
}

// Lock takes the lock exclusively.
func (m *catalogMutex) Lock() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writers++
	for !m.free() {
		m.wait()
	}
	m.writers--
	m.take()
}

// TryLock takes the lock exclusively if it can at once.
func (m *catalogMutex) TryLock() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.free() {
		return false
	}
	m.take()
	return true
}

// free reports whether nobody holds the lock, under m.mu.
func (m *catalogMutex) free() bool { return !m.writing && m.readers == 0 }

// take holds the lock exclusively, under m.mu.
func (m *catalogMutex) take() {
	m.writing, m.handoff = true, false
	if m.turn.IsZero() {
		m.turn = time.Now()
	}
}

// Unlock lets go of an exclusive hold: to the next writer waiting, within the
// writers' turn, and otherwise to the readers waiting.
func (m *catalogMutex) Unlock() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.writing {
		panic("engine: catalog lock let go of exclusively when not held so")
	}
	m.writing = false
	if m.writers > 0 && time.Since(m.turn) < writerTurn {
		m.handoff = true
	} else {
		m.readers += m.waiting
		m.waiting = 0
		m.admitted = m.arrived
		m.turn = time.Time{}
	}
	m.notify()
}
