// Package activity tracks whether Igloo is doing work: running statements
// or a maintenance pass. The scale-to-zero gateway (internal/gateway) reads
// it over HTTP so that it never stops Igloo in the middle of a query.
package activity

import (
	"sync/atomic"
	"time"
)

// Tracker counts work in progress and remembers when work last started or
// ended. A nil *Tracker records nothing.
type Tracker struct {
	running atomic.Int64
	last    atomic.Int64 // UnixNano of the last Begin or end
}

// New returns a tracker that has been idle since now.
func New() *Tracker {
	t := &Tracker{}
	t.touch()
	return t
}

// Begin marks the start of some work. Call the returned function when it
// ends.
func (t *Tracker) Begin() (end func()) {
	if t == nil {
		return func() {}
	}
	t.running.Add(1)
	t.touch()
	var done atomic.Bool
	return func() {
		if done.CompareAndSwap(false, true) {
			t.running.Add(-1)
			t.touch()
		}
	}
}

// Snapshot reports how much work is running and how long it has been since
// work last started or ended.
func (t *Tracker) Snapshot() (running int64, idle time.Duration) {
	if t == nil {
		return 0, 0
	}
	return t.running.Load(), time.Since(time.Unix(0, t.last.Load()))
}

func (t *Tracker) touch() { t.last.Store(time.Now().UnixNano()) }
