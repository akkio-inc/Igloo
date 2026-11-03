package engine

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// catlockTurn sets writerTurn for one test.
func catlockTurn(t *testing.T, turn time.Duration) {
	t.Helper()
	was := writerTurn
	t.Cleanup(func() { writerTurn = was })
	writerTurn = turn
}

// TestCatalogMutexExcludes takes the lock every way at once, many times, and checks
// that no reader ever holds it with a writer, nor two writers together.
func TestCatalogMutexExcludes(t *testing.T) {
	catlockTurn(t, time.Millisecond)
	var m catalogMutex
	var readers, writers atomic.Int64
	check := func() {
		if w := writers.Load(); w > 1 || w == 1 && readers.Load() > 0 {
			t.Errorf("%d writers and %d readers hold it", w, readers.Load())
		}
	}
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(g), 7))
			for range 2000 {
				switch r.IntN(10) {
				case 0, 1:
					m.Lock()
					writers.Add(1)
					check()
					time.Sleep(time.Duration(r.IntN(50)) * time.Microsecond)
					writers.Add(-1)
					m.Unlock()
				case 2:
					if m.TryLock() {
						writers.Add(1)
						check()
						writers.Add(-1)
						m.Unlock()
					}
				case 3:
					if m.TryRLock() {
						readers.Add(1)
						check()
						readers.Add(-1)
						m.RUnlock()
					}
				default:
					m.RLock()
					readers.Add(1)
					check()
					time.Sleep(time.Duration(r.IntN(50)) * time.Microsecond)
					readers.Add(-1)
					m.RUnlock()
				}
			}
		}()
	}
	wg.Wait()
	if !m.TryLock() {
		t.Fatal("the lock is held after everyone let go")
	}
	m.Unlock()
}

// TestCatalogMutexWriterKeepsReadersOut has a writer wait while a reader holds the
// lock: new readers wait for it, as with sync.RWMutex, and it takes the lock once the
// reader lets go.
func TestCatalogMutexWriterKeepsReadersOut(t *testing.T) {
	var m catalogMutex
	m.RLock()
	locked := make(chan struct{})
	go func() {
		m.Lock()
		close(locked)
	}()
	time.Sleep(50 * time.Millisecond)
	if m.TryRLock() {
		t.Fatal("a reader came in past a waiting writer")
	}
	m.RUnlock()
	select {
	case <-locked:
	case <-time.After(5 * time.Second):
		t.Fatal("the writer did not take the lock once the reader let go")
	}
	m.Unlock()
}

// TestCatalogMutexWritersTakeTurns has writers wait together behind a reader, and a
// reader come after them: the writers take the lock one after another before it,
// within writerTurn; past writerTurn, the reader goes after the first.
func TestCatalogMutexWritersTakeTurns(t *testing.T) {
	for _, tc := range []struct {
		turn        time.Duration
		readerFirst bool
	}{{time.Second, false}, {0, true}} {
		catlockTurn(t, tc.turn)
		var m catalogMutex
		var mu sync.Mutex
		var order []string
		note := func(s string) {
			mu.Lock()
			defer mu.Unlock()
			order = append(order, s)
		}
		m.RLock()
		var wg sync.WaitGroup
		for _, name := range []string{"w1", "w2", "w3"} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				m.Lock()
				note(name)
				time.Sleep(5 * time.Millisecond)
				m.Unlock()
			}()
		}
		time.Sleep(50 * time.Millisecond)
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.RLock()
			note("r")
			m.RUnlock()
		}()
		time.Sleep(50 * time.Millisecond)
		m.RUnlock()
		wg.Wait()
		if len(order) != 4 {
			t.Fatalf("turn %s: %v", tc.turn, order)
		}
		if first := order[1] == "r"; first != tc.readerFirst || order[3] == "r" == tc.readerFirst {
			t.Errorf("turn %s: they took it in the order %v", tc.turn, order)
		}
	}
}
