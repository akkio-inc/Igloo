package logwriter

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// gatedSink blocks every Write until open is closed, as a log driver behind a
// full disk does, and keeps what it was written.
type gatedSink struct {
	open chan struct{}
	mu   sync.Mutex
	buf  bytes.Buffer
}

func (s *gatedSink) Write(p []byte) (int, error) {
	<-s.open
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *gatedSink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// TestWriteNeverBlocks checks that writes go on at once while the sink blocks,
// keeping what fits, in order, and dropping the rest; that the lines kept are
// written once the sink takes them; and that the drops are counted and
// reported once.
func TestWriteNeverBlocks(t *testing.T) {
	sink := &gatedSink{open: make(chan struct{})}
	w := New(sink, 10_000)
	var reports []uint64
	var mu sync.Mutex
	w.OnDrop(func(n uint64) {
		mu.Lock()
		reports = append(reports, n)
		mu.Unlock()
		fmt.Fprintf(w, "dropped %d\n", n)
	})
	start := time.Now()
	const lines = 100_000
	for i := range lines {
		fmt.Fprintf(w, "line %06d\n", i)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("%d writes with the sink blocked took %s", lines, took)
	}
	dropped := w.Dropped()
	if dropped == 0 || dropped >= lines {
		t.Fatalf("dropped %d of %d lines", dropped, lines)
	}
	close(sink.open)
	w.Flush(10 * time.Second)
	out := sink.String()
	got := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	kept := got[:len(got)-1]
	if want := fmt.Sprintf("dropped %d", dropped); got[len(got)-1] != want {
		t.Errorf("the last line %q, want %q", got[len(got)-1], want)
	}
	if uint64(len(kept))+dropped != lines {
		t.Errorf("%d lines written and %d dropped of %d", len(kept), dropped, lines)
	}
	for i, l := range kept {
		if want := fmt.Sprintf("line %06d", i); l != want {
			t.Fatalf("line %d is %q, want %q", i, l, want)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reports) != 1 || reports[0] != dropped {
		t.Errorf("reports %v, want one of %d", reports, dropped)
	}
}

// TestFlush checks that Flush writes what is held, and that lines written
// after it are dropped; and that it gives up after its timeout while the sink
// blocks.
func TestFlush(t *testing.T) {
	sink := &gatedSink{open: make(chan struct{})}
	close(sink.open)
	w := New(sink, 1<<20)
	fmt.Fprintln(w, "before")
	w.Flush(5 * time.Second)
	fmt.Fprintln(w, "after")
	if got := sink.String(); got != "before\n" {
		t.Errorf("written %q", got)
	}

	blocked := New(&gatedSink{open: make(chan struct{})}, 1<<20)
	fmt.Fprintln(blocked, "held")
	start := time.Now()
	blocked.Flush(50 * time.Millisecond)
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("Flush with the sink blocked took %s", took)
	}
}
