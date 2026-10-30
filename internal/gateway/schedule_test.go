package gateway

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// scheduleServer is Igloo's /activity, idle, with scheduled work due at
// the time due returns.
func scheduleServer(t *testing.T, due func() time.Time) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		next := ""
		if d := due(); !d.IsZero() {
			next = fmt.Sprintf(`, "next_due": %q`, d.UTC().Format(time.RFC3339Nano))
		}
		_, _ = fmt.Fprintf(w, `{"running": 0, "idle_seconds": 100%s}`, next)
	}))
	t.Cleanup(srv.Close)
	return srv.URL + "/activity"
}

func TestScheduledWorkStartsIgloo(t *testing.T) {
	compute := newFakeCompute(t)
	var due atomic.Int64
	due.Store(time.Now().Add(time.Hour).UnixNano())
	addr := startGateway(t, Options{
		Backend:       compute.addr,
		ActivityURL:   scheduleServer(t, func() time.Time { return time.Unix(0, due.Load()) }),
		SuspendAfter:  200 * time.Millisecond,
		ResumeTimeout: 5 * time.Second,
		Compute:       compute,
	})
	// The first client starts Igloo; that start takes at most as long as
	// the client waits for its first reply.
	connecting := time.Now()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, conn, bufio.NewReader(conn), "hello")
	firstStart := time.Since(connecting)
	_ = conn.Close()

	// Work is due in a while: Igloo stops, and starts again in time for
	// it, with nobody connected.
	at := time.Now().Add(1500 * time.Millisecond)
	due.Store(at.UnixNano())
	waitFor(t, "suspend", func() bool { _, stops := compute.counts(); return stops == 1 })
	waitFor(t, "a start for the work", func() bool { starts, _ := compute.counts(); return starts == 2 })
	// Early by at most the last start's time. A fixed bound failed on
	// loaded machines, where that start itself took longer.
	if early := time.Until(at); early > firstStart {
		t.Errorf("started %s before the work was due, more than the last start's %s", early, firstStart)
	}
	// Once the work is done, it stops again until the next.
	due.Store(time.Now().Add(time.Hour).UnixNano())
	waitFor(t, "suspend after the work", func() bool { _, stops := compute.counts(); return stops == 2 })
	time.Sleep(500 * time.Millisecond)
	if starts, _ := compute.counts(); starts != 2 {
		t.Errorf("starts = %d, want 2", starts)
	}
}

func TestWorkDueSoonKeepsIglooRunning(t *testing.T) {
	compute := newFakeCompute(t)
	addr := startGateway(t, Options{
		Backend: compute.addr,
		// Work is always due in 100ms, sooner than Igloo would be stopped.
		ActivityURL:   scheduleServer(t, func() time.Time { return time.Now().Add(100 * time.Millisecond) }),
		SuspendAfter:  300 * time.Millisecond,
		ResumeTimeout: 5 * time.Second,
		Compute:       compute,
	})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, conn, bufio.NewReader(conn), "hello")
	_ = conn.Close()
	time.Sleep(1500 * time.Millisecond)
	if _, stops := compute.counts(); stops != 0 {
		t.Errorf("stopped %d times with work always about to be due", stops)
	}
}

func TestGatewayLearnsTheScheduleWhenItStarts(t *testing.T) {
	// Igloo cannot be asked: the gateway starts it once to learn when its
	// work is due.
	closed := reserveAddr(t)
	compute := newFakeCompute(t)
	startGateway(t, Options{
		Backend:       compute.addr,
		ActivityURL:   "http://" + closed + "/activity",
		SuspendAfter:  200 * time.Millisecond,
		ResumeTimeout: 5 * time.Second,
		Compute:       compute,
	})
	waitFor(t, "a start", func() bool { starts, _ := compute.counts(); return starts == 1 })
	// It still cannot be asked, so it may be working: it is not stopped.
	time.Sleep(500 * time.Millisecond)
	if _, stops := compute.counts(); stops != 0 {
		t.Fatalf("stopped %d times though its activity could not be read", stops)
	}
	// Once it goes down and refuses connections, it stops as usual, and is
	// not started again for a due time the gateway does not know.
	compute.shutdown()
	waitFor(t, "suspend", func() bool { _, stops := compute.counts(); return stops == 1 })
	time.Sleep(500 * time.Millisecond)
	if starts, _ := compute.counts(); starts != 1 {
		t.Errorf("starts = %d, want 1", starts)
	}

	// Igloo answers: nothing to learn by starting it.
	compute2 := newFakeCompute(t)
	startGateway(t, Options{
		Backend:       compute2.addr,
		ActivityURL:   scheduleServer(t, func() time.Time { return time.Time{} }),
		SuspendAfter:  time.Hour,
		ResumeTimeout: 5 * time.Second,
		Compute:       compute2,
	})
	time.Sleep(300 * time.Millisecond)
	if starts, _ := compute2.counts(); starts != 0 {
		t.Errorf("started %d times though Igloo answered", starts)
	}
}
