package gateway

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeCompute runs an echo server on a fixed address while "started".
type fakeCompute struct {
	t        *testing.T
	addr     string
	bootTime time.Duration
	// stopTime keeps the port open for a while after Stop returns, like a
	// machine shutting down.
	stopTime time.Duration
	fail     atomic.Bool

	mu     sync.Mutex
	ln     net.Listener
	conns  []net.Conn
	starts int
	stops  int
}

// reserved holds the addresses fakes have picked but not yet listen on (they
// open them only once "started"), so that no other listener in these tests
// takes one in the meantime. A gateway listening on its own backend's port
// forwards to itself: the cause of rare failures, an EOF or a hung round trip.
var reserved = struct {
	sync.Mutex
	addrs map[string]bool
}{addrs: map[string]bool{}}

// reserveAddr picks a free address for a fake to listen on later, and keeps
// the tests' own listeners off it until the test ends.
func reserveAddr(t *testing.T) string {
	ln := listenFree(t)
	addr := ln.Addr().String()
	_ = ln.Close()
	reserved.Lock()
	reserved.addrs[addr] = true
	reserved.Unlock()
	t.Cleanup(func() {
		reserved.Lock()
		delete(reserved.addrs, addr)
		reserved.Unlock()
	})
	return addr
}

// listenFree listens on a free loopback port that no fake has reserved.
func listenFree(t *testing.T) net.Listener {
	t.Helper()
	var taken []net.Listener
	defer func() {
		for _, ln := range taken {
			_ = ln.Close()
		}
	}()
	for range 100 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		reserved.Lock()
		clash := reserved.addrs[ln.Addr().String()]
		reserved.Unlock()
		if !clash {
			return ln
		}
		// Hold it until another port is found, so it is not handed out again.
		taken = append(taken, ln)
	}
	t.Fatal("no free port that a fake has not reserved")
	return nil
}

func newFakeCompute(t *testing.T) *fakeCompute {
	f := &fakeCompute{t: t, addr: reserveAddr(t), bootTime: 50 * time.Millisecond}
	t.Cleanup(func() { _ = f.Stop(context.Background()) })
	return f
}

func (f *fakeCompute) Start(context.Context) error {
	f.mu.Lock()
	f.starts++
	running := f.ln != nil
	f.mu.Unlock()
	if f.fail.Load() {
		return errors.New("no capacity")
	}
	if running {
		return nil
	}
	// Like a machine, the port opens a little after the start request.
	go func() {
		time.Sleep(f.bootTime)
		ln, err := net.Listen("tcp", f.addr)
		if err != nil {
			f.t.Errorf("listen: %v", err)
			return
		}
		f.mu.Lock()
		f.ln = ln
		f.mu.Unlock()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			f.mu.Lock()
			f.conns = append(f.conns, c)
			f.mu.Unlock()
			go func() { _, _ = io.Copy(c, c) }()
		}
	}()
	return nil
}

func (f *fakeCompute) Stop(context.Context) error {
	f.mu.Lock()
	f.stops++
	f.mu.Unlock()
	if f.stopTime == 0 {
		f.shutdown()
		return nil
	}
	go func() {
		time.Sleep(f.stopTime)
		f.shutdown()
	}()
	return nil
}

// shutdown closes the port and its connections, as Igloo going down does.
func (f *fakeCompute) shutdown() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ln != nil {
		_ = f.ln.Close()
		f.ln = nil
	}
	for _, c := range f.conns {
		_ = c.Close()
	}
	f.conns = nil
}

// listening reports whether the fake's port is open.
func (f *fakeCompute) listening() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ln != nil
}

func (f *fakeCompute) counts() (starts, stops int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.starts, f.stops
}

func startGateway(t *testing.T, opts Options) string {
	t.Helper()
	if opts.Log == nil {
		// The gateway's log goes with the test's output, so a failure shows
		// what the gateway did.
		opts.Log = slog.New(slog.NewTextHandler(newTestLog(t), &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	ln := listenFree(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- New(opts).Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})
	return ln.Addr().String()
}

// testLog writes the gateway's log lines to the test's output until the
// test ends; the gateway's goroutines may still log after that, which
// testing does not allow.
type testLog struct {
	t    *testing.T
	mu   sync.Mutex
	done bool
}

func newTestLog(t *testing.T) *testLog {
	l := &testLog{t: t}
	t.Cleanup(func() {
		l.mu.Lock()
		l.done = true
		l.mu.Unlock()
	})
	return l
}

func (l *testLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.done {
		l.t.Log(strings.TrimRight(string(p), "\n"))
	}
	return len(p), nil
}

// roundTrip sends a line through the gateway and expects it echoed.
func roundTrip(t *testing.T, conn net.Conn, r *bufio.Reader, msg string) {
	t.Helper()
	// Long enough to wait out a loaded machine; a start that fails closes
	// the client at once.
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	if _, err := fmt.Fprintln(conn, msg); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := r.ReadString('\n')
	if err != nil || got != msg+"\n" {
		t.Fatalf("read %q, %v; want %q", got, err, msg)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestResumeOnConnectAndSuspendWhenIdle(t *testing.T) {
	compute := newFakeCompute(t)
	addr := startGateway(t, Options{
		Backend:       compute.addr,
		SuspendAfter:  300 * time.Millisecond,
		ResumeTimeout: 5 * time.Second,
		Compute:       compute,
	})

	// Several clients arrive while the compute is stopped; one start
	// serves them all.
	var wg sync.WaitGroup
	for i := range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Errorf("dial: %v", err)
				return
			}
			defer conn.Close() //nolint:errcheck
			roundTrip(t, conn, bufio.NewReader(conn), fmt.Sprintf("hello %d", i))
		}()
	}
	wg.Wait()
	if starts, _ := compute.counts(); starts != 1 {
		t.Fatalf("starts = %d, want 1", starts)
	}

	// An open session that keeps talking holds the compute up.
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(conn)
	for range 6 {
		roundTrip(t, conn, r, "still here")
		time.Sleep(100 * time.Millisecond)
	}
	if _, stops := compute.counts(); stops != 0 {
		t.Fatalf("stopped while a client was active")
	}

	// Once quiet, it stops and the idle session is closed.
	waitFor(t, "suspend", func() bool { _, stops := compute.counts(); return stops == 1 })
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := r.ReadString('\n'); err == nil {
		t.Fatal("idle session survived the suspend")
	}

	// The next client starts it again.
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close() //nolint:errcheck
	roundTrip(t, conn2, bufio.NewReader(conn2), "again")
	if starts, _ := compute.counts(); starts != 2 {
		t.Fatalf("starts = %d, want 2", starts)
	}
}

func TestClientDuringStopGetsAFreshStart(t *testing.T) {
	compute := newFakeCompute(t)
	compute.stopTime = 300 * time.Millisecond
	addr := startGateway(t, Options{
		Backend:       compute.addr,
		SuspendAfter:  time.Second,
		ResumeTimeout: 5 * time.Second,
		Compute:       compute,
	})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, conn, bufio.NewReader(conn), "first")
	_ = conn.Close()

	// A client arrives just after the stop was requested, while the old
	// Igloo still accepts connections.
	waitFor(t, "stop request", func() bool { _, stops := compute.counts(); return stops == 1 })
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close() //nolint:errcheck
	r := bufio.NewReader(conn2)
	roundTrip(t, conn2, r, "during stop")
	// The session must outlive the old Igloo's shutdown.
	time.Sleep(500 * time.Millisecond)
	roundTrip(t, conn2, r, "after the old one is gone")
	if starts, _ := compute.counts(); starts != 2 {
		t.Fatalf("starts = %d, want 2", starts)
	}
}

func TestBusyIglooIsNotSuspended(t *testing.T) {
	compute := newFakeCompute(t)
	var running atomic.Int64
	running.Store(1)
	activity := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"running": %d, "idle_seconds": 100}`, running.Load())
	}))
	defer activity.Close()

	addr := startGateway(t, Options{
		Backend:       compute.addr,
		ActivityURL:   activity.URL + "/activity",
		SuspendAfter:  200 * time.Millisecond,
		ResumeTimeout: 5 * time.Second,
		Compute:       compute,
	})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	roundTrip(t, conn, bufio.NewReader(conn), "long query")

	// A statement runs without sending anything: no suspend.
	time.Sleep(time.Second)
	if _, stops := compute.counts(); stops != 0 {
		t.Fatal("stopped while a statement was running")
	}
	running.Store(0)
	waitFor(t, "suspend after the statement ended", func() bool { _, stops := compute.counts(); return stops == 1 })
}

func TestUnansweredActivityKeepsIglooRunning(t *testing.T) {
	compute := newFakeCompute(t)
	var answers atomic.Bool
	answers.Store(true)
	activity := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !answers.Load() {
			http.Error(w, "overloaded", http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprint(w, `{"running": 0, "idle_seconds": 100}`)
	}))
	defer activity.Close()

	addr := startGateway(t, Options{
		Backend:       compute.addr,
		ActivityURL:   activity.URL + "/activity",
		SuspendAfter:  200 * time.Millisecond,
		ResumeTimeout: 5 * time.Second,
		Compute:       compute,
	})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	roundTrip(t, conn, bufio.NewReader(conn), "long query")

	// A statement runs without sending anything, and Igloo cannot say so:
	// it is overloaded, or the gateway cannot reach its activity. Not
	// knowing is not idle, and Igloo still accepts connections: no suspend.
	answers.Store(false)
	time.Sleep(time.Second)
	if _, stops := compute.counts(); stops != 0 {
		t.Fatal("stopped while its activity could not be read")
	}
	answers.Store(true)
	waitFor(t, "suspend once Igloo says it is idle", func() bool { _, stops := compute.counts(); return stops == 1 })
}

func TestFailedStart(t *testing.T) {
	compute := newFakeCompute(t)
	compute.fail.Store(true)
	addr := startGateway(t, Options{
		Backend:       compute.addr,
		SuspendAfter:  time.Hour,
		ResumeTimeout: 5 * time.Second,
		Compute:       compute,
	})

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintln(conn, "first try"); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the gateway to close the connection")
	}
	_ = conn.Close()

	// The next client tries again.
	compute.fail.Store(false)
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close() //nolint:errcheck
	roundTrip(t, conn2, bufio.NewReader(conn2), "second try")
	if starts, _ := compute.counts(); starts != 2 {
		t.Fatalf("starts = %d, want 2", starts)
	}
}

func TestRunningIglooIsUsedWithoutStart(t *testing.T) {
	compute := newFakeCompute(t)
	if err := compute.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "fake compute", func() bool {
		c, err := net.Dial("tcp", compute.addr)
		if err == nil {
			_ = c.Close()
		}
		return err == nil
	})
	addr := startGateway(t, Options{
		Backend:       compute.addr,
		SuspendAfter:  time.Hour,
		ResumeTimeout: 5 * time.Second,
		Compute:       compute,
	})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	roundTrip(t, conn, bufio.NewReader(conn), "already up")
	if starts, _ := compute.counts(); starts != 1 {
		t.Fatalf("starts = %d, want only the test's own", starts)
	}
}

// A connection must send something within FirstByteTimeout before the
// gateway starts Igloo or connects to it, and only MaxWaiting connections
// may wait at once. Connections that sent nothing used to start the
// compute and hold a socket and a goroutine each for as long as their
// clients kept them open.
func TestSilentConnectionsAreLimited(t *testing.T) {
	compute := newFakeCompute(t)
	// Igloo takes longer to start than a client has to send its first
	// bytes, which bounds the client's silence, not that wait.
	compute.bootTime = time.Second
	addr := startGateway(t, Options{
		Backend:          compute.addr,
		SuspendAfter:     time.Hour,
		ResumeTimeout:    5 * time.Second,
		FirstByteTimeout: 500 * time.Millisecond,
		MaxWaiting:       3,
		Compute:          compute,
	})
	dial := func() net.Conn {
		t.Helper()
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	// closed reports whether the gateway closes conn within wait.
	closed := func(conn net.Conn, wait time.Duration) bool {
		_ = conn.SetReadDeadline(time.Now().Add(wait))
		_, err := conn.Read(make([]byte, 1))
		var ne net.Error
		return err != nil && !(errors.As(err, &ne) && ne.Timeout())
	}

	silent := []net.Conn{dial(), dial(), dial()}
	time.Sleep(200 * time.Millisecond)
	if starts, _ := compute.counts(); starts != 0 {
		t.Errorf("connections that sent nothing started igloo %d times", starts)
	}
	if extra := dial(); !closed(extra, 2*time.Second) {
		t.Error("a connection past MaxWaiting was not closed")
	}
	for i, conn := range silent {
		if !closed(conn, 5*time.Second) {
			t.Errorf("silent connection %d was not closed", i)
		}
	}
	if starts, _ := compute.counts(); starts != 0 {
		t.Errorf("connections that sent nothing started igloo %d times", starts)
	}

	// With them gone, clients get through again, and wait for the start.
	conn := dial()
	roundTrip(t, conn, bufio.NewReader(conn), "hello")
	if starts, _ := compute.counts(); starts != 1 {
		t.Errorf("starts = %d, want 1", starts)
	}
}

func TestLoadConfig(t *testing.T) {
	env := func(kv map[string]string) func(string) (string, bool) {
		return func(k string) (string, bool) { v, ok := kv[k]; return v, ok }
	}

	cfg, err := LoadConfig(env(map[string]string{
		"IGLOO_GATEWAY_BACKEND":          "10.0.0.5:5432",
		"IGLOO_GATEWAY_COMPUTE":          "ec2",
		"IGLOO_GATEWAY_EC2_INSTANCE_IDS": "i-1, i-2",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ActivityURL != "http://10.0.0.5:8080/activity" || cfg.SuspendAfter != 10*time.Minute ||
		len(cfg.EC2InstanceIDs) != 2 || cfg.ListenAddr != ":5432" || cfg.FirstByteTimeout != 30*time.Second || cfg.MaxWaiting != 200 {
		t.Fatalf("unexpected config: %+v", cfg)
	}

	cfg, err = LoadConfig(env(map[string]string{
		"IGLOO_GATEWAY_BACKEND":            "igloo:5432",
		"IGLOO_GATEWAY_ACTIVITY_URL":       "",
		"IGLOO_GATEWAY_COMPUTE":            "kubernetes",
		"IGLOO_GATEWAY_K8S_DEPLOYMENT":     "igloo",
		"IGLOO_GATEWAY_SUSPEND_AFTER":      "60s",
		"IGLOO_GATEWAY_FIRST_BYTE_TIMEOUT": "5s",
		"IGLOO_GATEWAY_MAX_WAITING":        "1000",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ActivityURL != "" || cfg.K8sReplicas != 1 || cfg.SuspendAfter != time.Minute || cfg.FirstByteTimeout != 5*time.Second ||
		cfg.MaxWaiting != 1000 {
		t.Fatalf("unexpected config: %+v", cfg)
	}

	for name, kv := range map[string]map[string]string{
		"no backend":    {"IGLOO_GATEWAY_COMPUTE": "command", "IGLOO_GATEWAY_START_COMMAND": "a", "IGLOO_GATEWAY_STOP_COMMAND": "b"},
		"no compute":    {"IGLOO_GATEWAY_BACKEND": "igloo:5432"},
		"no stop":       {"IGLOO_GATEWAY_BACKEND": "igloo:5432", "IGLOO_GATEWAY_COMPUTE": "command", "IGLOO_GATEWAY_START_COMMAND": "a"},
		"no instances":  {"IGLOO_GATEWAY_BACKEND": "igloo:5432", "IGLOO_GATEWAY_COMPUTE": "ec2"},
		"bad duration":  {"IGLOO_GATEWAY_BACKEND": "igloo:5432", "IGLOO_GATEWAY_COMPUTE": "kubernetes", "IGLOO_GATEWAY_K8S_DEPLOYMENT": "igloo", "IGLOO_GATEWAY_SUSPEND_AFTER": "soon"},
		"zero replicas": {"IGLOO_GATEWAY_BACKEND": "igloo:5432", "IGLOO_GATEWAY_COMPUTE": "kubernetes", "IGLOO_GATEWAY_K8S_DEPLOYMENT": "igloo", "IGLOO_GATEWAY_K8S_REPLICAS": "0"},
		"zero waiting":  {"IGLOO_GATEWAY_BACKEND": "igloo:5432", "IGLOO_GATEWAY_COMPUTE": "kubernetes", "IGLOO_GATEWAY_K8S_DEPLOYMENT": "igloo", "IGLOO_GATEWAY_MAX_WAITING": "0"},
		"no timeout":    {"IGLOO_GATEWAY_BACKEND": "igloo:5432", "IGLOO_GATEWAY_COMPUTE": "kubernetes", "IGLOO_GATEWAY_K8S_DEPLOYMENT": "igloo", "IGLOO_GATEWAY_FIRST_BYTE_TIMEOUT": "0"},
	} {
		if _, err := LoadConfig(env(kv)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestCommandCompute(t *testing.T) {
	dir := t.TempDir()
	c := Command{StartCommand: "echo up > " + dir + "/state", StopCommand: "exit 3"}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.Stop(context.Background()); err == nil {
		t.Fatal("expected the failing stop command to return an error")
	}
}
