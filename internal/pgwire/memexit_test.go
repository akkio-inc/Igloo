package pgwire

import (
	"context"
	"database/sql/driver"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"
)

// blockingUDF is a scalar function that sleeps until released, whatever
// interrupts DuckDB gets: a step of a statement that does not stop when
// canceled. It closes entered when it is first called.
type blockingUDF struct {
	released *atomic.Bool
	entered  chan struct{}
	once     *sync.Once
}

func (b blockingUDF) Config() duckdb.ScalarFuncConfig {
	in, _ := duckdb.NewTypeInfo(duckdb.TYPE_INTEGER)
	return duckdb.ScalarFuncConfig{InputTypeInfos: []duckdb.TypeInfo{in}, ResultTypeInfo: in, Volatile: true}
}

func (b blockingUDF) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{RowExecutor: func(values []driver.Value) (any, error) {
		b.once.Do(func() { close(b.entered) })
		for !b.released.Load() {
			time.Sleep(10 * time.Millisecond)
		}
		return values[0], nil
	}}
}

// TestShedMemoryGivesUpOnStuckStatements: a statement the memory watch
// canceled that stops causes nothing more; one that does not stop is
// interrupted again, and once memory is critical Igloo fails the other
// statements in flight with 57P01 and exits with MemoryExitCode. Each step
// waits for what it needs, a statement running or inside the function
// that ignores interrupts, rather than for some time to pass, so that it
// holds however slowly a loaded machine runs the statements.
func TestShedMemoryGivesUpOnStuckStatements(t *testing.T) {
	t.Parallel()
	exited := make(chan int, 1)
	ts := startServer(t, func(o *Options) {
		o.MemoryTotal = 1 << 30 // critical below 64 MiB
		o.shedStuckAfter = 20 * time.Millisecond
		o.exit = func(code int) { exited <- code }
	})
	srv := ts.srv
	ctx := context.Background()
	var released atomic.Bool
	t.Cleanup(func() { released.Store(true) })
	entered := make(chan struct{})
	conn, err := srv.engine.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := duckdb.RegisterScalarUDFOn(conn, "igloo_test_block", blockingUDF{&released, entered, &sync.Once{}}); err != nil {
		t.Fatal(err)
	}
	// One thread, the statement's own: DuckDB's threads would run the
	// function, and a canceled statement whose own thread only waits for
	// them may return while they go on, which is a statement that stopped.
	if _, err := conn.ExecContext(ctx, "SET threads = 1", nil); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	// run starts sql on a connection of its own and returns its outcome.
	run := func(sql string) <-chan error {
		c := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
		out := make(chan error, 1)
		go func() {
			_, err := c.Exec(ctx, sql)
			out <- err
		}()
		return out
	}
	// eventually waits up to a minute for cond.
	eventually := func(what string, cond func() bool) {
		t.Helper()
		for deadline := time.Now().Add(time.Minute); !cond(); time.Sleep(5 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatal(what)
			}
		}
	}
	running := func(n int) func() bool {
		return func() bool {
			got := 0
			srv.sessions.mu.Lock()
			defer srv.sessions.mu.Unlock()
			for _, sess := range srv.sessions.byPID {
				sess.mu.Lock()
				// Past waiting to start, as ShedMemory sheds only such: on a
				// loaded run, one still waiting had nothing shed.
				if sess.running != nil && !sess.queued {
					got++
				}
				sess.mu.Unlock()
			}
			return got == n
		}
	}
	notExited := func(why string) {
		t.Helper()
		select {
		case code := <-exited:
			t.Fatalf("exited (%d) %s", code, why)
		default:
		}
	}
	const enough, critical = 512 << 20, 16 << 20

	// A canceled statement that stops: nothing more once it has (for
	// memory critical with nothing running, see
	// TestShedMemoryExitsForOrphanedWork).
	behaved := run("SELECT count(*) FROM range(10000000000) a(i), range(1000) b(j) WHERE i + j < 0")
	eventually("the statement did not start", running(1))
	if !srv.ShedMemory(enough) {
		t.Fatal("nothing shed")
	}
	if err := <-behaved; pgCode(err) != "53200" {
		t.Errorf("the canceled statement: %v", err)
	}
	eventually("the canceled statement's session still runs it", running(0))
	if srv.ShedMemory(enough) {
		t.Error("shed with nothing running")
	}
	notExited("for a statement that stopped")

	// One that does not stop, canceled once it is in the function that
	// ignores interrupts: interrupted again once shedStuckAfter has passed,
	// and not given up on while memory lasts.
	stuck := run("SELECT igloo_test_block(1)")
	select {
	case <-entered:
	case <-time.After(time.Minute):
		t.Fatal("the statement did not reach the function")
	}
	if !srv.ShedMemory(enough) {
		t.Fatal("nothing shed")
	}
	eventually("the stuck statement was not interrupted again", func() bool { return srv.ShedMemory(enough) })
	notExited("with memory to spare")
	select {
	case err := <-stuck:
		t.Fatalf("the statement that ignores interrupts stopped: %v", err)
	default:
	}

	// Then memory runs critical: given up on, and the others fail.
	other := run("SELECT count(*) FROM range(10000000000) a(i), range(1000) b(j) WHERE i + j < 0")
	eventually("the other statement did not start", running(2))
	if !srv.ShedMemory(critical) {
		t.Error("gave up on nothing")
	}
	select {
	case code := <-exited:
		if code != MemoryExitCode {
			t.Errorf("exit code %d, want %d", code, MemoryExitCode)
		}
	case <-time.After(time.Minute):
		t.Fatal("did not exit")
	}
	if err := <-other; pgCode(err) != "57P01" {
		t.Errorf("the other statement: %v, want 57P01", err)
	}
	released.Store(true)
	<-stuck
}

// TestShedMemoryExitsForOrphanedWork: with nothing running and memory
// critical, Igloo exits only soon after the watch canceled a statement,
// and only once it has freed what it could and memory has stayed critical
// for shedStuckAfter. The work that goes on is a call of the function that
// ignores interrupts on several DuckDB threads, on a connection no session
// owns: what a canceled statement leaves when its own thread returns while
// DuckDB's others are still in such a step, which no test can bring about
// at will, DuckDB choosing which thread runs what.
func TestShedMemoryExitsForOrphanedWork(t *testing.T) {
	t.Parallel()
	exited := make(chan int, 1)
	var freed atomic.Int32
	ts := startServer(t, func(o *Options) {
		o.MemoryTotal = 1 << 30 // critical below 64 MiB
		o.shedStuckAfter = 20 * time.Millisecond
		o.exit = func(code int) { exited <- code }
		o.FreeMemory = func(context.Context) { freed.Add(1) }
	})
	srv := ts.srv
	ctx := context.Background()
	const enough, critical = 512 << 20, 16 << 20
	notExited := func(why string) {
		t.Helper()
		select {
		case code := <-exited:
			t.Fatalf("exited (%d) %s", code, why)
		default:
		}
	}
	// critically waits out shedStuckAfter with memory critical and nothing
	// running, as the watch calls ShedMemory while memory stays low.
	critically := func() {
		for deadline := time.Now().Add(100 * time.Millisecond); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			srv.ShedMemory(critical)
		}
	}

	// No statement canceled yet: whatever holds the memory is not one of
	// Igloo's, and Igloo stays.
	critically()
	notExited("with no statement canceled")
	if freed.Load() != 0 {
		t.Error("freed memory with no statement canceled")
	}

	// The watch cancels a statement, which stops.
	c := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	done := make(chan error, 1)
	go func() {
		_, err := c.Exec(ctx, "SELECT count(*) FROM range(10000000000) a(i), range(1000) b(j) WHERE i + j < 0")
		done <- err
	}()
	for deadline := time.Now().Add(time.Minute); !srv.ShedMemory(enough); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("nothing to shed")
		}
	}
	if err := <-done; pgCode(err) != "53200" {
		t.Fatalf("the canceled statement: %v", err)
	}
	for deadline := time.Now().Add(time.Minute); ; time.Sleep(5 * time.Millisecond) {
		srv.sessions.mu.Lock()
		running := 0
		for _, sess := range srv.sessions.byPID {
			sess.mu.Lock()
			if sess.running != nil {
				running++
			}
			sess.mu.Unlock()
		}
		srv.sessions.mu.Unlock()
		if running == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the canceled statement's session still runs it")
		}
	}

	// DuckDB work no session owns goes on.
	var released atomic.Bool
	entered := make(chan struct{})
	conn, err := srv.engine.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := duckdb.RegisterScalarUDFOn(conn, "igloo_test_block", blockingUDF{&released, entered, &sync.Once{}}); err != nil {
		t.Fatal(err)
	}
	orphan := make(chan struct{})
	go func() {
		defer close(orphan)
		rows, err := conn.QueryContext(ctx, "SELECT igloo_test_block(CAST(i AS INTEGER)) FROM range(100000) r(i)", nil)
		if err == nil {
			_ = rows.Close()
		}
	}()
	t.Cleanup(func() {
		released.Store(true)
		<-orphan
		_ = conn.Close()
	})
	select {
	case <-entered:
	case <-time.After(time.Minute):
		t.Fatal("the work did not start")
	}

	// Memory critical: Igloo frees what it can first, and starts over when
	// memory comes back above the critical floor.
	if !srv.ShedMemory(critical) || freed.Load() != 1 {
		t.Fatalf("did not free memory first (%d times)", freed.Load())
	}
	notExited("before memory had time to come back")
	srv.ShedMemory(enough)
	if !srv.ShedMemory(critical) || freed.Load() != 2 {
		t.Fatalf("did not start over once memory came back (%d frees)", freed.Load())
	}
	notExited("as soon as memory was critical again")

	// Still critical shedStuckAfter later: Igloo exits.
	critically()
	select {
	case code := <-exited:
		if code != MemoryExitCode {
			t.Errorf("exit code %d, want %d", code, MemoryExitCode)
		}
	case <-time.After(time.Minute):
		t.Fatal("did not exit")
	}
}
