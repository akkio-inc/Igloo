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
				if sess.running != nil {
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

	// A canceled statement that stops: nothing more once it has, even
	// with memory critical.
	behaved := run("SELECT count(*) FROM range(10000000000) a(i), range(1000) b(j) WHERE i + j < 0")
	eventually("the statement did not start", running(1))
	if !srv.ShedMemory(enough) {
		t.Fatal("nothing shed")
	}
	if err := <-behaved; pgCode(err) != "53200" {
		t.Errorf("the canceled statement: %v", err)
	}
	eventually("the canceled statement's session still runs it", running(0))
	if srv.ShedMemory(critical) {
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
