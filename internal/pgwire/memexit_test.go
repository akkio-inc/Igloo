package pgwire

import (
	"context"
	"database/sql/driver"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"
)

// blockingUDF is a scalar function that sleeps until released, whatever
// interrupts DuckDB gets: a step of a statement that does not stop when
// canceled.
type blockingUDF struct{ released *atomic.Bool }

func (b blockingUDF) Config() duckdb.ScalarFuncConfig {
	in, _ := duckdb.NewTypeInfo(duckdb.TYPE_INTEGER)
	return duckdb.ScalarFuncConfig{InputTypeInfos: []duckdb.TypeInfo{in}, ResultTypeInfo: in, Volatile: true}
}

func (b blockingUDF) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{RowExecutor: func(values []driver.Value) (any, error) {
		for !b.released.Load() {
			time.Sleep(10 * time.Millisecond)
		}
		return values[0], nil
	}}
}

// TestShedMemoryGivesUpOnStuckStatements: a statement the memory watch
// canceled that stops causes nothing more; one that does not stop is
// interrupted again, and once memory is critical Igloo fails the other
// statements in flight with 57P01 and exits with MemoryExitCode.
func TestShedMemoryGivesUpOnStuckStatements(t *testing.T) {
	t.Parallel()
	exited := make(chan int, 1)
	ts := startServer(t, func(o *Options) {
		o.MemoryTotal = 1 << 30 // critical below 64 MiB
		o.shedStuckAfter = 300 * time.Millisecond
		o.exit = func(code int) { exited <- code }
	})
	srv := ts.srv
	ctx := context.Background()
	var released atomic.Bool
	t.Cleanup(func() { released.Store(true) })
	conn, err := srv.engine.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := duckdb.RegisterScalarUDFOn(conn, "igloo_test_block", blockingUDF{&released}); err != nil {
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
	running := func(n int) {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			got := 0
			srv.sessions.mu.Lock()
			for _, sess := range srv.sessions.byPID {
				sess.mu.Lock()
				if sess.running != nil {
					got++
				}
				sess.mu.Unlock()
			}
			srv.sessions.mu.Unlock()
			if got == n {
				return
			}
		}
		t.Fatalf("expected %d statements running", n)
	}
	const enough, critical = 512 << 20, 16 << 20

	// A canceled statement that stops: nothing more, even past
	// shedStuckAfter with memory critical.
	behaved := run("SELECT count(*) FROM range(10000000000) a(i), range(1000) b(j) WHERE i + j < 0")
	running(1)
	if !srv.ShedMemory(enough) {
		t.Fatal("nothing shed")
	}
	if err := <-behaved; pgCode(err) != "53200" {
		t.Errorf("the canceled statement: %v", err)
	}
	time.Sleep(400 * time.Millisecond)
	if srv.ShedMemory(critical) {
		t.Error("shed with nothing running")
	}
	select {
	case code := <-exited:
		t.Fatalf("exited (%d) for a statement that stopped", code)
	default:
	}

	// One that does not stop: interrupted again, then given up on.
	stuck := run("SELECT igloo_test_block(1)")
	running(1)
	if !srv.ShedMemory(enough) {
		t.Fatal("nothing shed")
	}
	time.Sleep(400 * time.Millisecond)
	if !srv.ShedMemory(enough) {
		t.Error("the stuck statement was not interrupted again")
	}
	select {
	case code := <-exited:
		t.Fatalf("exited (%d) with memory to spare", code)
	default:
	}
	other := run("SELECT count(*) FROM range(10000000000) a(i), range(1000) b(j) WHERE i + j < 0")
	running(2)
	if !srv.ShedMemory(critical) {
		t.Error("gave up on nothing")
	}
	select {
	case code := <-exited:
		if code != MemoryExitCode {
			t.Errorf("exit code %d, want %d", code, MemoryExitCode)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("did not exit")
	}
	if err := <-other; pgCode(err) != "57P01" {
		t.Errorf("the other statement: %v, want 57P01", err)
	}
	released.Store(true)
	<-stuck
}
