package engine

import (
	"log/slog"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"

	"github.com/akkio-inc/igloo/internal/engine/terminate"
)

// A C++ exception that escapes DuckDB or an extension ends every process
// that runs DuckDB, server and worker, at once with status 134 (see package
// terminate), from before anything opens a database.
func init() { terminate.Install() }

// crashSignals are the signals a crash in native code raises.
var crashSignals = []os.Signal{syscall.SIGSEGV, syscall.SIGBUS, syscall.SIGFPE, syscall.SIGILL}

// ExitOnCrash makes a crash on one of DuckDB's own threads end the process,
// as one on a thread Go runs does, so that whatever runs Igloo restarts it.
// Call it once, at startup.
//
// DuckDB runs much of a query on worker threads it starts itself. When one
// of them crashes, Go's signal handler sets the signal back to its default
// action and raises it again, expecting that to kill the process. Igloo is
// usually its container's first process (PID 1), and Linux ignores a signal
// a PID 1 sends itself unless the process handles it, so the process lives
// on: the handler comes back, the crashing instruction runs again, and the
// thread loops on the crash for good. The statement it ran never finishes,
// and whatever lock its session holds stays held: the catalog lock, held by
// a listing or DDL, then blocks every new session and every statement.
//
// Asking for these signals makes Go deliver such a crash here instead of
// raising it again. A crash in Go code still panics, and one in C code on a
// thread Go runs still ends the process, as before.
func ExitOnCrash(log *slog.Logger) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, crashSignals...)
	go func() {
		sig := <-c
		log.Error("crashed outside Go code, in DuckDB or an extension; exiting", "signal", sig.String())
		_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 2)
		os.Exit(2)
	}()
}
