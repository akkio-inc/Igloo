package engine

import (
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/engine/crashtest"
	"github.com/akkio-inc/igloo/internal/engine/terminate"
)

// crashChildEnv, when set, makes TestExitOnCrash crash rather than spawn a
// subprocess to.
const crashChildEnv = "IGLOO_CRASH_CHILD"

// TestExitOnCrash crashes a thread Go does not run, as a crash in a DuckDB
// worker would, in a subprocess, and checks that the subprocess says so and
// exits with status 2. Without the guard it dies of the signal instead, or,
// as its container's first process, loops on the crash until the timeout.
func TestExitOnCrash(t *testing.T) {
	if os.Getenv(crashChildEnv) != "" {
		ExitOnCrash(slog.New(slog.NewTextHandler(os.Stderr, nil)))
		crashtest.CrashForeignThread()
		time.Sleep(time.Minute)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestExitOnCrash$", "-test.timeout=2m")
	cmd.Env = append(os.Environ(), crashChildEnv+"=1")
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 2 || !strings.Contains(string(out), "crashed outside Go code") {
		t.Fatalf("a crash on a foreign thread did not end the process with status 2: %v\n%s", err, out)
	}
}

// parkChildEnv, when set, makes TestParkOnTerminate throw rather than spawn a
// subprocess to.
const parkChildEnv = "IGLOO_PARK_CHILD"

// TestParkOnTerminate throws a C++ exception out of a destructor on a thread a
// statement runs on (see terminate.Statement), in a subprocess, as the iceberg
// extension's scan state did at the memory limit: the handler parks the thread,
// the statement's end hears what escaped, and the process goes on, here to
// write that it did and exit 0. A throw on any other thread still exits 134
// (see TestExitOnTerminate).
func TestParkOnTerminate(t *testing.T) {
	if os.Getenv(parkChildEnv) != "" {
		ended := make(chan string, 1)
		var heard atomic.Bool
		terminate.Watch(func(what string, known bool) { heard.Store(known) })
		go terminate.Statement(func(what string) { ended <- what }, crashtest.ThrowInDestructor)
		select {
		case what := <-ended:
			// The process goes on, and so do statements on other threads.
			n := 0
			terminate.Statement(func(string) {}, func() { n++ })
			os.Stdout.WriteString("ended: " + what + "; heard " + strconv.FormatBool(heard.Load()) + "; then ran " + strconv.Itoa(n) + "\n")
			os.Exit(0)
		case <-time.After(30 * time.Second):
			os.Stdout.WriteString("no end\n")
			os.Exit(3)
		}
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestParkOnTerminate$", "-test.timeout=2m")
	cmd.Env = append(os.Environ(), parkChildEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "ended: Out of Memory Error: failed to pin block of size 4.0 KiB, thrown out of a destructor; heard true; then ran 1") ||
		!strings.Contains(string(out), "parking its statement's thread") {
		t.Fatalf("an exception out of a destructor on a statement's thread did not end only the statement: %v\n%s", err, out)
	}
}

// terminateChildEnv, when set, makes TestExitOnTerminate throw rather than
// spawn a subprocess to.
const terminateChildEnv = "IGLOO_TERMINATE_CHILD"

// TestExitOnTerminate throws a C++ exception that nothing catches on a
// thread Go does not run, as one escaping a DuckDB worker would, in a
// subprocess, and checks that the subprocess says so, with the exception's
// type and message, and exits with status 134 at once (see package terminate). Without
// the handler, C++ aborts, and Go's signal handler takes the abort.
func TestExitOnTerminate(t *testing.T) {
	if os.Getenv(terminateChildEnv) != "" {
		crashtest.ThrowForeignThread()
		time.Sleep(time.Minute)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestExitOnTerminate$", "-test.timeout=2m")
	cmd.Env = append(os.Environ(), terminateChildEnv+"=1")
	start := time.Now()
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 134 || !strings.Contains(string(out), "a C++ exception escaped DuckDB or an extension") ||
		!strings.Contains(string(out), "runtime_error") || !strings.Contains(string(out), "thrown on a thread outside Go, and not caught") {
		t.Fatalf("an exception escaping a foreign thread did not end the process with status 134: %v\n%s", err, out)
	}
	if took := time.Since(start); took > 30*time.Second {
		t.Errorf("the process took %s to end", took)
	}
}
