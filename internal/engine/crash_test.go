package engine

import (
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/engine/crashtest"
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

// terminateChildEnv, when set, makes TestExitOnTerminate throw rather than
// spawn a subprocess to.
const terminateChildEnv = "IGLOO_TERMINATE_CHILD"

// TestExitOnTerminate throws a C++ exception that nothing catches on a
// thread Go does not run, as one escaping a DuckDB worker would, in a
// subprocess, and checks that the subprocess says so, with the exception's
// type, and exits with status 134 at once (see package terminate). Without
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
		!strings.Contains(string(out), "runtime_error") {
		t.Fatalf("an exception escaping a foreign thread did not end the process with status 134: %v\n%s", err, out)
	}
	if took := time.Since(start); took > 30*time.Second {
		t.Errorf("the process took %s to end", took)
	}
}
