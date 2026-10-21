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
