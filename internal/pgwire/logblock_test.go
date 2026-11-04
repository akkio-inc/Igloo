package pgwire

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/logwriter"
)

// TestStatementsWhileTheLogBlocks checks that statements logging at debug level
// go on while the log's output is a pipe nobody reads, as stderr is when what
// reads it stalls, once the pipe's buffer is full. Logged straight to the pipe,
// as slog's handlers wrote to stderr, they stop; through logwriter, they run as
// fast as with the log discarded, and the lines that do not fit are dropped.
func TestStatementsWhileTheLogBlocks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const statements = 300
	// run starts a server logging at debug level to out, and runs statements on
	// it for at most limit: it returns how many finished, and in how long.
	run := func(out io.Writer, limit time.Duration) (int64, time.Duration) {
		eng, err := engine.Open(ctx, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
		ts := startServerLogged(t, eng, slog.New(slog.NewTextHandler(out, &slog.HandlerOptions{Level: slog.LevelDebug})))
		conn := ts.admin(t, pgx.QueryExecModeExec)
		var done atomic.Int64
		finished := make(chan struct{})
		start := time.Now()
		go func() {
			defer close(finished)
			for i := range statements {
				var n int64
				if err := conn.QueryRow(ctx, "SELECT count(*) FROM range($1::BIGINT)", 1000+i).Scan(&n); err != nil {
					return
				}
				done.Add(1)
			}
		}()
		select {
		case <-finished:
		case <-time.After(limit):
		}
		return done.Load(), time.Since(start)
	}
	// stalled is a pipe nobody reads until the test ends, when its lines go.
	stalled := func() *os.File {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = r.Close()
			_ = w.Close()
		})
		return w
	}
	drain := func(w *os.File) {
		// Registered after the server's cleanup, so it runs first: the statements
		// held up finish, and the server closes.
		t.Cleanup(func() { _ = w.SetWriteDeadline(time.Now()) })
	}

	free, freeTook := run(io.Discard, time.Minute)
	if free != statements {
		t.Fatalf("with the log discarded, %d of %d statements finished", free, statements)
	}

	direct := stalled()
	n, _ := run(direct, 10*time.Second)
	drain(direct)
	if n >= statements {
		t.Fatalf("logged straight to a pipe nobody reads, all %d statements finished: the pipe never filled", n)
	}
	t.Logf("logged straight to a pipe nobody reads, %d of %d statements finished in 10 s", n, statements)

	buffered := stalled()
	// Less than the statements log once the pipe is full.
	lw := logwriter.New(buffered, 16<<10)
	n, took := run(lw, time.Minute)
	drain(buffered)
	if n != statements {
		t.Fatalf("through logwriter to a pipe nobody reads, %d of %d statements finished", n, statements)
	}
	if lw.Dropped() == 0 {
		t.Error("the log dropped nothing: the pipe did not fill")
	}
	t.Logf("%d statements: %s with the log discarded, %s through logwriter to a pipe nobody reads (%d lines dropped)",
		statements, freeTook, took, lw.Dropped())
	if took > 2*freeTook+2*time.Second {
		t.Errorf("through logwriter to a pipe nobody reads, %d statements took %s, against %s", statements, took, freeTook)
	}
}
