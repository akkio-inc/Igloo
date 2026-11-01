package pgwire

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
)

// A statement here whose read of the read cache raced another over a
// block runs again, and the client gets its rows; one that races every
// time fails after maxBlockRaces runs again.
func TestLocalBlockRace(t *testing.T) {
	t.Parallel()
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	race := errors.New(`IO Error: Could not remove file "/var/lib/igloo/cache/blocks/` +
		`5185916048c6aa68b0867467d6512d995b2ced9f1303c43323349e8c434303a4-f.parquet-1048576-524288": No such file or directory`)
	var runs atomic.Int32
	ts.srv.failLocal = func(context.Context, *statement) error {
		if runs.Add(1) <= 2 {
			return race
		}
		return nil
	}
	if got := count(t, conn, "SELECT count(*) FROM range(10)"); got != 10 {
		t.Errorf("got %d", got)
	}
	if n := runs.Load(); n != 3 {
		t.Errorf("ran %d times, want 3", n)
	}
	runs.Store(0)
	ts.srv.failLocal = func(context.Context, *statement) error {
		runs.Add(1)
		return race
	}
	if _, err := conn.Exec(context.Background(), "SELECT count(*) FROM range(10)"); err == nil || !strings.Contains(err.Error(), "Could not remove file") {
		t.Errorf("racing every time: %v", err)
	}
	if n := runs.Load(); n != maxBlockRaces+1 {
		t.Errorf("ran %d times, want %d", n, maxBlockRaces+1)
	}
	ts.srv.failLocal = nil
}
