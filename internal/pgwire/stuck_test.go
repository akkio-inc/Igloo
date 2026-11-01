package pgwire

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// A statement that does not stop once canceled, as DuckDB does not in a
// step that checks for no interruption, lets its client go stuckGrace
// after its statement timeout or its cancel request: the client's
// connection closes. One that stops in time answers as ever.
func TestStuckStatementLetsClientGo(t *testing.T) {
	t.Parallel()
	ts := startServer(t, func(o *Options) { o.stuckGrace = 500 * time.Millisecond })
	unblock := make(chan struct{})
	// Before the server's cleanup, which waits for its sessions.
	t.Cleanup(func() { close(unblock) })
	ts.srv.failLocal = func(ctx context.Context, st *statement) error {
		if strings.Contains(st.query, "stuck_marker") {
			// It goes on, whatever its context says.
			<-unblock
		}
		return nil
	}
	for _, how := range []string{"statement timeout", "cancel request"} {
		conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
		if how == "statement timeout" {
			mustExec(t, conn, "SET statement_timeout = '300ms'")
		} else {
			go func() {
				time.Sleep(300 * time.Millisecond)
				_ = conn.PgConn().CancelRequest(context.Background())
			}()
		}
		start := time.Now()
		_, err := conn.Exec(context.Background(), "SELECT 42 AS stuck_marker")
		if err == nil {
			t.Errorf("%s: a statement that did not stop returned", how)
		}
		if took := time.Since(start); took > 10*time.Second {
			t.Errorf("%s: the client waited %s", how, took)
		}
	}
	// One that stops answers with its timeout, its connection open.
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "SET statement_timeout = '300ms'")
	_, err := conn.Exec(context.Background(), "SELECT count(*) FROM range(10000000000) a, range(10) b WHERE a.range % 7 = b.range")
	if err == nil || !strings.Contains(err.Error(), "statement timeout") {
		t.Errorf("a statement past its timeout: %v", err)
	}
	if got := count(t, conn, "SELECT 1"); got != 1 {
		t.Errorf("the session after a timeout: %d", got)
	}
}
