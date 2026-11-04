package pgwire

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/memlimit"
)

// What held DuckDB's memory as a statement ran out of it. DuckDB's error
// says what the statement failed to get and the limit, not what held the
// rest: statements of a mixed load failed with 42.8 GiB used of 42.2, nothing to say
// what they or others held. So the first statement in captureEvery to fail
// so has Igloo log, in one line, DuckDB's limit and its memory by tag as
// duckdb_memory() counts it, what the workload controller has reserved and
// last saw held, and the statements running, the most reserved first.

const (
	// captureEvery is the least time between two captures: the statements
	// that fail at once fail for the same memory.
	captureEvery = 10 * time.Second
	// captureStatements is how many running statements a capture names.
	captureStatements = 5
)

// captureOutOfMemory logs what held DuckDB's memory (see above) if err is
// DuckDB's out-of-memory error and no capture was logged in captureEvery.
func (s *Server) captureOutOfMemory(sess *session, err error) {
	var de *duckdb.Error
	if err == nil || !errors.As(err, &de) || de.Type != duckdb.ErrorTypeOutOfMemory {
		return
	}
	now := time.Now().UnixNano()
	last := s.captured.Load()
	if now-last < int64(captureEvery) || !s.captured.CompareAndSwap(last, now) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	attrs := []any{"pid", sess.pid, "user", sess.currentUser().Name, "job", sess.job, "err", err}
	sess.mu.Lock()
	second := sess.inBackground
	sess.mu.Unlock()
	// The session's own connection may be in a transaction the error ended.
	var conn *duckdb.Conn
	if bc := sess.bgConn.Load(); second && bc != nil {
		conn = bc.conn
		attrs = append(attrs, "duckdb", "second")
	} else if c, cerr := s.engine.Connect(ctx); cerr == nil {
		defer c.Close() //nolint:errcheck
		conn = c
	}
	if conn != nil {
		attrs = append(attrs, duckDBMemory(ctx, conn)...)
	}
	if c := sess.wl.controller; c != nil {
		m := c.Memory()
		attrs = append(attrs, "budget", memlimit.Format(c.Budget()), "reserved", memlimit.Format(m.Reserved),
			"held_at_last_sample", memlimit.Format(m.Held), "statements_running", m.Running, "statements_waiting", m.Waiting)
	}
	attrs = append(attrs, "running", s.runningByReservation(captureStatements))
	s.log.Warn("a statement ran out of DuckDB's memory; what held it", attrs...)
}

// duckDBMemory returns the log attributes of DuckDB's limit and its memory
// by tag, the most first, on conn.
func duckDBMemory(ctx context.Context, conn *duckdb.Conn) []any {
	var attrs []any
	if rows, err := queryRows(ctx, conn, "SELECT current_setting('memory_limit')"); err == nil && len(rows) == 1 {
		attrs = append(attrs, "limit", fmt.Sprint(rows[0][0]))
	}
	rows, err := queryRows(ctx, conn, "SELECT tag, memory_usage_bytes, temporary_storage_bytes FROM duckdb_memory() "+
		"WHERE memory_usage_bytes > 0 OR temporary_storage_bytes > 0 ORDER BY memory_usage_bytes DESC")
	if err != nil {
		return append(attrs, "memory_err", err)
	}
	var used, spilled int64
	tags := make([]string, 0, len(rows))
	for _, r := range rows {
		mem, _ := r[1].(int64)
		tmp, _ := r[2].(int64)
		used, spilled = used+mem, spilled+tmp
		tag := fmt.Sprintf("%v=%s", r[0], memlimit.Format(mem))
		if tmp > 0 {
			tag += "+" + memlimit.Format(tmp) + " spilled"
		}
		tags = append(tags, tag)
	}
	return append(attrs, "used", memlimit.Format(used), "spilled", memlimit.Format(spilled), "by_tag", strings.Join(tags, ", "))
}

// runningByReservation names the n statements running that reserved the
// most memory, then those that ran longest: pid, user or job, what it
// reserved, how long it ran and its first words.
func (s *Server) runningByReservation(n int) string {
	type run struct {
		pid      int32
		who      string
		reserved int64
		since    time.Time
		query    string
	}
	r := s.sessions
	r.mu.Lock()
	all := make([]*session, 0, len(r.byPID))
	for _, sess := range r.byPID {
		all = append(all, sess)
	}
	r.mu.Unlock()
	var runs []run
	for _, sess := range all {
		sess.mu.Lock()
		if sess.running != nil && !sess.queued {
			who := sess.user.Name
			if sess.job != "" {
				who = sess.job
			}
			runs = append(runs, run{pid: sess.pid, who: who, reserved: sess.reserved, since: sess.started, query: sess.statement})
		}
		sess.mu.Unlock()
	}
	slices.SortFunc(runs, func(a, b run) int {
		return cmp.Or(cmp.Compare(b.reserved, a.reserved), a.since.Compare(b.since))
	})
	parts := make([]string, 0, min(n, len(runs)))
	for _, x := range runs[:min(n, len(runs))] {
		q := strings.Join(strings.Fields(x.query), " ")
		if len(q) > 80 {
			q = q[:80] + "..."
		}
		parts = append(parts, fmt.Sprintf("%d %s reserved %s for %s: %s", x.pid, x.who, memlimit.Format(x.reserved),
			time.Since(x.since).Round(time.Millisecond), q))
	}
	return fmt.Sprintf("%d: %s", len(runs), strings.Join(parts, "; "))
}
