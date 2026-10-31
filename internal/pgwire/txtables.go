package pgwire

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/akkio-inc/igloo/internal/catalogproxy"
)

// txIdleLimit is how long an idle transaction block holds back
// maintenance's rewrites of the tables it read: one left open longer, idle
// in a client that forgot it, no longer keeps their files from being
// compacted. A block running a statement holds them however long it runs.
const txIdleLimit = 15 * time.Minute

// holdWait is how long HoldTable tries for the catalog lock before
// maintenance gives up on its commit until the next pass.
const holdWait = 2 * time.Second

// txTables are the catalog's tables a session's DuckDB transaction that has
// not ended uses: those an open transaction block has read or written, and
// those a write running outside one reads and writes. DuckDB commits a
// transaction only on top of the snapshots it read, so a compaction that
// committed meanwhile made its COMMIT fail, as it did dbt's incremental
// models' (blocks, which Igloo cannot replay), or ran the write again (see
// runWithCommitRetry). Maintenance leaves these tables' files be (see
// HoldTable). The session's goroutine writes them; maintenance's reads them.
type txTables struct {
	mu     sync.Mutex
	tables map[string]bool
	// running says a statement of the session runs, and last is when one
	// last started or ended.
	running bool
	last    time.Time
}

// note adds the tables loads loaded to those the transaction uses.
func (t *txTables) note(loads []catalogproxy.Load) {
	if len(loads) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.tables == nil {
		t.tables = map[string]bool{}
	}
	for _, l := range loads {
		t.tables[l.Table()] = true
	}
	t.last = time.Now()
}

// start notes the tables of a statement about to run, before DuckDB binds it
// to run it, which loads them, and finish that it ended: the tables are kept
// if the transaction goes on (a block), forgotten if it ended with it.
func (t *txTables) start(loads []catalogproxy.Load) {
	t.note(loads)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.running, t.last = true, time.Now()
}

func (t *txTables) finish(goesOn bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.running, t.last = false, time.Now()
	if !goesOn {
		t.tables = nil
	}
}

// clear forgets the tables, once the transaction has ended.
func (t *txTables) clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tables = nil
}

// holds reports whether the transaction uses table, and is running a
// statement or has run one in the last txIdleLimit.
func (t *txTables) holds(table string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.tables[table] && (t.running || time.Since(t.last) < txIdleLimit)
}

// noteTx records the tables loads loaded while sess is in a transaction
// block, and forgets them once it is not.
func (sess *session) noteTx(loads []catalogproxy.Load) {
	if sess.inTx {
		sess.txTables.note(loads)
	} else {
		sess.txTables.clear()
	}
}

// useTables notes st's tables as it starts to run, if its transaction may
// commit a change of them: a write, or any statement of a block, which may
// write later on top of what it read. The returned func says it ended.
func (sess *session) useTables(st *statement) func() {
	if !sess.inTx && st.onlyReads() {
		return func() {}
	}
	sess.txTables.start(st.loads)
	return func() { sess.txTables.finish(sess.inTx) }
}

// InTransaction reports whether a session's transaction that has not ended
// uses table (namespace.table, in any case): an open transaction block that
// read or wrote it, or a write running (see txTables), for
// maintenance.Options.Busy. Other replicas' sessions are not seen.
func (s *Server) InTransaction(table string) bool {
	table = strings.ToLower(table)
	s.sessions.mu.Lock()
	defer s.sessions.mu.Unlock()
	for _, sess := range s.sessions.byPID {
		if sess.txTables.holds(table) {
			return true
		}
	}
	return false
}

// HoldTable is maintenance.Options.Hold: it keeps every session from
// starting to use table (namespace.table) until release is called, by
// holding the catalog lock, under which sessions bind statements and so
// load tables, and reports whether no session's transaction uses it
// already. Maintenance commits a rewrite of the table's files only then, so
// that it never makes a user's commit fail; ok is false, with nothing held,
// when a transaction uses the table or the lock stayed taken for holdWait.
func (s *Server) HoldTable(ctx context.Context, table string) (release func(), ok bool) {
	if s.InTransaction(table) {
		return nil, false
	}
	release, ok = s.engine.CatalogTryLock(ctx, holdWait)
	if !ok {
		return nil, false
	}
	// A block notes the tables a statement loaded while its prepare holds
	// the lock (see prepareHeld), and a write before it binds to run.
	if s.InTransaction(table) {
		release()
		return nil, false
	}
	return release, true
}
