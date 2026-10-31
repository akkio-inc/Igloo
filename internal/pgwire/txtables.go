package pgwire

import (
	"strings"
	"sync"
	"time"

	"github.com/akkio-inc/igloo/internal/catalogproxy"
)

// txHoldLimit is how long a transaction block holds back maintenance's
// rewrites of the tables it read: one left open longer, idle in a client
// that forgot it, no longer keeps their files from being compacted.
const txHoldLimit = time.Hour

// txTables are the catalog's tables an open transaction block has read or
// written. DuckDB commits a block only on top of the snapshots it read, so
// a compaction that commits meanwhile makes the block's COMMIT fail, as it
// did dbt's incremental models' (whose blocks Igloo cannot replay):
// maintenance leaves these tables' files for a later pass (see
// Server.InTransaction). The session's goroutine writes them;
// maintenance's reads them.
type txTables struct {
	mu     sync.Mutex
	tables map[string]bool
	since  time.Time
}

// note adds the tables loads loaded to those of the block.
func (t *txTables) note(loads []catalogproxy.Load) {
	if len(loads) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.tables == nil {
		t.tables, t.since = map[string]bool{}, time.Now()
	}
	for _, l := range loads {
		t.tables[l.Table()] = true
	}
}

// clear forgets the tables, once the block has ended.
func (t *txTables) clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tables = nil
}

// holds reports whether the block read table in the last txHoldLimit.
func (t *txTables) holds(table string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.tables[table] && time.Since(t.since) < txHoldLimit
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

// InTransaction reports whether a session's open transaction block has
// read or written table (namespace.table, in any case), for
// maintenance.Options.Busy. Blocks of other replicas are not seen.
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
