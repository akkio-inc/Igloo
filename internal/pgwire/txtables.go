package pgwire

import (
	"context"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalogproxy"
	"github.com/akkio-inc/igloo/internal/lockorder"
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
//
// A block that may write uses every table from its BEGIN (see beginBlock):
// DuckDB reads an Iceberg table whose metadata changed after the
// transaction began as it was when it began, from the metadata log, and
// commits a write on top of that. A compaction that committed after a dbt
// batch's BEGIN and before its MERGE first read the model made the batch's
// COMMIT fail though the block had read nothing of the model when
// compaction looked.
type txTables struct {
	mu     lockorder.Mutex
	tables map[string]bool
	// all says the block may use any table: it may write, or it listed the
	// catalog (see beginBlock, noteListing).
	all bool
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

// noteListing says the block listed the catalog: a listing, such as the
// information_schema.columns query dbt runs before each MERGE, takes the
// tables it lists into the block's transaction as DuckDB last loaded them,
// with no request to the catalog that says which, and the MERGE then loads
// nothing. So the block uses every table until it ends.
func (t *txTables) noteListing() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.all, t.last = true, time.Now()
}

// beginBlock says a block that may write is beginning: it uses every table
// until it ends, as DuckDB reads each as of the block's start (see above).
func (t *txTables) beginBlock() { t.noteListing() }

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
		t.tables, t.all = nil, false
	}
}

// clear forgets the tables, once the transaction has ended.
func (t *txTables) clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tables, t.all = nil, false
}

// holds reports whether the transaction uses table, and is running a
// statement or has run one in the last txIdleLimit.
func (t *txTables) holds(table string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return (t.all || t.tables[table]) && (t.running || time.Since(t.last) < txIdleLimit)
}

// noteTx records the tables st loaded as it was prepared, while sess is in
// a transaction block, and forgets them once it is not.
func (sess *session) noteTx(st *statement) {
	if !sess.inTx {
		sess.txTables.clear()
		return
	}
	sess.txTables.note(st.loads)
	if st.listsCatalog {
		sess.txTables.noteListing()
	}
}

// useTables notes st's tables as it starts to run, if its transaction may
// commit a change of them: a write, or any statement of a block, which may
// write later on top of what it read. A listing runs holding the catalog
// lock to itself (see catalogRunLock), so maintenance cannot commit between
// what it notes and what it lists. The returned func says it ended.
func (sess *session) useTables(st *statement) func() {
	if !sess.inTx && st.onlyReads() {
		return func() {}
	}
	sess.txTables.start(st.loads)
	if sess.inTx && st.listsCatalog {
		sess.txTables.noteListing()
	}
	// A BEGIN ends before the session is in the block it began (see
	// observe), and the block goes on.
	begins := !sess.inTx && opensBlock(st)
	return func() { sess.txTables.finish(sess.inTx || begins) }
}

// opensBlock reports whether st is a BEGIN or START TRANSACTION.
func opensBlock(st *statement) bool {
	return st.stmtType == duckdb.STATEMENT_TYPE_TRANSACTION && transactionChange(st.query) > 0
}

// beginBlock runs begin, a BEGIN of a block of sess's: for a user who may
// write, with the block using every table from before DuckDB begins its
// transaction (see txTables), and the catalog lock held shared meanwhile, so
// that maintenance's check and commit (HoldTable) fall before both or after
// both.
func (s *Server) beginBlock(ctx context.Context, sess *session, begin func() error) error {
	if !s.mayWrite(ctx, sess) {
		return begin()
	}
	defer s.shareCatalog(sess)()
	sess.txTables.beginBlock()
	return begin()
}

// mayWrite reports whether sess's user may write a catalog table: by role,
// or through a grant of INSERT, UPDATE or DELETE. A reader's block commits
// no write that a compaction could make fail.
func (s *Server) mayWrite(ctx context.Context, sess *session) bool {
	user := sess.currentUser()
	if user.Role == auth.RoleAdmin || user.Role == auth.RoleWriter {
		return true
	}
	privs, err := s.privileges(ctx, sess, user)
	if err != nil {
		return true
	}
	for _, g := range privs.Grants {
		switch g.Privilege {
		case auth.PrivilegeInsert, auth.PrivilegeUpdate, auth.PrivilegeDelete:
			return true
		}
	}
	return false
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
