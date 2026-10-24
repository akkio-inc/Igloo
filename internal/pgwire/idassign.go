package pgwire

import (
	"context"
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	wire "github.com/jeroenrinzema/psql-wire"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/ddl"
	"github.com/akkio-inc/igloo/internal/iddomain"
)

// How member columns get their ids (see iddomains.go and idwrite.go).
//
// An INSERT into a member table, whether a client's, one of COPY INTO's
// loads or the fill of a CREATE TABLE ... AS, writes each member column's
// ids as it writes the rows, looked up in the domain's dictionary
// (joinedInsert): values the dictionary has get their ids for the price of
// a join, and values it lacks get NULL. Once the write has committed,
// outside a transaction block, Igloo gives those values ids and fills them
// in before the statement returns (fillIDs, mapColumn), in three steps:
//
//  1. Without any lock, it takes the distinct values of the table's rows
//     that have no ids yet and that the dictionary lacks.
//  2. Under the domain's lease (iddomain.Store.Lock), it checks them again
//     against the dictionary's latest snapshot, the entries appended since
//     step 1 (by assigned_at, which the appends' time order lets DuckDB
//     skip the rest by), takes as many ids from the domain's counter and
//     appends them to the dictionary in one commit. The lease serializes
//     these appends and nothing else: writers of tables in one domain wait
//     for each other only while one appends, never for another's data
//     write. A value two writers both found new gets one id, as the second
//     finds it on the re-check.
//  3. Without the lease, it fills the ids of the rows that have none from
//     the ids it just assigned, and the rows left, if any, from the
//     dictionary.
//
// A writer that does not get the lease within idLeaseWait, a domain that
// has handed out every id below 2^32 (iddomain.ErrExhausted), and an
// erasure that deleted from the dictionary since the writer looked for new
// values (iddomain.ErrErased; see erasure's deleteRows) leave the ids NULL. So do writes inside a transaction block and other engines'
// writes. Maintenance fills them later (MapIDDomains), and until then
// queries take the table's ids as incomplete. An id is never wrong: a
// value's id is its dictionary entry's, entries are only ever appended,
// under the lease, and every write that changes a member column's value
// sets its id to NULL (see idwrite.go).

// idLeaseTTL is how long a domain's lease lasts unless its holder renews
// it, which it does every third of that while it holds it.
const idLeaseTTL = time.Minute

// idLeaseWait is how long a writer waits for a domain's lease before
// leaving its new values' ids to maintenance; tests shorten it.
var idLeaseWait = 5 * time.Second

// idMember is a member column with its domain.
type idMember struct {
	iddomain.Member
	domain iddomain.Domain
}

// idWrite is an INSERT, as prepared, into a table with member columns.
type idWrite struct {
	schema, table string
	members       []idMember
}

// idMembers returns the members of schema.table with their domains.
func idMembers(snap *iddomain.Snapshot, schema, table string) []idMember {
	var out []idMember
	for _, m := range snap.TableMembers(schema, table) {
		if d := snap.Domain(m.Domain); d != nil {
			out = append(out, idMember{Member: m, domain: *d})
		}
	}
	return out
}

// idMembersOf returns w's members, none for nil.
func idMembersOf(w *idWrite) []idMember {
	if w == nil {
		return nil
	}
	return w.members
}

// loadIDs returns COPY INTO's target as a member table, or nil.
func (s *Server) loadIDs(ctx context.Context, target auth.Object) *idWrite {
	if !s.idDomainsOn {
		return nil
	}
	snap := s.memberSnapshot(ctx)
	if snap == nil {
		return nil
	}
	ms := idMembers(snap, target.Schema, target.Table)
	if len(ms) == 0 {
		return nil
	}
	return &idWrite{schema: target.Schema, table: target.Table, members: ms}
}

// stagedIDs makes the new table of c, a CREATE TABLE ... AS of a member
// table's name, a member table too: each of its columns of a member's name
// and domain's type gets the column for its ids. It returns those members
// and the new table's columns, or nil when there are none. The caller
// holds the catalog lock exclusively.
func (s *Server) stagedIDs(ctx context.Context, sess *session, c *stagedCreate) (*idWrite, []string, error) {
	if !s.idDomainsOn || s.ddl == nil {
		return nil, nil, nil
	}
	snap := s.memberSnapshot(ctx)
	if snap == nil {
		return nil, nil, nil
	}
	members := idMembers(snap, c.target.Schema, c.target.Table)
	if len(members) == 0 {
		return nil, nil, nil
	}
	cols, err := describeTable(ctx, sess, quoteName(c.staging))
	if err != nil {
		return nil, nil, err
	}
	staging := c.staging[len(c.staging)-1]
	var visible []string
	for _, col := range cols {
		visible = append(visible, col.name)
	}
	w := &idWrite{schema: c.target.Schema, table: staging}
	for _, m := range members {
		i := slices.IndexFunc(cols, func(col columnType) bool {
			return strings.EqualFold(col.name, m.Column) && strings.EqualFold(col.typ, m.domain.Type)
		})
		if i < 0 {
			s.log.Info("a member table made again lacks a member column of the domain's type, which gets no ids",
				"table", c.target.String(), "column", m.Column, "domain", m.domain.Name)
			continue
		}
		m.Column = cols[i].name
		if _, err := s.ddl.JoinIDDomain(ctx, c.target.Schema, staging, m.Column, iddomain.HiddenColumn(m.Column), m.domain.Name, m.domain.Generation); err != nil {
			return nil, nil, err
		}
		w.members = append(w.members, m)
	}
	if len(w.members) == 0 {
		return nil, nil, nil
	}
	return w, visible, nil
}

// parsedInsert is INSERT INTO target [(columns)] [BY NAME] source.
type parsedInsert struct {
	target  string // as written
	columns []string
	byName  bool
	source  string
}

// parseInsert reads an INSERT whose rows come from a query, VALUES
// included, or reports false.
func parseInsert(text string) (parsedInsert, bool) {
	var p parsedInsert
	sc := &scanner{s: text}
	if !sc.keywords("INSERT", "INTO") {
		return p, false
	}
	sc.skip()
	start := sc.i
	if _, ok := sc.name(); !ok {
		return p, false
	}
	p.target = text[start:sc.i]
	switch {
	case sc.punct('('):
		for {
			n, ok := sc.name()
			if !ok || len(n) != 1 {
				return p, false
			}
			p.columns = append(p.columns, n[0])
			if sc.punct(',') {
				continue
			}
			if sc.punct(')') {
				break
			}
			return p, false
		}
		sc.keywords("BY", "POSITION")
	case sc.keywords("BY", "NAME"):
		p.byName = true
	default:
		return p, false
	}
	sc.skip()
	p.source = strings.TrimRight(sc.rest(), "; \t\r\n")
	switch first := (&scanner{s: p.source}).word(); first {
	case "SELECT", "WITH", "VALUES", "FROM":
	default:
		if !strings.HasPrefix(p.source, "(") {
			return p, false
		}
	}
	for _, t := range topWords(p.source) {
		if t.word == "RETURNING" || t.word == "CONFLICT" {
			return p, false
		}
	}
	return p, true
}

// lookupID is the id of value in m's domain's dictionary, or NULL.
func (s *Server) lookupID(m idMember, value string) string {
	return "(SELECT min(igloo_dict.id) FROM " + s.dictionary(m.domain.Name) + " AS igloo_dict WHERE igloo_dict.value = CAST(" +
		value + " AS " + m.domain.Type + "))"
}

// joinedInsert rewrites p to write its member columns' ids too, or
// reports false when it writes none of them.
func (s *Server) joinedInsert(p parsedInsert, members []idMember) (string, bool) {
	var cols, ids, lookups []string
	for _, c := range p.columns {
		cols = append(cols, quoteIdent(c))
	}
	for _, m := range members {
		if !p.byName && !slices.ContainsFunc(p.columns, func(c string) bool { return strings.EqualFold(c, m.Column) }) {
			continue
		}
		hidden := quoteIdent(iddomain.HiddenColumn(m.Column))
		lookup := s.lookupID(m, "igloo_src."+quoteIdent(m.Column))
		if p.byName {
			lookup += " AS " + hidden
		}
		ids, lookups = append(ids, hidden), append(lookups, lookup)
	}
	if len(ids) == 0 {
		return "", false
	}
	// The source on lines of its own, as it may end in a comment.
	if p.byName {
		return "INSERT INTO " + p.target + " BY NAME SELECT igloo_src.*, " + strings.Join(lookups, ", ") +
			" FROM (\n" + p.source + "\n) AS igloo_src", true
	}
	return "INSERT INTO " + p.target + " (" + strings.Join(append(cols, ids...), ", ") + ") SELECT igloo_src.*, " +
		strings.Join(lookups, ", ") + " FROM (\n" + p.source + "\n) AS igloo_src(" + strings.Join(cols, ", ") + ")", true
}

// joinFailed reports whether running the INSERT with its ids failed for
// something the ids brought, such as a table that lacks the columns for
// them, which the INSERT as written does not need.
func joinFailed(err error) bool {
	var de *duckdb.Error
	if !errors.As(err, &de) {
		return false
	}
	switch de.Type {
	case duckdb.ErrorTypeBinder, duckdb.ErrorTypeParser, duckdb.ErrorTypeCatalog:
		return true
	}
	return false
}

// execWithIDs runs st, an INSERT into a table with member columns, with
// their ids (see joinedInsert). done is false when it did not, and st
// should run as written: the INSERT is not of a form it rewrites, or the
// ids could not be written, for want of their columns, say.
func (s *Server) execWithIDs(ctx context.Context, sess *session, st *statement, args []driver.NamedValue) (n int64, done bool, err error) {
	if st.ids == nil || !s.idDomainsOn {
		return 0, false, nil
	}
	p, ok := parseInsert(st.prepared)
	if !ok {
		return 0, false, nil
	}
	text, ok := s.joinedInsert(p, st.ids.members)
	if !ok {
		return 0, false, nil
	}
	res, err := sess.conn.ExecContext(ctx, text, args)
	if err != nil {
		if joinFailed(err) {
			s.log.Debug("an INSERT runs without its ids", "table", st.ids.schema+"."+st.ids.table, "err", err)
			return 0, false, nil
		}
		return 0, true, err
	}
	n, err = res.RowsAffected()
	return n, true, err
}

// runWithIDs is execWithIDs for a client's statement, which completes it,
// and returns the rows it wrote.
func (s *Server) runWithIDs(ctx context.Context, sess *session, st *statement, args []driver.NamedValue, w wire.DataWriter) (bool, int64, error) {
	n, done, err := s.execWithIDs(ctx, sess, st, args)
	if !done || err != nil {
		return done, n, err
	}
	return true, n, w.Complete(commandTag(st.stmtType, st.query, n))
}

// fillIDs gives ids to the values of w's table's member columns that have
// none, once the write of rows rows (-1 when not known) committed, outside a
// transaction block (see the top of this file). It reports what it could not
// do in the log only: the write itself succeeded, and maintenance fills what
// is left. A write of fewer than IGLOO_ID_SYNC_MIN_ROWS rows leaves them to a
// mapping pass shortly after (see deferIDs), as a write in a transaction
// block leaves them to maintenance.
func (s *Server) fillIDs(ctx context.Context, sess *session, w *idWrite, rows int64) {
	if w == nil || !s.idDomainsOn || sess.inTx {
		return
	}
	if s.idSyncMinRows > 0 && rows >= 0 && rows < s.idSyncMinRows {
		s.deferIDs(w.schema, w.table)
		return
	}
	for _, m := range w.members {
		if err := s.ensureIDColumn(ctx, w.schema, w.table, m); err != nil {
			s.log.Warn("could not add the column for a member column's ids", "column", m.String(), "err", err)
			continue
		}
		if _, _, err := s.mapColumn(ctx, sess.conn, w.schema, w.table, m, false); err != nil && ctx.Err() == nil {
			s.log.Warn("could not fill a member column's ids; maintenance will", "column", m.String(), "err", err)
		}
	}
	s.idsWritten(w.schema, w.table)
}

// ensureIDColumn adds the column for m's ids to schema.table if it lacks
// it, as when the table was made again after its column joined the
// domain.
func (s *Server) ensureIDColumn(ctx context.Context, schema, table string, m idMember) error {
	if s.ddl == nil {
		return errors.New("no catalog")
	}
	_, err := s.ddl.JoinIDDomain(ctx, schema, table, m.Column, iddomain.HiddenColumn(m.Column), m.domain.Name, m.domain.Generation)
	return err
}

// idConn runs statements on a DuckDB connection: a session's, or one of
// maintenance's own.
type idConn interface {
	ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error)
	QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error)
}

// mapColumn gives ids to the values of m in schema.table that have none
// and fills them in (see the top of this file). It returns how many
// values got new ids and how many rows were filled. A busy lease, an
// exhausted domain or an erasure meanwhile still fills the rows whose
// values have ids, when thorough: maintenance's rows from other engines
// hold values the dictionary has, where a write's rows without ids hold
// those it lacked as it wrote them.
//
// A write's INSERT reads the dictionary once to look its values up; this
// reads it once more, to find the values it lacks, and fills the rows that
// got new ids from the ids just assigned. Only rows still without ids after
// that, as those of maintenance, are looked up in the dictionary again.
func (s *Server) mapColumn(ctx context.Context, conn idConn, schema, table string, m idMember, thorough bool) (assigned, filled int64, err error) {
	tbl := quoteName([]string{s.catalogName(), schema, table})
	col, hidden := quoteIdent(m.Column), quoteIdent(iddomain.HiddenColumn(m.Column))
	missing := " WHERE " + hidden + " IS NULL AND " + col + " IS NOT NULL"
	// Most often every row has its ids, which a scan of the column of ids
	// tells, skipping row groups whose statistics say they have no NULLs.
	anyMissing := func() (bool, error) {
		rows, err := conn.QueryContext(ctx, "SELECT count(*) FROM (SELECT 1 FROM "+tbl+missing+" LIMIT 1)", nil)
		if err != nil {
			return false, err
		}
		n, err := countOf(rows)
		return n > 0, err
	}
	if some, err := anyMissing(); err != nil || !some {
		return 0, 0, err
	}
	update := func(q string) error {
		return retryCommits(ctx, func() error {
			res, err := conn.ExecContext(ctx, q, nil)
			if err == nil {
				var n int64
				n, err = res.RowsAffected()
				filled += n
			}
			return err
		})
	}
	assigned, ids, done, err := s.assignIDs(ctx, conn, m.domain, "SELECT "+col+" FROM "+tbl+missing)
	if err != nil && !errors.Is(err, iddomain.ErrBusy) && !errors.Is(err, iddomain.ErrExhausted) && !errors.Is(err, iddomain.ErrErased) {
		return 0, 0, err
	}
	assignErr := err
	defer done()
	if thorough || assignErr == nil {
		// Many rows: their files written again, in their order (see
		// idfill.go).
		if n, err := missingRows(ctx, conn, tbl, col, hidden); err != nil {
			return assigned, filled, err
		} else if n >= s.idFillRewriteRows {
			n, err := s.fillByRewrite(ctx, conn, schema, table, m)
			if err == nil {
				filled += n
				s.metrics.idsFilled(m.domain.Name, filled)
				return assigned, filled, assignErr
			}
			if ctx.Err() != nil {
				return assigned, filled, err
			}
			s.log.Warn("could not fill ids by writing the files again; filling them by UPDATE", "column", m.String(), "rows", n, "err", err)
		}
	}
	if ids != "" && assigned <= idFillFromAssigned {
		// From the ids just assigned, one value each.
		if err := update("UPDATE " + tbl + " AS igloo_t SET " + hidden + " = igloo_a.id FROM " + ids + " AS igloo_a WHERE igloo_t." + hidden +
			" IS NULL AND CAST(igloo_t." + col + " AS " + m.domain.Type + ") = igloo_a.value"); err != nil {
			return assigned, filled, err
		}
	}
	if thorough || assignErr == nil {
		// Then from the dictionary, if rows are left: values another writer
		// gave ids meanwhile, or, for maintenance, values the dictionary
		// had. A write left without the lease leaves them to maintenance.
		some, err := anyMissing()
		if err != nil {
			return assigned, filled, err
		}
		if some {
			if err := update("UPDATE " + tbl + " AS igloo_t SET " + hidden + " = " + s.lookupID(m, "igloo_t."+col) +
				" WHERE igloo_t." + hidden + " IS NULL AND igloo_t." + col + " IS NOT NULL AND EXISTS (SELECT 1 FROM " +
				s.dictionary(m.domain.Name) + " AS igloo_dict WHERE igloo_dict.value = CAST(igloo_t." + col + " AS " + m.domain.Type + "))"); err != nil {
				return assigned, filled, err
			}
		}
	}
	s.metrics.idsFilled(m.domain.Name, filled)
	return assigned, filled, assignErr
}

// assignIDs gives ids to the distinct values of the first column of the
// query values that d's dictionary lacks (steps 1 and 2 at the top of this
// file). It returns how many it gave, and a temporary table of them and
// their ids (value, id), "" for none, which done drops. It returns
// iddomain.ErrBusy when another writer held the lease for longer than
// idLeaseWait, iddomain.ErrExhausted when d has no ids left for them, and
// iddomain.ErrErased when an erasure deleted from the dictionary meanwhile.
func (s *Server) assignIDs(ctx context.Context, conn idConn, d iddomain.Domain, values string) (n int64, ids string, done func(), err error) {
	dict := s.dictionary(d.Name)
	var b [8]byte
	_, _ = rand.Read(b[:])
	tmp := "igloo_new_ids_" + hex.EncodeToString(b[:])
	news, assigned := "temp.main."+quoteIdent(tmp), "temp.main."+quoteIdent(tmp+"_a")
	cast := "CAST(igloo_v.v AS " + d.Type + ")"
	exec := func(ctx context.Context, q string) error {
		_, err := conn.ExecContext(ctx, q, nil)
		return err
	}
	done = func() {
		_ = exec(context.WithoutCancel(ctx), "DROP TABLE IF EXISTS "+news)
		_ = exec(context.WithoutCancel(ctx), "DROP TABLE IF EXISTS "+assigned)
	}
	count := func() (int64, error) {
		rows, err := conn.QueryContext(ctx, "SELECT count(*) FROM "+news, nil)
		if err != nil {
			return 0, err
		}
		return countOf(rows)
	}
	// An erasure that deletes from the dictionary meanwhile may have
	// deleted rows that values are found in below; it counts itself under
	// the lease, which the count is checked against there.
	erased, err := s.idDomains.Erased(ctx, d.Name)
	if err != nil {
		return 0, "", done, err
	}
	// Entries appended from here on are what the check under the lease
	// looks for; their assigned_at is no earlier than this, give or take
	// the appending replica's clock.
	since := time.Now().Add(-idClockSkew).UTC().Format("2006-01-02 15:04:05.999999")
	// 1. The new values, as the dictionary stands.
	if err := exec(ctx, "CREATE TEMP TABLE "+quoteIdent(tmp)+" AS SELECT DISTINCT "+cast+" AS value FROM (\n"+values+
		"\n) AS igloo_v(v) WHERE igloo_v.v IS NOT NULL AND NOT EXISTS (SELECT 1 FROM "+dict+" AS igloo_dict WHERE igloo_dict.value = "+cast+")"); err != nil {
		return 0, "", done, err
	}
	n, err = count()
	if err != nil || n == 0 {
		return 0, "", done, err
	}
	if s.afterNewValues != nil {
		s.afterNewValues()
	}

	// 2. Under the lease: again against the entries appended since, ids
	// for those left, and one append.
	holder := s.idHolder + "/" + strconv.FormatUint(s.idSeq.Add(1), 10)
	unlock, err := s.idDomains.Lock(ctx, d.Name, holder, idLeaseTTL, idLeaseWait)
	if err != nil {
		if errors.Is(err, iddomain.ErrBusy) {
			s.metrics.idAssignSkipped("busy")
		}
		return 0, "", done, err
	}
	lost := s.renewLease(ctx, d.Name, holder)
	defer func() {
		lost.stop()
		unlock()
	}()
	if now, err := s.idDomains.Erased(ctx, d.Name); err != nil || now != erased {
		if err == nil {
			s.metrics.idAssignSkipped("erased")
			err = fmt.Errorf("ID domain %s %w", d.Name, iddomain.ErrErased)
		}
		return 0, "", done, err
	}
	// The dictionary's appends come in time order, so the row groups of
	// earlier ones, whose assigned_at is older, are skipped.
	if err := exec(ctx, "DELETE FROM "+news+" AS igloo_n WHERE EXISTS (SELECT 1 FROM "+dict+" AS igloo_dict WHERE igloo_dict.assigned_at >= TIMESTAMPTZ '"+
		since+"+00' AND igloo_dict.value = igloo_n.value)"); err != nil {
		return 0, "", done, err
	}
	if n, err = count(); err != nil || n == 0 {
		return 0, "", done, err
	}
	first, err := s.idDomains.Allocate(ctx, d.Name, d.Generation, n)
	if err != nil {
		if errors.Is(err, iddomain.ErrExhausted) {
			s.metrics.idAssignSkipped("exhausted")
			s.log.Error("an ID domain has handed out every id below 2^32 and gives new values none; counts over it fall back to the values",
				"domain", d.Name, "values", n)
		}
		return 0, "", done, err
	}
	if lost.lost() {
		return 0, "", done, fmt.Errorf("ID domain %s: the lease ran out before its new ids were appended", d.Name)
	}
	if err := exec(ctx, "CREATE TEMP TABLE "+quoteIdent(tmp+"_a")+" AS SELECT value, CAST("+strconv.FormatInt(first-1, 10)+
		" + row_number() OVER () AS BIGINT) AS id FROM "+news); err != nil {
		return 0, "", done, err
	}
	insert := "INSERT INTO " + dict + " (value, id, assigned_at) SELECT value, id, now() FROM " + assigned
	if err := retryCommits(ctx, func() error { return exec(ctx, insert) }); err != nil {
		return 0, "", done, err
	}
	s.metrics.idsAssigned(d.Name, n)
	return n, assigned, done, nil
}

// idFillFromAssigned is the most new ids whose rows are filled from the
// ids just assigned; past it, as in a table's first load, DuckDB fills them
// faster through the dictionary (31 s, against 70, for 40M people).
const idFillFromAssigned = 1_000_000

// idClockSkew is how far the clocks of replicas that append to a
// dictionary may disagree, which the check under the lease allows for.
const idClockSkew = 5 * time.Minute

// leaseRenewal renews a lease until stopped, noting when it could not.
type leaseRenewal struct {
	cancel context.CancelFunc
	done   chan struct{}
	failed atomic.Bool
}

func (r *leaseRenewal) stop() {
	r.cancel()
	<-r.done
}

func (r *leaseRenewal) lost() bool { return r.failed.Load() }

// renewLease renews holder's lease of domain every third of idLeaseTTL.
func (s *Server) renewLease(ctx context.Context, domain, holder string) *leaseRenewal {
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	r := &leaseRenewal{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		t := time.NewTicker(idLeaseTTL / 3)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if _, err := s.idDomains.Lock(ctx, domain, holder, idLeaseTTL, 0); err != nil && ctx.Err() == nil {
					r.failed.Store(true)
					return
				}
			}
		}
	}()
	return r
}

// retryCommits runs f again, with jittered backoff, while it loses
// Iceberg commit races, up to maxCommitRetries times.
func retryCommits(ctx context.Context, f func() error) error {
	backoff := 20 * time.Millisecond
	for attempt := 0; ; attempt++ {
		err := f()
		if err == nil || attempt == maxCommitRetries || !commitConflict(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(mrand.Int64N(int64(backoff)))):
		}
		backoff = min(2*backoff, time.Second)
	}
}

// newIDHolder names this replica in ID domains' leases.
func newIDHolder() string {
	host, _ := os.Hostname()
	var b [4]byte
	_, _ = rand.Read(b[:])
	return host + "/" + strconv.Itoa(os.Getpid()) + "/" + hex.EncodeToString(b[:])
}

// idDeferredDelay is how soon after a write that left its new values' ids
// to it a mapping pass of its table runs; the writes to the table meanwhile
// wait for the same pass. Tests shorten it.
var idDeferredDelay = 30 * time.Second

// idDeferral holds the mapping passes due, by table.
type idDeferral struct {
	mu      sync.Mutex
	pending map[string]*time.Timer
	stopped bool
}

// stop cancels the passes not yet begun, as the server stops; maintenance
// maps those tables later.
func (d *idDeferral) stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stopped = true
	for k, t := range d.pending {
		t.Stop()
		delete(d.pending, k)
	}
}

// deferIDs has a pass map schema.table in idDeferredDelay, unless one is
// due already: small writes, each of which would pay for a search of the
// dictionary and two commits of their own, share one.
func (s *Server) deferIDs(schema, table string) {
	d := &s.idDeferred
	key := strings.ToLower(schema + "." + table)
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopped || d.pending[key] != nil {
		return
	}
	if d.pending == nil {
		d.pending = map[string]*time.Timer{}
	}
	d.pending[key] = time.AfterFunc(idDeferredDelay, func() {
		d.mu.Lock()
		delete(d.pending, key)
		d.mu.Unlock()
		s.mapDeferred(schema, table)
	})
}

// mapDeferred maps the member columns schema.table has now, on a
// connection of its own, and tries again later if another writer holds a
// domain's lease.
func (s *Server) mapDeferred(schema, table string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	snap := s.memberSnapshot(ctx)
	if snap == nil {
		return
	}
	members := idMembers(snap, schema, table)
	if len(members) == 0 {
		return
	}
	conn, err := s.engine.Connect(ctx)
	if err != nil {
		s.log.Warn("could not connect to map a table's ids; maintenance will", "table", schema+"."+table, "err", err)
		return
	}
	defer conn.Close() //nolint:errcheck
	conn.SetBindHold(s.engine.CatalogShare)
	busy := false
	for _, m := range members {
		if err := s.ensureIDColumn(ctx, schema, table, m); err != nil {
			s.log.Warn("could not add the column for a member column's ids", "column", m.String(), "err", err)
			continue
		}
		_, _, err := s.mapColumn(ctx, conn, schema, table, m, true)
		switch {
		case errors.Is(err, iddomain.ErrBusy):
			busy = true
		case err != nil:
			s.log.Warn("could not map a member column's ids; maintenance will", "column", m.String(), "err", err)
		}
	}
	if busy {
		s.deferIDs(schema, table)
		return
	}
	s.idsWritten(schema, table)
}

// idMapped remembers, by member column, the table snapshot maintenance
// last found fully mapped, so it does not look again until it changes.
type idMapped struct {
	mu   sync.Mutex
	seen map[string]int64
}

// MapIDDomains gives ids to the values of member columns that have none,
// in every member table: the rows other engines wrote, those Igloo's own
// writes inside transaction blocks wrote, and those a busy lease left.
// Maintenance runs it at the start of each pass, on one replica at a time.
func (s *Server) MapIDDomains(ctx context.Context) {
	if !s.idDomainsOn || s.catalogName() == "" || s.ddl == nil {
		return
	}
	snap, err := s.idDomains.Load(ctx)
	if err != nil {
		s.log.Warn("could not load ID domains to map their members", "err", err)
		return
	}
	if len(snap.Members) == 0 {
		return
	}
	conn, err := s.engine.Connect(ctx)
	if err != nil {
		s.log.Warn("could not connect to map ID domains' members", "err", err)
		return
	}
	defer conn.Close() //nolint:errcheck
	// As a session's: binds hold the catalog lock shared (see
	// engine.Engine.CatalogLock).
	conn.SetBindHold(s.engine.CatalogShare)
	for _, mem := range snap.Members {
		if ctx.Err() != nil {
			return
		}
		d := snap.Domain(mem.Domain)
		if d == nil {
			continue
		}
		m := idMember{Member: mem, domain: *d}
		snapshot, err := s.ddl.JoinIDDomain(ctx, m.Schema, m.Table, m.Column, iddomain.HiddenColumn(m.Column), d.Name, d.Generation)
		if errors.Is(err, ddl.ErrNotFound) {
			continue
		}
		if err != nil {
			s.log.Warn("could not add the column for a member column's ids", "column", m.String(), "err", err)
			continue
		}
		key := strings.ToLower(m.String())
		s.idMapped.mu.Lock()
		seen, ok := s.idMapped.seen[key]
		s.idMapped.mu.Unlock()
		if ok && seen == snapshot {
			continue
		}
		assigned, filled, err := s.mapColumn(ctx, conn, m.Schema, m.Table, m, true)
		if err != nil {
			if ctx.Err() == nil {
				s.log.Warn("could not map a member column's ids", "column", m.String(), "err", err)
			}
			continue
		}
		if filled == 0 {
			s.idMapped.mu.Lock()
			if s.idMapped.seen == nil {
				s.idMapped.seen = map[string]int64{}
			}
			s.idMapped.seen[key] = snapshot
			s.idMapped.mu.Unlock()
		}
		if assigned > 0 || filled > 0 {
			s.log.Info("mapped a member column's ids", "column", m.String(), "new_ids", assigned, "rows", filled)
		}
	}
}
