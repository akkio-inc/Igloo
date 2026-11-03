package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/iddomain"
	"github.com/akkio-inc/igloo/internal/lockorder"
)

// Keeping bitmap indexes up to date. An index's table holds the bitmaps of
// one snapshot of its source table, the latest Igloo built them for, with
// the generation of the member's domain; they answer counts only for that
// snapshot while it is the table's current one, and only when the domain's
// generation is theirs (see the rewrite). After the source table changes,
// in the background soon after a statement wrote it (indexesWritten) and
// at each maintenance pass, after the ids are mapped (MaintainBitmapIndexes),
// the bitmaps move to the current snapshot S from the one they are of, B:
//
//   - where every snapshot after B up to S appended (Iceberg's operation
//     append), the files those added are read and their bitmaps OR-ed into
//     B's;
//   - where every one replaced files without changing the data (replace,
//     as compaction and clustering commit), B's bitmaps are S's;
//   - and otherwise, or when B is not S's ancestor, the domain's generation
//     changed or reading the added files fails, they are built again from
//     S.
//
// Bitmaps are built only from data whose member values all have ids: a
// row with a value but no id would leave it uncounted. Rows of other
// snapshots, B's among them and those of snapshots maintenance expired,
// are deleted once S's are written. A lease in the ID domains' store
// keeps two replicas from building one index at once.

// indexLeaseTTL bounds how long one replica holds an index's lease while it
// builds, and indexLeaseWait how long another waits for it (not at all:
// whoever holds it is building the same bitmaps).
const (
	indexLeaseTTL  = time.Hour
	indexLeaseWait = 0
)

// indexState is where an index stands.
type indexState struct {
	// built is the snapshot its bitmaps are of (0 for none), generation
	// theirs and bitmaps how many there are; current is the source
	// table's current snapshot (0 for none).
	built, current int64
	generation     int
	bitmaps        int64
}

// indexState reads where x stands.
func (s *Server) indexState(ctx context.Context, x iddomain.BitmapIndex) (indexState, error) {
	var st indexState
	tbl, err := s.ddl.LoadTable(ctx, x.Schema, x.Table)
	if err != nil {
		return st, err
	}
	if snap := tbl.CurrentSnapshot(); snap != nil {
		st.current = snap.SnapshotID
	}
	conn, err := s.engine.Connect(ctx)
	if err != nil {
		return st, err
	}
	defer conn.Close() //nolint:errcheck
	built, err := s.builtSnapshots(ctx, conn, x)
	if err != nil {
		return st, err
	}
	for _, b := range built {
		if b.snapshot > st.built {
			st.built, st.generation, st.bitmaps = b.snapshot, b.generation, b.bitmaps
		}
	}
	return st, nil
}

// builtSnapshot is a snapshot an index's table holds bitmaps of.
type builtSnapshot struct {
	snapshot   int64
	generation int
	bitmaps    int64
}

func (s *Server) builtSnapshots(ctx context.Context, conn *duckdb.Conn, x iddomain.BitmapIndex) ([]builtSnapshot, error) {
	rows, err := queryRows(ctx, conn, "SELECT source_snapshot_id, generation, count(*) FROM "+s.indexTable(x.Name)+" GROUP BY ALL")
	if err != nil {
		return nil, err
	}
	var out []builtSnapshot
	for _, r := range rows {
		b := builtSnapshot{}
		b.snapshot, _ = r[0].(int64)
		if g, ok := r[1].(int32); ok {
			b.generation = int(g)
		}
		b.bitmaps, _ = r[2].(int64)
		out = append(out, b)
	}
	return out, nil
}

// indexBuilds runs index maintenance in the background after writes, one
// run a table at a time, and another after it if the table was written
// again meanwhile.
type indexBuilds struct {
	mu      lockorder.Mutex
	running map[string]bool
	again   map[string]bool
	// failed counts each index's builds that failed in a row, which are
	// tried again after indexRetry, four times as long each time, up to
	// indexRetries times; a maintenance pass tries again after that.
	failed map[string]int
	// stopped ends the retries when the server shuts down.
	stopped atomic.Bool
	// last is how each index was last built, for tests.
	last map[string]string
}

// indexRetry is a variable for tests.
var indexRetry = 30 * time.Second

const indexRetries = 3

// buildFailed has x built again in a while, after its build failed: object
// storage that turns requests away under load (HTTP 503) fails a build of
// gigabytes now and then, which would otherwise wait for the next
// maintenance pass, an hour by default.
func (s *Server) buildFailed(x iddomain.BitmapIndex) {
	b := &s.indexBuilds
	b.mu.Lock()
	if b.failed == nil {
		b.failed = map[string]int{}
	}
	n := b.failed[x.Name]
	b.failed[x.Name] = n + 1
	b.mu.Unlock()
	if n >= indexRetries {
		return
	}
	time.AfterFunc(indexRetry<<(2*n), func() {
		if !b.stopped.Load() {
			s.indexesChanged(x.Schema, x.Table)
		}
	})
}

// buildDone forgets x's failed builds, and notes how it was built.
func (s *Server) buildDone(x iddomain.BitmapIndex, how string) {
	b := &s.indexBuilds
	b.mu.Lock()
	delete(b.failed, x.Name)
	if how != "" {
		if b.last == nil {
			b.last = map[string]string{}
		}
		b.last[x.Name] = how
	}
	b.mu.Unlock()
}

// indexesWritten has the bitmap indexes of the tables a statement wrote
// brought up to date in the background.
func (s *Server) indexesWritten(written []auth.Object) {
	if !s.bitmapIndexesOn {
		return
	}
	snap := s.memberSnapshot(context.Background())
	if snap == nil || len(snap.Indexes) == 0 {
		return
	}
	for _, o := range written {
		if !o.IsSchema() && o.Catalog == "" && len(snap.TableIndexes(o.Schema, o.Table)) > 0 {
			s.indexesChanged(o.Schema, o.Table)
		}
	}
}

// idsWritten has the bitmap indexes of schema.table brought up to date in
// the background, after a write into it gave its rows ids (see fillIDs):
// indexesWritten hears of writes from the history, which may be off.
func (s *Server) idsWritten(schema, table string) {
	if !s.bitmapIndexesOn {
		return
	}
	if snap := s.memberSnapshot(context.Background()); snap != nil && len(snap.TableIndexes(schema, table)) > 0 {
		s.indexesChanged(schema, table)
	}
}

// indexesChanged brings the bitmap indexes of schema.table up to date in
// the background.
func (s *Server) indexesChanged(schema, table string) {
	if !s.bitmapIndexesOn {
		return
	}
	key := strings.ToLower(schema + "." + table)
	b := &s.indexBuilds
	b.mu.Lock()
	if b.running == nil {
		b.running, b.again = map[string]bool{}, map[string]bool{}
	}
	if b.running[key] {
		b.again[key] = true
		b.mu.Unlock()
		return
	}
	b.running[key] = true
	b.mu.Unlock()
	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), indexLeaseTTL)
			s.maintainIndexes(ctx, func(x iddomain.BitmapIndex) bool { return x.OfTable(schema, table) })
			cancel()
			b.mu.Lock()
			if !b.again[key] {
				delete(b.running, key)
				b.mu.Unlock()
				return
			}
			delete(b.again, key)
			b.mu.Unlock()
		}
	}()
}

// MaintainBitmapIndexes brings every bitmap index up to date with its
// table, after ID domains' ids are mapped; maintenance passes call it.
func (s *Server) MaintainBitmapIndexes(ctx context.Context) {
	s.maintainIndexes(ctx, func(iddomain.BitmapIndex) bool { return true })
}

func (s *Server) maintainIndexes(ctx context.Context, which func(iddomain.BitmapIndex) bool) {
	if !s.bitmapIndexesOn || s.catalogName() == "" || s.ddl == nil {
		return
	}
	snap, err := s.idDomains.Load(ctx)
	if err != nil {
		s.log.Warn("could not load bitmap indexes to build them", "err", err)
		return
	}
	for _, x := range snap.Indexes {
		if ctx.Err() != nil {
			return
		}
		if !which(x) {
			continue
		}
		m := snap.MemberOf(x.Schema, x.Table, x.Column)
		if m == nil {
			continue
		}
		d := snap.Domain(m.Domain)
		if d == nil {
			continue
		}
		how, err := s.maintainIndex(ctx, x, d.Generation)
		switch {
		case errors.Is(err, iddomain.ErrBusy):
		case err != nil && ctx.Err() == nil:
			s.log.Warn("could not build a bitmap index", "index", x.Name, "table", x.Schema+"."+x.Table, "err", err)
			s.buildFailed(x)
		case err == nil:
			s.buildDone(x, how)
			if how != "" {
				s.log.Info("built a bitmap index", "index", x.Name, "table", x.Schema+"."+x.Table, "how", how)
			}
		}
	}
}

// maintainIndex brings x's bitmaps to its table's current snapshot, for the
// domain's generation gen, and says how: "" when they were already there,
// or "append", "carry", "rebuild", or "unmapped" when the snapshot has
// values without ids, which mapping fills in first.
func (s *Server) maintainIndex(ctx context.Context, x iddomain.BitmapIndex, gen int) (string, error) {
	holder := s.idHolder + "/bitmaps/" + strconv.FormatUint(s.idSeq.Add(1), 10)
	unlock, err := s.idDomains.Lock(ctx, "bitmap index "+x.Name, holder, indexLeaseTTL, indexLeaseWait)
	if err != nil {
		return "", err
	}
	defer unlock()
	tbl, err := s.ddl.LoadTable(ctx, x.Schema, x.Table)
	if err != nil {
		return "", err
	}
	conn, err := s.engine.Connect(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close() //nolint:errcheck
	// As a session's: binds hold the catalog lock shared (see
	// engine.Engine.CatalogLock).
	conn.SetBindHold(s.engine.CatalogShare)
	built, err := s.builtSnapshots(ctx, conn, x)
	if err != nil {
		return "", err
	}
	cur := tbl.CurrentSnapshot()
	if cur == nil {
		// No data: no bitmaps either.
		if len(built) > 0 {
			return "rebuild", s.execIndex(ctx, conn, "DELETE FROM "+s.indexTable(x.Name))
		}
		return "", nil
	}
	S := cur.SnapshotID
	var base *builtSnapshot
	for i, b := range built {
		if b.snapshot == S && b.generation == gen {
			// Up to date: what is left of others goes.
			if len(built) > 1 {
				return "", s.execIndex(ctx, conn, fmt.Sprintf("DELETE FROM %s WHERE source_snapshot_id <> %d", s.indexTable(x.Name), S))
			}
			return "", nil
		}
		if b.generation == gen && (base == nil || b.snapshot > base.snapshot) {
			base = &built[i]
		}
	}
	if err := s.sizeIndexRowGroups(ctx, conn, x, tbl); err != nil {
		s.log.Warn("could not size a bitmap index's row groups", "index", x.Name, "err", err)
	}
	src := s.indexSource(x)
	counted := s.indexCounted(ctx, x)
	how := "rebuild"
	var insert string
	if base != nil {
		switch op, added := snapshotsSince(ctx, tbl, base.snapshot, S); op {
		case table.OpReplace:
			how = "carry"
			counts := ""
			if counted {
				counts = ", row_count, members"
			}
			insert = fmt.Sprintf("INSERT INTO %s SELECT %d, %s generation, bm%s FROM %s WHERE source_snapshot_id = %d",
				s.indexTable(x.Name), S, groupList(x, ""), counts, s.indexTable(x.Name), base.snapshot)
		case table.OpAppend:
			files := "read_parquet([" + strings.Join(quoteAll(added), ", ") + "])"
			complete, err := s.idsCompleteIn(ctx, conn, x, files)
			if err == nil && !complete {
				return "unmapped", nil
			}
			if err == nil {
				how = "append"
				g, h := groupList(x, ""), quoteIdent(iddomain.HiddenColumn(x.Column))
				files = elementsOf(x, files)
				if counted {
					insert = fmt.Sprintf("INSERT INTO %s SELECT %d, %s %d, bm, n, rb_cardinality(bm) FROM (SELECT %s rb_or_agg(bm) AS bm, sum(n) AS n FROM ("+
						"SELECT %s bm, row_count AS n FROM %s WHERE source_snapshot_id = %d "+
						"UNION ALL SELECT %s rb_agg(CAST(%s AS UINTEGER)) AS bm, count(%s) AS n FROM %s %s) GROUP BY ALL)",
						s.indexTable(x.Name), S, g, gen, g, g, s.indexTable(x.Name), base.snapshot, g, h, h, files, groupBy(x))
				} else {
					insert = fmt.Sprintf("INSERT INTO %s SELECT %d, %s %d, rb_or_agg(bm) FROM (SELECT %s bm FROM %s WHERE source_snapshot_id = %d "+
						"UNION ALL SELECT %s rb_agg(CAST(%s AS UINTEGER)) FROM %s %s) GROUP BY ALL",
						s.indexTable(x.Name), S, g, gen, g, s.indexTable(x.Name), base.snapshot, g, h, files, groupBy(x))
				}
			}
		}
	}
	if insert == "" {
		at := fmt.Sprintf("%s AT (VERSION => %d)", src, S)
		complete, err := s.idsCompleteIn(ctx, conn, x, at)
		if err != nil {
			return "", err
		}
		if !complete {
			return "unmapped", nil
		}
		insert = s.rebuildSQL(x, S, gen, at, counted)
	}
	if err := s.execIndex(ctx, conn, insert); err != nil {
		if how != "append" {
			return "", err
		}
		// The added files could not be read as the table's columns (a
		// column renamed since, say): from the snapshot instead.
		s.log.Debug("could not add appended files to a bitmap index; building it again", "index", x.Name, "err", err)
		at := fmt.Sprintf("%s AT (VERSION => %d)", src, S)
		how = "rebuild"
		if err := s.execIndex(ctx, conn, s.rebuildSQL(x, S, gen, at, counted)); err != nil {
			return "", err
		}
	}
	if err := s.execIndex(ctx, conn, fmt.Sprintf("DELETE FROM %s WHERE source_snapshot_id <> %d", s.indexTable(x.Name), S)); err != nil {
		return how, err
	}
	s.ddl.TableChanged(iddomain.IndexSchema, x.Name)
	return how, nil
}

// Index tables' row groups. DuckDB writes an insert in row groups of the
// table's write.parquet.row-group-size rows, 122,880 by default, holding
// one uncompressed on each thread while it writes, and a scan reads a row
// group's column at a time, its strings outside DuckDB's memory limit. A
// bitmap takes up to two bytes an id, and an eighth of a byte for each id
// the domain has where dense: 90 MB at 730 million ids. An index of a few
// hundred such bitmaps in one row group took a scan of it to gigabytes, four
// times over in a report that counts four ways, past the machine's memory.
// So before each build an index's table gets row groups of about
// indexRowGroupBytes of bitmaps: at the average size of those it holds, or,
// before it holds any, at the most one bitmap of its source table can take.
// It changes only when off by half or more, as each change is a commit.
const (
	indexRowGroupBytes   = 32 << 20
	duckDBRowGroupsMax   = 122880
	rowGroupSizeProperty = "write.parquet.row-group-size"
)

// indexRowGroupRows is the rows a row group of bitmaps of bytes each takes.
func indexRowGroupRows(bytes int64) int64 {
	if bytes <= 0 {
		return 1
	}
	return min(max(indexRowGroupBytes/bytes, 1), duckDBRowGroupsMax)
}

// sizeIndexRowGroups sets x's table's row groups for the build about to
// write it from src.
func (s *Server) sizeIndexRowGroups(ctx context.Context, conn *duckdb.Conn, x iddomain.BitmapIndex, src *table.Table) error {
	idx, err := s.ddl.LoadTable(ctx, iddomain.IndexSchema, x.Name)
	if err != nil {
		return err
	}
	var per int64
	if size, rows, err := tableSize(ctx, idx); err != nil {
		return err
	} else if size > 0 && rows > 0 {
		per = size / rows
	} else if rows, err := snapshotRows(ctx, src); err != nil {
		return err
	} else {
		// No bitmaps yet: one of all the source's rows, at two bytes an
		// id, or of all 2^32 ids, whichever is less.
		per = min(2*rows+8, 1<<29)
	}
	want := indexRowGroupRows(per)
	if cur, err := strconv.ParseInt(idx.Properties()[rowGroupSizeProperty], 10, 64); err == nil && cur > 0 && 2*want < 3*cur && 2*cur < 3*want {
		return nil
	}
	return s.execIndex(ctx, conn, fmt.Sprintf("SELECT * FROM set_iceberg_table_properties(%s, {'%s': '%d'})", s.indexTable(x.Name), rowGroupSizeProperty, want))
}

// tableSize returns the bytes and rows of tbl's current data files: from
// its snapshot's summary, or from the files, as DuckDB's commits leave the
// files' size out of the summary.
func tableSize(ctx context.Context, tbl *table.Table) (size, rows int64, err error) {
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return 0, 0, nil
	}
	if snap.Summary != nil {
		size, _ = strconv.ParseInt(snap.Summary.Properties["total-files-size"], 10, 64)
		rows, _ = strconv.ParseInt(snap.Summary.Properties["total-records"], 10, 64)
		if size > 0 && rows > 0 {
			return size, rows, nil
		}
	}
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return 0, 0, err
	}
	size, rows = 0, 0
	for _, t := range tasks {
		size += t.File.FileSizeBytes()
		rows += t.File.Count()
	}
	return size, rows, nil
}

// snapshotRows returns the rows of tbl's current data files.
func snapshotRows(ctx context.Context, tbl *table.Table) (int64, error) {
	if snap := tbl.CurrentSnapshot(); snap != nil && snap.Summary != nil {
		if rows, err := strconv.ParseInt(snap.Summary.Properties["total-records"], 10, 64); err == nil && rows > 0 {
			return rows, nil
		}
	}
	_, rows, err := tableSize(ctx, tbl)
	return rows, err
}

// rebuildSQL builds x's bitmaps of snapshot S, at the generation gen, from
// at, the source read at S; counted, with each one's rows and members.
func (s *Server) rebuildSQL(x iddomain.BitmapIndex, S int64, gen int, at string, counted bool) string {
	g, h := groupList(x, ""), quoteIdent(iddomain.HiddenColumn(x.Column))
	at = elementsOf(x, at)
	if !counted {
		return fmt.Sprintf("INSERT INTO %s SELECT %d, %s %d, rb_agg(CAST(%s AS UINTEGER)) FROM %s %s", s.indexTable(x.Name), S, g, gen, h, at, groupBy(x))
	}
	return fmt.Sprintf("INSERT INTO %s SELECT %d, %s %d, bm, n, rb_cardinality(bm) FROM (SELECT %s rb_agg(CAST(%s AS UINTEGER)) AS bm, count(%s) AS n FROM %s %s)",
		s.indexTable(x.Name), S, g, gen, g, h, h, at, groupBy(x))
}

// elementsOf is the rows of from that x's bitmaps are of: from itself, or,
// for an index of a list's elements, one row for each element, which the
// index's column holds, with the row's id. Its rows count each element as
// often as lists hold it.
func elementsOf(x iddomain.BitmapIndex, from string) string {
	if !x.Elements {
		return from
	}
	col, h := quoteIdent(x.Groups[0]), quoteIdent(iddomain.HiddenColumn(x.Column))
	return "(SELECT unnest(" + col + ") AS " + col + ", " + h + " FROM " + from + ") AS e"
}

// indexCounted reports whether x's table records each bitmap's rows and
// members, which the tables of indexes added before they did lack (see
// addIndex).
func (s *Server) indexCounted(ctx context.Context, x iddomain.BitmapIndex) bool {
	idx, err := s.ddl.LoadTable(ctx, iddomain.IndexSchema, x.Name)
	if err != nil {
		return false
	}
	_, rows := idx.Schema().FindFieldByNameCaseInsensitive("row_count")
	_, members := idx.Schema().FindFieldByNameCaseInsensitive("members")
	return rows && members
}

// indexSource is the quoted name of x's source table.
func (s *Server) indexSource(x iddomain.BitmapIndex) string {
	return quoteName([]string{s.catalogName(), x.Schema, x.Table})
}

// groupList lists x's group columns, each followed by a comma, qualified
// by q if set.
func groupList(x iddomain.BitmapIndex, q string) string {
	var b strings.Builder
	for _, g := range x.Groups {
		if q != "" {
			b.WriteString(q + ".")
		}
		b.WriteString(quoteIdent(g) + ", ")
	}
	return b.String()
}

// groupBy is the GROUP BY of x's bitmaps, empty for an index of the whole
// table.
func groupBy(x iddomain.BitmapIndex) string {
	if len(x.Groups) == 0 {
		return ""
	}
	gs := make([]string, len(x.Groups))
	for i, g := range x.Groups {
		gs[i] = quoteIdent(g)
	}
	return "GROUP BY " + strings.Join(gs, ", ")
}

func quoteAll(ss []string) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = quoteLiteral(s)
	}
	return out
}

// idsCompleteIn reports whether every row of from (a relation's SQL) with a
// value of x's member column has its id.
func (s *Server) idsCompleteIn(ctx context.Context, conn *duckdb.Conn, x iddomain.BitmapIndex, from string) (bool, error) {
	rows, err := queryRows(ctx, conn, fmt.Sprintf("SELECT count(*) FROM %s WHERE %s IS NOT NULL AND %s IS NULL",
		from, quoteIdent(x.Column), quoteIdent(iddomain.HiddenColumn(x.Column))))
	if err != nil {
		return false, err
	}
	if len(rows) != 1 || len(rows[0]) != 1 {
		return false, errors.New("counting rows without ids returned no count")
	}
	n, _ := rows[0][0].(int64)
	return n == 0, nil
}

func (s *Server) execIndex(ctx context.Context, conn *duckdb.Conn, q string) error {
	_, err := conn.ExecContext(ctx, q, []driver.NamedValue{})
	return err
}

// snapshotsSince says how tbl's snapshot to went on from from: OpAppend,
// with the data files the snapshots in between added, when each of them
// appended and nothing else; OpReplace when each replaced files without
// changing the data; and "" otherwise, or when from is not to's ancestor.
func snapshotsSince(ctx context.Context, tbl *table.Table, from, to int64) (table.Operation, []string) {
	md := tbl.Metadata()
	var chain []table.Snapshot
	for id := to; id != from; {
		snap := md.SnapshotByID(id)
		if snap == nil || snap.ParentSnapshotID == nil || snap.Summary == nil {
			return "", nil
		}
		chain = append(chain, *snap)
		id = *snap.ParentSnapshotID
	}
	if len(chain) == 0 {
		return "", nil
	}
	op := chain[0].Summary.Operation
	for _, snap := range chain {
		if snap.Summary.Operation != op {
			return "", nil
		}
	}
	switch op {
	case table.OpReplace:
		return op, nil
	case table.OpAppend:
	default:
		return "", nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return "", nil
	}
	var added []string
	for _, snap := range chain {
		manifests, err := snap.Manifests(fs)
		if err != nil {
			return "", nil
		}
		for _, m := range manifests {
			if m.SnapshotID() != snap.SnapshotID {
				// Only a snapshot's own manifests hold files it added.
				continue
			}
			for e, err := range m.Entries(fs, true) {
				if err != nil {
					return "", nil
				}
				if e.Status() != iceberg.EntryStatusADDED || e.SnapshotID() != snap.SnapshotID {
					continue
				}
				f := e.DataFile()
				if f.ContentType() != iceberg.EntryContentData {
					// Deletes are no append.
					return "", nil
				}
				added = append(added, f.FilePath())
			}
		}
	}
	if len(added) == 0 {
		return table.OpReplace, nil
	}
	return op, added
}
