package pgwire

import (
	"cmp"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"

	"github.com/akkio-inc/igloo/internal/filerewrite"
	"github.com/akkio-inc/igloo/internal/iddomain"
)

// Filling many rows' ids by writing their files again (see mapColumn).
//
// An UPDATE of a member table's column of ids, as DuckDB carries one out on
// an Iceberg table, deletes the rows it changes and appends them again in
// the order its join produces them, from every thread at once: a table laid
// out by a column, as a table of audiences' members is by audience, comes
// out of its first mapping in hundreds of files with every value in each,
// and a filter on that column that read a few row groups reads them all
// (0.3 s against 14 s, for one audience of 141 million rows). So a fill of
// at least idFillRewriteRows rows writes again, one after another and each
// in its rows' order, the data files that hold rows without ids, with the
// ids the dictionary has added, as compaction writes files (see package
// filerewrite), and commits them in their place: each file's rows, and so
// the ranges of its columns, stay as they were.

// idFillRewriteRows is the fewest rows without ids that a fill writes their
// files again for; fewer are filled by an UPDATE. Tests lower it
// (Options.idFillRewriteRows).
const idFillRewriteRows = 1_000_000

// idFillRebases is how many times a fill commits its files again on a
// table that others only appended to meanwhile, before it leaves the
// rows to the next pass.
const idFillRebases = 5

// errIDFillUnsupported is a table whose files a fill cannot write again:
// the UPDATE fills its rows instead.
var errIDFillUnsupported = errors.New("the files cannot be written again")

// missingRows counts the rows of tbl, a quoted name, whose member column
// has a value and whose ids are NULL.
func missingRows(ctx context.Context, conn idConn, tbl, col, hidden string) (int64, error) {
	rows, err := conn.QueryContext(ctx, "SELECT count(*) FROM "+tbl+" WHERE "+hidden+" IS NULL AND "+col+" IS NOT NULL", nil)
	if err != nil {
		return 0, err
	}
	return countOf(rows)
}

// fillByRewrite fills the ids of m's rows in schema.table that have none,
// from the dictionary, by writing again the data files that hold such rows
// (see the top of this file). It returns how many rows it filled.
func (s *Server) fillByRewrite(ctx context.Context, conn idConn, schema, tableName string, m idMember) (int64, error) {
	if s.ddl == nil {
		return 0, errIDFillUnsupported
	}
	tbl, err := s.ddl.LoadTable(ctx, schema, tableName)
	if err != nil {
		return 0, err
	}
	colField, ok := tbl.Schema().FindFieldByNameCaseInsensitive(m.Column)
	if !ok {
		return 0, fmt.Errorf("no column %s: %w", m.Column, errIDFillUnsupported)
	}
	idField, ok := tbl.Schema().FindFieldByNameCaseInsensitive(iddomain.HiddenColumn(m.Column))
	if !ok {
		return 0, fmt.Errorf("no column of ids for %s: %w", m.Column, errIDFillUnsupported)
	}
	if idField.Type.Type() != iceberg.PrimitiveTypes.Int64.Type() {
		return 0, fmt.Errorf("ids of type %s: %w", idField.Type, errIDFillUnsupported)
	}
	if tbl.CurrentSnapshot() == nil {
		return 0, nil
	}
	tasks, err := tbl.Scan(table.WithRowFilter(iceberg.IsNull(iceberg.Reference(idField.Name)))).PlanFiles(ctx)
	if err != nil {
		return 0, err
	}
	if len(tasks) == 0 {
		return 0, nil
	}
	for _, t := range tasks {
		// iceberg-go cannot read deletion vectors yet.
		if len(t.DeletionVectorFiles) > 0 {
			return 0, fmt.Errorf("deletion vectors: %w", errIDFillUnsupported)
		}
	}
	budget := cmp.Or(s.rewriteMemory, filerewrite.DefaultMemory)
	ids, err := s.fillLookup(ctx, conn, quoteName([]string{s.catalogName(), schema, tableName}), m, budget/4)
	if err != nil {
		return 0, err
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return 0, err
	}
	rctx, _, stop := filerewrite.Metered(ctx, budget)
	defer stop(nil)
	var old, added []iceberg.DataFile
	var filled int64
	for _, t := range tasks {
		files, _, err := filerewrite.Rewrite(rctx, tbl, fs, []table.FileScanTask{t}, budget, func(rec arrow.RecordBatch) (arrow.RecordBatch, error) {
			out, n, err := fillRecord(compute.GetAllocator(rctx), rec, colField.Name, idField.Name, ids)
			rec.Release()
			filled += n
			return out, err
		})
		if err != nil {
			return 0, fmt.Errorf("write %s again: %w", t.File.FilePath(), err)
		}
		old = append(old, t.File)
		added = append(added, files...)
	}
	if err := s.commitFill(ctx, schema, tableName, tbl, old, added); err != nil {
		return 0, err
	}
	s.ddl.TableChanged(schema, tableName)
	s.indexesChanged(schema, tableName)
	return filled, nil
}

// commitFill puts added in the place of old in tbl, the table as the fill
// read it. When another commit came first and only appended since, it
// commits again on the new head: appends add files the fill did not read,
// and no delete that applies to one it wrote. Otherwise the fill is left to
// the next pass, which finds the rows still without ids.
func (s *Server) commitFill(ctx context.Context, schema, tableName string, tbl *table.Table, old, added []iceberg.DataFile) error {
	base := tbl.CurrentSnapshot().SnapshotID
	for attempt := 0; ; attempt++ {
		tx := tbl.NewTransaction()
		rw := tx.NewRewrite(map[string]string{"igloo.operation": "id-fill"})
		rw.Apply(old, added, nil)
		if err := rw.Commit(ctx); err != nil {
			return err
		}
		_, err := tx.CommitWithoutRetry(ctx)
		if err == nil || !errors.Is(err, table.ErrCommitFailed) || attempt+1 >= idFillRebases {
			return err
		}
		latest, lerr := s.ddl.LoadTable(ctx, schema, tableName)
		if lerr != nil {
			return lerr
		}
		head := latest.CurrentSnapshot()
		if head == nil || !onlyAppendedSince(latest, base, head.SnapshotID) {
			return err
		}
		tbl = latest
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(rand.Int64N(int64(10*time.Millisecond) << min(attempt, 4)))):
		}
	}
}

// onlyAppendedSince reports whether every snapshot from head back to base,
// in tbl, is an append.
func onlyAppendedSince(tbl *table.Table, base, head int64) bool {
	for id := head; id != base; {
		snap := tbl.SnapshotByID(id)
		if snap == nil || snap.Summary == nil || snap.Summary.Operation != table.OpAppend || snap.ParentSnapshotID == nil {
			return false
		}
		id = *snap.ParentSnapshotID
	}
	return true
}

// fillLookup returns the dictionary's ids of the values of m in tbl whose
// rows have none, keyed as fillKey keys them. Past limit bytes of keys it
// gives up, leaving the rows to the UPDATE, which DuckDB spills.
func (s *Server) fillLookup(ctx context.Context, conn idConn, tbl string, m idMember, limit int64) (map[string]int64, error) {
	col, hidden := quoteIdent(m.Column), quoteIdent(iddomain.HiddenColumn(m.Column))
	q := "SELECT CAST(igloo_dict.value AS VARCHAR), igloo_dict.id FROM " + s.dictionary(m.domain.Name) + " AS igloo_dict WHERE igloo_dict.value IN (SELECT DISTINCT CAST(igloo_t." +
		col + " AS " + m.domain.Type + ") FROM " + tbl + " AS igloo_t WHERE igloo_t." + hidden + " IS NULL AND igloo_t." + col + " IS NOT NULL)"
	rows, err := conn.QueryContext(ctx, q, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	ids := map[string]int64{}
	var bytes int64
	dest := make([]driver.Value, 2)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return ids, nil
			}
			return nil, err
		}
		key, _ := dest[0].(string)
		id, ok := dest[1].(int64)
		if !ok {
			continue
		}
		ids[key] = id
		if bytes += int64(len(key)) + 48; bytes > limit {
			return nil, fmt.Errorf("the values without ids take more than %d bytes: %w", limit, errIDFillUnsupported)
		}
	}
}

// fillRecord returns rec with the NULL ids of its rows whose member column
// (col) has a value the lookup has filled in, and how many it filled.
func fillRecord(mem memory.Allocator, rec arrow.RecordBatch, col, hidden string, ids map[string]int64) (arrow.RecordBatch, int64, error) {
	ci, hi := fieldIndex(rec.Schema(), col), fieldIndex(rec.Schema(), hidden)
	if ci < 0 || hi < 0 {
		return nil, 0, fmt.Errorf("columns %s and %s not in the files: %w", col, hidden, errIDFillUnsupported)
	}
	values, current := rec.Column(ci), rec.Column(hi)
	cur, ok := current.(*array.Int64)
	if !ok {
		return nil, 0, fmt.Errorf("ids as %s: %w", current.DataType(), errIDFillUnsupported)
	}
	key, err := fillKey(values)
	if err != nil {
		return nil, 0, err
	}
	b := array.NewInt64Builder(mem)
	defer b.Release()
	b.Reserve(int(rec.NumRows()))
	var filled int64
	for i := 0; i < int(rec.NumRows()); i++ {
		switch {
		case cur.IsValid(i):
			b.Append(cur.Value(i))
		case values.IsValid(i):
			if id, ok := ids[key(i)]; ok {
				b.Append(id)
				filled++
			} else {
				b.AppendNull()
			}
		default:
			b.AppendNull()
		}
	}
	filledIDs := b.NewInt64Array()
	defer filledIDs.Release()
	cols := make([]arrow.Array, rec.NumCols())
	for i := range cols {
		cols[i] = rec.Column(i)
	}
	cols[hi] = filledIDs
	return array.NewRecordBatch(rec.Schema(), cols, rec.NumRows()), filled, nil
}

func fieldIndex(s *arrow.Schema, name string) int {
	for i, f := range s.Fields() {
		if strings.EqualFold(f.Name, name) {
			return i
		}
	}
	return -1
}

// fillKey returns, for a member column's values, each value as DuckDB casts
// it to VARCHAR, as fillLookup keys the dictionary.
func fillKey(values arrow.Array) (func(int) string, error) {
	switch a := values.(type) {
	case *array.String:
		return a.Value, nil
	case *array.LargeString:
		return a.Value, nil
	case *array.StringView:
		return a.Value, nil
	case *array.Int64:
		return func(i int) string { return strconv.FormatInt(a.Value(i), 10) }, nil
	case *array.Int32:
		return func(i int) string { return strconv.FormatInt(int64(a.Value(i)), 10) }, nil
	case *array.FixedSizeBinary:
		if a.DataType().(*arrow.FixedSizeBinaryType).ByteWidth != 16 {
			break
		}
		return func(i int) string {
			u, _ := uuid.FromBytes(a.Value(i))
			return u.String()
		}, nil
	}
	return nil, fmt.Errorf("member values as %s: %w", values.DataType(), errIDFillUnsupported)
}
