package distributed

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/duckdb/duckdb-go/v2"
)

// MergedFunction is the table function through which the SQL of an
// execution reads a merge of sorted rows made as it reads them (see
// mergeOrdered): __igloo_merged(), whose rows are those of the merge that
// the context of the statement running it holds (WithMerged). Whoever
// runs the SQL registers it once on its database (RegisterMerged). It
// takes no arguments, and so no name or pointer a user could pass: a user
// who calls it gets nothing, the merge being in no context of theirs; and
// DuckDB keeps its arrow_scan, which reads a stream by its address, from
// sessions without external access, as Igloo's are.
const MergedFunction = "__igloo_merged"

type mergedKey struct{}

// WithMerged returns ctx holding the merges the execution's SQL reads,
// which its statement must run under, prepared and run.
func WithMerged(ctx context.Context, merged []*MergedView) context.Context {
	if len(merged) == 0 {
		return ctx
	}
	return context.WithValue(ctx, mergedKey{}, merged[0])
}

// errNoMerge is what a statement that reads MergedFunction outside an
// execution's SQL fails with.
var errNoMerge = errors.New(MergedFunction + " reads only the merge of a distributed statement's sorted rows")

// RegisterMerged registers MergedFunction on conn's database.
func RegisterMerged(conn *sql.Conn) error {
	return duckdb.RegisterTableUDF(conn, MergedFunction, duckdb.ChunkTableFunction{
		BindArgumentsContext: func(ctx context.Context, _ map[string]any, _ ...any) (duckdb.ChunkTableSource, error) {
			m, _ := ctx.Value(mergedKey{}).(*MergedView)
			if m == nil {
				return nil, errNoMerge
			}
			return newMergedSource(m.Reader)
		},
	})
}

// mergedSource is MergedFunction's rows: those of a merge's reader, copied
// into DuckDB's chunks a column at a time, read by one thread in order.
type mergedSource struct {
	r      array.RecordReader
	cols   []duckdb.ColumnInfo
	copies []columnCopy
	// rec is the reader's batch being copied, from its row next.
	rec  arrow.RecordBatch
	next int
	// out are the chunk's columns, ok those the statement reads.
	out []duckdb.ChunkColumn
	ok  []bool
}

// columnCopy copies n values of a, from its row from, into column dst
// from its row at, NULLs as NULLs.
type columnCopy func(dst duckdb.ChunkColumn, at int, a arrow.Array, from, n int)

// mergedColumns checks that MergedFunction can return columns of schema,
// and returns them with what copies their values.
func mergedColumns(schema *arrow.Schema) ([]duckdb.ColumnInfo, []columnCopy, error) {
	var cols []duckdb.ColumnInfo
	var copies []columnCopy
	for _, f := range schema.Fields() {
		info, copyCol, err := columnOf(f.Type)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: column %s of %s", errNotMergeable, f.Name, f.Type)
		}
		cols = append(cols, duckdb.ColumnInfo{Name: f.Name, T: info})
		copies = append(copies, copyCol)
	}
	return cols, copies, nil
}

func newMergedSource(r array.RecordReader) (*mergedSource, error) {
	cols, copies, err := mergedColumns(r.Schema())
	if err != nil {
		return nil, err
	}
	return &mergedSource{r: r, cols: cols, copies: copies, out: make([]duckdb.ChunkColumn, len(cols)), ok: make([]bool, len(cols))}, nil
}

func (s *mergedSource) ColumnInfos() []duckdb.ColumnInfo     { return s.cols }
func (s *mergedSource) Cardinality() *duckdb.CardinalityInfo { return nil }
func (s *mergedSource) Init()                                {}

// FillChunk copies the next rows of the merge, as many as a chunk holds.
func (s *mergedSource) FillChunk(chunk duckdb.DataChunk) error {
	for c := range s.copies {
		col, ok, err := chunk.ChunkColumn(c)
		if err != nil {
			return err
		}
		s.out[c], s.ok[c] = col, ok
	}
	capacity := duckdb.GetDataChunkCapacity()
	n := 0
	for n < capacity {
		if s.rec == nil || s.next >= int(s.rec.NumRows()) {
			if !s.r.Next() {
				if err := s.r.Err(); err != nil {
					return err
				}
				break
			}
			s.rec, s.next = s.r.RecordBatch(), 0
		}
		take := min(capacity-n, int(s.rec.NumRows())-s.next)
		for c, copyCol := range s.copies {
			if s.ok[c] {
				copyCol(s.out[c], n, s.rec.Column(c), s.next, take)
			}
		}
		n += take
		s.next += take
	}
	return chunk.SetSize(n)
}

// columnOf returns the DuckDB type of an Arrow type MergedFunction returns,
// and what copies its values.
func columnOf(dt arrow.DataType) (duckdb.TypeInfo, columnCopy, error) {
	typed := func(t duckdb.Type, copyCol columnCopy) (duckdb.TypeInfo, columnCopy, error) {
		info, err := duckdb.NewTypeInfo(t)
		return info, copyCol, err
	}
	switch dt := dt.(type) {
	case *arrow.Int8Type:
		return typed(duckdb.TYPE_TINYINT, copyFixed((*array.Int8).Int8Values))
	case *arrow.Int16Type:
		return typed(duckdb.TYPE_SMALLINT, copyFixed((*array.Int16).Int16Values))
	case *arrow.Int32Type:
		return typed(duckdb.TYPE_INTEGER, copyFixed((*array.Int32).Int32Values))
	case *arrow.Int64Type:
		return typed(duckdb.TYPE_BIGINT, copyFixed((*array.Int64).Int64Values))
	case *arrow.Uint8Type:
		return typed(duckdb.TYPE_UTINYINT, copyFixed((*array.Uint8).Uint8Values))
	case *arrow.Uint16Type:
		return typed(duckdb.TYPE_USMALLINT, copyFixed((*array.Uint16).Uint16Values))
	case *arrow.Uint32Type:
		return typed(duckdb.TYPE_UINTEGER, copyFixed((*array.Uint32).Uint32Values))
	case *arrow.Uint64Type:
		return typed(duckdb.TYPE_UBIGINT, copyFixed((*array.Uint64).Uint64Values))
	case *arrow.Float32Type:
		return typed(duckdb.TYPE_FLOAT, copyFixed((*array.Float32).Float32Values))
	case *arrow.Float64Type:
		return typed(duckdb.TYPE_DOUBLE, copyFixed((*array.Float64).Float64Values))
	case *arrow.Date32Type:
		// Days since 1970, as DuckDB's DATE.
		return typed(duckdb.TYPE_DATE, copyFixed((*array.Date32).Date32Values))
	case *arrow.TimestampType:
		// Ticks of the unit since 1970, as DuckDB's timestamps of the unit.
		t := map[arrow.TimeUnit]duckdb.Type{arrow.Second: duckdb.TYPE_TIMESTAMP_S, arrow.Millisecond: duckdb.TYPE_TIMESTAMP_MS,
			arrow.Microsecond: duckdb.TYPE_TIMESTAMP, arrow.Nanosecond: duckdb.TYPE_TIMESTAMP_NS}[dt.Unit]
		if dt.TimeZone != "" {
			if dt.Unit != arrow.Microsecond {
				break
			}
			t = duckdb.TYPE_TIMESTAMP_TZ
		}
		return typed(t, copyFixed((*array.Timestamp).TimestampValues))
	case *arrow.Time64Type:
		// Microseconds since midnight, as DuckDB's TIME.
		if dt.Unit == arrow.Microsecond {
			return typed(duckdb.TYPE_TIME, copyFixed((*array.Time64).Time64Values))
		}
		return typed(duckdb.TYPE_TIME, copyConverted((*array.Time64).Time64Values, func(v arrow.Time64) int64 { return int64(v) / 1000 }))
	case *arrow.BooleanType:
		return typed(duckdb.TYPE_BOOLEAN, copyBool)
	case *arrow.StringType:
		return typed(duckdb.TYPE_VARCHAR, copyStrings(func(a *array.String, i int) []byte {
			s := a.Value(i)
			return unsafe.Slice(unsafe.StringData(s), len(s))
		}))
	case *arrow.LargeStringType:
		return typed(duckdb.TYPE_VARCHAR, copyStrings(func(a *array.LargeString, i int) []byte {
			s := a.Value(i)
			return unsafe.Slice(unsafe.StringData(s), len(s))
		}))
	case *arrow.BinaryType:
		return typed(duckdb.TYPE_BLOB, copyStrings((*array.Binary).Value))
	case *arrow.LargeBinaryType:
		return typed(duckdb.TYPE_BLOB, copyStrings((*array.LargeBinary).Value))
	case *arrow.Decimal128Type:
		if dt.Precision < 1 || dt.Precision > 38 || dt.Scale < 0 || dt.Scale > dt.Precision {
			break
		}
		info, err := duckdb.NewDecimalInfo(uint8(dt.Precision), uint8(dt.Scale))
		values := (*array.Decimal128).Values
		// DuckDB keeps a decimal as the smallest integer its width fits.
		switch {
		case dt.Precision <= 4:
			return info, copyConverted(values, func(v decimal128.Num) int16 { return int16(v.LowBits()) }), err
		case dt.Precision <= 9:
			return info, copyConverted(values, func(v decimal128.Num) int32 { return int32(v.LowBits()) }), err
		case dt.Precision <= 18:
			return info, copyConverted(values, func(v decimal128.Num) int64 { return int64(v.LowBits()) }), err
		}
		// A HUGEINT: the low 64 bits, then the high, as decimal128.Num.
		return info, copyFixed(values), err
	}
	return nil, nil, fmt.Errorf("type %s", dt)
}

// setNulls makes the values at of dst NULL where a, from its row from, is.
func setNulls(dst duckdb.ChunkColumn, at int, a arrow.Array, from, n int) {
	if a.NullN() == 0 {
		return
	}
	for k := range n {
		if a.IsNull(from + k) {
			dst.SetNull(at + k)
		}
	}
}

// copyFixed copies values of a type whose memory is DuckDB's, in bulk.
func copyFixed[A arrow.Array, T any](values func(A) []T) columnCopy {
	return func(dst duckdb.ChunkColumn, at int, a arrow.Array, from, n int) {
		out := unsafe.Slice((*T)(dst.Data()), at+n)[at:]
		copy(out, values(a.(A))[from:from+n])
		setNulls(dst, at, a, from, n)
	}
}

// copyConverted copies values converted one by one to DuckDB's type U.
func copyConverted[A arrow.Array, T, U any](values func(A) []T, convert func(T) U) columnCopy {
	return func(dst duckdb.ChunkColumn, at int, a arrow.Array, from, n int) {
		out := unsafe.Slice((*U)(dst.Data()), at+n)[at:]
		for k, v := range values(a.(A))[from : from+n] {
			out[k] = convert(v)
		}
		setNulls(dst, at, a, from, n)
	}
}

// copyBool copies booleans, which Arrow keeps a bit each and DuckDB a
// byte.
func copyBool(dst duckdb.ChunkColumn, at int, a arrow.Array, from, n int) {
	b := a.(*array.Boolean)
	out := unsafe.Slice((*bool)(dst.Data()), at+n)[at:]
	for k := range n {
		out[k] = b.Value(from + k)
	}
	setNulls(dst, at, a, from, n)
}

// copyStrings copies text or bytes: short values into the vector, longer
// ones gathered for one call into DuckDB, which copies each.
func copyStrings[A arrow.Array](value func(A, int) []byte) columnCopy {
	var w *duckdb.Strings
	return func(dst duckdb.ChunkColumn, at int, a arrow.Array, from, n int) {
		arr := a.(A)
		w = dst.Strings(w)
		for k := range n {
			if a.IsNull(from + k) {
				dst.SetNull(at + k)
				continue
			}
			w.Add(at+k, value(arr, from+k))
		}
		w.Flush()
	}
}
