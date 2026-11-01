package distributed

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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
// a DuckDB chunk at a time, read by one thread in order.
type mergedSource struct {
	r    array.RecordReader
	cols []duckdb.ColumnInfo
	sets []columnSetter
	// rec is the reader's batch being copied, from its row next.
	rec  arrow.RecordBatch
	next int
}

// columnSetter writes value i of a into a chunk's column col at row.
type columnSetter func(chunk *duckdb.DataChunk, col, row int, a arrow.Array, i int) error

// mergedColumns checks that MergedFunction can return columns of schema,
// and returns them with what writes their values.
func mergedColumns(schema *arrow.Schema) ([]duckdb.ColumnInfo, []columnSetter, error) {
	var cols []duckdb.ColumnInfo
	var sets []columnSetter
	for _, f := range schema.Fields() {
		info, set, err := columnOf(f.Type)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: column %s of %s", errNotMergeable, f.Name, f.Type)
		}
		cols = append(cols, duckdb.ColumnInfo{Name: f.Name, T: info})
		sets = append(sets, set)
	}
	return cols, sets, nil
}

func newMergedSource(r array.RecordReader) (*mergedSource, error) {
	cols, sets, err := mergedColumns(r.Schema())
	if err != nil {
		return nil, err
	}
	return &mergedSource{r: r, cols: cols, sets: sets}, nil
}

func (s *mergedSource) ColumnInfos() []duckdb.ColumnInfo     { return s.cols }
func (s *mergedSource) Cardinality() *duckdb.CardinalityInfo { return nil }
func (s *mergedSource) Init()                                {}

// FillChunk copies the next rows of the merge, as many as a chunk holds.
func (s *mergedSource) FillChunk(chunk duckdb.DataChunk) error {
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
		for c, set := range s.sets {
			a := s.rec.Column(c)
			for k := range take {
				if err := set(&chunk, c, n+k, a, s.next+k); err != nil {
					return err
				}
			}
		}
		n += take
		s.next += take
	}
	return chunk.SetSize(n)
}

// columnOf returns the DuckDB type of an Arrow type MergedFunction returns,
// and what writes its values.
func columnOf(dt arrow.DataType) (duckdb.TypeInfo, columnSetter, error) {
	typed := func(t duckdb.Type, set columnSetter) (duckdb.TypeInfo, columnSetter, error) {
		info, err := duckdb.NewTypeInfo(t)
		return info, set, err
	}
	switch dt := dt.(type) {
	case *arrow.Int8Type:
		return typed(duckdb.TYPE_TINYINT, setOf((*array.Int8).Value))
	case *arrow.Int16Type:
		return typed(duckdb.TYPE_SMALLINT, setOf((*array.Int16).Value))
	case *arrow.Int32Type:
		return typed(duckdb.TYPE_INTEGER, setOf((*array.Int32).Value))
	case *arrow.Int64Type:
		return typed(duckdb.TYPE_BIGINT, setOf((*array.Int64).Value))
	case *arrow.Uint8Type:
		return typed(duckdb.TYPE_UTINYINT, setOf((*array.Uint8).Value))
	case *arrow.Uint16Type:
		return typed(duckdb.TYPE_USMALLINT, setOf((*array.Uint16).Value))
	case *arrow.Uint32Type:
		return typed(duckdb.TYPE_UINTEGER, setOf((*array.Uint32).Value))
	case *arrow.Uint64Type:
		return typed(duckdb.TYPE_UBIGINT, setOf((*array.Uint64).Value))
	case *arrow.Float32Type:
		return typed(duckdb.TYPE_FLOAT, setOf((*array.Float32).Value))
	case *arrow.Float64Type:
		return typed(duckdb.TYPE_DOUBLE, setOf((*array.Float64).Value))
	case *arrow.BooleanType:
		return typed(duckdb.TYPE_BOOLEAN, setOf((*array.Boolean).Value))
	case *arrow.StringType:
		return typed(duckdb.TYPE_VARCHAR, setOf((*array.String).Value))
	case *arrow.LargeStringType:
		return typed(duckdb.TYPE_VARCHAR, setOf((*array.LargeString).Value))
	case *arrow.BinaryType:
		return typed(duckdb.TYPE_BLOB, setOf((*array.Binary).Value))
	case *arrow.LargeBinaryType:
		return typed(duckdb.TYPE_BLOB, setOf((*array.LargeBinary).Value))
	case *arrow.Date32Type:
		return typed(duckdb.TYPE_DATE, setOf(func(a *array.Date32, i int) time.Time { return a.Value(i).ToTime() }))
	case *arrow.TimestampType:
		t := map[arrow.TimeUnit]duckdb.Type{arrow.Second: duckdb.TYPE_TIMESTAMP_S, arrow.Millisecond: duckdb.TYPE_TIMESTAMP_MS,
			arrow.Microsecond: duckdb.TYPE_TIMESTAMP, arrow.Nanosecond: duckdb.TYPE_TIMESTAMP_NS}[dt.Unit]
		if dt.TimeZone != "" {
			if dt.Unit != arrow.Microsecond {
				break
			}
			t = duckdb.TYPE_TIMESTAMP_TZ
		}
		unit := dt.Unit
		return typed(t, setOf(func(a *array.Timestamp, i int) time.Time { return a.Value(i).ToTime(unit) }))
	case *arrow.Time64Type:
		perTick := time.Microsecond
		if dt.Unit == arrow.Nanosecond {
			perTick = time.Nanosecond
		}
		midnight := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
		return typed(duckdb.TYPE_TIME, setOf(func(a *array.Time64, i int) time.Time {
			return midnight.Add(time.Duration(a.Value(i)) * perTick)
		}))
	case *arrow.Decimal128Type:
		if dt.Precision < 1 || dt.Precision > 38 || dt.Scale < 0 || dt.Scale > dt.Precision {
			break
		}
		info, err := duckdb.NewDecimalInfo(uint8(dt.Precision), uint8(dt.Scale))
		if dt.Precision <= 18 {
			return info, setOf(func(a *array.Decimal128, i int) int64 { return int64(a.Value(i).LowBits()) }), err
		}
		return info, setOf(func(a *array.Decimal128, i int) any { return a.Value(i).BigInt() }), err
	}
	return nil, nil, fmt.Errorf("type %s", dt)
}

// setOf is a columnSetter of arrays of type A, read by value, NULLs as
// NULLs.
func setOf[A arrow.Array, T any](value func(A, int) T) columnSetter {
	return func(chunk *duckdb.DataChunk, col, row int, a arrow.Array, i int) error {
		if a.IsNull(i) {
			return chunk.SetValue(col, row, nil)
		}
		return duckdb.SetChunkValue(*chunk, col, row, value(a.(A), i))
	}
}
