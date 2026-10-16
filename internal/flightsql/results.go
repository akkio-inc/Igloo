package flightsql

import (
	"bytes"
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/akkio-inc/igloo/internal/pgwire"
)

// textBatchRows is how many rows of a result Igloo produces itself (SHOW
// GRANTS, FETCH from a cursor) go in one record batch.
const textBatchRows = 4096

// streamWriter sends a statement's result to a DoGet stream. DuckDB's
// record batches go out as they are; a result Igloo produces itself
// arrives as text rows, which it gathers into batches of strings.
type streamWriter struct {
	ctx    context.Context
	chunks chan<- flight.StreamChunk
	// ready receives the error that ended the statement before it sent
	// anything, or nil once schema is set.
	ready  chan error
	schema *arrow.Schema
	// cols are the current result's columns, for a result of text rows.
	cols     []pgwire.Column
	text     *array.RecordBuilder
	textRows int
}

var _ pgwire.ArrowWriter = (*streamWriter)(nil)

// announce sets the stream's schema. A statement that runs once for
// each of several sets of parameters must return the same columns each
// time.
func (w *streamWriter) announce(schema *arrow.Schema) error {
	if w.schema == nil {
		w.schema = schema
		w.ready <- nil
		return nil
	}
	if !w.schema.Equal(schema) {
		return status.Error(codes.FailedPrecondition, "the statement returned different columns for different parameters")
	}
	return nil
}

// finish ends the stream with err, the statement's outcome.
func (w *streamWriter) finish(err error) {
	if w.text != nil {
		w.text.Release()
	}
	switch {
	case w.schema == nil:
		w.ready <- err
	case err != nil:
		// The client sees it as the call's status, after the batches sent.
		w.chunks <- flight.StreamChunk{Err: err}
	}
}

func (w *streamWriter) send(rec arrow.RecordBatch) error {
	select {
	case w.chunks <- flight.StreamChunk{Data: rec}:
		return nil
	case <-w.ctx.Done():
		rec.Release()
		return w.ctx.Err()
	}
}

func (w *streamWriter) Records(rdr array.RecordReader) (int64, error) {
	if err := w.announce(rdr.Schema()); err != nil {
		return 0, err
	}
	var n int64
	for rdr.Next() {
		rec := rdr.RecordBatch()
		rec.Retain()
		n += rec.NumRows()
		if err := w.send(rec); err != nil {
			return n, err
		}
	}
	return n, rdr.Err()
}

func (w *streamWriter) Columns(cols []pgwire.Column) error {
	w.cols = cols
	return nil
}

func (w *streamWriter) Row(values []any) error {
	if w.text == nil {
		schema := pgwire.TextSchema(w.cols)
		if err := w.announce(schema); err != nil {
			return err
		}
		w.text = array.NewRecordBuilder(memory.DefaultAllocator, schema)
	}
	for i, v := range values {
		b := w.text.Field(i).(*array.StringBuilder)
		if s, ok := v.(string); ok {
			b.Append(s)
		} else {
			b.AppendNull()
		}
	}
	if w.textRows++; w.textRows == textBatchRows {
		return w.flush()
	}
	return nil
}

func (w *streamWriter) flush() error {
	w.textRows = 0
	return w.send(w.text.NewRecordBatch())
}

func (w *streamWriter) Complete(string) error {
	if w.text != nil {
		err := w.flush()
		w.text.Release()
		w.text = nil
		return err
	}
	if w.schema == nil {
		// A statement that returns no rows, or a result of text rows
		// without any.
		schema := emptySchema
		if len(w.cols) > 0 {
			schema = pgwire.TextSchema(w.cols)
		}
		return w.announce(schema)
	}
	return nil
}

// paramRows reads parameter values from a record batch a client bound:
// each row is one set of parameters, each column one parameter.
func paramRows(rec arrow.RecordBatch) ([][]any, error) {
	rows := make([][]any, rec.NumRows())
	for i := range rows {
		rows[i] = make([]any, rec.NumCols())
		for j, col := range rec.Columns() {
			v, err := paramValue(col, i)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "parameter $%d: %v", j+1, err)
			}
			rows[i][j] = v
		}
	}
	return rows, nil
}

// paramValue converts a parameter to a Go value, which the statement then
// encodes for the parameter's type (see pgwire.PreparedStatement.Run).
// Values that do not have a natural Go type go as their text.
func paramValue(arr arrow.Array, i int) (any, error) {
	if arr.IsNull(i) {
		return nil, nil
	}
	switch a := arr.(type) {
	case *array.Boolean:
		return a.Value(i), nil
	case *array.Int8:
		return int64(a.Value(i)), nil
	case *array.Int16:
		return int64(a.Value(i)), nil
	case *array.Int32:
		return int64(a.Value(i)), nil
	case *array.Int64:
		return a.Value(i), nil
	case *array.Uint8:
		return int64(a.Value(i)), nil
	case *array.Uint16:
		return int64(a.Value(i)), nil
	case *array.Uint32:
		return int64(a.Value(i)), nil
	case *array.Uint64:
		return a.Value(i), nil
	case *array.Float16:
		return float64(a.Value(i).Float32()), nil
	case *array.Float32:
		return float64(a.Value(i)), nil
	case *array.Float64:
		return a.Value(i), nil
	case *array.String:
		return a.Value(i), nil
	case *array.LargeString:
		return a.Value(i), nil
	case *array.StringView:
		return a.Value(i), nil
	case *array.Binary:
		return bytes.Clone(a.Value(i)), nil
	case *array.LargeBinary:
		return bytes.Clone(a.Value(i)), nil
	case *array.BinaryView:
		return bytes.Clone(a.Value(i)), nil
	case *array.Date32:
		return a.Value(i).ToTime(), nil
	case *array.Date64:
		return a.Value(i).ToTime(), nil
	case *array.Timestamp:
		unit := a.DataType().(*arrow.TimestampType).Unit
		return a.Value(i).ToTime(unit), nil
	case *array.Time32:
		unit := a.DataType().(*arrow.Time32Type).Unit
		return a.Value(i).ToTime(unit).Format("15:04:05.999999999"), nil
	case *array.Time64:
		unit := a.DataType().(*arrow.Time64Type).Unit
		return a.Value(i).ToTime(unit).Format("15:04:05.999999999"), nil
	}
	// Decimals, lists and the like.
	return arr.ValueStr(i), nil
}
