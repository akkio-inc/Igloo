package duckdb

import (
	"database/sql/driver"
	"errors"
	"io"
	"unsafe"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// Igloo patch: reading a result a chunk at a time, a column at a time. Next
// reads a row at a time, each value through a callback that makes it a Go
// value: an allocation for most, a call into DuckDB for each date, a big.Int
// for each decimal. A server that sends a result on to a client, in the
// client's encoding, needs none of them: NextChunk gives a result's next
// chunk, and a ChunkColumn of it the column's values where DuckDB holds them.

// ChunkRows are rows that can also be read a chunk at a time: those of
// Stmt.QueryContext and Stmt.QueryStreamContext.
type ChunkRows interface {
	driver.Rows
	// NextChunk returns the chunk that holds the result's next row, and
	// that row's place in it: the chunk it returned before while Next and
	// Skip have left rows of it unread, and the result's next chunk with
	// rows after that. At the result's end it returns io.EOF, or the error
	// that ended a result DuckDB streams. The chunk is good until Next or
	// NextChunk move past its last row, or Close.
	NextChunk() (chunk *DataChunk, row int, err error)
	// Skip passes over the next n rows of the chunk NextChunk returned, as
	// n calls of Next would.
	Skip(n int)
}

var (
	_ ChunkRows = (*rows)(nil)
	_ ChunkRows = (*streamRows)(nil)
)

func (r *rows) NextChunk() (*DataChunk, int, error) {
	if err := r.advance(); err != nil {
		return nil, 0, err
	}
	return &r.chunk, r.rowCount, nil
}

func (r *rows) Skip(n int) {
	r.rowCount = min(r.rowCount+n, r.chunk.size)
}

// NextChunk reads the next chunk, which DuckDB may compute now. At the end of
// the rows, it returns io.EOF, or the error that ended them, as Next does.
func (r *streamRows) NextChunk() (*DataChunk, int, error) {
	chunk, row, err := r.rows.NextChunk()
	if !errors.Is(err, io.EOF) {
		return chunk, row, err
	}
	if msg := mapping.ResultError(&r.res); msg != "" {
		return nil, 0, streamError(r.ctx, msg)
	}
	return nil, 0, io.EOF
}

// RowValue returns the value of column colIdx at row rowIdx of a chunk
// NextChunk returned, as Next gives it.
func (chunk *DataChunk) RowValue(colIdx, rowIdx int) (any, error) {
	column := &chunk.columns[colIdx]
	value, err := column.getFn(column, mapping.IdxT(rowIdx))
	if err != nil {
		return nil, getError(errAPI, addIndexToError(err, colIdx))
	}
	if bit, ok := value.(Bit); ok {
		return bit.String(), nil
	}
	return value, nil
}

// Type returns the column's type. A JSON column's is TYPE_VARCHAR.
func (c ChunkColumn) Type() Type { return c.v.Type }

// IsJSON reports whether the column's type is JSON, which DuckDB holds as
// VARCHAR.
func (c ChunkColumn) IsJSON() bool { return c.v.isJSON }

// Decimal returns the type a DECIMAL column's values are held as (SMALLINT,
// INTEGER, BIGINT or HUGEINT, each value the decimal without its point) and
// the column's scale.
func (c ChunkColumn) Decimal() (internal Type, scale uint8) {
	return c.v.internalType, c.v.decimalScale
}

// Fixed returns the memory of the column's first n values, each size bytes in
// the machine's byte order, for types of a fixed size (see Data). A NULL's
// bytes mean nothing.
func (c ChunkColumn) Fixed(n, size int) []byte {
	if n == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(c.v.dataPtr), n*size)
}

// Validity returns the column's validity mask for its first n values, a bit
// for each, from the lowest of the first word: 0 for NULL. It is nil when
// none is NULL.
func (c ChunkColumn) Validity(n int) []uint64 {
	if c.v.maskPtr == nil || n == 0 {
		return nil
	}
	return unsafe.Slice((*uint64)(c.v.maskPtr), (n+63)/64)
}

// Borrowed returns the string at row of a VARCHAR or BLOB column where DuckDB
// holds it: it is good only as long as the chunk.
func (c ChunkColumn) Borrowed(row int) string {
	return getBorrowedStringAt(c.v.dataPtr, mapping.IdxT(row))
}
