package duckdb

/*
#include <stdint.h>

typedef struct _duckdb_vector *igloo_duckdb_vector;

void duckdb_vector_assign_string_element_len(igloo_duckdb_vector vector, uint64_t index, const char *str, uint64_t str_len);

// igloo_assign_strings assigns n strings, the bytes of data up to each of
// ends in turn, to the rows of vector rows lists.
static void igloo_assign_strings(igloo_duckdb_vector vector, const uint32_t *rows, const char *data, const uint64_t *ends, uint64_t n) {
	uint64_t start = 0;
	for (uint64_t i = 0; i < n; i++) {
		duckdb_vector_assign_string_element_len(vector, rows[i], data + start, ends[i] - start);
		start = ends[i];
	}
}
*/
import "C"

import (
	"errors"
	"unsafe"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// Igloo patch: writing a table function's output chunk a column at a time.
// SetValue and SetChunkValue check the column and convert each value on
// its way into the vector; a table function returning hundreds of
// millions of rows spent most of its time there. A ChunkColumn gives a
// column's memory, for values of fixed size copied in bulk, and writes
// NULLs and strings directly.

// ChunkColumn is a column of a chunk a table function fills.
type ChunkColumn struct {
	v *vector
}

// ChunkColumn returns the chunk's column colIdx, as the table function's
// columns number them; ok is false for a column the statement does not
// read (projection pushdown), which needs no values.
func (chunk *DataChunk) ChunkColumn(colIdx int) (c ChunkColumn, ok bool, err error) {
	i, err := chunk.verifyAndRewriteColIdx(colIdx)
	if errors.Is(err, errUnprojectedColumn) {
		return ChunkColumn{}, false, nil
	}
	if err != nil {
		return ChunkColumn{}, false, getError(errAPI, err)
	}
	return ChunkColumn{v: &chunk.columns[i]}, true, nil
}

// Data returns the column's values' memory: as many values of its type's
// size as a chunk holds, for types of a fixed size (numbers, dates, times,
// timestamps, decimals by their physical type).
func (c ChunkColumn) Data() unsafe.Pointer { return c.v.dataPtr }

// SetNull makes the value at row NULL.
func (c ChunkColumn) SetNull(row int) {
	mask := (*uint64)(unsafe.Add(c.v.maskPtr, (row/64)*8))
	*mask &^= 1 << (row % 64)
}

// stringInline is the longest string DuckDB keeps in the string_t itself.
const stringInline = 12

// SetString writes b at row of a VARCHAR or BLOB column. A string of up to
// twelve bytes goes into the vector itself; DuckDB copies a longer one.
func (c ChunkColumn) SetString(row int, b []byte) {
	if len(b) > stringInline {
		mapping.VectorAssignStringElementLen(c.v.vec, mapping.IdxT(row), b)
		return
	}
	p := unsafe.Add(c.v.dataPtr, uintptr(row)*unsafe.Sizeof(mapping.StringT{}))
	*(*uint32)(p) = uint32(len(b))
	inline := unsafe.Slice((*byte)(unsafe.Add(p, 4)), stringInline)
	n := copy(inline, b)
	clear(inline[n:])
}

// Strings writes strings to a VARCHAR or BLOB column: those of up to
// twelve bytes into the vector itself as they are added, and the longer
// ones, which DuckDB copies, in one call into DuckDB at Flush.
type Strings struct {
	col  ChunkColumn
	rows []uint32
	data []byte
	ends []uint64
}

// Strings returns a writer of the column's strings, reusing w's memory.
func (c ChunkColumn) Strings(w *Strings) *Strings {
	if w == nil {
		w = &Strings{}
	}
	w.col, w.rows, w.data, w.ends = c, w.rows[:0], w.data[:0], w.ends[:0]
	return w
}

// Add writes b at row, or keeps it for Flush.
func (w *Strings) Add(row int, b []byte) {
	if len(b) <= stringInline {
		w.col.SetString(row, b)
		return
	}
	w.rows = append(w.rows, uint32(row))
	w.data = append(w.data, b...)
	w.ends = append(w.ends, uint64(len(w.data)))
}

// Flush writes the strings kept.
func (w *Strings) Flush() {
	if len(w.rows) == 0 {
		return
	}
	C.igloo_assign_strings(C.igloo_duckdb_vector(w.col.v.vec.Ptr), (*C.uint32_t)(unsafe.Pointer(&w.rows[0])),
		(*C.char)(unsafe.Pointer(&w.data[0])), (*C.uint64_t)(unsafe.Pointer(&w.ends[0])), C.uint64_t(len(w.rows)))
	w.rows, w.data, w.ends = w.rows[:0], w.data[:0], w.ends[:0]
}
