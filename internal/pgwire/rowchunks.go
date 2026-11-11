package pgwire

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"strconv"
	"sync/atomic"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
)

// Rows sent a chunk at a time. A row at a time, DuckDB's driver makes each
// value of a result a Go value (an allocation for most, a call into DuckDB
// for each date, a big.Int for each decimal), the column's encoder makes
// that what pgx encodes, pgx finds how to encode it, and psql-wire copies it
// into a message and the message into a batch: a result of sixteen columns
// reached its client at 700,000 rows a second, while DuckDB produced it many
// times faster. A client's rows (a statement's, those FETCH reads from a
// cursor, a portal's with or without a row limit, and those of COPY ... TO
// STDOUT) are instead read chunk by chunk where DuckDB holds them, and
// written as the client gets them, each value's bytes once.
//
// The bytes are those a row at a time sends. A column of a type with no
// encoding here (nested types, JSON, intervals, times, blobs and the like),
// and a value out of the ordinary (a date before year 1 or after 9999,
// DuckDB's infinities), goes through the same value, encoder and pgx as it
// does a row at a time (see chunkEncoder.appendValue). The rows the result
// cache takes go a row at a time, as the values it keeps (see
// chunkSender.rowAtATime).

// For tests that compare the bytes of rows sent a chunk at a time with those
// of a row at a time: rowChunks is how many chunks of each result go a row
// at a time before the rest go whole, as while the result cache takes a
// result's rows, or -1 for every one, and chunksSent counts the chunks sent
// whole.
var (
	rowChunks  atomic.Int64
	chunksSent atomic.Int64
)

// chunkKind is how a column's values are read from a chunk.
type chunkKind uint8

const (
	// chunkValue values are read as the driver's values, as a row at a time.
	chunkValue chunkKind = iota
	chunkBool
	chunkInt8
	chunkInt16
	chunkInt32
	chunkInt64
	chunkUint8
	chunkUint16
	chunkUint32
	chunkFloat32
	chunkFloat64
	chunkText
	chunkDate
	chunkTimestamp
	chunkTimestampTZ
	chunkDecimal16
	chunkDecimal32
	chunkDecimal64
	chunkDecimal128
	// chunkUint64 and chunkHugeint are integers sent as numerics.
	chunkUint64
	chunkHugeint
	chunkUUID
)

// chunkKindOf is how the values of a chunk's column c, sent as the type oid,
// are read, and their size in the chunk. Only the pairs wireType makes are
// read directly.
func chunkKindOf(c duckdb.ChunkColumn, oid uint32) (kind chunkKind, size int) {
	if c.IsJSON() {
		return chunkValue, 0
	}
	switch t := c.Type(); {
	case t == duckdb.TYPE_BOOLEAN && oid == pgtype.BoolOID:
		return chunkBool, 1
	case t == duckdb.TYPE_TINYINT && oid == pgtype.Int2OID:
		return chunkInt8, 1
	case t == duckdb.TYPE_SMALLINT && oid == pgtype.Int2OID:
		return chunkInt16, 2
	case t == duckdb.TYPE_UTINYINT && oid == pgtype.Int2OID:
		return chunkUint8, 1
	case t == duckdb.TYPE_INTEGER && oid == pgtype.Int4OID:
		return chunkInt32, 4
	case t == duckdb.TYPE_USMALLINT && oid == pgtype.Int4OID:
		return chunkUint16, 2
	case t == duckdb.TYPE_BIGINT && oid == pgtype.Int8OID:
		return chunkInt64, 8
	case t == duckdb.TYPE_UINTEGER && oid == pgtype.Int8OID:
		return chunkUint32, 4
	case t == duckdb.TYPE_FLOAT && oid == pgtype.Float4OID:
		return chunkFloat32, 4
	case t == duckdb.TYPE_DOUBLE && oid == pgtype.Float8OID:
		return chunkFloat64, 8
	case t == duckdb.TYPE_VARCHAR && oid == pgtype.TextOID:
		return chunkText, 0
	case t == duckdb.TYPE_DATE && oid == pgtype.DateOID:
		return chunkDate, 4
	case t == duckdb.TYPE_TIMESTAMP && oid == pgtype.TimestampOID:
		return chunkTimestamp, 8
	case t == duckdb.TYPE_TIMESTAMP_TZ && oid == pgtype.TimestamptzOID:
		return chunkTimestampTZ, 8
	case t == duckdb.TYPE_DECIMAL && oid == pgtype.NumericOID:
		switch internal, _ := c.Decimal(); internal {
		case duckdb.TYPE_SMALLINT:
			return chunkDecimal16, 2
		case duckdb.TYPE_INTEGER:
			return chunkDecimal32, 4
		case duckdb.TYPE_BIGINT:
			return chunkDecimal64, 8
		case duckdb.TYPE_HUGEINT:
			return chunkDecimal128, 16
		}
	case t == duckdb.TYPE_UBIGINT && oid == pgtype.NumericOID:
		return chunkUint64, 8
	case t == duckdb.TYPE_HUGEINT && oid == pgtype.NumericOID:
		return chunkHugeint, 16
	case t == duckdb.TYPE_UUID && oid == pgtype.UUIDOID:
		return chunkUUID, 16
	}
	return chunkValue, 0
}

// chunkColumn is a column of the chunk a chunkEncoder is writing.
type chunkColumn struct {
	kind chunkKind
	// binary says the client asked for the column's values in binary.
	binary bool
	// scale is a decimal's.
	scale int
	col   duckdb.ChunkColumn
	// fixed is the values' memory, for kinds of a fixed size, and valid the
	// column's validity mask, nil if it has no NULL.
	fixed []byte
	valid []uint64
}

// chunkEncoder writes the rows of a statement's result, chunk by chunk, as
// the messages its client reads: DataRow messages, or the CopyData messages
// of COPY ... TO STDOUT.
type chunkEncoder struct {
	st    *statement
	types *pgtype.Map
	// copy is set for the rows of COPY ... TO STDOUT, each a CopyData
	// message. head, if set, goes before the first row, in its message.
	copy *copyTo
	head []byte
	cols []chunkColumn
	// buf holds the messages, and value a value of a copy in text or CSV
	// before it is quoted.
	buf, value []byte
}

// chunkWrite is how many bytes of messages an encoder gathers before it
// sends them: a chunk of ordinary rows goes in one write, and one of long
// values in several, rather than held whole a second time.
const chunkWrite = 1 << 20

// The days, counted from 1970-01-01, of 0001-01-01 and 9999-12-31: the
// dates and timestamps written here. Others (and DuckDB's infinities, which
// are beyond them) are written as a row at a time writes them.
const (
	minChunkDay = -719162
	maxChunkDay = 2932896
	dayMicros   = 86400 * 1000000
	// PostgreSQL's epoch, 2000-01-01, in days and microseconds from 1970's.
	pgEpochDays   = 10957
	pgEpochMicros = pgEpochDays * dayMicros
)

// encode writes chunk's rows from row from up to row to as messages and gives
// them to send, with how many rows they are, once for them all or each time
// they pass chunkWrite. The bytes are the encoder's, good until send returns.
func (e *chunkEncoder) encode(chunk *duckdb.DataChunk, from, to int, send func(rows int, data []byte) error) error {
	for i := range e.cols {
		c := &e.cols[i]
		col, ok, err := chunk.ChunkColumn(i)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("column %q is not in the result's chunk", e.st.columns[i].Name)
		}
		kind, size := chunkKindOf(col, e.st.columns[i].Oid)
		c.kind, c.col, c.fixed, c.valid = kind, col, nil, nil
		if kind == chunkValue {
			continue
		}
		if size > 0 {
			c.fixed = col.Fixed(to, size)
		}
		c.valid = col.Validity(to)
		_, scale := col.Decimal()
		c.scale = int(scale)
	}

	buf := e.buf[:0]
	if cap(buf) > 2*chunkWrite {
		// A row of long values made it grow.
		buf = nil
	}
	lines := e.copy != nil && e.copy.format != "binary"
	rows := 0
	for row := from; row < to; row++ {
		// The message's type and length, which is written at its end.
		start := len(buf)
		if e.copy == nil {
			buf = append(buf, 'D', 0, 0, 0, 0)
		} else {
			buf = append(append(buf, 'd', 0, 0, 0, 0), e.head...)
			e.head = nil
		}
		var err error
		if lines {
			buf, err = e.appendLine(buf, chunk, row)
		} else {
			buf, err = e.appendRow(buf, chunk, row)
		}
		if err != nil {
			return err
		}
		binary.BigEndian.PutUint32(buf[start+1:], uint32(len(buf)-start-1))
		if rows++; len(buf) >= chunkWrite || row == to-1 {
			if err := send(rows, buf); err != nil {
				return err
			}
			buf, rows = buf[:0], 0
		}
	}
	e.buf = buf
	return nil
}

// appendRow appends a row as a DataRow message and a binary copy hold it:
// the count of its values, and each value after its length, -1 for NULL.
func (e *chunkEncoder) appendRow(buf []byte, chunk *duckdb.DataChunk, row int) ([]byte, error) {
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(e.cols)))
	for i := range e.cols {
		at := len(buf)
		buf = append(buf, 0xff, 0xff, 0xff, 0xff)
		var null bool
		var err error
		if buf, null, err = e.appendValue(buf, chunk, i, row); err != nil {
			return nil, err
		}
		if !null {
			binary.BigEndian.PutUint32(buf[at:], uint32(len(buf)-at-4))
		}
	}
	return buf, nil
}

// appendLine appends a row as a line of a copy in text or CSV.
func (e *chunkEncoder) appendLine(buf []byte, chunk *duckdb.DataChunk, row int) ([]byte, error) {
	for i := range e.cols {
		if i > 0 {
			buf = append(buf, e.copy.delimiter...)
		}
		value, null, err := e.appendValue(e.value[:0], chunk, i, row)
		if err != nil {
			return nil, err
		}
		e.value = value
		if null {
			buf = append(buf, e.copy.null...)
			continue
		}
		buf = e.copy.appendText(buf, value)
	}
	return append(buf, '\n'), nil
}

// appendValue appends the value of the chunk's column i at row in the
// column's format, or reports that it is NULL.
func (e *chunkEncoder) appendValue(buf []byte, chunk *duckdb.DataChunk, i, row int) (_ []byte, null bool, err error) {
	c := &e.cols[i]
	if c.kind != chunkValue {
		if c.valid != nil && c.valid[row>>6]&(1<<(row&63)) == 0 {
			return buf, true, nil
		}
		var ok bool
		if c.binary {
			buf, ok = c.appendBinary(buf, row)
		} else {
			buf, ok = c.appendText(buf, row)
		}
		if ok {
			return buf, false, nil
		}
	}
	// As a row at a time: the driver's value, through the column's encoder
	// and pgx.
	v, err := chunk.RowValue(i, row)
	if err != nil {
		return nil, false, err
	}
	if v != nil {
		if v, err = e.st.encoders[i](v); err != nil {
			return nil, false, fmt.Errorf("column %q: %w", e.st.columns[i].Name, err)
		}
	}
	if v == nil {
		return buf, true, nil
	}
	format := int16(pgtype.TextFormatCode)
	if c.binary {
		format = pgtype.BinaryFormatCode
	}
	// pgx returns no bytes for a value that is not valid, which goes out
	// empty, as it does a row at a time.
	encoded, err := e.types.Encode(e.st.columns[i].Oid, format, v, buf)
	if err != nil {
		return nil, false, err
	}
	if encoded != nil {
		buf = encoded
	}
	return buf, false, nil
}

func (c *chunkColumn) int16At(row int) int16 {
	return int16(binary.NativeEndian.Uint16(c.fixed[row*2:]))
}

func (c *chunkColumn) int32At(row int) int32 {
	return int32(binary.NativeEndian.Uint32(c.fixed[row*4:]))
}

func (c *chunkColumn) int64At(row int) int64 {
	return int64(binary.NativeEndian.Uint64(c.fixed[row*8:]))
}

// integer returns the value at row of a column of integers, or of decimals
// without their point.
func (c *chunkColumn) integer(row int) int64 {
	switch c.kind {
	case chunkInt8:
		return int64(int8(c.fixed[row]))
	case chunkUint8:
		return int64(c.fixed[row])
	case chunkInt16, chunkDecimal16:
		return int64(c.int16At(row))
	case chunkUint16:
		return int64(uint16(c.int16At(row)))
	case chunkInt32, chunkDecimal32, chunkDate:
		return int64(c.int32At(row))
	case chunkUint32:
		return int64(uint32(c.int32At(row)))
	}
	return c.int64At(row)
}

// appendText appends the value at row in PostgreSQL's text format. ok is
// false for a value written only as a row at a time writes it.
func (c *chunkColumn) appendText(buf []byte, row int) (_ []byte, ok bool) {
	switch c.kind {
	case chunkBool:
		if c.fixed[row] != 0 {
			return append(buf, 't'), true
		}
		return append(buf, 'f'), true
	case chunkFloat32:
		return strconv.AppendFloat(buf, float64(math.Float32frombits(uint32(c.int32At(row)))), 'f', -1, 32), true
	case chunkFloat64:
		return strconv.AppendFloat(buf, math.Float64frombits(uint64(c.int64At(row))), 'f', -1, 64), true
	case chunkText:
		return append(buf, c.col.Borrowed(row)...), true
	case chunkDate:
		day := c.integer(row)
		if day < minChunkDay || day > maxChunkDay {
			return buf, false
		}
		return appendDay(buf, day), true
	case chunkTimestamp, chunkTimestampTZ:
		day, micros := splitMicros(c.int64At(row))
		if day < minChunkDay || day > maxChunkDay {
			return buf, false
		}
		buf = appendClock(append(appendDay(buf, day), ' '), micros)
		if c.kind == chunkTimestampTZ {
			// As PostgreSQL writes UTC (see timestamptzCodec).
			buf = append(buf, "+00"...)
		}
		return buf, true
	case chunkDecimal16, chunkDecimal32, chunkDecimal64, chunkDecimal128, chunkUint64, chunkHugeint:
		var tmp [40]byte
		digits, negative := c.digits(row, &tmp)
		return appendDecimalText(buf, digits, negative, c.scale), true
	case chunkUUID:
		id := c.uuid(row)
		buf = hex.AppendEncode(buf, id[:4])
		buf = hex.AppendEncode(append(buf, '-'), id[4:6])
		buf = hex.AppendEncode(append(buf, '-'), id[6:8])
		buf = hex.AppendEncode(append(buf, '-'), id[8:10])
		return hex.AppendEncode(append(buf, '-'), id[10:]), true
	}
	return strconv.AppendInt(buf, c.integer(row), 10), true
}

// appendBinary is appendText in PostgreSQL's binary format.
func (c *chunkColumn) appendBinary(buf []byte, row int) (_ []byte, ok bool) {
	switch c.kind {
	case chunkBool:
		if c.fixed[row] != 0 {
			return append(buf, 1), true
		}
		return append(buf, 0), true
	case chunkInt8, chunkUint8, chunkInt16:
		return binary.BigEndian.AppendUint16(buf, uint16(c.integer(row))), true
	case chunkInt32, chunkUint16:
		return binary.BigEndian.AppendUint32(buf, uint32(c.integer(row))), true
	case chunkInt64, chunkUint32:
		return binary.BigEndian.AppendUint64(buf, uint64(c.integer(row))), true
	case chunkFloat32:
		return binary.BigEndian.AppendUint32(buf, uint32(c.int32At(row))), true
	case chunkFloat64:
		return binary.BigEndian.AppendUint64(buf, uint64(c.int64At(row))), true
	case chunkText:
		return append(buf, c.col.Borrowed(row)...), true
	case chunkDate:
		day := c.integer(row)
		if day < minChunkDay || day > maxChunkDay {
			return buf, false
		}
		return binary.BigEndian.AppendUint32(buf, uint32(int32(day-pgEpochDays))), true
	case chunkTimestamp, chunkTimestampTZ:
		micros := c.int64At(row)
		if day, _ := splitMicros(micros); day < minChunkDay || day > maxChunkDay {
			return buf, false
		}
		return binary.BigEndian.AppendUint64(buf, uint64(micros-pgEpochMicros)), true
	case chunkUUID:
		id := c.uuid(row)
		return append(buf, id[:]...), true
	}
	var tmp [40]byte
	digits, negative := c.digits(row, &tmp)
	return appendDecimalBinary(buf, digits, negative, c.scale), true
}

// uuid returns the UUID at row, which DuckDB holds as a HUGEINT with its
// sign bit flipped.
func (c *chunkColumn) uuid(row int) (id [16]byte) {
	binary.BigEndian.PutUint64(id[:8], binary.NativeEndian.Uint64(c.fixed[row*16+8:])^1<<63)
	binary.BigEndian.PutUint64(id[8:], binary.NativeEndian.Uint64(c.fixed[row*16:]))
	return id
}

// digits returns the decimal digits of the value at row of a column sent
// as a numeric, a decimal without its point or an integer, in tmp, and
// whether it is negative.
func (c *chunkColumn) digits(row int, tmp *[40]byte) (digits []byte, negative bool) {
	switch c.kind {
	case chunkUint64:
		return strconv.AppendUint(tmp[:0], uint64(c.int64At(row)), 10), false
	case chunkDecimal128, chunkHugeint:
		// A HUGEINT: its low 64 bits, then its high ones, signed.
		low, high := binary.NativeEndian.Uint64(c.fixed[row*16:]), binary.NativeEndian.Uint64(c.fixed[row*16+8:])
		if negative = int64(high) < 0; negative {
			if low, high = -low, ^high; low == 0 {
				high++
			}
		}
		if high == 0 {
			return strconv.AppendUint(tmp[:0], low, 10), negative
		}
		// Up to 39 digits: those above the low nineteen, which fit 64 bits,
		// then the low nineteen.
		const e19 = 10000000000000000000
		top, rest := bits.Div64(high%e19, low, e19)
		digits = strconv.AppendUint(tmp[:0], top, 10)
		var low19 [20]byte
		tail := strconv.AppendUint(low19[:0], rest, 10)
		for range 19 - len(tail) {
			digits = append(digits, '0')
		}
		return append(digits, tail...), negative
	}
	v := c.integer(row)
	u := uint64(v)
	if negative = v < 0; negative {
		u = -u
	}
	return strconv.AppendUint(tmp[:0], u, 10), negative
}

// splitMicros splits microseconds from 1970-01-01 into the day and the
// microseconds of that day.
func splitMicros(micros int64) (day, ofDay int64) {
	day, ofDay = micros/dayMicros, micros%dayMicros
	if ofDay < 0 {
		day, ofDay = day-1, ofDay+dayMicros
	}
	return day, ofDay
}

// appendDay appends the date of a day from minChunkDay to maxChunkDay,
// counted from 1970-01-01, as 2006-01-02.
func appendDay(buf []byte, day int64) []byte {
	// The civil date of a day number, in eras of 400 years that begin on
	// March 1.
	z := day + 719468
	era := z / 146097
	dayOfEra := z - era*146097
	yearOfEra := (dayOfEra - dayOfEra/1460 + dayOfEra/36524 - dayOfEra/146096) / 365
	dayOfYear := dayOfEra - (365*yearOfEra + yearOfEra/4 - yearOfEra/100)
	m := (5*dayOfYear + 2) / 153
	d := dayOfYear - (153*m+2)/5 + 1
	y := yearOfEra + era*400
	if m += 3; m > 12 {
		m -= 12
	}
	if m <= 2 {
		y++
	}
	return append(buf, byte('0'+y/1000), byte('0'+y/100%10), byte('0'+y/10%10), byte('0'+y%10), '-',
		byte('0'+m/10), byte('0'+m%10), '-', byte('0'+d/10), byte('0'+d%10))
}

// appendClock appends the time of day at micros microseconds as 15:04:05,
// and its fraction, if it has one, without the zeros at its end.
func appendClock(buf []byte, micros int64) []byte {
	seconds, fraction := micros/1000000, micros%1000000
	h, m, s := seconds/3600, seconds/60%60, seconds%60
	buf = append(buf, byte('0'+h/10), byte('0'+h%10), ':', byte('0'+m/10), byte('0'+m%10), ':', byte('0'+s/10), byte('0'+s%10))
	if fraction == 0 {
		return buf
	}
	var digits [6]byte
	for i := 5; i >= 0; i-- {
		digits[i] = byte('0' + fraction%10)
		fraction /= 10
	}
	end := len(digits)
	for digits[end-1] == '0' {
		end--
	}
	return append(append(buf, '.'), digits[:end]...)
}

// appendDecimalText appends a numeric, the digits of it without its point,
// its sign and its scale, as PostgreSQL writes one.
func appendDecimalText(buf, digits []byte, negative bool, scale int) []byte {
	if negative {
		buf = append(buf, '-')
	}
	if scale == 0 {
		return append(buf, digits...)
	}
	if len(digits) <= scale {
		buf = append(buf, '0', '.')
		for range scale - len(digits) {
			buf = append(buf, '0')
		}
		return append(buf, digits...)
	}
	point := len(digits) - scale
	return append(append(append(buf, digits[:point]...), '.'), digits[point:]...)
}

// appendDecimalBinary appends a numeric, the digits of it without its point,
// its sign and its scale, in PostgreSQL's binary format as pgx writes it:
// the count of digits in base 10,000, the weight of the first, the sign, the
// scale, and the digits, those after the point for all of the scale, zeros
// included.
func appendDecimalBinary(buf, digits []byte, negative bool, scale int) []byte {
	whole := digits[:max(0, len(digits)-scale)]
	if len(whole) == 1 && whole[0] == '0' {
		whole = nil
	}
	wholeCount, fractionCount := (len(whole)+3)/4, (scale+3)/4
	var sign uint16
	if negative {
		sign = 0x4000
	}
	buf = binary.BigEndian.AppendUint16(buf, uint16(wholeCount+fractionCount))
	buf = binary.BigEndian.AppendUint16(buf, uint16(int16(wholeCount-1)))
	buf = binary.BigEndian.AppendUint16(buf, sign)
	buf = binary.BigEndian.AppendUint16(buf, uint16(scale))
	// The first digit before the point holds what the others, of four
	// decimal digits each, leave.
	at := 0
	for i := range wholeCount {
		n := 4
		if i == 0 {
			n = len(whole) - 4*(wholeCount-1)
		}
		var d uint16
		for range n {
			d = d*10 + uint16(whole[at]-'0')
			at++
		}
		buf = binary.BigEndian.AppendUint16(buf, d)
	}
	// After the point, the scale's decimal digits, which the value's last
	// ones are, with zeros before them and after to make fours.
	first := len(digits) - scale
	for i := range fractionCount {
		var d uint16
		for k := 4 * i; k < 4*i+4; k++ {
			d *= 10
			if k < scale && first+k >= 0 {
				d += uint16(digits[first+k] - '0')
			}
		}
		buf = binary.BigEndian.AppendUint16(buf, d)
	}
	return buf
}

// chunkSender sends a statement's rows to its client a chunk at a time.
type chunkSender struct {
	out wire.EncodedRowWriter
	// tags are Igloo's writers on the way to the client's.
	tags []*tagWriter
	enc  chunkEncoder
	// asRows is how many chunks still go a row at a time (see rowChunks).
	asRows int64
}

// chunkSender returns what sends st's rows to w a chunk at a time, or nil
// where they go a row at a time: to a writer that is not a client's, or
// through a wrapper of Igloo's that takes rows.
func (st *statement) chunkSender(ctx context.Context, w wire.DataWriter) *chunkSender {
	asRows := rowChunks.Load()
	if asRows < 0 {
		return nil
	}
	var tags []*tagWriter
	for {
		switch x := w.(type) {
		case *tagWriter:
			tags, w = append(tags, x), x.DataWriter
			continue
		case afterWriter:
			w = x.DataWriter
			continue
		case wire.EncodedRowWriter:
			types := wire.TypeMap(ctx)
			if types == nil {
				return nil
			}
			s := &chunkSender{out: x, tags: tags, asRows: asRows,
				enc: chunkEncoder{st: st, types: types, cols: make([]chunkColumn, len(st.columns))}}
			formats := x.RowFormats()
			for i := range s.enc.cols {
				// As psql-wire reads them: none is text, one is every column's.
				format := wire.TextFormat
				if len(formats) > i {
					format = formats[i]
				} else if len(formats) > 0 {
					format = formats[0]
				}
				s.enc.cols[i].binary = format == wire.BinaryFormat
			}
			return s
		}
		return nil
	}
}

// rowAtATime reports whether the rows NextChunk gave next go a row at a
// time: while the result cache still takes the statement's rows, which it
// keeps as the values a row at a time sends (see resultFill).
func (s *chunkSender) rowAtATime() bool {
	if s.asRows > 0 {
		s.asRows--
		return true
	}
	for _, t := range s.tags {
		if t.fill != nil && !t.fill.over {
			return true
		}
	}
	return false
}

// send sends chunk's rows from row from up to row to, or as many of them as
// the client takes before it reads on (a portal's row limit), and returns
// how many it sent.
func (s *chunkSender) send(chunk *duckdb.DataChunk, from, to int) (int, error) {
	if limit := s.out.RowLimit(); limit > 0 {
		to = min(to, from+limit)
	}
	sent := 0
	err := s.enc.encode(chunk, from, to, func(rows int, data []byte) error {
		for _, t := range s.tags {
			if err := t.gate.begin(); err != nil {
				return err
			}
		}
		if err := s.out.EncodedRows(rows, data); err != nil {
			return err
		}
		sent += rows
		return nil
	})
	chunksSent.Add(1)
	return sent, err
}

// sendChunks sends rows' next rows to their client, limit of them or, with
// limit negative, all there are: through sender a chunk at a time, and
// through b, which the caller flushes, those that go a row at a time. It
// returns how many it sent and whether the rows ended.
func (st *statement) sendChunks(rows duckdb.ChunkRows, sender *chunkSender, b *rowBatcher, limit int) (n int, ended bool, err error) {
	dest := make([]driver.Value, len(st.columns))
	for limit < 0 || n < limit {
		chunk, from, err := rows.NextChunk()
		if errors.Is(err, io.EOF) {
			return n, true, nil
		}
		if err != nil {
			return n, false, err
		}
		to := chunk.GetSize()
		if limit >= 0 {
			to = min(to, from+limit-n)
		}
		if sender.rowAtATime() || chunk.ColumnCount() != len(st.columns) {
			// The result cache takes the rows as values: a row at a time,
			// until the result is past what it keeps.
			for range to - from {
				if err := rows.Next(dest); err != nil {
					return n, false, err
				}
				if err := b.add(dest, st.encoders); err != nil {
					return n, false, err
				}
				n++
			}
			continue
		}
		if err := b.flush(); err != nil {
			return n, false, err
		}
		sent, err := sender.send(chunk, from, to)
		rows.Skip(sent)
		if n += sent; err != nil {
			return n, false, err
		}
	}
	return n, false, nil
}
