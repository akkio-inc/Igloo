package federation

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

// Snowflake's Arrow results in PostgreSQL's binary COPY format, which
// DuckDB's postgres extension reads. Snowflake's driver hands the gateway
// Snowflake's own Arrow (NUMBER as scaled integers, timestamps as scaled
// integers or structs of seconds and nanoseconds; see sfClient.query);
// each column is encoded as the PostgreSQL type the gateway reported for
// it (sfType.pgType).

// copyHeader starts binary COPY data: the signature, no flags and no header
// extension.
var copyHeader = []byte("PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00")

// copyTrailer ends it.
var copyTrailer = []byte{0xff, 0xff}

const (
	// pgEpochSeconds is PostgreSQL's epoch, 2000-01-01, in Unix seconds;
	// pgEpochDays in days.
	pgEpochSeconds = 946684800
	pgEpochDays    = 10957
)

// fieldEncoder appends the value of row i of a column to dst as a binary
// COPY field: its length, or -1 for NULL, then its bytes.
type fieldEncoder func(dst []byte, i int) ([]byte, error)

// encoders returns an encoder for each column of rec, which the gateway
// selected for cols.
func encoders(rec arrow.Record, cols []*sfColumn) ([]fieldEncoder, error) {
	if int(rec.NumCols()) != len(cols) {
		return nil, fmt.Errorf("Snowflake returned %d columns, not %d", rec.NumCols(), len(cols))
	}
	out := make([]fieldEncoder, len(cols))
	for i, c := range cols {
		enc, err := columnEncoder(rec.Column(i), c.typ, fieldScale(rec.Schema().Field(i), c.typ.scale))
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", c.pgName, err)
		}
		out[i] = enc
	}
	return out, nil
}

// fieldScale is the scale Snowflake gives a result column in its Arrow
// metadata, or def.
func fieldScale(f arrow.Field, def int) int {
	if i := f.Metadata.FindKey("scale"); i >= 0 {
		if s, err := strconv.Atoi(f.Metadata.Values()[i]); err == nil {
			return s
		}
	}
	return def
}

// withLength wraps an encoder of a field's bytes with its length and
// NULLs.
func withLength(a arrow.Array, f func(dst []byte, i int) ([]byte, error)) fieldEncoder {
	return func(dst []byte, i int) ([]byte, error) {
		if a.IsNull(i) {
			return binary.BigEndian.AppendUint32(dst, math.MaxUint32), nil
		}
		at := len(dst)
		dst = append(dst, 0, 0, 0, 0)
		dst, err := f(dst, i)
		binary.BigEndian.PutUint32(dst[at:], uint32(len(dst)-at-4))
		return dst, err
	}
}

func columnEncoder(a arrow.Array, t sfType, scale int) (fieldEncoder, error) {
	unexpected := func() (fieldEncoder, error) {
		return nil, fmt.Errorf("unexpected Arrow type %s for Snowflake %s", a.DataType(), t.name)
	}
	switch t.kind {
	case sfNumber:
		ints, ok := intValues(a)
		if ok {
			return withLength(a, func(dst []byte, i int) ([]byte, error) {
				return appendNumericInt(dst, ints(i), scale), nil
			}), nil
		}
		if d, ok := a.(*array.Decimal128); ok {
			return withLength(a, func(dst []byte, i int) ([]byte, error) {
				return appendNumericBig(dst, decimal128BigInt(d.Value(i)), scale), nil
			}), nil
		}
		return unexpected()
	case sfFloat:
		switch f := a.(type) {
		case *array.Float64:
			return withLength(a, func(dst []byte, i int) ([]byte, error) {
				return binary.BigEndian.AppendUint64(dst, math.Float64bits(f.Value(i))), nil
			}), nil
		case *array.Float32:
			return withLength(a, func(dst []byte, i int) ([]byte, error) {
				return binary.BigEndian.AppendUint64(dst, math.Float64bits(float64(f.Value(i)))), nil
			}), nil
		}
		return unexpected()
	case sfBoolean:
		b, ok := a.(*array.Boolean)
		if !ok {
			return unexpected()
		}
		return withLength(a, func(dst []byte, i int) ([]byte, error) {
			if b.Value(i) {
				return append(dst, 1), nil
			}
			return append(dst, 0), nil
		}), nil
	case sfArray:
		switch s := a.(type) {
		case *array.String:
			return withLength(a, func(dst []byte, i int) ([]byte, error) { return appendTextArray(dst, s.Value(i)) }), nil
		case *array.LargeString:
			return withLength(a, func(dst []byte, i int) ([]byte, error) { return appendTextArray(dst, s.Value(i)) }), nil
		}
		return unexpected()
	case sfText, sfJSON, sfAsText:
		switch s := a.(type) {
		case *array.String:
			return withLength(a, func(dst []byte, i int) ([]byte, error) { return append(dst, s.Value(i)...), nil }), nil
		case *array.LargeString:
			return withLength(a, func(dst []byte, i int) ([]byte, error) { return append(dst, s.Value(i)...), nil }), nil
		}
		return unexpected()
	case sfBinary:
		switch b := a.(type) {
		case *array.Binary:
			return withLength(a, func(dst []byte, i int) ([]byte, error) { return append(dst, b.Value(i)...), nil }), nil
		case *array.LargeBinary:
			return withLength(a, func(dst []byte, i int) ([]byte, error) { return append(dst, b.Value(i)...), nil }), nil
		case *array.FixedSizeBinary:
			return withLength(a, func(dst []byte, i int) ([]byte, error) { return append(dst, b.Value(i)...), nil }), nil
		}
		return unexpected()
	case sfDate:
		switch d := a.(type) {
		case *array.Date32:
			return withLength(a, func(dst []byte, i int) ([]byte, error) {
				return binary.BigEndian.AppendUint32(dst, uint32(int32(d.Value(i))-pgEpochDays)), nil
			}), nil
		case *array.Int32:
			return withLength(a, func(dst []byte, i int) ([]byte, error) {
				return binary.BigEndian.AppendUint32(dst, uint32(d.Value(i)-pgEpochDays)), nil
			}), nil
		}
		return unexpected()
	case sfTime:
		tm, ok := a.(*array.Time64)
		if !ok {
			return unexpected()
		}
		div := int64(1)
		if tm.DataType().(*arrow.Time64Type).Unit == arrow.Nanosecond {
			div = 1000
		}
		return withLength(a, func(dst []byte, i int) ([]byte, error) {
			return binary.BigEndian.AppendUint64(dst, uint64(int64(tm.Value(i))/div)), nil
		}), nil
	case sfTimestamp, sfTimestampLTZ, sfTimestampTZ:
		micros, err := timestampMicros(a, scale)
		if err != nil {
			return nil, fmt.Errorf("Snowflake %s: %w", t.name, err)
		}
		return withLength(a, func(dst []byte, i int) ([]byte, error) {
			return binary.BigEndian.AppendUint64(dst, uint64(micros(i)-pgEpochSeconds*1_000_000)), nil
		}), nil
	}
	return unexpected()
}

// textOID is PostgreSQL's text, the elements' type of an ARRAY's text[].
const textOID = 25

// appendTextArray appends doc, an ARRAY as Snowflake's TO_JSON writes it,
// as PostgreSQL's binary text[], which DuckDB reads as a VARCHAR[]: each
// element as Snowflake's TO_VARCHAR writes it, a string as itself and
// anything else as its JSON text, as Igloo stores Snowflake's ARRAY (see
// package snowflake), and JSON null (or undefined) as NULL.
func appendTextArray(dst []byte, doc string) ([]byte, error) {
	var elems []json.RawMessage
	if err := json.Unmarshal([]byte(doc), &elems); err != nil {
		return dst, fmt.Errorf("unexpected ARRAY value: %w", err)
	}
	hasNull := uint32(0)
	for _, e := range elems {
		if string(e) == "null" {
			hasNull = 1
		}
	}
	if len(elems) == 0 {
		// No dimensions.
		dst = binary.BigEndian.AppendUint32(dst, 0)
		dst = binary.BigEndian.AppendUint32(dst, 0)
		return binary.BigEndian.AppendUint32(dst, textOID), nil
	}
	dst = binary.BigEndian.AppendUint32(dst, 1)
	dst = binary.BigEndian.AppendUint32(dst, hasNull)
	dst = binary.BigEndian.AppendUint32(dst, textOID)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(elems)))
	dst = binary.BigEndian.AppendUint32(dst, 1) // the lower bound
	for _, e := range elems {
		switch {
		case string(e) == "null":
			dst = binary.BigEndian.AppendUint32(dst, math.MaxUint32)
		case e[0] == '"':
			var s string
			if err := json.Unmarshal(e, &s); err != nil {
				return dst, fmt.Errorf("unexpected ARRAY value: %w", err)
			}
			dst = binary.BigEndian.AppendUint32(dst, uint32(len(s)))
			dst = append(dst, s...)
		default:
			at := len(dst)
			dst = append(dst, 0, 0, 0, 0)
			buf := bytes.NewBuffer(dst)
			if err := json.Compact(buf, e); err != nil {
				return dst[:at], fmt.Errorf("unexpected ARRAY value: %w", err)
			}
			dst = buf.Bytes()
			binary.BigEndian.PutUint32(dst[at:], uint32(len(dst)-at-4))
		}
	}
	return dst, nil
}

// intValues returns the values of an integer array as int64s.
func intValues(a arrow.Array) (func(int) int64, bool) {
	switch v := a.(type) {
	case *array.Int8:
		return func(i int) int64 { return int64(v.Value(i)) }, true
	case *array.Int16:
		return func(i int) int64 { return int64(v.Value(i)) }, true
	case *array.Int32:
		return func(i int) int64 { return int64(v.Value(i)) }, true
	case *array.Int64:
		return func(i int) int64 { return v.Value(i) }, true
	}
	return nil, false
}

func decimal128BigInt(n decimal128.Num) *big.Int { return n.BigInt() }

// timestampMicros returns the microseconds since the Unix epoch of each
// value of a Snowflake timestamp column with scale fractional digits:
// integers of 10^-scale seconds, or structs of seconds (scaled, for
// TIMESTAMP_TZ with two fields), nanoseconds and, for TIMESTAMP_TZ, an
// offset that does not change the instant. Finer values are cut to the
// microsecond, down, as filters assume (see truncatedComparison).
func timestampMicros(a arrow.Array, scale int) (func(int) int64, error) {
	if scale < 0 || scale > 9 {
		return nil, fmt.Errorf("unexpected scale %d", scale)
	}
	scaled := func(v int64) int64 {
		if scale <= 6 {
			return v * pow10[6-scale]
		}
		return floorDiv(v, pow10[scale-6])
	}
	if ints, ok := intValues(a); ok {
		return func(i int) int64 { return scaled(ints(i)) }, nil
	}
	s, ok := a.(*array.Struct)
	if !ok || s.NumField() < 2 {
		return nil, fmt.Errorf("unexpected Arrow type %s", a.DataType())
	}
	epoch, ok := s.Field(0).(*array.Int64)
	if !ok {
		return nil, fmt.Errorf("unexpected Arrow type %s", a.DataType())
	}
	if s.NumField() == 2 {
		if _, ok := s.Field(1).(*array.Int32); !ok {
			return nil, fmt.Errorf("unexpected Arrow type %s", a.DataType())
		}
		if f := s.DataType().(*arrow.StructType).Field(1); f.Name == "timezone" {
			// TIMESTAMP_TZ: the scaled instant and its offset.
			return func(i int) int64 { return scaled(epoch.Value(i)) }, nil
		}
	}
	frac, ok := s.Field(1).(*array.Int32)
	if !ok {
		return nil, fmt.Errorf("unexpected Arrow type %s", a.DataType())
	}
	return func(i int) int64 { return epoch.Value(i)*1_000_000 + int64(frac.Value(i))/1000 }, nil
}

var pow10 = [...]int64{1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

// appendNumericInt appends v·10^-scale as PostgreSQL's binary numeric:
// the number of base-10000 digits, the weight of the first, the sign and
// the display scale, then the digits.
func appendNumericInt(dst []byte, v int64, scale int) []byte {
	var buf [24]byte
	neg := v < 0
	u := uint64(v)
	if neg {
		u = uint64(-v) // also right for math.MinInt64
	}
	return appendNumericDigits(dst, strconv.AppendUint(buf[:0], u, 10), neg, scale)
}

func appendNumericBig(dst []byte, v *big.Int, scale int) []byte {
	neg := v.Sign() < 0
	return appendNumericDigits(dst, []byte(new(big.Int).Abs(v).String()), neg, scale)
}

// appendNumericDigits encodes the decimal digits of an unscaled absolute
// value, without allocating: each digit is added to the base-10000 digit
// that holds its power of ten, counted from the decimal point.
func appendNumericDigits(dst, digits []byte, neg bool, scale int) []byte {
	var stack [16]uint16
	intDigits := int64(len(digits) - scale)
	first := floorDiv(intDigits-1, 4) // weight of the first group
	last := floorDiv(int64(-scale), 4)
	if first < last {
		first = last
	}
	groups := stack[:]
	if int(first-last) >= len(stack) {
		groups = make([]uint16, first-last+1)
	}
	for k, c := range digits {
		e := intDigits - 1 - int64(k) // the digit's power of ten
		w := floorDiv(e, 4)
		groups[first-w] += uint16(c-'0') * uint16(pow10[e-4*w])
	}
	from, to := 0, int(first-last)
	for from <= to && groups[from] == 0 {
		from++
	}
	for to >= from && groups[to] == 0 {
		to--
	}
	n, weight, sign := to-from+1, first-int64(from), uint16(0)
	if n <= 0 {
		n, weight = 0, 0
	} else if neg {
		sign = 0x4000
	}
	dst = binary.BigEndian.AppendUint16(dst, uint16(n))
	dst = binary.BigEndian.AppendUint16(dst, uint16(int16(weight)))
	dst = binary.BigEndian.AppendUint16(dst, sign)
	dst = binary.BigEndian.AppendUint16(dst, uint16(scale))
	for _, g := range groups[from : from+n] {
		dst = binary.BigEndian.AppendUint16(dst, g)
	}
	return dst
}
