package federation

import (
	"encoding/binary"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5/pgtype"
)

// TestNumeric checks NUMBER values against pgx's reading of PostgreSQL's
// binary numeric.
func TestNumeric(t *testing.T) {
	m := pgtype.NewMap()
	check := func(v *big.Int, scale int, enc []byte) {
		t.Helper()
		var n pgtype.Numeric
		if err := m.Scan(pgtype.NumericOID, pgtype.BinaryFormatCode, enc, &n); err != nil {
			t.Fatalf("%s e-%d: %v", v, scale, err)
		}
		got := new(big.Rat).SetInt(n.Int)
		if n.Exp > 0 {
			got.Mul(got, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n.Exp)), nil)))
		} else if n.Exp < 0 {
			got.Quo(got, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-n.Exp)), nil)))
		}
		want := new(big.Rat).SetFrac(v, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil))
		if got.Cmp(want) != 0 {
			t.Errorf("%s e-%d: got %s", v, scale, got.FloatString(scale))
		}
		if dscale := int(binary.BigEndian.Uint16(enc[6:8])); dscale != scale {
			t.Errorf("%s e-%d: display scale %d", v, scale, dscale)
		}
	}
	for _, tc := range []struct {
		v     int64
		scale int
	}{
		{0, 0}, {0, 2}, {1, 0}, {-1, 0}, {1050, 2}, {-1234, 2}, {5, 2}, {9999, 0}, {10000, 0}, {100000000, 4},
		{1, 9}, {-5, 18}, {math.MaxInt64, 0}, {math.MinInt64, 0}, {math.MaxInt64, 18}, {123456789012, 0}, {7, 37},
	} {
		check(big.NewInt(tc.v), tc.scale, appendNumericInt(nil, tc.v, tc.scale))
	}
	huge, _ := new(big.Int).SetString("12345678901234567890123456789123456789", 10)
	for _, scale := range []int{0, 9, 37} {
		check(huge, scale, appendNumericBig(nil, huge, scale))
		check(new(big.Int).Neg(huge), scale, appendNumericBig(nil, new(big.Int).Neg(huge), scale))
	}
}

// TestEncoders encodes Snowflake's Arrow forms of each kind, as its driver
// returns them (see sfClient.query).
func TestEncoders(t *testing.T) {
	pool := memory.NewGoAllocator()
	at := time.Date(2020, 1, 2, 3, 4, 5, 123456789, time.UTC)
	pgMicros := func(t time.Time) uint64 {
		return uint64(t.Unix()*1_000_000 + int64(t.Nanosecond()/1000) - pgEpochSeconds*1_000_000)
	}

	ib := array.NewInt64Builder(pool)
	ib.AppendValues([]int64{at.UnixMilli()}, nil)
	ib.AppendNull()
	ntz3 := ib.NewArray()

	st := arrow.StructOf(arrow.Field{Name: "epoch", Type: arrow.PrimitiveTypes.Int64}, arrow.Field{Name: "fraction", Type: arrow.PrimitiveTypes.Int32})
	sb := array.NewStructBuilder(pool, st)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.Int64Builder).Append(at.Unix())
	sb.FieldBuilder(1).(*array.Int32Builder).Append(int32(at.Nanosecond()))
	sb.AppendNull()
	sb.FieldBuilder(0).(*array.Int64Builder).Append(0)
	sb.FieldBuilder(1).(*array.Int32Builder).Append(0)
	ntz9 := sb.NewArray()

	tzt := arrow.StructOf(arrow.Field{Name: "epoch", Type: arrow.PrimitiveTypes.Int64}, arrow.Field{Name: "timezone", Type: arrow.PrimitiveTypes.Int32})
	tb := array.NewStructBuilder(pool, tzt)
	tb.Append(true)
	tb.FieldBuilder(0).(*array.Int64Builder).Append(at.Unix())
	tb.FieldBuilder(1).(*array.Int32Builder).Append(1440 + 120)
	tb.AppendNull()
	tb.FieldBuilder(0).(*array.Int64Builder).Append(0)
	tb.FieldBuilder(1).(*array.Int32Builder).Append(0)
	tz0 := tb.NewArray()

	// A time before 1970, in tenths of seconds: cut down to the
	// microsecond, not towards zero.
	old := time.Date(1900, 1, 1, 0, 0, 0, 500_000_000, time.UTC)
	ob := array.NewInt64Builder(pool)
	ob.AppendValues([]int64{old.Unix()*10 + 5, -1}, nil)
	ntz1 := ob.NewArray()
	beforeEpoch := int64(-100_000) - pgEpochSeconds*1_000_000

	db := array.NewDecimal128Builder(pool, &arrow.Decimal128Type{Precision: 38, Scale: 9})
	db.Append(decimal128.FromI64(-12345))
	db.AppendNull()
	dec := db.NewArray()

	for _, tc := range []struct {
		name  string
		a     arrow.Array
		typ   sfType
		scale int
		want  [][]byte
	}{
		{"ntz3", ntz3, sfType{kind: sfTimestamp, name: "TIMESTAMP_NTZ"}, 3,
			[][]byte{binary.BigEndian.AppendUint64(nil, pgMicros(at.Truncate(time.Millisecond))), nil}},
		{"ntz9", ntz9, sfType{kind: sfTimestamp, name: "TIMESTAMP_NTZ"}, 9,
			[][]byte{binary.BigEndian.AppendUint64(nil, pgMicros(at)), nil}},
		{"tz0", tz0, sfType{kind: sfTimestampTZ, name: "TIMESTAMP_TZ"}, 0,
			[][]byte{binary.BigEndian.AppendUint64(nil, pgMicros(at.Truncate(time.Second))), nil}},
		{"ntz1", ntz1, sfType{kind: sfTimestamp, name: "TIMESTAMP_NTZ"}, 1,
			[][]byte{binary.BigEndian.AppendUint64(nil, pgMicros(old)),
				binary.BigEndian.AppendUint64(nil, uint64(beforeEpoch))}},
		{"decimal", dec, sfType{kind: sfNumber, name: "FIXED", precision: 38, scale: 9}, 9,
			[][]byte{appendNumericInt(nil, -12345, 9), nil}},
	} {
		enc, err := columnEncoder(tc.a, tc.typ, tc.scale)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		for i, want := range tc.want {
			got, err := enc(nil, i)
			if err != nil {
				t.Fatal(err)
			}
			n := int32(binary.BigEndian.Uint32(got))
			if want == nil {
				if n != -1 {
					t.Errorf("%s row %d: not NULL", tc.name, i)
				}
				continue
			}
			if string(got[4:]) != string(want) || int(n) != len(want) {
				t.Errorf("%s row %d: got %x, want %x", tc.name, i, got[4:], want)
			}
		}
	}
}
