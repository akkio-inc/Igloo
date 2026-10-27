package models

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/akkio-inc/igloo/internal/ml"
)

// MaxValues bounds the values a model trains on (rows times columns): a
// few gigabytes in memory while it trains. A larger input fails, and
// should be sampled.
const MaxValues = 200_000_000

// ColType is how a column of training data is read.
type ColType int

const (
	TypeOther ColType = iota
	TypeNumber
	TypeBool
	TypeString
	TypeTime
)

func (t ColType) String() string {
	switch t {
	case TypeNumber:
		return "number"
	case TypeBool:
		return "boolean"
	case TypeString:
		return "string"
	case TypeTime:
		return "timestamp"
	}
	return "other"
}

// Table is a query's rows, read into memory a column at a time.
type Table struct {
	N    int
	Cols []*TableCol
}

// TableCol is one column: numbers in Num (NaN for NULL), strings and
// booleans ("true", "false") in Str, timestamps in Micros (microseconds
// since 1970, the wall clock in UTC), with Null marking NULLs.
type TableCol struct {
	Name     string
	Type     ColType
	TypeName string
	// SQLType is the column's type in DuckDB's SQL, where known.
	SQLType string
	Num     []float64
	Str     []string
	Micros  []int64
	Null    []bool
}

// Col returns the column called name, case-insensitively, or nil.
func (t *Table) Col(name string) *TableCol {
	for _, c := range t.Cols {
		if strings.EqualFold(c.Name, name) {
			return c
		}
	}
	return nil
}

// ReadArrow reads every record of rdr.
func ReadArrow(rdr array.RecordReader) (*Table, error) {
	schema := rdr.Schema()
	t := &Table{}
	for _, f := range schema.Fields() {
		t.Cols = append(t.Cols, &TableCol{Name: f.Name, Type: typeOf(f.Type), TypeName: f.Type.String(), SQLType: sqlType(f.Type)})
	}
	for rdr.Next() {
		rec := rdr.RecordBatch()
		n := int(rec.NumRows())
		if int64(t.N+n)*int64(max(len(t.Cols), 1)) > MaxValues {
			return nil, fmt.Errorf("the input has more than %d values (rows times columns); train on a sample of it", MaxValues)
		}
		for j, c := range t.Cols {
			if err := c.appendArrow(rec.Column(j), n); err != nil {
				return nil, fmt.Errorf("column %s: %w", c.Name, err)
			}
		}
		t.N += n
	}
	if err := rdr.Err(); err != nil {
		return nil, err
	}
	return t, nil
}

func typeOf(dt arrow.DataType) ColType {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64, arrow.DECIMAL128, arrow.DECIMAL256:
		return TypeNumber
	case arrow.BOOL:
		return TypeBool
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return TypeString
	case arrow.TIMESTAMP, arrow.DATE32, arrow.DATE64:
		return TypeTime
	case arrow.DICTIONARY:
		return typeOf(dt.(*arrow.DictionaryType).ValueType)
	case arrow.EXTENSION:
		return typeOf(dt.(arrow.ExtensionType).StorageType())
	}
	return TypeOther
}

// sqlType names an Arrow type as DuckDB's SQL does, or "" for one it
// cannot.
func sqlType(dt arrow.DataType) string {
	switch t := dt.(type) {
	case *arrow.Decimal128Type:
		return fmt.Sprintf("DECIMAL(%d, %d)", t.Precision, t.Scale)
	case *arrow.TimestampType:
		if t.TimeZone != "" {
			return "TIMESTAMPTZ"
		}
		return "TIMESTAMP"
	case *arrow.DictionaryType:
		return sqlType(t.ValueType)
	case arrow.ExtensionType:
		return sqlType(t.StorageType())
	}
	switch dt.ID() {
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.INT8:
		return "TINYINT"
	case arrow.INT16:
		return "SMALLINT"
	case arrow.INT32:
		return "INTEGER"
	case arrow.INT64:
		return "BIGINT"
	case arrow.UINT8:
		return "UTINYINT"
	case arrow.UINT16:
		return "USMALLINT"
	case arrow.UINT32:
		return "UINTEGER"
	case arrow.UINT64:
		return "UBIGINT"
	case arrow.FLOAT32:
		return "FLOAT"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return "VARCHAR"
	case arrow.DATE32, arrow.DATE64:
		return "DATE"
	}
	return ""
}

func (c *TableCol) appendArrow(a arrow.Array, n int) error {
	if ext, ok := a.(array.ExtensionArray); ok {
		a = ext.Storage()
	}
	if d, ok := a.(*array.Dictionary); ok {
		dict := d.Dictionary()
		for i := range n {
			if d.IsNull(i) {
				c.appendNull()
				continue
			}
			if err := c.appendOne(dict, d.GetValueIndex(i)); err != nil {
				return err
			}
		}
		return nil
	}
	for i := range n {
		if a.IsNull(i) {
			c.appendNull()
			continue
		}
		if err := c.appendOne(a, i); err != nil {
			return err
		}
	}
	return nil
}

func (c *TableCol) appendNull() {
	c.Null = append(c.Null, true)
	switch c.Type {
	case TypeNumber:
		c.Num = append(c.Num, math.NaN())
	case TypeBool, TypeString, TypeOther:
		c.Str = append(c.Str, "")
	case TypeTime:
		c.Micros = append(c.Micros, 0)
	}
}

func (c *TableCol) appendOne(a arrow.Array, i int) error {
	c.Null = append(c.Null, false)
	switch v := a.(type) {
	case *array.Int8:
		c.Num = append(c.Num, float64(v.Value(i)))
	case *array.Int16:
		c.Num = append(c.Num, float64(v.Value(i)))
	case *array.Int32:
		c.Num = append(c.Num, float64(v.Value(i)))
	case *array.Int64:
		c.Num = append(c.Num, float64(v.Value(i)))
	case *array.Uint8:
		c.Num = append(c.Num, float64(v.Value(i)))
	case *array.Uint16:
		c.Num = append(c.Num, float64(v.Value(i)))
	case *array.Uint32:
		c.Num = append(c.Num, float64(v.Value(i)))
	case *array.Uint64:
		c.Num = append(c.Num, float64(v.Value(i)))
	case *array.Float16:
		c.Num = append(c.Num, float64(v.Value(i).Float32()))
	case *array.Float32:
		c.Num = append(c.Num, float64(v.Value(i)))
	case *array.Float64:
		c.Num = append(c.Num, v.Value(i))
	case *array.Decimal128:
		c.Num = append(c.Num, v.Value(i).ToFloat64(v.DataType().(*arrow.Decimal128Type).Scale))
	case *array.Decimal256:
		c.Num = append(c.Num, v.Value(i).ToFloat64(v.DataType().(*arrow.Decimal256Type).Scale))
	case *array.Boolean:
		if v.Value(i) {
			c.Str = append(c.Str, "true")
		} else {
			c.Str = append(c.Str, "false")
		}
	// Arrow's strings point into the record's buffers, which DuckDB frees
	// with the record: keep copies.
	case *array.String:
		c.Str = append(c.Str, strings.Clone(v.Value(i)))
	case *array.LargeString:
		c.Str = append(c.Str, strings.Clone(v.Value(i)))
	case *array.StringView:
		c.Str = append(c.Str, strings.Clone(v.Value(i)))
	case *array.Timestamp:
		unit := v.DataType().(*arrow.TimestampType).Unit
		c.Micros = append(c.Micros, toMicros(int64(v.Value(i)), unit))
	case *array.Date32:
		c.Micros = append(c.Micros, int64(v.Value(i))*86_400_000_000)
	case *array.Date64:
		c.Micros = append(c.Micros, int64(v.Value(i))*1000)
	default:
		if c.Type != TypeOther {
			return fmt.Errorf("unexpected values of type %s", a.DataType())
		}
		// Kept as text, as a series' value may be.
		c.Str = append(c.Str, a.ValueStr(i))
	}
	return nil
}

func toMicros(v int64, unit arrow.TimeUnit) int64 {
	switch unit {
	case arrow.Second:
		return v * 1_000_000
	case arrow.Millisecond:
		return v * 1000
	case arrow.Nanosecond:
		return v / 1000
	}
	return v
}

// Time returns row i's timestamp.
func (c *TableCol) Time(i int) time.Time { return time.UnixMicro(c.Micros[i]).UTC() }

// Feature returns the column as a model's feature: numbers and times
// (in seconds) as numbers, strings and booleans as categories.
func (c *TableCol) Feature() (ml.Column, error) {
	switch c.Type {
	case TypeNumber:
		return ml.NumericColumn(c.Name, c.Num), nil
	case TypeTime:
		v := make([]float64, len(c.Micros))
		for i, m := range c.Micros {
			if c.Null[i] {
				v[i] = math.NaN()
			} else {
				v[i] = float64(m) / 1e6
			}
		}
		return ml.NumericColumn(c.Name, v), nil
	case TypeBool, TypeString:
		return ml.CategoricalColumn(c.Name, c.Str, c.Null), nil
	}
	return ml.Column{}, fmt.Errorf("column %s is of type %s; features are numbers, strings, booleans or timestamps (cast it)", c.Name, c.TypeName)
}

// Take returns the rows of the column at rows.
func (c *TableCol) Take(rows []int) *TableCol {
	out := &TableCol{Name: c.Name, Type: c.Type, TypeName: c.TypeName, SQLType: c.SQLType, Null: make([]bool, len(rows))}
	for k, r := range rows {
		out.Null[k] = c.Null[r]
	}
	switch c.Type {
	case TypeNumber:
		out.Num = make([]float64, len(rows))
		for k, r := range rows {
			out.Num[k] = c.Num[r]
		}
	case TypeBool, TypeString, TypeOther:
		out.Str = make([]string, len(rows))
		for k, r := range rows {
			out.Str[k] = c.Str[r]
		}
	case TypeTime:
		out.Micros = make([]int64, len(rows))
		for k, r := range rows {
			out.Micros[k] = c.Micros[r]
		}
	}
	return out
}

// Take returns the table's rows at rows.
func (t *Table) Take(rows []int) *Table {
	out := &Table{N: len(rows)}
	for _, c := range t.Cols {
		out.Cols = append(out.Cols, c.Take(rows))
	}
	return out
}
