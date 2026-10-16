package sharing

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
)

// Translating Iceberg to the Delta Sharing protocol's Parquet response
// format. Recipients read the table's Parquet data files directly, matching
// columns by name, so what they see is decided by the files and by the
// schema sent in the metadata, which must describe the files truthfully:
//
//   - Iceberg partitioning is hidden: partition source columns are stored in
//     the data files. So the Delta table has no partition columns, and every
//     file's partition values are empty.
//   - Iceberg matches Parquet columns by field ID, so a column can be renamed
//     or dropped without rewriting files. Delta readers would then read a
//     renamed column as missing, and could read a dropped column's values in
//     files written before the drop (a column dropped and re-added under the
//     same name would even show the old values). Tables with renamed or
//     dropped columns are therefore not shared until rewritten.
//   - Row-level deletes live in separate delete files that Delta readers do
//     not apply, so a data file with deletes is served as a copy with them
//     applied (see materialize.go), never with its deleted rows.

// errNotShareable marks reasons a table cannot be served. Its message is
// shown to admins and recipients as is.
type errNotShareable struct{ msg string }

func (e errNotShareable) Error() string { return e.msg }

func notShareable(format string, args ...any) error {
	return errNotShareable{msg: fmt.Sprintf(format, args...)}
}

// isNotShareable reports whether err says a table cannot be shared as it is.
func isNotShareable(err error) bool {
	var e errNotShareable
	return errors.As(err, &e)
}

// rewriteHint tells an admin how to make a table shareable again.
func rewriteHint(name string) string {
	return fmt.Sprintf("rewrite the table to share it, for example with CREATE OR REPLACE TABLE %s AS SELECT * FROM %s", name, name)
}

// deltaField is a field of Delta's JSON schema (a subset of Spark's).
type deltaField struct {
	Name     string            `json:"name"`
	Type     any               `json:"type"`
	Nullable bool              `json:"nullable"`
	Metadata map[string]string `json:"metadata"`
}

type deltaStruct struct {
	Type   string       `json:"type"`
	Fields []deltaField `json:"fields"`
}

type deltaArray struct {
	Type         string `json:"type"`
	ElementType  any    `json:"elementType"`
	ContainsNull bool   `json:"containsNull"`
}

type deltaMap struct {
	Type              string `json:"type"`
	KeyType           any    `json:"keyType"`
	ValueType         any    `json:"valueType"`
	ValueContainsNull bool   `json:"valueContainsNull"`
}

// deltaSchema returns the Delta schema string for an Iceberg schema.
func deltaSchema(s *iceberg.Schema) (string, error) {
	st := s.AsStruct()
	t, err := deltaType(&st, "")
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(t)
	return string(b), err
}

// deltaType converts an Iceberg type to Delta's. path names the column for
// errors.
func deltaType(t iceberg.Type, path string) (any, error) {
	switch t := t.(type) {
	case iceberg.BooleanType:
		return "boolean", nil
	case iceberg.Int32Type:
		return "integer", nil
	case iceberg.Int64Type:
		return "long", nil
	case iceberg.Float32Type:
		return "float", nil
	case iceberg.Float64Type:
		return "double", nil
	case iceberg.DecimalType:
		return fmt.Sprintf("decimal(%d,%d)", t.Precision(), t.Scale()), nil
	case iceberg.DateType:
		return "date", nil
	case iceberg.TimestampType, iceberg.TimestampTzType:
		// Delta's timestamp is the one every client reads; timestamp_ntz
		// needs a newer reader. Values without a time zone are read as UTC.
		return "timestamp", nil
	case iceberg.StringType:
		return "string", nil
	case iceberg.BinaryType, iceberg.FixedType, iceberg.UUIDType:
		// Parquet stores UUIDs as 16 fixed bytes; Delta has no UUID type.
		return "binary", nil
	case *iceberg.StructType:
		fields := make([]deltaField, 0, len(t.FieldList))
		for _, f := range t.FieldList {
			name := f.Name
			if path != "" {
				name = path + "." + f.Name
			}
			ft, err := deltaType(f.Type, name)
			if err != nil {
				return nil, err
			}
			meta := map[string]string{}
			if f.Doc != "" {
				meta["comment"] = f.Doc
			}
			fields = append(fields, deltaField{Name: f.Name, Type: ft, Nullable: !f.Required, Metadata: meta})
		}
		return deltaStruct{Type: "struct", Fields: fields}, nil
	case *iceberg.ListType:
		et, err := deltaType(t.Element, path+"[]")
		if err != nil {
			return nil, err
		}
		return deltaArray{Type: "array", ElementType: et, ContainsNull: !t.ElementRequired}, nil
	case *iceberg.MapType:
		kt, err := deltaType(t.KeyType, path+".key")
		if err != nil {
			return nil, err
		}
		vt, err := deltaType(t.ValueType, path+".value")
		if err != nil {
			return nil, err
		}
		return deltaMap{Type: "map", KeyType: kt, ValueType: vt, ValueContainsNull: !t.ValueRequired}, nil
	}
	return nil, notShareable("column %s has type %s, which Delta Sharing has no type for; share a table without it", path, t)
}

// checkSchemaHistory refuses tables whose columns were renamed or dropped
// (see the top of this file). name is the table's name for messages.
func checkSchemaHistory(current *iceberg.Schema, history []*iceberg.Schema, name string) error {
	names := map[int]string{}
	collectNames(current.AsStruct().FieldList, "", names)
	for _, old := range history {
		if old.ID == current.ID {
			continue
		}
		before := map[int]string{}
		collectNames(old.AsStruct().FieldList, "", before)
		for id, was := range before {
			now, ok := names[id]
			switch {
			case !ok:
				return notShareable("column %s of %s was dropped, and data files written before then may still hold it; "+
					"recipients receive whole data files, so %s", was, name, rewriteHint(name))
			case now != was:
				return notShareable("column %s of %s was renamed to %s; recipients match data file columns by name, "+
					"and files written before the rename use the old one, so %s", was, name, now, rewriteHint(name))
			}
		}
	}
	return nil
}

// collectNames maps the ID of every struct field, nested ones too, to its
// dotted path.
func collectNames(fields []iceberg.NestedField, prefix string, out map[int]string) {
	for _, f := range fields {
		path := prefix + f.Name
		out[f.ID] = path
		collectNested(f.Type, path+".", out)
	}
}

func collectNested(t iceberg.Type, prefix string, out map[int]string) {
	switch t := t.(type) {
	case *iceberg.StructType:
		collectNames(t.FieldList, prefix, out)
	case *iceberg.ListType:
		collectNested(t.Element, prefix, out)
	case *iceberg.MapType:
		collectNested(t.KeyType, prefix, out)
		collectNested(t.ValueType, prefix, out)
	}
}

// deltaStats is the protocol's per-file statistics.
type deltaStats struct {
	NumRecords int64            `json:"numRecords"`
	MinValues  map[string]any   `json:"minValues,omitempty"`
	MaxValues  map[string]any   `json:"maxValues,omitempty"`
	NullCount  map[string]int64 `json:"nullCount,omitempty"`
}

// fileStats returns a data file's statistics as the protocol's JSON string,
// from the bounds and counts in its manifest entry. Recipients may skip
// files by them, so only bounds known to be exact are sent: none for
// strings and binary, which writers may truncate, none for floating point
// columns that may hold NaN, and timestamp bounds widened to the
// millisecond, the precision of Delta's statistics.
func fileStats(df iceberg.DataFile, s *iceberg.Schema) string {
	st := deltaStats{NumRecords: df.Count(), MinValues: map[string]any{}, MaxValues: map[string]any{}, NullCount: map[string]int64{}}
	lower, upper, nulls, nans := df.LowerBoundValues(), df.UpperBoundValues(), df.NullValueCounts(), df.NaNValueCounts()
	for _, f := range s.Fields() {
		if _, ok := f.Type.(iceberg.PrimitiveType); !ok {
			continue
		}
		if n, ok := nulls[f.ID]; ok {
			st.NullCount[f.Name] = n
		}
		lo, hasLo := lower[f.ID]
		hi, hasHi := upper[f.ID]
		if !hasLo || !hasHi {
			continue
		}
		switch f.Type.(type) {
		case iceberg.Float32Type, iceberg.Float64Type:
			if n, ok := nans[f.ID]; !ok || n != 0 {
				continue
			}
		}
		min, ok1 := statValue(f.Type, lo, false)
		max, ok2 := statValue(f.Type, hi, true)
		if ok1 && ok2 {
			st.MinValues[f.Name], st.MaxValues[f.Name] = min, max
		}
	}
	b, err := json.Marshal(st)
	if err != nil {
		return ""
	}
	return string(b)
}

// statValue decodes an Iceberg bound into the JSON value Delta's
// statistics use. upper rounds timestamps up rather than down.
func statValue(t iceberg.Type, raw []byte, upper bool) (any, bool) {
	lit, err := iceberg.LiteralFromBytes(t, raw)
	if err != nil {
		return nil, false
	}
	switch v := lit.(type) {
	case iceberg.Int32Literal:
		return int64(v), true
	case iceberg.Int64Literal:
		return int64(v), true
	case iceberg.Float32Literal:
		f := float64(v)
		return f, !math.IsInf(f, 0) && !math.IsNaN(f)
	case iceberg.Float64Literal:
		f := float64(v)
		return f, !math.IsInf(f, 0) && !math.IsNaN(f)
	case iceberg.DecimalLiteral:
		return json.Number(iceberg.Decimal(v).String()), true
	case iceberg.DateLiteral:
		return iceberg.Date(v).ToTime().Format(time.DateOnly), true
	case iceberg.TimestampLiteral:
		us := int64(v)
		ms := us / 1000
		if us%1000 != 0 && (us < 0) != upper {
			// Truncating moves a negative lower bound up and a positive
			// upper bound down; widen instead.
			if upper {
				ms++
			} else {
				ms--
			}
		}
		return time.UnixMilli(ms).UTC().Format("2006-01-02T15:04:05.000Z"), true
	}
	return nil, false
}

// duckType converts a Delta schema type to DuckDB's, for the columns of an
// empty external table and for typed partition values.
func duckType(t any) (string, error) {
	switch t := t.(type) {
	case string:
		switch t {
		case "string":
			return "VARCHAR", nil
		case "long":
			return "BIGINT", nil
		case "integer":
			return "INTEGER", nil
		case "short":
			return "SMALLINT", nil
		case "byte":
			return "TINYINT", nil
		case "float":
			return "FLOAT", nil
		case "double":
			return "DOUBLE", nil
		case "boolean":
			return "BOOLEAN", nil
		case "binary":
			return "BLOB", nil
		case "date":
			return "DATE", nil
		case "timestamp":
			return "TIMESTAMPTZ", nil
		case "timestamp_ntz":
			return "TIMESTAMP", nil
		case "variant":
			return "JSON", nil
		}
		var p, s int
		if n, _ := fmt.Sscanf(strings.ReplaceAll(t, " ", ""), "decimal(%d,%d)", &p, &s); n == 2 {
			return fmt.Sprintf("DECIMAL(%d,%d)", p, s), nil
		}
	case map[string]any:
		switch t["type"] {
		case "struct":
			fields, _ := t["fields"].([]any)
			parts := make([]string, 0, len(fields))
			for _, f := range fields {
				fm, _ := f.(map[string]any)
				name, _ := fm["name"].(string)
				ft, err := duckType(fm["type"])
				if err != nil {
					return "", err
				}
				parts = append(parts, quoteIdent(name)+" "+ft)
			}
			return "STRUCT(" + strings.Join(parts, ", ") + ")", nil
		case "array":
			et, err := duckType(t["elementType"])
			if err != nil {
				return "", err
			}
			return et + "[]", nil
		case "map":
			kt, err := duckType(t["keyType"])
			if err != nil {
				return "", err
			}
			vt, err := duckType(t["valueType"])
			if err != nil {
				return "", err
			}
			return "MAP(" + kt + ", " + vt + ")", nil
		}
	}
	return "", fmt.Errorf("unsupported Delta type %v", t)
}

// remoteSchema is a Delta schema as a provider sends it.
type remoteSchema struct {
	Type   string `json:"type"`
	Fields []struct {
		Name string `json:"name"`
		Type any    `json:"type"`
	} `json:"fields"`
}

func parseRemoteSchema(s string) (remoteSchema, error) {
	var rs remoteSchema
	if err := json.Unmarshal([]byte(s), &rs); err != nil {
		return rs, fmt.Errorf("read the shared table's schema: %w", err)
	}
	if rs.Type != "struct" {
		return rs, fmt.Errorf("the shared table's schema is a %q, not a struct", rs.Type)
	}
	return rs, nil
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
