package pgwire

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"

	"github.com/akkio-inc/igloo/internal/sqllex"
)

// encoder turns a value scanned from DuckDB into one pgx can encode as the
// column's Postgres type, in text or binary format. Encoders never see nil.
type encoder func(any) (any, error)

// wireType picks the Postgres type a DuckDB column is sent as. Types Postgres
// has no equivalent for go out as text, and nested types (lists, structs,
// maps) go out as JSON, which every Postgres client can read.
// alias is DuckDB's logical type alias, such as "JSON".
func wireType(t duckdb.Type, alias string) (uint32, encoder) {
	if alias == "JSON" {
		return pgtype.JSONOID, toJSON
	}

	switch t {
	case duckdb.TYPE_BOOLEAN:
		return pgtype.BoolOID, same
	case duckdb.TYPE_TINYINT, duckdb.TYPE_SMALLINT, duckdb.TYPE_UTINYINT:
		return pgtype.Int2OID, toInt16
	case duckdb.TYPE_INTEGER, duckdb.TYPE_USMALLINT:
		return pgtype.Int4OID, toInt32
	case duckdb.TYPE_BIGINT, duckdb.TYPE_UINTEGER:
		return pgtype.Int8OID, toInt64
	case duckdb.TYPE_UBIGINT, duckdb.TYPE_HUGEINT, duckdb.TYPE_UHUGEINT, duckdb.TYPE_BIGNUM, duckdb.TYPE_DECIMAL:
		return pgtype.NumericOID, toNumeric
	case duckdb.TYPE_FLOAT:
		return pgtype.Float4OID, same
	case duckdb.TYPE_DOUBLE:
		return pgtype.Float8OID, same
	case duckdb.TYPE_VARCHAR, duckdb.TYPE_ENUM:
		return pgtype.TextOID, same
	case duckdb.TYPE_BLOB:
		return pgtype.ByteaOID, same
	case duckdb.TYPE_UUID:
		return pgtype.UUIDOID, toUUID
	case duckdb.TYPE_DATE:
		return pgtype.DateOID, same
	case duckdb.TYPE_TIME:
		return pgtype.TimeOID, toTime
	case duckdb.TYPE_TIMESTAMP, duckdb.TYPE_TIMESTAMP_S, duckdb.TYPE_TIMESTAMP_MS, duckdb.TYPE_TIMESTAMP_NS:
		return pgtype.TimestampOID, same
	case duckdb.TYPE_TIMESTAMP_TZ:
		return pgtype.TimestamptzOID, same
	case duckdb.TYPE_INTERVAL:
		return pgtype.IntervalOID, toInterval
	case duckdb.TYPE_LIST, duckdb.TYPE_ARRAY, duckdb.TYPE_STRUCT, duckdb.TYPE_MAP, duckdb.TYPE_UNION, duckdb.TYPE_VARIANT:
		return pgtype.JSONOID, toJSON
	case duckdb.TYPE_GEOMETRY:
		return pgtype.TextOID, toWKT
	default:
		return pgtype.TextOID, toText
	}
}

// paramOID is the type Igloo asks clients to send a parameter as. When
// DuckDB cannot infer one, text lets DuckDB cast the value itself.
func paramOID(t duckdb.Type) uint32 {
	switch t {
	case duckdb.TYPE_INVALID, duckdb.TYPE_ANY, duckdb.TYPE_SQLNULL:
		return pgtype.TextOID
	}
	oid, _ := wireType(t, "")
	return oid
}

func same(v any) (any, error) { return v, nil }

func toInt16(v any) (any, error) {
	switch n := v.(type) {
	case int8:
		return int16(n), nil
	case uint8:
		return int16(n), nil
	}
	return v, nil
}

func toInt32(v any) (any, error) {
	if n, ok := v.(uint16); ok {
		return int32(n), nil
	}
	return v, nil
}

func toInt64(v any) (any, error) {
	if n, ok := v.(uint32); ok {
		return int64(n), nil
	}
	return v, nil
}

func toNumeric(v any) (any, error) {
	switch n := v.(type) {
	case duckdb.Decimal:
		return pgtype.Numeric{Int: n.Value, Exp: -int32(n.Scale), Valid: true}, nil
	case *big.Int:
		return pgtype.Numeric{Int: n, Valid: true}, nil
	case uint64:
		return pgtype.Numeric{Int: new(big.Int).SetUint64(n), Valid: true}, nil
	}
	return nil, fmt.Errorf("cannot send %T as numeric", v)
}

func toUUID(v any) (any, error) {
	b, ok := v.([]byte)
	if !ok || len(b) != 16 {
		return nil, fmt.Errorf("cannot send %T as uuid", v)
	}
	return pgtype.UUID{Bytes: [16]byte(b), Valid: true}, nil
}

func toTime(v any) (any, error) {
	t, ok := v.(time.Time)
	if !ok {
		return nil, fmt.Errorf("cannot send %T as time", v)
	}
	midnight := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	return pgtype.Time{Microseconds: t.Sub(midnight).Microseconds(), Valid: true}, nil
}

func toInterval(v any) (any, error) {
	i, ok := v.(duckdb.Interval)
	if !ok {
		return nil, fmt.Errorf("cannot send %T as interval", v)
	}
	return pgtype.Interval{Months: i.Months, Days: i.Days, Microseconds: i.Micros, Valid: true}, nil
}

func toText(v any) (any, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case time.Time:
		// TIME WITH TIME ZONE, the only time type that reaches here.
		return t.Format("15:04:05.999999-07:00"), nil
	case fmt.Stringer:
		return t.String(), nil
	}
	return fmt.Sprint(v), nil
}

// toJSON renders nested values as JSON. pgx sends a string as-is for a
// json column, so marshalling here is the whole encoding.
func toJSON(v any) (any, error) {
	if s, ok := v.(string); ok {
		return s, nil
	}
	b, err := json.Marshal(jsonValue(v))
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

// jsonValue rewrites DuckDB values that encoding/json would mangle, such as
// decimals (which would lose precision) and maps with non-string keys.
func jsonValue(v any) any {
	switch t := v.(type) {
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = jsonValue(e)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = jsonValue(e)
		}
		return out
	case duckdb.OrderedMap:
		out := make(map[string]any, t.Len())
		keys, values := t.Keys(), t.Values()
		for i := range keys {
			out[fmt.Sprint(keys[i])] = jsonValue(values[i])
		}
		return out
	case duckdb.Union:
		return jsonValue(t.Value)
	case duckdb.Decimal:
		return json.Number(t.String())
	case *big.Int:
		return json.Number(t.String())
	case uint64:
		return json.Number(fmt.Sprint(t))
	}
	return v
}

// decodeParams converts bind parameters to values DuckDB can bind. oids are
// the parameter types Igloo described to the client.
func decodeParams(params []wire.Parameter, oids []uint32) ([]driver.NamedValue, error) {
	args := make([]driver.NamedValue, len(params))
	for i, p := range params {
		args[i].Ordinal = i + 1
		if p.Value() == nil {
			continue // NULL
		}

		oid := uint32(pgtype.TextOID)
		if i < len(oids) {
			oid = oids[i]
		}

		v, err := p.Scan(oid)
		if errors.Is(err, wire.ErrUnknownOid) && p.Format() == wire.TextFormat {
			v, err = string(p.Value()), nil
		}
		if err != nil {
			return nil, fmt.Errorf("parameter $%d: %w", i+1, err)
		}

		if arrayOIDs[oid] != "" {
			// DuckDB binds Go slices as lists, as = ANY($1) needs.
			args[i].Value, err = listValue(v)
		} else {
			args[i].Value, err = duckdbValue(v)
		}
		if err != nil {
			return nil, fmt.Errorf("parameter $%d: %w", i+1, err)
		}
	}
	return args, nil
}

// arrayOIDs maps the PostgreSQL array types Igloo binds as DuckDB lists to
// those lists' DuckDB types.
var arrayOIDs = map[uint32]string{
	pgtype.BoolArrayOID: "BOOLEAN[]", pgtype.Int2ArrayOID: "SMALLINT[]", pgtype.Int4ArrayOID: "INTEGER[]",
	pgtype.Int8ArrayOID: "BIGINT[]", pgtype.Float4ArrayOID: "FLOAT[]", pgtype.Float8ArrayOID: "DOUBLE[]",
	pgtype.TextArrayOID: "VARCHAR[]", pgtype.VarcharArrayOID: "VARCHAR[]", pgtype.BPCharArrayOID: "VARCHAR[]",
	pgtype.NumericArrayOID: "DOUBLE[]", pgtype.DateArrayOID: "DATE[]", pgtype.TimestampArrayOID: "TIMESTAMP[]",
	pgtype.TimestamptzArrayOID: "TIMESTAMPTZ[]", pgtype.UUIDArrayOID: "UUID[]",
}

// placeholder matches a $n parameter in SQL code.
var placeholder = regexp.MustCompile(`\$(\d+)\b`)

// anyPlaceholder matches a parameter that is the whole operand of ANY or ALL.
var anyPlaceholder = regexp.MustCompile(`(?i)\b(?:ANY|ALL)\s*\(\s*\$(\d+)\s*\)`)

// typeArrayParams casts list parameters to DuckDB list types, because
// DuckDB cannot infer them, as in = ANY($1), and then cannot describe the
// statement's result. A parameter is a list if the client declared an array
// type, or, like PostgreSQL infers, if it is the operand of ANY or ALL;
// those get text[]. It returns the query and the parameter types, the
// client's plus the inferred ones.
func typeArrayParams(query string, clientOIDs []uint32) (string, []uint32) {
	oids := append([]uint32(nil), clientOIDs...)
	declared := func(n int) uint32 {
		if n <= len(oids) {
			return oids[n-1]
		}
		return 0
	}
	for _, m := range anyPlaceholder.FindAllStringSubmatch(sqllex.CodeOnly(query), -1) {
		n, err := strconv.Atoi(m[1])
		if err != nil || n < 1 || declared(n) != 0 {
			continue
		}
		for len(oids) < n {
			oids = append(oids, 0)
		}
		oids[n-1] = pgtype.TextArrayOID
	}

	typed := false
	for _, oid := range oids {
		typed = typed || arrayOIDs[oid] != ""
	}
	if !typed {
		return query, clientOIDs
	}
	return sqllex.MapCode(query, func(code string) string {
		return placeholder.ReplaceAllStringFunc(code, func(m string) string {
			n, err := strconv.Atoi(m[1:])
			if err != nil || n < 1 || arrayOIDs[declared(n)] == "" {
				return m
			}
			return "(" + m + "::" + arrayOIDs[declared(n)] + ")"
		})
	}), oids
}

// listValue converts a decoded array into a slice of values DuckDB can bind.
func listValue(v any) (any, error) {
	items, ok := v.([]any)
	if !ok {
		return duckdbValue(v)
	}
	out := make([]any, len(items))
	for i, item := range items {
		if item == nil {
			continue
		}
		var err error
		if out[i], err = duckdbValue(item); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// duckdbValue converts a decoded pgx value into one DuckDB can bind. Most
// pgtype values implement driver.Valuer and reduce to text DuckDB casts.
func duckdbValue(v any) (any, error) {
	switch t := v.(type) {
	case [16]byte:
		return duckdb.UUID(t), nil
	case map[string]any, []any:
		b, err := json.Marshal(t)
		return string(b), err
	case driver.Valuer:
		return t.Value()
	}
	return v, nil
}
