package snowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

// runCase is a Snowflake statement and what Snowflake returns for it.
type runCase struct {
	name string
	sql  string
	// want is the result, one string per row, values separated by "|", as
	// format writes them; wantErr, if set, is part of the error instead.
	want    []string
	wantErr string
	// types are the DuckDB types of the result's columns, when they matter.
	types []string
	// cols are the result's column names as ColumnName presents them.
	cols []string
}

// session opens a DuckDB connection with the macros defined.
func session(t *testing.T) *sql.Conn {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if _, err := conn.ExecContext(ctx, "SET TimeZone = 'UTC'"); err != nil {
		t.Fatal(err)
	}
	for _, m := range Macros() {
		if _, err := conn.ExecContext(ctx, m); err != nil {
			t.Fatalf("%v\n%s", err, m)
		}
	}
	return conn
}

// format writes a value as the tests expect it. duckdb-go decodes JSON
// values, a JSON null to nil, so JSON is written as JSON again.
func format(v any, typ string) string {
	if typ == "JSON" && v != nil {
		b, err := json.Marshal(v)
		if err == nil {
			return string(b)
		}
	}
	switch v := v.(type) {
	case nil:
		return "NULL"
	case string:
		return v
	case []byte:
		return fmt.Sprintf("\\x%x", v)
	case bool:
		return strconv.FormatBool(v)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case *big.Int:
		return v.String()
	case duckdb.Decimal:
		s := new(big.Int).Abs(v.Value).String()
		if v.Scale > 0 {
			for len(s) <= int(v.Scale) {
				s = "0" + s
			}
			s = s[:len(s)-int(v.Scale)] + "." + s[len(s)-int(v.Scale):]
		}
		if v.Value.Sign() < 0 {
			s = "-" + s
		}
		return s
	case duckdb.Interval:
		return fmt.Sprintf("%d months %d days %d us", v.Months, v.Days, v.Micros)
	case time.Time:
		switch {
		case typ == "DATE":
			return v.Format("2006-01-02")
		case typ == "TIME":
			return v.Format("15:04:05.999999")
		case strings.HasPrefix(typ, "TIMESTAMP WITH TIME ZONE") || typ == "TIMESTAMPTZ":
			return v.UTC().Format("2006-01-02 15:04:05.999999Z")
		}
		return v.Format("2006-01-02 15:04:05.999999")
	case int, int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		return fmt.Sprint(v)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

// run translates and runs c.sql, comparing the result with c's.
func run(t *testing.T, conn *sql.Conn, c runCase) {
	t.Helper()
	ctx := context.Background()
	res, err := Translate(c.sql, Options{IsSetting: func(n string) bool { return n == "timezone" }})
	if err != nil {
		if c.wantErr != "" && strings.Contains(err.Error(), c.wantErr) {
			return
		}
		t.Fatalf("translate %s: %v", c.sql, err)
	}
	rows, err := conn.QueryContext(ctx, res.SQL)
	if err != nil {
		if c.wantErr != "" && strings.Contains(err.Error(), c.wantErr) {
			return
		}
		t.Fatalf("%v\nSnowflake: %s\nDuckDB:    %s", err, c.sql, res.SQL)
	}
	defer rows.Close()
	if c.wantErr != "" {
		for rows.Next() {
		}
		if err := rows.Err(); err != nil && strings.Contains(err.Error(), c.wantErr) {
			return
		}
		t.Fatalf("want an error with %q\nDuckDB: %s", c.wantErr, res.SQL)
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	names, _ := rows.Columns()
	var got []string
	for rows.Next() {
		vals := make([]any, len(types))
		ptrs := make([]any, len(types))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		parts := make([]string, len(vals))
		for i, v := range vals {
			parts[i] = format(v, types[i].DatabaseTypeName())
		}
		got = append(got, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%v\nSnowflake: %s\nDuckDB:    %s", err, c.sql, res.SQL)
	}
	if strings.Join(got, "\n") != strings.Join(c.want, "\n") {
		t.Errorf("got\n  %s\nwant\n  %s\nSnowflake: %s\nDuckDB:    %s", strings.Join(got, "\n  "),
			strings.Join(c.want, "\n  "), c.sql, res.SQL)
	}
	for i, typ := range c.types {
		if i < len(types) && typ != "" && types[i].DatabaseTypeName() != typ {
			t.Errorf("column %d is %s, want %s\nDuckDB: %s", i+1, types[i].DatabaseTypeName(), typ, res.SQL)
		}
	}
	for i, name := range c.cols {
		if i < len(names) && ColumnName(names[i], res.Quoted) != name {
			t.Errorf("column %d is named %q, want %q\nDuckDB: %s", i+1, ColumnName(names[i], res.Quoted), name, res.SQL)
		}
	}
}

var _ driver.Value // keep database/sql/driver for Scan helpers

// TestAgainstSnowflake runs translated statements on DuckDB and compares
// their results with what Snowflake returned for the same statements
// (recorded from Snowflake; see the comments).
func TestAgainstSnowflake(t *testing.T) {
	conn := session(t)
	for _, c := range snowflakeCases {
		t.Run(c.name, func(t *testing.T) { run(t, conn, c) })
	}
}
