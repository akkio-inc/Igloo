package engine

import (
	"bufio"
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/snowflake"
)

// testRB returns the rb extension built for the tests (extensions/rb/build.sh,
// named by IGLOO_TEST_RB_EXTENSION). CI always builds it, so there a test that
// needs it fails without it; elsewhere it is skipped.
func testRB(t *testing.T) string {
	t.Helper()
	p := os.Getenv(rbTestEnv)
	if p == "" {
		if os.Getenv("CI") != "" {
			t.Fatalf("%s is not set: CI builds the rb extension (extensions/rb/build.sh) before the tests", rbTestEnv)
		}
		t.Skipf("%s is not set; build the rb extension with extensions/rb/build.sh", rbTestEnv)
	}
	return p
}

// imageLayout writes data where an image keeps one of Igloo's extensions
// (file, such as rbFile), under a new extension directory, and returns that
// directory and the file's hash. With official, the directory also links to
// the official extensions the tests install (IGLOO_TEST_EXTENSION_DIR),
// which Open loads from it.
func imageLayout(t *testing.T, file string, data []byte, official bool) (string, string) {
	t.Helper()
	dir := t.TempDir()
	if official {
		src := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
		if src == "" {
			if os.Getenv("CI") != "" {
				t.Fatal("IGLOO_TEST_EXTENSION_DIR is not set")
			}
			t.Skip("IGLOO_TEST_EXTENSION_DIR is not set")
		}
		entries, err := os.ReadDir(src)
		if err != nil {
			t.Fatal(err)
		}
		for _, ent := range entries {
			if err := os.Symlink(filepath.Join(src, ent.Name()), filepath.Join(dir, ent.Name())); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := os.MkdirAll(filepath.Join(dir, "igloo"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, file), data, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return dir, hex.EncodeToString(sum[:])
}

// withHash makes this test's igloo a build with the given hash of one of its
// extensions (hash, such as &rbSHA256).
func withHash(t *testing.T, hash *string, sha string) {
	t.Helper()
	old := *hash
	*hash = sha
	t.Cleanup(func() { *hash = old })
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestReadFooter(t *testing.T) {
	field := func(s string) []byte { return append([]byte(s), make([]byte, 32-len(s))...) }
	var meta []byte
	for _, f := range []string{"", "", "", "CPP", "1", "v1.5.6", "linux_arm64", "4"} {
		meta = append(meta, field(f)...)
	}
	data := append(append([]byte("ELF..."), meta...), make([]byte, 256)...)
	f, err := readFooter(data)
	if err != nil {
		t.Fatal(err)
	}
	if f.platform != "linux_arm64" || f.duckdbVersion != "v1.5.6" || f.abi != "CPP" {
		t.Fatalf("read %+v", f)
	}
	if _, err := readFooter(make([]byte, 600)); err == nil {
		t.Fatal("a file without a footer passed")
	}
	if _, err := readFooter([]byte("short")); err == nil {
		t.Fatal("a short file passed")
	}
}

func TestRBNotLoadedWithoutExtension(t *testing.T) {
	t.Setenv(rbTestEnv, "")
	e, err := Open(context.Background(), config.Config{}, quietLog())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close() //nolint:errcheck
	if e.BitmapsLoaded() {
		t.Fatal("BitmapsLoaded without an extension")
	}
}

func TestRBRefusesAFileOtherThanTheBuilds(t *testing.T) {
	dir, _ := imageLayout(t, rbFile, append([]byte("not the extension this igloo was built with"), make([]byte, 512)...), false)
	withHash(t, &rbSHA256, strings.Repeat("ab", 32))
	_, err := Open(context.Background(), config.Config{DuckDB: config.DuckDB{ExtensionDir: dir}}, quietLog())
	if err == nil || !strings.Contains(err.Error(), "SHA-256") {
		t.Fatalf("got %v, want a SHA-256 mismatch", err)
	}
}

func TestRBRefusesAnotherDuckDB(t *testing.T) {
	data, err := os.ReadFile(testRB(t))
	if err != nil {
		t.Fatal(err)
	}
	// rewrite the footer's DuckDB version (the third field, stored sixth)
	at := len(data) - 512 + 5*32
	copy(data[at:at+32], append([]byte("v1.5.5"), make([]byte, 26)...))
	dir, sha := imageLayout(t, rbFile, data, false)
	withHash(t, &rbSHA256, sha)
	_, err = Open(context.Background(), config.Config{DuckDB: config.DuckDB{ExtensionDir: dir}}, quietLog())
	if err == nil || !strings.Contains(err.Error(), "built for DuckDB v1.5.5") {
		t.Fatalf("got %v, want a DuckDB version mismatch", err)
	}
}

// TestIcebergExtensionIsIgloos checks that an image's igloo loads its own
// build of the iceberg extension (extensions/iceberg), checked against the
// build's hash, so that the iceberg DuckDB attaches the catalog with is
// that one and not the official one in DuckDB's tree, and refuses another
// file in its place.
func TestIcebergExtensionIsIgloos(t *testing.T) {
	p := os.Getenv("IGLOO_TEST_ICEBERG_EXTENSION")
	if p == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("IGLOO_TEST_ICEBERG_EXTENSION is not set: CI builds the iceberg extension (extensions/iceberg/build.sh)")
		}
		t.Skip("IGLOO_TEST_ICEBERG_EXTENSION is not set; build the extension with extensions/iceberg/build.sh")
	}
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	dir, sha := imageLayout(t, "igloo/iceberg.duckdb_extension", data, true)
	withHash(t, &icebergSHA256, sha)
	e, err := Open(context.Background(), config.Config{DuckDB: config.DuckDB{ExtensionDir: dir}}, quietLog())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close() //nolint:errcheck
	if !e.icebergOwn {
		t.Fatal("Igloo's iceberg extension did not load")
	}
	withHash(t, &icebergSHA256, strings.Repeat("cd", 32))
	if _, err := Open(context.Background(), config.Config{DuckDB: config.DuckDB{ExtensionDir: dir}}, quietLog()); err == nil ||
		!strings.Contains(err.Error(), "SHA-256") {
		t.Fatalf("got %v, want a SHA-256 mismatch", err)
	}
}

// openWithRB opens an engine as an image would: the extension in its place
// under the extension directory, checked against the build's hash.
func openWithRB(t *testing.T) (*Engine, string) {
	t.Helper()
	data, err := os.ReadFile(testRB(t))
	if err != nil {
		t.Fatal(err)
	}
	dir, sha := imageLayout(t, rbFile, data, true)
	withHash(t, &rbSHA256, sha)
	e, err := Open(context.Background(), config.Config{DuckDB: config.DuckDB{ExtensionDir: dir}}, quietLog())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() }) //nolint:errcheck
	if !e.BitmapsLoaded() {
		t.Fatal("the rb extension did not load")
	}
	return e, dir
}

func TestRBLockdownHolds(t *testing.T) {
	ctx := context.Background()
	e, dir := openWithRB(t)
	conn, err := e.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	path := filepath.Join(dir, rbFile)
	// another extension's file, with a valid footer, outside every allowed directory
	evil := filepath.Join(t.TempDir(), "evil.duckdb_extension")
	if err := os.WriteFile(evil, append([]byte("ELF"), make([]byte, 600)...), 0o644); err != nil {
		t.Fatal(err)
	}
	try := func(stmt, want string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, stmt, nil)
		if err == nil {
			t.Errorf("after lockdown, %q ran", stmt)
		} else if !strings.Contains(err.Error(), want) {
			t.Errorf("after lockdown, %q failed with %q, want %q", stmt, err, want)
		}
	}
	check := func() {
		t.Helper()
		// nothing loads or installs, unsigned or signed, by path or by name
		try("LOAD "+literal(evil), "disabled")
		try("LOAD mysql", "disabled")
		try("LOAD postgres", "disabled")
		try("INSTALL "+literal(evil), "disabled")
		try("INSTALL rb FROM "+literal(dir), "disabled")
		try("FORCE INSTALL httpfs", "disabled")
		// nor can SQL read or write the extension directory
		try("SELECT * FROM read_blob("+literal(path)+")", "disabled")
		try("SELECT * FROM read_text("+literal(path)+")", "disabled")
		try("COPY (SELECT 1) TO "+literal(filepath.Join(dir, "igloo", "x.csv")), "disabled")
		try("SELECT * FROM glob("+literal(filepath.Join(dir, "*"))+")", "disabled")
		// nor can the settings that would allow any of it change
		try("SET allow_unsigned_extensions = true", "Cannot change")
		try("SET enable_external_access = true", "Cannot")
		try("SET allowed_directories = ['/']", "Cannot change")
		try("SET allowed_paths = "+literal("['"+path+"']"), "Cannot change")
	}
	check()
	// Settings DuckDB still lets a statement change after lockdown (Igloo lets
	// only admins SET a global setting; see pgwire's authorize) open nothing.
	for _, stmt := range []string{
		"SET extension_directory = " + literal(dir), "SET autoinstall_known_extensions = true",
		"SET autoload_known_extensions = true", "SET allow_extensions_metadata_mismatch = true",
	} {
		if _, err := conn.ExecContext(ctx, stmt, nil); err != nil {
			t.Logf("%s: %v", stmt, err)
		}
	}
	check()
	// the extension's own functions work
	if got := queryOne(t, conn, "SELECT rb_count_distinct(x::UINTEGER) FROM range(1000) t(x)"); got != "1000" {
		t.Fatalf("rb_count_distinct gave %s", got)
	}
	if got := queryOne(t, conn, "SELECT current_setting('allow_unsigned_extensions')::VARCHAR"); got != "false" {
		t.Fatalf("allow_unsigned_extensions is %s after loading rb", got)
	}
}

func queryOne(t *testing.T, conn *duckdb.Conn, q string) string {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(), q, nil)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return render(dest[0])
}

// TestRBFunctions runs extensions/rb/test/*.test against Igloo's DuckDB with
// the extension loaded.
// TestRBStringsMatchDuckDB checks the extension's trim, ltrim and rtrim
// (rb_strings.cpp) against DuckDB's own, in a database without it: strings
// with spaces and Unicode's other space separators at either end and inside,
// with characters other than ASCII, of 12 bytes, which DuckDB keeps inside
// the string, and of more, and NULL; as a table's rows, chunks of all of
// them and chunks of ASCII alone, and as constants.
func TestRBStringsMatchDuckDB(t *testing.T) {
	ctx := context.Background()
	e, _ := openWithRB(t)
	fast, err := e.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Close() //nolint:errcheck
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close() //nolint:errcheck
	dc, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer dc.Close() //nolint:errcheck
	plain := dc.(*duckdb.Conn)
	values := []string{"", " ", "   ", "a", " a", "a ", " a b ", " x　", "     ",
		"   ", "x᠎", "᠎x", "\tx\n", "  twelve byte", "exactly12byt", " exactly12byt ", "thirteen byte",
		"ÉCOLE École", "straße STRASSE", "İstanbul", "ΣΊΣΥΦΟΣ", "Mixed Case ASCII", "UPPER", "lower", " 😀 emoji 😀 ",
		strings.Repeat("ab ", 20) + "  ", "  " + strings.Repeat("Zz", 30)}
	list := make([]string, len(values))
	for i, v := range values {
		list[i] = "'" + v + "'"
	}
	table := "CREATE TEMP TABLE strs AS SELECT row_number() OVER () AS i, s FROM (SELECT unnest([" + strings.Join(list, ", ") +
		", NULL]) AS s), range(200)"
	for _, conn := range []*duckdb.Conn{fast, plain} {
		if _, err := conn.ExecContext(ctx, table, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, fn := range []string{"trim", "ltrim", "rtrim"} {
		for _, q := range []string{
			"SELECT i, " + fn + "(s) FROM strs ORDER BY i",
			"SELECT i, " + fn + "(s) FROM strs WHERE NOT regexp_matches(s, '[^\\x00-\\x7f]') ORDER BY i",
			"SELECT " + fn + "(' Mixed Case '), " + fn + "('é Î '), " + fn + "(NULL)",
		} {
			got, want := queryRows(t, fast, q, fn), queryRows(t, plain, q, fn)
			if strings.Join(got, "\n") != strings.Join(want, "\n") {
				t.Errorf("%s: the extension's differ from DuckDB's", q)
			}
		}
	}
}

// TestRBDivideMatchesMacro checks igloo_sf_div, which sf_div is with the rb
// extension (see snowflake.MacrosFor), against the sf_div macros it stands
// for: the same DOUBLEs, to the last digit, the same NULLs and the same
// errors, for operands of every type the macros take and some they refuse,
// as constants and in columns.
func TestRBDivideMatchesMacro(t *testing.T) {
	ctx := context.Background()
	fast, plain := nativeAndMacros(t)
	if def, err := firstRow(ctx, fast, "SELECT macro_definition FROM duckdb_functions() WHERE function_name = 'sf_div'"); err != nil ||
		!strings.Contains(def, "igloo_sf_div") {
		t.Fatalf("sf_div is %q (%v), not igloo_sf_div", def, err)
	}
	operands := []string{
		"0", "1", "7", "-3", "16", "1000000", "-999999937", "3999999999999", "4000000000000", "-4000000000001",
		"999999999999999999", "1000000000000000000", "9223372036854775807", "-9223372036854775807",
		"123456789012345678901234567890", "1000000000000000000000000000000", "-12345678901234567890123456789012345678",
		"5::TINYINT", "-7::SMALLINT", "100000::INTEGER", "200::UTINYINT", "60000::USMALLINT", "4000000000::UINTEGER",
		"18000000000000000000::UBIGINT", "5::UHUGEINT", "7::HUGEINT", "(-1152921504606846977)::BIGINT",
		"16.0", "1.5", "-0.25", "123.456", "0.000001", "1.5::DECIMAL(38,10)", "2::DECIMAL(4,0)", "-7.125::DECIMAL(9,3)",
		"12345678901234.5678::DECIMAL(18,4)", "0.1234567890123::DECIMAL(13,13)", "99999999999999999999.99::DECIMAL(22,2)",
		"1.5::FLOAT", "-3::FLOAT", "1e30::FLOAT", "7::DOUBLE", "-0.0::DOUBLE", "'nan'::DOUBLE", "'inf'::DOUBLE",
		"'-inf'::DOUBLE", "1e308::DOUBLE", "1e-308::DOUBLE", "2.5e-10::DOUBLE", "0.1::DOUBLE",
		"NULL", "NULL::INTEGER", "NULL::DOUBLE", "NULL::DECIMAL(9,2)",
		"'4'", "'4'::JSON", "INTERVAL 1 DAY", "DATE '2025-01-01'", "true",
	}
	for _, a := range operands {
		for _, b := range operands {
			q := "SELECT sf_div(" + a + ", " + b + "), typeof(sf_div(" + a + ", " + b + "))"
			got, gerr := firstRow(ctx, fast, q)
			want, werr := firstRow(ctx, plain, q)
			switch {
			case werr != nil && strings.Contains(werr.Error(), "Overflow on abs"):
				// The macros' abs() of a type's smallest integer
				// overflows; igloo_sf_div divides it.
			case werr != nil && gerr != nil:
				if zero := "Division by zero"; strings.Contains(werr.Error(), zero) != strings.Contains(gerr.Error(), zero) {
					t.Errorf("%s: %v, the macros %v", q, gerr, werr)
				}
			case werr != nil || gerr != nil:
				t.Errorf("%s: %q %v, the macros %q %v", q, got, gerr, want, werr)
			case got != want:
				t.Errorf("%s: %s, the macros %s", q, got, want)
			}
		}
	}
	// Parameters, which DuckDB binds once their values' types are known.
	for _, args := range [][]any{{int64(1), int64(3)}, {int64(7), 2.5}, {1.5, int64(4)}, {int64(5), nil}, {int64(1), int64(0)}} {
		q := "SELECT sf_div($1, $2), sf_div($1, 16.0), sf_div(10, $2)"
		vals := []driver.NamedValue{{Ordinal: 1, Value: args[0]}, {Ordinal: 2, Value: args[1]}}
		got, gerr := firstRowArgs(ctx, fast, q, vals)
		want, werr := firstRowArgs(ctx, plain, q, vals)
		if got != want || (gerr == nil) != (werr == nil) {
			t.Errorf("%s with %v: %q %v, the macros %q %v", q, args, got, gerr, want, werr)
		}
	}
	table := "CREATE TEMP TABLE divs AS SELECT row_number() OVER () AS i, v FROM " +
		"(SELECT unnest([-7, -3, 1, 2, 3, 5, 16, 1000003, NULL, 127, 255, 4000000000001]) AS v)"
	for _, conn := range []*duckdb.Conn{fast, plain} {
		if _, err := conn.ExecContext(ctx, table, nil); err != nil {
			t.Fatal(err)
		}
	}
	types := []string{"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT",
		"UHUGEINT", "DECIMAL(9,3)", "DECIMAL(38,10)", "FLOAT", "DOUBLE"}
	for _, ta := range types {
		for _, tb := range types {
			// Unsigned types hold the values' magnitudes; what a type cannot
			// hold is NULL. The divisors are never 0.
			q := "SELECT a.i, b.i, sf_div(try_cast(" + magnitude(ta, "a.v") + " AS " + ta + "), try_cast(" + magnitude(tb, "b.v") +
				" AS " + tb + ")) FROM divs AS a, divs AS b ORDER BY 1, 2"
			got, want := queryRows(t, fast, q, ta+"/"+tb), queryRows(t, plain, q, ta+"/"+tb)
			if strings.Join(got, "\n") != strings.Join(want, "\n") {
				t.Errorf("%s / %s in columns: igloo_sf_div's differ from the macros'", ta, tb)
			}
		}
	}
}

// nativeAndMacros returns a connection with the rb extension and the macros
// as a session with it defines them (snowflake.MacrosFor), and one of a
// plain DuckDB with the macros alone.
func nativeAndMacros(t *testing.T) (fast, plain *duckdb.Conn) {
	t.Helper()
	ctx := context.Background()
	e, _ := openWithRB(t)
	fast, err := e.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fast.Close() }) //nolint:errcheck
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connector.Close() }) //nolint:errcheck
	dc, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	plain = dc.(*duckdb.Conn)
	t.Cleanup(func() { plain.Close() }) //nolint:errcheck
	for conn, stmts := range map[*duckdb.Conn][]string{fast: snowflake.MacrosFor(true), plain: snowflake.Macros()} {
		for _, stmt := range stmts {
			if _, err := conn.ExecContext(ctx, stmt, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	return fast, plain
}

// TestRBFlattenMatchesMacro checks igloo_sf_flatten_values, _strings, _keys
// and _indexes, which FLATTEN's sf_flatten_ macros are with the rb extension
// (see snowflake.MacrosFor), against the macros they stand for: the same
// lists, element for element and byte for byte, of the same types, the same
// NULLs and the same errors, for inputs of every kind the macros tell apart
// and every MODE, as constants, parameters and columns, in lists that cross
// DuckDB's vectors, unnested side by side as FLATTEN does, and through the
// macros that call them.
func TestRBFlattenMatchesMacro(t *testing.T) {
	ctx := context.Background()
	fast, plain := nativeAndMacros(t)
	lists := []string{"values", "strings", "keys", "indexes"}
	for _, l := range lists {
		def, err := firstRow(ctx, fast, "SELECT macro_definition FROM duckdb_functions() WHERE function_name = 'sf_flatten_"+l+"'")
		if err != nil || !strings.Contains(def, "igloo_sf_flatten_"+l) {
			t.Fatalf("sf_flatten_%s is %q (%v), not igloo_sf_flatten_%s", l, def, err, l)
		}
	}
	// Each element's bytes, so that JSON that reads the same but is written
	// differently differs.
	shown := func(call string) string {
		return "CASE WHEN (" + call + ") IS NULL THEN 'NULL' ELSE CAST(list_transform(" + call +
			", lambda v: coalesce(hex(CAST(v AS VARCHAR)), '-')) AS VARCHAR) END"
	}
	same := func(where, q string) {
		t.Helper()
		got, gerr := firstRow(ctx, fast, q)
		want, werr := firstRow(ctx, plain, q)
		switch {
		case werr != nil || gerr != nil:
			if werr == nil || gerr == nil || werr.Error() != gerr.Error() {
				t.Errorf("%s: %q %v, the macros %q %v", where, got, gerr, want, werr)
			}
		case got != want:
			t.Errorf("%s: %s, the macros %s", where, got, want)
		}
	}
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	docs := []string{
		`[1, "a", null, true, {"b": [2, 3]}, [4], -0.5]`, `{"a": 1, "b": "x", "c": null, "d": {"e": 1}, "f": [1]}`,
		`[]`, `{}`, `"text"`, `5`, `null`, `true`, `[1.0, 1e2, 0.10, -0, 12345678901234567890, 1E+2, 1e400]`,
		`{"a": 1, "a": 2}`, `  [ 1 ,2 ]  `, `["é", "é", "a\"b", "\\", "\n", "日本", "\u0001", ""]`,
		`{"é": 1, "": 2, "a b": [null], "日": {"本": []}}`, `[[[[]]]]`, `[{"x": {"y": [1, {"z": null}]}}]`,
		`{"k": "{\"inner\": 1}"}`, `["[1, 2]", "{\"a\": 1}", "{bad"]`,
	}
	malformed := []string{`[1, 2`, `{"a": }`, `abc`, `[1] x`, ``, `{`}
	var inputs []string
	for _, d := range docs {
		inputs = append(inputs, quote(d), quote(d)+"::JSON")
	}
	for _, d := range malformed {
		inputs = append(inputs, quote(d), quote(d)+"::JSON")
	}
	inputs = append(inputs,
		`['a', '{"b": 2}', '[1, 2]', '{bad', '[', NULL, '', ' {"x": 1}', '"q"', 'é', '{"a": 1, "a": 2}']`,
		`[]::VARCHAR[]`, `NULL::VARCHAR[]`, `[NULL]::VARCHAR[]`, `['{ "spaced" : [ 1 ] }']`,
		`MAP {'k': 'v', 'j': '{"a": [1]}', 'n': NULL, 'e': '', 'l': '[1,2]', 'b': '{nope'}`,
		`MAP {}::MAP(VARCHAR, VARCHAR)`, `NULL::MAP(VARCHAR, VARCHAR)`,
		`1::INTEGER`, `5`, `1.5::DOUBLE`, `true`, `DATE '2025-01-01'`, `[1, 2]`, `['1'::JSON, '{"a": 1}'::JSON]`,
		`[['a', 'b'], ['c']]`, `MAP {'k': 1}`, `{'a': 1, 'b': 'x', 'c': [1, 2]}`, `[{'a': 1}]`, `'\x01'::BLOB`,
		`1.25::DECIMAL(5,2)`, `NULL::INTEGER`, `NULL::JSON`, `NULL::VARCHAR`, `NULL`,
	)
	modes := []string{"'BOTH'", "'ARRAY'", "'OBJECT'", "NULL::VARCHAR", "'both'", "'other'"}
	for _, l := range lists {
		for _, in := range inputs {
			for _, m := range modes {
				call := "sf_flatten_" + l + "(" + in + ", " + m + ")"
				same(call, "SELECT "+shown(call)+", typeof("+call+")")
				// sf_flatten_values of a constant NULL is not a list.
				same(call, "SELECT CAST("+call+" AS VARCHAR), typeof("+call+")")
			}
		}
	}

	// Parameters, which DuckDB binds once their values' types are known.
	for _, arg := range []any{`[1, {"a": 2}]`, `{"k": "v"}`, nil, int64(3)} {
		q := "SELECT " + shown("sf_flatten_values($1, 'BOTH')") + ", " + shown("sf_flatten_strings($1, 'BOTH')") + ", " +
			shown("sf_flatten_keys($1, 'BOTH')") + ", " + shown("sf_flatten_indexes($1, 'BOTH')")
		vals := []driver.NamedValue{{Ordinal: 1, Value: arg}}
		got, gerr := firstRowArgs(ctx, fast, q, vals)
		want, werr := firstRowArgs(ctx, plain, q, vals)
		if got != want || (gerr == nil) != (werr == nil) {
			t.Errorf("%s with %v: %q %v, the macros %q %v", q, arg, got, gerr, want, werr)
		}
	}

	// Columns, whose lists and maps together hold more elements than a
	// vector, some rows NULL.
	valid := make([]string, len(docs))
	for i, d := range docs {
		valid[i] = quote(d)
	}
	tables := []string{
		"CREATE TEMP TABLE flat_docs AS SELECT row_number() OVER () AS i, d AS doc, CAST(d AS JSON) AS js FROM " +
			"(SELECT unnest([" + strings.Join(valid, ", ") + ", NULL]) AS d)",
		"CREATE TEMP TABLE flat_lists AS SELECT i, CASE WHEN i % 5 = 3 THEN NULL ELSE list_transform(range(i * 397 % 3001), " +
			"lambda x: CASE WHEN x % 7 = 0 THEN '{\"n\": ' || x || '}' WHEN x % 11 = 0 THEN NULL WHEN x % 13 = 0 THEN '[' || x || ']' " +
			"ELSE 's' || x END) END AS l FROM range(12) AS r(i)",
		"CREATE TEMP TABLE flat_maps AS SELECT i, CASE WHEN i % 4 = 1 THEN NULL ELSE map_from_entries(list_transform(range(i * 211 % 2500), " +
			"lambda x: {'k': 'k' || x, 'v': CASE WHEN x % 5 = 0 THEN '{\"m\": [' || x || ']}' WHEN x % 9 = 0 THEN NULL ELSE 'v' || x END})) END AS mp " +
			"FROM range(10) AS r(i)",
		"CREATE TEMP TABLE flat_others AS SELECT i, i::INTEGER AS n, {'a': i, 'b': [i, i + 1]} AS st, [i, i * 2] AS il, " +
			"MAP {'x': i} AS im FROM range(5) AS r(i)",
		// Rows over several of DuckDB's vectors, every third one NULL.
		"CREATE TEMP TABLE flat_many AS SELECT i, CASE WHEN i % 3 = 1 THEN NULL ELSE list_transform(range(i % 6), " +
			"lambda x: CASE WHEN x = 2 THEN '{\"i\": ' || i || '}' ELSE 'v' || x END) END AS l, CASE WHEN i % 3 = 2 THEN NULL " +
			"ELSE map_from_entries(list_transform(range(i % 4), lambda x: {'k': 'k' || x, 'v': '[' || i || ']'})) END AS mp " +
			"FROM range(5000) AS r(i)",
	}
	for _, conn := range []*duckdb.Conn{fast, plain} {
		for _, stmt := range tables {
			if _, err := conn.ExecContext(ctx, stmt, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	columns := map[string][]string{"flat_docs": {"doc", "js"}, "flat_lists": {"l"}, "flat_maps": {"mp"}, "flat_others": {"n", "st", "il", "im"},
		"flat_many": {"l", "mp"}}
	for table, cols := range columns {
		for _, col := range cols {
			for _, l := range lists {
				for _, m := range modes[:3] {
					call := "sf_flatten_" + l + "(" + col + ", " + m + ")"
					q := "SELECT i, " + shown(call) + " FROM " + table + " ORDER BY i"
					if got, want := queryRows(t, fast, q, call), queryRows(t, plain, q, call); strings.Join(got, "\n") != strings.Join(want, "\n") {
						t.Errorf("%s over %s: the native lists differ from the macros'", call, table)
					}
				}
			}
			// FLATTEN's lists unnested side by side, as the translator writes them.
			q := "SELECT i, coalesce(hex(CAST(v AS VARCHAR)), '-'), k, x FROM (SELECT i, unnest(sf_flatten_values(" + col + ", 'BOTH')) AS v, " +
				"unnest(sf_flatten_keys(" + col + ", 'BOTH')) AS k, unnest(sf_flatten_indexes(" + col + ", 'BOTH')) AS x, " +
				"unnest(sf_flatten_strings(" + col + ", 'BOTH')) AS s FROM " + table + ") ORDER BY ALL"
			if got, want := queryRows(t, fast, q, table+"."+col), queryRows(t, plain, q, table+"."+col); strings.Join(got, "\n") != strings.Join(want, "\n") {
				t.Errorf("FLATTEN of %s.%s: the native rows differ from the macros'", table, col)
			}
			q = "SELECT i, CAST(sf_flatten_this(" + col + ", 'BOTH') AS VARCHAR) FROM " + table + " ORDER BY i"
			if got, want := queryRows(t, fast, q, table+"."+col), queryRows(t, plain, q, table+"."+col); strings.Join(got, "\n") != strings.Join(want, "\n") {
				t.Errorf("sf_flatten_this of %s.%s: the native rows differ from the macros'", table, col)
			}
		}
	}
	// The struct parts' macros, which call sf_flatten_values on their JSON
	// part, which is not a list where that part is a constant NULL.
	rows := func(conn *duckdb.Conn, q string) (string, error) {
		rs, err := conn.QueryContext(ctx, q, nil)
		if err != nil {
			return "", err
		}
		defer rs.Close() //nolint:errcheck
		dest := make([]driver.Value, len(rs.Columns()))
		var out []string
		for {
			if err := rs.Next(dest); err == io.EOF {
				return strings.Join(out, "\n"), nil
			} else if err != nil {
				return "", err
			}
			vals := make([]string, len(dest))
			for j, v := range dest {
				vals[j] = render(v)
			}
			out = append(out, strings.Join(vals, "\t"))
		}
	}
	for _, parts := range []string{"l, NULL::JSON", `NULL::VARCHAR[], CAST('[1, {"a": 2}, "x"]' AS JSON)`, `NULL::VARCHAR[], CAST('{"a": 2}' AS JSON)`} {
		for _, m := range []string{"values", "strings", "keys", "indexes"} {
			call := "sf_parts_" + m + "(" + parts + ", 'BOTH')"
			for _, q := range []string{
				"SELECT i, " + shown(call) + ", typeof(" + call + ") FROM flat_lists ORDER BY i",
				"SELECT i, CAST(" + call + " AS VARCHAR), typeof(" + call + ") FROM flat_lists ORDER BY i",
			} {
				got, gerr := rows(fast, q)
				want, werr := rows(plain, q)
				if got != want || (gerr == nil) != (werr == nil) || (gerr != nil && gerr.Error() != werr.Error()) {
					t.Errorf("%s: the native lists differ from the macros' (%v, the macros %v)", q, gerr, werr)
				}
			}
		}
	}
}

// magnitude is x, or its absolute value for an unsigned type t.
func magnitude(t, x string) string {
	if strings.HasPrefix(t, "U") {
		return "abs(" + x + ")"
	}
	return x
}

// firstRow returns q's first row, its values rendered and joined by tabs.
func firstRow(ctx context.Context, conn *duckdb.Conn, q string) (string, error) {
	return firstRowArgs(ctx, conn, q, nil)
}

// firstRowArgs is firstRow of q with the parameters args.
func firstRowArgs(ctx context.Context, conn *duckdb.Conn, q string, args []driver.NamedValue) (string, error) {
	rows, err := conn.QueryContext(ctx, q, args)
	if err != nil {
		return "", err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, len(rows.Columns()))
	if err := rows.Next(dest); err != nil {
		return "", err
	}
	vals := make([]string, len(dest))
	for i, v := range dest {
		vals[i] = render(v)
	}
	return strings.Join(vals, "\t"), nil
}

// TestRBFoldsImpossibleComparisons checks that a comparison with what a CASE of
// constants can never be takes the tables behind it out of the plan, a common table
// expression that reads them too, and that one with what it can be does not.
// extensions/rb/test/fold.test checks the rows.
func TestRBFoldsImpossibleComparisons(t *testing.T) {
	e, _ := openWithRB(t)
	ctx := context.Background()
	conn, err := e.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	for _, stmt := range []string{
		"CREATE TEMP TABLE visits AS SELECT range AS id, repeat('u', range % 8) AS url, ['a', 'b'] AS tags FROM range(1000)",
		"CREATE TEMP TABLE members AS SELECT range AS id, 1.0 AS w FROM range(100)",
	} {
		if _, err := conn.ExecContext(ctx, stmt, nil); err != nil {
			t.Fatal(err)
		}
	}
	const kinds = "(SELECT *, CASE WHEN length(url) % 4 IS NOT DISTINCT FROM 0 THEN 'search' WHEN length(url) % 4 IS NOT DISTINCT FROM 3 THEN 'other' ELSE 'page' END AS kind FROM visits) AS v"
	report := func(kind string) string {
		return "WITH weights AS (SELECT m.id, m.w FROM members AS m JOIN visits AS x ON x.id = m.id), " +
			"seen AS (SELECT DISTINCT v.id FROM " + kinds + " WHERE v.id IS NOT NULL AND v.kind = '" + kind + "'), " +
			"total AS (SELECT sum(w.w) AS t FROM seen AS s JOIN weights AS w ON w.id = s.id), " +
			"tags AS (SELECT DISTINCT v.id, u.tag FROM " + kinds + " JOIN weights AS w ON w.id = v.id " +
			"JOIN LATERAL (SELECT unnest(v.tags) AS tag) AS u ON v.id IS NOT NULL AND v.kind = '" + kind + "'), " +
			"byTag AS (SELECT t.tag, sum(w.w) AS n FROM tags AS t JOIN weights AS w ON w.id = t.id GROUP BY t.tag) " +
			"SELECT b.tag, b.n / total.t FROM byTag AS b CROSS JOIN total ORDER BY 2 DESC LIMIT 10"
	}
	explain := func(q string) string {
		plan, err := firstRow(ctx, conn, "EXPLAIN "+q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return plan
	}
	for _, c := range []struct {
		name, query string
		empty       bool
	}{
		{"a filter", "SELECT count(*) FROM " + kinds + " WHERE v.kind = 'click'", true},
		{"a list", "SELECT count(*) FROM " + kinds + " WHERE v.kind IN ('click', 'share')", true},
		{"a report", report("click"), true},
		{"a kind there is", "SELECT count(*) FROM " + kinds + " WHERE v.kind = 'search'", false},
		{"a list with one there is", "SELECT count(*) FROM " + kinds + " WHERE v.kind IN ('click', 'page')", false},
		{"a report of one there is", report("page"), false},
	} {
		plan := explain(c.query)
		if read := strings.Contains(plan, "visits"); read == c.empty {
			t.Errorf("%s: reads the table %v:\n%s", c.name, read, plan)
		}
	}
}

func TestRBFunctions(t *testing.T) {
	e, _ := openWithRB(t)
	files, err := filepath.Glob(filepath.Join("..", "..", "extensions", "rb", "test", "*.test"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no tests found: %v", err)
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			conn, err := e.Connect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close() //nolint:errcheck
			runLogicTest(t, conn, f)
		})
	}
}

// runLogicTest runs a sqllogictest file: records separated by blank lines,
// each "statement ok", "statement error" (the expected message's start or a
// part of it after ----) or "query <types> [rowsort]" (rows after ----,
// tab-separated, NULL for NULL).
func runLogicTest(t *testing.T, conn *duckdb.Conn, path string) {
	fh, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close() //nolint:errcheck
	var lines []string
	sc := bufio.NewScanner(fh)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	ctx := context.Background()
	for i := 0; i < len(lines); {
		line := strings.TrimSpace(lines[i])
		if line == "" || strings.HasPrefix(line, "#") {
			i++
			continue
		}
		start := i + 1
		head := strings.Fields(line)
		i++
		var sqlLines, want []string
		for i < len(lines) && strings.TrimSpace(lines[i]) != "" && lines[i] != "----" {
			sqlLines = append(sqlLines, lines[i])
			i++
		}
		if i < len(lines) && lines[i] == "----" {
			i++
			for i < len(lines) && lines[i] != "" {
				want = append(want, lines[i])
				i++
			}
		}
		q := strings.Join(sqlLines, "\n")
		where := fmt.Sprintf("%s:%d", filepath.Base(path), start)
		switch {
		case head[0] == "statement" && len(head) > 1 && head[1] == "ok":
			if _, err := conn.ExecContext(ctx, q, nil); err != nil {
				t.Fatalf("%s: %v", where, err)
			}
		case head[0] == "statement" && len(head) > 1 && head[1] == "error":
			_, err := conn.ExecContext(ctx, q, nil)
			if err == nil {
				t.Fatalf("%s: ran, want an error", where)
			}
			if len(want) > 0 && !strings.Contains(err.Error(), want[0]) {
				t.Fatalf("%s: error %q does not contain %q", where, err, want[0])
			}
		case head[0] == "query":
			got := queryRows(t, conn, q, where)
			if len(head) > 2 && head[2] == "rowsort" {
				sort.Strings(got)
				sort.Strings(want)
			}
			if strings.Join(got, "\n") != strings.Join(want, "\n") {
				t.Fatalf("%s: got\n%s\nwant\n%s", where, strings.Join(got, "\n"), strings.Join(want, "\n"))
			}
		default:
			t.Fatalf("%s: unknown record %q", where, line)
		}
	}
}

func queryRows(t *testing.T, conn *duckdb.Conn, q, where string) []string {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(), q, nil)
	if err != nil {
		t.Fatalf("%s: %v", where, err)
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, len(rows.Columns()))
	var out []string
	for {
		if err := rows.Next(dest); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("%s: %v", where, err)
		}
		vals := make([]string, len(dest))
		for j, v := range dest {
			vals[j] = render(v)
		}
		out = append(out, strings.Join(vals, "\t"))
	}
	return out
}

func render(v driver.Value) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case []byte:
		return strings.ToUpper(hex.EncodeToString(x))
	default:
		return fmt.Sprint(x)
	}
}

// TestRBSettingsStayInTheirSession checks that the rb extension's
// settings, set with SET SESSION as workers' tasks set a session's settings
// (see the distributed package's TestTaskSettingsStayInTheTask), change
// them for that connection only.
func TestRBSettingsStayInTheirSession(t *testing.T) {
	ctx := context.Background()
	e, _ := openWithRB(t)
	a, err := e.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close() //nolint:errcheck
	b, err := e.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close() //nolint:errcheck
	setting := func(conn *duckdb.Conn, name string) string {
		t.Helper()
		v, err := firstRow(ctx, conn, "SELECT current_setting('"+name+"')::VARCHAR")
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	exec := func(conn *duckdb.Conn, stmt string) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, stmt, nil); err != nil {
			t.Fatal(err)
		}
	}
	for name, value := range map[string]string{"rb_count_distinct": "always", "rb_memory_budget": "123456"} {
		before := setting(b, name)
		exec(a, "SET SESSION "+name+" = '"+value+"'")
		if got := setting(a, name); !strings.EqualFold(got, value) {
			t.Errorf("%s after SET SESSION: %s, want %s", name, got, value)
		}
		if got := setting(b, name); got != before {
			t.Errorf("%s on another connection: %s, want %s as before", name, got, before)
		}
		exec(a, "RESET SESSION "+name)
	}
}
