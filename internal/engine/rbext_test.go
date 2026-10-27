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

// imageLayout writes data where an image keeps the rb extension, under a new
// extension directory, and returns that directory and the file's hash. With
// official, the directory also links to the official extensions the tests
// install (IGLOO_TEST_EXTENSION_DIR), which Open loads from it.
func imageLayout(t *testing.T, data []byte, official bool) (string, string) {
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
	if err := os.WriteFile(filepath.Join(dir, rbFile), data, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return dir, hex.EncodeToString(sum[:])
}

// withHash makes this test's igloo a build with the given rb hash.
func withHash(t *testing.T, sha string) {
	t.Helper()
	old := rbSHA256
	rbSHA256 = sha
	t.Cleanup(func() { rbSHA256 = old })
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
	dir, _ := imageLayout(t, append([]byte("not the extension this igloo was built with"), make([]byte, 512)...), false)
	withHash(t, strings.Repeat("ab", 32))
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
	dir, sha := imageLayout(t, data, false)
	withHash(t, sha)
	_, err = Open(context.Background(), config.Config{DuckDB: config.DuckDB{ExtensionDir: dir}}, quietLog())
	if err == nil || !strings.Contains(err.Error(), "built for DuckDB v1.5.5") {
		t.Fatalf("got %v, want a DuckDB version mismatch", err)
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
	dir, sha := imageLayout(t, data, true)
	withHash(t, sha)
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
	for conn, stmts := range map[*duckdb.Conn][]string{fast: snowflake.MacrosFor(true), plain: snowflake.Macros()} {
		for _, stmt := range stmts {
			if _, err := conn.ExecContext(ctx, stmt, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
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
