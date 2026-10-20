package snowflake

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite testdata/statements.golden")

// TestStatements translates testdata/statements.sql, a statement a line,
// and compares the result with testdata/statements.golden, which shows
// what each construct becomes. Run with -update to rewrite it after a
// change to the translation, and review the diff.
func TestStatements(t *testing.T) {
	in, err := os.ReadFile("testdata/statements.sql")
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for _, line := range strings.Split(strings.TrimSpace(string(in)), "\n") {
		res, err := Translate(line, Options{IsSetting: func(n string) bool { return n == "timezone" || n == "search_path" }})
		b.WriteString("-- " + line + "\n")
		switch {
		case err != nil:
			b.WriteString("ERROR: " + err.Error() + "\n")
		case res.Command != "":
			b.WriteString("COMMAND: " + res.Command + "\n")
		default:
			b.WriteString(res.SQL + "\n")
		}
		for _, n := range res.Notices {
			b.WriteString("NOTICE: " + n + "\n")
		}
		b.WriteString("\n")
	}
	if *update {
		if err := os.WriteFile("testdata/statements.golden", []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile("testdata/statements.golden")
	if err != nil {
		t.Fatal(err)
	}
	if b.String() != string(want) {
		t.Errorf("translations differ from testdata/statements.golden; run go test -run TestStatements -update and review the diff\n%s", b.String())
	}
}

// TestTranslateFile translates the statements of $SF_TRANSLATE_IN (JSON
// lines with "sql") into $SF_TRANSLATE_OUT, for measuring a corpus.
func TestTranslateFile(t *testing.T) {
	in, out := os.Getenv("SF_TRANSLATE_IN"), os.Getenv("SF_TRANSLATE_OUT")
	if in == "" || out == "" {
		t.Skip("SF_TRANSLATE_IN and SF_TRANSLATE_OUT not set")
	}
	f, err := os.Open(in)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	enc := json.NewEncoder(w)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var rec map[string]any
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatal(err)
		}
		sql, _ := rec["sql"].(string)
		res, err := Translate(sql, Options{})
		var e *Error
		switch {
		case errors.As(err, &e):
			rec["error"], rec["code"] = e.Message, e.Code
		case err != nil:
			rec["error"] = err.Error()
		default:
			rec["out"], rec["command"], rec["untranslated"] = res.SQL, res.Command, res.Untranslated
		}
		if err := enc.Encode(rec); err != nil {
			t.Fatal(err)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
}
