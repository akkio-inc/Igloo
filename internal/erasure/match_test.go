package erasure

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
)

// Rows are found by hashing column values in DuckDB, and the hashes are
// computed in Go: both must agree on every kind of column.
func TestConditionMatchesInDuckDB(t *testing.T) {
	eng := newEngine(t)
	salt := "0123456789abcdef"
	r := &Request{Salt: salt, Subjects: []Subject{
		newSubject(salt, "pii", "email", []string{" Ann@Example.com "}),
		newSubject(salt, "pii", "device_id", []string{"6F9619FF-8B86-D011-B42D-00C04FC964FF"}),
		newSubject(salt, "pii", "phone", []string{"15551234567"}),
	}}
	eng.exec(t, `CREATE TABLE people AS SELECT * FROM (VALUES
		(1, 'ann@example.com', '6f9619ff-8b86-d011-b42d-00c04fc964ff'::UUID, 15551234567, sha256('ann@example.com'), unhex(md5('ann@example.com'))),
		(2, 'ANN@EXAMPLE.COM  ', NULL, NULL, NULL, NULL),
		(3, 'bob@example.com', '00000000-0000-0000-0000-000000000000'::UUID, 15550000000, sha256('bob@example.com'), unhex(md5('bob@example.com'))))
		AS t(id, email, device, phone, email_sha256, email_md5)`)
	for _, c := range []struct {
		column, value, digest string
		binary                bool
		want                  string
	}{
		{"email", "email", "", false, "1 2"},
		{"device", "device_id", "", false, "1"},
		{"phone", "phone", "", false, "1"},
		{"email_sha256", "email", "sha256", false, "1"},
		{"email_md5", "email", "md5", true, "1"},
	} {
		i := slices.IndexFunc(r.Subjects, func(s Subject) bool { return s.Value == c.value })
		cond := condition(salt, []scanColumn{{binary: c.binary, hashes: r.Subjects[i].hashes(c.digest)}}, []string{quoteIdent(c.column)})
		got := strings.Join(eng.values(t, "SELECT id FROM people WHERE "+cond+" ORDER BY id"), " ")
		if got != c.want {
			t.Errorf("%s: rows %q, want %q", c.column, got, c.want)
		}
	}
}

func TestMatchingFilesByIDAndName(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	eng := newEngine(t)
	salt := "s"
	sub := newSubject(salt, "pii", "email", []string{"ann@example.com"})
	withIDs, renamed, withoutIDs, other := dir+"/ids.parquet", dir+"/renamed.parquet", dir+"/names.parquet", dir+"/other.parquet"
	eng.exec(t, "COPY (SELECT 1 AS id, 'ann@example.com' AS email) TO '"+withIDs+"' (FIELD_IDS {id: 1, email: 2})")
	eng.exec(t, "COPY (SELECT 1 AS id, 'Ann@Example.com' AS mail) TO '"+renamed+"' (FIELD_IDS {id: 1, mail: 2})")
	eng.exec(t, "COPY (SELECT 'ann@example.com' AS email UNION ALL SELECT 'ann@example.com') TO '"+withoutIDs+"'")
	eng.exec(t, "COPY (SELECT 'bob@example.com' AS email) TO '"+other+"'")
	svc := New(Options{Engine: eng})
	got, err := svc.matchingFiles(ctx, salt, []string{withIDs, renamed, withoutIDs, other},
		[]scanColumn{{id: 2, names: []string{"email"}, hashes: sub.Plain}})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{withIDs: 1, renamed: 1, withoutIDs: 2}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("matching files: %v, want %v", got, want)
	}
	n, err := svc.matchingStatistics(ctx, newMatcher(&Request{Salt: salt, Subjects: []Subject{sub}}),
		[]string{withIDs, other}, []scanColumn{{id: 2, names: []string{"email"}, hashes: sub.Plain}})
	if err != nil || n != 1 {
		t.Errorf("matching statistics: %d, %v", n, err)
	}
}

func TestTruncations(t *testing.T) {
	if got := truncations("short@x.com"); got != nil {
		t.Errorf("short values are not truncated: %q", got)
	}
	got := truncations("subject@example.com")
	if len(got) != 2 || got[0] != "subject@example." || got[1] != "subject@example/" {
		t.Errorf("truncations: %q", got)
	}
}

func TestRedact(t *testing.T) {
	r := &Request{Salt: "salt", Subjects: []Subject{
		newSubject("salt", "pii", "email", []string{"ann@example.com"}),
		newSubject("salt", "pii", "phone", []string{"15551234567"}),
	}}
	m := newMatcher(r)
	for _, c := range []struct{ in, want string }{
		{"SELECT * FROM t WHERE email = 'Ann@Example.com'", "SELECT * FROM t WHERE email = '***'"},
		{"SELECT * FROM t WHERE email IN ('bob@example.com', ' ann@example.com ')", "SELECT * FROM t WHERE email IN ('bob@example.com', '***')"},
		{"SELECT * FROM t WHERE email LIKE '%ann@example.com%'", "SELECT * FROM t WHERE email LIKE '***'"},
		{`SELECT '{"email": "ann@example.com"}'::JSON`, `SELECT '***'::JSON`},
		{"SELECT $$ann@example.com$$", "SELECT '***'"},
		{"SELECT 1 -- for ann@example.com", "SELECT 1 -- for ***"},
		{`SELECT "ann@example.com" FROM t`, `SELECT "***" FROM t`},
		{"SELECT * FROM t WHERE phone = 15551234567", "SELECT * FROM t WHERE phone = ***"},
		{"SELECT * FROM t WHERE email = 'bob@example.com'", ""},
		{"SELECT 'ann@example.com.au'", ""},
	} {
		got, changed := m.RedactSQL(c.in)
		if c.want == "" {
			if changed {
				t.Errorf("%s: redacted to %s", c.in, got)
			}
			continue
		}
		if !changed || got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.in, got, c.want)
		}
	}
	got, changed := m.RedactText(`Could not convert string "ANN@EXAMPLE.COM" to INT32 in line 3`)
	if !changed || got != `Could not convert string "***" to INT32 in line 3` {
		t.Errorf("text: %s", got)
	}
	// Words the store finds candidates by are the words redaction checks.
	delims, salt, hashes := m.Prefilter()
	if delims != delimiters || salt != "salt" || len(hashes) != len(m.exact) {
		t.Errorf("prefilter: %q %q %d", delims, salt, len(hashes))
	}
}

func TestRowHidesHashes(t *testing.T) {
	r := &Request{ID: "r1", Salt: "salt", Status: StatusQueued, Step: StepFind,
		Subjects: []Subject{newSubject("salt", "pii", "email", []string{"ann@example.com"})}}
	for _, v := range Row(r) {
		if s, ok := v.(string); ok && (strings.Contains(s, "salt") || strings.Contains(s, r.Subjects[0].Plain[0])) {
			t.Errorf("a column shows a hash or the salt: %s", s)
		}
	}
	if got := Row(r)[7]; got != `[{"tag":"pii","value":"email","identifiers":1}]` {
		t.Errorf("subjects: %v", got)
	}
}
