package erasure

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
)

// testSubject is a subject for identifiers, normalized as DuckDB does for
// these, with its hashes under salt.
func testSubject(salt, tag, value string, ids ...string) Subject {
	sub := Subject{Tag: tag, Value: value, Count: len(ids)}
	for _, id := range ids {
		sub.ids = append(sub.ids, identifier{Given: id, Normal: (*folding)(nil).normal(id)})
	}
	sub.hash(salt)
	return sub
}

// Rows are found by hashing column values in DuckDB, and the hashes are
// computed in Go: both must agree on every kind of column.
func TestConditionMatchesInDuckDB(t *testing.T) {
	eng := newEngine(t)
	salt := "0123456789abcdef"
	r := &Request{salt: salt, Subjects: []Subject{
		testSubject(salt, "pii", "email", " Ann@Example.com "),
		testSubject(salt, "pii", "device_id", "6F9619FF-8B86-D011-B42D-00C04FC964FF"),
		testSubject(salt, "pii", "phone", "15551234567"),
	}}
	eng.exec(t, `CREATE TABLE people AS SELECT * FROM (VALUES
		(1, 'ann@example.com', '6f9619ff-8b86-d011-b42d-00c04fc964ff'::UUID, 15551234567, sha256('ann@example.com'), unhex(md5('ann@example.com'))),
		(2, 'ANN@EXAMPLE.COM  ', NULL, NULL, NULL, NULL),
		(3, 'bob@example.com', '00000000-0000-0000-0000-000000000000'::UUID, 15550000000, sha256('bob@example.com'), unhex(md5('bob@example.com'))),
		(4, chr(160) || 'ann@example.com' || chr(5760), NULL, NULL, NULL, NULL))
		AS t(id, email, device, phone, email_sha256, email_md5)`)
	for _, c := range []struct {
		column, value, digest string
		binary                bool
		want                  string
	}{
		{"email", "email", "", false, "1 2 4"},
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

// DuckDB trims spaces Go's strings.TrimSpace would not, and lower-cases
// by tables of its own; identifiers are normalized by DuckDB, so values
// compare as its scans compare them.
func TestNormalizedByDuckDB(t *testing.T) {
	ctx := context.Background()
	svc := New(Options{Engine: newEngine(t)})
	for in, want := range map[string]string{
		"\u00a0Ann@Example.COM\u1680": "ann@example.com",
		" bob@example.com\u3000":      "bob@example.com",
		"\u1c89x":                     "\u1c89x",
	} {
		got, err := svc.normalize(ctx, in)
		if err != nil || got != want {
			t.Errorf("normalize(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	// What Go compares, in history, manifest bounds and the rows a rewrite
	// filters, is normalized by DuckDB's own tables: Go's differ for
	// characters Unicode 16 added (U+A7CB, U+1C89, U+10D50), and in what
	// they trim.
	f, err := svc.folding(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"\u0130", "\u212a", "\u1e9e", "\u03a3", "\ua7cb", "\u1c89", "\U00010d50", "\u2c2f", "\u0345", "\u1fbc",
		" x ", "\u3000x", "\u200bx", "\u180ex", "\u0085x", "\tx", "\u00a0x", "\ufeffx", "A\u00a0B", "\u1680Ann@Example.COM\u2003"} {
		want, err := svc.normalize(ctx, s)
		if err != nil {
			t.Fatal(err)
		}
		if got := f.normal(s); got != want {
			t.Errorf("%+q: DuckDB %+q, Go with DuckDB's tables %+q", s, want, got)
		}
	}
}

func TestMatchingFilesByIDAndName(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	eng := newEngine(t)
	salt := "s"
	sub := testSubject(salt, "pii", "email", "ann@example.com")
	withIDs, renamed, withoutIDs, other := dir+"/ids.parquet", dir+"/renamed.parquet", dir+"/names.parquet", dir+"/other.parquet"
	eng.exec(t, "COPY (SELECT 1 AS id, 'ann@example.com' AS email) TO '"+withIDs+"' (FIELD_IDS {id: 1, email: 2})")
	eng.exec(t, "COPY (SELECT 1 AS id, 'Ann@Example.com' AS mail) TO '"+renamed+"' (FIELD_IDS {id: 1, mail: 2})")
	eng.exec(t, "COPY (SELECT 'ann@example.com' AS email UNION ALL SELECT 'ann@example.com') TO '"+withoutIDs+"'")
	eng.exec(t, "COPY (SELECT 'bob@example.com' AS email) TO '"+other+"'")
	svc := New(Options{Engine: eng})
	got, err := svc.matchingFiles(ctx, salt, []string{withIDs, renamed, withoutIDs, other},
		[]scanColumn{{id: 2, names: []string{"email"}, hashes: sub.plain}})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{withIDs: 1, renamed: 1, withoutIDs: 2}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("matching files: %v, want %v", got, want)
	}
	// By name alone, as copies made under another table's UUID are read.
	got, err = svc.matchingFiles(ctx, salt, []string{withIDs, renamed}, []scanColumn{{names: []string{"email"}, hashes: sub.plain}})
	if err != nil || fmt.Sprint(got) != fmt.Sprint(map[string]int64{withIDs: 1}) {
		t.Errorf("matching files by name: %v, %v", got, err)
	}
	n, err := svc.matchingStatistics(ctx, newMatcher(&Request{salt: salt, Subjects: []Subject{sub}}),
		[]string{withIDs, other}, []scanColumn{{id: 2, names: []string{"email"}, hashes: sub.plain}})
	if err != nil || n != 1 {
		t.Errorf("matching statistics: %d, %v", n, err)
	}
}

// Bounds are truncated as Iceberg truncates them: strings to 16
// characters, the upper bound with the last one that can be incremented
// incremented, and binary values, which compare as hex, to 16 bytes.
func TestTruncations(t *testing.T) {
	if got := truncations("short@x.com"); got != nil {
		t.Errorf("short values are not truncated: %q", got)
	}
	a := func(n int) string { return strings.Repeat("a", n) }
	for in, want := range map[string][]string{
		"subject@example.com": {"subject@example.", "subject@example/"},
		// The last character cannot be incremented: the one before is.
		a(15) + "\U0010ffffzz": {a(15) + "\U0010ffff", a(14) + "b"},
		// Past the surrogates.
		a(15) + "\ud7ffz": {a(15) + "\ud7ff", a(15) + "\ue000"},
		// 17 bytes in hex: 16 of them, and those incremented.
		strings.Repeat("ab", 16) + "cd": {strings.Repeat("ab", 16), strings.Repeat("ab", 15) + "ac"},
		// Trailing 0xff bytes cannot be incremented: they go.
		strings.Repeat("00", 15) + "ffff": {strings.Repeat("00", 15) + "ff", strings.Repeat("00", 14) + "01"},
	} {
		got := truncations(in)
		for _, w := range want {
			if !slices.Contains(got, w) {
				t.Errorf("truncations(%q) = %q, want %q among them", in, got, w)
			}
		}
	}
}

// History is redacted by the identifiers themselves, wherever they are in
// it: with characters that split words, such as a MAC address's colons,
// base64's padding, a name's space or a phone number's punctuation, and
// whatever the case, but not inside longer words.
func TestRedact(t *testing.T) {
	salt := "salt"
	r := &Request{salt: salt, Subjects: []Subject{
		testSubject(salt, "pii", "email", "ann@example.com"),
		testSubject(salt, "pii", "mac", "AA:BB:CC:DD:EE:FF"),
		testSubject(salt, "pii", "token", "dGVzdA=="),
		testSubject(salt, "pii", "name", "Ann Lee"),
		testSubject(salt, "pii", "phone", "+1 (555) 123-4567"),
	}}
	x := newRedactor(r)
	for _, c := range []struct{ in, want string }{
		{"SELECT * FROM t WHERE email = 'Ann@Example.com'", "SELECT * FROM t WHERE email = '***'"},
		{"SELECT * FROM t WHERE email IN ('bob@example.com', ' ann@example.com ')", "SELECT * FROM t WHERE email IN ('bob@example.com', ' *** ')"},
		{`SELECT '{"email":"ann@example.com"}'::JSON`, `SELECT '{"email":"***"}'::JSON`},
		{"SELECT 1 -- for ann@example.com", "SELECT 1 -- for ***"},
		{"WHERE mac = 'aa:bb:cc:dd:ee:ff'", "WHERE mac = '***'"},
		{"WHERE token = 'dGVzdA=='", "WHERE token = '***'"},
		{"WHERE name = 'ann lee' OR name = 'ANN LEE'", "WHERE name = '***' OR name = '***'"},
		{"WHERE phone = '+1 (555) 123-4567'", "WHERE phone = '***'"},
		{"SELECT 'annual', 'joann@example.com'", ""},
		{"WHERE email = 'bob@example.com'", ""},
		{"WHERE x = '" + hexDigest("sha256", "ann@example.com") + "'", "WHERE x = '***'"},
	} {
		got, changed := x.Redact(c.in)
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
	// Non-ASCII text keeps its other characters.
	if got, _ := x.Redact("Ünïcødé ANN@EXAMPLE.COM ünd"); got != "Ünïcødé *** ünd" {
		t.Errorf("non-ASCII: %s", got)
	}
	// Errors keep no identifier, nor hash, even as DuckDB truncates one.
	hash := r.Subjects[0].plain[0]
	msg := scrub(r, "Binder Error: sha256('"+salt+"' || x) IN ('"+hash+"') near 'ann@example.com', ... '"+hash[:31]+"...'")
	if strings.Contains(msg, "ann@example.com") || strings.Contains(msg, hash[:16]) {
		t.Errorf("scrubbed error: %s", msg)
	}
}

// History keeps identifiers as SQL quotes them in a string, and as URLs and
// paths escape them: the redactor finds those too. An identifier split
// across a concatenation of strings is not found: nothing short of
// evaluating the SQL could tell.
func TestRedactEscapedIdentifiers(t *testing.T) {
	r := &Request{salt: "s", Subjects: []Subject{testSubject("s", "pii", "email", "O'Brien@Example.com")}}
	x := newRedactor(r)
	for _, c := range []struct{ in, want string }{
		{"SELECT * FROM crm.people WHERE email = 'o''brien@example.com'", "SELECT * FROM crm.people WHERE email = '***'"},
		{`{"email":"o'brien@example.com"}`, `{"email":"***"}`},
		{"COPY people FROM 's3://landing/o%27brien%40example.com/part.parquet'", "COPY people FROM 's3://landing/***/part.parquet'"},
		{"GET /export?email=O%27Brien%40Example.com&x=1", "GET /export?email=***&x=1"},
	} {
		got, changed := x.Redact(c.in)
		if !changed || got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.in, got, c.want)
		}
	}
}

func hexDigest(name, s string) string {
	return fmt.Sprintf("%x", digests[name]([]byte(s)))
}

// A request's record holds its identifiers sealed under the erasure key,
// and neither them nor their hashes in the clear, so the metadata
// database tells nothing without the key.
func TestRecordHoldsNoIdentifiers(t *testing.T) {
	k := newSealer([]byte("0123456789abcdef0123456789abcdef"))
	ids := [][]identifier{{{Given: "Ann@Example.com", Normal: "ann@example.com"}}}
	sealed, err := k.sealIDs("r1", ids)
	if err != nil {
		t.Fatal(err)
	}
	r := &Request{ID: "r1", Salt: "pepper", Sealed: sealed, Status: StatusQueued, Step: StepFind,
		Subjects: []Subject{{Tag: "pii", Value: "email", Count: 1, ids: ids[0]}}}
	r.Subjects[0].hash(k.hashSalt(r.Salt))
	doc, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, leak := range []string{"ann@example", "Ann@Example", r.Subjects[0].plain[0], k.hashSalt(r.Salt)} {
		if bytes.Contains(doc, []byte(leak)) {
			t.Errorf("the record holds %q: %s", leak, doc)
		}
	}
	for _, v := range Row(r) {
		if s, ok := v.(string); ok && (strings.Contains(s, "pepper") || strings.Contains(s, sealed)) {
			t.Errorf("a column shows the salt or the sealed identifiers: %s", s)
		}
	}
	if got := Row(r)[7]; got != `[{"tag":"pii","value":"email","identifiers":1}]` {
		t.Errorf("subjects: %v", got)
	}
	if got, err := k.unsealIDs("r1", sealed); err != nil || got[0][0] != ids[0][0] {
		t.Errorf("unseal: %v, %v", got, err)
	}
	other := newSealer([]byte("another key, another 32 bytes...."))
	if _, err := other.unsealIDs("r1", sealed); err != errKey {
		t.Errorf("unseal with another key: %v", err)
	}
	if _, err := k.unsealIDs("r2", sealed); err != errKey {
		t.Errorf("unseal as another request: %v", err)
	}
	if k.hashSalt("pepper") == other.hashSalt("pepper") {
		t.Errorf("the hashes' salt does not depend on the key")
	}
}

func TestSentinels(t *testing.T) {
	var none *folding
	for _, s := range []string{"00000000-0000-0000-0000-000000000000", "null", "unknown", "0", "n/a", "-", "000", "00:00:00:00:00:00",
		// The second review's: braced zero GUIDs, -1, the all-F ID,
		// placeholders in brackets, NaN, hex and exponent zeros, other
		// scripts' zeros, and spaces and tabs around them.
		"{00000000-0000-0000-0000-000000000000}", "-1", "ffffffff-ffff-ffff-ffff-ffffffffffff", "FFFFFFFFFFFFFFFF",
		"(null)", "<null>", "[null]", "'null'", "nan", "NaN", "0x0", "0x00000000", "0e0", "00000000_0000",
		"\uff10\uff10\uff10\uff10\uff10", " null ", "\tnull", "null\t", "\t null \n", "(not set)", "noemail@noemail.com"} {
		if !sentinel(none.normal(s)) {
			t.Errorf("%q is not refused", s)
		}
	}
	for _, s := range []string{"ann@example.com", "6f9619ff-8b86-d011-b42d-00c04fc964ff", "15551234567", "1", "e", "-2", "0x1f",
		"fffffff", "nancy@example.com", "null@example.com", "{6f9619ff-8b86-d011-b42d-00c04fc964ff}", "10", "1e5"} {
		if sentinel(none.normal(s)) {
			t.Errorf("%q is refused", s)
		}
	}
	// Digests of the empty string and of placeholders stand for no one:
	// hashed tables hold sha256('') for every row without an email.
	for _, s := range []string{"", "null", "NULL", "unknown"} {
		for name := range digests {
			if d := hexDigest(name, s); !sentinelDigest(d) {
				t.Errorf("%s(%q) = %s is not refused", name, s, d)
			}
		}
	}
	if sentinelDigest(hexDigest("sha256", "ann@example.com")) {
		t.Errorf("a person's digest is refused")
	}
}
