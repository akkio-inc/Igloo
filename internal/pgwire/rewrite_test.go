package pgwire

import (
	"slices"
	"strings"
	"testing"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/policy"
)

func TestApplyEdits(t *testing.T) {
	got, err := applyEdits("SELECT a FROM t WHERE b", []edit{{14, 15, "(x) AS t"}, {7, 8, "c"}, {23, 23, " = 1"}})
	if err != nil || got != "SELECT c FROM (x) AS t WHERE b = 1" {
		t.Errorf("applyEdits: %q %v", got, err)
	}
	if _, err := applyEdits("abcdef", []edit{{1, 3, "x"}, {2, 4, "y"}}); err == nil {
		t.Error("overlapping edits applied")
	}
}

func TestScanTableRef(t *testing.T) {
	cases := []struct {
		text  string
		ref   refNode
		alias string
		at    string
		end   string
	}{
		{"SELECT * FROM lake.crm.contacts c WHERE x", refNode{loc: 14, parts: []string{"lake", "crm", "contacts"}, alias: "c"},
			"c", "", " WHERE x"},
		{`SELECT * FROM "Lake" . /* x */ crm."Contacts" AS "C"(a, b) AT (VERSION => 3) TABLESAMPLE 1`,
			refNode{loc: 14, parts: []string{"Lake", "crm", "Contacts"}, alias: "C", columnAliases: []string{"a", "b"}, at: true},
			`"C"(a, b)`, "AT (VERSION => 3)", " TABLESAMPLE 1"},
		{"FROM contacts AT (TIMESTAMP => now())", refNode{loc: 5, parts: []string{"contacts"}, at: true}, "", "AT (TIMESTAMP => now())", ""},
		{"SELECT * FROM 'contacts'", refNode{loc: 14, parts: []string{"contacts"}}, "", "", ""},
	}
	for _, c := range cases {
		sp, err := scanTableRef(c.text, &c.ref)
		if err != nil {
			t.Errorf("%s: %v", c.text, err)
			continue
		}
		if sp.alias != c.alias || sp.at != c.at || c.text[sp.end:] != c.end {
			t.Errorf("%s: alias %q at %q rest %q", c.text, sp.alias, sp.at, c.text[sp.end:])
		}
	}
	// What the parse says must be what the text has.
	if _, err := scanTableRef("SELECT * FROM other", &refNode{loc: 14, parts: []string{"contacts"}}); err == nil {
		t.Error("a name that is not the parse's")
	}
	if _, err := scanTableRef("SELECT * FROM contacts x", &refNode{loc: 14, parts: []string{"contacts"}, alias: "y"}); err == nil {
		t.Error("an alias that is not the parse's")
	}
}

func TestPrevWord(t *testing.T) {
	for text, want := range map[string]string{
		"TABLE /* c */ contacts":     "TABLE",
		"SELECT * FROM contacts":     "FROM",
		"SELECT * FROM a, contacts":  "",
		"SUMMARIZE\n  contacts":      "SUMMARIZE",
		"select 'table' || contacts": "",
	} {
		if got, _ := prevWord(text, strings.LastIndex(text, "contacts")); got != want {
			t.Errorf("prevWord(%q) = %q, want %q", text, got, want)
		}
	}
}

func TestParseDML(t *testing.T) {
	q := "UPDATE lake.crm.contacts AS c SET a = c.b + 1, d = (SELECT 1) FROM s WHERE c.id = s.id RETURNING a"
	d, ok := parseDML(duckdb.STATEMENT_TYPE_UPDATE, q)
	text := func(s span) string { return q[s.start:s.end] }
	if !ok || text(d.target) != "lake.crm.contacts" || d.alias != "c" || text(d.list) != "s" || text(d.cond) != "c.id = s.id" ||
		text(d.returning) != "a" || len(d.exprs) != 2 || text(d.exprs[0]) != "c.b + 1" || text(d.exprs[1]) != "(SELECT 1)" {
		t.Errorf("UPDATE: %+v %v", d, ok)
	}

	q = "delete from t x using s where x.id = s.id;"
	d, ok = parseDML(duckdb.STATEMENT_TYPE_DELETE, q)
	if !ok || text(d.target) != "t" || d.alias != "x" || text(d.list) != "s" || text(d.cond) != "x.id = s.id" || !d.where {
		t.Errorf("DELETE: %+v %v", d, ok)
	}
	q = "DELETE FROM t"
	if d, ok = parseDML(duckdb.STATEMENT_TYPE_DELETE, q); !ok || d.where || d.insertAt != len(q) {
		t.Errorf("DELETE without WHERE: %+v %v", d, ok)
	}

	q = `MERGE INTO t USING (SELECT * FROM s) s ON t.id = s.id AND CASE WHEN s.x THEN true END
		WHEN MATCHED AND t.v > CASE WHEN s.y THEN 1 ELSE 2 END THEN UPDATE SET v = s.v, w = 1
		WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v)
		WHEN NOT MATCHED BY SOURCE AND t.v < 0 THEN DELETE`
	d, ok = parseDML(statementTypeMergeInto, q)
	exprs := make([]string, len(d.exprs))
	for i, e := range d.exprs {
		exprs[i] = text(e)
	}
	srcs := make([]string, len(d.sourceExprs))
	for i, e := range d.sourceExprs {
		srcs[i] = text(e)
	}
	if !ok || text(d.source) != "(SELECT * FROM s) s" || text(d.cond) != "t.id = s.id AND CASE WHEN s.x THEN true END" ||
		!slices.Equal(exprs, []string{"t.v > CASE WHEN s.y THEN 1 ELSE 2 END", "s.v", "1", "t.v < 0"}) ||
		!slices.Equal(srcs, []string{"s.id", "s.v"}) || !d.bySource || !d.notMatched {
		t.Errorf("MERGE: %+v %q %q %v", d, exprs, srcs, ok)
	}

	for _, bad := range []string{
		"WITH x AS (SELECT 1) DELETE FROM t",
		"UPDATE t",
		"MERGE INTO t USING s ON a WHEN MATCHED THEN INSERT VALUES (1)",
	} {
		ty := duckdb.STATEMENT_TYPE_DELETE
		if strings.HasPrefix(bad, "UPDATE") {
			ty = duckdb.STATEMENT_TYPE_UPDATE
		} else if strings.HasPrefix(bad, "MERGE") {
			ty = statementTypeMergeInto
		}
		if _, ok := parseDML(ty, bad); ok {
			t.Errorf("parsed %q", bad)
		}
	}
}

func TestComposer(t *testing.T) {
	src := "UPDATE t SET a = 1 WHERE b"
	c := &composer{}
	c.lit("SELECT ")
	c.orig(src, 17, 18)
	c.lit(" FROM ")
	c.orig(src, 7, 8)
	if c.String() != "SELECT 1 FROM t" {
		t.Fatalf("composed %q", c.String())
	}
	if at, ok := c.toOrig(14); !ok || at != 7 {
		t.Errorf("toOrig(14) = %d %v", at, ok)
	}
	if _, ok := c.toOrig(2); ok {
		t.Error("a position of the composer's own text maps back")
	}
}

func TestViewParts(t *testing.T) {
	cols, body, ok := viewParts(`CREATE VIEW "public"."v"(a, "B") AS SELECT 1, 2;`)
	if !ok || !slices.Equal(cols, []string{"a", "B"}) || body != "SELECT 1, 2" {
		t.Errorf("viewParts: %q %q %v", cols, body, ok)
	}
	if cols, body, ok := viewParts("CREATE TEMP VIEW v AS WITH x AS (SELECT 1) SELECT * FROM x"); !ok || cols != nil ||
		body != "WITH x AS (SELECT 1) SELECT * FROM x" {
		t.Errorf("temp view: %q %q %v", cols, body, ok)
	}
}

func TestConstantExpr(t *testing.T) {
	for text, want := range map[string]bool{
		"0": true, "-1.5": true, "'x'": true, "NULL": true, "(true)": true,
		"a": false, "a + 1": false, "$1": false, "?": false, "now()": false, `"x"`: false, "(SELECT 1)": false,
	} {
		if got := constantExpr(text); got != want {
			t.Errorf("constantExpr(%q) = %v", text, got)
		}
	}
}

func TestPathLike(t *testing.T) {
	for name, want := range map[string]bool{
		"s3://bucket/x": true, "data.parquet": true, "x.CSV.gz": true, "a/b/c.json.zst": true,
		"contacts": false, "lake.crm.contacts": false, "2026-10-01": false, "a@b.com": false,
	} {
		if got := pathLike(name); got != want {
			t.Errorf("pathLike(%q) = %v", name, got)
		}
	}
}

func TestTextValues(t *testing.T) {
	got := literalsOf(`SELECT 'it''s', E'cont\x61cts', e'\143\n', $$a'b$$, $q$x$q$, "Quoted ""x""" FROM t`)
	want := []string{"it's", "contacts", "c\n", "a'b", "x", `Quoted "x"`}
	if !slices.Equal(got, want) {
		t.Errorf("literalsOf: %q, want %q", got, want)
	}
	for _, q := range []string{`SELECT * FROM lake.crm.U&"\0063ontacts"`, `SELECT * FROM u&'x'`} {
		if !tokensOf(q).opaque {
			t.Errorf("%s: not opaque", q)
		}
	}
	if tokensOf(`SELECT "U&"`).opaque || tokensOf(`SELECT xu&'x'`).opaque {
		t.Error("opaque names")
	}
	if tokensOf(`SELECT * FROM E'x'`).escaped != true || tokensOf(`SELECT 'E'`).escaped {
		t.Error("escaped strings")
	}
	if !tokensOf("SELECT * FROM 'lake.crm.Contacts'").mentions(map[string]bool{"contacts": true}) {
		t.Error("a table named in a string")
	}
	if !tokensOf("SELECT * FROM crm.contacts__igloo_new_0a1b2c3d").mentions(map[string]bool{"contacts": true}) {
		t.Error("the staging table of CREATE OR REPLACE TABLE")
	}
	// A U& token names something only DuckDB can decode, so any statement
	// with one is analyzed, even when its plain words name nothing
	// protected: it must not run raw if DuckDB ever decodes U& to a
	// protected table.
	sens := &sensitivity{names: map[string]bool{"contacts": true}}
	for _, q := range []string{`SELECT * FROM lake.crm.U&"\0063ontacts"`, `SELECT email FROM U&'x'`} {
		if !sens.triggers(q, false) || !sens.triggers(q, true) {
			t.Errorf("%s: does not trigger analysis", q)
		}
	}
	if sens.triggers(`SELECT 1`, false) {
		t.Error("a plain statement triggers analysis")
	}
}

func TestCheckPath(t *testing.T) {
	s := &Server{gov: &governance{allowed: []string{"s3://landing/", "/data/"}, warehouse: "/data/warehouse/"}}
	// With a table under a policy (allowWarehouse false), the warehouse is
	// off limits; the rest must still be plain, in-prefix paths.
	for path, ok := range map[string]bool{
		"s3://landing/in.parquet":              true,
		"s3://landing/day=*/part-*.parquet":    true,
		"s3://landing/sub/":                    true, // a directory's own trailing slash
		"/data/landing/x.csv":                  true,
		"/data/warehouse/t/data/x.parquet":     false, // the warehouse
		"/data/*/x.parquet":                    false, // glob covering the warehouse
		"/data/landing/../warehouse/x":         false, // climbs with ..
		"s3://other/x.parquet":                 false, // not an allowed location
		"s3://landing/../other/x":              false, // climbs out of the prefix
		"s3://landing/%2e%2e/x":                false, // percent-encoded ..
		"s3://landing/%2f/x":                   false, // percent-encoded /
		"s3://landing/%252e%252e/x":            false, // double-encoded ..
		"s3://landing/a//b.parquet":            false, // empty interior segment
		`s3://landing/a\b.parquet`:             false, // backslash
		"s3://landing/x.parquet?s3_endpoint=h": false, // query redirect
		"s3://landing/x.parquet#f":             false, // fragment
		"s3://landing/./x":                     false, // dot segment
		"x.parquet":                            false, // not in any location
		"":                                     false,
	} {
		if err := s.checkPath(path, false); (err == nil) != ok {
			t.Errorf("checkPath(%q, false): %v", path, err)
		}
	}
	// Without any policy (allowWarehouse true), readers and writers may read
	// the warehouse's own files, but the climb-out rules still hold.
	for path, ok := range map[string]bool{
		"/data/warehouse/t/data/x.parquet": true,
		"/data/warehouse/../other/x":       false,
		"/data/landing/x.csv":              true,
		"s3://other/x.parquet":             false,
	} {
		if err := s.checkPath(path, true); (err == nil) != ok {
			t.Errorf("checkPath(%q, true): %v", path, err)
		}
	}
}

func TestPolicyRules(t *testing.T) {
	for q, ok := range map[string]bool{
		"PRAGMA table_info('t')":        true,
		"PRAGMA storage_info('t')":      false,
		"PRAGMA tpch(1)":                false,
		"CALL pragma_table_info('t')":   true,
		"CALL pragma_storage_info('t')": false,
	} {
		ty := duckdb.STATEMENT_TYPE_PRAGMA
		if strings.HasPrefix(q, "CALL") {
			ty = duckdb.STATEMENT_TYPE_CALL
		}
		if err := policyRules(ty, q); (err == nil) != ok {
			t.Errorf("%s: %v", q, err)
		}
	}
}

func TestParsePolicyCommand(t *testing.T) {
	c, ok, err := parsePolicyCommand(`CREATE OR REPLACE MASKING POLICY mask_email AS (val VARCHAR) RETURNS VARCHAR ->
		CASE WHEN is_role_in_session('x') THEN val ELSE '***' END COMMENT = 'emails';`)
	if !ok || err != nil || c.kind != pcCreatePolicy || !c.replace || c.policy.Name != "mask_email" ||
		!slices.Equal(c.policy.Args, []policy.Arg{{Name: "val", Type: "VARCHAR"}}) || c.policy.Returns != "VARCHAR" ||
		c.policy.Body != "CASE WHEN is_role_in_session('x') THEN val ELSE '***' END" || c.policy.Comment != "emails" {
		t.Errorf("CREATE MASKING POLICY: %+v %v %v", c, ok, err)
	}
	c, ok, err = parsePolicyCommand("create row access policy if not exists r as (a varchar, b decimal(10, 2)) returns boolean -> a = 'x' and b > 1")
	if !ok || err != nil || c.policy.Kind != policy.RowAccess || !c.ifNotExists || len(c.policy.Args) != 2 ||
		c.policy.Args[1].Type != "decimal(10, 2)" || c.policy.Body != "a = 'x' and b > 1" {
		t.Errorf("CREATE ROW ACCESS POLICY: %+v %v %v", c, ok, err)
	}
	c, ok, err = parsePolicyCommand(`ALTER TABLE IF EXISTS lake.crm."Contacts" MODIFY COLUMN "E-mail" SET MASKING POLICY m FORCE`)
	if !ok || err != nil || c.kind != pcSetMask || !c.ifExists || !slices.Equal(c.table, []string{"lake", "crm", "Contacts"}) ||
		c.column != "E-mail" || c.name != "m" || !c.force {
		t.Errorf("SET MASKING POLICY: %+v %v %v", c, ok, err)
	}
	c, ok, err = parsePolicyCommand("ALTER TABLE t ALTER c SET TAG pii = 'email', owner = 'x'")
	if !ok || err != nil || c.kind != pcSetColumnTag || len(c.tags) != 2 || c.tags[1].Tag != "owner" || c.tags[1].Value != "x" {
		t.Errorf("SET TAG: %+v %v %v", c, ok, err)
	}
	c, ok, err = parsePolicyCommand("ALTER TABLE t ADD ROW ACCESS POLICY r ON (client_id, region)")
	if !ok || err != nil || c.kind != pcAddRowPolicy || !slices.Equal(c.columns, []string{"client_id", "region"}) {
		t.Errorf("ADD ROW ACCESS POLICY: %+v %v %v", c, ok, err)
	}
	c, ok, err = parsePolicyCommand("ALTER TAG pii SET MASKING POLICY a, MASKING POLICY b")
	if !ok || err != nil || c.kind != pcSetTagPolicy || !slices.Equal(c.policies, []string{"a", "b"}) {
		t.Errorf("ALTER TAG: %+v %v %v", c, ok, err)
	}
	c, ok, err = parsePolicyCommand("CREATE TAG pii ALLOWED_VALUES 'email', 'ip' COMMENT = 'x'")
	if !ok || err != nil || c.kind != pcCreateTag || !slices.Equal(c.tagDef.AllowedValues, []string{"email", "ip"}) || c.tagDef.Comment != "x" {
		t.Errorf("CREATE TAG: %+v %v %v", c, ok, err)
	}
	for _, q := range []string{"SHOW POLICIES", "show masking policies", "SHOW ROW ACCESS POLICIES", "SHOW TAGS",
		"DROP MASKING POLICY IF EXISTS m", "DROP TAG t", "ALTER TABLE t DROP ALL ROW ACCESS POLICIES",
		"ALTER TABLE t ALTER COLUMN c UNSET MASKING POLICY", "ALTER TABLE t ALTER COLUMN c UNSET TAG a, b"} {
		if _, ok, err := parsePolicyCommand(q); !ok || err != nil {
			t.Errorf("%s: %v %v", q, ok, err)
		}
	}
	// DuckDB's own statements are left to it.
	for _, q := range []string{"ALTER TABLE t ADD COLUMN c INTEGER", "ALTER TABLE t ALTER COLUMN c SET DEFAULT 1",
		"ALTER TABLE t RENAME TO u", "CREATE TABLE tag (x INTEGER)", "DROP TABLE tags", "SHOW TABLES"} {
		if _, ok, _ := parsePolicyCommand(q); ok {
			t.Errorf("%s was taken as a policy statement", q)
		}
	}
	for _, q := range []string{
		"CREATE MASKING POLICY m AS (v VARCHAR) RETURNS VARCHAR",
		`CREATE MASKING POLICY "M" AS (v VARCHAR) RETURNS VARCHAR -> v`,
		"CREATE MASKING POLICY m AS v VARCHAR RETURNS VARCHAR -> v",
		"CREATE OR REPLACE MASKING POLICY IF NOT EXISTS m AS (v VARCHAR) RETURNS VARCHAR -> v",
		"ALTER TABLE t ALTER c SET TAG pii = email",
		"ALTER TABLE t ADD ROW ACCESS POLICY r",
		"ALTER TABLE t ALTER c SET MASKING POLICY m USING (c, d)",
	} {
		if _, ok, err := parsePolicyCommand(q); !ok || err == nil {
			t.Errorf("%s: want an error, got %v %v", q, ok, err)
		}
	}
}
