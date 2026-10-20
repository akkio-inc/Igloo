package federation

import (
	"strings"
	"testing"
)

// testCatalog is a Snowflake database with one table of every kind of
// column, as SHOW COLUMNS would describe it.
func testCatalog(t *testing.T) *sfCatalog {
	t.Helper()
	cols := []struct{ name, dataType string }{
		{"ID", `{"type":"FIXED","precision":38,"scale":0,"nullable":false}`},
		{"AMT", `{"type":"FIXED","precision":12,"scale":2,"nullable":true}`},
		{"NAME", `{"type":"TEXT","length":100,"nullable":true}`},
		{"F", `{"type":"REAL","nullable":true}`},
		{"FLAG", `{"type":"BOOLEAN","nullable":true}`},
		{"D", `{"type":"DATE","nullable":true}`},
		{"TM", `{"type":"TIME","scale":9,"nullable":true}`},
		{"TS", `{"type":"TIMESTAMP_NTZ","scale":9,"nullable":true}`},
		{"TS3", `{"type":"TIMESTAMP_NTZ","scale":3,"nullable":true}`},
		{"LTZ", `{"type":"TIMESTAMP_LTZ","scale":9,"nullable":true}`},
		{"TZ", `{"type":"TIMESTAMP_TZ","scale":9,"nullable":true}`},
		{"V", `{"type":"VARIANT","nullable":true}`},
		{"B", `{"type":"BINARY","length":8,"nullable":true}`},
		{"G", `{"type":"GEOGRAPHY","nullable":true}`},
		{"Mixed Case", `{"type":"TEXT","length":10,"nullable":true}`},
	}
	table := &sfTable{name: "T1"}
	var names []string
	for _, c := range cols {
		typ, notNull, err := parseSnowflakeType(c.dataType)
		if err != nil {
			t.Fatal(err)
		}
		table.columns = append(table.columns, &sfColumn{name: c.name, typ: typ, notNull: notNull})
		names = append(names, c.name)
	}
	for i, n := range pgNames(names, true, nil) {
		table.columns[i].pgName = n
	}
	table.pgName = "t1"
	return &sfCatalog{database: "DB", schemas: []*sfSchema{{name: "TPCH", pgName: "tpch", oid: 16384, tables: []*sfTable{table}}}}
}

func TestParseScan(t *testing.T) {
	q, err := parseScan(`COPY (SELECT "AMT", "NAME", "ID" FROM "TPCH"."T1" WHERE ctid BETWEEN '(0,0)'::tid AND '(4294967295,0)'::tid ` +
		`AND "AMT" > '5.00' AND ("NAME" = 'a''b\c' COLLATE "C" OR "NAME" IS NULL) AND "ID" IN ('1', '2')) TO STDOUT (FORMAT "binary");`)
	if err != nil {
		t.Fatal(err)
	}
	if q.schema != "TPCH" || q.table != "T1" || strings.Join(q.columns, ",") != "AMT,NAME,ID" ||
		!q.ctid || q.ctidFrom != 0 || q.ctidTo != fullCTID || q.limit != -1 || q.offset != -1 {
		t.Fatalf("%+v", q)
	}
	f := q.filter
	if f.op != "and" || len(f.children) != 3 || f.children[0].cmp != ">" || f.children[0].values[0].text != "5.00" ||
		f.children[1].op != "or" || f.children[1].children[0].values[0].text != `a'b\c` ||
		f.children[1].children[1].op != "null" || f.children[2].op != "in" || len(f.children[2].values) != 2 {
		t.Fatalf("filter: %+v", f)
	}

	q, err = parseScan(`COPY (SELECT NULL FROM "TPCH"."T1"  LIMIT 1 OFFSET 2) TO STDOUT (FORMAT "binary");`)
	if err != nil || q.columns != nil || q.limit != 1 || q.offset != 2 || q.filter != nil || q.ctid {
		t.Fatalf("%+v %v", q, err)
	}
	q, err = parseScan(`COPY (SELECT "B" FROM "TPCH"."T1" WHERE "B" > '\x00'::BYTEA) TO STDOUT (FORMAT "binary")`)
	if err != nil || !q.filter.values[0].bytea {
		t.Fatalf("%+v %v", q, err)
	}

	// Anything else is refused: other statements, other functions, other
	// casts, more statements, other formats.
	for _, bad := range []string{
		`SELECT 1`,
		`COPY (SELECT "A" FROM "S"."T") TO STDOUT (FORMAT "text");`,
		`COPY (SELECT "A" FROM "S"."T"; DROP TABLE x) TO STDOUT (FORMAT "binary");`,
		`COPY (SELECT "A" FROM "S"."T") TO STDOUT (FORMAT "binary"); SELECT 1`,
		`COPY (SELECT upper("A") FROM "S"."T") TO STDOUT (FORMAT "binary");`,
		`COPY (SELECT "A" FROM "S"."T" WHERE "A" = '1'::INT) TO STDOUT (FORMAT "binary");`,
		`COPY (SELECT "A" FROM "S"."T" WHERE "A" = 1) TO STDOUT (FORMAT "binary");`,
		`COPY (SELECT "A" FROM "S"."T" WHERE "A" LIKE 'x%') TO STDOUT (FORMAT "binary");`,
		`COPY (SELECT "A" FROM "S"."T" WHERE "A" = 'x' COLLATE "en_US") TO STDOUT (FORMAT "binary");`,
		`COPY (SELECT "A" FROM "S"."T" WHERE ("A" = 'x' OR ctid BETWEEN '(0,0)'::tid AND '(1,0)'::tid)) TO STDOUT (FORMAT "binary");`,
		`COPY (SELECT "A" FROM T) TO STDOUT (FORMAT "binary");`,
		`COPY (SELECT "A" FROM "S"."T" WHERE "A" = 'unterminated) TO STDOUT (FORMAT "binary");`,
	} {
		if _, err := parseScan(bad); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
}

func TestPlanScan(t *testing.T) {
	cat := testCatalog(t)
	for _, tc := range []struct{ scan, want string }{
		{`COPY (SELECT "id", "amt" FROM "tpch"."t1" WHERE ctid BETWEEN '(0,0)'::tid AND '(4294967295,0)'::tid AND "amt" >= '1.50') TO STDOUT (FORMAT "binary");`,
			`SELECT "ID", "AMT" FROM "DB"."TPCH"."T1" WHERE "AMT" >= 1.50`},
		// Snowflake reads a backslash in a string as an escape.
		{`COPY (SELECT "name" FROM "tpch"."t1" WHERE "name" = 'it''s a\b' COLLATE "C") TO STDOUT (FORMAT "binary");`,
			`SELECT "NAME" FROM "DB"."TPCH"."T1" WHERE "NAME" = 'it''s a\\b'`},
		{`COPY (SELECT "Mixed Case" FROM "tpch"."t1" WHERE "Mixed Case" IN ('a', 'b')) TO STDOUT (FORMAT "binary");`,
			`SELECT "Mixed Case" FROM "DB"."TPCH"."T1" WHERE ("Mixed Case" = 'a' OR "Mixed Case" = 'b')`},
		{`COPY (SELECT "f", "flag" FROM "tpch"."t1" WHERE ("f" > 'inf' OR "f" < '1e+300') AND "flag" = 'true') TO STDOUT (FORMAT "binary");`,
			`SELECT "F", "FLAG" FROM "DB"."TPCH"."T1" WHERE ("F" > CAST('inf' AS FLOAT) OR "F" < CAST('1e+300' AS FLOAT)) AND "FLAG" = TRUE`},
		{`COPY (SELECT "d" FROM "tpch"."t1" WHERE "d" >= '2020-01-01' AND "d" < 'infinity') TO STDOUT (FORMAT "binary");`,
			`SELECT "D" FROM "DB"."TPCH"."T1" WHERE "D" >= TO_DATE('2020-01-01', 'YYYY-MM-DD') AND "D" IS NOT NULL`},
		// Nanoseconds DuckDB does not see: equal to the microsecond.
		{`COPY (SELECT "ts" FROM "tpch"."t1" WHERE "ts" = '2020-01-02 03:04:05.123456') TO STDOUT (FORMAT "binary");`,
			`SELECT "TS" FROM "DB"."TPCH"."T1" WHERE ("TS" >= TO_TIMESTAMP_NTZ('2020-01-02 03:04:05.123456', 'YYYY-MM-DD HH24:MI:SS.FF6') AND "TS" < TO_TIMESTAMP_NTZ('2020-01-02 03:04:05.123457', 'YYYY-MM-DD HH24:MI:SS.FF6'))`},
		{`COPY (SELECT "ts" FROM "tpch"."t1" WHERE "ts" > '2020-01-02 03:04:05') TO STDOUT (FORMAT "binary");`,
			`SELECT "TS" FROM "DB"."TPCH"."T1" WHERE "TS" >= TO_TIMESTAMP_NTZ('2020-01-02 03:04:05.000001', 'YYYY-MM-DD HH24:MI:SS.FF6')`},
		{`COPY (SELECT "ts3" FROM "tpch"."t1" WHERE "ts3" <= '2020-01-02 03:04:05.5') TO STDOUT (FORMAT "binary");`,
			`SELECT "TS3" FROM "DB"."TPCH"."T1" WHERE "TS3" <= TO_TIMESTAMP_NTZ('2020-01-02 03:04:05.500000', 'YYYY-MM-DD HH24:MI:SS.FF6')`},
		{`COPY (SELECT "tz" FROM "tpch"."t1" WHERE "tz" < '2020-01-02 01:04:05.5+05:30') TO STDOUT (FORMAT "binary");`,
			`SELECT "TZ" FROM "DB"."TPCH"."T1" WHERE "TZ" < TO_TIMESTAMP_TZ('2020-01-01 19:34:05.500000 +0000', 'YYYY-MM-DD HH24:MI:SS.FF6 TZHTZM')`},
		{`COPY (SELECT "ltz" FROM "tpch"."t1" WHERE "ltz" <> '2020-01-02 01:04:05-00:30') TO STDOUT (FORMAT "binary");`,
			`SELECT "LTZ" FROM "DB"."TPCH"."T1" WHERE ("LTZ" < TO_TIMESTAMP_LTZ('2020-01-02 01:34:05.000000 +0000', 'YYYY-MM-DD HH24:MI:SS.FF6 TZHTZM') OR "LTZ" >= TO_TIMESTAMP_LTZ('2020-01-02 01:34:05.000001 +0000', 'YYYY-MM-DD HH24:MI:SS.FF6 TZHTZM'))`},
		{`COPY (SELECT "tm" FROM "tpch"."t1" WHERE "tm" > '00:00:01') TO STDOUT (FORMAT "binary");`,
			`SELECT CAST("TM" AS TIME(9)) FROM "DB"."TPCH"."T1" WHERE "TM" >= TO_TIME('00:00:01.000001', 'HH24:MI:SS.FF6')`},
		{`COPY (SELECT "b" FROM "tpch"."t1" WHERE "b" = '\x0aff'::BYTEA) TO STDOUT (FORMAT "binary");`,
			`SELECT "B" FROM "DB"."TPCH"."T1" WHERE "B" = TO_BINARY('0AFF', 'HEX')`},
		// DuckDB compares the JSON text it reads.
		{`COPY (SELECT "v", "g" FROM "tpch"."t1" WHERE "v" = '{"a":1}' COLLATE "C" AND "v" IS NOT NULL) TO STDOUT (FORMAT "binary");`,
			`SELECT TO_JSON(CAST("V" AS VARIANT)), ST_ASWKT("G") FROM "DB"."TPCH"."T1" WHERE TO_JSON(CAST("V" AS VARIANT)) = '{"a":1}' AND "V" IS NOT NULL`},
		{`COPY (SELECT "id" FROM "tpch"."t1" WHERE "amt" > '0.00' LIMIT 10 OFFSET 5) TO STDOUT (FORMAT "binary");`,
			`SELECT "ID" FROM "DB"."TPCH"."T1" WHERE "AMT" > 0.00 LIMIT 10 OFFSET 5`},
		{`COPY (SELECT "id" FROM "tpch"."t1" OFFSET 5) TO STDOUT (FORMAT "binary");`,
			`SELECT "ID" FROM "DB"."TPCH"."T1" LIMIT NULL OFFSET 5`},
		// count(*) counts in Snowflake.
		{`COPY (SELECT NULL FROM "tpch"."t1" WHERE ctid BETWEEN '(0,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");`,
			`SELECT COUNT(*) FROM "DB"."TPCH"."T1"`},
		{`COPY (SELECT NULL FROM "tpch"."t1" WHERE "id" = '7' LIMIT 3) TO STDOUT (FORMAT "binary");`,
			`SELECT COUNT(*) FROM (SELECT 1 FROM "DB"."TPCH"."T1" WHERE "ID" = 7 LIMIT 3)`},
	} {
		q, err := parseScan(tc.scan)
		if err != nil {
			t.Errorf("%s: %v", tc.scan, err)
			continue
		}
		scan, err := planScan(cat, q)
		if err != nil {
			t.Errorf("%s: %v", tc.scan, err)
			continue
		}
		if scan.sql != tc.want {
			t.Errorf("%s:\n got %s\nwant %s", tc.scan, scan.sql, tc.want)
		}
	}

	// Constants that are not of the column's type, unknown names, and
	// filters Snowflake could not apply exactly fail the scan.
	for _, bad := range []string{
		`COPY (SELECT "id" FROM "tpch"."t1" WHERE "id" = '1; DROP TABLE x') TO STDOUT (FORMAT "binary");`,
		`COPY (SELECT "id" FROM "tpch"."t1" WHERE "f" = 'x') TO STDOUT (FORMAT "binary");`,
		`COPY (SELECT "id" FROM "tpch"."t1" WHERE "d" = '2020-01-01'' OR 1=1') TO STDOUT (FORMAT "binary");`,
		`COPY (SELECT "id" FROM "tpch"."t1" WHERE "d" = '0044-03-15 (BC)') TO STDOUT (FORMAT "binary");`,
		`COPY (SELECT "id" FROM "tpch"."t1" WHERE "ts" = '2020-01-01 00:00:00+00') TO STDOUT (FORMAT "binary");`,
		`COPY (SELECT "id" FROM "tpch"."t1" WHERE "tz" = '2020-01-01 00:00:00') TO STDOUT (FORMAT "binary");`,
		`COPY (SELECT "id" FROM "tpch"."t1" WHERE "b" = 'x') TO STDOUT (FORMAT "binary");`,
		`COPY (SELECT "id" FROM "tpch"."t1" WHERE "name" = '\x00'::BYTEA) TO STDOUT (FORMAT "binary");`,
		`COPY (SELECT "nope" FROM "tpch"."t1") TO STDOUT (FORMAT "binary");`,
		`COPY (SELECT "id" FROM "tpch"."nope") TO STDOUT (FORMAT "binary");`,
		`COPY (SELECT "id" FROM "nope"."t1") TO STDOUT (FORMAT "binary");`,
	} {
		q, err := parseScan(bad)
		if err != nil {
			t.Errorf("%s: %v", bad, err)
			continue
		}
		if scan, err := planScan(cat, q); err == nil {
			t.Errorf("planned %s as %s", bad, scan.sql)
		}
	}
}

func TestPgNames(t *testing.T) {
	got := pgNames([]string{"LINEITEM", "MixedCase", "Foo", "FOO", "_X1", "日本"}, false, nil)
	want := []string{"lineitem", "MixedCase", "Foo", "FOO", "_x1", "日本"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("tables: %q", got)
	}
	var renamed []string
	got = pgNames([]string{"A", "a", "B"}, true, func(from, to string) { renamed = append(renamed, from+">"+to) })
	if strings.Join(got, ",") != "A,a_2,b" || strings.Join(renamed, ",") != "a>a_2" {
		t.Errorf("columns: %q %q", got, renamed)
	}
}

func TestSnowflakeTypes(t *testing.T) {
	for _, tc := range []struct {
		dataType, pg string
		mod          int
		notNull      bool
	}{
		{`{"type":"FIXED","precision":38,"scale":0,"nullable":false}`, "numeric", 38<<16 + 4, true},
		{`{"type":"FIXED","precision":12,"scale":2,"nullable":true}`, "numeric", 12<<16 | 2 + 4, false},
		{`{"type":"REAL"}`, "float8", -1, false},
		{`{"type":"TEXT","length":16777216}`, "text", -1, false},
		{`{"type":"TIMESTAMP_TZ","scale":3}`, "timestamptz", -1, false},
		{`{"type":"OBJECT"}`, "json", -1, false},
		{`{"type":"VECTOR","dimension":3}`, "json", -1, false},
		{`{"type":"GEOMETRY"}`, "text", -1, false},
		{`{"type":"SOMETHING_NEW"}`, "text", -1, false},
	} {
		typ, notNull, err := parseSnowflakeType(tc.dataType)
		if err != nil {
			t.Fatal(err)
		}
		if pg, mod := typ.pgType(); pg != tc.pg || mod != tc.mod || notNull != tc.notNull {
			t.Errorf("%s: %s %d %v", tc.dataType, pg, mod, notNull)
		}
	}
	for _, bad := range []string{`nope`, `{"type":"FIXED","precision":39,"scale":0}`, `{"type":"TIME","scale":12}`} {
		if _, _, err := parseSnowflakeType(bad); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
}

func TestSnowflakeOptions(t *testing.T) {
	c := New("sf", Snowflake)
	for k, v := range map[string]string{
		"account": "myorg-myaccount", "user": "IGLOO", "private_key_secret": "sf-key", "warehouse": "WH",
		"role": "READER", "database": "analytics", "schema": "PUBLIC, raw", "query_timeout": "2m",
	} {
		if err := c.Set(k, v); err != nil {
			t.Fatalf("%s: %v", k, err)
		}
	}
	if err := c.Validate(nil); err != nil {
		t.Fatal(err)
	}
	if c.Port != 443 || c.ServerHost() != "myorg-myaccount.snowflakecomputing.com" ||
		c.PasswordSource() != "private key in secret sf-key" || c.Settings() != "account myorg-myaccount, warehouse WH, role READER" {
		t.Errorf("%+v", c)
	}
	// One way to sign in replaces another.
	if err := c.Set("password", "pw"); err != nil || c.PrivateKeySecret != "" || c.PasswordSource() != "stored" {
		t.Errorf("password: %v %+v", err, c)
	}
	if err := c.Set("tls_ca", "x"); err == nil {
		t.Error("tls_ca accepted for Snowflake")
	}
	pg := New("crm", Postgres)
	if err := pg.Set("warehouse", "WH"); err == nil {
		t.Error("warehouse accepted for PostgreSQL")
	}

	valid := func() Connection {
		c := New("sf", Snowflake)
		c.Account, c.User, c.Database, c.PrivateKeySecret = "myorg-acct", "u", "db", "key"
		return c
	}
	for name, change := range map[string]func(*Connection){
		"no account":      func(c *Connection) { c.Account = "" },
		"bad account":     func(c *Connection) { c.Account = "a/b" },
		"no database":     func(c *Connection) { c.Database = "" },
		"no credentials":  func(c *Connection) { c.PrivateKeySecret = "" },
		"two credentials": func(c *Connection) { c.Password = "x" },
		"bad secret name": func(c *Connection) { c.PrivateKeySecret = "../key" },
		"not a private key": func(c *Connection) {
			c.PrivateKeySecret, c.PrivateKey = "", "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----"
		},
		"encrypted key": func(c *Connection) {
			c.PrivateKeySecret, c.PrivateKey = "", "-----BEGIN ENCRYPTED PRIVATE KEY-----\nAAAA\n-----END ENCRYPTED PRIVATE KEY-----"
		},
		"tls":                func(c *Connection) { c.TLS = TLSDisable },
		"control characters": func(c *Connection) { c.Warehouse = "a\nb" },
	} {
		c := valid()
		change(&c)
		if err := c.Validate(nil); err == nil {
			t.Errorf("%s: accepted", name)
		} else if strings.Contains(err.Error(), "AAAA") {
			t.Errorf("%s: the error quotes the key: %v", name, err)
		}
	}
	if sfIdent("analytics") != "ANALYTICS" || sfIdent(`"My""Db"`) != `My"Db` {
		t.Error("sfIdent")
	}
}
