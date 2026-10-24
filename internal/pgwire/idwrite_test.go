package pgwire

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/iddomain"
)

func TestRewriteMemberWrite(t *testing.T) {
	members := []iddomain.Member{{Domain: "people", Schema: "public", Table: "t", Column: "person_id"}}
	visible := []string{"person_id", "n"}
	for q, want := range map[string]string{
		"INSERT INTO t VALUES ('a', 1)":                                   `INSERT INTO t ("person_id", "n") VALUES ('a', 1)`,
		"insert into lake.public.t select * from s":                       `insert into lake.public.t ("person_id", "n") select * from s`,
		"INSERT INTO t BY POSITION SELECT 'a', 1":                         `INSERT INTO t ("person_id", "n") SELECT 'a', 1`,
		"INSERT INTO t (n) VALUES (1)":                                    "INSERT INTO t (n) VALUES (1)",
		"INSERT INTO t BY NAME SELECT 1 AS n":                             "INSERT INTO t BY NAME SELECT 1 AS n",
		"INSERT INTO t DEFAULT VALUES":                                    "INSERT INTO t DEFAULT VALUES",
		"WITH s AS (SELECT 1) INSERT INTO t SELECT * FROM s":              `WITH s AS (SELECT 1) INSERT INTO t ("person_id", "n") SELECT * FROM s`,
		"UPDATE t SET n = 2":                                              "UPDATE t SET n = 2",
		"UPDATE t SET person_id = 'b' WHERE n = 1":                         "UPDATE t SET person_id = 'b' \n, \"_igloo_id_person_id\" = NULL WHERE n = 1",
		"UPDATE t AS x SET \"PERSON_ID\" = 'b';":                           "UPDATE t AS x SET \"PERSON_ID\" = 'b'\n, \"_igloo_id_person_id\" = NULL ;",
		"UPDATE t x SET x.person_id = 'b' -- why":                          "UPDATE t x SET x.person_id = 'b' -- why\n, \"_igloo_id_person_id\" = NULL ",
		"UPDATE t SET (n, person_id) = (1, 'b') FROM s":                    "UPDATE t SET (n, person_id) = (1, 'b') \n, \"_igloo_id_person_id\" = NULL FROM s",
		"UPDATE t SET n = (SELECT max(n) FROM s WHERE s.person_id = 'b')":  "UPDATE t SET n = (SELECT max(n) FROM s WHERE s.person_id = 'b')",
		"UPDATE t SET person_id = CASE WHEN n = 1 THEN 'a' END WHERE true": "UPDATE t SET person_id = CASE WHEN n = 1 THEN 'a' END \n, \"_igloo_id_person_id\" = NULL WHERE true",
		"MERGE INTO t USING s ON t.n = s.n WHEN MATCHED AND s.n = CASE WHEN true THEN 1 END THEN UPDATE SET person_id = CASE WHEN s.n > 0 THEN s.person_id END " +
			"WHEN MATCHED THEN UPDATE SET n = s.n WHEN NOT MATCHED THEN INSERT VALUES (s.person_id, s.n)": "MERGE INTO t USING s ON t.n = s.n WHEN MATCHED AND s.n = CASE WHEN true THEN 1 END " +
			"THEN UPDATE SET person_id = CASE WHEN s.n > 0 THEN s.person_id END \n, \"_igloo_id_person_id\" = NULL WHEN MATCHED THEN UPDATE SET n = s.n WHEN NOT MATCHED THEN INSERT " +
			`("person_id", "n") VALUES (s.person_id, s.n)`,
		"MERGE INTO t USING s ON t.n = s.n WHEN NOT MATCHED THEN INSERT (n) VALUES (s.n) WHEN MATCHED THEN DELETE": "MERGE INTO t USING s ON t.n = s.n WHEN NOT MATCHED THEN INSERT (n) VALUES (s.n) WHEN MATCHED THEN DELETE",
	} {
		kind, _, after, ok := writeTarget(q)
		if !ok {
			t.Errorf("%s: not taken for a write", q)
			continue
		}
		got, err := rewriteMemberWrite(q, kind, after, "public.t", members, visible)
		if err != nil || got != want {
			t.Errorf("%s:\n got %q %v\nwant %q", q, got, err, want)
		}
	}
	for _, q := range []string{
		"MERGE INTO t USING s ON t.n = s.n WHEN MATCHED THEN UPDATE BY NAME",
		"MERGE INTO t USING s ON t.n = s.n WHEN MATCHED THEN UPDATE BY POSITION WHEN NOT MATCHED THEN INSERT BY NAME",
	} {
		kind, _, after, _ := writeTarget(q)
		if _, err := rewriteMemberWrite(q, kind, after, "public.t", members, visible); pgerror.GetCode(err) != codes.FeatureNotSupported {
			t.Errorf("%s: %v, want it refused", q, err)
		}
	}
	for _, q := range []string{"SELECT 1", "DELETE FROM t", "WITH s AS (SELECT 1) SELECT * FROM s", "COPY t FROM 'x'"} {
		if _, _, _, ok := writeTarget(q); ok {
			t.Errorf("%s: taken for a write", q)
		}
	}
}

// TestMemberWrites writes a table with a member column through sessions:
// inserts that name no columns name the others, and updates of the member
// column leave no id beside a value it was not given for.
func TestMemberWrites(t *testing.T) {
	vc := newViewCatalog(t)
	r := vc.replica(t, func(o *Options) { o.IDDomainsOn = true })
	admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS lake.public",
		"CREATE TABLE lake.public.people AS SELECT 'p' || i AS person_id, CAST(i AS INTEGER) AS n FROM range(10) r(i)",
		"CREATE TABLE lake.public.src AS SELECT 's' || i AS person_id, CAST(i AS INTEGER) AS n FROM range(5, 12) r(i)",
		"CREATE TAG pii",
		"ALTER TABLE lake.public.people ALTER COLUMN person_id SET TAG pii = 'person_id'",
		"CREATE ID DOMAIN people (VARCHAR) TAG pii = 'person_id'",
		"ALTER TABLE lake.public.people ALTER COLUMN person_id SET ID DOMAIN people",
		"CREATE USER wanda WITH PASSWORD 'wanda-password' ROLE writer",
	} {
		mustExec(t, admin, q)
	}
	internal := func(q string) []string {
		t.Helper()
		conn, err := r.eng.Connect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close() //nolint:errcheck
		rows, err := queryRows(ctx, conn, q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		var out []string
		for _, row := range rows {
			parts := make([]string, len(row))
			for i, v := range row {
				parts[i] = fmt.Sprint(v)
			}
			out = append(out, strings.Join(parts, "|"))
		}
		return out
	}
	// Ids as the table had them, standing in for Igloo's.
	internal("UPDATE lake.public.people SET _igloo_id_person_id = 100 + n")

	wanda := connect(t, r.url("wanda", "wanda-password"), pgx.QueryExecModeSimpleProtocol)
	for _, c := range []*pgx.Conn{admin, wanda} {
		for _, q := range []string{
			"UPDATE lake.public.people SET n = n + 100 WHERE n = 0",
			"UPDATE lake.public.people AS p SET person_id = 'q1' WHERE p.n = 1",
			"UPDATE lake.public.people SET (person_id, n) = ('q2', 2) WHERE n = 2",
			"UPDATE lake.public.people SET n = CASE WHEN n = 3 THEN 33 ELSE n END, person_id = 'q3' WHERE n = 3",
			"MERGE INTO lake.public.people p USING (SELECT * FROM lake.public.src WHERE n = 5) s ON p.n = s.n " +
				"WHEN MATCHED THEN UPDATE SET person_id = s.person_id WHEN NOT MATCHED THEN INSERT VALUES (s.person_id, s.n)",
			"MERGE INTO lake.public.people p USING lake.public.src s ON p.n = s.n " +
				"WHEN MATCHED THEN UPDATE SET n = s.n WHEN NOT MATCHED THEN INSERT VALUES (s.person_id, s.n)",
		} {
			if c == wanda {
				// Once is enough; wanda's only need to be allowed.
				q = strings.ReplaceAll(strings.ReplaceAll(q, "n = 0", "n = -1"), "n = 1", "n = -1")
			}
			if _, err := c.Exec(ctx, q); err != nil {
				t.Errorf("%s: %v", q, err)
			}
		}
		for _, q := range []string{
			"INSERT INTO lake.public.people VALUES ('p20', 20)",
			"INSERT INTO lake.public.people SELECT 'p21', 21",
			"INSERT INTO lake.public.people BY POSITION VALUES ('p22', 22)",
		} {
			if c == wanda {
				q = strings.ReplaceAll(q, "2", "3")
			}
			if _, err := c.Exec(ctx, q); err != nil {
				t.Errorf("%s: %v", q, err)
			}
		}
		if _, err := c.Exec(ctx, "MERGE INTO lake.public.people p USING lake.public.src s ON p.n = s.n WHEN MATCHED THEN UPDATE BY NAME"); pgCode(err) != "0A000" {
			t.Errorf("MERGE ... UPDATE BY NAME: %v", err)
		}
	}
	// Rows whose value changed, and new ones, have no id or one Igloo gave
	// them since (below 100), never the one they had.
	got := strings.Join(internal("SELECT person_id, n, CASE WHEN _igloo_id_person_id >= 100 THEN CAST(_igloo_id_person_id AS VARCHAR) ELSE '-' END "+
		"FROM lake.public.people ORDER BY n, person_id"), " ")
	want := "q1|1|- q2|2|- p4|4|104 s5|5|- p6|6|106 p7|7|107 p8|8|108 p9|9|109 s10|10|- s11|11|- " +
		"p20|20|- p21|21|- p22|22|- p30|30|- p31|31|- p33|33|- q3|33|- p0|100|100"
	if got != want {
		t.Errorf("rows:\n got %s\nwant %s", got, want)
	}
}
