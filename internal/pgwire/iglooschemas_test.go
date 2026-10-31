package pgwire

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestIglooSchemasOffSearchPath checks that no one, admins included, puts
// one of Igloo's own schemas on the search path or makes it the current
// schema, however it is spelled: an unqualified name would then reach a
// bitmap index's or a dictionary's table without naming its schema, which
// Igloo's checks look for. A refused SET or USE leaves the path as it was.
func TestIglooSchemasOffSearchPath(t *testing.T) {
	t.Parallel()
	it := newIndexTest(t)
	ctx := context.Background()
	srv := it.r.srv
	mustExec(t, it.admin, "CREATE USER rita WITH PASSWORD 'rita-password' ROLE reader")
	rita := connect(t, it.r.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)
	it.exec("ALTER TABLE lake.public.segments ADD BITMAP INDEX seg ON (segment_name) FOR person_id")
	srv.waitIndexBuilds(t)

	for _, c := range []struct {
		name string
		conn *pgx.Conn
	}{{"admin", it.admin}, {"rita", rita}} {
		before := texts(t, c.conn, "SELECT current_setting('search_path'), current_schema()")
		for _, set := range []string{
			"SET search_path = 'lake.igloo_bitmaps'",
			"SET search_path = 'LAKE.IGLOO_BITMAPS'",
			`SET search_path = '"lake"."igloo_bitmaps"'`,
			"SET search_path TO 'lake.public,lake.igloo_bitmaps'",
			"SET SESSION search_path = 'lake.igloo_domains'",
			"SET search_path = 'igloo_domains'",
			"SET schema = 'igloo_bitmaps'",
			"PRAGMA search_path = 'lake.igloo_bitmaps'",
			"USE lake.igloo_bitmaps",
			`USE "lake"."IGLOO_DOMAINS"`,
		} {
			if _, err := c.conn.Exec(ctx, set); pgCode(err) != "42501" {
				t.Errorf("%s: %s: %v, want permission denied", c.name, set, err)
			}
			if got := texts(t, c.conn, "SELECT current_setting('search_path'), current_schema()"); strings.Join(got, ",") != strings.Join(before, ",") {
				t.Errorf("%s: after %s the path is %q, was %q", c.name, set, got, before)
			}
		}
		if _, err := c.conn.Exec(ctx, "SELECT count(*) FROM seg"); err == nil {
			t.Errorf("%s: an unqualified name reached the index's table", c.name)
		}
		mustExec(t, c.conn, "SET search_path = 'lake.public'")
		mustExec(t, c.conn, "RESET search_path")
	}
	// A global path reaches no session either.
	if _, err := it.admin.Exec(ctx, "SET GLOBAL search_path = 'lake.igloo_bitmaps'"); err == nil {
		t.Error("SET GLOBAL search_path to an index's schema ran")
	}
	other := connect(t, it.r.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)
	if got := strings.ToLower(strings.Join(texts(t, other, "SELECT current_setting('search_path')"), ",")); strings.Contains(got, "igloo_bitmaps") {
		t.Errorf("a new session's path: %s", got)
	}
}

// TestIndexReadsOutsideRewritesGetPolicies checks the depth behind that:
// were a session's path to reach the bitmap indexes' schema anyway, set
// behind Igloo's back here, an unqualified read of an index's table gets
// the index's policy, which denies every row (only the reads Igloo writes,
// named in full, are its own), and no write, drop or policy change reaches
// the table by such a name.
func TestIndexReadsOutsideRewritesGetPolicies(t *testing.T) {
	t.Parallel()
	it := newIndexTest(t)
	ctx := context.Background()
	srv := it.r.srv
	mustExec(t, it.admin, "CREATE USER rita WITH PASSWORD 'rita-password' ROLE reader")
	rita := connect(t, it.r.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)
	it.exec("ALTER TABLE lake.public.segments ADD BITMAP INDEX seg ON (segment_name) FOR person_id")
	srv.waitIndexBuilds(t)
	it.exec(
		`CREATE ROW ACCESS POLICY no_seg0 AS (s VARCHAR) RETURNS BOOLEAN -> is_role_in_session('admin') OR s <> 'seg0'`,
		"ALTER TABLE lake.public.segments ADD ROW ACCESS POLICY no_seg0 ON (segment_name)",
	)
	// The counts Igloo answers from the index keep working.
	if got := texts(t, rita, "SELECT count(DISTINCT person_id) FROM lake.public.segments WHERE segment_name = 'seg1'"); len(got) != 1 || got[0] == "0" {
		t.Errorf("rita's count: %q", got)
	}
	behind := func(conn *pgx.Conn, path string) {
		t.Helper()
		sess := srv.sessions.byPID[int32(conn.PgConn().PID())]
		if sess == nil {
			t.Fatal("no session")
		}
		if _, err := sess.conn.ExecContext(ctx, "SET SESSION search_path = "+quoteLiteral(path), nil); err != nil {
			t.Fatal(err)
		}
		sess.settingGen++
	}
	for _, c := range []struct {
		name string
		conn *pgx.Conn
	}{{"admin", it.admin}, {"rita", rita}} {
		behind(c.conn, "lake.igloo_bitmaps")
		// The name reaches the index's table, through its policy.
		if got := texts(t, c.conn, "SELECT count(*) FROM seg"); len(got) != 1 || got[0] != "0" {
			t.Errorf("%s: count through the path: %q, want 0", c.name, got)
		}
		for _, q := range []string{
			"SELECT segment_name, rb_cardinality(bm) FROM seg ORDER BY 1",
			"SELECT segment_name, rb_cardinality(bm) FROM seg WHERE segment_name IN ('seg0', 'seg1') ORDER BY 1",
			"SELECT * FROM (SELECT segment_name, source_snapshot_id FROM seg) AS s",
		} {
			// Refused, or no rows.
			if got, err := queryTexts(ctx, c.conn, q); err == nil && len(got) > 0 {
				t.Errorf("%s: %s returned the index's rows: %q", c.name, q, got)
			}
		}
		behind(c.conn, "lake.public")
	}
	behind(it.admin, "lake.igloo_bitmaps")
	for _, q := range []string{
		"INSERT INTO seg SELECT * FROM seg",
		"DELETE FROM seg",
		"DROP TABLE seg",
		"ALTER TABLE seg DROP ALL ROW ACCESS POLICIES",
	} {
		if _, err := it.admin.Exec(ctx, q); pgCode(err) != "42501" {
			t.Errorf("admin: %s: %v, want permission denied", q, err)
		}
	}
	behind(it.admin, "lake.public")
	if got := texts(t, rita, "SELECT count(DISTINCT person_id) FROM lake.public.segments WHERE segment_name = 'seg1'"); len(got) != 1 || got[0] == "0" {
		t.Errorf("rita's count after the attempts: %q", got)
	}
}

// queryTexts runs q and returns its rows' values as text, one string a row.
func queryTexts(ctx context.Context, conn *pgx.Conn, q string) ([]string, error) {
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		parts := make([]string, len(vals))
		for i, v := range vals {
			parts[i] = fmt.Sprint(v)
		}
		out = append(out, strings.Join(parts, "|"))
	}
	return out, rows.Err()
}
