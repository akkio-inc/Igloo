package pgwire

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// e2Report is the shape of Acme's segment-share report (E2): the members
// of one audience's latest version in each segment, against the general
// population's, through a view of the population's table.
const e2Report = `WITH AUDIENCE AS (SELECT person_id FROM audience_members WHERE audience_id = 'a1' AND ver = (SELECT max(ver) FROM audience_revisions WHERE audience_id = 'a1')),
BASE_POPULATION AS (SELECT person_id FROM v_attributes),
AUD_SEGMENTS AS (SELECT S.segment_name AS segment_name, count(DISTINCT S.person_id) AS audience_count FROM person_segments AS S INNER JOIN AUDIENCE AS A ON A.person_id = S.person_id WHERE S.segment_name LIKE 'SEG_%' GROUP BY S.segment_name),
BASEPOP_SEGMENTS AS (SELECT S.segment_name AS segment_name, count(DISTINCT S.person_id) AS basepop_count FROM person_segments AS S INNER JOIN BASE_POPULATION AS G ON G.person_id = S.person_id WHERE S.segment_name LIKE 'SEG_%' GROUP BY S.segment_name),
AUD_TOTAL AS (SELECT count(DISTINCT S.person_id) AS total_audience FROM person_segments AS S INNER JOIN AUDIENCE AS A ON A.person_id = S.person_id WHERE S.segment_name LIKE 'SEG_%')
SELECT A.segment_name, A.audience_count, G.basepop_count, T.total_audience, round(A.audience_count / T.total_audience, 4) AS share
FROM AUD_SEGMENTS AS A JOIN BASEPOP_SEGMENTS AS G ON A.segment_name = G.segment_name CROSS JOIN AUD_TOTAL AS T ORDER BY A.segment_name`

// newE2Test is an index test with E2's tables, members of one domain, and
// indexes that answer its counts, built.
func newE2Test(t *testing.T) *idTest {
	t.Helper()
	it := newIndexTest(t)
	it.exec(
		"CREATE TABLE lake.public.person_segments AS SELECT 'p' || (i % 400) AS person_id, 'SEG_' || (i % 11) AS segment_name FROM range(3000) r(i)",
		"INSERT INTO lake.public.person_segments SELECT 'p' || i, 'OTHER' FROM range(50) r(i)",
		"CREATE TABLE lake.public.audience_members AS SELECT 'p' || (i * 3 % 500) AS person_id, CASE WHEN i % 2 = 0 THEN 'a1' ELSE 'a2' END AS audience_id, CAST(1 + i % 3 AS INTEGER) AS ver FROM range(900) r(i)",
		"CREATE TABLE lake.public.audience_revisions AS SELECT * FROM (VALUES ('a1', 2), ('a1', 1), ('a2', 3)) v(audience_id, ver)",
		"CREATE TABLE lake.public.attributes AS SELECT 'p' || i AS person_id, CAST(i % 5 AS INTEGER) AS x FROM range(350) r(i)",
		"CREATE VIEW lake.public.v_attributes AS SELECT * FROM lake.public.attributes",
	)
	for _, tbl := range []string{"person_segments", "audience_members", "attributes"} {
		it.exec("ALTER TABLE lake.public."+tbl+" ALTER COLUMN person_id SET TAG pii = 'person_id'",
			"ALTER TABLE lake.public."+tbl+" ALTER COLUMN person_id SET ID DOMAIN people")
	}
	srv := it.r.srv
	srv.MapIDDomains(context.Background())
	it.exec(
		"ALTER TABLE lake.public.person_segments ADD BITMAP INDEX seg ON (segment_name) FOR person_id",
		"ALTER TABLE lake.public.audience_members ADD BITMAP INDEX aud ON (audience_id, ver) FOR person_id",
		"ALTER TABLE lake.public.attributes ADD BITMAP INDEX population ON () FOR person_id",
	)
	srv.waitIndexBuilds(t)
	srv.MaintainBitmapIndexes(context.Background())
	return it
}

// TestBitmapIndexRewrite answers E2's counts from bitmap indexes, with the
// same rows as from the tables, and only where no policy stands in the way
// and the bitmaps are of the tables' current snapshots.
func TestBitmapIndexRewrite(t *testing.T) {
	it := newE2Test(t)
	ctx := context.Background()
	srv := it.r.srv
	conn := it.admin
	mustExec(t, conn, "SET search_path = 'lake.public'")

	// The rows as the tables give them, the indexes out of the way.
	srv.bitmapIndexesOn = false
	want := texts(t, conn, e2Report)
	srv.bitmapIndexesOn = true
	if len(want) != 11 {
		t.Fatalf("report rows: %q", want)
	}
	// answered runs the report and checks how many of its three counting
	// SELECTs read bitmaps.
	answered := func(want int64, what string) {
		t.Helper()
		before := srv.indexRewrites.Load()
		got := texts(t, conn, e2Report)
		if strings.Join(got, "\n") != strings.Join(wantRows(t, it, conn), "\n") {
			t.Errorf("%s: rows\n%q\nwant\n%q", what, got, wantRows(t, it, conn))
		}
		if n := srv.indexRewrites.Load() - before; n != want {
			t.Errorf("%s: %d counting SELECTs read bitmaps, want %d", what, n, want)
		}
	}
	_ = want
	answered(3, "with the indexes built")

	// A row access policy on a table it answers for, or a masking policy on
	// a column it reads, there or under the view: the tables.
	it.exec("CREATE ROW ACCESS POLICY everyone AS (s VARCHAR) RETURNS BOOLEAN -> true",
		"ALTER TABLE lake.public.person_segments ADD ROW ACCESS POLICY everyone ON (segment_name)")
	answered(0, "with a row access policy on the segments")
	it.exec("ALTER TABLE lake.public.person_segments DROP ROW ACCESS POLICY everyone")
	answered(3, "the policy dropped")
	it.exec("CREATE MASKING POLICY same AS (v VARCHAR) RETURNS VARCHAR -> v",
		"ALTER TABLE lake.public.person_segments ALTER COLUMN segment_name SET MASKING POLICY same")
	answered(0, "with a mask on segment_name")
	it.exec("ALTER TABLE lake.public.person_segments ALTER COLUMN segment_name UNSET MASKING POLICY",
		"ALTER TABLE lake.public.attributes ALTER COLUMN person_id SET MASKING POLICY same")
	// The population's count reads the tables; the audience's, the
	// bitmaps, and the segments the population's count reads are held to
	// their snapshot.
	answered(2, "with a mask on the population's members, under the view")
	it.exec("ALTER TABLE lake.public.attributes ALTER COLUMN person_id UNSET MASKING POLICY")
	answered(3, "the masks dropped")

	// A statement prepared once and run after a table changed reads the
	// tables until the bitmaps catch up, then the bitmaps again.
	cached := it.r.admin(t, pgx.QueryExecModeCacheStatement)
	mustExec(t, cached, "SET search_path = 'lake.public'")
	if got := texts(t, cached, e2Report); strings.Join(got, "\n") != strings.Join(wantRows(t, it, conn), "\n") {
		t.Errorf("prepared: rows %q", got)
	}
	it.exec("INSERT INTO lake.public.person_segments SELECT 'p' || i, 'SEG_1' FROM range(400, 480) r(i)")
	before := srv.indexRewrites.Load()
	if got := texts(t, cached, e2Report); strings.Join(got, "\n") != strings.Join(wantRows(t, it, conn), "\n") {
		t.Errorf("prepared, after an insert: rows %q", got)
	}
	srv.MaintainBitmapIndexes(ctx)
	if got := texts(t, cached, e2Report); strings.Join(got, "\n") != strings.Join(wantRows(t, it, conn), "\n") {
		t.Errorf("prepared, after the bitmaps caught up: rows %q", got)
	}
	if srv.indexRewrites.Load() == before {
		t.Error("the prepared statement did not go back to the bitmaps once they caught up")
	}

	// Users whose reads grants decide read the tables.
	it.exec("CREATE USER nina WITH PASSWORD 'nina-password' ROLE none",
		"GRANT SELECT ON SCHEMA lake.public TO nina")
	nina := connect(t, it.r.url("nina", "nina-password"), pgx.QueryExecModeSimpleProtocol)
	mustExec(t, nina, "SET search_path = 'lake.public'")
	before = srv.indexRewrites.Load()
	if got := texts(t, nina, e2Report); strings.Join(got, "\n") != strings.Join(wantRows(t, it, conn), "\n") {
		t.Errorf("nina's rows %q", got)
	}
	if srv.indexRewrites.Load() != before {
		t.Error("a statement whose reads grants decide was answered from indexes")
	}
}

// wantRows is the report from the tables, the indexes out of the way.
func wantRows(t *testing.T, it *idTest, conn *pgx.Conn) []string {
	t.Helper()
	srv := it.r.srv
	srv.bitmapIndexesOn = false
	defer func() { srv.bitmapIndexesOn = true }()
	return texts(t, conn, e2Report)
}
