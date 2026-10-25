package pgwire

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/akkio-inc/igloo/internal/iddomain"
)

// e2Report is the shape of the segment-share report (E2): the members
// of one audience's latest version in each segment, against the base
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
func newE2Test(t *testing.T, opts ...func(*Options)) *idTest {
	t.Helper()
	return e2Fixture(newIndexTest(t, opts...))
}

// e2Fixture writes newE2Test's tables, ID domain and indexes into it.
func e2Fixture(it *idTest) *idTest {
	t := it.t
	t.Helper()
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
	t.Parallel()
	it := newE2Test(t, func(o *Options) { o.Metrics = NewMetrics(prometheus.NewRegistry()) })
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
	if n := testutil.ToFloat64(srv.metrics.indexStatements); n != 1 {
		t.Errorf("igloo_bitmap_index_statements_total %v, want 1", n)
	}

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

	// E2's own view names its columns and LEFT JOINs a survey, which
	// repeats some people and leaves others out: its people are the
	// table's. An inner join leaves people out, and a column list may give
	// person_id another table's values: the tables.
	it.exec("CREATE TABLE lake.public.survey AS SELECT i % 3 AS resp_id, 'answer ' || i AS answer FROM range(6) r(i)",
		"CREATE OR REPLACE VIEW lake.public.v_attributes (person_id, x, survey_resp_id, survey_answer) AS "+
			"SELECT s.*, sv.resp_id, sv.answer FROM lake.public.attributes AS s LEFT JOIN lake.public.survey AS sv ON CAST(s.x AS BIGINT) = sv.resp_id")
	answered(3, "through a view naming its columns, of the population LEFT JOINed to a survey")
	it.exec("CREATE OR REPLACE VIEW lake.public.v_attributes AS " +
		"SELECT s.person_id, sv.answer FROM lake.public.attributes AS s JOIN lake.public.survey AS sv ON CAST(s.x AS BIGINT) = sv.resp_id")
	answered(2, "through a view joining the population to a survey")
	it.exec("CREATE OR REPLACE VIEW lake.public.v_attributes (person_id, x) AS " +
		"SELECT sv.answer, s.person_id FROM lake.public.attributes AS s LEFT JOIN lake.public.survey AS sv ON CAST(s.x AS BIGINT) = sv.resp_id")
	answered(2, "through a view naming the survey's answers person_id")
	it.exec("CREATE OR REPLACE VIEW lake.public.v_attributes AS SELECT * FROM lake.public.attributes")
	answered(3, "the plain view again")

	// Bitmaps past their budget: the statement runs again from the tables.
	rows := wantRows(t, it, conn)
	mustExec(t, conn, "SET rb_memory_budget = '1B'")
	reruns := srv.bitmapReruns.Load()
	if got := texts(t, conn, e2Report); strings.Join(got, "\n") != strings.Join(rows, "\n") {
		t.Errorf("past the bitmaps' budget: rows\n%q\nwant\n%q", got, rows)
	}
	if srv.bitmapReruns.Load() == reruns {
		t.Error("bitmaps past their budget ran no statement again")
	}
	mustExec(t, conn, "RESET rb_memory_budget")

	// Each index's table has row groups sized for its bitmaps.
	for _, name := range []string{"seg", "aud", "population"} {
		tbl, err := srv.ddl.LoadTable(ctx, "igloo_bitmaps", name)
		if err != nil {
			t.Fatal(err)
		}
		if n, err := strconv.ParseInt(tbl.Properties()[rowGroupSizeProperty], 10, 64); err != nil || n < 1 || n > duckDBRowGroupsMax {
			t.Errorf("index %s's row groups: %q", name, tbl.Properties()[rowGroupSizeProperty])
		}
	}

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
	srv.waitIndexBuilds(t)
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

// TestIndexRowGroupRows sizes row groups of bitmaps at about 32 MiB.
func TestIndexRowGroupRows(t *testing.T) {
	t.Parallel()
	for _, c := range []struct{ bytes, rows int64 }{{0, 1}, {90 << 20, 1}, {1 << 20, 32}, {1 << 10, 32768}, {1, duckDBRowGroupsMax}} {
		if got := indexRowGroupRows(c.bytes); got != c.rows {
			t.Errorf("bitmaps of %d bytes: %d rows a row group, want %d", c.bytes, got, c.rows)
		}
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

// Igloo's own reads of bitmap indexes' tables, in a statement it wrote to
// answer from them, are reads the coordinator may compute for workers
// (sessionEnv.Computes), which the session computes as the statement
// would; no other statement's, and no other table's.
func TestIndexValues(t *testing.T) {
	t.Parallel()
	it := newE2Test(t)
	ctx := context.Background()
	srv := it.r.srv
	user, err := it.r.store.Get(ctx, "igloo")
	if err != nil {
		t.Fatal(err)
	}
	es, err := srv.OpenEmbedded(ctx, user, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer es.Close() //nolint:errcheck
	pinned := &statement{indexPins: []indexPin{{schema: "public", table: "person_segments", index: "seg"}}}
	index := []string{"lake", iddomain.IndexSchema, "seg"}
	for _, c := range []struct {
		st    *statement
		parts []string
		want  bool
	}{
		{pinned, index, true},
		{&statement{}, index, false},
		{pinned, []string{"lake", "public", "person_segments"}, false},
	} {
		env := &sessionEnv{s: srv, sess: es.sess, user: user, st: c.st}
		if got, err := env.Computes(ctx, c.parts); err != nil || got != c.want {
			t.Errorf("Computes(%v) with %d pins: %v, %v; want %v", c.parts, len(c.st.indexPins), got, err, c.want)
		}
	}
	env := &sessionEnv{s: srv, sess: es.sess, user: user, st: pinned}
	v, err := env.Value(ctx, "SELECT (SELECT count(*) FROM lake."+iddomain.IndexSchema+".seg)")
	if n, ok := v.(int64); err != nil || !ok || n == 0 {
		t.Errorf("Value: %v (%T), %v", v, v, err)
	}
}
