package pgwire

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/iddomain"
	"github.com/akkio-inc/igloo/internal/tasks"
)

func TestParseIndexCommand(t *testing.T) {
	for _, c := range []struct {
		query  string
		ok     bool
		bad    bool
		kind   indexCmdKind
		name   string
		groups string
		column string
	}{
		{query: "ALTER TABLE lake.public.seg ADD BITMAP INDEX seg_by_name ON (segment_name) FOR person_id", ok: true, kind: icAdd, name: "seg_by_name", groups: "segment_name", column: "person_id"},
		{query: "alter table seg add bitmap index if not exists S2 on (a, \"B c\") for person_id;", ok: true, kind: icAdd, name: "s2", groups: "a,B c", column: "person_id"},
		{query: "ALTER TABLE people ADD BITMAP INDEX everyone ON () FOR person_id", ok: true, kind: icAdd, name: "everyone", column: "person_id"},
		{query: "ALTER TABLE b ADD BITMAP INDEX k ON (ELEMENTS(keywords)) FOR person_id", ok: true, kind: icAdd, name: "k", groups: "keywords", column: "person_id"},
		{query: "ALTER TABLE b ADD BITMAP INDEX k ON (elements) FOR person_id", ok: true, kind: icAdd, name: "k", groups: "elements", column: "person_id"},
		{query: "ALTER TABLE b ADD BITMAP INDEX k ON (ELEMENTS(a), b) FOR person_id", ok: true, bad: true},
		{query: "ALTER TABLE seg DROP BITMAP INDEX IF EXISTS seg_by_name", ok: true, kind: icDrop, name: "seg_by_name"},
		{query: "SHOW BITMAP INDEXES", ok: true, kind: icShow},
		{query: "ALTER TABLE seg ADD BITMAP INDEX x ON (a) FOR", ok: true, bad: true},
		{query: "ALTER TABLE seg ADD BITMAP INDEX x (a) FOR b", ok: true, bad: true},
		{query: "ALTER TABLE seg ADD BITMAP INDEX 1x ON (a) FOR b", ok: true, bad: true},
		{query: "ALTER TABLE seg ADD COLUMN x INTEGER", ok: false},
		{query: "SELECT 1", ok: false},
	} {
		got, ok, err := parseIndexCommand(c.query)
		if ok != c.ok || (err != nil) != c.bad {
			t.Errorf("%s: ok %v, err %v", c.query, ok, err)
			continue
		}
		if !ok || c.bad {
			continue
		}
		if got.kind != c.kind || got.name != c.name || strings.Join(got.groups, ",") != c.groups || got.column != c.column ||
			got.elements != strings.Contains(c.query, "ELEMENTS(") {
			t.Errorf("%s: %+v", c.query, got)
		}
	}
}

// indexTest is an ID domain test with bitmap indexes on.
func newIndexTest(t *testing.T) *idTest {
	t.Helper()
	if os.Getenv("IGLOO_TEST_RB_EXTENSION") == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("IGLOO_TEST_RB_EXTENSION is not set: CI builds the rb extension (extensions/rb/build.sh)")
		}
		t.Skip("IGLOO_TEST_RB_EXTENSION is not set; build the rb extension with extensions/rb/build.sh")
	}
	vc := newViewCatalog(t)
	r := vc.replica(t, func(o *Options) {
		o.IDDomainsOn, o.BitmapIndexes = true, true
		o.Tasks = tasks.NewScheduler(tasks.NewMemoryStore(), tasks.Options{})
	})
	it := &idTest{t: t, vc: vc, r: r, admin: r.admin(t, pgx.QueryExecModeSimpleProtocol)}
	it.exec(
		"CREATE SCHEMA IF NOT EXISTS lake.public",
		"CREATE TABLE lake.public.segments AS SELECT 'p' || (i % 300) AS person_id, 'seg' || (i % 7) AS segment_name, CAST(i % 3 AS INTEGER) AS day FROM range(2000) r(i)",
		"CREATE TAG pii",
		"ALTER TABLE lake.public.segments ALTER COLUMN person_id SET TAG pii = 'person_id'",
		"CREATE ID DOMAIN people (VARCHAR) TAG pii = 'person_id'",
		"ALTER TABLE lake.public.segments ALTER COLUMN person_id SET ID DOMAIN people",
	)
	r.srv.MapIDDomains(context.Background())
	return it
}

// sameCounts checks that index's bitmaps are the table's distinct members
// by its groups (groups, a comma-separated list, or "" for the whole
// table) at the table's current snapshot.
func (it *idTest) sameCounts(index, groups string) {
	it.t.Helper()
	sel, by := "", ""
	if groups != "" {
		sel, by = groups+", ", " GROUP BY "+groups
	}
	want := it.internal("SELECT " + sel + "count(DISTINCT person_id) FROM lake.public.segments" + by + " ORDER BY ALL")
	got := it.internal("SELECT " + sel + "rb_cardinality(bm) FROM lake.igloo_bitmaps." + index + " ORDER BY ALL")
	if strings.Join(got, ",") != strings.Join(want, ",") {
		it.t.Errorf("bitmaps of %s:\n%q\nwant\n%q", index, got, want)
	}
	tbl, err := it.r.srv.ddl.LoadTable(context.Background(), "public", "segments")
	if err != nil {
		it.t.Fatal(err)
	}
	cur := strconv.FormatInt(tbl.CurrentSnapshot().SnapshotID, 10)
	if got := it.one("SELECT CAST(min(source_snapshot_id) AS VARCHAR) || ',' || CAST(max(source_snapshot_id) AS VARCHAR) FROM lake.igloo_bitmaps." + index); got != cur+","+cur {
		it.t.Errorf("bitmaps of %s are of snapshots %s, want the current %s", index, got, cur)
	}
}

// TestBitmapIndexElements keeps a bitmap of each element of a list
// column's lists, with each one's rows (as often as lists hold it) and
// members.
func TestBitmapIndexElements(t *testing.T) {
	it := newIndexTest(t)
	ctx := context.Background()
	srv := it.r.srv
	it.exec(
		"CREATE TABLE lake.public.browsing AS SELECT 'p' || (i % 90) AS person_id, CASE i % 4 WHEN 0 THEN ['cars', 'news'] WHEN 1 THEN ['', 'cars', 'cars'] WHEN 2 THEN NULL ELSE [] END AS keywords FROM range(600) r(i)",
		"ALTER TABLE lake.public.browsing ALTER COLUMN person_id SET TAG pii = 'person_id'",
		"ALTER TABLE lake.public.browsing ALTER COLUMN person_id SET ID DOMAIN people",
	)
	srv.MapIDDomains(ctx)
	if _, err := it.admin.Exec(ctx, "ALTER TABLE lake.public.browsing ADD BITMAP INDEX not_a_list ON (ELEMENTS(person_id)) FOR person_id"); pgCode(err) == "" {
		t.Error("an index of the elements of a column that is no list")
	}
	it.exec("ALTER TABLE lake.public.browsing ADD BITMAP INDEX keywords ON (ELEMENTS(keywords)) FOR person_id")
	srv.waitIndexBuilds(t)
	same := func(what string) {
		t.Helper()
		want := it.internal("SELECT k, count(DISTINCT person_id), count(*) FROM (SELECT unnest(keywords) AS k, person_id FROM lake.public.browsing) GROUP BY ALL ORDER BY ALL")
		got := it.internal("SELECT keywords, rb_cardinality(bm), row_count FROM lake.igloo_bitmaps.keywords ORDER BY ALL")
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("%s: bitmaps\n%q\nwant\n%q", what, got, want)
		}
	}
	same("built")
	if got := texts(t, it.admin, "SHOW BITMAP INDEXES"); len(got) != 1 || !strings.HasPrefix(got[0], "keywords|public.browsing|person_id|ELEMENTS(keywords)|") {
		t.Errorf("SHOW BITMAP INDEXES: %q", got)
	}
	// Appended with their ids, as a write of known values writes them.
	it.internal("INSERT INTO lake.public.browsing (person_id, keywords, _igloo_id_person_id) " +
		"SELECT value, ['news', 'sport'], id FROM lake.igloo_domains.people WHERE value IN ('p1', 'p2', 'p3', 'p95')")
	srv.indexesChanged("public", "browsing")
	srv.waitIndexBuilds(t)
	if how := srv.lastBuild("keywords"); how != "append" {
		t.Errorf("after an append: %q", how)
	}
	same("after an append")
}

// TestBitmapIndexes adds, builds, keeps up to date, lists and drops bitmap
// indexes, which only admins change and no user reads.
func TestBitmapIndexes(t *testing.T) {
	it := newIndexTest(t)
	ctx := context.Background()
	srv := it.r.srv

	// Admins only; member columns only; plain group columns.
	mustExec(t, it.admin, "CREATE USER rita WITH PASSWORD 'rita-password' ROLE reader")
	rita := connect(t, it.r.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)
	for q, code := range map[string]string{
		"ALTER TABLE lake.public.segments ADD BITMAP INDEX by_day ON (day) FOR segment_name":  "55000",
		"ALTER TABLE lake.public.segments ADD BITMAP INDEX by_day ON (nope) FOR person_id":     "42703",
		"ALTER TABLE lake.public.segments ADD BITMAP INDEX by_day ON (day, day) FOR person_id": "22023",
		"ALTER TABLE lake.public.segments ADD BITMAP INDEX by_day ON (person_id) FOR person_id": "22023",
		"ALTER TABLE lake.public.nope ADD BITMAP INDEX by_day ON (day) FOR person_id":          "42P01",
	} {
		if _, err := it.admin.Exec(ctx, q); pgCode(err) != code {
			t.Errorf("%s: %v, want %s", q, err, code)
		}
	}
	if _, err := rita.Exec(ctx, "ALTER TABLE lake.public.segments ADD BITMAP INDEX by_day ON (day) FOR person_id"); pgCode(err) != "42501" {
		t.Errorf("a reader's ADD BITMAP INDEX: %v", err)
	}

	it.exec(
		"ALTER TABLE lake.public.segments ADD BITMAP INDEX seg_day ON (segment_name, day) FOR person_id",
		"ALTER TABLE lake.public.segments ADD BITMAP INDEX everyone ON () FOR person_id",
	)
	if _, err := it.admin.Exec(ctx, "ALTER TABLE lake.public.segments ADD BITMAP INDEX seg_day ON (day) FOR person_id"); pgCode(err) != "42710" {
		t.Errorf("ADD again: %v", err)
	}
	it.exec("ALTER TABLE lake.public.segments ADD BITMAP INDEX IF NOT EXISTS seg_day ON (day) FOR person_id")

	// Built from the snapshot, and nothing to do until the table changes.
	srv.waitIndexBuilds(t)
	for _, name := range []string{"seg_day", "everyone"} {
		x := it.index(name)
		if how, err := srv.maintainIndex(ctx, x, 1); err != nil || how != "" && how != "rebuild" {
			t.Fatalf("building %s: %q, %v", name, how, err)
		}
		if how, err := srv.maintainIndex(ctx, x, 1); err != nil || how != "" {
			t.Errorf("building %s again: %q, %v", name, how, err)
		}
	}
	it.sameCounts("seg_day", "segment_name, day")
	it.sameCounts("everyone", "")

	// Appends of values that have ids add the appended files' bitmaps.
	it.exec("INSERT INTO lake.public.segments SELECT 'p' || (i % 250), 'seg' || (i % 9), CAST(i % 3 AS INTEGER) FROM range(3000) r(i)",
		"INSERT INTO lake.public.segments VALUES ('p7', 'seg1', 0), (NULL, 'seg_nobody', 1)")
	// The writes start the builds themselves.
	srv.waitIndexBuilds(t)
	for _, name := range []string{"seg_day", "everyone"} {
		if how := srv.lastBuild(name); how != "append" {
			t.Errorf("after appends, %s: %q", name, how)
		}
		if how, err := srv.maintainIndex(ctx, it.index(name), 1); err != nil || how != "" {
			t.Errorf("after appends, %s again: %q, %v", name, how, err)
		}
	}
	it.sameCounts("seg_day", "segment_name, day")
	it.sameCounts("everyone", "")

	// A write of new values, whose ids Igloo fills in after it, builds
	// them again.
	it.exec("INSERT INTO lake.public.segments VALUES ('newcomer', 'seg1', 0)")
	srv.waitIndexBuilds(t)
	if how := srv.lastBuild("everyone"); how != "rebuild" {
		t.Errorf("after new values: %q", how)
	}
	it.sameCounts("everyone", "")

	// A delete builds them again.
	it.exec("DELETE FROM lake.public.segments WHERE day = 2")
	if how, err := srv.maintainIndex(ctx, it.index("seg_day"), 1); err != nil || how != "rebuild" {
		t.Errorf("after a delete: %q, %v", how, err)
	}
	it.sameCounts("seg_day", "segment_name, day")

	// Rows that another engine wrote without ids wait for mapping.
	it.internal("INSERT INTO lake.public.segments (person_id, segment_name, day) VALUES ('raw', 'seg0', 0)")
	if how, err := srv.maintainIndex(ctx, it.index("seg_day"), 1); err != nil || how != "unmapped" {
		t.Errorf("with a row without its id: %q, %v", how, err)
	}
	srv.MapIDDomains(ctx)
	srv.MaintainBitmapIndexes(ctx)
	it.sameCounts("seg_day", "segment_name, day")
	it.sameCounts("everyone", "")

	// Listed for anyone; read and changed by no user.
	got := texts(t, rita, "SHOW BITMAP INDEXES")
	if len(got) != 2 || !strings.HasPrefix(got[0], "everyone|public.segments|person_id||people|") || !strings.HasPrefix(got[1], "seg_day|public.segments|person_id|segment_name, day|people|") ||
		!strings.Contains(got[1], "|true|") {
		t.Errorf("SHOW BITMAP INDEXES: %q", got)
	}
	for _, c := range []*pgx.Conn{it.admin, rita} {
		if _, err := c.Exec(ctx, "SELECT count(*) FROM lake.igloo_bitmaps.seg_day"); pgCode(err) != "42501" {
			t.Errorf("a user's read of the bitmaps: %v", err)
		}
	}
	for _, q := range []string{
		"INSERT INTO lake.igloo_bitmaps.seg_day SELECT * FROM lake.igloo_bitmaps.seg_day",
		"DROP TABLE lake.igloo_bitmaps.seg_day",
		"DROP ROW ACCESS POLICY igloo_bitmap_index",
		"ALTER TABLE lake.igloo_bitmaps.seg_day DROP ROW ACCESS POLICY igloo_bitmap_index",
		"ALTER TABLE lake.igloo_bitmaps.seg_day ADD BITMAP INDEX x ON () FOR bm",
	} {
		if _, err := it.admin.Exec(ctx, q); pgCode(err) != "42501" {
			t.Errorf("%s: %v", q, err)
		}
	}

	// The column keeps its domain while an index holds its ids.
	if _, err := it.admin.Exec(ctx, "ALTER TABLE lake.public.segments ALTER COLUMN person_id UNSET ID DOMAIN"); pgCode(err) != "2BP01" {
		t.Errorf("UNSET ID DOMAIN under an index: %v", err)
	}
	if _, err := it.admin.Exec(ctx, "ALTER TABLE lake.public.people DROP BITMAP INDEX seg_day"); pgCode(err) == "" {
		t.Error("dropped an index through another table")
	}
	it.exec("ALTER TABLE lake.public.segments DROP BITMAP INDEX seg_day", "ALTER TABLE lake.public.segments DROP BITMAP INDEX IF EXISTS seg_day")
	if got := it.internal("SELECT table_name FROM duckdb_tables() WHERE schema_name = 'igloo_bitmaps' ORDER BY ALL"); strings.Join(got, ",") != "everyone" {
		t.Errorf("index tables after DROP: %q", got)
	}
	it.exec("ALTER TABLE lake.public.segments DROP BITMAP INDEX everyone",
		"ALTER TABLE lake.public.segments ALTER COLUMN person_id UNSET ID DOMAIN")
}

// index returns the bitmap index named name from the store.
func (it *idTest) index(name string) iddomain.BitmapIndex {
	it.t.Helper()
	snap, err := it.r.srv.idDomains.Load(context.Background())
	if err != nil {
		it.t.Fatal(err)
	}
	x := snap.Index(name)
	if x == nil {
		it.t.Fatalf("no bitmap index %s", name)
	}
	return *x
}

// waitIndexBuilds waits for the bitmap index builds running in the
// background to end.
// TestBitmapIndexBuildRetries builds an index again a while after its
// build failed, a few times, then leaves it to maintenance.
func TestBitmapIndexBuildRetries(t *testing.T) {
	it := newIndexTest(t)
	srv := it.r.srv
	defer func(d time.Duration) { indexRetry = d }(indexRetry)
	indexRetry = 10 * time.Millisecond
	it.exec("ALTER TABLE lake.public.segments ADD BITMAP INDEX broken ON (segment_name) FOR person_id")
	srv.waitIndexBuilds(t)
	// Its table gone behind Igloo's back, every build after a write
	// (which starts one, history or not) fails.
	it.internal("DROP TABLE lake.igloo_bitmaps.broken")
	it.exec("INSERT INTO lake.public.segments VALUES ('p1', 'seg1', 1)")
	deadline := time.Now().Add(time.Minute)
	for {
		srv.indexBuilds.mu.Lock()
		n := srv.indexBuilds.failed["broken"]
		srv.indexBuilds.mu.Unlock()
		if n == indexRetries+1 {
			break
		}
		if n > indexRetries+1 || time.Now().After(deadline) {
			t.Fatalf("%d failed builds, want the first and %d more", n, indexRetries)
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(100 * indexRetry)
	srv.waitIndexBuilds(t)
	srv.indexBuilds.mu.Lock()
	n := srv.indexBuilds.failed["broken"]
	srv.indexBuilds.mu.Unlock()
	if n != indexRetries+1 {
		t.Errorf("%d failed builds after the retries ran out, want %d", n, indexRetries+1)
	}
}

// lastBuild is how index name was last built in the background.
func (s *Server) lastBuild(name string) string {
	s.indexBuilds.mu.Lock()
	defer s.indexBuilds.mu.Unlock()
	return s.indexBuilds.last[name]
}

func (s *Server) waitIndexBuilds(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		s.indexBuilds.mu.Lock()
		n := len(s.indexBuilds.running)
		s.indexBuilds.mu.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("bitmap index builds still running after a minute")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
