package distributed

import (
	"bytes"
	"context"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// valueEnv is the harness's Env with tables only the session has, which
// stand in for bitmap indexes' tables: the coordinator computes reads of
// them for workers. The session has no time travel, and its tables one
// snapshot, which a read at any version reads.
type valueEnv struct {
	harnessEnv
	private map[string]bool
}

var atVersion = regexp.MustCompile(`(?i)\s+AT\s*\(\s*VERSION\s*=>\s*\d+\s*\)`)

func (e valueEnv) Computes(_ context.Context, parts []string) (bool, error) {
	return e.private[strings.ToLower(parts[len(parts)-1])], nil
}

func (e valueEnv) Describe(ctx context.Context, query string) ([]Column, error) {
	return e.harnessEnv.Describe(ctx, atVersion.ReplaceAllString(query, ""))
}

func (e valueEnv) Value(ctx context.Context, query string) (any, error) {
	var v any
	err := e.h.session.QueryRowContext(ctx, atVersion.ReplaceAllString(query, "")).Scan(&v)
	return v, err
}

// runValues plans and executes query with env, and returns its rows next
// to those of running it on one machine.
func (h *harness) runValues(env valueEnv, query string) (got, want [][]any, plan *Plan, err error) {
	h.t.Helper()
	ctx := context.Background()
	want, wantCols, err := h.query(ctx, atVersion.ReplaceAllString(query, ""))
	if err != nil {
		h.t.Fatalf("%s\nlocal: %v", short(query), err)
	}
	plan, err = h.co.Plan(ctx, query, env, ModeOn)
	if err != nil {
		return nil, want, nil, err
	}
	ex, err := h.co.Execute(ctx, plan, h.settings(ctx))
	if err != nil {
		return nil, want, plan, err
	}
	defer ex.Close()
	got, gotCols, err := h.query(ctx, atVersion.ReplaceAllString(ex.SQL, ""))
	if err == nil && !slices.Equal(gotCols, wantCols) {
		h.t.Errorf("%s\ncolumns: got %v, want %v", short(query), gotCols, wantCols)
	}
	return got, want, plan, err
}

// The coordinator computes a scalar subquery over a table workers may not
// read, at a pinned snapshot, and the stages read its value: in WHERE, in
// a select list the coordinator's part computes (under its own name), as
// a BLOB, which crosses through object storage. Workers never see the
// table. A subquery that refers to the statement around it, a statement
// that reads variables itself, an Env that vouches for nothing, and a
// value over maxValueBytes keep the statement on one machine.
func TestComputedValues(t *testing.T) {
	h := newHarness(t, 0.01, 4, 2)
	h.exec(`CREATE TABLE cutoffs AS SELECT * FROM (VALUES ('a', 1000::BIGINT), ('b', 30000::BIGINT)) AS v(name, k)`)
	defer func(n int) { maxValueBytes = n }(maxValueBytes)
	maxValueBytes = 1 << 20
	h.exec(`CREATE TABLE blobs AS SELECT 'small' AS name, encode(repeat('x', 100000)) AS b UNION ALL
		SELECT 'large', encode(repeat('y', ` + strconv.Itoa(maxValueBytes+1) + `))`)
	env := valueEnv{harnessEnv{h}, map[string]bool{"cutoffs": true, "blobs": true}}

	ok := []string{
		`SELECT l_returnflag, count(*) AS n, sum(l_quantity) AS q FROM lineitem
			WHERE l_orderkey < (SELECT max(k) FROM cutoffs AT (VERSION => 3) WHERE name = 'b') GROUP BY 1 ORDER BY 1`,
		`SELECT l_returnflag, count(*) AS n, (SELECT min(k) FROM main.cutoffs AT (VERSION => 3)) AS cutoff FROM lineitem
			GROUP BY 1 ORDER BY 1`,
		`SELECT count(*) AS n FROM lineitem
			WHERE l_orderkey * 10 < octet_length((SELECT b FROM blobs AT (VERSION => 1) WHERE name = 'small'))`,
	}
	for _, q := range ok {
		got, want, plan, err := h.runValues(env, q)
		if err != nil {
			t.Fatalf("%s: %v", short(q), err)
		}
		if msg := compareRows(want, got, true); msg != "" {
			t.Errorf("%s\n%s\n%s", short(q), msg, plan.describe())
		}
		if len(plan.values) != 1 {
			t.Errorf("%s: %d values computed, want 1", short(q), len(plan.values))
		}
		for _, s := range plan.Stages {
			if strings.Contains(s.SQL, "cutoffs") || strings.Contains(s.SQL, "blobs") {
				t.Errorf("%s: stage %d reads the table itself: %s", short(q), s.ID, s.SQL)
			}
		}
	}

	// The statement is known by its subquery: its string literals aside,
	// as ever, not by the variable.
	key := func(q string) string {
		p, err := h.co.Plan(context.Background(), q, env, ModeOn)
		if err != nil {
			t.Fatalf("%s: %v", short(q), err)
		}
		return p.Key
	}
	base := `SELECT count(*) FROM lineitem WHERE l_orderkey < (SELECT %s(k) FROM cutoffs AT (VERSION => 3) WHERE name = '%s')`
	if key(fmt.Sprintf(base, "max", "a")) != key(fmt.Sprintf(base, "max", "b")) {
		t.Error("a string literal in the subquery changed the plan key")
	}
	if key(fmt.Sprintf(base, "max", "a")) == key(fmt.Sprintf(base, "min", "a")) {
		t.Error("another subquery has the same plan key")
	}

	rows, err := h.co.Explain(context.Background(), ok[0], env, ModeOn)
	if err != nil {
		t.Fatal(err)
	}
	explained := false
	for _, r := range rows {
		if r[0] == "computed first" && strings.Contains(r[1], valuePrefix+"0 BIGINT: (SELECT max(k)") {
			explained = true
		}
	}
	if !explained {
		t.Errorf("EXPLAIN DISTRIBUTED does not say what is computed first: %v", rows)
	}

	local := []struct {
		env   Env
		query string
	}{
		{env, `SELECT count(*) FROM lineitem WHERE l_orderkey < (SELECT max(k) FROM cutoffs AT (VERSION => 3) WHERE name = l_returnflag)`},
		{env, `SELECT count(*) FROM lineitem WHERE l_orderkey < (SELECT max(k) FROM cutoffs AT (VERSION => 3)) + getvariable('x')`},
		{env, `SELECT count(*) FROM lineitem WHERE l_orderkey < (SELECT max(k) FROM cutoffs)`},
		{harnessEnv{h}, ok[0]},
	}
	for _, c := range local {
		if _, err := h.co.Plan(context.Background(), c.query, c.env, ModeOn); !IsUnsupported(err) {
			t.Errorf("%s: planned with %v, want it kept on one machine", short(c.query), err)
		}
	}
	large := strings.ReplaceAll(ok[2], "'small'", "'large'")
	if _, _, _, err := h.runValues(env, large); !IsUnsupported(err) || !strings.Contains(err.Error(), "bytes") {
		t.Errorf("a value over the cap: %v, want it kept on one machine", err)
	}

	// The BLOBs written for the tasks are removed with the statement's
	// exchanges.
	deadline := time.Now().Add(10 * time.Second)
	for {
		h.removedMu.Lock()
		removed := slices.ContainsFunc(h.removed, func(d string) bool { return strings.HasSuffix(d, "-values") })
		h.removedMu.Unlock()
		if removed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the values written for tasks were not removed")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// With the rb extension, a cohort's bitmap that the coordinator computes
// filters the rows workers read, as a statement answered from bitmap
// indexes would.
func TestComputedBitmap(t *testing.T) {
	rb := os.Getenv("IGLOO_TEST_RB_EXTENSION")
	if rb == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("IGLOO_TEST_RB_EXTENSION is not set: CI builds the rb extension (extensions/rb/build.sh)")
		}
		t.Skip("IGLOO_TEST_RB_EXTENSION is not set; build the rb extension with extensions/rb/build.sh")
	}
	h := newHarnessRB(t, 0.01, 4, 2, rb)
	h.exec(`CREATE TABLE cohorts AS SELECT o_orderpriority AS name, rb_agg(CAST(o_orderkey AS UINTEGER)) AS bm FROM orders GROUP BY 1`)
	env := valueEnv{harnessEnv{h}, map[string]bool{"cohorts": true}}
	q := `SELECT l_shipmode, count(*) AS n, count(DISTINCT l_orderkey) AS orders FROM lineitem
		WHERE rb_contains((SELECT rb_or_agg(bm) FROM cohorts AT (VERSION => 2) WHERE name LIKE '1%' OR name LIKE '2%'),
			CAST(l_orderkey AS UINTEGER))
		GROUP BY 1 ORDER BY 1`
	got, want, plan, err := h.runValues(env, q)
	if err != nil {
		t.Fatal(err)
	}
	if msg := compareRows(want, got, true); msg != "" {
		t.Errorf("%s\n%s", msg, plan.describe())
	}
	if len(plan.values) != 1 || plan.values[0].typ != "BLOB" {
		t.Errorf("computed %v, want one BLOB", plan.values)
	}
}

// Tasks set only the variables of computed values, of the types those may
// have, and read a BLOB only from where the worker may read.
func TestSetVariables(t *testing.T) {
	h := newHarness(t, 0.01, 1, 1)
	w := h.workers[0]
	conn, err := w.opts.Engine.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	file := filepath.Join(h.dir, "warehouse", "v")
	if err := os.WriteFile(file, []byte{0, 1, 2}, 0o600); err != nil {
		t.Fatal(err)
	}
	good := []variable{
		{Name: valuePrefix + "0", Type: "BIGINT", Text: "42"},
		{Name: valuePrefix + "1", Type: "BLOB", File: file},
		{Name: valuePrefix + "2", Type: "VARCHAR", Null: true},
	}
	if err := w.setVariables(context.Background(), conn, good); err != nil {
		t.Fatal(err)
	}
	rows, err := conn.QueryContext(context.Background(),
		"SELECT getvariable('"+valuePrefix+"0'), getvariable('"+valuePrefix+"1'), getvariable('"+valuePrefix+"2')", nil)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]driver.Value, 3)
	err = rows.Next(got)
	_ = rows.Close()
	if err != nil || got[0] != int64(42) || !bytes.Equal(got[1].([]byte), []byte{0, 1, 2}) || got[2] != nil {
		t.Errorf("variables set to %v, %v", got, err)
	}
	for _, bad := range []variable{
		{Name: "x", Type: "BIGINT", Text: "1"},
		{Name: valuePrefix + "0", Type: "STRUCT(a INTEGER)", Null: true},
		{Name: valuePrefix + "0", Type: "BLOB", File: filepath.Join(h.dir, "elsewhere")},
		{Name: valuePrefix + "0", Type: "BIGINT", File: file},
	} {
		if err := w.setVariables(context.Background(), conn, []variable{bad}); err == nil {
			t.Errorf("set %+v", bad)
		}
	}
}

// A table read at a pinned snapshot, AT (VERSION => n), runs on workers
// when n is the snapshot Execute loads, which workers read; at another, it
// stays on one machine, and so does a statement that reads two snapshots
// of a table or reads one at a time. The snapshot is not part of the plan
// key. With the cohort a subquery computes, this is the shape of a
// statement Igloo answers from bitmap indexes that scans the indexed table.
func TestPinnedVersions(t *testing.T) {
	h := newHarness(t, 0.01, 4, 2)
	for name, snapshot := range map[string]int64{"lineitem": 7, "customer": 5, "orders": 9} {
		h.tables[name].Versioned, h.tables[name].Snapshot = true, snapshot
	}
	h.exec(`CREATE TABLE cutoffs AS SELECT * FROM (VALUES ('a', 1000::BIGINT), ('b', 30000::BIGINT)) AS v(name, k)`)
	env := valueEnv{harnessEnv{h}, map[string]bool{"cutoffs": true}}
	ok := []string{
		`SELECT l_returnflag, count(*) AS n, sum(l_quantity) AS q FROM lineitem AT (VERSION => 7) GROUP BY 1 ORDER BY 1`,
		`SELECT l_returnflag, count(*) AS n FROM lineitem AT (VERSION => 7)
			WHERE l_orderkey < (SELECT max(k) FROM cutoffs AT (VERSION => 3) WHERE name = 'b') GROUP BY 1 ORDER BY 1`,
		`SELECT o_orderpriority, count(*) AS n FROM orders AT (VERSION => 9)
			WHERE o_custkey IN (SELECT c_custkey FROM customer AT (VERSION => 5) WHERE c_mktsegment = 'BUILDING') GROUP BY 1 ORDER BY 1`,
	}
	for _, q := range ok {
		got, want, plan, err := h.runValues(env, q)
		if err != nil {
			t.Fatalf("%s: %v", short(q), err)
		}
		if msg := compareRows(want, got, true); msg != "" {
			t.Errorf("%s\n%s\n%s", short(q), msg, plan.describe())
		}
		for _, s := range plan.Stages {
			if strings.Contains(s.SQL, "VERSION") {
				t.Errorf("%s: stage %d reads at a snapshot: %s", short(q), s.ID, s.SQL)
			}
		}
	}

	moved := strings.ReplaceAll(ok[0], "VERSION => 7", "VERSION => 6")
	if _, _, _, err := h.runValues(env, moved); !IsUnsupported(err) || !strings.Contains(err.Error(), "snapshot 6") {
		t.Errorf("a pin to another snapshot: %v, want it kept on one machine", err)
	}
	p7, err := h.co.Plan(context.Background(), ok[0], env, ModeOn)
	if err != nil {
		t.Fatal(err)
	}
	p6, err := h.co.Plan(context.Background(), moved, env, ModeOn)
	if err != nil {
		t.Fatal(err)
	}
	if p7.Key != p6.Key {
		t.Error("the snapshot a pin names changed the plan key")
	}
	for _, q := range []string{
		`SELECT count(*) FROM lineitem AT (VERSION => 7) AS a JOIN lineitem AT (VERSION => 6) AS b ON a.l_orderkey = b.l_orderkey`,
		`SELECT count(*) FROM lineitem AT (TIMESTAMP => TIMESTAMP '2026-01-01')`,
	} {
		if _, err := h.co.Plan(context.Background(), q, env, ModeOn); !IsUnsupported(err) {
			t.Errorf("%s: planned with %v, want it kept on one machine", short(q), err)
		}
	}
}

// A report's cohort bitmaps, which the session holds in variables while
// the statement runs, cross to the stages as computed values, one a name
// however often read, and the coordinator's own part reads the variables.
func TestBitmapVariables(t *testing.T) {
	h := newHarness(t, 0.01, 3, 2)
	env := valueEnv{harnessEnv{h}, nil}
	const cohort = "__igloo_bitmap_0123456789abcdef01234567"
	h.exec(`SET VARIABLE ` + cohort + ` = encode(repeat('x', 30000))`)
	q := `SELECT l_returnflag, count(*) AS n FROM lineitem
		WHERE l_orderkey < octet_length(coalesce(getvariable('` + cohort + `'), CAST('gone' AS BLOB)))
		AND l_partkey * 10 < octet_length(getvariable('` + cohort + `')) GROUP BY 1 ORDER BY 1`
	got, want, plan, err := h.runValues(env, q)
	if err != nil {
		t.Fatal(err)
	}
	if msg := compareRows(want, got, true); msg != "" {
		t.Errorf("%s\n%s", msg, plan.describe())
	}
	if len(plan.values) != 1 || plan.values[0].typ != "BLOB" {
		t.Errorf("computed %v, want one BLOB", plan.values)
	}
	for _, s := range plan.Stages {
		if strings.Contains(s.SQL, "__igloo_bitmap_") {
			t.Errorf("stage %d reads the session's variable: %s", s.ID, s.SQL)
		}
	}
	// A variable of another name keeps the statement here.
	other := strings.ReplaceAll(q, cohort, "__igloo_bitmap_short")
	h.exec(`SET VARIABLE __igloo_bitmap_short = encode('y')`)
	if _, err := h.co.Plan(context.Background(), other, env, ModeOn); !IsUnsupported(err) {
		t.Errorf("another variable: %v, want it kept on one machine", err)
	}
}

// TestCohortsFromBitmaps joins a divided table to cohorts listed from
// bitmaps the coordinator computes first, as a report call answered from
// bitmap indexes does: each cohort's members, unnested from its bitmap
// under a check that reads a small table, in common table expressions read
// twice, through a UNION ALL of their labels. Every task lists them itself
// beside its share of the table, which one stage divides among the
// workers, with no stage of their own, and the rows are one machine's, the
// counts of distinct members in bitmaps too. A cohort that calls a volatile
// function, which each task would call again, is not listed so.
func TestCohortsFromBitmaps(t *testing.T) {
	rb := os.Getenv("IGLOO_TEST_RB_EXTENSION")
	if rb == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("IGLOO_TEST_RB_EXTENSION is not set: CI builds the rb extension (extensions/rb/build.sh)")
		}
		t.Skip("IGLOO_TEST_RB_EXTENSION is not set; build the rb extension with extensions/rb/build.sh")
	}
	h := newHarnessRB(t, 0.01, 3, 3, rb)
	h.addNestedTable("media_events", `(SELECT i AS person_id, DATE '2026-01-01' + (i % 30)::INTEGER AS day,
		['drama', 'news', 'sports', NULL][1 + i % 4] AS genre, (i % 3600)::BIGINT AS seconds,
		['kw' || (i % 40), 'kw' || (i % 7)] AS keywords FROM range(300000) t(i))`, 6,
		[]Column{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}, {ID: 5, Nested: &Nesting{Kind: "list", Fields: []Field{{Name: "element", ID: 6}}}}})
	h.addTable("audience_members", `(SELECT a AS audience_id, r AS revision, (i * 7 + a) % 150000 AS person_id
		FROM range(4) x(a), range(1, 3) y(r), range(20000) t(i))`, 3)
	h.addTable("audience_revisions", `(SELECT a AS audience_id, r AS revision FROM range(4) x(a), range(1, 3) y(r))`, 1)
	const b1, b2 = "__igloo_bitmap_0123456789abcdef01234567", "__igloo_bitmap_89abcdef0123456789abcdef"
	h.exec(`SET VARIABLE ` + b1 + ` = (SELECT rb_agg(CAST(person_id AS UINTEGER)) FROM audience_members WHERE audience_id = 1 AND revision = 2)`)
	h.exec(`SET VARIABLE ` + b2 + ` = (SELECT rb_agg(CAST(person_id AS UINTEGER)) FROM audience_members WHERE audience_id = 2 AND revision = 2)`)
	env := valueEnv{harnessEnv{h}, nil}
	on := true
	h.co.bitmaps = &on
	defer func() { h.co.bitmaps = nil }()
	cohort := func(v string, aud int, extra string) string {
		return `SELECT unnest(rb_to_list(x.bm)) AS member FROM (SELECT CASE WHEN getvariable('` + v + `') IS NULL THEN CAST('gone' AS BLOB)
			WHEN (SELECT max(r.revision) FROM audience_revisions AS r WHERE r.audience_id = ` + strconv.Itoa(aud) + `) IS DISTINCT FROM 2 THEN CAST('changed' AS BLOB)
			ELSE getvariable('` + v + `') END AS bm` + extra + `) AS x`
	}
	report := func(c1, c2 string) string {
		return `WITH cohort_1 AS (` + c1 + `), cohort_2 AS (` + c2 + `),
cohorts AS (SELECT 'one' AS cohort, member FROM cohort_1 UNION ALL SELECT 'two' AS cohort, member FROM cohort_2),
sizes AS (SELECT cohort, count(DISTINCT member) AS cohort_size FROM cohorts GROUP BY cohort),
"rows" AS (SELECT c.cohort AS cohort, CAST(s.person_id AS UINTEGER) AS member, s.seconds AS g0, unnest(list_concat([s.genre], s.keywords)) AS v
	FROM media_events AS s JOIN cohorts AS c ON c.member = CAST(s.person_id AS UINTEGER)),
counts AS (SELECT v, cohort, count(DISTINCT member) AS people, count(*) AS n, sum(g0) AS seconds FROM "rows" GROUP BY ALL)
SELECT n.v, n.cohort, n.people, n.n, n.seconds, z.cohort_size FROM counts AS n JOIN sizes AS z ON z.cohort = n.cohort ORDER BY 1, 2`
	}
	q := report(cohort(b1, 1, ""), cohort(b2, 2, ""))
	for _, mode := range []string{aggGather, aggByGroup, ""} {
		h.co.aggregation = mode
		got, want, plan, err := h.runValues(env, q)
		if err != nil {
			t.Fatalf("%q: %v", mode, err)
		}
		if msg := compareRows(want, got, true); msg != "" {
			t.Errorf("%q: %s\n%s", mode, msg, plan.describe())
		}
		splits := 0
		for _, s := range plan.Stages {
			if in := s.split(); in != nil {
				if in.Table.Name != "media_events" {
					t.Errorf("%q: a stage of its own divides %s:\n%s", mode, in.Table, plan.describe())
				}
				splits++
			}
			if !strings.Contains(s.SQL, "rb_to_list(") && s.split() != nil {
				t.Errorf("%q: stage %d does not list the cohorts: %s", mode, s.ID, s.SQL)
			}
		}
		if splits != 1 || len(plan.values) != 2 || !strings.Contains(plan.describe(), "rb_agg(") {
			t.Errorf("%q: %d stages divide media_events, %d values computed first, want 1 and 2, bitmaps:\n%s", mode, splits, len(plan.values), plan.describe())
		}
	}
	h.co.aggregation = ""
	// random() in a cohort: each task would draw again.
	volatile := report(cohort(b1, 1, ", random() AS r"), cohort(b2, 2, ""))
	if plan, err := h.co.Plan(context.Background(), volatile, env, ModeOn); err == nil && strings.Contains(plan.describe(), "random()") {
		t.Errorf("a cohort that calls random() is listed by every task:\n%s", plan.describe())
	}
}
