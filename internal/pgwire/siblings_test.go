package pgwire

import (
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// siblingTables are the tables sibling tests read: m, of members' rows by day, and
// c, a cohort of some of the members, a few twice.
var siblingTables = []string{
	`CREATE TABLE m AS SELECT i AS id, (i * 7919) % 900 AS member, (i % 30) AS day,
		CASE WHEN i % 17 = 0 THEN NULL ELSE ['ash', 'birch', 'cedar', 'dogwood', 'elm'][1 + (i * 3) % 5] END AS kind,
		CASE WHEN i % 31 = 0 THEN NULL ELSE ((i * 37) % 41) * 0.25 END::DOUBLE AS v,
		(((i * 13) % 997) / 4)::DECIMAL(10, 2) AS d, (i % 10) AS h FROM range(40000) t(i)`,
	`CREATE TABLE c AS SELECT DISTINCT (i * 13) % 900 AS member FROM range(300) t(i)
		UNION ALL SELECT i AS member FROM range(5) t(i)`,
}

func siblingDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	for _, s := range siblingTables {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// siblingRows returns q's rows as text, one row a line, in order.
func siblingRows(t *testing.T, db *sql.DB, q string) ([]string, error) {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out []string
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		out = append(out, fmt.Sprint(vals...))
	}
	return out, rows.Err()
}

// TestMergeSiblings checks that a report's sibling aggregations over one join
// become one materialized grouping that each reads, with the rows the statement
// as written returns; and that what may not be merged stays as written.
func TestMergeSiblings(t *testing.T) {
	db := siblingDB(t)
	vol := sharedVolatile(t, db)
	report := `SELECT * FROM (WITH cohort AS (SELECT member FROM c),
		seen AS (SELECT DISTINCT m.member AS member, m.kind AS kind FROM m JOIN cohort ON cohort.member = m.member WHERE m.day >= 10 AND m.kind IS NOT NULL),
		days AS (SELECT m.member AS member, m.kind AS kind, count(DISTINCT m.day) AS days FROM m JOIN cohort ON cohort.member = m.member
			WHERE m.day >= 10 AND m.kind IS NOT NULL GROUP BY m.member, m.kind),
		views AS (SELECT m.member, count(*) AS views, sum(m.v) AS total, avg(m.d) AS mean FROM m JOIN cohort ON cohort.member = m.member
			WHERE m.day >= 10  AND m.kind IS NOT NULL GROUP BY m.kind, m.member)
		SELECT seen.kind, count(*) AS people, sum(days.days) AS days, sum(views.views) AS views, sum(views.total) AS total, round(sum(views.mean), 6) AS mean
		FROM seen JOIN days ON days.member = seen.member AND days.kind = seen.kind JOIN views ON views.member = seen.member
		GROUP BY seen.kind ORDER BY seen.kind) AS t0 LIMIT 20`
	out, err := mergeSiblingText(report, vol)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(out, siblingPrefix+"1 AS MATERIALIZED") != 1 || strings.Count(out, "FROM "+siblingPrefix+"1") != 3 {
		t.Fatalf("not merged:\n%s", out)
	}
	want, err := siblingRows(t, db, report)
	if err != nil {
		t.Fatal(err)
	}
	got, err := siblingRows(t, db, out)
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") || len(want) == 0 {
		t.Errorf("rows\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if ctes, _ := planCTEs(t, db, out); !slices.Contains(ctes, siblingPrefix+"1") {
		t.Errorf("the merged grouping is not materialized: %v", ctes)
	}

	quoted := `WITH a AS (SELECT DISTINCT "M"."member" AS member, "M".kind AS kind FROM m AS "M" WHERE "M".h = 1),
		b AS (SELECT "M"."member" AS member, "M".kind AS kind, count(DISTINCT "M"."day") AS n FROM m AS "M" WHERE "M".h = 1 GROUP BY "M"."member", "M".kind),
		c AS (SELECT "M"."member" AS member, "M".kind AS kind, count(*) AS v FROM m AS "M" WHERE "M".h = 1 GROUP BY "M".kind, "M"."member")
		SELECT count(*), sum(n), sum(v) FROM a JOIN b USING (member, kind) JOIN c USING (member, kind)`
	out, err = mergeSiblingText(quoted, vol)
	if err != nil || !strings.Contains(out, siblingPrefix+"1 AS MATERIALIZED") {
		t.Fatalf("quoted names: not merged (%v):\n%s", err, out)
	}
	want, _ = siblingRows(t, db, quoted)
	if got, err := siblingRows(t, db, out); err != nil || strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("quoted names: %v %v, want %v", got, err, want)
	}

	base := `WITH a AS (SELECT DISTINCT m.member AS member FROM m WHERE m.h = 1),
		b AS (SELECT m.member AS member, count(*) AS n FROM m WHERE m.h = 1 GROUP BY m.member)
		SELECT count(*), sum(n) FROM a JOIN b USING (member)`
	// Two siblings stay as written; the guards below are checked on two.
	if out, _ := mergeSiblingText(base, vol); out != "" {
		t.Fatalf("two siblings merged: %s", out)
	}
	defer func(n int) { minSiblings = n }(minSiblings)
	minSiblings = 2
	if out, _ := mergeSiblingText(base, vol); out == "" {
		t.Fatalf("not merged: %s", base)
	}
	for name, q := range map[string]string{
		"another filter":   strings.Replace(base, "WHERE m.h = 1 GROUP", "WHERE m.h = 2 GROUP", 1),
		"another group":    strings.Replace(base, "GROUP BY m.member", "GROUP BY m.member, m.h", 1),
		"having":           strings.Replace(base, "GROUP BY m.member", "GROUP BY m.member HAVING count(*) > 1", 1),
		"limit":            strings.Replace(base, "WHERE m.h = 1),", "WHERE m.h = 1 LIMIT 5),", 1),
		"unnamed":          strings.Replace(base, "count(*) AS n", "count(*)", 1),
		"ordered":          strings.Replace(base, "count(*) AS n", "string_agg(m.kind, ',' ORDER BY m.kind) AS n", 1),
		"filtered":         strings.Replace(base, "count(*) AS n", "count(*) FILTER (WHERE m.day > 3) AS n", 1),
		"volatile":         strings.ReplaceAll(base, "m.h = 1", "m.h = floor(random() * 0)"),
		"parameter":        strings.ReplaceAll(base, "m.h = 1", "m.h = ?"),
		"materialized":     strings.Replace(base, "a AS (", "a AS MATERIALIZED (", 1),
		"column list":      strings.Replace(base, "a AS (", "a(member) AS (", 1),
		"recursive":        "WITH RECURSIVE " + strings.TrimPrefix(base, "WITH "),
		"defined later":    strings.Replace(strings.ReplaceAll(base, "FROM m WHERE", "FROM later m WHERE"), "SELECT count(*)", ", later AS (SELECT * FROM m) SELECT count(*)", 1),
		"distinct grouped": strings.Replace(base, "SELECT m.member AS member, count(*)", "SELECT DISTINCT m.member AS member, count(*)", 1),
	} {
		if out, err := mergeSiblingText(q, vol); err != nil || out != "" {
			t.Errorf("%s: merged (%v):\n%s", name, err, out)
		}
	}
}

// siblingGen writes random statements with sibling aggregations.
type siblingGen struct{ r *rand.Rand }

func (g *siblingGen) n(lo, hi int) int              { return lo + g.r.IntN(hi-lo+1) }
func (g *siblingGen) pick(options ...string) string { return options[g.r.IntN(len(options))] }

// statement is a WITH list of 2 to 4 siblings over one join and filter, and a
// select that reads each one's rows whole, as text in a fixed order.
func (g *siblingGen) statement() string {
	from := g.pick("m", "m JOIN c ON c.member = m.member", "m JOIN (SELECT DISTINCT member FROM c) c ON c.member = m.member",
		"m LEFT JOIN c ON c.member = m.member AND m.h > 4")
	conds := []string{"m.day >= 10", "m.kind IS NOT NULL", "m.h < 7", "m.v > 2", "trim(m.kind) <> 'elm'", "m.d BETWEEN 10 AND 200"}
	where := ""
	if g.n(0, 3) > 0 {
		var w []string
		for range g.n(1, 2) {
			w = append(w, conds[g.r.IntN(len(conds))])
		}
		where = " WHERE " + strings.Join(w, " AND ")
	}
	keyOptions := []string{"m.member", "m.kind", "m.h", "(m.day % 4)", "upper(m.kind)"}
	g.r.Shuffle(len(keyOptions), func(i, j int) { keyOptions[i], keyOptions[j] = keyOptions[j], keyOptions[i] })
	keys := keyOptions[:g.n(1, 3)]
	aggs := []string{"count(*)", "count(DISTINCT m.day)", "sum(m.v)", "min(m.kind)", "max(m.d)", "count(m.v)", "sum(m.d)", "count(DISTINCT m.kind)"}
	var ctes, reads []string
	siblings := g.n(2, 4)
	for s := range siblings {
		name := fmt.Sprintf("s%d", s)
		var items []string
		if g.n(0, 2) == 0 {
			for k, key := range keys {
				items = append(items, fmt.Sprintf("%s AS k%d", key, k))
			}
			ctes = append(ctes, name+" AS (SELECT DISTINCT "+strings.Join(items, ", ")+" FROM "+from+where+")")
		} else {
			for k, key := range keys {
				if g.n(0, 4) > 0 {
					items = append(items, fmt.Sprintf("%s AS k%d", key, k))
				}
			}
			for a := range g.n(1, 3) {
				items = append(items, fmt.Sprintf("%s AS a%d", aggs[g.r.IntN(len(aggs))], a))
			}
			group := append([]string(nil), keys...)
			g.r.Shuffle(len(group), func(i, j int) { group[i], group[j] = group[j], group[i] })
			ctes = append(ctes, name+" AS (SELECT "+strings.Join(items, ", ")+" FROM "+from+where+" GROUP BY "+strings.Join(group, ", ")+")")
		}
		reads = append(reads, fmt.Sprintf("(SELECT count(*) || ':' || coalesce(string_agg(%s::VARCHAR, ';' ORDER BY %s::VARCHAR), '') FROM %s) AS r%d", name, name, name, s))
	}
	return "WITH " + strings.Join(ctes, ", ") + " SELECT " + strings.Join(reads, ", ")
}

// TestMergeSiblingsRandom runs random statements with sibling aggregations
// (IGLOO_TEST_RANDOM_QUERIES, 300 by default, from IGLOO_TEST_RANDOM_SEED) as
// written and merged, and fails on any whose rows differ.
func TestMergeSiblingsRandom(t *testing.T) {
	queries, seed := 300, uint64(1)
	if v := os.Getenv("IGLOO_TEST_RANDOM_QUERIES"); v != "" {
		queries, _ = strconv.Atoi(v)
	}
	if v := os.Getenv("IGLOO_TEST_RANDOM_SEED"); v != "" {
		seed, _ = strconv.ParseUint(v, 10, 64)
	}
	db := siblingDB(t)
	vol := sharedVolatile(t, db)
	// From two siblings, so that every statement checks the merge.
	defer func(n int) { minSiblings = n }(minSiblings)
	minSiblings = 2
	g := &siblingGen{r: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
	merged := 0
	for i := range queries {
		q := g.statement()
		out, err := mergeSiblingText(q, vol)
		if err != nil {
			t.Fatalf("query %d: %v\n%s", i, err, q)
		}
		if out == "" {
			continue
		}
		merged++
		want, err := siblingRows(t, db, q)
		if err != nil {
			t.Fatalf("query %d as written: %v\n%s", i, err, q)
		}
		got, err := siblingRows(t, db, out)
		if err != nil {
			t.Errorf("query %d merged: %v\n%s\n%s", i, err, q, out)
			continue
		}
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Errorf("query %d: rows differ\n%s\nmerged\n%s", i, q, out)
		}
	}
	if merged < queries/2 {
		t.Errorf("merged %d of %d", merged, queries)
	}
	t.Logf("seed %d: %d of %d statements merged, all as written", seed, merged, queries)
}
