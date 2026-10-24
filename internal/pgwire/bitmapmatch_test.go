package pgwire

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
)

func TestMatchIndexCounts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	eng, err := engine.Open(ctx, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close() //nolint:errcheck
	conn, err := eng.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	audience := "WITH AUDIENCE AS (SELECT PERSON_ID FROM AUDIENCE_MEMBERS WHERE audience_id = 'a' AND ver = (SELECT max(ver) FROM AUDIENCE_REVISIONS WHERE audience_id = 'a')) "
	for _, c := range []struct {
		query string
		// want is the matched counts, as table(groups)[&set(groups)], or
		// the miss.
		want string
	}{
		{"SELECT segment_name, count(DISTINCT person_id) AS n FROM segments GROUP BY segment_name",
			"segments(segment_name)"},
		{"SELECT S.SEGMENT_NAME, count(DISTINCT S.PERSON_ID) FROM PERSON_SEGMENTS AS S WHERE S.SEGMENT_NAME LIKE 'SEG_%' AND S.day > 3 GROUP BY S.SEGMENT_NAME ORDER BY 2 DESC LIMIT 10",
			"person_segments(day,segment_name)"},
		{"SELECT count(DISTINCT CAST(s._igloo_id_person_id AS UINTEGER)) FROM lake.public.segments AS s",
			"lake.public.segments()"},
		{audience + "SELECT S.SEGMENT_NAME, count(DISTINCT S.PERSON_ID) FROM PERSON_SEGMENTS AS S INNER JOIN AUDIENCE AS A ON A.PERSON_ID = S.PERSON_ID WHERE S.SEGMENT_NAME LIKE 'SEG_HH_%' GROUP BY S.SEGMENT_NAME",
			"person_segments(segment_name)&audience_members(audience_id,ver)"},
		{audience + "SELECT count(DISTINCT S.PERSON_ID) FROM PERSON_SEGMENTS AS S INNER JOIN AUDIENCE AS A ON S.PERSON_ID = A.PERSON_ID WHERE S.SEGMENT_NAME LIKE 'SEG_HH_%'",
			"person_segments(segment_name)&audience_members(audience_id,ver)"},
		{"SELECT g, count(DISTINCT t.person_id) FROM t WHERE t.person_id IN (SELECT DISTINCT person_id FROM people WHERE region = 'x') GROUP BY g",
			"t(g)&people(region)"},
		{"SELECT g, count(DISTINCT t.person_id), count(*) FROM t GROUP BY g", "has other aggregates beside the count"},
		{"SELECT g, count(DISTINCT lower(t.person_id)) FROM t GROUP BY g", "counts an expression, not a column"},
		{"WITH x AS (SELECT * FROM t) SELECT g, count(DISTINCT person_id) FROM x GROUP BY g",
			"counts rows of a derived relation (a CTE, subquery, unnest or function), not a table"},
		{"SELECT g, count(DISTINCT t.person_id) FROM t JOIN u ON u.k = t.k GROUP BY g", "joins the set on other than the member column"},
		{"SELECT t.g, count(DISTINCT t.person_id) FROM t JOIN u ON u.person_id = t.person_id GROUP BY u.h", "groups by columns of other relations, or by the counted column"},
		{"SELECT g, count(DISTINCT person_id) FROM t GROUP BY g HAVING count(DISTINCT person_id) > 5", "has HAVING, QUALIFY, a sample or grouping sets"},
		{"SELECT g, count(DISTINCT person_id) FROM t GROUP BY g ORDER BY count(DISTINCT person_id)", "orders by an aggregate"},
	} {
		tree, err := parseStatement(ctx, conn, c.query)
		if err != nil {
			t.Fatalf("%s: %v", c.query, err)
		}
		var got []string
		for _, m := range matchIndexCounts(tree) {
			if m.miss != "" {
				got = append(got, m.miss)
				continue
			}
			desc := func(r indexRel) string {
				g := slices.Clone(r.groups)
				slices.Sort(g)
				return strings.ToLower(strings.Join(r.parts, ".")) + "(" + strings.Join(g, ",") + ")"
			}
			s := desc(m.target)
			if m.restrict != nil {
				s += "&" + desc(*m.restrict)
			}
			got = append(got, s)
		}
		if strings.Join(got, ";") != c.want {
			t.Errorf("%s:\n got %q\nwant %q", c.query, got, c.want)
		}
	}
}
