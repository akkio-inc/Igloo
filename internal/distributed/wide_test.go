package distributed

import (
	"fmt"
	"strings"
	"testing"
)

// TestWideTableExchanges checks that the stages of a statement over a view
// of a table of hundreds of columns, joined to a relation of the same view
// and read in a UNION ALL of branches that each read a few of its columns,
// write to their exchanges only the columns the stages after them read: a
// report of that shape, its union factored by pgwire and its view stored
// as Igloo stores views, wrote every column of such a table to an exchange
// whose readers read a few, and ran out of memory on the workers.
func TestWideTableExchanges(t *testing.T) {
	h := newHarness(t, 0.01, 2, 3)
	const width = 300
	cols := make([]string, width)
	for i := range cols {
		cols[i] = fmt.Sprintf("(i * %d) %% 7 AS c%03d", i+1, i)
	}
	h.addTable("wide", "(SELECT i AS id, CAST(i % 500 AS VARCHAR) AS hh, "+strings.Join(cols, ", ")+" FROM range(20000) t(i))", 4)
	h.addTable("extra", "(SELECT CAST(i AS DECIMAL(38,0)) AS luid, i % 3 AS e1, i % 5 AS e2 FROM range(500) t(i))", 1)
	// A view of the wide table alone, which the stages read as the table,
	// and one of it LEFT JOINed to a small table on an expression of its
	// columns, which they read as a relation its own stages write.
	h.addView("v_wide", "SELECT * FROM wide")
	h.addView("v_wide_extra", `SELECT s.*, x.e1, x.e2 FROM wide AS s
		LEFT JOIN extra AS x ON CAST(s.hh AS DECIMAL(38,0)) = x.luid`)
	// The same views as Igloo stores them, each table read through a
	// subquery of its columns other than those of ids (see ddl.hideIDs).
	const visible = "COLUMNS(lambda c: NOT starts_with(lower(c), '_igloo_id_'))"
	h.addView("v_wide_ids", "SELECT "+visible+" FROM wide")
	h.addView("v_wide_extra_ids", `SELECT s.*, x.e1, x.e2 FROM (SELECT `+visible+` FROM wide) AS s
		LEFT JOIN (SELECT `+visible+` FROM extra) AS x ON CAST(s.hh AS DECIMAL(38,0)) = x.luid`)
	branches := func(view string) (union, factored string) {
		var bs, styles, channels, strong, chs []string
		for b := range 4 {
			s, c := fmt.Sprintf("c%03d", 10*b+1), fmt.Sprintf("c%03d", 10*b+2)
			bs = append(bs, fmt.Sprintf(`SELECT v.id, '%s' AS style, '%s' AS channel, v.%s IN (1, 2) AS s, v.%s IN (1, 2) AS ch
				FROM %s AS v INNER JOIN gp AS g ON g.id = v.id`, s, c, s, c, view))
			styles = append(styles, fmt.Sprintf("WHEN b = %d THEN '%s'", b, s))
			channels = append(channels, fmt.Sprintf("WHEN b = %d THEN '%s'", b, c))
			strong = append(strong, fmt.Sprintf("WHEN b = %d THEN v.%s IN (1, 2)", b, s))
			chs = append(chs, fmt.Sprintf("WHEN b = %d THEN v.%s IN (1, 2)", b, c))
		}
		union = strings.Join(bs, " UNION ALL ")
		// The same union as pgwire writes it (see unionfactor.go): one SELECT
		// over the shared FROM, CROSS JOIN range(n), with a CASE on the
		// branch for each column that differs.
		factored = fmt.Sprintf(`SELECT v.id, CASE %s END AS style, CASE %s END AS channel, CASE %s END AS s, CASE %s END AS ch
			FROM %s AS v INNER JOIN gp AS g ON g.id = v.id INNER JOIN range(4) AS br(b) ON true`,
			strings.Join(styles, " "), strings.Join(channels, " "), strings.Join(strong, " "), strings.Join(chs, " "), view)
		return union, factored
	}
	for _, tc := range []struct {
		view      string
		broadcast int64
	}{
		{"v_wide", 1},
		{"v_wide_extra", h.tables["extra"].Bytes},
		{"v_wide_ids", 1},
		{"v_wide_extra_ids", h.tables["extra"].Bytes},
	} {
		h.co.maxBroadcast = tc.broadcast
		union, factored := branches(tc.view)
		for name, rows := range map[string]string{"union": union, "factored": factored} {
			t.Run(tc.view+"/"+name, func(t *testing.T) {
				// Read as the report reads it: a derived table named as the
				// view is, in a SELECT * of the whole statement.
				q := `SELECT * FROM (WITH gp AS (SELECT id FROM ` + tc.view + `)
					SELECT v.style, v.channel, count(CASE WHEN v.s AND v.ch THEN v.id END) AS strong, count(v.id) AS n
					FROM (` + rows + `) AS v GROUP BY 1, 2) ORDER BY 1, 2 LIMIT 100`
				if !h.check(q) {
					t.Fatal("ran on one machine")
				}
				narrowExchanges(t, h.planOf(q), 10)
			})
		}
	}
}

// TestWideTableJoinedToStoredView checks that a wide table read by
// partition, joined to a view as Igloo stores views, which read their
// tables through SELECT COLUMNS(...), writes only the columns the
// statement reads of it: the star in the view counted as one over the
// table, whose stage then wrote all of its columns.
func TestWideTableJoinedToStoredView(t *testing.T) {
	h := newHarness(t, 0.01, 2, 3)
	cols := make([]string, 300)
	for i := range cols {
		cols[i] = fmt.Sprintf("(i * %d) %% 7 AS c%03d", i+1, i)
	}
	h.addTable("wide", "(SELECT i AS id, "+strings.Join(cols, ", ")+" FROM range(20000) t(i))", 4)
	h.addTable("other", "(SELECT i AS id, i % 11 AS k FROM range(20000) t(i))", 4)
	h.addView("v_other", "SELECT COLUMNS(lambda c: NOT starts_with(lower(c), '_igloo_id_')) FROM other")
	h.co.maxBroadcast = 1
	q := `SELECT w.c001, o.k, count(*) AS n FROM wide AS w INNER JOIN v_other AS o ON o.id = w.id
		GROUP BY 1, 2 ORDER BY 1, 2`
	if !h.check(q) {
		t.Fatal("ran on one machine")
	}
	narrowExchanges(t, h.planOf(q), 10)
}

// narrowExchanges fails t if a stage of p writes more than max columns to
// an exchange.
func narrowExchanges(t *testing.T, p *Plan, max int) {
	t.Helper()
	for _, s := range p.Stages {
		if s.Out == nil {
			continue
		}
		n := len(list(s.fragment, "select_list"))
		if len(s.Columns) > 0 {
			n = len(s.Columns)
		}
		if n > max {
			t.Errorf("%s writes %d columns: %.300s", s, n, s.SQL)
		}
	}
}
