package pgwire

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// keywordsSQL counts browsing keywords per cohort as the signals reports
// do, distinct (person, day) pairs under a CASE, over unnested lists.
const keywordsSQL = `WITH ` + reportCohortsSQL + `,
BROWSING_COUNTS AS (
  SELECT 'DIGITAL' AS category, D.dim AS dimension, CAST(I.item AS VARCHAR) AS value, M.cohort AS cohort,
    count(DISTINCT b.person_id) AS people,
    count(DISTINCT CASE WHEN b.person_id IS NULL THEN NULL WHEN b.visited_date IS NULL THEN NULL ELSE (b.person_id, b.visited_date) END) AS days
  FROM browsing AS b JOIN COHORT_MEMBERS AS M ON M.person_id = b.person_id
  CROSS JOIN UNNEST([{'dim': 'KEYWORD', 'items': b.keywords}, {'dim': 'ACTIVITY', 'items': ['ANY_BROWSING']}]) AS BD(D)
  CROSS JOIN UNNEST(D.items) AS I(item)
  WHERE NOT CAST(I.item AS VARCHAR) IS NULL AND trim(CAST(I.item AS VARCHAR)) <> ''
  GROUP BY D.dim, CAST(I.item AS VARCHAR), M.cohort)
SELECT S.*, Z.cohort_size FROM BROWSING_COUNTS AS S JOIN COHORT_TOTALS AS Z ON Z.cohort = S.cohort ORDER BY ALL`

// TestExplainDistributedMatchesRun: where EXPLAIN DISTRIBUTED says a
// statement runs, workers or this replica, is where running it runs it:
// reports as written, with distributed execution on and in auto mode, with
// roaring bitmaps counting distinct values, and over a table whose list
// column the planner is not told how it nests, which workers refuse. Report
// recognition's guard leaves a report whole on what EXPLAIN DISTRIBUTED
// says of it.
func TestExplainDistributedMatchesRun(t *testing.T) {
	t.Parallel()
	ds := startDistServer(t, 2, func(o *Options) { o.Bitmaps, o.BitmapMinRows = true, 0 })
	for _, q := range reportFixture {
		mustExec(t, ds.admin, q)
	}
	conn := ds.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "SET search_path = 'lake.public'")
	// A list of structs, whose nesting the test's catalog does not give.
	mustExec(t, ds.admin, "CREATE TABLE lake.public.visits_nested AS SELECT 'p' || (i % 90) AS person_id, [{'k': 'a', 'n': i}] AS pairs FROM range(600) r(i)")
	nested := `SELECT count(DISTINCT person_id) AS people, sum(len(pairs)) AS n FROM visits_nested`
	ran := map[string]bool{}
	for _, mode := range []string{"on", "auto"} {
		mustExec(t, conn, "SET distributed_execution = "+mode)
		for _, tc := range []struct{ name, query string }{
			{"keywords", keywordsSQL},
			{"counts", reportCountsSQL},
			{"segments", reportSegmentsSQL},
			{"nested", nested},
		} {
			explained := texts(t, conn, "EXPLAIN DISTRIBUTED "+tc.query)
			if len(explained) == 0 {
				t.Fatalf("%s, %s: EXPLAIN DISTRIBUTED said nothing", mode, tc.name)
			}
			onWorkers := explained[0] == "runs on|workers"
			rowsOf(t, conn, tc.query)
			workers := lastWorkers(t, ds.history, tc.query) > 0
			if workers != onWorkers {
				t.Errorf("%s, %s: EXPLAIN DISTRIBUTED says %q, but it ran on workers: %v\n%s", mode, tc.name, explained[0], workers, strings.Join(explained, "\n"))
			}
			ran[mode+" "+tc.name] = workers
		}
	}
	// The cases tell the two apart.
	if !ran["on keywords"] || !ran["on counts"] || ran["on nested"] || ran["auto segments"] {
		t.Errorf("ran on workers: %v", ran)
	}
}
