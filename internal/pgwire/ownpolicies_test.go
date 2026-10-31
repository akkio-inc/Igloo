package pgwire

import (
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/akkio-inc/igloo/internal/audit"
)

// TestOwnPoliciesKeepNothingLocal checks statements on a replica whose
// only policies are Igloo's own, on the ID domain's dictionary (people)
// and the bitmap indexes' tables (seg, aud, population): statements that
// spell those names, as a count's alias or a view's column, are analyzed
// and found to read none of them, and run on workers with one machine's
// rows, through a view the rewriter expanded too; so does one over a
// member table, read without its column of ids. Reading the dictionary
// itself never reaches workers.
func TestOwnPoliciesKeepNothingLocal(t *testing.T) {
	t.Parallel()
	history := audit.NewMemoryStore(1000, 1000)
	it := newDistE2Test(t, 2, func(o *Options) {
		o.Audit = audit.New(history, audit.Options{}, slog.New(slog.DiscardHandler))
	})
	conn := it.admin
	it.exec(
		"CREATE TABLE lake.public.plays_own AS SELECT 'g' || (i % 6) AS genre, 'q' || (i % 900) AS pid, CAST(i % 40 AS INTEGER) AS minutes FROM range(20000) r(i)",
		"CREATE VIEW lake.public.v_plays_own AS SELECT genre AS seg, pid, minutes FROM lake.public.plays_own",
	)
	for _, q := range []string{
		"SELECT genre, count(*) AS population, sum(minutes) AS m FROM lake.public.plays_own GROUP BY 1 ORDER BY 1",
		"SELECT genre, count(DISTINCT pid) AS people FROM lake.public.plays_own WHERE minutes > 3 GROUP BY 1 ORDER BY 1",
		"SELECT seg, count(*) AS n, max(minutes) AS aud FROM lake.public.v_plays_own GROUP BY 1 ORDER BY 1",
		"SELECT x, count(*) AS n FROM lake.public.v_attributes GROUP BY 1 ORDER BY 1",
	} {
		mustExec(t, conn, "SET distributed_execution = off")
		local := texts(t, conn, q)
		mustExec(t, conn, "SET distributed_execution = on")
		if got := texts(t, conn, q); !slices.Equal(got, local) || len(local) == 0 {
			t.Errorf("%s: on workers\n%q\non one machine\n%q", q, got, local)
		}
		if w := lastWorkers(t, history, q); w == 0 {
			t.Errorf("%s: ran on one machine:\n%s", q, strings.Join(texts(t, conn, "EXPLAIN DISTRIBUTED "+q), "\n"))
		}
	}
	// The dictionary: no user's statement reads its rows, here or on
	// workers.
	q := "SELECT count(*) FROM lake.igloo_domains.people"
	if rows, err := conn.Query(t.Context(), q); err == nil {
		var n int64
		for rows.Next() {
			_ = rows.Scan(&n)
		}
		rows.Close()
		if n != 0 {
			t.Errorf("the dictionary showed %d rows", n)
		}
		if w := lastWorkers(t, history, q); w != 0 {
			t.Errorf("the dictionary was read on %d workers", w)
		}
	}
}
