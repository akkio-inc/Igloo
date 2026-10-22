package pgwire

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
)

// wideProfile is a column profile of the kind data tools run on a table of
// n columns, in Snowflake's dialect: three aggregates of each, which
// translate to thousands of casts.
func wideProfile(n int, from string) string {
	items := []string{"COUNT(*) AS N"}
	for i := range n {
		c := fmt.Sprintf(`"COL_%d"`, i)
		items = append(items, fmt.Sprintf(`COUNT(%s) AS C%d_NN, APPROX_COUNT_DISTINCT(%s) AS C%d_D, SUBSTR(MAX(TO_VARCHAR(%s)), 1, 200) AS C%d_EX`,
			c, i, c, i, c, i))
	}
	return "SELECT * FROM (SELECT " + strings.Join(items, ", ") + " FROM " + from + ") SAMPLE (1 ROWS)"
}

// wideTable makes lake.public.wide_n of n text columns and a view over it.
func wideTable(t testing.TB, cs *catalogServer, n int) string {
	t.Helper()
	cols := make([]string, n)
	for i := range cols {
		cols[i] = fmt.Sprintf(`'v' AS "COL_%d"`, i)
	}
	mustExec(t, cs.admin, fmt.Sprintf(`CREATE TABLE lake.public.wide_%d AS SELECT %s`, n, strings.Join(cols, ", ")))
	mustExec(t, cs.admin, fmt.Sprintf(`CREATE VIEW lake.public.v_wide_%d AS SELECT * FROM lake.public.wide_%d`, n, n))
	return fmt.Sprintf("lake.public.v_wide_%d", n)
}

// TestWideProfile runs a profile of a view of 449 columns, as an admin and
// as a user with a schema grant: preparing it once took seconds, rewriting
// catalog casts lexing the whole statement again for each of its casts
// (see BenchmarkWideProfile).
func TestWideProfile(t *testing.T) {
	cs := startCatalogServer(t)
	nina := cs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT SELECT ON SCHEMA lake.public TO nina")
	view := wideTable(t, cs, 449)
	for _, conn := range []*pgx.Conn{cs.admin, nina} {
		mustExec(t, conn, "SET sql_dialect = 'snowflake'")
		var n int64
		var c448 string
		if err := conn.QueryRow(t.Context(), "SELECT N, C448_EX FROM ("+wideProfile(449, view)+")").Scan(&n, &c448); err != nil {
			t.Fatal(err)
		}
		if n != 1 || c448 != "v" {
			t.Errorf("profile: N %d, C448_EX %q", n, c448)
		}
		mustExec(t, conn, "SET sql_dialect = 'duckdb'")
	}
}

// BenchmarkWideProfile prepares and runs profiles of growing width; the
// time per column should stay about the same.
func BenchmarkWideProfile(b *testing.B) {
	cs := startCatalogServer(b)
	mustExec(b, cs.admin, "SET sql_dialect = 'snowflake'")
	for _, n := range []int{56, 112, 224, 449} {
		q := wideProfile(n, wideTable(b, cs, n))
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			for b.Loop() {
				mustExec(b, cs.admin, q)
			}
		})
	}
}
