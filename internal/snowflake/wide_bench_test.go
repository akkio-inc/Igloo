package snowflake

import (
	"fmt"
	"strings"
	"testing"
)

// wideProfile is a column profile of the kind data tools run on a table
// of n columns: three aggregates of each, as in one select list.
func wideProfile(n int) string {
	items := []string{"COUNT(*) AS N"}
	for i := range n {
		c := fmt.Sprintf(`"COL_%d"`, i)
		items = append(items, fmt.Sprintf(`COUNT(%s) AS C%d_NN, APPROX_COUNT_DISTINCT(%s) AS C%d_D, SUBSTR(MAX(TO_VARCHAR(%s)), 1, 200) AS C%d_EX`,
			c, i, c, i, c, i))
	}
	return "SELECT * FROM (SELECT " + strings.Join(items, ", ") + " FROM WIDE_T) SAMPLE (1 ROWS)"
}

// BenchmarkTranslateWide translates column profiles of growing width; the
// time per column should stay about the same.
func BenchmarkTranslateWide(b *testing.B) {
	for _, n := range []int{56, 112, 224, 449} {
		q := wideProfile(n)
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			for b.Loop() {
				if _, err := Translate(q, Options{Sketches: true}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
