package maintenance

import (
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
)

// spansOf makes bounded spans of int64 ranges, sorted as spans sorts them.
func spansOf(ranges ...[2]int64) []fileSpan {
	s := make([]fileSpan, len(ranges))
	for i, r := range ranges {
		s[i] = fileSpan{lo: iceberg.NewLiteral(r[0]), hi: iceberg.NewLiteral(r[1]), bounded: true}
	}
	slices.SortStableFunc(s, func(a, b fileSpan) int {
		if c, _ := compareLiterals(a.lo, b.lo); c != 0 {
			return c
		}
		c, _ := compareLiterals(a.hi, b.hi)
		return c
	})
	return s
}

func all(n int) []bool {
	b := make([]bool, n)
	for i := range b {
		b[i] = true
	}
	return b
}

func TestDepths(t *testing.T) {
	for _, c := range []struct {
		name   string
		ranges [][2]int64
		want   []int // in spans' order
	}{
		{"touching", [][2]int64{{0, 10}, {10, 20}, {20, 30}}, []int{1, 1, 1}},
		{"one wide file over them", [][2]int64{{0, 10}, {10, 20}, {20, 30}, {0, 30}}, []int{2, 2, 2, 2}},
		{"three wide files", [][2]int64{{0, 10}, {10, 20}, {0, 30}, {0, 30}, {1, 29}}, []int{4, 4, 4, 4, 4}},
		{"one value in three files", [][2]int64{{0, 10}, {5, 5}, {5, 5}, {5, 5}, {10, 20}}, []int{2, 2, 2, 2, 1}},
		{"a value at a bound", [][2]int64{{0, 10}, {10, 10}, {10, 20}}, []int{1, 1, 1}},
		{"staggered", [][2]int64{{0, 10}, {5, 15}, {12, 20}}, []int{2, 2, 2}},
		{"nested", [][2]int64{{0, 100}, {10, 90}, {20, 80}, {95, 99}}, []int{3, 3, 3, 2}},
		{"one file", [][2]int64{{3, 7}}, []int{1}},
	} {
		s := spansOf(c.ranges...)
		got := depths(s, all(len(s)))
		if !slices.Equal(got, c.want) {
			t.Errorf("%s: depths %v, want %v", c.name, got, c.want)
		}
	}
	// Files left out do not count, and have no depth.
	s := spansOf([2]int64{0, 10}, [2]int64{0, 30}, [2]int64{10, 20})
	in := []bool{true, false, true}
	if got := depths(s, in); !slices.Equal(got, []int{1, 0, 1}) {
		t.Errorf("without the wide file: %v", got)
	}
	if avg, top := depthOf(s, all(3)); avg != 2 || top != 2 {
		t.Errorf("depthOf: %v %v", avg, top)
	}
}
