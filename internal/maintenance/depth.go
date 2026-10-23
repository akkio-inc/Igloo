package maintenance

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// Clustering depth. A file's depth is the largest number of files whose
// ranges of the sort order's first column hold one value of its range, the
// file included: 1 when it overlaps no other file, and as many as a filter
// on that value must read where they stack. Files whose ranges only touch
// do not overlap (as overlapping has it), and neither do two files of one
// value, as a sorted rewrite leaves a value many rows share. A table's
// clustering depth is the average of its files' (Snowflake's
// SYSTEM$CLUSTERING_DEPTH measures the same).

// depths returns the depth of each file of s (sorted by spans) that in
// marks, among the files in marks, and 0 for the others. Files without
// bounds are left out.
func depths(s []fileSpan, in []bool) []int {
	out := make([]int, len(s))
	// The files' bounds, in order and once each, number the positions: bound
	// k is position 2k, and the values between bounds k and k+1 are
	// position 2k+1.
	var bounds []iceberg.Literal
	for i, f := range s {
		if in[i] && f.bounded {
			bounds = append(bounds, f.lo, f.hi)
		}
	}
	if len(bounds) == 0 {
		return out
	}
	cmpLit := func(a, b iceberg.Literal) int { c, _ := compareLiterals(a, b); return c }
	slices.SortFunc(bounds, cmpLit)
	bounds = slices.CompactFunc(bounds, func(a, b iceberg.Literal) bool { return cmpLit(a, b) == 0 })
	rank := func(v iceberg.Literal) int {
		k, _ := slices.BinarySearchFunc(bounds, v, cmpLit)
		return k
	}
	// cover[p] counts the files over position p: a range covers the
	// positions strictly inside it; the files of one value count once at it.
	n := 2*len(bounds) - 1
	diff := make([]int, n+1)
	point := make([]bool, n)
	from := make([]int, len(s))
	to := make([]int, len(s))
	for i, f := range s {
		if !in[i] || !f.bounded {
			continue
		}
		lo, hi := rank(f.lo), rank(f.hi)
		if lo == hi {
			from[i], to[i] = 2*lo, 2*lo
			point[2*lo] = true
			continue
		}
		from[i], to[i] = 2*lo+1, 2*hi-1
		diff[from[i]]++
		diff[to[i]+1]--
	}
	cover := make([]int, n)
	for p, run := 0, 0; p < n; p++ {
		run += diff[p]
		cover[p] = run
		if point[p] {
			cover[p]++
		}
	}
	top := maxTable(cover)
	for i, f := range s {
		if in[i] && f.bounded {
			out[i] = top(from[i], to[i])
		}
	}
	return out
}

// maxTable returns a function that gives the largest of xs[a..b], a <= b,
// in constant time.
func maxTable(xs []int) func(a, b int) int {
	levels := [][]int{xs}
	for w := 1; 2*w <= len(xs); w *= 2 {
		prev := levels[len(levels)-1]
		next := make([]int, len(xs)-2*w+1)
		for i := range next {
			next[i] = max(prev[i], prev[i+w])
		}
		levels = append(levels, next)
	}
	return func(a, b int) int {
		k := 0
		for 1<<(k+1) <= b-a+1 {
			k++
		}
		return max(levels[k][a], levels[k][b-(1<<k)+1])
	}
}

// depthOf returns the average and the largest depth of the files in marks.
func depthOf(s []fileSpan, in []bool) (float64, int) {
	d := depths(s, in)
	sum, n, top := 0, 0, 0
	for i, x := range d {
		if in[i] && s[i].bounded {
			sum += x
			n++
			top = max(top, x)
		}
	}
	if n == 0 {
		return 0, 0
	}
	return float64(sum) / float64(n), top
}

// ErrNotClustered is the error of ClusteringDepth for a table without a sort
// order to measure by, when no column is given.
var ErrNotClustered = errors.New("the table has no clustering key; name the column to measure by")

// ErrNoSuchColumn is the error of ClusteringDepth for a column the table
// does not have at its top level, or whose values have no order.
var ErrNoSuchColumn = errors.New("no such column to measure by")

// Depth is a table's clustering depth by a column: the average and the
// largest depth of its files, and how many files it has.
type Depth struct {
	Average float64
	Max     int
	Files   int
}

// ClusteringDepth returns tbl's clustering depth by column, from its
// manifests' bounds of it, or with column "" by the first column of the sort
// order maintenance clusters it by. Files without bounds of the column are
// left out of the average; a table without files has depth 0.
func ClusteringDepth(ctx context.Context, tbl *table.Table, column string) (Depth, error) {
	var id int
	if column == "" {
		_, first, ok := clusterFields(tbl)
		if !ok {
			return Depth{}, ErrNotClustered
		}
		id = first
	} else {
		f, ok := tbl.Schema().FindFieldByNameCaseInsensitive(column)
		if !ok {
			return Depth{}, fmt.Errorf("%w: %s", ErrNoSuchColumn, column)
		}
		if _, primitive := f.Type.(iceberg.PrimitiveType); !primitive {
			return Depth{}, fmt.Errorf("%w: %s is not of a primitive type", ErrNoSuchColumn, column)
		}
		if top, ok := tbl.Schema().FindFieldByName(f.Name); !ok || top.ID != f.ID {
			return Depth{}, fmt.Errorf("%w: %s is not a column at the top level", ErrNoSuchColumn, column)
		}
		id = f.ID
	}
	if tbl.CurrentSnapshot() == nil {
		return Depth{}, nil
	}
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return Depth{}, err
	}
	s := spans(tasks, tbl.Schema(), id)
	in := make([]bool, len(s))
	for i := range in {
		in[i] = true
	}
	avg, top := depthOf(s, in)
	return Depth{Average: avg, Max: top, Files: len(tasks)}, nil
}
