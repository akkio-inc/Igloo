package distributed

import (
	"slices"
	"strings"
)

// Window functions over partitions. A SELECT that does not aggregate, and
// whose window functions all partition their rows by the same columns of
// its FROM clause, as row_number() OVER (PARTITION BY person ORDER BY day
// DESC) keeps each person's latest row, runs on workers with its inputs
// read by partition, keyed by those columns: each task holds whole
// partitions of the windows, and computes them, its QUALIFY and its
// filters over them as one machine would. The coordinator receives the
// rows they keep, rather than every row to compute the windows over. Such
// a SELECT may be the statement itself, a subquery or common table
// expression the coordinator reads, or a relation workers compute for a
// later stage.

// windowColumns sets c.window for a candidate that does not aggregate and
// has window functions, in its select list, QUALIFY or ORDER BY: the
// columns of its FROM clause that every one of them partitions by. It fails
// for windows that cannot be computed partition by partition: one over all
// rows, or partitions of an expression or of different columns.
func (pl *planner) windowColumns(c *candidate) error {
	c.window = nil
	if c.aggregating(pl) {
		// Windows over its groups, which the combine step computes.
		return nil
	}
	var windows []node
	walk([]any{c.sel["select_list"], c.sel["qualify"], c.sel["modifiers"]}, func(n node) bool {
		if str(n["class"]) == "WINDOW" {
			windows = append(windows, n)
			return false
		}
		return true
	})
	var cols []itemColumn
	for i, w := range windows {
		var these []itemColumn
		for _, p := range list(w, "partitions") {
			pn := p.(node)
			if str(pn["class"]) != "COLUMN_REF" {
				return unsupported("a window partitioned by an expression")
			}
			it := resolveColumn(pn, c.scope)
			if it == nil {
				return unsupported("a window partitioned by %s, of no one FROM item", strings.Join(columnNames(pn), "."))
			}
			if ic := (itemColumn{it, strings.ToLower(lastName(pn))}); !slices.Contains(these, ic) {
				these = append(these, ic)
			}
		}
		switch {
		case len(these) == 0:
			return unsupported("window functions over rows of no partition")
		case i > 0 && !sameColumns(cols, these):
			return unsupported("window functions partitioned by different columns")
		}
		cols = these
	}
	if len(cols) > 0 && c.sel[rankedMark] == true {
		// It ranks the groups of aggregations whose stages keep only those
		// that may rank within the first k (see markTopK): few rows, which
		// the coordinator ranks as they arrive.
		return unsupported("its windows rank the first groups of aggregations, which the coordinator ranks")
	}
	if len(cols) > 0 {
		c.window = cols
	}
	return nil
}

// sameColumns reports whether a and b hold the same columns, in any order.
func sameColumns(a, b []itemColumn) bool {
	if len(a) != len(b) {
		return false
	}
	for _, ic := range a {
		if !slices.Contains(b, ic) {
			return false
		}
	}
	return true
}

// partitionedWindows reports whether every window function of a SELECT's
// select list partitions its rows, as windowColumns needs: one that does
// not is over all rows (see overAll).
func partitionedWindows(sel node) bool {
	ok, any := true, false
	walk(sel["select_list"], func(n node) bool {
		if str(n["class"]) == "WINDOW" {
			any = true
			ok = ok && len(list(n, "partitions")) > 0
			return false
		}
		return ok
	})
	return ok && any
}

// windowKeys returns the columns a candidate's only input read by
// partition, k, is keyed by for its windows: those of c.window, when they
// are all k's; nil otherwise.
func windowKeys(c *candidate, k *keyed) []string {
	var names []string
	for _, ic := range c.window {
		if ic.item != k.item {
			return nil
		}
		names = append(names, ic.name)
	}
	return names
}

// checkWindows checks that the partitions a windowed candidate's stage
// reads hold whole partitions of its windows: values of the windows'
// columns fix the key of one of its inputs read by partition.
func (c *candidate) checkWindows() error {
	if c.window == nil {
		return nil
	}
	if len(c.keyed) == 0 || !c.fixesKey(c.window) {
		return unsupported("window functions partition rows otherwise than workers could")
	}
	return nil
}
