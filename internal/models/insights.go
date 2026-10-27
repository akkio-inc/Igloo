package models

import (
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// TOP_INSIGHTS: which segments of the data drove a metric's change between
// a control group and a test group (two periods, say). A tree splits the
// rows on their dimensions, at each node by the condition whose segment's
// change departs most from the node's own rate of change; every node is a
// segment, from Overall down.

// Insights bounds: the tree's depth, the categories of a dimension it
// considers (the most influential, as Snowflake's 25), and the thresholds
// of a continuous one.
const (
	insightDepth      = 3
	insightCategories = 25
	insightThresholds = 32
)

// GetDrivers computes GET_DRIVERS of rows t: label is the boolean column
// that marks test rows (TRUE) and control rows (FALSE), metric the number
// whose change it explains, and every other column a dimension: numbers
// continuous, strings and booleans categorical.
func GetDrivers(t *Table, labelCol, metricCol string) (*Result, error) {
	label, metric := t.Col(labelCol), t.Col(metricCol)
	switch {
	case label == nil:
		return nil, inputErr("LABEL_COLNAME %s is not a column of INPUT_DATA", labelCol)
	case label.Type != TypeBool:
		return nil, inputErr("the label %s is of type %s; it must be a boolean (TRUE for test rows, FALSE for control)", label.Name, label.TypeName)
	case metric == nil:
		return nil, inputErr("METRIC_COLNAME %s is not a column of INPUT_DATA", metricCol)
	case metric.Type != TypeNumber:
		return nil, inputErr("the metric %s is of type %s; it must be a number", metric.Name, metric.TypeName)
	}
	var dims []*TableCol
	for _, c := range t.Cols {
		if c == label || c == metric {
			continue
		}
		if c.Type == TypeOther {
			return nil, inputErr("dimension %s is of type %s; dimensions are numbers, strings, booleans or timestamps", c.Name, c.TypeName)
		}
		dims = append(dims, c)
	}
	if len(dims) == 0 {
		return nil, inputErr("INPUT_DATA has no dimensions besides the label and the metric")
	}
	var rows []int
	for i := range t.N {
		if !label.Null[i] && !metric.Null[i] && !math.IsNaN(metric.Num[i]) {
			rows = append(rows, i)
		}
	}
	d := &drivers{test: label, metric: metric, dims: dims}
	root := d.node(rows, nil)
	// A split must explain a twentieth of the change, or of a hundredth
	// of the totals when they barely changed: less is noise.
	d.minScore = 0.05 * math.Max(math.Abs(root.test-root.control), 0.01*(math.Abs(root.test)+math.Abs(root.control)))
	d.grow(root, 0)
	cols, _ := MethodColumns(ClassTopInsights, "GET_DRIVERS")
	res := &Result{Columns: cols}
	change := root.test - root.control
	var nodes []*segment
	var walk func(n *segment)
	walk = func(n *segment) {
		nodes = append(nodes, n)
		for _, c := range n.kids {
			walk(c)
		}
	}
	walk(root)
	rest := nodes[1:]
	sort.SliceStable(rest, func(a, b int) bool { return rest[a].test-rest[a].control > rest[b].test-rest[b].control })
	for _, n := range nodes {
		contributor := []string{"Overall"}
		if len(n.conds) > 0 {
			contributor = n.conds
		}
		var relative, growth any
		if change != 0 {
			relative = (n.test - n.control) / change
		}
		if n.control != 0 {
			growth = (n.test - n.control) / n.control
		}
		res.Rows = append(res.Rows, []any{jsonArray(contributor), n.control, n.test, n.test - n.control, relative, growth})
	}
	return res, nil
}

type drivers struct {
	test, metric *TableCol
	dims         []*TableCol
	minScore     float64
}

// segment is a node of the tree: its rows, their control and test totals,
// the conditions that make it, and its children.
type segment struct {
	rows          []int
	control, test float64
	conds         []string
	kids          []*segment
}

func (d *drivers) node(rows []int, conds []string) *segment {
	n := &segment{rows: rows, conds: conds}
	for _, i := range rows {
		if d.test.Str[i] == "true" {
			n.test += d.metric.Num[i]
		} else {
			n.control += d.metric.Num[i]
		}
	}
	return n
}

// dimName is how a condition names a dimension: in upper case, as
// Snowflake's do, unless it has upper-case letters of its own.
func dimName(name string) string {
	if strings.ToLower(name) == name {
		return strings.ToUpper(name)
	}
	return name
}

// split is a candidate split of a segment: the rows that meet a condition
// and those that do not.
type insightSplit struct {
	score     float64
	tie       float64
	yes, no   []int
	condition string
	negation  string
}

// grow splits n while a split explains some of its change.
func (d *drivers) grow(n *segment, depth int) {
	if depth >= insightDepth || len(n.rows) < 2 {
		return
	}
	best := d.bestSplit(n)
	if best == nil || best.score <= d.minScore || best.score <= 1e-12 {
		return
	}
	yes := d.node(best.yes, append(slices.Clone(n.conds), best.condition))
	no := d.node(best.no, append(slices.Clone(n.conds), best.negation))
	n.kids = []*segment{yes, no}
	d.grow(yes, depth+1)
	d.grow(no, depth+1)
}

// bestSplit finds the condition whose segment's change departs most from
// the node's rate: |test - control * rate| of the rows that meet it.
func (d *drivers) bestSplit(n *segment) *insightSplit {
	rate := 1.0
	if n.control != 0 {
		rate = n.test / n.control
	}
	surprise := func(rows []int) float64 {
		var c, t float64
		for _, i := range rows {
			if d.test.Str[i] == "true" {
				t += d.metric.Num[i]
			} else {
				c += d.metric.Num[i]
			}
		}
		return math.Abs(t - rate*c)
	}
	change := func(rows []int) float64 {
		var c, t float64
		for _, i := range rows {
			if d.test.Str[i] == "true" {
				t += d.metric.Num[i]
			} else {
				c += d.metric.Num[i]
			}
		}
		return math.Abs(t - c)
	}
	var best *insightSplit
	consider := func(s *insightSplit) {
		if len(s.yes) == 0 || len(s.no) == 0 {
			return
		}
		s.score = surprise(s.yes)
		// A split by either of two categories is the same split: name the
		// one that changed more.
		s.tie = change(s.yes)
		tol := 1e-9 * math.Max(1, s.score)
		if best == nil || s.score > best.score+tol || s.score >= best.score-tol && s.tie > best.tie+tol {
			best = s
		}
	}
	for _, c := range d.dims {
		name := dimName(c.Name)
		if c.Type == TypeNumber || c.Type == TypeTime {
			vals := make([]float64, 0, len(n.rows))
			for _, i := range n.rows {
				if v := numValue(c, i); !math.IsNaN(v) {
					vals = append(vals, v)
				}
			}
			slices.Sort(vals)
			vals = slices.Compact(vals)
			if len(vals) < 2 {
				continue
			}
			step := max(1, len(vals)/insightThresholds)
			for k := step - 1; k+1 < len(vals); k += step {
				th := vals[k] + (vals[k+1]-vals[k])/2
				var yes, no []int
				for _, i := range n.rows {
					if v := numValue(c, i); !math.IsNaN(v) && v <= th {
						yes = append(yes, i)
					} else {
						no = append(no, i)
					}
				}
				label := strconv.FormatFloat(th, 'f', -1, 64)
				consider(&insightSplit{yes: yes, no: no, condition: name + " <= " + label, negation: name + " > " + label})
			}
			continue
		}
		// Categories: the most influential, by how far each departs from
		// the node's rate.
		byValue := map[string][]int{}
		for _, i := range n.rows {
			if !c.Null[i] {
				byValue[c.Str[i]] = append(byValue[c.Str[i]], i)
			}
		}
		if len(byValue) < 2 && len(byValue) == len(n.rows) {
			continue
		}
		values := make([]string, 0, len(byValue))
		for v := range byValue {
			values = append(values, v)
		}
		slices.Sort(values)
		sort.SliceStable(values, func(a, b int) bool { return surprise(byValue[values[a]]) > surprise(byValue[values[b]]) })
		if len(values) > insightCategories {
			values = values[:insightCategories]
		}
		for _, v := range values {
			yes := byValue[v]
			in := map[int]bool{}
			for _, i := range yes {
				in[i] = true
			}
			var no []int
			for _, i := range n.rows {
				if !in[i] {
					no = append(no, i)
				}
			}
			shown := v
			if c.Type == TypeBool {
				shown = strings.ToUpper(v)
			}
			consider(&insightSplit{yes: yes, no: no, condition: name + " = " + shown, negation: "not " + name + " = " + shown})
		}
	}
	return best
}

func numValue(c *TableCol, i int) float64 {
	if c.Null[i] {
		return math.NaN()
	}
	if c.Type == TypeTime {
		return float64(c.Micros[i]) / 1e6
	}
	return c.Num[i]
}
