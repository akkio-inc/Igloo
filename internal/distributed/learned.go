package distributed

import (
	"encoding/json"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// learned corrects DuckDB's estimates of rows by the rows the operators of
// completed statements made, whatever statement they were in: how many
// groups a grouping's keys made of how many rows, and how many rows a join
// on a pair of keys made of its inputs' rows. Without distinct counts
// (ANALYZE, Iceberg's sketches) DuckDB takes a grouping to make a group of
// every row, and a join of keys it knows nothing of to keep its larger
// side's rows: a dashboard's tiles grouping a billion impressions by
// campaign and day were estimated at 1.8 to 7.1 GB, and held up to 1.8;
// a burst's joins of events to audiences' members, each in several, at 323
// MB, and held 3.4 GB. What one statement's operators made, any statement
// grouping or joining by the same columns of the same tables learns from.
type learned struct {
	mu sync.Mutex
	// groups are the distinct values a grouping's keys have, by its keys
	// (see groupingKey), as the groupings seen came to (see distinctFrom).
	groups map[string]float64
	// joins are what joins by the same keys made (see joinKeyOf).
	joins map[string]joinSeen
	// scans are the share of a table's rows its scans with the same
	// filters kept, by its table and filters (see scanKey).
	scans map[string]float64
}

// joinSeen is what a join made: d is the larger distinct count of its keys,
// as l*r/out of its inputs' rows and its own come to; fan is how many rows it
// made for each of its larger input's.
type joinSeen struct{ d, fan float64 }

// OperatorRows are the rows each operator of a completed statement made,
// as DuckDB's profiler counted them: its operator's name, as DuckDB's plan
// names it, its rows, and its children's, in the plan's order.
type OperatorRows struct {
	Name     string
	Rows     float64
	Children []OperatorRows
}

// ObserveOperators learns from the rows the operators of a statement made
// that ran to its end on this replica, whose plan key is key, where its
// plan was estimated (see learned): later statements grouping or joining
// by the same columns, or scanning a table with the same filters, are
// estimated by what these operators made.
func (co *Coordinator) ObserveOperators(key string, ops OperatorRows) {
	if co == nil {
		return
	}
	plan, st, ok := co.plans.get(key)
	if !ok {
		return
	}
	actual, ok := planWithRows(plan, ops)
	if !ok {
		return
	}
	st.learned, st.observe = co.learned, true
	_, _ = planOf(actual, st)
}

// planWithRows is plan (EXPLAIN (FORMAT JSON)) with each operator's
// estimate of its rows replaced by the rows the operator made, ops, where
// ops, below its root's operators that are not the plan's, is the plan's
// tree; ok is false where it is not.
func planWithRows(plan string, ops OperatorRows) (string, bool) {
	var roots []*planNode
	if json.Unmarshal([]byte(plan), &roots) != nil || len(roots) != 1 {
		return "", false
	}
	top := ops
	for !strings.EqualFold(top.Name, roots[0].Name) {
		if len(top.Children) != 1 {
			return "", false
		}
		top = top.Children[0]
	}
	same := true
	var walk func(n *planNode, o OperatorRows)
	walk = func(n *planNode, o OperatorRows) {
		if !strings.EqualFold(n.Name, o.Name) || len(n.Children) != len(o.Children) {
			same = false
			return
		}
		if n.Extra == nil {
			n.Extra = map[string]json.RawMessage{}
		}
		rows, _ := json.Marshal(strconv.FormatFloat(o.Rows, 'f', 0, 64))
		n.Extra["Estimated Cardinality"] = rows
		for i := range n.Children {
			walk(n.Children[i], o.Children[i])
		}
	}
	walk(roots[0], top)
	if !same {
		return "", false
	}
	out, err := json.Marshal(roots)
	return string(out), err == nil
}

// recentPlans are the plans of the statements estimated last, with their
// tables' statistics, by plan key, up to mostRecentPlans of them.
type recentPlans struct {
	mu    sync.Mutex
	plans map[string]recentPlan
	order []string
}

type recentPlan struct {
	plan string
	st   planStats
}

const mostRecentPlans = 1024

func newRecentPlans() *recentPlans { return &recentPlans{plans: map[string]recentPlan{}} }

func (r *recentPlans) put(key, plan string, st planStats) {
	if r == nil || key == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.plans[key]; !ok {
		r.order = append(r.order, key)
	}
	r.plans[key] = recentPlan{plan, st}
	for len(r.order) > mostRecentPlans {
		delete(r.plans, r.order[0])
		r.order = r.order[1:]
	}
}

func (r *recentPlans) get(key string) (string, planStats, bool) {
	if r == nil {
		return "", planStats{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.plans[key]
	return p.plan, p.st, ok
}

// maxLearned bounds what learned keeps of each kind.
const maxLearned = 100_000

func newLearned() *learned {
	return &learned{groups: map[string]float64{}, joins: map[string]joinSeen{}, scans: map[string]float64{}}
}

// distinctFrom is how many distinct values keys have that made groups
// groups of in rows, were each row's values drawn at random from them:
// groups = d(1 - exp(-in/d)). A grouping that kept nearly every row tells
// only that the values are many: infinity.
func distinctFrom(groups, in float64) float64 {
	if groups <= 0 || in <= 0 {
		return 0
	}
	if groups >= 0.99*in {
		return math.Inf(1)
	}
	lo, hi := groups, groups*1e9
	for range 100 {
		mid := math.Sqrt(lo * hi)
		if mid*(1-math.Exp(-in/mid)) < groups {
			lo = mid
		} else {
			hi = mid
		}
	}
	return hi
}

// groupsOf is how many groups keys of d distinct values make of in rows.
func groupsOf(d, in float64) float64 {
	if math.IsInf(d, 1) {
		return in
	}
	return d * (1 - math.Exp(-in/d))
}

// observeGroups learns that a grouping by key made groups of in rows.
func (l *learned) observeGroups(key string, groups, in float64) {
	if l == nil || key == "" {
		return
	}
	d := distinctFrom(groups, in)
	if d <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if old, ok := l.groups[key]; ok || len(l.groups) < maxLearned {
		l.groups[key] = max(old, d)
	}
}

// groupsFor is how many groups a grouping by key makes of in rows, by what
// was learned; ok is false if nothing was.
func (l *learned) groupsFor(key string, in float64) (float64, bool) {
	if l == nil || key == "" {
		return 0, false
	}
	l.mu.Lock()
	d, ok := l.groups[key]
	l.mu.Unlock()
	if !ok || math.IsInf(d, 1) {
		// Keys of about as many values as rows tell no more than DuckDB's
		// estimate, which an input taken for far too many rows would only
		// make worse.
		return 0, false
	}
	return groupsOf(d, in), true
}

// distinctOf is the distinct values learned of a grouping's keys, by key;
// zero if nothing was learned of them.
func (l *learned) distinctOf(key string) float64 {
	if l == nil || key == "" {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.groups[key]
}

// observeJoin learns that a join by key made out rows of left and right.
func (l *learned) observeJoin(key string, left, right, out float64) {
	if l == nil || key == "" || left <= 0 || right <= 0 || out <= 0 {
		return
	}
	seen := joinSeen{d: left * right / out, fan: out / max(left, right)}
	l.mu.Lock()
	defer l.mu.Unlock()
	if old, ok := l.joins[key]; ok || len(l.joins) < maxLearned {
		seen.fan = max(seen.fan, old.fan)
		l.joins[key] = seen
	}
}

// joinFor is how many rows a join by key makes of left and right, by what
// was learned: left*right over the larger distinct count of its keys, as
// their values were drawn apart from each other, but no more for each of
// its larger input's rows than it made before, as estimates of its inputs
// may be far off and their product further; ok is false if nothing was.
func (l *learned) joinFor(key string, left, right float64) (float64, bool) {
	if l == nil || key == "" {
		return 0, false
	}
	l.mu.Lock()
	seen, ok := l.joins[key]
	l.mu.Unlock()
	if !ok || seen.d <= 0 {
		return 0, false
	}
	return max(min(left*right/seen.d, max(left, right)*max(seen.fan, 1)), 1), true
}

// observeScan learns that a scan by key kept share of its table's rows.
func (l *learned) observeScan(key string, share float64) {
	if l == nil || key == "" || share < 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.scans[key]; ok || len(l.scans) < maxLearned {
		l.scans[key] = share
	}
}

// scanFor is the share of its table's rows a scan by key keeps, by what was
// learned; ok is false if nothing was.
func (l *learned) scanFor(key string) (float64, bool) {
	if l == nil || key == "" {
		return 0, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	share, ok := l.scans[key]
	return share, ok
}

// literal matches the constants of a scan's filters.
var literal = regexp.MustCompile(`'(?:[^']|'')*'|\b\d+(?:\.\d+)?\b`)

// scanKey names a scan by its table and its filters, their constants left
// out: a dashboard's scans filter by an advertiser and a date range, which
// keep about as much of the table whichever they are.
func scanKey(t *Table, filters []string) string {
	if t == nil || len(filters) == 0 {
		return ""
	}
	fs := make([]string, len(filters))
	for i, f := range filters {
		fs[i] = literal.ReplaceAllString(strings.ToLower(strings.TrimSpace(f)), "?")
	}
	slices.Sort(fs)
	return strings.ToLower(t.Schema+"."+t.Name) + "|" + strings.Join(fs, "&")
}

// groupingKey names a grouping by its keys, each a table's column, in
// order; empty if one is not.
func groupingKey(keys []column) string {
	if len(keys) == 0 {
		return ""
	}
	names := make([]string, len(keys))
	for i, k := range keys {
		if k.table == "" {
			return ""
		}
		names[i] = k.table + "." + k.name
	}
	slices.Sort(names)
	return strings.Join(names, ",")
}

// joinKeyOf names an equality join by the pairs of columns its conditions
// compare, each a table's column; empty if one is not.
func joinKeyOf(pairs [][2]column) string {
	if len(pairs) == 0 {
		return ""
	}
	names := make([]string, len(pairs))
	for i, p := range pairs {
		a, b := p[0], p[1]
		if a.table == "" || b.table == "" {
			return ""
		}
		x, y := a.table+"."+a.name, b.table+"."+b.name
		if y < x {
			x, y = y, x
		}
		names[i] = x + "=" + y
	}
	slices.Sort(names)
	return strings.Join(names, ",")
}
