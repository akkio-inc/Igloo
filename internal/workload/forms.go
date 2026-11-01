package workload

import (
	"slices"
	"sync"
	"time"
)

// FLATTEN forms. A Snowflake FLATTEN of a column whose select reads only
// its values unnests the column's list one of two ways (see
// snowflake.Options.FlattenLists). Computed in the lateral subquery, which
// DuckDB 1.5.6 plans as a dependent join, it copies the rows that the joins
// before the FLATTEN keep and joins them back on their lists; computed in
// the FLATTEN's FROM item, it is computed for every row of the item, before
// those joins. The first is the faster when the joins keep few rows, as an
// audience of a few percent of the people does, the second when they keep
// most, as the whole population does: one dashboard query took 0.9 s the
// first way and 3.5 s the second, another 88 s and 56 s.
//
// Igloo picks for each statement shape (its text with literals masked, see
// shapeKey). The first statement of a shape takes the form DuckDB's
// estimates point to (see pgwire's estimateFlattenLists), the next one the other
// form, and from then on the shape takes the form whose latest runs took the
// less CPU time at best.

// maxFlattenShapes bounds the shapes kept; they are forgotten all at once.
const maxFlattenShapes = 4096

// flattenForms holds what the replica knows of each shape's two forms.
type flattenForms struct {
	mu     sync.Mutex
	shapes map[string]*flattenForm
}

// formRuns is how many of each form's latest runs it is judged by.
const formRuns = 4

// flattenForm is one shape's: whether the estimate picked the FROM items'
// lists, once made, or is being made; and for each form, the lateral one
// first, the CPU times of its latest runs and whether a run of it was started.
type flattenForm struct {
	estimated, estimating, lists bool
	runs                         [2][]time.Duration
	started                      [2]bool
}

// cost is the least CPU time of the form's latest runs, zero for none yet:
// DuckDB counts time a thread waits for a core, so one run beside others
// takes up to several times another alone, and a form judged by its latest
// run alone went back and forth with what ran beside it.
func (s *flattenForm) cost(form int) time.Duration {
	if len(s.runs[form]) == 0 {
		return 0
	}
	return slices.Min(s.runs[form])
}

// ChooseForm says whether a statement of shape key unnests lists its FROM
// items compute; estimate is DuckDB's pick, asked for once a shape.
func (c *Controller) ChooseForm(key string, estimate func() bool) bool {
	f := &c.forms
	f.mu.Lock()
	if f.shapes == nil || len(f.shapes) >= maxFlattenShapes {
		f.shapes = make(map[string]*flattenForm)
	}
	s := f.shapes[key]
	if s == nil {
		s = &flattenForm{}
		f.shapes[key] = s
	}
	switch {
	case s.cost(0) > 0 && s.cost(1) > 0:
		lists := s.cost(1) < s.cost(0)
		f.mu.Unlock()
		return lists
	case s.estimated && !s.started[0] || s.estimated && !s.started[1]:
		// The form not tried yet.
		lists := s.started[0]
		s.started[b2i(lists)] = true
		f.mu.Unlock()
		return lists
	case s.estimated:
		// Both started, one or neither ended: the one that ended, or else
		// the estimate's.
		lists := s.cost(1) > 0 || s.cost(0) == 0 && s.lists
		f.mu.Unlock()
		return lists
	case s.estimating:
		// Another statement of the shape asks DuckDB: as it was.
		f.mu.Unlock()
		return false
	}
	s.estimating = true
	f.mu.Unlock()
	lists := estimate()
	f.mu.Lock()
	defer f.mu.Unlock()
	s.estimated, s.estimating, s.lists = true, false, lists
	s.started[b2i(lists)] = true
	return lists
}

// ObserveForm records the CPU time a statement of shape key took in the form
// lists says. The server seeds them from the query history when it starts, so
// that a restart does not try again what it knew: a dashboard's first build
// after one took half as long again as the next, for two statements that took
// 39 s and 52 s in the form they had settled on, and 86 s and 96 s in the other.
func (c *Controller) ObserveForm(key string, lists bool, cpu time.Duration) {
	if c == nil || key == "" || cpu <= 0 {
		return
	}
	f := &c.forms
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.shapes == nil || len(f.shapes) >= maxFlattenShapes {
		f.shapes = make(map[string]*flattenForm)
	}
	s := f.shapes[key]
	if s == nil {
		// From the history: what it ran in first stands in for the estimate.
		s = &flattenForm{estimated: true, lists: lists}
		f.shapes[key] = s
	}
	form := b2i(lists)
	s.started[form] = true
	s.runs[form] = append(s.runs[form], cpu)
	if len(s.runs[form]) > formRuns {
		s.runs[form] = s.runs[form][1:]
	}
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
