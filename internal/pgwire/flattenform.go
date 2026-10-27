package pgwire

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
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
// estimates point to (see estimateFlattenLists), the next one the other
// form, and from then on the shape takes the form whose latest run took the
// less CPU time.

// listsShare is the share of the flattened tables' rows that, estimated to
// reach the dependent joins, makes the FROM items compute the lists.
const listsShare = 0.25

// maxFlattenShapes bounds the shapes kept; they are forgotten all at once.
const maxFlattenShapes = 4096

// flattenForms holds what Igloo knows of each shape's two forms.
type flattenForms struct {
	mu     sync.Mutex
	shapes map[string]*flattenForm
}

// flattenForm is one shape's: whether the estimate picked the FROM items'
// lists, once made, or is being made; and for each form, the lateral one
// first, the CPU time of its latest run (zero for none yet) and whether a
// run of it was started.
type flattenForm struct {
	estimated, estimating, lists bool
	cost                         [2]time.Duration
	started                      [2]bool
}

// choose says whether a statement of shape key unnests lists its FROM
// items compute; estimate is DuckDB's pick, asked for once a shape.
func (f *flattenForms) choose(key string, estimate func() bool) bool {
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
	case s.cost[0] > 0 && s.cost[1] > 0:
		lists := s.cost[1] < s.cost[0]
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
		lists := s.cost[1] > 0 || s.cost[0] == 0 && s.lists
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

// observe records the CPU time a statement of shape key took in the form
// lists says.
func (f *flattenForms) observe(key string, lists bool, cpu time.Duration) {
	if cpu <= 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if s := f.shapes[key]; s != nil {
		s.cost[b2i(lists)] = cpu
	}
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// estimateFlattenLists reads, from DuckDB's plan of query, a statement whose
// FLATTENs compute their lists in their lateral subqueries, how many rows
// its dependent joins take from the joins before them, against how many
// rows the tables under those have, and picks the FROM items' lists when
// the joins keep listsShare of them or more. Without a dependent join, or a
// plan, it keeps the lateral form.
func estimateFlattenLists(ctx context.Context, sess *session, query string) bool {
	rows, err := sess.conn.QueryContext(ctx, "EXPLAIN (FORMAT JSON) "+query, nil)
	if err != nil {
		return false
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, len(rows.Columns()))
	var plan any
	for {
		if err := rows.Next(dest); err != nil {
			if !errors.Is(err, io.EOF) {
				return false
			}
			break
		}
		if key, _ := dest[0].(string); key == "physical_plan" && len(dest) > 1 {
			text, _ := dest[1].(string)
			if json.Unmarshal([]byte(text), &plan) != nil {
				return false
			}
		}
	}
	reach, tables := delimRows(plan)
	return tables > 0 && float64(reach) >= listsShare*float64(tables)
}

// delimRows sums, over the dependent joins of plan, DuckDB's physical plan
// as JSON, the estimated rows of their first child, which they copy and
// join back, and the most rows of a table scan under it.
func delimRows(plan any) (reach, tables int64) {
	estimate := func(m map[string]any) int64 {
		n, _ := strconv.ParseInt(strings.TrimSpace(str(asMap(m["extra_info"])["Estimated Cardinality"])), 10, 64)
		return n
	}
	walkTree(plan, func(m map[string]any) {
		if !strings.Contains(str(m["name"]), "DELIM_JOIN") {
			return
		}
		kids := asList(m["children"])
		if len(kids) == 0 {
			return
		}
		outer := asMap(kids[0])
		var most int64
		walkTree(outer, func(n map[string]any) {
			if str(n["name"]) == "TABLE_SCAN" {
				most = max(most, estimate(n))
			}
		})
		reach += estimate(outer)
		tables += most
	})
	return reach, tables
}
