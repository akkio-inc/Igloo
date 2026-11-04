package distributed

import (
	"slices"
	"strings"
)

// Outer joins by partition. A LEFT JOIN keeps every row of its left side,
// with NULLs for those no row of its right side matches: the right side's
// rows may not be divided among tasks by shares, as each task would miss
// the matches the others hold, and so a LEFT JOIN read its right side
// whole in every task. One too large for that, as a large relation or
// table is, is read by partition instead, by the key the join's condition
// makes equal (a.k = b.k), with the left side partitioned alike: every row
// meets all of its matches in its partition, and a left row no right row
// matches is in one partition only, which keeps it once. A FULL JOIN reads
// both sides so, or both whole, as each task would otherwise keep the
// unmatched rows of the side it read whole. RIGHT JOIN is LEFT JOIN turned
// around. The side that supplies NULLs keeps no group or partition of a
// window whole by its key, whose NULLs come from every partition (see
// fixesKey), and the statement's filters on it alone are not applied to it
// before the join, as they would let left rows through that the join
// matched (WHERE b.x IS NULL).

// outerJoin is an outer join of one FROM item to another.
type outerJoin struct {
	// kind is LEFT, RIGHT or FULL; cond the conjuncts of its condition.
	kind string
	cond []node
	// left and right are its sides' items.
	left, right *fromItem
}

// nullable reports whether side, oj's left or right item, supplies NULLs.
func (oj *outerJoin) nullable(side *fromItem) bool {
	switch oj.kind {
	case "LEFT":
		return side == oj.right
	case "RIGHT":
		return side == oj.left
	}
	return true
}

// usingConds are the conditions that ref's USING, or a natural join's common
// columns, stand for between the items of its sides, for telling what its
// sides are joined on: they are never printed. A column some items of a side
// share is the first's, which the side's own joins make equal to the others'.
func usingConds(ref node, left, right []*fromItem) []node {
	var columns []string
	for _, c := range list(ref, "using_columns") {
		columns = append(columns, strings.ToLower(str(c)))
	}
	if str(ref["ref_type"]) == "NATURAL" {
		for _, l := range left {
			if l.columns == nil {
				return nil
			}
			for _, col := range l.columns {
				if ownerIn(right, col) != nil && !slices.Contains(columns, col) {
					columns = append(columns, col)
				}
			}
		}
	}
	var out []node
	for _, col := range columns {
		l, r := ownerIn(left, col), ownerIn(right, col)
		if l == nil || r == nil {
			continue
		}
		out = append(out, node{"class": "COMPARISON", "type": "COMPARE_EQUAL",
			"left":  node{"class": "COLUMN_REF", "type": "COLUMN_REF", "column_names": []any{l.name, col}},
			"right": node{"class": "COLUMN_REF", "type": "COLUMN_REF", "column_names": []any{r.name, col}}})
	}
	return out
}

// ownerIn returns the first of items with a column called name (lower case).
func ownerIn(items []*fromItem, name string) *fromItem {
	for _, it := range items {
		if slices.Contains(it.columns, name) {
			return it
		}
	}
	return nil
}

// addOuterJoin records ref, a join whose sides added the uses and rels of
// rd between the indexes of us and rs (left side from [0] to [1], right
// from [1] to [2]), if it is a LEFT, RIGHT or FULL join of one item of sc
// to another with conditions conds, and marks the sides that may be read by
// partition for it.
func (sc *scope) addOuterJoin(ref node, rd *reads, conds []node, us, rs [3]int) {
	kind := str(ref["join_type"])
	switch kind {
	case "OUTER":
		kind = "FULL"
	case "ANTI":
		// It keeps the left rows that no right row matches: the right side
		// read by partition needs the left partitioned alike, as a LEFT JOIN's
		// does, or each task would keep a row the others match.
		kind = "LEFT"
	}
	if kind != "LEFT" && kind != "RIGHT" && kind != "FULL" || str(ref["ref_type"]) != "REGULAR" && str(ref["ref_type"]) != "NATURAL" || len(conds) == 0 {
		return
	}
	item := func(from, to [2]int) (*fromItem, func()) {
		uses, rels := rd.uses[from[0]:to[0]], rd.rels[from[1]:to[1]]
		switch {
		case len(uses)+len(rels) == 1 && len(uses) == 1 && uses[0].scope == sc:
			u := uses[0]
			return u.item, func() { u.outer = true }
		case len(uses)+len(rels) == 1 && len(rels) == 1 && rels[0].sc == sc:
			r := rels[0]
			return r.item, func() { r.outer = true }
		case len(uses) > 0 && len(rels) == 0:
			// A subquery that passes the rows of tables through: read by
			// partition, it is a relation (see chooseKeyed's tooDeep).
			inner := uses[0].scope
			if inner == nil || inner.ref == nil || inner.outer != sc || inner.item == nil ||
				slices.ContainsFunc(uses, func(u *tableUse) bool { return u.scope != inner }) {
				return nil, nil
			}
			return inner.item, func() {
				inner.outerSide = true
				for _, u := range uses {
					u.outerSub = true
				}
			}
		}
		return nil, nil
	}
	left, markLeft := item([2]int{us[0], rs[0]}, [2]int{us[1], rs[1]})
	if left == nil && str(ref["join_type"]) == "ANTI" {
		// An anti join of items joined to each other keeps each of their rows
		// by the item its condition reads: that one read by partition, every
		// row they make is in its key's partition alone (see existsJoin).
		left, markLeft = keyItem(conds, rd.uses[us[0]:us[1]], rd.rels[rs[0]:rs[1]], sc)
	}
	right, markRight := item([2]int{us[1], rs[1]}, [2]int{us[2], rs[2]})
	if left == nil || right == nil {
		return
	}
	oj := &outerJoin{kind: kind, cond: conds, left: left, right: right}
	sc.outerJoins = append(sc.outerJoins, oj)
	if oj.nullable(left) {
		markLeft()
	}
	if oj.nullable(right) {
		markRight()
	}
}

// keyItem returns the item of sc, among the tables uses and relations rels of
// a join's left side, whose columns the left of every one of conds, equalities
// of a column of the left side to one of the right, reads; and the func that
// marks it a side of the join. It returns nil if the conditions read more than
// one item, or one of none of them.
func keyItem(conds []node, uses []*tableUse, rels []*relUse, sc *scope) (*fromItem, func()) {
	name := ""
	for _, c := range conds {
		l := child(c, "left")
		parts := columnNames(l)
		if str(c["class"]) != "COMPARISON" || str(c["type"]) != "COMPARE_EQUAL" || str(l["class"]) != "COLUMN_REF" || len(parts) != 2 ||
			name != "" && !strings.EqualFold(name, parts[0]) {
			return nil, nil
		}
		name = strings.ToLower(parts[0])
	}
	for _, u := range uses {
		if u.scope == sc && u.item != nil && u.item.name == name {
			return u.item, func() {}
		}
	}
	for _, r := range rels {
		if r.sc == sc && r.item != nil && r.item.name == name {
			return r.item, func() {}
		}
	}
	return nil, nil
}

// outerPairs reads by partition the other side of each outer join of the
// candidate's keyed scope whose side that supplies NULLs is (or either
// side, for a FULL JOIN), and marks those sides nullable. A side that is
// not an input it may read by partition, as one read by bucket would be,
// keeps the candidate here.
func (pl *planner) outerPairs(c *candidate) error {
	if c.keyScope == nil {
		return nil
	}
	keyedOf := func(it *fromItem) *keyed {
		for _, k := range c.keyed {
			if k.item == it {
				return k
			}
		}
		return nil
	}
	for changed := true; changed; {
		changed = false
		for _, oj := range c.keyScope.outerJoins {
			l, r := keyedOf(oj.left), keyedOf(oj.right)
			if l == nil && r == nil || l != nil && r != nil {
				continue
			}
			// The side that supplies NULLs, read by partition, needs the
			// other partitioned alike; the side kept whole, read by
			// partition, may meet the other whole in every task, but for
			// a FULL JOIN, which keeps both.
			if !(oj.kind == "FULL" || oj.kind == "LEFT" && r != nil || oj.kind == "RIGHT" && l != nil) {
				continue
			}
			missing := oj.left
			if l != nil {
				missing = oj.right
			}
			k, err := c.inputOf(missing)
			if err != nil {
				return err
			}
			c.keyed = append(c.keyed, k)
			changed = true
		}
	}
	for _, oj := range c.keyScope.outerJoins {
		for _, side := range []*fromItem{oj.left, oj.right} {
			if k := keyedOf(side); k != nil && oj.nullable(side) {
				k.nullable = true
			}
		}
	}
	return nil
}

// inputOf returns the candidate's input of item, a table or a relation of
// its keyed scope, to read by partition, and takes it out of what every
// task reads whole.
func (c *candidate) inputOf(it *fromItem) (*keyed, error) {
	for _, u := range c.uses {
		if u.item == it && u.scope == c.keyScope {
			if len(list(u.ref, "column_name_alias")) > 0 {
				return nil, unsupported("%s with renamed columns", u.table)
			}
			return &keyed{use: u, item: it}, nil
		}
	}
	for i, r := range c.whole {
		if r.item == it && r.sc == c.keyScope {
			c.whole = slices.Delete(c.whole, i, i+1)
			return &keyed{rel: r, item: it}, nil
		}
	}
	return nil, unsupported("an outer join of %s, which workers cannot read by partition", it.name)
}

// checkOuterKeys checks that each outer join whose sides are read by
// partition is on every key they are partitioned by: its condition makes
// the two sides' key columns equal, so that the rows it matches meet.
func (c *candidate) checkOuterKeys() error {
	if c.keyScope == nil {
		return nil
	}
	keyedOf := func(it *fromItem) *keyed {
		for _, k := range c.keyed {
			if k.item == it {
				return k
			}
		}
		return nil
	}
	for _, oj := range c.keyScope.outerJoins {
		l, r := keyedOf(oj.left), keyedOf(oj.right)
		if l == nil || r == nil {
			continue
		}
		for i := range l.keys {
			if i >= len(r.keys) || !slices.ContainsFunc(oj.cond, func(cond node) bool { return equates(cond, c.keyScope, l, r, i) }) {
				return unsupported("an outer join of %s and %s not on the key they are partitioned by", l, r)
			}
		}
	}
	return nil
}

// equates reports whether cond makes key i of l equal to key i of r.
func equates(cond node, sc *scope, l, r *keyed, i int) bool {
	if str(cond["class"]) != "COMPARISON" || str(cond["type"]) != "COMPARE_EQUAL" {
		return false
	}
	a, b := child(cond, "left"), child(cond, "right")
	if str(a["class"]) != "COLUMN_REF" || str(b["class"]) != "COLUMN_REF" {
		return false
	}
	ai, bi := resolveColumn(a, sc), resolveColumn(b, sc)
	an, bn := strings.ToLower(lastName(a)), strings.ToLower(lastName(b))
	return ai == l.item && bi == r.item && an == l.keys[i] && bn == r.keys[i] ||
		ai == r.item && bi == l.item && an == r.keys[i] && bn == l.keys[i]
}

// hasOuter reports whether some input of the candidate may be read by
// partition for an outer join (see addOuterJoin).
func (c *candidate) hasOuter() bool {
	return slices.ContainsFunc(c.uses, func(u *tableUse) bool { return u.outer || u.outerSub }) ||
		slices.ContainsFunc(c.rels, func(r *relUse) bool { return r.outer })
}
