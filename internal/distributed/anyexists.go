package distributed

import (
	"slices"
	"strconv"
	"strings"
)

// anyExists returns the one EXISTS that cond, an OR of EXISTS subqueries of the
// shape correlationOf takes, each making the same columns of the statement's
// equal to its own, means: whether any of their rows has the statement's
// values, as TPC-DS q10 and q35 ask whether a customer bought on the web or
// from the catalog:
//
//	EXISTS (SELECT 1 FROM w WHERE w.k = c.k AND fw) OR EXISTS (SELECT 1 FROM s WHERE s.k = c.k AND fs)
//	EXISTS (SELECT 1 FROM (SELECT k AS __igloo_k0 FROM w WHERE fw UNION ALL SELECT k FROM s WHERE fs) AS __igloo_anyN
//	  WHERE __igloo_anyN.__igloo_k0 = c.k)
//
// which existsJoin makes one join. Each branch's other conditions read only
// its own columns, so a row of the statement has a row of either exactly when
// it has one of the union. It returns nil for any other cond.
func (pl *planner) anyExists(cond node, ctes cteScope) node {
	if str(cond["class"]) != "CONJUNCTION" || str(cond["type"]) != "CONJUNCTION_OR" {
		return nil
	}
	var branches []string
	var outer []node
	for _, e := range list(cond, "children") {
		q, negated := existsOf(e.(node))
		if q == nil || negated {
			return nil
		}
		c := pl.correlationOf(q, ctes)
		if c == nil || len(c.residual) > 0 {
			return nil
		}
		if outer == nil {
			outer = c.outer
		} else if !slices.EqualFunc(outer, c.outer, func(a, b node) bool {
			return slices.EqualFunc(columnNames(a), columnNames(b), strings.EqualFold)
		}) {
			return nil
		}
		sub := clone(q).(node)
		items := make([]any, len(c.inner))
		for i, e := range c.inner {
			k := clone(e).(node)
			k["alias"] = "__igloo_k" + strconv.Itoa(i)
			items[i] = k
		}
		sub["select_list"] = items
		if pl.setWhere(sub, c.where) != nil {
			return nil
		}
		text, err := pl.p.print(sub)
		if err != nil {
			return nil
		}
		branches = append(branches, "("+text+")")
	}
	alias := "__igloo_any" + strconv.Itoa(pl.ins)
	pl.ins++
	var on []string
	for i := range outer {
		on = append(on, alias+".__igloo_k"+strconv.Itoa(i)+" = __igloo_outer"+strconv.Itoa(i))
	}
	e, err := pl.p.expr("EXISTS (SELECT 1 FROM (" + strings.Join(branches, " UNION ALL ") + ") AS " + alias +
		" WHERE " + strings.Join(on, " AND ") + ")")
	if err != nil {
		return nil
	}
	// The statement's columns in place of the placeholders.
	return rewrite(e, func(n node) (node, bool) {
		names := columnNames(n)
		if str(n["class"]) != "COLUMN_REF" || len(names) != 1 || !strings.HasPrefix(names[0], "__igloo_outer") {
			return nil, false
		}
		i, err := strconv.Atoi(strings.TrimPrefix(names[0], "__igloo_outer"))
		if err != nil || i >= len(outer) {
			return nil, false
		}
		return clone(outer[i]).(node), true
	}).(node)
}
