package distributed

import "strings"

// Relations of uncertain size. A relation is estimated at the size of the
// tables it reads (see estimate), which a filter on a constant may cut a
// thousandfold: an audience, the members of one version of one audience
// in a lookup of all of them, is estimated at the lookup's 757 GB and
// holds a couple of million IDs. Estimated larger than every worker may
// read, it is partitioned, and so is every table joined to it: at a
// hundred times the data, a report shuffled 465 GB of tables to join them
// to its audience, in 925 s, where one machine filters each table by the
// audience as it reads it.

// uncertain is a relation of uncertain size that a stage reads by
// partition, with other large inputs partitioned to meet it.
type uncertain struct {
	// name is the relation's name, for messages.
	name string
}

// uncertainSize reports whether a relation's estimate may be far too
// large: its query, or a common table expression it reads, filters rows by
// comparing a column to a constant or to a subquery's value, or by IN.
func uncertainSize(r *relUse) bool {
	parts := []any{r.q}
	if ctes, err := closure(r.q, r.scope); err == nil {
		for _, d := range ctes {
			parts = append(parts, d.query())
		}
	}
	found := false
	for _, part := range parts {
		walk(part, func(m node) bool {
			if found {
				return false
			}
			for _, c := range conjuncts(child(m, "where_clause")) {
				found = found || selectiveFilter(c)
			}
			return !found
		})
	}
	return found
}

// selectiveFilter reports whether a conjunct compares a column to a
// constant or to a subquery's value, with = or IN.
func selectiveFilter(c node) bool {
	switch str(c["type"]) {
	case "COMPARE_EQUAL":
		l, r := child(c, "left"), child(c, "right")
		value := func(n node) bool { return str(n["class"]) == "CONSTANT" || str(n["class"]) == "SUBQUERY" }
		return str(l["class"]) == "COLUMN_REF" && value(r) || str(r["class"]) == "COLUMN_REF" && value(l)
	case "COMPARE_IN":
		kids := list(c, "children")
		first, _ := kids[0].(node)
		return len(kids) > 1 && str(first["class"]) == "COLUMN_REF"
	}
	return str(c["class"]) == "SUBQUERY" && strings.EqualFold(str(c["subquery_type"]), "ANY")
}

// uncertainOf returns the relations of uncertain size among a candidate's
// inputs read by partition, when other large inputs are partitioned to
// meet them.
func uncertainOf(c *candidate) []uncertain {
	if len(c.keyed) < 2 {
		return nil
	}
	var out []uncertain
	for _, k := range c.keyed {
		if k.rel != nil && uncertainSize(k.rel) {
			out = append(out, uncertain{name: k.item.name})
		}
	}
	return out
}

// keepUncertainLocal keeps on one machine a statement that auto mode would
// run on workers, onWorkers and explore with reason as choose says, whose
// stages partition large tables to meet relations of uncertain size u:
// its estimates count those tables partitioned, where one machine filters
// them by the relation as it reads them (an audience report took 17 s
// there and 517 s on 16 workers). It runs on workers once they are known
// to be faster, or after it ran out of memory on one machine.
func keepUncertainLocal(u []uncertain, r runs, found, onWorkers, explore bool, reason string) (bool, bool, string) {
	if len(u) == 0 || !onWorkers || found && (r.outOfMemory || r.local > 0 && r.dist > 0 && r.dist < r.local) {
		return onWorkers, explore, reason
	}
	return false, false, "it would partition large tables to meet " + u[0].name + ", whose size is not known; " + reason
}
