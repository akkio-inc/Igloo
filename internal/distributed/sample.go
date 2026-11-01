package distributed

import (
	"slices"
	"strings"
)

// Samples. TABLESAMPLE of a percentage of a table's rows, as Snowflake's
// SAMPLE (p) becomes, keeps each row with that probability (bernoulli) or
// each vector of rows (system): a share of the table sampled so is a sample
// of the share, and the shares' samples together one of the table. So a
// table the workers divide, by shares of its files or by bucket, is
// sampled where each task reads it, every task with the statement's seed:
// REPEATABLE (s) gives the same rows again for the same files on the same
// workers, as DuckDB gives them for the same files on one machine (which
// rows, not how many, differs from one machine's sample). A sample of a
// table every task reads whole, which tasks would each draw apart, or of
// one partitioned through an exchange, and one of a number of rows
// (reservoir), which shares do not add up to, keep the part on one machine.

// sampleOK checks a table reference's sample, if it has one: a percentage,
// by row or by vector.
func sampleOK(sample node) error {
	if sample == nil {
		return nil
	}
	if sample["is_percentage"] != true {
		return unsupported("TABLESAMPLE of a number of rows")
	}
	switch strings.ToLower(str(sample["method"])) {
	case "bernoulli", "system":
	default:
		return unsupported("TABLESAMPLE by %s", strings.ToLower(str(sample["method"])))
	}
	return nil
}

// checkSamples checks that the tables a candidate samples are each read by
// share or by bucket where its stage reads them (see the top of this file).
func (c *candidate) checkSamples() error {
	for _, u := range c.uses {
		if child(u.ref, "sample") == nil {
			continue
		}
		if u.scope == nil {
			return unsupported("TABLESAMPLE in a subquery expression")
		}
		if u != c.split && !slices.ContainsFunc(c.keyed, func(k *keyed) bool { return k.use == u && k.bucketed }) {
			return unsupported("TABLESAMPLE of %s, which tasks read whole or through an exchange", u.table)
		}
	}
	// A subquery that passes rows through, sampled where its rows are made:
	// each task's rows of it are its own, if it reads the divided table or
	// an input read by partition.
	var err error
	walk(child(c.sel, "from_table"), func(n node) bool {
		if err != nil || str(n["type"]) != "SUBQUERY" || child(n, "sample") == nil {
			return err == nil
		}
		if slices.ContainsFunc(c.rels, func(r *relUse) bool { return identity(r.ref) == identity(n) }) {
			// A relation's rows come from an exchange, read as a view.
			err = unsupported("TABLESAMPLE of a relation workers compute")
			return false
		}
		divided := c.split != nil && contains(n, c.split.ref)
		for _, k := range c.keyed {
			divided = divided || identity(k.ref()) != identity(n) && contains(n, k.ref())
		}
		if !divided {
			err = unsupported("TABLESAMPLE of a subquery whose rows every task would make")
		}
		return false
	})
	return err
}
