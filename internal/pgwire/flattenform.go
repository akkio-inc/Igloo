package pgwire

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
)

// listsShare is the share of the flattened tables' rows that, estimated to
// reach the dependent joins, makes the FROM items compute the lists.
const listsShare = 0.25

// formOf is the form of tr's FLATTENs for the history, if they could take either.
func formOf(tr *translation) statementForm {
	if tr == nil || tr.flattenKey == "" {
		return statementForm{}
	}
	return statementForm{key: tr.flattenKey, lists: tr.flattenLists}
}

// statementForm is what the history keeps of a statement's FLATTENs (see
// workload.Controller.ObserveForm): their shape's key, and whether they unnest
// lists their FROM items compute.
type statementForm struct {
	key   string
	lists bool
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
