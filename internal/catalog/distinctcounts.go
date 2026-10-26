package catalog

import (
	"context"
	"encoding/json"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// DistinctCountsProperty is the table property in which Igloo keeps the
// distinct counts of a table's columns, a JSON object of counts by column
// name, which DuckDB's planner estimates filters and joins by (see the rb
// extension's rb_stats.cpp and ANALYZE in docs/sql.md).
const DistinctCountsProperty = "igloo.distinct-counts"

// SetDistinctCounts records counts as the distinct counts of the table
// schema.name's columns.
func (c *Client) SetDistinctCounts(ctx context.Context, schema, name string, counts map[string]int64) error {
	tbl, err := c.LoadTable(ctx, Ident(schema, name))
	if err != nil {
		return err
	}
	value, err := json.Marshal(counts)
	if err != nil {
		return err
	}
	// Lakekeeper rejects a commit without requirements.
	_, _, err = c.CommitTable(ctx, tbl.Identifier(),
		[]table.Requirement{table.AssertTableUUID(tbl.Metadata().TableUUID())},
		[]table.Update{table.NewSetPropertiesUpdate(iceberg.Properties{DistinctCountsProperty: string(value)})})
	return err
}
