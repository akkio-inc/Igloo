package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// Clustering keys. A table's clustering key is how its rows should be
// ordered in its files: Snowflake's CLUSTER BY, DuckDB's SORTED BY. Igloo's
// maintenance rewrites the files of a table that has one so that their
// ranges of the key's first column do not overlap, which lets a filter on
// it skip most files (see package maintenance).
//
// Igloo keeps the key in the table property ClusteringKeyProperty, as the
// fields of an Iceberg sort order, and leaves the table's default Iceberg
// sort order unsorted: DuckDB 1.5.6, which writes Igloo's tables, refuses
// INSERT, UPDATE and MERGE on a table with a sort order. A table another
// engine gave a sort order is clustered by it, but DuckDB cannot write to
// it until SET SORTED BY (or RESET SORTED BY) moves the key into the
// property.

// ClusteringKeyProperty is the table property that holds a table's
// clustering key: a JSON array of Iceberg sort fields, as a sort order has
// them in a table's metadata.
const ClusteringKeyProperty = "igloo.clustering-key"

// SortColumn is a column of a sort order.
type SortColumn struct {
	Name       string
	Descending bool
	// NullsFirst, when set, says where NULLs go; by default first when
	// ascending and last when descending, as in Iceberg.
	NullsFirst *bool
}

var (
	// ErrNotSortable is the cause of a sort order refused for a column's
	// type: lists, maps and structs have no order Iceberg defines.
	ErrNotSortable = errors.New("a sort order's columns must be of a primitive type")
	// ErrNoSuchColumn is the cause of a sort order refused for a column the
	// table does not have.
	ErrNoSuchColumn = errors.New("no such column")
)

// SetSortOrder makes cols, in order, the clustering key of the table
// schema.name, or with no cols leaves the table without one. Either way the
// table's default Iceberg sort order becomes unsorted, so that DuckDB can
// write to it. Column names match case-insensitively, as in DuckDB.
func (c *Client) SetSortOrder(ctx context.Context, schema, name string, cols []SortColumn) error {
	tbl, err := c.LoadTable(ctx, Ident(schema, name))
	if err != nil {
		return err
	}
	meta := tbl.Metadata()
	reqs := []table.Requirement{
		table.AssertTableUUID(meta.TableUUID()),
		table.AssertDefaultSortOrderID(meta.SortOrder().OrderID()),
	}
	var updates []table.Update
	if !meta.SortOrder().IsUnsorted() {
		updates = append(updates, table.NewSetDefaultSortOrderUpdate(table.UnsortedSortOrderID))
	}
	current, has := meta.Properties()[ClusteringKeyProperty]
	if len(cols) == 0 {
		if has {
			updates = append(updates, table.NewRemovePropertiesUpdate([]string{ClusteringKeyProperty}))
		}
	} else {
		fields := make([]table.SortField, 0, len(cols))
		seen := map[int]bool{}
		for _, col := range cols {
			f, ok := tbl.Schema().FindFieldByNameCaseInsensitive(col.Name)
			if !ok {
				return fmt.Errorf("%w: column %q of table %q does not exist", ErrNoSuchColumn, col.Name, name)
			}
			if _, ok := f.Type.(iceberg.PrimitiveType); !ok {
				return fmt.Errorf("%w: %s is %s", ErrNotSortable, f.Name, f.Type)
			}
			if seen[f.ID] {
				return fmt.Errorf("column %s is in the sort order twice", f.Name)
			}
			seen[f.ID] = true
			sf := table.SortField{SourceIDs: []int{f.ID}, Transform: iceberg.IdentityTransform{}, Direction: table.SortASC, NullOrder: table.NullsFirst}
			if col.Descending {
				sf.Direction, sf.NullOrder = table.SortDESC, table.NullsLast
			}
			if col.NullsFirst != nil {
				sf.NullOrder = table.NullsLast
				if *col.NullsFirst {
					sf.NullOrder = table.NullsFirst
				}
			}
			fields = append(fields, sf)
		}
		key, err := json.Marshal(fields)
		if err != nil {
			return err
		}
		if !has || current != string(key) {
			updates = append(updates, table.NewSetPropertiesUpdate(iceberg.Properties{ClusteringKeyProperty: string(key)}))
		}
	}
	if len(updates) == 0 {
		return nil
	}
	_, _, err = c.CommitTable(ctx, tbl.Identifier(), reqs, updates)
	return err
}

// ClusteringOrder returns the fields of a table's clustering key: those of
// its ClusteringKeyProperty, or else of its default Iceberg sort order, or
// none.
func ClusteringOrder(meta table.Metadata) []table.SortField {
	if key, ok := meta.Properties()[ClusteringKeyProperty]; ok {
		var fields []table.SortField
		if err := json.Unmarshal([]byte(key), &fields); err == nil && len(fields) > 0 {
			return fields
		}
	}
	var fields []table.SortField
	for _, f := range meta.SortOrder().Fields() {
		fields = append(fields, f)
	}
	return fields
}

// ClusteringKey describes a table's clustering key (see ClusteringOrder) as
// Snowflake shows one, LINEAR(a, b DESC), or "" when it has none. A field
// with a transform other than identity, which other engines can write,
// shows as the transform of its column, such as bucket[16](id).
func ClusteringKey(meta table.Metadata) string {
	fields := ClusteringOrder(meta)
	if len(fields) == 0 {
		return ""
	}
	schema := meta.CurrentSchema()
	var parts []string
	for _, f := range fields {
		col := fmt.Sprint(f.SourceID())
		if field, ok := schema.FindFieldByID(f.SourceID()); ok {
			col = field.Name
		}
		if _, ok := f.Transform.(iceberg.IdentityTransform); !ok {
			col = fmt.Sprintf("%s(%s)", f.Transform, col)
		}
		if f.Direction == table.SortDESC {
			col += " DESC"
			if f.NullOrder == table.NullsFirst {
				col += " NULLS FIRST"
			}
		} else if f.NullOrder == table.NullsLast {
			col += " NULLS LAST"
		}
		parts = append(parts, col)
	}
	return "LINEAR(" + strings.Join(parts, ", ") + ")"
}
