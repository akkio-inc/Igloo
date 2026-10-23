package catalog

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// Sort orders. A table's default Iceberg sort order is how its rows should
// be ordered in its files: Snowflake's clustering key (CLUSTER BY), DuckDB's
// SORTED BY. Igloo's maintenance rewrites the files of a table that has one
// so that their ranges of the order's first column do not overlap, which
// lets a filter on it skip most files (see package maintenance). Other
// engines read the order from the table's metadata too.

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

// SetSortOrder makes cols, in order, the default sort order of the table
// schema.name, or with no cols makes the table unsorted. Column names match
// case-insensitively, as in DuckDB.
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
	if len(cols) == 0 {
		if meta.SortOrder().IsUnsorted() {
			return nil
		}
		_, _, err = c.CommitTable(ctx, tbl.Identifier(), reqs, []table.Update{table.NewSetDefaultSortOrderUpdate(table.UnsortedSortOrderID)})
		return err
	}
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
	// An order the table had before is made the default again; a new one
	// gets the next id.
	next := table.UnsortedSortOrderID + 1
	for _, o := range meta.SortOrders() {
		if sameFields(o, fields) {
			if o.OrderID() == meta.SortOrder().OrderID() {
				return nil
			}
			_, _, err = c.CommitTable(ctx, tbl.Identifier(), reqs, []table.Update{table.NewSetDefaultSortOrderUpdate(o.OrderID())})
			return err
		}
		next = max(next, o.OrderID()+1)
	}
	order, err := table.NewSortOrder(next, fields)
	if err != nil {
		return err
	}
	_, _, err = c.CommitTable(ctx, tbl.Identifier(), reqs,
		[]table.Update{table.NewAddSortOrderUpdate(&order), table.NewSetDefaultSortOrderUpdate(-1)})
	return err
}

// sameFields reports whether a sort order has exactly fields.
func sameFields(o table.SortOrder, fields []table.SortField) bool {
	if o.Len() != len(fields) {
		return false
	}
	for i, f := range o.Fields() {
		if !f.Equals(fields[i]) {
			return false
		}
	}
	return true
}

// ClusteringKey describes a table's default sort order as Snowflake shows a
// clustering key, LINEAR(a, b DESC), or "" when the table is unsorted. A
// field with a transform other than identity, which other engines can
// write, shows as the transform of its column, such as bucket[16](id).
func ClusteringKey(meta table.Metadata) string {
	order := meta.SortOrder()
	if order.IsUnsorted() {
		return ""
	}
	schema := meta.CurrentSchema()
	var parts []string
	for _, f := range order.Fields() {
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
