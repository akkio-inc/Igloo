package ddl

import (
	"context"
	"errors"
	"fmt"

	icecat "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/catalog"
)

// SetSortOrder makes cols the default sort order of the catalog's table
// schema.name, or with no cols makes it unsorted (see catalog.SetSortOrder),
// and reads the table's metadata again so the catalog views show it.
func (m *Manager) SetSortOrder(ctx context.Context, schema, name string, cols []catalog.SortColumn) error {
	if m.IsView(schema, name) {
		return fmt.Errorf("%s.%s is a view; only tables have a sort order", schema, name)
	}
	err := m.cat.SetSortOrder(ctx, schema, name, cols)
	if errors.Is(err, icecat.ErrNoSuchTable) {
		return fmt.Errorf("table %q %w", name, ErrNotFound)
	}
	if err != nil {
		return err
	}
	m.reloadTable(ctx, schema, name, false)
	return nil
}

// LoadTable loads the catalog's table schema.name.
func (m *Manager) LoadTable(ctx context.Context, schema, name string) (*table.Table, error) {
	tbl, err := m.cat.LoadTable(ctx, catalog.Ident(schema, name))
	if errors.Is(err, icecat.ErrNoSuchTable) {
		return nil, fmt.Errorf("table %q %w", name, ErrNotFound)
	}
	return tbl, err
}

// SetDistinctCounts records counts as the distinct counts of the table
// schema.name's columns (see catalog.DistinctCountsProperty).
func (m *Manager) SetDistinctCounts(ctx context.Context, schema, name string, counts map[string]int64) error {
	if m.IsView(schema, name) {
		return fmt.Errorf("%s.%s is a view; only tables have distinct counts", schema, name)
	}
	err := m.cat.SetDistinctCounts(ctx, schema, name, counts)
	if errors.Is(err, icecat.ErrNoSuchTable) {
		return fmt.Errorf("table %q %w", name, ErrNotFound)
	}
	return err
}
