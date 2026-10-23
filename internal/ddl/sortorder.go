package ddl

import (
	"context"
	"errors"
	"fmt"

	icecat "github.com/apache/iceberg-go/catalog"

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
