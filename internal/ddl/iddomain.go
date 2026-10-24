package ddl

import (
	"context"
	"errors"
	"fmt"

	icecat "github.com/apache/iceberg-go/catalog"
)

// JoinIDDomain makes column of the catalog's table schema.name a member of
// ID domain domain (see catalog.JoinIDDomain), with hidden the column that
// holds its ids, and reads the table's metadata again so the catalog views
// show it.
func (m *Manager) JoinIDDomain(ctx context.Context, schema, name, column, hidden, domain string, generation int) error {
	if m.IsView(schema, name) {
		return fmt.Errorf("%s.%s is a view; only tables' columns join ID domains", schema, name)
	}
	err := m.cat.JoinIDDomain(ctx, schema, name, column, hidden, domain, generation)
	if errors.Is(err, icecat.ErrNoSuchTable) {
		return fmt.Errorf("table %q %w", name, ErrNotFound)
	}
	if err != nil {
		return err
	}
	m.reloadTable(ctx, schema, name, false)
	return nil
}

// LeaveIDDomain undoes JoinIDDomain. A table that no longer exists has
// nothing to undo.
func (m *Manager) LeaveIDDomain(ctx context.Context, schema, name, column, hidden string) error {
	err := m.cat.LeaveIDDomain(ctx, schema, name, column, hidden)
	if errors.Is(err, icecat.ErrNoSuchTable) {
		return nil
	}
	if err != nil {
		return err
	}
	m.reloadTable(ctx, schema, name, false)
	return nil
}
