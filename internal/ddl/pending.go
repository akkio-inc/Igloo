package ddl

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
	icecat "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/catalog"
)

// PendingNotes returns the notes of tables being made in schema (see
// catalog.Pending).
func (m *Manager) PendingNotes(ctx context.Context, schema string) ([]catalog.Pending, error) {
	props, err := m.cat.LoadNamespaceProperties(ctx, table.Identifier{schema})
	if err != nil {
		return nil, err
	}
	return catalog.PendingNotes(props), nil
}

// NotePending writes note in schema's properties.
func (m *Manager) NotePending(ctx context.Context, schema string, note catalog.Pending) error {
	_, err := m.cat.UpdateNamespaceProperties(ctx, table.Identifier{schema}, nil, iceberg.Properties{note.Key: note.Value()})
	return err
}

// ForgetPending removes the notes keys from schema's properties.
func (m *Manager) ForgetPending(ctx context.Context, schema string, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	_, err := m.cat.UpdateNamespaceProperties(ctx, table.Identifier{schema}, keys, nil)
	return err
}

// holdSchema is the schema of the tables that ReleaseHold and HeldUnseen
// make for a moment.
var holdSchema = iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "igloo_hold", Type: iceberg.PrimitiveTypes.Int32})

// ReleaseHold makes a staged create's hold of name in schema go: it makes
// a table of the name, which Lakekeeper puts in the staged create's place,
// and drops it. The caller has made sure no table of the name loads.
func (m *Manager) ReleaseHold(ctx context.Context, schema, name string) error {
	ident := catalog.Ident(schema, name)
	if _, err := m.cat.CreateTable(ctx, ident, holdSchema); err != nil {
		return fmt.Errorf("release the hold of %s.%s: %w", schema, name, err)
	}
	if err := m.cat.PurgeTable(ctx, ident); err != nil {
		return fmt.Errorf("release the hold of %s.%s: %w", schema, name, err)
	}
	return nil
}

// HeldUnseen reports whether name in schema is held by a staged create
// that nobody committed: no table of the name loads, yet renaming one onto
// it fails as if one did. It renames an empty table of its own onto the
// name to tell, and drops it, so an empty table of the name shows for a
// moment when the name is free.
func (m *Manager) HeldUnseen(ctx context.Context, schema, name string) (bool, error) {
	if there, err := m.TableExists(ctx, schema, name); err != nil || there {
		return false, err
	}
	var b [4]byte
	_, _ = rand.Read(b[:])
	probe := catalog.Ident(schema, name+"__igloo_probe_"+hex.EncodeToString(b[:]))
	if _, err := m.cat.CreateTable(ctx, probe, holdSchema); err != nil {
		return false, err
	}
	target := catalog.Ident(schema, name)
	_, err := m.cat.RenameTable(ctx, probe, target)
	switch {
	case err == nil:
		// The name was free: the empty table under it goes.
		return false, m.cat.PurgeTable(context.WithoutCancel(ctx), target)
	case !alreadyExists(err):
		// The rename may have happened all the same, and only loading the
		// table after it failed: the table under the name goes if it is
		// the empty one of before.
		if tbl, lerr := m.cat.LoadTable(context.WithoutCancel(ctx), target); lerr == nil {
			if _, ok := tbl.Schema().FindFieldByName("igloo_hold"); ok && len(tbl.Schema().Fields()) == 1 {
				_ = m.cat.PurgeTable(context.WithoutCancel(ctx), target)
			}
		}
		_ = m.cat.PurgeTable(context.WithoutCancel(ctx), probe)
		return false, err
	}
	if err := m.cat.PurgeTable(context.WithoutCancel(ctx), probe); err != nil {
		return true, fmt.Errorf("drop %s after checking the name %s.%s: %w", probe, schema, name, err)
	}
	return true, nil
}

// alreadyExists reports whether err is the catalog's refusal of a name
// that is taken (409 AlreadyExistsException, which iceberg-go's rename
// does not map).
func alreadyExists(err error) bool {
	return err != nil && (errors.Is(err, icecat.ErrTableAlreadyExists) || strings.Contains(err.Error(), "AlreadyExists"))
}
