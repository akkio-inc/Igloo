package ddl

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	icecat "github.com/apache/iceberg-go/catalog"

	"github.com/akkio-inc/igloo/internal/catalog"
)

// CommentTarget is what a COMMENT ON statement names.
type CommentTarget struct {
	// Kind is SCHEMA, TABLE, VIEW or COLUMN.
	Kind         string
	Schema, Name string
	Column       string
	// Text is the new comment; nil removes it.
	Text *string
}

// Comment sets or removes a comment. COMMENT ON TABLE also works for views,
// as in PostgreSQL.
func (m *Manager) Comment(ctx context.Context, t CommentTarget) error {
	if t.Kind == "SCHEMA" {
		err := m.cat.SetSchemaComment(ctx, t.Schema, t.Text)
		if errors.Is(err, icecat.ErrNoSuchNamespace) {
			return fmt.Errorf("schema %q %w", t.Schema, ErrNotFound)
		}
		if err != nil {
			return err
		}
		m.reloadSchema(ctx, t.Schema)
		return nil
	}
	isView := m.IsView(t.Schema, t.Name)
	var err error
	switch {
	case t.Kind == "COLUMN" && isView:
		err = m.setViewColumnComment(ctx, t)
	case t.Kind == "COLUMN":
		err = m.cat.SetColumnComment(ctx, t.Schema, t.Name, t.Column, t.Text)
	case t.Kind == "VIEW" || isView:
		isView = true
		err = m.cat.SetViewComment(ctx, t.Schema, t.Name, t.Text)
	default:
		err = m.cat.SetTableComment(ctx, t.Schema, t.Name, t.Text)
	}
	if errors.Is(err, icecat.ErrNoSuchTable) || errors.Is(err, icecat.ErrNoSuchView) {
		return fmt.Errorf("relation %q %w", t.Name, ErrNotFound)
	}
	if err != nil {
		return err
	}
	m.reloadTable(ctx, t.Schema, t.Name, isView)
	return nil
}

// setViewColumnComment comments on a view's column, which gives the view a
// new version; the local copy records it, so the next sync does not
// recreate the copy for it.
func (m *Manager) setViewColumnComment(ctx context.Context, t CommentTarget) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.cat.SetViewColumnComment(ctx, t.Schema, t.Name, t.Column, t.Text); err != nil {
		return err
	}
	if v, ok, err := m.cat.LoadDuckDBView(ctx, t.Schema, t.Name); err == nil && ok {
		m.views[keyOf(t.Schema, t.Name)] = v
	}
	return nil
}

// Metadata returns the catalog's comments, columns and table sizes as last
// read (see RefreshMetadata), or nil before the first read.
func (m *Manager) Metadata() *catalog.Metadata {
	return m.meta.Load()
}

// RefreshMetadata makes sure the metadata is read before a query that
// reads it. Reading it loads every table's metadata, so it is read in full
// the first time, and afterwards again in the background once it is
// metadataTTL old, while queries see what was read last. Changes this
// replica makes to comments and views show at once: the objects changed
// are read again as part of the change.
func (m *Manager) RefreshMetadata(ctx context.Context) error {
	if m.meta.Load() == nil {
		m.readMu.Lock()
		defer m.readMu.Unlock()
		if m.meta.Load() != nil {
			return nil
		}
		return m.readMetadata(ctx)
	}
	m.metaMu.Lock()
	defer m.metaMu.Unlock()
	if time.Since(m.metaAt) < metadataTTL || m.metaReading {
		return nil
	}
	m.metaReading = true
	go func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Minute)
		defer cancel()
		m.readMu.Lock()
		err := m.readMetadata(ctx)
		m.readMu.Unlock()
		if err != nil {
			m.log.Warn("could not read the catalog's metadata", "err", err)
		}
		m.metaMu.Lock()
		m.metaReading = false
		m.metaMu.Unlock()
	}()
	return nil
}

// readMetadata reads every object's metadata. Objects read again on their
// own while it ran keep what that read found, which is newer. readMu must
// be held.
func (m *Manager) readMetadata(ctx context.Context) error {
	begin := time.Now()
	md, err := m.cat.Metadata(ctx)
	m.metaMu.Lock()
	defer m.metaMu.Unlock()
	if err != nil {
		return err
	}
	cur := m.meta.Load()
	for k, at := range m.touched {
		if at.Before(begin) {
			delete(m.touched, k)
			continue
		}
		if k.name == "" {
			var s *catalog.SchemaMeta
			if i := slices.IndexFunc(cur.Schemas, func(x catalog.SchemaMeta) bool { return strings.EqualFold(x.Name, k.schema) }); i >= 0 {
				s = &cur.Schemas[i]
			}
			md = md.WithSchema(k.schema, s)
			continue
		}
		var t *catalog.TableMeta
		if found, ok := cur.Table(k.schema, k.name); ok {
			t = &found
		}
		md = md.WithTable(k.schema, k.name, t)
	}
	m.meta.Store(md)
	m.metaAt = time.Now()
	return nil
}

// TableChanged reads the metadata of the table or view schema.name again
// in the background, after this replica wrote, created or dropped it, so
// its size and freshness show soon. Changes that come close together are
// read once, changedDelay after the first.
func (m *Manager) TableChanged(schema, name string) {
	if m.meta.Load() == nil {
		return
	}
	m.metaMu.Lock()
	defer m.metaMu.Unlock()
	if m.changed == nil {
		m.changed = map[key][2]string{}
		time.AfterFunc(changedDelay, m.reloadChanged)
	}
	m.changed[keyOf(schema, name)] = [2]string{schema, name}
}

// reloadChanged reads the tables TableChanged was told about.
func (m *Manager) reloadChanged() {
	m.metaMu.Lock()
	changed := m.changed
	m.changed = nil
	m.metaMu.Unlock()
	for _, t := range changed {
		m.reloadTable(context.Background(), t[0], t[1], m.IsView(t[0], t[1]))
	}
}

// reloadTable reads one table's or view's metadata into the last read, or
// takes it out if it is gone.
func (m *Manager) reloadTable(ctx context.Context, schema, name string, isView bool) {
	if m.meta.Load() == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	t, err := m.cat.TableMetadata(ctx, schema, name, isView)
	var replace *catalog.TableMeta
	switch {
	case err == nil:
		replace = &t
	case !errors.Is(err, icecat.ErrNoSuchTable) && !errors.Is(err, icecat.ErrNoSuchView):
		m.log.Warn("could not read a table's metadata", "table", schema+"."+name, "err", err)
		return
	}
	m.metaMu.Lock()
	defer m.metaMu.Unlock()
	m.meta.Store(m.meta.Load().WithTable(schema, name, replace))
	m.touched[keyOf(schema, name)] = time.Now()
}

// reloadSchema reads one schema's metadata into the last read.
func (m *Manager) reloadSchema(ctx context.Context, schema string) {
	if m.meta.Load() == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	s, err := m.cat.SchemaMetadata(ctx, schema)
	var replace *catalog.SchemaMeta
	switch {
	case err == nil:
		replace = &s
	case !errors.Is(err, icecat.ErrNoSuchNamespace):
		m.log.Warn("could not read a schema's metadata", "schema", schema, "err", err)
		return
	}
	m.metaMu.Lock()
	defer m.metaMu.Unlock()
	m.meta.Store(m.meta.Load().WithSchema(schema, replace))
	m.touched[keyOf(schema, "")] = time.Now()
}
