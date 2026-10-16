package catalog

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/iceberg-go"
	icecat "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/view"
	"golang.org/x/sync/errgroup"
)

// Dialect is the SQL dialect of the views Igloo stores.
const Dialect = "duckdb"

// View is a view as Igloo stores it in the catalog: an Iceberg view whose
// SQL is in DuckDB's dialect.
type View struct {
	Schema string
	Name   string
	// SQL is the view's SELECT.
	SQL string
	// Columns are the view's column names and DuckDB types.
	Columns []Column
	// Version is the catalog's current version id, which changes whenever
	// the view is replaced.
	Version int64
}

// Column is a view column.
type Column struct {
	Name string
	// Type is the DuckDB type, as DESCRIBE reports it.
	Type string
}

// LoadDuckDBView loads one view. ok is false if it has no DuckDB SQL, as
// with views other engines created.
func (c *Client) LoadDuckDBView(ctx context.Context, schema, name string) (v View, ok bool, err error) {
	loaded, err := c.LoadView(ctx, Ident(schema, name))
	if err != nil {
		return View{}, false, err
	}
	version := loaded.CurrentVersion()
	if version == nil {
		return View{}, false, nil
	}
	i := slices.IndexFunc(version.Representations, func(r view.Representation) bool {
		return strings.EqualFold(r.Dialect, Dialect)
	})
	if i < 0 {
		return View{}, false, nil
	}
	v = View{Schema: schema, Name: name, SQL: version.Representations[i].Sql, Version: version.VersionID}
	if sc := loaded.CurrentSchema(); sc != nil {
		for _, f := range sc.Fields() {
			v.Columns = append(v.Columns, Column{Name: f.Name, Type: duckDBType(f.Type)})
		}
	}
	return v, true, nil
}

// DuckDBViews returns every view that has DuckDB SQL.
func (c *Client) DuckDBViews(ctx context.Context) ([]View, error) {
	idents, err := c.Views(ctx)
	if err != nil {
		return nil, err
	}
	var (
		mu  sync.Mutex
		out []View
	)
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for _, ident := range idents {
		g.Go(func() error {
			schema, name := splitIdent(ident)
			v, ok, err := c.LoadDuckDBView(ctx, schema, name)
			if err != nil || !ok {
				return ignoreDropped(err)
			}
			mu.Lock()
			out = append(out, v)
			mu.Unlock()
			return nil
		})
	}
	return out, g.Wait()
}

// PutView stores v. If the view exists, replace says whether to replace
// it; otherwise PutView fails with ErrViewExists.
func (c *Client) PutView(ctx context.Context, v View, replace bool) error {
	ident := Ident(v.Schema, v.Name)
	existing, err := c.LoadView(ctx, ident)
	switch {
	case errors.Is(err, icecat.ErrNoSuchView):
		version, err := view.NewVersion(1, 0, []view.Representation{view.NewRepresentation(v.SQL, Dialect)},
			[]string{v.Schema}, view.WithVersionSummary(view.VersionSummary{"engine-name": "igloo"}))
		if err != nil {
			return err
		}
		_, err = c.Catalog.CreateView(ctx, ident, version, icebergSchema(0, v.Columns))
		if errors.Is(err, icecat.ErrViewAlreadyExists) {
			return ErrViewExists
		}
		return err
	case err != nil:
		return err
	case !replace:
		return ErrViewExists
	}

	requirements, updates, err := replaceView(existing.Metadata(), v)
	if err != nil {
		return err
	}
	_, err = c.UpdateView(ctx, ident, requirements, updates)
	return err
}

// replaceView returns the changes that make v the current version of the
// view whose metadata is meta.
//
// A view can go back to columns it had before, and the catalog then reuses
// the earlier schema rather than adding one, so the changes come from the
// view metadata builder, which names a reused schema by its ID and a new
// one as the last added, as the catalog expects. A version equal to an
// earlier one would be reused the same way, but Lakekeeper then fails to
// record the change in the view's version log (a duplicate key), so every
// replacement adds a version: its summary names the version it replaces,
// which no earlier version can, as the current version is always the
// latest.
func replaceView(meta view.Metadata, v View) ([]view.Requirement, []view.Update, error) {
	summary := view.VersionSummary{"engine-name": "igloo", "igloo.replaces-version": strconv.FormatInt(meta.CurrentVersionID(), 10)}
	// The builder assigns the version and schema IDs.
	version, err := view.NewVersion(1, view.LastAddedID, []view.Representation{view.NewRepresentation(v.SQL, Dialect)},
		[]string{v.Schema}, view.WithVersionSummary(summary))
	if err != nil {
		return nil, nil, err
	}
	b, err := view.MetadataBuilderFromBase(meta)
	if err != nil {
		return nil, nil, err
	}
	res, err := b.SetCurrentVersion(version, icebergSchema(0, v.Columns)).Build()
	if err != nil {
		return nil, nil, err
	}
	return []view.Requirement{view.AssertViewUUID(meta.ViewUUID())}, res.Changes, nil
}

// ErrViewExists is returned by PutView for a view that exists.
var ErrViewExists = errors.New("view already exists")

// icebergSchema describes columns for the catalog. Other engines cannot run
// DuckDB SQL anyway, so types Iceberg has no direct equivalent for are
// recorded as strings.
func icebergSchema(id int, cols []Column) *iceberg.Schema {
	fields := make([]iceberg.NestedField, len(cols))
	for i, c := range cols {
		fields[i] = iceberg.NestedField{ID: i + 1, Name: c.Name, Type: icebergType(c.Type)}
	}
	return iceberg.NewSchema(id, fields...)
}

var decimalType = regexp.MustCompile(`^DECIMAL\((\d+),\s*(\d+)\)$`)

// icebergType maps a DuckDB type name to the closest Iceberg type.
func icebergType(t string) iceberg.Type {
	switch t = strings.ToUpper(strings.TrimSpace(t)); t {
	case "BOOLEAN":
		return iceberg.PrimitiveTypes.Bool
	case "TINYINT", "SMALLINT", "INTEGER", "UTINYINT", "USMALLINT":
		return iceberg.PrimitiveTypes.Int32
	case "BIGINT", "UINTEGER":
		return iceberg.PrimitiveTypes.Int64
	case "FLOAT":
		return iceberg.PrimitiveTypes.Float32
	case "DOUBLE":
		return iceberg.PrimitiveTypes.Float64
	case "DATE":
		return iceberg.PrimitiveTypes.Date
	case "TIME":
		return iceberg.PrimitiveTypes.Time
	case "TIMESTAMP":
		return iceberg.PrimitiveTypes.Timestamp
	case "TIMESTAMP WITH TIME ZONE":
		return iceberg.PrimitiveTypes.TimestampTz
	case "UUID":
		return iceberg.PrimitiveTypes.UUID
	case "BLOB":
		return iceberg.PrimitiveTypes.Binary
	}
	if m := decimalType.FindStringSubmatch(t); m != nil {
		p, _ := strconv.Atoi(m[1])
		s, _ := strconv.Atoi(m[2])
		return iceberg.DecimalTypeOf(p, s)
	}
	return iceberg.PrimitiveTypes.String
}

// duckDBType maps an Iceberg type back to a DuckDB type name.
func duckDBType(t iceberg.Type) string {
	switch t := t.(type) {
	case iceberg.BooleanType:
		return "BOOLEAN"
	case iceberg.Int32Type:
		return "INTEGER"
	case iceberg.Int64Type:
		return "BIGINT"
	case iceberg.Float32Type:
		return "FLOAT"
	case iceberg.Float64Type:
		return "DOUBLE"
	case iceberg.DateType:
		return "DATE"
	case iceberg.TimeType:
		return "TIME"
	case iceberg.TimestampType:
		return "TIMESTAMP"
	case iceberg.TimestampTzType:
		return "TIMESTAMP WITH TIME ZONE"
	case iceberg.UUIDType:
		return "UUID"
	case iceberg.BinaryType:
		return "BLOB"
	case iceberg.DecimalType:
		return fmt.Sprintf("DECIMAL(%d,%d)", t.Precision(), t.Scale())
	}
	return "VARCHAR"
}
