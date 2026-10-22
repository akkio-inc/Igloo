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
	// Doc is the column's comment, if any.
	Doc string
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
			v.Columns = append(v.Columns, Column{Name: f.Name, Type: duckDBType(f.Type), Doc: f.Doc})
		}
	}
	return v, true, nil
}

// RenameView renames the view schema.from to schema.to.
func (c *Client) RenameView(ctx context.Context, schema, from, to string) error {
	err := c.Catalog.RenameView(ctx, Ident(schema, from), Ident(schema, to))
	if errors.Is(err, icecat.ErrViewAlreadyExists) {
		return ErrViewExists
	}
	return err
}

// SetViewColumnComment sets or, when text is nil, removes the comment on a
// view's column: its field's doc string in the view's schema, as for
// tables. Column names match case-insensitively, as in DuckDB. The view
// gets a new version with the new schema and the same SQL.
func (c *Client) SetViewColumnComment(ctx context.Context, schema, name, column string, text *string) error {
	loaded, err := c.LoadView(ctx, Ident(schema, name))
	if err != nil {
		return err
	}
	v, ok, err := c.LoadDuckDBView(ctx, schema, name)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("view %q has no DuckDB SQL", name)
	}
	i := slices.IndexFunc(v.Columns, func(col Column) bool { return strings.EqualFold(col.Name, column) })
	if i < 0 {
		return fmt.Errorf("column %q of view %q does not exist", column, name)
	}
	v.Columns[i].Doc = ""
	if text != nil {
		v.Columns[i].Doc = *text
	}
	requirements, updates, err := replaceView(loaded.Metadata(), v)
	if err != nil {
		return err
	}
	_, err = c.UpdateView(ctx, Ident(schema, name), requirements, updates)
	return err
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
// recorded as strings. Lists, maps and structs are Iceberg's, their fields
// numbered after the columns.
func icebergSchema(id int, cols []Column) *iceberg.Schema {
	fields := make([]iceberg.NestedField, len(cols))
	next := len(cols)
	for i, c := range cols {
		fields[i] = iceberg.NestedField{ID: i + 1, Name: c.Name, Type: icebergType(c.Type, &next), Doc: c.Doc}
	}
	return iceberg.NewSchema(id, fields...)
}

var decimalType = regexp.MustCompile(`^DECIMAL\((\d+),\s*(\d+)\)$`)

// icebergType maps a DuckDB type name to the closest Iceberg type. The
// fields of lists, maps and structs take IDs after *next.
func icebergType(t string, next *int) iceberg.Type {
	t = strings.TrimSpace(t)
	if n := nestedType(t, next); n != nil {
		return n
	}
	switch t = strings.ToUpper(t); t {
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

// nestedType maps DuckDB's T[], MAP(K, V) and STRUCT(name T, ...) to
// Iceberg's list, map and struct, or returns nil for any other type.
func nestedType(t string, next *int) iceberg.Type {
	id := func() int { *next++; return *next }
	switch {
	case strings.HasSuffix(t, "[]"):
		elemID := id()
		return &iceberg.ListType{ElementID: elemID, Element: icebergType(strings.TrimSuffix(t, "[]"), next)}
	case !strings.HasSuffix(t, ")"):
		return nil
	}
	open := strings.IndexByte(t, '(')
	if open < 0 {
		return nil
	}
	args := splitTypeArgs(t[open+1 : len(t)-1])
	switch strings.ToUpper(strings.TrimSpace(t[:open])) {
	case "MAP":
		if len(args) != 2 {
			return nil
		}
		keyID, valueID := id(), id()
		return &iceberg.MapType{KeyID: keyID, KeyType: icebergType(args[0], next), ValueID: valueID, ValueType: icebergType(args[1], next)}
	case "STRUCT":
		fields := make([]iceberg.NestedField, len(args))
		ids := make([]int, len(args))
		for i := range args {
			ids[i] = id()
		}
		for i, a := range args {
			name, typ := fieldNameType(a)
			fields[i] = iceberg.NestedField{ID: ids[i], Name: name, Type: icebergType(typ, next)}
		}
		return &iceberg.StructType{FieldList: fields}
	}
	return nil
}

// splitTypeArgs splits a type's parameters at the commas outside
// parentheses and quotes.
func splitTypeArgs(s string) []string {
	var out []string
	depth, quoted, start := 0, false, 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '"':
			quoted = !quoted
		case quoted:
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == ',' && depth == 0:
			out = append(out, strings.TrimSpace(s[start:i]))
			start = i + 1
		}
	}
	return append(out, strings.TrimSpace(s[start:]))
}

// fieldNameType splits a struct's field, name and type, the name perhaps
// quoted.
func fieldNameType(f string) (string, string) {
	if strings.HasPrefix(f, `"`) {
		for i := 1; i < len(f); i++ {
			if f[i] != '"' {
				continue
			}
			if i+1 < len(f) && f[i+1] == '"' {
				i++
				continue
			}
			return strings.ReplaceAll(f[1:i], `""`, `"`), strings.TrimSpace(f[i+1:])
		}
	}
	name, typ, _ := strings.Cut(f, " ")
	return name, strings.TrimSpace(typ)
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
	case *iceberg.ListType:
		return duckDBType(t.Element) + "[]"
	case *iceberg.MapType:
		return "MAP(" + duckDBType(t.KeyType) + ", " + duckDBType(t.ValueType) + ")"
	case *iceberg.StructType:
		fields := make([]string, len(t.FieldList))
		for i, f := range t.FieldList {
			fields[i] = `"` + strings.ReplaceAll(f.Name, `"`, `""`) + `" ` + duckDBType(f.Type)
		}
		return "STRUCT(" + strings.Join(fields, ", ") + ")"
	}
	return "VARCHAR"
}
