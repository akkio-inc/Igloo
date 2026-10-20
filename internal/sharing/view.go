package sharing

import (
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/catalog"
)

// An external table's view reads the shared table's columns, in the
// table's order and by the names in its schema, from the Parquet files the
// provider listed: read_parquet over the proxy's URLs of the files, with
// union_by_name, as files written at different times can have different
// columns. The view names every column rather than taking whatever the
// files hold, because the files are not the table:
//
//   - Under Delta column mapping, a column's name in the files (its
//     physical name, in the schema's metadata) differs from its name in the
//     table, nested struct fields' too. A dropped column's values stay in
//     the files until they are rewritten, and a renamed column keeps its
//     physical name.
//   - Delta's row tracking writes columns of its own into rewritten files.
//   - A column added to the table is in no file written before.
//
// So Igloo binds a scan of the files first, which tells it the columns
// DuckDB finds in them, and the view reads each of the schema's columns by
// its physical name, rebuilding structs whose fields the files name
// differently, or NULL where no file has the column. Files with deletion
// vectors are read by a second scan that leaves their deleted rows out (see
// vectors.go).
//
// Column mapping mode id matches columns by Parquet field ID rather than
// by physical name. Delta writes the physical name too, so Igloo reads by
// name, after checking that every file's field IDs agree with the names
// (checkFieldIDs).

// remoteColumn is a column of a shared table.
type remoteColumn struct {
	// Name is the column's name in the table, and Physical its name in
	// the data files: the same, or the one column mapping gives it.
	Name, Physical string
	// Type is the column's Delta type as the schema has it in JSON: a
	// string, or a map for nested types.
	Type any
	// Partition marks a partition column, whose values are in the
	// files' URLs (see hiveDirs) rather than in the files.
	Partition bool
}

// columns lists the shared table's columns.
func (s *remoteSnapshot) columns() ([]remoteColumn, error) {
	rs, err := parseRemoteSchema(s.schema)
	if err != nil {
		return nil, err
	}
	if len(rs.Fields) == 0 {
		return nil, errors.New("the shared table has no columns")
	}
	cols := make([]remoteColumn, len(rs.Fields))
	for i, f := range rs.Fields {
		phys, err := physicalName(f.Name, f.Metadata, s.columnMapping != "")
		if err != nil {
			return nil, err
		}
		cols[i] = remoteColumn{Name: f.Name, Physical: phys, Type: f.Type}
	}
	for _, p := range s.partitionColumns {
		found := false
		for i := range cols {
			if cols[i].Name == p {
				cols[i].Partition, found = true, true
			}
		}
		if !found {
			return nil, fmt.Errorf("the shared table's partition column %s is not among its columns", p)
		}
	}
	return cols, nil
}

// physicalName is a column's or struct field's name in the data files.
func physicalName(name string, metadata map[string]any, mapping bool) (string, error) {
	if !mapping {
		return name, nil
	}
	phys, _ := metadata["delta.columnMapping.physicalName"].(string)
	if phys == "" {
		return "", fmt.Errorf("column %s of the shared table has no physical name, which column mapping needs", name)
	}
	return phys, nil
}

// fieldID is a column's or struct field's ID under column mapping.
func fieldID(metadata map[string]any) (int64, bool) {
	switch id := metadata["delta.columnMapping.id"].(type) {
	case float64:
		return int64(id), id == float64(int64(id))
	case json.Number:
		n, err := id.Int64()
		return n, err == nil
	}
	return 0, false
}

// remoteColumns are the columns of a shared table's view, with their
// DuckDB types, for the data catalog.
func remoteColumns(snap *remoteSnapshot) ([]catalog.ColumnMeta, error) {
	cols, err := snap.columns()
	if err != nil {
		return nil, err
	}
	out := make([]catalog.ColumnMeta, len(cols))
	for i, c := range cols {
		typeOf := duckType
		if c.Partition && len(snap.files) > 0 {
			typeOf = hiveType
		}
		t, err := typeOf(c.Type)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", c.Name, err)
		}
		out[i] = catalog.ColumnMeta{Name: c.Name, Type: t}
	}
	return out, nil
}

// emptyView is the query of the view of a table without files: no rows,
// with the table's columns.
func emptyView(cols []remoteColumn) (string, error) {
	parts := make([]string, len(cols))
	for i, c := range cols {
		t, err := duckType(c.Type)
		if err != nil {
			return "", fmt.Errorf("column %s: %w", c.Name, err)
		}
		parts[i] = "CAST(NULL AS " + t + ") AS " + quoteIdent(c.Name)
	}
	return "SELECT " + strings.Join(parts, ", ") + " LIMIT 0", nil
}

// view registers a snapshot's files with the proxy and returns the query
// of the external table's view over them, and how many Parquet scans it
// makes. t.mu must be held.
func (c *Consumer) view(ctx context.Context, t *external, snap *remoteSnapshot, expires time.Time) (string, int, error) {
	cols, err := snap.columns()
	if err != nil {
		return "", 0, err
	}
	var plain, deleted []remoteFile
	for _, f := range snap.files {
		if f.DV != nil && f.DV.Cardinality > 0 {
			deleted = append(deleted, f)
		} else {
			plain = append(plain, f)
		}
	}
	// Partition columns go into the files' URLs as Hive-style
	// directories, named as in the files' partition values.
	var partitions, hiveTypes []string
	for _, col := range cols {
		if col.Partition {
			ht, err := hiveType(col.Type)
			if err != nil {
				return "", 0, fmt.Errorf("partition column %s: %w", col.Name, err)
			}
			partitions = append(partitions, col.Physical)
			hiveTypes = append(hiveTypes, quoteLiteral(col.Physical)+": "+ht)
		}
	}
	urls := c.proxy.register(t.key, append(plain[:len(plain):len(plain)], deleted...), func(i int) string {
		if i < len(plain) {
			return hiveDirs(partitions, plain[i].PartitionValues)
		}
		i -= len(plain)
		return hiveDirs(partitions, deleted[i].PartitionValues) + vectorFileColumn + "=" + strconv.Itoa(i) + "/"
	})
	plainURLs, deletedURLs := urls[:len(plain)], urls[len(plain):]
	if len(deleted) == 0 {
		t.vectors, t.deletesSum, t.deletes = nil, "", nil
	}
	if len(urls) == 0 {
		t.shapes = nil
		sql, err := emptyView(cols)
		return sql, 0, err
	}

	shapes := map[string]map[string]*duckShape{}
	mapping := snap.columnMapping != ""
	var selects []string
	scans := 0
	if len(plainURLs) > 0 {
		from, columns, err := c.describe(ctx, t, snap, plainURLs, hiveTypes, shapes)
		if err != nil {
			return "", 0, err
		}
		list, err := projection(cols, columns, "", mapping)
		if err != nil {
			return "", 0, err
		}
		selects = append(selects, "SELECT "+list+" FROM "+from)
		scans++
	}
	if len(deletedURLs) > 0 {
		for _, col := range cols {
			if strings.EqualFold(col.Physical, vectorFileColumn) {
				return "", 0, fmt.Errorf("the shared table has a column named %s, which Igloo needs for deletion vectors", vectorFileColumn)
			}
		}
		deletesURL, err := c.deletes(ctx, t, deleted, expires)
		if err != nil {
			return "", 0, err
		}
		from, columns, err := c.describe(ctx, t, snap, deletedURLs, append(hiveTypes, quoteLiteral(vectorFileColumn)+": INTEGER"), shapes)
		if err != nil {
			return "", 0, err
		}
		if columns["file_row_number"] != nil {
			// It would hide DuckDB's row positions.
			return "", 0, errors.New("the shared table's files have a column named file_row_number, which Igloo needs for deletion vectors")
		}
		list, err := projection(cols, columns, "d.", mapping)
		if err != nil {
			return "", 0, err
		}
		selects = append(selects, "SELECT "+list+" FROM "+from+" AS d ANTI JOIN "+
			`(SELECT "file", unnest(range("start", "stop")) AS "row" FROM read_parquet(`+quoteLiteral(deletesURL)+`)) AS x `+
			"ON d."+quoteIdent(vectorFileColumn)+` = x."file" AND d.file_row_number = x."row"`)
		scans += 2
	}
	t.shapes = shapes
	return strings.Join(selects, " UNION ALL "), scans, nil
}

// readParquet is a scan of urls. hiveTypes type the Hive-style
// directories in them, if any.
func readParquet(urls, hiveTypes []string) string {
	quoted := make([]string, len(urls))
	for i, u := range urls {
		quoted[i] = quoteLiteral(u)
	}
	opts := ", union_by_name = true, hive_partitioning = false"
	if len(hiveTypes) > 0 {
		opts = ", union_by_name = true, hive_partitioning = true, hive_types = {" + strings.Join(hiveTypes, ", ") + "}"
	}
	return "read_parquet([" + strings.Join(quoted, ", ") + "]" + opts + ")"
}

// describe returns a scan of urls (see readParquet) and the columns DuckDB
// reads from it, by lower-cased name. Binding the scan reads every file's
// footer, so what it found is kept for the next listing, in next, by the
// scan's SQL: it names the same URLs while the files are the same.
func (c *Consumer) describe(ctx context.Context, t *external, snap *remoteSnapshot, urls, hiveTypes []string, next map[string]map[string]*duckShape) (string, map[string]*duckShape, error) {
	from := readParquet(urls, hiveTypes)
	if s, ok := t.shapes[from]; ok {
		next[from] = s
		return from, s, nil
	}
	c.connMu.Lock()
	defer c.connMu.Unlock()
	st, err := c.conn.PrepareContext(ctx, "SELECT * FROM "+from)
	if err != nil {
		return "", nil, fmt.Errorf("read the shared table's files: %w", err)
	}
	defer st.Close() //nolint:errcheck
	stmt, ok := st.(*duckdb.Stmt)
	if !ok {
		return "", nil, errors.New("read the shared table's files: not a DuckDB statement")
	}
	n, err := stmt.ColumnCount()
	if err != nil {
		return "", nil, err
	}
	out := make(map[string]*duckShape, n)
	for i := range n {
		name, err := stmt.ColumnName(i)
		if err != nil {
			return "", nil, err
		}
		s := &duckShape{}
		if info, err := stmt.ColumnTypeInfo(i); err == nil {
			s = shapeOf(info)
		}
		out[strings.ToLower(name)] = s
	}
	if snap.columnMapping == "id" {
		if err := c.checkFieldIDs(ctx, snap, urls); err != nil {
			return "", nil, err
		}
	}
	next[from] = out
	return from, out, nil
}

// checkFieldIDs refuses files, of a table in column mapping mode id, whose
// columns' field IDs disagree with their names: Igloo reads columns by
// name, and Delta matches them by ID in this mode. c.connMu must be held.
func (c *Consumer) checkFieldIDs(ctx context.Context, snap *remoteSnapshot, urls []string) error {
	rs, err := parseRemoteSchema(snap.schema)
	if err != nil {
		return err
	}
	names := map[int64]string{}
	var collect func(fields []remoteField) error
	collect = func(fields []remoteField) error {
		for _, f := range fields {
			phys, err := physicalName(f.Name, f.Metadata, true)
			if err != nil {
				return err
			}
			id, ok := fieldID(f.Metadata)
			if !ok {
				return fmt.Errorf("column %s of the shared table has no field ID, which column mapping mode id needs", f.Name)
			}
			names[id] = phys
			if err := collect(nestedFields(f.Type)); err != nil {
				return err
			}
		}
		return nil
	}
	if err := collect(rs.Fields); err != nil {
		return err
	}
	quoted := make([]string, len(urls))
	for i, u := range urls {
		quoted[i] = quoteLiteral(u)
	}
	rows, err := c.conn.QueryContext(ctx, "SELECT DISTINCT name, CAST(field_id AS BIGINT) FROM parquet_schema(["+
		strings.Join(quoted, ", ")+"]) WHERE field_id IS NOT NULL", nil)
	if err != nil {
		return fmt.Errorf("read the shared table's files: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	vals := make([]driver.Value, 2)
	for {
		if err := rows.Next(vals); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return fmt.Errorf("read the shared table's files: %w", err)
		}
		name, _ := vals[0].(string)
		id, _ := vals[1].(int64)
		if want, known := names[id]; known && name != want {
			return fmt.Errorf("a data file of the shared table has a column %s with the field ID of %s; "+
				"Igloo reads tables in column mapping mode id only when their files name their columns as the table's metadata does", name, want)
		}
	}
}

// nestedFields are the struct fields a Delta type holds, at any depth
// within arrays and maps too.
func nestedFields(t any) []remoteField {
	m, ok := t.(map[string]any)
	if !ok {
		return nil
	}
	switch m["type"] {
	case "struct":
		raw, _ := json.Marshal(m["fields"])
		var fields []remoteField
		_ = json.Unmarshal(raw, &fields)
		return fields
	case "array":
		return nestedFields(m["elementType"])
	case "map":
		return append(nestedFields(m["keyType"]), nestedFields(m["valueType"])...)
	}
	return nil
}

// deletes returns the URL of the file of rows that files' deletion vectors
// delete (see deletesFile), which the proxy serves until expires. The
// vectors and the file are kept for the next listing, which needs only the
// vectors that changed.
func (c *Consumer) deletes(ctx context.Context, t *external, files []remoteFile, expires time.Time) (string, error) {
	dvs := make([]*deletionVector, len(files))
	h := sha256.New()
	for i, f := range files {
		dvs[i] = f.DV
		h.Write([]byte(f.DV.key() + "\n"))
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if sum != t.deletesSum {
		vectors, cache, err := loadVectors(ctx, c.proxy.fetch, dvs, t.vectors)
		if err != nil {
			return "", err
		}
		data, err := deletesFile(dvs, vectors)
		if err != nil {
			return "", err
		}
		t.vectors, t.deletesSum, t.deletes = cache, sum, data
	}
	return c.proxy.serve(t.key, t.deletes, expires), nil
}

// duckShape is the type of a column DuckDB reads, as far as matching it to
// a Delta type needs: its struct fields, list elements and map entries.
type duckShape struct {
	// kind is 's' for a struct, 'l' for a list or array, 'm' for a map,
	// and 0 for anything else.
	kind       byte
	fields     []shapeField
	elem       *duckShape
	key, value *duckShape
}

type shapeField struct {
	name  string
	shape *duckShape
}

func shapeOf(info duckdb.TypeInfo) *duckShape {
	switch d := info.Details().(type) {
	case *duckdb.StructDetails:
		s := &duckShape{kind: 's'}
		for _, e := range d.Entries {
			s.fields = append(s.fields, shapeField{name: e.Name(), shape: shapeOf(e.Info())})
		}
		return s
	case *duckdb.ListDetails:
		return &duckShape{kind: 'l', elem: shapeOf(d.Child)}
	case *duckdb.ArrayDetails:
		return &duckShape{kind: 'l', elem: shapeOf(d.Child)}
	case *duckdb.MapDetails:
		return &duckShape{kind: 'm', key: shapeOf(d.Key), value: shapeOf(d.Value)}
	}
	return &duckShape{}
}

// field is a struct's field, by name as DuckDB matches names: without case.
func (s *duckShape) field(name string) *duckShape {
	for _, f := range s.fields {
		if strings.EqualFold(f.name, name) {
			return f.shape
		}
	}
	return nil
}

// projection is the select list that reads cols from a scan whose columns
// DuckDB found to be columns (by lower-cased name), each column qualified
// by q. mapping says whether the table uses column mapping.
func projection(cols []remoteColumn, columns map[string]*duckShape, q string, mapping bool) (string, error) {
	parts := make([]string, len(cols))
	for i, col := range cols {
		var expr string
		switch s := columns[strings.ToLower(col.Physical)]; {
		case col.Partition:
			expr = q + quoteIdent(col.Physical)
		case s == nil:
			// In none of the files.
			t, err := duckType(col.Type)
			if err != nil {
				return "", fmt.Errorf("column %s: %w", col.Name, err)
			}
			expr = "CAST(NULL AS " + t + ")"
		default:
			var err error
			if expr, _, err = valueExpr(q+quoteIdent(col.Physical), col.Type, s, mapping, 0); err != nil {
				return "", fmt.Errorf("column %s: %w", col.Name, err)
			}
		}
		parts[i] = expr + " AS " + quoteIdent(col.Name)
	}
	return strings.Join(parts, ", "), nil
}

// valueExpr is the SQL that reads a value of Delta type t from expr, which
// DuckDB reads with shape s: expr itself where the files hold it as t has
// it, or else (changed) the value rebuilt, its struct fields read by their
// physical names, named as in t and in t's order, and NULL where the files
// lack one. mapping says whether the table uses column mapping, and depth
// names the variables of nested lambdas.
func valueExpr(expr string, t any, s *duckShape, mapping bool, depth int) (sql string, changed bool, err error) {
	m, ok := t.(map[string]any)
	if !ok {
		return expr, false, nil
	}
	v := "x" + strconv.Itoa(depth)
	switch m["type"] {
	case "struct":
		if s.kind != 's' {
			return "", false, errors.New("the files hold another type than the schema's struct")
		}
		raw, _ := json.Marshal(m["fields"])
		var fields []remoteField
		if err := json.Unmarshal(raw, &fields); err != nil {
			return "", false, err
		}
		changed = len(fields) != len(s.fields)
		parts := make([]string, len(fields))
		for i, f := range fields {
			phys, err := physicalName(f.Name, f.Metadata, mapping)
			if err != nil {
				return "", false, err
			}
			if i >= len(s.fields) || s.fields[i].name != f.Name {
				changed = true
			}
			var part string
			if child := s.field(phys); child == nil {
				ft, err := duckType(f.Type)
				if err != nil {
					return "", false, err
				}
				part, changed = "CAST(NULL AS "+ft+")", true
			} else {
				sub, ch, err := valueExpr("struct_extract("+expr+", "+quoteLiteral(phys)+")", f.Type, child, mapping, depth)
				if err != nil {
					return "", false, err
				}
				part, changed = sub, changed || ch
			}
			parts[i] = quoteIdent(f.Name) + " := " + part
		}
		if !changed {
			return expr, false, nil
		}
		return "CASE WHEN " + expr + " IS NULL THEN NULL ELSE struct_pack(" + strings.Join(parts, ", ") + ") END", true, nil
	case "array":
		if s.kind != 'l' {
			return "", false, errors.New("the files hold another type than the schema's array")
		}
		sub, ch, err := valueExpr(v, m["elementType"], s.elem, mapping, depth+1)
		if err != nil || !ch {
			return expr, false, err
		}
		return "list_transform(" + expr + ", lambda " + v + ": " + sub + ")", true, nil
	case "map":
		if s.kind != 'm' {
			return "", false, errors.New("the files hold another type than the schema's map")
		}
		k, chk, err := valueExpr("struct_extract("+v+", 'key')", m["keyType"], s.key, mapping, depth+1)
		if err != nil {
			return "", false, err
		}
		val, chv, err := valueExpr("struct_extract("+v+", 'value')", m["valueType"], s.value, mapping, depth+1)
		if err != nil {
			return "", false, err
		}
		if !chk && !chv {
			return expr, false, nil
		}
		return "map_from_entries(list_transform(map_entries(" + expr + "), lambda " + v + `: struct_pack("key" := ` + k +
			`, "value" := ` + val + ")))", true, nil
	}
	return "", false, fmt.Errorf("unsupported Delta type %v", m["type"])
}
