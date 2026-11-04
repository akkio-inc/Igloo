// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Igloo's addition to iceberg-go; see IGLOO_PATCHES.md.

package table

import (
	"context"
	"fmt"
	"runtime"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

// DataFiles returns the data files of the Parquet files at paths as
// AddFiles would add them to the table: checked against its current
// schema, with the metrics its properties ask for read from the files'
// footers, by their field ids when the files have them. It commits
// nothing, so the files can be committed with ReplaceFiles, in one
// snapshot with the files they replace.
func (t *Transaction) DataFiles(ctx context.Context, paths []string) ([]iceberg.DataFile, error) {
	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return nil, err
	}

	return filesToDataFiles(ctx, fs, t.meta, paths, runtime.GOMAXPROCS(0))
}

// DataFilesFrom is DataFiles of files another engine wrote as it could, reading
// their footers from fsys, for a caller that checks what its parser is to read. A
// column the table requires may be optional in a file, as COPY writes them all: the
// caller is to check that the file's null counts for it are none. The caller wrote
// each file to hold the rows of one partition, so a transform that does not keep
// order, bucket's, gives its partition from its lowest value, which its highest
// must agree with (see PartitionValueOfOne).
func (t *Transaction) DataFilesFrom(ctx context.Context, fsys iceio.IO, paths []string) ([]iceberg.DataFile, error) {
	spec, err := t.meta.CurrentSpec()
	if err != nil || spec == nil {
		return nil, fmt.Errorf("%w: cannot add files without a current spec", err)
	}
	schema := t.meta.CurrentSchema()
	fields := schema.Fields()
	for i := range fields {
		fields[i] = optionalField(fields[i])
	}

	return filesToDataFilesOf(ctx, fsys, iceberg.NewSchema(schema.ID, fields...), *spec, t.meta.defaultSortOrderID, t.meta.props, paths, runtime.GOMAXPROCS(0), true)
}

// optionalField is field with it and all it holds optional.
func optionalField(field iceberg.NestedField) iceberg.NestedField {
	field.Required = false
	field.Type = optionalType(field.Type)

	return field
}

func optionalType(t iceberg.Type) iceberg.Type {
	switch t := t.(type) {
	case *iceberg.StructType:
		fields := make([]iceberg.NestedField, len(t.FieldList))
		for i, f := range t.FieldList {
			fields[i] = optionalField(f)
		}

		return &iceberg.StructType{FieldList: fields}
	case *iceberg.ListType:
		return &iceberg.ListType{ElementID: t.ElementID, Element: optionalType(t.Element), ElementRequired: false}
	case *iceberg.MapType:
		return &iceberg.MapType{
			KeyID: t.KeyID, KeyType: t.KeyType, ValueID: t.ValueID, ValueType: optionalType(t.ValueType), ValueRequired: false,
		}
	}

	return t
}
