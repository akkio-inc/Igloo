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

package table

import "github.com/apache/iceberg-go"

// partitionSchema is the schema a manifest of spec is written with, whose
// partition type comes from the types of spec's source columns in it: the
// current schema when it has all of them, or else the newest of the
// table's schemas that does. Igloo patch: a column an older spec
// partitions by can be dropped once the default spec no longer uses it,
// and with the current schema the field was left out of the partition
// type, so every file of that spec in a manifest a commit wrote again,
// kept ones included, lost its partition value.
func (b *MetadataBuilder) partitionSchema(spec iceberg.PartitionSpec) *iceberg.Schema {
	current := b.CurrentSchema()
	if HasSourceColumns(spec, current) {
		return current
	}
	var newest *iceberg.Schema
	for _, s := range b.schemaList {
		if HasSourceColumns(spec, s) && (newest == nil || s.ID > newest.ID) {
			newest = s
		}
	}
	if newest == nil {
		return current
	}
	return newest
}

// HasSourceColumns reports whether schema has every column spec partitions
// by. Igloo addition.
func HasSourceColumns(spec iceberg.PartitionSpec, schema *iceberg.Schema) bool {
	for _, f := range spec.Fields() {
		for _, id := range f.SourceIDs {
			if _, ok := schema.FindFieldByID(id); !ok {
				return false
			}
		}
	}
	return true
}
