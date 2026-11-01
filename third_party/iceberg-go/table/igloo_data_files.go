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
	"runtime"

	"github.com/apache/iceberg-go"
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
