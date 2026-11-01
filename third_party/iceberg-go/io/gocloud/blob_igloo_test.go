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

package gocloud

import (
	"context"
	"io/fs"
	"slices"
	"testing"

	"gocloud.dev/blob/memblob"
)

// TestWalkDirRootIsAlsoABlob checks that a walk from a directory that is also a blob
// of no length, as on an Azure account with a hierarchical namespace, lists what is
// under it, and that a walk from a file or from nothing still does what it did.
func TestWalkDirRootIsAlsoABlob(t *testing.T) {
	ctx := context.Background()
	b := memblob.OpenBucket(nil)
	defer b.Close() //nolint:errcheck
	for key, data := range map[string]string{"w/t": "", "w/t/data/a.parquet": "a", "w/t/metadata/m.json": "m", "w/file": "f"} {
		if err := b.WriteAll(ctx, key, []byte(data), nil); err != nil {
			t.Fatal(err)
		}
	}
	bfs := &blobFileIO{Bucket: b, ctx: ctx}
	walk := func(root string) ([]string, error) {
		var got []string
		err := bfs.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				got = append(got, p)
			}

			return nil
		})
		slices.Sort(got)

		return got, err
	}
	got, err := walk("abfss://c@acct.host/w/t")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"abfss://c@acct.host/w/t/data/a.parquet", "abfss://c@acct.host/w/t/metadata/m.json"}; !slices.Equal(got, want) {
		t.Errorf("listed %q, want %q", got, want)
	}
	if got, err := walk("abfss://c@acct.host/w/file"); err != nil || !slices.Equal(got, []string{"abfss://c@acct.host/w/file"}) {
		t.Errorf("a file: %q, %v", got, err)
	}
	if got, err := walk("abfss://c@acct.host/w/none"); err == nil && len(got) != 0 {
		t.Errorf("nothing: %q", got)
	}
}
