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
	"errors"
	"io/fs"
	"slices"
	"testing"

	"gocloud.dev/blob/memblob"
)

// walked lists the files of a walk from root.
func walked(t *testing.T, bfs *blobFileIO, root string) ([]string, error) {
	t.Helper()
	var got []string
	err := bfs.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			got = append(got, p)
		}
		if len(got) > 1000 {
			t.Fatalf("a walk that does not end: %q", got[:5])
		}

		return nil
	})
	slices.Sort(got)

	return got, err
}

// TestWalkDirRootIsAlsoABlob checks that a walk from a directory that is also a blob
// of no length, as on an Azure account with a hierarchical namespace, lists what is
// under it, and that a walk from a file or from nothing still does what it did.
func TestWalkDirRootIsAlsoABlob(t *testing.T) {
	ctx := context.Background()
	b := memblob.OpenBucket(nil)
	defer b.Close() //nolint:errcheck
	for key, data := range map[string]string{"w/t": "", "w/t/data/a.parquet": "a", "w/t/metadata/m.json": "m", "w/file": "f", "w/t2/other": "o"} {
		if err := b.WriteAll(ctx, key, []byte(data), nil); err != nil {
			t.Fatal(err)
		}
	}
	bfs := &blobFileIO{Bucket: b, ctx: ctx}
	got, err := walked(t, bfs, "abfss://c@acct.host/w/t")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"abfss://c@acct.host/w/t/data/a.parquet", "abfss://c@acct.host/w/t/metadata/m.json"}; !slices.Equal(got, want) {
		t.Errorf("listed %q, want %q", got, want)
	}
	if got, err := walked(t, bfs, "abfss://c@acct.host/w/file"); err != nil || !slices.Equal(got, []string{"abfss://c@acct.host/w/file"}) {
		t.Errorf("a file: %q, %v", got, err)
	}
	if got, err := walked(t, bfs, "abfss://c@acct.host/w/none"); !errors.Is(err, fs.ErrNotExist) || len(got) != 0 {
		t.Errorf("nothing: %q, %v", got, err)
	}
}

// TestWalkDirDotSegment checks that a key with a .. segment, which a store allows and
// a worker could write, ends the walk and is listed as it is: a walk by directories
// comes back to where it was until the stack runs out.
func TestWalkDirDotSegment(t *testing.T) {
	ctx := context.Background()
	b := memblob.OpenBucket(nil)
	defer b.Close() //nolint:errcheck
	for _, key := range []string{"w/t/data/d/f/a.parquet", "w/t/data/d/f/../evil.parquet", "w/t/data//double.parquet"} {
		if err := b.WriteAll(ctx, key, []byte("x"), nil); err != nil {
			t.Fatal(err)
		}
	}
	bfs := &blobFileIO{Bucket: b, ctx: ctx}
	got, err := walked(t, bfs, "s3://b/w/t")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"s3://b/w/t/data//double.parquet", "s3://b/w/t/data/d/f/../evil.parquet", "s3://b/w/t/data/d/f/a.parquet"}
	if !slices.Equal(got, want) {
		t.Errorf("listed %q, want %q", got, want)
	}
	for key, dot := range map[string]bool{"a/../b": true, "../b": true, "a/..": false, "a/b..": false, "a/..b/c": false, "a/b": false} {
		if hasDotSegment(key) != dot {
			t.Errorf("%s: %v", key, !dot)
		}
	}
}

// TestWalkDirStops checks that a function that asks to stop stops it.
func TestWalkDirStops(t *testing.T) {
	ctx := context.Background()
	b := memblob.OpenBucket(nil)
	defer b.Close() //nolint:errcheck
	for _, key := range []string{"w/a", "w/b", "w/c"} {
		if err := b.WriteAll(ctx, key, []byte("x"), nil); err != nil {
			t.Fatal(err)
		}
	}
	bfs := &blobFileIO{Bucket: b, ctx: ctx}
	n := 0
	if err := bfs.WalkDir("s3://b/w", func(string, fs.DirEntry, error) error {
		n++

		return fs.SkipAll
	}); err != nil || n != 1 {
		t.Errorf("%d calls, %v", n, err)
	}
}
