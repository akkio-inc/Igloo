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

package gocloud

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	icebergio "github.com/apache/iceberg-go/io"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

// blobOpenFile describes a single open blob as a File.
// It implements the iceberg-go/io.File interface.
// It is based on gocloud.dev/blob.iofsOpenFile which:
// - Doesn't support the `io.ReaderAt` interface
// - Is not externally accessible, so copied here
type blobOpenFile struct {
	*blob.Reader

	name, key string
	b         *blobFileIO
	ctx       context.Context
}

func (f *blobOpenFile) ReadAt(p []byte, off int64) (n int, err error) {
	var rdr io.ReadCloser
	if f.b.newRangeReader != nil {
		rdr, err = f.b.newRangeReader(f.ctx, f.key, off, int64(len(p)))
	} else {
		rdr, err = f.b.NewRangeReader(f.ctx, f.key, off, int64(len(p)), nil)
	}
	if err != nil {
		return 0, err
	}
	// not using internal.CheckedClose due to import cycle
	defer func() { err = errors.Join(err, rdr.Close()) }()

	return io.ReadFull(rdr, p)
}

// Functions to implement the `Stat()` function in the `io/fs.File` interface

func (f *blobOpenFile) Name() string               { return f.name }
func (f *blobOpenFile) Mode() fs.FileMode          { return fs.ModeIrregular }
func (f *blobOpenFile) Sys() any                   { return f.b }
func (f *blobOpenFile) IsDir() bool                { return false }
func (f *blobOpenFile) Stat() (fs.FileInfo, error) { return f, nil }

// KeyExtractor extracts the object key from an input path
type KeyExtractor func(path string) (string, error)

// defaultKeyExtractor extracts the object key by removing the scheme and bucket name from the URI
// e.g., s3://bucket/path/file -> path/file
func defaultKeyExtractor(bucketName string) KeyExtractor {
	return func(location string) (string, error) {
		_, after, found := strings.Cut(location, "://")
		if found {
			location = after
		}

		key := strings.TrimPrefix(location, bucketName+"/")

		if key == "" {
			return "", fmt.Errorf("URI path is empty: %s", location)
		}

		return key, nil
	}
}

type blobFileIO struct {
	*blob.Bucket

	keyExtractor KeyExtractor
	ctx          context.Context

	// newRangeReader is an optional hook for testing.
	// It allows injecting a mock reader to verify Close calls.
	newRangeReader func(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
}

func (bfs *blobFileIO) preprocess(path string) (string, error) {
	return bfs.keyExtractor(path)
}

func (bfs *blobFileIO) Open(path string) (icebergio.File, error) {
	var err error
	path, err = bfs.preprocess(path)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: path, Err: err}
	}
	if !fs.ValidPath(path) {
		return nil, &fs.PathError{Op: "open", Path: path, Err: fs.ErrInvalid}
	}

	key, name := path, filepath.Base(path)

	r, err := bfs.NewReader(bfs.ctx, key, nil)
	if err != nil {
		return nil, err
	}

	return &blobOpenFile{Reader: r, name: name, key: key, b: bfs, ctx: bfs.ctx}, nil
}

func (bfs *blobFileIO) Remove(name string) error {
	var err error
	name, err = bfs.preprocess(name)
	if err != nil {
		return &fs.PathError{Op: "remove", Path: name, Err: err}
	}

	return bfs.Delete(bfs.ctx, name)
}

// Copy copies the file at src to dst, in the store, in the same bucket.
func (bfs *blobFileIO) Copy(ctx context.Context, src, dst string) error {
	srcKey, err := bfs.preprocess(src)
	if err != nil {
		return &fs.PathError{Op: "copy", Path: src, Err: err}
	}
	dstKey, err := bfs.preprocess(dst)
	if err != nil {
		return &fs.PathError{Op: "copy", Path: dst, Err: err}
	}

	return bfs.Bucket.Copy(ctx, dstKey, srcKey, nil)
}

func (bfs *blobFileIO) Create(name string) (icebergio.FileWriter, error) {
	return bfs.NewWriter(bfs.ctx, name, true, nil)
}

func (bfs *blobFileIO) WriteFile(name string, content []byte) error {
	var err error
	name, err = bfs.preprocess(name)
	if err != nil {
		return &fs.PathError{Op: "write file", Path: name, Err: err}
	}

	return bfs.WriteAll(bfs.ctx, name, content, nil)
}

// NewWriter returns a Writer that writes to the blob stored at path.
// A nil WriterOptions is treated the same as the zero value.
//
// If overwrite is disabled and a blob with this path already exists,
// an error will be returned.
//
// The caller must call Close on the returned Writer, even if the write is
// aborted.
func (bfs *blobFileIO) NewWriter(ctx context.Context, path string, overwrite bool, opts *blob.WriterOptions) (w *blobWriteFile, err error) {
	path, err = bfs.preprocess(path)
	if err != nil {
		return nil, &fs.PathError{Op: "new writer", Path: path, Err: err}
	}
	if !fs.ValidPath(path) {
		return nil, &fs.PathError{Op: "new writer", Path: path, Err: fs.ErrInvalid}
	}

	if !overwrite {
		if exists, err := bfs.Exists(ctx, path); err != nil || exists {
			if err != nil {
				return nil, &fs.PathError{Op: "new writer", Path: path, Err: err}
			}

			return nil, &fs.PathError{Op: "new writer", Path: path, Err: fs.ErrInvalid}
		}
	}
	bw, err := bfs.Bucket.NewWriter(ctx, path, opts)
	if err != nil {
		return nil, err
	}

	return &blobWriteFile{
			Writer: bw,
			name:   path,
		},
		nil
}

func createBlobFS(ctx context.Context, bucket *blob.Bucket, keyExtractor KeyExtractor) icebergio.IO {
	return &blobFileIO{Bucket: bucket, keyExtractor: keyExtractor, ctx: ctx}
}

func (bfs *blobFileIO) WalkDir(root string, fn fs.WalkDirFunc) error {
	parsed, err := url.Parse(root)
	if err != nil {
		return fmt.Errorf("invalid URL %s: %w", root, err)
	}

	walkPath := strings.TrimPrefix(parsed.Path, "/")
	if walkPath == "" {
		walkPath = "."
	}

	parsed.Path = ""

	return fs.WalkDir(walkFS{Bucket: bfs.Bucket, root: walkPath, ctx: bfs.ctx}, walkPath, func(path string, d fs.DirEntry, err error) error {
		return fn(parsed.JoinPath(path).String(), d, err)
	})
}

// walkFS is a bucket walked by its listings, in which what has something under it is
// a directory, whatever a blob of its name says: on an Azure account with a
// hierarchical namespace a directory also opens as a blob of no length, and fs.WalkDir
// would take it for one file, and list nothing under it.
type walkFS struct {
	*blob.Bucket
	root string
	ctx  context.Context
}

// Stat implements fs.StatFS.
func (w walkFS) Stat(name string) (fs.FileInfo, error) {
	if name == w.root && name != "." {
		it := w.Bucket.List(&blob.ListOptions{Prefix: name + "/"})
		if _, err := it.Next(w.ctx); err == nil {
			return walkEntry{name: path.Base(name), dir: true}, nil
		}
	}
	f, err := w.Bucket.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck

	return f.Stat()
}

// ReadDir implements fs.ReadDirFS.
func (w walkFS) ReadDir(name string) ([]fs.DirEntry, error) {
	prefix := name + "/"
	if name == "." {
		prefix = ""
	}
	it := w.Bucket.List(&blob.ListOptions{Prefix: prefix, Delimiter: "/"})
	var out []fs.DirEntry
	for {
		o, err := it.Next(w.ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return out, err
		}
		out = append(out, walkEntry{name: path.Base(strings.TrimSuffix(o.Key, "/")), dir: o.IsDir, size: o.Size, modTime: o.ModTime})
	}
	slices.SortFunc(out, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })

	return out, nil
}

// walkEntry is what a listing says of a file or directory.
type walkEntry struct {
	name    string
	dir     bool
	size    int64
	modTime time.Time
}

func (e walkEntry) Name() string { return e.name }
func (e walkEntry) Size() int64  { return e.size }
func (e walkEntry) IsDir() bool  { return e.dir }
func (e walkEntry) Sys() any     { return nil }

func (e walkEntry) Type() fs.FileMode { return e.Mode().Type() }

func (e walkEntry) Mode() fs.FileMode {
	if e.dir {
		return fs.ModeDir | 0o555
	}

	return 0o444
}

func (e walkEntry) ModTime() time.Time { return e.modTime }

func (e walkEntry) Info() (fs.FileInfo, error) { return e, nil }

func (bfs *blobFileIO) DeleteFiles(ctx context.Context, paths []string) ([]string, error) {
	if len(paths) == 0 {
		return nil, nil
	}

	deleted := make([]string, 0, len(paths))

	var errs error

	for _, p := range paths {
		key, err := bfs.preprocess(p)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("failed to delete %s: %w", p, err))

			continue
		}

		if err := bfs.Delete(ctx, key); err != nil {
			// Missing files are not errors per the interface contract.
			if gcerrors.Code(err) == gcerrors.NotFound {
				deleted = append(deleted, p)

				continue
			}

			errs = errors.Join(errs, fmt.Errorf("failed to delete %s: %w", p, err))

			continue
		}

		deleted = append(deleted, p)
	}

	return deleted, errs
}

type blobWriteFile struct {
	*blob.Writer
	name string
	b    *blobFileIO
}

func (f *blobWriteFile) Name() string                { return f.name }
func (f *blobWriteFile) Sys() any                    { return f.b }
func (f *blobWriteFile) Close() error                { return f.Writer.Close() }
func (f *blobWriteFile) Write(p []byte) (int, error) { return f.Writer.Write(p) }
