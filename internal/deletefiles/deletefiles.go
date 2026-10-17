// Package deletefiles tells which data files position delete files delete
// rows of, and so which of them a commit that replaces data files may
// remove.
//
// A position delete file can be scoped to one data file, which it then
// names as its referenced data file, or to a partition (as Spark before
// Iceberg 1.6, iceberg-go and DuckDB write them), and then delete rows of
// any of the partition's files: its file_path column says which. Planning
// a scan attaches a partition-scoped file to every data file of the
// partition, so a rewrite that removed every position delete file attached
// to the files it replaced would remove deletes of files it keeps too, and
// the rows they deleted would come back.
package deletefiles

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// Canonical spells a file's location the one way locations compare in:
// without the scheme, so that s3a:// and s3:// name the same object, and
// file:// a local path, with escaped characters unescaped and without
// doubled or trailing slashes. A table's metadata, a listing of its
// location and a position delete file can each spell a file otherwise, as
// after a migration from another engine. It may take two files for one, so
// it serves only where doing so keeps a file or a delete: never to decide
// that a file is gone or replaced.
func Canonical(p string) string {
	if scheme, rest, ok := strings.Cut(p, "://"); ok {
		p = rest
		if strings.EqualFold(scheme, "file") {
			p = strings.TrimPrefix(p, "localhost")
		}
	}
	if u, err := url.PathUnescape(p); err == nil {
		p = u
	}
	for strings.Contains(p, "//") {
		p = strings.ReplaceAll(p, "//", "/")
	}
	return strings.TrimSuffix(p, "/")
}

// Paths is a set of file locations, compared as Canonical spells them.
type Paths map[string]bool

// Add adds p.
func (s Paths) Add(p string) { s[Canonical(p)] = true }

// Has reports whether p, however spelled, is in s.
func (s Paths) Has(p string) bool { return s[Canonical(p)] }

// maxKnown bounds how many delete files' targets Targets remembers.
const maxKnown = 10_000

// Targets reads, and remembers, the data files position delete files
// delete rows of: delete files never change. It is safe for concurrent
// use.
type Targets struct {
	mu    sync.Mutex
	known map[string]map[string]bool
}

// NewTargets returns an empty Targets.
func NewTargets() *Targets { return &Targets{known: map[string]map[string]bool{}} }

// Of returns the data files the position delete file d deletes rows of, by
// path as d spells them.
func (t *Targets) Of(ctx context.Context, fs iceio.IO, d iceberg.DataFile) (map[string]bool, error) {
	if ref := d.ReferencedDataFile(); ref != nil {
		return map[string]bool{*ref: true}, nil
	}
	t.mu.Lock()
	targets, ok := t.known[d.FilePath()]
	t.mu.Unlock()
	if ok {
		return targets, nil
	}
	targets, err := read(ctx, fs, d)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	if len(t.known) >= maxKnown {
		clear(t.known)
	}
	t.known[d.FilePath()] = targets
	t.mu.Unlock()
	return targets, nil
}

// filePathID is the field ID of a position delete file's file_path column.
const filePathID = 2147483546

// readBatch is how many rows of a delete file's file_path column are read
// at a time.
const readBatch = 64 << 10

// read returns the distinct values of d's file_path column. A file whose
// manifest bounds say it names one file, as a file-scoped one does even
// without a referenced data file, is not opened; a row group whose
// statistics say it names one file is not read. Only the file_path column
// of the other row groups is, a batch at a time: a partition-wide delete
// file, as Spark writes them, can be large, and maintenance looks at many
// on every pass.
func read(ctx context.Context, fs iceio.IO, d iceberg.DataFile) (map[string]bool, error) {
	path := d.FilePath()
	if lo, hi := d.LowerBoundValues()[filePathID], d.UpperBoundValues()[filePathID]; lo != nil && bytes.Equal(lo, hi) {
		return map[string]bool{string(lo): true}, nil
	}
	f, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck
	rdr, err := file.NewParquetReader(f)
	if err != nil {
		return nil, fmt.Errorf("read position delete file %s: %w", path, err)
	}
	defer rdr.Close() //nolint:errcheck
	col := rdr.MetaData().Schema.ColumnIndexByName("file_path")
	if col < 0 {
		return nil, fmt.Errorf("position delete file %s has no file_path column", path)
	}
	targets := map[string]bool{}
	var scan []int
	for i := range rdr.NumRowGroups() {
		if one, ok := single(rdr.MetaData().RowGroup(i), col); ok {
			targets[one] = true
			continue
		}
		scan = append(scan, i)
	}
	if len(scan) == 0 {
		return targets, nil
	}
	fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{BatchSize: readBatch}, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}
	rr, err := fr.GetRecordReader(ctx, []int{col}, scan)
	if err != nil {
		return nil, fmt.Errorf("read position delete file %s: %w", path, err)
	}
	defer rr.Release()
	for rr.Next() {
		values, ok := rr.RecordBatch().Column(0).(interface {
			Len() int
			IsNull(int) bool
			Value(int) string
		})
		if !ok {
			return nil, fmt.Errorf("position delete file %s: file_path is %s", path, rr.RecordBatch().Column(0).DataType())
		}
		for i := range values.Len() {
			if !values.IsNull(i) {
				targets[values.Value(i)] = true
			}
		}
	}
	if err := rr.Err(); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("read position delete file %s: %w", path, err)
	}
	return targets, nil
}

// single returns the one value of column col in row group rg, when its
// statistics say it has only one.
func single(rg *metadata.RowGroupMetaData, col int) (string, bool) {
	cc, err := rg.ColumnChunk(col)
	if err != nil {
		return "", false
	}
	stats, err := cc.Statistics()
	if err != nil || stats == nil {
		return "", false
	}
	bs, ok := stats.(*metadata.ByteArrayStatistics)
	if !ok || !bs.HasMinMax() || !bytes.Equal(bs.Min(), bs.Max()) || bs.HasNullCount() && bs.NullCount() > 0 {
		return "", false
	}
	return string(bs.Min()), true
}

// Removable returns the position delete files attached to tasks that a
// commit replacing the data files rewritten may remove: those that delete
// rows only of files it replaces, or of files not live in the snapshot it
// commits on. One that deletes rows of a live file the commit keeps stays,
// and applies to that file as before.
//
// A target counts as replaced only if rewritten has it spelled exactly as
// the delete file spells it: two spellings Canonical takes for one file
// could be two files, and taking a kept file for a replaced one would drop
// deletes that still apply to it. A target counts as live if live has it
// in any spelling, which only keeps a delete file.
func (t *Targets) Removable(ctx context.Context, fs iceio.IO, tasks []table.FileScanTask, rewritten map[string]bool, live Paths) ([]iceberg.DataFile, error) {
	seen := map[string]bool{}
	var out []iceberg.DataFile
	for _, task := range tasks {
		for _, d := range task.DeleteFiles {
			if d.ContentType() != iceberg.EntryContentPosDeletes || seen[d.FilePath()] {
				continue
			}
			seen[d.FilePath()] = true
			targets, err := t.Of(ctx, fs, d)
			if err != nil {
				return nil, err
			}
			removable := true
			for p := range targets {
				if !rewritten[p] && live.Has(p) {
					removable = false
					break
				}
			}
			if removable {
				out = append(out, d)
			}
		}
	}
	return out, nil
}

// Live returns the data files live in snap.
func Live(fs iceio.IO, snap *table.Snapshot) (Paths, error) {
	out := Paths{}
	if snap == nil {
		return out, nil
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return nil, err
	}
	for _, m := range manifests {
		if m.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		for e, err := range m.Entries(fs, true) {
			if err != nil {
				return nil, err
			}
			out.Add(e.DataFile().FilePath())
		}
	}
	return out, nil
}
