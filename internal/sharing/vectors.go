package sharing

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"golang.org/x/sync/errgroup"

	"github.com/akkio-inc/igloo/internal/deletefiles"
)

// Deletion vectors. A Delta table with the deletionVectors feature deletes
// rows by marking them in a deletion vector beside the data file, which
// keeps them, rather than by rewriting the file. The delta response format
// lists each data file with its vector: inline, or at an offset in a file
// of vectors behind a pre-signed URL. Readers must leave the marked rows
// out; the parquet response format cannot say which they are, so providers
// refuse it for such tables.
//
// When Igloo lists an external table's files it reads every vector, from
// the provider or from what it read for the previous listing, and writes
// the deleted rows as runs of row positions to one small Parquet file that
// the proxy serves from memory. The table's view reads the data files that
// have vectors with DuckDB's file_row_number and leaves out, with an anti
// join, the positions that file lists for them. DuckDB holds the deleted
// positions of a query in memory and spills them to disk when they do not
// fit, so a query's cost grows with the rows deleted, not with the table,
// and files without vectors are read as they are.
//
// Files are matched to their vectors by a number in their proxy URL, which
// DuckDB reads as a Hive partition column (vectorFileColumn): its own
// file_index renumbers the files left after partition pruning.

const (
	// maxVectorBytes bounds one serialized deletion vector. A Roaring
	// bitmap takes at most 8 KiB for 65,536 rows, so this covers files of
	// half a billion rows.
	maxVectorBytes = 64 << 20
	// maxVectorSpan bounds one download of several vectors that lie
	// close together in the same file.
	maxVectorSpan = 64 << 20
	// maxDeletedRuns bounds the runs of deleted rows Igloo keeps for one
	// table, in memory: consecutive deleted rows make one run.
	maxDeletedRuns = 20_000_000
	// maxRunRows bounds the rows of one run in the deleted rows' file:
	// the view expands each run into a list of its row positions.
	maxRunRows = 1 << 20
	// vectorDownloads is how many downloads of vectors run at once.
	vectorDownloads = 8
	// vectorFileColumn is the Hive partition column that numbers the data
	// files with deletion vectors in an external table's view.
	vectorFileColumn = "__igloo_dv_file"
)

// deletionVector is Delta's deletion vector descriptor, as the delta
// response format lists it in a data file's add action.
type deletionVector struct {
	StorageType string `json:"storageType"`
	// PathOrInline is a pre-signed URL (storage type p) or the vector
	// itself in Z85 (storage type i).
	PathOrInline string `json:"pathOrInlineDv"`
	// Offset is where the vector starts in its file.
	Offset      *int64 `json:"offset"`
	SizeInBytes int64  `json:"sizeInBytes"`
	// Cardinality is how many rows the vector deletes.
	Cardinality int64 `json:"cardinality"`
	// fileID is the provider's ID of the file the vector is stored in
	// (deletionVectorFileId), when it gives one.
	fileID string
}

// check validates a descriptor as the provider sent it.
func (dv *deletionVector) check() error {
	switch dv.StorageType {
	case "i":
	case "p":
		u, err := url.Parse(dv.PathOrInline)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
			return errors.New("the provider listed a deletion vector without a URL")
		}
		if dv.offset() < 0 || dv.offset() > 1<<40 {
			return fmt.Errorf("the provider listed a deletion vector at offset %d", dv.offset())
		}
	case "u":
		return errors.New("the provider listed a deletion vector by its path in the table's storage rather than by a URL; " +
			"Igloo cannot read it")
	default:
		return fmt.Errorf("the provider listed a deletion vector of unknown storage type %q", dv.StorageType)
	}
	if dv.SizeInBytes <= 0 || dv.SizeInBytes > maxVectorBytes {
		return fmt.Errorf("the provider listed a deletion vector of %d bytes; Igloo reads vectors of up to %d", dv.SizeInBytes, maxVectorBytes)
	}
	if dv.Cardinality < 0 {
		return errors.New("the provider listed a deletion vector that deletes a negative number of rows")
	}
	return nil
}

func (dv *deletionVector) offset() int64 {
	if dv.Offset == nil {
		return 0
	}
	return *dv.Offset
}

// end is where a stored vector's framing ends: its size, the bitmap, and
// the bitmap's checksum.
func (dv *deletionVector) end() int64 { return dv.offset() + 8 + dv.SizeInBytes }

// source names the file a stored vector is in: the provider's ID for it,
// or its URL without the query, which holds the signature.
func (dv *deletionVector) source() string {
	if dv.fileID != "" {
		return "id:" + dv.fileID
	}
	u, err := url.Parse(dv.PathOrInline)
	if err != nil {
		return "url:" + dv.PathOrInline
	}
	u.RawQuery, u.Fragment = "", ""
	return "url:" + u.String()
}

// key identifies a vector's contents across listings, whose URLs change.
// Delta never rewrites a file of vectors.
func (dv *deletionVector) key() string {
	h := sha256.New()
	if dv.StorageType == "i" {
		h.Write([]byte("i\x00" + dv.PathOrInline))
	} else {
		h.Write([]byte("p\x00" + dv.source()))
	}
	fmt.Fprintf(h, "\x00%d\x00%d\x00%d", dv.offset(), dv.SizeInBytes, dv.Cardinality)
	return hex.EncodeToString(h.Sum(nil))
}

// rangeFetcher downloads a byte range of a file the provider listed. The
// consumer's goes through the proxy's HTTP client, which never connects to
// loopback, link-local or multicast addresses other than the provider's
// endpoint, and sends no bearer token.
type rangeFetcher func(ctx context.Context, fileURL, rangeHeader string) (*http.Response, error)

// loadVectors returns the serialized bitmap of each vector, taking those a
// previous listing read from cache and downloading the rest; vectors that
// lie close together in the same file are downloaded at once. It also
// returns the cache for the next listing: the vectors of this one.
func loadVectors(ctx context.Context, fetch rangeFetcher, dvs []*deletionVector, cache map[string][]byte) ([][]byte, map[string][]byte, error) {
	next := make(map[string][]byte, len(dvs))
	sources := map[string][]*deletionVector{}
	var order []string
	for _, dv := range dvs {
		k := dv.key()
		if _, ok := next[k]; ok {
			continue
		}
		if b, ok := cache[k]; ok {
			next[k] = b
			continue
		}
		if dv.StorageType == "i" {
			b, err := inlineVector(dv)
			if err != nil {
				return nil, nil, err
			}
			next[k] = b
			continue
		}
		next[k] = nil
		src := dv.source()
		if sources[src] == nil {
			order = append(order, src)
		}
		sources[src] = append(sources[src], dv)
	}

	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(vectorDownloads)
	for _, src := range order {
		group := sources[src]
		slices.SortFunc(group, func(a, b *deletionVector) int { return cmp.Compare(a.offset(), b.offset()) })
		for len(group) > 0 {
			// One download per span of vectors no longer than
			// maxVectorSpan.
			lo, hi, n := group[0].offset(), group[0].end(), 1
			for n < len(group) && max(hi, group[n].end())-lo <= maxVectorSpan {
				hi = max(hi, group[n].end())
				n++
			}
			span := group[:n]
			group = group[n:]
			g.Go(func() error {
				raw, err := fetchRange(gctx, fetch, span[0].PathOrInline, lo, hi)
				if err != nil {
					return err
				}
				for _, dv := range span {
					b, err := storedVector(raw[dv.offset()-lo:dv.end()-lo], dv.SizeInBytes)
					if err != nil {
						return err
					}
					mu.Lock()
					next[dv.key()] = b
					mu.Unlock()
				}
				return nil
			})
		}
	}
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}
	out := make([][]byte, len(dvs))
	for i, dv := range dvs {
		out[i] = next[dv.key()]
	}
	return out, next, nil
}

// fetchRange downloads bytes [lo, hi) of a file.
func fetchRange(ctx context.Context, fetch rangeFetcher, fileURL string, lo, hi int64) ([]byte, error) {
	resp, err := fetch(ctx, fileURL, fmt.Sprintf("bytes=%d-%d", lo, hi-1))
	if err != nil {
		return nil, fmt.Errorf("read a deletion vector: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	switch resp.StatusCode {
	case http.StatusPartialContent:
		if start, ok := rangeStart(resp.Header.Get("Content-Range")); !ok || start != lo {
			return nil, errors.New("read a deletion vector: the provider sent another part of the file than asked for")
		}
	case http.StatusOK:
		// The whole file, from its start.
		if lo > maxVectorSpan {
			return nil, errors.New("read a deletion vector: the provider sent the whole file of vectors rather than the part asked for")
		}
		if _, err := io.CopyN(io.Discard, resp.Body, lo); err != nil {
			return nil, fmt.Errorf("read a deletion vector: %w", err)
		}
	default:
		return nil, fmt.Errorf("the provider refused a deletion vector: %s", resp.Status)
	}
	buf := make([]byte, hi-lo)
	if _, err := io.ReadFull(resp.Body, buf); err != nil {
		return nil, fmt.Errorf("read a deletion vector: %w", err)
	}
	return buf, nil
}

// rangeStart returns the first byte of a Content-Range header's range.
func rangeStart(h string) (int64, bool) {
	rest, ok := strings.CutPrefix(h, "bytes ")
	if !ok {
		return 0, false
	}
	first, _, ok := strings.Cut(rest, "-")
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(first, 10, 64)
	return n, err == nil
}

// storedVector checks the framing of a vector in a file of vectors: its
// size and, after the bitmap, the bitmap's CRC-32, both big-endian.
func storedVector(b []byte, size int64) ([]byte, error) { return deletefiles.StoredVector(b, size) }

// inlineVector decodes a vector stored in its descriptor.
func inlineVector(dv *deletionVector) ([]byte, error) {
	b, err := z85Decode(dv.PathOrInline)
	if err != nil {
		return nil, fmt.Errorf("an inline deletion vector: %w", err)
	}
	// Delta pads the bytes to a multiple of four before encoding them.
	if int64(len(b)) < dv.SizeInBytes || int64(len(b))-dv.SizeInBytes >= 4 {
		return nil, errors.New("an inline deletion vector has another size than the provider listed")
	}
	return b[:dv.SizeInBytes], nil
}

const z85Alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"

// z85Decode decodes ZeroMQ's Base85 (Z85), which Delta uses for inline
// vectors: every five characters are four bytes, big-endian.
func z85Decode(s string) ([]byte, error) {
	if len(s)%5 != 0 {
		return nil, errors.New("Z85 text must be a multiple of five characters long")
	}
	out := make([]byte, 0, len(s)/5*4)
	for i := 0; i < len(s); i += 5 {
		var v uint64
		for j := range 5 {
			d := strings.IndexByte(z85Alphabet, s[i+j])
			if d < 0 {
				return nil, fmt.Errorf("%q is not a Z85 character", s[i+j])
			}
			v = v*85 + uint64(d)
		}
		if v > math.MaxUint32 {
			return nil, errors.New("Z85 text out of range")
		}
		out = binary.BigEndian.AppendUint32(out, uint32(v))
	}
	return out, nil
}

// Delta's RoaringBitmapArray, as Iceberg's deletion vectors are too (see
// package deletefiles).
const (
	portableBitmapMagic = deletefiles.PortableBitmapMagic
	nativeBitmapMagic   = deletefiles.NativeBitmapMagic
	roaringCookie       = deletefiles.RoaringCookie
	roaringCookieNoRun  = deletefiles.RoaringCookieNoRun
)

// vectorRuns decodes a serialized deletion vector and calls emit with each
// run of deleted rows, [start, stop), in ascending order. It returns how
// many rows the vector deletes.
func vectorRuns(b []byte, emit func(start, stop uint64) error) (uint64, error) {
	return deletefiles.VectorRuns(b, emit)
}

var errCorruptVector = deletefiles.ErrCorruptVector

// deletesFile writes the rows that vectors delete as Parquet, for an
// external table's view to leave out: one row per run of deleted rows,
// ("file", "start", "stop"), where file is the data file's number among
// those with vectors (its vectorFileColumn) and [start, stop) are row
// positions in it. Each vector must delete as many rows as the provider
// said it does.
func deletesFile(dvs []*deletionVector, vectors [][]byte) ([]byte, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "file", Type: arrow.PrimitiveTypes.Int32},
		{Name: "start", Type: arrow.PrimitiveTypes.Int64},
		{Name: "stop", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	var buf bytes.Buffer
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithDictionaryDefault(false),
		// Ascending positions with small gaps take a few bits each.
		parquet.WithEncoding(parquet.Encodings.DeltaBinaryPacked),
		parquet.WithMaxRowGroupLength(1<<20),
	)
	w, err := pqarrow.NewFileWriter(schema, &buf, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}
	rb := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer rb.Release()
	files, starts, stops := rb.Field(0).(*array.Int32Builder), rb.Field(1).(*array.Int64Builder), rb.Field(2).(*array.Int64Builder)
	flush := func() error {
		rec := rb.NewRecordBatch()
		defer rec.Release()
		return w.WriteBuffered(rec)
	}
	runs := 0
	for i, v := range vectors {
		n, err := vectorRuns(v, func(start, stop uint64) error {
			if stop > math.MaxInt64 {
				return errCorruptVector
			}
			for start < stop {
				end := min(stop, start+maxRunRows)
				if runs++; runs > maxDeletedRuns {
					return fmt.Errorf("the shared table's deletion vectors delete more than %d separate runs of rows, more than Igloo keeps; "+
						"ask the provider to purge them (REORG TABLE ... APPLY (PURGE))", maxDeletedRuns)
				}
				files.Append(int32(i))
				starts.Append(int64(start))
				stops.Append(int64(end))
				if files.Len() == 1<<20 {
					if err := flush(); err != nil {
						return err
					}
				}
				start = end
			}
			return nil
		})
		if err != nil {
			_ = w.Close()
			return nil, err
		}
		if n != uint64(dvs[i].Cardinality) {
			_ = w.Close()
			return nil, fmt.Errorf("a deletion vector deletes %d rows where the provider listed %d", n, dvs[i].Cardinality)
		}
	}
	if files.Len() > 0 || runs == 0 {
		if err := flush(); err != nil {
			_ = w.Close()
			return nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
