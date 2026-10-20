package sharing

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// encodeVector serializes ascending row positions as Delta does: a
// RoaringBitmapArray in the portable format. Containers are arrays or
// bitmaps by their size, and with runs, run containers where those are
// smaller, as Roaring's runOptimize makes them.
func encodeVector(positions []uint64, runs bool) []byte {
	type bucket struct {
		key  uint32
		lows []uint32
	}
	var buckets []bucket
	for _, p := range positions {
		if len(buckets) == 0 || buckets[len(buckets)-1].key != uint32(p>>32) {
			buckets = append(buckets, bucket{key: uint32(p >> 32)})
		}
		buckets[len(buckets)-1].lows = append(buckets[len(buckets)-1].lows, uint32(p))
	}
	out := binary.LittleEndian.AppendUint32(nil, portableBitmapMagic)
	out = binary.LittleEndian.AppendUint64(out, uint64(len(buckets)))
	for _, b := range buckets {
		out = binary.LittleEndian.AppendUint32(out, b.key)
		out = append(out, encodeRoaring(b.lows, runs)...)
	}
	return out
}

func encodeRoaring(values []uint32, runs bool) []byte {
	type container struct {
		key  uint16
		vals []uint16
	}
	var cs []container
	for _, v := range values {
		if len(cs) == 0 || cs[len(cs)-1].key != uint16(v>>16) {
			cs = append(cs, container{key: uint16(v >> 16)})
		}
		cs[len(cs)-1].vals = append(cs[len(cs)-1].vals, uint16(v))
	}
	runsOf := func(vals []uint16) [][2]int {
		var rs [][2]int
		for _, v := range vals {
			if len(rs) > 0 && rs[len(rs)-1][1] == int(v) {
				rs[len(rs)-1][1]++
			} else {
				rs = append(rs, [2]int{int(v), int(v) + 1})
			}
		}
		return rs
	}
	isRun := make([]bool, len(cs))
	for i, c := range cs {
		plain := 8192
		if len(c.vals) <= 4096 {
			plain = 2 * len(c.vals)
		}
		isRun[i] = runs && 2+4*len(runsOf(c.vals)) < plain
	}
	n := len(cs)
	var out []byte
	if runs {
		out = binary.LittleEndian.AppendUint32(out, roaringCookie|uint32(n-1)<<16)
		flags := make([]byte, (n+7)/8)
		for i := range cs {
			if isRun[i] {
				flags[i/8] |= 1 << (i % 8)
			}
		}
		out = append(out, flags...)
	} else {
		out = binary.LittleEndian.AppendUint32(out, roaringCookieNoRun)
		out = binary.LittleEndian.AppendUint32(out, uint32(n))
	}
	for _, c := range cs {
		out = binary.LittleEndian.AppendUint16(out, c.key)
		out = binary.LittleEndian.AppendUint16(out, uint16(len(c.vals)-1))
	}
	if !runs || n >= 4 {
		// Offsets; the decoder reads the containers in order instead.
		out = append(out, make([]byte, 4*n)...)
	}
	for i, c := range cs {
		switch {
		case isRun[i]:
			rs := runsOf(c.vals)
			out = binary.LittleEndian.AppendUint16(out, uint16(len(rs)))
			for _, r := range rs {
				out = binary.LittleEndian.AppendUint16(out, uint16(r[0]))
				out = binary.LittleEndian.AppendUint16(out, uint16(r[1]-r[0]-1))
			}
		case len(c.vals) <= 4096:
			for _, v := range c.vals {
				out = binary.LittleEndian.AppendUint16(out, v)
			}
		default:
			words := make([]uint64, 1024)
			for _, v := range c.vals {
				words[v/64] |= 1 << (v % 64)
			}
			for _, w := range words {
				out = binary.LittleEndian.AppendUint64(out, w)
			}
		}
	}
	return out
}

// vectorFile is a file of deletion vectors as Delta writes them: a version
// byte, then each vector framed by its size and CRC-32, big-endian. It
// returns the file and each vector's offset.
func vectorFile(vectors ...[]byte) ([]byte, []int64) {
	out := []byte{1}
	var offsets []int64
	for _, v := range vectors {
		offsets = append(offsets, int64(len(out)))
		out = binary.BigEndian.AppendUint32(out, uint32(len(v)))
		out = append(out, v...)
		out = binary.BigEndian.AppendUint32(out, crc32.ChecksumIEEE(v))
	}
	return out, offsets
}

// z85Encode encodes b, padded with zeros to a multiple of four bytes, as
// Delta does inline vectors.
func z85Encode(b []byte) string {
	for len(b)%4 != 0 {
		b = append(b, 0)
	}
	var s strings.Builder
	for i := 0; i < len(b); i += 4 {
		v := binary.BigEndian.Uint32(b[i:])
		var chunk [5]byte
		for j := 4; j >= 0; j-- {
			chunk[j] = z85Alphabet[v%85]
			v /= 85
		}
		s.Write(chunk[:])
	}
	return s.String()
}

// expand decodes a vector into its positions.
func expand(t *testing.T, b []byte) ([]uint64, [][2]uint64) {
	t.Helper()
	var positions []uint64
	var runs [][2]uint64
	n, err := vectorRuns(b, func(start, stop uint64) error {
		runs = append(runs, [2]uint64{start, stop})
		for p := start; p < stop; p++ {
			positions = append(positions, p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != uint64(len(positions)) {
		t.Fatalf("decoded %d rows but counted %d", len(positions), n)
	}
	return positions, runs
}

func TestVectorRuns(t *testing.T) {
	var positions []uint64
	positions = append(positions, 0, 3, 4, 5, 9)
	for p := uint64(65530); p < 65546; p++ { // across two containers
		positions = append(positions, p)
	}
	for p := uint64(100_000); p <= 105_000; p++ { // a bitmap container
		positions = append(positions, p)
	}
	for p := uint64(200_000); p < 230_000; p += 3 { // an array, then a bitmap
		positions = append(positions, p)
	}
	positions = append(positions, 1<<32+7, 1<<32+8, 3<<32+65535)
	for _, runs := range []bool{false, true} {
		got, rs := expand(t, encodeVector(positions, runs))
		if !slices.Equal(got, positions) {
			t.Fatalf("runs=%v: decoded %d positions, want %d", runs, len(got), len(positions))
		}
		// Consecutive rows make one run, across containers too.
		if !slices.Contains(rs, [2]uint64{65530, 65546}) || !slices.Contains(rs, [2]uint64{3, 6}) ||
			!slices.Contains(rs, [2]uint64{1<<32 + 7, 1<<32 + 9}) {
			t.Errorf("runs=%v: runs %v", runs, rs[:8])
		}
	}
	if got, _ := expand(t, encodeVector(nil, false)); len(got) != 0 {
		t.Errorf("an empty vector deletes %v", got)
	}

	// A vector Databricks wrote, for 14,285 deletions spread over a file
	// of 100,000 rows, starts like this: magic, one bitmap, key 0, cookie
	// without runs, two containers.
	start := []byte{0xd1, 0xd3, 0x39, 0x64, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x3a, 0x30, 0, 0, 2, 0, 0, 0}
	var sample []uint64
	for p := uint64(6); p < 100_000; p += 7 {
		sample = append(sample, p)
	}
	if enc := encodeVector(sample, false); !bytes.HasPrefix(enc, start) {
		t.Errorf("fixture encoder: % x", enc[:24])
	}
}

func TestVectorRunsRefusesCorruptVectors(t *testing.T) {
	good := encodeVector([]uint64{1, 2, 70000, 1<<32 + 5}, false)
	native := slices.Clone(good)
	binary.LittleEndian.PutUint32(native, nativeBitmapMagic)
	unsorted := encodeVector([]uint64{5, 7}, false)
	// The array container's values, swapped.
	n := len(unsorted)
	unsorted[n-4], unsorted[n-3], unsorted[n-2], unsorted[n-1] = unsorted[n-2], unsorted[n-1], unsorted[n-4], unsorted[n-3]
	keys := slices.Clone(good)
	// The second bitmap's key, made the first's.
	binary.LittleEndian.PutUint32(keys[len(keys)-len(encodeRoaring([]uint32{5}, false))-4:], 0)
	card := encodeVector([]uint64{1, 2, 3}, false)
	// The header's cardinality, made 4.
	binary.LittleEndian.PutUint16(card[4+8+4+8+2:], 3)

	for name, b := range map[string][]byte{
		"empty":         nil,
		"wrong magic":   append([]byte{1, 2, 3, 4}, good[4:]...),
		"native format": native,
		"truncated":     good[:len(good)-3],
		"unsorted":      unsorted,
		"repeated key":  keys,
		"cardinality":   card,
		"too many":      append(binary.LittleEndian.AppendUint64(binary.LittleEndian.AppendUint32(nil, portableBitmapMagic), 1<<40), 0, 0),
	} {
		if _, err := vectorRuns(b, func(uint64, uint64) error { return nil }); err == nil {
			t.Errorf("%s: decoded", name)
		}
	}
}

func TestZ85(t *testing.T) {
	// ZeroMQ's example.
	b, err := z85Decode("HelloWorld")
	if err != nil || !bytes.Equal(b, []byte{0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B}) {
		t.Errorf("z85Decode: % x, %v", b, err)
	}
	for _, bad := range []string{"Hell", "Hell~World", "%%%%%"} {
		if _, err := z85Decode(bad); err == nil {
			t.Errorf("%q decoded", bad)
		}
	}
	vec := encodeVector([]uint64{2, 3, 99}, false)
	dv := &deletionVector{StorageType: "i", PathOrInline: z85Encode(vec), SizeInBytes: int64(len(vec)), Cardinality: 3}
	got, err := inlineVector(dv)
	if err != nil || !bytes.Equal(got, vec) {
		t.Errorf("inlineVector: %v", err)
	}
	dv.SizeInBytes += 4
	if _, err := inlineVector(dv); err == nil {
		t.Error("an inline vector of the wrong size decoded")
	}
}

func TestLoadVectors(t *testing.T) {
	v1 := encodeVector([]uint64{0, 1, 2}, false)
	v2 := encodeVector([]uint64{10, 20}, true)
	v3 := encodeVector([]uint64{5}, false)
	data, offsets := vectorFile(v1, v2, v3)
	other, otherOffsets := vectorFile(v3)
	var requests, ranged atomic.Int32
	mode := "ranges"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Header.Get("Authorization") != "" {
			t.Error("a vector download carried a bearer token")
		}
		body := data
		if strings.HasPrefix(r.URL.Path, "/other") {
			body = other
		}
		switch mode {
		case "whole":
			_, _ = w.Write(body)
		case "refuse":
			http.Error(w, "expired", http.StatusForbidden)
		default:
			if r.Header.Get("Range") != "" {
				ranged.Add(1)
			}
			http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(body))
		}
	}))
	defer srv.Close()
	g := newGuardedDialer()
	g.allow(endpointHostPort(srv.URL))
	p := &Proxy{http: newHTTPClient(g)}

	off := func(i int64) *int64 { return &i }
	dvs := []*deletionVector{
		{StorageType: "p", PathOrInline: srv.URL + "/dv.bin?sig=a", Offset: off(offsets[1]), SizeInBytes: int64(len(v2)), Cardinality: 2, fileID: "f1"},
		{StorageType: "p", PathOrInline: srv.URL + "/dv.bin?sig=b", Offset: off(offsets[0]), SizeInBytes: int64(len(v1)), Cardinality: 3, fileID: "f1"},
		{StorageType: "i", PathOrInline: z85Encode(v3), SizeInBytes: int64(len(v3)), Cardinality: 1},
		{StorageType: "p", PathOrInline: srv.URL + "/other.bin?sig=c", Offset: off(otherOffsets[0]), SizeInBytes: int64(len(v3)), Cardinality: 1},
	}
	got, cache, err := loadVectors(context.Background(), p.fetch, dvs, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[0], v2) || !bytes.Equal(got[1], v1) || !bytes.Equal(got[2], v3) || !bytes.Equal(got[3], v3) {
		t.Error("loadVectors returned other vectors")
	}
	// The two vectors in one file came with one request.
	if requests.Load() != 2 || ranged.Load() != 2 {
		t.Errorf("%d requests, %d with a range", requests.Load(), ranged.Load())
	}

	// The next listing reads only what changed, though its URLs differ.
	dvs[0].PathOrInline, dvs[1].PathOrInline = srv.URL+"/dv.bin?sig=d", srv.URL+"/dv.bin?sig=e"
	dvs = append(dvs, &deletionVector{StorageType: "p", PathOrInline: srv.URL + "/dv.bin?sig=f", Offset: off(offsets[2]),
		SizeInBytes: int64(len(v3)), Cardinality: 1, fileID: "f1"})
	requests.Store(0)
	if got, _, err = loadVectors(context.Background(), p.fetch, dvs, cache); err != nil || !bytes.Equal(got[4], v3) {
		t.Fatal(err)
	}
	if requests.Load() != 1 {
		t.Errorf("%d requests with a cache", requests.Load())
	}

	// A server that ignores ranges.
	mode = "whole"
	if got, _, err = loadVectors(context.Background(), p.fetch, dvs, nil); err != nil || !bytes.Equal(got[1], v1) {
		t.Errorf("without ranges: %v", err)
	}

	mode = "refuse"
	if _, _, err := loadVectors(context.Background(), p.fetch, dvs[:1], nil); err == nil || !strings.Contains(err.Error(), "403") {
		t.Errorf("a refused download: %v", err)
	}

	mode = "ranges"
	data[offsets[0]+5] ^= 0xff // corrupt v1
	if _, _, err := loadVectors(context.Background(), p.fetch, dvs[1:2], nil); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Errorf("a corrupt vector: %v", err)
	}
	data[offsets[0]+5] ^= 0xff
	wrongSize := *dvs[1]
	wrongSize.SizeInBytes--
	if _, _, err := loadVectors(context.Background(), p.fetch, []*deletionVector{&wrongSize}, nil); err == nil {
		t.Error("a vector of the wrong size loaded")
	}
}

func TestDeletionVectorCheck(t *testing.T) {
	for _, dv := range []deletionVector{
		{StorageType: "u", PathOrInline: "ab^7j0abcdefghijklmn", SizeInBytes: 10, Cardinality: 1},
		{StorageType: "x", PathOrInline: "https://x.example.com/dv", SizeInBytes: 10, Cardinality: 1},
		{StorageType: "p", PathOrInline: "s3://bucket/dv.bin", SizeInBytes: 10, Cardinality: 1},
		{StorageType: "p", PathOrInline: "https://x.example.com/dv", SizeInBytes: 0, Cardinality: 1},
		{StorageType: "p", PathOrInline: "https://x.example.com/dv", SizeInBytes: maxVectorBytes + 1, Cardinality: 1},
		{StorageType: "p", PathOrInline: "https://x.example.com/dv", SizeInBytes: 10, Cardinality: -1},
	} {
		if err := dv.check(); err == nil {
			t.Errorf("%+v passed", dv)
		}
	}
	ok := deletionVector{StorageType: "p", PathOrInline: "https://x.example.com/dv?sig=1", SizeInBytes: 10, Cardinality: 1}
	if err := ok.check(); err != nil {
		t.Error(err)
	}
}

func TestDeletesFile(t *testing.T) {
	long := make([]uint64, 0, maxRunRows+10)
	for p := uint64(0); p < maxRunRows+10; p++ {
		long = append(long, p)
	}
	vectors := [][]byte{encodeVector([]uint64{3, 4, 5, 9}, false), encodeVector(long, true)}
	dvs := []*deletionVector{{Cardinality: 4}, {Cardinality: int64(len(long))}}
	data, err := deletesFile(dvs, vectors)
	if err != nil {
		t.Fatal(err)
	}
	rdr, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := fr.ReadTable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Release()
	var rows [][3]int64
	tr := array.NewTableReader(tbl, 1024)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		files, starts, stops := rec.Column(0).(*array.Int32), rec.Column(1).(*array.Int64), rec.Column(2).(*array.Int64)
		for i := range int(rec.NumRows()) {
			rows = append(rows, [3]int64{int64(files.Value(i)), starts.Value(i), stops.Value(i)})
		}
	}
	// Long runs are cut into pieces of at most maxRunRows.
	want := [][3]int64{{0, 3, 6}, {0, 9, 10}, {1, 0, maxRunRows}, {1, maxRunRows, maxRunRows + 10}}
	if !slices.Equal(rows, want) {
		t.Errorf("rows %v, want %v", rows, want)
	}

	// A vector that deletes other rows than the provider says is refused.
	dvs[0].Cardinality = 5
	if _, err := deletesFile(dvs, vectors); err == nil || !strings.Contains(err.Error(), "deletes 4 rows") {
		t.Errorf("a wrong cardinality: %v", err)
	}
}
