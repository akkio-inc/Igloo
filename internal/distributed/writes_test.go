package distributed

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestUnderDir(t *testing.T) {
	dir := "s3://bucket/t/data/igloo-write-ab/q/s1-0/a0"
	for p, want := range map[string]bool{
		dir + "/part_0.parquet":                       true,
		dir + "/__igloo_p0=123/part_0.parquet":        true,
		dir:                                           false,
		dir + "/":                                     false,
		dir + "x/part_0.parquet":                      false,
		dir + "/../a1/part_0.parquet":                 false,
		dir + "/x/../../a1/part_0.parquet":            false,
		dir + "/./part_0.parquet":                     false,
		dir + "//part_0.parquet":                      false,
		dir + "/%2e%2e/part_0.parquet":                false,
		dir + "/part_0.parquet?versionId=1":           false,
		dir + "/part_0.parquet#fragment":              false,
		dir + `/..\part_0.parquet`:                    false,
		dir + "/part_*.parquet":                       false,
		dir + "/part_[0-9].parquet":                   false,
		dir + "/part_{0,1}.parquet":                   false,
		dir + "/part_?.parquet":                       false,
		dir + "/part 0.parquet":                       false,
		"s3://bucket/other/part_0.parquet":            false,
		"s3://other/t/data/igloo-write-ab/q/s1-0/a0/": false,
		"/etc/passwd":                                 false,
	} {
		if got := UnderDir(p, dir); got != want {
			t.Errorf("UnderDir(%q) = %v, want %v", p, got, want)
		}
	}
}

func TestCheckWritten(t *testing.T) {
	dir := "/w/t/data/igloo-write-ab/q/s1-0/a0"
	good := []shuffleFile{{Path: dir + "/part_0.parquet", Rows: 3, Size: 10}, {Path: dir + "/part_1.parquet", Rows: 2, Size: 9}}
	if err := checkWritten(dir, good, 5); err != nil {
		t.Error(err)
	}
	if err := checkWritten(dir, nil, 0); err != nil {
		t.Error(err)
	}
	for name, c := range map[string]struct {
		files []shuffleFile
		rows  int64
	}{
		"rows":        {good, 6},
		"outside":     {[]shuffleFile{{Path: "/w/other/part_0.parquet", Rows: 5}}, 5},
		"not parquet": {[]shuffleFile{{Path: dir + "/part_0.txt", Rows: 5}}, 5},
		"negative":    {[]shuffleFile{{Path: dir + "/part_0.parquet", Rows: -5}, {Path: dir + "/part_1.parquet", Rows: 10}}, 5},
		"size":        {[]shuffleFile{{Path: dir + "/part_0.parquet", Rows: 5, Size: -1}}, 5},
		"twice":       {[]shuffleFile{{Path: dir + "/part_0.parquet", Rows: 2}, {Path: dir + "/part_0.parquet", Rows: 3}}, 5},
	} {
		if err := checkWritten(dir, c.files, c.rows); !WriteRefused(err) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// TestWriteDenied checks that only a refusal of the write itself counts: a
// worker that may not read a source fails as it would have.
func TestWriteDenied(t *testing.T) {
	for msg, want := range map[string]bool{
		"HTTP Error: HTTP PUT error on 's3://b/t/data/igloo-write-ab/q/s1-0/a0/part_0.parquet' (HTTP 403 Forbidden)": true,
		"IO Error: Cannot open file \"/w/t/data/igloo-write-ab/a0\": Permission denied":                              true,
		"HTTP GET error on 's3://b/t/data/source.parquet' (HTTP 403 Forbidden)":                                      false,
		"Out of memory writing igloo-write-ab":                                                                       false,
	} {
		if got := writeDenied(errors.New(msg)); got != want {
			t.Errorf("%q: %v, want %v", msg, got, want)
		}
	}
	if writeDenied(nil) {
		t.Error("no error")
	}
	if !writeDenied(ErrWriteDenied) {
		t.Error("a task's own denial")
	}
}

// TestCheckWriteRequest checks what a worker refuses to write, whoever asks.
func TestCheckWriteRequest(t *testing.T) {
	w := &Worker{opts: WorkerOptions{Locations: []string{"s3://bucket/warehouse/"}}}
	good := func() *writeRequest {
		return &writeRequest{Dir: "s3://bucket/warehouse/ns/t/data/igloo-write-ab12/q/s1-0/a0", Columns: []WriteColumn{{Name: "a", ID: 1}},
			Partitions: []string{"__igloo_p0"}, Compression: "zstd"}
	}
	if err := w.checkWriteRequest(good()); err != nil {
		t.Error(err)
	}
	for name, change := range map[string]func(*writeRequest){
		"another bucket":  func(r *writeRequest) { r.Dir = "s3://other/warehouse/ns/t/data/igloo-write-ab12/a0" },
		"a table's files": func(r *writeRequest) { r.Dir = "s3://bucket/warehouse/ns/t/data" },
		"climbs":          func(r *writeRequest) { r.Dir = "s3://bucket/warehouse/ns/t/data/igloo-write-ab12/../../a0" },
		"mark in a name":  func(r *writeRequest) { r.Dir = "s3://bucket/warehouse/ns/igloo-write-table/data" },
		"partition":       func(r *writeRequest) { r.Partitions = []string{"a); COPY x TO 'y"} },
		"compression":     func(r *writeRequest) { r.Compression = "zstd'); DROP" },
		"no columns":      func(r *writeRequest) { r.Columns = nil },
	} {
		r := good()
		change(r)
		if err := w.checkWriteRequest(r); err == nil {
			t.Errorf("%s: not refused", name)
		}
	}
}

// TestMarkWrites checks which plans' tasks hold the statement's rows, each
// its own: a copy's and a filter's, and a grouping completed on workers' with
// nothing above it; but not one the combine step orders, limits or groups.
func TestRowWiseFinal(t *testing.T) {
	s := &Stage{placeholder: "__igloo_partials_0"}
	pl := &planner{}
	base := func() node {
		return node{"type": "SELECT_NODE", "from_table": map[string]any{"type": "BASE_TABLE", "table_name": s.placeholder}}
	}
	if !pl.rowWise(base(), s) {
		t.Error("a plain read of the stage's rows")
	}
	for name, change := range map[string]func(node){
		"another table": func(n node) { n["from_table"] = map[string]any{"type": "BASE_TABLE", "table_name": "other"} },
		"order":         func(n node) { n["modifiers"] = []any{map[string]any{"type": "ORDER_MODIFIER"}} },
		"group":         func(n node) { n["group_expressions"] = []any{map[string]any{}} },
		"having":        func(n node) { n["having"] = map[string]any{"class": "CONSTANT"} },
		"window":        func(n node) { n["select_list"] = []any{map[string]any{"class": "WINDOW"}} },
		"subquery":      func(n node) { n["where_clause"] = map[string]any{"class": "SUBQUERY"} },
		"not a select":  func(n node) { n["type"] = "SET_OPERATION_NODE" },
	} {
		n := base()
		change(n)
		if pl.rowWise(n, s) {
			t.Errorf("%s: taken for row-wise", name)
		}
	}
	if !strings.Contains(WriteSource, "source") {
		t.Error("the name of a task's rows")
	}
}

// TestPartitionFiles checks how many partitions a sample of a fill's rows says it
// has, and which fills' files are worth writing on workers.
func TestPartitionFiles(t *testing.T) {
	for _, c := range []struct {
		name               string
		half, all, sampled int64
		rows, want         float64
	}{
		{"all of its rows", 3, 10, 1000, 1000, 10},
		{"fewer rows than the plan says", 3, 10, 1000, 500, 10},
		{"a few values, soon all seen", 128, 128, 1 << 20, 6e9, 128},
		{"rows in the order of the column", 2, 4, 1 << 20, 4 << 20, 16},
		{"a value a row", 1 << 19, 1 << 20, 1 << 20, 1 << 24, 1 << 24},
		{"nothing sampled", 0, 0, 0, 100, 0},
	} {
		if got := partitionsOf(c.half, c.all, c.sampled, c.rows); got != c.want {
			t.Errorf("%s: %v partitions, want %v", c.name, got, c.want)
		}
	}
	const mib, gib = 1 << 20, 1 << 30
	// As on real S3: 60 million rows of 34 bytes a row in files.
	lineitem := features{rows: 60e6, bytes: 2 * gib}
	if lineitem.replicaHoldsParts(128, 5500*mib, 4) {
		t.Error("128 partitions took a replica of 5.5 GiB out of memory")
	}
	if !lineitem.replicaHoldsParts(3, 5500*mib, 4) || !lineitem.replicaHoldsParts(128, 64*gib, 16) || !lineitem.replicaHoldsParts(128, 0, 4) {
		t.Error("a replica that has the room")
	}
	for _, c := range []struct {
		memory int64
		want   int
	}{
		{953 * mib, 59},
		{2355 * mib, 147},
		{64 * gib, 4096},
		{100 * mib, leastWriteFiles},
		{0, 100},
	} {
		if got := writeFilesFit(c.memory, true); got != c.want {
			t.Errorf("%d MiB: %d files fit, want %d", c.memory/mib, got, c.want)
		}
	}
	if got := writeFilesFit(953*mib, false); got != mostWriteFiles {
		t.Errorf("%d files fit on disk", got)
	}
	for _, c := range []struct {
		name  string
		bytes int64
		f     fleet
		want  float64
	}{
		{"a small table on few", 1 << 28, fleet{most: 16}, 2},
		{"one worker", 1 << 28, fleet{running: 1}, 1},
		{"a gigabyte a worker", 5 << 30, fleet{most: 16}, 5},
		{"more than there are", 100 << 30, fleet{most: 16, running: 2}, 16},
	} {
		if got := writeTasks(c.bytes, c.f); got != c.want {
			t.Errorf("%s: %v tasks, want %v", c.name, got, c.want)
		}
	}
}

// TestWriteBounds checks what a task's COPY may hold: little on a small worker,
// and Iceberg's own row groups on one with room.
func TestWriteBounds(t *testing.T) {
	const mib, gib = 1 << 20, 1 << 30
	for _, c := range []struct {
		memory          int64
		shares, writers int
		want            int64
	}{
		{953 * mib, 1, 2, 59 * mib},
		{953 * mib, 1, 45, 2*mib + 677*mib/1024},
		{16 * gib, 4, 16, 32 * mib},
		{512 * gib, 8, 8, 128 * mib},
		{256 * mib, 1, 256, 1 * mib},
		{0, 1, 2, 128 * mib},
	} {
		got := writeRowGroup(c.memory, c.shares, c.writers)
		if got < c.want*9/10 || got > c.want*11/10 {
			t.Errorf("writeRowGroup(%d MiB, %d, %d) = %d MiB, want about %d", c.memory/mib, c.shares, c.writers, got/mib, c.want/mib)
		}
	}
	if got := writeFilesOpen(953*mib, 1, true); got < 40 || got > 50 {
		t.Errorf("files open on a small worker, uploading: %d", got)
	}
	if got := writeFilesOpen(64*gib, 4, true); got != mostWriteFiles {
		t.Errorf("files open with room: %d", got)
	}
	if got := writeFilesOpen(100*mib, 1, true); got != leastWriteFiles {
		t.Errorf("files open with next to none: %d", got)
	}
	if got := writeFlush(953*mib, 1, 2); got < 7_000 || got > 8_000 {
		t.Errorf("rows gathered on a small worker: %d", got)
	}
	if got := writeFlush(64*mib, 1, 2); got != 4_096 {
		t.Errorf("rows gathered with next to no memory: %d", got)
	}
	if got := writeFlush(2048*gib, 1, 2); got != 524_288 {
		t.Errorf("rows gathered with room: %d", got)
	}
}

// TestWriteFits checks whether auto mode takes a worker's task to hold a fill's
// rows: by the width of the rows the fill reads, as its files show it.
func TestWriteFits(t *testing.T) {
	small := fleet{memory: 1 << 30, cores: 2, threads: 2}
	large := fleet{memory: 64 << 30, cores: 16, threads: 16}
	narrow := features{bytes: 5e10, rows: 1e9, fill: true}
	wide := features{bytes: 1e11, rows: 1e6, fill: true}
	for _, c := range []struct {
		name string
		x    features
		f    fleet
		want bool
	}{
		{"narrow rows on a small worker", narrow, small, true},
		{"rows of 100 KB of files on a small worker", wide, small, false},
		{"rows of 100 KB of files on a large worker", wide, large, false},
		{"rows of 1 KB of files on a small worker", features{bytes: 1e10, rows: 1e7, fill: true}, small, true},
		{"rows of 10 KB of files on a large worker", features{bytes: 1e11, rows: 1e7, fill: true}, large, true},
		{"rows not known", features{bytes: 1e11, fill: true}, small, true},
		{"memory not known", wide, fleet{cores: 2, threads: 2}, true},
	} {
		if got := c.x.writeFits(c.f); got != c.want {
			t.Errorf("%s: %v (rows of %.0f bytes)", c.name, got, c.x.rowBytes())
		}
	}
}

// TestWriteCredentials checks that a task's attempt carries what was made for
// its directory, and none where none can be.
func TestWriteCredentials(t *testing.T) {
	co := &Coordinator{log: slog.New(slog.DiscardHandler)}
	if got := co.writeCredentials(context.Background(), "s3://b/d/a0"); got != nil {
		t.Errorf("no way to make them: %+v", got)
	}
	co.opts.WriteCredentials = func(_ context.Context, dir string) (*WriteCredentials, error) {
		if dir != "s3://b/d/a0" {
			return nil, errors.New("none for " + dir)
		}
		return &WriteCredentials{KeyID: "for " + dir}, nil
	}
	if got := co.writeCredentials(context.Background(), "s3://b/d/a0"); got == nil || got.KeyID != "for s3://b/d/a0" {
		t.Errorf("for the attempt: %+v", got)
	}
	if got := co.writeCredentials(context.Background(), "s3://b/d/a1"); got != nil {
		t.Errorf("none to be had: %+v", got)
	}
}
