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
