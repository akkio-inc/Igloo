package pgwire

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/fs"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"

	"github.com/akkio-inc/igloo/internal/distributed"
)

// footerBuilder writes a Parquet footer in Thrift's compact protocol.
type footerBuilder struct{ bytes.Buffer }

func (b *footerBuilder) varint(v uint64) {
	for v >= 0x80 {
		b.WriteByte(byte(v) | 0x80)
		v >>= 7
	}
	b.WriteByte(byte(v))
}

func (b *footerBuilder) i32(v int32) { b.varint(uint64(uint32((v << 1) ^ (v >> 31)))) }

func (b *footerBuilder) listHead(n int, typ byte) {
	if n < 15 {
		b.WriteByte(byte(n<<4) | typ)
		return
	}
	b.WriteByte(0xF0 | typ)
	b.varint(uint64(n))
}

// element writes a schema element.
func (b *footerBuilder) element(name string, children int32, leaf bool) {
	last := byte(0)
	field := func(id, typ byte) {
		b.WriteByte((id-last)<<4 | typ)
		last = id
	}
	if leaf {
		field(1, thriftI32) // type: INT64
		b.i32(2)
	}
	field(4, thriftBinary)
	b.varint(uint64(len(name)))
	b.WriteString(name)
	if children != 0 {
		field(5, thriftI32)
		b.i32(children)
	}
	b.WriteByte(thriftStop)
}

// craftedFooter is the footer of levels nested groups, each saying it has
// children children, and a column; with extra written after them.
func craftedFooter(levels int, children int32, extra func(*footerBuilder)) []byte {
	var f footerBuilder
	f.WriteByte(0x10 | thriftI32) // 1: version
	f.i32(2)
	f.WriteByte(0x10 | thriftList) // 2: schema
	f.listHead(levels+1, thriftStruct)
	for range levels {
		f.element("a", children, false)
	}
	f.element("b", 0, true)
	f.WriteByte(0x10 | thriftI64) // 3: num_rows
	f.WriteByte(0)
	f.WriteByte(0x10 | thriftList) // 4: row_groups
	f.listHead(0, thriftStruct)
	if extra != nil {
		extra(&f)
	}
	f.WriteByte(thriftStop)
	return f.Bytes()
}

// craftedParquet is a Parquet file of a footer.
func craftedParquet(footer []byte) []byte {
	var out bytes.Buffer
	out.WriteString("PAR1")
	out.Write(footer)
	var size [4]byte
	binary.LittleEndian.PutUint32(size[:], uint32(len(footer)))
	out.Write(size[:])
	out.WriteString("PAR1")
	return out.Bytes()
}

// TestCheckFooter checks what a footer may not say, and that a plain one passes.
func TestCheckFooter(t *testing.T) {
	const columns = 4
	nestedList := func(depth int) func(*footerBuilder) {
		return func(f *footerBuilder) {
			f.WriteByte(0x30 | thriftList) // 7: column_orders
			for range depth {
				f.listHead(1, thriftList)
			}
			f.listHead(0, thriftByte)
		}
	}
	for _, c := range []struct {
		name   string
		footer []byte
		depth  int
		refuse string
	}{
		{"plain", craftedFooter(1, 1, nil), 1, ""},
		{"more children than elements", craftedFooter(1, 2, nil), 1, "children"},
		{"two billion children, 60 KB's worth", craftedFooter(5000, math.MaxInt32, nil), 1, "elements"},
		{"two billion children, one level", craftedFooter(1, math.MaxInt32, nil), 1, "children"},
		{"no children by a negative number", craftedFooter(1, -1, nil), 1, "children"},
		{"five million levels", craftedFooter(5_000_000, 1, nil), 1, "elements"},
		{"nested, in a flat table", craftedFooter(3, 1, nil), 1, "deeper"},
		{"nested within what the table may", craftedFooter(3, 1, nil), 3, ""},
		{"more columns than the table", craftedFooter(columns+1, 1, nil), columns + 1, "elements"},
		{"an Arrow schema in its metadata", craftedFooter(1, 1, func(f *footerBuilder) {
			f.WriteByte(0x10 | thriftList) // 5: key_value_metadata
			f.listHead(1, thriftStruct)
			f.WriteByte(0x10 | thriftBinary)
			f.varint(uint64(len(arrowSchemaKey)))
			f.WriteString(arrowSchemaKey)
			f.WriteByte(thriftStop)
		}), 1, arrowSchemaKey},
		{"a list longer than the footer", craftedFooter(1, 1, func(f *footerBuilder) {
			f.WriteByte(0x30 | thriftList) // 7: column_orders
			f.listHead(1<<31-1, thriftStruct)
		}), 1, "holds more"},
		{"a string longer than the footer", craftedFooter(1, 1, func(f *footerBuilder) {
			f.WriteByte(0x20 | thriftBinary) // 6: created_by
			f.varint(1 << 40)
		}), 1, "holds more"},
		{"a count that 32 bits make small", craftedFooter(1, 1, func(f *footerBuilder) {
			f.WriteByte(0x30 | thriftList) // 7: column_orders
			f.WriteByte(0xF0 | thriftByte)
			f.varint(1<<32 + 1)
			f.WriteByte(0)
		}), 1, "holds more"},
		{"nesting that goes on", craftedFooter(1, 1, nestedList(footerNesting+1)), 1, "deeper"},
		{"nesting there may be", craftedFooter(1, 1, nestedList(4)), 1, ""},
		{"encrypted", craftedFooter(1, 1, func(f *footerBuilder) {
			f.WriteByte(0x40 | thriftStruct) // 8: encryption_algorithm
			f.WriteByte(thriftStop)
		}), 1, "encrypted"},
		{"a number with no end", append([]byte{0x15}, bytes.Repeat([]byte{0xff}, 32)...), 1, "number"},
		{"cut short", craftedFooter(1, 1, nil)[:12], 1, "holds more"},
		{"no schema", []byte{thriftStop}, 1, "no schema"},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := checkFooter(c.footer, columns, c.depth)
			switch {
			case c.refuse == "" && err != nil:
				t.Errorf("refused: %v", err)
			case c.refuse != "" && (err == nil || !strings.Contains(err.Error(), c.refuse)):
				t.Errorf("got %v, want it refused for %q", err, c.refuse)
			}
		})
	}
}

// TestFooterFile checks that what the parser reads of a file is the footer that
// was checked, however the file changes after it, and nothing else of it.
func TestFooterFile(t *testing.T) {
	footer := craftedFooter(1, 1, nil)
	file := &footerFile{name: "f", size: 1000, tail: craftedParquet(footer)[4:]}
	start := file.size - int64(len(file.tail))
	if end, err := file.Seek(0, io.SeekEnd); err != nil || end != 1000 {
		t.Errorf("the end is at %d, %v", end, err)
	}
	got := make([]byte, len(footer))
	if n, err := file.ReadAt(got, start); err != nil || n != len(footer) || !bytes.Equal(got, footer) {
		t.Errorf("read %d bytes of the footer, %v", n, err)
	}
	if _, err := file.ReadAt(got, start-1); err == nil {
		t.Error("read what comes before the footer")
	}
	if _, err := file.Read(got); err == nil {
		t.Error("read from the start")
	}
	if info, err := file.Stat(); err != nil || info.Size() != 1000 {
		t.Errorf("stat: %v, %v", info, err)
	}
}

// slowFiles is a store whose files take a while to open, saying how many were
// being opened at a time.
type slowFiles struct {
	icebergio.IO
	data          []byte
	opens, at, hi atomic.Int64
	bad           string
}

func (s *slowFiles) Open(name string) (icebergio.File, error) {
	s.opens.Add(1)
	n := s.at.Add(1)
	defer s.at.Add(-1)
	for {
		hi := s.hi.Load()
		if n <= hi || s.hi.CompareAndSwap(hi, n) {
			break
		}
	}
	time.Sleep(5 * time.Millisecond)
	data := s.data
	if name == s.bad {
		data = []byte("not a Parquet file")
	}
	return &bytesFile{Reader: bytes.NewReader(data)}, nil
}

// bytesFile is a file of bytes.
type bytesFile struct{ *bytes.Reader }

func (f *bytesFile) Stat() (fs.FileInfo, error) { return footerInfo{&footerFile{size: f.Size()}}, nil }
func (f *bytesFile) Close() error               { return nil }

// TestFetchFooters checks that a statement reads its files' footers more at a time
// than a small replica's cores, once each, and that a bad one refuses them all.
func TestFetchFooters(t *testing.T) {
	store := &slowFiles{data: craftedParquet(craftedFooter(1, 1, nil))}
	paths := make([]string, 100)
	for i := range paths {
		paths[i] = "f" + strconv.Itoa(i)
	}
	footers := &writtenFooters{IO: store, columns: 2, depth: 2}
	if err := footers.fetch(t.Context(), paths); err != nil {
		t.Fatal(err)
	}
	if n := store.opens.Load(); n != 100 {
		t.Errorf("%d files opened, of 100", n)
	}
	if hi := store.hi.Load(); hi < 8 || hi > footerReads {
		t.Errorf("%d read at a time", hi)
	}
	for range 2 {
		f, err := footers.Open(paths[7])
		if err != nil {
			t.Fatal(err)
		}
		if end, err := f.Seek(0, io.SeekEnd); err != nil || end != int64(len(store.data)) {
			t.Errorf("the end is at %d, %v", end, err)
		}
	}
	if n := store.opens.Load(); n != 100 {
		t.Errorf("Open read a footer that was read: %d opens", n)
	}
	store.bad = paths[50]
	if err := (&writtenFooters{IO: store, columns: 2, depth: 2}).fetch(t.Context(), paths); err == nil || !strings.Contains(err.Error(), "footer") && !strings.Contains(err.Error(), "Parquet") {
		t.Errorf("a file that is not Parquet: %v", err)
	}
}

// TestWorkersWriteCraftedFooter checks that a worker's file with a footer made to
// kill the replica is not parsed: the statement's rows are written here instead,
// and that file is in no commit. The second would otherwise end the process, and
// the first take all its memory, so that this test would not end by failing.
func TestWorkersWriteCraftedFooter(t *testing.T) {
	t.Parallel()
	env := startWriteServer(t)
	env.force(t)
	inDir := func(f string) bool { return strings.Contains(f, "/"+distributed.SealedDirMark) }
	for i, crafted := range [][]byte{
		craftedParquet(craftedFooter(5000, math.MaxInt32, nil)),
		craftedParquet(craftedFooter(5_000_000, 1, nil)),
		craftedParquet(craftedFooter(2, 1, nil)),
	} {
		table := "w_footer_" + string(rune('a'+i))
		mustExec(t, env.admin, "CREATE TABLE "+table+" (id BIGINT, x BIGINT)")
		var attacked atomic.Bool
		attack := func(_, head map[string]any) faultAction {
			if attacked.CompareAndSwap(false, true) {
				files, _ := head["files"].([]any)
				for _, f := range files {
					p, _ := f.(map[string]any)["path"].(string)
					if err := os.WriteFile(p, crafted, 0o600); err != nil {
						t.Error(err)
					}
				}
			}
			return faultNone
		}
		for _, f := range env.faults {
			f.set(func(f *writeFault) { f.answer = attack })
		}
		mustExec(t, env.admin, "INSERT INTO "+table+" SELECT id, x FROM src")
		if !attacked.Load() {
			t.Errorf("%s: no task wrote", table)
		}
		if got, want := env.local(t, "SELECT count(*), sum(x) FROM "+table), env.local(t, "SELECT count(*), sum(x) FROM src"); got != want {
			t.Errorf("%s: %s, want %s", table, got, want)
		}
		if data, _ := liveFiles(t, env.admin, table); slices.ContainsFunc(data, inDir) {
			t.Errorf("%s: files of tasks committed: %q", table, data)
		}
	}
	env.checkNoNotes(t)
}

// TestParquetShape checks how far a table's types let a file's schema go, against
// a file DuckDB wrote of them (see TestWorkersWriteNested).
func TestParquetShape(t *testing.T) {
	prim := iceberg.PrimitiveTypes.Int64
	list := &iceberg.ListType{ElementID: 2, Element: prim}
	strct := &iceberg.StructType{FieldList: []iceberg.NestedField{{ID: 3, Name: "a", Type: prim}, {ID: 4, Name: "b", Type: list}}}
	mp := &iceberg.MapType{KeyID: 5, KeyType: iceberg.PrimitiveTypes.String, ValueID: 6, ValueType: strct}
	for _, c := range []struct {
		t               iceberg.Type
		elements, depth int
	}{{prim, 1, 0}, {list, 3, 2}, {strct, 5, 3}, {mp, 8, 5}} {
		if e, d := parquetShape(c.t); e != c.elements || d != c.depth {
			t.Errorf("%s: %d elements, %d deep; want %d, %d", c.t, e, d, c.elements, c.depth)
		}
	}
}

// TestWorkersWriteNested checks that workers write nested columns, with the field
// IDs of what they hold, and that the table reads as what the query made, through
// a column added since.
func TestWorkersWriteNested(t *testing.T) {
	t.Parallel()
	env := startWriteServer(t)
	env.force(t)
	query := `SELECT id, {'a': x, 'b': name, 'c': [x, x + 1]} AS s, [name, name || '!'] AS l, map([name], [x]) AS m,
		[{'k': x}] AS ls FROM src`
	mustExec(t, env.admin, "CREATE TABLE w_nested AS "+query)
	check := "SELECT id, s.a, s.b, s.c[2], l[2], m[name], ls[1].k FROM w_nested AS n, (SELECT id AS i, name FROM src) AS o WHERE n.id = o.i ORDER BY ALL"
	want := "SELECT id, s.a, s.b, s.c[2], l[2], m[name], ls[1].k FROM (" + query + ") AS n, (SELECT id AS i, name FROM src) AS o WHERE n.id = o.i ORDER BY ALL"
	if got, want := env.local(t, check), env.local(t, want); got != want {
		t.Errorf("rows differ:\n%.400s\nwant\n%.400s", got, want)
	}
	env.checkWrote(t, "w_nested")
	// The table's nested columns are read by field ID.
	mustExec(t, env.admin, "ALTER TABLE w_nested RENAME COLUMN s TO renamed")
	if got, want := env.local(t, "SELECT count(*), sum(renamed.a) FROM w_nested"), env.local(t, "SELECT count(*), sum(x) FROM src"); got != want {
		t.Errorf("after a rename: %s, want %s", got, want)
	}
	mustExec(t, env.admin, "INSERT INTO w_nested SELECT id, {'a': x, 'b': name, 'c': [x, x + 1]}, [name, name], map([name], [x]), [{'k': x}] FROM src")
	if got, want := env.local(t, "SELECT count(*), sum(renamed.a) FROM w_nested"), env.local(t, "SELECT 2 * count(*), 2 * sum(x) FROM src"); got != want {
		t.Errorf("after an INSERT: %s, want %s", got, want)
	}
	env.checkWrote(t, "w_nested")
}
