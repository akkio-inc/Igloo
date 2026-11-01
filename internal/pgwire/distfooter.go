package pgwire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sync/atomic"
	"time"

	icebergio "github.com/apache/iceberg-go/io"
)

// Footers of files workers wrote. The coordinator parses them in its own
// process, which DuckDB and every session share, and a worker can write what it
// likes: a Parquet schema of 60 KB that says a group has two billion children
// has the parser allocate that for each of its levels, and one nested five
// million levels deep overflows the stack, which nothing can recover from. So
// before anything parses a footer, checkFooter walks it without allocating, and
// the parser reads those very bytes, whatever the store holds by then.
//
// What the rest of a footer can have the parser allocate is what it says of
// row groups, at most some sixteen times its bytes (a list of empty strings
// takes a byte each), so that with mostFooterBytes a file takes at most 256 MiB
// while it is read, a replica's cores of them at a time.

const (
	// mostFooterBytes is the longest footer of a written file. A column's chunk
	// takes some 150 bytes in one: this is a file of a hundred columns and a
	// thousand row groups.
	mostFooterBytes = 16 << 20
	// mostFootersBytes bounds a statement's footers together, since what they
	// say of each column's bounds stays until the commit. Past it, a statement
	// that is really that many files gets the error, not the replica's end.
	mostFootersBytes = 1 << 30
	// parquetTail is the footer's length and the magic that end a file.
	parquetTail = 8
	// footerNesting is how deep structs and lists go in a footer: a column's
	// statistics are six down from the top, and nothing of the format goes
	// deeper.
	footerNesting = 16
	// mostFixedLength is the longest fixed-length column a written file may say
	// it has.
	mostFixedLength = 1 << 20
	// arrowSchemaKey is the metadata key whose value the parser takes for an
	// Arrow schema, in a format that nests as deep as its bytes allow. DuckDB
	// writes no such thing.
	arrowSchemaKey = "ARROW:schema"
)

// Types of Thrift's compact protocol.
const (
	thriftStop   = 0
	thriftTrue   = 1
	thriftFalse  = 2
	thriftByte   = 3
	thriftI16    = 4
	thriftI32    = 5
	thriftI64    = 6
	thriftDouble = 7
	thriftBinary = 8
	thriftList   = 9
	thriftSet    = 10
	thriftMap    = 11
	thriftStruct = 12
)

// errFooterEnds is a footer cut short, or one that says it holds more than it has.
var errFooterEnds = errors.New("it says it holds more than it has")

// footerWalk reads Thrift's compact protocol as the parser does, and takes
// anything that it could read otherwise for an error: a count or a length beyond
// the bytes left, which the parser's 32 bits could make small again, and a number
// longer than a number can be.
type footerWalk struct {
	b []byte
	i int
}

func (w *footerWalk) left() int { return len(w.b) - w.i }

func (w *footerWalk) next() (byte, error) {
	if w.i >= len(w.b) {
		return 0, errFooterEnds
	}
	c := w.b[w.i]
	w.i++
	return c, nil
}

func (w *footerWalk) varint() (uint64, error) {
	var v uint64
	for n := range 10 {
		c, err := w.next()
		if err != nil {
			return 0, err
		}
		v |= uint64(c&0x7f) << (7 * n)
		if c&0x80 == 0 {
			return v, nil
		}
	}
	return 0, errors.New("a number is longer than a number can be")
}

// i32 is a zigzag number, of the 32 bits the parser keeps.
func (w *footerWalk) i32() (int32, error) {
	v, err := w.varint()
	if err != nil {
		return 0, err
	}
	u := uint32(v)
	return int32(u>>1) ^ -int32(u&1), nil
}

// count is a varint that counts what takes at least size bytes each.
func (w *footerWalk) count(size int) (int, error) {
	v, err := w.varint()
	if err != nil {
		return 0, err
	}
	if v > uint64(w.left()/size) {
		return 0, errFooterEnds
	}
	return int(v), nil
}

func (w *footerWalk) binary() ([]byte, error) {
	n, err := w.count(1)
	if err != nil {
		return nil, err
	}
	s := w.b[w.i : w.i+n]
	w.i += n
	return s, nil
}

// list reads a list's or a set's head.
func (w *footerWalk) list() (typ byte, n int, err error) {
	c, err := w.next()
	if err != nil {
		return 0, 0, err
	}
	typ, n = c&0x0f, int(c>>4)
	if n == 15 {
		n, err = w.count(1)
	} else if n > w.left() {
		err = errFooterEnds
	}
	return typ, n, err
}

// fields walks a struct: visit says it read a field's value, or that it wants
// it skipped.
func (w *footerWalk) fields(depth int, visit func(id int16, typ byte) (bool, error)) error {
	var id int16
	for {
		c, err := w.next()
		if err != nil {
			return err
		}
		typ := c & 0x0f
		if typ == thriftStop {
			if c != 0 {
				return errors.New("a field of no type")
			}
			return nil
		}
		if delta := c >> 4; delta != 0 {
			id += int16(delta)
		} else {
			long, err := w.i32()
			if err != nil {
				return err
			}
			id = int16(long)
		}
		read := false
		if visit != nil {
			if read, err = visit(id, typ); err != nil {
				return err
			}
		}
		if !read {
			if err := w.skip(typ, depth); err != nil {
				return err
			}
		}
	}
}

// skip walks a value of type typ, which is depth down from the top.
func (w *footerWalk) skip(typ byte, depth int) error {
	if depth > footerNesting {
		return fmt.Errorf("it nests deeper than %d", footerNesting)
	}
	switch typ {
	case thriftTrue, thriftFalse:
		return nil
	case thriftByte:
		_, err := w.next()
		return err
	case thriftI16, thriftI32, thriftI64:
		_, err := w.varint()
		return err
	case thriftDouble:
		if w.left() < 8 {
			return errFooterEnds
		}
		w.i += 8
		return nil
	case thriftBinary:
		_, err := w.binary()
		return err
	case thriftList, thriftSet:
		elem, n, err := w.list()
		if err != nil {
			return err
		}
		for range n {
			if elem == thriftTrue || elem == thriftFalse {
				elem = thriftByte // A byte each, in a list.
			}
			if err := w.skip(elem, depth+1); err != nil {
				return err
			}
		}
		return nil
	case thriftMap:
		n, err := w.count(2)
		if err != nil || n == 0 {
			return err
		}
		types, err := w.next()
		if err != nil {
			return err
		}
		for range n {
			if err := w.skip(types>>4, depth+1); err != nil {
				return err
			}
			if err := w.skip(types&0x0f, depth+1); err != nil {
				return err
			}
		}
		return nil
	case thriftStruct:
		return w.fields(depth+1, nil)
	}
	return fmt.Errorf("a value of unknown type %d", typ)
}

// schema walks the footer's list of schema elements, a tree written depth first:
// each element says how many of those after it are its children. It refuses what
// the parser would take a stack or a heap to follow: more columns than the file
// can have, deeper than depth, or a group of more children than there are
// elements left.
func (w *footerWalk) schema(columns, depth int) error {
	typ, n, err := w.list()
	if err != nil {
		return err
	}
	switch {
	case typ != thriftStruct:
		return errors.New("its schema is not a list of elements")
	case n == 0:
		return errors.New("it has no schema")
	case n-1 > columns:
		return fmt.Errorf("its schema has %d elements, for a table of %d columns", n-1, columns)
	}
	waiting := make([]int32, 0, depth)
	for i := range n {
		var children int32
		err := w.fields(1, func(id int16, typ byte) (bool, error) {
			if typ != thriftI32 {
				return false, nil
			}
			switch id {
			case 2: // type_length
				l, err := w.i32()
				if err == nil && (l < 0 || l > mostFixedLength) {
					err = fmt.Errorf("a column says its values take %d bytes", l)
				}
				return true, err
			case 5: // num_children
				var err error
				children, err = w.i32()
				return true, err
			}
			return false, nil
		})
		if err != nil {
			return err
		}
		if children < 0 || int(children) > n-1-i {
			return fmt.Errorf("a group of its schema says it has %d children, with %d elements left", children, n-1-i)
		}
		if i > 0 {
			if len(waiting) == 0 {
				return errors.New("its schema has elements of no group")
			}
			waiting[len(waiting)-1]--
		}
		if children > 0 {
			if len(waiting) >= depth {
				return fmt.Errorf("its schema nests deeper than %d", depth)
			}
			waiting = append(waiting, children)
		}
		for len(waiting) > 0 && waiting[len(waiting)-1] == 0 {
			waiting = waiting[:len(waiting)-1]
		}
	}
	if len(waiting) > 0 {
		return errors.New("a group of its schema has more children than there are elements")
	}
	return nil
}

// keyValues refuses the one key that has the parser read something other than
// the footer's own schema.
func (w *footerWalk) keyValues() error {
	typ, n, err := w.list()
	if err != nil {
		return err
	}
	if typ != thriftStruct {
		return errors.New("its metadata is not a list of keys and values")
	}
	for range n {
		err := w.fields(1, func(id int16, typ byte) (bool, error) {
			if id != 1 || typ != thriftBinary {
				return false, nil
			}
			key, err := w.binary()
			if err == nil && string(key) == arrowSchemaKey {
				err = fmt.Errorf("its metadata holds %s", arrowSchemaKey)
			}
			return true, err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// checkFooter refuses a Parquet footer that the parser could spend more than its
// own size on, or could not come back from. columns and depth are what the file
// may hold: the table's columns and how far they nest, one for a flat table.
func checkFooter(footer []byte, columns, depth int) error {
	w := &footerWalk{b: footer}
	hasSchema := false
	err := w.fields(0, func(id int16, typ byte) (bool, error) {
		switch {
		case id == 2 && typ == thriftList:
			hasSchema = true
			return true, w.schema(columns, depth)
		case id == 5 && typ == thriftList:
			return true, w.keyValues()
		case id == 8:
			return false, errors.New("it is encrypted")
		}
		return false, nil
	})
	if err == nil && !hasSchema {
		err = errors.New("it has no schema")
	}
	return err
}

// writtenFooters is the store as the parser of a statement's written files sees
// it: Open reads a file's footer, once, and checks it, and what it returns holds
// those bytes and nothing else of the file.
type writtenFooters struct {
	icebergio.IO
	columns, depth int
	read           atomic.Int64
}

// Open reads and checks the footer of the file at name.
func (o *writtenFooters) Open(name string) (icebergio.File, error) {
	f, err := o.IO.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	var tail [parquetTail]byte
	if size < parquetTail {
		return nil, fmt.Errorf("the file written at %s has no footer", name)
	}
	if _, err := f.ReadAt(tail[:], size-parquetTail); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	n := int64(binary.LittleEndian.Uint32(tail[:4]))
	switch {
	case string(tail[4:]) != "PAR1":
		return nil, fmt.Errorf("the file written at %s does not end as a plain Parquet file", name)
	case n > mostFooterBytes:
		return nil, fmt.Errorf("the file written at %s has a footer of %d bytes, more than %d", name, n, mostFooterBytes)
	case n+parquetTail > size:
		return nil, fmt.Errorf("the file written at %s says its footer is longer than it", name)
	case o.read.Add(n) > mostFootersBytes:
		return nil, fmt.Errorf("the files of this statement have footers of more than %d bytes", mostFootersBytes)
	}
	buf := make([]byte, n+parquetTail)
	if _, err := f.ReadAt(buf, size-int64(len(buf))); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if !bytes.Equal(buf[n:], tail[:]) {
		return nil, fmt.Errorf("the file written at %s changed as its footer was read", name)
	}
	if err := checkFooter(buf[:n], o.columns, o.depth); err != nil {
		return nil, fmt.Errorf("the footer of the file written at %s is refused: %w", name, err)
	}
	return &footerFile{name: name, size: size, tail: buf}, nil
}

// footerFile is a file of which only the tail, the footer that was checked, can
// be read.
type footerFile struct {
	name string
	size int64
	pos  int64
	tail []byte
}

func (f *footerFile) Stat() (fs.FileInfo, error) { return footerInfo{f}, nil }
func (f *footerFile) Close() error               { return nil }

func (f *footerFile) Read([]byte) (int, error) {
	return 0, fmt.Errorf("only the footer of %s can be read", f.name)
}

func (f *footerFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		offset += f.pos
	case io.SeekEnd:
		offset += f.size
	}
	if offset < 0 {
		return 0, fmt.Errorf("seek before the start of %s", f.name)
	}
	f.pos = offset
	return offset, nil
}

func (f *footerFile) ReadAt(p []byte, off int64) (int, error) {
	start := f.size - int64(len(f.tail))
	if off < start {
		return 0, fmt.Errorf("only the footer of %s can be read", f.name)
	}
	if off >= f.size {
		return 0, io.EOF
	}
	n := copy(p, f.tail[off-start:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// footerInfo says of a footerFile what its store said of the file.
type footerInfo struct{ f *footerFile }

func (i footerInfo) Name() string       { return i.f.name }
func (i footerInfo) Size() int64        { return i.f.size }
func (i footerInfo) Mode() fs.FileMode  { return 0o400 }
func (i footerInfo) ModTime() time.Time { return time.Time{} }
func (i footerInfo) IsDir() bool        { return false }
func (i footerInfo) Sys() any           { return nil }
