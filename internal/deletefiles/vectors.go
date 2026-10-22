package deletefiles

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/bits"
)

// Deletion vectors. Iceberg (format version 3, as a Puffin file's
// deletion-vector-v1 blobs) and Delta Lake (the deletionVectors feature)
// both delete rows of a data file by marking their positions in a bitmap
// beside it, and both store it alike: the bitmap's size and, after it, its
// CRC-32, both big-endian, around the bitmap, a RoaringBitmapArray in
// Roaring's portable format (https://github.com/RoaringBitmap/RoaringFormatSpec),
// little-endian: the magic number, the number of 32-bit bitmaps (8 bytes),
// then each bitmap's key, the high 32 bits of its positions (4 bytes), and
// the bitmap.

const (
	// PortableBitmapMagic starts a vector in Roaring's portable format.
	PortableBitmapMagic = 1681511377
	// NativeBitmapMagic starts one in Delta's native format.
	NativeBitmapMagic = 1681511376
	// Roaring's cookies: whether the bitmap has run containers.
	RoaringCookie      = 12347
	RoaringCookieNoRun = 12346
)

// ErrCorruptVector is a deletion vector that does not decode.
var ErrCorruptVector = errors.New("a deletion vector is corrupt")

// StoredVector checks the framing of a stored vector: its size and, after
// the bitmap, the bitmap's CRC-32, both big-endian. size is the bitmap's,
// without the framing's eight bytes.
func StoredVector(b []byte, size int64) ([]byte, error) {
	if int64(len(b)) != 8+size {
		return nil, errors.New("a deletion vector is cut short")
	}
	if int64(binary.BigEndian.Uint32(b)) != size {
		return nil, errors.New("a deletion vector's file says it has another size than its metadata")
	}
	data := b[4 : 4+size]
	if crc32.ChecksumIEEE(data) != binary.BigEndian.Uint32(b[4+size:]) {
		return nil, errors.New("a deletion vector's checksum does not match")
	}
	return data, nil
}

// VectorRuns decodes a serialized deletion vector and calls emit with each
// run of deleted rows, [start, stop), in ascending order, consecutive rows
// in one run. It returns how many rows the vector deletes.
func VectorRuns(b []byte, emit func(start, stop uint64) error) (uint64, error) {
	r := &bitmapReader{b: b}
	switch magic := r.u32(); {
	case r.err != nil:
		return 0, r.err
	case magic == NativeBitmapMagic:
		return 0, errors.New("the deletion vector is in Delta's native bitmap format, which Igloo does not read")
	case magic != PortableBitmapMagic:
		return 0, fmt.Errorf("not a deletion vector (magic number %d)", magic)
	}
	count := r.u64()
	// Each bitmap takes at least eight bytes: its key and cookie.
	if r.err != nil || count > uint64(len(r.b))/8 {
		return 0, ErrCorruptVector
	}
	m := &runMerger{emit: emit}
	var total uint64
	var lastKey uint32
	for i := range count {
		key := r.u32()
		if r.err != nil {
			return 0, r.err
		}
		if i > 0 && key <= lastKey {
			return 0, ErrCorruptVector
		}
		lastKey = key
		n, err := roaringRuns(r, uint64(key)<<32, m)
		if err != nil {
			return 0, err
		}
		total += n
	}
	if err := m.flush(); err != nil {
		return 0, err
	}
	return total, nil
}

// roaringRuns decodes one 32-bit Roaring bitmap, whose values are row
// positions plus high, into m.
func roaringRuns(r *bitmapReader, high uint64, m *runMerger) (uint64, error) {
	var n uint32
	var runFlags []byte
	switch cookie := r.u32(); {
	case r.err != nil:
		return 0, r.err
	case cookie&0xFFFF == RoaringCookie:
		n = cookie>>16 + 1
		runFlags = r.take(int(n+7) / 8)
	case cookie == RoaringCookieNoRun:
		n = r.u32()
	default:
		return 0, ErrCorruptVector
	}
	if r.err != nil || n > 1<<16 {
		return 0, ErrCorruptVector
	}
	header := r.take(int(n) * 4)
	if runFlags == nil || n >= 4 {
		// Offsets of the containers, which follow in order anyway.
		r.take(int(n) * 4)
	}
	if r.err != nil {
		return 0, r.err
	}
	var total uint64
	prevKey := -1
	for i := range int(n) {
		key := int(binary.LittleEndian.Uint16(header[4*i:]))
		card := uint64(binary.LittleEndian.Uint16(header[4*i+2:])) + 1
		if key <= prevKey {
			return 0, ErrCorruptVector
		}
		prevKey = key
		base := high | uint64(key)<<16
		var got uint64
		switch {
		case runFlags != nil && runFlags[i/8]&(1<<(i%8)) != 0:
			runs := int(r.u16())
			data := r.take(runs * 4)
			if r.err != nil {
				return 0, r.err
			}
			next := 0
			for j := range runs {
				start := int(binary.LittleEndian.Uint16(data[4*j:]))
				length := int(binary.LittleEndian.Uint16(data[4*j+2:])) + 1
				if start < next || start+length > 1<<16 {
					return 0, ErrCorruptVector
				}
				next = start + length
				m.add(base+uint64(start), base+uint64(next))
				got += uint64(length)
			}
		case card <= 4096:
			data := r.take(int(card) * 2)
			if r.err != nil {
				return 0, r.err
			}
			next := 0
			for j := range int(card) {
				v := int(binary.LittleEndian.Uint16(data[2*j:]))
				if v < next {
					return 0, ErrCorruptVector
				}
				next = v + 1
				m.add(base+uint64(v), base+uint64(next))
			}
			got = card
		default:
			data := r.take(8192)
			if r.err != nil {
				return 0, r.err
			}
			for w := range 1024 {
				word := binary.LittleEndian.Uint64(data[8*w:])
				for word != 0 {
					start := bits.TrailingZeros64(word)
					length := min(bits.TrailingZeros64(^(word >> start)), 64-start)
					lo := base + uint64(w*64+start)
					m.add(lo, lo+uint64(length))
					got += uint64(length)
					if start+length == 64 {
						break
					}
					word &^= (uint64(1)<<length - 1) << start
				}
			}
		}
		if got != card {
			return 0, ErrCorruptVector
		}
		total += got
	}
	return total, m.err
}

// bitmapReader reads little-endian values, failing past the end.
type bitmapReader struct {
	b   []byte
	err error
}

func (r *bitmapReader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || n > len(r.b) {
		r.err = ErrCorruptVector
		return nil
	}
	out := r.b[:n]
	r.b = r.b[n:]
	return out
}

func (r *bitmapReader) u16() uint16 {
	if b := r.take(2); b != nil {
		return binary.LittleEndian.Uint16(b)
	}
	return 0
}

func (r *bitmapReader) u32() uint32 {
	if b := r.take(4); b != nil {
		return binary.LittleEndian.Uint32(b)
	}
	return 0
}

func (r *bitmapReader) u64() uint64 {
	if b := r.take(8); b != nil {
		return binary.LittleEndian.Uint64(b)
	}
	return 0
}

// runMerger joins runs that touch and passes the result on.
type runMerger struct {
	start, stop uint64
	open        bool
	emit        func(start, stop uint64) error
	err         error
}

func (m *runMerger) add(start, stop uint64) {
	switch {
	case m.err != nil:
	case m.open && start < m.stop:
		m.err = ErrCorruptVector
	case m.open && start == m.stop:
		m.stop = stop
	default:
		if m.open {
			m.err = m.emit(m.start, m.stop)
		}
		m.start, m.stop, m.open = start, stop, true
	}
}

func (m *runMerger) flush() error {
	if m.err == nil && m.open {
		m.err = m.emit(m.start, m.stop)
		m.open = false
	}
	return m.err
}
