package pgwire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Geometries reach Igloo from DuckDB as WKB and go to clients as text, in
// the WKT that DuckDB itself prints, such as POINT (-73.98 40.75): psql
// and BI tools show it as is, and every GIS library parses it.

// toWKT renders a GEOMETRY value as WKT.
func toWKT(v any) (any, error) {
	b, ok := v.([]byte)
	if !ok {
		return toText(v)
	}
	r := &wkbReader{b: b}
	var out strings.Builder
	if err := r.geometry(&out, true); err != nil {
		return nil, fmt.Errorf("geometry: %w", err)
	}
	return out.String(), nil
}

// wkbReader reads ISO WKB, and the Z, M and SRID flags of extended WKB.
type wkbReader struct {
	b     []byte
	order binary.ByteOrder
}

var errShortWKB = errors.New("truncated WKB")

const (
	wkbPoint = iota + 1
	wkbLineString
	wkbPolygon
	wkbMultiPoint
	wkbMultiLineString
	wkbMultiPolygon
	wkbCollection
)

var wkbNames = [...]string{wkbPoint: "POINT", wkbLineString: "LINESTRING", wkbPolygon: "POLYGON",
	wkbMultiPoint: "MULTIPOINT", wkbMultiLineString: "MULTILINESTRING", wkbMultiPolygon: "MULTIPOLYGON",
	wkbCollection: "GEOMETRYCOLLECTION"}

// header reads a geometry's byte order and type, and returns its kind and
// how many coordinates each of its points has.
func (r *wkbReader) header() (kind uint32, dims int, tag string, err error) {
	if len(r.b) < 5 {
		return 0, 0, "", errShortWKB
	}
	r.order = binary.ByteOrder(binary.LittleEndian)
	if r.b[0] == 0 {
		r.order = binary.BigEndian
	}
	code := r.order.Uint32(r.b[1:5])
	r.b = r.b[5:]
	hasZ, hasM := code&0x80000000 != 0, code&0x40000000 != 0
	if code&0x20000000 != 0 { // an SRID, which WKT does not carry
		if len(r.b) < 4 {
			return 0, 0, "", errShortWKB
		}
		r.b = r.b[4:]
	}
	code &= 0x0fffffff
	switch code / 1000 {
	case 1:
		hasZ = true
	case 2:
		hasM = true
	case 3:
		hasZ, hasM = true, true
	}
	kind = code % 1000
	if kind == 0 || int(kind) >= len(wkbNames) {
		return 0, 0, "", fmt.Errorf("unknown WKB geometry type %d", code)
	}
	dims = 2
	switch {
	case hasZ && hasM:
		dims, tag = 4, " ZM"
	case hasZ:
		dims, tag = 3, " Z"
	case hasM:
		dims, tag = 3, " M"
	}
	return kind, dims, tag, nil
}

// geometry writes one geometry, with its type when tagged.
func (r *wkbReader) geometry(out *strings.Builder, tagged bool) error {
	kind, dims, tag, err := r.header()
	if err != nil {
		return err
	}
	if tagged {
		out.WriteString(wkbNames[kind] + tag + " ")
	}
	switch kind {
	case wkbPoint:
		return r.point(out, dims, true)
	case wkbLineString:
		return r.line(out, dims)
	}
	n, err := r.count()
	if err != nil || n == 0 {
		out.WriteString("EMPTY")
		return err
	}
	out.WriteByte('(')
	for i := range n {
		if i > 0 {
			out.WriteString(", ")
		}
		switch kind {
		case wkbPolygon:
			err = r.line(out, dims)
		case wkbMultiPoint:
			// MULTIPOINT (1 2, 3 4), as DuckDB writes it.
			if _, dims, _, err = r.header(); err == nil {
				err = r.point(out, dims, false)
			}
		case wkbCollection:
			err = r.geometry(out, true)
		default:
			err = r.geometry(out, false)
		}
		if err != nil {
			return err
		}
	}
	out.WriteByte(')')
	return nil
}

func (r *wkbReader) count() (int, error) {
	if len(r.b) < 4 {
		return 0, errShortWKB
	}
	n := r.order.Uint32(r.b[:4])
	r.b = r.b[4:]
	if int64(n) > int64(len(r.b)) {
		return 0, errShortWKB
	}
	return int(n), nil
}

// line writes a line string or a polygon's ring: a count, then the points.
func (r *wkbReader) line(out *strings.Builder, dims int) error {
	n, err := r.count()
	if err != nil {
		return err
	}
	if n == 0 {
		out.WriteString("EMPTY")
		return nil
	}
	out.WriteByte('(')
	for i := range n {
		if i > 0 {
			out.WriteString(", ")
		}
		if err := r.point(out, dims, false); err != nil {
			return err
		}
	}
	out.WriteByte(')')
	return nil
}

// point writes a point's coordinates, in parentheses when wrapped. An
// empty point has NaN coordinates.
func (r *wkbReader) point(out *strings.Builder, dims int, wrapped bool) error {
	if len(r.b) < dims*8 {
		return errShortWKB
	}
	coords := make([]string, dims)
	empty := true
	for d := range coords {
		f := math.Float64frombits(r.order.Uint64(r.b[:8]))
		r.b = r.b[8:]
		empty = empty && math.IsNaN(f)
		coords[d] = strconv.FormatFloat(f, 'f', -1, 64)
	}
	switch {
	case empty:
		out.WriteString("EMPTY")
	case wrapped:
		out.WriteString("(" + strings.Join(coords, " ") + ")")
	default:
		out.WriteString(strings.Join(coords, " "))
	}
	return nil
}
