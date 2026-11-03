package pgwire

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
)

// COPY ... TO STDOUT sends a query's result in PostgreSQL's text, CSV or
// binary COPY format, which psql's \copy, psycopg's copy(), ADBC and
// connectorx read: the fastest way to move a large result into a notebook.
// Igloo runs it as the query it is, checked as a SELECT, so it needs only
// SELECT on what it reads, not the writer role COPY TO a file needs.

// copyTo is a parsed COPY ... TO STDOUT.
type copyTo struct {
	// query is the SELECT whose rows are copied.
	query     string
	format    string // text, csv or binary
	header    bool
	delimiter string
	null      string
}

// parseCopyTo recognizes COPY (query) TO STDOUT and COPY table [(columns)]
// TO STDOUT, with options in either PostgreSQL syntax. ok is false for any
// other statement, such as DuckDB's COPY to a file; err reports options
// Igloo does not support.
func parseCopyTo(query string) (c copyTo, ok bool, err error) {
	sc := &scanner{s: query}
	if !sc.keywords("COPY") {
		return c, false, nil
	}
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
		end, ok := closingParen(sc.s, sc.i)
		if !ok {
			return c, false, nil
		}
		c.query = strings.TrimSpace(sc.s[sc.i+1 : end])
		sc.i = end + 1
	} else {
		name, ok := sc.name()
		if !ok {
			return c, false, nil
		}
		columns := "*"
		sc.skip()
		if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
			end, ok := closingParen(sc.s, sc.i)
			if !ok {
				return c, false, nil
			}
			columns = sc.s[sc.i+1 : end]
			sc.i = end + 1
		}
		c.query = "SELECT " + columns + " FROM " + quoteName(name)
	}
	if !sc.keywords("TO", "STDOUT") {
		return c, false, nil
	}

	c.format = "text"
	var delimiter, null *string
	unsupported := func(what string) (copyTo, bool, error) {
		return c, true, pgerror.WithCode(fmt.Errorf("COPY TO STDOUT does not support %s", what), codes.FeatureNotSupported)
	}
	sc.keywords("WITH")
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
		// (FORMAT csv, HEADER true, DELIMITER ';', NULL '')
		sc.i++
		for {
			option := sc.word()
			sc.i += len(option)
			switch option {
			case "FORMAT":
				format := strings.ToLower(sc.word())
				sc.i += len(format)
				if format != "text" && format != "csv" && format != "binary" {
					return unsupported("FORMAT " + format)
				}
				c.format = format
			case "HEADER":
				switch v := sc.word(); v {
				case "TRUE", "ON", "1":
					sc.i += len(v)
					c.header = true
				case "FALSE", "OFF", "0":
					sc.i += len(v)
				default:
					c.header = true
				}
			case "DELIMITER", "NULL":
				v, ok := sc.literal()
				if !ok || v == nil {
					return unsupported(option + " without a string")
				}
				if option == "DELIMITER" {
					delimiter = v
				} else {
					null = v
				}
			case "ENCODING":
				if v, ok := sc.literal(); !ok || v == nil || !strings.EqualFold(strings.ReplaceAll(*v, "-", ""), "utf8") {
					return unsupported("encodings other than UTF8")
				}
			default:
				return unsupported("the option " + option)
			}
			sc.skip()
			if sc.i < len(sc.s) && sc.s[sc.i] == ',' {
				sc.i++
				continue
			}
			if sc.i < len(sc.s) && sc.s[sc.i] == ')' {
				sc.i++
				break
			}
			return c, false, nil
		}
	} else {
		// The syntax before PostgreSQL 9.0, which libraries still send:
		// [BINARY] [CSV [HEADER]] [DELIMITER [AS] 'x'] [NULL [AS] 'x'].
		for !sc.done() {
			switch word := sc.word(); word {
			case "BINARY", "CSV":
				sc.i += len(word)
				c.format = strings.ToLower(word)
			case "HEADER":
				sc.i += len(word)
				c.header = true
			case "DELIMITER", "NULL":
				sc.i += len(word)
				sc.keywords("AS")
				v, ok := sc.literal()
				if !ok || v == nil {
					return unsupported(word + " without a string")
				}
				if word == "DELIMITER" {
					delimiter = v
				} else {
					null = v
				}
			case "":
				return c, false, nil
			default:
				return unsupported("the option " + word)
			}
		}
	}
	if !sc.done() {
		return c, false, nil
	}

	c.delimiter, c.null = "\t", `\N`
	if c.format == "csv" {
		c.delimiter, c.null = ",", ""
	}
	if delimiter != nil {
		if len(*delimiter) != 1 {
			return unsupported("a delimiter that is not one character")
		}
		c.delimiter = *delimiter
	}
	if null != nil {
		c.null = *null
	}
	if c.format == "binary" && (c.header || delimiter != nil || null != nil) {
		return unsupported("HEADER, DELIMITER or NULL in binary format")
	}
	return c, true, nil
}

// copyToCommand handles COPY ... TO STDOUT.
func (s *Server) copyToCommand(ctx context.Context, sess *session, user auth.User, q wire.Query) (wire.PreparedStatements, bool, error) {
	c, ok, err := parseCopyTo(q.Query)
	if !ok {
		return nil, false, nil
	}
	if err != nil {
		s.recordQuery(sess, queryRecord{text: q.Query, start: time.Now(), err: err, rows: -1})
		return nil, true, err
	}
	st, err := s.prepareStatement(ctx, sess, user, c.query, nil)
	if err != nil {
		return nil, true, err
	}
	if st.kind != returnsRows || len(st.params) > 0 {
		_ = st.close()
		return nil, true, pgerror.WithCode(errors.New("COPY TO STDOUT needs a query that returns rows and takes no parameters"),
			codes.FeatureNotSupported)
	}

	var ps *wire.PreparedStatement
	ps = wire.NewStatement(s.audited(sess, q.Query, func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		if q.SimpleQuery {
			defer sess.release(ps) //nolint:errcheck
		}
		relist := s.shareCatalog(sess)
		err := s.recheck(ctx, sess, st)
		relist()
		if err != nil {
			return err
		}
		start := time.Now()
		var n int64
		releaseCatalog := s.catalogRunLock(sess, st)
		defer releaseCatalog()
		err = sess.runCancelable(ctx, q.Query, func(ctx context.Context) error {
			var err error
			n, err = c.write(ctx, st, w)
			return err
		})
		s.metrics.query(start, err)
		sess.observe(st, err)
		if err != nil {
			return pgError(err)
		}
		return w.Complete(fmt.Sprintf("COPY %d", n))
	}))
	sess.open[ps] = st
	return wire.Prepared(ps), true, nil
}

// write runs st and sends its rows to w in c's format. It returns how many
// rows it sent.
func (c copyTo) write(ctx context.Context, st *statement, w wire.DataWriter) (int64, error) {
	out, ok := w.(wire.CopyOuter)
	if tw, isTag := w.(*tagWriter); isTag {
		out, ok = tw.DataWriter.(wire.CopyOuter)
	}
	if !ok {
		return 0, pgerror.WithCode(errors.New("COPY TO STDOUT needs a SQL client such as psql"), codes.FeatureNotSupported)
	}
	rows, err := st.stmt.QueryContext(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer rows.Close() //nolint:errcheck

	format := wire.TextFormat
	if c.format == "binary" {
		format = wire.BinaryFormat
	}
	if err := out.CopyOut(format, len(st.columns)); err != nil {
		return 0, err
	}
	n, err := c.copyRows(ctx, st, rows, out)
	if err != nil {
		return n, errors.Join(err, out.CopyAbort())
	}
	return n, out.CopyDone()
}

func (c copyTo) copyRows(ctx context.Context, st *statement, rows driver.Rows, out wire.CopyOuter) (int64, error) {
	types := wire.TypeMap(ctx)
	if types == nil {
		types = newTypeMap()
	}
	// As from PostgreSQL, each row is a CopyData message, and a binary
	// copy's header comes with the first row (or the trailer): psycopg and
	// ADBC read it so.
	var buf []byte
	switch {
	case c.format == "binary":
		// Signature, flags and header extension length.
		buf = append([]byte("PGCOPY\n\xff\r\n\x00"), 0, 0, 0, 0, 0, 0, 0, 0)
	case c.header:
		for i, col := range st.columns {
			if i > 0 {
				buf = append(buf, c.delimiter...)
			}
			buf = c.appendText(buf, []byte(col.Name))
		}
		buf = append(buf, '\n')
		if err := out.CopyData(buf); err != nil {
			return 0, err
		}
		buf = buf[:0]
	}

	dest := make([]driver.Value, len(st.columns))
	var value []byte
	var n int64
	for {
		err := rows.Next(dest)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return n, err
		}
		if c.format == "binary" {
			buf = binary.BigEndian.AppendUint16(buf, uint16(len(dest)))
		}
		for i, v := range dest {
			if c.format != "binary" && i > 0 {
				buf = append(buf, c.delimiter...)
			}
			if v == nil {
				if c.format == "binary" {
					buf = binary.BigEndian.AppendUint32(buf, 0xffffffff)
				} else {
					buf = append(buf, c.null...)
				}
				continue
			}
			encoded, err := st.encoders[i](v)
			if err != nil {
				return n, fmt.Errorf("column %q: %w", st.columns[i].Name, err)
			}
			code := int16(pgtype.TextFormatCode)
			if c.format == "binary" {
				code = pgtype.BinaryFormatCode
			}
			if value, err = types.Encode(st.columns[i].Oid, code, encoded, value[:0]); err != nil {
				return n, fmt.Errorf("column %q: %w", st.columns[i].Name, err)
			}
			if c.format == "binary" {
				buf = binary.BigEndian.AppendUint32(buf, uint32(len(value)))
				buf = append(buf, value...)
			} else {
				buf = c.appendText(buf, value)
			}
		}
		if c.format != "binary" {
			buf = append(buf, '\n')
		}
		if err := out.CopyData(buf); err != nil {
			return n, err
		}
		buf = buf[:0]
		n++
	}
	if c.format == "binary" {
		return n, out.CopyData(append(buf, 0xff, 0xff))
	}
	return n, nil
}

// appendText appends a value in text or CSV COPY format.
func (c copyTo) appendText(buf, value []byte) []byte {
	if c.format == "csv" {
		quote := len(value) == 0 && c.null == "" || string(value) == c.null ||
			strings.ContainsAny(string(value), c.delimiter+"\"\r\n")
		if !quote {
			return append(buf, value...)
		}
		buf = append(buf, '"')
		for _, b := range value {
			if b == '"' {
				buf = append(buf, '"')
			}
			buf = append(buf, b)
		}
		return append(buf, '"')
	}
	for _, b := range value {
		switch b {
		case '\\':
			buf = append(buf, '\\', '\\')
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\r':
			buf = append(buf, '\\', 'r')
		case '\t':
			buf = append(buf, '\\', 't')
		default:
			if b == c.delimiter[0] {
				buf = append(buf, '\\')
			}
			buf = append(buf, b)
		}
	}
	return buf
}
