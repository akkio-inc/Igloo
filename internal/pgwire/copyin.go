package pgwire

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
)

// COPY ... FROM STDIN loads rows a client sends, as PostgreSQL's does, in
// its text, CSV and binary formats: psql's \copy, psycopg's copy, pgx's
// CopyFrom and pg_dump's data sections.
//
//	COPY table [(column, ...)] FROM STDIN [[WITH] (option [value], ...)] [WHERE condition]
//
// The rows go into a temporary table of this session, each value as the
// client wrote it, and from there into the table by an INSERT that is the
// user's own statement: prepared, checked against their role, grants and
// policies before the client sends anything, and run as any INSERT runs, so
// it writes the table as an INSERT does (ID domains' ids, commits, the
// history that lineage and erasure requests read). The text the client sent
// becomes each column's type by DuckDB's casts, after Igloo has put what
// PostgreSQL writes differently into a form they read: booleans, bytea,
// arrays, intervals. A value that does not convert fails the statement with
// its line and column, before anything is written.

// copyIn is a parsed COPY ... FROM STDIN.
type copyIn struct {
	table   []string
	columns []string
	format  string // text, csv or binary
	delim   byte
	null    string
	// header is 0 for none, 1 to skip the first line, 2 to check it names
	// the columns.
	header        int
	quote, escape byte
	// quoteSet says QUOTE or ESCAPE was given, which only CSV takes.
	quoteSet bool
	// forceNotNull and forceNull list the columns of those CSV options;
	// "*" stands for every column.
	forceNotNull, forceNull []string
	where                   string
}

// Header settings.
const (
	copyNoHeader = iota
	copySkipHeader
	copyMatchHeader
)

// copySyntax is a syntax error in COPY's options.
func copySyntax(format string, args ...any) error {
	return pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
}

// parseCopyFromStdin recognizes COPY ... FROM STDIN. ok is false for any
// other statement; err is set for one that is COPY ... FROM STDIN but whose
// options are wrong.
func parseCopyFromStdin(query string) (c copyIn, ok bool, err error) {
	sc := &scanner{s: query}
	if !sc.keywords("COPY") {
		return c, false, nil
	}
	if c.table, ok = sc.name(); !ok {
		return c, false, nil
	}
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
		sc.i++
		for {
			col, ok := sc.name()
			if !ok || len(col) != 1 {
				return c, false, nil
			}
			c.columns = append(c.columns, col[0])
			if sc.punct(',') {
				continue
			}
			if !sc.punct(')') {
				return c, false, nil
			}
			break
		}
	}
	if !sc.keywords("FROM", "STDIN") {
		return c, false, nil
	}
	c.format, c.delim, c.quote, c.escape = "text", 0, '"', 0
	nullSet := false
	sc.keywords("WITH")
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
		sc.i++
		for {
			key := strings.ToLower(sc.word())
			if key == "" {
				return c, true, copySyntax("syntax error in COPY's options at %q", strings.TrimSpace(sc.rest()))
			}
			sc.i += len(key)
			if key == "force_not_null" || key == "force_null" {
				cols, err := copyColumnList(sc)
				if err != nil {
					return c, true, err
				}
				if key == "force_not_null" {
					c.forceNotNull = cols
				} else {
					c.forceNull = cols
				}
			} else {
				value, given, err := copyOptionValue(sc)
				if err != nil {
					return c, true, err
				}
				if err := c.set(key, value, given); err != nil {
					return c, true, err
				}
				nullSet = nullSet || key == "null"
			}
			if sc.punct(',') {
				continue
			}
			if !sc.punct(')') {
				return c, true, copySyntax("syntax error in COPY's options at %q", strings.TrimSpace(sc.rest()))
			}
			break
		}
	} else {
		// The options as PostgreSQL wrote them before 9.0, which pgx and
		// older scripts still send: BINARY, DELIMITER [AS] 'x', NULL [AS]
		// 'x', CSV, HEADER, QUOTE [AS] 'x', ESCAPE [AS] 'x', FORCE NOT NULL
		// column, ...
		legacy := func(key string) error {
			sc.keywords("AS")
			v, err := copyString(sc)
			if err == nil {
				err = c.set(key, v, true)
			}
			nullSet = nullSet || key == "null"
			return err
		}
	options:
		for {
			var err error
			switch {
			case sc.keywords("BINARY"):
				c.format = "binary"
			case sc.keywords("CSV"):
				c.format = "csv"
			case sc.keywords("HEADER"):
				c.header = copySkipHeader
			case sc.keywords("DELIMITER"):
				err = legacy("delimiter")
			case sc.keywords("NULL"):
				err = legacy("null")
			case sc.keywords("QUOTE"):
				err = legacy("quote")
			case sc.keywords("ESCAPE"):
				err = legacy("escape")
			case sc.keywords("ENCODING"):
				err = legacy("encoding")
			case sc.keywords("FORCE", "NOT", "NULL"):
				for {
					col, ok := sc.name()
					if !ok || len(col) != 1 {
						return c, true, copySyntax("FORCE NOT NULL takes column names")
					}
					c.forceNotNull = append(c.forceNotNull, col[0])
					if !sc.punct(',') {
						break
					}
				}
			default:
				break options
			}
			if err != nil {
				return c, true, err
			}
		}
	}
	if sc.keywords("WHERE") {
		c.where = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sc.rest()), ";"))
		if c.where == "" {
			return c, true, copySyntax("WHERE needs a condition")
		}
	} else if !sc.done() {
		return c, true, copySyntax("syntax error in COPY at %q", strings.TrimSpace(sc.rest()))
	}
	return c, true, c.validate(nullSet)
}

// set sets option key to value; given says the option came with a value.
func (c *copyIn) set(key, value string, given bool) error {
	single := func() (byte, error) {
		if len(value) != 1 {
			return 0, pgerror.WithCode(fmt.Errorf("COPY %s must be a single one-byte character", key), codes.FeatureNotSupported)
		}
		return value[0], nil
	}
	var err error
	switch key {
	case "format":
		switch v := strings.ToLower(value); v {
		case "text", "csv", "binary":
			c.format = v
		default:
			return pgerror.WithCode(fmt.Errorf("COPY format %q not recognized", value), codes.InvalidParameterValue)
		}
	case "delimiter":
		c.delim, err = single()
	case "null":
		c.null = value
	case "quote":
		c.quote, err = single()
		c.quoteSet = true
	case "escape":
		c.escape, err = single()
		c.quoteSet = true
	case "header":
		switch v := strings.ToLower(value); {
		case !given || slices.Contains([]string{"true", "on", "1", "yes"}, v):
			c.header = copySkipHeader
		case slices.Contains([]string{"false", "off", "0", "no"}, v):
			c.header = copyNoHeader
		case v == "match":
			c.header = copyMatchHeader
		default:
			return pgerror.WithCode(fmt.Errorf("header requires a Boolean value or \"match\""), codes.InvalidParameterValue)
		}
	case "encoding":
		switch strings.ToLower(strings.NewReplacer("-", "", "_", "").Replace(value)) {
		case "utf8", "unicode", "sqlascii":
		default:
			return pgerror.WithCode(fmt.Errorf("COPY of encoding %s is not supported: send UTF8", value), codes.FeatureNotSupported)
		}
	case "freeze", "log_verbosity":
		// What these change in PostgreSQL does not apply.
	case "on_error":
		if strings.ToLower(value) != "stop" {
			return pgerror.WithCode(fmt.Errorf("COPY ON_ERROR %s is not supported; a value that does not convert fails the COPY", value),
				codes.FeatureNotSupported)
		}
	case "default":
		return pgerror.WithCode(errors.New("COPY DEFAULT is not supported; leave such columns out of the column list"), codes.FeatureNotSupported)
	default:
		return pgerror.WithCode(fmt.Errorf("option %q not recognized", key), codes.Syntax)
	}
	return err
}

// validate fills in the defaults and checks the options together, as
// PostgreSQL does.
func (c *copyIn) validate(nullSet bool) error {
	invalid := func(format string, args ...any) error {
		return pgerror.WithCode(fmt.Errorf(format, args...), codes.FeatureNotSupported)
	}
	csv := c.format == "csv"
	if c.format == "binary" {
		if c.delim != 0 || nullSet || c.header != copyNoHeader {
			return invalid("cannot specify DELIMITER, NULL or HEADER in BINARY mode")
		}
	}
	if !csv && (c.quoteSet || c.forceNull != nil || c.forceNotNull != nil) {
		return invalid("COPY QUOTE, ESCAPE, FORCE_NULL and FORCE_NOT_NULL are available only in CSV mode")
	}
	if c.delim == 0 {
		c.delim = '\t'
		if csv {
			c.delim = ','
		}
	}
	if !nullSet && !csv {
		c.null = `\N`
	}
	if c.escape == 0 {
		c.escape = c.quote
	}
	switch {
	case c.delim == '\r' || c.delim == '\n':
		return invalid("COPY delimiter cannot be newline or carriage return")
	case !csv && (c.delim == '\\' || strings.ContainsRune("abcdefghijklmnopqrstuvwxyz0123456789.", rune(c.delim))):
		return invalid("COPY delimiter cannot be %q", c.delim)
	case strings.ContainsAny(c.null, "\r\n"):
		return invalid("COPY null representation cannot use newline or carriage return")
	case csv && c.delim == c.quote:
		return invalid("COPY delimiter and quote must be different")
	case strings.IndexByte(c.null, c.delim) >= 0:
		return invalid("COPY delimiter character must not appear in the NULL specification")
	}
	return nil
}

// copyOptionValue reads the value of an option in parentheses: a string, a
// word or a number, or none, as a flag alone means true.
func copyOptionValue(sc *scanner) (value string, given bool, err error) {
	sc.skip()
	if sc.i >= len(sc.s) || sc.s[sc.i] == ',' || sc.s[sc.i] == ')' {
		return "", false, nil
	}
	if sc.s[sc.i] == '\'' || (sc.s[sc.i] == 'E' || sc.s[sc.i] == 'e') && sc.i+1 < len(sc.s) && sc.s[sc.i+1] == '\'' {
		v, err := copyString(sc)
		return v, true, err
	}
	j := sc.i
	for j < len(sc.s) && (sqlIdentOrDigit(sc.s[j]) || sc.s[j] == '-' || sc.s[j] == '.') {
		j++
	}
	if j == sc.i {
		return "", false, copySyntax("syntax error in COPY's options at %q", strings.TrimSpace(sc.rest()))
	}
	v := sc.s[sc.i:j]
	sc.i = j
	return v, true, nil
}

func sqlIdentOrDigit(b byte) bool {
	return b == '_' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9'
}

// copyString reads a string literal: 'standard', a quote in it doubled, or
// E'escaped', with backslash escapes, as psql users write E'\t'.
func copyString(sc *scanner) (string, error) {
	sc.skip()
	if sc.i < len(sc.s) && (sc.s[sc.i] == 'E' || sc.s[sc.i] == 'e') && sc.i+1 < len(sc.s) && sc.s[sc.i+1] == '\'' {
		var b []byte
		for j := sc.i + 2; j < len(sc.s); j++ {
			switch c := sc.s[j]; {
			case c == '\'' && j+1 < len(sc.s) && sc.s[j+1] == '\'':
				b = append(b, '\'')
				j++
			case c == '\'':
				sc.i = j + 1
				return string(b), nil
			case c == '\\' && j+1 < len(sc.s):
				v, n := unescapeText([]byte(sc.s[j:]))
				b = append(b, v...)
				j += n - 1
			default:
				b = append(b, c)
			}
		}
		return "", copySyntax("unterminated string in COPY's options")
	}
	v, ok := sc.literal()
	if !ok || v == nil {
		return "", copySyntax("COPY's options take strings in single quotes")
	}
	return *v, nil
}

// copyColumnList reads FORCE_NOT_NULL's and FORCE_NULL's (columns) or *.
func copyColumnList(sc *scanner) ([]string, error) {
	if sc.punct('*') {
		return []string{"*"}, nil
	}
	if !sc.punct('(') {
		return nil, copySyntax("FORCE_NOT_NULL and FORCE_NULL take (columns) or *")
	}
	var cols []string
	for {
		col, ok := sc.name()
		if !ok || len(col) != 1 {
			return nil, copySyntax("FORCE_NOT_NULL and FORCE_NULL take column names")
		}
		cols = append(cols, col[0])
		if sc.punct(',') {
			continue
		}
		if !sc.punct(')') {
			return nil, copySyntax("FORCE_NOT_NULL and FORCE_NULL take (columns) or *")
		}
		return cols, nil
	}
}

// copyKind is how a column's values are put into a form DuckDB's casts read.
type copyKind int

const (
	copyOther copyKind = iota
	copyVarchar
	copyBool
	copyBlob
	copyList
	copyNested
	copyInterval
	copyDate
)

func copyKindOf(typ string) copyKind {
	t := strings.ToUpper(typ)
	switch {
	case t == "VARCHAR":
		return copyVarchar
	case t == "BOOLEAN":
		return copyBool
	case t == "BLOB":
		return copyBlob
	case strings.HasSuffix(t, "]"):
		return copyList
	case strings.HasPrefix(t, "STRUCT(") || strings.HasPrefix(t, "MAP(") || strings.HasPrefix(t, "UNION("):
		return copyNested
	case t == "INTERVAL":
		return copyInterval
	case t == "DATE" || strings.HasPrefix(t, "TIMESTAMP"):
		return copyDate
	}
	return copyOther
}

// copyColumn is a column COPY fills.
type copyColumn struct {
	name, typ string
	kind      copyKind
	// oid is the type the client encodes binary values as: the column's
	// type as Igloo describes it (see wireType).
	oid uint32
	// forceNotNull and forceNull are the CSV options' for this column.
	forceNotNull, forceNull bool
}

// staged is the column of the temporary table that holds column i.
func stagedColumn(i int) string { return quoteIdent("c" + strconv.Itoa(i)) }

// value is the column's value in the temporary table: the cast of the text
// the client sent to its type.
func (col copyColumn) value(i int) string {
	v := stagedColumn(i)
	switch col.kind {
	case copyVarchar, copyBlob:
		return v
	case copyList, copyNested:
		// JSON, as PostgreSQL's arrays become and as Igloo writes nested
		// values, or DuckDB's own literals.
		return "CASE WHEN json_valid(" + v + ") THEN CAST(CAST(" + v + " AS JSON) AS " + col.typ + ") ELSE CAST(" + v + " AS " + col.typ + ") END"
	}
	return "CAST(" + v + " AS " + col.typ + ")"
}

// columns resolves the columns c fills, from the table's (described).
func (c copyIn) targetColumns(described []columnType) ([]copyColumn, error) {
	var out []copyColumn
	add := func(d columnType) {
		out = append(out, copyColumn{name: d.name, typ: d.typ, kind: copyKindOf(d.typ)})
	}
	if len(c.columns) == 0 {
		for _, d := range described {
			add(d)
		}
	}
	for _, name := range c.columns {
		i := slices.IndexFunc(described, func(d columnType) bool { return d.name == name })
		if i < 0 {
			i = slices.IndexFunc(described, func(d columnType) bool { return strings.EqualFold(d.name, name) })
		}
		if i < 0 {
			return nil, pgerror.WithCode(fmt.Errorf("column %q of relation %q does not exist", name, strings.Join(c.table, ".")), codes.UndefinedColumn)
		}
		if slices.ContainsFunc(out, func(o copyColumn) bool { return o.name == described[i].name }) {
			return nil, pgerror.WithCode(fmt.Errorf("column %q specified more than once", name), codes.DuplicateColumn)
		}
		add(described[i])
	}
	mark := func(names []string, set func(*copyColumn)) error {
		for _, n := range names {
			found := false
			for i := range out {
				if n == "*" || strings.EqualFold(out[i].name, n) {
					set(&out[i])
					found = true
				}
			}
			if !found {
				return pgerror.WithCode(fmt.Errorf("FORCE_NOT_NULL or FORCE_NULL column %q is not referenced by COPY", n), codes.InvalidParameterValue)
			}
		}
		return nil
	}
	if err := mark(c.forceNotNull, func(col *copyColumn) { col.forceNotNull = true }); err != nil {
		return nil, err
	}
	if err := mark(c.forceNull, func(col *copyColumn) { col.forceNull = true }); err != nil {
		return nil, err
	}
	return out, nil
}

// copyFromStdinStatement carries out c, the COPY ... FROM STDIN query.
func (s *Server) copyFromStdinStatement(sess *session, query string, c copyIn) wire.PreparedStatements {
	return wire.Prepared(wire.NewStatement(func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		start := time.Now()
		rows, held, ran, err := s.copyFromStdin(ctx, sess, query, c, w)
		if !ran {
			// Nothing was run as a statement of its own: the history and
			// the metrics have not heard of it.
			s.metrics.query(start, err)
			s.recordQuery(sess, queryRecord{text: query, start: start, err: err, rows: -1})
			return pgError(err)
		}
		for _, r := range held {
			// The INSERT that wrote the rows, under the statement the
			// client sent.
			r.text, r.start = query, start
			s.recordQuery(sess, r)
		}
		if err != nil {
			return err
		}
		return w.Complete(fmt.Sprintf("COPY %d", rows))
	}))
}

// copyFromStdin receives the rows of c from the client and inserts them.
// ran says the INSERT ran, whose history records it returns in held.
func (s *Server) copyFromStdin(ctx context.Context, sess *session, query string, c copyIn, w wire.DataWriter) (rows int64, held []queryRecord, ran bool, err error) {
	ci, ok := w.(wire.CopyIner)
	if !ok {
		return 0, nil, false, pgerror.WithCode(errors.New("COPY FROM STDIN needs a SQL client such as psql"), codes.FeatureNotSupported)
	}
	if err := sess.checkAborted(query); err != nil {
		return 0, nil, false, err
	}
	target := quoteName(c.table)
	// A table that is not there has DuckDB list the schema's tables, for its
	// error, as a listing does: holding the catalog (see holdCatalog).
	release := s.holdCatalog(sess)
	described, err := describeTable(ctx, sess, target)
	var cols []copyColumn
	if err == nil {
		cols, err = c.targetColumns(described)
	}
	if err == nil && c.format == "binary" {
		err = s.copyOIDs(ctx, sess, target, cols)
	}
	release()
	if err != nil {
		return 0, nil, false, pgError(err)
	}

	var b [8]byte
	_, _ = rand.Read(b[:])
	staging := "__igloo_copy_" + hex.EncodeToString(b[:])
	defs := []string{`"__line" BIGINT`}
	exprs := make([]string, len(cols))
	names := make([]string, len(cols))
	for i, col := range cols {
		t := "VARCHAR"
		if col.kind == copyBlob {
			t = "BLOB"
		}
		defs = append(defs, stagedColumn(i)+" "+t)
		exprs[i] = col.value(i) + " AS " + quoteIdent(col.name)
		names[i] = quoteIdent(col.name)
	}
	stagingName := quoteName([]string{"temp", "main", staging})
	if err := s.execHeldTemp(ctx, sess, "CREATE TEMP TABLE "+quoteIdent(staging)+" ("+strings.Join(defs, ", ")+")"); err != nil {
		return 0, nil, false, pgError(err)
	}
	defer func() {
		// Rolled back with its transaction if this one failed in a block.
		_ = s.execHeldTemp(context.WithoutCancel(ctx), sess, "DROP TABLE IF EXISTS "+stagingName)
	}()
	from := "SELECT " + strings.Join(exprs, ", ") + " FROM " + stagingName
	insert := "INSERT INTO " + target + " (" + strings.Join(names, ", ") + ") "
	if c.where != "" {
		insert += "SELECT * FROM (" + from + ") AS copy WHERE " + c.where
	} else {
		insert += from
	}

	// Checked as the user's own INSERT before the client sends anything.
	user := sess.currentUser()
	st, err := s.prepareChecked(ctx, sess, user, insert, nil)
	if err != nil {
		return 0, nil, false, err
	}
	_ = st.close()

	format := wire.TextFormat
	if c.format == "binary" {
		format = wire.BinaryFormat
	}
	reader, err := ci.CopyInColumns(format, len(cols))
	if err != nil {
		return 0, nil, false, err
	}
	stream := &copyStream{r: reader}
	if err := s.stageCopyRows(ctx, sess, c, cols, staging, stream); err != nil {
		stream.drain()
		return 0, nil, false, err
	}
	if err := s.checkCopyValues(ctx, sess, c, cols, stagingName); err != nil {
		return 0, nil, false, err
	}

	outer := sess.held
	collect := sess.holdRecords()
	tw := &innerWriter{DataWriter: w}
	err = s.runScriptStatement(WithIglooSQL(ctx), insert, tw)
	held = collect()
	sess.held = outer
	return tw.rows, held, true, err
}

// execHeldTemp runs DDL of the session's temporary objects, holding the
// catalog lock exclusively as such DDL does (see holdCatalog).
func (s *Server) execHeldTemp(ctx context.Context, sess *session, q string) error {
	release := s.holdCatalog(sess)
	defer release()
	return sess.exec(ctx, q)
}

// copyOIDs sets the types the client encodes cols' binary values as: those
// Igloo describes them as, which pgx learns by preparing a query of them.
func (s *Server) copyOIDs(ctx context.Context, sess *session, target string, cols []copyColumn) error {
	names := make([]string, len(cols))
	for i, col := range cols {
		names[i] = quoteIdent(col.name)
	}
	stmt, err := sess.conn.PrepareContext(ctx, "SELECT "+strings.Join(names, ", ")+" FROM "+target)
	if err != nil {
		return err
	}
	defer stmt.Close() //nolint:errcheck
	ds, ok := stmt.(*duckdb.Stmt)
	if !ok {
		return errors.New("cannot describe the table's columns")
	}
	for i := range cols {
		info, err := ds.ColumnTypeInfo(i)
		if err != nil {
			return err
		}
		cols[i].oid, _ = wireType(info.InternalType(), info.Alias())
	}
	return nil
}

// innerWriter is the writer of a statement Igloo runs within another, the
// INSERT of COPY or the drops of DROP SCHEMA ... CASCADE: it keeps the
// statement's command tag, and the rows it counts, for the outer one's own,
// and passes on everything else.
type innerWriter struct {
	wire.DataWriter
	rows int64
}

func (w *innerWriter) Complete(tag string) error {
	if f := strings.Fields(tag); len(f) > 0 {
		w.rows, _ = strconv.ParseInt(f[len(f)-1], 10, 64)
	}
	return nil
}

func (w *innerWriter) Notice(code, msg string) error {
	if n, ok := w.DataWriter.(wire.Noticer); ok {
		return n.Notice(code, msg)
	}
	return nil
}

// copyStream reads the data the client sends, CopyData message by message,
// until CopyDone.
type copyStream struct {
	r    *wire.CopyReader
	buf  []byte
	done bool
}

func (c *copyStream) Read(p []byte) (int, error) {
	for len(c.buf) == 0 {
		if c.done {
			return 0, io.EOF
		}
		t, _, err := c.r.ReadTypedMsg()
		if err != nil {
			c.done = true
			return 0, err
		}
		switch t {
		case types.ClientCopyData:
			c.buf = c.r.Msg
		case types.ClientCopyDone:
			c.done = true
		case types.ClientCopyFail:
			desc, _ := c.r.GetString()
			c.done = true
			return 0, pgerror.WithCode(fmt.Errorf("COPY from stdin failed: %s", desc), codes.QueryCanceled)
		case types.ClientFlush, types.ClientSync:
			// Ignored during COPY, as PostgreSQL does.
		default:
			c.done = true
			return 0, pgerror.WithCode(fmt.Errorf("unexpected message type 0x%02x during COPY from stdin", byte(t)), codes.ProtocolViolation)
		}
	}
	n := copy(p, c.buf)
	c.buf = c.buf[n:]
	return n, nil
}

// drain reads what the client still sends of the copy, after an error, so
// that the connection is ready for its next statement.
func (c *copyStream) drain() {
	var scratch [4096]byte
	for !c.done {
		c.buf = nil
		if _, err := c.Read(scratch[:]); err != nil {
			return
		}
	}
}

// copyField is one value of a row the client sent, as it sent it.
type copyField struct {
	v    []byte
	null bool
	// quoted says a CSV value was in quotes.
	quoted bool
}

// copyRows reads rows from the client's data in one of COPY's formats.
type copyRows interface {
	// next returns the next row, and io.EOF after the last.
	next() ([]copyField, error)
	// line is the line the last row read began on, for errors.
	line() int64
}

// copyDataError is an error in the data at a line.
func copyDataError(c copyIn, line int64, code codes.Code, format string, args ...any) error {
	return pgerror.WithCode(fmt.Errorf("%s (COPY %s, line %d)", fmt.Sprintf(format, args...), strings.Join(c.table, "."), line), code)
}

// stageCopyRows reads the client's rows into the temporary table staging.
func (s *Server) stageCopyRows(ctx context.Context, sess *session, c copyIn, cols []copyColumn, staging string, stream io.Reader) error {
	br := bufio.NewReaderSize(stream, 256<<10)
	var rows copyRows
	switch c.format {
	case "csv":
		rows = &csvCopyRows{br: br, c: c}
	case "binary":
		bin := &binaryCopyRows{br: br}
		if err := bin.readHeader(); err != nil {
			return copyDataError(c, 1, codes.BadCopyFileFormat, "%v", err)
		}
		rows = bin
	default:
		rows = &textCopyRows{br: br, c: c}
	}
	app, err := duckdb.NewAppender(sess.conn, "temp", "main", staging)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = app.Close()
		}
	}()
	vals := make([]driver.Value, len(cols)+1)
	first := true
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		row, err := rows.next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if pgerror.GetCode(err) != codes.Uncategorized {
				return err
			}
			return copyDataError(c, rows.line(), codes.BadCopyFileFormat, "%v", err)
		}
		if first && c.header != copyNoHeader {
			first = false
			if err := c.checkHeader(cols, row, rows.line()); err != nil {
				return err
			}
			continue
		}
		first = false
		if len(row) != len(cols) {
			if len(row) > len(cols) {
				return copyDataError(c, rows.line(), codes.BadCopyFileFormat, "extra data after last expected column")
			}
			return copyDataError(c, rows.line(), codes.BadCopyFileFormat, "missing data for column %q", cols[len(row)].name)
		}
		vals[0] = rows.line()
		for i, f := range row {
			var v driver.Value
			switch {
			case f.null && !cols[i].forceNotNull,
				f.quoted && cols[i].forceNull && string(f.v) == c.null:
				v = nil
			case c.format == "binary":
				v, err = binaryCopyValue(cols[i], f.v)
			default:
				v, err = textCopyValue(cols[i], f.v)
			}
			if err != nil {
				return copyDataError(c, rows.line(), codes.InvalidTextRepresentation, "column %s: %v", cols[i].name, err)
			}
			vals[i+1] = v
		}
		if err := app.AppendRow(vals...); err != nil {
			return copyDataError(c, rows.line(), codes.InvalidTextRepresentation, "%v", err)
		}
	}
	closed = true
	return app.Close()
}

// checkHeader checks the header line against the columns, for HEADER MATCH.
func (c copyIn) checkHeader(cols []copyColumn, row []copyField, line int64) error {
	if c.header != copyMatchHeader {
		return nil
	}
	if len(row) != len(cols) {
		return copyDataError(c, line, codes.BadCopyFileFormat, "wrong number of fields in header line: got %d, expected %d", len(row), len(cols))
	}
	for i, f := range row {
		if f.null || string(f.v) != cols[i].name {
			return copyDataError(c, line, codes.BadCopyFileFormat, "column name mismatch in header line field %d: got %q, expected %q",
				i+1, string(f.v), cols[i].name)
		}
	}
	return nil
}

// checkCopyValues checks that every value staged converts to its column's
// type, and fails on the first that does not, with its line and column.
func (s *Server) checkCopyValues(ctx context.Context, sess *session, c copyIn, cols []copyColumn, staging string) error {
	var checks []string
	var checked []int
	for i, col := range cols {
		v := stagedColumn(i)
		if expr := col.value(i); expr != v {
			checks = append(checks, "min(__line) FILTER (WHERE "+v+" IS NOT NULL AND try("+expr+") IS NULL)")
			checked = append(checked, i)
		}
	}
	if len(checks) == 0 {
		return nil
	}
	rows, err := queryRows(ctx, sess.conn, "SELECT "+strings.Join(checks, ", ")+" FROM "+staging)
	if err != nil || len(rows) != 1 {
		return pgError(err)
	}
	line, bad := int64(-1), -1
	for k, v := range rows[0] {
		if n, ok := v.(int64); ok && (line < 0 || n < line) {
			line, bad = n, checked[k]
		}
	}
	if bad < 0 {
		return nil
	}
	col := cols[bad]
	_, err = queryRows(ctx, sess.conn, "SELECT "+col.value(bad)+" FROM "+staging+" WHERE __line = "+strconv.FormatInt(line, 10))
	msg := "cannot convert the value to " + col.typ
	if err != nil {
		msg = strings.TrimSpace(pgerror.Flatten(pgError(err)).Message)
	}
	return pgerror.WithCode(fmt.Errorf("%s (COPY %s, line %d, column %s)", msg, strings.Join(c.table, "."), line, col.name),
		codes.InvalidTextRepresentation)
}

// textCopyValue puts a value of the text or CSV format into the form the
// column's cast reads.
func textCopyValue(col copyColumn, v []byte) (driver.Value, error) {
	switch col.kind {
	case copyBlob:
		return decodeBytea(v)
	case copyBool:
		b, err := pgBool(v)
		if err != nil {
			return nil, err
		}
		return strconv.FormatBool(b), nil
	case copyList:
		if len(v) > 0 && (v[0] == '{' || v[0] == '[' && bytes.Contains(v, []byte("]={"))) {
			j, err := pgArrayJSON(v)
			if err != nil {
				return nil, err
			}
			return j, nil
		}
	case copyInterval:
		// PostgreSQL writes -1 days +02:00:00; DuckDB reads the time with no
		// sign.
		v = bytes.ReplaceAll(v, []byte(" +"), []byte(" "))
	case copyDate:
		if bytes.HasSuffix(bytes.ToUpper(v), []byte(" BC")) {
			return nil, fmt.Errorf("dates before the Common Era (%q) are not supported", v)
		}
	}
	if !utf8.Valid(v) {
		return nil, pgerror.WithCode(errors.New(`invalid byte sequence for encoding "UTF8"`), codes.CharacterNotInRepertoire)
	}
	return string(v), nil
}

// pgBool reads a boolean as PostgreSQL does.
func pgBool(v []byte) (bool, error) {
	s := strings.ToLower(strings.TrimSpace(string(v)))
	switch {
	case s == "":
	case strings.HasPrefix("true", s), strings.HasPrefix("yes", s), s == "on", s == "1":
		return true, nil
	case strings.HasPrefix("false", s), strings.HasPrefix("no", s), s == "off", s == "of", s == "0":
		return false, nil
	}
	return false, fmt.Errorf("invalid input syntax for type boolean: %q", v)
}

// decodeBytea reads bytea's text forms: hex (\x0102) and escape (\001).
func decodeBytea(v []byte) ([]byte, error) {
	if bytes.HasPrefix(v, []byte(`\x`)) {
		h := bytes.Map(func(r rune) rune {
			if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
				return -1
			}
			return r
		}, v[2:])
		out := make([]byte, hex.DecodedLen(len(h)))
		if _, err := hex.Decode(out, h); err != nil {
			return nil, fmt.Errorf("invalid hexadecimal data in bytea: %v", err)
		}
		return out, nil
	}
	out := make([]byte, 0, len(v))
	for i := 0; i < len(v); i++ {
		if v[i] != '\\' {
			out = append(out, v[i])
			continue
		}
		switch {
		case i+1 < len(v) && v[i+1] == '\\':
			out = append(out, '\\')
			i++
		case i+3 < len(v) && isOctal(v[i+1]) && isOctal(v[i+2]) && isOctal(v[i+3]):
			n, _ := strconv.ParseUint(string(v[i+1:i+4]), 8, 8)
			out = append(out, byte(n))
			i += 3
		default:
			return nil, errors.New("invalid input syntax for type bytea")
		}
	}
	return out, nil
}

func isOctal(b byte) bool { return b >= '0' && b <= '7' }

// pgArrayJSON turns PostgreSQL's text form of an array, {1,2,"a b",NULL},
// into JSON, every element a string, which DuckDB casts to the list's
// element type.
func pgArrayJSON(v []byte) (string, error) {
	if v[0] == '[' {
		// Dimensions, [1:3]={...}: the elements are all there is.
		v = v[bytes.Index(v, []byte("]={"))+2:]
	}
	var out bytes.Buffer
	i := 0
	var parse func() error
	parse = func() error {
		if i >= len(v) || v[i] != '{' {
			return fmt.Errorf("malformed array literal: %q", v)
		}
		i++
		out.WriteByte('[')
		first := true
		for {
			for i < len(v) && (v[i] == ' ' || v[i] == '\t' || v[i] == '\n') {
				i++
			}
			if i < len(v) && v[i] == '}' && first {
				i++
				out.WriteByte(']')
				return nil
			}
			if !first {
				out.WriteByte(',')
			}
			first = false
			switch {
			case i >= len(v):
				return fmt.Errorf("malformed array literal: %q", v)
			case v[i] == '{':
				if err := parse(); err != nil {
					return err
				}
			case v[i] == '"':
				i++
				var elem []byte
				for i < len(v) && v[i] != '"' {
					if v[i] == '\\' && i+1 < len(v) {
						i++
					}
					elem = append(elem, v[i])
					i++
				}
				if i >= len(v) {
					return fmt.Errorf("malformed array literal: %q", v)
				}
				i++
				b, _ := json.Marshal(string(elem))
				out.Write(b)
			default:
				var elem []byte
				for i < len(v) && v[i] != ',' && v[i] != '}' {
					if v[i] == '\\' && i+1 < len(v) {
						i++
					}
					elem = append(elem, v[i])
					i++
				}
				s := strings.TrimSpace(string(elem))
				if strings.EqualFold(s, "NULL") {
					out.WriteString("null")
				} else {
					b, _ := json.Marshal(s)
					out.Write(b)
				}
			}
			for i < len(v) && (v[i] == ' ' || v[i] == '\t' || v[i] == '\n') {
				i++
			}
			switch {
			case i < len(v) && v[i] == ',':
				i++
			case i < len(v) && v[i] == '}':
				i++
				out.WriteByte(']')
				return nil
			default:
				return fmt.Errorf("malformed array literal: %q", v)
			}
		}
	}
	if err := parse(); err != nil {
		return "", err
	}
	if strings.TrimSpace(string(v[i:])) != "" {
		return "", fmt.Errorf("malformed array literal: %q", v)
	}
	if !utf8.Valid(out.Bytes()) {
		return "", pgerror.WithCode(errors.New(`invalid byte sequence for encoding "UTF8"`), codes.CharacterNotInRepertoire)
	}
	return out.String(), nil
}

// --- text format ------------------------------------------------------------

// textCopyRows reads COPY's text format: a line a row, values separated by
// the delimiter, backslash escapes, \N for NULL.
type textCopyRows struct {
	br      *bufio.Reader
	c       copyIn
	lineNo  int64
	pending [][]byte
	eof     bool
}

func (r *textCopyRows) line() int64 { return r.lineNo }

// readLine returns the next line, without its end: \n, \r\n or a bare \r,
// all of which end a line in PostgreSQL's text format.
func (r *textCopyRows) readLine() ([]byte, error) {
	for len(r.pending) == 0 {
		if r.eof {
			return nil, io.EOF
		}
		line, err := r.br.ReadBytes('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if errors.Is(err, io.EOF) {
			r.eof = true
			if len(line) == 0 {
				return nil, io.EOF
			}
		}
		line = bytes.TrimSuffix(line, []byte{'\n'})
		line = bytes.TrimSuffix(line, []byte{'\r'})
		r.pending = bytes.Split(line, []byte{'\r'})
	}
	l := r.pending[0]
	r.pending = r.pending[1:]
	r.lineNo++
	return l, nil
}

func (r *textCopyRows) next() ([]copyField, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if string(line) == `\.` {
		r.eof, r.pending = true, nil
		return nil, io.EOF
	}
	var fields []copyField
	start := 0
	for i := 0; i <= len(line); i++ {
		if i < len(line)-1 && line[i] == '\\' {
			i++
			continue
		}
		if i < len(line) && line[i] != r.c.delim {
			continue
		}
		raw := line[start:min(i, len(line))]
		start = i + 1
		if string(raw) == r.c.null {
			fields = append(fields, copyField{null: true})
			continue
		}
		v, _ := unescapeAll(raw)
		fields = append(fields, copyField{v: v})
	}
	return fields, nil
}

// unescapeAll undoes the text format's backslash escapes.
func unescapeAll(raw []byte) ([]byte, error) {
	if bytes.IndexByte(raw, '\\') < 0 {
		return raw, nil
	}
	out := make([]byte, 0, len(raw))
	for i := 0; i < len(raw); {
		if raw[i] != '\\' {
			out = append(out, raw[i])
			i++
			continue
		}
		v, n := unescapeText(raw[i:])
		out = append(out, v...)
		i += n
	}
	return out, nil
}

// unescapeText reads the escape at the start of b, a backslash and what
// follows, and returns what it stands for and how many bytes it took.
func unescapeText(b []byte) ([]byte, int) {
	if len(b) < 2 {
		return []byte{'\\'}, 1
	}
	switch c := b[1]; c {
	case 'b':
		return []byte{'\b'}, 2
	case 'f':
		return []byte{'\f'}, 2
	case 'n':
		return []byte{'\n'}, 2
	case 'r':
		return []byte{'\r'}, 2
	case 't':
		return []byte{'\t'}, 2
	case 'v':
		return []byte{'\v'}, 2
	case 'x':
		n := 2
		for n < len(b) && n < 4 && isHex(b[n]) {
			n++
		}
		if n == 2 {
			return []byte{'x'}, 2
		}
		v, _ := strconv.ParseUint(string(b[2:n]), 16, 8)
		return []byte{byte(v)}, n
	default:
		if isOctal(c) {
			n := 1
			for n < len(b) && n < 4 && isOctal(b[n]) {
				n++
			}
			v, _ := strconv.ParseUint(string(b[1:n]), 8, 16)
			return []byte{byte(v)}, n
		}
		return []byte{c}, 2
	}
}

// --- CSV format -------------------------------------------------------------

// csvCopyRows reads COPY's CSV format, as PostgreSQL writes and reads it:
// quoted values may hold delimiters and line ends, and only an unquoted
// value that matches the NULL string is NULL.
type csvCopyRows struct {
	br     *bufio.Reader
	c      copyIn
	lineNo int64
	start  int64
}

func (r *csvCopyRows) line() int64 { return r.start }

func (r *csvCopyRows) next() ([]copyField, error) {
	var (
		fields  []copyField
		cur     []byte
		quoted  bool
		inQuote bool
		any     bool
	)
	r.lineNo++
	r.start = r.lineNo
	c := r.c
	finish := func() []copyField {
		return append(fields, copyField{v: cur, quoted: quoted, null: !quoted && string(cur) == c.null})
	}
	for {
		b, err := r.br.ReadByte()
		if errors.Is(err, io.EOF) {
			if inQuote {
				return nil, errors.New("unterminated CSV quoted field")
			}
			if !any {
				return nil, io.EOF
			}
			return r.end(finish())
		}
		if err != nil {
			return nil, err
		}
		any = true
		if inQuote {
			switch {
			case b == c.escape && c.escape != c.quote:
				next, err := r.br.ReadByte()
				if err == nil && (next == c.quote || next == c.escape) {
					cur = append(cur, next)
					continue
				}
				if err == nil {
					_ = r.br.UnreadByte()
				}
				cur = append(cur, b)
			case b == c.quote:
				next, err := r.br.ReadByte()
				if err == nil && next == c.quote && c.escape == c.quote {
					cur = append(cur, c.quote)
					continue
				}
				if err == nil {
					_ = r.br.UnreadByte()
				}
				inQuote = false
			default:
				if b == '\n' {
					r.lineNo++
				}
				cur = append(cur, b)
			}
			continue
		}
		switch b {
		case c.delim:
			fields = finish()
			cur, quoted = nil, false
		case c.quote:
			inQuote, quoted = true, true
		case '\n':
			return r.end(finish())
		case '\r':
			if next, err := r.br.ReadByte(); err == nil && next != '\n' {
				_ = r.br.UnreadByte()
			}
			return r.end(finish())
		default:
			cur = append(cur, b)
		}
	}
}

// end finishes a row: the end-of-data marker \. alone on its line ends the
// data, and the FORCE options' columns have their NULLs set.
func (r *csvCopyRows) end(fields []copyField) ([]copyField, error) {
	if len(fields) == 1 && !fields[0].quoted && string(fields[0].v) == `\.` {
		return nil, io.EOF
	}
	return fields, nil
}

// --- binary format ----------------------------------------------------------

// binaryCopyRows reads COPY's binary format: a header, then each row as its
// field count and each value as its length and bytes.
type binaryCopyRows struct {
	br  *bufio.Reader
	row int64
}

func (r *binaryCopyRows) line() int64 { return r.row }

func (r *binaryCopyRows) readHeader() error {
	sig := make([]byte, len(wire.CopySignature))
	if _, err := io.ReadFull(r.br, sig); err != nil || !bytes.Equal(sig, wire.CopySignature) {
		return errors.New("COPY file signature not recognized")
	}
	var head [8]byte
	if _, err := io.ReadFull(r.br, head[:]); err != nil {
		return errors.New("invalid COPY file header (missing flags)")
	}
	if flags := binary.BigEndian.Uint32(head[:4]); flags&(1<<16) != 0 {
		return errors.New("COPY with OIDs is not supported")
	}
	if ext := binary.BigEndian.Uint32(head[4:]); ext > 0 {
		if _, err := r.br.Discard(int(ext)); err != nil {
			return errors.New("invalid COPY file header (wrong length)")
		}
	}
	return nil
}

func (r *binaryCopyRows) next() ([]copyField, error) {
	var n [2]byte
	if _, err := io.ReadFull(r.br, n[:]); err != nil {
		if errors.Is(err, io.EOF) {
			// The end of the data, as pgx sends it, without the trailer.
			return nil, io.EOF
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errors.New("unexpected EOF in COPY data")
		}
		return nil, err
	}
	count := int16(binary.BigEndian.Uint16(n[:]))
	if count == -1 {
		return nil, io.EOF
	}
	r.row++
	fields := make([]copyField, count)
	for i := range fields {
		var l [4]byte
		if _, err := io.ReadFull(r.br, l[:]); err != nil {
			return nil, errors.New("unexpected EOF in COPY data")
		}
		size := int32(binary.BigEndian.Uint32(l[:]))
		if size == -1 {
			fields[i].null = true
			continue
		}
		if size < 0 {
			return nil, errors.New("invalid field size")
		}
		v := make([]byte, size)
		if _, err := io.ReadFull(r.br, v); err != nil {
			return nil, errors.New("unexpected EOF in COPY data")
		}
		fields[i].v = v
	}
	return fields, nil
}

// pgEpoch is where PostgreSQL's binary dates and timestamps count from.
var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// binaryCopyValue reads a value of the binary format, encoded as the type
// the client was told the column has (col.oid), into the form the column's
// cast reads.
func binaryCopyValue(col copyColumn, v []byte) (driver.Value, error) {
	size := func(n int) error {
		if len(v) != n {
			return fmt.Errorf("incorrect binary data format: %d bytes for a value of %d", len(v), n)
		}
		return nil
	}
	switch col.oid {
	case pgtype.BoolOID:
		if err := size(1); err != nil {
			return nil, err
		}
		return strconv.FormatBool(v[0] != 0), nil
	case pgtype.Int2OID:
		if err := size(2); err != nil {
			return nil, err
		}
		return strconv.Itoa(int(int16(binary.BigEndian.Uint16(v)))), nil
	case pgtype.Int4OID:
		if err := size(4); err != nil {
			return nil, err
		}
		return strconv.Itoa(int(int32(binary.BigEndian.Uint32(v)))), nil
	case pgtype.Int8OID:
		if err := size(8); err != nil {
			return nil, err
		}
		return strconv.FormatInt(int64(binary.BigEndian.Uint64(v)), 10), nil
	case pgtype.Float4OID:
		if err := size(4); err != nil {
			return nil, err
		}
		return formatFloat(float64(math.Float32frombits(binary.BigEndian.Uint32(v))), 32), nil
	case pgtype.Float8OID:
		if err := size(8); err != nil {
			return nil, err
		}
		return formatFloat(math.Float64frombits(binary.BigEndian.Uint64(v)), 64), nil
	case pgtype.NumericOID:
		return binaryNumeric(v)
	case pgtype.ByteaOID:
		return v, nil
	case pgtype.UUIDOID:
		if err := size(16); err != nil {
			return nil, err
		}
		h := hex.EncodeToString(v)
		return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:], nil
	case pgtype.DateOID:
		if err := size(4); err != nil {
			return nil, err
		}
		switch d := int32(binary.BigEndian.Uint32(v)); d {
		case math.MaxInt32:
			return "infinity", nil
		case math.MinInt32:
			return "-infinity", nil
		default:
			return pgEpoch.AddDate(0, 0, int(d)).Format("2006-01-02"), nil
		}
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		if err := size(8); err != nil {
			return nil, err
		}
		switch us := int64(binary.BigEndian.Uint64(v)); us {
		case math.MaxInt64:
			return "infinity", nil
		case math.MinInt64:
			return "-infinity", nil
		default:
			t := pgEpoch.Add(time.Duration(us%86_400_000_000)*time.Microsecond).AddDate(0, 0, int(us/86_400_000_000))
			s := t.Format("2006-01-02 15:04:05.999999")
			if col.oid == pgtype.TimestamptzOID {
				s += "+00"
			}
			return s, nil
		}
	case pgtype.TimeOID:
		if err := size(8); err != nil {
			return nil, err
		}
		us := int64(binary.BigEndian.Uint64(v))
		return time.UnixMicro(us).UTC().Format("15:04:05.999999"), nil
	case pgtype.IntervalOID:
		if err := size(16); err != nil {
			return nil, err
		}
		us := int64(binary.BigEndian.Uint64(v[:8]))
		days := int32(binary.BigEndian.Uint32(v[8:12]))
		months := int32(binary.BigEndian.Uint32(v[12:]))
		return fmt.Sprintf("%d months %d days %d microseconds", months, days, us), nil
	case pgtype.JSONBOID:
		if len(v) == 0 || v[0] != 1 {
			return nil, errors.New("unsupported jsonb version")
		}
		v = v[1:]
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.JSONOID, pgtype.BPCharOID, pgtype.NameOID:
	default:
		return nil, fmt.Errorf("binary COPY of a value of type %d is not supported; send text or CSV", col.oid)
	}
	if !utf8.Valid(v) {
		return nil, pgerror.WithCode(errors.New(`invalid byte sequence for encoding "UTF8"`), codes.CharacterNotInRepertoire)
	}
	return string(v), nil
}

func formatFloat(f float64, bits int) string {
	switch {
	case math.IsNaN(f):
		return "nan"
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	return strconv.FormatFloat(f, 'g', -1, bits)
}

// binaryNumeric reads numeric's binary form: its digit count, weight, sign
// and display scale, then its digits in base 10000.
func binaryNumeric(v []byte) (driver.Value, error) {
	if len(v) < 8 {
		return nil, errors.New("invalid numeric")
	}
	ndigits := int(binary.BigEndian.Uint16(v[0:2]))
	weight := int(int16(binary.BigEndian.Uint16(v[2:4])))
	sign := binary.BigEndian.Uint16(v[4:6])
	dscale := int(binary.BigEndian.Uint16(v[6:8]))
	switch sign {
	case 0xC000:
		return "NaN", nil
	case 0xD000:
		return "Infinity", nil
	case 0xF000:
		return "-Infinity", nil
	}
	if len(v) != 8+2*ndigits {
		return nil, errors.New("invalid numeric")
	}
	n := new(big.Int)
	base := big.NewInt(10000)
	for i := range ndigits {
		n.Mul(n, base)
		n.Add(n, big.NewInt(int64(binary.BigEndian.Uint16(v[8+2*i:]))))
	}
	// n is the digits; its last digit's place is 10000^(weight-ndigits+1).
	exp := 4 * (weight - ndigits + 1)
	digits := n.String()
	if n.Sign() == 0 {
		digits = "0"
		exp = 0
	}
	var s string
	if exp >= 0 {
		s = digits + strings.Repeat("0", exp)
		if dscale > 0 {
			s += "." + strings.Repeat("0", dscale)
		}
	} else {
		frac := -exp
		if len(digits) <= frac {
			digits = strings.Repeat("0", frac-len(digits)+1) + digits
		}
		s = digits[:len(digits)-frac] + "." + digits[len(digits)-frac:]
	}
	if sign == 0x4000 {
		s = "-" + s
	}
	return s, nil
}
