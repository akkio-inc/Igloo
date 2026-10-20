package federation

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// The scans DuckDB's postgres extension sends the gateway, and the
// Snowflake queries they become.
//
// For every scan of a table the extension sends one statement, of a fixed
// shape built from DuckDB's plan:
//
//	COPY (SELECT "A", "B" FROM "SCHEMA"."TABLE" WHERE ctid BETWEEN '(0,0)'::tid AND '(4294967295,0)'::tid
//	    AND "A" >= '5.00' AND ("B" = 'x' COLLATE "C" OR "B" IS NULL) LIMIT 10 OFFSET 5) TO STDOUT (FORMAT "binary");
//
// with SELECT NULL when DuckDB needs no column (count(*)), filters on the
// columns pushed down from the query (comparisons with constants written as
// strings, IN lists, IS [NOT] NULL, AND and OR), and LIMIT and OFFSET when
// they apply to the scan alone. The gateway parses exactly that grammar and
// refuses anything else, so no other SQL can reach Snowflake through it,
// and writes the Snowflake query anew from what it parsed: names quoted for
// Snowflake, constants converted to the column's Snowflake type in
// Snowflake's syntax (where a backslash in a string is an escape), so a
// constant from a user's query can never become SQL.
//
// A pushed filter replaces DuckDB's own: DuckDB does not check the rows a
// scan returns against it again. So a filter the gateway cannot express in
// Snowflake exactly fails the scan rather than being dropped.

// scanQuery is a scan DuckDB's postgres extension asked for.
type scanQuery struct {
	schema, table string
	// columns are the columns to read, in order; nil when the extension
	// selects NULL, needing only the number of rows.
	columns []string
	filter  *scanFilter
	// ctid is the range of pages a parallel scan task reads, when given.
	ctid          bool
	ctidFrom      int64
	ctidTo        int64
	limit, offset int64 // -1 when not given
}

// scanFilter is a filter of a scan: an AND or OR of filters, or a
// condition on one column.
type scanFilter struct {
	op       string // "and", "or", "cmp", "null", "notnull", "in"
	children []*scanFilter
	column   string
	cmp      string // = <> < <= > >=
	values   []scanValue
}

// scanValue is a constant of a filter, as DuckDB wrote it.
type scanValue struct {
	text  string
	bytea bool
}

// fullCTID is the end of the last task's range, past any page.
const fullCTID = 4294967295

// scanToken is a token of the extension's statement.
type scanToken struct {
	kind byte // 'w' word, 'i' quoted identifier, 's' string, 'n' number, or the punctuation
	text string
}

func (t scanToken) is(word string) bool { return t.kind == 'w' && strings.EqualFold(t.text, word) }

// tokenizeScan splits the extension's statement into tokens.
func tokenizeScan(q string) ([]scanToken, error) {
	var out []scanToken
	for i := 0; i < len(q); {
		c := q[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '"' || c == '\'':
			var b strings.Builder
			j := i + 1
			for {
				if j >= len(q) {
					return nil, errors.New("unterminated quote")
				}
				if q[j] == c {
					if j+1 < len(q) && q[j+1] == c {
						b.WriteByte(c)
						j += 2
						continue
					}
					break
				}
				b.WriteByte(q[j])
				j++
			}
			kind := byte('s')
			if c == '"' {
				kind = 'i'
			}
			out = append(out, scanToken{kind, b.String()})
			i = j + 1
		case c >= '0' && c <= '9':
			j := i
			for j < len(q) && q[j] >= '0' && q[j] <= '9' {
				j++
			}
			out = append(out, scanToken{'n', q[i:j]})
			i = j
		case c == '_' || c < 0x80 && unicode.IsLetter(rune(c)):
			j := i
			for j < len(q) && (q[j] == '_' || q[j] < 0x80 && (unicode.IsLetter(rune(q[j])) || unicode.IsDigit(rune(q[j])))) {
				j++
			}
			out = append(out, scanToken{'w', q[i:j]})
			i = j
		default:
			for _, op := range []string{"::", "<>", "!=", "<=", ">=", "=", "<", ">", "(", ")", ",", ".", ";"} {
				if strings.HasPrefix(q[i:], op) {
					out = append(out, scanToken{op[0], op})
					if len(op) == 2 {
						out[len(out)-1].kind = 'o'
					}
					i += len(op)
					goto next
				}
			}
			return nil, fmt.Errorf("unexpected %q", q[i:i+1])
		next:
		}
	}
	return out, nil
}

// scanParser reads tokens.
type scanParser struct {
	toks []scanToken
	i    int
}

func (p *scanParser) peek() scanToken {
	if p.i < len(p.toks) {
		return p.toks[p.i]
	}
	return scanToken{}
}

func (p *scanParser) next() scanToken {
	t := p.peek()
	p.i++
	return t
}

func (p *scanParser) word(w string) bool {
	if p.peek().is(w) {
		p.i++
		return true
	}
	return false
}

func (p *scanParser) punct(s string) bool {
	if t := p.peek(); t.text == s && t.kind != 's' && t.kind != 'i' && t.kind != 'w' {
		p.i++
		return true
	}
	return false
}

func (p *scanParser) expect(ok bool, what string) error {
	if !ok {
		return fmt.Errorf("expected %s at %q", what, p.peek().text)
	}
	return nil
}

func (p *scanParser) ident() (string, error) {
	t := p.next()
	if t.kind != 'i' {
		return "", fmt.Errorf("expected a quoted name, not %q", t.text)
	}
	return t.text, nil
}

func (p *scanParser) number() (int64, error) {
	t := p.next()
	if t.kind != 'n' {
		return 0, fmt.Errorf("expected a number, not %q", t.text)
	}
	return strconv.ParseInt(t.text, 10, 64)
}

// parseScan parses a statement of the extension's: COPY (SELECT ...) TO
// STDOUT (FORMAT "binary").
func parseScan(q string) (*scanQuery, error) {
	toks, err := tokenizeScan(q)
	if err != nil {
		return nil, err
	}
	p := &scanParser{toks: toks}
	s := &scanQuery{limit: -1, offset: -1}
	if err := p.expect(p.word("COPY") && p.punct("(") && p.word("SELECT"), "COPY (SELECT"); err != nil {
		return nil, err
	}
	if p.word("NULL") {
		s.columns = nil
	} else {
		s.columns = []string{}
		for {
			c, err := p.ident()
			if err != nil {
				return nil, err
			}
			s.columns = append(s.columns, c)
			if !p.punct(",") {
				break
			}
		}
	}
	if err := p.expect(p.word("FROM"), "FROM"); err != nil {
		return nil, err
	}
	if s.schema, err = p.ident(); err != nil {
		return nil, err
	}
	if err := p.expect(p.punct("."), "."); err != nil {
		return nil, err
	}
	if s.table, err = p.ident(); err != nil {
		return nil, err
	}
	if p.word("WHERE") {
		f, err := p.disjunction(s)
		if err != nil {
			return nil, err
		}
		s.filter = f
	}
	if p.word("LIMIT") {
		if s.limit, err = p.number(); err != nil {
			return nil, err
		}
	}
	if p.word("OFFSET") {
		if s.offset, err = p.number(); err != nil {
			return nil, err
		}
	}
	if err := p.expect(p.punct(")") && p.word("TO") && p.word("STDOUT") && p.punct("(") && p.word("FORMAT"), ") TO STDOUT (FORMAT"); err != nil {
		return nil, err
	}
	if t := p.next(); t.kind != 'i' || !strings.EqualFold(t.text, "binary") {
		return nil, errors.New("only binary COPY is supported")
	}
	if err := p.expect(p.punct(")"), ")"); err != nil {
		return nil, err
	}
	p.punct(";")
	if p.i != len(p.toks) {
		return nil, fmt.Errorf("unexpected %q", p.peek().text)
	}
	return s, nil
}

// disjunction reads filters joined by OR; conjunction those joined by AND.
// A ctid range, which the extension puts first, is taken into s.
func (p *scanParser) disjunction(s *scanQuery) (*scanFilter, error) {
	f, err := p.conjunction(s)
	if err != nil {
		return nil, err
	}
	if !p.peek().is("OR") {
		return f, nil
	}
	or := &scanFilter{op: "or", children: []*scanFilter{f}}
	for p.word("OR") {
		g, err := p.conjunction(s)
		if err != nil {
			return nil, err
		}
		or.children = append(or.children, g)
	}
	if slices.Contains(or.children, nil) {
		return nil, errors.New("a ctid range inside OR")
	}
	return or, nil
}

func (p *scanParser) conjunction(s *scanQuery) (*scanFilter, error) {
	and := &scanFilter{op: "and"}
	for {
		f, err := p.primary(s)
		if err != nil {
			return nil, err
		}
		if f != nil {
			and.children = append(and.children, f)
		}
		if !p.word("AND") {
			break
		}
	}
	switch len(and.children) {
	case 0:
		return nil, nil
	case 1:
		return and.children[0], nil
	}
	return and, nil
}

var tidLiteral = regexp.MustCompile(`^\((\d+),0\)$`)

func (p *scanParser) primary(s *scanQuery) (*scanFilter, error) {
	if p.punct("(") {
		f, err := p.disjunction(s)
		if err != nil {
			return nil, err
		}
		if f == nil {
			return nil, errors.New("a ctid range inside parentheses")
		}
		return f, p.expect(p.punct(")"), ")")
	}
	if p.peek().is("ctid") {
		if s.ctid {
			return nil, errors.New("two ctid ranges")
		}
		p.next()
		if err := p.expect(p.word("BETWEEN"), "BETWEEN"); err != nil {
			return nil, err
		}
		from, err := p.tid()
		if err != nil {
			return nil, err
		}
		if err := p.expect(p.word("AND"), "AND"); err != nil {
			return nil, err
		}
		to, err := p.tid()
		if err != nil {
			return nil, err
		}
		s.ctid, s.ctidFrom, s.ctidTo = true, from, to
		return nil, nil
	}
	col, err := p.ident()
	if err != nil {
		return nil, err
	}
	switch {
	case p.word("IS"):
		op := "null"
		if p.word("NOT") {
			op = "notnull"
		}
		return &scanFilter{op: op, column: col}, p.expect(p.word("NULL"), "NULL")
	case p.word("IN"):
		f := &scanFilter{op: "in", column: col}
		if err := p.expect(p.punct("("), "("); err != nil {
			return nil, err
		}
		for {
			v, err := p.value()
			if err != nil {
				return nil, err
			}
			f.values = append(f.values, v)
			if !p.punct(",") {
				break
			}
		}
		return f, p.expect(p.punct(")"), ")")
	}
	t := p.next()
	cmp := t.text
	switch {
	case t.kind == 's' || t.kind == 'i' || t.kind == 'w' || t.kind == 'n':
		return nil, fmt.Errorf("unsupported condition on %q at %q", col, t.text)
	case cmp == "!=":
		cmp = "<>"
	case cmp != "=" && cmp != "<>" && cmp != "<" && cmp != "<=" && cmp != ">" && cmp != ">=":
		return nil, fmt.Errorf("unsupported condition on %q at %q", col, t.text)
	}
	v, err := p.value()
	if err != nil {
		return nil, err
	}
	if p.word("COLLATE") {
		// DuckDB compares strings byte by byte: COLLATE "C".
		if c := p.next(); c.kind != 'i' || c.text != "C" {
			return nil, fmt.Errorf("unsupported collation %q", c.text)
		}
	}
	return &scanFilter{op: "cmp", column: col, cmp: cmp, values: []scanValue{v}}, nil
}

func (p *scanParser) tid() (int64, error) {
	t := p.next()
	m := tidLiteral.FindStringSubmatch(t.text)
	if t.kind != 's' || m == nil {
		return 0, fmt.Errorf("unexpected ctid %q", t.text)
	}
	if err := p.expect(p.punct("::") && p.word("tid"), "::tid"); err != nil {
		return 0, err
	}
	return strconv.ParseInt(m[1], 10, 64)
}

func (p *scanParser) value() (scanValue, error) {
	t := p.next()
	if t.kind != 's' {
		return scanValue{}, fmt.Errorf("expected a constant, not %q", t.text)
	}
	v := scanValue{text: t.text}
	if p.punct("::") {
		if !p.word("BYTEA") {
			return v, fmt.Errorf("unsupported cast to %q", p.peek().text)
		}
		v.bytea = true
	}
	return v, nil
}

// sfScan is a scan resolved against the catalog: the table and columns it
// reads, and the Snowflake query that reads them.
type sfScan struct {
	table   *sfTable
	columns []*sfColumn
	// count is set for SELECT NULL: the query counts the rows, and the
	// gateway sends that many rows of one NULL.
	count bool
	sql   string
}

// planScan resolves q against cat and writes its Snowflake query.
func planScan(cat *sfCatalog, q *scanQuery) (*sfScan, error) {
	schema := cat.schema(q.schema)
	if schema == nil {
		return nil, fmt.Errorf("schema %q does not exist", q.schema)
	}
	table := schema.table(q.table)
	if table == nil {
		return nil, fmt.Errorf("relation \"%s.%s\" does not exist", q.schema, q.table)
	}
	scan := &sfScan{table: table, count: q.columns == nil}
	from := sfQuote(cat.database) + "." + sfQuote(schema.name) + "." + sfQuote(table.name)
	var b strings.Builder
	if scan.count {
		b.WriteString("SELECT 1")
	} else {
		b.WriteString("SELECT ")
		for i, name := range q.columns {
			c := table.column(name)
			if c == nil {
				return nil, fmt.Errorf("column %q of relation %q does not exist", name, q.table)
			}
			scan.columns = append(scan.columns, c)
			if i > 0 {
				b.WriteString(", ")
			}
			expr, _ := c.typ.selectExpr(sfQuote(c.name))
			b.WriteString(expr)
		}
	}
	b.WriteString(" FROM " + from)
	if q.filter != nil {
		where, err := filterSQL(table, q.filter)
		if err != nil {
			return nil, fmt.Errorf("cannot pass a filter on to Snowflake exactly: %w", err)
		}
		if q.filter.op == "and" || q.filter.op == "or" {
			where = where[1 : len(where)-1]
		}
		b.WriteString(" WHERE " + where)
	}
	if q.limit >= 0 {
		fmt.Fprintf(&b, " LIMIT %d", q.limit)
	}
	if q.offset >= 0 {
		if q.limit < 0 {
			// Snowflake's OFFSET needs a LIMIT; NULL is none.
			b.WriteString(" LIMIT NULL")
		}
		fmt.Fprintf(&b, " OFFSET %d", q.offset)
	}
	scan.sql = b.String()
	if scan.count {
		// Only the number of rows is needed; Snowflake answers COUNT(*) of a
		// whole table from its metadata, without the warehouse.
		if q.filter == nil && q.limit < 0 && q.offset < 0 {
			scan.sql = "SELECT COUNT(*) FROM " + from
		} else {
			scan.sql = "SELECT COUNT(*) FROM (" + scan.sql + ")"
		}
	}
	return scan, nil
}

// filterSQL writes f in Snowflake's SQL.
func filterSQL(t *sfTable, f *scanFilter) (string, error) {
	switch f.op {
	case "and", "or":
		parts := make([]string, len(f.children))
		for i, c := range f.children {
			s, err := filterSQL(t, c)
			if err != nil {
				return "", err
			}
			parts[i] = s
		}
		return "(" + strings.Join(parts, " "+strings.ToUpper(f.op)+" ") + ")", nil
	}
	c := t.column(f.column)
	if c == nil {
		return "", fmt.Errorf("column %q of relation %q does not exist", f.column, t.pgName)
	}
	col := sfQuote(c.name)
	switch f.op {
	case "null":
		return col + " IS NULL", nil
	case "notnull":
		return col + " IS NOT NULL", nil
	case "in":
		parts := make([]string, len(f.values))
		for i, v := range f.values {
			s, err := comparisonSQL(c, "=", v)
			if err != nil {
				return "", err
			}
			parts[i] = s
		}
		return "(" + strings.Join(parts, " OR ") + ")", nil
	case "cmp":
		return comparisonSQL(c, f.cmp, f.values[0])
	}
	return "", fmt.Errorf("unsupported filter %q", f.op)
}

var (
	decimalText = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?$`)
	dateText    = regexp.MustCompile(`^([0-9]{4})-([0-9]{2})-([0-9]{2})$`)
	timeText    = regexp.MustCompile(`^([0-9]{2}):([0-9]{2}):([0-9]{2})(\.[0-9]{1,6})?$`)
	// DuckDB writes timestamps with a time zone with an offset of hours,
	// and minutes when there are any: +00, +05:30.
	timestampText = regexp.MustCompile(`^([0-9]{4}-[0-9]{2}-[0-9]{2}) ([0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?)(?:([+-])([0-9]{2})(?::([0-9]{2}))?)?$`)
)

// comparisonSQL writes column c compared with v, a constant DuckDB wrote
// of c's type, in Snowflake's SQL.
func comparisonSQL(c *sfColumn, cmp string, v scanValue) (string, error) {
	col := sfQuote(c.name)
	bad := func() (string, error) {
		return "", fmt.Errorf("unexpected constant %q for column %q (%s)", v.text, c.pgName, c.typ.name)
	}
	if v.bytea != (c.typ.kind == sfBinary) {
		return bad()
	}
	switch c.typ.kind {
	case sfNumber:
		if !decimalText.MatchString(v.text) {
			return bad()
		}
		return col + " " + cmp + " " + v.text, nil
	case sfFloat:
		f, err := strconv.ParseFloat(v.text, 64)
		if err != nil {
			return bad()
		}
		var lit string
		switch {
		case math.IsNaN(f):
			lit = "'NaN'"
		case math.IsInf(f, 1):
			lit = "'inf'"
		case math.IsInf(f, -1):
			lit = "'-inf'"
		default:
			lit = "'" + strconv.FormatFloat(f, 'g', -1, 64) + "'"
		}
		return col + " " + cmp + " CAST(" + lit + " AS FLOAT)", nil
	case sfBoolean:
		switch strings.ToLower(v.text) {
		case "true", "t":
			return col + " " + cmp + " TRUE", nil
		case "false", "f":
			return col + " " + cmp + " FALSE", nil
		}
		return bad()
	case sfBinary:
		if !strings.HasPrefix(v.text, `\x`) {
			return bad()
		}
		b, err := hex.DecodeString(v.text[2:])
		if err != nil {
			return bad()
		}
		return col + " " + cmp + " TO_BINARY('" + strings.ToUpper(hex.EncodeToString(b)) + "', 'HEX')", nil
	case sfText:
		// DuckDB compares strings byte by byte. Snowflake compares a column
		// with a collation (such as en-ci, which an account's
		// DEFAULT_DDL_COLLATION gives every new column) as the collation
		// says, so that 'EU' would match 'eu' and a row access policy's
		// pushed filter would keep rows it hides. Under the utf8 collation
		// Snowflake compares as DuckDB does, whatever the column's, and
		// still prunes micro-partitions by the filter.
		return "COLLATE(" + col + ", 'utf8') " + cmp + " " + sfString(v.text), nil
	case sfJSON, sfAsText:
		// DuckDB compares the text the gateway selected.
		expr, _ := c.typ.selectExpr(col)
		return expr + " " + cmp + " " + sfString(v.text), nil
	case sfDate:
		if inf, ok := infinity(v.text); ok {
			return infiniteComparison(col, cmp, inf), nil
		}
		m := dateText.FindStringSubmatch(v.text)
		if m == nil || m[1] == "0000" {
			return bad()
		}
		return col + " " + cmp + " TO_DATE('" + v.text + "', 'YYYY-MM-DD')", nil
	case sfTime:
		m := timeText.FindStringSubmatch(v.text)
		if m == nil {
			return bad()
		}
		h, _ := strconv.Atoi(m[1])
		mi, _ := strconv.Atoi(m[2])
		s, _ := strconv.Atoi(m[3])
		us := fraction6(m[4])
		if h > 23 || mi > 59 || s > 59 {
			// DuckDB's 24:00:00 is after any Snowflake time.
			if v.text == "24:00:00" {
				return infiniteComparison(col, cmp, 1), nil
			}
			return bad()
		}
		at := time.Duration(h)*time.Hour + time.Duration(mi)*time.Minute + time.Duration(s)*time.Second + time.Duration(us)*time.Microsecond
		lit := func(d time.Duration) string {
			if d >= 24*time.Hour {
				return ""
			}
			return fmt.Sprintf("TO_TIME('%02d:%02d:%02d.%06d', 'HH24:MI:SS.FF6')",
				int(d/time.Hour), int(d/time.Minute)%60, int(d/time.Second)%60, int(d/time.Microsecond)%1000000)
		}
		return truncatedComparison(col, cmp, c.typ.scale, lit(at), lit(at+time.Microsecond)), nil
	case sfTimestamp, sfTimestampLTZ, sfTimestampTZ:
		if inf, ok := infinity(v.text); ok {
			return infiniteComparison(col, cmp, inf), nil
		}
		m := timestampText.FindStringSubmatch(v.text)
		if m == nil {
			return bad()
		}
		withZone := c.typ.kind != sfTimestamp
		if withZone != (m[3] != "") {
			return bad()
		}
		zone := time.UTC
		if withZone {
			h, _ := strconv.Atoi(m[4])
			mi, _ := strconv.Atoi(m[5])
			off := h*3600 + mi*60
			if m[3] == "-" {
				off = -off
			}
			zone = time.FixedZone("", off)
		}
		at, err := time.ParseInLocation("2006-01-02 15:04:05.999999", m[1]+" "+m[2], zone)
		if err != nil || at.Year() < 1 || at.Year() > 9999 {
			return bad()
		}
		fn := map[sfKind]string{sfTimestamp: "TO_TIMESTAMP_NTZ", sfTimestampLTZ: "TO_TIMESTAMP_LTZ", sfTimestampTZ: "TO_TIMESTAMP_TZ"}[c.typ.kind]
		lit := func(t time.Time) string {
			if !withZone {
				return fn + "('" + t.Format("2006-01-02 15:04:05.000000") + "', 'YYYY-MM-DD HH24:MI:SS.FF6')"
			}
			return fn + "('" + t.UTC().Format("2006-01-02 15:04:05.000000") + " +0000', 'YYYY-MM-DD HH24:MI:SS.FF6 TZHTZM')"
		}
		return truncatedComparison(col, cmp, c.typ.scale, lit(at), lit(at.Add(time.Microsecond))), nil
	}
	return bad()
}

// fraction6 reads .123 as 123000 microseconds.
func fraction6(frac string) int {
	if frac == "" {
		return 0
	}
	d := (frac[1:] + "000000")[:6]
	n, _ := strconv.Atoi(d)
	return n
}

// truncatedComparison compares a column of times with scale fractional
// digits of seconds with at, a time DuckDB wrote to the microsecond; next
// is at plus a microsecond, or "" if there is none. With more digits than
// microseconds, DuckDB sees each value cut to the microsecond (the
// gateway truncates them), so the comparison is of that: a column is
// equal to at from at up to next, less than or equal to at below next.
// Written on the column itself, not a function of it, Snowflake can still
// skip the micro-partitions the filter rules out.
func truncatedComparison(col, cmp string, scale int, at, next string) string {
	if scale <= 6 {
		return col + " " + cmp + " " + at
	}
	if next == "" {
		// The last microsecond of the day.
		switch cmp {
		case "=", ">=":
			return col + " >= " + at
		case "<>", "<":
			return col + " < " + at
		case "<=":
			return col + " IS NOT NULL"
		case ">":
			return "FALSE"
		}
	}
	switch cmp {
	case "=":
		return "(" + col + " >= " + at + " AND " + col + " < " + next + ")"
	case "<>":
		return "(" + col + " < " + at + " OR " + col + " >= " + next + ")"
	case "<":
		return col + " < " + at
	case "<=":
		return col + " < " + next
	case ">":
		return col + " >= " + next
	}
	return col + " >= " + at
}

// infinity reports DuckDB's infinite dates and timestamps: 1 for infinity,
// -1 for -infinity.
func infinity(s string) (int, bool) {
	switch s {
	case "infinity":
		return 1, true
	case "-infinity":
		return -1, true
	}
	return 0, false
}

// infiniteComparison compares a column with an infinite value, which
// Snowflake has none of: every value is less than infinity.
func infiniteComparison(col, cmp string, inf int) string {
	less := cmp == "<" || cmp == "<=" || cmp == "<>"
	if inf < 0 {
		less = cmp == ">" || cmp == ">=" || cmp == "<>"
	}
	if less {
		return col + " IS NOT NULL"
	}
	return "FALSE"
}
