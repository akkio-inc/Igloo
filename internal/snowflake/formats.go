package snowflake

import (
	"fmt"
	"strings"
)

// dateFormat converts a Snowflake date and time format, such as
// 'YYYY-MM-DD HH24:MI:SS', to a DuckDB strftime/strptime format. Format
// elements are matched without regard to case, as Snowflake does; text in
// double quotes is literal.
func dateFormat(f string) (string, error) {
	var b strings.Builder
	upper := strings.ToUpper(f)
	elements := []struct{ el, conv string }{
		{"YYYY", "%Y"}, {"HH24", "%H"}, {"HH12", "%I"}, {"MMMM", "%B"}, {"FF9", "%n"}, {"FF6", "%f"},
		{"FF3", "%g"}, {"FF0", ""}, {"TZH:TZM", "%z"}, {"A.M.", "%p"}, {"P.M.", "%p"},
		{"MON", "%b"}, {"TZR", "%Z"}, {"YY", "%y"}, {"MM", "%m"}, {"DD", "%d"}, {"DY", "%a"}, {"HH", "%H"},
		{"MI", "%M"}, {"SS", "%S"}, {"AM", "%p"}, {"PM", "%p"}, {"FF", "%n"},
	}
	for i := 0; i < len(f); {
		if f[i] == '"' {
			j := strings.IndexByte(f[i+1:], '"')
			if j < 0 {
				return "", &Error{Code: CodeInvalidValue, Message: fmt.Sprintf("unterminated quoted text in format %q", f)}
			}
			b.WriteString(strings.ReplaceAll(f[i+1:i+1+j], "%", "%%"))
			i += j + 2
			continue
		}
		matched := false
		for _, e := range elements {
			if strings.HasPrefix(upper[i:], e.el) {
				if strings.HasPrefix(e.el, "FF") && len(upper) > i+2 && upper[i+2] >= '1' && upper[i+2] <= '9' &&
					e.el == "FF" {
					return "", unsupported(fmt.Sprintf("The format element %s", f[i:i+3]),
						"use FF3 (milliseconds), FF6 (microseconds) or FF9 (nanoseconds)")
				}
				b.WriteString(e.conv)
				i += len(e.el)
				matched = true
				break
			}
		}
		if matched {
			continue
		}
		if c := f[i]; c == '%' {
			b.WriteString("%%")
		} else {
			b.WriteByte(c)
		}
		i++
	}
	return b.String(), nil
}

// looksLikeDateFormat reports whether a TO_CHAR format formats dates
// rather than numbers.
func looksLikeDateFormat(f string) bool {
	u := strings.ToUpper(f)
	for _, el := range []string{"YY", "MM", "DD", "HH", "MI", "SS", "MON", "DY", "AM", "PM", "FF", "TZH"} {
		if strings.Contains(u, el) {
			return true
		}
	}
	return false
}

// numberFormat is a parsed Snowflake numeric format, such as '999,999.00'
// or 'FM$9,990.0', of the kinds Igloo supports: digits (9 and 0), group
// separators, a decimal point, an optional leading $ and the FM modifier.
type numberFormat struct {
	fm, dollar, group bool
	// intDigits is how many digits the integer part may have, minInt how
	// many it always shows (zeros included), fracDigits how many the
	// fraction has and minFrac how many of them always show.
	intDigits, minInt, fracDigits, minFrac int
	// width is the formatted width, a position for the sign included, and
	// text the format without FM.
	width int
	text  string
}

// parseNumberFormat parses f. ok is false for a format Igloo does not
// support.
func parseNumberFormat(f string) (numberFormat, bool) {
	var n numberFormat
	s := strings.ToUpper(strings.TrimSpace(f))
	if strings.HasPrefix(s, "FM") {
		n.fm, s = true, s[2:]
	}
	n.text = s
	if strings.HasPrefix(s, "$") {
		n.dollar, s = true, s[1:]
	}
	intPart, frac, hasPoint := strings.Cut(s, ".")
	if intPart == "" && !hasPoint {
		return n, false
	}
	zeroFrom := -1
	for _, c := range intPart {
		switch c {
		case '9':
			n.intDigits++
		case '0':
			n.intDigits++
			if zeroFrom < 0 {
				zeroFrom = n.intDigits - 1
			}
		case ',':
			n.group = true
		default:
			return n, false
		}
	}
	if zeroFrom >= 0 {
		n.minInt = n.intDigits - zeroFrom
	}
	// Snowflake shows a zero before the point however the format starts.
	n.minInt = max(n.minInt, 1)
	n.intDigits = max(n.intDigits, 1)
	for i, c := range frac {
		switch c {
		case '0':
			n.fracDigits++
			n.minFrac = i + 1
		case '9':
			n.fracDigits++
		default:
			return n, false
		}
	}
	// The sign takes a position of its own.
	n.width = len(n.text) + 1
	return n, true
}

// sql formats x, a SQL expression, as n does.
func (n numberFormat) sql(x string) string {
	r := fmt.Sprintf("round(CAST(%s AS DOUBLE), %d)", x, n.fracDigits)
	body := fmt.Sprintf("format('{:.%df}', abs(%s))", n.fracDigits, r)
	if n.group {
		body = fmt.Sprintf("format('{:,.%df}', abs(%s))", n.fracDigits, r)
	}
	// Zeros before the point up to minInt digits, and trailing zeros of the
	// fraction beyond minFrac written as spaces (or dropped with FM).
	intLen := fmt.Sprintf("strpos(%s || '.', '.') - 1", body)
	if n.minInt > 1 {
		body = fmt.Sprintf("repeat('0', greatest(%d - (%s), 0)) || %s", n.minInt, intLen, body)
	}
	if n.fracDigits > n.minFrac {
		trimmed := fmt.Sprintf("regexp_replace(%s, '(\\.\\d{%d}\\d*?)0+$', '\\1')", body, n.minFrac)
		if n.minFrac == 0 {
			trimmed = fmt.Sprintf("regexp_replace(regexp_replace(%s, '0+$', ''), '\\.$', '.')", body)
		}
		if n.fm {
			body = trimmed
		} else {
			body = fmt.Sprintf("rpad(%s, CAST(length(%s) AS INTEGER), ' ')", trimmed, body)
		}
	}
	if n.dollar {
		body = "'$' || " + body
	}
	signed := fmt.Sprintf("CASE WHEN %s < 0 THEN '-' ELSE '' END || %s", r, body)
	if !n.fm {
		signed = fmt.Sprintf("lpad(%s, %d, ' ')", signed, n.width)
	}
	// A number too large for the format shows as the format with # for
	// every digit, as in Snowflake.
	hashes := strings.Map(func(c rune) rune {
		if c == '9' || c == '0' {
			return '#'
		}
		return c
	}, n.text)
	if !n.fm {
		hashes = " " + hashes
	}
	limit := "1" + strings.Repeat("0", n.intDigits)
	return fmt.Sprintf("CASE WHEN %s IS NULL THEN NULL WHEN abs(%s) >= %s THEN %s ELSE %s END",
		x, r, limit, quoteString(hashes), signed)
}
