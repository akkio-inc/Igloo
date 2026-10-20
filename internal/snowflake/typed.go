package snowflake

import "regexp"

// selector describes a call of an aggregate or window function that
// returns one of the values of its argument: MIN, MAX, ANY_VALUE,
// FIRST_VALUE, LAST_VALUE, LAG and LEAD of one argument.
type selector struct {
	name     string
	arg      []*node
	distinct bool
	// tail is what follows the arguments: OVER (...), or nothing.
	tail []*node
}

// selectors are the functions a selector can be.
var selectors = map[string]bool{
	"ANY_VALUE": true, "FIRST_VALUE": true, "LAST_VALUE": true, "LAG": true, "LEAD": true, "ARBITRARY": true,
}

// untypable matches what stops DuckDB from choosing a typed macro's
// overload for an argument: an aggregate or window function in it (DuckDB
// 1.5 cannot tell their types when it chooses), or a parameter.
var untypable = regexp.MustCompile(`(?i)\b(min|max|sum|avg|count|count_if|any_value|arbitrary|arg_min|arg_max|min_by|max_by|` +
	`median|mode|first|last|first_value|last_value|lag|lead|nth_value|string_agg|listagg|list|array_agg|bool_and|bool_or|` +
	`quantile_cont|quantile_disc|percentile_cont|percentile_disc|approx_count_distinct|approx_quantile|row_number|rank|` +
	`dense_rank|percent_rank|cume_dist|ntile|stddev|stddev_pop|stddev_samp|variance|var_pop|var_samp|bit_and|bit_or|` +
	`bit_xor|histogram|json_group_array|json_group_object)\s*\(|\bOVER\b|\$\d+|\?`)

// typed calls a macro with overloads by type, built by build, on x, the
// argument it chooses its overload by. Where DuckDB cannot choose (see
// untypable), a selector takes the macro inside it, since MAX(F(x)) is
// F(MAX(x)) for a function that picks one value; anything else gets
// fallback, the same without the macro, which returns a TIMESTAMP where
// Snowflake's result would be a DATE.
func typed(x []*node, build, fallback func(x *node) *node) *node {
	x = trim(x)
	if !untypable.MatchString(render(x)) {
		return build(seq(x, x...))
	}
	if s := selectorOf(x); s != nil {
		parts := []*node{raw(s.name + "(")}
		if s.distinct {
			parts = append(parts, raw("DISTINCT "))
		}
		inner := typed(s.arg, build, fallback)
		parts = append(parts, inner, raw(")"))
		parts = append(parts, s.tail...)
		return seq(x, parts...)
	}
	return fallback(seq(x, x...))
}

// selectorOf returns the selector x is, or nil.
func selectorOf(x []*node) *selector {
	if len(x) == 1 && x[0].sel != nil {
		return x[0].sel
	}
	idx := sig(x)
	// A translated call, then its window.
	if len(idx) == 3 && x[idx[0]].sel != nil && x[idx[0]].sel.tail == nil && x[idx[1]].word() == "OVER" {
		s := *x[idx[0]].sel
		s.tail = []*node{raw(" OVER "), x[idx[2]]}
		return &s
	}
	if len(idx) < 2 || !selectors[x[idx[0]].word()] || !x[idx[1]].isGroup('(') {
		return nil
	}
	args := splitArgs(x[idx[1]].kids)
	if len(args) != 1 {
		return nil
	}
	s := &selector{name: x[idx[0]].tok.text, arg: trim(args[0])}
	switch len(idx) {
	case 2:
	case 4:
		if x[idx[2]].word() != "OVER" {
			return nil
		}
		s.tail = []*node{raw(" OVER "), x[idx[3]]}
	default:
		return nil
	}
	return s
}
