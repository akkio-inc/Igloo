package cleanroom

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Built-in templates are the analyses media agencies run with publishers
// and brands, written by Igloo from a few arguments that name the room's
// datasets and columns. Their SQL is checked like any template's, and
// collaborators approve it as it is; SHOW TEMPLATES shows it.
//
//	audience_overlap (left => d.id, right => d.id [, by => d.col] [, right_by => d.col] [, approximate => true])
//	reach_frequency (exposures => d.id, campaign => d.col, time => d.col [, audience => d.id] [, approximate => true])
//	conversion_lift (exposures => d.id, group => d.col, conversions => d.id [, campaign => d.col]
//	                 [, exposed => 'exposed'] [, control => 'control']
//	                 [, exposure_time => d.col, conversion_time => d.col, window_days => 7])

// Builtins names the built-in templates.
var Builtins = []string{"audience_overlap", "reach_frequency", "conversion_lift"}

// builtinArgs are a built-in's arguments: dataset.column names, or
// constants.
type builtinArgs struct {
	name string
	args map[string]string
	used []string
}

func (b *builtinArgs) column(key string, required bool) (dataset, column string, err error) {
	v, ok := b.args[key]
	b.used = append(b.used, key)
	if !ok {
		if required {
			return "", "", invalid("%s needs %s => dataset.column", b.name, key)
		}
		return "", "", nil
	}
	dataset, column, ok = strings.Cut(v, ".")
	if !ok || dataset == "" || column == "" || strings.Contains(column, ".") {
		return "", "", invalid("%s: %s takes dataset.column, not %q", b.name, key, v)
	}
	return dataset, column, nil
}

func (b *builtinArgs) constant(key, fallback string) string {
	b.used = append(b.used, key)
	if v, ok := b.args[key]; ok {
		return v
	}
	return fallback
}

func (b *builtinArgs) flag(key string) (bool, error) {
	v := b.constant(key, "false")
	f, err := strconv.ParseBool(v)
	if err != nil {
		return false, invalid("%s: %s takes true or false", b.name, key)
	}
	return f, nil
}

func (b *builtinArgs) done() error {
	for k := range b.args {
		if !slices.Contains(b.used, k) {
			return invalid("%s takes no argument %s", b.name, k)
		}
	}
	return nil
}

// literal writes s as an SQL string.
func literal(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// col writes alias.column.
func col(alias, column string) string { return quote(alias) + "." + quote(column) }

// Builtin writes the template name makes from args, and describes it.
func Builtin(name string, args map[string]string) (sql string, params []Parameter, describe string, err error) {
	b := &builtinArgs{name: name, args: args}
	var keys []string
	for k := range args {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + " => " + args[k]
	}
	describe = name + "(" + strings.Join(parts, ", ") + ")"
	switch name {
	case "audience_overlap":
		sql, err = audienceOverlap(b)
	case "reach_frequency":
		sql, err = reachFrequency(b)
		params = []Parameter{{Name: "start_date", Type: "DATE"}, {Name: "end_date", Type: "DATE"}}
	case "conversion_lift":
		sql, err = conversionLift(b)
	default:
		return "", nil, "", invalid("there is no built-in template %q; the built-ins are %s", name, strings.Join(Builtins, ", "))
	}
	if err == nil {
		err = b.done()
	}
	return sql, params, describe, err
}

// distinct counts a column's distinct values, exactly or with an HLL
// sketch.
func distinct(column string, approximate bool) string {
	if approximate {
		return "round(datasketch_hll_estimate(datasketch_hll(14, " + column + ")))"
	}
	return "count(DISTINCT " + column + ")"
}

// audienceOverlap: how much of one audience another has, by a dimension of
// the first; or, by dimensions of both, the people they have in common.
func audienceOverlap(b *builtinArgs) (string, error) {
	left, lid, err := b.column("left", true)
	if err != nil {
		return "", err
	}
	right, rid, err := b.column("right", true)
	if err != nil {
		return "", err
	}
	_, by, err := b.column("by", false)
	if err != nil {
		return "", err
	}
	_, rightBy, err := b.column("right_by", false)
	if err != nil {
		return "", err
	}
	approximate, err := b.flag("approximate")
	if err != nil {
		return "", err
	}
	l, r := col("l", lid), col("r", rid)
	var items, groups []string
	if by != "" {
		items, groups = append(items, col("l", by)+" AS "+quote(by)), append(groups, quote(by))
	}
	if rightBy != "" {
		name := rightBy
		if name == by {
			name = "right_" + name
		}
		items, groups = append(items, col("r", rightBy)+" AS "+quote(name)), append(groups, quote(name))
	}
	join := "LEFT JOIN"
	if rightBy != "" {
		// By the other side's dimensions there is only the overlap: the
		// first audience's other people have none.
		join = "JOIN"
		items = append(items, distinct(l, approximate)+" AS overlap")
	} else {
		items = append(items, distinct(l, approximate)+" AS audience", distinct(r, approximate)+" AS overlap",
			"round(100.0 * "+distinct(r, approximate)+" / "+distinct(l, approximate)+", 2) AS overlap_percent")
	}
	sql := "SELECT " + strings.Join(items, ", ") + "\nFROM " + quote(left) + " AS l " + join + " " + quote(right) + " AS r ON " + l + " = " + r
	if len(groups) > 0 {
		sql += "\nGROUP BY ALL ORDER BY " + strings.Join(groups, ", ")
	}
	return sql, nil
}

// reachFrequency: impressions, reach and frequency by campaign and day,
// between $start_date and $end_date. Totals over a campaign's flight are
// a template of their own: one with them would show, less the days shown,
// the days left out.
func reachFrequency(b *builtinArgs) (string, error) {
	exposures, id, err := b.column("exposures", true)
	if err != nil {
		return "", err
	}
	_, campaign, err := b.column("campaign", true)
	if err != nil {
		return "", err
	}
	_, at, err := b.column("time", true)
	if err != nil {
		return "", err
	}
	audience, aid, err := b.column("audience", false)
	if err != nil {
		return "", err
	}
	approximate, err := b.flag("approximate")
	if err != nil {
		return "", err
	}
	e := col("e", id)
	day := "CAST(" + col("e", at) + " AS DATE)"
	reach := distinct(e, approximate)
	sql := "SELECT " + col("e", campaign) + " AS campaign, " + day + " AS day, count(*) AS impressions, " + reach + " AS reach, " +
		"round(count(*) / " + reach + ", 2) AS frequency\nFROM " + quote(exposures) + " AS e"
	if audience != "" {
		sql += " JOIN " + quote(audience) + " AS a ON " + e + " = " + col("a", aid)
	}
	sql += "\nWHERE " + day + " BETWEEN $start_date AND $end_date" +
		"\nGROUP BY " + col("e", campaign) + ", " + day +
		"\nORDER BY campaign, day"
	return sql, nil
}

// conversionLift: of the people in an exposure log's exposed and control
// groups, how many converted (within a window after exposure, if one is
// given), and the lift of exposure.
func conversionLift(b *builtinArgs) (string, error) {
	exposures, eid, err := b.column("exposures", true)
	if err != nil {
		return "", err
	}
	_, group, err := b.column("group", true)
	if err != nil {
		return "", err
	}
	conversions, cid, err := b.column("conversions", true)
	if err != nil {
		return "", err
	}
	_, campaign, err := b.column("campaign", false)
	if err != nil {
		return "", err
	}
	_, exposedAt, err := b.column("exposure_time", false)
	if err != nil {
		return "", err
	}
	_, convertedAt, err := b.column("conversion_time", false)
	if err != nil {
		return "", err
	}
	window := b.constant("window_days", "")
	exposed, control := b.constant("exposed", "exposed"), b.constant("control", "control")
	timed := exposedAt != "" || convertedAt != "" || window != ""
	if timed && (exposedAt == "" || convertedAt == "" || window == "") {
		return "", invalid("conversion_lift takes exposure_time, conversion_time and window_days together")
	}
	days := 0
	if timed {
		if days, err = strconv.Atoi(window); err != nil || days < 1 || days > 366 {
			return "", invalid("conversion_lift: window_days takes a number of days from 1 to 366")
		}
	}
	e, c := col("e", eid), col("c", cid)
	in := func(label string) string { return " FILTER (WHERE " + col("e", group) + " = " + literal(label) + ")" }
	people := func(label string) string { return "count(DISTINCT " + e + ")" + in(label) }
	converters := func(label string) string { return "count(DISTINCT " + c + ")" + in(label) }
	rate := func(label string) string { return converters(label) + " / " + people(label) }
	var items []string
	if campaign != "" {
		items = append(items, col("e", campaign)+" AS campaign")
	}
	items = append(items,
		people(exposed)+" AS exposed", converters(exposed)+" AS exposed_converters",
		people(control)+" AS control", converters(control)+" AS control_converters",
		"round(100.0 * "+rate(exposed)+", 3) AS exposed_rate",
		"round(100.0 * "+rate(control)+", 3) AS control_rate",
		"round(100.0 * (("+rate(exposed)+") / nullif("+rate(control)+", 0) - 1), 2) AS lift_percent")
	sql := "SELECT " + strings.Join(items, ",\n  ") + "\nFROM " + quote(exposures) + " AS e LEFT JOIN " + quote(conversions) +
		" AS c ON " + e + " = " + c
	if timed {
		sql += fmt.Sprintf(" AND %s >= %s AND %s < %s + INTERVAL %d DAY", col("c", convertedAt), col("e", exposedAt),
			col("c", convertedAt), col("e", exposedAt), days)
	}
	if campaign != "" {
		sql += "\nGROUP BY " + col("e", campaign) + " ORDER BY campaign"
	}
	return sql, nil
}
