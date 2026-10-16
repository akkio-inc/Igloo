// Package schedule parses the schedules of tasks, pipes and materialized
// views, and works out when each runs next. A schedule is either an
// interval, such as '5 minutes', or a cron expression, such as '0 6 * * 1-5',
// read in a time zone.
package schedule

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	// Time zones must work in images without a zoneinfo database.
	_ "time/tzdata"
)

// MinInterval is the shortest interval a schedule may have.
const MinInterval = time.Second

// Schedule says when work runs.
type Schedule struct {
	// Every is the interval, for interval schedules.
	Every time.Duration
	cron  *cron
	loc   *time.Location
	text  string
}

// Parse reads spec, an interval or a cron expression, in the time zone tz
// (an IANA name such as America/New_York; empty means UTC).
//
// Intervals are a number and a unit: '30 seconds', '5 minutes', '1 hour',
// '1 day' (singular, plural and abbreviated units are accepted, as are Go
// durations such as '90s'). Cron expressions have five fields (minute,
// hour, day of month, month, day of week) with *, lists, ranges, steps and
// month and weekday names, or are one of @hourly, @daily, @weekly, @monthly
// and @yearly. Snowflake's 'USING CRON <expression> <time zone>' is
// accepted too.
func Parse(spec, tz string) (Schedule, error) {
	text := strings.Join(strings.Fields(spec), " ")
	if text == "" {
		return Schedule{}, errors.New("the schedule is empty")
	}
	if rest, ok := cutPrefixFold(text, "USING CRON "); ok {
		fields := strings.Fields(rest)
		if len(fields) == 6 {
			if tz != "" && !strings.EqualFold(tz, fields[5]) {
				return Schedule{}, fmt.Errorf("the schedule names time zone %s and TIMEZONE names %s", fields[5], tz)
			}
			tz, rest = fields[5], strings.Join(fields[:5], " ")
		}
		text = rest
	}
	loc, err := Location(tz)
	if err != nil {
		return Schedule{}, err
	}
	if d, ok, err := parseInterval(text); ok {
		if err != nil {
			return Schedule{}, err
		}
		return Schedule{Every: d, loc: loc, text: text}, nil
	}
	c, err := parseCron(text)
	if err != nil {
		return Schedule{}, err
	}
	return Schedule{cron: c, loc: loc, text: text}, nil
}

// Location loads the time zone tz; empty means UTC.
func Location(tz string) (*time.Location, error) {
	if tz == "" || strings.EqualFold(tz, "UTC") {
		return time.UTC, nil
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, fmt.Errorf("unknown time zone %q; use an IANA name such as America/New_York", tz)
	}
	return loc, nil
}

// String is the schedule as written, normalized.
func (s Schedule) String() string { return s.text }

// IsInterval reports whether the schedule is an interval.
func (s Schedule) IsInterval() bool { return s.cron == nil }

// First is when work on this schedule first runs, once it starts at now:
// one interval later, or the cron expression's next time.
func (s Schedule) First(now time.Time) time.Time {
	if s.cron == nil {
		return now.Add(s.Every)
	}
	return s.cron.next(now, s.loc)
}

// Next is the run after the one due at prev, at or after now: runs missed
// while nothing ran are skipped, not made up. Intervals keep to prev's
// grid.
func (s Schedule) Next(prev, now time.Time) time.Time {
	if s.cron == nil {
		next := prev.Add(s.Every)
		if !next.After(now) {
			missed := now.Sub(prev) / s.Every
			next = prev.Add((missed + 1) * s.Every)
		}
		return next
	}
	return s.cron.next(maxTime(prev, now), s.loc)
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func cutPrefixFold(s, prefix string) (string, bool) {
	if len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix) {
		return s[len(prefix):], true
	}
	return s, false
}

var units = map[string]time.Duration{
	"s": time.Second, "sec": time.Second, "secs": time.Second, "second": time.Second, "seconds": time.Second,
	"m": time.Minute, "min": time.Minute, "mins": time.Minute, "minute": time.Minute, "minutes": time.Minute,
	"h": time.Hour, "hr": time.Hour, "hrs": time.Hour, "hour": time.Hour, "hours": time.Hour,
	"d": 24 * time.Hour, "day": 24 * time.Hour, "days": 24 * time.Hour,
}

// parseInterval reads an interval. ok is false when text does not look
// like one, so it may be a cron expression.
func parseInterval(text string) (time.Duration, bool, error) {
	fields := strings.Fields(strings.ToLower(text))
	if len(fields) > 0 && fields[0] == "every" {
		fields = fields[1:]
	}
	var d time.Duration
	switch {
	case len(fields) == 2:
		n, err := strconv.ParseInt(fields[0], 10, 64)
		unit, known := units[fields[1]]
		if err != nil || !known {
			return 0, false, nil
		}
		if n <= 0 || n > int64(100*365*24*time.Hour/unit) {
			return 0, true, fmt.Errorf("invalid interval %q", text)
		}
		d = time.Duration(n) * unit
	case len(fields) == 1:
		var err error
		if d, err = time.ParseDuration(fields[0]); err != nil {
			return 0, false, nil
		}
	default:
		return 0, false, nil
	}
	if d < MinInterval {
		return 0, true, fmt.Errorf("the interval %q is shorter than %s", text, MinInterval)
	}
	return d, true, nil
}

// cron is a parsed five-field cron expression. Each field is the set of
// values that match.
type cron struct {
	minute, hour, dom, month, dow uint64
	// domStar and dowStar are set when the field is *: then the other
	// day field alone decides, and otherwise a day matches if either does.
	domStar, dowStar bool
}

var macros = map[string]string{
	"@yearly": "0 0 1 1 *", "@annually": "0 0 1 1 *", "@monthly": "0 0 1 * *",
	"@weekly": "0 0 * * 0", "@daily": "0 0 * * *", "@midnight": "0 0 * * *", "@hourly": "0 * * * *",
}

var (
	monthNames = map[string]int{"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
		"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}
	dayNames = map[string]int{"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6}
)

func parseCron(text string) (*cron, error) {
	if m, ok := macros[strings.ToLower(text)]; ok {
		text = m
	}
	fields := strings.Fields(text)
	if len(fields) != 5 {
		return nil, fmt.Errorf("invalid schedule %q: use an interval such as '5 minutes' or a cron expression "+
			"with five fields (minute hour day-of-month month day-of-week) such as '0 6 * * *'", text)
	}
	c := &cron{}
	var err error
	field := func(i, lo, hi int, names map[string]int) uint64 {
		if err != nil {
			return 0
		}
		var bits uint64
		bits, err = parseField(fields[i], lo, hi, names)
		return bits
	}
	c.minute = field(0, 0, 59, nil)
	c.hour = field(1, 0, 23, nil)
	c.dom = field(2, 1, 31, nil)
	c.month = field(3, 1, 12, monthNames)
	c.dow = field(4, 0, 7, dayNames)
	if err != nil {
		return nil, fmt.Errorf("invalid cron expression %q: %w", text, err)
	}
	// 7 is Sunday too.
	if c.dow&(1<<7) != 0 {
		c.dow |= 1
	}
	c.domStar, c.dowStar = fields[2] == "*" || fields[2] == "?", fields[4] == "*" || fields[4] == "?"
	return c, nil
}

// parseField reads one cron field: a comma-separated list of *, n, n-m,
// each optionally with /step.
func parseField(f string, lo, hi int, names map[string]int) (uint64, error) {
	var bits uint64
	for _, item := range strings.Split(f, ",") {
		rng, stepText, hasStep := strings.Cut(item, "/")
		step := 1
		if hasStep {
			n, err := strconv.Atoi(stepText)
			if err != nil || n <= 0 {
				return 0, fmt.Errorf("invalid step in %q", item)
			}
			step = n
		}
		start, end := lo, hi
		switch {
		case rng == "*" || rng == "?":
		case strings.Contains(rng, "-"):
			a, b, _ := strings.Cut(rng, "-")
			var err error
			if start, err = value(a, lo, hi, names); err != nil {
				return 0, err
			}
			if end, err = value(b, lo, hi, names); err != nil {
				return 0, err
			}
			if end < start {
				return 0, fmt.Errorf("range %q runs backwards", rng)
			}
		default:
			v, err := value(rng, lo, hi, names)
			if err != nil {
				return 0, err
			}
			start = v
			if !hasStep {
				end = v
			}
		}
		for v := start; v <= end; v += step {
			bits |= 1 << v
		}
	}
	return bits, nil
}

func value(s string, lo, hi int, names map[string]int) (int, error) {
	if v, ok := names[strings.ToLower(s)]; ok {
		return v, nil
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < lo || v > hi {
		return 0, fmt.Errorf("%q is not a value from %d to %d", s, lo, hi)
	}
	return v, nil
}

// next returns the first time after t, to the minute, that matches c in
// loc.
func (c *cron) next(t time.Time, loc *time.Location) time.Time {
	t = t.In(loc)
	t = t.Add(time.Minute - time.Duration(t.Second())*time.Second - time.Duration(t.Nanosecond()))
	// Five years covers every combination that can match at all, such as
	// February 29th on a Monday; an expression that never matches (31 in
	// February only) gives up there.
	limit := t.AddDate(5, 0, 0)
	for t.Before(limit) {
		switch {
		case c.month&(1<<uint(t.Month())) == 0:
			t = advance(t, time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, loc))
		case !c.dayMatches(t):
			t = advance(t, time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, loc))
		case c.hour&(1<<uint(t.Hour())) == 0:
			t = t.Add(time.Hour - time.Duration(t.Minute())*time.Minute)
		case c.minute&(1<<uint(t.Minute())) == 0:
			t = t.Add(time.Minute)
		default:
			return t
		}
	}
	return time.Time{}
}

// advance moves to next, or a minute on where daylight saving time makes
// next no later than t.
func advance(t, next time.Time) time.Time {
	if next.After(t) {
		return next
	}
	return t.Add(time.Minute)
}

func (c *cron) dayMatches(t time.Time) bool {
	dom := c.dom&(1<<uint(t.Day())) != 0
	dow := c.dow&(1<<uint(t.Weekday())) != 0
	switch {
	case c.domStar && c.dowStar:
		return true
	case c.domStar:
		return dow
	case c.dowStar:
		return dom
	}
	return dom || dow
}

// Never reports whether a time from Next or First means the schedule never
// matches again.
func Never(t time.Time) bool { return t.IsZero() }
