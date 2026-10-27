package models

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Frequency is a time series' step: a fixed duration, or a number of
// calendar months (months, quarters, years).
type Frequency struct {
	Micros int64  `json:"micros,omitempty"`
	Months int    `json:"months,omitempty"`
	Name   string `json:"name"`
}

const (
	microsPerSecond = int64(1_000_000)
	microsPerMinute = 60 * microsPerSecond
	microsPerHour   = 60 * microsPerMinute
	microsPerDay    = 24 * microsPerHour
	microsPerWeek   = 7 * microsPerDay
)

// ParseFrequency reads Snowflake's frequency, such as '1 day' or
// '15 minutes': a count and a unit, singular or plural, not abbreviated.
func ParseFrequency(s string) (Frequency, error) {
	fields := strings.Fields(strings.ToLower(strings.TrimSpace(s)))
	n := 1
	if len(fields) == 2 {
		v, err := strconv.Atoi(fields[0])
		if err != nil || v <= 0 {
			return Frequency{}, inputErr("frequency %q: the count must be a positive integer", s)
		}
		n, fields = v, fields[1:]
	}
	if len(fields) != 1 {
		return Frequency{}, inputErr("frequency %q: write a count and a unit, such as '1 day'", s)
	}
	unit := strings.TrimSuffix(fields[0], "s")
	f := Frequency{Name: strconv.Itoa(n) + " " + unit}
	if n > 1 {
		f.Name += "s"
	}
	switch unit {
	case "second":
		f.Micros = int64(n) * microsPerSecond
	case "minute":
		f.Micros = int64(n) * microsPerMinute
	case "hour":
		f.Micros = int64(n) * microsPerHour
	case "day":
		f.Micros = int64(n) * microsPerDay
	case "week":
		f.Micros = int64(n) * microsPerWeek
	case "month":
		f.Months = n
	case "quarter":
		f.Months = 3 * n
	case "year":
		f.Months = 12 * n
	default:
		return Frequency{}, inputErr("frequency %q: the unit is one of seconds, minutes, hours, days, weeks, months, quarters or years", s)
	}
	return f, nil
}

// inferFrequency is the most common gap between consecutive distinct
// timestamps of the series, read as calendar months when it is about
// one, three or twelve months.
func inferFrequency(series [][]int64) (Frequency, error) {
	counts := map[int64]int{}
	for _, ts := range series {
		for i := 1; i < len(ts); i++ {
			if d := ts[i] - ts[i-1]; d > 0 {
				counts[d]++
			}
		}
	}
	if len(counts) == 0 {
		return Frequency{}, inputErr("at least two unique timestamps are required")
	}
	var best int64
	for d, c := range counts {
		if c > counts[best] || c == counts[best] && d < best {
			best = d
		}
	}
	days := float64(best) / float64(microsPerDay)
	switch {
	case days >= 28 && days <= 31:
		return Frequency{Months: 1, Name: "1 month"}, nil
	case days >= 89 && days <= 92:
		return Frequency{Months: 3, Name: "1 quarter"}, nil
	case days >= 365 && days <= 366:
		return Frequency{Months: 12, Name: "1 year"}, nil
	}
	return Frequency{Micros: best, Name: describeMicros(best)}, nil
}

func describeMicros(m int64) string {
	for _, u := range []struct {
		micros int64
		name   string
	}{{microsPerWeek, "week"}, {microsPerDay, "day"}, {microsPerHour, "hour"}, {microsPerMinute, "minute"}, {microsPerSecond, "second"}} {
		if m%u.micros == 0 {
			n := m / u.micros
			if n == 1 {
				return "1 " + u.name
			}
			return strconv.FormatInt(n, 10) + " " + u.name + "s"
		}
	}
	return strconv.FormatInt(m, 10) + " microseconds"
}

func monthsOf(t time.Time) int { return t.Year()*12 + int(t.Month()) - 1 }

// index is the grid step of timestamp t from start: the step whose
// interval holds it.
func (f Frequency) index(start, t int64) int {
	if f.Months > 0 {
		a, b := time.UnixMicro(start).UTC(), time.UnixMicro(t).UTC()
		k := (monthsOf(b) - monthsOf(a)) / f.Months
		if monthsOf(b)-monthsOf(a) < 0 && (monthsOf(b)-monthsOf(a))%f.Months != 0 {
			k--
		}
		// Within the month, before start's day and time: the step before.
		if f.at(start, k) > t {
			k--
		}
		return k
	}
	d := t - start
	k := d / f.Micros
	if d < 0 && d%f.Micros != 0 {
		k--
	}
	return int(k)
}

// at is the timestamp of grid step k from start.
func (f Frequency) at(start int64, k int) int64 {
	if f.Months > 0 {
		return time.UnixMicro(start).UTC().AddDate(0, k*f.Months, 0).UnixMicro()
	}
	return start + int64(k)*f.Micros
}

// season is the number of steps in the series' main cycle: a day of hours,
// a week of days, a year of weeks or months; 0 without one.
func (f Frequency) season() int {
	switch {
	case f.Months == 1:
		return 12
	case f.Months == 3:
		return 4
	case f.Months > 0:
		return 0
	case f.Micros < microsPerDay && microsPerDay%f.Micros == 0:
		return int(min(microsPerDay/f.Micros, 288))
	case f.Micros == microsPerDay:
		return 7
	case f.Micros == microsPerWeek:
		return 52
	}
	return 0
}

// calendarFeatures are the cyclic features of a timestamp worth having at
// a frequency.
func (f Frequency) calendarFeatures() []string {
	switch {
	case f.Months == 1:
		return []string{"month"}
	case f.Months == 3:
		return []string{"quarter"}
	case f.Months > 0:
		return nil
	case f.Micros < microsPerDay:
		return []string{"hour_of_day", "day_of_week", "minute_of_hour"}
	case f.Micros < microsPerWeek:
		return []string{"day_of_week", "day_of_month", "month", "week_of_year"}
	case f.Micros < 28*microsPerDay:
		return []string{"week_of_year", "month"}
	}
	return []string{"month"}
}

func calendarValue(name string, micros int64) float64 {
	t := time.UnixMicro(micros).UTC()
	switch name {
	case "month":
		return float64(t.Month())
	case "quarter":
		return float64((int(t.Month())-1)/3 + 1)
	case "hour_of_day":
		return float64(t.Hour())
	case "minute_of_hour":
		return float64(t.Minute())
	case "day_of_week":
		return float64(t.Weekday())
	case "day_of_month":
		return float64(t.Day())
	case "week_of_year":
		_, w := t.ISOWeek()
		return float64(w)
	}
	return math.NaN()
}

// aggregate combines the values a grid step has more than one of, by one
// of Snowflake's aggregations.
func aggregate(how string, vals []float64) float64 {
	clean := vals[:0:0]
	for _, v := range vals {
		if !math.IsNaN(v) {
			clean = append(clean, v)
		}
	}
	if len(clean) == 0 {
		return math.NaN()
	}
	switch how {
	case "SUM":
		s := 0.0
		for _, v := range clean {
			s += v
		}
		return s
	case "MIN":
		return slices.Min(clean)
	case "MAX":
		return slices.Max(clean)
	case "FIRST":
		return clean[0]
	case "LAST":
		return clean[len(clean)-1]
	case "MEDIAN":
		return quantileOf(clean, 0.5)
	case "MODE":
		counts := map[float64]int{}
		best := clean[0]
		for _, v := range clean {
			counts[v]++
			if counts[v] > counts[best] || counts[v] == counts[best] && v < best {
				best = v
			}
		}
		return best
	}
	return mean(clean)
}

func aggregateText(how string, vals []string) (string, bool) {
	if len(vals) == 0 {
		return "", false
	}
	switch how {
	case "FIRST":
		return vals[0], true
	case "LAST":
		return vals[len(vals)-1], true
	}
	counts := map[string]int{}
	best := vals[0]
	for _, v := range vals {
		counts[v]++
		if counts[v] > counts[best] || counts[v] == counts[best] && v < best {
			best = v
		}
	}
	return best, true
}

func checkAggregation(name, how string, categorical bool) error {
	allowed := []string{"MEAN", "MEDIAN", "MODE", "MIN", "MAX", "SUM", "FIRST", "LAST"}
	if categorical {
		allowed = []string{"MODE", "FIRST", "LAST"}
	}
	if !slices.Contains(allowed, how) {
		return inputErr("%s: %q is not one of %s", name, how, strings.Join(allowed, ", "))
	}
	return nil
}

func mean(v []float64) float64 {
	if len(v) == 0 {
		return math.NaN()
	}
	s := 0.0
	for _, x := range v {
		s += x
	}
	return s / float64(len(v))
}

func quantileOf(v []float64, q float64) float64 {
	s := slices.Clone(v)
	slices.Sort(s)
	if len(s) == 0 {
		return math.NaN()
	}
	pos := q * float64(len(s)-1)
	lo := int(math.Floor(pos))
	hi := min(lo+1, len(s)-1)
	return s[lo] + (s[hi]-s[lo])*(pos-float64(lo))
}

// robustSigma is a standard deviation that outliers barely move: the
// median absolute deviation from zero, scaled to a normal's.
func robustSigma(res []float64) float64 {
	if len(res) == 0 {
		return 0
	}
	abs := make([]float64, len(res))
	for i, r := range res {
		abs[i] = math.Abs(r)
	}
	s := 1.4826 * quantileOf(abs, 0.5)
	if s == 0 {
		// Mostly exact: the root mean square of what is not.
		var sq float64
		for _, r := range res {
			sq += r * r
		}
		s = math.Sqrt(sq / float64(len(res)))
	}
	return s
}

func fmtSeries(key string, single bool) any {
	if single {
		return nil
	}
	return key
}

func errSeries(key string, err error) error {
	if key == "" {
		return err
	}
	return fmt.Errorf("series %s: %w", key, err)
}
