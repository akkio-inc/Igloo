package cleanroom

import (
	"fmt"
	"math"
	"slices"
	"strings"
)

// Policy is what templates may do with a dataset. Every column a template
// uses must be in it; the others are out of reach.
type Policy struct {
	// Join are the identifier columns, such as hashed emails and device
	// IDs: templates use them only to join datasets (a.id = b.id) and to
	// count them (count(DISTINCT a.id)). They never reach a result, a
	// filter or a GROUP BY, through a function or otherwise.
	Join []string `json:"join"`
	// Group are the dimensions, such as campaign, day or segment: results
	// may be grouped by them, show them, and filter on them.
	Group []string `json:"group,omitempty"`
	// Aggregates are the aggregates templates may compute of the
	// dataset's other columns, such as sum(spend).
	Aggregates []Aggregate `json:"aggregates,omitempty"`
	// Bounds are the range of a measure's value per person, by column. A
	// template that joins datasets of different parties may sum a measure
	// only within them: each person's total in a group is clamped to them,
	// and the sum's noise is scaled to them.
	Bounds map[string]Bound `json:"bounds,omitempty"`
	// Threshold is the fewest distinct identifiers of the dataset that a
	// result row must count. Rows with fewer are left out of the result.
	Threshold int `json:"threshold"`
	// MaxRows, when set, caps the rows of a result that reads the dataset.
	MaxRows int `json:"max_rows,omitempty"`
	// RunsPerDay caps how many runs each collaborator makes, in any 24
	// hours, of templates that read the dataset: 100 unless the policy
	// says otherwise, or 10 with noise.
	RunsPerDay int `json:"runs_per_day"`
	// Noise, when set, is the epsilon of the Laplace noise added to the
	// counts and the sums of results that read the dataset.
	Noise float64 `json:"noise,omitempty"`
}

// Aggregate is an aggregate a policy allows, such as sum(spend).
type Aggregate struct {
	Function string `json:"function"`
	Column   string `json:"column"`
}

func (a Aggregate) String() string { return a.Function + "(" + a.Column + ")" }

// Bound is the range of a measure's value per person: Lo to Hi.
type Bound struct {
	Lo float64 `json:"lo"`
	Hi float64 `json:"hi"`
}

const (
	// DefaultThreshold and MinThreshold bound Policy.Threshold.
	DefaultThreshold = 50
	MinThreshold     = 10
	// DefaultRunsPerDay is Policy.RunsPerDay unless a policy says
	// otherwise, and DefaultNoisyRunsPerDay with NOISE: averaging n runs'
	// noisy answers shrinks their noise by the square root of n.
	DefaultRunsPerDay      = 100
	DefaultNoisyRunsPerDay = 10
	// MaxResultRows caps every run's result.
	MaxResultRows = 10000
	// MaxNoise bounds Policy.Noise: a larger epsilon adds noise too small
	// to matter.
	MaxNoise = 10
	// MaxBound bounds the magnitude of BOUNDS, so a sum of clamped values,
	// as a DOUBLE, cannot overflow.
	MaxBound = 1e15
)

// AggregateFunctions are the aggregates a policy may allow on a column.
// count is allowed on every column of a policy without it.
var AggregateFunctions = []string{"sum", "avg", "min", "max", "median", "stddev"}

// Check validates p against columns, the source's column names, and fills
// in its defaults. Names are matched without regard to case and kept as
// the source spells them.
func (p *Policy) Check(columns []string) error {
	find := func(what, name string) (string, error) {
		i := slices.IndexFunc(columns, func(c string) bool { return strings.EqualFold(c, name) })
		if i < 0 {
			return "", invalid("%s column %q is not a column of the dataset (it has %s)", what, name, strings.Join(columns, ", "))
		}
		return columns[i], nil
	}
	if len(p.Join) == 0 {
		return invalid("a dataset needs at least one identifier column: JOIN ON (column, ...), such as a hashed email or device ID")
	}
	var err error
	for _, list := range []struct {
		what  string
		names []string
	}{{"JOIN ON", p.Join}, {"GROUP BY", p.Group}} {
		for i := range list.names {
			if list.names[i], err = find(list.what, list.names[i]); err != nil {
				return err
			}
			if slices.Contains(list.names[:i], list.names[i]) {
				return invalid("column %q is in %s twice", list.names[i], list.what)
			}
		}
	}
	for _, g := range p.Group {
		if slices.Contains(p.Join, g) {
			return invalid("column %q is an identifier (JOIN ON), so it cannot also be in GROUP BY", g)
		}
	}
	var aggs []Aggregate
	for _, a := range p.Aggregates {
		a.Function = strings.ToLower(a.Function)
		if !slices.Contains(AggregateFunctions, a.Function) {
			return invalid("AGGREGATE allows %s, not %s", strings.Join(AggregateFunctions, ", "), a.Function)
		}
		if a.Column, err = find("AGGREGATE", a.Column); err != nil {
			return err
		}
		if slices.Contains(p.Join, a.Column) {
			return invalid("column %q is an identifier (JOIN ON), so it can only be counted, not %s", a.Column, a)
		}
		if !slices.Contains(aggs, a) {
			aggs = append(aggs, a)
		}
	}
	p.Aggregates = aggs
	bounds := map[string]Bound{}
	for name, b := range p.Bounds {
		column, err := find("BOUNDS", name)
		if err != nil {
			return err
		}
		switch {
		case !slices.ContainsFunc(aggs, func(a Aggregate) bool { return a.Column == column }):
			return invalid("BOUNDS are for the columns of AGGREGATE; %q is not one", column)
		case math.IsNaN(b.Lo) || math.IsNaN(b.Hi) || b.Lo >= b.Hi:
			return invalid("BOUNDS (%s BETWEEN lo AND hi) takes two numbers, the lower first", column)
		case b.Lo > 0 || b.Hi < 0:
			// Otherwise each person adds at least lo (or at most hi), and
			// a sum is that times a count of people, under noise scaled
			// to hi - lo however large lo is.
			return invalid("BOUNDS (%s BETWEEN lo AND hi) must include 0: lo <= 0 <= hi", column)
		case -b.Lo > MaxBound || b.Hi > MaxBound:
			return invalid("BOUNDS take values from -%g to %g", float64(MaxBound), float64(MaxBound))
		}
		if _, dup := bounds[column]; dup {
			return invalid("column %q has BOUNDS twice", column)
		}
		bounds[column] = b
	}
	p.Bounds = nil
	if len(bounds) > 0 {
		p.Bounds = bounds
	}
	switch {
	case p.Threshold == 0:
		p.Threshold = DefaultThreshold
	case p.Threshold < MinThreshold:
		return invalid("THRESHOLD must be at least %d: smaller groups are too easy to tell people apart in", MinThreshold)
	}
	switch {
	case p.RunsPerDay == 0 && p.Noise > 0:
		p.RunsPerDay = DefaultNoisyRunsPerDay
	case p.RunsPerDay == 0:
		p.RunsPerDay = DefaultRunsPerDay
	case p.RunsPerDay < 0:
		return invalid("RUNS PER DAY must be positive")
	}
	if p.MaxRows < 0 || p.MaxRows > MaxResultRows {
		return invalid("MAX ROWS must be between 1 and %d", MaxResultRows)
	}
	if p.Noise < 0 || p.Noise > MaxNoise {
		return invalid("NOISE takes an epsilon greater than 0 and at most %d, such as NOISE 1", MaxNoise)
	}
	return nil
}

// columnRole is what a policy lets templates do with a column.
type columnRole int

const (
	roleNone columnRole = iota
	roleIdentifier
	roleDimension
	// roleMeasure columns are only aggregated, as the policy allows.
	roleMeasure
)

// role says what templates may do with column.
func (p Policy) role(column string) (columnRole, string) {
	is := func(c string) bool { return strings.EqualFold(c, column) }
	if i := slices.IndexFunc(p.Join, is); i >= 0 {
		return roleIdentifier, p.Join[i]
	}
	if i := slices.IndexFunc(p.Group, is); i >= 0 {
		return roleDimension, p.Group[i]
	}
	if i := slices.IndexFunc(p.Aggregates, func(a Aggregate) bool { return is(a.Column) }); i >= 0 {
		return roleMeasure, p.Aggregates[i].Column
	}
	return roleNone, column
}

// bound returns the bounds of the measure column, if the policy sets them.
func (p Policy) bound(column string) (Bound, bool) {
	for c, b := range p.Bounds {
		if strings.EqualFold(c, column) {
			return b, true
		}
	}
	return Bound{}, false
}

// allows reports whether the policy allows fn of the measure column.
func (p Policy) allows(fn, column string) bool {
	return slices.ContainsFunc(p.Aggregates, func(a Aggregate) bool {
		return a.Function == fn && strings.EqualFold(a.Column, column)
	})
}

// measureAggregates lists the aggregates allowed of column, for messages.
func (p Policy) measureAggregates(column string) string {
	var out []string
	for _, a := range p.Aggregates {
		if strings.EqualFold(a.Column, column) {
			out = append(out, a.String())
		}
	}
	return strings.Join(out, ", ")
}

// String writes the policy as the clauses of ADD DATASET.
func (p Policy) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "JOIN ON (%s)", strings.Join(p.Join, ", "))
	if len(p.Group) > 0 {
		fmt.Fprintf(&b, " GROUP BY (%s)", strings.Join(p.Group, ", "))
	}
	if len(p.Aggregates) > 0 {
		aggs := make([]string, len(p.Aggregates))
		for i, a := range p.Aggregates {
			aggs[i] = a.String()
		}
		fmt.Fprintf(&b, " AGGREGATE (%s)", strings.Join(aggs, ", "))
	}
	if len(p.Bounds) > 0 {
		var bounds []string
		for column, r := range p.Bounds {
			bounds = append(bounds, fmt.Sprintf("%s BETWEEN %g AND %g", column, r.Lo, r.Hi))
		}
		slices.Sort(bounds)
		fmt.Fprintf(&b, " BOUNDS (%s)", strings.Join(bounds, ", "))
	}
	fmt.Fprintf(&b, " THRESHOLD %d", p.Threshold)
	if p.MaxRows > 0 {
		fmt.Fprintf(&b, " MAX ROWS %d", p.MaxRows)
	}
	fmt.Fprintf(&b, " RUNS PER DAY %d", p.RunsPerDay)
	if p.Noise > 0 {
		fmt.Fprintf(&b, " NOISE %g", p.Noise)
	}
	return b.String()
}
