package models

import (
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/akkio-inc/igloo/internal/ml"
)

// FORECAST and ANOMALY_DETECTION, by Snowflake's "fast" method: one
// gradient-boosted model of every series, on lags and rolling means of the
// target, calendar features of the timestamp, the other columns (exogenous
// variables) and the series, forecasting a step at a time. Each series is
// scaled by its mean absolute value and, when it trends, has its linear
// trend taken out first, so the trees model what repeats and the trend
// carries on. Series of 2 to 11 points get a naive forecast, their last
// value; fewer than 60 training rows in all train a linear model rather
// than trees, as Snowflake's do.
//
// Prediction intervals come from cross-validation: the model trained on
// each series but its last steps forecasts them, and the spread of its
// errors, by how far ahead, sets the intervals. A point outside its
// interval is an anomaly.

// minPoints is the fewest points a series needs for a model rather than a
// naive forecast, and minTreeRows the fewest training rows for trees.
const (
	minPoints   = 12
	minTreeRows = 60
)

// endogenous names the features made of the target, which explanations
// report together, as Snowflake does.
const endogenous = "aggregated_endogenous_features"

// ForecastConfig is CONFIG_OBJECT of FORECAST and ANOMALY_DETECTION.
type ForecastConfig struct {
	Anomaly   bool
	Frequency string
	Evaluate  bool
	// NSplits, MaxTrainSize, TestSize and Gap are evaluation_config's, as
	// scikit-learn's TimeSeriesSplit takes them; PredictionInterval is
	// the interval its metrics use.
	NSplits, MaxTrainSize, TestSize, Gap int
	PredictionInterval                   float64
	LowerBound, UpperBound               *float64
	AggNumeric, AggCategorical           string
	AggTarget                            string
	AggColumn                            map[string]string
	SkipErrors                           bool
	Seed                                 uint64
	// Interrupt, if set, stops training when it returns an error.
	Interrupt func() error
}

// Forecaster is a trained FORECAST or ANOMALY_DETECTION model.
type Forecaster struct {
	TimeCol   string    `json:"time_col"`
	TargetCol string    `json:"target_col"`
	SeriesCol string    `json:"series_col,omitempty"`
	Freq      Frequency `json:"frequency"`
	Lags      []int     `json:"lags"`
	Windows   []int     `json:"windows,omitempty"`
	Calendar  []string  `json:"calendar,omitempty"`
	Exog      []Feature `json:"exog,omitempty"`
	ExogAgg   []string  `json:"exog_agg,omitempty"`
	AggTarget string    `json:"agg_target"`
	// Series are each series' state at the end of training.
	Series []*SeriesState `json:"series"`
	Model  *ml.GBM        `json:"model,omitempty"`
	Linear *ml.Linear     `json:"linear,omitempty"`
	// Sigma is the cross-validated error's spread, in a series' scale,
	// for forecasts 1, 2-3, 4-7, ... steps ahead.
	Sigma      []float64       `json:"sigma"`
	Lower      *float64        `json:"lower,omitempty"`
	Upper      *float64        `json:"upper,omitempty"`
	Metrics    []SeriesMetrics `json:"metrics,omitempty"`
	Importance []SeriesWeights `json:"importance,omitempty"`
	Logs       []SeriesLog     `json:"logs,omitempty"`
	// MetricsInterval is the prediction interval the metrics used.
	MetricsInterval float64 `json:"metrics_interval,omitempty"`

	interrupt func() error
}

// SeriesState is what forecasting a series needs.
type SeriesState struct {
	// Key is the series' value as text; "" for a model of one series.
	Key   string  `json:"key"`
	Scale float64 `json:"scale"`
	// A and B are the trend taken out: A + B*step, in the scaled target.
	A float64 `json:"a"`
	B float64 `json:"b"`
	// Start is the timestamp of step 0, N the steps of training.
	Start int64 `json:"start"`
	N     int   `json:"n"`
	// History holds the last steps' detrended, scaled values, the last
	// one step N-1's.
	History []float64 `json:"history"`
	// Naive series forecast their Last value, with NaiveSigma's spread.
	Naive      bool    `json:"naive,omitempty"`
	Last       float64 `json:"last"`
	NaiveSigma float64 `json:"naive_sigma,omitempty"`
	Points     int     `json:"points"`
}

// SeriesMetrics are a series' cross-validated errors.
type SeriesMetrics struct {
	Key    string             `json:"key"`
	Values map[string]float64 `json:"values"`
	StdDev map[string]float64 `json:"stddev,omitempty"`
}

// SeriesWeights are a series' feature importances.
type SeriesWeights struct {
	Key      string    `json:"key"`
	Features []string  `json:"features"`
	Types    []string  `json:"types"`
	Scores   []float64 `json:"scores"`
}

// SeriesLog is what training had to say of a series.
type SeriesLog struct {
	Key     string   `json:"key"`
	Message []string `json:"messages"`
}

func (f *Forecaster) single() bool { return f.SeriesCol == "" }

func (f *Forecaster) prepare() {
	if f.Model != nil {
		f.Model.Prepare()
	}
}

func (f *Forecaster) maxLag() int {
	m := 0
	for _, l := range f.Lags {
		m = max(m, l)
	}
	for _, w := range f.Windows {
		m = max(m, w)
	}
	return m
}

// series is a series' data on its grid of steps.
type series struct {
	key   string
	start int64
	// y holds the target of each step, NaN where there is none; known says
	// which steps train the model (not a labeled or found anomaly).
	y     []float64
	known []bool
	// exog holds each exogenous column's value at each step: numbers in
	// num, categories in cat (ok false where missing).
	num   [][]float64
	cat   [][]string
	catOK [][]bool
	st    *SeriesState
	// r is the scaled, detrended target, with missing steps filled in.
	r []float64
	// rowAt says which steps the input had a row at.
	rowAt []bool
}

// seriesKey is a row's series as text.
func seriesKey(c *TableCol, i int) (string, bool) {
	if c == nil {
		return "", true
	}
	if c.Null[i] {
		return "", false
	}
	switch c.Type {
	case TypeNumber:
		return strconv.FormatFloat(c.Num[i], 'f', -1, 64), true
	case TypeTime:
		return c.Time(i).Format("2006-01-02 15:04:05.999999"), true
	}
	return c.Str[i], true
}

// grid lays rows of t out on series' grids of steps: aggregated per step.
type gridInput struct {
	t                          *Table
	ts, target, seriesC, label *TableCol
	exog                       []*TableCol
}

func newGridInput(t *Table, timeCol, targetCol, seriesCol, labelCol string, exogNames []string) (*gridInput, error) {
	g := &gridInput{t: t}
	if g.ts = t.Col(timeCol); g.ts == nil {
		return nil, inputErr("TIMESTAMP_COLNAME %s is not a column of the input", timeCol)
	}
	if g.ts.Type != TypeTime {
		return nil, inputErr("the timestamp %s is of type %s; it must be a timestamp or date", g.ts.Name, g.ts.TypeName)
	}
	if targetCol != "" {
		if g.target = t.Col(targetCol); g.target == nil {
			return nil, inputErr("TARGET_COLNAME %s is not a column of the input", targetCol)
		}
		if g.target.Type != TypeNumber {
			return nil, inputErr("the target %s is of type %s; it must be a number", g.target.Name, g.target.TypeName)
		}
	}
	if seriesCol != "" {
		if g.seriesC = t.Col(seriesCol); g.seriesC == nil {
			return nil, inputErr("SERIES_COLNAME %s is not a column of the input", seriesCol)
		}
	}
	if labelCol != "" {
		if g.label = t.Col(labelCol); g.label == nil {
			return nil, inputErr("LABEL_COLNAME %s is not a column of the input", labelCol)
		}
		if g.label.Type != TypeBool {
			return nil, inputErr("the label %s is of type %s; it must be a boolean", g.label.Name, g.label.TypeName)
		}
	}
	for _, name := range exogNames {
		c := t.Col(name)
		if c == nil {
			return nil, inputErr("the model was trained with the column %s, which the input lacks", name)
		}
		if c.Type == TypeOther {
			return nil, inputErr("column %s is of type %s; exogenous columns are numbers, strings, booleans or timestamps", c.Name, c.TypeName)
		}
		g.exog = append(g.exog, c)
	}
	return g, nil
}

// byseries groups row numbers by series, each group's in time order, and
// returns the keys in order.
func (g *gridInput) bySeries() ([]string, map[string][]int, int) {
	groups := map[string][]int{}
	skipped := 0
	for i := range g.t.N {
		if g.ts.Null[i] {
			skipped++
			continue
		}
		k, ok := seriesKey(g.seriesC, i)
		if !ok {
			skipped++
			continue
		}
		groups[k] = append(groups[k], i)
	}
	keys := make([]string, 0, len(groups))
	for k, rows := range groups {
		keys = append(keys, k)
		sort.SliceStable(rows, func(a, b int) bool { return g.ts.Micros[rows[a]] < g.ts.Micros[rows[b]] })
	}
	slices.Sort(keys)
	return keys, groups, skipped
}

func exogNumeric(c *TableCol) bool { return c.Type == TypeNumber || c.Type == TypeTime }

func exogNum(c *TableCol, i int) float64 {
	if c.Null[i] {
		return math.NaN()
	}
	if c.Type == TypeTime {
		return float64(c.Micros[i]) / 1e6
	}
	return c.Num[i]
}

// layout aggregates a series' rows onto its grid from start: steps up to
// the last row's.
func (g *gridInput) layout(key string, rows []int, freq Frequency, start int64, aggTarget string, exogAgg []string) (*series, error) {
	s := &series{key: key, start: start}
	if len(rows) == 0 {
		return s, nil
	}
	n := freq.index(start, g.ts.Micros[rows[len(rows)-1]]) + 1
	if n > 20*len(rows)+1000 {
		return nil, inputErr("the timestamps of series %s are too sparse for a step of %s; set the frequency in CONFIG_OBJECT", key, freq.Name)
	}
	if n <= 0 {
		return nil, inputErr("series %s has timestamps before its start", key)
	}
	byStep := map[int][]int{}
	for _, i := range rows {
		k := freq.index(start, g.ts.Micros[i])
		if k < 0 {
			return nil, inputErr("series %s has timestamps before %s", key, time.UnixMicro(start).UTC().Format(time.DateTime))
		}
		byStep[k] = append(byStep[k], i)
	}
	s.y, s.known, s.rowAt = make([]float64, n), make([]bool, n), make([]bool, n)
	for k := range byStep {
		s.rowAt[k] = true
	}
	s.num, s.cat, s.catOK = make([][]float64, len(g.exog)), make([][]string, len(g.exog)), make([][]bool, len(g.exog))
	for j, c := range g.exog {
		if exogNumeric(c) {
			s.num[j] = make([]float64, n)
		} else {
			s.cat[j], s.catOK[j] = make([]string, n), make([]bool, n)
		}
	}
	for k := range n {
		rs := byStep[k]
		s.y[k] = math.NaN()
		if g.target != nil {
			var vals []float64
			for _, i := range rs {
				if g.label != nil && !g.label.Null[i] && g.label.Str[i] == "true" {
					continue // a known anomaly does not train
				}
				vals = append(vals, g.target.Num[i])
			}
			s.y[k] = aggregate(aggTarget, vals)
			s.known[k] = !math.IsNaN(s.y[k])
		}
		for j, c := range g.exog {
			if s.num[j] != nil {
				vals := make([]float64, 0, len(rs))
				for _, i := range rs {
					vals = append(vals, exogNum(c, i))
				}
				s.num[j][k] = aggregate(exogAgg[j], vals)
				continue
			}
			var vals []string
			for _, i := range rs {
				if !c.Null[i] {
					vals = append(vals, c.Str[i])
				}
			}
			s.cat[j][k], s.catOK[j][k] = aggregateText(exogAgg[j], vals)
		}
	}
	return s, nil
}

// TrainForecaster trains a FORECAST or ANOMALY_DETECTION model.
func TrainForecaster(t *Table, timeCol, targetCol, seriesCol, labelCol string, cfg ForecastConfig) (*Forecaster, error) {
	f := &Forecaster{AggTarget: strings.ToUpper(cfg.AggTarget), interrupt: cfg.Interrupt}
	if f.AggTarget == "" {
		f.AggTarget = strings.ToUpper(cfg.AggNumeric)
	}
	if f.AggTarget == "" {
		f.AggTarget = "MEAN"
	}
	if err := checkAggregation("aggregation_target", f.AggTarget, false); err != nil {
		return nil, err
	}
	var exogNames []string
	for _, c := range t.Cols {
		if !slices.ContainsFunc([]string{timeCol, targetCol, seriesCol, labelCol}, func(n string) bool { return n != "" && strings.EqualFold(n, c.Name) }) {
			exogNames = append(exogNames, c.Name)
		}
	}
	g, err := newGridInput(t, timeCol, targetCol, seriesCol, labelCol, exogNames)
	if err != nil {
		return nil, err
	}
	if g.target == nil {
		return nil, inputErr("TARGET_COLNAME is required")
	}
	f.TimeCol, f.TargetCol = g.ts.Name, g.target.Name
	if g.seriesC != nil {
		f.SeriesCol = g.seriesC.Name
	}
	for _, c := range g.exog {
		how := strings.ToUpper(cfg.AggNumeric)
		kind := TypeNumber.String()
		if !exogNumeric(c) {
			how, kind = strings.ToUpper(cfg.AggCategorical), TypeString.String()
		}
		if v, ok := cfg.AggColumn[strings.ToLower(c.Name)]; ok {
			how = strings.ToUpper(v)
		}
		if how == "" {
			how = "MEAN"
			if !exogNumeric(c) {
				how = "MODE"
			}
		}
		if err := checkAggregation("the aggregation of "+c.Name, how, !exogNumeric(c)); err != nil {
			return nil, err
		}
		f.Exog = append(f.Exog, Feature{Name: c.Name, Type: kind})
		f.ExogAgg = append(f.ExogAgg, how)
	}
	keys, groups, skipped := g.bySeries()
	if len(keys) == 0 {
		return nil, inputErr("the input has no rows with a timestamp")
	}
	if skipped > 0 {
		f.Logs = append(f.Logs, SeriesLog{Message: []string{strconv.Itoa(skipped) + " rows with a NULL timestamp or series were left out"}})
	}
	if cfg.Frequency != "" {
		if f.Freq, err = ParseFrequency(cfg.Frequency); err != nil {
			return nil, err
		}
	} else {
		var stamps [][]int64
		for _, k := range keys {
			var ts []int64
			for _, i := range groups[k] {
				if len(ts) == 0 || ts[len(ts)-1] != g.ts.Micros[i] {
					ts = append(ts, g.ts.Micros[i])
				}
			}
			stamps = append(stamps, ts)
		}
		if f.Freq, err = inferFrequency(stamps); err != nil {
			return nil, err
		}
	}
	f.Lower, f.Upper = cfg.LowerBound, cfg.UpperBound
	var all []*series
	for _, k := range keys {
		rows := groups[k]
		s, err := g.layout(k, rows, f.Freq, g.ts.Micros[rows[0]], f.AggTarget, f.ExogAgg)
		if err != nil {
			return nil, err
		}
		points := 0
		for _, ok := range s.known {
			if ok {
				points++
			}
		}
		if points < 2 {
			err := errSeries(k, inputErr("at least two unique timestamps are required"))
			if cfg.SkipErrors && !f.single() {
				f.Logs = append(f.Logs, SeriesLog{Key: k, Message: []string{err.Error()}})
				continue
			}
			return nil, err
		}
		s.st = &SeriesState{Key: k, Start: s.start, N: len(s.y), Points: points}
		if f.single() {
			s.st.Key = ""
		}
		all = append(all, s)
	}
	if len(all) == 0 {
		return nil, inputErr("no series has two points to train on")
	}
	f.chooseFeatures(all)
	for _, s := range all {
		f.normalize(s)
	}
	if err := f.fit(all, cfg); err != nil {
		return nil, err
	}
	if cfg.Anomaly {
		// Anomalies in the training data are not the series: points far
		// from the first fit are left out of a second.
		if f.dropOutliers(all) {
			for _, s := range all {
				f.normalize(s)
			}
			if err := f.fit(all, cfg); err != nil {
				return nil, err
			}
		}
	}
	if err := f.crossValidate(all, cfg); err != nil {
		return nil, err
	}
	for _, s := range all {
		f.finish(s)
		f.Series = append(f.Series, s.st)
	}
	f.importance(all)
	return f, nil
}

// chooseFeatures picks lags, rolling windows and calendar features for the
// series' frequency and lengths.
func (f *Forecaster) chooseFeatures(all []*series) {
	shortest := math.MaxInt
	for _, s := range all {
		if s.st.Points >= minPoints {
			shortest = min(shortest, len(s.y))
		}
	}
	if shortest == math.MaxInt {
		shortest = 0
	}
	limit := max(shortest/3, 1)
	add := func(l int) {
		if l >= 1 && l <= limit && !slices.Contains(f.Lags, l) {
			f.Lags = append(f.Lags, l)
		}
	}
	for _, l := range []int{1, 2, 3} {
		add(l)
	}
	if m := f.Freq.season(); m > 3 {
		add(m - 1)
		add(m)
		add(m + 1)
		add(2 * m)
	}
	if len(f.Lags) == 0 {
		f.Lags = []int{1}
	}
	slices.Sort(f.Lags)
	for _, w := range []int{3, f.Freq.season()} {
		if w >= 2 && w <= limit && !slices.Contains(f.Windows, w) {
			f.Windows = append(f.Windows, w)
		}
	}
	f.Calendar = f.Freq.calendarFeatures()
}

// normalize scales a series, takes out its trend if it has one, and
// fills in the steps it lacks.
func (f *Forecaster) normalize(s *series) {
	st := s.st
	var obs []float64
	var ks []float64
	for k, ok := range s.known {
		if ok {
			obs = append(obs, s.y[k])
			ks = append(ks, float64(k))
		}
	}
	st.Points = len(obs)
	st.Last = obs[len(obs)-1]
	var absSum float64
	for _, v := range obs {
		absSum += math.Abs(v)
	}
	st.Scale = absSum / float64(len(obs))
	if st.Scale < 1e-12 {
		st.Scale = 1
	}
	st.Naive = len(obs) < minPoints
	if st.Naive {
		var diffs []float64
		for i := 1; i < len(obs); i++ {
			diffs = append(diffs, obs[i]-obs[i-1])
		}
		st.NaiveSigma = robustSigma(diffs)
	}
	// The trend: least squares of the scaled target on the step, kept when
	// it moves the level over the series by more than half the spread
	// around it.
	z := make([]float64, len(obs))
	for i, v := range obs {
		z[i] = v / st.Scale
	}
	mk, mz := mean(ks), mean(z)
	var sxy, sxx float64
	for i := range z {
		sxy += (ks[i] - mk) * (z[i] - mz)
		sxx += (ks[i] - mk) * (ks[i] - mk)
	}
	st.A, st.B = mz, 0
	if sxx > 0 && len(z) >= minPoints {
		b := sxy / sxx
		var res []float64
		for i := range z {
			res = append(res, z[i]-(mz+b*(ks[i]-mk)))
		}
		var sq float64
		for _, r := range res {
			sq += r * r
		}
		spread := math.Sqrt(sq / float64(len(res)))
		if math.Abs(b)*float64(len(s.y)) > 0.5*spread {
			st.A, st.B = mz-b*mk, b
		}
	}
	// Detrended values, steps without a target filled in from their
	// neighbors.
	n := len(s.y)
	s.r = make([]float64, n)
	last := -1
	for k := range n {
		if !s.known[k] {
			continue
		}
		s.r[k] = s.y[k]/st.Scale - (st.A + st.B*float64(k))
		if last >= 0 && k-last > 1 {
			for m := last + 1; m < k; m++ {
				w := float64(m-last) / float64(k-last)
				s.r[m] = s.r[last]*(1-w) + s.r[k]*w
			}
		}
		if last < 0 {
			for m := 0; m < k; m++ {
				s.r[m] = s.r[k]
			}
		}
		last = k
	}
	for m := last + 1; m < n; m++ {
		s.r[m] = s.r[last]
	}
}

// featureNames are the model's features, in order.
func (f *Forecaster) featureNames() ([]string, []ml.Kind) {
	var names []string
	var kinds []ml.Kind
	for _, l := range f.Lags {
		names, kinds = append(names, "lag_"+strconv.Itoa(l)), append(kinds, ml.Numeric)
	}
	for _, w := range f.Windows {
		names, kinds = append(names, "rolling_mean_"+strconv.Itoa(w)), append(kinds, ml.Numeric)
	}
	for _, c := range f.Calendar {
		names, kinds = append(names, c), append(kinds, ml.Numeric)
	}
	for _, e := range f.Exog {
		k := ml.Numeric
		if e.Type != TypeNumber.String() {
			k = ml.Categorical
		}
		names, kinds = append(names, e.Name), append(kinds, k)
	}
	if !f.single() {
		names, kinds = append(names, "__series"), append(kinds, ml.Categorical)
	}
	return names, kinds
}

// row holds one step's features: numbers, or categories (ok false where
// missing).
type row struct {
	num []float64
	cat []string
	ok  []bool
}

func newRow(n int) *row {
	return &row{num: make([]float64, n), cat: make([]string, n), ok: make([]bool, n)}
}

// exogAt gives a step's exogenous values.
type exogAt func(j, k int) (float64, string, bool)

// fill writes step k's features; rAt gives the detrended target at a step
// (NaN before the first).
func (f *Forecaster) fill(r *row, key string, start int64, k int, rAt func(int) float64, exog exogAt) {
	j := 0
	for _, l := range f.Lags {
		r.num[j] = rAt(k - l)
		j++
	}
	for _, w := range f.Windows {
		var sum float64
		n := 0
		for i := k - w; i < k; i++ {
			if v := rAt(i); !math.IsNaN(v) {
				sum += v
				n++
			}
		}
		r.num[j] = math.NaN()
		if n > 0 {
			r.num[j] = sum / float64(n)
		}
		j++
	}
	ts := f.Freq.at(start, k)
	for _, c := range f.Calendar {
		r.num[j] = calendarValue(c, ts)
		j++
	}
	for e := range f.Exog {
		num, cat, ok := exog(e, k)
		r.num[j], r.cat[j], r.ok[j] = num, cat, ok
		j++
	}
	if !f.single() {
		r.cat[j], r.ok[j] = key, true
	}
}

func seriesExog(s *series) exogAt {
	return func(j, k int) (float64, string, bool) {
		if k >= len(s.y) {
			return math.NaN(), "", false
		}
		if s.num[j] != nil {
			return s.num[j][k], "", true
		}
		return math.NaN(), s.cat[j][k], s.catOK[j][k]
	}
}

func rFrom(r []float64) func(int) float64 {
	return func(i int) float64 {
		if i < 0 || i >= len(r) {
			return math.NaN()
		}
		return r[i]
	}
}

// trainingFrame builds the training rows of the series' steps in
// [from(s), to(s)) that have a target.
func (f *Forecaster) trainingFrame(all []*series, from, to func(*series) int) (*ml.Frame, []float64, []int) {
	names, kinds := f.featureNames()
	builders := make([]*ml.Builder, len(names))
	for j := range names {
		builders[j] = ml.NewBuilder(names[j], kinds[j])
	}
	var y []float64
	var owner []int
	r := newRow(len(names))
	for si, s := range all {
		if s.st.Naive {
			continue
		}
		rAt := rFrom(s.r)
		ex := seriesExog(s)
		for k := max(from(s), 1); k < to(s); k++ {
			if !s.known[k] {
				continue
			}
			f.fill(r, s.st.Key, s.start, k, rAt, ex)
			for j, b := range builders {
				if kinds[j] == ml.Numeric {
					b.AddNum(r.num[j])
				} else if r.ok[j] {
					b.AddString(r.cat[j])
				} else {
					b.AddMissing()
				}
			}
			y = append(y, s.r[k])
			owner = append(owner, si)
		}
	}
	fr := &ml.Frame{N: len(y)}
	for _, b := range builders {
		fr.Cols = append(fr.Cols, b.Column())
	}
	return fr, y, owner
}

// model is a trained model of the detrended target: trees or linear.
type model struct {
	gbm    *ml.GBM
	linear *ml.Linear
}

// train fits a model on frame rows, trees when there are enough of them.
func (f *Forecaster) train(fr *ml.Frame, y []float64, owner []int, all []*series, seed uint64) (model, error) {
	if fr.N < minTreeRows {
		l, err := ml.FitLinear(fr, y, false, 0)
		return model{linear: l}, err
	}
	// Early stopping on each series' last tenth.
	count := map[int]int{}
	for _, o := range owner {
		count[o]++
	}
	var keep, held []int
	seen := map[int]int{}
	for i, o := range owner {
		seen[o]++
		if count[o] >= 20 && seen[o] > count[o]-max(count[o]/10, 1) {
			held = append(held, i)
		} else {
			keep = append(keep, i)
		}
	}
	p := ml.Params{Objective: ml.Regression, Rounds: 500, LearningRate: 0.05, NumLeaves: 31, Seed: seed,
		Interrupt: f.interrupt}
	if len(held) >= 10 {
		p.EarlyStopping = 30
		tf, ty := subset(fr, y, keep)
		vf, vy := subset(fr, y, held)
		_, info, err := ml.TrainGBM(tf, ty, vf, vy, p)
		if err != nil {
			return model{}, err
		}
		p.Rounds, p.EarlyStopping = max(info.Rounds, 10), 0
	} else {
		p.Rounds = 200
	}
	m, _, err := ml.TrainGBM(fr, y, nil, nil, p)
	return model{gbm: m}, err
}

// fit trains the model on every series' steps.
func (f *Forecaster) fit(all []*series, cfg ForecastConfig) error {
	fr, y, owner := f.trainingFrame(all, func(*series) int { return 0 }, func(s *series) int { return len(s.y) })
	if fr.N == 0 {
		f.Model, f.Linear = nil, nil
		return nil
	}
	m, err := f.train(fr, y, owner, all, cfg.Seed)
	if err != nil {
		return err
	}
	f.Model, f.Linear = m.gbm, m.linear
	return nil
}

// predict returns the model's detrended forecast for a row of features.
func (m model) predict(f *Forecaster, r *row) float64 {
	switch {
	case m.gbm != nil:
		bins := make([]uint8, len(m.gbm.Features))
		for j, b := range m.gbm.Features {
			if b.Kind == ml.Numeric {
				bins[j] = b.NumBin(r.num[j])
			} else if r.ok[j] {
				bins[j] = b.CatBin(r.cat[j])
			} else {
				bins[j] = ml.MissingBin
			}
		}
		out := make([]float64, 1)
		m.gbm.RawRow(bins, out)
		return out[0]
	case m.linear != nil:
		names, kinds := f.featureNames()
		fr := &ml.Frame{N: 1}
		for j, name := range names {
			if kinds[j] == ml.Numeric {
				fr.Cols = append(fr.Cols, ml.NumericColumn(name, []float64{r.num[j]}))
			} else {
				fr.Cols = append(fr.Cols, ml.CategoricalColumn(name, []string{r.cat[j]}, []bool{!r.ok[j]}))
			}
		}
		return m.linear.Predict(fr)[0]
	}
	return 0
}

func (f *Forecaster) current() model { return model{gbm: f.Model, linear: f.Linear} }

// path forecasts a series' detrended target from step from (its history
// up to there being hist) through step to, and returns the forecasts of
// steps from..to.
func (f *Forecaster) path(m model, st *SeriesState, hist []float64, histEnd, from, to int, ex exogAt) []float64 {
	r := newRow(len(f.featureNamesCache()))
	known := slices.Clone(hist)
	base := histEnd - len(hist) // the step of known[0]
	rAt := func(i int) float64 {
		if i < base || i-base >= len(known) {
			return math.NaN()
		}
		return known[i-base]
	}
	out := make([]float64, 0, to-from+1)
	for k := histEnd; k <= to; k++ {
		f.fill(r, st.Key, st.Start, k, rAt, ex)
		v := m.predict(f, r)
		known = append(known, v)
		if k >= from {
			out = append(out, v)
		}
	}
	return out
}

func (f *Forecaster) featureNamesCache() []string {
	names, _ := f.featureNames()
	return names
}

// level turns a detrended forecast of step k into the target's units,
// within the bounds.
func (f *Forecaster) level(st *SeriesState, k int, r float64) float64 {
	v := st.Scale * (st.A + st.B*float64(k) + r)
	if f.Lower != nil && v < *f.Lower {
		v = *f.Lower
	}
	if f.Upper != nil && v > *f.Upper {
		v = *f.Upper
	}
	return v
}

// horizonBucket groups forecasts by how far ahead: 1, 2-3, 4-7, ...
func horizonBucket(h int) int { return bits(h) }

func bits(h int) int {
	b := 0
	for h > 1 {
		h >>= 1
		b++
	}
	return b
}

// sigma is the spread of a forecast h steps ahead, in a series' scale.
func (f *Forecaster) sigma(h int) float64 {
	if len(f.Sigma) == 0 {
		return 0
	}
	b := horizonBucket(h)
	if b < len(f.Sigma) {
		return f.Sigma[b]
	}
	last := len(f.Sigma) - 1
	return f.Sigma[last] * math.Sqrt(float64(h)/float64(int(1)<<last))
}

// dropOutliers marks the steps far from the model's in-sample fit as not
// training, and reports whether it marked any.
func (f *Forecaster) dropOutliers(all []*series) bool {
	m := f.current()
	if m.gbm == nil && m.linear == nil {
		return false
	}
	type miss struct {
		s *series
		k int
		e float64
	}
	var errs []miss
	var res []float64
	r := newRow(len(f.featureNamesCache()))
	for _, s := range all {
		if s.st.Naive {
			continue
		}
		rAt := rFrom(s.r)
		for k := 1; k < len(s.y); k++ {
			if !s.known[k] {
				continue
			}
			f.fill(r, s.st.Key, s.start, k, rAt, seriesExog(s))
			e := s.r[k] - m.predict(f, r)
			errs = append(errs, miss{s, k, e})
			res = append(res, e)
		}
	}
	sd := robustSigma(res)
	if sd == 0 {
		return false
	}
	dropped := false
	for _, e := range errs {
		if math.Abs(e.e) > 5*sd {
			e.s.known[e.k] = false
			dropped = true
		}
	}
	return dropped
}

// crossValidate measures the model on each series' last steps, as
// scikit-learn's TimeSeriesSplit splits them, training a model on what
// comes before each split, and sets the intervals from its errors; the
// metrics are kept when evaluating.
func (f *Forecaster) crossValidate(all []*series, cfg ForecastConfig) error {
	splits := max(cfg.NSplits, 1)
	interval := cfg.PredictionInterval
	if interval <= 0 || interval >= 1 {
		interval = 0.95
	}
	f.MetricsInterval = interval
	type fold struct{ cut, end int }
	// folds[s][i] is series s's split i, or cut 0 when it is too short.
	folds := map[*series][]fold{}
	for _, s := range all {
		if s.st.Naive {
			continue
		}
		n := len(s.y)
		test := cfg.TestSize
		if test <= 0 {
			test = n / (splits + 1)
		}
		fs := make([]fold, splits)
		for i := range splits {
			cut := n - (splits-i)*test
			if test > 0 && cut-cfg.Gap >= max(f.maxLag()+2, minPoints/2) {
				fs[i] = fold{cut: cut, end: min(cut+test, n)}
			}
		}
		folds[s] = fs
	}
	errsByBucket := map[int][]float64{}
	runs := map[*series][]*cvRun{}
	for i := range splits {
		from := func(s *series) int {
			if fd := folds[s]; fd != nil && fd[i].cut > 0 && cfg.MaxTrainSize > 0 {
				return max(0, fd[i].cut-cfg.Gap-cfg.MaxTrainSize)
			}
			return 0
		}
		to := func(s *series) int {
			if fd := folds[s]; fd != nil && fd[i].cut > 0 {
				return fd[i].cut - cfg.Gap
			}
			return 0
		}
		fr, y, owner := f.trainingFrame(all, from, to)
		if fr.N < 2 {
			continue
		}
		m, err := f.train(fr, y, owner, all, cfg.Seed+uint64(i)+1)
		if err != nil {
			return err
		}
		for _, s := range all {
			fd := folds[s]
			if fd == nil || fd[i].cut == 0 {
				continue
			}
			cut, end := fd[i].cut, fd[i].end
			histEnd := cut - cfg.Gap
			pred := f.path(m, s.st, s.r[:histEnd], histEnd, cut, end-1, seriesExog(s))
			run := &cvRun{prev: math.NaN(), scale: s.st.Scale}
			for k := cut - 1; k >= 0; k-- {
				if s.known[k] {
					run.prev = s.y[k]
					break
				}
			}
			for h, k := 1+cfg.Gap, cut; k < end; h, k = h+1, k+1 {
				if !s.known[k] {
					continue
				}
				b := horizonBucket(h)
				errsByBucket[b] = append(errsByBucket[b], s.r[k]-pred[k-cut])
				run.ys = append(run.ys, s.y[k])
				run.fs = append(run.fs, f.level(s.st, k, pred[k-cut]))
				run.hs = append(run.hs, h)
			}
			if len(run.ys) > 0 {
				runs[s] = append(runs[s], run)
			}
		}
	}
	// Spreads by how far ahead, never narrower further ahead.
	maxBucket := -1
	for b := range errsByBucket {
		maxBucket = max(maxBucket, b)
	}
	f.Sigma = nil
	var pooled []float64
	for b := 0; b <= maxBucket; b++ {
		pooled = append(pooled, errsByBucket[b]...)
		sd := robustSigma(errsByBucket[b])
		if len(errsByBucket[b]) < 10 {
			sd = robustSigma(pooled)
		}
		if b > 0 && sd < f.Sigma[b-1] {
			sd = f.Sigma[b-1]
		}
		f.Sigma = append(f.Sigma, sd)
	}
	if len(f.Sigma) == 0 {
		f.Sigma = []float64{1.5 * f.inSampleSigma(all)}
	}
	for _, s := range all {
		if s.st.Naive {
			f.Logs = append(f.Logs, SeriesLog{Key: s.st.Key, Message: []string{"fewer than 12 points: a naive forecast, its last value"}})
		}
	}
	if !cfg.Evaluate {
		return nil
	}
	for _, s := range all {
		if rs := runs[s]; len(rs) > 0 {
			f.Metrics = append(f.Metrics, f.seriesMetrics(s.st.Key, rs, interval))
		}
	}
	return nil
}

// cvRun is a series' forecasts of one split: actual values, forecasts and
// how far ahead each was, and the value before the split.
type cvRun struct {
	ys, fs []float64
	hs     []int
	prev   float64
	scale  float64
}

// metricNames are the metrics SHOW_EVALUATION_METRICS lists, in order;
// the interval metrics' names carry their interval.
func metricNames(interval float64) []string {
	return []string{"MAE", "MAPE", "MDA", "MSE", "SMAPE",
		"WINKLER_ALPHA=" + strconv.FormatFloat(round6(1-interval), 'f', -1, 64),
		"COVERAGE_INTERVAL=" + strconv.FormatFloat(interval, 'f', -1, 64)}
}

func round6(v float64) float64 { return math.Round(v*1e6) / 1e6 }

// runMetrics computes a run's metrics, with intervals at interval.
func (f *Forecaster) runMetrics(r *cvRun, interval float64) []float64 {
	z := ml.NormalQuantile(0.5 + interval/2)
	lo, hi := make([]float64, len(r.ys)), make([]float64, len(r.ys))
	for i := range r.ys {
		w := z * f.sigma(r.hs[i]) * r.scale
		lo[i], hi[i] = r.fs[i]-w, r.fs[i]+w
	}
	return []float64{ml.MAE(r.ys, r.fs), ml.MAPE(r.ys, r.fs), ml.MDA(r.prev, r.ys, r.fs), ml.MSE(r.ys, r.fs), ml.SMAPE(r.ys, r.fs),
		ml.Winkler(r.ys, lo, hi, 1-interval), ml.Coverage(r.ys, lo, hi)}
}

// seriesMetrics averages a series' runs, with their standard deviation
// over the runs when there are several.
func (f *Forecaster) seriesMetrics(key string, runs []*cvRun, interval float64) SeriesMetrics {
	names := metricNames(interval)
	m := SeriesMetrics{Key: key, Values: map[string]float64{}, StdDev: map[string]float64{}}
	vals := make([][]float64, len(names))
	for _, r := range runs {
		for i, v := range f.runMetrics(r, interval) {
			vals[i] = append(vals[i], v)
		}
	}
	for i, name := range names {
		m.Values[name] = mean(vals[i])
		if len(vals[i]) > 1 {
			mu := mean(vals[i])
			var sq float64
			for _, v := range vals[i] {
				sq += (v - mu) * (v - mu)
			}
			m.StdDev[name] = math.Sqrt(sq / float64(len(vals[i])-1))
		}
	}
	return m
}

// inSampleSigma is the spread of the model's in-sample errors.
func (f *Forecaster) inSampleSigma(all []*series) float64 {
	m := f.current()
	var res []float64
	r := newRow(len(f.featureNamesCache()))
	for _, s := range all {
		if s.st.Naive {
			continue
		}
		for k := 1; k < len(s.y); k++ {
			if s.known[k] {
				f.fill(r, s.st.Key, s.start, k, rFrom(s.r), seriesExog(s))
				res = append(res, s.r[k]-m.predict(f, r))
			}
		}
	}
	return robustSigma(res)
}

// finish keeps what forecasting a series needs: its last steps.
func (f *Forecaster) finish(s *series) {
	keep := min(len(s.r), f.maxLag()+1)
	s.st.History = slices.Clone(s.r[len(s.r)-keep:])
}

// importance is each series' features' shares of its mean absolute SHAP
// values, the target's own features together.
func (f *Forecaster) importance(all []*series) {
	if f.Model == nil && f.Linear == nil {
		return
	}
	names, _ := f.featureNames()
	group := func(j int) (string, string) {
		switch {
		case j < len(f.Lags)+len(f.Windows):
			return endogenous, "derived_from_endogenous"
		case j < len(f.Lags)+len(f.Windows)+len(f.Calendar):
			return names[j], "derived_from_timestamp"
		case j < len(names)-btoi(!f.single()):
			return names[j], "user_provided"
		}
		return "", ""
	}
	for _, s := range all {
		if s.st.Naive {
			continue
		}
		fr, _, _ := f.trainingFrame([]*series{s}, func(s *series) int { return max(len(s.y)-200, 0) }, func(s *series) int { return len(s.y) })
		if fr.N == 0 {
			continue
		}
		var phi []float64
		if f.Model != nil {
			v, err := f.Model.MeanAbsContributions(fr, 200)
			if err != nil {
				continue
			}
			phi = v
		} else {
			c := f.Linear.Contributions(fr)
			T := len(f.Linear.Terms)
			phi = make([]float64, len(names))
			for i := range fr.N {
				for k, term := range f.Linear.Terms {
					j := slices.Index(names, term.Feature)
					if j >= 0 {
						phi[j] += math.Abs(c[i*(T+1)+k]) / float64(fr.N)
					}
				}
			}
		}
		scores := map[string]float64{}
		types := map[string]string{}
		var order []string
		var total float64
		for j := range names {
			name, typ := group(j)
			if name == "" {
				continue
			}
			if _, ok := scores[name]; !ok {
				order = append(order, name)
			}
			scores[name] += phi[j]
			types[name] = typ
			total += phi[j]
		}
		w := SeriesWeights{Key: s.st.Key}
		for _, name := range order {
			v := scores[name]
			if total > 0 {
				v /= total
			}
			w.Features, w.Types, w.Scores = append(w.Features, name), append(w.Types, types[name]), append(w.Scores, v)
		}
		f.Importance = append(f.Importance, w)
	}
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// seriesByKey finds a series' state.
func (f *Forecaster) seriesByKey(key string) *SeriesState {
	for _, s := range f.Series {
		if s.Key == key {
			return s
		}
	}
	return nil
}
