package models

import (
	"math"
	"math/rand/v2"
	"strings"
	"testing"
	"time"
)

var day0 = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

// seriesTable builds a table of (day, series, value[, promo]) rows.
type seriesRow struct {
	day    int
	series string
	y      float64
	promo  float64
	noY    bool
}

func seriesTable(rows []seriesRow, withSeries, withPromo bool) *Table {
	t := &Table{N: len(rows)}
	ts := &TableCol{Name: "day", Type: TypeTime, TypeName: "TIMESTAMP"}
	y := &TableCol{Name: "spend", Type: TypeNumber, TypeName: "DOUBLE"}
	sc := &TableCol{Name: "campaign", Type: TypeString, TypeName: "VARCHAR"}
	pc := &TableCol{Name: "promo", Type: TypeNumber, TypeName: "DOUBLE"}
	for _, r := range rows {
		ts.Micros, ts.Null = append(ts.Micros, day0.AddDate(0, 0, r.day).UnixMicro()), append(ts.Null, false)
		if r.noY {
			y.Num, y.Null = append(y.Num, math.NaN()), append(y.Null, true)
		} else {
			y.Num, y.Null = append(y.Num, r.y), append(y.Null, false)
		}
		sc.Str, sc.Null = append(sc.Str, r.series), append(sc.Null, false)
		pc.Num, pc.Null = append(pc.Num, r.promo), append(pc.Null, false)
	}
	t.Cols = []*TableCol{ts, y}
	if withSeries {
		t.Cols = append(t.Cols, sc)
	}
	if withPromo {
		t.Cols = append(t.Cols, pc)
	}
	return t
}

// seasonal is a daily series with a trend, a weekly cycle and noise.
func seasonal(t int, r *rand.Rand, level, noise float64) float64 {
	return level + 0.1*float64(t) + 0.2*level*math.Sin(2*math.Pi*float64(t)/7) + noise*r.NormFloat64()
}

func TestForecastSeasonalSeries(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	var train []seriesRow
	truth := map[int]float64{}
	for d := range 414 {
		v := seasonal(d, r, 100, 2)
		if d < 400 {
			train = append(train, seriesRow{day: d, y: v})
		} else {
			truth[d] = v
		}
	}
	f, err := TrainForecaster(seriesTable(train, false, false), "day", "spend", "", "", ForecastConfig{Evaluate: true, NSplits: 1})
	if err != nil {
		t.Fatal(err)
	}
	if f.Freq.Name != "1 day" || f.Series[0].B == 0 || f.Model == nil {
		t.Errorf("frequency %q, trend %v, trees %v", f.Freq.Name, f.Series[0].B, f.Model != nil)
	}
	res, err := f.Forecast(ForecastQuery{Periods: 14, Interval: 0.95})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 14 {
		t.Fatalf("%d rows", len(res.Rows))
	}
	var absErr float64
	in := 0
	for i, row := range res.Rows {
		ts := row[1].(time.Time)
		if want := day0.AddDate(0, 0, 400+i); !ts.Equal(want) {
			t.Errorf("row %d at %v, want %v", i, ts, want)
		}
		if row[0] != nil {
			t.Errorf("a single series' SERIES is %v", row[0])
		}
		fc, lo, hi := row[2].(float64), row[3].(float64), row[4].(float64)
		y := truth[400+i]
		absErr += math.Abs(fc - y)
		if y >= lo && y <= hi {
			in++
		}
		if lo > fc || hi < fc {
			t.Errorf("interval [%v, %v] around %v", lo, hi, fc)
		}
	}
	if mae := absErr / 14; mae > 4 {
		t.Errorf("MAE %.2f against noise of 2", mae)
	}
	if in < 11 {
		t.Errorf("%d of 14 true values inside the 95%% intervals", in)
	}
	m, err := f.Method(ClassForecast, "SHOW_EVALUATION_METRICS")
	if err != nil || len(m.Rows) != 7 || m.Rows[0][1] != "MAE" || m.Rows[5][1] != "WINKLER_ALPHA=0.05" || m.Rows[6][1] != "COVERAGE_INTERVAL=0.95" {
		t.Fatalf("metrics %v, %v", m, err)
	}
	if mae := m.Rows[0][2].(float64); mae > 6 {
		t.Errorf("cross-validated MAE %.2f", mae)
	}
	if m.Rows[0][3] != nil {
		t.Errorf("one split has a standard deviation: %v", m.Rows[0][3])
	}
	imp, err := f.Method(ClassForecast, "EXPLAIN_FEATURE_IMPORTANCE")
	if err != nil || len(imp.Rows) == 0 {
		t.Fatalf("importance %v, %v", imp, err)
	}
	var total float64
	names := []string{}
	for _, row := range imp.Rows {
		total += row[3].(float64)
		names = append(names, row[2].(string))
	}
	if math.Abs(total-1) > 1e-9 || !strings.Contains(strings.Join(names, ","), endogenous) {
		t.Errorf("importances %v add up to %v", names, total)
	}
}

func TestDetectAnomalies(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	var train, check []seriesRow
	spikes := map[int]bool{405: true, 413: true, 421: true}
	for d := range 430 {
		v := seasonal(d, r, 200, 3)
		switch {
		case d == 100 || d == 250:
			// Anomalies in the training data too, which training leaves out.
			train = append(train, seriesRow{day: d, y: v + 80})
		case d < 400:
			train = append(train, seriesRow{day: d, y: v})
		case spikes[d]:
			check = append(check, seriesRow{day: d, y: v + 60})
		default:
			check = append(check, seriesRow{day: d, y: v})
		}
	}
	f, err := TrainForecaster(seriesTable(train, false, false), "day", "spend", "", "", ForecastConfig{Anomaly: true, Evaluate: true, NSplits: 2})
	if err != nil {
		t.Fatal(err)
	}
	res, err := f.Detect(ForecastQuery{Input: seriesTable(check, false, false), TimeCol: "day", TargetCol: "spend", Interval: 0.99})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 30 {
		t.Fatalf("%d rows", len(res.Rows))
	}
	falsePositives := 0
	for _, row := range res.Rows {
		d := int(row[1].(time.Time).Sub(day0).Hours() / 24)
		anomaly := row[6].(bool)
		pct, dist := row[7].(float64), row[8].(float64)
		if spikes[d] && (!anomaly || pct < 0.995 || dist < 2.6) {
			t.Errorf("day %d: a spike of 60 not flagged: %v", d, row)
		}
		if !spikes[d] && anomaly {
			falsePositives++
		}
	}
	if falsePositives > 2 {
		t.Errorf("%d of 27 ordinary days flagged", falsePositives)
	}
	if m, err := f.Method(ClassAnomaly, "SHOW_EVALUATION_METRICS"); err != nil || len(m.Rows) != 7 || m.Rows[0][3] == nil {
		t.Errorf("two splits' metrics need standard deviations: %v, %v", m, err)
	}
	// Points before the training data's end are refused.
	if _, err := f.Detect(ForecastQuery{Input: seriesTable(train[:5], false, false), TimeCol: "day", TargetCol: "spend", Interval: 0.99}); err == nil ||
		!strings.Contains(err.Error(), "after the training data") {
		t.Errorf("old points: %v", err)
	}
}

func TestForecastSeveralSeriesWithExogenous(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	levels := map[string]float64{"c1": 10, "c2": 100, "c3": 1000}
	var train, future []seriesRow
	truth := map[string]map[int]float64{}
	for _, c := range []string{"c1", "c2", "c3"} {
		truth[c] = map[int]float64{}
		for d := range 220 {
			promo := float64(r.IntN(2))
			v := levels[c] * (1 + 0.5*promo + 0.2*math.Sin(2*math.Pi*float64(d)/7) + 0.02*r.NormFloat64())
			if d < 200 {
				train = append(train, seriesRow{day: d, series: c, y: v, promo: promo})
			} else {
				future = append(future, seriesRow{day: d, series: c, promo: promo, noY: true})
				truth[c][d] = v
			}
		}
	}
	// A short series gets a naive forecast.
	for d := range 5 {
		train = append(train, seriesRow{day: 195 + d, series: "c4", y: float64(7 + d), promo: 0})
	}
	f, err := TrainForecaster(seriesTable(train, true, true), "day", "spend", "campaign", "", ForecastConfig{Evaluate: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Forecast(ForecastQuery{Periods: 3, Interval: 0.95}); err == nil || !strings.Contains(err.Error(), "promo") {
		t.Errorf("forecast without the exogenous column: %v", err)
	}
	res, err := f.Forecast(ForecastQuery{Input: seriesTable(future, true, true), TimeCol: "day", SeriesCol: "campaign", Interval: 0.9})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 60 {
		t.Fatalf("%d rows", len(res.Rows))
	}
	for _, row := range res.Rows {
		c := row[0].(string)
		d := int(row[1].(time.Time).Sub(day0).Hours() / 24)
		if rel := math.Abs(row[2].(float64)-truth[c][d]) / levels[c]; rel > 0.15 {
			t.Errorf("%s day %d: forecast %.1f, truth %.1f", c, d, row[2], truth[c][d])
		}
	}
	one := "c4"
	res, err = f.Forecast(ForecastQuery{Input: seriesTable([]seriesRow{{day: 200, series: "c4", noY: true}}, true, true), TimeCol: "day",
		SeriesCol: "campaign", SeriesValue: &one, Interval: 0.9})
	if err != nil || len(res.Rows) != 1 || res.Rows[0][2].(float64) != 11 {
		t.Errorf("the naive series: %v, %v", res, err)
	}
	logs, _ := f.Method(ClassForecast, "SHOW_TRAINING_LOGS")
	found := false
	for _, row := range logs.Rows {
		if row[0] == "c4" && row[1] != nil && strings.Contains(row[1].(string), "naive") {
			found = true
		}
	}
	if !found {
		t.Errorf("logs %v", logs.Rows)
	}
}

func TestForecastIsRepeatable(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 8))
	var rows []seriesRow
	for d := range 120 {
		rows = append(rows, seriesRow{day: d, y: seasonal(d, r, 50, 1)})
	}
	train := func(rows []seriesRow) string {
		f, err := TrainForecaster(seriesTable(rows, false, false), "day", "spend", "", "", ForecastConfig{Evaluate: true})
		if err != nil {
			t.Fatal(err)
		}
		_, sha, err := (&Artifact{Class: ClassForecast, Forecaster: f}).Encode()
		if err != nil {
			t.Fatal(err)
		}
		return sha
	}
	a := train(rows)
	shuffled := append([]seriesRow(nil), rows...)
	r.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	if b := train(shuffled); a != b {
		t.Errorf("rows in another order trained %s, then %s", a, b)
	}
}

func TestFrequencies(t *testing.T) {
	for in, want := range map[string]Frequency{
		"1 day": {Micros: microsPerDay, Name: "1 day"}, "15 minutes": {Micros: 15 * microsPerMinute, Name: "15 minutes"},
		"month": {Months: 1, Name: "1 month"}, "2 quarters": {Months: 6, Name: "2 quarters"},
	} {
		if got, err := ParseFrequency(in); err != nil || got != want {
			t.Errorf("%s: %+v, %v", in, got, err)
		}
	}
	for _, bad := range []string{"1 fortnight", "0 days", "1 hr", "a day"} {
		if _, err := ParseFrequency(bad); err == nil {
			t.Errorf("%s: no error", bad)
		}
	}
	// Monthly data, on the first of each month.
	var stamps []int64
	for m := range 24 {
		stamps = append(stamps, day0.AddDate(0, m, 0).UnixMicro())
	}
	f, err := inferFrequency([][]int64{stamps})
	if err != nil || f.Months != 1 {
		t.Errorf("monthly: %+v, %v", f, err)
	}
	for k, ts := range stamps {
		if got := f.index(stamps[0], ts); got != k {
			t.Errorf("step of %v: %d, want %d", time.UnixMicro(ts).UTC(), got, k)
		}
		if got := f.at(stamps[0], k); got != ts {
			t.Errorf("step %d at %v", k, time.UnixMicro(got).UTC())
		}
	}
}
