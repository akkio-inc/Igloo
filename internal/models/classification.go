package models

import (
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/akkio-inc/igloo/internal/ml"
)

// CLASSIFICATION: a gradient-boosted classifier of 2 to 255 classes, with
// metrics on a held-out sample, as Snowflake's.

// MaxClasses is the most classes a classifier may have.
const MaxClasses = 255

// ClassifierConfig is CREATE SNOWFLAKE.ML.CLASSIFICATION's CONFIG_OBJECT.
type ClassifierConfig struct {
	// Evaluate holds back rows to measure the model on (default true).
	Evaluate bool
	// TestFraction is the share of rows held back; 0 means Snowflake's
	// rule: enough for 500 rows of the rarest class, at most 20%.
	TestFraction float64
	// Seed seeds sampling; models are the same for the same rows and seed.
	Seed uint64
	// Interrupt, if set, stops training when it returns an error: the
	// statement was canceled or timed out.
	Interrupt func() error
}

// Classifier is a trained CLASSIFICATION model.
type Classifier struct {
	Target     string    `json:"target"`
	TargetType string    `json:"target_type"`
	Classes    []string  `json:"classes"`
	Features   []Feature `json:"features"`
	Model      *ml.GBM   `json:"model"`
	// Importance is each feature's share of the mean absolute SHAP value.
	Importance []float64  `json:"importance"`
	Rows       int        `json:"rows"`
	Eval       *ClassEval `json:"eval,omitempty"`
	Logs       []string   `json:"logs,omitempty"`
}

// ClassEval is a classifier's metrics on held-out rows.
type ClassEval struct {
	Rows       int              `json:"rows"`
	PerClass   []ClassMetrics   `json:"per_class"`
	Global     []GlobalMetric   `json:"global"`
	Confusion  [][]int          `json:"confusion"`
	Thresholds [][]ThresholdRow `json:"thresholds"`
}

// ClassMetrics are one class's metrics against the rest.
type ClassMetrics struct {
	Precision float64 `json:"precision"`
	Recall    float64 `json:"recall"`
	F1        float64 `json:"f1"`
	Support   int     `json:"support"`
}

// GlobalMetric is a metric over every class: AverageType is macro or
// weighted, or empty for log loss.
type GlobalMetric struct {
	AverageType string  `json:"average_type,omitempty"`
	Metric      string  `json:"metric"`
	Value       float64 `json:"value"`
}

// ThresholdRow is a class's metrics at one probability threshold.
type ThresholdRow struct {
	Threshold float64 `json:"threshold"`
	Precision float64 `json:"precision"`
	Recall    float64 `json:"recall"`
	F1        float64 `json:"f1"`
	TPR       float64 `json:"tpr"`
	FPR       float64 `json:"fpr"`
	TP        int     `json:"tp"`
	FP        int     `json:"fp"`
	TN        int     `json:"tn"`
	FN        int     `json:"fn"`
	Accuracy  float64 `json:"accuracy"`
	Support   int     `json:"support"`
}

// gbmParams are the boosting settings every class trains with: up to 500
// rounds, stopped early on a held-out tenth of the training rows.
func gbmParams(seed uint64) ml.Params {
	return ml.Params{Rounds: 500, LearningRate: 0.1, NumLeaves: 31, EarlyStopping: 20, Seed: seed}
}

// labeled is a training set: rows with a label, their classes and frame.
type labeled struct {
	frame    *ml.Frame
	features []Feature
	y        []float64
	classes  []string
}

// classLabels reads target's rows that are not NULL, and returns them
// with their class numbers and the classes, sorted as the target's values.
func classLabels(t *Table, target string) (rows []int, y []float64, classes []string, err error) {
	tc := t.Col(target)
	if tc == nil {
		return nil, nil, nil, inputErr("TARGET_COLNAME %s is not a column of INPUT_DATA", target)
	}
	if tc.Type == TypeOther {
		return nil, nil, nil, inputErr("the target %s is of type %s; it must be a number, string or boolean", tc.Name, tc.TypeName)
	}
	type cls struct {
		label string
		num   float64
	}
	seen := map[string]cls{}
	for i := range t.N {
		if tc.Null[i] {
			continue
		}
		rows = append(rows, i)
		l := labelText(tc, i)
		if _, ok := seen[l]; !ok {
			c := cls{label: l}
			if tc.Type == TypeNumber {
				c.num = tc.Num[i]
			} else if tc.Type == TypeTime {
				c.num = float64(tc.Micros[i])
			}
			seen[l] = c
		}
	}
	all := make([]cls, 0, len(seen))
	for _, c := range seen {
		all = append(all, c)
	}
	sort.Slice(all, func(i, j int) bool {
		if tc.Type == TypeNumber || tc.Type == TypeTime {
			return all[i].num < all[j].num
		}
		return all[i].label < all[j].label
	})
	if len(all) < 2 {
		return nil, nil, nil, inputErr("the target %s needs at least two classes; it has %d", tc.Name, len(all))
	}
	if len(all) > MaxClasses {
		return nil, nil, nil, inputErr("the target %s has %d classes; a classifier may have at most %d", tc.Name, len(all), MaxClasses)
	}
	if len(all) >= len(rows) {
		return nil, nil, nil, inputErr("the target %s has as many classes as rows", tc.Name)
	}
	index := map[string]int{}
	for k, c := range all {
		index[c.label] = k
		classes = append(classes, c.label)
	}
	y = make([]float64, len(rows))
	for k, i := range rows {
		y[k] = float64(index[labelText(tc, i)])
	}
	return rows, y, classes, nil
}

// prepareLabeled reads a labeled training set in canonical row order.
func prepareLabeled(t *Table, target string) (*labeled, error) {
	rows, y, classes, err := classLabels(t, target)
	if err != nil {
		return nil, err
	}
	names := otherNames(t, target)
	if len(names) == 0 {
		return nil, inputErr("INPUT_DATA has no columns besides the target")
	}
	frame, feats, err := featureFrame(t.Take(rows), names)
	if err != nil {
		return nil, err
	}
	order := frame.CanonicalOrder(y)
	ys := make([]float64, len(order))
	for k, o := range order {
		ys[k] = y[o]
	}
	return &labeled{frame: frame.Take(order), features: feats, y: ys, classes: classes}, nil
}

// stratified splits rows 0..n-1 by class, putting about fraction of each
// class (at least one row of a class of two or more) in the second part,
// chosen by a hash of seed and the row's position.
func stratified(y []float64, fraction float64, seed, salt uint64) (keep, held []int) {
	byClass := map[int][]int{}
	for i, v := range y {
		byClass[int(v)] = append(byClass[int(v)], i)
	}
	classes := make([]int, 0, len(byClass))
	for c := range byClass {
		classes = append(classes, c)
	}
	slices.Sort(classes)
	heldSet := make([]bool, len(y))
	for _, c := range classes {
		rows := byClass[c]
		k := int(math.Round(fraction * float64(len(rows))))
		if k == 0 && len(rows) >= 2 && fraction > 0 {
			k = 1
		}
		if k >= len(rows) {
			k = len(rows) - 1
		}
		sort.SliceStable(rows, func(a, b int) bool { return rank(seed, salt, rows[a]) < rank(seed, salt, rows[b]) })
		for _, r := range rows[:k] {
			heldSet[r] = true
		}
	}
	for i, h := range heldSet {
		if h {
			held = append(held, i)
		} else {
			keep = append(keep, i)
		}
	}
	return keep, held
}

// rank orders rows pseudo-randomly by seed.
func rank(seed, salt uint64, i int) uint64 {
	x := seed ^ salt*0x9e3779b97f4a7c15 ^ uint64(i)*0xbf58476d1ce4e5b9
	x ^= x >> 31
	x *= 0x94d049bb133111eb
	x ^= x >> 29
	return x
}

func subset(f *ml.Frame, y []float64, rows []int) (*ml.Frame, []float64) {
	ys := make([]float64, len(rows))
	for k, r := range rows {
		ys[k] = y[r]
	}
	return f.Take(rows), ys
}

// trainBoosted trains on (f, y) with p, stopping early on a stratified
// tenth of the rows when there are enough, and returns the rounds kept.
func trainBoosted(f *ml.Frame, y []float64, p ml.Params, classify bool) (*ml.GBM, int, error) {
	if f.N < 50 {
		p.EarlyStopping = 0
		p.Rounds = 100
		m, info, err := ml.TrainGBM(f, y, nil, nil, p)
		return m, info.Rounds, err
	}
	var keep, held []int
	if classify {
		keep, held = stratified(y, 0.1, p.Seed, 0x7a1)
	} else {
		for i := range f.N {
			if rank(p.Seed, 0x7a1, i)%10 == 0 {
				held = append(held, i)
			} else {
				keep = append(keep, i)
			}
		}
	}
	tf, ty := subset(f, y, keep)
	vf, vy := subset(f, y, held)
	m, info, err := ml.TrainGBM(tf, ty, vf, vy, p)
	return m, info.Rounds, err
}

// TrainClassifier trains a CLASSIFICATION model on t's rows whose target
// is not NULL, every other column a feature.
func TrainClassifier(t *Table, target string, cfg ClassifierConfig) (*Classifier, error) {
	d, err := prepareLabeled(t, target)
	if err != nil {
		return nil, err
	}
	K := len(d.classes)
	p := gbmParams(cfg.Seed)
	p.Interrupt = cfg.Interrupt
	p.Objective = ml.Binary
	if K > 2 {
		p.Objective, p.Classes = ml.Multiclass, K
	}
	c := &Classifier{Target: t.Col(target).Name, TargetType: t.Col(target).Type.String(), Classes: d.classes,
		Features: d.features, Rows: d.frame.N}
	if skipped := t.N - d.frame.N; skipped > 0 {
		c.Logs = append(c.Logs, strconv.Itoa(skipped)+" rows with a NULL target were left out")
	}
	rounds := 0
	if cfg.Evaluate {
		frac := cfg.TestFraction
		if frac <= 0 || frac >= 1 {
			counts := make([]int, K)
			for _, v := range d.y {
				counts[int(v)]++
			}
			minority := slices.Min(counts)
			frac = math.Min(0.2, 500/float64(minority))
		}
		train, test := stratified(d.y, frac, cfg.Seed, 0xe7a1)
		tf, ty := subset(d.frame, d.y, train)
		ef, ey := subset(d.frame, d.y, test)
		m, r, err := trainBoosted(tf, ty, p, true)
		if err != nil {
			return nil, err
		}
		rounds = r
		if c.Eval, err = evaluateClassifier(m, ef, ey, K); err != nil {
			return nil, err
		}
	} else {
		_, r, err := trainBoosted(d.frame, d.y, p, true)
		if err != nil {
			return nil, err
		}
		rounds = r
	}
	p.Rounds, p.EarlyStopping = max(rounds, 1), 0
	if c.Model, _, err = ml.TrainGBM(d.frame, d.y, nil, nil, p); err != nil {
		return nil, err
	}
	if c.Importance, err = importance(c.Model, d.frame); err != nil {
		return nil, err
	}
	return c, nil
}

// importance is each feature's share of the mean absolute SHAP value over
// up to 10,000 rows.
func importance(m *ml.GBM, f *ml.Frame) ([]float64, error) {
	imp, err := m.MeanAbsContributions(f, 10_000)
	if err != nil {
		return nil, err
	}
	var total float64
	for _, v := range imp {
		total += v
	}
	if total > 0 {
		for i := range imp {
			imp[i] /= total
		}
	}
	return imp, nil
}

// probabilities returns each row's class probabilities, row i's of class
// k at k*n+i.
func probabilities(m *ml.GBM, f *ml.Frame, K int) ([]float64, error) {
	p, err := m.Predict(f)
	if err != nil {
		return nil, err
	}
	if K > 2 {
		return p, nil
	}
	n := f.N
	out := make([]float64, 2*n)
	for i := range n {
		out[i], out[n+i] = 1-p[i], p[i]
	}
	return out, nil
}

func evaluateClassifier(m *ml.GBM, f *ml.Frame, y []float64, K int) (*ClassEval, error) {
	probs, err := probabilities(m, f, K)
	if err != nil {
		return nil, err
	}
	n := f.N
	actual, predicted := make([]int, n), make([]int, n)
	for i := range n {
		best := 0
		for k := 1; k < K; k++ {
			if probs[k*n+i] > probs[best*n+i] {
				best = k
			}
		}
		actual[i], predicted[i] = int(y[i]), best
	}
	stats, confusion := ml.ClassReport(actual, predicted, K)
	e := &ClassEval{Rows: n, Confusion: confusion}
	support := make([]int, K)
	var prec, rec, f1, auc []float64
	for k, s := range stats {
		e.PerClass = append(e.PerClass, ClassMetrics{Precision: s.Precision, Recall: s.Recall, F1: s.F1, Support: s.Support})
		support[k] = s.Support
		prec, rec, f1 = append(prec, s.Precision), append(rec, s.Recall), append(f1, s.F1)
		pos := make([]bool, n)
		for i := range n {
			pos[i] = actual[i] == k
		}
		a := ml.AUC(probs[k*n:(k+1)*n], pos)
		if math.IsNaN(a) {
			a = 0
		}
		auc = append(auc, a)
		var rows []ThresholdRow
		for _, th := range ml.Thresholds(probs[k*n:(k+1)*n], pos) {
			rows = append(rows, ThresholdRow{Threshold: th.Threshold, Precision: th.Precision, Recall: th.Recall, F1: th.F1,
				TPR: th.TPR, FPR: th.FPR, TP: th.TP, FP: th.FP, TN: th.TN, FN: th.FN, Accuracy: th.Accuracy, Support: th.Support})
		}
		e.Thresholds = append(e.Thresholds, rows)
	}
	for _, avg := range []string{"macro", "weighted"} {
		for _, mv := range []struct {
			name string
			v    []float64
		}{{"precision", prec}, {"recall", rec}, {"f1", f1}, {"auc", auc}} {
			macro, weighted := ml.Averages(mv.v, support)
			v := macro
			if avg == "weighted" {
				v = weighted
			}
			e.Global = append(e.Global, GlobalMetric{AverageType: avg, Metric: mv.name, Value: v})
		}
	}
	e.Global = append(e.Global, GlobalMetric{Metric: "log_loss", Value: ml.LogLoss(probs, actual)})
	return e, nil
}

// PredictRow scores one row of bins: the class probabilities into probs
// (len(Classes)).
func (c *Classifier) PredictRow(bins []uint8, raw, probs []float64) {
	c.Model.RawRow(bins, raw)
	if len(c.Classes) == 2 {
		p := 1 / (1 + math.Exp(-raw[0]))
		probs[0], probs[1] = 1-p, p
		return
	}
	hi := math.Inf(-1)
	for _, v := range raw {
		hi = math.Max(hi, v)
	}
	var sum float64
	for k, v := range raw {
		probs[k] = math.Exp(v - hi)
		sum += probs[k]
	}
	for k := range probs {
		probs[k] /= sum
	}
}

// PredictJSON writes a prediction as Snowflake's PREDICT returns it:
// {"class": ..., "logs": null, "probability": {class: p, ...}}.
func (c *Classifier) PredictJSON(buf []byte, probs []float64) []byte {
	best := 0
	for k := range probs {
		if probs[k] > probs[best] {
			best = k
		}
	}
	buf = append(buf, `{"class":`...)
	buf = append(buf, jsonString(c.Classes[best])...)
	buf = append(buf, `,"logs":null,"probability":{`...)
	for k, l := range c.Classes {
		if k > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, jsonString(l)...)
		buf = append(buf, ':')
		buf = strconv.AppendFloat(buf, probs[k], 'g', -1, 64)
	}
	return append(buf, "}}"...)
}

// jsonString quotes s as a JSON string.
func jsonString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch {
		case r == '"' || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r < 0x20:
			b.WriteString(`\u00`)
			b.WriteByte("0123456789abcdef"[r>>4])
			b.WriteByte("0123456789abcdef"[r&15])
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}
