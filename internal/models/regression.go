package models

import (
	"strconv"

	"github.com/akkio-inc/igloo/internal/ml"
)

// Databricks' train-and-score functions, ai_predict_class and
// ai_predict_value: a model trained on the rows whose target is set scores
// those whose target is NULL, within one statement, and is not kept.

// Regressor is a gradient-boosted regression, as ai_predict_value trains.
type Regressor struct {
	Target   string    `json:"target"`
	Features []Feature `json:"features"`
	Model    *ml.GBM   `json:"model"`
	Rows     int       `json:"rows"`
}

// TrainRegressor trains a regression of target on the table's other
// columns, on the rows whose target is not NULL.
func TrainRegressor(t *Table, target string, seed uint64, interrupt func() error) (*Regressor, error) {
	tc := t.Col(target)
	if tc == nil {
		return nil, inputErr("target_col %s is not a column of the input", target)
	}
	if tc.Type != TypeNumber {
		return nil, inputErr("the target %s is of type %s; ai_predict_value predicts numbers", tc.Name, tc.TypeName)
	}
	var rows []int
	for i := range t.N {
		if !tc.Null[i] {
			rows = append(rows, i)
		}
	}
	if len(rows) < 2 {
		return nil, inputErr("the target %s needs at least two rows that are not NULL to train on", tc.Name)
	}
	names := otherNames(t, target)
	if len(names) == 0 {
		return nil, inputErr("feature_cols names no features")
	}
	sub := t.Take(rows)
	frame, feats, err := featureFrame(sub, names)
	if err != nil {
		return nil, err
	}
	y := make([]float64, len(rows))
	copy(y, sub.Col(target).Num)
	order := frame.CanonicalOrder(y)
	ys := make([]float64, len(order))
	for k, o := range order {
		ys[k] = y[o]
	}
	frame = frame.Take(order)
	p := gbmParams(seed)
	p.Objective, p.Interrupt = ml.Regression, interrupt
	_, rounds, err := trainBoosted(frame, ys, p, false)
	if err != nil {
		return nil, err
	}
	p.Rounds, p.EarlyStopping = max(rounds, 1), 0
	m, _, err := ml.TrainGBM(frame, ys, nil, nil, p)
	if err != nil {
		return nil, err
	}
	return &Regressor{Target: tc.Name, Features: feats, Model: m, Rows: len(rows)}, nil
}

// scoreJSON writes what igloo_ml_score returns for a row of bins: the
// class and its probability, or the value.
func (a *Artifact) scoreJSON(buf []byte, s *scratch) []byte {
	if c := a.Classifier; c != nil {
		c.PredictRow(s.bins, s.raw, s.probs)
		best := 0
		for k := range s.probs {
			if s.probs[k] > s.probs[best] {
				best = k
			}
		}
		buf = append(buf, `{"class":`...)
		buf = append(buf, jsonString(c.Classes[best])...)
		buf = append(buf, `,"confidence":`...)
		buf = strconv.AppendFloat(buf, s.probs[best], 'g', -1, 64)
		return append(buf, '}')
	}
	a.Regressor.Model.RawRow(s.bins, s.raw)
	buf = append(buf, `{"value":`...)
	buf = strconv.AppendFloat(buf, s.raw[0], 'g', -1, 64)
	return append(buf, '}')
}
