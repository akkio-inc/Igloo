package models

import (
	"fmt"
	"sort"
	"strings"
)

// ColumnKind is the type of a method result's column.
type ColumnKind int

const (
	// KindVariant is Snowflake's VARIANT, a series value or logs: text.
	KindVariant ColumnKind = iota
	KindText
	KindDouble
	KindBigint
	KindBoolean
	KindTimestamp
)

// ResultColumn is a column of a method's result, named as Snowflake
// names it (upper case).
type ResultColumn struct {
	Name string
	Kind ColumnKind
}

// Result is what a model's method returns: rows of nil, string, float64,
// int64, bool or time.Time values.
type Result struct {
	Columns []ResultColumn
	Rows    [][]any
}

func columns(spec ...any) []ResultColumn {
	out := make([]ResultColumn, 0, len(spec)/2)
	for i := 0; i < len(spec); i += 2 {
		out = append(out, ResultColumn{Name: spec[i].(string), Kind: spec[i+1].(ColumnKind)})
	}
	return out
}

// MethodColumns returns the columns a class's method returns, for a
// statement's description before it runs; ok is false for a method the
// class does not have as a table.
func MethodColumns(class, method string) ([]ResultColumn, bool) {
	switch class + "!" + method {
	case ClassClassification + "!SHOW_EVALUATION_METRICS":
		return columns("DATASET_TYPE", KindText, "CLASS", KindText, "ERROR_METRIC", KindText, "METRIC_VALUE", KindDouble,
			"LOGS", KindVariant), true
	case ClassClassification + "!SHOW_GLOBAL_EVALUATION_METRICS":
		return columns("DATASET_TYPE", KindText, "AVERAGE_TYPE", KindText, "ERROR_METRIC", KindText, "METRIC_VALUE", KindDouble,
			"LOGS", KindVariant), true
	case ClassClassification + "!SHOW_CONFUSION_MATRIX":
		return columns("DATASET_TYPE", KindText, "ACTUAL_CLASS", KindText, "PREDICTED_CLASS", KindText, "COUNT", KindBigint,
			"LOGS", KindVariant), true
	case ClassClassification + "!SHOW_THRESHOLD_METRICS":
		return columns("DATASET_TYPE", KindText, "CLASS", KindText, "THRESHOLD", KindDouble, "PRECISION", KindDouble,
			"RECALL", KindDouble, "F1", KindDouble, "TPR", KindDouble, "FPR", KindDouble, "TP", KindBigint, "FP", KindBigint,
			"TN", KindBigint, "FN", KindBigint, "ACCURACY", KindDouble, "SUPPORT", KindBigint, "LOGS", KindVariant), true
	case ClassClassification + "!SHOW_FEATURE_IMPORTANCE":
		return columns("RANK", KindBigint, "FEATURE", KindText, "SCORE", KindDouble, "FEATURE_TYPE", KindText), true
	case ClassClassification + "!SHOW_TRAINING_LOGS":
		return columns("LOGS", KindVariant), true
	case ClassTopInsights + "!GET_DRIVERS":
		return columns("CONTRIBUTOR", KindVariant, "METRIC_CONTROL", KindDouble, "METRIC_TEST", KindDouble, "CONTRIBUTION", KindDouble,
			"RELATIVE_CONTRIBUTION", KindDouble, "GROWTH_RATE", KindDouble), true
	case ClassForecast + "!FORECAST":
		return columns("SERIES", KindVariant, "TS", KindTimestamp, "FORECAST", KindDouble, "LOWER_BOUND", KindDouble,
			"UPPER_BOUND", KindDouble), true
	case ClassAnomaly + "!DETECT_ANOMALIES":
		return columns("SERIES", KindVariant, "TS", KindTimestamp, "Y", KindDouble, "FORECAST", KindDouble, "LOWER_BOUND", KindDouble,
			"UPPER_BOUND", KindDouble, "IS_ANOMALY", KindBoolean, "PERCENTILE", KindDouble, "DISTANCE", KindDouble), true
	case ClassForecast + "!SHOW_EVALUATION_METRICS", ClassAnomaly + "!SHOW_EVALUATION_METRICS":
		return columns("SERIES", KindVariant, "ERROR_METRIC", KindText, "METRIC_VALUE", KindDouble, "STANDARD_DEVIATION", KindDouble,
			"LOGS", KindVariant), true
	case ClassForecast + "!EXPLAIN_FEATURE_IMPORTANCE", ClassAnomaly + "!EXPLAIN_FEATURE_IMPORTANCE":
		return columns("SERIES", KindVariant, "RANK", KindBigint, "FEATURE_NAME", KindText, "IMPORTANCE_SCORE", KindDouble,
			"FEATURE_TYPE", KindText), true
	case ClassForecast + "!SHOW_TRAINING_LOGS", ClassAnomaly + "!SHOW_TRAINING_LOGS":
		return columns("SERIES", KindVariant, "LOGS", KindVariant), true
	}
	return nil, false
}

// Methods lists a class's methods, for messages.
func Methods(class string) []string {
	var out []string
	for _, m := range []string{"PREDICT", "FORECAST", "DETECT_ANOMALIES", "GET_DRIVERS", "SHOW_EVALUATION_METRICS",
		"SHOW_GLOBAL_EVALUATION_METRICS", "SHOW_CONFUSION_MATRIX", "SHOW_THRESHOLD_METRICS", "SHOW_FEATURE_IMPORTANCE",
		"EXPLAIN_FEATURE_IMPORTANCE", "SHOW_TRAINING_LOGS"} {
		if _, ok := MethodColumns(class, m); ok || class == ClassClassification && m == "PREDICT" {
			out = append(out, m)
		}
	}
	sort.Strings(out)
	return out
}

// errNoEvaluation is a SHOW method of a model trained without evaluation.
func errNoEvaluation(method string) error {
	return inputErr("the model was trained with 'evaluate': FALSE, so it has no %s; create it again with evaluation on", strings.ToLower(method))
}

// ClassifierMethod computes one of a CLASSIFICATION model's SHOW methods.
func (c *Classifier) Method(method string) (*Result, error) {
	cols, ok := MethodColumns(ClassClassification, method)
	if !ok {
		if method == "PREDICT" {
			return nil, inputErr("PREDICT scores rows in a query: SELECT m!PREDICT(INPUT_DATA => {*}) FROM t")
		}
		return nil, inputErr("CLASSIFICATION models have no method %s; they have %s", method, strings.Join(Methods(ClassClassification), ", "))
	}
	res := &Result{Columns: cols}
	add := func(row ...any) { res.Rows = append(res.Rows, row) }
	switch method {
	case "SHOW_FEATURE_IMPORTANCE":
		order := make([]int, len(c.Features))
		for i := range order {
			order[i] = i
		}
		sort.SliceStable(order, func(a, b int) bool { return c.Importance[order[a]] > c.Importance[order[b]] })
		for r, i := range order {
			add(int64(r+1), c.Features[i].Name, c.Importance[i], "user_provided")
		}
		return res, nil
	case "SHOW_TRAINING_LOGS":
		var logs any
		if len(c.Logs) > 0 {
			logs = jsonArray(c.Logs)
		}
		add(logs)
		return res, nil
	}
	e := c.Eval
	if e == nil {
		return nil, errNoEvaluation(method)
	}
	switch method {
	case "SHOW_EVALUATION_METRICS":
		for k, m := range e.PerClass {
			for _, mv := range []struct {
				name string
				v    any
			}{{"precision", m.Precision}, {"recall", m.Recall}, {"f1", m.F1}, {"support", float64(m.Support)}} {
				add("EVAL", c.Classes[k], mv.name, mv.v, nil)
			}
		}
	case "SHOW_GLOBAL_EVALUATION_METRICS":
		for _, g := range e.Global {
			var avg any
			if g.AverageType != "" {
				avg = g.AverageType
			}
			add("EVAL", avg, g.Metric, g.Value, nil)
		}
	case "SHOW_CONFUSION_MATRIX":
		for a := range e.Confusion {
			for p, n := range e.Confusion[a] {
				add("EVAL", c.Classes[a], c.Classes[p], int64(n), nil)
			}
		}
	case "SHOW_THRESHOLD_METRICS":
		for k, rows := range e.Thresholds {
			for _, t := range rows {
				add("EVAL", c.Classes[k], t.Threshold, t.Precision, t.Recall, t.F1, t.TPR, t.FPR, int64(t.TP), int64(t.FP),
					int64(t.TN), int64(t.FN), t.Accuracy, int64(t.Support), nil)
			}
		}
	}
	return res, nil
}

// jsonArray writes strings as a JSON array.
func jsonArray(items []string) string {
	parts := make([]string, len(items))
	for i, s := range items {
		parts[i] = jsonString(s)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// Method computes one of the artifact's methods that need no input data.
func (a *Artifact) Method(method string) (*Result, error) {
	switch {
	case a.Classifier != nil:
		return a.Classifier.Method(method)
	case a.Forecaster != nil:
		return a.Forecaster.Method(a.Class, method)
	}
	return nil, fmt.Errorf("model of class %s has no methods here", a.Class)
}
