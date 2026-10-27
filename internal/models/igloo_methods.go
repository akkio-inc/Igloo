package models

import (
	"slices"
	"strings"
)

// The table methods of Igloo's classes, named after BigQuery ML's
// functions (ML.EVALUATE is EVALUATE, ML.WEIGHTS is WEIGHTS, ...), with
// their columns.

var (
	regressionMetricColumns = columns("MEAN_ABSOLUTE_ERROR", KindDouble, "MEAN_SQUARED_ERROR", KindDouble,
		"MEAN_SQUARED_LOG_ERROR", KindDouble, "MEDIAN_ABSOLUTE_ERROR", KindDouble, "R2_SCORE", KindDouble,
		"EXPLAINED_VARIANCE", KindDouble)
	classificationMetricColumns = columns("PRECISION", KindDouble, "RECALL", KindDouble, "ACCURACY", KindDouble,
		"F1_SCORE", KindDouble, "LOG_LOSS", KindDouble, "ROC_AUC", KindDouble)
	trainingInfoColumns = columns("ITERATION", KindBigint, "LOSS", KindDouble, "EVAL_LOSS", KindDouble, "LEARNING_RATE", KindDouble)
)

// iglooTableMethods lists each class's table methods.
var iglooTableMethods = map[string][]string{
	ClassRegression:      {"EVALUATE", "GLOBAL_EXPLAIN", "FEATURE_IMPORTANCE", "WEIGHTS", "TRAINING_INFO"},
	ClassIglooClassifier: {"EVALUATE", "CONFUSION_MATRIX", "ROC_CURVE", "GLOBAL_EXPLAIN", "FEATURE_IMPORTANCE", "WEIGHTS", "TRAINING_INFO"},
	ClassKMeans:          {"EVALUATE", "CENTROIDS", "TRAINING_INFO"},
	ClassPCA:             {"EVALUATE", "PRINCIPAL_COMPONENTS", "PRINCIPAL_COMPONENT_INFO"},
	ClassFactorization:   {"EVALUATE", "TRAINING_INFO"},
	ClassDNNRegressor:    {"EVALUATE", "GLOBAL_EXPLAIN", "TRAINING_INFO"},
	ClassDNNClassifier:   {"EVALUATE", "CONFUSION_MATRIX", "ROC_CURVE", "GLOBAL_EXPLAIN", "TRAINING_INFO"},
}

// iglooRowMethods lists each class's row methods, for messages; a model's
// own (Artifact.RowMethods) may have fewer.
var iglooRowMethods = map[string][]string{
	ClassRegression:      {"PREDICT", "EXPLAIN_PREDICT"},
	ClassIglooClassifier: {"PREDICT", "EXPLAIN_PREDICT"},
	ClassKMeans:          {"PREDICT", "DETECT_ANOMALIES"},
	ClassPCA:             {"PREDICT", "DETECT_ANOMALIES"},
	ClassFactorization:   {"PREDICT", "RECOMMEND"},
	ClassDNNRegressor:    {"PREDICT"},
	ClassDNNClassifier:   {"PREDICT"},
}

// IsRowMethodOf reports whether method is a row method of class.
func IsRowMethodOf(class, method string) bool {
	return slices.Contains(iglooRowMethods[class], method)
}

// iglooMethodColumns returns the columns of an Igloo class's table method.
func iglooMethodColumns(class, method string) ([]ResultColumn, bool) {
	if !slices.Contains(iglooTableMethods[class], method) {
		return nil, false
	}
	switch method {
	case "EVALUATE":
		switch class {
		case ClassRegression, ClassDNNRegressor, ClassAutoMLRegressor:
			return regressionMetricColumns, true
		case ClassIglooClassifier, ClassDNNClassifier, ClassAutoMLClassifier:
			return classificationMetricColumns, true
		case ClassKMeans:
			return columns("DAVIES_BOULDIN_INDEX", KindDouble, "MEAN_SQUARED_DISTANCE", KindDouble), true
		case ClassPCA:
			return columns("TOTAL_EXPLAINED_VARIANCE_RATIO", KindDouble), true
		case ClassFactorization:
			return append(slices.Clone(regressionMetricColumns), columns("MEAN_AVERAGE_PRECISION", KindDouble,
				"NORMALIZED_DISCOUNTED_CUMULATIVE_GAIN", KindDouble, "AVERAGE_RANK", KindDouble)...), true
		case ClassAutoencoder:
			return columns("MEAN_ABSOLUTE_ERROR", KindDouble, "MEAN_SQUARED_ERROR", KindDouble, "MEAN_SQUARED_LOG_ERROR", KindDouble), true
		}
	case "CONFUSION_MATRIX":
		return columns("EXPECTED_LABEL", KindText, "PREDICTED_LABEL", KindText, "COUNT", KindBigint), true
	case "ROC_CURVE":
		return columns("THRESHOLD", KindDouble, "RECALL", KindDouble, "FALSE_POSITIVE_RATE", KindDouble, "TRUE_POSITIVES", KindBigint,
			"FALSE_POSITIVES", KindBigint, "TRUE_NEGATIVES", KindBigint, "FALSE_NEGATIVES", KindBigint), true
	case "GLOBAL_EXPLAIN":
		return columns("FEATURE", KindText, "ATTRIBUTION", KindDouble), true
	case "FEATURE_IMPORTANCE":
		return columns("FEATURE", KindText, "IMPORTANCE_WEIGHT", KindBigint, "IMPORTANCE_GAIN", KindDouble, "IMPORTANCE_COVER", KindDouble), true
	case "WEIGHTS":
		return columns("CLASS", KindText, "PROCESSED_INPUT", KindText, "CATEGORY", KindText, "WEIGHT", KindDouble,
			"STANDARD_ERROR", KindDouble, "P_VALUE", KindDouble), true
	case "TRAINING_INFO":
		return trainingInfoColumns, true
	case "TRIAL_INFO":
		return columns("TRIAL_ID", KindBigint, "ALGORITHM", KindText, "HYPERPARAMETERS", KindVariant, "STATUS", KindText,
			"OBJECTIVE", KindDouble, "IS_OPTIMAL", KindBoolean), true
	case "CENTROIDS":
		return columns("CENTROID_ID", KindBigint, "FEATURE", KindText, "NUMERICAL_VALUE", KindDouble, "CATEGORY", KindText,
			"CATEGORICAL_VALUE", KindDouble), true
	case "PRINCIPAL_COMPONENTS":
		return columns("PRINCIPAL_COMPONENT_ID", KindBigint, "FEATURE", KindText, "NUMERICAL_VALUE", KindDouble, "CATEGORY", KindText,
			"CATEGORICAL_VALUE", KindDouble), true
	case "PRINCIPAL_COMPONENT_INFO":
		return columns("PRINCIPAL_COMPONENT_ID", KindBigint, "EIGENVALUE", KindDouble, "EXPLAINED_VARIANCE_RATIO", KindDouble,
			"CUMULATIVE_EXPLAINED_VARIANCE_RATIO", KindDouble), true
	}
	return nil, false
}

// joinMethods lists a class's methods for messages.
func joinMethods(class string) string {
	return strings.Join(Methods(class), ", ")
}

// iglooMethod computes a table method of one of Igloo's models.
func (a *Artifact) iglooMethod(method string) (*Result, error) {
	if _, ok := iglooMethodColumns(a.Class, method); !ok {
		if IsRowMethodOf(a.Class, method) {
			return nil, inputErr("%s scores rows: SELECT * FROM TABLE(model!%s(INPUT_DATA => TABLE(t))), or "+
				"model!%s(INPUT_DATA => {*}) in a query", method, method, method)
		}
		return nil, inputErr("%s models have no method %s; they have %s", QualifiedClass(a.Class), method, joinMethods(a.Class))
	}
	if m := a.igloo(); m != nil {
		return m.method(a.Class, method)
	}
	return nil, inputErr("the model holds nothing to compute %s from", method)
}

// EvaluateOn computes EVALUATE on rows the user gives.
func (a *Artifact) EvaluateOn(t *Table) (*Result, error) {
	cols, ok := iglooMethodColumns(a.Class, "EVALUATE")
	if !ok {
		return nil, inputErr("%s models have no EVALUATE", QualifiedClass(a.Class))
	}
	var e *Evaluation
	var err error
	if m := a.igloo(); m != nil {
		e, err = m.evaluateOn(t)
	}
	if err != nil {
		return nil, err
	}
	if e == nil {
		return nil, inputErr("nothing to evaluate")
	}
	return &Result{Columns: cols, Rows: [][]any{evaluationRow(e, cols)}}, nil
}
