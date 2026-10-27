package models

import (
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/akkio-inc/igloo/internal/ml"
)

// Row methods give a value per row: PREDICT, EXPLAIN_PREDICT,
// DETECT_ANOMALIES, RECOMMEND and RECONSTRUCTION_LOSS of Igloo's classes.
// igloo_ml_predict scores them as a query's rows are read, on the server
// or on workers. Its first argument names the model's blob and, after it,
// the method and its options:
//
//	<sha>                          PREDICT
//	<sha>!METHOD{"key": value}     another method, with its CONFIG_OBJECT
//
// A row method's result is a JSON object. Called as a table,
// TABLE(m!METHOD(INPUT_DATA => ...)), its rows are INPUT_DATA's with
// columns read from the object (RowTable).

// RowOptions are a row method's options, read from its CONFIG_OBJECT.
type RowOptions struct {
	// TopK is EXPLAIN_PREDICT's top_k_features and RECOMMEND's top_k.
	TopK int
	// Contamination is DETECT_ANOMALIES's share of training rows that are
	// anomalies; Threshold the distance or error above which a row is one.
	Contamination, Threshold float64
	// ExcludeSeen leaves out of RECOMMEND the items a user rated in
	// training.
	ExcludeSeen bool
}

// RowColumn is a column the table form of a row method adds: Name, as
// DuckDB names it, read from the method's object at JSON path Path as
// Type (DuckDB's: DOUBLE, BIGINT, BOOLEAN, VARCHAR, JSON or a target's).
type RowColumn struct {
	Name, Path, Type string
}

// RowTable lays out a row method's table: INPUT_DATA's columns, then
// Columns. Unnest, if set, is the path of an array in the method's object
// whose elements are rows of their own, Columns' paths then in them.
type RowTable struct {
	Columns []RowColumn
	Unnest  string
}

// RowMethod is a row method ready to score.
type RowMethod struct {
	Method   string
	Features []Feature
	Table    RowTable
	// Spec is igloo_ml_predict's first argument after the blob's SHA-256.
	Spec string
	opts *RowOptions
}

// rowModel is a model of one of Igloo's classes that scores rows.
type rowModel interface {
	// rowFeatures returns the features method reads, in order; ok is
	// false for a method that is not one of the model's row methods.
	rowFeatures(method string) (feats []Feature, ok bool)
	// rowOptions reads method's options.
	rowOptions(method string, cfg *config) (*RowOptions, error)
	// rowTable lays out method's table.
	rowTable(method string, opt *RowOptions) RowTable
	// scoreRow appends method's object for one row.
	scoreRow(buf []byte, method string, opt *RowOptions, r *ml.RowValues) []byte
}

func (a *Artifact) rowModel() rowModel {
	if m := a.igloo(); m != nil {
		return m
	}
	return nil
}

// RowMethods lists the model's row methods.
func (a *Artifact) RowMethods() []string {
	rm := a.rowModel()
	if rm == nil {
		return nil
	}
	var out []string
	for _, m := range []string{"PREDICT", "EXPLAIN_PREDICT", "DETECT_ANOMALIES", "RECOMMEND", "RECONSTRUCTION_LOSS"} {
		if _, ok := rm.rowFeatures(m); ok {
			out = append(out, m)
		}
	}
	return out
}

// IsRowMethod reports whether method is one of the model's row methods.
func (a *Artifact) IsRowMethod(method string) bool {
	rm := a.rowModel()
	if rm == nil {
		return false
	}
	_, ok := rm.rowFeatures(method)
	return ok
}

// RowMethod prepares method with options cfg (CONFIG_OBJECT's JSON values).
func (a *Artifact) RowMethod(method string, cfg map[string]any) (*RowMethod, error) {
	rm := a.rowModel()
	if rm == nil {
		return nil, inputErr("%s models do not score rows", QualifiedClass(a.Class))
	}
	feats, ok := rm.rowFeatures(method)
	if !ok {
		return nil, inputErr("%s models have no row method %s; they have %s", QualifiedClass(a.Class), method,
			strings.Join(a.RowMethods(), ", "))
	}
	c := newConfig(cfg)
	opt, err := rm.rowOptions(method, c)
	if err != nil {
		return nil, err
	}
	if err := c.unused(); err != nil {
		return nil, err
	}
	rmeth := &RowMethod{Method: method, Features: feats, Table: rm.rowTable(method, opt), opts: opt}
	if method != "PREDICT" || len(cfg) > 0 {
		rmeth.Spec = "!" + method
		if len(cfg) > 0 {
			b, err := json.Marshal(lowerKeys(cfg))
			if err != nil {
				return nil, err
			}
			rmeth.Spec += string(b)
		}
	}
	return rmeth, nil
}

func lowerKeys(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[strings.ToLower(k)] = v
	}
	return out
}

// TrainingInputs returns the rows a row method reads when it is called
// without INPUT_DATA, or nil when it needs INPUT_DATA.
func (a *Artifact) TrainingInputs(method string) *Result {
	if a.Factors != nil {
		return a.Factors.trainingInputs(method)
	}
	return nil
}

// ParseScoreSpec splits igloo_ml_predict's first argument into the blob's
// SHA-256, the method and its options.
func ParseScoreSpec(spec string) (sha, method string, cfg map[string]any, err error) {
	if len(spec) < 64 {
		return "", "", nil, inputErr("not a model: %q", spec)
	}
	sha, rest := spec[:64], spec[64:]
	if rest == "" {
		return sha, "PREDICT", nil, nil
	}
	if rest[0] != '!' {
		return "", "", nil, inputErr("not a model's method: %q", spec)
	}
	rest = rest[1:]
	method = rest
	if i := strings.IndexByte(rest, '{'); i >= 0 {
		method = rest[:i]
		if err := json.Unmarshal([]byte(rest[i:]), &cfg); err != nil {
			return "", "", nil, inputErr("bad options of %s: %v", method, err)
		}
	}
	return sha, method, cfg, nil
}

// Score appends a row method's object for one row of values, read as the
// method's features (ToRow).
func (m *RowMethod) Score(art *Artifact, buf []byte, r *ml.RowValues) []byte {
	return art.rowModel().scoreRow(buf, m.Method, m.opts, r)
}

// featureKinds are how features reach models: numbers and times as
// numbers, the rest as categories.
func featureKind(f Feature) ml.Kind {
	if f.Type == TypeNumber.String() || f.Type == TypeTime.String() {
		return ml.Numeric
	}
	return ml.Categorical
}

// --- JSON helpers -----------------------------------------------------------

// jsonText writes v as JSON, "null" if it cannot.
func jsonText(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "null"
	}
	return string(b)
}

// appendNum writes a number, null for NaN and infinities.
func appendNum(buf []byte, v float64) []byte {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return append(buf, "null"...)
	}
	return strconv.AppendFloat(buf, v, 'g', -1, 64)
}

// appendKey writes "key": after a comma unless first.
func appendKey(buf []byte, key string, first bool) []byte {
	if !first {
		buf = append(buf, ',')
	}
	buf = append(buf, jsonString(key)...)
	return append(buf, ':')
}

// attribution is a feature's share of a prediction.
type attribution struct {
	feature string
	value   float64
}

// appendAttributions writes the k largest attributions by size as
// [{"feature": ..., "attribution": ...}, ...].
func appendAttributions(buf []byte, attrs []attribution, k int) []byte {
	sort.SliceStable(attrs, func(i, j int) bool { return math.Abs(attrs[i].value) > math.Abs(attrs[j].value) })
	if k > 0 && len(attrs) > k {
		attrs = attrs[:k]
	}
	buf = append(buf, '[')
	for i, a := range attrs {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, `{"feature":`...)
		buf = append(buf, jsonString(a.feature)...)
		buf = append(buf, `,"attribution":`...)
		buf = appendNum(buf, a.value)
		buf = append(buf, '}')
	}
	return append(buf, ']')
}

// sqlName makes a column name of a prefix and a feature's name, as
// BigQuery's predicted_<label>.
func sqlName(parts ...string) string { return strings.ToLower(strings.Join(parts, "")) }

// contaminationOptions reads DETECT_ANOMALIES's contamination and sets the
// threshold of a model whose training rows' scores (distances or errors)
// have quantiles q (q[i] is the i/(len(q)-1) quantile).
func contaminationOptions(cfg *config, q []float64) (*RowOptions, error) {
	v, ok := cfg.get("contamination")
	f, isNum := v.(float64)
	if !ok || !isNum || f <= 0 || f > 0.5 {
		return nil, inputErr("DETECT_ANOMALIES needs CONFIG_OBJECT => {'contamination': x}, the share of anomalies, from 0 to 0.5 (more than 0)")
	}
	return &RowOptions{Contamination: f, Threshold: quantileOf(q, 1-f)}, nil
}

// quantiles returns the 0, 1/(k-1), ..., 1 quantiles of v (sorting it).
func quantiles(v []float64, k int) []float64 {
	if len(v) == 0 {
		return nil
	}
	sort.Float64s(v)
	out := make([]float64, k)
	for i := range k {
		pos := float64(i) / float64(k-1) * float64(len(v)-1)
		lo := int(math.Floor(pos))
		hi := min(lo+1, len(v)-1)
		out[i] = v[lo] + (v[hi]-v[lo])*(pos-float64(lo))
	}
	return out
}
