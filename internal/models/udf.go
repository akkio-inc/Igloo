package models

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/ml"
)

// Scoring in DuckDB:
//
//	igloo_ml_predict(sha, feature, ...)          → VARCHAR, PREDICT's JSON object
//	igloo_ml_predict(sha!METHOD{...}, feature, ...) → another row method's (rows.go)
//	igloo_ml_result(token)                       → a method's result, as a table
//
// igloo_ml_predict reads the model whose blob has SHA-256 sha, and its
// features in the model's order: numbers as DOUBLE, categories as VARCHAR
// (package pgwire writes the calls; see its mlrewrite.go). Workers have it
// too, and fetch blobs from the warehouse, so a statement that scores rows
// runs on them like any other. igloo_ml_result is the server's: it reads
// what a model's method computed for a statement before DuckDB prepared it.
const (
	PredictFunction = "igloo_ml_predict"
	ResultFunction  = "igloo_ml_result"
	// ScoreFunction scores the model a statement's ai_predict_class or
	// ai_predict_value trained, which only the server that trained it
	// has: {"class": ..., "confidence": p} or {"value": v}.
	ScoreFunction = "igloo_ml_score"
)

// RegisterScoring defines igloo_ml_predict on a DuckDB database.
func (r *Runtime) RegisterScoring(conn *sql.Conn) error {
	return duckdb.RegisterScalarUDF(conn, PredictFunction, &predictUDF{r: r})
}

// RegisterServer defines igloo_ml_predict, igloo_ml_score and
// igloo_ml_result.
func (r *Runtime) RegisterServer(conn *sql.Conn) error {
	if err := r.RegisterScoring(conn); err != nil {
		return err
	}
	if err := duckdb.RegisterScalarUDF(conn, ScoreFunction, &predictUDF{r: r, score: true}); err != nil {
		return err
	}
	varchar, err := duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
	if err != nil {
		return err
	}
	return duckdb.RegisterTableUDF(conn, ResultFunction, duckdb.RowTableFunction{
		Config: duckdb.TableFunctionConfig{Arguments: []duckdb.TypeInfo{varchar}},
		BindArguments: func(_ map[string]any, args ...any) (duckdb.RowTableSource, error) {
			token, _ := args[0].(string)
			res, err := r.result(token)
			if err != nil {
				return nil, err
			}
			cols, err := columnInfos(res.Columns)
			if err != nil {
				return nil, err
			}
			return &resultSource{res: res, cols: cols}, nil
		},
	})
}

func columnInfos(cols []ResultColumn) ([]duckdb.ColumnInfo, error) {
	out := make([]duckdb.ColumnInfo, len(cols))
	for i, c := range cols {
		t := duckdb.TYPE_VARCHAR
		switch c.Kind {
		case KindDouble:
			t = duckdb.TYPE_DOUBLE
		case KindBigint:
			t = duckdb.TYPE_BIGINT
		case KindBoolean:
			t = duckdb.TYPE_BOOLEAN
		case KindTimestamp:
			t = duckdb.TYPE_TIMESTAMP
		}
		info, err := duckdb.NewTypeInfo(t)
		if err != nil {
			return nil, err
		}
		// DuckDB's columns are named as tables' are, in lower case; the
		// Snowflake dialect shows them in upper case.
		out[i] = duckdb.ColumnInfo{Name: lower(c.Name), T: info}
	}
	return out, nil
}

func lower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 'a' - 'A'
		}
	}
	return string(b)
}

type resultSource struct {
	res  *Result
	cols []duckdb.ColumnInfo
	pos  int
}

func (s *resultSource) ColumnInfos() []duckdb.ColumnInfo { return s.cols }
func (s *resultSource) Cardinality() *duckdb.CardinalityInfo {
	return &duckdb.CardinalityInfo{Cardinality: uint(len(s.res.Rows)), Exact: true}
}
func (s *resultSource) Init() {}

func (s *resultSource) FillRow(row duckdb.Row) (bool, error) {
	if s.pos >= len(s.res.Rows) {
		return false, nil
	}
	for i, v := range s.res.Rows[s.pos] {
		if f, ok := v.(float64); ok && (math.IsNaN(f) || math.IsInf(f, 0)) {
			v = nil
		}
		if err := row.SetRowValue(i, v); err != nil {
			return false, err
		}
	}
	s.pos++
	return true, nil
}

// predictUDF is igloo_ml_predict.
type predictUDF struct {
	r *Runtime
	// score makes it igloo_ml_score.
	score bool
	last  atomic.Pointer[lastModel]
}

func (f *predictUDF) Config() duckdb.ScalarFuncConfig {
	varchar, _ := duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
	anyType, _ := duckdb.NewTypeInfo(duckdb.TYPE_ANY)
	return duckdb.ScalarFuncConfig{
		InputTypeInfos:      []duckdb.TypeInfo{varchar},
		VariadicTypeInfo:    anyType,
		ResultTypeInfo:      varchar,
		SpecialNullHandling: true,
	}
}

// Executor scores a row at a time. It uses neither a bind function nor a
// context: duckdb-go's binding folds constant arguments, and aborts the
// process on a constant NULL (duckdb_get_double of a NULL value), which
// a feature's value may well be; and a context kept from binding ends
// before a prepared statement runs again. The model is loaded as rows
// arrive, from the runtime's cache.
func (f *predictUDF) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{RowExecutor: func(values []driver.Value) (any, error) {
		spec, _ := values[0].(string)
		lm, err := f.model(spec)
		if err != nil {
			return nil, err
		}
		art := lm.art
		if lm.method != nil {
			return f.scoreRow(lm, values[1:])
		}
		gbm, classes := (*ml.GBM)(nil), 1
		if c := art.Classifier; c != nil {
			gbm, classes = c.Model, len(c.Classes)
		} else {
			gbm = art.Regressor.Model
		}
		feats := art.Features()
		if len(values)-1 != len(feats) {
			return nil, fmt.Errorf("the model has %d features, not %d", len(feats), len(values)-1)
		}
		s := scratchPool.Get().(*scratch)
		defer scratchPool.Put(s)
		s.size(len(feats), len(gbm.Init), classes)
		for j := range feats {
			s.bins[j] = binValue(gbm.Features[j], values[j+1])
		}
		if f.score {
			s.buf = art.scoreJSON(s.buf[:0], s)
			return string(s.buf), nil
		}
		c := art.Classifier
		c.PredictRow(s.bins, s.raw, s.probs)
		s.buf = c.PredictJSON(s.buf[:0], s.probs)
		return string(s.buf), nil
	}}
}

// model returns the model, and the method, spec names (see rows.go): the
// one the last row used, without a lock, or one from the runtime.
func (f *predictUDF) model(spec string) (*lastModel, error) {
	if last := f.last.Load(); last != nil && last.spec == spec {
		return last, nil
	}
	sha, method, cfg, err := ParseScoreSpec(spec)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	art, err := f.r.Load(ctx, sha)
	switch {
	case errors.Is(err, ErrNoBlob) && f.score:
		return nil, errors.New("the model this statement trained is gone; run the statement again")
	case errors.Is(err, ErrNoBlob):
		return nil, errors.New("the model was dropped or replaced since this statement was prepared; run it again")
	case err != nil:
		return nil, err
	}
	lm := &lastModel{spec: spec, art: art}
	switch {
	case art.rowModel() != nil && !f.score:
		if lm.method, err = art.RowMethod(method, cfg); err != nil {
			return nil, err
		}
		lm.kinds = make([]ml.Kind, len(lm.method.Features))
		for j, ft := range lm.method.Features {
			lm.kinds[j] = featureKind(ft)
		}
	case method != "PREDICT":
		return nil, fmt.Errorf("%s models have no method %s", QualifiedClass(art.Class), method)
	case art.Classifier == nil && (!f.score || art.Regressor == nil):
		return nil, fmt.Errorf("%s scores classification models, not %s", PredictFunction, art.Class)
	}
	f.last.Store(lm)
	return lm, nil
}

type lastModel struct {
	spec string
	art  *Artifact
	// method is an Igloo model's row method, with its features' kinds.
	method *RowMethod
	kinds  []ml.Kind
}

// scoreRow scores a row with an Igloo model's row method.
func (f *predictUDF) scoreRow(lm *lastModel, values []driver.Value) (any, error) {
	if len(values) != len(lm.kinds) {
		return nil, fmt.Errorf("the model's %s reads %d features, not %d", lm.method.Method, len(lm.kinds), len(values))
	}
	s := scratchPool.Get().(*scratch)
	defer scratchPool.Put(s)
	ToRow(values, lm.kinds, &s.row)
	s.buf = lm.method.Score(lm.art, s.buf[:0], &s.row)
	return string(s.buf), nil
}

// ToRow reads DuckDB's values for features of the kinds given into r.
func ToRow(values []driver.Value, kinds []ml.Kind, r *ml.RowValues) {
	r.Size(len(kinds))
	for j, k := range kinds {
		v := values[j]
		r.Missing[j] = v == nil
		r.Num[j], r.Str[j] = math.NaN(), ""
		if v == nil {
			continue
		}
		if k == ml.Numeric {
			if x, ok := number(v); ok {
				r.Num[j] = x
			}
			continue
		}
		switch s := v.(type) {
		case string:
			r.Str[j] = s
		case bool:
			r.Str[j] = "false"
			if s {
				r.Str[j] = "true"
			}
		default:
			r.Str[j] = fmt.Sprint(v)
		}
	}
}

// scratch holds a row's buffers.
type scratch struct {
	bins       []uint8
	raw, probs []float64
	buf        []byte
	row        ml.RowValues
}

func (s *scratch) size(features, raw, classes int) {
	if cap(s.bins) < features {
		s.bins = make([]uint8, features)
	}
	if cap(s.raw) < raw {
		s.raw = make([]float64, raw)
	}
	if cap(s.probs) < classes {
		s.probs = make([]float64, classes)
	}
	s.bins, s.raw, s.probs = s.bins[:features], s.raw[:raw], s.probs[:classes]
}

var scratchPool = sync.Pool{New: func() any { return &scratch{} }}

// binValue bins a value DuckDB passed for a feature: a number for a
// numeric feature, text for a categorical one.
func binValue(b *ml.Binner, v driver.Value) uint8 {
	if v == nil {
		return ml.MissingBin
	}
	if b.Kind == ml.Numeric {
		f, ok := number(v)
		if !ok {
			return ml.MissingBin
		}
		return b.NumBin(f)
	}
	switch s := v.(type) {
	case string:
		return b.CatBin(s)
	case bool:
		if s {
			return b.CatBin("true")
		}
		return b.CatBin("false")
	}
	return b.CatBin(fmt.Sprint(v))
}

func number(v driver.Value) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, !math.IsNaN(x)
	case float32:
		return float64(x), true
	case int64:
		return float64(x), true
	case int32:
		return float64(x), true
	case int16:
		return float64(x), true
	case int8:
		return float64(x), true
	case uint64:
		return float64(x), true
	case uint32:
		return float64(x), true
	case uint16:
		return float64(x), true
	case uint8:
		return float64(x), true
	case int:
		return float64(x), true
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	case time.Time:
		return float64(x.UnixMicro()) / 1e6, true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	case duckdb.Decimal:
		return x.Float64(), true
	}
	return 0, false
}
