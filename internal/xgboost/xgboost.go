// Package xgboost wraps the part of XGBoost's C API (libxgboost) that
// IGLOO.ML.XGBOOST trains and serves models with: matrices of features,
// boosters, prediction, and models saved as XGBoost saves them. The library
// is linked into the binary statically; third_party/xgboost/build.sh builds
// it and writes the pkg-config file cgo links it by (igloo-xgboost). Go's
// build cache does not see that file change: after building XGBoost with
// other link flags, run go clean -cache.
//
// XGBoost keeps its last error, and the buffers it returns results in, per
// thread, and a goroutine can move to another thread between two cgo calls.
// So each call below is a small C function that makes XGBoost's call and
// copies out what it returned, its error included, before it returns to
// Go. Every non-zero return is an *Error carrying XGBoost's message.
//
// Prediction is safe from many goroutines at once on one Booster (XGBoost's
// tree boosters predict thread-safely); training, setting parameters and
// loading configuration are not, and must not overlap anything else done
// with the same Booster.
package xgboost

/*
#cgo pkg-config: igloo-xgboost
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <xgboost/c_api.h>

static int igloo_fail(int rc, char **err) {
	if (rc != 0) {
		*err = strdup(XGBGetLastError());
	}
	return rc;
}

static int igloo_dmatrix(const float *data, bst_ulong rows, bst_ulong cols, DMatrixHandle *out, char **err) {
	return igloo_fail(XGDMatrixCreateFromMat(data, rows, cols, NAN, out), err);
}

static int igloo_set_float(DMatrixHandle m, const char *field, const float *v, bst_ulong n, char **err) {
	return igloo_fail(XGDMatrixSetFloatInfo(m, field, v, n), err);
}

static int igloo_set_names(DMatrixHandle m, const char **names, bst_ulong n, char **err) {
	return igloo_fail(XGDMatrixSetStrFeatureInfo(m, "feature_name", names, n), err);
}

static int igloo_booster_names(BoosterHandle b, const char **names, bst_ulong n, char **err) {
	return igloo_fail(XGBoosterSetStrFeatureInfo(b, "feature_name", names, n), err);
}

static int igloo_booster(DMatrixHandle *cache, bst_ulong n, BoosterHandle *out, char **err) {
	return igloo_fail(XGBoosterCreate(cache, n, out), err);
}

static int igloo_load(const void *buf, bst_ulong len, BoosterHandle *out, char **err) {
	int rc = XGBoosterCreate(NULL, 0, out);
	if (rc != 0) {
		return igloo_fail(rc, err);
	}
	rc = XGBoosterLoadModelFromBuffer(*out, buf, len);
	if (rc != 0) {
		igloo_fail(rc, err);
		XGBoosterFree(*out);
	}
	return rc;
}

static int igloo_set_param(BoosterHandle b, const char *name, const char *value, char **err) {
	return igloo_fail(XGBoosterSetParam(b, name, value), err);
}

// XGBoost logs through a callback each thread registers for itself. While
// a round trains, the thread's warnings are kept for Update to return
// (XGBoost warns of parameters nothing used there); otherwise they go to
// standard error, as XGBoost's own callback sends them.
static __thread char *igloo_log_buf;
static __thread size_t igloo_log_len;
static __thread int igloo_log_keep;

static void igloo_log(const char *msg) {
	if (!igloo_log_keep) {
		fprintf(stderr, "%s\n", msg);
		return;
	}
	size_t n = strlen(msg);
	if (igloo_log_len + n + 2 > 65536) {
		return;
	}
	char *grown = realloc(igloo_log_buf, igloo_log_len + n + 2);
	if (grown == NULL) {
		return;
	}
	igloo_log_buf = grown;
	memcpy(igloo_log_buf + igloo_log_len, msg, n);
	igloo_log_len += n;
	igloo_log_buf[igloo_log_len++] = '\n';
	igloo_log_buf[igloo_log_len] = 0;
}

static int igloo_update(BoosterHandle b, int iter, DMatrixHandle train, char **log, char **err) {
	XGBRegisterLogCallback(igloo_log);
	igloo_log_keep = 1;
	int rc = XGBoosterUpdateOneIter(b, iter, train);
	igloo_log_keep = 0;
	*log = igloo_log_buf;
	igloo_log_buf = NULL;
	igloo_log_len = 0;
	return igloo_fail(rc, err);
}

static int igloo_eval(BoosterHandle b, int iter, DMatrixHandle *sets, const char **names, bst_ulong n, char **out, char **err) {
	const char *res;
	int rc = XGBoosterEvalOneIter(b, iter, sets, names, n, &res);
	if (rc != 0) {
		return igloo_fail(rc, err);
	}
	*out = strdup(res);
	return 0;
}

static int igloo_predict(BoosterHandle b, DMatrixHandle m, const char *config, float **out, bst_ulong *shape, bst_ulong *dim,
	char **err) {
	const bst_ulong *s;
	bst_ulong d;
	const float *res;
	int rc = XGBoosterPredictFromDMatrix(b, m, config, &s, &d, &res);
	if (rc != 0) {
		return igloo_fail(rc, err);
	}
	if (d > 4) {
		*err = strdup("XGBoost's prediction has more than 4 dimensions");
		return -1;
	}
	bst_ulong total = 1;
	for (bst_ulong i = 0; i < d; i++) {
		shape[i] = s[i];
		total *= s[i];
	}
	*dim = d;
	*out = malloc(total > 0 ? total * sizeof(float) : 1);
	if (*out == NULL) {
		*err = strdup("out of memory copying XGBoost's prediction");
		return -1;
	}
	memcpy(*out, res, total * sizeof(float));
	return 0;
}

static int igloo_save(BoosterHandle b, const char *config, char **out, bst_ulong *len, char **err) {
	const char *buf;
	int rc = XGBoosterSaveModelToBuffer(b, config, len, &buf);
	if (rc != 0) {
		return igloo_fail(rc, err);
	}
	*out = malloc(*len > 0 ? *len : 1);
	if (*out == NULL) {
		*err = strdup("out of memory copying XGBoost's model");
		return -1;
	}
	memcpy(*out, buf, *len);
	return 0;
}

static int igloo_save_config(BoosterHandle b, char **out, char **err) {
	bst_ulong len;
	const char *s;
	int rc = XGBoosterSaveJsonConfig(b, &len, &s);
	if (rc != 0) {
		return igloo_fail(rc, err);
	}
	*out = strndup(s, len);
	return 0;
}

static int igloo_load_config(BoosterHandle b, const char *config, char **err) {
	return igloo_fail(XGBoosterLoadJsonConfig(b, config), err);
}

static int igloo_set_attr(BoosterHandle b, const char *key, const char *value, char **err) {
	return igloo_fail(XGBoosterSetAttr(b, key, value), err);
}

static int igloo_slice(BoosterHandle b, int begin, int end, BoosterHandle *out, char **err) {
	return igloo_fail(XGBoosterSlice(b, begin, end, 1, out), err);
}

static int igloo_rounds(BoosterHandle b, int *out, char **err) {
	return igloo_fail(XGBoosterBoostedRounds(b, out), err);
}

static int igloo_feature_score(BoosterHandle b, const char *config, char ***names, float **scores, bst_ulong *n, char **err) {
	bst_ulong nf, dim;
	const char **fs;
	const bst_ulong *shape;
	const float *sc;
	int rc = XGBoosterFeatureScore(b, config, &nf, &fs, &dim, &shape, &sc);
	if (rc != 0) {
		return igloo_fail(rc, err);
	}
	bst_ulong total = 1;
	for (bst_ulong i = 0; i < dim; i++) {
		total *= shape[i];
	}
	if (total != nf) {
		*err = strdup("XGBoost gave a score per feature and output; Igloo reads one per feature");
		return -1;
	}
	*names = calloc(nf > 0 ? nf : 1, sizeof(char *));
	*scores = malloc(nf > 0 ? nf * sizeof(float) : 1);
	if (*names == NULL || *scores == NULL) {
		free(*names);
		free(*scores);
		*err = strdup("out of memory copying XGBoost's feature scores");
		return -1;
	}
	for (bst_ulong i = 0; i < nf; i++) {
		(*names)[i] = strdup(fs[i]);
	}
	memcpy(*scores, sc, nf * sizeof(float));
	*n = nf;
	return 0;
}
*/
import "C"

import (
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"unsafe"
)

// Error is a failed call of XGBoost's: Message is XGBoost's own, without
// the time, source line and stack trace it adds.
type Error struct{ Message string }

func (e *Error) Error() string { return e.Message }

// check turns a call's return code and copied error into a Go error.
func check(rc C.int, cerr *C.char) error {
	if rc == 0 {
		return nil
	}
	msg := "XGBoost failed and gave no message"
	if cerr != nil {
		msg = cleanMessage(C.GoString(cerr))
		C.free(unsafe.Pointer(cerr))
	}
	return &Error{Message: msg}
}

// cleanMessage keeps what an XGBoost error says: it starts each message
// with "[hh:mm:ss] file:line: " and may end it with a stack trace.
func cleanMessage(s string) string {
	if i := strings.Index(s, "\nStack trace:"); i >= 0 {
		s = s[:i]
	}
	if strings.HasPrefix(s, "[") {
		if i := strings.Index(s, "] "); i > 0 {
			rest := s[i+2:]
			// file:line: message
			if j := strings.Index(rest, ": "); j > 0 && strings.Contains(rest[:j], ":") && !strings.Contains(rest[:j], " ") {
				rest = rest[j+2:]
			}
			s = rest
		}
	}
	return strings.TrimSpace(s)
}

// Version is the XGBoost release the binary links, such as 3.4.2.
func Version() string {
	var major, minor, patch C.int
	C.XGBoostVersion(&major, &minor, &patch)
	return fmt.Sprintf("%d.%d.%d", major, minor, patch)
}

// Matrix is XGBoost's DMatrix: rows of features, and the labels and
// weights that go with them. Free it when done.
type Matrix struct {
	h          C.DMatrixHandle
	rows, cols int
	cleanup    runtime.Cleanup
}

// NewMatrix makes a matrix of rows rows of cols features each, from values
// in row-major order. NaN is a missing value, as in XGBoost's Python
// package.
func NewMatrix(values []float32, rows, cols int) (*Matrix, error) {
	if rows <= 0 || cols <= 0 || len(values) != rows*cols {
		return nil, fmt.Errorf("xgboost: a matrix of %d rows and %d columns needs %d values, not %d", rows, cols, rows*cols, len(values))
	}
	m := &Matrix{rows: rows, cols: cols}
	var cerr *C.char
	if err := check(C.igloo_dmatrix((*C.float)(unsafe.Pointer(&values[0])), C.bst_ulong(rows), C.bst_ulong(cols), &m.h, &cerr), cerr); err != nil {
		return nil, err
	}
	m.cleanup = runtime.AddCleanup(m, func(h C.DMatrixHandle) { C.XGDMatrixFree(h) }, m.h)
	return m, nil
}

// Rows is the number of rows.
func (m *Matrix) Rows() int { return m.rows }

// SetFloatInfo sets one of the rows' fields: label, weight,
// label_lower_bound or label_upper_bound.
func (m *Matrix) SetFloatInfo(field string, v []float32) error {
	if len(v) != m.rows {
		return fmt.Errorf("xgboost: %s has %d values for %d rows", field, len(v), m.rows)
	}
	f := C.CString(field)
	defer C.free(unsafe.Pointer(f))
	var cerr *C.char
	err := check(C.igloo_set_float(m.h, f, (*C.float)(unsafe.Pointer(&v[0])), C.bst_ulong(len(v)), &cerr), cerr)
	runtime.KeepAlive(m)
	return err
}

// SetFeatureNames names the features, which the booster records.
func (m *Matrix) SetFeatureNames(names []string) error {
	if len(names) != m.cols {
		return fmt.Errorf("xgboost: %d names for %d features", len(names), m.cols)
	}
	cs, free := cStrings(names)
	defer free()
	var cerr *C.char
	err := check(C.igloo_set_names(m.h, cs, C.bst_ulong(len(names)), &cerr), cerr)
	runtime.KeepAlive(m)
	return err
}

// Free frees the matrix.
func (m *Matrix) Free() {
	if m != nil && m.h != nil {
		m.cleanup.Stop()
		C.XGDMatrixFree(m.h)
		m.h = nil
	}
}

// cStrings copies strs into C memory, as an array of C strings, and
// returns a function that frees it.
func cStrings(strs []string) (**C.char, func()) {
	p := (**C.char)(C.malloc(C.size_t(max(len(strs), 1)) * C.size_t(unsafe.Sizeof((*C.char)(nil)))))
	arr := unsafe.Slice(p, len(strs))
	for i, s := range strs {
		arr[i] = C.CString(s)
	}
	return p, func() {
		for _, s := range arr {
			C.free(unsafe.Pointer(s))
		}
		C.free(unsafe.Pointer(p))
	}
}

// Booster is an XGBoost model, trained here or loaded. Free it when done;
// one no longer reachable is freed by the garbage collector.
type Booster struct {
	h       C.BoosterHandle
	cleanup runtime.Cleanup
}

func newBooster(h C.BoosterHandle) *Booster {
	b := &Booster{h: h}
	b.cleanup = runtime.AddCleanup(b, func(h C.BoosterHandle) { C.XGBoosterFree(h) }, h)
	return b
}

// NewBooster makes a booster to train, caching predictions for the
// matrices given (the training rows and those it is evaluated on), as
// XGBoost's Python package does.
func NewBooster(cache ...*Matrix) (*Booster, error) {
	hs := make([]C.DMatrixHandle, max(len(cache), 1))
	for i, m := range cache {
		hs[i] = m.h
	}
	var h C.BoosterHandle
	var cerr *C.char
	if err := check(C.igloo_booster(&hs[0], C.bst_ulong(len(cache)), &h, &cerr), cerr); err != nil {
		return nil, err
	}
	runtime.KeepAlive(cache)
	return newBooster(h), nil
}

// LoadBooster loads a model Save saved (or XGBoost's Python package did).
func LoadBooster(model []byte) (*Booster, error) {
	if len(model) == 0 {
		return nil, fmt.Errorf("xgboost: no model to load")
	}
	var h C.BoosterHandle
	var cerr *C.char
	buf := C.CBytes(model)
	defer C.free(buf)
	if err := check(C.igloo_load(buf, C.bst_ulong(len(model)), &h, &cerr), cerr); err != nil {
		return nil, err
	}
	return newBooster(h), nil
}

// SetFeatureNames names the model's features, as XGBoost's Python package
// names them after its training matrix's.
func (b *Booster) SetFeatureNames(names []string) error {
	cs, free := cStrings(names)
	defer free()
	var cerr *C.char
	err := check(C.igloo_booster_names(b.h, cs, C.bst_ulong(len(names)), &cerr), cerr)
	runtime.KeepAlive(b)
	return err
}

// SetParam sets one of XGBoost's parameters, by its name, as text.
func (b *Booster) SetParam(name, value string) error {
	n, v := C.CString(name), C.CString(value)
	defer C.free(unsafe.Pointer(n))
	defer C.free(unsafe.Pointer(v))
	var cerr *C.char
	err := check(C.igloo_set_param(b.h, n, v, &cerr), cerr)
	runtime.KeepAlive(b)
	return err
}

// Update trains one round, round iter (from 0), on train, and returns the
// warnings XGBoost logged meanwhile.
func (b *Booster) Update(iter int, train *Matrix) (warnings []string, err error) {
	var log, cerr *C.char
	err = check(C.igloo_update(b.h, C.int(iter), train.h, &log, &cerr), cerr)
	runtime.KeepAlive(b)
	runtime.KeepAlive(train)
	if log != nil {
		for line := range strings.SplitSeq(C.GoString(log), "\n") {
			if line = cleanMessage(line); line != "" {
				warnings = append(warnings, line)
			}
		}
		C.free(unsafe.Pointer(log))
	}
	return warnings, err
}

// Eval evaluates the model after round iter on the matrices given, named
// by names, and returns XGBoost's line of results, as
// "[iter]\tname-metric:value...". ParseEval reads it.
func (b *Booster) Eval(iter int, sets []*Matrix, names []string) (string, error) {
	if len(sets) == 0 || len(sets) != len(names) {
		return "", fmt.Errorf("xgboost: evaluating %d matrices named %d ways", len(sets), len(names))
	}
	hs := make([]C.DMatrixHandle, len(sets))
	for i, m := range sets {
		hs[i] = m.h
	}
	cs, free := cStrings(names)
	defer free()
	var out, cerr *C.char
	if err := check(C.igloo_eval(b.h, C.int(iter), &hs[0], cs, C.bst_ulong(len(sets)), &out, &cerr), cerr); err != nil {
		return "", err
	}
	runtime.KeepAlive(b)
	runtime.KeepAlive(sets)
	defer C.free(unsafe.Pointer(out))
	return C.GoString(out), nil
}

// PredictType is what Predict computes, as XGBoost numbers it.
type PredictType int

const (
	// Value is the model's prediction: a probability, a value, a class.
	Value PredictType = 0
	// Margin is the raw score before the objective's transformation.
	Margin PredictType = 1
	// Contributions are each feature's SHAP value, in margin space, and
	// the bias last.
	Contributions PredictType = 2
)

// Prediction is Predict's result, in row-major order, with its shape:
// (rows, outputs) for values and margins, (rows, outputs, features + 1)
// for contributions.
type Prediction struct {
	Values []float32
	Shape  []int
}

// Predict predicts m's rows with the trees of rounds [0, end), or every
// tree when end is 0, with XGBoost's strict shapes.
func (b *Booster) Predict(m *Matrix, typ PredictType, end int) (*Prediction, error) {
	cfg := C.CString(`{"type": ` + strconv.Itoa(int(typ)) + `, "training": false, "iteration_begin": 0, "iteration_end": ` +
		strconv.Itoa(end) + `, "strict_shape": true}`)
	defer C.free(unsafe.Pointer(cfg))
	var out *C.float
	var shape [4]C.bst_ulong
	var dim C.bst_ulong
	var cerr *C.char
	if err := check(C.igloo_predict(b.h, m.h, cfg, &out, &shape[0], &dim, &cerr), cerr); err != nil {
		return nil, err
	}
	runtime.KeepAlive(b)
	runtime.KeepAlive(m)
	defer C.free(unsafe.Pointer(out))
	p := &Prediction{Shape: make([]int, int(dim))}
	total := 1
	for i := range p.Shape {
		p.Shape[i] = int(shape[i])
		total *= p.Shape[i]
	}
	p.Values = make([]float32, total)
	copy(p.Values, unsafe.Slice((*float32)(unsafe.Pointer(out)), total))
	return p, nil
}

// Save returns the model as XGBoost saves it, in Universal Binary JSON.
func (b *Booster) Save() ([]byte, error) {
	cfg := C.CString(`{"format": "ubj"}`)
	defer C.free(unsafe.Pointer(cfg))
	var out, cerr *C.char
	var n C.bst_ulong
	if err := check(C.igloo_save(b.h, cfg, &out, &n, &cerr), cerr); err != nil {
		return nil, err
	}
	runtime.KeepAlive(b)
	defer C.free(unsafe.Pointer(out))
	return C.GoBytes(unsafe.Pointer(out), C.int(n)), nil
}

// SaveConfig returns XGBoost's configuration of the booster, as JSON: its
// parameters, objective and metrics, which a saved model leaves out.
func (b *Booster) SaveConfig() (string, error) {
	var out, cerr *C.char
	if err := check(C.igloo_save_config(b.h, &out, &cerr), cerr); err != nil {
		return "", err
	}
	runtime.KeepAlive(b)
	defer C.free(unsafe.Pointer(out))
	return C.GoString(out), nil
}

// LoadConfig restores a configuration SaveConfig returned.
func (b *Booster) LoadConfig(config string) error {
	c := C.CString(config)
	defer C.free(unsafe.Pointer(c))
	var cerr *C.char
	err := check(C.igloo_load_config(b.h, c, &cerr), cerr)
	runtime.KeepAlive(b)
	return err
}

// SetAttr sets one of the model's attributes, which saving keeps.
func (b *Booster) SetAttr(key, value string) error {
	k, v := C.CString(key), C.CString(value)
	defer C.free(unsafe.Pointer(k))
	defer C.free(unsafe.Pointer(v))
	var cerr *C.char
	err := check(C.igloo_set_attr(b.h, k, v, &cerr), cerr)
	runtime.KeepAlive(b)
	return err
}

// Slice returns a new booster of the trees of rounds [begin, end).
func (b *Booster) Slice(begin, end int) (*Booster, error) {
	var h C.BoosterHandle
	var cerr *C.char
	if err := check(C.igloo_slice(b.h, C.int(begin), C.int(end), &h, &cerr), cerr); err != nil {
		return nil, err
	}
	runtime.KeepAlive(b)
	return newBooster(h), nil
}

// Rounds is the number of rounds the model was trained for.
func (b *Booster) Rounds() (int, error) {
	var n C.int
	var cerr *C.char
	if err := check(C.igloo_rounds(b.h, &n, &cerr), cerr); err != nil {
		return 0, err
	}
	runtime.KeepAlive(b)
	return int(n), nil
}

// FeatureScore returns each feature's importance of the given type, as
// XGBoost computes it ("weight", "gain", "cover", "total_gain",
// "total_cover"), by the feature's name. Features no split uses are left
// out.
func (b *Booster) FeatureScore(importance string) (map[string]float32, error) {
	cfg := C.CString(`{"importance_type": ` + strconv.Quote(importance) + `}`)
	defer C.free(unsafe.Pointer(cfg))
	var names **C.char
	var scores *C.float
	var n C.bst_ulong
	var cerr *C.char
	if err := check(C.igloo_feature_score(b.h, cfg, &names, &scores, &n, &cerr), cerr); err != nil {
		return nil, err
	}
	runtime.KeepAlive(b)
	defer C.free(unsafe.Pointer(names))
	defer C.free(unsafe.Pointer(scores))
	ns := unsafe.Slice(names, int(n))
	ss := unsafe.Slice((*float32)(unsafe.Pointer(scores)), int(n))
	out := make(map[string]float32, int(n))
	for i := range ns {
		out[C.GoString(ns[i])] = ss[i]
		C.free(unsafe.Pointer(ns[i]))
	}
	return out, nil
}

// Free frees the booster.
func (b *Booster) Free() {
	if b != nil && b.h != nil {
		b.cleanup.Stop()
		C.XGBoosterFree(b.h)
		b.h = nil
	}
}

// EvalResult is one metric of Eval's line: data's metric, its value.
type EvalResult struct {
	Data, Metric string
	Value        float64
}

// ParseEval reads Eval's line as XGBoost's Python package does: fields
// apart by white space after the round, each data-metric:value, the data's
// name up to the first '-'.
func ParseEval(line string) ([]EvalResult, error) {
	fields := strings.Fields(line)
	if len(fields) == 0 || !strings.HasPrefix(fields[0], "[") {
		return nil, fmt.Errorf("xgboost: unexpected evaluation %q", line)
	}
	var out []EvalResult
	for _, f := range fields[1:] {
		key, val, ok := strings.Cut(f, ":")
		data, metric, ok2 := strings.Cut(key, "-")
		if !ok || !ok2 {
			return nil, fmt.Errorf("xgboost: unexpected evaluation %q", f)
		}
		// C++ writes a NaN with its sign, which Python's float reads and
		// Go's does not.
		v, err := strconv.ParseFloat(val, 64)
		if err != nil && strings.EqualFold(strings.TrimLeft(val, "+-"), "nan") {
			v, err = math.NaN(), nil
		}
		if err != nil {
			return nil, fmt.Errorf("xgboost: unexpected evaluation %q: %w", f, err)
		}
		out = append(out, EvalResult{Data: data, Metric: metric, Value: v})
	}
	return out, nil
}
