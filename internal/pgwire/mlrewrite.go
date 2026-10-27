package pgwire

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/models"
	"github.com/akkio-inc/igloo/internal/snowflake"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Models' methods in queries. The Snowflake dialect's translation writes
// them as calls Igloo resolves here, before the statement is checked and
// prepared (see snowflake/model.go):
//
//	igloo_ml_method('m!FORECAST(...)')   → igloo_ml_result('<token>'): the method's rows, computed now
//	igloo_ml_predict_ref('m', '*')       → igloo_ml_predict('<sha>', <the model's features>)
//
// Resolving checks that the user may use the model, by its name, each time
// the statement is prepared: a task or a query run again uses the model
// as it is then. igloo_ml_predict reads the model's blob by its SHA-256,
// on the server or on workers. Users may not call igloo_ml_predict or
// igloo_ml_result themselves: their arguments are only ever what this
// resolved, after its checks.

const (
	methodMarker  = "igloo_ml_method"
	predictMarker = "igloo_ml_predict_ref"
)

// maxModelResults bounds the results a session keeps for statements that
// may run again; older ones are dropped.
const maxModelResults = 64

// mlToken is a token of SQL code for the rewrite: a name (lower-cased),
// a quoted identifier, a string literal or a punctuation character.
type mlToken struct {
	kind       byte // 'w' word, 'i' quoted identifier, 's' string, 'p' punctuation
	text       string
	start, end int
}

func mlTokens(query string) []mlToken {
	var out []mlToken
	pos := 0
	for _, seg := range sqllex.Segments(query) {
		start := pos
		pos += len(seg.Text)
		switch seg.Kind {
		case sqllex.String:
			out = append(out, mlToken{kind: 's', text: seg.Text, start: start, end: pos})
		case sqllex.Ident:
			name := strings.ReplaceAll(strings.Trim(seg.Text, `"`), `""`, `"`)
			out = append(out, mlToken{kind: 'i', text: strings.ToLower(name), start: start, end: pos})
		case sqllex.Code:
			t := seg.Text
			for i := 0; i < len(t); {
				c := t[i]
				switch {
				case sqllex.IdentByte(c):
					j := i
					for j < len(t) && sqllex.IdentByte(t[j]) {
						j++
					}
					out = append(out, mlToken{kind: 'w', text: strings.ToLower(t[i:j]), start: start + i, end: start + j})
					i = j
				case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
					i++
				default:
					out = append(out, mlToken{kind: 'p', text: t[i : i+1], start: start + i, end: start + i + 1})
					i++
				}
			}
		}
	}
	return out
}

// mlCall is a call found in a query: its name, its span, and its
// arguments' spans.
type mlCall struct {
	name       string
	start, end int
	args       [][2]int
}

// mlCalls finds the calls of names in toks that are not inside another
// such call, by their span in query.
func mlCalls(toks []mlToken, names map[string]bool) ([]mlCall, error) {
	var out []mlCall
	for i := 0; i < len(toks); i++ {
		t := toks[i]
		if t.kind != 'w' && t.kind != 'i' || !names[t.text] || i+1 >= len(toks) || toks[i+1].text != "(" || toks[i+1].kind != 'p' {
			continue
		}
		c := mlCall{name: t.text, start: t.start}
		depth, argStart := 0, toks[i+1].end
		j := i + 1
		for ; j < len(toks); j++ {
			if toks[j].kind != 'p' {
				continue
			}
			switch toks[j].text {
			case "(", "[", "{":
				depth++
			case ")", "]", "}":
				depth--
			case ",":
				if depth == 1 {
					c.args = append(c.args, [2]int{argStart, toks[j].start})
					argStart = toks[j].end
				}
			}
			if depth == 0 {
				break
			}
		}
		if j >= len(toks) {
			return nil, pgerror.WithCode(fmt.Errorf("unclosed call of %s", t.text), codes.Syntax)
		}
		if argStart < toks[j].start || len(c.args) > 0 {
			c.args = append(c.args, [2]int{argStart, toks[j].start})
		}
		c.end = toks[j].end
		out = append(out, c)
		i = j
	}
	return out, nil
}

// stringLiteral reads a standard SQL string literal, as the translation
// writes them.
func stringLiteral(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '\'' || s[len(s)-1] != '\'' {
		return "", false
	}
	inner := s[1 : len(s)-1]
	if strings.Count(inner, "'")%2 != 0 || strings.Contains(strings.ReplaceAll(inner, "''", ""), "'") {
		return "", false
	}
	return strings.ReplaceAll(inner, "''", "'"), true
}

// keepsSQL reports what a statement that keeps SQL to run later does with
// models' methods in it: views and macros cannot have them (their SQL runs
// inside other statements, where nothing resolves them), and tasks,
// alerts and pipes keep them as written, resolved when they run.
func keepsSQL(query string) (keeps bool, refuse string) {
	words := sqllex.CodeWords(query)
	if len(words) < 2 || words[0] != "CREATE" {
		return false, ""
	}
	for _, w := range words[1:min(len(words), 6)] {
		switch w {
		case "VIEW", "MACRO", "FUNCTION":
			return true, w
		case "TASK", "ALERT", "PIPE":
			return true, ""
		case "MATERIALIZED":
			return true, "MATERIALIZED VIEW"
		case "TABLE", "SCHEMA", "SNOWFLAKE":
			return false, ""
		}
	}
	return false, ""
}

// modelRewrite resolves the calls of models' methods in query (see the
// top of this file), for user.
func (s *Server) modelRewrite(ctx context.Context, sess *session, user auth.User, query string) (string, error) {
	// Forecasting functions in DuckDB's dialect (see tsforecast.go).
	query, err := s.forecastRewrite(ctx, sess, user, query)
	if err != nil {
		return "", err
	}
	low := strings.ToLower(query)
	if !strings.Contains(low, "igloo_ml_") && !strings.Contains(low, "ai_predict_") {
		return query, nil
	}
	toks := mlTokens(query)
	calls, err := mlCalls(toks, map[string]bool{methodMarker: true, predictMarker: true, models.PredictFunction: true,
		models.ResultFunction: true, models.ScoreFunction: true, "ai_predict_class": true, "ai_predict_value": true})
	if err != nil {
		return query, err
	}
	// ai_predict_class and ai_predict_value written in DuckDB's dialect
	// are calls with named arguments; the same names otherwise, as a
	// table's, are left alone.
	calls = slices.DeleteFunc(calls, func(c mlCall) bool {
		return strings.HasPrefix(c.name, "ai_predict_") && !strings.Contains(query[c.start:c.end], "=>")
	})
	if len(calls) == 0 {
		return query, nil
	}
	keeps, refuse := keepsSQL(query)
	for _, c := range calls {
		switch c.name {
		case models.PredictFunction, models.ResultFunction, models.ScoreFunction:
			return "", pgerror.WithCode(fmt.Errorf("%s is Igloo's own: score rows with model!PREDICT(INPUT_DATA => {*}) "+
				"and read methods with TABLE(model!METHOD(...))", c.name), codes.InsufficientPrivilege)
		}
	}
	if refuse != "" {
		return "", pgerror.WithHint(pgerror.WithCode(fmt.Errorf("a %s cannot call a model's methods", strings.ToLower(refuse)),
			codes.FeatureNotSupported), "create a table from them (CREATE TABLE ... AS), or a task that does so on a schedule")
	}
	if keeps {
		return query, nil
	}
	var b strings.Builder
	last := 0
	for _, c := range calls {
		repl, err := s.resolveModelCall(ctx, sess, user, query, c)
		if err != nil {
			return "", err
		}
		b.WriteString(query[last:c.start])
		b.WriteString(repl)
		last = c.end
	}
	b.WriteString(query[last:])
	return b.String(), nil
}

// resolveModelCall writes what replaces one call.
func (s *Server) resolveModelCall(ctx context.Context, sess *session, user auth.User, query string, c mlCall) (string, error) {
	arg := func(i int) string { return strings.TrimSpace(query[c.args[i][0]:c.args[i][1]]) }
	switch c.name {
	case "ai_predict_class", "ai_predict_value":
		return s.resolveModelText(ctx, sess, user, query[c.start:c.end])
	case methodMarker:
		if len(c.args) != 1 {
			return "", modelSyntax("%s takes one string", methodMarker)
		}
		text, ok := stringLiteral(arg(0))
		if !ok {
			return "", modelSyntax("%s takes one string", methodMarker)
		}
		return s.resolveModelText(ctx, sess, user, text)
	case predictMarker:
		return s.resolvePredict(ctx, sess, user, query, c)
	}
	return "", fmt.Errorf("unexpected call of %s", c.name)
}

// resolveModelText writes what replaces a call of a model's method that
// returns a table, or of ai_predict_class or ai_predict_value, as written.
func (s *Server) resolveModelText(ctx context.Context, sess *session, user auth.User, text string) (string, error) {
	if repl, ok := sess.modelMemo[text]; ok {
		return repl, nil
	}
	if snowflake.IsForecastCall(text) {
		// AI.FORECAST and the like (see tsforecast.go).
		repl, err := s.forecastQuery(ctx, sess, user, text)
		if err != nil {
			return "", modelError(err)
		}
		return repl, nil
	}
	call, err := snowflake.ParseModelCall(text)
	if err != nil {
		return "", modelError(err)
	}
	remember := func(repl string) string {
		if sess.modelMemo == nil {
			sess.modelMemo = map[string]string{}
		}
		sess.modelMemo[text] = repl
		return repl
	}
	if call.Name == nil {
		repl, err := s.trainAndScore(ctx, sess, user, call)
		if err != nil {
			return "", modelError(err)
		}
		return remember(repl), nil
	}
	// An Igloo model's row method, as a table, scores INPUT_DATA's rows as
	// they are read.
	if repl, ok, err := s.rowMethodTable(ctx, sess, user, call); ok || err != nil {
		if err != nil {
			return "", modelError(err)
		}
		return remember(repl), nil
	}
	{
		var res *models.Result
		if res, err = s.modelMethod(ctx, sess, user, call); err != nil {
			return "", modelError(err)
		}
		token := s.modelRuntime.AddResult(res)
		sess.keepModelResult(s.modelRuntime, token)
		sess.allowModel(token)
		return remember(models.ResultFunction + "(" + quoteLiteral(token) + ")"), nil
	}
}

// trainAndScore carries out ai_predict_class or ai_predict_value: it
// trains a model on the input's rows whose target is set, keeps it for the
// session, and writes the query that scores the rows whose target is
// NULL, as Databricks' functions return them: every column of the input,
// then <target>_prediction (and <target>_confidence for classes).
func (s *Server) trainAndScore(ctx context.Context, sess *session, user auth.User, call *snowflake.ModelCall) (string, error) {
	fn := strings.ToLower(call.Method)
	if fn != "ai_predict_class" && fn != "ai_predict_value" {
		return "", pgerror.WithCode(fmt.Errorf("%s is not a model's method", fn), codes.UndefinedFunction)
	}
	if len(call.Args) > 0 && call.Args[0].Name == "" {
		call.Args[0].Name = "INPUT"
	}
	if err := onlyArgs(call, "INPUT", "TARGET_COL", "FEATURE_COLS", "SEED"); err != nil {
		return "", err
	}
	target, err := stringArg(call, "TARGET_COL", true)
	if err != nil {
		return "", err
	}
	fv := call.Arg("FEATURE_COLS")
	if fv == nil || fv.Kind != snowflake.ValueArray || len(fv.Elems) == 0 {
		return "", modelSyntax("%s's feature_cols is a non-empty array of column names", fn)
	}
	var features []string
	for _, e := range fv.Elems {
		if e.Kind != snowflake.ValueString {
			return "", modelSyntax("%s's feature_cols are column names, as strings", fn)
		}
		if strings.EqualFold(e.Str, target) {
			return "", modelSyntax("%s's target %s cannot be a feature too", fn, target)
		}
		features = append(features, e.Str)
	}
	var seed uint64
	if v := call.Arg("SEED"); v != nil {
		if v.Kind != snowflake.ValueNumber || v.Num < 0 || v.Num != math.Trunc(v.Num) {
			return "", modelSyntax("%s's seed is a whole number", fn)
		}
		seed = uint64(v.Num)
	}
	input, err := s.inputQuery(ctx, sess, user, call.Arg("INPUT"), fn+"'s input")
	if err != nil {
		return "", err
	}
	cols := make([]string, 0, len(features)+1)
	for _, f := range features {
		cols = append(cols, quoteIdent(f))
	}
	t, err := s.readRows(ctx, sess, user, "SELECT "+strings.Join(cols, ", ")+", "+quoteIdent(target)+" FROM ("+input+
		") AS __igloo_in WHERE "+quoteIdent(target)+" IS NOT NULL")
	if err != nil {
		return "", err
	}
	tc := t.Col(target)
	art := &models.Artifact{Class: models.ClassClassification}
	if fn == "ai_predict_class" {
		if art.Classifier, err = models.TrainClassifier(t, target, models.ClassifierConfig{Seed: seed, Interrupt: ctx.Err}); err != nil {
			return "", err
		}
	} else {
		art.Class = models.ClassValue
		if art.Regressor, err = models.TrainRegressor(t, target, seed, ctx.Err); err != nil {
			return "", err
		}
	}
	_, sha, err := art.Encode()
	if err != nil {
		return "", err
	}
	s.modelRuntime.Remember(sha, art, true)
	sess.pinModel(s.modelRuntime, sha)
	sess.allowModel(sha)

	var b strings.Builder
	b.WriteString("(SELECT * EXCLUDE (__igloo_score), ")
	prediction := quoteIdent(tc.Name + "_prediction")
	if art.Classifier != nil {
		typ := tc.SQLType
		if typ == "" {
			typ = "VARCHAR"
		}
		b.WriteString("CAST(json_extract_string(__igloo_score, '$.class') AS " + typ + ") AS " + prediction +
			", CAST(json_extract(__igloo_score, '$.confidence') AS DOUBLE) AS " + quoteIdent(tc.Name+"_confidence"))
	} else {
		b.WriteString("CAST(json_extract(__igloo_score, '$.value') AS DOUBLE) AS " + prediction)
	}
	b.WriteString(" FROM (SELECT *, " + models.ScoreFunction + "(" + quoteLiteral(sha))
	for _, f := range art.Features() {
		b.WriteString(", " + featureExpr(f, quoteIdent(f.Name)))
	}
	b.WriteString(") AS __igloo_score FROM (" + input + ") AS __igloo_in WHERE " + quoteIdent(target) + " IS NULL) AS __igloo_scored)")
	return b.String(), nil
}

// featureExpr reads expr as scoring reads a feature of type f.Type.
func featureExpr(f models.Feature, expr string) string {
	switch f.Type {
	case models.TypeNumber.String():
		return "TRY_CAST(" + expr + " AS DOUBLE)"
	case models.TypeTime.String():
		if expr == "NULL" {
			return "CAST(NULL AS DOUBLE)"
		}
		return "epoch(" + expr + ")"
	}
	return "CAST(" + expr + " AS VARCHAR)"
}

// maxModelAllowed bounds what a session remembers having resolved.
const maxModelAllowed = 4096

// allowModel lets the session's statements name a model blob or result,
// which one of its statements resolved, after its checks.
func (sess *session) allowModel(v string) {
	if sess.modelAllowed == nil {
		sess.modelAllowed = map[string]int{}
	}
	sess.modelSeq++
	sess.modelAllowed[v] = sess.modelSeq
	if len(sess.modelAllowed) > maxModelAllowed {
		for k, seq := range sess.modelAllowed {
			if seq <= sess.modelSeq-maxModelAllowed {
				delete(sess.modelAllowed, k)
			}
		}
	}
}

// checkModelCalls refuses a statement that calls Igloo's model functions
// with what no statement of the session resolved: a blob it did not check
// the user may use, written some other way than through modelRewrite (a
// clean room's template, say, which is prepared without it).
func (sess *session) checkModelCalls(query string) error {
	if !strings.Contains(strings.ToLower(query), "igloo_ml_") {
		return nil
	}
	calls, err := mlCalls(mlTokens(query), map[string]bool{models.PredictFunction: true, models.ScoreFunction: true,
		models.ResultFunction: true})
	if err != nil {
		return err
	}
	for _, c := range calls {
		v, ok := "", len(c.args) > 0
		if ok {
			v, ok = stringLiteral(query[c.args[0][0]:c.args[0][1]])
		}
		if _, allowed := sess.modelAllowed[v]; !ok || !allowed {
			return denied("%s is Igloo's own: score rows with model!PREDICT(INPUT_DATA => {*}) and read methods with "+
				"TABLE(model!METHOD(...))", c.name)
		}
	}
	return nil
}

// maxModelPins bounds the models a session's train-and-score calls keep.
const maxModelPins = 16

// pinModel keeps a model a statement of the session trained until the
// session ends, or has trained more.
func (sess *session) pinModel(r *models.Runtime, sha string) {
	sess.modelPins = append(sess.modelPins, sha)
	if len(sess.modelPins) > maxModelPins {
		r.Unpin(sess.modelPins[0])
		sess.modelPins = sess.modelPins[1:]
	}
}

// resolvePredict writes igloo_ml_predict for igloo_ml_predict_ref.
func (s *Server) resolvePredict(ctx context.Context, sess *session, user auth.User, query string, c mlCall) (string, error) {
	arg := func(i int) string { return strings.TrimSpace(query[c.args[i][0]:c.args[i][1]]) }
	if len(c.args) < 2 {
		return "", modelSyntax("%s takes a model and its input", predictMarker)
	}
	nameText, ok := stringLiteral(arg(0))
	mode, ok2 := stringLiteral(arg(1))
	if !ok || !ok2 {
		return "", modelSyntax("%s takes a model's name and how its input is given, as strings", predictMarker)
	}
	// The model's name, then, for a method other than PREDICT, the method
	// and its options: m!DETECT_ANOMALIES{"contamination": 0.01}.
	method, cfgText := "PREDICT", ""
	sc := &scanner{s: nameText}
	name, ok := sc.name()
	if ok && strings.HasPrefix(sc.rest(), "!") {
		rest := sc.rest()[1:]
		j := strings.IndexByte(rest, '{')
		if j < 0 {
			j = len(rest)
		}
		method, cfgText = strings.ToUpper(rest[:j]), rest[j:]
	} else if !ok || !sc.done() {
		return "", modelSyntax("invalid model name %q", nameText)
	}
	m, err := s.findModel(ctx, sess, user, name, "")
	if err != nil {
		return "", err
	}
	if err := s.mayCall(ctx, user, m, method); err != nil {
		return "", err
	}
	igloo := models.IsIgloo(m.Class)
	if !igloo && (m.Class != models.ClassClassification || method != "PREDICT") {
		return "", pgerror.WithCode(fmt.Errorf("model %s.%s is a %s, which has no %s in a query", m.Schema, m.Name,
			models.QualifiedClass(m.Class), method), codes.UndefinedFunction)
	}
	art, err := s.modelRuntime.Load(ctx, m.Blob)
	if err != nil {
		return "", fmt.Errorf("load model %s.%s: %w", m.Schema, m.Name, err)
	}
	spec, feats := m.Blob, art.Features()
	if igloo {
		var cfg map[string]any
		if cfgText != "" {
			if err := json.Unmarshal([]byte(cfgText), &cfg); err != nil {
				return "", modelSyntax("%s's CONFIG_OBJECT: %v", method, err)
			}
		}
		if !models.IsRowMethodOf(m.Class, method) {
			return "", pgerror.WithCode(fmt.Errorf("%s has no method %s that scores rows; it has %s", models.QualifiedClass(m.Class),
				method, strings.Join(art.RowMethods(), ", ")), codes.UndefinedFunction)
		}
		rm, err := art.RowMethod(method, cfg)
		if err != nil {
			return "", modelError(err)
		}
		spec, feats = m.Blob+rm.Spec, rm.Features
	}
	// Each feature's value: a column of the row, an expression given for
	// it, or NULL.
	values := map[string]string{}
	exclude := map[string]bool{}
	qualifier := ""
	all := false
	switch {
	case mode == "*":
		all = true
		for i := 2; i < len(c.args); i++ {
			name, ok := stringLiteral(arg(i))
			if !ok {
				return "", modelSyntax("%s's exclusions are names", predictMarker)
			}
			exclude[strings.ToLower(name)] = true
		}
	case strings.HasPrefix(mode, "*:"):
		all = true
		qsc := &scanner{s: mode[2:]}
		parts, ok := qsc.name()
		if !ok || !qsc.done() {
			return "", modelSyntax("invalid qualifier %q", mode[2:])
		}
		qualifier = quoteName(parts) + "."
	case mode == "=":
		if (len(c.args)-2)%2 != 0 {
			return "", modelSyntax("%s takes keys and values", predictMarker)
		}
		for i := 2; i < len(c.args); i += 2 {
			key, ok := stringLiteral(arg(i))
			if !ok {
				return "", modelSyntax("%s's keys are strings", predictMarker)
			}
			v, err := s.modelRewrite(ctx, sess, user, arg(i+1))
			if err != nil {
				return "", err
			}
			values[strings.ToLower(key)] = v
		}
	default:
		return "", modelSyntax("unknown input %q", mode)
	}
	sess.allowModel(spec)
	var b strings.Builder
	b.WriteString(models.PredictFunction + "(" + quoteLiteral(spec))
	for _, f := range feats {
		expr := "NULL"
		switch {
		case all && !exclude[strings.ToLower(f.Name)]:
			expr = qualifier + quoteIdent(f.Name)
		case !all:
			if v, ok := values[strings.ToLower(f.Name)]; ok {
				expr = "(" + v + ")"
			}
		}
		b.WriteString(", " + featureExpr(f, expr))
	}
	b.WriteString(")")
	return b.String(), nil
}

// keepModelResult remembers a result the session's statements read, so
// that it is dropped with the session, or once the session has more.
func (sess *session) keepModelResult(r *models.Runtime, token string) {
	sess.modelResults = append(sess.modelResults, token)
	if len(sess.modelResults) > maxModelResults {
		r.DropResult(sess.modelResults[0])
		sess.modelResults = sess.modelResults[1:]
	}
}

// dropModelResults drops every result and model the session kept.
func (sess *session) dropModelResults(r *models.Runtime) {
	for _, t := range sess.modelResults {
		r.DropResult(t)
	}
	for _, sha := range sess.modelPins {
		r.Unpin(sha)
	}
	sess.modelResults, sess.modelPins = nil, nil
}
