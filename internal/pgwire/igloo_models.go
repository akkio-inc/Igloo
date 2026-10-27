package pgwire

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/models"
	"github.com/akkio-inc/igloo/internal/snowflake"
)

// Igloo's own classes, IGLOO.ML.<class> (see models/igloo.go): BigQuery
// ML's model types with the statements of Snowflake's classes. Training
// reads INPUT_DATA as the user, sampled down to models.TrainRowLimit rows;
// row methods (PREDICT and the like) are scored by igloo_ml_predict, and
// called as tables, TABLE(m!PREDICT(INPUT_DATA => ...)), become a query
// that scores INPUT_DATA's rows (rowMethodTable).

// trainIglooModel trains the model CREATE IGLOO.ML.<class> describes.
func (s *Server) trainIglooModel(ctx context.Context, sess *session, user auth.User, st *snowflake.ModelStatement) (*models.Artifact, string, string, error) {
	call := st.Call
	args := []string{"INPUT_DATA", "CONFIG_OBJECT"}
	switch st.Class {
	case models.ClassFactorization:
		args = append(args, "USER_COLNAME", "ITEM_COLNAME", "RATING_COLNAME")
	case models.ClassKMeans, models.ClassPCA, models.ClassAutoencoder:
	default:
		args = append(args, "TARGET_COLNAME")
	}
	if err := onlyArgs(call, args...); err != nil {
		return nil, "", "", err
	}
	a := models.IglooArgs{Interrupt: ctx.Err}
	var err error
	for _, p := range []struct {
		name string
		dst  *string
	}{{"TARGET_COLNAME", &a.Target}, {"USER_COLNAME", &a.User}, {"ITEM_COLNAME", &a.Item}, {"RATING_COLNAME", &a.Rating}} {
		if slices.Contains(args, p.name) {
			if *p.dst, err = stringArg(call, p.name, true); err != nil {
				return nil, "", "", err
			}
		}
	}
	cfgVal := call.Arg("CONFIG_OBJECT")
	if cfgVal != nil && cfgVal.Kind != snowflake.ValueNull {
		if cfgVal.Kind != snowflake.ValueObject {
			return nil, "", "", modelSyntax("CONFIG_OBJECT is an object, such as {'algorithm': 'random_forest'}")
		}
		a.Config, _ = cfgVal.Any().(map[string]any)
	}
	q, err := s.inputQuery(ctx, sess, user, call.Arg("INPUT_DATA"), "INPUT_DATA")
	if err != nil {
		return nil, "", "", err
	}
	if st.Class == models.ClassFactorization {
		// Users and items are read as text, as scoring reads them.
		q = "SELECT CAST(" + quoteIdent(a.User) + " AS VARCHAR) AS " + quoteIdent(a.User) + ", CAST(" + quoteIdent(a.Item) +
			" AS VARCHAR) AS " + quoteIdent(a.Item) + ", TRY_CAST(" + quoteIdent(a.Rating) + " AS DOUBLE) AS " + quoteIdent(a.Rating) +
			" FROM (" + q + ") AS __igloo_in"
	}
	t, err := s.readTraining(ctx, sess, user, q, &a)
	if err != nil {
		return nil, "", "", err
	}
	art, summary, err := models.TrainIgloo(st.Class, t, a)
	if err != nil {
		return nil, "", "", err
	}
	if a.Sampled {
		summary["sampled_rows"], summary["input_rows"] = a.Rows, a.Total
	}
	for k, v := range summary {
		if f, ok := v.(float64); ok {
			summary[k] = finite(f)
		}
	}
	return art, valueJSON(cfgVal), jsonText(summary), nil
}

// readTraining reads a model's training rows as user: every row of query,
// or, above models.TrainRowLimit of them, a sample of about that many,
// chosen by a hash of each row's values, so that the same rows give the
// same sample.
func (s *Server) readTraining(ctx context.Context, sess *session, user auth.User, query string, a *models.IglooArgs) (*models.Table, error) {
	shape, err := s.readRows(ctx, sess, user, "SELECT * FROM ("+query+") AS __igloo_in LIMIT 0")
	if err != nil {
		return nil, err
	}
	counted, err := s.readRows(ctx, sess, user, "SELECT count(*) AS n FROM ("+query+") AS __igloo_in")
	if err != nil {
		return nil, err
	}
	total := 0
	if len(counted.Cols) == 1 && counted.N == 1 {
		total = int(counted.Cols[0].Num[0])
	}
	limit := models.TrainRowLimit(len(shape.Cols))
	if s.trainRows > 0 {
		limit = s.trainRows
	}
	if total <= limit {
		return s.readRows(ctx, sess, user, query)
	}
	const buckets = 1_000_000
	keep := int(math.Ceil(float64(limit) / float64(total) * buckets))
	t, err := s.readRows(ctx, sess, user, "SELECT * FROM ("+query+") AS __igloo_in WHERE hash(__igloo_in) % "+
		strconv.Itoa(buckets)+" < "+strconv.Itoa(keep))
	if err != nil {
		return nil, err
	}
	a.Sampled, a.Total, a.Rows = true, total, t.N
	return t, nil
}

// iglooMethod computes a table method of one of Igloo's models: EVALUATE
// on the rows held out of training or on INPUT_DATA, and the methods that
// take no arguments.
func (s *Server) iglooMethod(ctx context.Context, sess *session, user auth.User, art *models.Artifact, call *snowflake.ModelCall) (*models.Result, error) {
	if call.Method == "EVALUATE" && call.Arg("INPUT_DATA") != nil {
		if err := onlyArgs(call, "INPUT_DATA"); err != nil {
			return nil, err
		}
		t, err := s.readInput(ctx, sess, user, call.Arg("INPUT_DATA"), "INPUT_DATA")
		if err != nil {
			return nil, err
		}
		return art.EvaluateOn(t)
	}
	if len(call.Args) > 0 {
		hint := ""
		if call.Method == "EVALUATE" {
			hint = ", or INPUT_DATA => TABLE(t) to evaluate on t's rows"
		}
		return nil, modelSyntax("%s!%s takes no arguments%s", strings.Join(call.Name, "."), call.Method, hint)
	}
	return art.Method(call.Method)
}

// rowMethodTable writes the query a row method called as a table becomes:
// INPUT_DATA's rows, each with the method's columns, scored as the rows are
// read. ok is false for a call that is not of an Igloo model's row method.
func (s *Server) rowMethodTable(ctx context.Context, sess *session, user auth.User, call *snowflake.ModelCall) (string, bool, error) {
	if !slices.Contains(snowflake.RowMethods, call.Method) {
		return "", false, nil
	}
	gen := s.modelRuntime.Gen()
	m, err := s.findModel(ctx, sess, user, call.Name, "")
	if err != nil {
		return "", true, err
	}
	if !models.IsIgloo(m.Class) {
		return "", false, nil
	}
	if err := s.mayCall(ctx, user, m, call.Method); err != nil {
		return "", true, err
	}
	if !models.IsRowMethodOf(m.Class, call.Method) {
		return "", true, pgerror.WithCode(fmt.Errorf("%s has no method %s; it has %s", models.QualifiedClass(m.Class), call.Method,
			strings.Join(models.Methods(m.Class), ", ")), codes.UndefinedFunction)
	}
	// PREDICT(TABLE(t)) is PREDICT(INPUT_DATA => TABLE(t)).
	if len(call.Args) > 0 && call.Args[0].Name == "" {
		call.Args[0].Name = "INPUT_DATA"
	}
	if err := onlyArgs(call, "INPUT_DATA", "CONFIG_OBJECT"); err != nil {
		return "", true, err
	}
	art, err := s.modelRuntime.Load(ctx, m.Blob)
	if err != nil {
		return "", true, fmt.Errorf("load model %s.%s: %w", m.Schema, m.Name, err)
	}
	var cfg map[string]any
	if v := call.Arg("CONFIG_OBJECT"); v != nil && v.Kind != snowflake.ValueNull {
		if v.Kind != snowflake.ValueObject {
			return "", true, modelSyntax("CONFIG_OBJECT is an object")
		}
		cfg, _ = v.Any().(map[string]any)
	}
	rm, err := art.RowMethod(call.Method, cfg)
	if err != nil {
		return "", true, modelError(err)
	}
	input, err := s.rowMethodInput(ctx, sess, user, art, call, gen)
	if err != nil {
		return "", true, err
	}
	spec := m.Blob + rm.Spec
	sess.allowModel(spec, gen)
	var score strings.Builder
	score.WriteString(models.PredictFunction + "(" + quoteLiteral(spec))
	for _, f := range rm.Features {
		score.WriteString(", " + featureExpr(f, "__igloo_in."+quoteIdent(f.Name)))
	}
	score.WriteString(")")
	var b strings.Builder
	if rm.Table.Unnest != "" {
		b.WriteString("(SELECT * EXCLUDE (__igloo_rec)")
		for _, c := range rm.Table.Columns {
			b.WriteString(", " + jsonColumn("__igloo_rec", c))
		}
		b.WriteString(" FROM (SELECT __igloo_in.*, unnest(CAST(json_extract(" + score.String() + ", " + quoteLiteral(rm.Table.Unnest) +
			") AS JSON[])) AS __igloo_rec FROM (" + input + ") AS __igloo_in) AS __igloo_scored)")
		return b.String(), true, nil
	}
	b.WriteString("(SELECT * EXCLUDE (__igloo_score)")
	for _, c := range rm.Table.Columns {
		b.WriteString(", " + jsonColumn("__igloo_score", c))
	}
	b.WriteString(" FROM (SELECT __igloo_in.*, " + score.String() + " AS __igloo_score FROM (" + input + ") AS __igloo_in) AS __igloo_scored)")
	return b.String(), true, nil
}

// rowMethodInput is the query of a row method's INPUT_DATA; RECOMMEND
// without it recommends to every user the model was trained on.
func (s *Server) rowMethodInput(ctx context.Context, sess *session, user auth.User, art *models.Artifact, call *snowflake.ModelCall,
	gen uint64) (string, error) {
	if in := call.Arg("INPUT_DATA"); in != nil {
		return s.inputQuery(ctx, sess, user, in, "INPUT_DATA")
	}
	res := art.TrainingInputs(call.Method)
	if res == nil {
		return "", modelSyntax("%s needs INPUT_DATA, such as INPUT_DATA => TABLE(t)", call.Method)
	}
	token, err := s.keepResult(sess, res, gen)
	if err != nil {
		return "", err
	}
	return "SELECT * FROM " + models.ResultFunction + "(" + quoteLiteral(token) + ")", nil
}

// jsonColumn reads a row method's column from its object.
func jsonColumn(obj string, c models.RowColumn) string {
	name := quoteIdent(c.Name)
	path := quoteLiteral(c.Path)
	switch c.Type {
	case "JSON":
		return "json_extract(" + obj + ", " + path + ") AS " + name
	case "VARCHAR":
		return "json_extract_string(" + obj + ", " + path + ") AS " + name
	case "DOUBLE", "BIGINT", "BOOLEAN":
		return "CAST(json_extract(" + obj + ", " + path + ") AS " + c.Type + ") AS " + name
	}
	return "CAST(json_extract_string(" + obj + ", " + path + ") AS " + c.Type + ") AS " + name
}
