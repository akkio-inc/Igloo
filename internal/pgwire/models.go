package pgwire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/ai"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/models"
	"github.com/akkio-inc/igloo/internal/snowflake"
)

// Snowflake's ML classes, and Igloo's own IGLOO.ML.<class> with the same
// statements (docs/ml-functions.md; igloo_models.go). Igloo carries out
// their statements itself, in either dialect, reading them as Snowflake's
// SQL:
//
//	CREATE [OR REPLACE] SNOWFLAKE.ML.<class> [IF NOT EXISTS] name(INPUT_DATA => ..., ...)
//	DROP SNOWFLAKE.ML.<class> [IF EXISTS] name
//	SHOW SNOWFLAKE.ML.<class> [LIKE 'pattern'] [IN ACCOUNT | DATABASE | SCHEMA [name]]
//	GRANT | REVOKE USAGE ON SNOWFLAKE.ML.<class> name TO | FROM [ROLE | USER] grantee
//	GRANT | REVOKE SNOWFLAKE.ML.<class> ROLE name!MLADMIN | MLCONSUMER TO | FROM [ROLE | USER] grantee
//	CALL name!METHOD(...)
//
// A model lives in a schema, like a table: its row in the metadata
// database (package models) names its blob, which is in the warehouse. A
// model is its creator's. Creating one takes what creating a table in its
// schema takes (the writer role or CREATE on the schema), and reads the
// training data as the creator, through their grants and policies. Its
// owner and admins may use, replace, drop and grant it; others need USAGE,
// or PREDICT (Snowflake's MLCONSUMER) for the methods that predict.
//
// Methods in queries (TABLE(m!FORECAST(...)), m!PREDICT(...)) are resolved
// before the statement is prepared; see mlrewrite.go.

// predictingMethods need only PREDICT.
var predictingMethods = map[string]bool{"PREDICT": true, "FORECAST": true, "DETECT_ANOMALIES": true, "GET_DRIVERS": true,
	"EXPLAIN_PREDICT": true, "RECOMMEND": true, "RECONSTRUCTION_LOSS": true}

// modelStatement recognizes the statements above.
func (s *Server) modelStatement(ctx context.Context, sess *session, q wire.Query) (wire.PreparedStatements, bool, error) {
	st, ok, err := snowflake.ParseModelStatement(q.Query)
	if !ok {
		return nil, false, nil
	}
	fail := func(err error) (wire.PreparedStatements, bool, error) {
		err = pgError(modelError(err))
		s.recordQuery(sess, queryRecord{text: q.Query, start: time.Now(), err: err, rows: -1})
		return nil, true, err
	}
	if err != nil {
		return fail(err)
	}
	var cols wire.Columns
	switch st.Verb {
	case "SHOW":
		cols = modelShowColumns
	case "CALL":
		user := sess.currentUser()
		m, err := s.findModel(ctx, sess, user, st.Name, "")
		if err != nil {
			return fail(err)
		}
		rc, err := methodColumns(m.Class, st.Call.Method)
		if err != nil {
			return fail(err)
		}
		cols = wireColumns(rc, sess.sqlDialect() == dialectSnowflake)
	}
	return wire.Prepared(wire.NewStatement(s.audited(sess, q.Query, func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		start := time.Now()
		user, err := s.refreshUser(ctx, sess)
		if err == nil {
			err = sess.runCancelable(ctx, q.Query, func(ctx context.Context) error {
				return s.runModelStatement(ctx, sess, user, st, q.Query, w)
			})
		}
		s.metrics.query(start, err)
		return pgError(modelError(err))
	}), wire.WithColumns(cols))), true, nil
}

// modelError gives errors their SQLSTATE: what the user gave wrong is
// 22023, and the translator's errors keep theirs.
func modelError(err error) error {
	var te *snowflake.Error
	switch {
	case err == nil:
		return nil
	case errors.As(err, &te):
		out := pgerror.WithCode(errors.New(te.Message), codes.Code(te.Code))
		if te.Hint != "" {
			out = pgerror.WithHint(out, te.Hint)
		}
		return out
	case models.InputError(err):
		return pgerror.WithCode(errors.New(strings.TrimPrefix(err.Error(), "invalid input: ")), codes.InvalidParameterValue)
	}
	return err
}

func modelSyntax(format string, args ...any) error {
	return pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
}

// knownClass refuses classes Igloo does not have.
func knownClass(class string) error {
	if slices.Contains(snowflake.ModelClasses, class) || slices.Contains(models.IglooClasses, class) {
		return nil
	}
	return pgerror.WithHint(pgerror.WithCode(fmt.Errorf("%s is not supported", models.QualifiedClass(class)), codes.FeatureNotSupported),
		"Igloo has SNOWFLAKE.ML."+strings.Join(snowflake.ModelClasses, ", SNOWFLAKE.ML.")+", and "+strings.Join(models.IglooClasses, ", "))
}

func (s *Server) runModelStatement(ctx context.Context, sess *session, user auth.User, st *snowflake.ModelStatement, text string,
	w wire.DataWriter) error {
	if st.Class != "" {
		if err := knownClass(st.Class); err != nil {
			return err
		}
	}
	switch st.Verb {
	case "CREATE":
		return s.createModel(ctx, sess, user, st, text, w)
	case "DROP":
		return s.dropModel(ctx, sess, user, st, w)
	case "SHOW":
		return s.showModels(ctx, sess, user, st, w)
	case "GRANT", "REVOKE":
		return s.grantModel(ctx, sess, user, st, w)
	case "CALL":
		return s.callModel(ctx, sess, user, st, w)
	case "ALTER":
		return pgerror.WithHint(pgerror.WithCode(fmt.Errorf("ALTER %s is not supported", models.QualifiedClass(st.Class)), codes.FeatureNotSupported),
			"a model does not change: CREATE OR REPLACE trains it again, and COMMENT is set when it is created")
	}
	return pgerror.WithCode(fmt.Errorf("%s %s is not supported", st.Verb, models.QualifiedClass(st.Class)), codes.FeatureNotSupported)
}

// modelRef resolves a model's name to its schema and name: a name alone
// is in the session's current schema, a database's must be the catalog's.
func (s *Server) modelRef(ctx context.Context, sess *session, name []string) (string, string, error) {
	switch len(name) {
	case 1:
		_, schema, err := currentSchema(ctx, sess.conn)
		if err != nil {
			return "", "", err
		}
		return strings.ToLower(schema), strings.ToLower(name[0]), nil
	case 2:
		return strings.ToLower(name[0]), strings.ToLower(name[1]), nil
	case 3:
		db, _, err := currentSchema(ctx, sess.conn)
		if err != nil {
			return "", "", err
		}
		if !strings.EqualFold(name[0], s.catalogName()) && !strings.EqualFold(name[0], db) {
			return "", "", pgerror.WithCode(fmt.Errorf("database %s does not exist: models live in the catalog %s",
				name[0], s.catalogName()), codes.UndefinedObject)
		}
		return strings.ToLower(name[1]), strings.ToLower(name[2]), nil
	}
	return "", "", modelSyntax("invalid model name %s", strings.Join(name, "."))
}

func modelMissing(schema, name string) error {
	return pgerror.WithCode(fmt.Errorf("model %s.%s does not exist or not authorized", schema, name), codes.UndefinedObject)
}

// findModel returns the model a statement names, which must be of class
// unless class is empty. Users who may not use it are told it does not
// exist, as Snowflake does.
func (s *Server) findModel(ctx context.Context, sess *session, user auth.User, name []string, class string) (models.Model, error) {
	schema, n, err := s.modelRef(ctx, sess, name)
	if err != nil {
		return models.Model{}, err
	}
	m, err := s.models.Get(ctx, schema, n)
	if errors.Is(err, models.ErrNotFound) {
		return m, modelMissing(schema, n)
	}
	if err != nil {
		return m, err
	}
	if class != "" && m.Class != class {
		return m, pgerror.WithCode(fmt.Errorf("model %s.%s is a %s, not a %s", schema, n, models.QualifiedClass(m.Class),
			models.QualifiedClass(class)), codes.WrongObjectType)
	}
	noteTrainingModel(sess, m)
	return m, nil
}

// modelPrivilege returns the privilege user has on m: "OWNER" for its
// owner and admins, USAGE, PREDICT, or "".
func (s *Server) modelPrivilege(ctx context.Context, user auth.User, m models.Model) (string, error) {
	if user.Role == auth.RoleAdmin || m.Owner == user.Name {
		return "OWNER", nil
	}
	grants, err := s.models.Grants(ctx)
	if err != nil {
		return "", err
	}
	roles, err := s.customRoles(ctx, user)
	if err != nil {
		return "", err
	}
	best := ""
	for _, g := range grants {
		if !strings.EqualFold(g.Schema, m.Schema) || !strings.EqualFold(g.Name, m.Name) {
			continue
		}
		if g.GranteeType == auth.GranteeUser && g.Grantee == user.Name || g.GranteeType == auth.GranteeRole && slices.Contains(roles, g.Grantee) {
			if g.Privilege == models.PrivilegeUsage || best == "" {
				best = g.Privilege
			}
		}
	}
	return best, nil
}

// mayCall checks that user may call method on m.
func (s *Server) mayCall(ctx context.Context, user auth.User, m models.Model, method string) error {
	p, err := s.modelPrivilege(ctx, user, m)
	if err != nil {
		return err
	}
	switch {
	case p == "OWNER", p == models.PrivilegeUsage:
		return nil
	case p == models.PrivilegePredict && predictingMethods[method]:
		return nil
	case p == models.PrivilegePredict:
		return denied("%s may only predict with model %s.%s (MLCONSUMER); %s needs USAGE (MLADMIN)", user.Name, m.Schema, m.Name, method)
	}
	return modelMissing(m.Schema, m.Name)
}

// mayCreateModel checks that user may create a model in schema: what
// creating a table there takes.
func (s *Server) mayCreateModel(ctx context.Context, sess *session, user auth.User, schema string) error {
	if rank[user.Role] >= rank[auth.RoleWriter] {
		return nil
	}
	privs, err := s.privileges(ctx, sess, user)
	if err != nil {
		return err
	}
	if privs.AllowsSchema(auth.PrivilegeCreate, schema) {
		return nil
	}
	return denied("%s may not create models in schema %s: that needs the writer role or CREATE on the schema", user.Name, schema)
}

func (s *Server) createModel(ctx context.Context, sess *session, user auth.User, st *snowflake.ModelStatement, text string,
	w wire.DataWriter) error {
	return s.createModelAs(ctx, sess, user, st, text, w, "")
}

// createModelAs carries out CREATE: when ifBlob is set, as an erasure
// request training a model again, replacing only the model with that blob
// (see models.Model.IfBlob).
func (s *Server) createModelAs(ctx context.Context, sess *session, user auth.User, st *snowflake.ModelStatement, text string,
	w wire.DataWriter, ifBlob string) error {
	tag := "CREATE " + models.QualifiedClass(st.Class)
	schema, name, err := s.modelRef(ctx, sess, st.Name)
	if err != nil {
		return err
	}
	if err := s.mayCreateModel(ctx, sess, user, schema); err != nil {
		return err
	}
	if old, err := s.models.Get(ctx, schema, name); err == nil {
		switch {
		case st.IfNotExists:
			return w.Complete(tag)
		case !st.Replace:
			return pgerror.WithCode(fmt.Errorf("model %s.%s already exists", schema, name), codes.DuplicateObject)
		case old.Owner != user.Name && user.Role != auth.RoleAdmin:
			return denied("%s does not own model %s.%s", user.Name, schema, name)
		}
	} else if !errors.Is(err, models.ErrNotFound) {
		return err
	}
	if st.Tags {
		if n, ok := w.(wire.Noticer); ok {
			_ = n.Notice("01000", "TAG is ignored: models have no tags in Igloo")
		}
	}
	// The erasure epoch before training reads anything: a request that
	// moves it on meanwhile may have erased rows training read, and the
	// model is not stored (see models.Store.Put).
	epoch, err := s.models.Epoch(ctx)
	if err != nil {
		return err
	}
	searchPath, err := settingValue(ctx, sess.conn, "search_path")
	if err != nil {
		return err
	}
	tables := []string{}
	sess.training = &tables
	art, cfg, metrics, err := s.trainModel(ctx, sess, user, st)
	sess.training = nil
	if err != nil {
		return err
	}
	blob, sha, err := art.Encode()
	if err != nil {
		return err
	}
	m := models.Model{Schema: schema, Name: name, Class: st.Class, Owner: user.Name, TrainingSQL: text, Config: cfg,
		Metrics: metrics, Blob: sha, BlobSize: int64(len(blob)), Tables: ai.Tables(tables), Epoch: epoch, SearchPath: searchPath,
		Dialect: sess.sqlDialect(), IfBlob: ifBlob}
	if m.Tables == nil {
		m.Tables = []string{}
	}
	if st.Comment != nil {
		m.Comment = *st.Comment
	}
	old, err := s.publishModel(ctx, m, art, blob, st.Replace)
	switch {
	case errors.Is(err, models.ErrExists) && st.IfNotExists:
		return w.Complete(tag)
	case errors.Is(err, models.ErrExists):
		return pgerror.WithCode(fmt.Errorf("model %s.%s already exists", schema, name), codes.DuplicateObject)
	case errors.Is(err, models.ErrStale):
		return pgerror.WithCode(err, codes.SerializationFailure)
	case err != nil:
		return err
	}
	if old != nil && old.Blob != sha {
		s.dropBlob(ctx, old.Blob)
	}
	s.log.Info("model trained", "model", schema+"."+name, "class", st.Class, "by", user.Name, "blob", sha, "bytes", len(blob))
	return w.Complete(tag)
}

// noteTraining adds, while CREATE trains a model, what a statement it
// prepared reads to the tables the model records (see models.Model.Tables).
func (s *Server) noteTraining(ctx context.Context, sess *session, st *statement) {
	if sess.training != nil {
		*sess.training = append(*sess.training, s.aiTables(ctx, sess, st)...)
	}
}

// noteTrainingModel adds, while CREATE trains a model, what a model its
// input uses had read: the new one learns from what that one learned.
func noteTrainingModel(sess *session, m models.Model) {
	if sess.training == nil {
		return
	}
	if m.Tables == nil {
		*sess.training = append(*sess.training, models.AnyTable)
		return
	}
	*sess.training = append(*sess.training, m.Tables...)
}

// publishModel writes the blob of m, art encoded, and stores m (see
// models.Store.Put), holding the blob's lock throughout: another model
// trained the same way may have the blob, and dropping it meanwhile must
// not delete the blob m is about to use (models.Store.LockBlob). If m is
// not stored, its blob is deleted unless a model uses it.
func (s *Server) publishModel(ctx context.Context, m models.Model, art *models.Artifact, blob []byte, replace bool) (*models.Model, error) {
	unlock, err := s.models.LockBlob(ctx, m.Blob)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := s.modelRuntime.Blobs().Put(ctx, m.Blob, blob); err != nil {
		return nil, err
	}
	old, err := s.models.Put(ctx, m, replace)
	if err != nil {
		s.dropLockedBlob(ctx, m.Blob)
		return nil, err
	}
	s.modelRuntime.Remember(m.Blob, art, false)
	return old, nil
}

// dropBlob deletes a blob no model uses any more.
func (s *Server) dropBlob(ctx context.Context, sha string) {
	unlock, err := s.models.LockBlob(ctx, sha)
	if err != nil {
		return
	}
	defer unlock()
	s.dropLockedBlob(ctx, sha)
}

// dropLockedBlob deletes blob sha, whose lock the caller holds, unless a
// model uses it.
func (s *Server) dropLockedBlob(ctx context.Context, sha string) {
	used, err := s.models.BlobUsed(ctx, sha)
	if err != nil || used {
		return
	}
	if err := s.modelRuntime.Blobs().Delete(context.WithoutCancel(ctx), sha); err != nil {
		s.log.Warn("could not delete a model's blob", "blob", sha, "err", err)
	}
	s.modelRuntime.Forget(sha)
}

// trainModel trains the model CREATE describes, and returns it with its
// configuration and a summary of its metrics, as JSON.
func (s *Server) trainModel(ctx context.Context, sess *session, user auth.User, st *snowflake.ModelStatement) (*models.Artifact, string, string, error) {
	if models.IsIgloo(st.Class) {
		return s.trainIglooModel(ctx, sess, user, st)
	}
	call := st.Call
	cfgVal := call.Arg("CONFIG_OBJECT")
	cfg := valueJSON(cfgVal)
	switch st.Class {
	case models.ClassClassification:
		if err := onlyArgs(call, "INPUT_DATA", "TARGET_COLNAME", "CONFIG_OBJECT"); err != nil {
			return nil, "", "", err
		}
		target, err := stringArg(call, "TARGET_COLNAME", true)
		if err != nil {
			return nil, "", "", err
		}
		conf := models.ClassifierConfig{Evaluate: true, Interrupt: ctx.Err}
		if v := cfgVal.Field("evaluate"); v != nil {
			conf.Evaluate = v.Kind != snowflake.ValueBool || v.Bool
		}
		if v := cfgVal.Field("evaluation_config").Field("test_fraction"); v != nil && v.Kind == snowflake.ValueNumber {
			conf.TestFraction = v.Num
		}
		t, err := s.readInput(ctx, sess, user, call.Arg("INPUT_DATA"), "INPUT_DATA")
		if err != nil {
			return nil, "", "", err
		}
		c, err := models.TrainClassifier(t, target, conf)
		if err != nil {
			return nil, "", "", err
		}
		summary := map[string]any{"rows": c.Rows, "classes": len(c.Classes), "features": len(c.Features)}
		if c.Eval != nil {
			for _, g := range c.Eval.Global {
				if g.AverageType == "macro" || g.AverageType == "" {
					summary[strings.Trim(g.AverageType+"_"+g.Metric, "_")] = finite(g.Value)
				}
			}
		}
		return &models.Artifact{Class: st.Class, Classifier: c}, cfg, jsonText(summary), nil
	case models.ClassForecast, models.ClassAnomaly:
		args := []string{"INPUT_DATA", "SERIES_COLNAME", "TIMESTAMP_COLNAME", "TARGET_COLNAME", "CONFIG_OBJECT"}
		if st.Class == models.ClassAnomaly {
			args = append(args, "LABEL_COLNAME")
		}
		if err := onlyArgs(call, args...); err != nil {
			return nil, "", "", err
		}
		names := map[string]string{}
		for _, a := range []string{"SERIES_COLNAME", "TIMESTAMP_COLNAME", "TARGET_COLNAME", "LABEL_COLNAME"} {
			v, err := stringArg(call, a, a == "TIMESTAMP_COLNAME" || a == "TARGET_COLNAME")
			if err != nil {
				return nil, "", "", err
			}
			names[a] = v
		}
		conf, err := forecastConfig(cfgVal, st.Class == models.ClassAnomaly)
		if err != nil {
			return nil, "", "", err
		}
		conf.Interrupt = ctx.Err
		conf.Foundation, conf.Context = s.forecasting, ctx
		if err := s.checkForecastMethod(conf.Method); err != nil {
			return nil, "", "", err
		}
		t, err := s.readInput(ctx, sess, user, call.Arg("INPUT_DATA"), "INPUT_DATA")
		if err != nil {
			return nil, "", "", err
		}
		f, err := models.TrainForecaster(t, names["TIMESTAMP_COLNAME"], names["TARGET_COLNAME"], names["SERIES_COLNAME"],
			names["LABEL_COLNAME"], conf)
		if err != nil {
			return nil, "", "", err
		}
		summary := map[string]any{"series": len(f.Series), "frequency": f.Freq.Name, "trees": f.Model != nil}
		if len(f.Metrics) > 0 {
			for name, v := range f.Metrics[0].Values {
				if len(f.Metrics) == 1 {
					summary[name] = finite(v)
				}
			}
		}
		return &models.Artifact{Class: st.Class, Forecaster: f}, cfg, jsonText(summary), nil
	case models.ClassTopInsights:
		// An instance holds nothing: GET_DRIVERS analyses what it is given.
		if len(call.Args) > 0 {
			return nil, "", "", modelSyntax("CREATE SNOWFLAKE.ML.TOP_INSIGHTS takes no arguments: name()")
		}
		return &models.Artifact{Class: st.Class}, cfg, "{}", nil
	}
	return nil, "", "", knownClass(st.Class)
}

// forecastConfig reads FORECAST's and ANOMALY_DETECTION's CONFIG_OBJECT.
func forecastConfig(v *snowflake.ModelValue, anomaly bool) (models.ForecastConfig, error) {
	c := models.ForecastConfig{Anomaly: anomaly, Evaluate: true, NSplits: 1}
	if anomaly {
		c.NSplits = 5
	}
	if v == nil || v.Kind == snowflake.ValueNull {
		return c, nil
	}
	if v.Kind != snowflake.ValueObject {
		return c, modelSyntax("CONFIG_OBJECT is an object, such as {'frequency': '1 day'}")
	}
	str := func(v *snowflake.ModelValue, name string) (string, error) {
		if v.Kind != snowflake.ValueString {
			return "", modelSyntax("%s takes a string", name)
		}
		return v.Str, nil
	}
	num := func(v *snowflake.ModelValue, name string) (*float64, error) {
		switch v.Kind {
		case snowflake.ValueNull:
			return nil, nil
		case snowflake.ValueNumber:
			n := v.Num
			return &n, nil
		}
		return nil, modelSyntax("%s takes a number", name)
	}
	integer := func(v *snowflake.ModelValue, name string) (int, error) {
		n, err := num(v, name)
		if err != nil || n == nil {
			return 0, err
		}
		if *n < 0 || *n != math.Trunc(*n) {
			return 0, modelSyntax("%s takes a whole number", name)
		}
		return int(*n), nil
	}
	for i, key := range v.Keys {
		e := &v.Elems[i]
		var err error
		switch strings.ToLower(key) {
		case "method":
			// 'best', 'fast', or a foundation model's name, which
			// training resolves (see models/foundation.go).
			if c.Method, err = str(e, key); err == nil && strings.TrimSpace(c.Method) == "" {
				err = modelSyntax("method is 'best', 'fast' or a forecasting model's name")
			}
		case "context_window":
			c.ContextWindow, err = integer(e, key)
		case "frequency":
			c.Frequency, err = str(e, key)
		case "on_error":
			var m string
			if m, err = str(e, key); err == nil {
				c.SkipErrors = strings.EqualFold(m, "skip")
			}
		case "evaluate":
			if e.Kind != snowflake.ValueBool {
				err = modelSyntax("evaluate takes TRUE or FALSE")
			}
			c.Evaluate = e.Bool
		case "lower_bound":
			c.LowerBound, err = num(e, key)
		case "upper_bound":
			c.UpperBound, err = num(e, key)
		case "aggregation_numeric":
			c.AggNumeric, err = str(e, key)
		case "aggregation_categorical":
			c.AggCategorical, err = str(e, key)
		case "aggregation_target":
			c.AggTarget, err = str(e, key)
		case "aggregation_column":
			if e.Kind != snowflake.ValueObject {
				return c, modelSyntax("aggregation_column is an object of columns' aggregations")
			}
			c.AggColumn = map[string]string{}
			for j, col := range e.Keys {
				var a string
				if a, err = str(&e.Elems[j], "aggregation_column"); err != nil {
					return c, err
				}
				c.AggColumn[strings.ToLower(col)] = a
			}
		case "evaluation_config":
			if e.Kind != snowflake.ValueObject {
				return c, modelSyntax("evaluation_config is an object")
			}
			for j, k := range e.Keys {
				ev := &e.Elems[j]
				switch strings.ToLower(k) {
				case "n_splits":
					if c.NSplits, err = integer(ev, k); err == nil && c.NSplits < 1 {
						err = modelSyntax("n_splits must be at least 1")
					}
				case "max_train_size":
					c.MaxTrainSize, err = integer(ev, k)
				case "test_size":
					c.TestSize, err = integer(ev, k)
				case "gap":
					c.Gap, err = integer(ev, k)
				case "prediction_interval":
					var p *float64
					if p, err = num(ev, k); err == nil && p != nil {
						c.PredictionInterval = *p
					}
				default:
					err = modelSyntax("unknown evaluation_config key %s", k)
				}
				if err != nil {
					return c, err
				}
			}
		default:
			err = modelSyntax("unknown CONFIG_OBJECT key %s", key)
		}
		if err != nil {
			return c, err
		}
	}
	if p := c.PredictionInterval; p != 0 && (p <= 0 || p >= 1) {
		return c, modelSyntax("prediction_interval must be between 0 and 1")
	}
	return c, nil
}

func finite(v float64) any {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	return v
}

func jsonText(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// valueJSON writes an argument's value as JSON, "{}" for none.
func valueJSON(v *snowflake.ModelValue) string {
	if v == nil {
		return "{}"
	}
	return jsonText(valueAny(*v))
}

func valueAny(v snowflake.ModelValue) any {
	switch v.Kind {
	case snowflake.ValueString, snowflake.ValueQuery:
		return v.Str
	case snowflake.ValueNumber:
		return finite(v.Num)
	case snowflake.ValueBool:
		return v.Bool
	case snowflake.ValueObject:
		m := map[string]any{}
		for i, k := range v.Keys {
			m[k] = valueAny(v.Elems[i])
		}
		return m
	case snowflake.ValueArray:
		out := make([]any, len(v.Elems))
		for i, e := range v.Elems {
			out[i] = valueAny(e)
		}
		return out
	case snowflake.ValueTable:
		return strings.Join(v.Table, ".")
	}
	return nil
}

// onlyArgs refuses arguments other than those named, and positional ones.
func onlyArgs(c *snowflake.ModelCall, names ...string) error {
	for _, a := range c.Args {
		if a.Name == "" {
			return modelSyntax("name %s's arguments, as in %s => ...", c.Method, names[0])
		}
		if !slices.Contains(names, a.Name) {
			return modelSyntax("%s takes %s, not %s", c.Method, strings.Join(names, ", "), a.Name)
		}
	}
	return nil
}

// stringArg returns a string argument.
func stringArg(c *snowflake.ModelCall, name string, required bool) (string, error) {
	v := c.Arg(name)
	if v == nil || v.Kind == snowflake.ValueNull {
		if required {
			return "", modelSyntax("%s needs %s", c.Method, name)
		}
		return "", nil
	}
	if v.Kind != snowflake.ValueString {
		return "", modelSyntax("%s takes a string", name)
	}
	return v.Str, nil
}

// inputQuery is the DuckDB query that reads an INPUT_DATA reference.
func (s *Server) inputQuery(ctx context.Context, sess *session, user auth.User, v *snowflake.ModelValue, what string) (string, error) {
	if v == nil || v.Kind == snowflake.ValueNull {
		return "", modelSyntax("%s is required", what)
	}
	switch v.Kind {
	case snowflake.ValueTable:
		return "SELECT * FROM " + quoteName(v.Table), nil
	case snowflake.ValueQuery:
		tr, err := s.translate(ctx, sess, v.Str, false)
		if err != nil {
			return "", err
		}
		if tr.Command != "" || tr.SQL == "" {
			return "", pgerror.WithCode(fmt.Errorf("%s must be a query", what), codes.InvalidParameterValue)
		}
		q := tr.SQL
		if tr.Untranslated {
			q = v.Str
		}
		if n, err := s.engine.CountStatements(q); err != nil || n != 1 {
			return "", pgerror.WithCode(fmt.Errorf("%s must be one query", what), codes.InvalidParameterValue)
		}
		// As for a statement of the user's own: the warehouse's files are
		// read through its tables.
		if err := s.checkWarehouseFiles(ctx, user, q); err != nil {
			return "", err
		}
		// A query may score rows with other models.
		return s.modelRewrite(ctx, sess, user, q)
	}
	return "", pgerror.WithCode(fmt.Errorf("%s takes a table, view or query: TABLE(name), SYSTEM$REFERENCE('TABLE', 'name') or "+
		"SYSTEM$QUERY_REFERENCE('SELECT ...')", what), codes.InvalidParameterValue)
}

// readInput reads the rows of an INPUT_DATA reference as user, through
// their grants and policies, as they would read them themselves.
func (s *Server) readInput(ctx context.Context, sess *session, user auth.User, v *snowflake.ModelValue, what string) (*models.Table, error) {
	q, err := s.inputQuery(ctx, sess, user, v, what)
	if err != nil {
		return nil, err
	}
	return s.readRows(ctx, sess, user, q)
}

// readRows runs a query as user and reads its rows into memory.
func (s *Server) readRows(ctx context.Context, sess *session, user auth.User, query string) (*models.Table, error) {
	st, err := s.prepareChecked(ctx, sess, user, query, nil)
	if err != nil {
		return nil, err
	}
	defer st.close() //nolint:errcheck
	if st.stmtType != duckdb.STATEMENT_TYPE_SELECT {
		return nil, pgerror.WithCode(errors.New("a model's input must be a query"), codes.InvalidParameterValue)
	}
	s.noteTraining(ctx, sess, st)
	if err := st.freshForRun(ctx, sess.conn); err != nil {
		return nil, err
	}
	rdr, err := st.stmt.QueryArrowContext(ctx, nil)
	if err != nil {
		return nil, pgError(err)
	}
	defer rdr.Release()
	t, err := models.ReadArrow(rdr)
	if err != nil {
		return nil, pgerror.WithCode(err, codes.ProgramLimitExceeded)
	}
	return t, nil
}

// disownModels marks the models of a dropped user as a dropped user's,
// "name (dropped)", as disownJobs does their jobs: a user created later
// with the same name does not own them. Admins may use, grant and drop
// them, or CREATE OR REPLACE them as their own; an erasure request cannot
// train them again.
func (s *Server) disownModels(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.models.DisownModels(ctx, name, models.Dropped(name)); err != nil {
		s.log.Warn("could not disown a dropped user's models", "user", name, "err", err)
	}
}

// ForgetDroppedPrincipals disowns the models, and removes the model
// grants, that users and roles dropped before DROP USER and DROP ROLE did
// so themselves left behind, which a user or role created later with the
// same name would otherwise get (see models.Store.ForgetDropped). Each
// replica runs it as it starts.
func (s *Server) ForgetDroppedPrincipals(ctx context.Context) error {
	disowned, forgotten, err := s.models.ForgetDropped(ctx, func(ctx context.Context) (models.Principals, error) {
		p := models.Principals{Users: map[string]time.Time{}, Roles: map[string]time.Time{}}
		users, err := s.users.List(ctx)
		if err != nil {
			return p, err
		}
		for _, u := range users {
			p.Users[u.Name] = u.CreatedAt
		}
		roles, err := s.grants.Roles(ctx)
		if err != nil {
			return p, err
		}
		for _, r := range roles {
			p.Roles[r.Name] = r.CreatedAt
		}
		return p, nil
	})
	if disowned > 0 || forgotten > 0 {
		s.log.Info("disowned the models, and removed the model grants, of users and roles dropped before",
			"models", disowned, "grants", forgotten)
	}
	return err
}

func (s *Server) dropModel(ctx context.Context, sess *session, user auth.User, st *snowflake.ModelStatement, w wire.DataWriter) error {
	tag := "DROP " + models.QualifiedClass(st.Class)
	m, err := s.findModel(ctx, sess, user, st.Name, st.Class)
	if err != nil {
		if pgerror.GetCode(err) == codes.UndefinedObject && st.IfExists {
			return w.Complete(tag)
		}
		return err
	}
	if m.Owner != user.Name && user.Role != auth.RoleAdmin {
		if p, err := s.modelPrivilege(ctx, user, m); err != nil || p == "" {
			return modelMissing(m.Schema, m.Name)
		}
		return denied("%s does not own model %s.%s", user.Name, m.Schema, m.Name)
	}
	if _, err := s.models.Delete(ctx, m.Schema, m.Name); err != nil {
		if errors.Is(err, models.ErrNotFound) && st.IfExists {
			return w.Complete(tag)
		}
		return err
	}
	s.dropBlob(ctx, m.Blob)
	return w.Complete(tag)
}

// modelShowColumns describe SHOW SNOWFLAKE.ML.<class>, as Snowflake's.
var modelShowColumns = wire.Columns{
	{Name: "created_on", Oid: pgtype.TimestamptzOID, Width: 8},
	{Name: "name", Oid: pgtype.TextOID, Width: -1},
	{Name: "database_name", Oid: pgtype.TextOID, Width: -1},
	{Name: "schema_name", Oid: pgtype.TextOID, Width: -1},
	{Name: "current_version", Oid: pgtype.TextOID, Width: -1},
	{Name: "comment", Oid: pgtype.TextOID, Width: -1},
	{Name: "owner", Oid: pgtype.TextOID, Width: -1},
}

func (s *Server) showModels(ctx context.Context, sess *session, user auth.User, st *snowflake.ModelStatement, w wire.DataWriter) error {
	all, err := s.models.List(ctx)
	if err != nil {
		return err
	}
	inSchema := ""
	if st.In == "SCHEMA" {
		if len(st.InName) > 0 {
			inSchema = strings.ToLower(st.InName[len(st.InName)-1])
		} else if _, cur, err := currentSchema(ctx, sess.conn); err == nil {
			inSchema = strings.ToLower(cur)
		}
	}
	for _, m := range all {
		if m.Class != st.Class || inSchema != "" && m.Schema != inSchema {
			continue
		}
		if st.Like != nil && !likeMatch(strings.ToLower(*st.Like), m.Name) {
			continue
		}
		if p, err := s.modelPrivilege(ctx, user, m); err != nil {
			return err
		} else if p == "" {
			continue
		}
		if err := w.Row([]any{m.CreatedAt, m.Name, s.catalogName(), m.Schema, "1", m.Comment, m.Owner}); err != nil {
			return err
		}
	}
	return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
}

// likeMatch matches SQL LIKE's % and _ (case-insensitively, as SHOW's
// LIKE does).
func likeMatch(pattern, s string) bool {
	var b strings.Builder
	for _, r := range pattern {
		switch r {
		case '%':
			b.WriteByte('*')
		case '_':
			b.WriteByte('?')
		case '*', '?', '[', '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		default:
			b.WriteRune(r)
		}
	}
	ok, _ := path.Match(b.String(), strings.ToLower(s))
	return ok
}

func (s *Server) grantModel(ctx context.Context, sess *session, user auth.User, st *snowflake.ModelStatement, w wire.DataWriter) error {
	privilege := ""
	switch st.Privilege {
	case "USAGE", "MLADMIN":
		privilege = models.PrivilegeUsage
	case "MLCONSUMER":
		privilege = models.PrivilegePredict
	case "CREATE":
		return pgerror.WithHint(pgerror.WithCode(fmt.Errorf("%s CREATE %s is not supported", st.Verb, models.QualifiedClass(st.Class)),
			codes.FeatureNotSupported), "creating a model takes what creating a table does: the writer role, or GRANT CREATE ON SCHEMA "+
			strings.Join(st.Name, ".")+" TO ...")
	default:
		return modelSyntax("unknown instance role %s: models have MLADMIN (every method) and MLCONSUMER (predicting)", st.Privilege)
	}
	m, err := s.findModel(ctx, sess, user, st.Name, st.Class)
	if err != nil {
		return err
	}
	if m.Owner != user.Name && user.Role != auth.RoleAdmin {
		if p, err := s.modelPrivilege(ctx, user, m); err != nil || p == "" {
			return modelMissing(m.Schema, m.Name)
		}
		return denied("only the owner of model %s.%s and admins may grant it", m.Schema, m.Name)
	}
	granteeType, err := s.granteeType(ctx, st.Grantee)
	if err != nil {
		return err
	}
	g := models.Grant{Schema: m.Schema, Name: m.Name, Grantee: st.Grantee, GranteeType: granteeType, Privilege: privilege, GrantedBy: user.Name}
	if st.Verb == "GRANT" {
		err = s.models.AddGrant(ctx, g)
	} else {
		err = s.models.RevokeGrant(ctx, g)
	}
	if errors.Is(err, models.ErrNotFound) {
		return modelMissing(m.Schema, m.Name)
	}
	if err != nil {
		return err
	}
	return w.Complete(st.Verb)
}

// methodColumns are the columns a model's method returns.
func methodColumns(class, method string) ([]models.ResultColumn, error) {
	if cols, ok := models.MethodColumns(class, method); ok {
		return cols, nil
	}
	if class == models.ClassClassification && method == "PREDICT" {
		return nil, pgerror.WithHint(pgerror.WithCode(errors.New("PREDICT is not a procedure"), codes.WrongObjectType),
			"score rows in a query: SELECT model!PREDICT(INPUT_DATA => {*}) FROM t")
	}
	if models.IsRowMethodOf(class, method) {
		return nil, pgerror.WithHint(pgerror.WithCode(fmt.Errorf("%s is not a procedure: it scores rows", method), codes.WrongObjectType),
			"call it as a table, SELECT * FROM TABLE(model!"+method+"(INPUT_DATA => TABLE(t))), or in a query, model!"+method+
				"(INPUT_DATA => {*})")
	}
	return nil, pgerror.WithCode(fmt.Errorf("%s has no method %s; it has %s", models.QualifiedClass(class), method,
		strings.Join(models.Methods(class), ", ")), codes.UndefinedFunction)
}

// wireColumns describes a method's result: in upper case as Snowflake
// names it in that dialect, lower case otherwise.
func wireColumns(cols []models.ResultColumn, upper bool) wire.Columns {
	out := make(wire.Columns, len(cols))
	for i, c := range cols {
		name := c.Name
		if !upper {
			name = strings.ToLower(name)
		}
		col := wire.Column{Name: name, Oid: pgtype.TextOID, Width: -1}
		switch c.Kind {
		case models.KindDouble:
			col.Oid, col.Width = pgtype.Float8OID, 8
		case models.KindBigint:
			col.Oid, col.Width = pgtype.Int8OID, 8
		case models.KindBoolean:
			col.Oid, col.Width = pgtype.BoolOID, 1
		case models.KindTimestamp:
			col.Oid, col.Width = pgtype.TimestampOID, 8
		}
		out[i] = col
	}
	return out
}

// callModel carries out CALL model!METHOD(...).
func (s *Server) callModel(ctx context.Context, sess *session, user auth.User, st *snowflake.ModelStatement, w wire.DataWriter) error {
	res, err := s.modelMethod(ctx, sess, user, st.Call)
	if err != nil {
		return err
	}
	for _, row := range res.Rows {
		out := make([]any, len(row))
		for i, v := range row {
			if f, ok := v.(float64); ok && (math.IsNaN(f) || math.IsInf(f, 0)) {
				v = nil
			}
			out[i] = v
		}
		if err := w.Row(out); err != nil {
			return err
		}
	}
	return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
}

// modelMethod computes a model's method for user, for CALL and for TABLE()
// in a query.
func (s *Server) modelMethod(ctx context.Context, sess *session, user auth.User, call *snowflake.ModelCall) (*models.Result, error) {
	m, err := s.findModel(ctx, sess, user, call.Name, "")
	if err != nil {
		return nil, err
	}
	if err := s.mayCall(ctx, user, m, call.Method); err != nil {
		return nil, err
	}
	if _, err := methodColumns(m.Class, call.Method); err != nil {
		return nil, err
	}
	art, err := s.modelRuntime.Load(ctx, m.Blob)
	if err != nil {
		return nil, fmt.Errorf("load model %s.%s: %w", m.Schema, m.Name, err)
	}
	if art.Forecaster != nil {
		return s.forecastMethod(ctx, sess, user, art, call)
	}
	if models.IsIgloo(art.Class) {
		return s.iglooMethod(ctx, sess, user, art, call)
	}
	if art.Class == models.ClassTopInsights {
		if err := onlyArgs(call, "INPUT_DATA", "LABEL_COLNAME", "METRIC_COLNAME"); err != nil {
			return nil, err
		}
		label, err := stringArg(call, "LABEL_COLNAME", true)
		if err != nil {
			return nil, err
		}
		metric, err := stringArg(call, "METRIC_COLNAME", true)
		if err != nil {
			return nil, err
		}
		t, err := s.readInput(ctx, sess, user, call.Arg("INPUT_DATA"), "INPUT_DATA")
		if err != nil {
			return nil, err
		}
		return models.GetDrivers(t, label, metric)
	}
	if len(call.Args) > 0 {
		return nil, modelSyntax("%s!%s takes no arguments", strings.Join(call.Name, "."), call.Method)
	}
	return art.Method(call.Method)
}

// forecastMethod computes a FORECAST or ANOMALY_DETECTION model's method,
// reading INPUT_DATA, if given, as user.
func (s *Server) forecastMethod(ctx context.Context, sess *session, user auth.User, art *models.Artifact, call *snowflake.ModelCall) (*models.Result, error) {
	f := art.Forecaster
	interval := 0.95
	if call.Method == "DETECT_ANOMALIES" {
		interval = 0.99
	}
	q := models.ForecastQuery{}
	var accepted []string
	switch call.Method {
	case "FORECAST":
		accepted = []string{"FORECASTING_PERIODS", "INPUT_DATA", "TIMESTAMP_COLNAME", "SERIES_COLNAME", "SERIES_VALUE", "CONFIG_OBJECT"}
		// FORECAST(7) is FORECAST(FORECASTING_PERIODS => 7).
		if len(call.Args) == 1 && call.Args[0].Name == "" {
			call.Args[0].Name = "FORECASTING_PERIODS"
		}
	case "DETECT_ANOMALIES", "SHOW_EVALUATION_METRICS":
		accepted = []string{"INPUT_DATA", "TIMESTAMP_COLNAME", "TARGET_COLNAME", "SERIES_COLNAME", "CONFIG_OBJECT"}
	default:
		if len(call.Args) > 0 {
			return nil, modelSyntax("%s!%s takes no arguments", strings.Join(call.Name, "."), call.Method)
		}
		return f.Method(art.Class, call.Method)
	}
	if err := onlyArgs(call, accepted...); err != nil {
		return nil, err
	}
	if call.Method == "SHOW_EVALUATION_METRICS" && call.Arg("INPUT_DATA") == nil {
		if len(call.Args) > 0 {
			return nil, modelSyntax("SHOW_EVALUATION_METRICS takes INPUT_DATA with its other arguments, or none")
		}
		return f.Method(art.Class, call.Method)
	}
	if cfg := call.Arg("CONFIG_OBJECT"); cfg != nil {
		for i, k := range cfg.Keys {
			switch strings.ToLower(k) {
			case "prediction_interval":
				v := cfg.Elems[i]
				if v.Kind != snowflake.ValueNumber || v.Num <= 0 || v.Num >= 1 {
					return nil, modelSyntax("prediction_interval is a number between 0 and 1")
				}
				interval = v.Num
			case "on_error":
				q.SkipErrors = cfg.Elems[i].Kind == snowflake.ValueString && strings.EqualFold(cfg.Elems[i].Str, "skip")
			default:
				return nil, modelSyntax("unknown CONFIG_OBJECT key %s", k)
			}
		}
	}
	q.Interval = interval
	q.Foundation, q.Context = s.forecasting, ctx
	var err error
	if q.TimeCol, err = stringArg(call, "TIMESTAMP_COLNAME", false); err != nil {
		return nil, err
	}
	if q.TargetCol, err = stringArg(call, "TARGET_COLNAME", false); err != nil {
		return nil, err
	}
	if q.SeriesCol, err = stringArg(call, "SERIES_COLNAME", false); err != nil {
		return nil, err
	}
	if v := call.Arg("SERIES_VALUE"); v != nil {
		key := ""
		switch v.Kind {
		case snowflake.ValueString:
			key = v.Str
		case snowflake.ValueNumber:
			key = strconv.FormatFloat(v.Num, 'f', -1, 64)
		default:
			return nil, modelSyntax("SERIES_VALUE takes a series' value")
		}
		q.SeriesValue = &key
	}
	if v := call.Arg("FORECASTING_PERIODS"); v != nil {
		if v.Kind != snowflake.ValueNumber || v.Num < 1 || v.Num != math.Trunc(v.Num) || v.Num > 1_000_000 {
			return nil, modelSyntax("FORECASTING_PERIODS takes a positive whole number")
		}
		q.Periods = int(v.Num)
	}
	if in := call.Arg("INPUT_DATA"); in != nil {
		if q.TimeCol == "" {
			return nil, modelSyntax("%s with INPUT_DATA needs TIMESTAMP_COLNAME", call.Method)
		}
		if q.Input, err = s.readInput(ctx, sess, user, in, "INPUT_DATA"); err != nil {
			return nil, err
		}
	} else if call.Method != "FORECAST" {
		return nil, modelSyntax("%s needs INPUT_DATA, TIMESTAMP_COLNAME and TARGET_COLNAME", call.Method)
	}
	switch call.Method {
	case "FORECAST":
		if q.Periods > 0 && q.Input != nil {
			return nil, modelSyntax("FORECAST takes FORECASTING_PERIODS or INPUT_DATA, not both")
		}
		return f.Forecast(q)
	case "DETECT_ANOMALIES":
		return f.Detect(q)
	}
	return f.EvaluateOn(q)
}
