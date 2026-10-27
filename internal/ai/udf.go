package ai

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/duckdb/duckdb-go/v2"
)

// Hook connects the functions to the statements that call them. Bind is
// called as DuckDB binds a call, with the context the statement is being
// prepared or run with; it notes that the statement calls an AI function
// and returns how to find the Statement it runs under, which the caller
// starts before the statement runs (Runtime.Begin). It returns nil outside
// a session, where the functions fail (ErrNoStatement).
type Hook interface {
	Bind(ctx context.Context) func() *Statement
}

// Function is one of the AI functions as DuckDB knows it: its name, its
// arguments' and result's types, and what it does with a chunk of rows.
type Function struct {
	Name   string
	Args   []string
	Result string
	// Eval answers rows, each the arguments of one row whose arguments
	// are all non-NULL; NULL in, NULL out.
	Eval func(s *Statement, rows []row) ([]any, error)
}

// Functions are the AI functions, by the names DuckDB's dialect calls
// them; the Snowflake dialect's AI_* and SNOWFLAKE.CORTEX.* translate to
// them (see package snowflake). Options are a JSON object, as text.
var Functions = []Function{
	{Name: "ai_complete", Args: []string{"VARCHAR", "ANY", "VARCHAR"}, Result: "VARCHAR",
		Eval: func(s *Statement, rows []row) ([]any, error) { return s.Complete(rows, false) }},
	{Name: "ai_complete_object", Args: []string{"VARCHAR", "ANY", "VARCHAR"}, Result: "JSON",
		Eval: func(s *Statement, rows []row) ([]any, error) { return s.Complete(rows, true) }},
	{Name: "ai_count_tokens", Args: []string{"VARCHAR", "VARCHAR", "ANY", "VARCHAR"}, Result: "BIGINT",
		Eval: func(s *Statement, rows []row) ([]any, error) { return s.CountTokens(rows) }},
	{Name: "ai_decide", Args: []string{"ANY", "VARCHAR", "VARCHAR"}, Result: "JSON",
		Eval: func(s *Statement, rows []row) ([]any, error) { return s.Decide(rows) }},
	{Name: "ai_classify", Args: []string{"ANY", "ANY", "VARCHAR"}, Result: "JSON",
		Eval: func(s *Statement, rows []row) ([]any, error) { return s.Classify(rows) }},
	{Name: "ai_filter", Args: []string{"ANY", "VARCHAR"}, Result: "BOOLEAN",
		Eval: func(s *Statement, rows []row) ([]any, error) { return s.Filter(rows) }},
	{Name: "ai_sentiment", Args: []string{"ANY", "ANY", "VARCHAR"}, Result: "JSON",
		Eval: func(s *Statement, rows []row) ([]any, error) { return s.Sentiment(rows) }},
	{Name: "ai_sentiment_score", Args: []string{"ANY", "VARCHAR"}, Result: "DOUBLE",
		Eval: func(s *Statement, rows []row) ([]any, error) { return s.SentimentScore(rows) }},
	{Name: "ai_extract", Args: []string{"ANY", "ANY", "VARCHAR"}, Result: "JSON",
		Eval: func(s *Statement, rows []row) ([]any, error) { return s.Extract(rows) }},
	{Name: "ai_extract_answer", Args: []string{"ANY", "ANY", "VARCHAR"}, Result: "JSON",
		Eval: func(s *Statement, rows []row) ([]any, error) { return s.ExtractAnswer(rows) }},
	{Name: "ai_summarize", Args: []string{"ANY", "VARCHAR"}, Result: "VARCHAR",
		Eval: func(s *Statement, rows []row) ([]any, error) { return s.Summarize(rows) }},
	{Name: "ai_translate", Args: []string{"ANY", "VARCHAR", "VARCHAR", "VARCHAR"}, Result: "VARCHAR",
		Eval: func(s *Statement, rows []row) ([]any, error) { return s.Translate(rows) }},
	{Name: "ai_redact", Args: []string{"ANY", "ANY", "VARCHAR"}, Result: "VARCHAR",
		Eval: func(s *Statement, rows []row) ([]any, error) { return s.Redact(rows, false) }},
	{Name: "ai_redact_detect", Args: []string{"ANY", "ANY", "VARCHAR"}, Result: "JSON",
		Eval: func(s *Statement, rows []row) ([]any, error) { return s.Redact(rows, true) }},
	{Name: "ai_embed", Args: []string{"VARCHAR", "ANY", "VARCHAR"}, Result: "FLOAT[]",
		Eval: func(s *Statement, rows []row) ([]any, error) { return s.Embed(rows) }},
	{Name: "ai_similarity", Args: []string{"ANY", "ANY", "VARCHAR"}, Result: "DOUBLE",
		Eval: func(s *Statement, rows []row) ([]any, error) { return s.Similarity(rows) }},
	{Name: "ai_agg", Args: []string{"VARCHAR[]", "VARCHAR", "VARCHAR"}, Result: "VARCHAR",
		Eval: func(s *Statement, rows []row) ([]any, error) { return s.Agg(rows) }},
}

// Names lists the AI functions' names, which the distributed planner keeps
// on the coordinator, where the keys are.
func Names() []string {
	var out []string
	for _, f := range Functions {
		out = append(out, f.Name, f.Name+"_details")
	}
	out = append(out, ForecastFunction, DetectFunction)
	slices.Sort(out)
	return out
}

// details are the functions with a twin that returns {"value", "error"}
// objects (return_error_details => TRUE), as Snowflake's AI functions
// since its 2026_02 bundle do: those whose result is not already an
// object that carries errors.
var details = map[string]bool{
	"ai_complete": true, "ai_complete_object": true, "ai_count_tokens": true, "ai_classify": true, "ai_filter": true,
	"ai_sentiment": true, "ai_summarize": true, "ai_translate": true, "ai_redact": true, "ai_redact_detect": true,
	"ai_embed": true, "ai_similarity": true,
}

// statementKey carries, in a call's bind context, how it finds its
// Statement.
type statementKey struct{}

// Register defines the AI functions on conn's database. hook, when it
// returns nil, has every call fail.
func Register(conn *sql.Conn, hook func() Hook) error {
	types := map[string]duckdb.TypeInfo{}
	typeOf := func(name string) (duckdb.TypeInfo, error) {
		if t, ok := types[name]; ok {
			return t, nil
		}
		var t duckdb.TypeInfo
		var err error
		switch name {
		case "ANY":
			t, err = duckdb.NewTypeInfo(duckdb.TYPE_ANY)
		case "VARCHAR":
			t, err = duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
		case "BOOLEAN":
			t, err = duckdb.NewTypeInfo(duckdb.TYPE_BOOLEAN)
		case "BIGINT":
			t, err = duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
		case "DOUBLE":
			t, err = duckdb.NewTypeInfo(duckdb.TYPE_DOUBLE)
		default:
			// JSON, FLOAT[], VARCHAR[]: as DuckDB describes them, which
			// keeps JSON's alias.
			t, err = describedType(conn, name)
		}
		if err == nil {
			types[name] = t
		}
		return t, err
	}
	for _, f := range Functions {
		variants := []bool{false}
		if details[f.Name] {
			variants = append(variants, true)
		}
		for _, withDetails := range variants {
			u := &udf{f: f, hook: hook, details: withDetails}
			name := f.Name
			result := f.Result
			if withDetails {
				name += "_details"
				result = "JSON"
			}
			u.json = result == "JSON"
			for _, a := range f.Args {
				t, err := typeOf(a)
				if err != nil {
					return fmt.Errorf("register %s: %w", name, err)
				}
				u.args = append(u.args, t)
			}
			t, err := typeOf(result)
			if err != nil {
				return fmt.Errorf("register %s: %w", name, err)
			}
			u.result = t
			if err := duckdb.RegisterScalarUDF(conn, name, u); err != nil {
				return fmt.Errorf("register %s: %w", name, err)
			}
		}
	}
	return nil
}

// describedType is the type DuckDB gives a NULL cast to name.
func describedType(conn *sql.Conn, name string) (duckdb.TypeInfo, error) {
	var t duckdb.TypeInfo
	err := conn.Raw(func(dc any) error {
		c := dc.(*duckdb.Conn)
		st, err := c.PrepareContext(context.Background(), "SELECT CAST(NULL AS "+name+")")
		if err != nil {
			return err
		}
		defer st.Close() //nolint:errcheck
		t, err = st.(*duckdb.Stmt).ColumnTypeInfo(0)
		return err
	})
	return t, err
}

// udf is a Function as a DuckDB scalar function: volatile, so that DuckDB
// neither folds a call of constants while it plans nor takes two calls
// for one, and calls it once a chunk.
type udf struct {
	f       Function
	hook    func() Hook
	details bool
	args    []duckdb.TypeInfo
	result  duckdb.TypeInfo
	// json says the result is JSON, which functions give as its text.
	json bool
}

func (u *udf) Config() duckdb.ScalarFuncConfig {
	return duckdb.ScalarFuncConfig{InputTypeInfos: u.args, ResultTypeInfo: u.result, Volatile: true}
}

func (u *udf) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{
		ScalarBinder: func(ctx context.Context, _ []duckdb.ScalarUDFArg) (context.Context, error) {
			var get func() *Statement
			if h := u.hook(); h != nil {
				get = h.Bind(ctx)
			}
			// The statement's own context ends with each step of it that
			// binds the call; the call's run reads its Statement's.
			return context.WithValue(context.WithoutCancel(ctx), statementKey{}, get), nil
		},
		ChunkContextExecutor: func(ctx context.Context, chunk *duckdb.ChunkIteratorState) error {
			get, _ := ctx.Value(statementKey{}).(func() *Statement)
			var s *Statement
			if get != nil {
				s = get()
			}
			if s == nil {
				return ErrNoStatement
			}
			var rows []row
			for st, err := range chunk.Rows() {
				if err != nil {
					return err
				}
				r := make(row, st.ColumnCount())
				for i := range r {
					r[i] = *st.GetValuePtr(i)
				}
				rows = append(rows, r)
			}
			if len(rows) == 0 {
				return nil
			}
			eval := u.f.Eval
			if u.details {
				eval = func(s *Statement, rows []row) ([]any, error) {
					for _, r := range rows {
						r[len(r)-1] = withOption(r[len(r)-1], "on_error", string(ErrorsDetails))
					}
					return u.f.Eval(s, rows)
				}
			}
			out, err := eval(s, rows)
			if err != nil {
				return err
			}
			k := 0
			for st, err := range chunk.Rows() {
				if err != nil {
					return err
				}
				var v any
				if k < len(out) {
					v = out[k]
				}
				k++
				if err := st.SetResult(resultValue(v, u.json)); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

// withOption returns the options, as text, with k set to v.
func withOption(opts any, k, v string) any {
	o, err := parseOptions(opts)
	if err != nil {
		return opts
	}
	o[k] = v
	return jsonText(o)
}

// resultValue is v as the result vector takes it: a vector as a list, and
// JSON text, for a JSON result, as itself, which the vector would
// otherwise write as a JSON string.
func resultValue(v any, isJSON bool) any {
	if s, ok := v.(string); ok && isJSON {
		return json.RawMessage(s)
	}
	if f, ok := v.([]float32); ok {
		out := make([]any, len(f))
		for i, x := range f {
			out[i] = x
		}
		return out
	}
	return v
}
