package ai

import (
	"context"
	"database/sql"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/duckdb/duckdb-go/v2"
)

// fixedHook gives every call the one Statement it holds, and counts binds.
type fixedHook struct {
	s     atomic.Pointer[Statement]
	binds atomic.Int64
}

func (h *fixedHook) Bind(context.Context) func() *Statement {
	h.binds.Add(1)
	return func() *Statement { return h.s.Load() }
}

func duckWithAI(t *testing.T, hook Hook) *sql.DB {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	if err := Register(conn, func() Hook { return hook }); err != nil {
		t.Fatal(err)
	}
	return db
}

// TestFunctionsInDuckDB runs the functions as DuckDB calls them: on its
// threads, a chunk at a time, each distinct input sent once.
func TestFunctionsInDuckDB(t *testing.T) {
	t.Parallel()
	f := newFake(t, echo)
	rt := testRuntime(t, f)
	hook := &fixedHook{}
	db := duckWithAI(t, hook)
	ctx := context.Background()

	// Without a Statement, every call fails.
	_, err := db.ExecContext(ctx, "SELECT ai_complete('m', 'x', '{}')")
	if err == nil || !strings.Contains(err.Error(), "AI functions run only in statements") {
		t.Errorf("without a statement: %v", err)
	}

	s := begin(t, rt)
	hook.s.Store(s)
	var n, distinct int64
	err = db.QueryRowContext(ctx, `SELECT count(*), count(DISTINCT a) FROM (
		SELECT ai_complete('m', 'review ' || (i % 7), '{}') AS a FROM range(50000) t(i))`).Scan(&n, &distinct)
	if err != nil {
		t.Fatal(err)
	}
	if n != 50000 || distinct != 7 {
		t.Errorf("%d rows, %d distinct answers", n, distinct)
	}
	if c := f.calls.Load(); c != 7 {
		t.Errorf("%d requests for 7 distinct inputs over DuckDB's threads", c)
	}

	// NULL in, NULL out; the details twin returns objects.
	var a, details sql.NullString
	err = db.QueryRowContext(ctx, `SELECT ai_complete('m', NULL, '{}'), CAST(ai_complete_details('m', 'review 1', '{}') AS VARCHAR)`).Scan(&a, &details)
	if err != nil {
		t.Fatal(err)
	}
	if a.Valid || details.String != `{"error":null,"value":"echo: review 1"}` {
		t.Errorf("null %v, details %q", a, details.String)
	}

	// A row's error, with return_error_details.
	f.answer = func(r fakeRequest) (int, any) {
		if strings.Contains(userText(r), "bad") {
			return 400, map[string]any{"error": map[string]any{"message": "not this one"}}
		}
		return echo(r)
	}
	var plain, errText, raw sql.NullString
	err = db.QueryRowContext(ctx, `SELECT ai_complete('m', 'bad', '{}'), json_extract_string(ai_complete_details('m', 'bad', '{}'), '$.error'),
		CAST(ai_complete_details('m', 'bad', '{}') AS VARCHAR)`).Scan(&plain, &errText, &raw)
	if err != nil || plain.Valid || !strings.Contains(errText.String, "not this one") {
		t.Errorf("a row's error: %v %v %v %v", plain, errText, raw, err)
	}

	// A PROMPT object, as JSON.
	var p string
	if err := db.QueryRowContext(ctx, `SELECT ai_complete('m', json_object('template', 'Rate {0}', 'args', ['tea']), '{}')`).Scan(&p); err != nil {
		t.Fatal(err)
	}
	if p != "echo: Rate tea" {
		t.Errorf("prompt %q", p)
	}

	// An unmapped model fails the statement, with the hint.
	if _, err := db.ExecContext(ctx, `SELECT ai_complete('nope', 'x' || i, '{}') FROM range(3) t(i)`); err == nil ||
		!strings.Contains(err.Error(), "MODELS") {
		t.Errorf("unmapped: %v", err)
	}
}

// TestGuardedPredicates: DuckDB 1.5.6 orders a filter's conjuncts with
// the AI function's first, whatever order the statement writes them in,
// even across a subquery, and so calls it for every row; a CASE that
// tests the plain predicates first calls it only for the rows they keep.
// The Snowflake dialect's translation writes AI predicates so (see package
// snowflake's guardAI).
func TestGuardedPredicates(t *testing.T) {
	t.Parallel()
	f := newFake(t, echo)
	rt := testRuntime(t, f)
	hook := &fixedHook{}
	db := duckWithAI(t, hook)
	hook.s.Store(begin(t, rt))
	for _, q := range []string{
		`SELECT count(*) FROM range(100000) t(i) WHERE i % 20000 = 0 AND CASE WHEN i % 20000 = 0 THEN ai_complete('m', 'x' || i, '{}') IS NOT NULL ELSE false END`,
		`SELECT count(*) FROM (SELECT * FROM range(100000) t(i) WHERE i % 20000 = 0 OFFSET 0) WHERE ai_complete('m', 'y' || i, '{}') <> ''`,
	} {
		before := f.calls.Load()
		var n int64
		if err := db.QueryRowContext(context.Background(), q).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 5 {
			t.Errorf("%s: %d rows", q, n)
		}
		if c := f.calls.Load() - before; c != 5 {
			t.Errorf("%s: %d requests; the plain predicate should run first and keep 5 rows", q, c)
		}
	}
}

// TestTaskFunctionsInDuckDB runs the task functions as DuckDB calls them,
// with their result types: BOOLEAN, JSON, FLOAT[] and DOUBLE.
func TestTaskFunctionsInDuckDB(t *testing.T) {
	t.Parallel()
	var embeds atomic.Int64
	tasks, vectors := tasksFake(t), embedFake(t, &embeds)
	f := newFake(t, func(r fakeRequest) (int, any) {
		if strings.HasSuffix(r.Path, "/embeddings") {
			return vectors.answer(r)
		}
		return tasks.answer(r)
	})
	rt := testRuntime(t, f, func(in *Integration) {
		in.Models["e5-base-v2"] = "fake-embed"
		in.EmbeddingModel = "fake-embed"
	})
	hook := &fixedHook{}
	db := duckWithAI(t, hook)
	hook.s.Store(begin(t, rt))
	ctx := context.Background()

	var kept int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM range(1000) t(i) WHERE i % 250 = 0 AND
		CASE WHEN i % 250 = 0 THEN ai_filter('asks for a refund ' || (i % 500), '{}') ELSE false END`).Scan(&kept); err != nil {
		t.Fatal(err)
	}
	if kept != 4 || f.calls.Load() != 2 {
		t.Errorf("ai_filter kept %d rows with %d requests", kept, f.calls.Load())
	}

	var labels, details, vector string
	var similarity, score float64
	err := db.QueryRowContext(ctx, `SELECT
		json_extract_string(ai_classify('my flight', '["travel", "cooking"]', '{}'), '$.labels[0]'),
		CAST(ai_filter_details('says thanks', '{}') AS VARCHAR),
		CAST(ai_embed('e5-base-v2', 'bab', '{}') AS VARCHAR),
		ai_similarity('aa', 'aa', '{}'),
		ai_sentiment_score('awful', '{"on_error": "fail"}')`).Scan(&labels, &details, &vector, &similarity, &score)
	if err != nil {
		t.Fatal(err)
	}
	if labels != "travel" || details != `{"error":null,"value":false}` || vector != "[3.0, 1.0]" || similarity < 0.999999 || score != -1 {
		t.Errorf("classify %q, filter %q, embed %q, similarity %v, score %v", labels, details, vector, similarity, score)
	}
}
