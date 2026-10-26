package pgwire

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/ai"
	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/policy"
)

// fakeModels is a provider for tests, in OpenAI's shape: chat completions
// answer "echo: " and the user's text, unless answer says otherwise. No
// test reaches a real provider or uses a real key.
type fakeModels struct {
	srv   *httptest.Server
	calls atomic.Int64
	mu    sync.Mutex
	// prompts are the user texts it was sent, in order.
	prompts []string
	// systems are the system texts.
	systems []string
	answer  func(user, system string, body map[string]any) (int, any)
}

func newFakeModels(t *testing.T) *fakeModels {
	f := &fakeModels{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(b, &body)
		var user, system string
		msgs, _ := body["messages"].([]any)
		for _, m := range msgs {
			mm, _ := m.(map[string]any)
			switch mm["role"] {
			case "user":
				user, _ = mm["content"].(string)
			case "system":
				system, _ = mm["content"].(string)
			}
		}
		f.calls.Add(1)
		f.mu.Lock()
		f.prompts = append(f.prompts, user)
		f.systems = append(f.systems, system)
		answer := f.answer
		f.mu.Unlock()
		status, out := 200, any(openAIText("echo: "+user))
		if answer != nil {
			status, out = answer(user, system, body)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func openAIText(text string) map[string]any {
	return map[string]any{"model": "fake-1", "choices": []any{map[string]any{"message": map[string]any{"content": text}, "finish_reason": "stop"}},
		"usage": map[string]any{"prompt_tokens": 100, "completion_tokens": 10}}
}

func (f *fakeModels) sent() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.prompts...)
}

// withAI gives the server a runtime of its own, in memory, that may
// reach the fake provider on the loopback address.
func withAI(o *Options) {
	o.AI = ai.NewRuntime(ai.Options{Cache: true, MaxCalls: 1000, MaxCostUSD: 1, RequestTimeout: 10 * time.Second})
}

// createFakeIntegration makes the integration "models" at f, mapping
// claude-4-sonnet and mistral-large2 to the fake's model, with a price.
func createFakeIntegration(t *testing.T, admin *pgx.Conn, f *fakeModels) {
	t.Helper()
	mustExec(t, admin, `CREATE AI INTEGRATION models PROVIDER = openai BASE_URL = '`+f.srv.URL+`/v1' API_KEY = 'sk-test-not-a-real-key'
		MODELS = ('claude-4-sonnet' = 'fake-1', 'mistral-large2' = 'fake-1') DEFAULT_MODEL = 'fake-1'
		PRICES = ('fake-1' = (3, 15)) COMMENT 'a fake provider'`)
}

func TestAICompleteEndToEnd(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cs := startCatalogServer(t, withAI)
	f := newFakeModels(t)
	mustExec(t, cs.admin, "CREATE TABLE lake.public.reviews (id INTEGER, review VARCHAR, stars INTEGER)")
	mustExec(t, cs.admin, `INSERT INTO lake.public.reviews VALUES (1, 'loved the kettle', 5), (2, 'too loud', 2), (3, 'loved the kettle', 5),
		(4, NULL, 1)`)

	// Off until an integration exists.
	expectCode(t, cs.admin, "SELECT ai_complete('claude-4-sonnet', review, '{}') FROM lake.public.reviews", "55000", "CREATE AI INTEGRATION")
	createFakeIntegration(t, cs.admin, f)
	expectCode(t, cs.admin, "CREATE AI INTEGRATION models PROVIDER = openai API_KEY = 'x'", "42710")
	expectCode(t, cs.admin, "CREATE AI INTEGRATION other PROVIDER = cohere API_KEY = 'x'", "22023", "PROVIDER")
	// Keys are never shown, nor kept in the history.
	show := strings.Join(texts(t, cs.admin, "SHOW AI INTEGRATIONS"), "\n")
	if strings.Contains(show, "sk-test") || !strings.Contains(show, "claude-4-sonnet = fake-1") {
		t.Errorf("SHOW AI INTEGRATIONS: %s", show)
	}
	if q := lastQuery(t, cs, "igloo", "CREATE AI INTEGRATION models PROVIDER = openai BASE_URL"); strings.Contains(q.Text, "sk-test") {
		t.Errorf("the history keeps the key: %s", q.Text)
	}

	// Snowflake's dialect: each distinct input is sent once; NULL in, NULL out.
	sf := cs.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, sf, "SET sql_dialect = 'snowflake'")
	mustExec(t, sf, "SET query_tag = 'client:demo'")
	got := texts(t, sf, `SELECT id, AI_COMPLETE(model => 'claude-4-sonnet', prompt => PROMPT('Review: {0}. Stars: {1}.', review, stars))
		FROM lake.public.reviews ORDER BY id`)
	want := []string{"1|echo: Review: loved the kettle. Stars: 5.", "2|echo: Review: too loud. Stars: 2.",
		"3|echo: Review: loved the kettle. Stars: 5.", "4|echo: Review: . Stars: 1."}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("AI_COMPLETE:\n got %q\nwant %q", got, want)
	}
	if n := f.calls.Load(); n != 3 {
		t.Errorf("%d requests for 3 distinct prompts", n)
	}
	// Again: the cache answers, and costs nothing.
	texts(t, sf, `SELECT AI_COMPLETE('claude-4-sonnet', review) FROM lake.public.reviews`)
	n := f.calls.Load()
	texts(t, sf, `SELECT AI_COMPLETE('claude-4-sonnet', review) FROM lake.public.reviews`)
	if f.calls.Load() != n {
		t.Errorf("a second run sent %d requests; the cache should answer", f.calls.Load()-n)
	}
	texts(t, sf, `SELECT AI_COMPLETE('claude-4-sonnet', review, refresh => TRUE) FROM lake.public.reviews`)
	if f.calls.Load() != n+2 {
		t.Errorf("refresh => TRUE sent %d requests, want 2", f.calls.Load()-n)
	}

	// The spend reaches the history and igloo.usage, by query tag.
	q := lastAIQuery(t, cs, "igloo", "PROMPT")
	if q.AI == nil || q.AI.Calls != 3 || q.AI.InputTokens != 300 || q.AI.OutputTokens != 30 || q.QueryTag != "client:demo" {
		t.Errorf("history %+v %+v", q, q.AI)
	} else if want := (300*3 + 30*15) / 1e6; q.AI.CostUSD < want*0.99 || q.AI.CostUSD > want*1.01 {
		t.Errorf("cost %v, want %v", q.AI.CostUSD, want)
	}
	usage := texts(t, cs.admin, "SELECT query_tag, ai_calls FROM igloo.usage WHERE ai_calls > 0")
	if len(usage) != 1 || usage[0] != "client:demo|7" {
		t.Errorf("igloo.usage %q", usage)
	}

	// SNOWFLAKE.CORTEX.COMPLETE, AI_COUNT_TOKENS and the error forms.
	if got := texts(t, sf, `SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', 'hello')`); len(got) != 1 || got[0] != "echo: hello" {
		t.Errorf("CORTEX.COMPLETE %q", got)
	}
	if got := texts(t, sf, `SELECT AI_COUNT_TOKENS('ai_complete', 'claude-4-sonnet', 'twelve chars')`); len(got) != 1 || got[0] != "3" {
		t.Errorf("AI_COUNT_TOKENS %q", got)
	}
	expectCode(t, sf, `SELECT AI_COMPLETE('gpt-nope', review) FROM lake.public.reviews`, "42704", "MODELS")
	f.mu.Lock()
	f.answer = func(user, _ string, _ map[string]any) (int, any) {
		if strings.Contains(user, "loud") {
			return 400, map[string]any{"error": map[string]any{"message": "not this one"}}
		}
		return 200, openAIText("ok")
	}
	f.mu.Unlock()
	got = texts(t, sf, `SELECT id, AI_COMPLETE('claude-4-sonnet', 'x ' || review), AI_COMPLETE('claude-4-sonnet', 'x ' || review,
		return_error_details => TRUE):error::VARCHAR FROM lake.public.reviews WHERE id IN (1, 2) ORDER BY id`)
	if len(got) != 2 || got[0] != "1|ok|NULL" || !strings.HasPrefix(got[1], "2|NULL|the provider answered 400: not this one") {
		t.Errorf("row errors %q", got)
	}
	expectCode(t, sf, `SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-4-sonnet', 'y ' || review) FROM lake.public.reviews WHERE id = 2`, "58030",
		"not this one")
	_ = ctx
}

// TestAIGrantsAndCaps: users need USAGE on an integration; a statement
// that would go over its cap fails, whether the server's, a role's or the
// session's lower one.
func TestAIGrantsAndCaps(t *testing.T) {
	t.Parallel()
	cs := startCatalogServer(t, withAI)
	f := newFakeModels(t)
	createFakeIntegration(t, cs.admin, f)
	mustExec(t, cs.admin, "CREATE TABLE lake.public.notes (id INTEGER, note VARCHAR)")
	mustExec(t, cs.admin, "INSERT INTO lake.public.notes SELECT i, 'note ' || i FROM range(20) t(i)")
	mustExec(t, cs.admin, "CREATE ROLE analysts")
	ana := cs.user(t, "ana", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	expectCode(t, ana, "SELECT ai_complete('claude-4-sonnet', note, '{}') FROM lake.public.notes", "42501", "GRANT USAGE ON INTEGRATION")
	expectCode(t, ana, "GRANT USAGE ON INTEGRATION models TO ROLE analysts", "42501")
	mustExec(t, cs.admin, "GRANT USAGE ON INTEGRATION models TO ROLE analysts")
	mustExec(t, cs.admin, "GRANT analysts TO ana")
	if got := texts(t, ana, "SELECT count(ai_complete('claude-4-sonnet', note, '{}')) FROM lake.public.notes WHERE id < 3"); len(got) != 1 || got[0] != "3" {
		t.Errorf("granted: %q", got)
	}
	if got := texts(t, ana, "SHOW AI INTEGRATIONS"); len(got) != 1 || strings.Contains(got[0], "role analysts") {
		t.Errorf("a user sees the integrations they may use, without who else may: %q", got)
	}

	// The session lowers its cap.
	mustExec(t, ana, "SET ai_max_calls = 5")
	expectCode(t, ana, "SELECT ai_complete('claude-4-sonnet', note || ' again', '{}') FROM lake.public.notes", "54000", "ai_max_calls")
	mustExec(t, ana, "RESET ai_max_calls")
	// A role's cap, over the server's.
	mustExec(t, cs.admin, "ALTER ROLE analysts SET ai_max_calls = 4")
	ana2 := connect(t, cs.url("ana", "ana-password"), pgx.QueryExecModeSimpleProtocol)
	mustExec(t, ana2, "SET search_path = 'lake.public,memory.main'")
	expectCode(t, ana2, "SELECT ai_complete('claude-4-sonnet', note || ' twice', '{}') FROM lake.public.notes", "54000", "role analysts")
	// The cost cap: at $3 a million tokens, 100 tokens a call, a
	// hundredth of a cent is three calls.
	mustExec(t, ana2, "SET ai_max_calls = 1000")
	mustExec(t, cs.admin, "ALTER ROLE analysts RESET ai_max_calls")
	mustExec(t, cs.admin, "ALTER ROLE analysts SET ai_max_cost_usd = 0.0001")
	ana3 := connect(t, cs.url("ana", "ana-password"), pgx.QueryExecModeSimpleProtocol)
	mustExec(t, ana3, "SET search_path = 'lake.public,memory.main'")
	expectCode(t, ana3, "SELECT ai_complete('claude-4-sonnet', note || ' thrice', '{}') FROM lake.public.notes", "54000", "ai_max_cost_usd")

	// Revoked, refused again; dropped, gone.
	mustExec(t, cs.admin, "REVOKE USAGE ON INTEGRATION models FROM ROLE analysts")
	cs.srv.ai.Changed()
	expectCode(t, ana3, "SELECT ai_complete('claude-4-sonnet', 'x', '{}')", "42501")
	mustExec(t, cs.admin, "DROP INTEGRATION models")
	expectCode(t, cs.admin, "SELECT ai_complete('claude-4-sonnet', 'x', '{}')", "55000")
}

// TestAIReadsMaskedValues: masking policies apply before values reach a
// provider, and a table a clean room confines is refused, admins
// included.
func TestAIReadsMaskedValues(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cs := startPolicyServer(t, withAI)
	f := newFakeModels(t)
	createFakeIntegration(t, cs.admin, f)
	// The admin sees every row, and emails masked: the role pii_reader
	// would unmask them.
	got := texts(t, cs.admin, "SELECT ai_complete('claude-4-sonnet', 'write to ' || email, '{}') FROM lake.crm.contacts")
	for _, p := range f.sent() {
		if strings.Contains(p, "@") {
			t.Errorf("a masked email reached the provider: %q", p)
		}
	}
	if len(f.sent()) == 0 {
		t.Errorf("nothing was sent: %q", got)
	}

	err := cs.srv.gov.store.Update(ctx, func(c *policy.Catalog) error {
		return c.SetConfinements("tv", []policy.Confinement{{Schema: "public", Table: "orders", Room: "tv", Users: []string{"igloo"}}})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cs.srv.gov.reload(ctx); err != nil {
		t.Fatal(err)
	}
	expectAllowed(t, cs.admin, "SELECT amount FROM lake.public.orders")
	expectCode(t, cs.admin, "SELECT ai_complete('claude-4-sonnet', CAST(amount AS VARCHAR), '{}') FROM lake.public.orders", "42501",
		"clean room tv")
}

// TestAIExplain: EXPLAIN of a query that calls AI functions says what they
// would send, model by model, with the cost where the integration has a
// price, and what the cache would answer; it sends nothing.
func TestAIExplain(t *testing.T) {
	t.Parallel()
	cs := startCatalogServer(t, withAI)
	f := newFakeModels(t)
	createFakeIntegration(t, cs.admin, f)
	mustExec(t, cs.admin, "CREATE TABLE lake.public.ads (id INTEGER, copy VARCHAR)")
	mustExec(t, cs.admin, "INSERT INTO lake.public.ads SELECT i, 'ad ' || (i % 4) FROM range(40) t(i)")
	texts(t, cs.admin, "SELECT ai_complete('claude-4-sonnet', copy, '{}') FROM lake.public.ads WHERE id < 2")
	sent := f.calls.Load()
	got := strings.Join(texts(t, cs.admin, "EXPLAIN SELECT ai_complete('claude-4-sonnet', copy, '{}') FROM lake.public.ads"), "\n")
	if f.calls.Load() != sent {
		t.Errorf("EXPLAIN sent %d requests", f.calls.Load()-sent)
	}
	for _, want := range []string{"ai calls|models/fake-1: 2 distinct inputs to send", "about $", "ai cache|2 distinct inputs answered from the cache",
		"ai caps|at most 1000 calls"} {
		if !strings.Contains(got, want) {
			t.Errorf("EXPLAIN does not say %q:\n%s", want, got)
		}
	}
}

// TestAISettings reads and writes the session's AI settings.
func TestAISettings(t *testing.T) {
	t.Parallel()
	cs := startCatalogServer(t, withAI)
	for _, c := range []struct{ set, show, want string }{
		{"SET ai_integration = 'models'", "SHOW ai_integration", "models"},
		{"ALTER SESSION SET ai_max_calls = 7", "SHOW ai_max_calls", "7"},
		{"SET ai_max_cost_usd = 0.5", "SHOW ai_max_cost_usd", "0.5"},
		{"SET ai_decision_threshold = 0.8", "SHOW ai_decision_threshold", "0.8"},
		{"RESET ai_max_calls", "SHOW ai_max_calls", "1000"},
	} {
		mustExec(t, cs.admin, c.set)
		if got := texts(t, cs.admin, c.show); len(got) != 1 || got[0] != c.want {
			t.Errorf("%s: %q, want %s", c.show, got, c.want)
		}
	}
	expectCode(t, cs.admin, "SET ai_decision_threshold = 2", "22023")
	expectCode(t, cs.admin, "SET ai_max_calls = 0", "22023")
}

// TestAIOnTheCoordinator: the distributed planner keeps the AI functions
// on the coordinator, which holds the keys: workers read and filter, and
// the coordinator calls the provider for what they return.
func TestAIOnTheCoordinator(t *testing.T) {
	t.Parallel()
	ds := startDistServer(t, 2, withAI)
	f := newFakeModels(t)
	createFakeIntegration(t, ds.admin, f)
	conn := ds.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "SET distributed_execution = on")
	// Workers group, and the coordinator calls the provider for the
	// groups they return.
	query := `SELECT ai_complete('claude-4-sonnet', CAST(amount AS VARCHAR), '{}') AS a
		FROM (SELECT amount FROM lake.public.orders WHERE amount > 0 GROUP BY amount) s`
	got := texts(t, conn, query)
	if len(got) == 0 || !strings.HasPrefix(got[0], "echo: ") {
		t.Fatalf("results %q", got)
	}
	for _, row := range texts(t, conn, "EXPLAIN DISTRIBUTED "+query) {
		step, sql, _ := strings.Cut(row, "|")
		if strings.HasPrefix(step, "stage") && strings.Contains(sql, "ai_complete") {
			t.Errorf("a worker's stage calls the AI function: %s", row)
		}
		if step == "the coordinator" && !strings.Contains(sql, "ai_complete") {
			t.Errorf("the coordinator's step does not call it: %s", row)
		}
	}
	q := lastAIQuery(t, ds.catalogServer, "igloo", "GROUP BY amount")
	if q.AI == nil || q.AI.Calls == 0 {
		t.Errorf("history %+v", q.AI)
	}
	// A scan of the table itself: the planner refuses to give workers the
	// call, and says why.
	scan := `SELECT ai_complete('claude-4-sonnet', CAST(amount AS VARCHAR), '{}') AS a, count(*) FROM lake.public.orders WHERE amount > 0 GROUP BY 1`
	explained := strings.Join(texts(t, conn, "EXPLAIN DISTRIBUTED "+scan), "\n")
	if !strings.Contains(explained, "calls a model provider with keys only the coordinator holds") {
		t.Errorf("EXPLAIN DISTRIBUTED does not say the AI function stays on the coordinator:\n%s", explained)
	}
	texts(t, conn, scan)
	if q := lastAIQuery(t, ds.catalogServer, "igloo", "WHERE amount > 0"); q.Workers != 0 {
		t.Errorf("a scan calling an AI function ran on %d workers", q.Workers)
	}
}

// TestAICacheTagsIcebergTables: what AI functions cache of an Iceberg
// table's rows is tagged with the table, as erasure requests name it, and
// an erasure's purge of the table removes it.
func TestAICacheTagsIcebergTables(t *testing.T) {
	t.Parallel()
	env := newCatalogEnv(t)
	rt := ai.NewRuntime(ai.Options{Cache: true, MaxCalls: 1000, MaxCostUSD: 1, RequestTimeout: 10 * time.Second})
	env.srv.ai = rt
	ctx := context.Background()
	f := newFakeModels(t)
	admin := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	createFakeIntegration(t, admin, f)
	mustExec(t, admin, "CREATE TABLE notes (id INTEGER, note VARCHAR)")
	mustExec(t, admin, "INSERT INTO notes VALUES (1, 'call back on monday'), (2, 'renewal due')")
	texts(t, admin, "SELECT ai_complete('claude-4-sonnet', note, '{}') FROM (SELECT note FROM notes WHERE id > 1) recent")
	store := rt.Store()
	if n, _ := store.Remaining(ctx, []string{"public.notes"}); n != 0 {
		t.Errorf("%d entries remaining before any purge", n)
	}
	deleted, err := store.Purge(ctx, []string{"public.notes"})
	if err != nil || deleted != 1 {
		t.Errorf("purged %d entries of public.notes, want 1: %v", deleted, err)
	}
	// Run again, it asks the provider again: nothing of the table is left.
	before := f.calls.Load()
	texts(t, admin, "SELECT ai_complete('claude-4-sonnet', note, '{}') FROM notes WHERE id = 2")
	if f.calls.Load() != before+1 {
		t.Errorf("the purged answer was still cached")
	}
}

// lastAIQuery is the newest history record of user that called an AI
// function and whose text has part.
func lastAIQuery(t *testing.T, cs *catalogServer, user, part string) audit.Query {
	t.Helper()
	qs, err := cs.history.Queries(context.Background(), audit.Viewer{All: true}, audit.Page{})
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range qs {
		if q.User == user && q.AI != nil && strings.Contains(q.Text, part) {
			return q
		}
	}
	t.Fatalf("no statement of %s with %q calling AI functions in the history", user, part)
	return audit.Query{}
}
