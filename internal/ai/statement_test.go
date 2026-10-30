package ai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func completeRows(model string, prompts ...string) []row {
	rows := make([]row, len(prompts))
	for i, p := range prompts {
		rows[i] = row{model, p, "{}"}
	}
	return rows
}

// TestCompleteDedupsAcrossThreads: rows that ask the same of the same
// model, from several of DuckDB's threads at once, make one request.
func TestCompleteDedupsAcrossThreads(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	seen := map[string]int{}
	f := newFake(t, func(r fakeRequest) (int, any) {
		mu.Lock()
		seen[userText(r)]++
		mu.Unlock()
		time.Sleep(20 * time.Millisecond) // so threads overlap
		return echo(r)
	})
	rt := testRuntime(t, f)
	s := begin(t, rt)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			// A chunk of 30 rows of three distinct texts.
			var prompts []string
			for i := range 30 {
				prompts = append(prompts, fmt.Sprintf("review %d", i%3))
			}
			out, err := s.Complete(completeRows("m", prompts...), false)
			if err != nil {
				t.Error(err)
				return
			}
			for i, v := range out {
				if want := fmt.Sprintf("echo: review %d", i%3); v != want {
					t.Errorf("row %d: %v, want %s", i, v, want)
				}
			}
		})
	}
	wg.Wait()
	if n := f.calls.Load(); n != 3 {
		t.Errorf("%d requests for 3 distinct inputs", n)
	}
	for text, n := range seen {
		if n != 1 {
			t.Errorf("%q sent %d times", text, n)
		}
	}
	sp := s.End()
	if sp.Calls != 3 || sp.InputTokens != 30 || sp.OutputTokens != 15 || sp.CachedInputTokens != 6 {
		t.Errorf("spend %+v", sp)
	}
	// $1 and $2 per million: 3 × (8 fresh + 2 cached/10, 5 out × 2).
	if want := 3 * (8 + 0.2 + 10) / 1e6; abs(sp.CostUSD-want) > 1e-12 {
		t.Errorf("cost %v, want %v", sp.CostUSD, want)
	}
	if len(sp.Models) != 1 || sp.Models[0] != "main/fake-model" {
		t.Errorf("models %v", sp.Models)
	}
	req := f.last()
	if req.Path != "/v1/chat/completions" || req.Header.Get("Authorization") != "Bearer sk-test-not-a-real-key" ||
		req.Body["model"] != "fake-model" || req.Body["max_tokens"] != float64(defaultMaxTokens) {
		t.Errorf("request %+v", req)
	}
	if _, ok := req.Body["temperature"]; ok {
		t.Errorf("temperature sent without being asked for")
	}
}

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

// TestCacheAcrossStatements: a statement asking what an earlier one asked
// reads the cache; refresh => TRUE asks again; the cache's entries carry
// the tables their statement read and its epoch.
func TestCacheAcrossStatements(t *testing.T) {
	t.Parallel()
	f := newFake(t, echo)
	rt := testRuntime(t, f)
	s1 := begin(t, rt)
	if _, err := s1.Complete(completeRows("m", "a", "b"), false); err != nil {
		t.Fatal(err)
	}
	s2 := begin(t, rt)
	out, err := s2.Complete(completeRows("m", "a", "b", "c"), false)
	if err != nil {
		t.Fatal(err)
	}
	if out[0] != "echo: a" || out[2] != "echo: c" {
		t.Errorf("results %v", out)
	}
	if n := f.calls.Load(); n != 3 {
		t.Errorf("%d requests, want 3: a and b come from the cache", n)
	}
	if sp := s2.End(); sp.Calls != 1 || sp.CacheHits != 2 {
		t.Errorf("second statement %+v", sp)
	}
	s3 := begin(t, rt)
	if _, err := s3.Complete([]row{{"m", "a", `{"refresh": true}`}}, false); err != nil {
		t.Fatal(err)
	}
	if n := f.calls.Load(); n != 4 {
		t.Errorf("%d requests: refresh => TRUE should ask again", n)
	}

	// Another model's answer to the same text is another entry.
	ms := rt.opts.Store.(*MemoryStore)
	ms.mu.Lock()
	n := len(ms.cache)
	for _, e := range ms.cache {
		if len(e.Tables) != 1 || e.Tables[0] != "demo.reviews" || e.Epoch != 1 || e.Function != "ai_complete" || e.Model != "main/fake-model" {
			t.Errorf("entry %+v", e)
		}
	}
	ms.mu.Unlock()
	if n != 3 {
		t.Errorf("%d cache entries, want 3", n)
	}

	// Off, nothing is read or kept.
	rt.opts.Cache = false
	s4 := begin(t, rt)
	if _, err := s4.Complete(completeRows("m", "a"), false); err != nil {
		t.Fatal(err)
	}
	if n := f.calls.Load(); n != 5 {
		t.Errorf("%d requests with the cache off", n)
	}
}

// TestCacheIsPerIntegration: integrations at one host, granted apart,
// never answer each other's calls from the cache, nor does an integration
// made again under an old one's name, or one whose API version changed;
// CREATE OR REPLACE with the same settings keeps the answers.
func TestCacheIsPerIntegration(t *testing.T) {
	t.Parallel()
	f := newFake(t, echo)
	rt := testRuntime(t, f)
	store := rt.opts.Store
	main, err := store.Integration(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	calls := int64(0)
	ask := func(name, what string, wantCall bool) {
		t.Helper()
		s := begin(t, rt, func(o *StatementOptions) { o.Integration = name })
		if out, err := s.Complete(completeRows("m", "same prompt"), false); err != nil || out[0] != "echo: same prompt" {
			t.Fatalf("%s: %v %v", what, out, err)
		}
		if wantCall {
			calls++
		}
		if n := f.calls.Load(); n != calls {
			t.Errorf("%s: %d requests, want %d", what, n, calls)
		}
	}
	ask("main", "the first call", true)
	ask("main", "the same call again", false)

	for _, other := range []Integration{
		{Name: "tenant_b", BaseURL: main.BaseURL},
		{Name: "gateway_b", BaseURL: f.srv.URL + "/tenant-b/v1"},
	} {
		in := main.Clone()
		in.Name, in.BaseURL = other.Name, other.BaseURL
		if err := store.CreateIntegration(ctx, in, false); err != nil {
			t.Fatal(err)
		}
		if err := store.GrantUsage(ctx, Grant{Integration: in.Name, Grantee: "analysts", GranteeType: "role"}); err != nil {
			t.Fatal(err)
		}
		ask(in.Name, "another integration at "+in.BaseURL, true)
	}

	replaced := main.Clone()
	replaced.APIKey = "sk-rotated-not-a-real-key"
	if err := store.CreateIntegration(ctx, replaced, true); err != nil {
		t.Fatal(err)
	}
	ask("main", "main replaced with a new key", false)
	replaced.APIVersion = "2026-01-01"
	if err := store.UpdateIntegration(ctx, replaced); err != nil {
		t.Fatal(err)
	}
	ask("main", "main with another API version", true)

	if err := store.DropIntegration(ctx, "tenant_b"); err != nil {
		t.Fatal(err)
	}
	again := main.Clone()
	again.Name, again.CreatedAt = "tenant_b", time.Time{}
	if err := store.CreateIntegration(ctx, again, false); err != nil {
		t.Fatal(err)
	}
	if err := store.GrantUsage(ctx, Grant{Integration: "tenant_b", Grantee: "analysts", GranteeType: "role"}); err != nil {
		t.Fatal(err)
	}
	ask("tenant_b", "an integration made again under a dropped one's name", true)
}

// TestPurgeStopsStatementsUnderway: an erasure purges the entries derived
// from its tables, and a statement that began before it keeps nothing more:
// it may have read the rows the purge was for. A statement that begins
// after it caches as before.
func TestPurgeStopsStatementsUnderway(t *testing.T) {
	t.Parallel()
	testStores(t, func(t *testing.T, store Store) {
		f := newFake(t, echo)
		rt := testRuntime(t, f)
		rt.opts.Store = store
		if err := store.CreateIntegration(ctx, Integration{Name: "main", Provider: ProviderOpenAI, BaseURL: f.srv.URL + "/v1",
			APIKey: "k", Models: map[string]string{"m": "fake-model"}, Enabled: true}, true); err != nil {
			t.Fatal(err)
		}
		admin := func(o *StatementOptions) { o.Admin = true }
		early := begin(t, rt, admin) // began before the erasure, reads demo.reviews
		other := begin(t, rt, admin, func(o *StatementOptions) { o.Tables = []string{"demo.campaigns"} })
		files := begin(t, rt, admin, func(o *StatementOptions) { o.Tables = []string{AnyTable} })
		for _, s := range []*Statement{early, other, files} {
			if _, err := s.Complete(completeRows("m", "x-"+s.opts.Tables[0]), false); err != nil {
				t.Fatal(err)
			}
		}
		if n, _ := store.Remaining(ctx, []string{"demo.reviews"}); n != 0 {
			t.Errorf("%d remaining before any purge", n)
		}
		deleted, err := store.Purge(ctx, []string{"DEMO.Reviews"})
		if err != nil {
			t.Fatal(err)
		}
		if deleted != 2 {
			t.Errorf("purged %d, want the reviews' entry and the files'", deleted)
		}
		// The early statement goes on, and keeps nothing.
		if _, err := early.Complete(completeRows("m", "late"), false); err != nil {
			t.Fatal(err)
		}
		if n, _ := store.Remaining(ctx, []string{"demo.reviews"}); n != 0 {
			t.Errorf("%d remaining: the early statement kept what it may have read from the erased rows", n)
		}
		hits, _ := store.Lookup(ctx, []string{cacheKeyOf(t, early, "late")}, time.Now())
		if len(hits) != 0 {
			t.Errorf("the early statement's answer was kept after the purge")
		}
		// The campaigns' entry stays, and counts for an erasure of them.
		if n, _ := store.Remaining(ctx, []string{"demo.campaigns"}); n != 1 {
			t.Errorf("%d remaining of the campaigns, want the one from before the purge", n)
		}
		// A statement begun after the purge keeps its answers, which read
		// the erased tables as they are now.
		late := begin(t, rt, admin)
		if _, err := late.Complete(completeRows("m", "after"), false); err != nil {
			t.Fatal(err)
		}
		if hits, _ := store.Lookup(ctx, []string{cacheKeyOf(t, late, "after")}, time.Now()); len(hits) != 1 {
			t.Errorf("a statement begun after the purge kept nothing")
		}
		if n, _ := store.Remaining(ctx, []string{"demo.reviews"}); n != 0 {
			t.Errorf("%d remaining: what a statement begun after the purge keeps is not the erased rows'", n)
		}
	})
}

// cacheKeyOf is the key AI_COMPLETE of model m keeps prompt under.
func cacheKeyOf(t *testing.T, s *Statement, prompt string) string {
	t.Helper()
	b, provider, err := s.binding("ai_complete", "m", "")
	if err != nil {
		t.Fatal(err)
	}
	req := CompletionRequest{Model: provider, MaxTokens: defaultMaxTokens, Messages: []Message{{Role: "user", Content: prompt}}}
	return s.completeCall("ai_complete", completeVersion, b, req, map[string]any{"model_parameters": map[string]any(nil)}, false).key
}

// TestCapsFailClosed: a statement that would go over its cap on calls or
// cost fails, and sends nothing more.
func TestCapsFailClosed(t *testing.T) {
	t.Parallel()
	f := newFake(t, echo)
	rt := testRuntime(t, f)
	s := begin(t, rt, func(o *StatementOptions) { o.MaxCalls = 2 })
	_, err := s.Complete(completeRows("m", "a", "b", "c", "d"), false)
	if !errors.Is(err, ErrCap) || !strings.Contains(err.Error(), "ai_max_calls") {
		t.Fatalf("over the call cap: %v", err)
	}
	if n := f.calls.Load(); n > 2 {
		t.Errorf("%d requests went out with a cap of 2", n)
	}
	if _, err := s.Complete(completeRows("m", "e"), false); !errors.Is(err, ErrCap) {
		t.Errorf("after the cap: %v", err)
	}

	// $1 per million input tokens: a cap of a hundredth of a cent fails a
	// long prompt before it is sent.
	f2 := newFake(t, echo)
	rt2 := testRuntime(t, f2)
	s2 := begin(t, rt2, func(o *StatementOptions) { o.MaxCostUSD = 0.0001 })
	_, err = s2.Complete(completeRows("m", strings.Repeat("long ", 200)), false)
	if !errors.Is(err, ErrCap) || !strings.Contains(err.Error(), "ai_max_cost_usd") {
		t.Fatalf("over the cost cap: %v", err)
	}
	if n := f2.calls.Load(); n != 0 {
		t.Errorf("%d requests went out over the cost cap", n)
	}
}

// TestGrantsAndModels: AI functions are off without an integration; a
// user needs USAGE on one; a model name no integration maps fails with a
// hint.
func TestGrantsAndModels(t *testing.T) {
	t.Parallel()
	off := NewRuntime(Options{})
	if _, err := off.Begin(context.Background(), StatementOptions{User: "ana"}); !errors.Is(err, ErrOff) {
		t.Errorf("without integrations: %v", err)
	}
	f := newFake(t, echo)
	rt := testRuntime(t, f)
	if _, err := rt.Begin(context.Background(), StatementOptions{User: "bob", Roles: []string{"interns"}}); !errors.Is(err, ErrDenied) ||
		!strings.Contains(err.Error(), "GRANT USAGE ON INTEGRATION") {
		t.Errorf("without usage: %v", err)
	}
	if _, err := rt.Begin(context.Background(), StatementOptions{User: "root", Admin: true}); err != nil {
		t.Errorf("an admin: %v", err)
	}
	if err := rt.opts.Store.GrantUsage(context.Background(), Grant{Integration: "main", Grantee: "bob", GranteeType: "user"}); err != nil {
		t.Fatal(err)
	}
	s, err := rt.Begin(context.Background(), StatementOptions{User: "bob"})
	if err != nil {
		t.Fatalf("granted to bob: %v", err)
	}
	_, err = s.Complete(completeRows("claude-4-sonnet", "hi"), false)
	if !errors.Is(err, ErrModel) || !strings.Contains(err.Error(), "MODELS = ('claude-4-sonnet'") {
		t.Errorf("an unmapped model: %v", err)
	}
	if f.calls.Load() != 0 {
		t.Errorf("a request went out for an unmapped model")
	}
	if _, err := rt.Begin(context.Background(), StatementOptions{User: "bob", Integration: "nope"}); !errors.Is(err, ErrNotFound) {
		t.Errorf("an unknown ai_integration: %v", err)
	}
}

// TestRowErrorsAndDetails: a provider refusing a row's input gives that
// row NULL, or its error with return_error_details; other rows answer.
// A provider that cannot be reached fails the statement.
func TestRowErrorsAndDetails(t *testing.T) {
	t.Parallel()
	f := newFake(t, func(r fakeRequest) (int, any) {
		if userText(r) == "bad" {
			return 400, map[string]any{"error": map[string]any{"message": "the input is not allowed", "type": "invalid_request_error"}}
		}
		return echo(r)
	})
	rt := testRuntime(t, f)
	s := begin(t, rt)
	out, err := s.Complete(completeRows("m", "good", "bad"), false)
	if err != nil {
		t.Fatal(err)
	}
	if out[0] != "echo: good" || out[1] != nil {
		t.Errorf("results %v", out)
	}
	out, err = s.Complete([]row{{"m", "good", `{"on_error": "details"}`}, {"m", "bad", `{"on_error": "details"}`}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if out[0] != `{"error":null,"value":"echo: good"}` ||
		!strings.Contains(out[1].(string), `"error":"the provider answered 400: the input is not allowed"`) {
		t.Errorf("details %v", out)
	}
	if sp := s.End(); sp.RowErrors != 2 || !strings.Contains(sp.FirstError, "not allowed") {
		t.Errorf("spend %+v", sp)
	}
	// Failing for good: the statement fails, rather than NULLs.
	out, err = s.Complete([]row{{"m", "bad", `{"on_error": "fail"}`}}, false)
	if err == nil || out != nil {
		t.Errorf("on_error fail: %v %v", out, err)
	}

	dead := newFake(t, func(fakeRequest) (int, any) {
		return 401, map[string]any{"error": map[string]any{"message": "bad key sk-test-not-a-real-key"}}
	})
	s2 := begin(t, testRuntime(t, dead))
	_, err = s2.Complete(completeRows("m", "x"), false)
	if err == nil || IsRowError(err) || strings.Contains(err.Error(), "sk-test-not-a-real-key") || !strings.Contains(err.Error(), "***") {
		t.Errorf("a bad key: %v", err)
	}
}

// TestRowErrorsAreScrubbed: a provider refusing a row's input may echo the
// request's credentials; they never reach the row's error, the statement's
// first error or the failed statement.
func TestRowErrorsAreScrubbed(t *testing.T) {
	t.Parallel()
	f := newFake(t, func(r fakeRequest) (int, any) {
		return 400, map[string]any{"error": map[string]any{"type": "invalid_request_error",
			"message": "rejected " + r.Header.Get("Authorization") + " with gateway key " + r.Header.Get("X-Gateway-Key")}}
	})
	rt := testRuntime(t, f, func(in *Integration) { in.Headers = map[string]string{"X-Gateway-Key": "gw-secret-123456"} })
	leaks := func(s string) bool {
		return strings.Contains(s, "sk-test-not-a-real-key") || strings.Contains(s, "gw-secret-123456")
	}
	s := begin(t, rt)
	out, err := s.Complete([]row{{"m", "x", `{"on_error": "details"}`}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if d, _ := out[0].(string); !strings.Contains(d, "rejected") || !strings.Contains(d, "***") || leaks(d) {
		t.Errorf("details %v", out)
	}
	if sp := s.End(); sp.FirstError == "" || leaks(sp.FirstError) {
		t.Errorf("first error %q", sp.FirstError)
	}
	_, err = begin(t, rt).Complete([]row{{"m", "y", `{"on_error": "fail"}`}}, false)
	if err == nil || leaks(err.Error()) {
		t.Errorf("on_error fail: %v", err)
	}
}

// TestRetries: 429s with retry-after and 5xx are sent again; a provider
// that keeps failing fails the statement after the attempts run out.
func TestRetries(t *testing.T) {
	t.Parallel()
	var n int
	var mu sync.Mutex
	f := newFake(t, func(r fakeRequest) (int, any) {
		mu.Lock()
		defer mu.Unlock()
		n++
		switch n {
		case 1:
			return 429, map[string]any{"error": map[string]any{"message": "slow down"}}
		case 2:
			return 529, map[string]any{"error": map[string]any{"message": "overloaded"}}
		}
		return echo(r)
	})
	rt := testRuntime(t, f)
	var waits []time.Duration
	rt.client.wait = func(_ context.Context, d time.Duration) error {
		mu.Lock()
		waits = append(waits, d)
		mu.Unlock()
		return nil
	}
	s := begin(t, rt)
	out, err := s.Complete(completeRows("m", "x"), false)
	if err != nil || out[0] != "echo: x" {
		t.Fatalf("after retries: %v %v", out, err)
	}
	if len(waits) != 2 {
		t.Errorf("waited %v", waits)
	}

	always := newFake(t, func(fakeRequest) (int, any) { return 503, map[string]any{"error": "down"} })
	rt2 := testRuntime(t, always)
	s2 := begin(t, rt2)
	_, err = s2.Complete(completeRows("m", "x"), false)
	if err == nil || IsRowError(err) || !strings.Contains(err.Error(), "gave up after 5 attempts") {
		t.Errorf("a provider down: %v", err)
	}
	if always.calls.Load() != defaultAttempts {
		t.Errorf("%d attempts", always.calls.Load())
	}
}

// TestRetryAfterIsHonored reads retry-after in seconds and as a date.
func TestRetryAfterIsHonored(t *testing.T) {
	t.Parallel()
	for v, want := range map[string]time.Duration{"2": 2 * time.Second, "0.5": 500 * time.Millisecond, "9999": maxRetryAfter, "": 0} {
		h := make(map[string][]string)
		if v != "" {
			h["Retry-After"] = []string{v}
		}
		if got := retryAfter(h); got != want {
			t.Errorf("retry-after %q: %v, want %v", v, got, want)
		}
	}
}

// TestTimeouts: a request the provider does not answer in time is sent
// again, then fails the statement.
func TestTimeouts(t *testing.T) {
	t.Parallel()
	f := newFake(t, func(r fakeRequest) (int, any) {
		time.Sleep(300 * time.Millisecond)
		return echo(r)
	})
	rt := testRuntime(t, f)
	rt.client.timeout = 50 * time.Millisecond
	rt.client.attempts = 2
	s := begin(t, rt)
	_, err := s.Complete(completeRows("m", "x"), false)
	if err == nil || IsRowError(err) || !strings.Contains(err.Error(), "gave up after 2 attempts") {
		t.Errorf("timeouts: %v", err)
	}
}

// TestMalformedAnswers are row errors.
func TestMalformedAnswers(t *testing.T) {
	t.Parallel()
	f := newFake(t, func(r fakeRequest) (int, any) {
		switch userText(r) {
		case "garbage":
			return 200, "not json at all"
		case "empty":
			return 200, map[string]any{"choices": []any{}}
		}
		return echo(r)
	})
	rt := testRuntime(t, f)
	s := begin(t, rt)
	out, err := s.Complete(completeRows("m", "garbage", "empty", "fine"), false)
	if err != nil {
		t.Fatal(err)
	}
	if out[0] != nil || out[1] != nil || out[2] != "echo: fine" {
		t.Errorf("results %v", out)
	}
	if sp := s.End(); sp.RowErrors != 2 {
		t.Errorf("row errors %d", sp.RowErrors)
	}
}

// TestCompleteShapes: response_format, show_details and the conversation
// form, in Snowflake's shapes; model_parameters reach the provider.
func TestCompleteShapes(t *testing.T) {
	t.Parallel()
	f := newFake(t, func(r fakeRequest) (int, any) {
		if _, ok := r.Body["response_format"]; ok {
			return 200, openAIAnswer(`{"sentiment": "positive"}`)
		}
		return echo(r)
	})
	rt := testRuntime(t, f)
	s := begin(t, rt)
	schema := `{"response_format": {"type": "json", "schema": {"type": "object", "properties": {"sentiment": {"type": "string"}},
		"required": ["sentiment"], "additionalProperties": false}}}`
	out, err := s.Complete([]row{{"m", "great", schema}}, true)
	if err != nil {
		t.Fatal(err)
	}
	if out[0] != `{"sentiment":"positive"}` {
		t.Errorf("structured: %v", out[0])
	}
	rf := f.last().Body["response_format"].(map[string]any)
	if js := rf["json_schema"].(map[string]any); js["strict"] != true {
		t.Errorf("response_format sent %v", rf)
	}

	out, err = s.Complete([]row{{"m", "hello", `{"show_details": true, "model_parameters": {"temperature": 0.3, "max_tokens": 50}}`}}, true)
	if err != nil {
		t.Fatal(err)
	}
	var d map[string]any
	if err := json.Unmarshal([]byte(out[0].(string)), &d); err != nil {
		t.Fatal(err)
	}
	choices := d["choices"].([]any)
	if choices[0].(map[string]any)["messages"] != "echo: hello" || d["model"] != "fake-model-2026-01-01" ||
		d["usage"].(map[string]any)["total_tokens"] != float64(15) {
		t.Errorf("details %v", d)
	}
	if b := f.last().Body; b["temperature"] != 0.3 || b["max_tokens"] != float64(50) {
		t.Errorf("model parameters sent %v", b)
	}

	convo := `[{"role": "system", "content": "be brief"}, {"role": "user", "content": "first"}, {"role": "assistant", "content": "ok"},
		{"role": "user", "content": "second"}]`
	out, err = s.Complete([]row{{"m", convo, `{"history": true}`}}, true)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out[0].(string), `"messages":"echo: second"`) || systemText(f.last()) != "be brief" {
		t.Errorf("conversation %v, system %q", out[0], systemText(f.last()))
	}

	// A PROMPT object.
	prompt := map[string]any{"template": "Review: {0}. Product: {1}.", "args": []any{"loved it", "kettle"}}
	out, err = s.Complete([]row{{"m", prompt, "{}"}}, false)
	if err != nil || out[0] != "echo: Review: loved it. Product: kettle." {
		t.Errorf("prompt: %v %v", out, err)
	}
	bad := map[string]any{"template": "{0} and {1}", "args": []any{"one"}}
	if _, err := s.Complete([]row{{"m", bad, "{}"}}, false); !errors.Is(err, ErrInvalid) {
		t.Errorf("a placeholder without an argument: %v", err)
	}
	// A statement that failed stays failed; a new one fails on guardrails.
	s = begin(t, rt)
	if _, err := s.Complete([]row{{"m", "x", `{"model_parameters": {"guardrails": true}}`}}, false); !errors.Is(err, ErrUnsupported) {
		t.Errorf("guardrails: %v", err)
	}
}

// TestDryRun counts what a statement would send, without sending it.
func TestDryRun(t *testing.T) {
	t.Parallel()
	f := newFake(t, echo)
	rt := testRuntime(t, f)
	s := begin(t, rt, func(o *StatementOptions) { o.DryRun = true })
	out, err := s.Complete(completeRows("m", "a", "a", strings.Repeat("b", 400)), false)
	if err != nil {
		t.Fatal(err)
	}
	if out[0] != nil || f.calls.Load() != 0 {
		t.Errorf("a dry run answered %v and sent %d", out, f.calls.Load())
	}
	sp := s.End()
	if sp.Calls != 2 || sp.InputTokens < 100 || sp.CostUSD <= 0 {
		t.Errorf("dry run counted %+v", sp)
	}
}
