package ai

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
)

// fakeRequest is a request a fake provider got.
type fakeRequest struct {
	Method, Path, Query string
	Header              http.Header
	Body                map[string]any
}

// fake is a provider's API for tests: answer decides each answer, and
// requests are recorded. No test reaches a real provider.
type fake struct {
	srv *httptest.Server
	t   testing.TB

	mu       sync.Mutex
	requests []fakeRequest
	calls    atomic.Int64
	// answer returns the status and the JSON body for a request.
	answer func(r fakeRequest) (int, any)
}

func newFake(t testing.TB, answer func(r fakeRequest) (int, any)) *fake {
	f := &fake{t: t, answer: answer}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		req := fakeRequest{Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, Header: r.Header.Clone()}
		if len(b) > 0 && strings.Contains(r.Header.Get("Content-Type"), "json") {
			_ = json.Unmarshal(b, &req.Body)
		}
		if strings.Contains(r.Header.Get("Content-Type"), "x-www-form-urlencoded") {
			req.Body = map[string]any{"form": string(b)}
		}
		f.mu.Lock()
		f.requests = append(f.requests, req)
		f.mu.Unlock()
		f.calls.Add(1)
		status, body := f.answer(req)
		if h, ok := body.(http.Header); ok {
			for k, v := range h {
				w.Header()[k] = v
			}
			w.WriteHeader(status)
			return
		}
		if raw, ok := body.(string); ok {
			w.WriteHeader(status)
			_, _ = io.WriteString(w, raw)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fake) last() fakeRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) == 0 {
		f.t.Fatal("the fake provider got no request")
	}
	return f.requests[len(f.requests)-1]
}

func (f *fake) all() []fakeRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeRequest(nil), f.requests...)
}

// userText is the last user message of an OpenAI-shaped request.
func userText(r fakeRequest) string {
	msgs, _ := r.Body["messages"].([]any)
	for i := len(msgs) - 1; i >= 0; i-- {
		m, _ := msgs[i].(map[string]any)
		if m["role"] == "user" {
			s, _ := m["content"].(string)
			return s
		}
	}
	return ""
}

// systemText is the system message of an OpenAI-shaped request.
func systemText(r fakeRequest) string {
	msgs, _ := r.Body["messages"].([]any)
	for _, m := range msgs {
		mm, _ := m.(map[string]any)
		if mm["role"] == "system" {
			s, _ := mm["content"].(string)
			return s
		}
	}
	return ""
}

// openAIAnswer is a chat completion answering text.
func openAIAnswer(text string) map[string]any {
	return map[string]any{
		"model":   "fake-model-2026-01-01",
		"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": text}, "finish_reason": "stop"}},
		"usage":   map[string]any{"prompt_tokens": 10, "completion_tokens": 5, "prompt_tokens_details": map[string]any{"cached_tokens": 2}},
	}
}

// echo answers every chat completion with "echo: " and the user's text.
func echo(r fakeRequest) (int, any) {
	return 200, openAIAnswer("echo: " + userText(r))
}

// testRuntime is a runtime with one OpenAI-shaped integration, "main",
// at f, mapping "m" to "fake-model", usable by everyone through the role
// "analysts".
func testRuntime(t testing.TB, f *fake, edit ...func(*Integration)) *Runtime {
	t.Helper()
	store := NewMemoryStore()
	in := Integration{Name: "main", Provider: ProviderOpenAI, BaseURL: f.srv.URL + "/v1", APIKey: "sk-test-not-a-real-key",
		Models: map[string]string{"m": "fake-model"}, DefaultModel: "fake-model", Enabled: true,
		Prices: map[string]Price{"fake-model": {Input: 1, Output: 2}}}
	for _, e := range edit {
		e(&in)
	}
	ctx := context.Background()
	if err := store.CreateIntegration(ctx, in, false); err != nil {
		t.Fatal(err)
	}
	if err := store.GrantUsage(ctx, Grant{Integration: in.Name, Grantee: "analysts", GranteeType: "role"}); err != nil {
		t.Fatal(err)
	}
	rt := NewRuntime(Options{Store: store, Cache: true, RequestTimeout: 5 * time.Second})
	rt.client.wait = func(context.Context, time.Duration) error { return nil }
	return rt
}

// begin starts a statement as ana, of the role analysts.
func begin(t testing.TB, rt *Runtime, edit ...func(*StatementOptions)) *Statement {
	t.Helper()
	o := StatementOptions{User: "ana", Roles: []string{"analysts"}, Tables: []string{"demo.reviews"}}
	for _, e := range edit {
		e(&o)
	}
	s, err := rt.Begin(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
