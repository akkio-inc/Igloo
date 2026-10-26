package ai

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

func newTestClient() *client {
	c := newClient((&net.Dialer{}).DialContext, 5*time.Second)
	c.wait = func(context.Context, time.Duration) error { return nil }
	return c
}

func mustProvider(t *testing.T, in Integration) Provider {
	t.Helper()
	if err := in.Check(nil); err != nil {
		t.Fatalf("check %s: %v", in.Provider, err)
	}
	p, err := newProvider(in, newTestClient())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

var ctx = context.Background()

func TestOpenAIShapes(t *testing.T) {
	t.Parallel()
	f := newFake(t, func(r fakeRequest) (int, any) {
		switch r.Path {
		case "/v1/embeddings":
			return 200, map[string]any{"data": []any{
				map[string]any{"index": 1, "embedding": []float32{0, 1}},
				map[string]any{"index": 0, "embedding": []float32{1, 0}},
			}, "usage": map[string]any{"prompt_tokens": 4}}
		case "/v1/decisions":
			return 200, map[string]any{"answers": []any{
				map[string]any{"type": "predicate", "name": "urgent", "probability": 0.9},
				map[string]any{"type": "choice", "name": "team", "choice": "billing",
					"probabilities": []any{map[string]any{"value": "billing", "probability": 0.8}, map[string]any{"value": "tech", "probability": 0.2}}},
				map[string]any{"type": "score", "name": "anger", "score": 1.5,
					"probabilities": []any{map[string]any{"value": 0, "label": "0", "probability": 0.1}, map[string]any{"value": 1, "probability": 0.3},
						map[string]any{"value": 2, "probability": 0.6}}},
			}, "usage": map[string]any{"input_tokens": 50, "input_tokens_details": map[string]any{"cached_tokens": 10}}}
		}
		return echo(r)
	})
	p := mustProvider(t, Integration{Name: "o", Provider: ProviderOpenAI, BaseURL: f.srv.URL + "/v1", APIKey: "sk-test"})
	c, err := p.Complete(ctx, CompletionRequest{Model: "gpt", System: "be brief", Messages: []Message{{Role: "user", Content: "hi"}}, MaxTokens: 9})
	if err != nil || c.Text != "echo: hi" || c.Usage.InputTokens != 10 {
		t.Fatalf("complete: %+v %v", c, err)
	}
	r := f.last()
	if systemText(r) != "be brief" || r.Body["max_tokens"] != float64(9) {
		t.Errorf("request %+v", r.Body)
	}
	e, err := p.Embed(ctx, EmbeddingRequest{Model: "emb", Inputs: []string{"a", "b"}})
	if err != nil || e.Vectors[0][0] != 1 || e.Vectors[1][1] != 1 || e.Usage.InputTokens != 4 {
		t.Errorf("embed: %+v %v", e, err)
	}
	d, err := p.Decide(ctx, DecisionRequest{Model: "luna", State: "the customer is furious about a double charge", Questions: []Question{
		{Name: "urgent", Type: QuestionYesNo, Instructions: "Is it urgent?"},
		{Name: "team", Type: QuestionChoice, Instructions: "Which team?", Choices: []Choice{{"billing", "money"}, {"tech", ""}}},
		{Name: "anger", Type: QuestionScore, Instructions: "How angry?", Levels: []string{"calm", "annoyed", "furious"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if d.Answers["urgent"].Probability != 0.9 || d.Answers["urgent"].Type != QuestionYesNo || d.Answers["team"].Probabilities["tech"] != 0.2 ||
		d.Answers["anger"].Probabilities["2"] != 0.6 || d.Usage.CachedInputTokens != 10 {
		t.Errorf("decide: %+v", d)
	}
	qs := f.last().Body["questions"].([]any)
	if q := qs[0].(map[string]any); q["type"] != "predicate" || f.last().Body["input"] == nil {
		t.Errorf("decision request %v", f.last().Body)
	}
	if q := qs[2].(map[string]any); len(q["levels"].([]any)) != 3 {
		t.Errorf("score levels %v", q)
	}
	if _, err := p.CountTokens(ctx, CompletionRequest{}); !errors.Is(err, ErrUnsupported) {
		t.Errorf("count: %v", err)
	}

	// Refusals and content filters are the row's.
	refuse := newFake(t, func(fakeRequest) (int, any) {
		return 200, map[string]any{"choices": []any{map[string]any{"message": map[string]any{"refusal": "no"}, "finish_reason": "stop"}}}
	})
	p2 := mustProvider(t, Integration{Name: "o", Provider: ProviderOpenAI, BaseURL: refuse.srv.URL, APIKey: "k"})
	if _, err := p2.Complete(ctx, CompletionRequest{Model: "m"}); !IsRowError(err) {
		t.Errorf("refusal: %v", err)
	}
}

func TestAzureShape(t *testing.T) {
	t.Parallel()
	f := newFake(t, echo)
	p := mustProvider(t, Integration{Name: "az", Provider: ProviderAzure, BaseURL: f.srv.URL + "/openai/v1", APIKey: "azure-key",
		APIVersion: "2026-01-01"})
	if _, err := p.Complete(ctx, CompletionRequest{Model: "deployment", Messages: []Message{{"user", "x"}}, MaxTokens: 5}); err != nil {
		t.Fatal(err)
	}
	r := f.last()
	if r.Path != "/openai/v1/chat/completions" || r.Query != "api-version=2026-01-01" || r.Header.Get("api-key") != "azure-key" ||
		r.Header.Get("Authorization") != "" || r.Body["max_completion_tokens"] != float64(5) {
		t.Errorf("azure request %+v", r)
	}
}

func TestAnthropicShape(t *testing.T) {
	t.Parallel()
	f := newFake(t, func(r fakeRequest) (int, any) {
		if r.Path == "/v1/messages/count_tokens" {
			return 200, map[string]any{"input_tokens": 14}
		}
		return 200, map[string]any{"model": "claude-x", "stop_reason": "end_turn",
			"content": []any{map[string]any{"type": "text", "text": "```json\n{\"labels\": [\"a\"]}\n```"}},
			"usage":   map[string]any{"input_tokens": 3, "output_tokens": 4, "cache_read_input_tokens": 100, "cache_creation_input_tokens": 0}}
	})
	p := mustProvider(t, Integration{Name: "a", Provider: ProviderAnthropic, BaseURL: f.srv.URL, APIKey: "ant-key"})
	temp := 0.5
	c, err := p.Complete(ctx, CompletionRequest{Model: "claude", System: "classify", Messages: []Message{{"user", "x"}},
		Temperature: &temp, TopP: &temp, Schema: json.RawMessage(`{"type":"object"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if c.Text != `{"labels": ["a"]}` || c.Usage.InputTokens != 103 || c.Usage.CachedInputTokens != 100 {
		t.Errorf("completion %+v", c)
	}
	r := f.last()
	if r.Path != "/v1/messages" || r.Header.Get("x-api-key") != "ant-key" || r.Header.Get("anthropic-version") != anthropicVersion {
		t.Errorf("headers %v", r.Header)
	}
	for _, k := range []string{"temperature", "top_p", "top_k"} {
		if _, ok := r.Body[k]; ok {
			t.Errorf("%s sent to Anthropic", k)
		}
	}
	system := r.Body["system"].([]any)[0].(map[string]any)
	if !strings.HasPrefix(system["text"].(string), "classify") || system["cache_control"] == nil || r.Body["max_tokens"] != float64(anthropicMaxTokens) {
		t.Errorf("body %v", r.Body)
	}
	n, err := p.CountTokens(ctx, CompletionRequest{Model: "claude", Messages: []Message{{"user", "count me"}}})
	if err != nil || n != 14 {
		t.Errorf("count: %d %v", n, err)
	}
	if _, err := p.Embed(ctx, EmbeddingRequest{}); !errors.Is(err, ErrUnsupported) {
		t.Errorf("embed: %v", err)
	}

	spent := newFake(t, func(fakeRequest) (int, any) {
		return 429, map[string]any{"type": "error", "error": map[string]any{"type": "rate_limit_error", "message": "spend limit",
			"details": map[string]any{"error_code": "enforced_spend_limit_reached"}}}
	})
	p2 := mustProvider(t, Integration{Name: "a", Provider: ProviderAnthropic, BaseURL: spent.srv.URL, APIKey: "k"})
	if _, err := p2.Complete(ctx, CompletionRequest{Model: "m"}); err == nil || IsRowError(err) || spent.calls.Load() != 1 {
		t.Errorf("a spend limit is not retried: %v after %d", err, spent.calls.Load())
	}
}

func TestGeminiShape(t *testing.T) {
	t.Parallel()
	f := newFake(t, func(r fakeRequest) (int, any) {
		switch {
		case strings.HasSuffix(r.Path, ":batchEmbedContents"):
			return 200, map[string]any{"embeddings": []any{map[string]any{"values": []float32{1, 2}}}}
		case strings.HasSuffix(r.Path, ":countTokens"):
			return 200, map[string]any{"totalTokens": 7}
		}
		return 200, map[string]any{"candidates": []any{map[string]any{"content": map[string]any{"parts": []any{map[string]any{"text": "hello"}}},
			"finishReason": "STOP"}}, "usageMetadata": map[string]any{"promptTokenCount": 5, "candidatesTokenCount": 2, "thoughtsTokenCount": 3}}
	})
	p := mustProvider(t, Integration{Name: "g", Provider: ProviderGemini, BaseURL: f.srv.URL + "/v1beta", APIKey: "g-key"})
	c, err := p.Complete(ctx, CompletionRequest{Model: "gemini-x", System: "sys", Messages: []Message{{"user", "hi"}}, MaxTokens: 8})
	if err != nil || c.Text != "hello" || c.Usage.OutputTokens != 5 {
		t.Fatalf("complete %+v %v", c, err)
	}
	r := f.last()
	if r.Path != "/v1beta/models/gemini-x:generateContent" || r.Header.Get("x-goog-api-key") != "g-key" ||
		r.Body["systemInstruction"] == nil || r.Body["generationConfig"].(map[string]any)["maxOutputTokens"] != float64(8) {
		t.Errorf("request %+v", r)
	}
	e, err := p.Embed(ctx, EmbeddingRequest{Model: "gemini-embedding-001", Inputs: []string{"a"}})
	if err != nil || len(e.Vectors[0]) != 2 {
		t.Errorf("embed %+v %v", e, err)
	}
	if n, err := p.CountTokens(ctx, CompletionRequest{Model: "gemini-x"}); err != nil || n != 7 {
		t.Errorf("count %d %v", n, err)
	}
	blocked := newFake(t, func(fakeRequest) (int, any) {
		return 200, map[string]any{"promptFeedback": map[string]any{"blockReason": "SAFETY"}}
	})
	p2 := mustProvider(t, Integration{Name: "g", Provider: ProviderGemini, BaseURL: blocked.srv.URL, APIKey: "k"})
	if _, err := p2.Complete(ctx, CompletionRequest{Model: "m"}); !IsRowError(err) {
		t.Errorf("blocked: %v", err)
	}
}

// TestVertexServiceAccount mints a token from a service account's key at
// the key's token URI, and calls Vertex AI with it.
func TestVertexServiceAccount(t *testing.T) {
	t.Parallel()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	f := newFake(t, func(r fakeRequest) (int, any) {
		if r.Path == "/token" {
			return 200, map[string]any{"access_token": "vertex-token", "token_type": "Bearer", "expires_in": 3600}
		}
		return 200, map[string]any{"candidates": []any{map[string]any{"content": map[string]any{"parts": []any{map[string]any{"text": "ok"}}}}}}
	})
	sa, _ := json.Marshal(map[string]string{"type": "service_account", "client_email": "igloo@demo-project.iam.gserviceaccount.com",
		"private_key": string(pemKey), "private_key_id": "kid", "token_uri": f.srv.URL + "/token"})
	p := mustProvider(t, Integration{Name: "v", Provider: ProviderVertex, ServiceAccount: string(sa), Project: "demo-project",
		Region: "us-central1", BaseURL: f.srv.URL + "/v1/projects/demo-project/locations/us-central1/publishers/google/models"})
	c, err := p.Complete(ctx, CompletionRequest{Model: "gemini-y", Messages: []Message{{"user", "hi"}}})
	if err != nil || c.Text != "ok" {
		t.Fatalf("complete %+v %v", c, err)
	}
	r := f.last()
	if r.Path != "/v1/projects/demo-project/locations/us-central1/publishers/google/models/gemini-y:generateContent" ||
		r.Header.Get("Authorization") != "Bearer vertex-token" {
		t.Errorf("request %+v", r)
	}
	if got := (&Integration{Provider: ProviderVertex, Project: "p1", Region: "europe-west4"}).Base(); got !=
		"https://europe-west4-aiplatform.googleapis.com/v1/projects/p1/locations/europe-west4/publishers/google/models" {
		t.Errorf("vertex base %s", got)
	}
}

func TestBedrockShape(t *testing.T) {
	t.Parallel()
	f := newFake(t, func(r fakeRequest) (int, any) {
		switch {
		case strings.Contains(r.Path, "titan-embed"):
			return 200, map[string]any{"embedding": []float32{0.5}, "inputTextTokenCount": 2}
		case strings.Contains(r.Path, "cohere.embed"):
			texts := r.Body["texts"].([]any)
			var out [][]float32
			for range texts {
				out = append(out, []float32{1})
			}
			return 200, map[string]any{"embeddings": out}
		}
		return 200, map[string]any{"output": map[string]any{"message": map[string]any{"role": "assistant",
			"content": []any{map[string]any{"text": "fine"}}}}, "stopReason": "end_turn", "usage": map[string]any{"inputTokens": 4, "outputTokens": 1}}
	})
	p := mustProvider(t, Integration{Name: "b", Provider: ProviderBedrock, Region: "us-east-1", BaseURL: f.srv.URL,
		AWSAccessKeyID: "AKIDEXAMPLE", AWSSecretAccessKey: "secret-example-key"})
	c, err := p.Complete(ctx, CompletionRequest{Model: "anthropic.claude-x", System: "s", Messages: []Message{{"user", "hi"}}, MaxTokens: 3})
	if err != nil || c.Text != "fine" || c.Usage.InputTokens != 4 {
		t.Fatalf("complete %+v %v", c, err)
	}
	r := f.last()
	auth := r.Header.Get("Authorization")
	if r.Path != "/model/anthropic.claude-x/converse" || !strings.Contains(auth, "Credential=AKIDEXAMPLE/") ||
		!strings.Contains(auth, "/us-east-1/bedrock/aws4_request") || r.Header.Get("X-Amz-Date") == "" {
		t.Errorf("request %s, %s", r.Path, auth)
	}
	if cfg := r.Body["inferenceConfig"].(map[string]any); cfg["maxTokens"] != float64(3) {
		t.Errorf("inference config %v", cfg)
	}
	e, err := p.Embed(ctx, EmbeddingRequest{Model: "amazon.titan-embed-text-v2:0", Inputs: []string{"a", "b"}})
	if err != nil || len(e.Vectors) != 2 || e.Usage.InputTokens != 4 {
		t.Errorf("titan %+v %v", e, err)
	}
	e, err = p.Embed(ctx, EmbeddingRequest{Model: "cohere.embed-english-v3", Inputs: make([]string, 100)})
	if err != nil || len(e.Vectors) != 100 {
		t.Errorf("cohere %d %v", len(e.Vectors), err)
	}

	// A Bedrock API key instead.
	p2 := mustProvider(t, Integration{Name: "b", Provider: ProviderBedrock, Region: "us-east-1", BaseURL: f.srv.URL, APIKey: "bedrock-api-key"})
	if _, err := p2.Complete(ctx, CompletionRequest{Model: "m"}); err != nil {
		t.Fatal(err)
	}
	if got := f.last().Header.Get("Authorization"); got != "Bearer bedrock-api-key" {
		t.Errorf("api key %q", got)
	}
}

func TestDecisionsShape(t *testing.T) {
	t.Parallel()
	f := newFake(t, func(r fakeRequest) (int, any) {
		if r.Body["state"] == "wrapped" {
			return 200, map[string]any{"response": map[string]any{"answers": map[string]any{"q": map[string]any{"type": "noul", "probability": 0.25}}}}
		}
		return 200, map[string]any{"model": "jev-1", "answers": map[string]any{
			"q":    map[string]any{"type": "noul", "noul": 0.75},
			"pick": map[string]any{"type": "choice", "choice": "b", "probabilities": map[string]any{"a": 0.1, "b": 0.9}, "confidence": 0.8},
		}, "usage": map[string]any{"input_tokens": 30, "output_tokens": 2}}
	})
	p := mustProvider(t, Integration{Name: "d", Provider: ProviderDecisions, BaseURL: f.srv.URL + "/v1", APIKey: "dk",
		DecisionModel: "jev-latest", Headers: map[string]string{"x-portkey-provider": "typesafe"}})
	d, err := p.Decide(ctx, DecisionRequest{Model: "jev-latest", State: "row", Questions: []Question{
		{Name: "q", Type: QuestionYesNo, Instructions: "?", True: "yes", False: "no"},
		{Name: "pick", Type: QuestionChoice, Instructions: "which", Choices: []Choice{{"a", "first"}, {"b", ""}}},
	}})
	if err != nil || d.Answers["q"].Probability != 0.75 || d.Answers["pick"].Choice != "b" || d.Usage.InputTokens != 30 {
		t.Fatalf("decide %+v %v", d, err)
	}
	r := f.last()
	qs := r.Body["questions"].(map[string]any)
	if r.Path != "/v1/decisions" || r.Header.Get("x-portkey-provider") != "typesafe" || r.Header.Get("Authorization") != "Bearer dk" ||
		qs["q"].(map[string]any)["criteria"].(map[string]any)["true"] != "yes" || qs["pick"].(map[string]any)["criteria"].(map[string]any)["a"] != "first" {
		t.Errorf("request %+v", r)
	}
	d, err = p.Decide(ctx, DecisionRequest{Model: "m", State: "wrapped", Questions: []Question{{Name: "q", Type: QuestionYesNo}}})
	if err != nil || d.Answers["q"].Probability != 0.25 {
		t.Errorf("Databricks' shape %+v %v", d, err)
	}
	if _, err := p.Complete(ctx, CompletionRequest{}); !errors.Is(err, ErrUnsupported) {
		t.Errorf("complete: %v", err)
	}
}

func TestIntegrationCheck(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		in   Integration
		want string
	}{
		{Integration{Provider: "nope"}, "unknown PROVIDER"},
		{Integration{Provider: ProviderOpenAI}, "needs an API_KEY"},
		{Integration{Provider: ProviderAzure, APIKey: "k"}, "needs BASE_URL"},
		{Integration{Provider: ProviderBedrock, APIKey: "k"}, "needs REGION"},
		{Integration{Provider: ProviderBedrock, Region: "us-east-1"}, "AWS_ACCESS_KEY_ID"},
		{Integration{Provider: ProviderVertex}, "SERVICE_ACCOUNT"},
		{Integration{Provider: ProviderVertex, ServiceAccount: "{}", Project: "p", Region: "r"}, "client_email"},
		{Integration{Provider: ProviderDecisions, BaseURL: "https://decide.example.com"}, "DECISION_MODEL"},
		{Integration{Provider: ProviderAnthropic, APIKey: "k", DecisionModel: "x"}, "no decision models"},
		{Integration{Provider: ProviderAnthropic, APIKey: "k", EmbeddingModel: "x"}, "no embedding models"},
		{Integration{Provider: ProviderOpenAI, APIKey: "k", Headers: map[string]string{"Host": "x"}}, "cannot set Host"},
		{Integration{Provider: ProviderOpenAI, APIKey: "k", MaxConcurrency: 1000}, "MAX_CONCURRENCY"},
		{Integration{Provider: ProviderOpenAI, APIKey: "k", Prices: map[string]Price{"m": {Input: -1}}}, "negative"},
		{Integration{Provider: ProviderOpenAI, BaseURL: "http://10.0.0.1:8000/v1"}, ""},
		{Integration{Provider: ProviderOpenAI, APIKey: "k"}, ""},
	} {
		err := c.in.Check(nil)
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%+v: %v", c.in, err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
			t.Errorf("%+v: %v, want %q", c.in, err, c.want)
		}
	}
	in := Integration{APIKey: "sk-live-abcdefghijkl", Headers: map[string]string{"x-gateway": "Bearer gw-secret-value"}}
	if got := in.Scrub("key sk-live-abcdefghijkl and gw-secret-value"); strings.Contains(got, "abcdefghijkl") || strings.Contains(got, "gw-secret") {
		t.Errorf("scrubbed %q", got)
	}
}
