package ai

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

// anthropicVersion is the API version Igloo asks for unless the
// integration sets another. Version 2026-10-21 refuses temperature, top_p
// and top_k on every model, which Igloo never sends to Anthropic anyway.
const anthropicVersion = "2023-06-01"

// anthropicMaxTokens is the answer's bound where the call sets none: the
// Messages API needs one.
const anthropicMaxTokens = 4096

// anthropic speaks Anthropic's Messages API.
type anthropic struct {
	in Integration
	c  *client
}

func (p *anthropic) header() http.Header {
	h := http.Header{}
	for k, v := range p.in.Headers {
		h.Set(k, v)
	}
	if p.in.APIKey != "" {
		h.Set("x-api-key", p.in.APIKey)
	}
	version := p.in.APIVersion
	if version == "" {
		version = anthropicVersion
	}
	h.Set("anthropic-version", version)
	return h
}

type anthropicBlock struct {
	Type         string            `json:"type"`
	Text         string            `json:"text"`
	CacheControl map[string]string `json:"cache_control,omitempty"`
}

// body is the request for r, without max_tokens. Structured output is
// asked for in the instructions, which every version of the API takes, and
// read from the answer.
func (p *anthropic) body(r CompletionRequest) map[string]any {
	body := map[string]any{"model": r.Model}
	system := r.System
	if len(r.Schema) > 0 {
		system = strings.TrimSpace(system + "\n\n" + schemaInstruction(r.Schema))
	}
	if system != "" {
		// The template, the same for every row, is cached by Anthropic once
		// it is long enough; shorter, the mark costs nothing.
		body["system"] = []anthropicBlock{{Type: "text", Text: system, CacheControl: map[string]string{"type": "ephemeral"}}}
	}
	messages := r.Messages
	if len(messages) == 0 {
		messages = []Message{{Role: "user", Content: "(empty)"}}
	}
	body["messages"] = messages
	return body
}

func (p *anthropic) Complete(ctx context.Context, r CompletionRequest) (Completion, error) {
	body := p.body(r)
	body["max_tokens"] = anthropicMaxTokens
	if r.MaxTokens > 0 {
		body["max_tokens"] = r.MaxTokens
	}
	var out struct {
		Model      string           `json:"model"`
		Content    []anthropicBlock `json:"content"`
		StopReason string           `json:"stop_reason"`
		Usage      struct {
			InputTokens              int64 `json:"input_tokens"`
			OutputTokens             int64 `json:"output_tokens"`
			CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
			CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
		} `json:"usage"`
	}
	if err := p.c.postJSON(ctx, p.in.Base()+"/v1/messages", p.header(), body, &out); err != nil {
		return Completion{}, err
	}
	u := out.Usage
	c := Completion{Model: out.Model, Usage: Usage{InputTokens: u.InputTokens + u.CacheReadInputTokens + u.CacheCreationInputTokens,
		OutputTokens: u.OutputTokens, CachedInputTokens: u.CacheReadInputTokens}}
	if out.StopReason == "refusal" {
		return c, rowErrorf("the model refused")
	}
	var text strings.Builder
	for _, b := range out.Content {
		if b.Type == "text" {
			text.WriteString(b.Text)
		}
	}
	c.Text, c.Truncated = text.String(), out.StopReason == "max_tokens"
	if len(r.Schema) > 0 {
		c.Text = extractJSON(c.Text)
	}
	return c, nil
}

func (p *anthropic) Embed(context.Context, EmbeddingRequest) (Embeddings, error) {
	return Embeddings{}, unsupported("Anthropic", "embedding models")
}

func (p *anthropic) Decide(context.Context, DecisionRequest) (Decision, error) {
	return Decision{}, unsupported("Anthropic", "decision models")
}

// CountTokens asks POST /v1/messages/count_tokens, which costs nothing.
func (p *anthropic) CountTokens(ctx context.Context, r CompletionRequest) (int64, error) {
	var out struct {
		InputTokens int64 `json:"input_tokens"`
	}
	if err := p.c.postJSON(ctx, p.in.Base()+"/v1/messages/count_tokens", p.header(), p.body(r), &out); err != nil {
		return 0, err
	}
	return out.InputTokens, nil
}

// schemaInstruction asks, in words, for an answer that is a JSON object of
// schema, for providers whose structured outputs Igloo does not use.
func schemaInstruction(schema json.RawMessage) string {
	return "Answer with a single JSON object, and nothing else, that matches this JSON Schema:\n" + string(schema)
}

// extractJSON returns the JSON object in a model's answer, without the
// code fence or words a model may put around it.
func extractJSON(text string) string {
	t := strings.TrimSpace(text)
	if strings.HasPrefix(t, "```") {
		t = strings.TrimPrefix(t, "```json")
		t = strings.TrimPrefix(t, "```")
		t = strings.TrimSuffix(strings.TrimSpace(t), "```")
		t = strings.TrimSpace(t)
	}
	if json.Valid([]byte(t)) {
		return t
	}
	if i, j := strings.Index(t, "{"), strings.LastIndex(t, "}"); i >= 0 && j > i && json.Valid([]byte(t[i:j+1])) {
		return t[i : j+1]
	}
	return t
}
