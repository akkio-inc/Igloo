package ai

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// openAI speaks OpenAI's API: chat completions, embeddings and decisions,
// at OpenAI, Azure OpenAI's v1 API, and the servers that speak it (vLLM,
// llama.cpp, Ollama, OpenRouter).
type openAI struct {
	in Integration
	c  *client
}

func (p *openAI) url(path string) string {
	u := p.in.Base() + path
	if p.in.Provider == ProviderAzure && p.in.APIVersion != "" {
		u += "?api-version=" + url.QueryEscape(p.in.APIVersion)
	}
	return u
}

func (p *openAI) header() http.Header {
	h := http.Header{}
	for k, v := range p.in.Headers {
		h.Set(k, v)
	}
	if p.in.APIKey != "" {
		if p.in.Provider == ProviderAzure {
			h.Set("api-key", p.in.APIKey)
		} else {
			h.Set("Authorization", "Bearer "+p.in.APIKey)
		}
	}
	return h
}

// newTokensParameter reports whether the endpoint takes
// max_completion_tokens, as OpenAI's and Azure's do (their reasoning
// models refuse max_tokens), rather than max_tokens, as the servers that
// speak OpenAI's API mostly do.
func (p *openAI) newTokensParameter() bool {
	if p.in.Provider == ProviderAzure {
		return true
	}
	u, err := url.Parse(p.in.Base())
	return err == nil && strings.EqualFold(u.Hostname(), "api.openai.com")
}

type openAIMessage struct {
	Role    string  `json:"role"`
	Content *string `json:"content"`
	Refusal *string `json:"refusal,omitempty"`
}

func (p *openAI) Complete(ctx context.Context, r CompletionRequest) (Completion, error) {
	var messages []openAIMessage
	if r.System != "" {
		system := r.System
		messages = append(messages, openAIMessage{Role: "system", Content: &system})
	}
	for _, m := range r.Messages {
		content := m.Content
		messages = append(messages, openAIMessage{Role: m.Role, Content: &content})
	}
	body := map[string]any{"model": r.Model, "messages": messages}
	if r.MaxTokens > 0 {
		if p.newTokensParameter() {
			body["max_completion_tokens"] = r.MaxTokens
		} else {
			body["max_tokens"] = r.MaxTokens
		}
	}
	if r.Temperature != nil {
		body["temperature"] = *r.Temperature
	}
	if r.TopP != nil {
		body["top_p"] = *r.TopP
	}
	if len(r.Schema) > 0 {
		body["response_format"] = map[string]any{"type": "json_schema", "json_schema": map[string]any{
			"name": "answer", "schema": r.Schema, "strict": strictSchema(r.Schema),
		}}
	}
	var out struct {
		Model   string `json:"model"`
		Choices []struct {
			Message      openAIMessage `json:"message"`
			FinishReason string        `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			PromptTokens        int64 `json:"prompt_tokens"`
			CompletionTokens    int64 `json:"completion_tokens"`
			PromptTokensDetails struct {
				CachedTokens int64 `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	if err := p.c.postJSON(ctx, p.url("/chat/completions"), p.header(), body, &out); err != nil {
		return Completion{}, err
	}
	c := Completion{Model: out.Model, Usage: Usage{InputTokens: out.Usage.PromptTokens, OutputTokens: out.Usage.CompletionTokens,
		CachedInputTokens: out.Usage.PromptTokensDetails.CachedTokens}}
	if len(out.Choices) == 0 {
		return c, rowErrorf("the provider answered with no choices")
	}
	choice := out.Choices[0]
	if choice.Message.Refusal != nil && *choice.Message.Refusal != "" {
		return c, rowErrorf("the model refused: %s", firstLine(*choice.Message.Refusal, 300))
	}
	if choice.FinishReason == "content_filter" {
		return c, rowErrorf("the provider's content filter stopped the answer")
	}
	if choice.Message.Content != nil {
		c.Text = *choice.Message.Content
	}
	c.Truncated = choice.FinishReason == "length"
	return c, nil
}

// strictSchema reports whether schema can be enforced strictly, as
// OpenAI's strict structured outputs need: every object closed, with every
// property required. Igloo's own schemas are; a user's may not be.
func strictSchema(schema json.RawMessage) bool {
	var v any
	if json.Unmarshal(schema, &v) != nil {
		return false
	}
	var strict func(v any) bool
	strict = func(v any) bool {
		switch v := v.(type) {
		case map[string]any:
			if props, ok := v["properties"].(map[string]any); ok {
				if v["additionalProperties"] != false {
					return false
				}
				req, _ := v["required"].([]any)
				if len(req) != len(props) {
					return false
				}
			}
			for _, x := range v {
				if !strict(x) {
					return false
				}
			}
		case []any:
			for _, x := range v {
				if !strict(x) {
					return false
				}
			}
		}
		return true
	}
	return strict(v)
}

func (p *openAI) Embed(ctx context.Context, r EmbeddingRequest) (Embeddings, error) {
	var out struct {
		Data []struct {
			Index     int       `json:"index"`
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
		Usage struct {
			PromptTokens int64 `json:"prompt_tokens"`
		} `json:"usage"`
	}
	if err := p.c.postJSON(ctx, p.url("/embeddings"), p.header(), map[string]any{"model": r.Model, "input": r.Inputs}, &out); err != nil {
		return Embeddings{}, err
	}
	e := Embeddings{Vectors: make([][]float32, len(r.Inputs)), Usage: Usage{InputTokens: out.Usage.PromptTokens}}
	for _, d := range out.Data {
		if d.Index < 0 || d.Index >= len(e.Vectors) {
			return e, rowErrorf("the provider answered with an embedding for input %d of %d", d.Index, len(r.Inputs))
		}
		e.Vectors[d.Index] = d.Embedding
	}
	for i, v := range e.Vectors {
		if len(v) == 0 {
			return e, rowErrorf("the provider answered with no embedding for input %d", i)
		}
	}
	return e, nil
}

// Decide asks OpenAI's Decisions API, POST /decisions.
func (p *openAI) Decide(ctx context.Context, r DecisionRequest) (Decision, error) {
	type level struct {
		Label       string `json:"label"`
		Description string `json:"description,omitempty"`
	}
	type choice struct {
		Value       string `json:"value"`
		Description string `json:"description,omitempty"`
	}
	type question struct {
		Type         string   `json:"type"`
		Name         string   `json:"name"`
		Instructions string   `json:"instructions"`
		Choices      []choice `json:"choices,omitempty"`
		Levels       []level  `json:"levels,omitempty"`
	}
	var qs []question
	for _, q := range r.Questions {
		oq := question{Type: q.Type, Name: q.Name, Instructions: q.Instructions}
		switch q.Type {
		case QuestionYesNo:
			oq.Type = "predicate"
			if q.True != "" || q.False != "" {
				oq.Instructions += "\nTrue means: " + q.True + "\nFalse means: " + q.False
			}
		case QuestionChoice:
			for _, c := range q.Choices {
				oq.Choices = append(oq.Choices, choice(c))
			}
		case QuestionScore:
			for i, l := range q.Levels {
				oq.Levels = append(oq.Levels, level{Label: strconv.Itoa(i), Description: l})
			}
		}
		qs = append(qs, oq)
	}
	var out struct {
		Answers []struct {
			Type          string          `json:"type"`
			Name          string          `json:"name"`
			Probability   float64         `json:"probability"`
			Choice        string          `json:"choice"`
			Score         float64         `json:"score"`
			Confidence    float64         `json:"confidence"`
			Probabilities json.RawMessage `json:"probabilities"`
		} `json:"answers"`
		Usage struct {
			InputTokens        int64 `json:"input_tokens"`
			InputTokensDetails struct {
				CachedTokens int64 `json:"cached_tokens"`
			} `json:"input_tokens_details"`
		} `json:"usage"`
	}
	body := map[string]any{"model": r.Model, "input": r.State, "questions": qs}
	if err := p.c.postJSON(ctx, p.url("/decisions"), p.header(), body, &out); err != nil {
		return Decision{}, err
	}
	d := Decision{Answers: map[string]Answer{}, Usage: Usage{InputTokens: out.Usage.InputTokens,
		CachedInputTokens: out.Usage.InputTokensDetails.CachedTokens}}
	for _, a := range out.Answers {
		ans := Answer{Type: a.Type, Probability: a.Probability, Choice: a.Choice, Score: a.Score, Confidence: a.Confidence}
		switch a.Type {
		case "predicate":
			ans.Type = QuestionYesNo
		case "refusal":
			ans.Refused = true
		}
		var probs []struct {
			Value       any     `json:"value"`
			Label       string  `json:"label"`
			Probability float64 `json:"probability"`
		}
		if len(a.Probabilities) > 0 && json.Unmarshal(a.Probabilities, &probs) == nil {
			ans.Probabilities = map[string]float64{}
			for _, pr := range probs {
				key := pr.Label
				switch v := pr.Value.(type) {
				case string:
					key = v
				case float64:
					key = strconv.Itoa(int(v))
				}
				ans.Probabilities[key] = pr.Probability
			}
		}
		d.Answers[a.Name] = ans
	}
	return d, nil
}

func (p *openAI) CountTokens(context.Context, CompletionRequest) (int64, error) {
	return 0, unsupported(p.in.Provider, "token counting")
}
