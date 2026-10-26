package ai

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
)

// decisions speaks the decision endpoints that take a state and named
// questions, POST /decisions: Liquid's, Portkey's gateway, servers of open
// decision models, in the shape Snowflake's snowflake-decision and
// Databricks' ai_decide take too.
type decisions struct {
	in Integration
	c  *client
}

func (p *decisions) header() http.Header {
	h := http.Header{}
	for k, v := range p.in.Headers {
		h.Set(k, v)
	}
	if p.in.APIKey != "" {
		h.Set("Authorization", "Bearer "+p.in.APIKey)
	}
	return h
}

func (p *decisions) Complete(context.Context, CompletionRequest) (Completion, error) {
	return Completion{}, unsupported("a decisions integration", "completions")
}

func (p *decisions) Embed(context.Context, EmbeddingRequest) (Embeddings, error) {
	return Embeddings{}, unsupported("a decisions integration", "embeddings")
}

func (p *decisions) CountTokens(context.Context, CompletionRequest) (int64, error) {
	return 0, unsupported("a decisions integration", "token counting")
}

// wireQuestion is a question as the state-and-questions shape writes it.
type wireQuestion struct {
	Type         string `json:"type"`
	Instructions string `json:"instructions"`
	Criteria     any    `json:"criteria,omitempty"`
}

func wireQuestions(qs []Question) map[string]wireQuestion {
	out := map[string]wireQuestion{}
	for _, q := range qs {
		w := wireQuestion{Type: q.Type, Instructions: q.Instructions}
		switch q.Type {
		case QuestionChoice:
			criteria := map[string]string{}
			for _, c := range q.Choices {
				criteria[c.Value] = c.Description
			}
			w.Criteria = criteria
		case QuestionScore:
			w.Criteria = q.Levels
		case QuestionYesNo:
			if q.True != "" || q.False != "" {
				w.Criteria = map[string]string{"true": q.True, "false": q.False}
			}
		}
		out[q.Name] = w
	}
	return out
}

func (p *decisions) Decide(ctx context.Context, r DecisionRequest) (Decision, error) {
	body := map[string]any{"model": r.Model, "state": r.State, "questions": wireQuestions(r.Questions)}
	var out struct {
		Answers  map[string]json.RawMessage `json:"answers"`
		Response *struct {
			Answers map[string]json.RawMessage `json:"answers"`
		} `json:"response"`
		Usage struct {
			InputTokens  int64 `json:"input_tokens"`
			OutputTokens int64 `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := p.c.postJSON(ctx, p.in.Base()+"/decisions", p.header(), body, &out); err != nil {
		return Decision{}, err
	}
	answers := out.Answers
	if answers == nil && out.Response != nil {
		answers = out.Response.Answers
	}
	d := Decision{Answers: map[string]Answer{}, Usage: Usage{InputTokens: out.Usage.InputTokens, OutputTokens: out.Usage.OutputTokens}}
	for name, raw := range answers {
		a, err := readAnswer(raw)
		if err != nil {
			return d, rowErrorf("the provider's answer to %s is not what was expected: %v", name, err)
		}
		d.Answers[name] = a
	}
	return d, nil
}

// readAnswer reads an answer in the state-and-questions shape: a yes-or-no
// question's probability as "noul" (Snowflake's, Portkey's) or
// "probability" (Databricks').
func readAnswer(raw json.RawMessage) (Answer, error) {
	var a struct {
		Type          string             `json:"type"`
		Noul          *float64           `json:"noul"`
		Probability   *float64           `json:"probability"`
		Choice        any                `json:"choice"`
		Score         float64            `json:"score"`
		Confidence    float64            `json:"confidence"`
		Probabilities map[string]float64 `json:"probabilities"`
		Refusal       any                `json:"refusal"`
	}
	if err := json.Unmarshal(raw, &a); err != nil {
		return Answer{}, err
	}
	out := Answer{Type: a.Type, Score: a.Score, Confidence: a.Confidence, Probabilities: a.Probabilities,
		Refused: a.Type == "refusal" || a.Refusal != nil}
	switch {
	case a.Noul != nil:
		out.Probability = *a.Noul
	case a.Probability != nil:
		out.Probability = *a.Probability
	}
	switch c := a.Choice.(type) {
	case string:
		out.Choice = c
	case float64:
		out.Choice = strconv.FormatFloat(c, 'f', -1, 64)
	}
	return out, nil
}
