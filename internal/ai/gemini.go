package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/jwt"
)

// gemini speaks Google's Gemini API, and Gemini on Vertex AI.
type gemini struct {
	in Integration
	c  *client
	// tokens mint Vertex AI's access tokens from a service account's key.
	tokens oauth2.TokenSource
	mu     sync.Mutex
}

// googleTokenURL is where service accounts' keys get access tokens,
// unless the key names another.
const googleTokenURL = "https://oauth2.googleapis.com/token"

func newGemini(in Integration, c *client) (*gemini, error) {
	g := &gemini{in: in, c: c}
	if in.Provider == ProviderVertex && in.ServiceAccount != "" {
		var sa struct {
			ClientEmail  string `json:"client_email"`
			PrivateKey   string `json:"private_key"`
			PrivateKeyID string `json:"private_key_id"`
			TokenURI     string `json:"token_uri"`
		}
		if err := json.Unmarshal([]byte(in.ServiceAccount), &sa); err != nil {
			return nil, fmt.Errorf("%w: SERVICE_ACCOUNT is not a service account's JSON key", ErrInvalid)
		}
		conf := &jwt.Config{Email: sa.ClientEmail, PrivateKey: []byte(sa.PrivateKey), PrivateKeyID: sa.PrivateKeyID,
			Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}, TokenURL: sa.TokenURI}
		if conf.TokenURL == "" {
			conf.TokenURL = googleTokenURL
		}
		// Tokens are minted through the same guarded connections.
		ctx := context.WithValue(context.Background(), oauth2.HTTPClient, c.http)
		g.tokens = oauth2.ReuseTokenSource(nil, conf.TokenSource(ctx))
	}
	return g, nil
}

// url is the address of model's method, such as generateContent.
func (p *gemini) url(model, method string) string {
	if p.in.Provider == ProviderVertex {
		return p.in.Base() + "/" + model + ":" + method
	}
	return p.in.Base() + "/models/" + model + ":" + method
}

func (p *gemini) header() (http.Header, error) {
	h := http.Header{}
	for k, v := range p.in.Headers {
		h.Set(k, v)
	}
	if p.tokens != nil {
		p.mu.Lock()
		tok, err := p.tokens.Token()
		p.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("could not get an access token for the service account: %s", p.in.Scrub(err.Error()))
		}
		h.Set("Authorization", "Bearer "+tok.AccessToken)
	} else if p.in.APIKey != "" {
		h.Set("x-goog-api-key", p.in.APIKey)
	}
	return h, nil
}

type geminiPart struct {
	Text string `json:"text"`
}

type geminiContent struct {
	Role  string       `json:"role,omitempty"`
	Parts []geminiPart `json:"parts"`
}

func geminiContents(r CompletionRequest) []geminiContent {
	var out []geminiContent
	for _, m := range r.Messages {
		role := m.Role
		if role == "assistant" {
			role = "model"
		}
		out = append(out, geminiContent{Role: role, Parts: []geminiPart{{Text: m.Content}}})
	}
	if len(out) == 0 {
		out = []geminiContent{{Role: "user", Parts: []geminiPart{{Text: "(empty)"}}}}
	}
	return out
}

func (p *gemini) Complete(ctx context.Context, r CompletionRequest) (Completion, error) {
	h, err := p.header()
	if err != nil {
		return Completion{}, err
	}
	body := map[string]any{"contents": geminiContents(r)}
	system := r.System
	if len(r.Schema) > 0 {
		system = strings.TrimSpace(system + "\n\n" + schemaInstruction(r.Schema))
	}
	if system != "" {
		body["systemInstruction"] = geminiContent{Parts: []geminiPart{{Text: system}}}
	}
	gen := map[string]any{}
	if r.MaxTokens > 0 {
		gen["maxOutputTokens"] = r.MaxTokens
	}
	if r.Temperature != nil {
		gen["temperature"] = *r.Temperature
	}
	if r.TopP != nil {
		gen["topP"] = *r.TopP
	}
	if len(r.Schema) > 0 {
		gen["responseMimeType"] = "application/json"
	}
	if len(gen) > 0 {
		body["generationConfig"] = gen
	}
	var out struct {
		Candidates []struct {
			Content      geminiContent `json:"content"`
			FinishReason string        `json:"finishReason"`
		} `json:"candidates"`
		PromptFeedback struct {
			BlockReason string `json:"blockReason"`
		} `json:"promptFeedback"`
		UsageMetadata struct {
			PromptTokenCount        int64 `json:"promptTokenCount"`
			CandidatesTokenCount    int64 `json:"candidatesTokenCount"`
			CachedContentTokenCount int64 `json:"cachedContentTokenCount"`
			ThoughtsTokenCount      int64 `json:"thoughtsTokenCount"`
		} `json:"usageMetadata"`
		ModelVersion string `json:"modelVersion"`
	}
	if err := p.c.postJSON(ctx, p.url(r.Model, "generateContent"), h, body, &out); err != nil {
		return Completion{}, err
	}
	u := out.UsageMetadata
	c := Completion{Model: out.ModelVersion, Usage: Usage{InputTokens: u.PromptTokenCount,
		OutputTokens: u.CandidatesTokenCount + u.ThoughtsTokenCount, CachedInputTokens: u.CachedContentTokenCount}}
	if out.PromptFeedback.BlockReason != "" {
		return c, rowErrorf("the provider blocked the prompt: %s", out.PromptFeedback.BlockReason)
	}
	if len(out.Candidates) == 0 {
		return c, rowErrorf("the provider answered with no candidates")
	}
	cand := out.Candidates[0]
	switch cand.FinishReason {
	case "SAFETY", "RECITATION", "BLOCKLIST", "PROHIBITED_CONTENT", "SPII":
		return c, rowErrorf("the provider stopped the answer: %s", cand.FinishReason)
	}
	var text strings.Builder
	for _, part := range cand.Content.Parts {
		text.WriteString(part.Text)
	}
	c.Text, c.Truncated = text.String(), cand.FinishReason == "MAX_TOKENS"
	if len(r.Schema) > 0 {
		c.Text = extractJSON(c.Text)
	}
	return c, nil
}

func (p *gemini) Embed(ctx context.Context, r EmbeddingRequest) (Embeddings, error) {
	h, err := p.header()
	if err != nil {
		return Embeddings{}, err
	}
	e := Embeddings{Vectors: make([][]float32, len(r.Inputs))}
	if p.in.Provider == ProviderVertex {
		// Vertex AI's text embedding models answer predict.
		type instance struct {
			Content string `json:"content"`
		}
		var instances []instance
		for _, in := range r.Inputs {
			instances = append(instances, instance{in})
		}
		var out struct {
			Predictions []struct {
				Embeddings struct {
					Values     []float32 `json:"values"`
					Statistics struct {
						TokenCount float64 `json:"token_count"`
					} `json:"statistics"`
				} `json:"embeddings"`
			} `json:"predictions"`
		}
		if err := p.c.postJSON(ctx, p.url(r.Model, "predict"), h, map[string]any{"instances": instances}, &out); err != nil {
			return e, err
		}
		if len(out.Predictions) != len(r.Inputs) {
			return e, rowErrorf("the provider answered with %d embeddings for %d inputs", len(out.Predictions), len(r.Inputs))
		}
		for i, pr := range out.Predictions {
			e.Vectors[i] = pr.Embeddings.Values
			e.Usage.InputTokens += int64(pr.Embeddings.Statistics.TokenCount)
		}
		return e, nil
	}
	type request struct {
		Model   string        `json:"model"`
		Content geminiContent `json:"content"`
	}
	var reqs []request
	for _, in := range r.Inputs {
		reqs = append(reqs, request{Model: "models/" + r.Model, Content: geminiContent{Parts: []geminiPart{{Text: in}}}})
	}
	var out struct {
		Embeddings []struct {
			Values []float32 `json:"values"`
		} `json:"embeddings"`
	}
	if err := p.c.postJSON(ctx, p.url(r.Model, "batchEmbedContents"), h, map[string]any{"requests": reqs}, &out); err != nil {
		return e, err
	}
	if len(out.Embeddings) != len(r.Inputs) {
		return e, rowErrorf("the provider answered with %d embeddings for %d inputs", len(out.Embeddings), len(r.Inputs))
	}
	for i, v := range out.Embeddings {
		e.Vectors[i] = v.Values
	}
	// The embedding API reports no usage; inputs are counted as estimated.
	for _, in := range r.Inputs {
		e.Usage.InputTokens += estimateTokens(in)
	}
	return e, nil
}

func (p *gemini) Decide(context.Context, DecisionRequest) (Decision, error) {
	return Decision{}, unsupported("Gemini", "decision models")
}

func (p *gemini) CountTokens(ctx context.Context, r CompletionRequest) (int64, error) {
	h, err := p.header()
	if err != nil {
		return 0, err
	}
	body := map[string]any{"contents": geminiContents(r)}
	var out struct {
		TotalTokens int64 `json:"totalTokens"`
	}
	if err := p.c.postJSON(ctx, p.url(r.Model, "countTokens"), h, body, &out); err != nil {
		return 0, err
	}
	return out.TotalTokens, nil
}
