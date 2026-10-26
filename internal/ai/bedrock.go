package ai

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// bedrock speaks Amazon Bedrock's Converse API, for completions, and its
// InvokeModel API, for Titan's and Cohere's embedding models; requests are
// signed with AWS credentials (SigV4, as the service bedrock) or carry a
// Bedrock API key.
type bedrock struct {
	in     Integration
	c      *client
	signer *v4.Signer
}

func newBedrock(in Integration, c *client) (*bedrock, error) {
	return &bedrock{in: in, c: c, signer: v4.NewSigner()}, nil
}

func (p *bedrock) region() string {
	if p.in.Region != "" {
		return p.in.Region
	}
	// From a base URL such as https://bedrock-runtime.us-east-1.amazonaws.com.
	if u, err := url.Parse(p.in.Base()); err == nil {
		parts := strings.Split(u.Hostname(), ".")
		if len(parts) > 2 && parts[0] == "bedrock-runtime" {
			return parts[1]
		}
	}
	return "us-east-1"
}

// post sends body to path, signed, and decodes the answer into out.
func (p *bedrock) post(ctx context.Context, path string, body, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	h := http.Header{"Accept": {"application/json"}}
	for k, v := range p.in.Headers {
		h.Set(k, v)
	}
	r := request{method: http.MethodPost, url: p.in.Base() + path, header: h, body: b}
	if p.in.APIKey != "" && p.in.AWSAccessKeyID == "" {
		h.Set("Authorization", "Bearer "+p.in.APIKey)
	} else {
		creds := aws.Credentials{AccessKeyID: p.in.AWSAccessKeyID, SecretAccessKey: p.in.AWSSecretAccessKey,
			SessionToken: p.in.AWSSessionToken}
		r.sign = func(ctx context.Context, req *http.Request, body []byte) error {
			sum := sha256.Sum256(body)
			return p.signer.SignHTTP(ctx, creds, req, hex.EncodeToString(sum[:]), "bedrock", p.region(), time.Now())
		}
	}
	return p.c.send(ctx, r, out)
}

func modelPath(model, method string) string {
	return "/model/" + url.PathEscape(model) + "/" + method
}

type bedrockText struct {
	Text string `json:"text"`
}

type bedrockMessage struct {
	Role    string        `json:"role"`
	Content []bedrockText `json:"content"`
}

func (p *bedrock) Complete(ctx context.Context, r CompletionRequest) (Completion, error) {
	body := map[string]any{}
	system := r.System
	if len(r.Schema) > 0 {
		system = strings.TrimSpace(system + "\n\n" + schemaInstruction(r.Schema))
	}
	if system != "" {
		body["system"] = []bedrockText{{Text: system}}
	}
	var messages []bedrockMessage
	for _, m := range r.Messages {
		messages = append(messages, bedrockMessage{Role: m.Role, Content: []bedrockText{{Text: m.Content}}})
	}
	if len(messages) == 0 {
		messages = []bedrockMessage{{Role: "user", Content: []bedrockText{{Text: "(empty)"}}}}
	}
	body["messages"] = messages
	inference := map[string]any{}
	if r.MaxTokens > 0 {
		inference["maxTokens"] = r.MaxTokens
	}
	// Anthropic's models on Bedrock refuse sampling settings as Anthropic's
	// own API does.
	if !strings.Contains(r.Model, "anthropic.") {
		if r.Temperature != nil {
			inference["temperature"] = *r.Temperature
		}
		if r.TopP != nil {
			inference["topP"] = *r.TopP
		}
	}
	if len(inference) > 0 {
		body["inferenceConfig"] = inference
	}
	var out struct {
		Output struct {
			Message bedrockMessage `json:"message"`
		} `json:"output"`
		StopReason string `json:"stopReason"`
		Usage      struct {
			InputTokens          int64 `json:"inputTokens"`
			OutputTokens         int64 `json:"outputTokens"`
			CacheReadInputTokens int64 `json:"cacheReadInputTokens"`
		} `json:"usage"`
	}
	if err := p.post(ctx, modelPath(r.Model, "converse"), body, &out); err != nil {
		return Completion{}, err
	}
	c := Completion{Model: r.Model, Usage: Usage{InputTokens: out.Usage.InputTokens, OutputTokens: out.Usage.OutputTokens,
		CachedInputTokens: out.Usage.CacheReadInputTokens}}
	switch out.StopReason {
	case "guardrail_intervened", "content_filtered":
		return c, rowErrorf("the provider stopped the answer: %s", out.StopReason)
	case "malformed_model_output":
		return c, rowErrorf("the model's answer was malformed")
	}
	var text strings.Builder
	for _, t := range out.Output.Message.Content {
		text.WriteString(t.Text)
	}
	c.Text, c.Truncated = text.String(), out.StopReason == "max_tokens"
	if len(r.Schema) > 0 {
		c.Text = extractJSON(c.Text)
	}
	return c, nil
}

// cohereBatch is how many texts Cohere's embedding models take at once.
const cohereBatch = 96

func (p *bedrock) Embed(ctx context.Context, r EmbeddingRequest) (Embeddings, error) {
	e := Embeddings{Vectors: make([][]float32, len(r.Inputs))}
	switch {
	case strings.Contains(r.Model, "cohere.embed"):
		for start := 0; start < len(r.Inputs); start += cohereBatch {
			end := min(start+cohereBatch, len(r.Inputs))
			var out struct {
				Embeddings [][]float32 `json:"embeddings"`
			}
			body := map[string]any{"texts": r.Inputs[start:end], "input_type": "search_document"}
			if err := p.post(ctx, modelPath(r.Model, "invoke"), body, &out); err != nil {
				return e, err
			}
			if len(out.Embeddings) != end-start {
				return e, rowErrorf("the provider answered with %d embeddings for %d inputs", len(out.Embeddings), end-start)
			}
			copy(e.Vectors[start:end], out.Embeddings)
			for _, in := range r.Inputs[start:end] {
				e.Usage.InputTokens += estimateTokens(in)
			}
		}
		return e, nil
	case strings.Contains(r.Model, "titan-embed"):
		// One text a request.
		for i, in := range r.Inputs {
			var out struct {
				Embedding           []float32 `json:"embedding"`
				InputTextTokenCount int64     `json:"inputTextTokenCount"`
			}
			if err := p.post(ctx, modelPath(r.Model, "invoke"), map[string]any{"inputText": in}, &out); err != nil {
				return e, err
			}
			e.Vectors[i] = out.Embedding
			e.Usage.InputTokens += out.InputTextTokenCount
		}
		return e, nil
	}
	return e, unsupported("Bedrock", "embeddings from "+r.Model+" through Igloo; use an Amazon Titan or Cohere embedding model")
}

func (p *bedrock) Decide(context.Context, DecisionRequest) (Decision, error) {
	return Decision{}, unsupported("Bedrock", "decision models")
}

func (p *bedrock) CountTokens(context.Context, CompletionRequest) (int64, error) {
	return 0, unsupported("Bedrock", "token counting")
}
