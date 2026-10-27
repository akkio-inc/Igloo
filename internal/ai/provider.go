package ai

import (
	"context"
	"encoding/json"
	"fmt"
)

// Message is a turn of a conversation.
type Message struct {
	// Role is user or assistant; a system message goes in
	// CompletionRequest.System.
	Role    string `json:"role"`
	Content string `json:"content"`
}

// CompletionRequest asks a model for text.
type CompletionRequest struct {
	// Model is the provider's model.
	Model string
	// System is the instructions, a function's template: it comes first,
	// and is the same for every row, so that providers' prompt caches
	// serve it (Anthropic's is asked for; OpenAI's and Gemini's apply by
	// themselves to a prefix long enough).
	System   string
	Messages []Message
	// MaxTokens bounds the answer; 0 is the provider's default.
	MaxTokens int
	// Temperature and TopP, when set, are sent to providers that take
	// them. Anthropic's newer models refuse them, and none promise the
	// same answer twice: repeatability comes from the cache.
	Temperature, TopP *float64
	// Schema, when set, is the JSON Schema of an object the answer must
	// be, as structured outputs give it.
	Schema json.RawMessage
}

// Completion is a model's answer.
type Completion struct {
	Text string
	// Model is the model that answered, as the provider named it.
	Model string
	Usage Usage
	// Truncated says the answer ran out of tokens.
	Truncated bool
}

// EmbeddingRequest asks for the embeddings of several texts at once.
type EmbeddingRequest struct {
	Model  string
	Inputs []string
}

// Embeddings are the vectors of EmbeddingRequest's inputs, in order.
type Embeddings struct {
	Vectors [][]float32
	Usage   Usage
}

// Question types, as decision models take them.
const (
	// QuestionYesNo is answered with the probability that it is true
	// (OpenAI's "predicate", the state-and-questions shape's "noul").
	QuestionYesNo = "noul"
	// QuestionChoice picks one of Choices.
	QuestionChoice = "choice"
	// QuestionScore places the state on Levels, lowest first.
	QuestionScore = "score"
)

// Question is one question a decision model answers about a state.
type Question struct {
	Name, Type, Instructions string
	// Choices are a choice question's options, with descriptions (which
	// may be empty).
	Choices []Choice
	// Levels describe a score question's levels, lowest first.
	Levels []string
	// True and False, when set, say what yes and no mean for a yes-or-no
	// question.
	True, False string
}

// Choice is an option of a choice question.
type Choice struct{ Value, Description string }

// DecisionRequest asks a decision model questions about a state, such as
// a row.
type DecisionRequest struct {
	Model string
	// State is what the questions are about: text, or JSON.
	State     string
	Questions []Question
}

// Answer is a decision model's answer to one question.
type Answer struct {
	Type string
	// Probability is a yes-or-no question's: that it is true.
	Probability float64
	// Choice is the likeliest option of a choice question.
	Choice string
	// Probabilities are by option, or by level ("0", "1", ...) for a score.
	Probabilities map[string]float64
	// Score is a score question's expected level, Σ i·pᵢ.
	Score float64
	// Confidence is how far the likeliest option stands out, from 0 to 1.
	Confidence float64
	// Refused says the model would not answer.
	Refused bool
}

// Decision is a decision model's answers, by question name.
type Decision struct {
	Answers map[string]Answer
	Usage   Usage
}

// Provider is a model provider's API. Errors are RowError for a request
// the provider refused for its content, and anything else for one that
// could not be served.
type Provider interface {
	Complete(ctx context.Context, r CompletionRequest) (Completion, error)
	// Embed returns ErrUnsupported where the provider has no embeddings.
	Embed(ctx context.Context, r EmbeddingRequest) (Embeddings, error)
	// Decide returns ErrUnsupported where it has no decision models.
	Decide(ctx context.Context, r DecisionRequest) (Decision, error)
	// CountTokens counts the input tokens of r, or returns
	// ErrUnsupported; counting costs nothing.
	CountTokens(ctx context.Context, r CompletionRequest) (int64, error)
}

// newProvider makes the Provider for in, which sends through c.
func newProvider(in Integration, c *client) (Provider, error) {
	switch in.Provider {
	case ProviderOpenAI, ProviderAzure:
		return &openAI{in: in, c: c}, nil
	case ProviderAnthropic:
		return &anthropic{in: in, c: c}, nil
	case ProviderGemini, ProviderVertex:
		return newGemini(in, c)
	case ProviderBedrock:
		return newBedrock(in, c)
	case ProviderDecisions:
		return &decisions{in: in, c: c}, nil
	case ProviderNixtla:
		return &nixtla{in: in, c: c}, nil
	case ProviderSageMaker:
		return newSageMaker(in, c), nil
	case ProviderBigQuery:
		return newBigQuery(in, c)
	}
	return nil, fmt.Errorf("%w: unknown provider %s", ErrInvalid, quote(in.Provider))
}

func unsupported(provider, what string) error {
	return fmt.Errorf("%w: %s has no %s", ErrUnsupported, provider, what)
}

// estimateTokens guesses the tokens of text where a provider cannot count
// them: about four bytes each, as English text runs with providers'
// tokenizers.
func estimateTokens(text string) int64 {
	return int64(len(text)+3) / 4
}

// requestTokens estimates r's input tokens.
func requestTokens(r CompletionRequest) int64 {
	n := estimateTokens(r.System) + int64(len(r.Schema))/4
	for _, m := range r.Messages {
		n += estimateTokens(m.Content) + 4
	}
	return n
}
