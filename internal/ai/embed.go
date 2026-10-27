package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
)

// AI_EMBED and AI_SIMILARITY: embeddings from an integration's embedding
// models, sent in batches of distinct texts, and kept in the cache by
// model and text, which the two functions share.

const embedVersion = "embed/1"

// embedBatch is how many texts one embeddings request sends.
const embedBatch = 64

// embedCall is the call that asks b's embedding model for text's vector.
func (s *Statement) embedCall(b *binding, model, text string, refresh bool) *call {
	key := cacheKey("ai_embed", embedVersion, b.in.Provider, b.in.Target(), model, text)
	return &call{key: key, fn: "ai_embed", b: b, model: model, refresh: refresh, group: b.in.Name + "/" + model, input: text,
		estimate: Usage{InputTokens: estimateTokens(text)}}
}

// embedBatches sends the texts of calls, all of one group, in one request.
func embedBatches(ctx context.Context, calls []*call) ([][]byte, Usage, error) {
	inputs := make([]string, len(calls))
	for i, c := range calls {
		inputs[i] = c.input
	}
	first := calls[0]
	e, err := first.b.provider.Embed(ctx, EmbeddingRequest{Model: first.model, Inputs: inputs})
	if err != nil {
		return nil, e.Usage, err
	}
	if len(e.Vectors) != len(inputs) {
		return nil, e.Usage, rowErrorf("the provider answered with %d embeddings for %d texts", len(e.Vectors), len(inputs))
	}
	out := make([][]byte, len(inputs))
	for i, v := range e.Vectors {
		if len(v) == 0 {
			return nil, e.Usage, rowErrorf("the provider answered with no embedding for a text")
		}
		if out[i], err = json.Marshal(v); err != nil {
			return nil, e.Usage, err
		}
	}
	return out, e.Usage, nil
}

// embeddings runs calls and reads their results as vectors.
func (s *Statement) embeddings(calls []*call) ([][]float32, []error, error) {
	values, errs, err := s.do(calls, embedBatches, embedBatch)
	if err != nil {
		return nil, nil, err
	}
	out := make([][]float32, len(calls))
	for i, v := range values {
		if errs[i] != nil || v == nil {
			continue
		}
		if err := json.Unmarshal(v, &out[i]); err != nil {
			errs[i] = rowErrorf("the cached embedding cannot be read: %v", err)
		}
	}
	return out, errs, nil
}

// Embed runs AI_EMBED, and SNOWFLAKE.CORTEX.EMBED_TEXT_768 and _1024, for
// rows of (model, text, options): the text's vector from the model. The
// option dimensions says how many dimensions the vector must have.
func (s *Statement) Embed(rows []row) ([]any, error) {
	opts, mode, err := s.taskRows(rows, 2)
	if err != nil {
		return nil, err
	}
	results := make([]result, len(rows))
	calls := make([]*call, len(rows))
	for i, r := range rows {
		model, _ := r[0].(string)
		if model == "" {
			return nil, s.fail(fmt.Errorf("%w: AI_EMBED needs the model", ErrInvalid))
		}
		b, provider, err := s.binding("ai_embed", model, "")
		if err != nil {
			return nil, s.fail(err)
		}
		input, err := s.inputText(r[1])
		if err != nil {
			return nil, err
		}
		calls[i] = s.embedCall(b, provider, input, opts[i].bool("refresh"))
	}
	vectors, errs, err := s.embeddings(calls)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		if errs[i] != nil {
			results[i].err = errs[i]
			continue
		}
		v := vectors[i]
		if d, ok := opts[i]["dimensions"].(float64); ok && int(d) != len(v) {
			model, _ := rows[i][0].(string)
			return nil, s.fail(fmt.Errorf("%w: the model %s gives vectors of %d dimensions, not %d; map %s to a model of %d "+
				"dimensions", ErrInvalid, quote(model), len(v), int(d), quote(model), int(d)))
		}
		results[i].value = v
	}
	return s.finish(results, mode)
}

// Similarity runs AI_SIMILARITY for rows of (input1, input2, options): the
// cosine similarity of the two texts' vectors, from the model the config
// names, or the integration's EMBEDDING_MODEL.
func (s *Statement) Similarity(rows []row) ([]any, error) {
	opts, mode, err := s.taskRows(rows, 2)
	if err != nil {
		return nil, err
	}
	results := make([]result, len(rows))
	calls := make([]*call, 0, 2*len(rows))
	for i, r := range rows {
		model, _ := opts[i].object("config")["model"].(string)
		want := ""
		if model == "" {
			want = "embedding"
		}
		b, provider, err := s.binding("ai_similarity", model, want)
		if err != nil {
			return nil, s.fail(err)
		}
		for k := range 2 {
			input, err := s.inputText(r[k])
			if err != nil {
				return nil, err
			}
			calls = append(calls, s.embedCall(b, provider, input, opts[i].bool("refresh")))
		}
	}
	vectors, errs, err := s.embeddings(calls)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		a, b := 2*i, 2*i+1
		switch {
		case errs[a] != nil:
			results[i].err = errs[a]
		case errs[b] != nil:
			results[i].err = errs[b]
		default:
			results[i].value, results[i].err = cosine(vectors[a], vectors[b])
		}
	}
	return s.finish(results, mode)
}

// cosine is the cosine similarity of a and b.
func cosine(a, b []float32) (any, error) {
	if len(a) != len(b) {
		return nil, rowErrorf("the vectors have %d and %d dimensions", len(a), len(b))
	}
	var dot, na, nb float64
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot += x * y
		na += x * x
		nb += y * y
	}
	if na == 0 || nb == 0 {
		return nil, nil
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb)), nil
}
