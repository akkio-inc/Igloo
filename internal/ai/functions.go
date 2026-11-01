package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// A function's arguments for one row, as DuckDB passes them: nil for
// NULL, strings, numbers, lists ([]any), structs (map[string]any).
type row = []any

// ErrorMode is what a function does with a row's own failure.
type ErrorMode string

const (
	// ErrorsNull gives the row NULL, as Snowflake's AI functions do.
	ErrorsNull ErrorMode = "null"
	// ErrorsDetails gives the row {"value": ..., "error": ...}
	// (return_error_details => TRUE).
	ErrorsDetails ErrorMode = "details"
	// ErrorsFail fails the statement, as SNOWFLAKE.CORTEX's functions,
	// AI_REDACT and AI_AGG do.
	ErrorsFail ErrorMode = "fail"
)

// options are a call's options, which the translation gives as a JSON
// object of constants or of the row's values.
type options map[string]any

func parseOptions(v any) (options, error) {
	switch v := v.(type) {
	case nil:
		return options{}, nil
	case map[string]any:
		return options(v), nil
	case string:
		if strings.TrimSpace(v) == "" {
			return options{}, nil
		}
		var o options
		if err := json.Unmarshal([]byte(v), &o); err != nil {
			return nil, fmt.Errorf("%w: the options must be an object: %v", ErrInvalid, err)
		}
		if o == nil {
			o = options{}
		}
		return o, nil
	}
	return nil, fmt.Errorf("%w: the options must be an object, not %T", ErrInvalid, v)
}

func (o options) bool(k string) bool {
	switch v := o[k].(type) {
	case bool:
		return v
	case string:
		b, _ := strconv.ParseBool(v)
		return b
	}
	return false
}

func (o options) string(k string) string {
	if v, ok := o[k].(string); ok {
		return v
	}
	return ""
}

func (o options) object(k string) map[string]any {
	switch v := o[k].(type) {
	case map[string]any:
		return v
	case string:
		var m map[string]any
		if json.Unmarshal([]byte(v), &m) == nil {
			return m
		}
	}
	return nil
}

// mode is the call's ErrorMode: on_error, or def.
func (o options) mode(def ErrorMode) ErrorMode {
	switch ErrorMode(o.string("on_error")) {
	case ErrorsNull:
		return ErrorsNull
	case ErrorsDetails:
		return ErrorsDetails
	case ErrorsFail:
		return ErrorsFail
	}
	if o.bool("return_error_details") {
		return ErrorsDetails
	}
	return def
}

// canonical writes v as JSON with sorted keys, for cache keys.
func canonical(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

var placeholder = regexp.MustCompile(`\{(\d+)\}`)

// text reads an input as text: a string, a PROMPT object (its template with
// {0}, {1}, ... replaced by its arguments), or any other value as its text.
func text(v any) (string, error) {
	switch v := v.(type) {
	case string:
		return v, nil
	case map[string]any:
		if tmpl, ok := v["template"].(string); ok {
			args, _ := v["args"].([]any)
			var err error
			out := placeholder.ReplaceAllStringFunc(tmpl, func(m string) string {
				n, _ := strconv.Atoi(m[1 : len(m)-1])
				if n >= len(args) {
					err = fmt.Errorf("%w: the PROMPT template has %s, but %d arguments", ErrInvalid, m, len(args))
					return m
				}
				s, _ := text(args[n])
				return s
			})
			return out, err
		}
		b, _ := json.Marshal(v)
		return string(b), nil
	case nil:
		return "", nil
	case []byte:
		return string(v), nil
	case time.Time:
		return v.Format("2006-01-02 15:04:05.999999"), nil
	case []any:
		b, _ := json.Marshal(v)
		return string(b), nil
	}
	return fmt.Sprint(v), nil
}

// jsonValue reads a JSON argument: text of JSON, or a value DuckDB decoded.
func jsonValue(v any) (any, error) {
	if s, ok := v.(string); ok {
		var out any
		if err := json.Unmarshal([]byte(s), &out); err != nil {
			return nil, fmt.Errorf("%w: not JSON: %v", ErrInvalid, err)
		}
		return out, nil
	}
	return v, nil
}

// result is a row's answer: its value, or the error of its own input.
type result struct {
	value any
	err   error
}

// finish turns rows' results into what the function returns, by mode:
// errors as NULL, as {"value", "error"} objects, or failing the statement.
// Values that are JSON text (jsonResult) are objects in {"value", "error"}.
func (s *Statement) finish(results []result, mode ErrorMode) ([]any, error) {
	out := make([]any, len(results))
	for i, r := range results {
		if r.err != nil {
			if errors.Is(r.err, errDryRun) {
				out[i] = nil
				continue
			}
			if !IsRowError(r.err) || mode == ErrorsFail {
				return nil, s.fail(r.err)
			}
			s.rowError(r.err)
		}
		switch mode {
		case ErrorsDetails:
			var errText any
			if r.err != nil {
				errText = r.err.Error()
			}
			value := r.value
			if j, ok := value.(jsonResult); ok {
				value = json.RawMessage(j)
			}
			if r.err != nil {
				value = nil
			}
			b, err := json.Marshal(map[string]any{"value": value, "error": errText})
			if err != nil {
				return nil, err
			}
			out[i] = string(b)
		default:
			if r.err == nil {
				out[i] = r.value
				if j, ok := r.value.(jsonResult); ok {
					out[i] = string(j)
				}
			}
		}
	}
	return out, nil
}

// jsonResult is a result that is JSON text, for a function that returns
// JSON: an object in {"value", "error"}.
type jsonResult string

// completion is what the cache keeps of a completion.
type completion struct {
	Text  string `json:"text"`
	Model string `json:"model,omitempty"`
	Usage Usage  `json:"usage"`
	// Created is when the provider answered, in Unix seconds.
	Created int64 `json:"created"`
}

// completeCall is the call that asks b's model for req's completion, keyed
// by fn, its template's version, and opts.
func (s *Statement) completeCall(fn, version string, b *binding, req CompletionRequest, opts any, refresh bool) *call {
	key := cacheKey(fn, version, b.in.cacheScope(), req.Model, canonical(opts), req.System, canonical(req.Messages),
		string(req.Schema), strconv.Itoa(req.MaxTokens))
	est := Usage{InputTokens: requestTokens(req), OutputTokens: int64(min(max(req.MaxTokens, 64), 1024))}
	return &call{key: key, fn: fn, b: b, model: req.Model, refresh: refresh, estimate: est,
		run: func(ctx context.Context) ([]byte, Usage, error) {
			c, err := b.provider.Complete(ctx, req)
			if err != nil {
				return nil, c.Usage, err
			}
			model := c.Model
			if model == "" {
				model = req.Model
			}
			v, err := json.Marshal(completion{Text: c.Text, Model: model, Usage: c.Usage, Created: time.Now().Unix()})
			return v, c.Usage, err
		}}
}

// completions runs calls and reads their results as completions.
func (s *Statement) completions(calls []*call) ([]completion, []error, error) {
	values, errs, err := s.do(calls, nil, 0)
	if err != nil {
		return nil, nil, err
	}
	out := make([]completion, len(calls))
	for i, v := range values {
		if errs[i] != nil || v == nil {
			continue
		}
		if err := json.Unmarshal(v, &out[i]); err != nil {
			errs[i] = rowErrorf("the cached answer cannot be read: %v", err)
		}
	}
	return out, errs, nil
}

// --- AI_COMPLETE ------------------------------------------------------------

const completeVersion = "complete/1"

// defaultMaxTokens is AI_COMPLETE's max_tokens unless model_parameters
// sets it, as Snowflake's.
const defaultMaxTokens = 4096

// modelParameters reads AI_COMPLETE's model_parameters into req.
func modelParameters(o options, req *CompletionRequest) error {
	p := o.object("model_parameters")
	req.MaxTokens = defaultMaxTokens
	for k, v := range p {
		f, isNum := v.(float64)
		switch strings.ToLower(k) {
		case "temperature":
			if !isNum || f < 0 || f > 2 {
				return fmt.Errorf("%w: temperature must be a number from 0 to 1", ErrInvalid)
			}
			req.Temperature = &f
		case "top_p":
			if !isNum || f < 0 || f > 1 {
				return fmt.Errorf("%w: top_p must be a number from 0 to 1", ErrInvalid)
			}
			req.TopP = &f
		case "max_tokens":
			if !isNum || f < 1 || f != float64(int(f)) {
				return fmt.Errorf("%w: max_tokens must be a whole number", ErrInvalid)
			}
			req.MaxTokens = int(f)
		case "guardrails":
			if b, _ := v.(bool); b {
				return fmt.Errorf("%w: guardrails (Cortex Guard) are not available in Igloo", ErrUnsupported)
			}
		default:
			return fmt.Errorf("%w: unknown model parameter %s; the parameters are temperature, top_p, max_tokens and guardrails",
				ErrInvalid, quote(k))
		}
	}
	return nil
}

// responseSchema reads AI_COMPLETE's response_format: {'type': 'json',
// 'schema': {...}}, or a schema itself.
func responseSchema(o options) (json.RawMessage, error) {
	f := o.object("response_format")
	if f == nil {
		return nil, nil
	}
	schema := f
	if t, ok := f["type"].(string); ok && strings.EqualFold(t, "json") {
		s, ok := f["schema"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: response_format's schema must be an object", ErrInvalid)
		}
		schema = s
	}
	if t, _ := schema["type"].(string); t != "object" {
		return nil, fmt.Errorf("%w: response_format's schema must describe an object", ErrInvalid)
	}
	b, err := json.Marshal(schema)
	return b, err
}

// Complete runs AI_COMPLETE, and SNOWFLAKE.CORTEX.COMPLETE, for rows of
// (model, prompt, options). object says the function returns an OBJECT:
// the answer as structured output (response_format), with details
// (show_details) or as the conversation form of COMPLETE gives it (options
// history); otherwise text.
func (s *Statement) Complete(rows []row, object bool) ([]any, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	results := make([]result, len(rows))
	calls := make([]*call, 0, len(rows))
	callRow := make([]int, 0, len(rows))
	type shape struct{ schema, details, convo bool }
	shapes := make([]shape, len(rows))
	var mode ErrorMode = ErrorsNull
	var decisionCalls []*call
	var decisionRows []int
	for i, r := range rows {
		model, _ := r[0].(string)
		o, err := parseOptions(r[2])
		if err != nil {
			return nil, s.fail(err)
		}
		if i == 0 {
			mode = o.mode(ErrorsNull)
		}
		if co := o.object("cortex_options"); co != nil {
			// SNOWFLAKE.CORTEX.COMPLETE's options: the model parameters and
			// response_format in one object.
			params := map[string]any{}
			for k, v := range co {
				if strings.EqualFold(k, "response_format") {
					o["response_format"] = v
				} else {
					params[k] = v
				}
			}
			o["model_parameters"] = params
		}
		b, provider, err := s.binding("ai_complete", model, "")
		if err != nil {
			return nil, s.fail(err)
		}
		if b.in.Provider == ProviderDecisions || b.in.DecisionModel != "" && provider == b.in.resolve(b.in.DecisionModel) {
			// snowflake-decision: the prompt is TO_JSON({'state': ..., 'questions': {...}}).
			p, err := text(r[1])
			if err != nil {
				return nil, s.fail(err)
			}
			c, err := s.decisionCall("ai_complete", b, provider, p, o.bool("refresh"))
			if err != nil {
				results[i].err = err
				continue
			}
			decisionCalls = append(decisionCalls, c)
			decisionRows = append(decisionRows, i)
			continue
		}
		req := CompletionRequest{Model: provider}
		if err := modelParameters(o, &req); err != nil {
			return nil, s.fail(err)
		}
		if req.Schema, err = responseSchema(o); err != nil {
			return nil, s.fail(err)
		}
		shapes[i] = shape{schema: req.Schema != nil, details: o.bool("show_details"), convo: o.bool("history")}
		if shapes[i].convo {
			if err := conversation(r[1], &req); err != nil {
				results[i].err = err
				continue
			}
		} else {
			p, err := text(r[1])
			if err != nil {
				if IsRowError(err) {
					results[i].err = err
					continue
				}
				return nil, s.fail(err)
			}
			req.Messages = []Message{{Role: "user", Content: p}}
		}
		keyOpts := map[string]any{"model_parameters": o.object("model_parameters")}
		calls = append(calls, s.completeCall("ai_complete", completeVersion, b, req, keyOpts, o.bool("refresh")))
		callRow = append(callRow, i)
	}
	if len(decisionCalls) > 0 {
		values, errs, err := s.do(decisionCalls, nil, 0)
		if err != nil {
			return nil, err
		}
		for k, i := range decisionRows {
			if errs[k] != nil {
				results[i].err = errs[k]
				continue
			}
			v, err := snowflakeDecision(values[k])
			results[i] = result{value: v, err: err}
		}
	}
	comps, errs, err := s.completions(calls)
	if err != nil {
		return nil, err
	}
	for k, i := range callRow {
		if errs[k] != nil {
			results[i].err = errs[k]
			continue
		}
		c, sh := comps[k], shapes[i]
		switch {
		case sh.schema:
			var obj any
			if err := json.Unmarshal([]byte(c.Text), &obj); err != nil {
				results[i].err = rowErrorf("the model's answer is not JSON of the response_format: %s", firstLine(c.Text, 200))
				continue
			}
			if !sh.details {
				results[i].value = jsonResult(jsonText(obj))
				continue
			}
			results[i].value = jsonResult(jsonText(map[string]any{"created": c.Created, "model": c.Model,
				"structured_output": []any{map[string]any{"raw_message": obj, "type": "json"}}, "usage": usageObject(c.Usage)}))
		case sh.details || sh.convo:
			results[i].value = jsonResult(jsonText(map[string]any{"choices": []any{map[string]any{"messages": c.Text}}, "created": c.Created,
				"model": c.Model, "usage": usageObject(c.Usage)}))
		case object:
			results[i].value = jsonResult(jsonText(c.Text))
		default:
			results[i].value = c.Text
		}
	}
	return s.finish(results, mode)
}

func usageObject(u Usage) map[string]any {
	return map[string]any{"completion_tokens": u.OutputTokens, "prompt_tokens": u.InputTokens,
		"total_tokens": u.InputTokens + u.OutputTokens}
}

// jsonText writes v as JSON text, for a function that returns JSON.
func jsonText(v any) string {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return "null"
	}
	return strings.TrimRight(b.String(), "\n")
}

// conversation reads COMPLETE's history, [{'role': ..., 'content': ...}],
// into req: at most one system message, first.
func conversation(v any, req *CompletionRequest) error {
	h, err := jsonValue(v)
	if err != nil {
		return rowErrorf("the conversation must be an array of {'role', 'content'} objects")
	}
	list, ok := h.([]any)
	if !ok || len(list) == 0 {
		return rowErrorf("the conversation must be a non-empty array of {'role', 'content'} objects")
	}
	for i, m := range list {
		obj, ok := m.(map[string]any)
		if !ok {
			return rowErrorf("message %d of the conversation is not an object", i+1)
		}
		role, _ := obj["role"].(string)
		content, err := text(obj["content"])
		if err != nil {
			return err
		}
		switch role {
		case "system":
			if i != 0 {
				return rowErrorf("the conversation's system message must come first")
			}
			req.System = content
		case "user", "assistant":
			req.Messages = append(req.Messages, Message{Role: role, Content: content})
		default:
			return rowErrorf("message %d of the conversation has role %s; use system, user or assistant", i+1, quote(role))
		}
	}
	if len(req.Messages) == 0 {
		return rowErrorf("the conversation has no user message")
	}
	return nil
}

// --- AI_COUNT_TOKENS --------------------------------------------------------

// countsTokens reports whether provider counts tokens for nothing:
// Anthropic and Gemini do; elsewhere they are estimated.
func countsTokens(provider string) bool {
	switch provider {
	case ProviderAnthropic, ProviderGemini, ProviderVertex:
		return true
	}
	return false
}

// countable are the functions AI_COUNT_TOKENS counts for, and whether each
// takes a model.
var countable = map[string]bool{
	"ai_complete": true, "ai_embed": true, "ai_similarity": false, "ai_classify": false, "ai_sentiment": false,
	"ai_summarize": false, "ai_redact": false, "ai_translate": false, "ai_filter": false, "ai_extract": false,
}

const countVersion = "count/1"

// CountTokens runs AI_COUNT_TOKENS for rows of (function, model, text,
// options): the input tokens of text for the model that would serve the
// function, as the provider counts them where it can, and estimated
// otherwise.
func (s *Statement) CountTokens(rows []row) ([]any, error) {
	results := make([]result, len(rows))
	var calls []*call
	var callRow []int
	var mode ErrorMode = ErrorsNull
	for i, r := range rows {
		fn, _ := r[0].(string)
		model, _ := r[1].(string)
		o, err := parseOptions(r[3])
		if err != nil {
			return nil, s.fail(err)
		}
		if i == 0 {
			mode = o.mode(ErrorsNull)
		}
		takesModel, ok := countable[fn]
		if !ok {
			return nil, s.fail(fmt.Errorf("%w: AI_COUNT_TOKENS counts for %s, not %s", ErrInvalid,
				strings.Join(slices.Sorted(maps.Keys(countable)), ", "), quote(fn)))
		}
		if takesModel && model == "" {
			return nil, s.fail(fmt.Errorf("%w: AI_COUNT_TOKENS for %s needs the model", ErrInvalid, fn))
		}
		if m := o.string("model"); m != "" && model == "" {
			model = m
		}
		want := ""
		if fn == "ai_embed" || fn == "ai_similarity" {
			want = "embedding"
		}
		b, provider, err := s.binding(fn, model, want)
		if err != nil {
			return nil, s.fail(err)
		}
		t, err := text(r[2])
		if err != nil {
			return nil, s.fail(err)
		}
		if !countsTokens(b.in.Provider) {
			// Estimated here: there is nothing to ask.
			results[i].value = estimateTokens(t)
			continue
		}
		req := CompletionRequest{Model: provider, Messages: []Message{{Role: "user", Content: t}}}
		key := cacheKey("ai_count_tokens", countVersion, b.in.cacheScope(), provider, t)
		calls = append(calls, &call{key: key, fn: "ai_count_tokens", b: b, model: provider, refresh: o.bool("refresh"),
			run: func(ctx context.Context) ([]byte, Usage, error) {
				n, err := b.provider.CountTokens(ctx, req)
				if errors.Is(err, ErrUnsupported) {
					n, err = estimateTokens(t), nil
				}
				if err != nil {
					return nil, Usage{}, err
				}
				v, err := json.Marshal(n)
				return v, Usage{}, err
			}})
		callRow = append(callRow, i)
	}
	values, errs, err := s.do(calls, nil, 0)
	if err != nil {
		return nil, err
	}
	for k, i := range callRow {
		if errs[k] != nil {
			results[i].err = errs[k]
			continue
		}
		var n int64
		if err := json.Unmarshal(values[k], &n); err != nil {
			results[i].err = rowErrorf("the cached count cannot be read: %v", err)
			continue
		}
		results[i].value = n
	}
	return s.finish(results, mode)
}
