package ai

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"
)

// The task functions: AI_CLASSIFY, AI_FILTER, AI_SENTIMENT, AI_EXTRACT,
// AI_SUMMARIZE, AI_TRANSLATE and AI_REDACT, and SNOWFLAKE.CORTEX's older
// forms of them. Each asks the default model of the integration that
// serves it (or the model its options name), with instructions that are
// the same for every row, its template, as the system prompt, and the
// row's text as the user's message, so that providers' prompt caches can
// serve the instructions; and asks for JSON of a schema where the answer
// has a shape. A template's version is part of the cache's key: changing a
// template changes its version.

// Templates' versions.
const (
	classifyVersion  = "classify/1"
	filterVersion    = "filter/1"
	sentimentVersion = "sentiment/1"
	scoreVersion     = "sentiment-score/1"
	extractVersion   = "extract/1"
	answerVersion    = "extract-answer/1"
	summarizeVersion = "summarize/1"
	translateVersion = "translate/1"
	redactVersion    = "redact/1"
)

// taskCall is the call that asks model, or the default model of the
// integration serving fn, for an answer to system about input.
func (s *Statement) taskCall(fn, version, model, system string, schema map[string]any, input string, maxTokens int, refresh bool) (*call, error) {
	want := ""
	if model == "" {
		want = "default"
	}
	b, provider, err := s.binding(fn, model, want)
	if err != nil {
		return nil, err
	}
	req := CompletionRequest{Model: provider, System: system, Messages: []Message{{Role: "user", Content: input}}, MaxTokens: maxTokens}
	if schema != nil {
		raw, err := json.Marshal(schema)
		if err != nil {
			return nil, err
		}
		req.Schema = raw
	}
	return s.completeCall(fn, version, b, req, nil, refresh), nil
}

// task is one row's call, and how its answer becomes the row's result.
type task struct {
	call *call
	// read makes the row's result of the model's answer.
	read func(text string) (any, error)
}

// runTasks runs the rows' tasks (nil for a row whose result is set
// already) and sets the rows' results.
func (s *Statement) runTasks(tasks []*task, results []result) error {
	var calls []*call
	var idx []int
	for i, t := range tasks {
		if t != nil {
			calls = append(calls, t.call)
			idx = append(idx, i)
		}
	}
	comps, errs, err := s.completions(calls)
	if err != nil {
		return err
	}
	for k, i := range idx {
		if errs[k] != nil {
			results[i].err = errs[k]
			continue
		}
		results[i].value, results[i].err = tasks[i].read(comps[k].Text)
	}
	return nil
}

// readJSON reads a model's answer, JSON of the schema it was given, into v.
func readJSON(text string, v any) error {
	if err := json.Unmarshal([]byte(extractJSON(text)), v); err != nil {
		return rowErrorf("the model's answer is not the JSON asked for: %s", firstLine(text, 200))
	}
	return nil
}

// objectSchema is the JSON schema of an object of props, all required, no
// other.
func objectSchema(props map[string]any) map[string]any {
	return map[string]any{"type": "object", "properties": props, "required": slices.Sorted(maps.Keys(props)),
		"additionalProperties": false}
}

// taskRows reads the rows' options, at optionsAt, and the mode of the
// first row's (a call's options are the same in every row).
func (s *Statement) taskRows(rows []row, optionsAt int) ([]options, ErrorMode, error) {
	opts := make([]options, len(rows))
	mode := ErrorsNull
	for i, r := range rows {
		o, err := parseOptions(r[optionsAt])
		if err != nil {
			return nil, "", s.fail(err)
		}
		opts[i] = o
		if i == 0 {
			mode = o.mode(ErrorsNull)
		}
	}
	return opts, mode, nil
}

// inputText reads a row's input; an input that cannot be read is the
// statement's error.
func (s *Statement) inputText(v any) (string, error) {
	t, err := text(v)
	if err != nil {
		return "", s.fail(err)
	}
	return t, nil
}

// --- AI_CLASSIFY ------------------------------------------------------------

// category is a category AI_CLASSIFY may choose.
type category struct{ Label, Description string }

// example is an example AI_CLASSIFY's config gives.
type example struct {
	Input, Explanation string
	Labels             []string
}

// classifyConfig is AI_CLASSIFY's config_object, and CLASSIFY_TEXT's
// options.
type classifyConfig struct {
	Task     string
	Multi    bool
	Examples []example
	// Model is the model the config names, if any.
	Model string
}

func parseCategories(v any) ([]category, error) {
	j, err := jsonValue(v)
	if err != nil {
		return nil, err
	}
	list, ok := j.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: the categories must be an array of labels, or of {'label', 'description'} objects", ErrInvalid)
	}
	var out []category
	seen := map[string]bool{}
	for _, x := range list {
		var c category
		switch x := x.(type) {
		case string:
			c.Label = x
		case map[string]any:
			c.Label, _ = x["label"].(string)
			c.Description, _ = x["description"].(string)
		}
		if strings.TrimSpace(c.Label) == "" {
			return nil, fmt.Errorf("%w: a category's label must be a non-empty string", ErrInvalid)
		}
		if seen[c.Label] {
			return nil, fmt.Errorf("%w: the category %s is given twice", ErrInvalid, quote(c.Label))
		}
		seen[c.Label] = true
		out = append(out, c)
	}
	if len(out) < 2 {
		return nil, fmt.Errorf("%w: AI_CLASSIFY needs at least two categories", ErrInvalid)
	}
	return out, nil
}

func parseClassifyConfig(o options) (classifyConfig, error) {
	var c classifyConfig
	cfg := o.object("config")
	for _, k := range slices.Sorted(maps.Keys(cfg)) {
		v := cfg[k]
		switch strings.ToLower(k) {
		case "task_description":
			c.Task, _ = v.(string)
		case "output_mode":
			m, _ := v.(string)
			switch strings.ToLower(m) {
			case "single", "":
			case "multi":
				c.Multi = true
			default:
				return c, fmt.Errorf("%w: output_mode is 'single' or 'multi'", ErrInvalid)
			}
		case "model":
			c.Model, _ = v.(string)
		case "examples":
			list, _ := v.([]any)
			for _, e := range list {
				m, _ := e.(map[string]any)
				var ex example
				ex.Input, _ = m["input"].(string)
				ex.Explanation, _ = m["explanation"].(string)
				switch l := m["labels"].(type) {
				case []any:
					for _, x := range l {
						if s, ok := x.(string); ok {
							ex.Labels = append(ex.Labels, s)
						}
					}
				case string:
					ex.Labels = []string{l}
				}
				if l, ok := m["label"].(string); ok {
					ex.Labels = append(ex.Labels, l)
				}
				c.Examples = append(c.Examples, ex)
			}
		default:
			return c, fmt.Errorf("%w: AI_CLASSIFY's config takes task_description, output_mode, examples and model, not %s",
				ErrInvalid, quote(k))
		}
	}
	return c, nil
}

func classifyTemplate(cats []category, cfg classifyConfig) string {
	var b strings.Builder
	if cfg.Multi {
		b.WriteString("Classify the text the user gives: choose every category below that applies to it, or none, by label.\n")
	} else {
		b.WriteString("Classify the text the user gives: choose the one category below that fits it best, by label.\n")
	}
	for _, c := range cats {
		fmt.Fprintf(&b, "- %q", c.Label)
		if c.Description != "" {
			b.WriteString(": " + c.Description)
		}
		b.WriteByte('\n')
	}
	if cfg.Task != "" {
		b.WriteString("\nThe task: " + cfg.Task + "\n")
	}
	if len(cfg.Examples) > 0 {
		b.WriteString("\nExamples:\n")
		for _, e := range cfg.Examples {
			labels, _ := json.Marshal(e.Labels)
			fmt.Fprintf(&b, "Text: %s\nLabels: %s\n", e.Input, labels)
			if e.Explanation != "" {
				b.WriteString("Why: " + e.Explanation + "\n")
			}
		}
	}
	b.WriteString("\nAnswer with a JSON object {\"labels\": [...]} of the labels exactly as given.")
	return b.String()
}

// Classify runs AI_CLASSIFY for rows of (input, categories, options):
// {"labels": [...]}; with the option label, as SNOWFLAKE.CORTEX.CLASSIFY_TEXT,
// {"label": ...}.
func (s *Statement) Classify(rows []row) ([]any, error) {
	opts, mode, err := s.taskRows(rows, 2)
	if err != nil {
		return nil, err
	}
	results := make([]result, len(rows))
	tasks := make([]*task, len(rows))
	for i, r := range rows {
		cats, err := parseCategories(r[1])
		if err != nil {
			return nil, s.fail(err)
		}
		cfg, err := parseClassifyConfig(opts[i])
		if err != nil {
			return nil, s.fail(err)
		}
		input, err := s.inputText(r[0])
		if err != nil {
			return nil, err
		}
		labels := make([]any, len(cats))
		for k, c := range cats {
			labels[k] = c.Label
		}
		schema := objectSchema(map[string]any{"labels": map[string]any{"type": "array", "items": map[string]any{"type": "string", "enum": labels}}})
		c, err := s.taskCall("ai_classify", classifyVersion, cfg.Model, classifyTemplate(cats, cfg), schema, input, 256, opts[i].bool("refresh"))
		if err != nil {
			return nil, s.fail(err)
		}
		multi, legacy := cfg.Multi, opts[i].bool("label")
		tasks[i] = &task{call: c, read: func(answer string) (any, error) {
			var a struct {
				Labels []string `json:"labels"`
			}
			if err := readJSON(answer, &a); err != nil {
				return nil, err
			}
			var chosen []string
			for _, l := range a.Labels {
				if slices.ContainsFunc(cats, func(c category) bool { return c.Label == l }) && !slices.Contains(chosen, l) {
					chosen = append(chosen, l)
				}
			}
			if !multi {
				if len(chosen) == 0 {
					return nil, rowErrorf("the model chose none of the categories")
				}
				chosen = chosen[:1]
			}
			return classifyResult(chosen, legacy), nil
		}}
	}
	if err := s.runTasks(tasks, results); err != nil {
		return nil, err
	}
	return s.finish(results, mode)
}

// classifyResult is AI_CLASSIFY's result, or with legacy CLASSIFY_TEXT's.
func classifyResult(labels []string, legacy bool) jsonResult {
	if labels == nil {
		labels = []string{}
	}
	if legacy {
		var label any
		if len(labels) > 0 {
			label = labels[0]
		}
		return jsonResult(jsonText(map[string]any{"label": label}))
	}
	return jsonResult(jsonText(map[string]any{"labels": labels}))
}

// --- AI_FILTER --------------------------------------------------------------

const filterTemplate = "Decide whether what the user gives is true: a statement that is true, or a question whose answer is yes. " +
	"Answer with a JSON object: {\"value\": true} or {\"value\": false}."

// Filter runs AI_FILTER for rows of (input, options): whether each row's
// statement is true. In a dry run, rows the cache cannot answer pass, so
// that the functions after the filter are counted for every row it might
// let through.
func (s *Statement) Filter(rows []row) ([]any, error) {
	opts, mode, err := s.taskRows(rows, 1)
	if err != nil {
		return nil, err
	}
	results := make([]result, len(rows))
	tasks := make([]*task, len(rows))
	schema := objectSchema(map[string]any{"value": map[string]any{"type": "boolean"}})
	for i, r := range rows {
		input, err := s.inputText(r[0])
		if err != nil {
			return nil, err
		}
		c, err := s.taskCall("ai_filter", filterVersion, opts[i].string("model"), filterTemplate, schema, input, 32, opts[i].bool("refresh"))
		if err != nil {
			return nil, s.fail(err)
		}
		tasks[i] = &task{call: c, read: readFilter}
	}
	if err := s.runTasks(tasks, results); err != nil {
		return nil, err
	}
	return s.filterResults(results, mode)
}

func readFilter(answer string) (any, error) {
	var a struct {
		Value *bool `json:"value"`
	}
	if err := readJSON(answer, &a); err != nil {
		return nil, err
	}
	if a.Value == nil {
		return nil, rowErrorf("the model's answer has no value")
	}
	return *a.Value, nil
}

// filterResults finishes AI_FILTER's results: in a dry run, TRUE for the
// rows the cache did not answer.
func (s *Statement) filterResults(results []result, mode ErrorMode) ([]any, error) {
	if s.opts.DryRun {
		for i := range results {
			if errors.Is(results[i].err, errDryRun) {
				results[i] = result{value: true}
			}
		}
	}
	return s.finish(results, mode)
}

// --- AI_SENTIMENT -----------------------------------------------------------

var sentiments = []any{"positive", "negative", "neutral", "mixed", "unknown"}

// Limits on AI_SENTIMENT's categories, as Snowflake's.
const (
	maxSentimentCategories = 10
	maxCategoryLength      = 30
)

// Sentiment runs AI_SENTIMENT for rows of (text, categories, options):
// {"categories": [{"name": "overall", "sentiment": ...}, ...]}, overall
// first and then the categories asked about, in their order.
func (s *Statement) Sentiment(rows []row) ([]any, error) {
	opts, mode, err := s.taskRows(rows, 2)
	if err != nil {
		return nil, err
	}
	results := make([]result, len(rows))
	tasks := make([]*task, len(rows))
	for i, r := range rows {
		names := []string{"overall"}
		j, err := jsonValue(r[1])
		if err != nil {
			return nil, s.fail(err)
		}
		list, _ := j.([]any)
		for _, x := range list {
			c, ok := x.(string)
			if !ok || strings.TrimSpace(c) == "" || utf8.RuneCountInString(c) > maxCategoryLength {
				return nil, s.fail(fmt.Errorf("%w: AI_SENTIMENT's categories are strings of 1 to %d characters", ErrInvalid, maxCategoryLength))
			}
			names = append(names, c)
		}
		if len(names)-1 > maxSentimentCategories {
			return nil, s.fail(fmt.Errorf("%w: AI_SENTIMENT takes at most %d categories", ErrInvalid, maxSentimentCategories))
		}
		input, err := s.inputText(r[0])
		if err != nil {
			return nil, err
		}
		var b strings.Builder
		b.WriteString("Rate the sentiment of the text the user gives, overall")
		if len(names) > 1 {
			b.WriteString(" and about each of these: " + strings.Join(names[1:], "; "))
		}
		b.WriteString(". Rate each positive, negative, neutral or mixed, or unknown for a category the text does not speak of. " +
			"Answer with a JSON object {\"categories\": [{\"name\": ..., \"sentiment\": ...}, ...]}, overall first.")
		enum := make([]any, len(names))
		for k, n := range names {
			enum[k] = n
		}
		schema := objectSchema(map[string]any{"categories": map[string]any{"type": "array", "items": objectSchema(map[string]any{
			"name": map[string]any{"type": "string", "enum": enum}, "sentiment": map[string]any{"type": "string", "enum": sentiments},
		})}})
		c, err := s.taskCall("ai_sentiment", sentimentVersion, opts[i].string("model"), b.String(), schema, input, 64+32*len(names),
			opts[i].bool("refresh"))
		if err != nil {
			return nil, s.fail(err)
		}
		tasks[i] = &task{call: c, read: func(answer string) (any, error) {
			var a struct {
				Categories []struct{ Name, Sentiment string } `json:"categories"`
			}
			if err := readJSON(answer, &a); err != nil {
				return nil, err
			}
			got := map[string]string{}
			for _, c := range a.Categories {
				v := strings.ToLower(c.Sentiment)
				if slices.Contains(sentiments, any(v)) {
					got[strings.ToLower(c.Name)] = v
				}
			}
			out := make([]map[string]string, 0, len(names))
			for _, n := range names {
				v := got[strings.ToLower(n)]
				if v == "" {
					v = "unknown"
				}
				out = append(out, map[string]string{"name": n, "sentiment": v})
			}
			return jsonResult(jsonText(map[string]any{"categories": out})), nil
		}}
	}
	if err := s.runTasks(tasks, results); err != nil {
		return nil, err
	}
	return s.finish(results, mode)
}

const scoreTemplate = "Score the sentiment of the text the user gives, from -1, the most negative, through 0, neutral, " +
	"to 1, the most positive. Answer with a JSON object {\"score\": n}."

// SentimentScore runs SNOWFLAKE.CORTEX.SENTIMENT for rows of (text,
// options): a score from -1 to 1.
func (s *Statement) SentimentScore(rows []row) ([]any, error) {
	opts, mode, err := s.taskRows(rows, 1)
	if err != nil {
		return nil, err
	}
	results := make([]result, len(rows))
	tasks := make([]*task, len(rows))
	schema := objectSchema(map[string]any{"score": map[string]any{"type": "number"}})
	for i, r := range rows {
		input, err := s.inputText(r[0])
		if err != nil {
			return nil, err
		}
		c, err := s.taskCall("ai_sentiment", scoreVersion, "", scoreTemplate, schema, input, 32, opts[i].bool("refresh"))
		if err != nil {
			return nil, s.fail(err)
		}
		tasks[i] = &task{call: c, read: func(answer string) (any, error) {
			var a struct {
				Score *float64 `json:"score"`
			}
			if err := readJSON(answer, &a); err != nil {
				return nil, err
			}
			if a.Score == nil {
				return nil, rowErrorf("the model's answer has no score")
			}
			return max(-1, min(1, *a.Score)), nil
		}}
	}
	if err := s.runTasks(tasks, results); err != nil {
		return nil, err
	}
	return s.finish(results, mode)
}

// --- AI_EXTRACT -------------------------------------------------------------

// question is something AI_EXTRACT asks of the text, and the key of its
// answer.
type question struct{ key, text string }

// parseResponseFormat reads AI_EXTRACT's responseFormat: {'key':
// 'question', ...}, ['question', ...], [['key', 'question'], ...], or
// {'schema': {...}}, a JSON schema of an object. It returns the
// questions, or the schema.
func parseResponseFormat(v any) ([]question, map[string]any, error) {
	j, err := jsonValue(v)
	if err != nil {
		return nil, nil, err
	}
	invalid := fmt.Errorf("%w: responseFormat is an object of questions by key, an array of questions or of [key, question] "+
		"pairs, or {'schema': {...}}", ErrInvalid)
	var qs []question
	switch f := j.(type) {
	case map[string]any:
		if schema, ok := f["schema"].(map[string]any); ok && len(f) == 1 {
			if t, _ := schema["type"].(string); t != "object" {
				return nil, nil, fmt.Errorf("%w: responseFormat's schema must describe an object", ErrInvalid)
			}
			return nil, schema, nil
		}
		for _, k := range slices.Sorted(maps.Keys(f)) {
			q, ok := f[k].(string)
			if !ok {
				return nil, nil, invalid
			}
			qs = append(qs, question{k, q})
		}
	case []any:
		for _, x := range f {
			switch x := x.(type) {
			case string:
				qs = append(qs, question{x, x})
			case []any:
				if len(x) != 2 {
					return nil, nil, invalid
				}
				k, ok1 := x[0].(string)
				q, ok2 := x[1].(string)
				if !ok1 || !ok2 {
					return nil, nil, invalid
				}
				qs = append(qs, question{k, q})
			default:
				return nil, nil, invalid
			}
		}
	}
	if len(qs) == 0 {
		return nil, nil, invalid
	}
	return qs, nil, nil
}

// Extract runs AI_EXTRACT for rows of (text, responseFormat, options):
// {"error": null, "response": {...}}, or for a row the model could not
// answer {"error": "...", "response": null}, as Snowflake's AI_EXTRACT
// gives in place of NULL.
func (s *Statement) Extract(rows []row) ([]any, error) {
	opts, mode, err := s.taskRows(rows, 2)
	if err != nil {
		return nil, err
	}
	results := make([]result, len(rows))
	tasks := make([]*task, len(rows))
	for i, r := range rows {
		qs, schema, err := parseResponseFormat(r[1])
		if err != nil {
			return nil, s.fail(err)
		}
		input, err := s.inputText(r[0])
		if err != nil {
			return nil, err
		}
		var b strings.Builder
		b.WriteString("Extract from the text the user gives what answers each question below, from the text alone. " +
			"Where the text does not answer a question, answer null.\n")
		if schema == nil {
			props := map[string]any{}
			for _, q := range qs {
				fmt.Fprintf(&b, "- %s: %s\n", q.key, q.text)
				props[q.key] = map[string]any{"type": []any{"string", "null"}}
			}
			schema = objectSchema(props)
			b.WriteString("Answer with a JSON object of the answers, by the keys given.")
		} else {
			b.WriteString("Answer with a JSON object of the JSON Schema given, whose descriptions are the questions.")
		}
		c, err := s.taskCall("ai_extract", extractVersion, opts[i].string("model"), b.String(), schema, input, 4096, opts[i].bool("refresh"))
		if err != nil {
			return nil, s.fail(err)
		}
		tasks[i] = &task{call: c, read: func(answer string) (any, error) {
			var a map[string]any
			if err := readJSON(answer, &a); err != nil {
				return nil, err
			}
			return jsonResult(jsonText(map[string]any{"error": nil, "response": a})), nil
		}}
	}
	if err := s.runTasks(tasks, results); err != nil {
		return nil, err
	}
	if mode == ErrorsFail {
		return s.finish(results, mode)
	}
	// Its own shape for errors, whatever return_error_details says.
	out := make([]any, len(rows))
	for i, r := range results {
		switch {
		case r.err == nil:
			out[i] = string(r.value.(jsonResult))
		case errors.Is(r.err, errDryRun):
		case IsRowError(r.err):
			s.rowError(r.err)
			out[i] = jsonText(map[string]any{"error": r.err.Error(), "response": nil})
		default:
			return nil, s.fail(r.err)
		}
	}
	return out, nil
}

const answerTemplate = "Answer the question below from the text the user gives, in the text's own words where it answers it; " +
	"where it does not, answer null. Answer with a JSON object {\"answer\": ...}.\nThe question: "

// ExtractAnswer runs SNOWFLAKE.CORTEX.EXTRACT_ANSWER for rows of
// (source_document, question, options): [{"answer": ..., "score": null}],
// or [] where the text does not answer it. The models Igloo calls give no
// score.
func (s *Statement) ExtractAnswer(rows []row) ([]any, error) {
	opts, mode, err := s.taskRows(rows, 2)
	if err != nil {
		return nil, err
	}
	results := make([]result, len(rows))
	tasks := make([]*task, len(rows))
	schema := objectSchema(map[string]any{"answer": map[string]any{"type": []any{"string", "null"}}})
	for i, r := range rows {
		input, err := s.inputText(r[0])
		if err != nil {
			return nil, err
		}
		q, err := s.inputText(r[1])
		if err != nil {
			return nil, err
		}
		c, err := s.taskCall("ai_extract", answerVersion, "", answerTemplate+q, schema, input, 512, opts[i].bool("refresh"))
		if err != nil {
			return nil, s.fail(err)
		}
		tasks[i] = &task{call: c, read: func(answer string) (any, error) {
			var a struct {
				Answer *string `json:"answer"`
			}
			if err := readJSON(answer, &a); err != nil {
				return nil, err
			}
			if a.Answer == nil {
				return jsonResult("[]"), nil
			}
			return jsonResult(jsonText([]any{map[string]any{"answer": *a.Answer, "score": nil}})), nil
		}}
	}
	if err := s.runTasks(tasks, results); err != nil {
		return nil, err
	}
	return s.finish(results, mode)
}

// --- AI_SUMMARIZE -----------------------------------------------------------

const summarizeTemplate = "Summarize the text the user gives in a few sentences, in the language it is written in. " +
	"Reply with the summary alone."

// summarizeShortest is the longest text AI_SUMMARIZE gives back as it is.
const summarizeShortest = 110

// Summarize runs AI_SUMMARIZE, and SNOWFLAKE.CORTEX.SUMMARIZE, for rows of
// (text, options): text of at most summarizeShortest characters as it is,
// and a summary of anything longer.
func (s *Statement) Summarize(rows []row) ([]any, error) {
	opts, mode, err := s.taskRows(rows, 1)
	if err != nil {
		return nil, err
	}
	results := make([]result, len(rows))
	tasks := make([]*task, len(rows))
	for i, r := range rows {
		input, err := s.inputText(r[0])
		if err != nil {
			return nil, err
		}
		if utf8.RuneCountInString(input) <= summarizeShortest {
			results[i].value = input
			continue
		}
		c, err := s.taskCall("ai_summarize", summarizeVersion, opts[i].string("model"), summarizeTemplate, nil, input, 1024,
			opts[i].bool("refresh"))
		if err != nil {
			return nil, s.fail(err)
		}
		tasks[i] = &task{call: c, read: trimmed}
	}
	if err := s.runTasks(tasks, results); err != nil {
		return nil, err
	}
	return s.finish(results, mode)
}

func trimmed(answer string) (any, error) { return strings.TrimSpace(answer), nil }

// --- AI_TRANSLATE -----------------------------------------------------------

// languages are the languages AI_TRANSLATE knows, by their codes, as
// Snowflake's.
var languages = map[string]string{
	"ar": "Arabic", "zh": "Chinese", "hr": "Croatian", "cs": "Czech", "nl": "Dutch", "en": "English", "fi": "Finnish",
	"fr": "French", "de": "German", "el": "Greek", "he": "Hebrew", "hi": "Hindi", "it": "Italian", "ja": "Japanese",
	"ko": "Korean", "no": "Norwegian", "pl": "Polish", "pt": "Portuguese", "ro": "Romanian", "ru": "Russian",
	"es": "Spanish", "sv": "Swedish", "tr": "Turkish",
}

// Translate runs AI_TRANSLATE, and SNOWFLAKE.CORTEX.TRANSLATE, for rows of
// (text, source_language, target_language, options); an empty source
// language is the text's own, which the model works out.
func (s *Statement) Translate(rows []row) ([]any, error) {
	opts, mode, err := s.taskRows(rows, 3)
	if err != nil {
		return nil, err
	}
	results := make([]result, len(rows))
	tasks := make([]*task, len(rows))
	for i, r := range rows {
		input, err := s.inputText(r[0])
		if err != nil {
			return nil, err
		}
		src, _ := r[1].(string)
		tgt, _ := r[2].(string)
		to, ok := languages[strings.ToLower(strings.TrimSpace(tgt))]
		if !ok {
			return nil, s.fail(fmt.Errorf("%w: %s is not a language AI_TRANSLATE knows; use a code such as 'en', 'de' or 'ja'",
				ErrInvalid, quote(tgt)))
		}
		from := "whatever language it is written in"
		if strings.TrimSpace(src) != "" {
			if from, ok = languages[strings.ToLower(strings.TrimSpace(src))]; !ok {
				return nil, s.fail(fmt.Errorf("%w: %s is not a language AI_TRANSLATE knows; use a code such as 'en', 'de' or 'ja', "+
					"or '' for the text's own", ErrInvalid, quote(src)))
			}
		}
		template := "Translate the text the user gives from " + from + " into " + to + ". Reply with the translation alone."
		c, err := s.taskCall("ai_translate", translateVersion, opts[i].string("model"), template, nil, input, 4096, opts[i].bool("refresh"))
		if err != nil {
			return nil, s.fail(err)
		}
		tasks[i] = &task{call: c, read: trimmed}
	}
	if err := s.runTasks(tasks, results); err != nil {
		return nil, err
	}
	return s.finish(results, mode)
}

// --- AI_REDACT --------------------------------------------------------------

// redactCategory is a category of personal information AI_REDACT finds.
type redactCategory struct{ name, what string }

// redactCategories are AI_REDACT's categories, as Snowflake's, and what
// each is.
var redactCategories = []redactCategory{
	{"NAME", "a person's name"}, {"EMAIL", "an email address"}, {"PHONE_NUMBER", "a phone number"},
	{"DATE_OF_BIRTH", "a date of birth"}, {"GENDER", "a person's gender"}, {"AGE", "a person's age"},
	{"ADDRESS", "a postal address, or part of one"}, {"NATIONAL_ID", "a national identity number, such as a social security number"},
	{"PASSPORT", "a passport number"}, {"TAX_IDENTIFIER", "a tax identification number"},
	{"PAYMENT_CARD_DATA", "a payment card's number, expiry date or security code"}, {"DRIVERS_LICENSE", "a driver's license number"},
	{"IP_ADDRESS", "an IP address"},
}

// span is personal information AI_REDACT found, at characters [Start, End)
// of the text.
type span struct {
	Category string `json:"category"`
	Start    int    `json:"start"`
	End      int    `json:"end"`
	Text     string `json:"text"`
}

// Redact runs AI_REDACT for rows of (text, categories, options): the text
// with each piece of personal information replaced by its category in
// brackets, as [NAME], or with detect, {"spans": [{"category", "start",
// "end", "text"}]}, offsets in characters. As Snowflake's, it fails the
// statement on an error unless asked for the error's details.
func (s *Statement) Redact(rows []row, detect bool) ([]any, error) {
	opts, mode, err := s.taskRows(rows, 2)
	if err != nil {
		return nil, err
	}
	if len(opts) > 0 && opts[0].mode("") == "" {
		mode = ErrorsFail
	}
	results := make([]result, len(rows))
	tasks := make([]*task, len(rows))
	for i, r := range rows {
		var want []string
		j, err := jsonValue(r[1])
		if err != nil {
			return nil, s.fail(err)
		}
		list, _ := j.([]any)
		for _, x := range list {
			c, _ := x.(string)
			c = strings.ToUpper(strings.TrimSpace(c))
			if !slices.ContainsFunc(redactCategories, func(rc redactCategory) bool { return rc.name == c }) {
				return nil, s.fail(fmt.Errorf("%w: AI_REDACT has no category %s; its categories are NAME, EMAIL, PHONE_NUMBER, "+
					"DATE_OF_BIRTH, GENDER, AGE, ADDRESS, NATIONAL_ID, PASSPORT, TAX_IDENTIFIER, PAYMENT_CARD_DATA, DRIVERS_LICENSE "+
					"and IP_ADDRESS", ErrInvalid, quote(c)))
			}
			want = append(want, c)
		}
		var b strings.Builder
		b.WriteString("Find the personal information in the text the user gives, of these categories:\n")
		var names []any
		for _, rc := range redactCategories {
			if len(want) == 0 || slices.Contains(want, rc.name) {
				fmt.Fprintf(&b, "- %s: %s\n", rc.name, rc.what)
				names = append(names, rc.name)
			}
		}
		b.WriteString("Give each piece in the order it appears, with its category and its text exactly as written. " +
			"Answer with a JSON object {\"spans\": [{\"category\": ..., \"text\": ...}, ...]}, with no spans if there are none.")
		schema := objectSchema(map[string]any{"spans": map[string]any{"type": "array", "items": objectSchema(map[string]any{
			"category": map[string]any{"type": "string", "enum": names}, "text": map[string]any{"type": "string"},
		})}})
		input, err := s.inputText(r[0])
		if err != nil {
			return nil, err
		}
		c, err := s.taskCall("ai_redact", redactVersion, opts[i].string("model"), b.String(), schema, input, 2048, opts[i].bool("refresh"))
		if err != nil {
			return nil, s.fail(err)
		}
		tasks[i] = &task{call: c, read: func(answer string) (any, error) {
			var a struct {
				Spans []span `json:"spans"`
			}
			if err := readJSON(answer, &a); err != nil {
				return nil, err
			}
			spans := locate(input, a.Spans, names)
			if detect {
				return jsonResult(jsonText(map[string]any{"spans": spans})), nil
			}
			return redacted(input, spans), nil
		}}
	}
	if err := s.runTasks(tasks, results); err != nil {
		return nil, err
	}
	return s.finish(results, mode)
}

// locate finds each span's text in input, in characters, each after the
// one before it where it can, and drops spans of other categories, spans
// it cannot find, and spans that overlap one found before.
func locate(input string, spans []span, categories []any) []span {
	runes := []rune(input)
	out := []span{}
	from := 0
	for _, sp := range spans {
		t := []rune(sp.Text)
		if len(t) == 0 || !slices.Contains(categories, any(sp.Category)) {
			continue
		}
		at := indexRunes(runes, t, from)
		if at < 0 {
			at = indexRunes(runes, t, 0)
		}
		if at < 0 || slices.ContainsFunc(out, func(o span) bool { return at < o.End && o.Start < at+len(t) }) {
			continue
		}
		out = append(out, span{Category: sp.Category, Start: at, End: at + len(t), Text: sp.Text})
		from = at + len(t)
	}
	slices.SortFunc(out, func(a, b span) int { return a.Start - b.Start })
	return out
}

func indexRunes(s, sub []rune, from int) int {
	for i := from; i+len(sub) <= len(s); i++ {
		if slices.Equal(s[i:i+len(sub)], sub) {
			return i
		}
	}
	return -1
}

// redacted is input with each span, in order and not overlapping, replaced
// by [CATEGORY].
func redacted(input string, spans []span) string {
	runes := []rune(input)
	var b strings.Builder
	last := 0
	for _, sp := range spans {
		b.WriteString(string(runes[last:sp.Start]))
		b.WriteString("[" + sp.Category + "]")
		last = sp.End
	}
	b.WriteString(string(runes[last:]))
	return b.String()
}
