package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// Decision models answer yes-or-no, choice and score questions about a
// state with probabilities, in one forward pass and without writing text.
// AI_DECIDE asks them in the shape of Databricks' ai_decide; AI_COMPLETE
// with a model mapped to an integration's DECISION_MODEL, as Snowflake's
// snowflake-decision, in Snowflake's; and AI_FILTER and AI_CLASSIFY go to
// one when an integration has one (see tasks.go).

const decideVersion = "decide/1"

// Limits on questions, as decision models' APIs have them.
const (
	maxQuestions = 400
	maxOptions   = 255
	maxLevels    = 10
)

var questionName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// parseQuestions reads questions in the state-and-questions shape:
// {"name": {"type": "noul" | "choice" | "score", "instructions": ...,
// "criteria": ...}}, in name order.
func parseQuestions(v any) ([]Question, error) {
	j, err := jsonValue(v)
	if err != nil {
		return nil, fmt.Errorf("%w: the questions must be a JSON object: %v", ErrInvalid, err)
	}
	m, ok := j.(map[string]any)
	if !ok || len(m) == 0 {
		return nil, fmt.Errorf("%w: the questions must be a JSON object of named questions, such as "+
			`{"urgent": {"type": "noul", "instructions": "Is it urgent?"}}`, ErrInvalid)
	}
	if len(m) > maxQuestions {
		return nil, fmt.Errorf("%w: at most %d questions", ErrInvalid, maxQuestions)
	}
	var out []Question
	for _, name := range slices.Sorted(maps.Keys(m)) {
		if !questionName.MatchString(name) {
			return nil, fmt.Errorf("%w: question name %s must be 1 to 64 letters, digits, _ and -", ErrInvalid, quote(name))
		}
		qm, ok := m[name].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: question %s must be an object with type and instructions", ErrInvalid, name)
		}
		q := Question{Name: name}
		q.Type, _ = qm["type"].(string)
		q.Type = strings.ToLower(q.Type)
		switch ins := qm["instructions"].(type) {
		case string:
			q.Instructions = ins
		case nil:
		default:
			b, _ := json.Marshal(ins)
			q.Instructions = string(b)
		}
		criteria := qm["criteria"]
		switch q.Type {
		case "noul", "yes_no", "predicate", "boolean":
			q.Type = QuestionYesNo
			if c, ok := criteria.(map[string]any); ok {
				q.True, _ = c["true"].(string)
				q.False, _ = c["false"].(string)
			}
		case QuestionChoice:
			switch c := criteria.(type) {
			case map[string]any:
				for _, k := range slices.Sorted(maps.Keys(c)) {
					d, _ := c[k].(string)
					q.Choices = append(q.Choices, Choice{Value: k, Description: d})
				}
			case []any:
				for _, x := range c {
					s, ok := x.(string)
					if !ok {
						return nil, fmt.Errorf("%w: question %s's choices must be strings", ErrInvalid, name)
					}
					q.Choices = append(q.Choices, Choice{Value: s})
				}
			}
			if len(q.Choices) < 1 || len(q.Choices) > maxOptions {
				return nil, fmt.Errorf("%w: choice question %s needs 1 to %d choices in its criteria", ErrInvalid, name, maxOptions)
			}
		case QuestionScore:
			c, ok := criteria.([]any)
			if !ok || len(c) < 2 || len(c) > maxLevels {
				return nil, fmt.Errorf("%w: score question %s needs 2 to %d levels in its criteria, lowest first", ErrInvalid, name, maxLevels)
			}
			for _, x := range c {
				s, _ := x.(string)
				q.Levels = append(q.Levels, s)
			}
		default:
			return nil, fmt.Errorf("%w: question %s has type %s; use noul, choice or score", ErrInvalid, name, quote(q.Type))
		}
		out = append(out, q)
	}
	return out, nil
}

// decision is what the cache keeps of a decision.
type decision struct {
	Answers map[string]Answer `json:"answers"`
	Usage   Usage             `json:"usage"`
	// Levels are the score questions' levels, for their legends.
	Levels map[string][]string `json:"levels,omitempty"`
}

// decideCall is the call that asks b's decision model questions about
// state.
func (s *Statement) decideCall(fn string, b *binding, model, state string, qs []Question, refresh bool) *call {
	key := cacheKey(fn, decideVersion, b.in.Provider, b.in.Target(), model, canonical(qs), state)
	est := Usage{InputTokens: estimateTokens(state) + 40*int64(len(qs))}
	return &call{key: key, fn: fn, b: b, model: model, refresh: refresh, estimate: est,
		run: func(ctx context.Context) ([]byte, Usage, error) {
			d, err := b.provider.Decide(ctx, DecisionRequest{Model: model, State: state, Questions: qs})
			if err != nil {
				return nil, d.Usage, err
			}
			for _, q := range qs {
				a, ok := d.Answers[q.Name]
				if !ok {
					return nil, d.Usage, rowErrorf("the decision model did not answer %s", q.Name)
				}
				d.Answers[q.Name] = complete(q, a)
			}
			out := decision{Answers: d.Answers, Usage: d.Usage, Levels: map[string][]string{}}
			for _, q := range qs {
				if q.Type == QuestionScore {
					out.Levels[q.Name] = q.Levels
				}
			}
			v, err := json.Marshal(out)
			return v, d.Usage, err
		}}
}

// complete fills in what a provider left out of an answer that its
// probabilities give: the likeliest choice, the expected score, the
// confidence.
func complete(q Question, a Answer) Answer {
	if a.Type == "" {
		a.Type = q.Type
	}
	if len(a.Probabilities) == 0 || a.Refused {
		return a
	}
	var best string
	var pmax float64
	for _, k := range slices.Sorted(maps.Keys(a.Probabilities)) {
		if p := a.Probabilities[k]; p > pmax {
			best, pmax = k, p
		}
	}
	if a.Type == QuestionChoice && a.Choice == "" {
		a.Choice = best
	}
	if a.Type == QuestionScore && a.Score == 0 {
		for k, p := range a.Probabilities {
			if i, err := strconv.Atoi(k); err == nil {
				a.Score += float64(i) * p
			}
		}
	}
	if n := float64(len(a.Probabilities)); a.Confidence == 0 && n > 1 {
		a.Confidence = max(0, (n*pmax-1)/(n-1))
	}
	return a
}

// stateText reads a decision's state: text, or any other value as JSON.
func stateText(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return jsonText(v)
}

// decisionCall reads the prompt of AI_COMPLETE with a decision model,
// TO_JSON({'state': ..., 'questions': {...}}), and returns its call.
func (s *Statement) decisionCall(fn string, b *binding, model, prompt string, refresh bool) (*call, error) {
	var p struct {
		State     any `json:"state"`
		Questions any `json:"questions"`
	}
	if err := json.Unmarshal([]byte(prompt), &p); err != nil || p.State == nil || p.Questions == nil {
		return nil, rowErrorf("a decision model's prompt is TO_JSON({'state': ..., 'questions': {...}})")
	}
	qs, err := parseQuestions(p.Questions)
	if err != nil {
		return nil, &RowError{Msg: err.Error()}
	}
	return s.decideCall(fn, b, model, stateText(p.State), qs, refresh), nil
}

// answerObject writes an answer in the state-and-questions shape; yesNo
// is the key of a yes-or-no question's probability: "noul" in
// Snowflake's, "probability" in Databricks'.
func answerObject(a Answer, levels []string, yesNo string) map[string]any {
	if a.Refused {
		return map[string]any{"type": "refusal"}
	}
	o := map[string]any{"type": a.Type}
	switch a.Type {
	case QuestionYesNo:
		o[yesNo] = a.Probability
	case QuestionChoice:
		o["choice"], o["probabilities"], o["confidence"] = a.Choice, a.Probabilities, a.Confidence
	case QuestionScore:
		legend := map[string]string{}
		for i, l := range levels {
			legend[strconv.Itoa(i)] = l
		}
		o["score"], o["legend"], o["probabilities"], o["confidence"] = a.Score, legend, a.Probabilities, a.Confidence
	}
	return o
}

// snowflakeDecision writes a cached decision as Snowflake's
// snowflake-decision does: {"answers": {...}}.
func snowflakeDecision(v []byte) (any, error) {
	var d decision
	if err := json.Unmarshal(v, &d); err != nil {
		return nil, rowErrorf("the cached decision cannot be read: %v", err)
	}
	answers := map[string]any{}
	for name, a := range d.Answers {
		answers[name] = answerObject(a, d.Levels[name], "noul")
	}
	return jsonResult(jsonText(map[string]any{"answers": answers})), nil
}

// threshold is the probability from which a decision model's yes counts.
func (s *Statement) threshold() float64 {
	if t := s.opts.Threshold; t > 0 {
		return t
	}
	return 0.5
}

// decisionFor returns the decision model that serves AI_FILTER and
// AI_CLASSIFY in place of a generative one: the one an integration the
// user may use names, unless the call names a model; ok is false where
// none does.
func (s *Statement) decisionFor(opts []options, model func(options) string) (*binding, string, bool, error) {
	for _, o := range opts {
		if model(o) != "" {
			return nil, "", false, nil
		}
	}
	b, m, ok, err := s.decisionBinding()
	if err != nil {
		return nil, "", false, s.fail(err)
	}
	return b, m, ok, nil
}

// readDecision reads a cached decision's answer to question.
func readDecision(v []byte, question string) (Answer, error) {
	var d decision
	if err := json.Unmarshal(v, &d); err != nil {
		return Answer{}, rowErrorf("the cached decision cannot be read: %v", err)
	}
	a, ok := d.Answers[question]
	if !ok {
		return Answer{}, rowErrorf("the decision model did not answer %s", question)
	}
	if a.Refused {
		return Answer{}, rowErrorf("the decision model would not answer")
	}
	return a, nil
}

// filterQuestion is AI_FILTER's question to a decision model, whose state
// is the row's predicate.
var filterQuestion = Question{Name: "holds", Type: QuestionYesNo,
	Instructions: "Is the statement true, or the answer to the question yes?"}

// decideFilter answers AI_FILTER's rows with a decision model: TRUE where
// the probability that the predicate holds reaches the threshold. ok is
// false where no decision model serves it.
func (s *Statement) decideFilter(rows []row, opts []options, mode ErrorMode) ([]any, bool, error) {
	b, model, ok, err := s.decisionFor(opts, func(o options) string { return o.string("model") })
	if err != nil || !ok {
		return nil, ok, err
	}
	calls := make([]*call, len(rows))
	for i, r := range rows {
		input, err := s.inputText(r[0])
		if err != nil {
			return nil, true, err
		}
		calls[i] = s.decideCall("ai_filter", b, model, input, []Question{filterQuestion}, opts[i].bool("refresh"))
	}
	values, errs, err := s.do(calls, nil, 0)
	if err != nil {
		return nil, true, err
	}
	results := make([]result, len(rows))
	for i := range rows {
		if errs[i] != nil {
			results[i].err = errs[i]
			continue
		}
		a, err := readDecision(values[i], filterQuestion.Name)
		results[i] = result{value: a.Probability >= s.threshold(), err: err}
	}
	out, err := s.filterResults(results, mode)
	return out, true, err
}

// classifyQuestions are AI_CLASSIFY's questions to a decision model: one
// choice of the categories, or for several labels a yes-or-no question
// for each, named label_0, label_1, ....
func classifyQuestions(cats []category, cfg classifyConfig) []Question {
	var context strings.Builder
	if cfg.Task != "" {
		context.WriteString(" The task: " + cfg.Task)
	}
	for _, e := range cfg.Examples {
		labels, _ := json.Marshal(e.Labels)
		fmt.Fprintf(&context, " Example: %q has labels %s.", e.Input, labels)
	}
	if !cfg.Multi {
		q := Question{Name: "category", Type: QuestionChoice, Instructions: "Which category fits the text best?" + context.String()}
		for _, c := range cats {
			q.Choices = append(q.Choices, Choice{Value: c.Label, Description: c.Description})
		}
		return []Question{q}
	}
	qs := make([]Question, len(cats))
	for k, c := range cats {
		ins := fmt.Sprintf("Does the category %q apply to the text?", c.Label)
		if c.Description != "" {
			ins += " It means: " + c.Description + "."
		}
		qs[k] = Question{Name: "label_" + strconv.Itoa(k), Type: QuestionYesNo, Instructions: ins + context.String()}
	}
	return qs
}

// decideClassify answers AI_CLASSIFY's rows with a decision model: the
// likeliest category, or every category whose probability reaches the
// threshold. ok is false where no decision model serves it.
func (s *Statement) decideClassify(rows []row, opts []options, mode ErrorMode) ([]any, bool, error) {
	b, model, ok, err := s.decisionFor(opts, func(o options) string {
		m, _ := o.object("config")["model"].(string)
		return m
	})
	if err != nil || !ok {
		return nil, ok, err
	}
	calls := make([]*call, len(rows))
	type shape struct {
		cats   []category
		multi  bool
		legacy bool
	}
	shapes := make([]shape, len(rows))
	for i, r := range rows {
		cats, err := parseCategories(r[1])
		if err != nil {
			return nil, true, s.fail(err)
		}
		cfg, err := parseClassifyConfig(opts[i])
		if err != nil {
			return nil, true, s.fail(err)
		}
		input, err := s.inputText(r[0])
		if err != nil {
			return nil, true, err
		}
		shapes[i] = shape{cats, cfg.Multi, opts[i].bool("label")}
		calls[i] = s.decideCall("ai_classify", b, model, input, classifyQuestions(cats, cfg), opts[i].bool("refresh"))
	}
	values, errs, err := s.do(calls, nil, 0)
	if err != nil {
		return nil, true, err
	}
	results := make([]result, len(rows))
	for i, sh := range shapes {
		if errs[i] != nil {
			results[i].err = errs[i]
			continue
		}
		var chosen []string
		if !sh.multi {
			a, err := readDecision(values[i], "category")
			if err == nil && !slices.ContainsFunc(sh.cats, func(c category) bool { return c.Label == a.Choice }) {
				err = rowErrorf("the decision model chose none of the categories")
			}
			if err != nil {
				results[i].err = err
				continue
			}
			chosen = []string{a.Choice}
		} else {
			for k, c := range sh.cats {
				a, err := readDecision(values[i], "label_"+strconv.Itoa(k))
				if err != nil {
					results[i].err = err
					break
				}
				if a.Probability >= s.threshold() {
					chosen = append(chosen, c.Label)
				}
			}
			if results[i].err != nil {
				continue
			}
		}
		results[i].value = classifyResult(chosen, sh.legacy)
	}
	out, err := s.finish(results, mode)
	return out, true, err
}

// Decide runs AI_DECIDE for rows of (state, questions, options), with the
// decision model of the integration that has one, and returns what
// Databricks' ai_decide does: {"response": {"answers": {...}},
// "metadata": {"version": "1.0"}, "error_message": null}. A row the model
// could not answer gets its error_message, and a null response.
func (s *Statement) Decide(rows []row) ([]any, error) {
	results := make([]any, len(rows))
	var calls []*call
	var callRow []int
	var b *binding
	var model string
	for i, r := range rows {
		o, err := parseOptions(r[2])
		if err != nil {
			return nil, s.fail(err)
		}
		if b == nil {
			var ok bool
			var err error
			b, model, ok, err = s.decisionBinding()
			if err != nil {
				return nil, s.fail(err)
			}
			if !ok {
				return nil, s.fail(fmt.Errorf("%w: AI_DECIDE needs a decision model, and no AI integration %s may use has one; "+
					"an admin sets one with ALTER AI INTEGRATION name SET DECISION_MODEL = 'model'", ErrModel, s.opts.User))
			}
		}
		qs, err := parseQuestions(r[1])
		if err != nil {
			return nil, s.fail(err)
		}
		calls = append(calls, s.decideCall("ai_decide", b, model, stateText(r[0]), qs, o.bool("refresh")))
		callRow = append(callRow, i)
	}
	values, errs, err := s.do(calls, nil, 0)
	if err != nil {
		return nil, err
	}
	meta := map[string]any{"version": "1.0"}
	for k, i := range callRow {
		if errs[k] != nil {
			if errs[k] == errDryRun {
				continue
			}
			s.rowError(errs[k])
			results[i] = jsonText(map[string]any{"response": nil, "metadata": meta, "error_message": errs[k].Error()})
			continue
		}
		var d decision
		if err := json.Unmarshal(values[k], &d); err != nil {
			results[i] = jsonText(map[string]any{"response": nil, "metadata": meta, "error_message": "the cached decision cannot be read"})
			continue
		}
		answers := map[string]any{}
		for name, a := range d.Answers {
			answers[name] = answerObject(a, d.Levels[name], "probability")
		}
		results[i] = jsonText(map[string]any{"response": map[string]any{"answers": answers}, "metadata": meta, "error_message": nil})
	}
	return results, nil
}
