package ai

import (
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"strings"
	"testing"
)

var quotedLabel = regexp.MustCompile(`"([^"]+)"`)

// decisionsFake is a decision endpoint in the state-and-questions shape:
// a yes-or-no question about refunds holds with 0.7 where the state
// mentions one; a category's question with 0.9 where the state names the
// category; a choice picks the category the state names; and a state that
// says "refuse" is refused.
func decisionsFake(t *testing.T) *fake {
	return newFake(t, func(r fakeRequest) (int, any) {
		if !strings.HasSuffix(r.Path, "/decisions") {
			return 404, map[string]any{"error": map[string]any{"message": "decisions only"}}
		}
		state, _ := r.Body["state"].(string)
		qs, _ := r.Body["questions"].(map[string]any)
		answers := map[string]any{}
		for name, q := range qs {
			qm := q.(map[string]any)
			ins, _ := qm["instructions"].(string)
			switch {
			case strings.Contains(state, "refuse"):
				answers[name] = map[string]any{"type": "refusal"}
			case qm["type"] == "choice":
				criteria := qm["criteria"].(map[string]any)
				var labels []string
				for l := range criteria {
					labels = append(labels, l)
				}
				slices.Sort(labels)
				pick := labels[0]
				probs := map[string]any{}
				for _, l := range labels {
					if strings.Contains(state, l) {
						pick = l
					}
				}
				for _, l := range labels {
					probs[l] = map[bool]float64{true: 0.8, false: 0.2 / float64(len(labels)-1)}[l == pick]
				}
				answers[name] = map[string]any{"type": "choice", "probabilities": probs}
			case strings.HasPrefix(name, "label_"):
				p := 0.05
				if m := quotedLabel.FindStringSubmatch(ins); m != nil && strings.Contains(state, m[1]) {
					p = 0.9
				}
				answers[name] = map[string]any{"type": "noul", "noul": p}
			default:
				p := 0.3
				if strings.Contains(state, "refund") {
					p = 0.7
				}
				answers[name] = map[string]any{"type": "noul", "noul": p}
			}
		}
		return 200, map[string]any{"answers": answers, "usage": map[string]any{"input_tokens": 20, "output_tokens": 1}}
	})
}

// decisionsRuntime is a runtime whose one integration is the decision
// endpoint f, with no generative model.
func decisionsRuntime(t *testing.T, f *fake) *Runtime {
	return testRuntime(t, f, func(in *Integration) {
		in.Provider = ProviderDecisions
		in.BaseURL = f.srv.URL + "/v1"
		in.DefaultModel = ""
		in.DecisionModel = "jev-1"
		in.Models = map[string]string{"snowflake-decision": "jev-1"}
		in.Prices = map[string]Price{"jev-1": {Input: 0.5}}
	})
}

// TestFilterWithDecisionModel: AI_FILTER asks a decision model whether
// the predicate holds, and keeps rows from the session's threshold.
func TestFilterWithDecisionModel(t *testing.T) {
	t.Parallel()
	f := decisionsFake(t)
	rt := decisionsRuntime(t, f)
	rows := []row{{"please refund me", "{}"}, {"great service", "{}"}, {"please refund me", "{}"}, {"refuse this", "{}"}}
	s := begin(t, rt)
	out, err := s.Filter(rows)
	if err != nil {
		t.Fatal(err)
	}
	if out[0] != true || out[1] != false || out[2] != true || out[3] != nil {
		t.Errorf("filter %v", out)
	}
	if n := f.calls.Load(); n != 3 {
		t.Errorf("%d requests for 3 distinct inputs", n)
	}
	req := f.last()
	q := req.Body["questions"].(map[string]any)["holds"].(map[string]any)
	if req.Body["model"] != "jev-1" || q["type"] != "noul" {
		t.Errorf("request %+v", req.Body)
	}
	if sp := s.End(); sp.RowErrors != 1 || !strings.Contains(sp.FirstError, "would not answer") || sp.CostUSD == 0 {
		t.Errorf("spend %+v", sp)
	}

	// A threshold of 0.8 keeps none; the cache answers.
	s = begin(t, rt, func(o *StatementOptions) { o.Threshold = 0.8 })
	out, err = s.Filter(rows[:2])
	if err != nil || out[0] != false || out[1] != false || f.calls.Load() != 3 {
		t.Errorf("threshold 0.8: %v %v (%d requests)", out, err, f.calls.Load())
	}

	// A model named in the options asks it, not the decision model.
	if _, err := begin(t, rt).Filter([]row{{"x", `{"model": "claude-4-sonnet"}`}}); !errors.Is(err, ErrModel) {
		t.Errorf("named model: %v", err)
	}
}

// TestClassifyWithDecisionModel: AI_CLASSIFY asks a choice question, or
// a yes-or-no question for each category with output_mode multi.
func TestClassifyWithDecisionModel(t *testing.T) {
	t.Parallel()
	f := decisionsFake(t)
	rt := decisionsRuntime(t, f)
	s := begin(t, rt)
	cats := `["billing", {"label": "shipping", "description": "parcels and delivery"}, "other"]`
	out, err := s.Classify([]row{{"a billing question", cats, "{}"}, {"shipping and billing", cats, `{"config": {"output_mode": "multi"}}`}})
	if err != nil {
		t.Fatal(err)
	}
	if out[0] != `{"labels":["billing"]}` || out[1] != `{"labels":["billing","shipping"]}` {
		t.Errorf("classify %v", out)
	}
	var choice, multi map[string]any
	for _, r := range f.all() {
		qs := r.Body["questions"].(map[string]any)
		if c, ok := qs["category"].(map[string]any); ok {
			choice = c
		} else {
			multi = qs
		}
	}
	if choice["type"] != "choice" || choice["criteria"].(map[string]any)["shipping"] != "parcels and delivery" {
		t.Errorf("choice question %v", choice)
	}
	if len(multi) != 3 || !strings.Contains(multi["label_1"].(map[string]any)["instructions"].(string), `"shipping"`) {
		t.Errorf("multi-label questions %v", multi)
	}
	out, err = s.Classify([]row{{"a billing question", cats, `{"label": true, "on_error": "fail"}`}})
	if err != nil || out[0] != `{"label":"billing"}` {
		t.Errorf("CLASSIFY_TEXT %v %v", out, err)
	}
}

// TestDecide: AI_DECIDE answers in Databricks' shape, and AI_COMPLETE with
// snowflake-decision in Snowflake's.
func TestDecide(t *testing.T) {
	t.Parallel()
	f := decisionsFake(t)
	rt := decisionsRuntime(t, f)
	s := begin(t, rt)
	questions := `{"refund": {"type": "noul", "instructions": "Does it ask for a refund?"},
		"team": {"type": "choice", "instructions": "Which team?", "criteria": {"billing": "", "shipping": ""}}}`
	out, err := s.Decide([]row{{"a billing refund", questions, "{}"}, {"refuse", questions, "{}"}})
	if err != nil {
		t.Fatal(err)
	}
	var d struct {
		Response struct {
			Answers map[string]map[string]any `json:"answers"`
		} `json:"response"`
		Metadata     map[string]any `json:"metadata"`
		ErrorMessage any            `json:"error_message"`
	}
	if err := json.Unmarshal([]byte(out[0].(string)), &d); err != nil {
		t.Fatal(err)
	}
	if d.Response.Answers["refund"]["probability"] != 0.7 || d.Response.Answers["team"]["choice"] != "billing" ||
		d.Metadata["version"] != "1.0" || d.ErrorMessage != nil {
		t.Errorf("AI_DECIDE %s", out[0])
	}
	if !strings.Contains(out[1].(string), `"answers":{"refund":{"type":"refusal"}`) {
		t.Errorf("refused %s", out[1])
	}

	prompt := `{"state": "please refund me", "questions": {"urgent": {"type": "noul", "instructions": "Is it urgent?"}}}`
	out, err = s.Complete([]row{{"snowflake-decision", prompt, "{}"}}, true)
	if err != nil || out[0] != `{"answers":{"urgent":{"noul":0.7,"type":"noul"}}}` {
		t.Errorf("snowflake-decision %v %v", out, err)
	}
	out, err = s.Complete([]row{{"snowflake-decision", "not the decision shape", "{}"}}, true)
	if err != nil || out[0] != nil {
		t.Errorf("a prompt not in the decision shape: %v %v", out, err)
	}

	// Without a decision model, AI_DECIDE fails with how to set one.
	_, err = begin(t, testRuntime(t, newFake(t, echo))).Decide([]row{{"x", questions, "{}"}})
	if !errors.Is(err, ErrModel) || !strings.Contains(err.Error(), "DECISION_MODEL") {
		t.Errorf("no decision model: %v", err)
	}
}

// TestFilterWithOpenAIDecisions: an OpenAI integration's DECISION_MODEL
// answers AI_FILTER through POST /decisions, as a predicate question.
func TestFilterWithOpenAIDecisions(t *testing.T) {
	t.Parallel()
	f := newFake(t, func(r fakeRequest) (int, any) {
		if r.Path != "/v1/decisions" {
			return 404, map[string]any{}
		}
		p := 0.2
		if strings.Contains(r.Body["input"].(string), "refund") {
			p = 0.95
		}
		return 200, map[string]any{"answers": []any{map[string]any{"type": "predicate", "name": "holds", "probability": p}},
			"usage": map[string]any{"input_tokens": 12}}
	})
	rt := testRuntime(t, f, func(in *Integration) { in.DecisionModel = "decide-mini" })
	out, err := begin(t, rt).Filter([]row{{"refund please", "{}"}, {"thanks", "{}"}})
	if err != nil || out[0] != true || out[1] != false {
		t.Errorf("filter %v %v", out, err)
	}
	q := f.last().Body["questions"].([]any)[0].(map[string]any)
	if q["type"] != "predicate" || q["name"] != "holds" || f.last().Body["model"] != "decide-mini" {
		t.Errorf("request %+v", f.last().Body)
	}
}
