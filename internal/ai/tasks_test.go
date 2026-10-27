package ai

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync/atomic"
	"testing"
)

// schemaOf is the JSON schema an OpenAI-shaped request asked its answer
// to have.
func schemaOf(r fakeRequest) map[string]any {
	rf, _ := r.Body["response_format"].(map[string]any)
	js, _ := rf["json_schema"].(map[string]any)
	s, _ := js["schema"].(map[string]any)
	return s
}

// tasksFake answers the task functions' requests as a model might.
func tasksFake(t *testing.T) *fake {
	return newFake(t, func(r fakeRequest) (int, any) {
		sys, user := systemText(r), userText(r)
		switch {
		case strings.HasPrefix(sys, "Classify"):
			switch {
			case strings.Contains(user, "flight"):
				return 200, openAIAnswer(`{"labels": ["travel"]}`)
			case strings.Contains(user, "both"):
				return 200, openAIAnswer(`{"labels": ["travel", "cooking", "travel"]}`)
			case strings.Contains(user, "nonsense"):
				return 200, openAIAnswer(`{"labels": ["sports"]}`)
			}
			return 200, openAIAnswer("```json\n{\"labels\": [\"cooking\"]}\n```")
		case strings.HasPrefix(sys, "Decide whether"):
			return 200, openAIAnswer(`{"value": ` + map[bool]string{true: "true", false: "false"}[strings.Contains(user, "refund")] + `}`)
		case strings.HasPrefix(sys, "Rate the sentiment"):
			return 200, openAIAnswer(`{"categories": [{"name": "price", "sentiment": "Negative"}, {"name": "overall", "sentiment": "mixed"}]}`)
		case strings.HasPrefix(sys, "Score the sentiment"):
			return 200, openAIAnswer(`{"score": -3}`)
		case strings.HasPrefix(sys, "Extract"):
			if strings.Contains(user, "garbled") {
				return 200, openAIAnswer("I cannot answer that")
			}
			return 200, openAIAnswer(`{"city": "Lisbon", "date": null}`)
		case strings.HasPrefix(sys, "Answer the question"):
			return 200, openAIAnswer(`{"answer": "Lisbon"}`)
		case strings.HasPrefix(sys, "Summarize"):
			return 200, openAIAnswer("  A short summary.\n")
		case strings.HasPrefix(sys, "Translate"):
			return 200, openAIAnswer("Hallo Welt")
		case strings.HasPrefix(sys, "Find the personal information"):
			if strings.Contains(user, "unreadable") {
				return 200, openAIAnswer("no")
			}
			return 200, openAIAnswer(`{"spans": [{"category": "NAME", "text": "Zoë Park"}, {"category": "EMAIL", "text": "zoe@example.com"},` +
				` {"category": "NAME", "text": "nobody"}, {"category": "PASSPORT", "text": "zoe"}]}`)
		}
		return 400, map[string]any{"error": map[string]any{"message": "unexpected request"}}
	})
}

func TestClassify(t *testing.T) {
	t.Parallel()
	f := tasksFake(t)
	rt := testRuntime(t, f)
	s := begin(t, rt)
	cats := `["travel", {"label": "cooking", "description": "recipes and kitchens"}]`
	out, err := s.Classify([]row{
		{"my flight was late", cats, "{}"},
		{"a pie recipe", cats, "{}"},
		{"nonsense", cats, "{}"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []any{`{"labels":["travel"]}`, `{"labels":["cooking"]}`, nil}
	for i := range want {
		if out[i] != want[i] {
			t.Errorf("row %d: %v, want %v", i, out[i], want[i])
		}
	}
	req := f.all()[0]
	if !strings.Contains(systemText(req), `"cooking": recipes and kitchens`) {
		t.Errorf("system prompt %q", systemText(req))
	}
	enum := schemaOf(req)["properties"].(map[string]any)["labels"].(map[string]any)["items"].(map[string]any)["enum"]
	if b, _ := json.Marshal(enum); string(b) != `["travel","cooking"]` {
		t.Errorf("schema enum %s", b)
	}
	if sp := s.End(); sp.RowErrors != 1 || !strings.Contains(sp.FirstError, "none of the categories") {
		t.Errorf("spend %+v", sp)
	}

	// Several labels, and CLASSIFY_TEXT's shape.
	s = begin(t, rt)
	out, err = s.Classify([]row{{"both", cats, `{"config": {"output_mode": "multi", "task_description": "topics"}}`}})
	if err != nil || out[0] != `{"labels":["travel","cooking"]}` {
		t.Errorf("multi: %v %v", out, err)
	}
	out, err = s.Classify([]row{{"my flight was late", cats, `{"label": true, "on_error": "fail"}`}})
	if err != nil || out[0] != `{"label":"travel"}` {
		t.Errorf("legacy: %v %v", out, err)
	}
	if _, err := s.Classify([]row{{"nonsense", cats, `{"label": true, "on_error": "fail"}`}}); err == nil {
		t.Error("CLASSIFY_TEXT's error did not fail the statement")
	}

	for _, bad := range []string{`["only one"]`, `["a", "a"]`, `[""]`, `{"a": 1}`} {
		if _, err := begin(t, rt).Classify([]row{{"x", bad, "{}"}}); !errors.Is(err, ErrInvalid) {
			t.Errorf("categories %s: %v", bad, err)
		}
	}
	if _, err := begin(t, rt).Classify([]row{{"x", cats, `{"config": {"mode": "fast"}}`}}); !errors.Is(err, ErrInvalid) {
		t.Errorf("unknown config: %v", err)
	}
}

func TestFilter(t *testing.T) {
	t.Parallel()
	f := tasksFake(t)
	rt := testRuntime(t, f)
	s := begin(t, rt)
	out, err := s.Filter([]row{{"asks for a refund", "{}"}, {"says thanks", "{}"}, {"asks for a refund", "{}"}})
	if err != nil {
		t.Fatal(err)
	}
	if out[0] != true || out[1] != false || out[2] != true {
		t.Errorf("filter %v", out)
	}
	if n := f.calls.Load(); n != 2 {
		t.Errorf("%d requests for 2 distinct inputs", n)
	}
	if mt := f.last().Body["max_tokens"]; mt != float64(32) {
		t.Errorf("max_tokens %v", mt)
	}

	// A dry run lets the rows the cache cannot answer through, and
	// counts their calls; the cache answers the rest.
	d := begin(t, rt, func(o *StatementOptions) { o.DryRun = true })
	out, err = d.Filter([]row{{"says thanks", "{}"}, {"never seen", "{}"}})
	if err != nil {
		t.Fatal(err)
	}
	if out[0] != false || out[1] != true {
		t.Errorf("dry run %v", out)
	}
	if sp := d.End(); sp.Calls != 1 || sp.CacheHits != 1 || f.calls.Load() != 2 {
		t.Errorf("dry run spend %+v, %d requests", sp, f.calls.Load())
	}

	out, err = begin(t, rt).Filter([]row{{"asks for a refund", `{"on_error": "details"}`}})
	if err != nil || out[0] != `{"error":null,"value":true}` {
		t.Errorf("details %v %v", out, err)
	}
}

func TestSentiment(t *testing.T) {
	t.Parallel()
	f := tasksFake(t)
	s := begin(t, testRuntime(t, f))
	out, err := s.Sentiment([]row{{"good food, steep prices", `["price", "service"]`, "{}"}})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"categories":[{"name":"overall","sentiment":"mixed"},{"name":"price","sentiment":"negative"},` +
		`{"name":"service","sentiment":"unknown"}]}`
	if out[0] != want {
		t.Errorf("sentiment %v", out[0])
	}
	out, err = s.SentimentScore([]row{{"awful", `{"on_error": "fail"}`}})
	if err != nil || out[0] != -1.0 {
		t.Errorf("score %v %v", out, err)
	}
	if _, err := s.Sentiment([]row{{"x", `["` + strings.Repeat("a", 31) + `"]`, "{}"}}); !errors.Is(err, ErrInvalid) {
		t.Errorf("long category: %v", err)
	}
}

func TestExtract(t *testing.T) {
	t.Parallel()
	f := tasksFake(t)
	rt := testRuntime(t, f)
	s := begin(t, rt)
	out, err := s.Extract([]row{
		{"I fly to Lisbon", `{"city": "Which city?", "date": "When?"}`, "{}"},
		{"garbled", `[["city", "Which city?"]]`, "{}"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if out[0] != `{"error":null,"response":{"city":"Lisbon","date":null}}` {
		t.Errorf("extract %v", out[0])
	}
	if s, _ := out[1].(string); !strings.HasPrefix(s, `{"error":"the model's answer is not the JSON asked for`) ||
		!strings.HasSuffix(s, `"response":null}`) {
		t.Errorf("extract error %v", out[1])
	}
	var props map[string]any
	for _, r := range f.all() {
		if userText(r) == "I fly to Lisbon" {
			props, _ = schemaOf(r)["properties"].(map[string]any)
		}
	}
	if len(props) != 2 || props["date"] == nil {
		t.Errorf("schema %v", props)
	}
	if _, err := begin(t, rt).Extract([]row{{"x", `{"schema": {"type": "array"}}`, "{}"}}); !errors.Is(err, ErrInvalid) {
		t.Errorf("array schema: %v", err)
	}

	out, err = s.ExtractAnswer([]row{{"I fly to Lisbon", "Which city?", `{"on_error": "fail"}`}})
	if err != nil || out[0] != `[{"answer":"Lisbon","score":null}]` {
		t.Errorf("extract answer %v %v", out, err)
	}
	if !strings.HasSuffix(systemText(f.last()), "The question: Which city?") {
		t.Errorf("system %q", systemText(f.last()))
	}
}

func TestSummarizeAndTranslate(t *testing.T) {
	t.Parallel()
	f := tasksFake(t)
	s := begin(t, testRuntime(t, f))
	short := "Short enough to stay as it is."
	long := strings.Repeat("A long review of a stay. ", 10)
	out, err := s.Summarize([]row{{short, "{}"}, {long, "{}"}})
	if err != nil || out[0] != short || out[1] != "A short summary." || f.calls.Load() != 1 {
		t.Errorf("summarize %v %v (%d requests)", out, err, f.calls.Load())
	}
	out, err = s.Translate([]row{{"Hello world", "", "de", "{}"}, {"Hello world", "en", "DE", "{}"}})
	if err != nil || out[0] != "Hallo Welt" || out[1] != "Hallo Welt" {
		t.Errorf("translate %v %v", out, err)
	}
	var froms []string
	for _, r := range f.all() {
		if sys := systemText(r); strings.HasPrefix(sys, "Translate") {
			froms = append(froms, sys)
		}
	}
	if len(froms) != 2 || !strings.Contains(strings.Join(froms, "|"), "from English into German") {
		t.Errorf("system prompts %q", froms)
	}
	if _, err := s.Translate([]row{{"x", "", "klingon", "{}"}}); !errors.Is(err, ErrInvalid) {
		t.Errorf("unknown language: %v", err)
	}
}

func TestRedact(t *testing.T) {
	t.Parallel()
	f := tasksFake(t)
	rt := testRuntime(t, f)
	s := begin(t, rt)
	text := "Écrivez à Zoë Park: zoe@example.com, merci Zoë Park"
	out, err := s.Redact([]row{{text, "[]", "{}"}}, false)
	if err != nil {
		t.Fatal(err)
	}
	// "nobody" is not in the text, and the PASSPORT is in the email.
	if want := "Écrivez à [NAME]: [EMAIL], merci Zoë Park"; out[0] != want {
		t.Errorf("redact %q, want %q", out[0], want)
	}
	out, err = s.Redact([]row{{text, `["name", "email", "passport"]`, "{}"}}, true)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"spans":[{"category":"NAME","start":10,"end":18,"text":"Zoë Park"},` +
		`{"category":"EMAIL","start":20,"end":35,"text":"zoe@example.com"}]}`; out[0] != want {
		t.Errorf("detect %v", out[0])
	}
	// Only the categories asked for.
	out, err = s.Redact([]row{{text, `["EMAIL"]`, "{}"}}, false)
	if err != nil || out[0] != "Écrivez à Zoë Park: [EMAIL], merci Zoë Park" {
		t.Errorf("email only %v %v", out, err)
	}
	if !strings.Contains(systemText(f.last()), "- EMAIL:") || strings.Contains(systemText(f.last()), "- NAME:") {
		t.Errorf("system %q", systemText(f.last()))
	}
	// An answer it cannot read fails the statement, unless asked for its
	// details.
	if _, err := begin(t, rt).Redact([]row{{"unreadable", "[]", "{}"}}, false); err == nil {
		t.Error("AI_REDACT's error did not fail the statement")
	}
	out, err = begin(t, rt).Redact([]row{{"unreadable", "[]", `{"on_error": "details"}`}}, false)
	if err != nil || !strings.Contains(out[0].(string), `"value":null`) {
		t.Errorf("details %v %v", out, err)
	}
	if _, err := s.Redact([]row{{"x", `["SHOE_SIZE"]`, "{}"}}, false); !errors.Is(err, ErrInvalid) {
		t.Errorf("unknown category: %v", err)
	}
}

// embedFake answers embeddings requests with a vector per text: its
// length and its count of the letter a.
func embedFake(t *testing.T, requests *atomic.Int64) *fake {
	return newFake(t, func(r fakeRequest) (int, any) {
		if !strings.HasSuffix(r.Path, "/embeddings") {
			return 404, map[string]any{}
		}
		requests.Add(1)
		inputs, _ := r.Body["input"].([]any)
		var data []any
		for i, in := range inputs {
			s, _ := in.(string)
			data = append(data, map[string]any{"index": i, "embedding": []float64{float64(len(s)), float64(strings.Count(s, "a"))}})
		}
		return 200, map[string]any{"data": data, "usage": map[string]any{"prompt_tokens": 3 * len(inputs)}}
	})
}

func TestEmbedAndSimilarity(t *testing.T) {
	t.Parallel()
	var requests atomic.Int64
	f := embedFake(t, &requests)
	rt := testRuntime(t, f, func(in *Integration) {
		in.Models["e5-base-v2"] = "fake-embed"
		in.EmbeddingModel = "fake-embed"
		in.Prices["fake-embed"] = Price{Input: 0.1}
	})
	s := begin(t, rt)
	var rows []row
	for _, x := range []string{"aa", "bab", "aa", "c", "bab"} {
		rows = append(rows, row{"e5-base-v2", x, "{}"})
	}
	out, err := s.Embed(rows)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := out[1].([]float32); !ok || len(v) != 2 || v[0] != 3 || v[1] != 1 {
		t.Errorf("embedding %v", out[1])
	}
	// Three distinct texts, in one request.
	if n := requests.Load(); n != 1 {
		t.Errorf("%d requests", n)
	}
	if b := f.last().Body["input"].([]any); len(b) != 3 {
		t.Errorf("inputs %v", b)
	}
	if sp := s.End(); sp.Calls != 1 || sp.InputTokens != 9 {
		t.Errorf("spend %+v", sp)
	}

	// AI_SIMILARITY with the EMBEDDING_MODEL reads the vectors AI_EMBED
	// kept.
	s = begin(t, rt)
	out, err = s.Similarity([]row{{"aa", "bab", "{}"}, {"aa", "aa", "{}"}})
	if err != nil {
		t.Fatal(err)
	}
	want := (2*3 + 2*1) / (math.Sqrt(8) * math.Sqrt(10))
	if v, _ := out[0].(float64); math.Abs(v-want) > 1e-9 || math.Abs(out[1].(float64)-1) > 1e-9 {
		t.Errorf("similarity %v, want %v and 1", out, want)
	}
	if sp := s.End(); sp.Calls != 0 || sp.CacheHits != 2 || requests.Load() != 1 {
		t.Errorf("spend %+v, %d requests", sp, requests.Load())
	}

	// EMBED_TEXT_768 wants 768 dimensions.
	if _, err := begin(t, rt).Embed([]row{{"e5-base-v2", "aa", `{"dimensions": 768, "on_error": "fail"}`}}); err == nil ||
		!strings.Contains(err.Error(), "2 dimensions, not 768") {
		t.Errorf("dimensions: %v", err)
	}
	// An unmapped model fails with how to map it.
	if _, err := begin(t, rt).Embed([]row{{"snowflake-arctic-embed-l-v2.0", "aa", "{}"}}); !errors.Is(err, ErrModel) {
		t.Errorf("unmapped model: %v", err)
	}
}

// TestEmbedFromAnthropic: a provider without embeddings fails the
// statement.
func TestEmbedFromAnthropic(t *testing.T) {
	t.Parallel()
	f := newFake(t, echo)
	rt := testRuntime(t, f, func(in *Integration) {
		in.Provider = ProviderAnthropic
		in.BaseURL = f.srv.URL
		in.EmbeddingModel = "fake-model"
	})
	if _, err := begin(t, rt).Similarity([]row{{"a", "b", "{}"}}); !errors.Is(err, ErrUnsupported) {
		t.Errorf("Anthropic embeddings: %v", err)
	}
}

func TestLocate(t *testing.T) {
	t.Parallel()
	cats := []any{"NAME"}
	got := locate("Ann and Ann", []span{{Category: "NAME", Text: "Ann"}, {Category: "NAME", Text: "Ann"}, {Category: "NAME", Text: "Ann"}}, cats)
	if len(got) != 2 || got[0].Start != 0 || got[1].Start != 8 {
		t.Errorf("locate %+v", got)
	}
	if r := redacted("Ann and Ann", got); r != "[NAME] and [NAME]" {
		t.Errorf("redacted %q", r)
	}
}
