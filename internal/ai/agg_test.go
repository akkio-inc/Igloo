package ai

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

var intermediate = regexp.MustCompile(`intermediate: (\d+)`)

// aggFake answers AI_AGG's requests by counting: over records, how many
// the chunk holds; over intermediate results, their sum.
func aggFake(t *testing.T) *fake {
	return newFake(t, func(r fakeRequest) (int, any) {
		sys, user := systemText(r), userText(r)
		if strings.Contains(user, "refuse") {
			return 400, map[string]any{"error": map[string]any{"message": "refused"}}
		}
		n := 0
		if strings.HasPrefix(sys, "The user gives records") || strings.HasPrefix(sys, "The user gives some of the records") {
			n = strings.Count(user, aggSeparator) + 1
		} else {
			for _, m := range intermediate.FindAllStringSubmatch(user, -1) {
				k, _ := strconv.Atoi(m[1])
				n += k
			}
		}
		if strings.Contains(sys, "reply with the answer alone") {
			return 200, openAIAnswer(fmt.Sprintf("final: %d (%s)", n, sys[strings.Index(sys, "The task: ")+10:]))
		}
		return 200, openAIAnswer(fmt.Sprintf("intermediate: %d", n))
	})
}

func values(n int, size int, prefix string) []any {
	out := make([]any, n)
	for i := range out {
		v := fmt.Sprintf("%s%04d ", prefix, i)
		out[i] = v + strings.Repeat("x", size-len(v))
	}
	return out
}

func TestAgg(t *testing.T) {
	t.Parallel()
	f := aggFake(t)
	rt := testRuntime(t, f)

	// A small group: one request. A group of NULLs: NULL, with none.
	s := begin(t, rt)
	out, err := s.Agg([]row{{[]any{"b", nil, "a"}, "Count them", "{}"}, {[]any{nil}, "Count them", "{}"}})
	if err != nil {
		t.Fatal(err)
	}
	if out[0] != "final: 2 (Count them)" || out[1] != nil || f.calls.Load() != 1 {
		t.Errorf("small groups %v (%d requests)", out, f.calls.Load())
	}
	if u := userText(f.last()); u != "a"+aggSeparator+"b" {
		t.Errorf("values sent %q", u)
	}

	// 200 values of 1,000 bytes: four chunks, then their intermediate
	// results in one request.
	big := values(200, 1000, "review ")
	before := f.calls.Load()
	out, err = begin(t, rt).Agg([]row{{big, "Count them", "{}"}})
	if err != nil {
		t.Fatal(err)
	}
	if out[0] != "final: 200 (Count them)" || f.calls.Load()-before != 5 {
		t.Errorf("a large group: %v (%d requests)", out, f.calls.Load()-before)
	}

	// The same group with one more value: its chunk and the combining
	// request are asked again; the cache answers the other chunks.
	before = f.calls.Load()
	more := append(append([]any{}, big...), "zzz the last review")
	s = begin(t, rt)
	out, err = s.Agg([]row{{more, "Count them", "{}"}})
	if err != nil {
		t.Fatal(err)
	}
	if out[0] != "final: 201 (Count them)" || f.calls.Load()-before != 2 {
		t.Errorf("a grown group: %v (%d requests)", out, f.calls.Load()-before)
	}
	if sp := s.End(); sp.CacheHits != 3 {
		t.Errorf("spend %+v", sp)
	}

	// AI_SUMMARIZE_AGG's task, and errors failing the statement.
	out, err = begin(t, rt).Agg([]row{{[]any{"a"}, "", `{"summarize": true}`}})
	if err != nil || !strings.HasPrefix(out[0].(string), "final: 1 (Summarize the records") {
		t.Errorf("summarize %v %v", out, err)
	}
	if _, err := begin(t, rt).Agg([]row{{[]any{"refuse"}, "Count them", "{}"}}); err == nil {
		t.Error("AI_AGG's error did not fail the statement")
	}
	if _, err := begin(t, rt).Agg([]row{{[]any{"a"}, " ", "{}"}}); err == nil {
		t.Error("AI_AGG without a task")
	}
}

func TestAggChunks(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("é", 50000) // 100,000 bytes
	cs := aggChunks([]string{"a", long, "b"}, 16000)
	if len(cs) != 3 {
		t.Fatalf("%d chunks", len(cs))
	}
	if strings.Join(cs, "") != "a"+aggSeparator+long+aggSeparator+"b" && strings.ReplaceAll(strings.Join(cs, ""), aggSeparator, "") != "a"+long+"b" {
		t.Errorf("chunks lose text")
	}
	for _, c := range cs {
		if estimateTokens(c) > 16000 {
			t.Errorf("a chunk of %d tokens", estimateTokens(c))
		}
	}
}
