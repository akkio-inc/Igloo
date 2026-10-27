package ai

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"
)

// AI_AGG and AI_SUMMARIZE_AGG: a task over a group's values, however many
// there are. The translation collects a group's values into a list, and
// ai_agg answers the task over it by map and reduce: the values, sorted so
// that the same group makes the same requests, go in chunks of at most
// aggChunkTokens; each chunk's intermediate result is asked for, and the
// intermediate results combined, a round at a time, until one request
// holds them all and gives the answer. Each request is cached by its chunk,
// so a group that gained rows asks again only for the chunks that changed.

const aggVersion = "agg/1"

const (
	// aggChunkTokens is the most a request's chunk holds, in estimated
	// tokens.
	aggChunkTokens = 16000
	// aggRounds is the most rounds of combining a group may take.
	aggRounds = 8
	// aggSeparator separates values, and intermediate results, in a chunk.
	aggSeparator = "\n---\n"
)

const summarizeAggTask = "Summarize the records: what they say, in a few sentences."

// aggPrompt is the system prompt of a request of AI_AGG: over values
// (partial false) or intermediate results, giving the answer (final) or an
// intermediate result.
func aggPrompt(task string, partial, final bool) string {
	var b strings.Builder
	switch {
	case !partial && final:
		b.WriteString("The user gives records from a table, separated by lines of ---. Do the task below over all of them, " +
			"and reply with the answer alone.")
	case !partial:
		b.WriteString("The user gives some of the records of a table, separated by lines of ---; other steps see the rest. " +
			"Do the task below over these records as far as they allow, and reply with an intermediate result alone, which " +
			"another step will combine with the others' into the answer: keep every fact, count and example the answer may need.")
	case final:
		b.WriteString("The user gives intermediate results, separated by lines of ---, each from part of a table's records. " +
			"Combine them into the answer to the task below, and reply with the answer alone.")
	default:
		b.WriteString("The user gives some intermediate results, separated by lines of ---, each from part of a table's " +
			"records. Combine them into one intermediate result, alone, which another step will combine with others' into the " +
			"answer to the task below: keep every fact, count and example the answer may need.")
	}
	b.WriteString("\nThe task: " + task)
	return b.String()
}

// aggChunks packs items, in order, into chunks of at most budget
// estimated tokens; an item larger than that is cut into pieces.
func aggChunks(items []string, budget int64) []string {
	var chunks []string
	var cur []string
	var size int64
	flush := func() {
		if len(cur) > 0 {
			chunks = append(chunks, strings.Join(cur, aggSeparator))
			cur, size = nil, 0
		}
	}
	for _, it := range items {
		for _, piece := range pieces(it, int(budget*4)) {
			n := estimateTokens(piece) + estimateTokens(aggSeparator)
			if size+n > budget {
				flush()
			}
			cur = append(cur, piece)
			size += n
		}
	}
	flush()
	return chunks
}

// pieces cuts s into pieces of at most max bytes, at characters' bounds.
func pieces(s string, max int) []string {
	var out []string
	for len(s) > max {
		cut := max
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		out = append(out, s[:cut])
		s = s[cut:]
	}
	return append(out, s)
}

// aggValues reads a group's values, a list, as text, without NULLs,
// sorted.
func aggValues(v any) []string {
	list, _ := v.([]any)
	out := make([]string, 0, len(list))
	for _, x := range list {
		if x == nil {
			continue
		}
		t, _ := text(x)
		out = append(out, t)
	}
	slices.Sort(out)
	return out
}

// Agg runs AI_AGG, and with the option summarize AI_SUMMARIZE_AGG, for rows
// of (values, task, options), each a group's: the answer to the task over
// the values, or NULL for a group of none. As Snowflake's, it fails the
// statement on an error. A dry run counts the first round of requests.
func (s *Statement) Agg(rows []row) ([]any, error) {
	opts, mode, err := s.taskRows(rows, 2)
	if err != nil {
		return nil, err
	}
	if len(opts) > 0 && opts[0].mode("") == "" {
		mode = ErrorsFail
	}
	type group struct {
		task    string
		items   []string
		partial bool
		// calls are the group's requests of this round, from first.
		first, calls int
		final        bool
	}
	results := make([]result, len(rows))
	groups := make([]*group, len(rows))
	for i, r := range rows {
		task, _ := r[1].(string)
		if opts[i].bool("summarize") {
			task = summarizeAggTask
		}
		if strings.TrimSpace(task) == "" {
			return nil, s.fail(fmt.Errorf("%w: AI_AGG needs the task to do over the values", ErrInvalid))
		}
		if values := aggValues(r[0]); len(values) > 0 {
			groups[i] = &group{task: task, items: values}
		}
	}
	for round := 0; ; round++ {
		var calls []*call
		var chunks [][]string
		for i, g := range groups {
			if g == nil || results[i].err != nil || results[i].value != nil {
				continue
			}
			if round == aggRounds {
				return nil, s.fail(fmt.Errorf("AI_AGG could not combine a group's intermediate results in %d rounds", aggRounds))
			}
			cs := aggChunks(g.items, aggChunkTokens)
			g.final, g.first, g.calls = len(cs) == 1, len(calls), len(cs)
			maxTokens := 1024
			if g.final {
				maxTokens = 2048
			}
			for _, chunk := range cs {
				c, err := s.taskCall("ai_agg", aggVersion, opts[i].string("model"), aggPrompt(g.task, g.partial, g.final), nil, chunk,
					maxTokens, opts[i].bool("refresh"))
				if err != nil {
					return nil, s.fail(err)
				}
				calls = append(calls, c)
			}
			chunks = append(chunks, cs)
		}
		if len(calls) == 0 {
			break
		}
		comps, errs, err := s.completions(calls)
		if err != nil {
			return nil, err
		}
		for i, g := range groups {
			if g == nil || results[i].err != nil || results[i].value != nil {
				continue
			}
			var partials []string
			for k := g.first; k < g.first+g.calls; k++ {
				if errs[k] != nil {
					results[i].err = errs[k]
					break
				}
				partials = append(partials, strings.TrimSpace(comps[k].Text))
			}
			switch {
			case results[i].err != nil:
			case g.final:
				results[i].value = partials[0]
			default:
				g.items, g.partial = partials, true
			}
		}
	}
	return s.finish(results, mode)
}
