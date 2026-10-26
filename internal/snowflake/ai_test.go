package snowflake

import (
	"strings"
	"testing"
)

// TestTranslateAI checks what Snowflake's AI functions become: Igloo's,
// with their arguments by position and their options as JSON.
func TestTranslateAI(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{`SELECT AI_COMPLETE('claude-4-sonnet', 'Say hi') AS a`,
			`SELECT ai_complete('claude-4-sonnet', 'Say hi', '{}') AS "A"`},
		{`SELECT AI_COMPLETE(model => 'm', prompt => review, model_parameters => {'temperature': 0.2, 'max_tokens': 10}) FROM reviews`,
			`SELECT ai_complete('m', review, CAST(json_object('model_parameters', to_json(map_from_entries(`},
		{`SELECT AI_COMPLETE(model => 'm', prompt => review, response_format => {'type': 'json', 'schema': {'type': 'object'}}, refresh => TRUE) AS a FROM reviews`,
			`SELECT ai_complete_object('m', review, CAST(json_object('response_format', to_json(`},
		{`SELECT AI_COMPLETE('m', review, return_error_details => TRUE) AS a FROM reviews`,
			`SELECT ai_complete_details('m', review, '{}') AS "A" FROM reviews`},
		{`SELECT AI_COMPLETE('m', review, return_error_details => TRUE):error::VARCHAR AS e FROM reviews`,
			`SELECT json_extract_string(ai_complete_details('m', review, '{}'), '$.error') AS "E" FROM reviews`},
		{`SELECT AI_COMPLETE(model => 'snowflake-decision', prompt => TO_JSON({'state': review, 'questions': {'urgent': {'type': 'noul'}}})) AS a FROM t`,
			`SELECT ai_complete_object('snowflake-decision', `},
		{`SELECT SNOWFLAKE.CORTEX.COMPLETE('m', 'Say hi') AS a`, `SELECT ai_complete('m', 'Say hi', CAST(json_object('on_error', 'fail') AS VARCHAR)) AS "A"`},
		{`SELECT snowflake.cortex.complete('m', [{'role': 'user', 'content': 'hi'}], {'temperature': 0.5}) AS a`,
			`SELECT CAST(ai_complete_object('m', json_array(to_json(`},
		{`SELECT AI_COMPLETE('m', PROMPT('Rate {0} from {1}', review, stars)) AS a FROM reviews`,
			`SELECT ai_complete('m', json_object('template', 'Rate {0} from {1}', 'args', json_array(CAST(review AS VARCHAR), CAST(stars AS VARCHAR))), '{}') AS "A" FROM reviews`},
		{`SELECT AI_COUNT_TOKENS('ai_complete', 'm', review) AS n, AI_COUNT_TOKENS('ai_sentiment', review) AS s FROM reviews`,
			`SELECT ai_count_tokens('ai_complete', 'm', review, '{}') AS "N", ai_count_tokens('ai_sentiment', '', review, '{}') AS "S" FROM reviews`},
	} {
		got := translate(t, c.in).SQL
		if !strings.HasPrefix(got, c.want) {
			t.Errorf("%s:\n got %s\nwant %s", c.in, got, c.want)
		}
	}
	for _, c := range []struct{ in, code, msg string }{
		{`SELECT AI_COMPLETE('m')`, CodeSyntax, "needs prompt"},
		{`SELECT AI_COMPLETE(model => 'm', prompt => 'x', nope => 1)`, CodeSyntax, "no argument nope"},
		{`SELECT AI_COUNT_TOKENS(fn, x) FROM t`, CodeUnsupported, "AI_COUNT_TOKENS"},
		{`SELECT AI_COUNT_TOKENS('ai_nothing', x) FROM t`, CodeInvalidValue, "cannot count"},
		{`SELECT SNOWFLAKE.CORTEX.NO_SUCH(x) FROM t`, CodeUnsupported, "SNOWFLAKE.CORTEX.NO_SUCH"},
	} {
		_, err := Translate(c.in, Options{})
		e, ok := err.(*Error)
		if !ok || e.Code != c.code || !strings.Contains(e.Message, c.msg) {
			t.Errorf("%s: %v, want %s %q", c.in, err, c.code, c.msg)
		}
	}
}
