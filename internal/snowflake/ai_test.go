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
		{`SELECT AI_FILTER() FROM t`, CodeSyntax, "needs the predicate"},
		{`SELECT AI_EXTRACT(file => f, responseFormat => ['a']) FROM t`, CodeUnsupported, "AI_EXTRACT of a file"},
		{`SELECT AI_REDACT(x, ['NAME'], 'mask') FROM t`, CodeInvalidValue, "'redact' or 'detect'"},
		{`SELECT AI_TRANSLATE(x, 'en') FROM t`, CodeSyntax, "needs target_language"},
		{`SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_768(x) FROM t`, CodeSyntax, "takes 2 arguments"},
		{`SELECT AI_AGG(x, 'task') OVER (PARTITION BY y) FROM t`, CodeUnsupported, "AI_AGG as a window function"},
		{`SELECT AI_AGG(x) FROM t`, CodeSyntax, "takes 2 arguments"},
	} {
		_, err := Translate(c.in, Options{})
		e, ok := err.(*Error)
		if !ok || e.Code != c.code || !strings.Contains(e.Message, c.msg) {
			t.Errorf("%s: %v, want %s %q", c.in, err, c.code, c.msg)
		}
	}
}

// TestTranslateAITasks checks the task functions' translations, and their
// SNOWFLAKE.CORTEX forms', which fail the statement on an error.
func TestTranslateAITasks(t *testing.T) {
	const fail = `CAST(json_object('on_error', 'fail') AS VARCHAR)`
	for _, c := range []struct{ in, want string }{
		{`SELECT AI_CLASSIFY(review, ['travel', 'cooking']) AS c FROM reviews`,
			`SELECT ai_classify(review, json_array('travel', 'cooking'), '{}') AS "C" FROM reviews`},
		{`SELECT AI_CLASSIFY(review, ['travel', 'cooking'], {'output_mode': 'multi'}):labels AS c FROM reviews`,
			`SELECT json_extract(ai_classify(review, json_array('travel', 'cooking'), CAST(json_object('config', to_json(`},
		{`SELECT AI_CLASSIFY(review, ['a', 'b'], return_error_details => TRUE) AS c FROM reviews`,
			`SELECT ai_classify_details(review, json_array('a', 'b'), '{}') AS "C" FROM reviews`},
		{`SELECT SNOWFLAKE.CORTEX.CLASSIFY_TEXT(review, ['a', 'b']):label::VARCHAR AS c FROM reviews`,
			`SELECT json_extract_string(ai_classify(review, json_array('a', 'b'), CAST(json_object('label', true, 'on_error', 'fail') AS VARCHAR)), '$.label') AS "C" FROM reviews`},
		{`SELECT AI_FILTER(review) AS f FROM reviews`, `SELECT ai_filter(review, '{}') AS "F" FROM reviews`},
		{`SELECT AI_SENTIMENT(review, ['price', 'service']):categories[0]:sentiment::VARCHAR AS s FROM reviews`,
			`SELECT json_extract_string(ai_sentiment(review, json_array('price', 'service'), '{}'), '$.categories[0].sentiment') AS "S" FROM reviews`},
		{`SELECT AI_SENTIMENT(review) AS s FROM reviews`, `SELECT ai_sentiment(review, '[]', '{}') AS "S" FROM reviews`},
		{`SELECT SNOWFLAKE.CORTEX.SENTIMENT(review) AS s FROM reviews`, `SELECT ai_sentiment_score(review, ` + fail + `) AS "S" FROM reviews`},
		{`SELECT AI_EXTRACT(text => review, responseFormat => {'city': 'Which city?'}) AS e FROM reviews`,
			`SELECT ai_extract(review, to_json(map_from_entries(`},
		{`SELECT SNOWFLAKE.CORTEX.EXTRACT_ANSWER(review, 'Which city?')[0]:answer::VARCHAR AS e FROM reviews`,
			`SELECT json_extract_string(ai_extract_answer(review, 'Which city?', ` + fail + `), '$[0].answer') AS "E" FROM reviews`},
		{`SELECT AI_SUMMARIZE(review) AS s, SNOWFLAKE.CORTEX.SUMMARIZE(review) AS t FROM reviews`,
			`SELECT ai_summarize(review, '{}') AS "S", ai_summarize(review, ` + fail + `) AS "T" FROM reviews`},
		{`SELECT AI_TRANSLATE(review, '', 'de') AS s, SNOWFLAKE.CORTEX.TRANSLATE(review, 'en', 'de') AS t FROM reviews`,
			`SELECT ai_translate(review, '', 'de', '{}') AS "S", ai_translate(review, 'en', 'de', ` + fail + `) AS "T" FROM reviews`},
		{`SELECT AI_REDACT(review) AS r, AI_REDACT(review, ['NAME'], 'detect') AS d, AI_REDACT(review, 'detect') AS e FROM reviews`,
			`SELECT ai_redact(review, '[]', '{}') AS "R", ai_redact_detect(review, json_array('NAME'), '{}') AS "D", ai_redact_detect(review, '[]', '{}') AS "E" FROM reviews`},
		{`SELECT AI_EMBED('e5-base-v2', review) AS v, SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', review) AS w FROM reviews`,
			`SELECT ai_embed('e5-base-v2', review, '{}') AS "V", CAST(ai_embed('e5-base-v2', review, CAST(json_object('dimensions', 768, 'on_error', 'fail') AS VARCHAR)) AS FLOAT[768]) AS "W" FROM reviews`},
		{`SELECT VECTOR_COSINE_SIMILARITY(AI_EMBED('e5-base-v2', review), AI_EMBED('e5-base-v2', 'refund')) AS v FROM reviews`,
			`SELECT sf_vector_cosine_similarity(CAST(ai_embed('e5-base-v2', review, '{}') AS DOUBLE[]), CAST(ai_embed('e5-base-v2', 'refund', '{}') AS DOUBLE[])) AS "V" FROM reviews`},
		{`SELECT AI_SIMILARITY(review, 'refund') AS v FROM reviews`, `SELECT ai_similarity(review, 'refund', '{}') AS "V" FROM reviews`},
		{`SELECT SNOWFLAKE.CORTEX.COUNT_TOKENS('m', review) AS n FROM reviews`,
			`SELECT ai_count_tokens('ai_complete', 'm', review, ` + fail + `) AS "N" FROM reviews`},
		{`SELECT region, AI_AGG(review, 'What do customers complain about?') AS a FROM reviews GROUP BY region`,
			`SELECT region, ai_agg(list_filter(list(CAST(review AS VARCHAR)), lambda v: v IS NOT NULL), 'What do customers complain about?', '{}') AS "A" FROM reviews GROUP BY region`},
		{`SELECT AI_SUMMARIZE_AGG(DISTINCT review) AS a FROM reviews`,
			`SELECT ai_agg(list_filter(list(DISTINCT CAST(review AS VARCHAR)), lambda v: v IS NOT NULL), '', CAST(json_object('summarize', true) AS VARCHAR)) AS "A" FROM reviews`},
		{`SELECT AI_DECIDE(review, {'refund': {'type': 'noul', 'instructions': 'Does it ask for a refund?'}}):response:answers:refund:probability::FLOAT AS p FROM reviews`,
			`SELECT CAST(json_extract_string(ai_decide(review, CAST(to_json(map_from_entries(`},
	} {
		got := translate(t, c.in).SQL
		if !strings.HasPrefix(got, c.want) {
			t.Errorf("%s:\n got %s\nwant %s", c.in, got, c.want)
		}
	}
}

// TestGuardAI: a clause's plain conjuncts are tested before its AI
// predicates are called (see guardAI); clauses that are not conjunctions
// stay as they are.
func TestGuardAI(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{`SELECT * FROM reviews WHERE stars < 3 AND AI_FILTER(review) AND region = 'emea'`,
			`SELECT * FROM reviews WHERE stars < 3 AND region = 'emea' AND CASE WHEN (stars < 3) AND (region = 'emea') THEN (ai_filter(review, '{}')) ELSE FALSE END`},
		{`SELECT * FROM reviews WHERE stars BETWEEN 1 AND 2 AND AI_FILTER(review) AND CASE WHEN a AND b THEN true END ORDER BY 1`,
			`SELECT * FROM reviews WHERE stars BETWEEN 1 AND 2 AND CASE WHEN a AND b THEN true END AND CASE WHEN (stars BETWEEN 1 AND 2) AND (CASE WHEN a AND b THEN true END) THEN (ai_filter(review, '{}')) ELSE FALSE END ORDER BY 1`},
		{`SELECT * FROM reviews WHERE stars < 3 OR AI_FILTER(review)`,
			`SELECT * FROM reviews WHERE stars < 3 OR ai_filter(review, '{}')`},
		{`SELECT * FROM reviews WHERE AI_FILTER(review)`, `SELECT * FROM reviews WHERE ai_filter(review, '{}')`},
		{`SELECT * FROM reviews WHERE random() < 0.1 AND AI_FILTER(review)`,
			`SELECT * FROM reviews WHERE CAST((random() - 0.5) * 18446744073709549568.0 AS BIGINT) < 0.1 AND ai_filter(review, '{}')`},
		{`SELECT a.id FROM a JOIN b ON a.k = b.k AND AI_FILTER(PROMPT('{0} matches {1}', a.x, b.y)) WHERE a.z > 1`,
			`SELECT a.id FROM a JOIN b ON a.k = b.k AND CASE WHEN (a.k = b.k) THEN (ai_filter(json_object('template', '{0} matches {1}', 'args', json_array(CAST(a.x AS VARCHAR), CAST(b.y AS VARCHAR))), '{}')) ELSE FALSE END WHERE a.z > 1`},
		{`SELECT region FROM reviews GROUP BY region HAVING count(*) > 10 AND AI_CLASSIFY(region, ['a', 'b']):labels[0] = 'a'`,
			`SELECT region FROM reviews GROUP BY region HAVING count(*) > 10 AND CASE WHEN (count(*) > 10) THEN (`},
		{`SELECT * FROM reviews WHERE id IN (SELECT id FROM t WHERE x = 1 AND AI_FILTER(y)) AND stars < 2`,
			`SELECT * FROM reviews WHERE stars < 2 AND CASE WHEN (stars < 2) THEN (id IN (SELECT id FROM t WHERE x = 1 AND CASE WHEN (x = 1) THEN (ai_filter(y, '{}')) ELSE FALSE END )) ELSE FALSE END`},
	} {
		got := translate(t, c.in).SQL
		if !strings.HasPrefix(got, c.want) {
			t.Errorf("%s:\n got %s\nwant %s", c.in, got, c.want)
		}
	}
}
