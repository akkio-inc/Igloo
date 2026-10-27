package snowflake

import (
	"maps"
	"slices"
	"strconv"
	"strings"
)

// Snowflake's AI functions (Cortex AI SQL) become calls of Igloo's AI
// functions (package ai), which take their arguments by position and
// their options as a JSON object, as text:
//
//	AI_COMPLETE(model => m, prompt => p, model_parameters => {...})
//	  -> ai_complete(m, p, CAST(json_object('model_parameters', {...}) AS VARCHAR))
//
// A function whose result Snowflake gives as an OBJECT (response_format,
// show_details) calls the _object form, which returns JSON, and
// return_error_details => TRUE the _details form, which returns
// {"value": ..., "error": ...}. SNOWFLAKE.CORTEX's older functions fail the
// statement on an error, as Snowflake's do, where the AI_ functions give
// the row NULL. Translation is not a security boundary: the functions
// check grants, caps and the rest when they run, whichever dialect calls
// them.

// aiOption is an option given to an AI function: its name in the options
// object, and its value, translated.
type aiOption struct {
	name  string
	value []*node
}

// aiOptions builds the options object of an AI function's call, as text.
func aiOptions(opts []aiOption, fixed map[string]string) []any {
	var parts []any
	add := func(p ...any) {
		if len(parts) > 0 {
			parts = append(parts, ", ")
		}
		parts = append(parts, p...)
	}
	for _, k := range slices.Sorted(maps.Keys(fixed)) {
		add(quoteString(k)+", ", fixed[k])
	}
	for _, o := range opts {
		add(quoteString(o.name)+", ", o.value)
	}
	if len(parts) == 0 {
		return []any{"'{}'"}
	}
	return append(append([]any{"CAST(json_object("}, parts...), ") AS VARCHAR)")
}

// aiArgs reads an AI function's arguments, named (name => value) or by
// position in the order names gives, into a map by upper-case name.
// Named arguments not in names, and too many by position, are an error.
func aiArgs(c *call, names ...string) (map[string][]*node, error) {
	out := map[string][]*node{}
	if len(c.args) > len(names) {
		return nil, &Error{Code: CodeSyntax, Message: c.name + " takes at most " + strconv.Itoa(len(names)) + " arguments",
			Hint: "its arguments are " + strings.ToLower(strings.Join(names, ", "))}
	}
	for i, a := range c.args {
		out[names[i]] = a
	}
	for k, v := range c.named {
		known := false
		for _, n := range names {
			if n == k {
				known = true
			}
		}
		if !known && k != "RETURN_ERROR_DETAILS" && k != "REFRESH" {
			return nil, &Error{Code: CodeSyntax, Message: c.name + " has no argument " + strings.ToLower(k),
				Hint: "its arguments are " + strings.ToLower(strings.Join(names, ", "))}
		}
		if _, dup := out[k]; dup {
			return nil, &Error{Code: CodeSyntax, Message: c.name + " is given " + strings.ToLower(k) + " twice"}
		}
		out[k] = v
	}
	return out, nil
}

// isFalse reports whether nodes are the literal FALSE.
func isFalse(nodes []*node) bool {
	t := trim(nodes)
	return len(t) == 1 && t[0].isWord("FALSE")
}

// given reports whether args has name, other than a literal FALSE.
func given(args map[string][]*node, name string) bool {
	v, ok := args[name]
	return ok && !isFalse(v)
}

// errorOptions adds return_error_details and refresh to opts, and
// returns the suffix of the function's name for its errors' form.
func errorOptions(args map[string][]*node, opts *[]aiOption) string {
	if v, ok := args["REFRESH"]; ok {
		*opts = append(*opts, aiOption{"refresh", v})
	}
	if given(args, "RETURN_ERROR_DETAILS") {
		return "_details"
	}
	return ""
}

// required returns argument name, or an error naming it.
func required(c *call, args map[string][]*node, name string) ([]*node, error) {
	v, ok := args[name]
	if !ok || len(trim(v)) == 0 {
		return nil, &Error{Code: CodeSyntax, Message: c.name + " needs " + strings.ToLower(name)}
	}
	return v, nil
}

// decisionModels are Snowflake's decision models, whose answers are
// objects.
var decisionModels = map[string]bool{"snowflake-decision": true, "snowflake-decision-fast": true}

// aiComplete translates AI_COMPLETE(model, prompt [, model_parameters [,
// response_format [, show_details]]]).
func aiComplete(c *call) (*node, error) {
	args, err := aiArgs(c, "MODEL", "PROMPT", "MODEL_PARAMETERS", "RESPONSE_FORMAT", "SHOW_DETAILS")
	if err != nil {
		return nil, err
	}
	model, err := required(c, args, "MODEL")
	if err != nil {
		return nil, err
	}
	prompt, err := required(c, args, "PROMPT")
	if err != nil {
		return nil, err
	}
	var opts []aiOption
	for _, k := range []string{"MODEL_PARAMETERS", "RESPONSE_FORMAT", "SHOW_DETAILS"} {
		if v, ok := args[k]; ok {
			opts = append(opts, aiOption{strings.ToLower(k), v})
		}
	}
	suffix := errorOptions(args, &opts)
	fn := "ai_complete"
	object := given(args, "RESPONSE_FORMAT") || given(args, "SHOW_DETAILS")
	if m, ok := literalOf(model); ok && decisionModels[strings.ToLower(m)] {
		object = true
	}
	if object {
		fn = "ai_complete_object"
	}
	out := c.out(append(append([]any{fn + suffix + "(", model, ", ", prompt, ", "}, aiOptions(opts, nil)...), ")")...)
	out.json = object || suffix != ""
	return out, nil
}

// literalOf returns the string literal nodes are, if they are one.
func literalOf(nodes []*node) (string, bool) {
	t := trim(nodes)
	if len(t) != 1 {
		return "", false
	}
	return t[0].stringValue()
}

// cortexComplete translates SNOWFLAKE.CORTEX.COMPLETE(model, prompt): text,
// or with a conversation and options, (model, [{'role', 'content'}, ...],
// {options}), a string holding the answer's JSON. It fails the statement on
// an error.
func cortexComplete(c *call) (*node, error) {
	if err := c.argc(2, 3); err != nil {
		return nil, err
	}
	fixed := map[string]string{"on_error": "'fail'"}
	if len(c.args) == 2 {
		return c.out(append(append([]any{"ai_complete(", c.arg(0), ", ", c.arg(1), ", "}, aiOptions(nil, fixed)...), ")")...), nil
	}
	fixed["history"] = "true"
	opts := []aiOption{{"cortex_options", c.args[2]}}
	return c.out(append(append([]any{"CAST(ai_complete_object(", c.arg(0), ", ", c.arg(1), ", "}, aiOptions(opts, fixed)...),
		") AS VARCHAR)")...), nil
}

// prompt translates PROMPT(template, arg, ...): the object {'template',
// 'args'} that the AI functions read as the template with {0}, {1}, ...
// replaced by the arguments, as text.
func prompt(c *call) (*node, error) {
	if err := c.argc(1, -1); err != nil {
		return nil, err
	}
	parts := []any{"json_object('template', ", c.arg(0), ", 'args', json_array("}
	for i := 1; i < len(c.args); i++ {
		if i > 1 {
			parts = append(parts, ", ")
		}
		parts = append(parts, "CAST(", c.arg(i), " AS VARCHAR)")
	}
	parts = append(parts, "))")
	out := c.out(parts...)
	out.json, out.semi = true, 'O'
	return out, nil
}

// countTokensForms are, by the function AI_COUNT_TOKENS counts for, the
// arguments after its name: whether a model comes first, and how many
// inputs follow.
var countTokensForms = map[string]struct {
	model  bool
	inputs int
}{
	"ai_complete": {true, 1}, "ai_embed": {true, 1}, "ai_similarity": {false, 2}, "ai_classify": {false, 2},
	"ai_translate": {false, 1}, "ai_sentiment": {false, 1}, "ai_summarize": {false, 1}, "ai_redact": {false, 1},
	"ai_filter": {false, 1}, "ai_extract": {false, 1},
}

// aiCountTokens translates AI_COUNT_TOKENS(function, [model,] input, ...):
// the input tokens of the input for the model that would serve the
// function.
func aiCountTokens(c *call) (*node, error) {
	if len(c.args) < 2 {
		return nil, &Error{Code: CodeSyntax, Message: "AI_COUNT_TOKENS takes the function's name and its input, such as " +
			"AI_COUNT_TOKENS('ai_sentiment', text)"}
	}
	fn, ok := c.literal(0)
	if !ok {
		return nil, unsupported("AI_COUNT_TOKENS of a function named by an expression", "name the function in a string literal")
	}
	fn = strings.ToLower(fn)
	form, known := countTokensForms[fn]
	if !known {
		return nil, &Error{Code: CodeInvalidValue, Message: "AI_COUNT_TOKENS cannot count for " + fn,
			Hint: "it counts for ai_complete, ai_embed, ai_similarity, ai_classify, ai_translate, ai_sentiment, ai_summarize, ai_redact, ai_filter and ai_extract"}
	}
	model := []*node{raw("''")}
	rest := c.args[1:]
	if form.model {
		model, rest = c.args[1], c.args[2:]
	}
	if len(rest) < 1 {
		return nil, &Error{Code: CodeSyntax, Message: "AI_COUNT_TOKENS for " + fn + " needs its input"}
	}
	input := []any{rest[0]}
	if form.inputs == 2 && len(rest) > 1 {
		input = []any{"concat_ws(' ', CAST(", rest[0], " AS VARCHAR), CAST(", rest[1], " AS VARCHAR))"}
	}
	var opts []aiOption
	if fn == "ai_similarity" && len(rest) > 2 {
		opts = append(opts, aiOption{"config", rest[2]})
	}
	if fn != "ai_similarity" && form.model && len(rest) > 1 {
		opts = append(opts, aiOption{"model_parameters", rest[1]})
	}
	suffix := errorOptions(c.named, &opts)
	parts := []any{"ai_count_tokens" + suffix + "(" + quoteString(fn) + ", ", model, ", "}
	parts = append(parts, input...)
	parts = append(parts, ", ")
	parts = append(parts, aiOptions(opts, nil)...)
	parts = append(parts, ")")
	out := c.out(parts...)
	out.json = suffix != ""
	return out, nil
}

// cortexName returns the name of the SNOWFLAKE.CORTEX function called at
// nodes[i], "SNOWFLAKE.CORTEX.COMPLETE" and the like, and the index of its
// arguments' group; ok is false if nodes[i] starts no such call.
func cortexName(nodes []*node, i int) (name string, g int, ok bool) {
	if !nodes[i].isWord("SNOWFLAKE") {
		return "", 0, false
	}
	j := nextSig(nodes, i)
	if j < 0 || !nodes[j].isOp(".") {
		return "", 0, false
	}
	k := nextSig(nodes, j)
	if k < 0 || !nodes[k].isWord("CORTEX") {
		return "", 0, false
	}
	l := nextSig(nodes, k)
	if l < 0 || !nodes[l].isOp(".") {
		return "", 0, false
	}
	m := nextSig(nodes, l)
	if m < 0 || nodes[m].kind != kWord {
		return "", 0, false
	}
	g = nextSig(nodes, m)
	if g < 0 || !nodes[g].isGroup('(') {
		return "", 0, false
	}
	return "SNOWFLAKE.CORTEX." + nodes[m].word(), g, true
}

// aiCall builds the call of Igloo's AI function fn with args, and the
// options object.
func aiCall(c *call, fn string, args []any, opts []aiOption, fixed map[string]string) *node {
	parts := []any{fn + "("}
	for _, a := range args {
		parts = append(parts, a, ", ")
	}
	parts = append(parts, aiOptions(opts, fixed)...)
	return c.out(append(parts, ")")...)
}

// legacy are the options of SNOWFLAKE.CORTEX's functions, which fail the
// statement on an error.
func legacy() map[string]string { return map[string]string{"on_error": "'fail'"} }

// objectResult marks n as returning an OBJECT (JSON).
func objectResult(n *node, semi byte) *node {
	n.json, n.semi = true, semi
	return n
}

// aiClassify translates AI_CLASSIFY(input, list_of_categories [,
// config_object]): {"labels": [...]}.
func aiClassify(c *call) (*node, error) {
	args, err := aiArgs(c, "INPUT", "LIST_OF_CATEGORIES", "CONFIG_OBJECT")
	if err != nil {
		return nil, err
	}
	input, err := required(c, args, "INPUT")
	if err != nil {
		return nil, err
	}
	cats, err := required(c, args, "LIST_OF_CATEGORIES")
	if err != nil {
		return nil, err
	}
	var opts []aiOption
	if v, ok := args["CONFIG_OBJECT"]; ok {
		opts = append(opts, aiOption{"config", v})
	}
	suffix := errorOptions(args, &opts)
	return objectResult(aiCall(c, "ai_classify"+suffix, []any{input, cats}, opts, nil), 'O'), nil
}

// cortexClassify translates SNOWFLAKE.CORTEX.CLASSIFY_TEXT(input,
// list_of_categories [, options]): {"label": ...}.
func cortexClassify(c *call) (*node, error) {
	if err := c.argc(2, 3); err != nil {
		return nil, err
	}
	var opts []aiOption
	if len(c.args) == 3 {
		opts = append(opts, aiOption{"config", c.args[2]})
	}
	fixed := legacy()
	fixed["label"] = "true"
	return objectResult(aiCall(c, "ai_classify", []any{c.arg(0), c.arg(1)}, opts, fixed), 'O'), nil
}

// aiFilter translates AI_FILTER(predicate), whose argument is text or a
// PROMPT object: BOOLEAN.
func aiFilter(c *call) (*node, error) {
	args, err := aiArgs(c, "PREDICATE", "INPUT")
	if err != nil {
		return nil, err
	}
	input, ok := args["PREDICATE"]
	if v, given := args["INPUT"]; given {
		if ok {
			return nil, &Error{Code: CodeSyntax, Message: "AI_FILTER takes one argument: the predicate, as text or PROMPT(...)"}
		}
		input, ok = v, true
	}
	if !ok || len(trim(input)) == 0 {
		return nil, &Error{Code: CodeSyntax, Message: "AI_FILTER needs the predicate, as text or PROMPT(...)"}
	}
	var opts []aiOption
	suffix := errorOptions(args, &opts)
	out := aiCall(c, "ai_filter"+suffix, []any{input}, opts, nil)
	out.json = suffix != ""
	return out, nil
}

// aiSentiment translates AI_SENTIMENT(text [, categories]):
// {"categories": [{"name", "sentiment"}, ...]}.
func aiSentiment(c *call) (*node, error) {
	args, err := aiArgs(c, "TEXT", "CATEGORIES")
	if err != nil {
		return nil, err
	}
	input, err := required(c, args, "TEXT")
	if err != nil {
		return nil, err
	}
	var cats any = "'[]'"
	if v, ok := args["CATEGORIES"]; ok {
		cats = v
	}
	var opts []aiOption
	suffix := errorOptions(args, &opts)
	return objectResult(aiCall(c, "ai_sentiment"+suffix, []any{input, cats}, opts, nil), 'O'), nil
}

// aiExtract translates AI_EXTRACT(text, responseFormat): {"error",
// "response"}.
func aiExtract(c *call) (*node, error) {
	args, err := aiArgs(c, "TEXT", "RESPONSEFORMAT", "FILE")
	if err != nil {
		return nil, err
	}
	if _, ok := args["FILE"]; ok {
		return nil, unsupported("AI_EXTRACT of a file", "extract from the file's text, as AI_EXTRACT(text => ..., responseFormat => ...)")
	}
	input, err := required(c, args, "TEXT")
	if err != nil {
		return nil, err
	}
	format, err := required(c, args, "RESPONSEFORMAT")
	if err != nil {
		return nil, err
	}
	var opts []aiOption
	errorOptions(args, &opts) // its errors are in its result already
	return objectResult(aiCall(c, "ai_extract", []any{input, format}, opts, nil), 'O'), nil
}

// aiSummarize translates AI_SUMMARIZE(text).
func aiSummarize(c *call) (*node, error) {
	args, err := aiArgs(c, "TEXT")
	if err != nil {
		return nil, err
	}
	input, err := required(c, args, "TEXT")
	if err != nil {
		return nil, err
	}
	var opts []aiOption
	suffix := errorOptions(args, &opts)
	out := aiCall(c, "ai_summarize"+suffix, []any{input}, opts, nil)
	out.json, out.str = suffix != "", suffix == ""
	return out, nil
}

// aiTranslate translates AI_TRANSLATE(text, source_language,
// target_language).
func aiTranslate(c *call) (*node, error) {
	args, err := aiArgs(c, "TEXT", "SOURCE_LANGUAGE", "TARGET_LANGUAGE")
	if err != nil {
		return nil, err
	}
	var parts []any
	for _, k := range []string{"TEXT", "SOURCE_LANGUAGE", "TARGET_LANGUAGE"} {
		v, ok := args[k]
		if !ok {
			return nil, &Error{Code: CodeSyntax, Message: c.name + " needs " + strings.ToLower(k)}
		}
		parts = append(parts, v)
	}
	var opts []aiOption
	suffix := errorOptions(args, &opts)
	out := aiCall(c, "ai_translate"+suffix, parts, opts, nil)
	out.json, out.str = suffix != "", suffix == ""
	return out, nil
}

// aiRedact translates AI_REDACT(input [, categories] [, mode]): the text
// redacted, or with mode 'detect' the spans found.
func aiRedact(c *call) (*node, error) {
	args, err := aiArgs(c, "INPUT", "CATEGORIES", "MODE")
	if err != nil {
		return nil, err
	}
	input, err := required(c, args, "INPUT")
	if err != nil {
		return nil, err
	}
	// AI_REDACT(input, 'detect'): the mode in categories' place.
	if v, ok := args["CATEGORIES"]; ok && args["MODE"] == nil {
		if _, isText := literalOf(v); isText {
			args["MODE"] = v
			delete(args, "CATEGORIES")
		}
	}
	detect := false
	if v, ok := args["MODE"]; ok {
		m, isText := literalOf(v)
		switch strings.ToLower(m) {
		case "redact":
		case "detect":
			detect = true
		default:
			if !isText {
				return nil, unsupported("AI_REDACT with a mode given by an expression", "give the mode as 'redact' or 'detect'")
			}
			return nil, &Error{Code: CodeInvalidValue, Message: "AI_REDACT's mode is 'redact' or 'detect', not " + quoteString(m)}
		}
	}
	var cats any = "'[]'"
	if v, ok := args["CATEGORIES"]; ok {
		cats = v
	}
	var opts []aiOption
	suffix := errorOptions(args, &opts)
	fn := "ai_redact"
	if detect {
		fn = "ai_redact_detect"
	}
	out := aiCall(c, fn+suffix, []any{input, cats}, opts, nil)
	if detect || suffix != "" {
		objectResult(out, 'O')
	} else {
		out.str = true
	}
	return out, nil
}

// aiEmbed translates AI_EMBED(model, input): the input's vector, a list of
// FLOATs.
func aiEmbed(c *call) (*node, error) {
	args, err := aiArgs(c, "MODEL", "INPUT")
	if err != nil {
		return nil, err
	}
	model, err := required(c, args, "MODEL")
	if err != nil {
		return nil, err
	}
	input, err := required(c, args, "INPUT")
	if err != nil {
		return nil, err
	}
	var opts []aiOption
	suffix := errorOptions(args, &opts)
	out := aiCall(c, "ai_embed"+suffix, []any{model, input}, opts, nil)
	out.json = suffix != ""
	return out, nil
}

// cortexEmbed translates SNOWFLAKE.CORTEX.EMBED_TEXT_768 and _1024(model,
// text): a VECTOR(FLOAT, dims).
func cortexEmbed(dims string) func(c *call) (*node, error) {
	return func(c *call) (*node, error) {
		if err := c.argc(2, 2); err != nil {
			return nil, err
		}
		fixed := legacy()
		fixed["dimensions"] = dims
		return c.out("CAST(", aiCall(c, "ai_embed", []any{c.arg(0), c.arg(1)}, nil, fixed), " AS FLOAT["+dims+"])"), nil
	}
}

// aiSimilarity translates AI_SIMILARITY(input1, input2 [, config_object]):
// the cosine similarity of their vectors.
func aiSimilarity(c *call) (*node, error) {
	args, err := aiArgs(c, "INPUT1", "INPUT2", "CONFIG_OBJECT")
	if err != nil {
		return nil, err
	}
	a, err := required(c, args, "INPUT1")
	if err != nil {
		return nil, err
	}
	b, err := required(c, args, "INPUT2")
	if err != nil {
		return nil, err
	}
	var opts []aiOption
	if v, ok := args["CONFIG_OBJECT"]; ok {
		opts = append(opts, aiOption{"config", v})
	}
	suffix := errorOptions(args, &opts)
	out := aiCall(c, "ai_similarity"+suffix, []any{a, b}, opts, nil)
	out.json, out.num = suffix != "", suffix == ""
	return out, nil
}

// cortexSimple translates one of SNOWFLAKE.CORTEX's functions of fixed
// arguments to fn, which fails the statement on an error.
func cortexSimple(fn string, n int, semi byte) func(c *call) (*node, error) {
	return func(c *call) (*node, error) {
		if err := c.argc(n, n); err != nil {
			return nil, err
		}
		args := make([]any, n)
		for i := range n {
			args[i] = c.arg(i)
		}
		out := aiCall(c, fn, args, nil, legacy())
		if semi != 0 {
			objectResult(out, semi)
		}
		return out, nil
	}
}

// cortexCountTokens translates SNOWFLAKE.CORTEX.COUNT_TOKENS(model, text):
// the text's tokens for the model, as AI_COMPLETE would send it.
func cortexCountTokens(c *call) (*node, error) {
	if err := c.argc(2, 2); err != nil {
		return nil, err
	}
	out := aiCall(c, "ai_count_tokens", []any{"'ai_complete'", c.arg(0), c.arg(1)}, nil, legacy())
	out.num = true
	return out, nil
}

// aiDecide translates AI_DECIDE(state, questions), as Databricks' ai_decide
// takes them: the decision model's answers, {"response": {"answers":
// {...}}, "metadata": {...}, "error_message": ...}.
func aiDecide(c *call) (*node, error) {
	args, err := aiArgs(c, "STATE", "QUESTIONS")
	if err != nil {
		return nil, err
	}
	state, err := required(c, args, "STATE")
	if err != nil {
		return nil, err
	}
	questions, err := required(c, args, "QUESTIONS")
	if err != nil {
		return nil, err
	}
	var opts []aiOption
	errorOptions(args, &opts) // its errors are in its result already
	return objectResult(aiCall(c, "ai_decide", []any{state, c.out("CAST(", questions, " AS VARCHAR)")}, opts, nil), 'O'), nil
}

// aiAgg translates AI_AGG(expr, task) and, with summarize,
// AI_SUMMARIZE_AGG(expr): an aggregate of the group's values as a list,
// which ai_agg answers the task over.
func aiAgg(summarize bool) func(c *call) (*node, error) {
	return func(c *call) (*node, error) {
		if c.over != nil {
			return nil, unsupported(c.name+" as a window function", "aggregate with GROUP BY")
		}
		n := 2
		if summarize {
			n = 1
		}
		if err := c.argc(n, n); err != nil {
			return nil, err
		}
		values := c.out("list_filter(list(", distinctPrefix(c), "CAST(", c.arg(0), " AS VARCHAR)), lambda v: v IS NOT NULL)")
		task := any("''")
		var fixed map[string]string
		if summarize {
			fixed = map[string]string{"summarize": "true"}
		} else {
			task = c.arg(1)
		}
		out := aiCall(c, "ai_agg", []any{values, task}, nil, fixed)
		out.str = true
		return out, nil
	}
}

// aiRewrite marks fn's translations as calling a model provider.
func aiRewrite(fn func(c *call) (*node, error)) rewrite {
	return rewrite{fn: func(c *call) (*node, error) {
		out, err := fn(c)
		if out != nil {
			out.ai = true
		}
		return out, err
	}}
}

// addAIRewrites registers the AI functions' translations.
func addAIRewrites(add func(rw rewrite, names ...string)) {
	add(aiRewrite(aiComplete), "AI_COMPLETE")
	add(aiRewrite(cortexComplete), "SNOWFLAKE.CORTEX.COMPLETE")
	add(rewrite{fn: prompt}, "PROMPT")
	add(aiRewrite(aiCountTokens), "AI_COUNT_TOKENS")
	add(aiRewrite(cortexCountTokens), "SNOWFLAKE.CORTEX.COUNT_TOKENS")
	add(aiRewrite(aiClassify), "AI_CLASSIFY")
	add(aiRewrite(cortexClassify), "SNOWFLAKE.CORTEX.CLASSIFY_TEXT")
	add(aiRewrite(aiFilter), "AI_FILTER")
	add(aiRewrite(aiSentiment), "AI_SENTIMENT")
	add(aiRewrite(cortexSimple("ai_sentiment_score", 1, 0)), "SNOWFLAKE.CORTEX.SENTIMENT")
	add(aiRewrite(aiExtract), "AI_EXTRACT")
	add(aiRewrite(cortexSimple("ai_extract_answer", 2, 'A')), "SNOWFLAKE.CORTEX.EXTRACT_ANSWER")
	add(aiRewrite(aiSummarize), "AI_SUMMARIZE")
	add(aiRewrite(cortexSimple("ai_summarize", 1, 0)), "SNOWFLAKE.CORTEX.SUMMARIZE")
	add(aiRewrite(aiTranslate), "AI_TRANSLATE")
	add(aiRewrite(cortexSimple("ai_translate", 3, 0)), "SNOWFLAKE.CORTEX.TRANSLATE")
	add(aiRewrite(aiRedact), "AI_REDACT")
	add(aiRewrite(aiEmbed), "AI_EMBED")
	add(aiRewrite(cortexEmbed("768")), "SNOWFLAKE.CORTEX.EMBED_TEXT_768")
	add(aiRewrite(cortexEmbed("1024")), "SNOWFLAKE.CORTEX.EMBED_TEXT_1024")
	add(aiRewrite(aiSimilarity), "AI_SIMILARITY")
	add(aiRewrite(aiDecide), "AI_DECIDE")
	for name, summarize := range map[string]bool{"AI_AGG": false, "AI_SUMMARIZE_AGG": true} {
		rw := aiRewrite(aiAgg(summarize))
		rw.over = true
		add(rw, name)
	}
}

// --- Plain predicates first -------------------------------------------------

// guardedClauses are the clauses whose AI predicates guardAI guards, and
// the words that end each.
var guardedClauses = map[string]map[string]bool{
	"WHERE":   clauseEnds,
	"HAVING":  clauseEnds,
	"QUALIFY": clauseEnds,
	"ON":      joinClauseEnds,
}

var clauseEnds = map[string]bool{
	"GROUP": true, "HAVING": true, "QUALIFY": true, "WINDOW": true, "ORDER": true, "LIMIT": true, "OFFSET": true,
	"FETCH": true, "UNION": true, "EXCEPT": true, "MINUS": true, "INTERSECT": true, "RETURNING": true,
}

var joinClauseEnds = func() map[string]bool {
	m := map[string]bool{"JOIN": true, "INNER": true, "LEFT": true, "RIGHT": true, "FULL": true, "CROSS": true,
		"NATURAL": true, "ASOF": true, "POSITIONAL": true, "LATERAL": true, "WHERE": true, "WHEN": true, "USING": true}
	for k := range clauseEnds {
		m[k] = true
	}
	return m
}()

// volatileWords are functions whose value changes with each call, which
// guardAI does not repeat.
var volatileWords = []string{"random", "uniform", "uuid", "nextval", "setseed", "normal(", "seq1", "seq2", "seq4", "seq8"}

// hasAI reports whether nodes call a model provider.
func hasAI(nodes []*node) bool {
	for _, n := range nodes {
		if n.ai || hasAI(n.kids) || n.v != nil && n.v.base != nil && hasAI([]*node{n.v.base}) {
			return true
		}
	}
	return false
}

// guardAI makes the plain predicates of a WHERE, HAVING, QUALIFY or ON
// clause that is a conjunction run before its AI predicates. DuckDB orders
// a filter's conjuncts by its own estimate of their cost, in which a call
// of an AI function is cheap, and so would call the provider for every row
// the plain predicates would have dropped:
//
//	WHERE a AND AI_FILTER(x) AND b
//	  -> WHERE a AND b AND CASE WHEN a AND b THEN ai_filter(x, ...) ELSE FALSE END
//
// which a filter keeps the same rows of, NULL being FALSE there.
func (t *translator) guardAI(nodes []*node) ([]*node, error) {
	if !hasAI(nodes) {
		return nodes, nil
	}
	for i := 0; i < len(nodes); i++ {
		ends, ok := guardedClauses[nodes[i].word()]
		if !ok {
			continue
		}
		start := i + 1
		end := clauseEnd(nodes, start, ends)
		if guarded, ok := guard(nodes[start:end]); ok {
			nodes = replace(nodes, start, end, guarded...)
			end = start + len(guarded)
		}
		i = end - 1
	}
	return nodes, nil
}

// clauseEnd returns the index of the word that ends the clause starting at
// nodes[start], or len(nodes).
func clauseEnd(nodes []*node, start int, ends map[string]bool) int {
	depth := 0
	for k := start; k < len(nodes); k++ {
		switch w := nodes[k].word(); {
		case w == "CASE":
			depth++
		case w == "END" && depth > 0:
			depth--
		case depth == 0 && (ends[w] || nodes[k].isOp(",") || nodes[k].isOp(";")):
			return k
		}
	}
	return len(nodes)
}

// guard rewrites a clause's condition so its plain conjuncts run first;
// ok is false where it is not a conjunction of plain and AI predicates.
func guard(cond []*node) ([]*node, bool) {
	var conjuncts [][]*node
	depth, between, from := 0, false, 0
	for k, n := range cond {
		switch w := n.word(); {
		case w == "CASE":
			depth++
		case w == "END" && depth > 0:
			depth--
		case depth > 0:
		case w == "OR":
			return nil, false
		case w == "BETWEEN":
			between = true
		case w == "AND" && between:
			between = false
		case w == "AND":
			conjuncts = append(conjuncts, trim(cond[from:k]))
			from = k + 1
		}
	}
	conjuncts = append(conjuncts, trim(cond[from:]))
	var plain, ai []string
	for _, c := range conjuncts {
		if len(c) == 0 {
			return nil, false
		}
		if hasAI(c) {
			ai = append(ai, "("+render(c)+")")
		} else {
			plain = append(plain, render(c))
		}
	}
	if len(plain) == 0 || len(ai) == 0 {
		return nil, false
	}
	all := strings.ToLower(strings.Join(plain, " "))
	for _, v := range volatileWords {
		if strings.Contains(all, v) {
			return nil, false
		}
	}
	tests := make([]string, len(plain))
	for k, p := range plain {
		tests[k] = "(" + p + ")"
	}
	text := " " + strings.Join(plain, " AND ") + " AND CASE WHEN " + strings.Join(tests, " AND ") + " THEN " +
		strings.Join(ai, " AND ") + " ELSE FALSE END "
	out := raw(text)
	out.ai = true
	return []*node{out}, true
}
