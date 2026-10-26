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

// addAIRewrites registers the AI functions' translations.
func addAIRewrites(add func(rw rewrite, names ...string)) {
	add(rewrite{fn: aiComplete}, "AI_COMPLETE")
	add(rewrite{fn: cortexComplete}, "SNOWFLAKE.CORTEX.COMPLETE")
	add(rewrite{fn: prompt}, "PROMPT")
	add(rewrite{fn: aiCountTokens}, "AI_COUNT_TOKENS")
}
