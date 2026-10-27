package snowflake

import "strings"

// Other warehouses' forecasting functions, which Igloo runs with the model
// pack's foundation models (package tsforecast): BigQuery's AI.FORECAST
// and AI.DETECT_ANOMALIES and Databricks' ai_forecast and
// ai_detect_anomalies. As Databricks' ai_predict_class, a call in FROM
// becomes igloo_ml_method('<the call as written>'), which package pgwire
// writes as a query.

// ForecastFunctions are the functions, by the names ModelCall.Method
// gives them.
var ForecastFunctions = []string{"AI.FORECAST", "AI.DETECT_ANOMALIES", "AI_FORECAST", "AI_DETECT_ANOMALIES"}

// forecastCallAt returns the index after the call of a forecasting
// function starting at nodes[i], or -1.
func forecastCallAt(nodes []*node, i int) int {
	if isQualified(nodes, i) {
		return -1
	}
	g := nextSig(nodes, i)
	switch nodes[i].word() {
	case "AI_FORECAST", "AI_DETECT_ANOMALIES":
	case "AI":
		dot := g
		if dot < 0 || !nodes[dot].isOp(".") {
			return -1
		}
		name := nextSig(nodes, dot)
		if name < 0 || (nodes[name].word() != "FORECAST" && nodes[name].word() != "DETECT_ANOMALIES") {
			return -1
		}
		g = nextSig(nodes, name)
	default:
		return -1
	}
	if g < 0 || !nodes[g].isGroup('(') {
		return -1
	}
	return g + 1
}

// IsForecastCall reports whether text is a call of a forecasting
// function.
func IsForecastCall(text string) bool {
	toks, err := lex(text)
	if err != nil {
		return false
	}
	nodes, err := build(toks)
	if err != nil {
		return false
	}
	ix := sig(nodes)
	return len(ix) > 0 && forecastCallAt(nodes, ix[0]) == ix[len(ix)-1]+1
}

// ParseForecastCall reads a call of a forecasting function: its arguments
// as ParseModelCall reads them, and also BigQuery's TABLE name and
// (query), and TIMESTAMP '...' and DATE '...' literals, as strings.
func ParseForecastCall(text string) (*ModelCall, error) {
	toks, err := lex(text)
	if err != nil {
		return nil, err
	}
	nodes, err := build(toks)
	if err != nil {
		return nil, err
	}
	ix := sig(nodes)
	if len(ix) == 0 || forecastCallAt(nodes, ix[0]) != ix[len(ix)-1]+1 {
		return nil, &Error{Code: CodeSyntax, Message: "expected AI.FORECAST, AI.DETECT_ANOMALIES, ai_forecast or ai_detect_anomalies"}
	}
	c := &ModelCall{Text: text}
	if w := nodes[ix[0]].word(); w == "AI" {
		c.Method = "AI." + nodes[ix[2]].word()
	} else {
		c.Method = w
	}
	p := &modelParser{src: text, nodes: nodes, idx: ix}
	for _, a := range splitArgs(nodes[ix[len(ix)-1]].kids) {
		a = trim(a)
		ax := sig(a)
		if len(ax) == 0 {
			return nil, p.errorf("an argument is empty")
		}
		var arg ModelArg
		if len(ax) >= 2 && a[ax[0]].kind == kWord && a[ax[1]].isOp("=>") {
			arg.Name = strings.ToUpper(a[ax[0]].tok.text)
			a = trim(a[ax[1]+1:])
			ax = sig(a)
		}
		v, perr := p.forecastValue(a, ax)
		if perr != nil {
			if arg.Name != "" {
				perr.Message = arg.Name + ": " + perr.Message
			}
			return nil, perr
		}
		arg.Value = v
		c.Args = append(c.Args, arg)
	}
	return c, nil
}

func (p *modelParser) forecastValue(a []*node, ax []int) (ModelValue, *Error) {
	if len(ax) == 0 {
		return ModelValue{}, p.errorf("expected a value")
	}
	first := a[ax[0]]
	switch {
	case first.isWord("TABLE") && len(ax) >= 2 && !a[ax[1]].isGroup('('):
		// BigQuery's TABLE dataset.name.
		sub := &modelParser{src: p.src, nodes: a[ax[1]:], idx: sig(a[ax[1]:])}
		parts, err := sub.name()
		if err != nil || !sub.done() {
			return ModelValue{}, p.errorf("TABLE needs a table or view's name")
		}
		return ModelValue{Kind: ValueTable, Table: parts}, nil
	case len(ax) == 1 && first.isGroup('('):
		kids := trim(first.kids)
		if w := firstWord(kids); w == "SELECT" || w == "WITH" || w == "FROM" {
			return ModelValue{Kind: ValueQuery, Str: source(p.src, kids)}, nil
		}
	case len(ax) == 2 && (first.isWord("TIMESTAMP") || first.isWord("DATE") || first.isWord("DATETIME")) && a[ax[1]].kind == kString:
		s, _ := a[ax[1]].stringValue()
		return ModelValue{Kind: ValueString, Str: s}, nil
	}
	return p.value(a)
}

func firstWord(nodes []*node) string {
	for _, n := range nodes {
		if n.significant() {
			return n.word()
		}
	}
	return ""
}
