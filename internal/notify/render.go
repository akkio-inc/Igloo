package notify

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// Rendering a message for each destination. Text the alert's owner or
// Igloo wrote goes as written, so a MESSAGE may mention @here or link to a
// dashboard. Values, which come from data, are made inert:
//
//   - Slack: in code spans and code blocks, where Slack makes no links and
//     no mentions, with &, < and > escaped as Slack requires, so a value
//     cannot write <!here>, <@U123> or <https://...|text>, and backticks
//     replaced so it cannot close the code.
//   - Teams: in Adaptive Card TextRuns, which Teams shows as plain text, never
//     as Markdown, with a word joiner after "://", "www." and "@" so
//     nothing in them is made a link.
//   - Webhooks: as JSON strings.
//   - Email: as plain text in a text/plain body.

// cut shortens s to at most n bytes, at a character boundary, marking the
// cut with an ellipsis.
func cut(s string, n int) string {
	if len(s) <= n {
		return s
	}
	i := max(n-len("…"), 0)
	for i > 0 && !utf8.RuneStart(s[i]) {
		i--
	}
	return s[:i] + "…"
}

// capParts cuts parts to n bytes of text in all.
func capParts(parts []Part, n int) []Part {
	var out []Part
	for _, p := range parts {
		if n <= 0 {
			break
		}
		if len(p.Text) > n {
			p.Text = cut(p.Text, n)
		}
		n -= len(p.Text)
		out = append(out, p)
	}
	return out
}

// capped returns m within the caps: title, text, details and result
// (rows, columns, bytes of each value and of all of them). Every
// destination gets at most this much, whatever wrote the message.
func capped(m Message) Message {
	m.Title = cut(m.Title, MaxTitleBytes)
	m.Body = capParts(m.Body, MaxMessageBytes)
	if len(m.Details) > 0 {
		details := make(map[string]string, len(m.Details))
		for k, v := range m.Details {
			details[k] = cut(v, MaxDetailBytes)
		}
		m.Details = details
	}
	if r := m.Result; r != nil {
		c := &Result{Columns: r.Columns, More: r.More, HiddenColumns: r.HiddenColumns}
		if len(c.Columns) > MaxResultColumns {
			c.HiddenColumns += len(c.Columns) - MaxResultColumns
			c.Columns = c.Columns[:MaxResultColumns]
		}
		budget := MaxResultBytes
		for i, row := range r.Rows {
			if i == MaxResultRows || budget <= 0 {
				c.More = true
				break
			}
			out := make([]*string, len(c.Columns))
			for j := range out {
				if j >= len(row) || row[j] == nil {
					continue
				}
				v := "…"
				if budget > 0 {
					v = cut(*row[j], min(MaxValueBytes, budget))
				}
				if len(*row[j]) > budget {
					c.More = true
				}
				budget -= len(v)
				out[j] = &v
			}
			c.Rows = append(c.Rows, out)
		}
		m.Result = c
	}
	return m
}

// sanitize cuts v to MaxValueBytes and writes control characters as
// escapes, so a value is one line.
func sanitize(v string) string {
	v = cut(v, MaxValueBytes)
	if !strings.ContainsFunc(v, isControl) {
		return v
	}
	var b strings.Builder
	for _, r := range v {
		switch {
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case isControl(r):
			b.WriteRune('�')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

func isControl(r rune) bool {
	return r < 0x20 || r == 0x7f || r >= 0x80 && r < 0xa0 || r == ' ' || r == ' '
}

// slackEscape escapes what Slack reads as control sequences.
var slackEscape = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")

// slackValue writes v as inline code.
func slackValue(v string) string {
	v = strings.ReplaceAll(sanitize(v), "`", "ˋ")
	if strings.TrimSpace(v) == "" {
		return "` `"
	}
	return "`" + slackEscape.Replace(v) + "`"
}

// slackText is Slack's message text for m.
func slackText(m Message) string {
	var b strings.Builder
	b.WriteString("*" + slackEscape.Replace(m.Title) + "*\n")
	for _, p := range m.Body {
		if p.Value {
			b.WriteString(slackValue(p.Text))
		} else {
			b.WriteString(p.Text)
		}
	}
	if m.Result != nil {
		b.WriteString("\n```\n")
		for _, line := range table(m.Result, func(v string) string { return slackEscape.Replace(strings.ReplaceAll(v, "`", "ˋ")) }) {
			b.WriteString(line + "\n")
		}
		b.WriteString("```")
	}
	return b.String()
}

func slackPayload(m Message) ([]byte, error) {
	return json.Marshal(map[string]any{
		"text": slackText(m),
		// No previews: Slack would fetch the links a message has.
		"unfurl_links": false,
		"unfurl_media": false,
	})
}

// teamsDefang keeps Teams from making links of a value's text.
var teamsDefang = strings.NewReplacer("://", ":⁠//", "www.", "www⁠.", "WWW.", "WWW⁠.", "@", "@⁠")

func teamsRun(text string, value bool) map[string]any {
	run := map[string]any{"type": "TextRun", "text": text}
	if value {
		run["text"] = teamsDefang.Replace(sanitize(text))
		run["fontType"] = "Monospace"
	}
	return run
}

func teamsPayload(m Message) ([]byte, error) {
	body := []any{map[string]any{
		"type": "RichTextBlock", "inlines": []any{map[string]any{"type": "TextRun", "text": teamsDefang.Replace(m.Title),
			"weight": "Bolder", "size": "Medium"}},
	}}
	if len(m.Body) > 0 {
		var inlines []any
		for _, p := range m.Body {
			inlines = append(inlines, teamsRun(p.Text, p.Value))
		}
		body = append(body, map[string]any{"type": "RichTextBlock", "inlines": inlines})
	}
	if m.Result != nil {
		// A line each, in a fixed-width font; no-break spaces keep the
		// columns lined up.
		for i, line := range table(m.Result, func(v string) string { return teamsDefang.Replace(v) }) {
			block := map[string]any{"type": "RichTextBlock", "spacing": "None", "inlines": []any{map[string]any{
				"type": "TextRun", "text": strings.ReplaceAll(line, " ", " "), "fontType": "Monospace", "size": "Small"}}}
			if i == 0 {
				block["spacing"] = "Medium"
			}
			body = append(body, block)
		}
	}
	return json.Marshal(map[string]any{
		"type": "message",
		"attachments": []any{map[string]any{
			"contentType": "application/vnd.microsoft.card.adaptive",
			"content": map[string]any{
				"$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
				"type":    "AdaptiveCard",
				"version": "1.4",
				"msteams": map[string]any{"width": "Full"},
				"body":    body,
			},
		}},
	})
}

// webhookPayload is the JSON a generic webhook gets. Values are one line
// each, as everywhere else.
func webhookPayload(in Integration, m Message) ([]byte, error) {
	var message strings.Builder
	for _, part := range m.Body {
		if part.Value {
			message.WriteString(sanitize(part.Text))
		} else {
			message.WriteString(part.Text)
		}
	}
	p := map[string]any{
		"type":        m.Type,
		"name":        m.Name,
		"integration": in.Name,
		"title":       m.Title,
		"message":     message.String(),
		"time":        m.Time.UTC().Format(time.RFC3339),
	}
	if m.User != "" {
		p["user"] = m.User
	}
	if len(m.Details) > 0 {
		details := make(map[string]string, len(m.Details))
		for k, v := range m.Details {
			details[k] = strings.Map(func(r rune) rune {
				if isControl(r) && r != '\n' && r != '\t' {
					return '\uFFFD'
				}
				return r
			}, v)
		}
		p["details"] = details
	}
	if r := m.Result; r != nil {
		rows := make([][]*string, len(r.Rows))
		for i, row := range r.Rows {
			rows[i] = make([]*string, len(row))
			for j, v := range row {
				if v != nil {
					s := sanitize(*v)
					rows[i][j] = &s
				}
			}
		}
		p["result"] = map[string]any{"columns": r.Columns, "rows": rows, "more": r.More, "hidden_columns": r.HiddenColumns}
	}
	return json.Marshal(p)
}

// emailContent is an email's subject and plain-text body.
func emailContent(m Message) (subject, body string) {
	subject = strings.Map(func(r rune) rune {
		if isControl(r) {
			return ' '
		}
		return r
	}, m.Title)
	var b strings.Builder
	for _, p := range m.Body {
		if p.Value {
			b.WriteString(sanitize(p.Text))
		} else {
			b.WriteString(p.Text)
		}
	}
	if m.Result != nil {
		b.WriteString("\n\n")
		for _, line := range table(m.Result, func(v string) string { return v }) {
			b.WriteString(line + "\n")
		}
	}
	b.WriteString("\n-- \nSent by Igloo")
	if m.Type == RecordAlert {
		b.WriteString(" for the alert " + m.Name)
	}
	b.WriteString(".\n")
	return subject, b.String()
}

// table lays a result out as lines of text, columns aligned, each value
// sanitized and then passed through esc.
// A table's lines are cut at maxLineBytes, and it stops at maxTableBytes,
// so a wide or long result fits every destination: Teams takes cards of
// about 28 KB.
const (
	maxLineBytes  = 1000
	maxTableBytes = 8 << 10
)

func table(r *Result, esc func(string) string) []string {
	cells := make([][]string, 0, len(r.Rows)+1)
	head := make([]string, len(r.Columns))
	for i, c := range r.Columns {
		head[i] = sanitize(c)
	}
	cells = append(cells, head)
	for _, row := range r.Rows {
		line := make([]string, len(r.Columns))
		for i := range line {
			line[i] = "NULL"
			if i < len(row) && row[i] != nil {
				line[i] = sanitize(*row[i])
			}
		}
		cells = append(cells, line)
	}
	widths := make([]int, len(r.Columns))
	for _, line := range cells {
		for i, v := range line {
			widths[i] = max(widths[i], utf8.RuneCountInString(v))
		}
	}
	pad := func(line []string) string {
		parts := make([]string, len(line))
		for i, v := range line {
			parts[i] = esc(v) + strings.Repeat(" ", widths[i]-utf8.RuneCountInString(v))
		}
		return strings.TrimRight(strings.Join(parts, " | "), " ")
	}
	rule := make([]string, len(widths))
	for i, w := range widths {
		rule[i] = strings.Repeat("-", w)
	}
	out := []string{cut(pad(cells[0]), maxLineBytes), cut(strings.Join(rule, "-+-"), maxLineBytes)}
	size, n, more := len(out[0])+len(out[1]), 0, r.More
	for _, line := range cells[1:] {
		text := cut(pad(line), maxLineBytes)
		if size += len(text); size > maxTableBytes {
			more = true
			break
		}
		out = append(out, text)
		n++
	}
	footer := fmt.Sprintf("(%d rows)", n)
	switch {
	case more && n == 1:
		footer = "(1 row shown; there is more)"
	case more:
		footer = fmt.Sprintf("(%d rows shown; there is more)", n)
	case n == 1:
		footer = "(1 row)"
	}
	if r.HiddenColumns > 0 {
		footer += fmt.Sprintf(" (%d more columns not shown)", r.HiddenColumns)
	}
	return append(out, footer)
}
