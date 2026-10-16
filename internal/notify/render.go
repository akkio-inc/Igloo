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

// sanitize cuts v to MaxValueBytes and writes control characters as
// escapes, so a value is one line.
func sanitize(v string) string {
	if len(v) > MaxValueBytes {
		cut := MaxValueBytes
		for cut > 0 && !utf8.RuneStart(v[cut]) {
			cut--
		}
		v = v[:cut] + "…"
	}
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

// webhookPayload is the JSON a generic webhook gets.
func webhookPayload(in Integration, m Message) ([]byte, error) {
	p := map[string]any{
		"type":        m.Type,
		"name":        m.Name,
		"integration": in.Name,
		"title":       m.Title,
		"message":     plain(m.Body),
		"time":        m.Time.UTC().Format(time.RFC3339),
	}
	if m.User != "" {
		p["user"] = m.User
	}
	if len(m.Details) > 0 {
		p["details"] = m.Details
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
		p["result"] = map[string]any{"columns": r.Columns, "rows": rows, "more": r.More}
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
	out := []string{pad(cells[0])}
	rule := make([]string, len(widths))
	for i, w := range widths {
		rule[i] = strings.Repeat("-", w)
	}
	out = append(out, strings.Join(rule, "-+-"))
	for _, line := range cells[1:] {
		out = append(out, pad(line))
	}
	switch n := len(r.Rows); {
	case r.More && n == 1:
		out = append(out, "(1 row shown; there are more)")
	case r.More:
		out = append(out, fmt.Sprintf("(%d rows shown; there are more)", n))
	case n == 1:
		out = append(out, "(1 row)")
	default:
		out = append(out, fmt.Sprintf("(%d rows)", n))
	}
	return out
}
