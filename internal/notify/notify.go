// Package notify sends Igloo's notifications: what alerts find, messages
// sent with SYSTEM$SEND_NOTIFICATION, and the built-in operational alerts,
// to webhooks, Slack, Microsoft Teams and email. It keeps the notification
// integrations admins create, who may use them, and the alert history.
//
// Every connection it makes goes through a Guard, which reaches only
// public addresses, over TLS, unless IGLOO_NOTIFICATION_ALLOW_HOSTS names
// the host. The Guard resolves the host for every connection and connects
// to the address it checked, so a name cannot be pointed at an internal
// address between a check and a connection. Integrations' URLs, headers
// and signing secrets are stored encrypted, shown to nobody, and replaced
// by *** in errors.
package notify

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Types of integration. Slack and Teams are incoming webhooks that get the
// payload their service expects.
const (
	TypeWebhook = "webhook"
	TypeSlack   = "slack"
	TypeTeams   = "teams"
	TypeEmail   = "email"
)

// Types lists the integration types.
var Types = []string{TypeWebhook, TypeSlack, TypeTeams, TypeEmail}

var (
	// ErrNotFound is returned for an integration that does not exist.
	ErrNotFound = errors.New("does not exist")
	// ErrExists is returned for an integration whose name is taken.
	ErrExists = errors.New("already exists")
	// ErrRateLimited is returned by Send when an integration has been sent
	// more than it may be.
	ErrRateLimited = errors.New("rate limited")
	// ErrInvalid is wrapped by errors about an integration's settings.
	ErrInvalid = errors.New("invalid")
)

// Integration is somewhere notifications go.
type Integration struct {
	// Name is a lowercase SQL name.
	Name    string
	Type    string
	Enabled bool
	// URL is where a webhook, Slack or Teams integration posts. It, Headers
	// and Secret are stored encrypted and never shown: incoming webhook
	// URLs carry their token.
	URL string
	// Headers are sent with every request of a webhook integration, such
	// as Authorization.
	Headers map[string]string
	// Secret, for a webhook integration, signs each request's body.
	Secret string
	// Recipients are the addresses an email integration sends to.
	Recipients []string
	// Builtin are the operational alerts it receives.
	Builtin   Builtin
	Comment   string
	CreatedBy string
	CreatedAt time.Time
}

// Target describes where in sends, without secrets: a webhook's scheme and
// host, or an email integration's recipients.
func (in Integration) Target() string {
	if in.Type == TypeEmail {
		return strings.Join(in.Recipients, ", ")
	}
	u, err := url.Parse(in.URL)
	if err != nil || u.Host == "" {
		return ""
	}
	return u.Scheme + "://" + u.Host
}

// secrets lists what of in must never be shown.
func (in Integration) secrets() []string {
	out := []string{in.URL, in.Secret}
	if u, err := url.Parse(in.URL); err == nil {
		out = append(out, u.EscapedPath(), u.Path, u.RawQuery, u.Fragment)
		for _, vs := range u.Query() {
			out = append(out, vs...)
		}
	}
	for _, v := range in.Headers {
		out = append(out, v)
	}
	return out
}

// Scrub replaces the integration's secrets in s with ***.
func (in Integration) Scrub(s string) string { return scrub(s, in.secrets()) }

// scrub replaces each secret in s with ***. Values shorter than six bytes,
// such as a header's "1" or a path of "/", are left alone: they are not
// secret, and replacing them would garble messages.
func scrub(s string, secrets []string) string {
	secrets = slices.DeleteFunc(slices.Clone(secrets), func(x string) bool { return len(x) < 6 })
	// Longer first, so a secret containing another is replaced whole.
	slices.SortFunc(secrets, func(a, b string) int { return len(b) - len(a) })
	for _, x := range secrets {
		s = strings.ReplaceAll(s, x, "***")
	}
	return s
}

// Names of the built-in operational alerts, as OPERATIONAL_ALERTS lists
// them and igloo.alert_history names them.
const (
	BuiltinFailedRuns   = "failed_runs"
	BuiltinLoadErrors   = "load_errors"
	BuiltinIdlePipes    = "idle_pipes"
	BuiltinLongQueries  = "long_queries"
	BuiltinFailedLogins = "failed_logins"
	BuiltinMaintenance  = "maintenance_failures"
)

// Thresholds of the built-in alerts that take one: idle pipes in hours,
// long queries in minutes.
const (
	defaultIdlePipeHours    = 24
	maxIdlePipeHours        = 24 * 30
	defaultLongQueryMinutes = 60
	maxLongQueryMinutes     = 24 * 60 * 7
)

// Builtin are the operational alerts an integration receives.
type Builtin struct {
	FailedRuns bool
	LoadErrors bool
	// IdlePipes is how long a started pipe may go without loading files;
	// zero is off.
	IdlePipes time.Duration
	// LongQueries is how long a statement may run; zero is off.
	LongQueries         time.Duration
	FailedLogins        bool
	MaintenanceFailures bool
}

// Empty reports whether b sends nothing.
func (b Builtin) Empty() bool { return b == Builtin{} }

// Has reports whether b includes the built-in alert called name.
func (b Builtin) Has(name string) bool {
	switch name {
	case BuiltinFailedRuns:
		return b.FailedRuns
	case BuiltinLoadErrors:
		return b.LoadErrors
	case BuiltinIdlePipes:
		return b.IdlePipes > 0
	case BuiltinLongQueries:
		return b.LongQueries > 0
	case BuiltinFailedLogins:
		return b.FailedLogins
	case BuiltinMaintenance:
		return b.MaintenanceFailures
	}
	return false
}

// Set turns on the built-in alert called name, as OPERATIONAL_ALERTS
// names it. n is idle_pipes' hours or long_queries' minutes, when given.
func (b *Builtin) Set(name string, n int, given bool) error {
	name = strings.ToLower(name)
	takes := name == BuiltinIdlePipes || name == BuiltinLongQueries
	if given && !takes {
		return fmt.Errorf("%w: %s takes no value", ErrInvalid, strings.ToUpper(name))
	}
	switch name {
	case BuiltinFailedRuns:
		b.FailedRuns = true
	case BuiltinLoadErrors:
		b.LoadErrors = true
	case BuiltinFailedLogins:
		b.FailedLogins = true
	case BuiltinMaintenance:
		b.MaintenanceFailures = true
	case BuiltinIdlePipes:
		if !given {
			n = defaultIdlePipeHours
		}
		if n < 1 || n > maxIdlePipeHours {
			return fmt.Errorf("%w: IDLE_PIPES takes a number of hours from 1 to %d", ErrInvalid, maxIdlePipeHours)
		}
		b.IdlePipes = time.Duration(n) * time.Hour
	case BuiltinLongQueries:
		if !given {
			n = defaultLongQueryMinutes
		}
		if n < 1 || n > maxLongQueryMinutes {
			return fmt.Errorf("%w: LONG_QUERIES takes a number of minutes from 1 to %d", ErrInvalid, maxLongQueryMinutes)
		}
		b.LongQueries = time.Duration(n) * time.Minute
	default:
		return fmt.Errorf("%w: unknown operational alert %s; they are FAILED_RUNS, LOAD_ERRORS, IDLE_PIPES [= hours], "+
			"LONG_QUERIES [= minutes], FAILED_LOGINS and MAINTENANCE_FAILURES", ErrInvalid, strings.ToUpper(name))
	}
	return nil
}

// String lists b as OPERATIONAL_ALERTS takes it, such as
// "failed_runs, idle_pipes = 24".
func (b Builtin) String() string {
	var out []string
	if b.FailedRuns {
		out = append(out, BuiltinFailedRuns)
	}
	if b.LoadErrors {
		out = append(out, BuiltinLoadErrors)
	}
	if b.IdlePipes > 0 {
		out = append(out, BuiltinIdlePipes+" = "+strconv.Itoa(int(b.IdlePipes/time.Hour)))
	}
	if b.LongQueries > 0 {
		out = append(out, BuiltinLongQueries+" = "+strconv.Itoa(int(b.LongQueries/time.Minute)))
	}
	if b.FailedLogins {
		out = append(out, BuiltinFailedLogins)
	}
	if b.MaintenanceFailures {
		out = append(out, BuiltinMaintenance)
	}
	return strings.Join(out, ", ")
}

// ParseBuiltin reads what String writes.
func ParseBuiltin(s string) (Builtin, error) {
	var b Builtin
	for _, item := range strings.Split(s, ",") {
		if item = strings.TrimSpace(item); item == "" {
			continue
		}
		name, value, given := strings.Cut(item, "=")
		n := 0
		if given {
			var err error
			if n, err = strconv.Atoi(strings.TrimSpace(value)); err != nil {
				return b, fmt.Errorf("%w: operational alert %q", ErrInvalid, item)
			}
		}
		if err := b.Set(strings.TrimSpace(name), n, given); err != nil {
			return b, err
		}
	}
	return b, nil
}

// Usage lets a user, or the members of a custom role, send to an
// integration: in alerts, and with SYSTEM$SEND_NOTIFICATION.
type Usage struct {
	Integration string
	Grantee     string
	// GranteeType is auth.GranteeUser or auth.GranteeRole.
	GranteeType string
	GrantedBy   string
	GrantedAt   time.Time
}

// Types of alert history record.
const (
	// RecordAlert is a run of an alert.
	RecordAlert = "alert"
	// RecordBuiltin is a notification of a built-in operational alert.
	RecordBuiltin = "builtin"
	// RecordCall is a message sent with SYSTEM$SEND_NOTIFICATION.
	RecordCall = "call"
)

// States of an alert history record, as in Snowflake's ALERT_HISTORY.
const (
	// StateConditionFalse is a run whose condition returned no rows.
	StateConditionFalse = "condition_false"
	// StateConditionFailed is a run whose condition failed.
	StateConditionFailed = "condition_failed"
	// StateTriggered is a notification that was sent.
	StateTriggered = "triggered"
	// StateActionFailed is a notification that could not be sent.
	StateActionFailed = "action_failed"
	// StateSkipped is a run that did not evaluate its condition, because
	// its owner is disabled or dropped.
	StateSkipped = "skipped"
)

// Triggers of an alert history record, besides tasks' schedule and manual.
const TriggerEvent = "event"

// Record is one row of igloo.alert_history: a run of an alert, a built-in
// alert's notification, or a message sent with SYSTEM$SEND_NOTIFICATION.
type Record struct {
	ID string
	// Type is RecordAlert, RecordBuiltin or RecordCall.
	Type string
	// Name is the alert's, the built-in alert's, or the task's that sent a
	// message; empty for messages sent from a session.
	Name string
	// User is whom it ran as; empty for built-in alerts.
	User      string
	Trigger   string
	Scheduled time.Time
	Start     time.Time
	End       time.Time
	State     string
	// Rows is how many rows the condition returned, or -1.
	Rows        int64
	Integration string
	// Attempts is how many times sending was tried.
	Attempts int
	Error    string
}

// Caps on what a notification carries of a result.
const (
	// MaxResultRows is the most rows a notification shows.
	MaxResultRows = 20
	// MaxResultBytes caps the bytes of all values shown.
	MaxResultBytes = 16 << 10
	// MaxValueBytes caps each value; longer ones are cut.
	MaxValueBytes = 500
)

// Part is a stretch of a message's text. Text written by the alert's
// owner, or by Igloo, is sent as written; a Value, which comes from data
// (a cell of a result, a file name, an error), is escaped for the
// destination so that it cannot add mentions, links or formatting.
type Part struct {
	Text  string
	Value bool
}

// Text is a Part sent as written.
func Text(s string) Part { return Part{Text: s} }

// Value is a Part from data.
func Value(s string) Part { return Part{Text: s, Value: true} }

// Result is part of a query result, for a notification to show.
type Result struct {
	Columns []string
	// Rows are values in PostgreSQL's text format; nil is NULL.
	Rows [][]*string
	// More says the query returned more rows than Rows.
	More bool
}

// Message is what a notification says.
type Message struct {
	// Type and Name say what sends it, as in a Record.
	Type string
	Name string
	// User is whom an alert ran as, or who sent a message.
	User string
	// Title is a heading Igloo writes, of plain text.
	Title string
	Body  []Part
	// Details are the facts of a built-in alert, for webhooks.
	Details map[string]string
	Result  *Result
	Time    time.Time
}

// Template turns an alert's MESSAGE into parts: its text as written, and
// each {{column}} replaced by that column's value in row, as a Value.
// Placeholders that name no column are left as written.
func Template(tmpl string, columns []string, row []*string) []Part {
	var parts []Part
	for tmpl != "" {
		start := strings.Index(tmpl, "{{")
		if start < 0 {
			break
		}
		end := strings.Index(tmpl[start:], "}}")
		if end < 0 {
			break
		}
		end += start
		name := strings.TrimSpace(tmpl[start+2 : end])
		i := slices.IndexFunc(columns, func(c string) bool { return strings.EqualFold(c, name) })
		if i < 0 || i >= len(row) {
			parts = append(parts, Text(tmpl[:end+2]))
			tmpl = tmpl[end+2:]
			continue
		}
		if start > 0 {
			parts = append(parts, Text(tmpl[:start]))
		}
		v := "NULL"
		if row[i] != nil {
			v = *row[i]
		}
		parts = append(parts, Value(v))
		tmpl = tmpl[end+2:]
	}
	if tmpl != "" {
		parts = append(parts, Text(tmpl))
	}
	return parts
}

// Placeholders lists the column names a template's {{name}}s use.
func Placeholders(tmpl string) []string {
	var out []string
	for {
		start := strings.Index(tmpl, "{{")
		if start < 0 {
			return out
		}
		end := strings.Index(tmpl[start:], "}}")
		if end < 0 {
			return out
		}
		out = append(out, strings.TrimSpace(tmpl[start+2:start+end]))
		tmpl = tmpl[start+end+2:]
	}
}

// plain joins parts as plain text, values unescaped.
func plain(parts []Part) string {
	var b strings.Builder
	for _, p := range parts {
		b.WriteString(p.Text)
	}
	return b.String()
}
