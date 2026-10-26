// Package ai runs Igloo's AI functions: AI_COMPLETE, AI_CLASSIFY,
// AI_FILTER, AI_EMBED and the other Cortex AI functions of Snowflake's
// dialect, and AI_DECIDE. Each is a DuckDB scalar function written in Go,
// whose calls to model providers go out from Igloo's own process, through
// Igloo's egress guard, with the keys of an AI integration an admin created:
// never through a DuckDB extension, whose HTTP client Igloo's lockdown of
// DuckDB would not see. Igloo never resells inference: every call is made
// with the customer's own keys, at the provider's prices.
//
// A statement's calls go through a Statement, which keeps them to the
// statement's caps, sends each distinct input once however many rows and
// DuckDB threads ask for it, and keeps results in the cache, which a later
// statement asking the same of the same model reads instead of calling the
// provider again. Cached results are tagged with the tables their
// statement read, which an erasure request purges (see Store.Purge).
package ai

import (
	"errors"
	"fmt"
	"strings"
)

// Errors a Statement fails with, rather than giving one row NULL.
var (
	// ErrOff means no AI integration exists: AI functions are off.
	ErrOff = errors.New("AI functions are off: no AI integration exists; an admin creates one with CREATE AI INTEGRATION " +
		"(see docs/ai-functions.md)")
	// ErrCap is a statement that reached its cap on calls or cost.
	ErrCap = errors.New("the statement reached its cap on AI calls")
	// ErrDenied is a user without USAGE on any integration that can serve
	// the call.
	ErrDenied = errors.New("permission denied")
	// ErrModel is a model name no integration maps.
	ErrModel = errors.New("unknown model")
	// ErrInvalid is an argument or setting that cannot be used.
	ErrInvalid = errors.New("invalid")
	// ErrUnsupported is something a provider cannot do, such as
	// embeddings from Anthropic.
	ErrUnsupported = errors.New("not supported")
	// ErrNotFound is an integration that does not exist.
	ErrNotFound = errors.New("does not exist")
	// ErrExists is an integration whose name is taken.
	ErrExists = errors.New("already exists")
	// ErrNoStatement is a call made outside a statement Igloo set up for
	// AI functions.
	ErrNoStatement = errors.New("AI functions run only in statements Igloo runs for a session: a query, INSERT, " +
		"CREATE TABLE ... AS or a task's statement; not in a cursor, COPY ... TO or a statement run on workers")
)

// RowError is a call that failed for its own input, as a provider refusing
// it or answering with something the function cannot read: the row gets
// NULL, or its error with return_error_details. Errors of any other kind
// fail the statement.
type RowError struct{ Msg string }

func (e *RowError) Error() string { return e.Msg }

func rowErrorf(format string, args ...any) error { return &RowError{Msg: fmt.Sprintf(format, args...)} }

// IsRowError reports whether err is a RowError.
func IsRowError(err error) bool {
	var r *RowError
	return errors.As(err, &r)
}

// Usage is what calls to a provider used, as it reported them.
type Usage struct {
	InputTokens, OutputTokens int64
	// CachedInputTokens are input tokens the provider read from its own
	// prompt cache, which it bills for less (counted in InputTokens too).
	CachedInputTokens int64
}

func (u *Usage) add(v Usage) {
	u.InputTokens += v.InputTokens
	u.OutputTokens += v.OutputTokens
	u.CachedInputTokens += v.CachedInputTokens
}

// Spend is what a statement's AI functions did, for the query history and
// igloo.usage.
type Spend struct {
	// Calls are requests sent to providers; CacheHits the distinct inputs
	// answered from the cache; RowErrors the rows that got NULL or an error
	// for their own input.
	Calls, CacheHits, RowErrors int64
	Usage
	// CostUSD is the estimated cost, from the integrations' prices; Unpriced
	// counts calls to models without a price, not in CostUSD.
	CostUSD  float64
	Unpriced int64
	// BytesSent is the size of the requests' bodies.
	BytesSent int64
	// Models are the provider models called, as integration/model, and
	// ByModel what was sent to each.
	Models  []string
	ByModel map[string]ModelSpend
	// FirstError is the first row error, for the notice that tells of them.
	FirstError string
}

// ModelSpend is what a statement sent to one model.
type ModelSpend struct {
	Calls, InputTokens, OutputTokens int64
	// CostUSD is at the integration's price; Priced says it has one.
	CostUSD float64
	Priced  bool
}

// Empty reports whether the statement used no AI function at all.
func (s Spend) Empty() bool {
	return s.Calls == 0 && s.CacheHits == 0 && s.RowErrors == 0
}

// quote quotes a SQL string literal, for messages.
func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
