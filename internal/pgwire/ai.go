package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/ai"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// AI functions (package ai, docs/ai-functions.md). They are off until an
// admin creates an AI integration, which holds a provider's keys:
//
//	CREATE [OR REPLACE] AI INTEGRATION [IF NOT EXISTS] name PROVIDER [=] openai | azure_openai | anthropic | gemini | vertex | bedrock | decisions [option ...]
//	ALTER AI INTEGRATION [IF EXISTS] name SET option ... | UNSET name [, ...]
//	DROP [AI] INTEGRATION [IF EXISTS] name
//	SHOW AI INTEGRATIONS
//	GRANT USAGE ON INTEGRATION name TO [ROLE | USER] grantee
//	REVOKE USAGE ON INTEGRATION name FROM [ROLE | USER] grantee
//
// with the options BASE_URL, API_KEY, HEADERS ('name' = 'value', ...),
// AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN,
// SERVICE_ACCOUNT, API_VERSION, REGION, PROJECT, MODELS ('sql name' =
// 'provider model', ...), DEFAULT_MODEL, EMBEDDING_MODEL, DECISION_MODEL,
// PRICES ('model' = (input, output), ...), in US dollars per million
// tokens, MAX_CONCURRENCY n, ENABLED TRUE | FALSE and COMMENT. AI and
// notification integrations share one set of names, as Snowflake's
// integrations do: GRANT, REVOKE and DROP INTEGRATION reach whichever has
// the name. Only admins manage integrations; users need USAGE on one to
// call it.
//
// A statement that calls an AI function binds it while it is prepared,
// which the session notes (aiHook); before such a statement runs, Igloo
// starts its ai.Statement, with the tables it reads, its caps and the
// cache's erasure epoch, and the functions find it through the session.

// aiHook connects the engine's AI functions to sessions (ai.Hook).
type aiHook struct{}

// Bind notes that the statement the session is preparing or running calls
// an AI function, and returns how the call finds the session's Statement.
func (aiHook) Bind(ctx context.Context) func() *ai.Statement {
	sess := sessionFrom(ctx)
	if sess == nil {
		return nil
	}
	sess.aiBound.Store(true)
	return sess.aiRun.Load
}

// AIHook is the hook the engine's AI functions find sessions with
// (engine.Engine.SetAI).
func (s *Server) AIHook() ai.Hook { return aiHook{} }

// aiError gives an AI functions' error its SQLSTATE.
func aiError(err error) error {
	if err == nil || pgerror.GetCode(err) != codes.Uncategorized {
		return err
	}
	switch ai.ErrorCode(err) {
	case "cap":
		return pgerror.WithCode(err, codes.ProgramLimitExceeded)
	case "denied":
		return pgerror.WithCode(err, codes.InsufficientPrivilege)
	case "off":
		return pgerror.WithCode(err, codes.ObjectNotInPrerequisiteState)
	case "model":
		return pgerror.WithCode(err, codes.UndefinedObject)
	case "invalid":
		return pgerror.WithCode(err, codes.InvalidParameterValue)
	}
	if errors.Is(err, ai.ErrNoStatement) {
		return pgerror.WithCode(err, codes.FeatureNotSupported)
	}
	return pgerror.WithCode(err, codes.Io)
}

// plainExplain returns the query st explains, when st is EXPLAIN, not
// EXPLAIN ANALYZE, of a query: its AI functions are counted rather than
// called (see explainAI).
func plainExplain(st *statement) (string, bool) {
	if st.stmtType != duckdb.STATEMENT_TYPE_EXPLAIN {
		return "", false
	}
	sc := &scanner{s: st.query}
	if !sc.keywords("EXPLAIN") || sc.keywords("ANALYZE") || sc.keywords("ANALYSE") {
		return "", false
	}
	query := strings.TrimSpace(sc.rest())
	return query, startsQuery(query) || sc.keywords("WITH")
}

// explainAI runs query, which a plain EXPLAIN explains, with the
// statement's AI functions in a dry run, which counts the distinct inputs
// they would send and reads the cache, and returns what EXPLAIN adds: for
// each model, the calls, the estimated tokens and, where the integration
// has a price, the estimated cost.
func (s *Server) explainAI(ctx context.Context, sess *session, aist *ai.Statement, query string, args []driver.NamedValue) [][]any {
	rows, err := sess.conn.QueryContext(ctx, query, args)
	if err == nil {
		dest := make([]driver.Value, len(rows.Columns()))
		for err == nil {
			err = rows.Next(dest)
		}
		if errors.Is(err, io.EOF) {
			err = nil
		}
		_ = rows.Close()
	}
	sp := aist.End()
	var out [][]any
	for _, m := range sp.Models {
		ms := sp.ByModel[m]
		price := "no price set (ALTER AI INTEGRATION ... SET PRICES)"
		if ms.Priced {
			price = fmt.Sprintf("about $%.4f", ms.CostUSD)
		}
		out = append(out, []any{"ai calls", fmt.Sprintf("%s: %d distinct inputs to send, about %d input and at most %d output tokens, %s",
			m, ms.Calls, ms.InputTokens, ms.OutputTokens, price)})
	}
	if sp.CacheHits > 0 {
		out = append(out, []any{"ai cache", fmt.Sprintf("%d distinct inputs answered from the cache, at no cost", sp.CacheHits)})
	}
	calls, cost, _ := s.aiCaps(sess)
	out = append(out, []any{"ai caps", fmt.Sprintf("at most %d calls and $%g a statement (0 is no cap)", calls, cost)})
	note := "counted by running the query without calling a provider; tokens are estimated"
	if err != nil {
		note = "counting stopped: " + firstLine(err.Error(), 200)
	}
	return append(out, []any{"ai estimate", note})
}

// extraRows sends rows after a statement's own, before it completes.
type extraRows struct {
	wire.DataWriter
	rows [][]any
}

func (e *extraRows) Complete(tag string) error {
	for _, r := range e.rows {
		if err := e.DataWriter.Row(r); err != nil {
			return err
		}
	}
	return e.DataWriter.Complete(tag)
}

// beginAI starts the AI functions of st, which calls them, as it is about
// to run: before it binds again and reads its tables, so the cache's
// epoch it reads is from before any row it reads (see ai.Store.Purge). A
// dry run counts their calls rather than sending them (see explainAI).
func (s *Server) beginAI(ctx context.Context, sess *session, st *statement, dryRun bool) (*ai.Statement, error) {
	if s.ai == nil {
		return nil, aiError(ai.ErrOff)
	}
	user := sess.currentUser()
	tables := s.aiTables(ctx, sess, st)
	if err := s.aiConfined(ctx, tables); err != nil {
		return nil, err
	}
	privs, err := s.privileges(ctx, sess, user)
	if err != nil {
		return nil, denied("could not load roles: %v", err)
	}
	o := ai.StatementOptions{User: user.Name, Roles: privs.Roles, Admin: user.Role == auth.RoleAdmin,
		Integration: sess.aiIntegration, Tables: tables, Threshold: sess.aiThreshold, DryRun: dryRun}
	o.MaxCalls, o.MaxCostUSD, o.CapSource = s.aiCaps(sess)
	aist, err := s.ai.Begin(ctx, o)
	if err != nil {
		return nil, aiError(err)
	}
	return aist, nil
}

// aiRunning runs fn with aist as the session's statement's AI functions'
// Statement, under ctx, and returns fn's error, or the error that failed
// aist, which DuckDB reports in its own words.
func (sess *session) aiRunning(ctx context.Context, aist *ai.Statement, fn func() error) error {
	if aist == nil {
		return fn()
	}
	aist.Run(ctx)
	sess.aiRun.Store(aist)
	defer sess.aiRun.Store(nil)
	err := fn()
	if err != nil {
		if failed := aist.Failed(); failed != nil && ctx.Err() == nil {
			return aiError(failed)
		}
	}
	return err
}

// aiNotice tells the client of the rows AI functions gave NULL or an error
// for their own input.
func aiNotice(w wire.DataWriter, sp ai.Spend) {
	if sp.RowErrors == 0 {
		return
	}
	if n, ok := w.(wire.Noticer); ok {
		_ = n.Notice("01000", fmt.Sprintf("AI functions gave %d rows NULL or an error for their own input; the first: %s",
			sp.RowErrors, firstLine(sp.FirstError, 300)))
	}
}

func firstLine(s string, n int) string {
	if i := strings.IndexAny(s, "\r\n"); i >= 0 {
		s = s[:i]
	}
	if len(s) > n {
		s = s[:n] + "..."
	}
	return s
}

// aiCaps are the statement's caps on AI calls and cost: the user's roles'
// ai_max_calls and ai_max_cost_usd, or the server's defaults, lowered by
// the session's own.
func (s *Server) aiCaps(sess *session) (calls int64, cost float64, source string) {
	l := sess.wl.limits
	calls, cost = s.aiMaxCalls, s.aiMaxCost
	sources := []string{"the server's IGLOO_AI_MAX_CALLS", "the server's IGLOO_AI_MAX_COST_USD"}
	if l.AISet[0] {
		calls, sources[0] = l.AIMaxCalls, l.AISource[0]+"'s ai_max_calls"
	}
	if l.AISet[1] {
		cost, sources[1] = l.AIMaxCostUSD, l.AISource[1]+"'s ai_max_cost_usd"
	}
	if c := sess.aiMaxCalls; c > 0 && (calls == 0 || c < calls) {
		calls, sources[0] = c, "the session's ai_max_calls"
	}
	if c := sess.aiMaxCost; c > 0 && (cost == 0 || c < cost) {
		cost, sources[1] = c, "the session's ai_max_cost_usd"
	}
	return calls, cost, strings.Join(sources, "; ")
}

// readsFiles matches statements that may read files rather than tables,
// whose AI results cannot be tied to the tables they come from.
var readsFiles = func() *regexp.Regexp {
	names := slices.Sorted(maps.Keys(fileFunctions))
	names = append(names, "iceberg_scan", "delta_scan", "read_xlsx", "query_table", "query")
	return regexp.MustCompile(`(?i)\b(` + strings.Join(names, "|") + `)\s*\(`)
}()

// aiTables are the catalog tables st reads, namespace.name in lower case,
// which tag what its AI functions cache: those DuckDB loaded as it prepared
// it, those grants checked, and those its parse names, views expanded. A
// statement that may read files, or that Igloo could not analyze, is also
// tagged ai.AnyTable, which every erasure request purges.
func (s *Server) aiTables(ctx context.Context, sess *session, st *statement) []string {
	if s.catalogName() == "" {
		return nil
	}
	var out []string
	for _, l := range st.loads {
		out = append(out, l.Table())
	}
	if st.access != nil {
		for _, o := range st.access.read {
			if o.Catalog == "" && o.Table != "" {
				out = append(out, o.Schema+"."+o.Table)
			}
		}
	}
	if readsFiles.MatchString(sqllex.MapCode(st.query, func(code string) string { return code })) {
		out = append(out, ai.AnyTable)
	}
	if s.analyzer != nil {
		names, err := s.names(ctx, sess)
		var objs *statementObjects
		if err == nil {
			objs = s.analyzer.objects(analysis{text: st.sourceText(), kind: st.stmtType, names: names})
		}
		switch {
		case objs == nil && len(st.loads) == 0:
			out = append(out, ai.AnyTable)
		case objs != nil:
			for _, o := range objs.read {
				if o.Catalog == "" && o.Table != "" {
					out = append(out, o.Schema+"."+o.Table)
				}
			}
			if len(objs.locations) > 0 {
				out = append(out, ai.AnyTable)
			}
		}
	}
	return ai.Tables(out)
}

// aiConfined refuses AI functions in a statement that reads a table a
// clean room confines: its rows are for the room's approved templates
// alone, not for a model provider.
func (s *Server) aiConfined(ctx context.Context, tables []string) error {
	snap := s.gov.snapshot(ctx)
	if snap == nil {
		return nil
	}
	for _, t := range tables {
		i := strings.LastIndexByte(t, '.')
		if i < 0 {
			continue
		}
		if r := snap.Rules(t[:i], t[i+1:]); r != nil && len(r.Confined) > 0 {
			return denied("%s is in clean room %s, whose tables AI functions may not read: their rows would go to a model provider",
				t, strings.Join(r.Rooms(), ", "))
		}
	}
	return nil
}

// --- integrations -----------------------------------------------------------

// aiCommand is a parsed statement on AI integrations.
type aiCommand struct {
	verb string // CREATE, ALTER, DROP, SHOW, GRANT or REVOKE
	name string
	// generic says the statement names an integration of either kind
	// (DROP INTEGRATION, GRANT and REVOKE USAGE ON INTEGRATION).
	generic                        bool
	replace, ifExists, ifNotExists bool
	set                            aiOptions
	unset                          []string
	grantee                        string
}

// aiOptions are an integration's settings as a statement gives them; nil
// means not given.
type aiOptions struct {
	provider, baseURL, apiKey, apiVersion, region, project *string
	awsKeyID, awsSecret, awsToken, serviceAccount          *string
	defaultModel, embeddingModel, decisionModel, comment   *string
	headers, models                                        map[string]string
	prices                                                 map[string]ai.Price
	maxConcurrency                                         *int
	enabled                                                *bool
}

func aiSyntax(format string, args ...any) error {
	return pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
}

// parseAICommand recognizes the statements above. ok is false for any
// other; err reports a malformed one.
func parseAICommand(query string) (c aiCommand, ok bool, err error) {
	sc := &scanner{s: query}
	switch {
	case sc.keywords("CREATE"):
		c.replace = sc.keywords("OR", "REPLACE")
		if !sc.keywords("AI", "INTEGRATION") {
			return c, false, nil
		}
		c.verb = "CREATE"
		c.ifNotExists = sc.keywords("IF", "NOT", "EXISTS")
	case sc.keywords("ALTER", "AI", "INTEGRATION"):
		c.verb = "ALTER"
		c.ifExists = sc.keywords("IF", "EXISTS")
	case sc.keywords("DROP", "AI", "INTEGRATION"):
		c.verb = "DROP"
		c.ifExists = sc.keywords("IF", "EXISTS")
	case sc.keywords("DROP", "INTEGRATION"):
		c.verb, c.generic = "DROP", true
		c.ifExists = sc.keywords("IF", "EXISTS")
	case sc.keywords("SHOW", "AI", "INTEGRATIONS"):
		c.verb = "SHOW"
		if !sc.done() {
			return c, true, aiSyntax("unexpected text after SHOW AI INTEGRATIONS")
		}
		return c, true, nil
	case sc.keywords("GRANT", "USAGE", "ON", "INTEGRATION"):
		c.verb, c.generic = "GRANT", true
	case sc.keywords("REVOKE", "USAGE", "ON", "INTEGRATION"):
		c.verb, c.generic = "REVOKE", true
	default:
		return c, false, nil
	}
	if c.name, err = integrationName(sc); err != nil {
		return c, true, err
	}
	switch c.verb {
	case "CREATE":
		err = parseAIOptions(sc, &c.set)
		if err == nil && c.set.provider == nil {
			err = aiSyntax("CREATE AI INTEGRATION needs a PROVIDER: %s", strings.Join(ai.Providers, ", "))
		}
		return c, true, err
	case "ALTER":
		switch {
		case sc.keywords("SET"):
			err = parseAIOptions(sc, &c.set)
		case sc.keywords("UNSET"):
			for {
				w := sc.word()
				if w == "" {
					return c, true, aiSyntax("expected the setting to UNSET, such as UNSET API_KEY")
				}
				sc.i += len(w)
				c.unset = append(c.unset, w)
				if !sc.punct(',') {
					break
				}
			}
		default:
			return c, true, aiSyntax("expected SET or UNSET after ALTER AI INTEGRATION %s", c.name)
		}
		if err == nil && !sc.done() {
			err = aiSyntax("unexpected text after ALTER AI INTEGRATION %s: %q", c.name, strings.TrimSpace(sc.rest()))
		}
		return c, true, err
	case "GRANT", "REVOKE":
		to := "TO"
		if c.verb == "REVOKE" {
			to = "FROM"
		}
		if !sc.keywords(to) {
			return c, true, aiSyntax("expected %s USAGE ON INTEGRATION %s %s role_or_user", c.verb, c.name, to)
		}
		_ = sc.keywords("ROLE") || sc.keywords("USER")
		name, ok := sc.name()
		if !ok || len(name) != 1 || !config.ValidName(name[0]) {
			return c, true, aiSyntax("expected the role or user %s %s", strings.ToLower(to), c.verb)
		}
		c.grantee = name[0]
	}
	if !sc.done() {
		return c, true, aiSyntax("unexpected text after %s ... INTEGRATION %s: %q", c.verb, c.name, strings.TrimSpace(sc.rest()))
	}
	return c, true, nil
}

// parseAIOptions reads option [=] value ..., separated by spaces or
// commas, until the statement ends.
func parseAIOptions(sc *scanner, o *aiOptions) error {
	seen := map[string]bool{}
	for !sc.done() {
		sc.punct(',')
		key := sc.word()
		if key == "" {
			return aiSyntax("expected an option, such as API_KEY '...', at %q", strings.TrimSpace(sc.rest()))
		}
		sc.i += len(key)
		if seen[key] {
			return aiSyntax("conflicting or redundant option %s", key)
		}
		seen[key] = true
		sc.punct('=')
		str := func() (*string, error) {
			v, ok := sc.literal()
			if !ok || v == nil {
				return nil, aiSyntax("%s takes a string", key)
			}
			return v, nil
		}
		// A word or a string, as PROVIDER = openai or 'openai'.
		wordOrString := func() (*string, error) {
			if w := sc.word(); w != "" {
				sc.i += len(w)
				v := strings.ToLower(w)
				return &v, nil
			}
			v, err := str()
			if v != nil {
				lower := strings.ToLower(*v)
				v = &lower
			}
			return v, err
		}
		pairs := func(into map[string]string, lower bool) error {
			return parseList(sc, key, func() error {
				k, ok := sc.literal()
				if !ok || k == nil || !sc.punct('=') {
					return aiSyntax("%s takes ('name' = 'value', ...)", key)
				}
				v, ok := sc.literal()
				if !ok || v == nil {
					return aiSyntax("%s takes ('name' = 'value', ...)", key)
				}
				name := *k
				if lower {
					name = strings.ToLower(strings.TrimSpace(name))
				}
				if _, dup := into[name]; dup {
					return aiSyntax("%s gives %s twice", key, quoteLiteral(name))
				}
				into[name] = *v
				return nil
			})
		}
		var err error
		switch key {
		case "PROVIDER", "TYPE":
			o.provider, err = wordOrString()
		case "BASE_URL", "ENDPOINT":
			o.baseURL, err = str()
		case "API_KEY":
			o.apiKey, err = str()
		case "API_VERSION":
			o.apiVersion, err = str()
		case "REGION", "LOCATION":
			o.region, err = str()
		case "PROJECT":
			o.project, err = str()
		case "AWS_ACCESS_KEY_ID":
			o.awsKeyID, err = str()
		case "AWS_SECRET_ACCESS_KEY":
			o.awsSecret, err = str()
		case "AWS_SESSION_TOKEN":
			o.awsToken, err = str()
		case "SERVICE_ACCOUNT":
			o.serviceAccount, err = str()
		case "DEFAULT_MODEL":
			o.defaultModel, err = str()
		case "EMBEDDING_MODEL":
			o.embeddingModel, err = str()
		case "DECISION_MODEL":
			o.decisionModel, err = str()
		case "COMMENT":
			o.comment, err = str()
		case "HEADERS":
			o.headers = map[string]string{}
			err = pairs(o.headers, false)
		case "MODELS":
			o.models = map[string]string{}
			err = pairs(o.models, true)
		case "PRICES":
			o.prices = map[string]ai.Price{}
			err = parseList(sc, key, func() error {
				usage := aiSyntax("PRICES takes ('model' = (input, output), ...), in US dollars per million tokens")
				k, ok := sc.literal()
				if !ok || k == nil || !sc.punct('=') || !sc.punct('(') {
					return usage
				}
				in, ok1 := scanNumber(sc)
				ok2 := sc.punct(',')
				out, ok3 := scanNumber(sc)
				if !ok1 || !ok2 || !ok3 || !sc.punct(')') {
					return usage
				}
				o.prices[*k] = ai.Price{Input: in, Output: out}
				return nil
			})
		case "MAX_CONCURRENCY":
			n, ok := scanNumber(sc)
			if !ok || n != float64(int(n)) {
				return aiSyntax("MAX_CONCURRENCY takes a whole number")
			}
			v := int(n)
			o.maxConcurrency = &v
		case "ENABLED":
			switch sc.word() {
			case "TRUE":
				v := true
				o.enabled, sc.i = &v, sc.i+4
			case "FALSE":
				v := false
				o.enabled, sc.i = &v, sc.i+5
			default:
				err = aiSyntax("ENABLED takes TRUE or FALSE")
			}
		default:
			return aiSyntax("unknown option %s; the options are PROVIDER, BASE_URL, API_KEY, HEADERS, AWS_ACCESS_KEY_ID, "+
				"AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, SERVICE_ACCOUNT, API_VERSION, REGION, PROJECT, MODELS, DEFAULT_MODEL, "+
				"EMBEDDING_MODEL, DECISION_MODEL, PRICES, MAX_CONCURRENCY, ENABLED and COMMENT", key)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// scanNumber reads a number such as 0.25 or 3.
func scanNumber(sc *scanner) (float64, bool) {
	sc.skip()
	j := sc.i
	for j < len(sc.s) && (sc.s[j] >= '0' && sc.s[j] <= '9' || sc.s[j] == '.' || j == sc.i && sc.s[j] == '-') {
		j++
	}
	f, err := strconv.ParseFloat(sc.s[sc.i:j], 64)
	if err != nil {
		return 0, false
	}
	sc.i = j
	return f, true
}

// apply puts o's settings into in.
func (o aiOptions) apply(in *ai.Integration) {
	set := func(dst *string, v *string) {
		if v != nil {
			*dst = *v
		}
	}
	set(&in.Provider, o.provider)
	set(&in.BaseURL, o.baseURL)
	set(&in.APIKey, o.apiKey)
	set(&in.APIVersion, o.apiVersion)
	set(&in.Region, o.region)
	set(&in.Project, o.project)
	set(&in.AWSAccessKeyID, o.awsKeyID)
	set(&in.AWSSecretAccessKey, o.awsSecret)
	set(&in.AWSSessionToken, o.awsToken)
	set(&in.ServiceAccount, o.serviceAccount)
	set(&in.DefaultModel, o.defaultModel)
	set(&in.EmbeddingModel, o.embeddingModel)
	set(&in.DecisionModel, o.decisionModel)
	set(&in.Comment, o.comment)
	if o.headers != nil {
		in.Headers = o.headers
	}
	if o.models != nil {
		in.Models = o.models
	}
	if o.prices != nil {
		in.Prices = o.prices
	}
	if o.maxConcurrency != nil {
		in.MaxConcurrency = *o.maxConcurrency
	}
	if o.enabled != nil {
		in.Enabled = *o.enabled
	}
}

// aiCommand carries out the statements above. ok is false for every other
// statement, and for GRANT, REVOKE and DROP INTEGRATION of a notification
// integration.
func (s *Server) aiCommand(ctx context.Context, sess *session, query string) (wire.PreparedStatements, bool, error) {
	c, ok, err := parseAICommand(query)
	if !ok {
		return nil, false, nil
	}
	if err == nil && c.generic {
		// Ours only if an AI integration has the name.
		if s.ai == nil {
			return nil, false, nil
		}
		if _, gerr := s.ai.Store().Integration(ctx, c.name); errors.Is(gerr, ai.ErrNotFound) {
			return nil, false, nil
		}
	}
	if len(sqllex.Split(query)) != 1 {
		err = pgerror.WithCode(errors.New("send statements on AI integrations on their own"), codes.FeatureNotSupported)
	}
	if err == nil && s.ai == nil {
		err = pgerror.WithCode(errors.New("AI integrations are not available on this server"), codes.FeatureNotSupported)
	}
	if err != nil {
		s.recordQuery(sess, queryRecord{text: query, start: time.Now(), err: err, rows: -1})
		return nil, true, err
	}
	var opts []wire.PreparedOptionFn
	if c.verb == "SHOW" {
		opts = append(opts, wire.WithColumns(aiIntegrationColumns))
	}
	return wire.Prepared(wire.NewStatement(s.audited(sess, query, func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		start := time.Now()
		user, err := s.refreshUser(ctx, sess)
		if err == nil {
			// The console lists the running statement; this one may hold a
			// provider's key.
			err = sess.runCancelable(ctx, redactQuery(query), func(ctx context.Context) error {
				return s.runAICommand(ctx, user, c, w)
			})
		}
		s.metrics.query(start, err)
		return pgError(err)
	}), opts...)), true, nil
}

// aiIntegrationColumns describe SHOW AI INTEGRATIONS.
var aiIntegrationColumns = wire.Columns{
	{Name: "name", Oid: pgtype.TextOID, Width: -1},
	{Name: "provider", Oid: pgtype.TextOID, Width: -1},
	{Name: "enabled", Oid: pgtype.BoolOID, Width: 1},
	{Name: "target", Oid: pgtype.TextOID, Width: -1},
	{Name: "models", Oid: pgtype.TextOID, Width: -1},
	{Name: "default_model", Oid: pgtype.TextOID, Width: -1},
	{Name: "embedding_model", Oid: pgtype.TextOID, Width: -1},
	{Name: "decision_model", Oid: pgtype.TextOID, Width: -1},
	{Name: "prices", Oid: pgtype.TextOID, Width: -1},
	{Name: "max_concurrency", Oid: pgtype.Int4OID, Width: 4},
	{Name: "usage", Oid: pgtype.TextOID, Width: -1},
	{Name: "comment", Oid: pgtype.TextOID, Width: -1},
	{Name: "created_by", Oid: pgtype.TextOID, Width: -1},
	{Name: "created_at", Oid: pgtype.TimestamptzOID, Width: 8},
}

func aiMissing(name string) error {
	return pgerror.WithCode(fmt.Errorf("AI integration %s does not exist", name), codes.UndefinedObject)
}

func (s *Server) runAICommand(ctx context.Context, user auth.User, c aiCommand, w wire.DataWriter) error {
	if c.verb == "SHOW" {
		return s.showAIIntegrations(ctx, user, w)
	}
	tag := c.verb + " AI INTEGRATION"
	switch c.verb {
	case "GRANT", "REVOKE":
		tag = c.verb
	case "DROP":
		if c.generic {
			tag = "DROP INTEGRATION"
		}
	}
	if user.Role != auth.RoleAdmin {
		return pgerror.WithCode(fmt.Errorf("permission denied: %s requires the admin role", tag), codes.InsufficientPrivilege)
	}
	store := s.ai.Store()
	checkURL := func(raw string) error {
		if err := s.ai.CheckURL(raw); err != nil {
			return pgerror.WithCode(errors.New(strings.Replace(err.Error(), "IGLOO_NOTIFICATION_ALLOW_HOSTS", "IGLOO_AI_ALLOW_HOSTS", 1)),
				codes.InvalidParameterValue)
		}
		return nil
	}
	check := func(in ai.Integration) error {
		if err := in.Check(checkURL); err != nil {
			if pgerror.GetCode(err) != codes.Uncategorized {
				return err
			}
			return pgerror.WithCode(errors.New(strings.TrimPrefix(err.Error(), ai.ErrInvalid.Error()+": ")), codes.InvalidParameterValue)
		}
		return nil
	}
	var err error
	switch c.verb {
	case "CREATE":
		if _, gerr := store.Integration(ctx, c.name); gerr == nil && c.ifNotExists {
			return w.Complete(tag)
		}
		if s.notify != nil {
			if _, gerr := s.notify.Store().Integration(ctx, c.name); gerr == nil {
				return pgerror.WithCode(fmt.Errorf("a notification integration is called %s; integrations of both kinds share names", c.name),
					codes.DuplicateObject)
			}
		}
		in := ai.Integration{Name: c.name, Enabled: true, CreatedBy: user.Name}
		c.set.apply(&in)
		if err = check(in); err != nil {
			return err
		}
		err = store.CreateIntegration(ctx, in, c.replace)
	case "ALTER":
		var in ai.Integration
		if in, err = store.Integration(ctx, c.name); err != nil {
			break
		}
		if c.set.provider != nil && *c.set.provider != in.Provider {
			return pgerror.WithCode(errors.New("an AI integration's PROVIDER cannot change; create another integration"),
				codes.FeatureNotSupported)
		}
		c.set.apply(&in)
		for _, u := range c.unset {
			switch u {
			case "API_KEY":
				in.APIKey = ""
			case "HEADERS":
				in.Headers = nil
			case "AWS_SESSION_TOKEN":
				in.AWSSessionToken = ""
			case "DEFAULT_MODEL":
				in.DefaultModel = ""
			case "EMBEDDING_MODEL":
				in.EmbeddingModel = ""
			case "DECISION_MODEL":
				in.DecisionModel = ""
			case "PRICES":
				in.Prices = nil
			case "COMMENT":
				in.Comment = ""
			default:
				return aiSyntax("only API_KEY, HEADERS, AWS_SESSION_TOKEN, DEFAULT_MODEL, EMBEDDING_MODEL, DECISION_MODEL, PRICES and "+
					"COMMENT can be unset, not %s", u)
			}
		}
		if err = check(in); err != nil {
			return err
		}
		err = store.UpdateIntegration(ctx, in)
	case "DROP":
		err = store.DropIntegration(ctx, c.name)
	case "GRANT", "REVOKE":
		var granteeType string
		if granteeType, err = s.granteeType(ctx, c.grantee); err != nil {
			return err
		}
		if c.verb == "GRANT" {
			err = store.GrantUsage(ctx, ai.Grant{Integration: c.name, Grantee: c.grantee, GranteeType: granteeType, GrantedBy: user.Name})
		} else if _, err = store.Integration(ctx, c.name); err == nil {
			err = store.RevokeUsage(ctx, c.name, granteeType, c.grantee)
		}
	}
	switch {
	case errors.Is(err, ai.ErrNotFound) && c.ifExists:
		return w.Complete(tag)
	case errors.Is(err, ai.ErrNotFound):
		return aiMissing(c.name)
	case errors.Is(err, ai.ErrExists):
		return pgerror.WithCode(fmt.Errorf("AI integration %s already exists", c.name), codes.DuplicateObject)
	case err != nil:
		return err
	}
	s.ai.Changed()
	s.log.Info("AI integration changed", "action", strings.ToLower(c.verb), "integration", c.name, "grantee", c.grantee, "by", user.Name)
	return w.Complete(tag)
}

// showAIIntegrations lists integrations: every one, with who may use it,
// for admins; those they may use for others. Keys are never shown.
func (s *Server) showAIIntegrations(ctx context.Context, user auth.User, w wire.DataWriter) error {
	store := s.ai.Store()
	all, err := store.Integrations(ctx)
	if err != nil {
		return err
	}
	grants, err := store.Grants(ctx)
	if err != nil {
		return err
	}
	admin := user.Role == auth.RoleAdmin
	var roles []string
	if !admin {
		if roles, err = s.customRoles(ctx, user); err != nil {
			return err
		}
	}
	for _, in := range all {
		var who []string
		mayUse := admin
		for _, g := range grants {
			if g.Integration != in.Name {
				continue
			}
			who = append(who, g.GranteeType+" "+g.Grantee)
			if g.GranteeType == auth.GranteeUser && g.Grantee == user.Name || g.GranteeType == auth.GranteeRole && slices.Contains(roles, g.Grantee) {
				mayUse = true
			}
		}
		if !mayUse {
			continue
		}
		var models, prices []string
		for _, k := range slices.Sorted(maps.Keys(in.Models)) {
			models = append(models, k+" = "+in.Models[k])
		}
		for _, k := range slices.Sorted(maps.Keys(in.Prices)) {
			p := in.Prices[k]
			prices = append(prices, fmt.Sprintf("%s = (%g, %g)", k, p.Input, p.Output))
		}
		var usage any
		if admin {
			usage = nullText(strings.Join(who, ", "))
		}
		var conc any
		if in.MaxConcurrency > 0 {
			conc = int32(in.MaxConcurrency)
		}
		row := []any{in.Name, in.Provider, in.Enabled, nullText(in.Target()), nullText(strings.Join(models, ", ")),
			nullText(in.DefaultModel), nullText(in.EmbeddingModel), nullText(in.DecisionModel), nullText(strings.Join(prices, ", ")),
			conc, usage, nullText(in.Comment), in.CreatedBy, in.CreatedAt}
		if err := w.Row(row); err != nil {
			return err
		}
	}
	return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
}

// aiNameTaken reports whether an AI integration has name, which a
// notification integration then may not take.
func (s *Server) aiNameTaken(ctx context.Context, name string) bool {
	if s.ai == nil {
		return false
	}
	_, err := s.ai.Store().Integration(ctx, name)
	return err == nil
}

// --- session settings -------------------------------------------------------

// The AI settings a session sets for itself, in either dialect:
//
//	SET ai_integration = 'name' | DEFAULT     -- which integration serves its calls
//	SET ai_max_calls = n                      -- lowers its statements' cap on calls
//	SET ai_max_cost_usd = x                   -- and on cost
//	SET ai_decision_threshold = p             -- a decision model's yes from probability p (0.5)
//
// with RESET, SHOW and Snowflake's ALTER SESSION SET and UNSET.
var (
	setAISetting   = regexp.MustCompile(`(?is)^\s*(?:SET\s+(?:SESSION\s+)?|ALTER\s+SESSION\s+SET\s+)"?(ai_integration|ai_max_calls|ai_max_cost_usd|ai_decision_threshold)"?\s*(?:=|\bTO\b)\s*(.*?)\s*;?\s*$`)
	resetAISetting = regexp.MustCompile(`(?is)^\s*(?:RESET|ALTER\s+SESSION\s+UNSET)\s+"?(ai_integration|ai_max_calls|ai_max_cost_usd|ai_decision_threshold)"?\s*;?\s*$`)
	showAISetting  = regexp.MustCompile(`(?is)^\s*SHOW\s+"?(ai_integration|ai_max_calls|ai_max_cost_usd|ai_decision_threshold)"?\s*;?\s*$`)
)

// aiSettingCommand handles SET, RESET and SHOW of the AI settings.
func (s *Server) aiSettingCommand(sess *session, text string) (wire.PreparedStatements, bool) {
	query := stripComments(text)
	if !strings.Contains(strings.ToLower(query), "ai_") || len(sqllex.Split(text)) > 1 {
		return nil, false
	}
	respond := func(fn wire.PreparedStatementFn, opts ...wire.PreparedOptionFn) (wire.PreparedStatements, bool) {
		return wire.Prepared(wire.NewStatement(s.audited(sess, text, fn), opts...)), true
	}
	if m := setAISetting.FindStringSubmatch(query); m != nil {
		name := strings.ToLower(m[1])
		apply, err := parseAISetting(sess, name, strings.Trim(m[2], `'"`))
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			if err != nil {
				return err
			}
			apply()
			return w.Complete("SET")
		})
	}
	if m := resetAISetting.FindStringSubmatch(query); m != nil {
		name := strings.ToLower(m[1])
		apply, _ := parseAISetting(sess, name, "default")
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			apply()
			return w.Complete("RESET")
		})
	}
	if m := showAISetting.FindStringSubmatch(query); m != nil {
		name := strings.ToLower(m[1])
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			calls, cost, _ := s.aiCaps(sess)
			v := map[string]string{
				"ai_integration":        sess.aiIntegration,
				"ai_max_calls":          strconv.FormatInt(calls, 10),
				"ai_max_cost_usd":       strconv.FormatFloat(cost, 'f', -1, 64),
				"ai_decision_threshold": strconv.FormatFloat(threshold(sess.aiThreshold), 'f', -1, 64),
			}[name]
			if err := w.Row([]any{v}); err != nil {
				return err
			}
			return w.Complete("SHOW")
		}, wire.WithColumns(wire.Columns{{Name: name, Oid: pgtype.TextOID, Width: -1}}))
	}
	return nil, false
}

func threshold(t float64) float64 {
	if t <= 0 {
		return 0.5
	}
	return t
}

// parseAISetting checks value for the AI setting name and returns what
// sets it on sess; DEFAULT unsets it.
func parseAISetting(sess *session, name, value string) (func(), error) {
	if strings.EqualFold(value, "default") {
		return func() {
			switch name {
			case "ai_integration":
				sess.aiIntegration = ""
			case "ai_max_calls":
				sess.aiMaxCalls = 0
			case "ai_max_cost_usd":
				sess.aiMaxCost = 0
			case "ai_decision_threshold":
				sess.aiThreshold = 0
			}
		}, nil
	}
	invalid := func(format string, args ...any) (func(), error) {
		return nil, pgerror.WithCode(fmt.Errorf(format, args...), codes.InvalidParameterValue)
	}
	switch name {
	case "ai_integration":
		v := strings.ToLower(value)
		if !config.ValidName(v) {
			return invalid("ai_integration takes an AI integration's name")
		}
		return func() { sess.aiIntegration = v }, nil
	case "ai_max_calls":
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n < 1 {
			return invalid("ai_max_calls takes a whole number from 1; a session can lower its role's cap, not lift it")
		}
		return func() { sess.aiMaxCalls = n }, nil
	case "ai_max_cost_usd":
		f, err := strconv.ParseFloat(value, 64)
		if err != nil || f <= 0 {
			return invalid("ai_max_cost_usd takes an amount in US dollars above 0; a session can lower its role's cap, not lift it")
		}
		return func() { sess.aiMaxCost = f }, nil
	}
	f, err := strconv.ParseFloat(value, 64)
	if err != nil || f <= 0 || f >= 1 {
		return invalid("ai_decision_threshold takes a probability between 0 and 1")
	}
	return func() { sess.aiThreshold = f }, nil
}
