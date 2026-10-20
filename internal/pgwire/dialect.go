package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/snowflake"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// SQL dialects. A session speaks DuckDB's unless it chose Snowflake's:
//
//	SET sql_dialect = 'snowflake'          -- also ALTER SESSION SET
//	RESET sql_dialect
//	SHOW sql_dialect
//
// or its connection did (the startup parameter sql_dialect, or options
// -c sql_dialect=snowflake), or a role setting gives it to its user
// (ALTER ROLE ... SET sql_dialect = 'snowflake'). Statements in Snowflake's
// dialect are translated to DuckDB's as the very first step of handling
// them (see parse), so everything after, Igloo's rewrites and every check
// of the user's role, grants and policies included, sees only the DuckDB
// statement, as if the client had sent it.
const (
	dialectDuckDB    = "duckdb"
	dialectSnowflake = "snowflake"
)

// parseDialect reads a value of sql_dialect.
func parseDialect(value string) (string, error) {
	v := strings.ToLower(strings.TrimSpace(value))
	v = strings.Trim(v, `'"`)
	switch v {
	case dialectDuckDB, dialectSnowflake:
		return v, nil
	}
	return "", pgerror.WithHint(pgerror.WithCode(fmt.Errorf("invalid value for parameter sql_dialect: %q", value),
		codes.InvalidParameterValue), "use 'duckdb' or 'snowflake'")
}

// iglooSQLKey marks a context whose statements Igloo wrote itself.
type iglooSQLKey struct{}

// WithIglooSQL returns ctx for running statements that Igloo wrote itself
// in an embedded session, such as the web console's and Flight SQL's
// catalog lookups: they are read in DuckDB's dialect whatever the
// session's sql_dialect.
func WithIglooSQL(ctx context.Context) context.Context {
	return context.WithValue(ctx, iglooSQLKey{}, true)
}

func isIglooSQL(ctx context.Context) bool {
	v, _ := ctx.Value(iglooSQLKey{}).(bool)
	return v
}

// sqlDialect is the dialect the session's statements are written in.
// Tasks, pipes, alerts and materialized views run SQL Igloo stored in
// DuckDB's dialect (translated when they were created), whatever their
// owner's settings.
func (sess *session) sqlDialect() string {
	switch {
	case sess.method == methodTask:
		return dialectDuckDB
	case sess.dialect != "":
		return sess.dialect
	case sess.startDialect != "":
		return sess.startDialect
	case sess.wl.limits.SQLDialect != "":
		return sess.wl.limits.SQLDialect
	}
	return dialectDuckDB
}

var (
	setDialect   = regexp.MustCompile(`(?is)^\s*(?:SET\s+(?:SESSION\s+)?|ALTER\s+SESSION\s+SET\s+)"?sql_dialect"?\s*(?:=|\bTO\b)\s*(.*?)\s*;?\s*$`)
	resetDialect = regexp.MustCompile(`(?is)^\s*(?:RESET|ALTER\s+SESSION\s+UNSET)\s+"?sql_dialect"?\s*;?\s*$`)
	showDialect  = regexp.MustCompile(`(?is)^\s*SHOW\s+"?sql_dialect"?\s*;?\s*$`)
)

// dialectCommand handles SET, RESET and SHOW sql_dialect, in either
// dialect.
func (s *Server) dialectCommand(sess *session, text string) (wire.PreparedStatements, bool) {
	query := stripComments(text)
	if !strings.Contains(strings.ToLower(query), "sql_dialect") {
		return nil, false
	}
	// One statement of several runs when its turn comes (see parseScript).
	if len(sqllex.Split(text)) > 1 || len(snowflake.Split(text)) > 1 {
		return nil, false
	}
	respond := func(fn wire.PreparedStatementFn, opts ...wire.PreparedOptionFn) (wire.PreparedStatements, bool) {
		return wire.Prepared(wire.NewStatement(s.audited(sess, text, fn), opts...)), true
	}
	if m := setDialect.FindStringSubmatch(query); m != nil {
		value, isDefault := m[1], strings.EqualFold(strings.TrimSpace(m[1]), "default")
		var dialect string
		var err error
		if !isDefault {
			dialect, err = parseDialect(value)
		}
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			if err != nil {
				return err
			}
			sess.dialect = dialect
			sess.settingGen++
			return w.Complete("SET")
		})
	}
	if resetDialect.MatchString(query) {
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			sess.dialect = ""
			sess.settingGen++
			return w.Complete("RESET")
		})
	}
	if showDialect.MatchString(query) {
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			if err := w.Row([]any{sess.sqlDialect()}); err != nil {
				return err
			}
			return w.Complete("SHOW")
		}, wire.WithColumns(wire.Columns{{Name: "sql_dialect", Oid: pgtype.TextOID, Width: -1}}))
	}
	return nil, false
}

// startupDialect reads sql_dialect from a connection's startup
// parameters: given on its own, as PostgreSQL takes any setting, or in
// options as -c sql_dialect=value or --sql_dialect=value. "" means none.
func startupDialect(params wire.Parameters) (string, error) {
	value, ok := params["sql_dialect"]
	if options := params["options"]; options != "" {
		args := splitOptions(options)
		for i := 0; i < len(args); i++ {
			a := args[i]
			switch {
			case a == "-c" && i+1 < len(args):
				i++
				a = args[i]
			case strings.HasPrefix(a, "-c"):
				a = a[2:]
			case strings.HasPrefix(a, "--"):
				a = a[2:]
			default:
				continue
			}
			if name, v, found := strings.Cut(a, "="); found && strings.EqualFold(strings.ReplaceAll(name, "-", "_"), "sql_dialect") {
				value, ok = v, true
			}
		}
	}
	if !ok {
		return "", nil
	}
	return parseDialect(value)
}

// splitOptions splits PostgreSQL's options parameter at white space that a
// backslash does not escape.
func splitOptions(s string) []string {
	var out []string
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '\\' && i+1 < len(s):
			i++
			b.WriteByte(s[i])
		case c == ' ' || c == '\t':
			if b.Len() > 0 {
				out = append(out, b.String())
				b.Reset()
			}
		default:
			b.WriteByte(c)
		}
	}
	if b.Len() > 0 {
		out = append(out, b.String())
	}
	return out
}

// translation is how a statement in Snowflake's dialect was translated.
type translation struct {
	snowflake.Result
}

// columns presents a translated statement's result columns as Snowflake
// names them.
func (tr *translation) columns(cols wire.Columns) wire.Columns {
	if tr == nil || tr.Untranslated || len(cols) == 0 {
		return cols
	}
	out := make(wire.Columns, len(cols))
	copy(out, cols)
	for i := range out {
		out[i].Name = snowflake.ColumnName(out[i].Name, tr.Quoted)
	}
	return out
}

// translate translates one statement in Snowflake's dialect for sess. The
// macros translated statements call are defined in every session (see
// sessionCatalogSetup), so that views, tasks and the like created in
// Snowflake's dialect run in any session.
func (s *Server) translate(ctx context.Context, sess *session, text string) (*translation, error) {
	res, err := snowflake.Translate(text, snowflake.Options{
		IsSetting: func(name string) bool { return s.isSetting(ctx, sess, name) },
		Variable:  func(name string) (string, bool, error) { return sessionVariable(ctx, sess, name) },
	})
	if err != nil {
		var te *snowflake.Error
		if errors.As(err, &te) {
			out := pgerror.WithCode(fmt.Errorf("%s", te.Message), codes.Code(te.Code))
			if te.Hint != "" {
				out = pgerror.WithHint(out, te.Hint)
			}
			return nil, out
		}
		return nil, err
	}
	return &translation{Result: res}, nil
}

// settingNames are the settings Igloo handles itself or accepts from
// PostgreSQL clients, which SET keeps in the Snowflake dialect; SET of any
// other name that is not one of DuckDB's sets a session variable.
var settingNames = func() map[string]bool {
	m := map[string]bool{"statement_timeout": true, "query_tag": true, "distributed_execution": true,
		"sql_dialect": true, "search_path": true, "timezone": true, "schema": true}
	for name := range postgresOnlySettings {
		m[name] = true
	}
	for name := range pgSettings {
		m[name] = true
	}
	for name := range sessionSettings {
		m[name] = true
	}
	return m
}()

var (
	duckdbSettingsOnce sync.Once
	duckdbSettings     map[string]bool
)

// isSetting reports whether name, lower-cased, is a setting rather than
// a session variable.
func (s *Server) isSetting(ctx context.Context, sess *session, name string) bool {
	if settingNames[name] {
		return true
	}
	duckdbSettingsOnce.Do(func() {
		names := map[string]bool{}
		rows, err := sess.conn.QueryContext(ctx, "SELECT lower(name) FROM duckdb_settings()", nil)
		if err == nil {
			dest := make([]driver.Value, 1)
			for rows.Next(dest) == nil {
				if n, ok := dest[0].(string); ok {
					names[n] = true
				}
			}
			_ = rows.Close()
		}
		duckdbSettings = names
	})
	return duckdbSettings[name]
}

// sessionVariable returns the value of the session's variable name as
// text, for IDENTIFIER($name).
func sessionVariable(ctx context.Context, sess *session, name string) (string, bool, error) {
	rows, err := sess.conn.QueryContext(ctx, "SELECT CAST(value AS VARCHAR) FROM duckdb_variables() WHERE name = "+quoteLiteral(name), nil)
	if err != nil {
		return "", false, err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		if errors.Is(err, io.EOF) {
			return "", false, nil
		}
		return "", false, err
	}
	v, ok := dest[0].(string)
	return v, ok, nil
}

// snowflakeScript handles a simple query of several statements in the
// Snowflake dialect: it is split as Snowflake reads it, and each statement
// is translated when it runs, so one that changes sql_dialect applies to
// those after it. The checks a DuckDB script gets before it runs (see
// parse) are made on the statements' translations.
func (s *Server) snowflakeScript(ctx context.Context, sess *session, q wire.Query, parts []string) (wire.PreparedStatements, error) {
	var texts []string
	stmts := make([]sqllex.Statement, len(parts))
	for i, p := range parts {
		stmts[i] = sqllex.Statement{Text: p}
		text := p
		if tr, err := snowflake.Translate(p, snowflake.Options{}); err == nil && tr.SQL != "" {
			text = tr.SQL
		}
		texts = append(texts, text)
	}
	joined := strings.Join(texts, ";\n")
	if _, users, _ := parseUserCommands(joined); !users {
		if ps, ok, err := s.policyCommand(ctx, sess, wire.Query{Query: joined, SimpleQuery: true}); ok || err != nil {
			return ps, err
		}
		for _, text := range texts {
			if err := aloneError(text); err != nil {
				s.recordQuery(sess, queryRecord{text: q.Query, err: err, rows: -1})
				return nil, err
			}
		}
	}
	return s.parseScript(sess, stmts), nil
}

// renamedReader is a record reader whose schema has other column names.
type renamedReader struct {
	array.RecordReader
	schema *arrow.Schema
	rec    arrow.RecordBatch
}

func (r *renamedReader) Schema() *arrow.Schema { return r.schema }

func (r *renamedReader) Next() bool {
	if r.rec != nil {
		r.rec.Release()
		r.rec = nil
	}
	if !r.RecordReader.Next() {
		return false
	}
	src := r.RecordReader.RecordBatch()
	r.rec = array.NewRecordBatch(r.schema, src.Columns(), src.NumRows())
	return true
}

func (r *renamedReader) RecordBatch() arrow.RecordBatch { return r.rec }

func (r *renamedReader) Record() arrow.RecordBatch { return r.rec }

func (r *renamedReader) Release() {
	if r.rec != nil {
		r.rec.Release()
		r.rec = nil
	}
	r.RecordReader.Release()
}

// renameSchema returns schema with the fields named names, or schema when
// names is nil.
func renameSchema(schema *arrow.Schema, names []string) *arrow.Schema {
	if names == nil || len(names) != schema.NumFields() {
		return schema
	}
	fields := make([]arrow.Field, schema.NumFields())
	for i, f := range schema.Fields() {
		f.Name = names[i]
		fields[i] = f
	}
	md := schema.Metadata()
	return arrow.NewSchema(fields, &md)
}
