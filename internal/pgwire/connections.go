package pgwire

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/federation"
)

// Connections to other databases (package federation). DuckDB has no such
// statements, so Igloo handles them itself, for admins only:
//
//	CREATE CONNECTION [IF NOT EXISTS] name TYPE postgres|mysql
//	    WITH (host 'h', port 5432, database 'db', user 'u', password 'p', ...)
//	ALTER CONNECTION name SET (option value, ...)
//	ALTER CONNECTION name REFRESH
//	DROP CONNECTION [IF EXISTS] name
//	SHOW CONNECTIONS
//
// Options may also be written option = value. Passwords are stored
// encrypted and never shown again.

type connCommand struct {
	verb        string // CREATE, ALTER, DROP or SHOW
	name        string
	typ         federation.Type
	ifExists    bool
	ifNotExists bool
	refresh     bool
	options     [][2]string
}

// tag is the statement's command tag.
func (c *connCommand) tag() string {
	if c.verb == "SHOW" {
		return "SHOW"
	}
	return c.verb + " CONNECTION"
}

var connectionColumns = wire.Columns{
	{Name: "name", Oid: pgtype.TextOID, Width: -1},
	{Name: "type", Oid: pgtype.TextOID, Width: -1},
	{Name: "host", Oid: pgtype.TextOID, Width: -1},
	{Name: "port", Oid: pgtype.Int4OID, Width: 4},
	{Name: "database", Oid: pgtype.TextOID, Width: -1},
	{Name: "user", Oid: pgtype.TextOID, Width: -1},
	{Name: "password", Oid: pgtype.TextOID, Width: -1},
	{Name: "tls", Oid: pgtype.TextOID, Width: -1},
	{Name: "schemas", Oid: pgtype.TextOID, Width: -1},
	{Name: "connect_timeout", Oid: pgtype.TextOID, Width: -1},
	{Name: "query_timeout", Oid: pgtype.TextOID, Width: -1},
	{Name: "state", Oid: pgtype.TextOID, Width: -1},
	{Name: "detail", Oid: pgtype.TextOID, Width: -1},
	{Name: "created_by", Oid: pgtype.TextOID, Width: -1},
	{Name: "created_at", Oid: pgtype.TimestamptzOID, Width: 8},
	{Name: "updated_at", Oid: pgtype.TimestamptzOID, Width: 8},
}

func (c *connCommand) columns() wire.Columns {
	if c.verb == "SHOW" {
		return connectionColumns
	}
	return nil
}

// connToken is a token of a connection statement.
type connToken struct {
	kind byte // 'w' word, 's' string literal, 'n' number, or the punctuation itself
	text string
}

func (t connToken) upper() string {
	if t.kind != 'w' {
		return ""
	}
	return strings.ToUpper(t.text)
}

// tokenizeConnection splits a connection statement into words, quoted
// identifiers (as words), string literals, numbers and ( ) , = ;.
func tokenizeConnection(query string) ([]connToken, bool) {
	var toks []connToken
	s := []rune(query)
	for i := 0; i < len(s); {
		r := s[i]
		switch {
		case unicode.IsSpace(r):
			i++
		case r == '-' && i+1 < len(s) && s[i+1] == '-':
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case r == '/' && i+1 < len(s) && s[i+1] == '*':
			j := i + 2
			for j+1 < len(s) && (s[j] != '*' || s[j+1] != '/') {
				j++
			}
			if j+1 >= len(s) {
				return nil, false
			}
			i = j + 2
		case r == '\'' || r == '"':
			text, n, err := quoted(s[i:], r)
			if err != nil {
				return nil, false
			}
			kind := byte('s')
			if r == '"' {
				kind = 'w'
			}
			toks = append(toks, connToken{kind: kind, text: text})
			i += n
		case unicode.IsLetter(r) || r == '_':
			j := i
			for j < len(s) && (unicode.IsLetter(s[j]) || unicode.IsDigit(s[j]) || s[j] == '_') {
				j++
			}
			toks = append(toks, connToken{kind: 'w', text: string(s[i:j])})
			i = j
		case unicode.IsDigit(r):
			j := i
			for j < len(s) && (unicode.IsDigit(s[j]) || unicode.IsLetter(s[j]) || s[j] == '.') {
				j++
			}
			toks = append(toks, connToken{kind: 'n', text: string(s[i:j])})
			i = j
		case strings.ContainsRune("(),=;", r):
			toks = append(toks, connToken{kind: byte(r), text: string(r)})
			i++
		default:
			return nil, false
		}
	}
	for len(toks) > 0 && toks[len(toks)-1].kind == ';' {
		toks = toks[:len(toks)-1]
	}
	return toks, true
}

// parseConnectionCommand recognizes a connection statement. ok is false
// for anything else.
func parseConnectionCommand(query string) (c *connCommand, ok bool, err error) {
	toks, ok := tokenizeConnection(query)
	if !ok || len(toks) < 2 {
		return nil, false, nil
	}
	verb, noun := toks[0].upper(), toks[1].upper()
	switch {
	case verb == "SHOW" && noun == "CONNECTIONS":
		if len(toks) != 2 {
			return nil, true, syntaxError("unexpected %q after SHOW CONNECTIONS", toks[2].text)
		}
		return &connCommand{verb: "SHOW"}, true, nil
	case noun != "CONNECTION" || verb != "CREATE" && verb != "ALTER" && verb != "DROP":
		return nil, false, nil
	}
	for _, t := range toks {
		if t.kind == ';' {
			return nil, true, syntaxError("send connection statements one at a time")
		}
	}

	c = &connCommand{verb: verb}
	rest := toks[2:]
	word := func(i int) string {
		if i < len(rest) {
			return rest[i].upper()
		}
		return ""
	}
	switch {
	case verb == "DROP" && word(0) == "IF" && word(1) == "EXISTS":
		c.ifExists, rest = true, rest[2:]
	case verb == "CREATE" && word(0) == "IF" && word(1) == "NOT" && word(2) == "EXISTS":
		c.ifNotExists, rest = true, rest[3:]
	}
	if len(rest) == 0 || rest[0].kind != 'w' {
		return nil, true, syntaxError("%s CONNECTION needs a connection name", verb)
	}
	c.name = rest[0].text
	if rest[0].upper() != "" {
		c.name = strings.ToLower(c.name)
	}
	if !config.ValidName(c.name) {
		return nil, true, syntaxError("invalid connection name %q: use lowercase letters, digits and underscores", c.name)
	}
	rest = rest[1:]

	switch verb {
	case "DROP":
		if len(rest) > 0 {
			return nil, true, syntaxError("unexpected %q after DROP CONNECTION %s", rest[0].text, c.name)
		}
		return c, true, nil
	case "ALTER":
		if len(rest) == 1 && rest[0].upper() == "REFRESH" {
			c.refresh = true
			return c, true, nil
		}
		if len(rest) == 0 || rest[0].upper() != "SET" && rest[0].upper() != "WITH" {
			return nil, true, syntaxError("expected ALTER CONNECTION %s SET (option value, ...) or REFRESH", c.name)
		}
		rest = rest[1:]
	case "CREATE":
		if len(rest) < 2 || rest[0].upper() != "TYPE" {
			return nil, true, syntaxError("expected CREATE CONNECTION %s TYPE postgres|mysql WITH (...)", c.name)
		}
		if c.typ, err = federation.ParseType(rest[1].text); err != nil {
			return nil, true, pgerror.WithCode(err, codes.FeatureNotSupported)
		}
		rest = rest[2:]
		if len(rest) == 0 || rest[0].upper() != "WITH" {
			return nil, true, syntaxError("expected WITH (host ..., user ..., ...) after the type")
		}
		rest = rest[1:]
	}
	if c.options, err = parseConnectionOptions(rest); err != nil {
		return nil, true, err
	}
	return c, true, nil
}

// parseConnectionOptions reads (name value, name = value, ...).
func parseConnectionOptions(toks []connToken) ([][2]string, error) {
	if len(toks) < 2 || toks[0].kind != '(' || toks[len(toks)-1].kind != ')' {
		return nil, syntaxError("expected options in parentheses, such as (host 'db.example.com', port 5432)")
	}
	toks = toks[1 : len(toks)-1]
	var out [][2]string
	for len(toks) > 0 {
		if toks[0].kind != 'w' {
			return nil, syntaxError("expected an option name, got %q", toks[0].text)
		}
		name := strings.ToLower(toks[0].text)
		toks = toks[1:]
		if len(toks) > 0 && toks[0].kind == '=' {
			toks = toks[1:]
		}
		if len(toks) == 0 || toks[0].kind != 's' && toks[0].kind != 'n' && toks[0].kind != 'w' {
			return nil, syntaxError("option %s needs a value", name)
		}
		if federation.CanonicalOption(name) == "" {
			return nil, pgerror.WithCode(fmt.Errorf("unknown connection option %q", name), codes.InvalidParameterValue)
		}
		out = append(out, [2]string{name, toks[0].text})
		toks = toks[1:]
		if len(toks) > 0 {
			if toks[0].kind != ',' {
				return nil, syntaxError("expected a comma between options, got %q", toks[0].text)
			}
			toks = toks[1:]
		}
	}
	if len(out) == 0 {
		return nil, syntaxError("expected at least one option")
	}
	return out, nil
}

func syntaxError(format string, args ...any) error {
	return pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
}

// runConnectionCommand carries out a connection statement for sess.
func (s *Server) runConnectionCommand(ctx context.Context, sess *session, c *connCommand, w wire.DataWriter) error {
	me := sess.currentUser()
	if me.Role != auth.RoleAdmin {
		return pgerror.WithCode(fmt.Errorf("permission denied: %s requires the admin role", c.tag()), codes.InsufficientPrivilege)
	}
	if s.fed == nil {
		return pgerror.WithCode(errors.New("connections need IGLOO_METADATA_DATABASE_URL"), codes.FeatureNotSupported)
	}
	var err error
	switch {
	case c.verb == "SHOW":
		return s.showConnections(ctx, w)
	case c.verb == "CREATE":
		err = s.createConnection(ctx, sess, me, c)
	case c.refresh:
		err = s.fed.Refresh(ctx, c.name)
	case c.verb == "ALTER":
		_, err = s.fed.Alter(ctx, c.name, func(conn *federation.Connection) error {
			for _, o := range c.options {
				if err := conn.Set(o[0], o[1]); err != nil {
					return err
				}
			}
			return nil
		})
	case c.verb == "DROP":
		err = s.fed.Drop(ctx, c.name)
		if err == nil {
			// The metadata database's store drops them with the connection;
			// other grant stores keep them apart.
			if ferr := s.grants.ForgetObject(ctx, auth.Object{Catalog: c.name}); ferr != nil {
				s.log.Warn("could not remove a dropped connection's grants", "connection", c.name, "err", ferr)
			}
			s.sessions.expireAll()
		}
		if errors.Is(err, federation.ErrNotFound) && c.ifExists {
			err = nil
		}
	}
	if err != nil {
		return connectionError(err, c.name)
	}
	s.ddlGen.Add(1)
	s.log.Info("connection changed", "action", strings.ToLower(c.verb), "connection", c.name, "by", me.Name)
	return w.Complete(c.tag())
}

func (s *Server) createConnection(ctx context.Context, sess *session, me auth.User, c *connCommand) error {
	conn := federation.New(c.name, c.typ)
	for _, o := range c.options {
		if err := conn.Set(o[0], o[1]); err != nil {
			return &federation.InvalidError{Err: err}
		}
	}
	conn.CreatedBy = me.Name
	taken, err := queryStrings(ctx, sess, "SELECT database_name FROM duckdb_databases()")
	if err != nil {
		return err
	}
	if s.catalogName() != "" {
		taken = append(taken, s.catalogName())
	}
	err = s.fed.Create(ctx, conn, taken)
	if errors.Is(err, federation.ErrExists) && c.ifNotExists {
		return nil
	}
	return err
}

func (s *Server) showConnections(ctx context.Context, w wire.DataWriter) error {
	list, err := s.fed.List(ctx)
	if err != nil {
		return err
	}
	for _, st := range list {
		timeout := func(d time.Duration) string {
			if d == 0 {
				return "none"
			}
			return d.String()
		}
		row := []any{st.Name, string(st.Type), st.Host, int32(st.Port), st.Database, st.User, st.PasswordSource(),
			string(st.TLS), strings.Join(st.Schemas, ", "), timeout(st.ConnectTimeout), timeout(st.QueryTimeout),
			st.State, st.Detail, st.CreatedBy, st.CreatedAt, st.UpdatedAt}
		if err := w.Row(row); err != nil {
			return err
		}
	}
	return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
}

// connectionError gives connection errors PostgreSQL codes.
func connectionError(err error, name string) error {
	var invalid *federation.InvalidError
	var unreachable *federation.UnreachableError
	switch {
	case pgerror.GetCode(err) != codes.Uncategorized:
		return err
	case errors.As(err, &invalid):
		return pgerror.WithCode(err, codes.InvalidParameterValue)
	case errors.As(err, &unreachable):
		return pgerror.WithCode(err, codes.SQLclientUnableToEstablishSQLconnection)
	case errors.Is(err, federation.ErrExists):
		return pgerror.WithCode(fmt.Errorf("connection %q already exists", name), codes.DuplicateObject)
	case errors.Is(err, federation.ErrNotFound):
		return pgerror.WithCode(fmt.Errorf("connection %q does not exist", name), codes.UndefinedObject)
	}
	return pgerror.WithCode(errors.New(federation.Scrub(err.Error())), codes.Internal)
}
