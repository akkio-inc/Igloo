package pgwire

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
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
//	CREATE CONNECTION [IF NOT EXISTS] name TYPE postgres|mysql|snowflake
//	    WITH (host 'h', port 5432, database 'db', user 'u', password 'p', ...)
//	CREATE CONNECTION [IF NOT EXISTS] name TYPE iceberg
//	    WITH (endpoint 'https://...', warehouse 'w', token 't' | credential 'id:secret', ...)
//	ALTER CONNECTION name SET (option value, ...)
//	ALTER CONNECTION name REFRESH
//	DROP CONNECTION [IF EXISTS] name
//	SHOW CONNECTIONS
//
// Options may also be written option = value. Passwords, private keys,
// tokens and credentials are stored encrypted and never shown again.

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
	// The settings only some types have, such as a Snowflake account or an
	// Iceberg REST catalog's scope; never a credential.
	{Name: "settings", Oid: pgtype.TextOID, Width: -1},
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
			return nil, true, syntaxError("expected CREATE CONNECTION %s TYPE postgres|mysql|snowflake|iceberg WITH (...)", c.name)
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
		option := federation.CanonicalOption(name)
		if option == "" {
			return nil, pgerror.WithCode(fmt.Errorf("unknown connection option %q", name), codes.InvalidParameterValue)
		}
		if option != "password" && slices.Contains(federation.SecretOptions, option) && toks[0].kind != 's' {
			// The history keeps a statement's string literals out, not
			// its words.
			return nil, syntaxError("write the value of %s as a string literal, in single quotes", name)
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
	// changed is the connection as CREATE or ALTER left it.
	var changed *federation.Connection
	switch {
	case c.verb == "SHOW":
		return s.showConnections(ctx, w)
	case c.verb == "CREATE":
		var conn federation.Connection
		var created bool
		if conn, created, err = s.createConnection(ctx, sess, me, c); created {
			changed = &conn
		}
	case c.refresh:
		// Refreshing lists every database's tables with duckdb_tables() on a
		// connection of its own, which looks the catalog's tables up too:
		// hold the catalog lock shared (see holdCatalog).
		release := s.shareCatalog(sess)
		err = s.fed.Refresh(ctx, c.name)
		release()
	case c.verb == "ALTER":
		var conn federation.Connection
		conn, err = s.fed.Alter(ctx, c.name, func(conn *federation.Connection) error {
			for _, o := range c.options {
				if err := conn.Set(o[0], o[1]); err != nil {
					return err
				}
			}
			return nil
		})
		changed = &conn
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
	s.catalogChanged()
	s.log.Info("connection changed", "action", strings.ToLower(c.verb), "connection", c.name, "by", me.Name)
	if changed != nil {
		if warning := changed.TLSWarning(); warning != "" {
			// Allowed, as an explicit choice, but said every time.
			s.log.Warn("connection does not verify its server", "connection", changed.Name, "tls", changed.TLS)
			if n, ok := w.(wire.Noticer); ok {
				if err := n.Notice(string(codes.Warning), warning); err != nil {
					return err
				}
			}
		}
	}
	return w.Complete(c.tag())
}

// createConnection carries out CREATE CONNECTION, reporting whether it
// created one: IF NOT EXISTS may leave one there.
func (s *Server) createConnection(ctx context.Context, sess *session, me auth.User, c *connCommand) (federation.Connection, bool, error) {
	conn := federation.New(c.name, c.typ)
	for _, o := range c.options {
		if err := conn.Set(o[0], o[1]); err != nil {
			return conn, false, &federation.InvalidError{Err: err}
		}
	}
	conn.CreatedBy = me.Name
	taken, err := s.takenNames(ctx, sess, conn.Name)
	if err != nil {
		return conn, false, err
	}
	err = s.fed.Create(ctx, conn, taken)
	if errors.Is(err, federation.ErrExists) && c.ifNotExists {
		return conn, false, nil
	}
	return conn, err == nil, err
}

// takenNames lists the names a new connection called name may not take:
// the databases DuckDB has and the catalog. It fails with an
// InvalidError when the catalog has a schema called name.
func (s *Server) takenNames(ctx context.Context, sess *session, name string) ([]string, error) {
	// Listing the catalog's schemas looks them up (see holdCatalog).
	defer s.shareCatalog(sess)()
	taken, err := queryStrings(ctx, sess, "SELECT database_name FROM system.main.duckdb_databases()")
	if err != nil {
		return nil, err
	}
	if cat := s.catalogName(); cat != "" {
		taken = append(taken, cat)
		// DuckDB refuses a two-part name such as crm.contacts as
		// ambiguous when crm is both a schema of the default catalog and a
		// database.
		schemas, err := queryStrings(ctx, sess, `SELECT schema_name FROM system.main.duckdb_schemas()
			WHERE system.main.lower(database_name) = system.main.lower($1)`, cat)
		if err != nil {
			return nil, err
		}
		if slices.ContainsFunc(schemas, func(n string) bool { return strings.EqualFold(n, name) }) {
			return nil, &federation.InvalidError{Err: fmt.Errorf("the %s catalog has a schema named %s, and a connection named so too "+
				"would make names such as %s.contacts ambiguous; choose another name", cat, name, name)}
		}
	}
	return taken, nil
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
		host, database := st.ServerHost(), st.Database
		if st.Type == federation.Iceberg {
			// The catalog's endpoint and warehouse.
			host, database = st.Iceberg.Endpoint, st.Iceberg.Warehouse
		}
		row := []any{st.Name, string(st.Type), host, int32(st.Port), database, st.User, st.PasswordSource(),
			string(st.TLS), strings.Join(st.Schemas, ", "), timeout(st.ConnectTimeout), timeout(st.QueryTimeout),
			st.State, st.Detail, st.CreatedBy, st.CreatedAt, st.UpdatedAt, st.Settings()}
		if err := w.Row(row); err != nil {
			return err
		}
	}
	return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
}

// missingCatalog matches DuckDB's error for a catalog it does not have.
var missingCatalog = regexp.MustCompile(`Catalog "([^"]+)" does not exist`)

// connectionHint explains DuckDB's error for a missing catalog when the
// catalog is a connection this replica has not attached: one created since
// it started, or one it could not reach then.
func (s *Server) connectionHint(ctx context.Context, err error) error {
	if err == nil || s.fed == nil {
		return err
	}
	m := missingCatalog.FindStringSubmatch(err.Error())
	if m == nil {
		return err
	}
	name := strings.ToLower(m[1])
	if _, attached := s.connectionCatalogs()[name]; attached {
		return err
	}
	if exists, ferr := s.fed.Exists(ctx, name); ferr != nil || !exists {
		return err
	}
	return pgerror.WithCode(fmt.Errorf("connection %s is not attached on this replica: connections are attached when Igloo starts, "+
		"so restart it to attach one created since (SHOW CONNECTIONS says why one is not attached)", name), codes.ObjectNotInPrerequisiteState)
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
