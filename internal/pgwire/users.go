package pgwire

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
)

// minPasswordLength applies to plain passwords; verifiers sent by psql's
// \password were checked by the client.
const minPasswordLength = 8

// userCommand is a parsed CREATE USER, ALTER USER, DROP USER or SHOW USERS.
// DuckDB has no users, so Igloo handles these itself.
type userCommand struct {
	verb     string // CREATE, ALTER, DROP or SHOW
	name     string // empty for SHOW
	password *string
	role     *auth.Role
	ifExists bool
	// currentUser is set for ALTER USER CURRENT_USER.
	currentUser bool
}

// parseUserCommands recognises queries made of user-management statements:
//
//	CREATE USER name [WITH] PASSWORD 'secret' [ROLE admin|writer|reader]
//	ALTER USER name|CURRENT_USER [WITH] [PASSWORD 'secret'] [ROLE role]
//	DROP USER [IF EXISTS] name
//	SHOW USERS
//
// ok is false when the query holds none. err reports a malformed command,
// or one mixed with other SQL, which DuckDB would reject anyway.
func parseUserCommands(query string) (cmds []userCommand, ok bool, err error) {
	toks, err := tokenize(query)
	if err != nil {
		return nil, false, nil
	}

	var stmts [][]token
	start := 0
	for i, t := range toks {
		if t.kind == tokSemicolon {
			if i > start {
				stmts = append(stmts, toks[start:i])
			}
			start = i + 1
		}
	}
	if start < len(toks) {
		stmts = append(stmts, toks[start:])
	}

	others := 0
	for _, stmt := range stmts {
		cmd, isUserCmd, err := parseUserCommand(stmt)
		if !isUserCmd {
			others++
			continue
		}
		if err != nil {
			return nil, true, err
		}
		cmds = append(cmds, cmd)
	}
	if len(cmds) > 0 && others > 0 {
		return nil, true, pgerror.WithCode(
			errors.New("send user management statements separately from other SQL"), codes.FeatureNotSupported)
	}
	return cmds, len(cmds) > 0, nil
}

// parseUserCommand parses one statement's tokens. ok is false when it is not
// a user-management statement.
func parseUserCommand(toks []token) (cmd userCommand, ok bool, err error) {
	if len(toks) < 2 {
		return cmd, false, nil
	}
	verb, noun := toks[0].upper(), toks[1].upper()
	if noun != "USER" && noun != "USERS" {
		return cmd, false, nil
	}

	switch {
	case verb == "SHOW" && noun == "USERS" && len(toks) == 2:
		return userCommand{verb: "SHOW"}, true, nil
	case verb == "CREATE" || verb == "ALTER" || verb == "DROP":
		if noun != "USER" {
			return cmd, false, nil
		}
	default:
		return cmd, false, nil
	}

	cmd.verb = verb
	rest := toks[2:]
	syntax := func(format string, args ...any) (userCommand, bool, error) {
		return userCommand{}, true, pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
	}

	if verb == "DROP" && len(rest) >= 2 && rest[0].upper() == "IF" && rest[1].upper() == "EXISTS" {
		cmd.ifExists, rest = true, rest[2:]
	}
	if len(rest) == 0 || rest[0].kind == tokString {
		return syntax("%s USER needs a user name", verb)
	}
	if verb == "ALTER" && rest[0].kind == tokWord && rest[0].upper() == "CURRENT_USER" {
		cmd.currentUser = true
	} else {
		cmd.name = rest[0].ident()
		if !config.ValidName(cmd.name) {
			return syntax("invalid user name %q: use lowercase letters, digits and underscores", cmd.name)
		}
	}
	rest = rest[1:]

	if verb == "DROP" {
		if len(rest) > 0 {
			return syntax("unexpected %q after DROP USER", rest[0].text)
		}
		return cmd, true, nil
	}

	if len(rest) > 0 && rest[0].upper() == "WITH" {
		rest = rest[1:]
	}
	for len(rest) > 0 {
		if len(rest) < 2 {
			return syntax("expected a value after %s", rest[0].text)
		}
		switch rest[0].upper() {
		case "PASSWORD":
			if rest[1].kind != tokString {
				return syntax("PASSWORD needs a quoted string")
			}
			pw := rest[1].text
			cmd.password = &pw
		case "ROLE":
			role, err := auth.ParseRole(rest[1].ident())
			if err != nil {
				return userCommand{}, true, pgerror.WithCode(err, codes.InvalidParameterValue)
			}
			cmd.role = &role
		default:
			return syntax("unexpected %q; expected PASSWORD or ROLE", rest[0].text)
		}
		rest = rest[2:]
	}

	if verb == "CREATE" && cmd.password == nil {
		return syntax("CREATE USER needs a PASSWORD")
	}
	if verb == "ALTER" && cmd.password == nil && cmd.role == nil {
		return syntax("ALTER USER needs PASSWORD or ROLE")
	}
	return cmd, true, nil
}

// runUserCommand executes cmd on behalf of sess and writes the result to w.
func (s *Server) runUserCommand(ctx context.Context, sess *session, cmd userCommand, w wire.DataWriter) error {
	me := sess.currentUser()
	isAdmin := me.Role == auth.RoleAdmin
	if cmd.currentUser {
		cmd.name = me.Name
	}

	if cmd.verb == "SHOW" {
		users, err := s.users.List(ctx)
		if err != nil {
			return err
		}
		for _, u := range users {
			// Non-admins see only themselves.
			if !isAdmin && u.Name != me.Name {
				continue
			}
			if err := w.Row([]any{u.Name, string(u.Role), u.CreatedAt}); err != nil {
				return err
			}
		}
		return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
	}

	// Anyone may change their own password; everything else needs admin.
	ownPassword := cmd.verb == "ALTER" && cmd.name == me.Name && cmd.role == nil
	if !isAdmin && !ownPassword {
		return pgerror.WithCode(fmt.Errorf("permission denied: %s USER requires the admin role", cmd.verb),
			codes.InsufficientPrivilege)
	}

	var verifier string
	if cmd.password != nil {
		v, err := verifierFor(*cmd.password)
		if err != nil {
			return err
		}
		verifier = v
	}

	var err error
	switch cmd.verb {
	case "CREATE":
		role := auth.RoleReader
		if cmd.role != nil {
			role = *cmd.role
		}
		err = s.users.Create(ctx, auth.User{Name: cmd.name, Role: role, Verifier: verifier})
	case "ALTER":
		if cmd.role != nil {
			err = s.users.SetRole(ctx, cmd.name, *cmd.role)
		}
		if err == nil && cmd.password != nil {
			err = s.users.SetVerifier(ctx, cmd.name, verifier)
		}
	case "DROP":
		err = s.users.Delete(ctx, cmd.name)
		if errors.Is(err, auth.ErrNotFound) && cmd.ifExists {
			err = nil
		}
	}
	if err != nil {
		return userStoreError(err, cmd.name)
	}

	// Sessions of a changed user re-read their role before their next
	// statement.
	s.sessions.expire(cmd.name)
	s.log.Info("user changed", "action", strings.ToLower(cmd.verb), "user", cmd.name, "by", me.Name)
	return w.Complete(cmd.verb + " ROLE")
}

// userColumns describe SHOW USERS.
var userColumns = wire.Columns{
	{Name: "name", Oid: pgtype.TextOID, Width: -1},
	{Name: "role", Oid: pgtype.TextOID, Width: -1},
	{Name: "created_at", Oid: pgtype.TimestamptzOID, Width: 8},
}

func verifierFor(password string) (string, error) {
	if auth.IsVerifier(password) {
		return password, nil
	}
	if strings.HasPrefix(password, "md5") && len(password) == 35 {
		return "", pgerror.WithCode(errors.New("MD5 password hashes are not supported; use SCRAM-SHA-256"),
			codes.InvalidParameterValue)
	}
	if len(password) < minPasswordLength {
		return "", pgerror.WithCode(fmt.Errorf("password must be at least %d characters", minPasswordLength),
			codes.InvalidParameterValue)
	}
	return auth.NewVerifier(password)
}

func userStoreError(err error, name string) error {
	switch {
	case errors.Is(err, auth.ErrNotFound):
		return pgerror.WithCode(fmt.Errorf("user %q does not exist", name), codes.UndefinedObject)
	case errors.Is(err, auth.ErrExists):
		return pgerror.WithCode(fmt.Errorf("user %q already exists", name), codes.DuplicateObject)
	case errors.Is(err, auth.ErrReadOnly), errors.Is(err, auth.ErrLastAdmin):
		return pgerror.WithCode(err, codes.FeatureNotSupported)
	}
	return err
}

// --- tokenizer --------------------------------------------------------------

type tokKind int

const (
	tokWord tokKind = iota
	tokQuoted
	tokString
	tokSemicolon
)

type token struct {
	kind tokKind
	text string
}

func (t token) upper() string {
	if t.kind != tokWord {
		return ""
	}
	return strings.ToUpper(t.text)
}

// ident is the identifier the token names: unquoted words fold to lower
// case, as in PostgreSQL.
func (t token) ident() string {
	if t.kind == tokWord {
		return strings.ToLower(t.text)
	}
	return t.text
}

// tokenize splits SQL into words, "quoted" identifiers, 'string' literals
// and semicolons, ignoring comments.
func tokenize(query string) ([]token, error) {
	var toks []token
	s := []rune(query)
	for i := 0; i < len(s); {
		r := s[i]
		switch {
		case unicode.IsSpace(r):
			i++
		case r == ';':
			toks = append(toks, token{kind: tokSemicolon, text: ";"})
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
				return nil, errors.New("unterminated comment")
			}
			i = j + 2
		case r == '\'' || r == '"':
			text, n, err := quoted(s[i:], r)
			if err != nil {
				return nil, err
			}
			kind := tokString
			if r == '"' {
				kind = tokQuoted
			}
			toks = append(toks, token{kind: kind, text: text})
			i += n
		case unicode.IsLetter(r) || r == '_' || unicode.IsDigit(r):
			j := i
			for j < len(s) && (unicode.IsLetter(s[j]) || unicode.IsDigit(s[j]) || s[j] == '_' || s[j] == '$') {
				j++
			}
			toks = append(toks, token{kind: tokWord, text: string(s[i:j])})
			i = j
		default:
			return nil, fmt.Errorf("unexpected %q", r)
		}
	}
	return toks, nil
}

// quoted reads a quoted string starting at s[0] == q, where a doubled quote
// stands for one quote character. It returns the contents and the number of
// runes consumed.
func quoted(s []rune, q rune) (string, int, error) {
	var b strings.Builder
	for i := 1; i < len(s); i++ {
		if s[i] == q {
			if i+1 < len(s) && s[i+1] == q {
				b.WriteRune(q)
				i++
				continue
			}
			return b.String(), i + 1, nil
		}
		b.WriteRune(s[i])
	}
	return "", 0, errors.New("unterminated quoted string")
}
