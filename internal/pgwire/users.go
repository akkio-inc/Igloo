package pgwire

import (
	"context"
	"errors"
	"fmt"
	"slices"
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
	// noPassword is set by PASSWORD NULL.
	noPassword bool
	role       *auth.Role
	kind       *auth.Kind
	owner      *string
	// email is empty for EMAIL NULL.
	email *string
	// login is set by LOGIN (true) and NOLOGIN (false).
	login *bool
	// unlinkSSO is set by SSO NULL: forget the user's single sign-on
	// identity, so their next sign-in links one again.
	unlinkSSO bool
	ifExists  bool
	// currentUser is set for ALTER USER CURRENT_USER.
	currentUser bool
	// acl, token, group or setting, when set, makes this a role or grant
	// command (see grants.go), an access token command (tokens.go), a
	// single sign-on group mapping (ssogroups.go) or a role settings
	// command (workload.go) instead.
	acl     *aclCommand
	token   *tokenCommand
	group   *groupCommand
	setting *settingCommand
	// share, when set, makes this a Delta Sharing command (see sharing.go).
	share *shareCommand
}

// columns describes the rows the command sends, if any.
func (c userCommand) columns() wire.Columns {
	switch {
	case c.share != nil:
		return c.share.columns()
	case c.acl != nil:
		return c.acl.columns()
	case c.token != nil:
		return c.token.columns()
	case c.group != nil:
		return c.group.columns()
	case c.setting != nil:
		return c.setting.columns()
	case c.verb == "SHOW":
		return userColumns
	}
	return nil
}

// passwordOnly reports whether an ALTER USER changes nothing but the
// password, which users may do to their own.
func (c userCommand) passwordOnly() bool {
	return (c.password != nil || c.noPassword) && c.role == nil && c.kind == nil && c.owner == nil &&
		c.email == nil && c.login == nil && !c.unlinkSSO
}

// parseUserCommands recognises queries made of user-management statements:
//
//	CREATE USER name [WITH] option ...
//	ALTER USER name|CURRENT_USER [WITH] option ...
//	DROP USER [IF EXISTS] name
//	SHOW USERS
//
// where the options are
//
//	PASSWORD 'secret' | PASSWORD NULL
//	ROLE admin|writer|reader|none
//	TYPE PERSON|SERVICE            (CREATE only)
//	OWNER name                     (service accounts)
//	EMAIL 'address' | EMAIL NULL
//	LOGIN | NOLOGIN
//	SSO NULL                       (ALTER only: unlink from single sign-on)
//
// and the statements of grants.go, tokens.go and ssogroups.go.
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
	if tc, ok, err := parseTokenCommand(toks); ok {
		if err != nil {
			return userCommand{}, true, err
		}
		return userCommand{verb: tc.verb(), token: tc}, true, nil
	}
	if gc, ok, err := parseGroupCommand(toks); ok {
		if err != nil {
			return userCommand{}, true, err
		}
		return userCommand{verb: gc.verb(), group: gc}, true, nil
	}
	if set, ok, err := parseSettingCommand(toks); ok {
		if err != nil {
			return userCommand{}, true, err
		}
		verb := "SHOW"
		if !set.show {
			verb = "ALTER"
		}
		return userCommand{verb: verb, setting: set}, true, nil
	}
	// Before the grant statements: GRANT ... ON SHARE is a sharing one.
	if sc, ok, err := parseShareCommand(toks); ok {
		if err != nil {
			return userCommand{}, true, err
		}
		return userCommand{verb: sc.tag, share: sc}, true, nil
	}
	if acl, ok, err := parseACLCommand(toks); ok {
		if err != nil {
			return userCommand{}, true, err
		}
		return userCommand{verb: acl.verb(), acl: acl}, true, nil
	}
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
	seen := map[string]bool{}
	for len(rest) > 0 {
		option := rest[0].upper()
		if option == "NOLOGIN" {
			option = "LOGIN"
		}
		if seen[option] {
			return syntax("conflicting or redundant option %s", rest[0].text)
		}
		seen[option] = true
		// LOGIN and NOLOGIN take no value; the others take one.
		if option == "LOGIN" {
			login := rest[0].upper() == "LOGIN"
			cmd.login, rest = &login, rest[1:]
			continue
		}
		if len(rest) < 2 {
			return syntax("expected a value after %s", rest[0].text)
		}
		value := rest[1]
		isNull := value.kind == tokWord && value.upper() == "NULL"
		switch option {
		case "PASSWORD":
			switch {
			case isNull:
				cmd.noPassword = true
			case value.kind == tokString:
				pw := value.text
				cmd.password = &pw
			default:
				return syntax("PASSWORD needs a quoted string, or NULL")
			}
		case "ROLE":
			role, err := auth.ParseRole(value.ident())
			if err != nil {
				return userCommand{}, true, pgerror.WithCode(err, codes.InvalidParameterValue)
			}
			cmd.role = &role
		case "TYPE":
			if verb != "CREATE" {
				return userCommand{}, true, pgerror.WithCode(errors.New("a user's type cannot change; create another user"),
					codes.FeatureNotSupported)
			}
			kind, err := auth.ParseKind(value.ident())
			if err != nil {
				return userCommand{}, true, pgerror.WithCode(err, codes.InvalidParameterValue)
			}
			cmd.kind = &kind
		case "OWNER":
			owner, err := principalName(value)
			if err != nil {
				return userCommand{}, true, err
			}
			cmd.owner = &owner
		case "EMAIL":
			email := ""
			switch {
			case isNull:
			case value.kind == tokString:
				if email = auth.NormalizeEmail(value.text); !validEmail(email) {
					return userCommand{}, true, pgerror.WithCode(fmt.Errorf("%q is not an email address", value.text),
						codes.InvalidParameterValue)
				}
			default:
				return syntax("EMAIL needs a quoted string, or NULL")
			}
			cmd.email = &email
		case "SSO":
			if verb != "ALTER" || !isNull {
				return syntax("only ALTER USER name SSO NULL, which unlinks the user from single sign-on, is supported")
			}
			cmd.unlinkSSO = true
		default:
			return syntax("unexpected %q; expected PASSWORD, ROLE, TYPE, OWNER, EMAIL, LOGIN, NOLOGIN or SSO", rest[0].text)
		}
		rest = rest[2:]
	}

	service := cmd.kind != nil && *cmd.kind == auth.KindService
	switch {
	case verb == "CREATE" && service && cmd.password != nil:
		return userCommand{}, true, pgerror.WithCode(
			errors.New("service accounts have no password; they sign in with access tokens (CREATE TOKEN ... FOR USER)"),
			codes.InvalidParameterValue)
	case verb == "CREATE" && !service && cmd.owner != nil:
		return syntax("OWNER applies to service accounts (TYPE SERVICE)")
	case verb == "CREATE" && !service && cmd.password == nil && !cmd.noPassword:
		return syntax("CREATE USER needs a PASSWORD, or PASSWORD NULL for a user who signs in with single sign-on or access tokens")
	case verb == "ALTER" && len(seen) == 0:
		return syntax("ALTER USER needs PASSWORD, ROLE, OWNER, EMAIL, LOGIN, NOLOGIN or SSO NULL")
	}
	return cmd, true, nil
}

// validEmail is a loose check that s is an email address.
func validEmail(s string) bool {
	local, domain, ok := strings.Cut(s, "@")
	return ok && local != "" && domain != "" && !strings.Contains(domain, "@") && len(s) <= 254 &&
		!strings.ContainsFunc(s, unicode.IsSpace)
}

// runUserCommand executes cmd on behalf of sess and writes the result to w.
func (s *Server) runUserCommand(ctx context.Context, sess *session, cmd userCommand, w wire.DataWriter) error {
	switch {
	case cmd.share != nil:
		return s.runShareCommand(ctx, sess, cmd.share, w)
	case cmd.acl != nil:
		return s.runACLCommand(ctx, sess, cmd.acl, w)
	case cmd.token != nil:
		return s.runTokenCommand(ctx, sess, cmd.token, w)
	case cmd.group != nil:
		return s.runGroupCommand(ctx, sess, cmd.group, w)
	}
	if cmd.setting != nil {
		return s.runSettingCommand(ctx, sess, cmd.setting, w)
	}
	me := sess.currentUser()
	isAdmin := me.Role == auth.RoleAdmin
	if cmd.currentUser {
		cmd.name = me.Name
	}

	if cmd.verb == "SHOW" {
		return s.showUsers(ctx, me, w)
	}

	// Anyone may change their own password; everything else needs admin.
	ownPassword := cmd.verb == "ALTER" && cmd.name == me.Name && cmd.passwordOnly()
	if !isAdmin && !ownPassword {
		return pgerror.WithCode(fmt.Errorf("permission denied: %s USER requires the admin role", cmd.verb),
			codes.InsufficientPrivilege)
	}
	// A stolen access token must not let its holder set a password that
	// outlives it.
	if cmd.name == me.Name && (cmd.password != nil || cmd.noPassword) && sess.bearer() {
		return pgerror.WithCode(errors.New("permission denied: a session that signed in with an access token, or a task, cannot change its user's password"),
			codes.InsufficientPrivilege)
	}

	var err error
	switch cmd.verb {
	case "CREATE":
		err = s.createUser(ctx, me, cmd)
	case "ALTER":
		err = s.alterUser(ctx, cmd)
	case "DROP":
		err = s.dropUser(ctx, cmd)
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

func (s *Server) createUser(ctx context.Context, me auth.User, cmd userCommand) error {
	u := auth.User{Name: cmd.name, Role: auth.RoleReader, Kind: auth.KindPerson}
	if cmd.role != nil {
		u.Role = *cmd.role
	}
	if cmd.kind != nil {
		u.Kind = *cmd.kind
	}
	if u.IsService() {
		u.Owner = me.Name
		if cmd.owner != nil {
			if err := s.checkOwner(ctx, *cmd.owner); err != nil {
				return err
			}
			u.Owner = *cmd.owner
		}
	}
	if cmd.password != nil {
		v, err := verifierFor(*cmd.password)
		if err != nil {
			return err
		}
		u.Verifier = v
	} else {
		// Access tokens of a user without a password use a salt of
		// their own.
		salt, err := auth.NewSalt()
		if err != nil {
			return err
		}
		u.TokenSalt = salt
	}
	if cmd.email != nil {
		u.Email = *cmd.email
	}
	if cmd.login != nil {
		u.Disabled = !*cmd.login
	}
	// Users and custom roles share names. The Postgres store checks this
	// itself, atomically; this covers the others.
	if isRole, err := s.isCustomRole(ctx, cmd.name); err != nil {
		return err
	} else if isRole {
		return auth.ErrNameTaken
	}
	return s.users.Create(ctx, u)
}

// checkOwner checks that a service account's owner is an admin.
func (s *Server) checkOwner(ctx context.Context, name string) error {
	owner, err := s.users.Get(ctx, name)
	if errors.Is(err, auth.ErrNotFound) {
		return pgerror.WithCode(fmt.Errorf("owner %q does not exist", name), codes.UndefinedObject)
	}
	if err != nil {
		return err
	}
	if owner.Role != auth.RoleAdmin || owner.IsService() {
		return pgerror.WithCode(fmt.Errorf("the owner of a service account must be a person with the admin role, not %q", name),
			codes.InvalidParameterValue)
	}
	return nil
}

func (s *Server) alterUser(ctx context.Context, cmd userCommand) error {
	target, err := s.users.Get(ctx, cmd.name)
	if err != nil {
		return err
	}
	needsAccounts := cmd.owner != nil || cmd.email != nil || cmd.login != nil || cmd.noPassword
	if needsAccounts && s.accounts == nil || cmd.unlinkSSO && s.sso == nil {
		return auth.ErrReadOnly
	}
	if cmd.owner != nil && !target.IsService() {
		return pgerror.WithCode(fmt.Errorf("%q is not a service account; only service accounts have an owner", cmd.name),
			codes.InvalidParameterValue)
	}
	if (cmd.password != nil || cmd.noPassword) && target.IsService() {
		return pgerror.WithCode(errors.New("service accounts have no password; they sign in with access tokens"),
			codes.InvalidParameterValue)
	}

	// Everything that can be refused is checked before anything changes.
	if cmd.owner != nil {
		if err := s.checkOwner(ctx, *cmd.owner); err != nil {
			return err
		}
	}
	var verifier string
	if cmd.password != nil {
		if verifier, err = s.newPassword(ctx, target, *cmd.password); err != nil {
			return err
		}
	}
	// The user's access tokens keep the removed password's salt.
	tokenSalt, err := s.keptTokenSalt(ctx, target, cmd.noPassword)
	if err != nil {
		return err
	}

	if cmd.role != nil {
		if err := s.users.SetRole(ctx, cmd.name, *cmd.role); err != nil {
			return err
		}
	}
	if cmd.owner != nil {
		if err := s.accounts.SetOwner(ctx, cmd.name, *cmd.owner); err != nil {
			return err
		}
	}
	if cmd.email != nil {
		if err := s.accounts.SetEmail(ctx, cmd.name, *cmd.email); err != nil {
			return err
		}
	}
	if cmd.unlinkSSO {
		if err := s.sso.UnlinkIdentity(ctx, cmd.name); err != nil {
			return err
		}
	}
	switch {
	case cmd.password != nil:
		if err := s.users.SetVerifier(ctx, cmd.name, verifier); err != nil {
			return err
		}
	case tokenSalt != nil:
		if err := s.accounts.SetPassword(ctx, cmd.name, "", tokenSalt); err != nil {
			return err
		}
	}
	if cmd.login != nil {
		return s.accounts.SetLogin(ctx, cmd.name, *cmd.login)
	}
	return nil
}

// newPassword returns the verifier for target's new password. While target
// has access tokens, which share the password's salt (auth.TokenFactors),
// the new password keeps that salt, so the tokens keep working.
func (s *Server) newPassword(ctx context.Context, target auth.User, password string) (string, error) {
	factors, hasFactors := auth.TokenFactors(target)
	var tokens []auth.TokenCredential
	if s.tokens != nil && hasFactors {
		var err error
		if tokens, err = s.tokens.TokenCredentials(ctx, target.Name); err != nil {
			return "", err
		}
	}
	if !slices.ContainsFunc(tokens, func(t auth.TokenCredential) bool { return t.SCRAM.KeyFactors == factors }) {
		return verifierFor(password)
	}
	if auth.IsVerifier(password) {
		creds, _ := auth.ParseVerifier(password)
		if creds.KeyFactors != factors {
			return "", pgerror.WithCode(fmt.Errorf("%s has access tokens, which share the password's salt; send the new "+
				"password itself (ALTER USER %s PASSWORD '...') rather than one psql's \\password encrypted, or drop the tokens first",
				target.Name, target.Name), codes.InvalidParameterValue)
		}
		return password, nil
	}
	if _, err := verifierFor(password); err != nil {
		return "", err
	}
	return auth.NewVerifierWith(password, factors)
}

// keptTokenSalt returns the token salt target keeps when PASSWORD NULL
// removes their password: the password's, which their access tokens were
// made with. It is nil when there is no password to remove. Tokens of a
// user without a password use PostgreSQL's iteration count, so removing a
// password with another one is refused while tokens made with it exist.
func (s *Server) keptTokenSalt(ctx context.Context, target auth.User, noPassword bool) ([]byte, error) {
	if !noPassword || !target.HasPassword() {
		return nil, nil
	}
	factors, ok := auth.TokenFactors(target)
	if !ok {
		return auth.NewSalt()
	}
	salt := []byte(factors.Salt)
	if after, _ := auth.TokenFactors(auth.User{TokenSalt: salt}); after == factors || s.tokens == nil {
		return salt, nil
	}
	tokens, err := s.tokens.TokenCredentials(ctx, target.Name)
	if err != nil {
		return nil, err
	}
	if slices.ContainsFunc(tokens, func(t auth.TokenCredential) bool { return t.SCRAM.KeyFactors == factors }) {
		return nil, pgerror.WithCode(fmt.Errorf("%s has access tokens made with the password's %d SCRAM iterations, which "+
			"would stop working without it; drop the tokens first, or set a new password instead", target.Name, factors.Iters),
			codes.ObjectNotInPrerequisiteState)
	}
	return salt, nil
}

func (s *Server) dropUser(ctx context.Context, cmd userCommand) error {
	// A service account must always have someone responsible for it.
	users, err := s.users.List(ctx)
	if err != nil {
		return err
	}
	var owned []string
	for _, u := range users {
		if u.IsService() && u.Owner == cmd.name && u.Name != cmd.name {
			owned = append(owned, u.Name)
		}
	}
	if len(owned) > 0 {
		return pgerror.WithCode(fmt.Errorf("%s owns the service accounts %s; give them another owner first (ALTER USER %s OWNER name)",
			cmd.name, strings.Join(owned, ", "), owned[0]), codes.DependentObjectsStillExist)
	}

	err = s.users.Delete(ctx, cmd.name)
	if err == nil {
		// The Postgres store drops them with the user; other stores
		// keep grants apart.
		if ferr := s.grants.ForgetUser(ctx, cmd.name); ferr != nil {
			s.log.Warn("could not remove a dropped user's grants", "user", cmd.name, "err", ferr)
		}
		s.forgetGrantee(auth.GranteeUser, cmd.name)
	}
	if errors.Is(err, auth.ErrNotFound) && cmd.ifExists {
		err = nil
	}
	return err
}

// showUsers lists users: all of them for admins, themselves for others.
func (s *Server) showUsers(ctx context.Context, me auth.User, w wire.DataWriter) error {
	users, err := s.users.List(ctx)
	if err != nil {
		return err
	}
	linked := map[string]bool{}
	if s.sso != nil {
		ids, err := s.sso.Identities(ctx)
		if err != nil {
			return err
		}
		for _, id := range ids {
			linked[id.User] = true
		}
	}
	nullable := func(s string) any {
		if s == "" {
			return nil
		}
		return s
	}
	for _, u := range users {
		if me.Role != auth.RoleAdmin && u.Name != me.Name {
			continue
		}
		kind := u.Kind
		if kind == "" {
			kind = auth.KindPerson
		}
		row := []any{u.Name, string(u.Role), u.CreatedAt, string(kind), nullable(u.Owner), !u.Disabled,
			u.HasPassword(), nullable(u.Email), linked[u.Name]}
		if err := w.Row(row); err != nil {
			return err
		}
	}
	return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
}

// userColumns describe SHOW USERS.
var userColumns = wire.Columns{
	{Name: "name", Oid: pgtype.TextOID, Width: -1},
	{Name: "role", Oid: pgtype.TextOID, Width: -1},
	{Name: "created_at", Oid: pgtype.TimestamptzOID, Width: 8},
	{Name: "type", Oid: pgtype.TextOID, Width: -1},
	{Name: "owner", Oid: pgtype.TextOID, Width: -1},
	{Name: "can_login", Oid: pgtype.BoolOID, Width: 1},
	{Name: "has_password", Oid: pgtype.BoolOID, Width: 1},
	{Name: "email", Oid: pgtype.TextOID, Width: -1},
	{Name: "sso", Oid: pgtype.BoolOID, Width: 1},
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
	case pgerror.GetCode(err) != codes.Uncategorized:
		return err
	case errors.Is(err, auth.ErrNotFound):
		return pgerror.WithCode(fmt.Errorf("user %q does not exist", name), codes.UndefinedObject)
	case errors.Is(err, auth.ErrExists):
		return pgerror.WithCode(fmt.Errorf("user %q already exists", name), codes.DuplicateObject)
	case errors.Is(err, auth.ErrNameTaken):
		return pgerror.WithCode(fmt.Errorf("a user or role named %q already exists", name), codes.DuplicateObject)
	case errors.Is(err, auth.ErrEmailTaken):
		return pgerror.WithCode(errors.New("another user has that email"), codes.UniqueViolation)
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
	tokDot
	tokComma
	// tokEquals is the = of SET name = value.
	tokEquals
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

// tokenize splits SQL into words, "quoted" identifiers, 'string' literals,
// semicolons, dots, commas and equals signs, ignoring comments.
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
		case r == '.':
			toks = append(toks, token{kind: tokDot, text: "."})
			i++
		case r == ',':
			toks = append(toks, token{kind: tokComma, text: ","})
			i++
		case r == '=':
			toks = append(toks, token{kind: tokEquals, text: "="})
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
