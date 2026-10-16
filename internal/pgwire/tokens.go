package pgwire

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
)

// Access tokens (see auth/token.go), which SQL clients send where they
// would send a password:
//
//	CREATE TOKEN [name] [FOR USER user] [EXPIRES [IN] 'duration']
//	SHOW TOKENS [FOR USER user]
//	DROP TOKEN [IF EXISTS] name [FOR USER user]
//
// Anyone creates, lists and drops their own tokens, except in a session
// that signed in with a token: a stolen token must not mint longer-lived
// ones. Admins also create tokens for service accounts, and list and drop
// anyone's. CREATE TOKEN returns the token once; Igloo keeps only hashes.

type tokenVerb int

const (
	tokenCreate tokenVerb = iota
	tokenShow
	tokenDrop
)

// tokenCommand is a parsed access token statement.
type tokenCommand struct {
	kind tokenVerb
	name string
	// user is whose tokens; empty means the session's user.
	user     string
	lifetime time.Duration
	ifExists bool
}

func (c *tokenCommand) verb() string {
	switch c.kind {
	case tokenCreate:
		return "CREATE TOKEN"
	case tokenDrop:
		return "DROP TOKEN"
	}
	return "SHOW"
}

var (
	newTokenColumns = wire.Columns{
		{Name: "user_name", Oid: pgtype.TextOID, Width: -1},
		{Name: "name", Oid: pgtype.TextOID, Width: -1},
		{Name: "token", Oid: pgtype.TextOID, Width: -1},
		{Name: "expires_at", Oid: pgtype.TimestamptzOID, Width: 8},
	}
	tokenColumns = wire.Columns{
		{Name: "user_name", Oid: pgtype.TextOID, Width: -1},
		{Name: "name", Oid: pgtype.TextOID, Width: -1},
		{Name: "prefix", Oid: pgtype.TextOID, Width: -1},
		{Name: "created_at", Oid: pgtype.TimestamptzOID, Width: 8},
		{Name: "created_by", Oid: pgtype.TextOID, Width: -1},
		{Name: "expires_at", Oid: pgtype.TimestamptzOID, Width: 8},
		{Name: "last_used_at", Oid: pgtype.TimestamptzOID, Width: 8},
		{Name: "expired", Oid: pgtype.BoolOID, Width: 1},
	}
)

func (c *tokenCommand) columns() wire.Columns {
	switch c.kind {
	case tokenCreate:
		return newTokenColumns
	case tokenShow:
		return tokenColumns
	}
	return nil
}

// parseTokenCommand parses one statement's tokens. ok is false when it is
// not an access token statement.
func parseTokenCommand(toks []token) (c *tokenCommand, ok bool, err error) {
	if len(toks) < 2 {
		return nil, false, nil
	}
	verb, noun := toks[0].upper(), toks[1].upper()
	switch {
	case verb == "CREATE" && noun == "TOKEN":
		c = &tokenCommand{kind: tokenCreate}
	case verb == "SHOW" && noun == "TOKENS":
		c = &tokenCommand{kind: tokenShow}
	case verb == "DROP" && noun == "TOKEN":
		c = &tokenCommand{kind: tokenDrop}
	default:
		return nil, false, nil
	}
	syntax := func(format string, args ...any) (*tokenCommand, bool, error) {
		return nil, true, pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
	}
	rest := toks[2:]
	word := func(i int) string {
		if i < len(rest) {
			return rest[i].upper()
		}
		return ""
	}

	if c.kind == tokenDrop && word(0) == "IF" && word(1) == "EXISTS" {
		c.ifExists, rest = true, rest[2:]
	}
	// The name, unless the next word starts a clause.
	if c.kind != tokenShow && len(rest) > 0 && !(rest[0].kind == tokWord && (word(0) == "FOR" || word(0) == "EXPIRES")) {
		t := rest[0]
		if t.kind != tokWord && t.kind != tokQuoted && t.kind != tokString {
			return syntax("expected a token name, got %q", t.text)
		}
		c.name, rest = t.ident(), rest[1:]
		if t.kind == tokString {
			c.name = t.text
		}
		if !auth.ValidTokenName(c.name) {
			return nil, true, pgerror.WithCode(fmt.Errorf("invalid token name %q: use up to 63 letters, digits, dots, dashes and underscores", c.name),
				codes.InvalidName)
		}
	}
	if c.kind == tokenDrop && c.name == "" {
		return syntax("DROP TOKEN needs the token's name")
	}
	for len(rest) > 0 {
		switch {
		case word(0) == "FOR" && word(1) == "USER" && len(rest) >= 3 && c.user == "":
			if c.user, err = principalName(rest[2]); err != nil {
				return nil, true, err
			}
			rest = rest[3:]
		case word(0) == "EXPIRES" && c.kind == tokenCreate && c.lifetime == 0:
			rest = rest[1:]
			if word(0) == "IN" {
				rest = rest[1:]
			}
			if len(rest) == 0 || rest[0].kind != tokString {
				return syntax("EXPIRES needs a quoted duration, such as '90d'")
			}
			d, perr := parseLifetime(rest[0].text)
			if perr != nil {
				return nil, true, pgerror.WithCode(perr, codes.InvalidParameterValue)
			}
			c.lifetime, rest = d, rest[1:]
		default:
			if c.kind == tokenCreate {
				return syntax("unexpected %q; expected FOR USER name or EXPIRES 'duration'", rest[0].text)
			}
			return syntax("unexpected %q; expected FOR USER name", rest[0].text)
		}
	}
	return c, true, nil
}

// parseLifetime reads a token's lifetime: a number of days, weeks or years
// such as 90d, 2w or 1y, or a duration such as 12h.
func parseLifetime(s string) (time.Duration, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	units := map[string]time.Duration{"d": 24 * time.Hour, "w": 7 * 24 * time.Hour, "y": 365 * 24 * time.Hour}
	var (
		d   time.Duration
		err error
	)
	number, suffix := s, ""
	if len(s) > 1 {
		number, suffix = s[:len(s)-1], s[len(s)-1:]
	}
	if unit, ok := units[suffix]; ok {
		var n int
		// Capped at a century, so that a huge number cannot overflow.
		n, err = strconv.Atoi(number)
		d = time.Duration(min(n, int(100*365*24*time.Hour/unit))) * unit
	} else {
		d, err = time.ParseDuration(s)
	}
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("invalid token lifetime %q: use a duration such as 90d, 2w, 1y or 12h", s)
	}
	return d, nil
}

// runTokenCommand executes an access token statement for sess.
func (s *Server) runTokenCommand(ctx context.Context, sess *session, c *tokenCommand, w wire.DataWriter) error {
	if s.tokens == nil {
		return pgerror.WithCode(errors.New("access tokens need IGLOO_METADATA_DATABASE_URL"), codes.FeatureNotSupported)
	}
	me := sess.currentUser()
	isAdmin := me.Role == auth.RoleAdmin
	target := c.user
	if target == "" {
		if c.kind != tokenShow || !isAdmin {
			target = me.Name
		}
	}
	if target != me.Name && !isAdmin {
		return pgerror.WithCode(fmt.Errorf("permission denied: only admins manage the access tokens of other users"),
			codes.InsufficientPrivilege)
	}

	switch c.kind {
	case tokenShow:
		return s.showTokens(ctx, target, w)
	case tokenDrop:
		t, err := s.tokens.DropToken(ctx, target, c.name)
		if errors.Is(err, auth.ErrTokenNotFound) {
			if c.ifExists {
				return w.Complete("DROP TOKEN")
			}
			return pgerror.WithCode(fmt.Errorf("access token %q of user %q does not exist", c.name, target), codes.UndefinedObject)
		}
		if err != nil {
			return err
		}
		// Sessions that signed in with the token end at their next
		// statement here, and within the role refresh elsewhere.
		s.sessions.expire(target)
		s.log.Info("access token dropped", "user", target, "token", t.Name, "prefix", t.Prefix(), "by", me.Name)
		return w.Complete("DROP TOKEN")
	}
	return s.createToken(ctx, sess, me, target, c, w)
}

func (s *Server) createToken(ctx context.Context, sess *session, me auth.User, target string, c *tokenCommand, w wire.DataWriter) error {
	denied := func(format string, args ...any) error {
		return pgerror.WithCode(fmt.Errorf(format, args...), codes.InsufficientPrivilege)
	}
	// The token's SCRAM keys take the salt the user has now, not the one
	// the session saw when it last read the user.
	user, err := s.users.Get(ctx, target)
	if err != nil {
		return userStoreError(err, target)
	}
	if target == me.Name {
		switch {
		case user.IsService():
			return denied("service accounts cannot create their own access tokens; an admin creates them (CREATE TOKEN name FOR USER %s)", me.Name)
		case sess.bearer():
			return denied("a session that signed in with an access token, or a task, cannot create access tokens for its user; sign in with a password or single sign-on")
		}
	} else if !user.IsService() {
		return denied("admins create access tokens for service accounts only; %s creates their own", target)
	}

	lifetime := c.lifetime
	if lifetime == 0 {
		lifetime = min(auth.DefaultTokenLifetime, s.tokenMaxLifetime)
	}
	if lifetime > s.tokenMaxLifetime {
		return pgerror.WithCode(fmt.Errorf("access tokens may last at most %s (IGLOO_TOKEN_MAX_LIFETIME)", formatLifetime(s.tokenMaxLifetime)),
			codes.InvalidParameterValue)
	}

	factors, ok := auth.TokenFactors(user)
	if !ok {
		// A user without a password needs a salt of their own first;
		// concurrent creations agree on one.
		if s.accounts == nil {
			return auth.ErrReadOnly
		}
		salt, err := auth.NewSalt()
		if err != nil {
			return err
		}
		if user.TokenSalt, err = s.accounts.EnsureTokenSalt(ctx, user.Name, salt); err != nil {
			return userStoreError(err, user.Name)
		}
		factors, _ = auth.TokenFactors(user)
	}
	expires := time.Now().Add(lifetime).Truncate(time.Second)
	secret, t, hashes, err := auth.NewToken(user.Name, c.name, me.Name, expires, factors)
	if err != nil {
		return err
	}
	if t.Name == "" {
		t.Name = "token_" + t.ID[:6]
	}
	switch err := s.tokens.CreateToken(ctx, t, hashes); {
	case errors.Is(err, auth.ErrTokenExists):
		return pgerror.WithCode(fmt.Errorf("user %q already has an access token named %q", user.Name, t.Name), codes.DuplicateObject)
	case errors.Is(err, auth.ErrTooManyTokens):
		return pgerror.WithCode(fmt.Errorf("user %q has %d access tokens, the most allowed; drop unused ones first", user.Name, auth.MaxTokensPerUser),
			codes.ProgramLimitExceeded)
	case err != nil:
		return userStoreError(err, user.Name)
	}
	s.log.Info("access token created", "user", user.Name, "token", t.Name, "prefix", t.Prefix(), "expires", t.ExpiresAt, "by", me.Name)
	if err := w.Row([]any{user.Name, t.Name, secret, t.ExpiresAt}); err != nil {
		return err
	}
	return w.Complete("CREATE TOKEN")
}

// formatLifetime renders a lifetime in days when it is whole days.
func formatLifetime(d time.Duration) string {
	if d%(24*time.Hour) == 0 {
		return fmt.Sprintf("%d days", d/(24*time.Hour))
	}
	return d.String()
}

// showTokens lists user's tokens, or everyone's when user is empty.
func (s *Server) showTokens(ctx context.Context, user string, w wire.DataWriter) error {
	tokens, err := s.tokens.Tokens(ctx, user)
	if err != nil {
		return err
	}
	now := time.Now()
	for _, t := range tokens {
		var used any
		if !t.LastUsedAt.IsZero() {
			used = t.LastUsedAt
		}
		row := []any{t.User, t.Name, t.Prefix(), t.CreatedAt, t.CreatedBy, t.ExpiresAt, used, t.Expired(now)}
		if err := w.Row(row); err != nil {
			return err
		}
	}
	return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
}
