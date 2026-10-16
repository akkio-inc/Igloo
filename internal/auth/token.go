package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base32"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/xdg-go/scram"
)

// Access tokens let programs and people without a password (single sign-on
// users, service accounts) sign in to SQL: a client sends one where it
// would send a password. A token looks like
//
//	igloo_<16 characters of ID><52 characters of secret>
//
// all lowercase letters and digits after the prefix. The ID, 80 random
// bits, names the token in listings and finds it for HTTP APIs; the rest is
// 256 random bits. Igloo keeps only hashes: a SHA-256 of the whole token,
// for APIs that receive it as a bearer token, and SCRAM-SHA-256 keys derived
// with the user's salt (TokenFactors), for SQL logins, where the token never
// crosses the network. Tokens always expire.

// TokenPrefix starts every access token, so that secret scanners and people
// recognise one.
const TokenPrefix = "igloo_"

const (
	tokenIDLength     = 16 // 10 random bytes in base32
	tokenSecretLength = 52 // 32 random bytes in base32
)

// MaxTokensPerUser bounds a user's unexpired tokens. Each SQL login checks
// the client's proof against every one of them.
const MaxTokensPerUser = 50

// DefaultTokenLifetime is how long a token lasts when CREATE TOKEN does not
// say.
const DefaultTokenLifetime = 90 * 24 * time.Hour

var tokenEncoding = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// Token describes an access token; the secret itself is shown once, when
// it is created, and never stored.
type Token struct {
	// ID is the token's public identifier, the start of the token after
	// TokenPrefix.
	ID   string
	User string
	// Name tells a user's tokens apart, such as "dbt" or "tableau".
	Name      string
	CreatedBy string
	CreatedAt time.Time
	ExpiresAt time.Time
	// LastUsedAt is when the token last signed in, to the minute; zero if
	// never.
	LastUsedAt time.Time
}

// Prefix is the start of the token, enough to recognise it.
func (t Token) Prefix() string { return TokenPrefix + t.ID }

// Expired reports whether t has expired at now.
func (t Token) Expired(now time.Time) bool { return !now.Before(t.ExpiresAt) }

// TokenSecret is what Igloo keeps of a token's secret.
type TokenSecret struct {
	// Hash is the SHA-256 of the whole token.
	Hash []byte
	// SCRAM holds the keys a SQL login checks, derived with the user's
	// TokenFactors when the token was created.
	SCRAM scram.StoredCredentials
}

// TokenCredential is a token and its SCRAM keys, for a SQL login.
type TokenCredential struct {
	Token Token
	SCRAM scram.StoredCredentials
}

var (
	ErrTokenNotFound = errors.New("access token does not exist")
	ErrTokenExists   = errors.New("an access token of that name already exists")
	ErrTooManyTokens = fmt.Errorf("a user may have at most %d access tokens", MaxTokensPerUser)
	// ErrInvalidToken is returned by TokenChecker.Verify for every token
	// that does not sign in: malformed, unknown, revoked or expired, or of a
	// user who does not exist or is disabled. The wrapped error says which,
	// for logs; clients should be told no more than this.
	ErrInvalidToken = errors.New("invalid access token")
)

// TokenStore keeps access tokens. Implementations must be safe for
// concurrent use.
type TokenStore interface {
	// CreateToken stores t, which must have a unique ID. It fails with
	// ErrTokenExists if the user has a token named t.Name, ErrTooManyTokens
	// if they have MaxTokensPerUser unexpired ones, and ErrNotFound if the
	// user does not exist.
	CreateToken(ctx context.Context, t Token, secret TokenSecret) error
	// Tokens lists user's tokens, or everyone's when user is empty, by user
	// and name.
	Tokens(ctx context.Context, user string) ([]Token, error)
	// DropToken deletes user's token called name and returns it.
	DropToken(ctx context.Context, user, name string) (Token, error)
	// TokenCredentials returns the SCRAM keys of user's unexpired tokens.
	TokenCredentials(ctx context.Context, user string) ([]TokenCredential, error)
	// TokenByID returns a token and its secret's hash.
	TokenByID(ctx context.Context, id string) (Token, []byte, error)
	// TokenUsed records that the token signed in at at.
	TokenUsed(ctx context.Context, id string, at time.Time) error
}

// NewToken makes a token for user, with SCRAM keys derived with factors.
// It returns the token to show the user once, and what to store.
func NewToken(user, name, by string, expires time.Time, factors scram.KeyFactors) (string, Token, TokenSecret, error) {
	raw := make([]byte, 42)
	if _, err := rand.Read(raw); err != nil {
		return "", Token{}, TokenSecret{}, err
	}
	id := tokenEncoding.EncodeToString(raw[:10])
	secret := TokenPrefix + id + tokenEncoding.EncodeToString(raw[10:])
	creds, err := deriveCredentials(secret, factors)
	if err != nil {
		return "", Token{}, TokenSecret{}, err
	}
	t := Token{ID: id, User: user, Name: name, CreatedBy: by, CreatedAt: time.Now(), ExpiresAt: expires}
	return secret, t, TokenSecret{Hash: HashToken(secret), SCRAM: creds}, nil
}

// HashToken is the SHA-256 of a token. Tokens are long and random, so a
// plain hash protects them as well as a slow one would.
func HashToken(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

// TokenID returns the ID of a well-formed token.
func TokenID(secret string) (string, bool) {
	rest, ok := strings.CutPrefix(secret, TokenPrefix)
	if !ok || len(rest) != tokenIDLength+tokenSecretLength {
		return "", false
	}
	if _, err := tokenEncoding.DecodeString(rest[:tokenIDLength]); err != nil {
		return "", false
	}
	if _, err := tokenEncoding.DecodeString(rest[tokenIDLength:]); err != nil {
		return "", false
	}
	return rest[:tokenIDLength], true
}

// LooksLikeToken reports whether s has the form of an access token, so a
// client that sent one as a password can be told what went wrong.
func LooksLikeToken(s string) bool {
	_, ok := TokenID(s)
	return ok
}

var tokenName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.\-]{0,62}$`)

// ValidTokenName reports whether name may name a token: up to 63 letters,
// digits, dots, dashes and underscores, starting with a letter or digit.
func ValidTokenName(name string) bool { return tokenName.MatchString(name) }

// TokenChecker verifies access tokens presented as bearer tokens, for HTTP
// APIs such as an Iceberg REST endpoint, and records when tokens are used.
// SQL logins check tokens through SCRAM instead (see SCRAMExchange) and use
// only Used. It is safe for concurrent use.
type TokenChecker struct {
	users  Store
	tokens TokenStore
	now    func() time.Time

	mu sync.Mutex
	// recorded is when each token's use was last written, so that a busy
	// client writes at most once a minute.
	recorded map[string]time.Time
}

// tokenUseInterval is how often a token's last use is written.
const tokenUseInterval = time.Minute

// NewTokenChecker checks tokens kept in tokens, of users kept in users.
func NewTokenChecker(users Store, tokens TokenStore) *TokenChecker {
	return &TokenChecker{users: users, tokens: tokens, now: time.Now, recorded: map[string]time.Time{}}
}

// Verify checks a token, such as the value of an "Authorization: Bearer"
// header, and returns its user and the token. The user exists and may sign
// in. Every failure wraps ErrInvalidToken; respond to all of them alike.
// Verify records the token's use; callers should also keep a login history
// and limit failed attempts, as SQL logins do.
func (c *TokenChecker) Verify(ctx context.Context, secret string) (User, Token, error) {
	invalid := func(reason string) (User, Token, error) {
		return User{}, Token{}, fmt.Errorf("%w: %s", ErrInvalidToken, reason)
	}
	id, ok := TokenID(secret)
	if !ok {
		return invalid("malformed")
	}
	t, hash, err := c.tokens.TokenByID(ctx, id)
	if errors.Is(err, ErrTokenNotFound) {
		// Compare anyway, so an unknown ID takes as long as a wrong secret.
		subtle.ConstantTimeCompare(HashToken(secret), make([]byte, sha256.Size))
		return invalid("unknown or revoked")
	}
	if err != nil {
		return User{}, Token{}, err
	}
	if subtle.ConstantTimeCompare(HashToken(secret), hash) != 1 {
		return invalid("wrong secret")
	}
	if t.Expired(c.now()) {
		return invalid("expired")
	}
	u, err := c.users.Get(ctx, t.User)
	if errors.Is(err, ErrNotFound) {
		return invalid("user does not exist")
	}
	if err != nil {
		return User{}, Token{}, err
	}
	if u.Disabled {
		return invalid("user is disabled")
	}
	c.Used(t.ID)
	return u, t, nil
}

// Used records, in the background and at most once a minute per token,
// that a token signed in now.
func (c *TokenChecker) Used(id string) {
	now := c.now()
	c.mu.Lock()
	if last, ok := c.recorded[id]; ok && now.Sub(last) < tokenUseInterval {
		c.mu.Unlock()
		return
	}
	c.recorded[id] = now
	if len(c.recorded) > 100_000 {
		// Forget old entries rather than grow without bound.
		for k, t := range c.recorded {
			if now.Sub(t) >= tokenUseInterval {
				delete(c.recorded, k)
			}
		}
	}
	c.mu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = c.tokens.TokenUsed(ctx, id, now)
	}()
}
