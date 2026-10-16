package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"encoding/base64"
	"errors"

	"github.com/xdg-go/scram"
)

// A SCRAM-SHA-256 login proves knowledge of one secret: the server sends a
// salt and iteration count, the client derives a proof from its secret, and
// the server checks the proof against what it stored. A user may have
// several secrets, their password and their access tokens, and a client
// sends any one of them as "the password". All of them are derived with the
// same salt and iteration count (TokenFactors), so the server can send
// those before it knows which secret the client holds, and then check the
// proof against each.

// TokenFactors returns the salt and iteration count of u's SCRAM secrets:
// those of u's password verifier, or with no password u.TokenSalt. ok is
// false when u has neither yet.
func TokenFactors(u User) (factors scram.KeyFactors, ok bool) {
	if u.Verifier != "" {
		creds, err := ParseVerifier(u.Verifier)
		if err != nil {
			return factors, false
		}
		return creds.KeyFactors, true
	}
	if len(u.TokenSalt) == 0 {
		return factors, false
	}
	return scram.KeyFactors{Salt: string(u.TokenSalt), Iters: scramIterations}, true
}

// CheckSecret derives the stored key from secret the way a SCRAM client
// does and compares it with creds', in constant time. It checks a password
// sent in the clear, as the web console's sign-in does.
func CheckSecret(creds scram.StoredCredentials, secret string) bool {
	derived, err := deriveCredentials(secret, creds.KeyFactors)
	if err != nil {
		return false
	}
	return hmac.Equal(derived.StoredKey, creds.StoredKey)
}

// SCRAMExchange is the server side of one SCRAM-SHA-256 login that accepts
// any of several secrets sharing a salt and iteration count. It runs one
// conversation of the scram library per secret, all with the same server
// nonce, so they send the client identical messages; the one whose stored
// key matches the client's proof wins. Every candidate is checked, so the
// time taken does not depend on which one matches.
type SCRAMExchange struct {
	convs   []*scram.ServerConversation
	matched int
}

// ErrSCRAMProof is returned when the client's proof matches no candidate.
var ErrSCRAMProof = errors.New("SCRAM proof matches no secret")

// NewSCRAMExchange starts an exchange that accepts any of candidates, which
// must share their KeyFactors. Pass mock credentials, which nothing can
// match, rather than none.
func NewSCRAMExchange(candidates []scram.StoredCredentials) (*SCRAMExchange, error) {
	if len(candidates) == 0 {
		return nil, errors.New("scram: no credentials to check against")
	}
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return nil, err
	}
	nonce := base64.StdEncoding.EncodeToString(raw)
	x := &SCRAMExchange{matched: -1}
	for _, c := range candidates {
		if c.Salt != candidates[0].Salt || c.Iters != candidates[0].Iters {
			return nil, errors.New("scram: candidates have different salts")
		}
		srv, err := scram.SHA256.NewServer(func(string) (scram.StoredCredentials, error) { return c, nil })
		if err != nil {
			return nil, err
		}
		srv.WithNonceGenerator(func() string { return nonce })
		x.convs = append(x.convs, srv.NewConversation())
	}
	return x, nil
}

// First answers the client-first message with the server-first message.
func (x *SCRAMExchange) First(clientFirst string) (string, error) {
	var first string
	for i, conv := range x.convs {
		msg, err := conv.Step(clientFirst)
		if err != nil {
			return "", err
		}
		if i == 0 {
			first = msg
		} else if msg != first {
			// Same nonce, salt and iterations make the same message.
			return "", errors.New("scram: conversations diverged")
		}
	}
	return first, nil
}

// Final checks the client-final message. It returns the server-final
// message of the matching candidate, or an error wrapping ErrSCRAMProof.
func (x *SCRAMExchange) Final(clientFinal string) (string, error) {
	var (
		final    string
		firstErr error
	)
	for i, conv := range x.convs {
		msg, err := conv.Step(clientFinal)
		switch {
		case err == nil && conv.Valid() && x.matched < 0:
			x.matched, final = i, msg
		case err != nil && firstErr == nil:
			firstErr = err
		}
	}
	if x.matched >= 0 {
		return final, nil
	}
	return "", errors.Join(ErrSCRAMProof, firstErr)
}

// Matched is the index of the candidate the client proved it knows, or -1.
func (x *SCRAMExchange) Matched() int { return x.matched }
