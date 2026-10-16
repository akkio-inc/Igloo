package auth

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xdg-go/scram"
)

// exchange runs a SCRAM login of a client holding secret against an
// exchange accepting candidates, returning the matched index or -1.
func exchange(t *testing.T, candidates []scram.StoredCredentials, secret string) int {
	t.Helper()
	x, err := NewSCRAMExchange(candidates)
	if err != nil {
		t.Fatal(err)
	}
	client, err := scram.SHA256.NewClient("", secret, "")
	if err != nil {
		t.Fatal(err)
	}
	cc := client.NewConversation()
	first, err := cc.Step("")
	if err != nil {
		t.Fatal(err)
	}
	serverFirst, err := x.First(first)
	if err != nil {
		t.Fatal(err)
	}
	final, err := cc.Step(serverFirst)
	if err != nil {
		t.Fatal(err)
	}
	serverFinal, err := x.Final(final)
	if err != nil {
		if !errors.Is(err, ErrSCRAMProof) || x.Matched() != -1 {
			t.Errorf("failed exchange: %v, matched %d", err, x.Matched())
		}
		return -1
	}
	// The client checks the server's signature too.
	if _, err := cc.Step(serverFinal); err != nil || !cc.Valid() {
		t.Errorf("client rejected the server: %v", err)
	}
	return x.Matched()
}

func TestSCRAMExchange(t *testing.T) {
	verifier, err := NewVerifier("the-password")
	if err != nil {
		t.Fatal(err)
	}
	password, _ := ParseVerifier(verifier)
	user := User{Name: "ana", Verifier: verifier}
	factors, ok := TokenFactors(user)
	if !ok || factors != password.KeyFactors {
		t.Fatalf("TokenFactors with a password: %+v %v", factors, ok)
	}
	a, _, ah, err := NewToken("ana", "a", "ana", time.Now().Add(time.Hour), factors)
	if err != nil {
		t.Fatal(err)
	}
	b, _, bh, _ := NewToken("ana", "b", "ana", time.Now().Add(time.Hour), factors)
	candidates := []scram.StoredCredentials{password, ah.SCRAM, bh.SCRAM}

	for want, secret := range []string{"the-password", a, b} {
		if got := exchange(t, candidates, secret); got != want {
			t.Errorf("secret %d matched %d", want, got)
		}
	}
	for _, wrong := range []string{"not-the-password", flipLast(a), ""} {
		if got := exchange(t, candidates, wrong); got != -1 {
			t.Errorf("%q matched %d", wrong, got)
		}
	}

	other, _ := NewVerifier("other")
	otherCreds, _ := ParseVerifier(other)
	if _, err := NewSCRAMExchange([]scram.StoredCredentials{password, otherCreds}); err == nil {
		t.Error("candidates with different salts accepted")
	}
	if _, err := NewSCRAMExchange(nil); err == nil {
		t.Error("an exchange with no candidates")
	}

	// A new password can keep the salt, so tokens keep working.
	again, err := NewVerifierWith("new-password", factors)
	if err != nil {
		t.Fatal(err)
	}
	newCreds, _ := ParseVerifier(again)
	if newCreds.KeyFactors != factors || exchange(t, []scram.StoredCredentials{newCreds, ah.SCRAM}, a) != 1 ||
		exchange(t, []scram.StoredCredentials{newCreds, ah.SCRAM}, "new-password") != 0 {
		t.Error("a token stopped working after a password change that kept the salt")
	}
	if !CheckSecret(newCreds, "new-password") || CheckSecret(newCreds, "the-password") {
		t.Error("CheckSecret")
	}

	// Without a password, tokens use the user's own salt.
	salt, _ := NewSalt()
	if f, ok := TokenFactors(User{TokenSalt: salt}); !ok || f.Salt != string(salt) || f.Iters != scramIterations {
		t.Errorf("TokenFactors without a password: %+v %v", f, ok)
	}
	if _, ok := TokenFactors(User{}); ok {
		t.Error("TokenFactors of a user with neither")
	}
}

func TestTokenFormat(t *testing.T) {
	salt, _ := NewSalt()
	f := scram.KeyFactors{Salt: string(salt), Iters: 4096}
	secret, tok, hashes, err := NewToken("ana", "dbt", "root", time.Now().Add(time.Hour), f)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(secret, TokenPrefix) || len(secret) != len(TokenPrefix)+68 || strings.ToLower(secret) != secret {
		t.Errorf("token %q", secret)
	}
	if id, ok := TokenID(secret); !ok || id != tok.ID || !strings.HasPrefix(secret, tok.Prefix()) {
		t.Errorf("TokenID: %q %v", id, ok)
	}
	if string(hashes.Hash) != string(HashToken(secret)) || !CheckSecret(hashes.SCRAM, secret) {
		t.Error("the token's hashes do not match it")
	}
	other, _, _, _ := NewToken("ana", "dbt", "root", time.Now().Add(time.Hour), f)
	if other == secret {
		t.Error("two tokens are the same")
	}
	for _, bad := range []string{"", "igloo_", secret[1:], secret + "a", strings.ToUpper(secret), "igloo_" + strings.Repeat("1", 68),
		strings.Replace(secret, "igloo_", "iglu_", 1)} {
		if LooksLikeToken(bad) {
			t.Errorf("%q looks like a token", bad)
		}
	}
	for name, ok := range map[string]bool{"dbt": true, "Tableau-prod.2": true, "a": true, "": false, "-x": false,
		"has space": false, strings.Repeat("x", 64): false, "ümlaut": false} {
		if ValidTokenName(name) != ok {
			t.Errorf("ValidTokenName(%q) = %v", name, !ok)
		}
	}
}

func TestTokenChecker(t *testing.T) {
	ctx := context.Background()
	d := NewMemoryDirectory()
	salt, _ := NewSalt()
	for _, u := range []User{{Name: "root", Role: RoleAdmin, Verifier: "v"}, {Name: "svc", Role: RoleReader, Kind: KindService, TokenSalt: salt}} {
		if err := d.Create(ctx, u); err != nil {
			t.Fatal(err)
		}
	}
	svc, _ := d.Get(ctx, "svc")
	factors, _ := TokenFactors(svc)
	mint := func(name string, expires time.Time) string {
		secret, tok, hashes, err := NewToken("svc", name, "root", expires, factors)
		if err != nil {
			t.Fatal(err)
		}
		if err := d.CreateToken(ctx, tok, hashes); err != nil {
			t.Fatal(err)
		}
		return secret
	}
	good := mint("good", time.Now().Add(time.Hour))
	expired := mint("expired", time.Now().Add(-time.Second))

	c := NewTokenChecker(d, d)
	u, tok, err := c.Verify(ctx, good)
	if err != nil || u.Name != "svc" || tok.Name != "good" {
		t.Fatalf("Verify: %+v %+v %v", u, tok, err)
	}
	waitUsed := func() time.Time {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if list, _ := d.Tokens(ctx, "svc"); !list[1].LastUsedAt.IsZero() {
				return list[1].LastUsedAt
			}
			time.Sleep(10 * time.Millisecond)
		}
		return time.Time{}
	}
	if waitUsed().IsZero() {
		t.Error("Verify did not record the token's use")
	}

	wrong := flipLast(good)
	unknown := mint("unknown", time.Now().Add(time.Hour))
	if _, err := d.DropToken(ctx, "svc", "unknown"); err != nil {
		t.Fatal(err)
	}
	for name, secret := range map[string]string{"malformed": "Bearer x", "wrong secret": wrong, "expired": expired, "revoked": unknown} {
		if _, _, err := c.Verify(ctx, secret); !errors.Is(err, ErrInvalidToken) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := d.SetLogin(ctx, "svc", false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Verify(ctx, good); !errors.Is(err, ErrInvalidToken) || !strings.Contains(err.Error(), "disabled") {
		t.Errorf("a disabled user's token: %v", err)
	}
	if err := d.Delete(ctx, "svc"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Verify(ctx, good); !errors.Is(err, ErrInvalidToken) {
		t.Errorf("a dropped user's token: %v", err)
	}
}

// flipLast changes a token's last character to another valid one.
func flipLast(secret string) string {
	last := "a"
	if strings.HasSuffix(secret, "a") {
		last = "b"
	}
	return secret[:len(secret)-1] + last
}

func TestTokenUseIsThrottled(t *testing.T) {
	d := NewMemoryDirectory()
	c := NewTokenChecker(d, d)
	now := time.Now()
	c.now = func() time.Time { return now }
	c.Used("a")
	first := c.recorded["a"]
	now = now.Add(30 * time.Second)
	c.Used("a")
	if c.recorded["a"] != first {
		t.Error("a second use within a minute was written")
	}
	now = now.Add(time.Minute)
	c.Used("a")
	if c.recorded["a"] == first {
		t.Error("a use a minute later was not written")
	}
}
