// Package ssotest runs a small OpenID Connect provider for tests: discovery,
// signing keys, an authorization endpoint that signs in whoever the test
// names, and a token endpoint that checks the client, the redirect URI and
// PKCE before it issues an ID token. It is not for use outside tests.
package ssotest

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/coreos/go-oidc/v3/oidc/oidctest"
)

// Provider is a running test provider.
type Provider struct {
	ClientID     string
	ClientSecret string

	srv  *httptest.Server
	keys *oidctest.Server
	key  *rsa.PrivateKey

	mu    sync.Mutex
	next  map[string]any
	codes map[string]grant
	// Tokens counts the ID tokens issued.
	Tokens int
}

// grant is an authorization code waiting to be exchanged.
type grant struct {
	claims      map[string]any
	redirectURI string
	challenge   string
	nonce       string
}

const keyID = "test-key"

// New starts a provider for one client; it stops when the test ends.
func New(t testing.TB, clientID, clientSecret string) *Provider {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	p := &Provider{ClientID: clientID, ClientSecret: clientSecret, key: key, codes: map[string]grant{},
		keys: &oidctest.Server{PublicKeys: []oidctest.PublicKey{{PublicKey: key.Public(), KeyID: keyID, Algorithm: oidc.RS256}}}}
	mux := http.NewServeMux()
	mux.Handle("GET /.well-known/openid-configuration", p.keys)
	mux.Handle("GET /keys", p.keys)
	mux.HandleFunc("GET /auth", p.authorize)
	mux.HandleFunc("POST /token", p.token)
	p.srv = httptest.NewServer(mux)
	p.keys.SetIssuer(p.srv.URL)
	t.Cleanup(p.srv.Close)
	return p
}

// Issuer is the provider's issuer URL.
func (p *Provider) Issuer() string { return p.srv.URL }

// SignInAs sets the claims of whoever signs in next, besides iss, aud, sub
// (taken from claims["sub"]), exp, iat and nonce.
func (p *Provider) SignInAs(claims map[string]any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.next = maps.Clone(claims)
}

// Authorize visits an authorization URL as the browser would, signing in
// as SignInAs said, and returns where the provider redirects the browser:
// the client's redirect URI with a code and the state.
func (p *Provider) Authorize(authURL string) (*url.URL, error) {
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Get(authURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck
	return resp.Location()
}

func (p *Provider) authorize(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Get("client_id") != p.ClientID || q.Get("response_type") != "code" || q.Get("code_challenge_method") != "S256" ||
		q.Get("code_challenge") == "" || q.Get("redirect_uri") == "" {
		http.Error(w, "bad authorization request", http.StatusBadRequest)
		return
	}
	back, err := url.Parse(q.Get("redirect_uri"))
	if err != nil {
		http.Error(w, "bad redirect_uri", http.StatusBadRequest)
		return
	}
	p.mu.Lock()
	claims := p.next
	code := random()
	if claims != nil {
		p.codes[code] = grant{claims: claims, redirectURI: q.Get("redirect_uri"), challenge: q.Get("code_challenge"), nonce: q.Get("nonce")}
	}
	p.mu.Unlock()
	values := url.Values{"state": {q.Get("state")}}
	if claims == nil {
		values.Set("error", "access_denied")
		values.Set("error_description", "the user declined")
	} else {
		values.Set("code", code)
	}
	back.RawQuery = values.Encode()
	http.Redirect(w, r, back.String(), http.StatusFound)
}

func (p *Provider) token(w http.ResponseWriter, r *http.Request) {
	fail := func(code string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": code})
	}
	if err := r.ParseForm(); err != nil || r.PostForm.Get("grant_type") != "authorization_code" {
		fail("invalid_request")
		return
	}
	id, secret, ok := r.BasicAuth()
	if ok {
		id, _ = url.QueryUnescape(id)
		secret, _ = url.QueryUnescape(secret)
	} else {
		id, secret = r.PostForm.Get("client_id"), r.PostForm.Get("client_secret")
	}
	if id != p.ClientID || subtle.ConstantTimeCompare([]byte(secret), []byte(p.ClientSecret)) != 1 {
		fail("invalid_client")
		return
	}
	p.mu.Lock()
	g, found := p.codes[r.PostForm.Get("code")]
	delete(p.codes, r.PostForm.Get("code"))
	p.mu.Unlock()
	sum := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
	if !found || g.redirectURI != r.PostForm.Get("redirect_uri") ||
		base64.RawURLEncoding.EncodeToString(sum[:]) != g.challenge {
		fail("invalid_grant")
		return
	}
	claims := maps.Clone(g.claims)
	claims["nonce"] = g.nonce
	idToken := p.Sign(claims, p.ClientID, time.Hour)
	p.mu.Lock()
	p.Tokens++
	p.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token": random(), "token_type": "Bearer", "expires_in": 3600, "id_token": idToken,
	})
}

// Sign makes a token signed with the provider's key for audience, valid
// for ttl (negative for one that has expired), with claims besides iss,
// aud, exp and iat.
func (p *Provider) Sign(claims map[string]any, audience string, ttl time.Duration) string {
	all := maps.Clone(claims)
	now := time.Now()
	all["iss"] = p.srv.URL
	if _, ok := all["aud"]; !ok {
		all["aud"] = audience
	}
	all["exp"] = now.Add(ttl).Unix()
	all["iat"] = now.Add(-time.Minute).Unix()
	raw, err := json.Marshal(all)
	if err != nil {
		panic(err)
	}
	return oidctest.SignIDToken(p.key, keyID, oidc.RS256, string(raw))
}

// SignWithOtherKey makes a token like Sign but with a key the provider
// does not publish.
func (p *Provider) SignWithOtherKey(claims map[string]any, audience string) string {
	other, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	all := maps.Clone(claims)
	all["iss"], all["aud"], all["exp"] = p.srv.URL, audience, time.Now().Add(time.Hour).Unix()
	raw, _ := json.Marshal(all)
	return oidctest.SignIDToken(other, keyID, oidc.RS256, string(raw))
}

func random() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}
