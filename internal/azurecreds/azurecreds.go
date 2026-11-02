// Package azurecreds keeps one Azure identity's tokens for the whole process, and
// refreshes them in the background ahead of their expiry, as package awscreds does for
// AWS: Igloo's own clients take them from it, and DuckDB through an endpoint of its own.
//
// DuckDB's credential chain asks the identity for a token as it opens a client, and on
// an Azure machine that is the instance metadata service, which allows a VM a few
// requests a second: on a real account, a table of 128 partitions took a statement past
// that within a second, and every statement after it for a while ("Failed to get token
// from ChainedTokenCredential", from a 429). The chain takes no client ID, so that a
// machine of two identities got no token. And a token given to DuckDB in a secret lasts
// as long as a statement that holds it: its clients could not renew it. A chain whose
// credential asks an endpoint does: it asks as a client opens and again as a token nears
// its expiry, the Azure SDK's way, however long the statement runs. So this package
// serves the managed identity protocol of Azure App Service on the loopback, which the
// SDK prefers to the metadata service where the environment names it.
package azurecreds

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
)

// fetchTimeout bounds a fetch from the identity, for every caller waiting on it.
// validMargin is how long before their expiry tokens are no longer handed out.
// refreshAhead is how long before their expiry the background asks for new ones, or
// halfway through what is left of them when that is sooner. retryAfter is the wait
// before it tries again after a failure, or for a token no newer than the one held,
// doubling up to maxRetryAfter. Variables, for tests.
var (
	fetchTimeout  = 30 * time.Second
	validMargin   = time.Minute
	refreshAhead  = 15 * time.Minute
	retryAfter    = 5 * time.Second
	maxRetryAfter = time.Minute
)

// Provider is an azcore.TokenCredential over an identity, which it asks once at a time
// for each scope, for every caller, and ahead of expiry in the background.
type Provider struct {
	cred azcore.TokenCredential
	log  *slog.Logger
	// now is the clock, and after the timer the background waits on; both are
	// variables for tests.
	now   func() time.Time
	after func(time.Duration) <-chan time.Time

	mu     sync.Mutex
	scopes map[string]*scope
}

// scope is the token held of a scope.
type scope struct {
	token    azcore.AccessToken
	have     bool
	err      error
	fetching chan struct{}
	started  bool
}

// New returns a provider over cred.
func New(cred azcore.TokenCredential, log *slog.Logger) *Provider {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Provider{cred: cred, log: log, now: time.Now, after: time.After, scopes: map[string]*scope{}}
}

// GetToken implements azcore.TokenCredential: the token held, at once while it is valid;
// otherwise it waits, until ctx ends, for the fetch under way or one it starts, which
// ctx does not end. Tokens go by scopes alone.
func (p *Provider) GetToken(ctx context.Context, opts policy.TokenRequestOptions) (azcore.AccessToken, error) {
	key := strings.Join(opts.Scopes, " ")
	p.mu.Lock()
	sc := p.scopes[key]
	if sc == nil {
		sc = &scope{}
		p.scopes[key] = sc
	}
	if p.validLocked(sc) {
		t := sc.token
		p.mu.Unlock()
		return t, nil
	}
	done := p.fetchLocked(key, sc, opts.Scopes)
	p.mu.Unlock()
	select {
	case <-done:
	case <-ctx.Done():
		return azcore.AccessToken{}, ctx.Err()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case p.validLocked(sc):
		return sc.token, nil
	case sc.err != nil:
		return azcore.AccessToken{}, sc.err
	}
	return azcore.AccessToken{}, errors.New("azurecreds: the token fetched has expired")
}

func (p *Provider) validLocked(sc *scope) bool {
	return sc.have && p.now().Add(validMargin).Before(sc.token.ExpiresOn)
}

// fetchLocked starts a fetch of sc, unless one is under way, and returns the channel
// closed when it ends.
func (p *Provider) fetchLocked(key string, sc *scope, scopes []string) <-chan struct{} {
	if sc.fetching != nil {
		return sc.fetching
	}
	done := make(chan struct{})
	sc.fetching = done
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), fetchTimeout)
		defer cancel()
		t, err := p.cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: scopes})
		p.mu.Lock()
		if err == nil {
			sc.err = nil
			// One that expires no later is of no use, as an identity's cache gives.
			if !sc.have || t.ExpiresOn.After(sc.token.ExpiresOn) {
				sc.token, sc.have = t, true
			}
		} else {
			sc.err = err
		}
		sc.fetching = nil
		start := err == nil && !sc.started
		if start {
			sc.started = true
		}
		p.mu.Unlock()
		close(done)
		if start {
			go p.refresh(key, sc, scopes)
		}
	}()
	return done
}

// refresh asks for new tokens ahead of the expiry of what sc holds, for good, keeping
// what it holds through failures, until it expires.
func (p *Provider) refresh(key string, sc *scope, scopes []string) {
	wait := retryAfter
	for {
		p.mu.Lock()
		expires := sc.token.ExpiresOn
		next := refreshAt(expires, p.now()).Sub(p.now())
		p.mu.Unlock()
		<-p.after(max(next, time.Second))
		p.mu.Lock()
		done := p.fetchLocked(key, sc, scopes)
		p.mu.Unlock()
		<-done
		p.mu.Lock()
		err, renewed := sc.err, sc.token.ExpiresOn.After(expires)
		p.mu.Unlock()
		if err == nil && renewed {
			wait = retryAfter
			continue
		}
		if err != nil {
			p.log.Warn("could not refresh the Azure token; keeping the one held until it expires", "expires", expires, "retry_in", wait, "err", err)
		}
		// An identity may give the token it has until shortly before its expiry.
		<-p.after(wait)
		wait = min(2*wait, maxRetryAfter)
	}
}

// refreshAt is when a token that expires at expires, held at now, is refreshed:
// refreshAhead before, or halfway through what is left when that is sooner, and not
// before now.
func refreshAt(expires, now time.Time) time.Time {
	left := expires.Sub(now)
	at := expires.Add(-min(refreshAhead, left/2))
	if at.Before(now) {
		return now
	}
	return at
}

// Handler serves p's tokens as Azure App Service's managed identity endpoint does, to
// callers that send header.
func (p *Provider) Handler(header string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-IDENTITY-HEADER") != header {
			http.Error(w, "not yours to ask", http.StatusForbidden)
			return
		}
		resource := r.URL.Query().Get("resource")
		if resource == "" {
			http.Error(w, "no resource", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), fetchTimeout)
		defer cancel()
		t, err := p.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{strings.TrimSuffix(resource, "/") + "/.default"}})
		if err != nil {
			p.log.Warn("could not get a token of the Azure identity for DuckDB", "err", err)
			http.Error(w, "no token", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"access_token": t.Token,
			"expires_on":   strconv.FormatInt(t.ExpiresOn.Unix(), 10),
			"resource":     resource,
			"token_type":   "Bearer",
		})
	})
}

var shared struct {
	once    sync.Once
	p       *Provider
	serving bool
	err     error
}

// Shared returns the process's provider of its Azure identity, as Igloo's own clients
// find it: AZURE_CLIENT_ID, Workload Identity, a managed identity.
func Shared(log *slog.Logger) (*Provider, error) {
	shared.once.Do(func() {
		// Before the environment names the endpoint below, which this credential
		// would otherwise ask.
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			shared.err = fmt.Errorf("the Azure identity: %w", err)
			return
		}
		shared.p = New(cred, log)
		if err := shared.p.serve(); err != nil {
			log.Warn("no endpoint of the Azure identity for DuckDB; it asks the metadata service itself", "err", err)
			return
		}
		shared.serving = true
	})
	return shared.p, shared.err
}

// Serving reports whether DuckDB's managed identity credential asks this process, as
// Shared arranged.
func Serving() bool {
	return shared.serving
}

// serve starts the endpoint on the loopback, and names it in the environment.
func (p *Provider) serve() error {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return err
	}
	header := hex.EncodeToString(b[:])
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: p.Handler(header), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
			p.log.Error("the endpoint of the Azure identity for DuckDB stopped", "err", err)
		}
	}()
	if err := os.Setenv("IDENTITY_ENDPOINT", "http://"+l.Addr().String()+"/msi/token"); err != nil {
		return err
	}
	return os.Setenv("IDENTITY_HEADER", header)
}
