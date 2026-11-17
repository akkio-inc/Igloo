// Package awscreds keeps one set of AWS credentials for the whole process,
// from the SDK's default chain (an instance profile, IAM roles for service
// accounts, the environment), and refreshes it in the background ahead of
// its expiry.
//
// iceberg-go builds an S3 file system for each table it loads, and with it
// a new provider of the default chain, whose cache starts empty: the first
// request of every statement's scan planning fetched credentials from the
// EC2 instance metadata service, under the statement's deadline. With
// sixteen BI users and heavy reports on one replica, the metadata service
// answered late, and statements fell back to the replica for want of
// credentials ("failed to refresh cached credentials ... ec2imds:
// GetMetadata, request canceled, context deadline exceeded"). Here a fetch
// runs once for every caller waiting on it, under a deadline of its own,
// not any caller's; while the credentials held are valid, a caller gets
// them at once, whatever a refresh is doing.
package awscreds

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// fetchTimeout bounds a fetch from the chain, for every caller waiting on
// it. validMargin is how long before their expiry credentials are no
// longer handed out: requests signed with them take a while. refreshAhead
// is how long before their expiry the background refreshes credentials,
// or halfway through what is left of them when that is sooner.
// retryAfter is the wait before the background tries again after a failed
// refresh, doubling up to maxRetryAfter. Variables, for tests.
var (
	fetchTimeout  = 30 * time.Second
	validMargin   = time.Minute
	refreshAhead  = 15 * time.Minute
	retryAfter    = 5 * time.Second
	maxRetryAfter = time.Minute
)

// Provider is an aws.CredentialsProvider over the chain, which it calls once
// at a time, for every caller, and ahead of expiry in the background.
type Provider struct {
	// chain resolves the chain's provider; it is called once.
	chain func(context.Context) (aws.CredentialsProvider, error)
	log   *slog.Logger
	// now is the clock, and after the timer the background waits on; both
	// are variables for tests.
	now   func() time.Time
	after func(time.Duration) <-chan time.Time

	mu       sync.Mutex
	inner    aws.CredentialsProvider
	creds    aws.Credentials
	have     bool
	err      error
	fetching chan struct{}
	started  bool
}

// New returns a provider over the provider chain returns, which it calls
// once, when credentials are first needed.
func New(chain func(context.Context) (aws.CredentialsProvider, error), log *slog.Logger) *Provider {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Provider{chain: chain, log: log, now: time.Now, after: time.After}
}

// Retrieve returns the credentials held, at once while they are valid;
// otherwise it waits, until ctx ends, for the fetch under way or one it
// starts, which ctx does not end.
func (p *Provider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	// A fetch under way when it came may have begun before the chain
	// could answer, as a refresh still waiting on a stuck metadata
	// service: once it fails, a fetch of its own follows.
	for joined := true; ; {
		p.mu.Lock()
		if p.validLocked() {
			c := p.creds
			p.mu.Unlock()
			return c, nil
		}
		joined = joined && p.fetching != nil
		done := p.fetchLocked()
		p.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return aws.Credentials{}, ctx.Err()
		}
		p.mu.Lock()
		valid, creds, err := p.validLocked(), p.creds, p.err
		p.mu.Unlock()
		switch {
		case valid:
			return creds, nil
		case joined:
			joined = false
			continue
		case err == nil:
			return aws.Credentials{}, errors.New("awscreds: the credentials fetched have expired")
		}
		return aws.Credentials{}, err
	}
}

// validLocked reports whether the credentials held may be handed out.
func (p *Provider) validLocked() bool {
	return p.have && (!p.creds.CanExpire || p.now().Add(validMargin).Before(p.creds.Expires))
}

// fetchLocked starts a fetch, unless one is under way, and returns the
// channel closed when it ends.
func (p *Provider) fetchLocked() <-chan struct{} {
	if p.fetching != nil {
		return p.fetching
	}
	done := make(chan struct{})
	p.fetching = done
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), fetchTimeout)
		defer cancel()
		creds, err := p.fetch(ctx)
		p.mu.Lock()
		if err == nil {
			p.creds, p.have, p.err = creds, true, nil
		} else {
			p.err = err
		}
		p.fetching = nil
		start := err == nil && creds.CanExpire && !p.started
		if start {
			p.started = true
		}
		p.mu.Unlock()
		close(done)
		if start {
			go p.refresh()
		}
	}()
	return done
}

// fetch calls the chain's provider, resolving it first if need be.
func (p *Provider) fetch(ctx context.Context) (aws.Credentials, error) {
	p.mu.Lock()
	inner := p.inner
	p.mu.Unlock()
	if inner == nil {
		var err error
		if inner, err = p.chain(ctx); err != nil {
			return aws.Credentials{}, err
		}
		p.mu.Lock()
		p.inner = inner
		p.mu.Unlock()
	}
	// The default chain's provider is the SDK's cache of it, which hands
	// out what it holds until that has expired. Asked ahead of the expiry,
	// as refresh asks, it gave the same credentials back, and for their
	// last validMargin, which Retrieve does not hand out, every caller
	// failed ("the credentials fetched have expired"): a minute an hour
	// with IRSA. This fetch is for new credentials, so the cache gets them.
	if c, ok := inner.(interface{ Invalidate() }); ok {
		c.Invalidate()
	}
	return inner.Retrieve(ctx)
}

// refresh fetches credentials ahead of the expiry of those held, for good,
// and after a failure again in a while, keeping those held meanwhile.
func (p *Provider) refresh() {
	wait := retryAfter
	for {
		p.mu.Lock()
		next := refreshAt(p.creds, p.now()).Sub(p.now())
		p.mu.Unlock()
		<-p.after(max(next, time.Second))
		p.mu.Lock()
		done := p.fetchLocked()
		p.mu.Unlock()
		<-done
		p.mu.Lock()
		err, expires := p.err, p.creds.Expires
		p.mu.Unlock()
		if err == nil {
			wait = retryAfter
			continue
		}
		p.log.Warn("could not refresh the AWS credentials; keeping those held until they expire", "expires", expires,
			"retry_in", wait, "err", err)
		<-p.after(wait)
		wait = min(2*wait, maxRetryAfter)
	}
}

// refreshAt is when credentials c, held at now, are refreshed: refreshAhead
// before they expire, or halfway through what is left of them when that
// is sooner, and not before now.
func refreshAt(c aws.Credentials, now time.Time) time.Time {
	left := c.Expires.Sub(now)
	at := c.Expires.Add(-min(refreshAhead, left/2))
	if at.Before(now) {
		return now
	}
	return at
}
