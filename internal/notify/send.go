package notify

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/mail"
	"net/textproto"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// Sending. Every integration has an outbox of queueSize notifications and a
// goroutine of its own that sends them in turn, so a slow or saturated
// destination holds up only its own notifications: never the scheduler,
// other integrations, or built-in alerts. A full outbox fails a
// notification at once. Each integration may send a burst of sendBurst
// notifications, then one every sendEvery, on each replica; a notification
// waits up to maxWait for its turn and fails as rate limited after that.
// Low-priority notifications (failed-login notices, whose numbers clients
// drive) never wait, and leave lowReserve of the outbox and reserveTokens
// of the burst to the rest. Each attempt has attemptTimeout; failures that
// may pass (a network error, a timeout, 429 or 5xx, a temporary SMTP error)
// are tried again after backoff, up to maxAttempts in all.
const (
	queueSize      = 64
	lowReserve     = 16
	sendBurst      = 10
	reserveTokens  = 5
	sendEvery      = time.Second
	maxWait        = time.Minute
	attemptTimeout = 10 * time.Second
	maxAttempts    = 3
	// idleTimeout is how long an outbox's goroutine waits for more before
	// it ends.
	idleTimeout = time.Minute
	// maxRetryAfter caps how long a Retry-After header makes Igloo wait.
	maxRetryAfter = 10 * time.Second
	// maxResponse is how much of a webhook's answer is read.
	maxResponse = 4 << 10
)

// errorCode matches the answers of webhooks that are worth quoting in an
// error: a short error code such as Slack's "invalid_token". Anything else
// a webhook answers may echo what it was sent, the integration's headers
// included, and is not quoted: the alert history shows errors to the
// alert's owner.
var errorCode = regexp.MustCompile(`^[a-z][a-z_]{2,39}$`)

// backoff is how long to wait before each attempt after the first.
var backoff = []time.Duration{time.Second, 3 * time.Second}

// Options configure a Service.
type Options struct {
	Store Store
	// AllowHosts are hosts notifications may reach though they are not
	// public, and without TLS (IGLOO_NOTIFICATION_ALLOW_HOSTS).
	AllowHosts []string
	SMTP       config.SMTP
	// Jobs is where pipes are, for the idle pipe alert.
	Jobs tasks.Store
	// Statements lists the statements running on this replica, for the
	// long query alert.
	Statements func() []Statement
	// Retention is how long alert history is kept. Zero keeps it.
	Retention time.Duration
	// Resolver looks up hosts; nil means the system's.
	Resolver Resolver
	Metrics  *Metrics
	Log      *slog.Logger
}

// Service sends notifications and keeps integrations.
type Service struct {
	store      Store
	guard      *Guard
	smtp       config.SMTP
	jobs       tasks.Store
	statements func() []Statement
	retention  time.Duration
	metrics    *Metrics
	log        *slog.Logger
	http       *http.Client
	// tlsConfig is the base of every TLS connection's; tests add roots.
	tlsConfig *tls.Config
	// backoff and timeout are the waits between attempts and each
	// attempt's limit (tests shorten them).
	backoff []time.Duration
	timeout time.Duration

	// ctx ends with Close, and with it every delivery.
	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	buckets  map[string]*bucket
	outboxes map[string]chan delivery
	// pending counts the deliveries queued and not yet done.
	pending sync.WaitGroup
	// burst, every and wait are the rate limit (tests shorten them).
	burst int
	every time.Duration
	wait  time.Duration

	// events and logins are the built-in alerts waiting to go out:
	// failed-login notices have their own, small queue, so they cannot
	// crowd out the rest.
	events chan event
	logins chan event
	// loginMu guards loginSent, when failed-login notices went out.
	loginMu   sync.Mutex
	loginSent []time.Time
	// cache holds the integrations for built-in alerts; see integrations.
	cacheMu  sync.Mutex
	cache    []Integration
	cacheAt  time.Time
	cacheTTL time.Duration
	nowFunc  func() time.Time
}

// New returns a service that keeps integrations in opts.Store.
func New(opts Options) *Service {
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	if opts.Store == nil {
		opts.Store = NewMemoryStore()
	}
	s := &Service{
		store:      opts.Store,
		guard:      NewGuard(opts.AllowHosts, opts.Resolver),
		smtp:       opts.SMTP,
		jobs:       opts.Jobs,
		statements: opts.Statements,
		retention:  opts.Retention,
		metrics:    opts.Metrics,
		log:        opts.Log.With("component", "notifications"),
		tlsConfig:  &tls.Config{MinVersion: tls.VersionTLS12},
		backoff:    backoff,
		timeout:    attemptTimeout,
		buckets:    map[string]*bucket{},
		outboxes:   map[string]chan delivery{},
		burst:      sendBurst,
		every:      sendEvery,
		wait:       maxWait,
		events:     make(chan event, eventQueue),
		logins:     make(chan event, loginQueue),
		cacheTTL:   cacheTTL,
		nowFunc:    time.Now,
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.http = &http.Client{
		Transport: s.transport(),
		// Never follow a redirect: it could lead elsewhere with the
		// integration's headers. A redirect is a failure.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return s
}

func (s *Service) transport() *http.Transport {
	return &http.Transport{
		// No proxy: connections go to the address the guard vetted.
		Proxy:       nil,
		DialContext: s.guard.DialContext,
		// A new connection, so a new lookup and check, for every send.
		DisableKeepAlives:     true,
		TLSClientConfig:       s.tlsConfig,
		TLSHandshakeTimeout:   attemptTimeout,
		ResponseHeaderTimeout: attemptTimeout,
		ForceAttemptHTTP2:     true,
	}
}

// Store is where integrations, who may use them and the alert history are.
func (s *Service) Store() Store { return s.store }

// Close ends every delivery under way, as Igloo stops; those waiting fail.
func (s *Service) Close() { s.cancel() }

// Guard is what decides which hosts may be reached.
func (s *Service) Guard() *Guard { return s.guard }

// Check validates in's settings, and that its URL's host may be reached
// now, as CREATE and ALTER NOTIFICATION INTEGRATION do.
func (s *Service) Check(ctx context.Context, in Integration) error {
	invalid := func(format string, args ...any) error {
		return fmt.Errorf("%w: "+format, append([]any{ErrInvalid}, args...)...)
	}
	if !slices.Contains(Types, in.Type) {
		return invalid("TYPE must be webhook, slack, teams or email")
	}
	webhook := in.Type != TypeEmail
	switch {
	case webhook && in.URL == "":
		return invalid("a %s integration needs a URL", in.Type)
	case webhook && len(in.Recipients) > 0:
		return invalid("RECIPIENTS are for email integrations")
	case in.Type != TypeWebhook && (len(in.Headers) > 0 || in.Secret != ""):
		return invalid("HEADERS and SECRET are for webhook integrations; Slack and Teams take only a URL")
	case !webhook && in.URL != "":
		return invalid("an email integration takes RECIPIENTS, not a URL")
	case !webhook && len(in.Recipients) == 0:
		return invalid("an email integration needs RECIPIENTS ('name@example.com', ...)")
	}
	if len(in.Comment) > 1000 {
		return invalid("the comment is longer than 1000 characters")
	}
	if !webhook {
		return s.checkRecipients(in.Recipients)
	}
	if len(in.URL) > 2048 {
		return invalid("the URL is longer than 2048 characters")
	}
	if in.Secret != "" && len(in.Secret) < 16 {
		return invalid("SECRET must be at least 16 characters")
	}
	if len(in.Headers) > 20 {
		return invalid("at most 20 HEADERS")
	}
	for k, v := range in.Headers {
		if !validHeaderName(k) {
			return invalid("%q is not a header name", k)
		}
		switch textproto.CanonicalMIMEHeaderKey(k) {
		case "Host", "Content-Length", "Content-Type", "Transfer-Encoding", "Connection", "Upgrade", "Te", "Trailer",
			"Proxy-Authorization", "X-Igloo-Signature", "X-Igloo-Timestamp":
			return invalid("the header %s is Igloo's to set", textproto.CanonicalMIMEHeaderKey(k))
		}
		if len(v) > 4096 || strings.ContainsAny(v, "\r\n\x00") {
			return invalid("the value of the header %s is too long or has line breaks", k)
		}
	}
	u, err := s.guard.CheckURL(in.URL)
	if err != nil {
		return err
	}
	_, err = s.guard.Resolve(ctx, u.Hostname())
	return err
}

func validHeaderName(k string) bool {
	if k == "" || len(k) > 100 {
		return false
	}
	for _, r := range k {
		if r > 0x7e || r <= ' ' || strings.ContainsRune(`"(),/:;<=>?@[\]{}`, r) {
			return false
		}
	}
	return true
}

// checkRecipients allows bare addresses in IGLOO_SMTP_ALLOWED_DOMAINS.
func (s *Service) checkRecipients(rs []string) error {
	if !s.smtp.Enabled() {
		return fmt.Errorf("%w: email integrations need a mail server: set IGLOO_SMTP_HOST", ErrInvalid)
	}
	if len(rs) > 50 {
		return fmt.Errorf("%w: at most 50 RECIPIENTS", ErrInvalid)
	}
	for _, r := range rs {
		a, err := mail.ParseAddress(r)
		if err != nil || a.Name != "" || a.Address != r || strings.ContainsAny(r, "\r\n") {
			return fmt.Errorf("%w: %q is not an email address", ErrInvalid, r)
		}
		_, domain, _ := strings.Cut(strings.ToLower(r), "@")
		if !slices.Contains(s.smtp.AllowedDomains, domain) {
			return fmt.Errorf("%w: %s is not in a domain IGLOO_SMTP_ALLOWED_DOMAINS allows (%s)", ErrInvalid, r,
				strings.Join(s.smtp.AllowedDomains, ", "))
		}
	}
	return nil
}

// SendError is a failed send. Its message never carries the
// integration's secrets.
type SendError struct {
	Integration string
	Msg         string
	// Temporary says another attempt may succeed.
	Temporary bool
	// RetryAfter is how long the destination asked Igloo to wait.
	RetryAfter time.Duration
}

func (e *SendError) Error() string { return e.Msg }

// delivery is a notification in an outbox.
type delivery struct {
	in  Integration
	msg Message
	low bool
	// done is told how it went.
	done func(attempts int, err error)
}

// Deliver queues m, within the caps, in in's outbox, and calls done from the
// outbox's goroutine once it is sent or has failed; at once, if it cannot
// be queued. low marks a low-priority notification. Errors never carry in's
// secrets.
func (s *Service) Deliver(in Integration, m Message, low bool, done func(attempts int, err error)) {
	fail := func(err error) {
		s.metrics.sent(in.Type, err)
		done(0, err)
	}
	if !in.Enabled {
		fail(fmt.Errorf("the notification integration %s is disabled", in.Name))
		return
	}
	if s.ctx.Err() != nil {
		fail(errors.New("Igloo is shutting down"))
		return
	}
	if m.Time.IsZero() {
		m.Time = s.nowFunc()
	}
	d := delivery{in: in, msg: capped(m), low: low, done: done}
	s.mu.Lock()
	q, ok := s.outboxes[in.Name]
	if !ok {
		q = make(chan delivery, queueSize)
		s.outboxes[in.Name] = q
		go s.drain(in.Name, q)
	}
	free := cap(q) - len(q)
	if free == 0 || low && free <= lowReserve {
		waiting := len(q)
		s.mu.Unlock()
		fail(fmt.Errorf("%w: %d notifications are waiting to go to %s", ErrRateLimited, waiting, in.Name))
		return
	}
	s.pending.Add(1)
	q <- d
	s.mu.Unlock()
}

// Send delivers m to in and waits for it to be sent, or to fail, or for ctx
// to end. It returns how many attempts were made.
func (s *Service) Send(ctx context.Context, in Integration, m Message) (attempts int, err error) {
	type result struct {
		attempts int
		err      error
	}
	res := make(chan result, 1)
	s.Deliver(in, m, false, func(attempts int, err error) { res <- result{attempts, err} })
	select {
	case r := <-res:
		return r.attempts, r.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// drain sends what comes into name's outbox, until it has been empty for
// idleTimeout or the service closes.
func (s *Service) drain(name string, q chan delivery) {
	idle := time.NewTimer(idleTimeout)
	defer idle.Stop()
	for {
		select {
		case d := <-q:
			attempts, err := s.send(s.ctx, d)
			d.done(attempts, err)
			s.pending.Done()
			idle.Reset(idleTimeout)
		case <-idle.C:
			s.mu.Lock()
			if len(q) == 0 {
				delete(s.outboxes, name)
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
			idle.Reset(idleTimeout)
		}
	}
}

// send sends d, waiting its turn under the rate limit and trying again
// after failures that may pass.
func (s *Service) send(ctx context.Context, d delivery) (attempts int, err error) {
	in, m := d.in, d.msg
	defer func() { s.metrics.sent(in.Type, err) }()
	if err := s.take(ctx, in.Name, d.low); err != nil {
		return 0, err
	}
	for attempts = 1; ; attempts++ {
		actx, cancel := context.WithTimeout(ctx, s.timeout)
		err = s.attempt(actx, in, m)
		cancel()
		var se *SendError
		if err == nil || attempts == maxAttempts || !errors.As(err, &se) || !se.Temporary || ctx.Err() != nil {
			break
		}
		wait := s.backoff[min(attempts-1, len(s.backoff)-1)]
		wait += rand.N(wait/4 + 1)
		wait = max(wait, se.RetryAfter)
		select {
		case <-ctx.Done():
			return attempts, fmt.Errorf("%w (canceled before attempt %d)", err, attempts+1)
		case <-time.After(wait):
		}
	}
	if err != nil {
		s.log.Info("notification failed", "integration", in.Name, "type", in.Type, "target", in.Target(),
			"attempts", attempts, "err", err)
		return attempts, err
	}
	s.log.Debug("notification sent", "integration", in.Name, "type", in.Type, "attempts", attempts)
	return attempts, nil
}

// attempt sends m once.
func (s *Service) attempt(ctx context.Context, in Integration, m Message) error {
	var (
		body []byte
		err  error
	)
	switch in.Type {
	case TypeEmail:
		return s.sendEmail(ctx, in, m)
	case TypeSlack:
		body, err = slackPayload(m)
	case TypeTeams:
		body, err = teamsPayload(m)
	default:
		body, err = webhookPayload(in, m)
	}
	if err != nil {
		return err
	}
	return s.post(ctx, in, body)
}

// post sends body to in's URL.
func (s *Service) post(ctx context.Context, in Integration, body []byte) error {
	fail := func(temporary bool, format string, args ...any) *SendError {
		return &SendError{Integration: in.Name, Msg: in.Scrub(fmt.Sprintf(format, args...)), Temporary: temporary}
	}
	u, err := s.guard.CheckURL(in.URL)
	if err != nil {
		return fail(false, "%v", err)
	}
	where := u.Scheme + "://" + u.Host
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, in.URL, bytes.NewReader(body))
	if err != nil {
		return fail(false, "the URL cannot be used")
	}
	for k, v := range in.Headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Igloo")
	if in.Secret != "" {
		ts := strconv.FormatInt(s.nowFunc().Unix(), 10)
		req.Header.Set("X-Igloo-Timestamp", ts)
		req.Header.Set("X-Igloo-Signature", "v1="+Sign(in.Secret, ts, body))
	}
	resp, err := s.http.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		var ae *AddressError
		if errors.As(err, &ae) {
			return fail(false, "%v", ae)
		}
		return fail(true, "POST %s: %v", where, err)
	}
	defer resp.Body.Close() //nolint:errcheck
	answer, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponse))
	switch c := resp.StatusCode; {
	case c >= 200 && c < 300:
		return nil
	case c >= 300 && c < 400:
		return fail(false, "%s answered %s, a redirect, which Igloo does not follow; use the URL it redirects to", where, resp.Status)
	default:
		e := fail(c == http.StatusRequestTimeout || c == http.StatusTooManyRequests || c >= 500,
			"%s answered %s%s", where, resp.Status, quoteAnswer(answer))
		if secs, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && secs > 0 {
			e.RetryAfter = min(time.Duration(secs)*time.Second, maxRetryAfter)
		}
		return e
	}
}

// quoteAnswer quotes a webhook's answer if it is an error code.
func quoteAnswer(b []byte) string {
	if s := strings.TrimSpace(string(b)); errorCode.MatchString(s) {
		return ": " + strconv.Quote(s)
	}
	return ""
}

// Sign is the signature of a webhook request: the hex HMAC-SHA256, keyed
// with the integration's SECRET, of the X-Igloo-Timestamp header, a dot,
// and the body. Receivers compute it, compare it in constant time with
// X-Igloo-Signature (after "v1="), and refuse old timestamps.
func Sign(secret, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp + "."))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// bucket is an integration's rate limit: tokens may go negative, each
// missing token a send waiting its turn.
type bucket struct {
	tokens float64
	at     time.Time
}

// take waits for name's turn to send. A low-priority send does not wait,
// and leaves reserveTokens for the rest.
func (s *Service) take(ctx context.Context, name string, low bool) error {
	s.mu.Lock()
	now := s.nowFunc()
	b, ok := s.buckets[name]
	if !ok {
		b = &bucket{tokens: float64(s.burst), at: now}
		s.buckets[name] = b
	}
	b.tokens = min(float64(s.burst), b.tokens+float64(now.Sub(b.at))/float64(s.every))
	b.at = now
	if low && b.tokens-1 < float64(min(reserveTokens, s.burst-1)) {
		s.mu.Unlock()
		return fmt.Errorf("%w: %s keeps its last notifications for alerts other than failed logins", ErrRateLimited, name)
	}
	b.tokens--
	wait := time.Duration(0)
	if b.tokens < 0 {
		wait = time.Duration(-b.tokens * float64(s.every))
	}
	if wait > s.wait {
		b.tokens++
		s.mu.Unlock()
		return fmt.Errorf("%w: %s may send %d notifications at once and then one every %s on each replica, "+
			"and more are waiting", ErrRateLimited, name, s.burst, s.every)
	}
	s.mu.Unlock()
	if wait == 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		s.mu.Lock()
		b.tokens++
		s.mu.Unlock()
		return ctx.Err()
	case <-time.After(wait):
		return nil
	}
}
