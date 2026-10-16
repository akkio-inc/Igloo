// Package openlineage sends the lineage of the statements that write
// tables to an OpenLineage endpoint, such as Marquez's or DataHub's, as
// RunEvents: one for each statement that completes, naming the tables and
// locations it read as inputs and the tables it wrote as outputs.
//
// Events say what moved where, never any data: no query results, no
// values, and the statement's text only when configured. They are sent in
// the background; when the endpoint is slow or down, events are dropped
// rather than slowing anything down. The endpoint is chosen by whoever
// configures Igloo, and requests go to nowhere else: not to private or
// loopback addresses unless allowed, never to link-local ones such as cloud
// metadata services, and redirects are not followed.
package openlineage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/akkio-inc/igloo/internal/audit"
)

const (
	// producer identifies Igloo in events.
	producer = "https://github.com/akkio-inc/Igloo"
	// schemaURL is the version of the OpenLineage spec events follow.
	schemaURL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"

	queueSize   = 10_000
	sendTimeout = 10 * time.Second
	// attempts is how often an event is sent before it is dropped.
	attempts   = 3
	retryDelay = time.Second
)

// Options configure a Client.
type Options struct {
	// URL is the endpoint events are POSTed to, such as
	// https://marquez.example.com/api/v1/lineage.
	URL string
	// APIKey, if set, is sent as a bearer token.
	APIKey string
	// Namespace names Igloo's jobs and tables in events.
	Namespace string
	// AllowPrivate allows private and loopback addresses, and http.
	AllowPrivate bool
	// SendSQL includes statements' text in events.
	SendSQL bool
	// Registerer, if set, gets the events' metrics.
	Registerer prometheus.Registerer
	Log        *slog.Logger
}

// Client sends events.
type Client struct {
	opts   Options
	http   *http.Client
	queue  chan audit.Query
	log    *slog.Logger
	events *prometheus.CounterVec

	dropMu   sync.Mutex
	dropped  int
	lastWarn time.Time
}

// New checks opts and makes a client; Run sends its events.
func New(opts Options) (*Client, error) {
	u, err := url.Parse(opts.URL)
	switch {
	case err != nil:
		return nil, fmt.Errorf("IGLOO_OPENLINEAGE_URL: %w", err)
	case u.Scheme != "https" && !(u.Scheme == "http" && opts.AllowPrivate):
		return nil, errors.New("IGLOO_OPENLINEAGE_URL must be https (http only with IGLOO_OPENLINEAGE_ALLOW_PRIVATE)")
	case u.Host == "" || u.User != nil:
		return nil, errors.New("IGLOO_OPENLINEAGE_URL must name a host, and no user or password")
	}
	if ip, err := netip.ParseAddr(strings.Trim(u.Hostname(), "[]")); err == nil {
		if err := allowed(ip, opts.AllowPrivate); err != nil {
			return nil, fmt.Errorf("IGLOO_OPENLINEAGE_URL: %w", err)
		}
	}
	if opts.Namespace == "" {
		opts.Namespace = "igloo"
	}
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	c := &Client{
		opts:  opts,
		queue: make(chan audit.Query, queueSize),
		log:   opts.Log.With("component", "openlineage"),
		events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "igloo_openlineage_events_total",
			Help: "OpenLineage events, by result: sent, failed (the endpoint refused them or could not be reached) or dropped (too many waited).",
		}, []string{"result"}),
	}
	if opts.Registerer != nil {
		opts.Registerer.MustRegister(c.events)
	}
	g := &guard{private: opts.AllowPrivate, lookup: net.DefaultResolver.LookupNetIP}
	g.dialer = &net.Dialer{Timeout: sendTimeout, Control: func(_, address string, _ syscall.RawConn) error {
		ap, err := netip.ParseAddrPort(address)
		if err != nil {
			return err
		}
		return allowed(ap.Addr(), opts.AllowPrivate)
	}}
	c.http = &http.Client{
		Timeout: sendTimeout,
		Transport: &http.Transport{
			// Straight to the endpoint, never through a proxy from the
			// environment: the address checks would check the proxy's.
			Proxy:               nil,
			DialContext:         g.dial,
			TLSHandshakeTimeout: sendTimeout,
			MaxIdleConns:        2,
			IdleConnTimeout:     time.Minute,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirects are not followed") },
	}
	return c, nil
}

// guard dials only addresses events may go to.
type guard struct {
	private bool
	lookup  func(ctx context.Context, network, host string) ([]netip.Addr, error)
	dialer  *net.Dialer
}

// dial resolves the endpoint's name once, refuses it if any of its
// addresses is one events may not go to, and connects to those addresses
// themselves, so that a name cannot point somewhere else between the check
// and the connection (DNS rebinding). The dialer checks the address it
// connects to again.
func (g *guard) dial(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	ips, err := g.lookup(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("%s has no address", host)
	}
	for _, ip := range ips {
		if err := allowed(ip, g.private); err != nil {
			return nil, fmt.Errorf("%s: %w", host, err)
		}
	}
	var errs []error
	for _, ip := range ips {
		conn, err := g.dialer.DialContext(ctx, network, net.JoinHostPort(ip.Unmap().String(), port))
		if err == nil {
			return conn, nil
		}
		errs = append(errs, err)
	}
	return nil, errors.Join(errs...)
}

func prefixes(ps ...string) []netip.Prefix {
	out := make([]netip.Prefix, len(ps))
	for i, p := range ps {
		out[i] = netip.MustParsePrefix(p)
	}
	return out
}

var (
	// never are addresses events never go to, besides unspecified,
	// link-local and multicast ones: "this network" and the reserved
	// 240.0.0.0/4, cloud metadata services outside link-local space
	// (Alibaba's, AWS's over IPv6), and IPv6 addresses that carry an IPv4
	// one that cannot be read off them: IPv4-compatible, Teredo and
	// local-use NAT64.
	never = prefixes("0.0.0.0/8", "240.0.0.0/4", "100.100.100.200/32", "fd00:ec2::254/128",
		"::/96", "2001::/32", "64:ff9b:1::/48")
	// privateRanges are addresses events go to only when private ones are
	// allowed, besides loopback and the private networks (10.0.0.0/8,
	// 172.16.0.0/12, 192.168.0.0/16, fc00::/7): carrier-grade NAT,
	// benchmarking networks and deprecated site-local IPv6.
	privateRanges = prefixes("100.64.0.0/10", "198.18.0.0/15", "fec0::/10")
	// embedded are IPv6 addresses that carry an IPv4 one, which is
	// checked instead, with the byte it starts at: IPv4-translated,
	// NAT64 and 6to4. IPv4-mapped ones are unmapped first.
	embedded = []struct {
		prefix netip.Prefix
		at     int
	}{
		{netip.MustParsePrefix("::ffff:0:0:0/96"), 12},
		{netip.MustParsePrefix("64:ff9b::/96"), 12},
		{netip.MustParsePrefix("2002::/16"), 2},
	}
)

func within(ps []netip.Prefix, ip netip.Addr) bool {
	for _, p := range ps {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// allowed refuses addresses events must not go to: unspecified,
// link-local, multicast and the never addresses always, and loopback and
// private ones unless private says they may.
func allowed(ip netip.Addr, private bool) error {
	ip = ip.Unmap().WithZone("")
	for _, e := range embedded {
		if e.prefix.Contains(ip) {
			b := ip.As16()
			v4 := netip.AddrFrom4([4]byte(b[e.at : e.at+4]))
			if err := allowed(v4, private); err != nil {
				return fmt.Errorf("%s carries %s: %w", ip, v4, err)
			}
			return nil
		}
	}
	switch {
	case ip.IsUnspecified(), ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast(), ip.IsInterfaceLocalMulticast(),
		ip.IsMulticast(), !ip.IsLoopback() && within(never, ip):
		return fmt.Errorf("lineage events may not go to %s", ip)
	case !private && (ip.IsLoopback() || ip.IsPrivate() || within(privateRanges, ip)):
		return fmt.Errorf("lineage events go to %s, a private address, only with IGLOO_OPENLINEAGE_ALLOW_PRIVATE=true", ip)
	}
	return nil
}

// Statement queues the event of a statement that wrote tables. It never
// blocks: when too many wait, it is dropped.
func (c *Client) Statement(q audit.Query) {
	select {
	case c.queue <- q:
	default:
		c.events.WithLabelValues("dropped").Inc()
		c.warn("dropped lineage events: too many waited to be sent")
	}
}

// Run sends queued events until ctx ends.
func (c *Client) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case q := <-c.queue:
			c.send(ctx, q)
		}
	}
}

// send sends q's event, trying again a few times.
func (c *Client) send(ctx context.Context, q audit.Query) {
	body, err := json.Marshal(c.event(q))
	if err != nil {
		c.events.WithLabelValues("failed").Inc()
		return
	}
	delay := retryDelay
	for attempt := 1; ; attempt++ {
		err := c.post(ctx, body)
		if err == nil {
			c.events.WithLabelValues("sent").Inc()
			return
		}
		var refused *refusedError
		if attempt == attempts || errors.As(err, &refused) || ctx.Err() != nil {
			c.events.WithLabelValues("failed").Inc()
			c.warn("could not send a lineage event", "err", err)
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(delay):
		}
		delay *= 2
	}
}

// refusedError is a response that sending again would not change.
type refusedError struct{ status int }

func (e *refusedError) Error() string { return fmt.Sprintf("the endpoint answered %d", e.status) }

func (c *Client) post(ctx context.Context, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.opts.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.opts.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.opts.APIKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	switch {
	case resp.StatusCode/100 == 2:
		return nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode/100 == 5:
		return fmt.Errorf("the endpoint answered %d", resp.StatusCode)
	}
	return &refusedError{status: resp.StatusCode}
}

// warn logs at most once a minute.
func (c *Client) warn(msg string, args ...any) {
	c.dropMu.Lock()
	defer c.dropMu.Unlock()
	c.dropped++
	if time.Since(c.lastWarn) < time.Minute {
		return
	}
	c.log.Warn(msg, append(args, "count", c.dropped)...)
	c.dropped, c.lastWarn = 0, time.Now()
}

// event, facet and dataset are the parts of a RunEvent that Igloo fills.
type (
	event struct {
		EventType string    `json:"eventType"`
		EventTime string    `json:"eventTime"`
		Producer  string    `json:"producer"`
		SchemaURL string    `json:"schemaURL"`
		Run       run       `json:"run"`
		Job       job       `json:"job"`
		Inputs    []dataset `json:"inputs"`
		Outputs   []dataset `json:"outputs"`
	}
	run struct {
		RunID string `json:"runId"`
	}
	job struct {
		Namespace string           `json:"namespace"`
		Name      string           `json:"name"`
		Facets    map[string]facet `json:"facets"`
	}
	facet   map[string]any
	dataset struct {
		Namespace string `json:"namespace"`
		Name      string `json:"name"`
	}
)

// event describes q, a statement that completed.
func (c *Client) event(q audit.Query) event {
	e := event{
		EventType: "COMPLETE",
		EventTime: q.Start.Add(q.Duration).UTC().Format(time.RFC3339Nano),
		Producer:  producer,
		SchemaURL: schemaURL,
		Run:       run{RunID: q.ID},
		Job:       job{Namespace: c.opts.Namespace, Facets: map[string]facet{}},
		Inputs:    []dataset{},
		Outputs:   []dataset{},
	}
	jobType := "QUERY"
	switch kind, name, _ := strings.Cut(q.Job, " "); {
	case q.Job == "":
		e.Job.Name = "sql." + firstOr(q.TablesWritten, "statement")
	case kind == "materialized":
		jobType = "MATERIALIZED_VIEW"
		e.Job.Name = "materialized_view." + strings.TrimPrefix(name, "view ")
	default:
		jobType = strings.ToUpper(kind)
		e.Job.Name = kind + "." + name
	}
	e.Job.Facets["jobType"] = facet{
		"_producer": producer, "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
		"processingType": "BATCH", "integration": "IGLOO", "jobType": jobType,
	}
	if c.opts.SendSQL && q.Text != "" {
		e.Job.Facets["sql"] = facet{
			"_producer": producer, "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/SQLJobFacet.json#/$defs/SQLJobFacet",
			"query": q.Text,
		}
	}
	for _, t := range q.TablesRead {
		e.Inputs = append(e.Inputs, dataset{Namespace: c.opts.Namespace, Name: t})
	}
	for _, l := range q.LocationsRead {
		e.Inputs = append(e.Inputs, location(l))
	}
	for _, t := range q.TablesWritten {
		e.Outputs = append(e.Outputs, dataset{Namespace: c.opts.Namespace, Name: t})
	}
	return e
}

// location names files read from a location as OpenLineage does: the
// bucket's URL as the namespace, the path in it as the name.
func location(l string) dataset {
	if u, err := url.Parse(l); err == nil && u.Scheme != "" && u.Host != "" {
		return dataset{Namespace: u.Scheme + "://" + u.Host, Name: strings.TrimPrefix(u.Path, "/")}
	}
	return dataset{Namespace: "file", Name: l}
}

func firstOr(s []string, fallback string) string {
	if len(s) > 0 {
		return s[0]
	}
	return fallback
}
