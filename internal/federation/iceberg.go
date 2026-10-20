package federation

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/aws"
	"golang.org/x/sync/errgroup"
)

// Connections of TYPE iceberg reach another Iceberg REST catalog: Snowflake's
// Horizon Catalog, a Databricks share served to Iceberg clients, Unity
// Catalog, Polaris, AWS Glue and S3 Tables, another Lakekeeper. DuckDB's
// iceberg extension attaches it read-only, through an icebergProxy on the
// loopback interface, under a random path, as it reaches Igloo's own
// catalog. The proxy:
//
//   - holds the catalog's credentials (a bearer token, an OAuth2 client
//     whose tokens it fetches and renews, or an AWS access key it signs
//     requests with), so DuckDB attaches with none and never sees them;
//   - forwards only reads, and only of the tables it found when the replica
//     started; everything else is refused or answered by the proxy itself,
//     so nothing Igloo's SQL does can change the remote catalog;
//   - bounds every request to the catalog by the connection's timeouts;
//   - checks where each table's files are. DuckDB's sandbox, locked at
//     startup, lets SQL read files only in the warehouse, the allowed
//     locations and, for these connections, the locations of the tables the
//     proxy found then (SandboxDirectories). A location must be an object
//     store's, must not overlap the warehouse or the allowed locations, and
//     must not change while the replica runs; a table whose location does
//     not qualify is left out, and one whose location changes is refused
//     until the replica restarts;
//   - passes on the storage credentials the catalog hands out
//     (X-Iceberg-Access-Delegation: vended-credentials) for DuckDB to read
//     the table's files with, keeping only the settings DuckDB needs and
//     never the catalog's choice of storage endpoint or of plain HTTP, which
//     could send the credentials, and the reads, elsewhere.
//
// DuckDB keeps each set of credentials it is handed as a temporary secret,
// scoped to the table's location, and never drops it; Manager.sweep does.

const (
	// discoveryTimeout bounds finding a catalog's tables at startup.
	discoveryTimeout = 2 * time.Minute
	// discoveryParallel is how many tables are loaded at once.
	discoveryParallel = 8
	// maxTables bounds the tables one connection attaches.
	maxTables = 5000
	// maxCatalogResponse bounds an answer of the catalog. Table metadata
	// with a long history can run to tens of megabytes.
	maxCatalogResponse = 256 << 20
	// warmFor is how long the answer about a table found at startup is
	// kept for DuckDB's first load of it.
	warmFor = 2 * time.Minute
	// tokenMargin is how long before it expires an OAuth2 token is
	// renewed.
	tokenMargin = time.Minute
)

// remoteTable is a table of the remote catalog that the replica attached.
type remoteTable struct {
	namespace []string
	name      string
	// roots are the locations its files may be in, each ending in a slash.
	roots []string
	// warm is the catalog's answer about it found at startup, for DuckDB's
	// first load of the table, until warmUntil.
	warm      []byte
	warmUntil time.Time
}

// icebergProxy stands between DuckDB and one remote Iceberg REST catalog.
type icebergProxy struct {
	name     string
	secrets  string
	locs     locationRules
	log      *slog.Logger
	ln       net.Listener
	srv      *http.Server
	path     string
	failures atomic.Int32
	lastErr  atomic.Pointer[string]

	mu       sync.Mutex
	attached Connection
	current  Connection
	client   *http.Client
	token    oauthToken
	// aws keeps AWS's default credentials, for signing_name.
	aws *aws.CredentialsCache
	// prefix is the remote catalog's path prefix for the warehouse, from
	// its /v1/config.
	prefix     string
	namespaces [][]string
	tables     map[string]*remoteTable
	// skipped are the tables left out, each with why.
	skipped []string
}

type oauthToken struct {
	value   string
	expires time.Time
}

// newIcebergProxy makes a proxy for c. locs decides where the catalog's
// tables may keep their files; secrets is IGLOO_CONNECTION_SECRETS_DIR.
func newIcebergProxy(c Connection, secrets string, locs locationRules, log *slog.Logger) (*icebergProxy, error) {
	client, err := catalogClient(c)
	if err != nil {
		return nil, err
	}
	return &icebergProxy{
		name: c.Name, secrets: secrets, locs: locs, log: log.With("connection", c.Name),
		attached: c, current: c, client: client, tables: map[string]*remoteTable{},
	}, nil
}

// listen starts serving DuckDB on a random loopback port, under a random
// path.
func (p *icebergProxy) listen() error {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("connection %s: listen for DuckDB: %w", p.name, err)
	}
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	p.ln, p.path = ln, "/"+hex.EncodeToString(b)
	p.srv = &http.Server{Handler: p, ReadHeaderTimeout: 10 * time.Second,
		ErrorLog: slog.NewLogLogger(p.log.Handler(), slog.LevelDebug)}
	go func() {
		if err := p.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			p.log.Warn("catalog proxy stopped", "err", err)
		}
	}()
	proxyURLs.Store(p.url(), p.attached.Iceberg.Endpoint)
	return nil
}

// url is the proxy's URL, for DuckDB's ATTACH.
func (p *icebergProxy) url() string { return "http://" + p.ln.Addr().String() + p.path }

// proxyURLs maps the URLs of this process's catalog proxies, which DuckDB
// quotes in errors, to their catalogs' endpoints (see Scrub).
var proxyURLs sync.Map

func (p *icebergProxy) settings() Connection {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.current
}

func (p *icebergProxy) attachedConn() Connection { return p.attached }

// port is the loopback port DuckDB reaches the proxy at.
func (p *icebergProxy) port() int { return p.ln.Addr().(*net.TCPAddr).Port }

// probe finds the catalog's tables (see discover), within
// discoveryTimeout.
func (p *icebergProxy) probe() error { return p.discover(context.Background()) }

// update applies new settings: credentials, TLS and timeouts.
func (p *icebergProxy) update(c Connection) {
	client, err := catalogClient(c)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current = c
	p.token = oauthToken{}
	p.aws = nil
	if err == nil {
		p.client = client
	}
	// Answers fetched with the old settings are not served.
	for _, t := range p.tables {
		t.warm = nil
	}
	p.failures.Store(0)
}

func (p *icebergProxy) close() {
	if p.srv != nil {
		_ = p.srv.Close()
		proxyURLs.Delete(p.url())
	}
}

func (p *icebergProxy) lastError() string {
	if e := p.lastErr.Load(); e != nil {
		return *e
	}
	return ""
}

// record notes whether a request to the catalog worked, logging the first
// failure in a row and the recovery after it.
func (p *icebergProxy) record(err error) {
	if err == nil {
		p.lastErr.Store(nil)
		if p.failures.Swap(0) > 0 {
			p.log.Info("remote catalog reachable again")
		}
		return
	}
	msg := Scrub(err.Error())
	p.lastErr.Store(&msg)
	if p.failures.Add(1) == 1 {
		p.log.Warn("remote catalog request failed", "err", msg)
	}
}

// catalogClient returns an HTTP client for c's catalog: its TLS settings,
// its connect timeout for dialing and the TLS handshake, and no redirects,
// which would carry its credentials elsewhere.
func catalogClient(c Connection) (*http.Client, error) {
	tlsCfg, err := tlsConfig(&c)
	if err != nil {
		return nil, err
	}
	if tlsCfg != nil {
		// Each request's own host: the token endpoint can be another.
		tlsCfg.ServerName = ""
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: c.ConnectTimeout, KeepAlive: keepAlive}).DialContext,
		TLSHandshakeTimeout:   c.ConnectTimeout,
		ResponseHeaderTimeout: c.QueryTimeout,
		TLSClientConfig:       tlsCfg,
		ForceAttemptHTTP2:     true,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
	}
	return &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}, nil
}

// --- credentials ------------------------------------------------------------

// secretValue returns a credential: the stored value, or the contents of
// the file of IGLOO_CONNECTION_SECRETS_DIR named, read now.
func (p *icebergProxy) secretValue(c Connection, stored, file, what string) (string, error) {
	if c.Unusable != "" {
		return "", errors.New(c.Unusable)
	}
	if file == "" {
		if stored != "" {
			tokens.Store(stored, true)
		}
		return stored, nil
	}
	if p.secrets == "" {
		return "", fmt.Errorf("%s_secret needs IGLOO_CONNECTION_SECRETS_DIR", what)
	}
	if !validSecretName(file) {
		return "", fmt.Errorf("invalid %s_secret %q", what, file)
	}
	b, err := os.ReadFile(filepath.Join(p.secrets, file))
	if err != nil {
		return "", fmt.Errorf("read %s_secret %s: %w", what, file, errors.Unwrap(err))
	}
	v := strings.TrimRight(string(b), "\r\n")
	tokens.Store(v, true)
	return v, nil
}

// authorization returns the Authorization header for the catalog, or ""
// for a catalog without credentials. fresh fetches a new OAuth2 token.
func (p *icebergProxy) authorization(ctx context.Context, c Connection, fresh bool) (string, error) {
	s := c.Iceberg
	switch {
	case s.Token != "" || s.TokenSecret != "":
		t, err := p.secretValue(c, s.Token, s.TokenSecret, "token")
		if err != nil || t == "" {
			return "", errors.Join(err, errNoValue("token"))
		}
		return "Bearer " + t, nil
	case s.Credential != "" || s.CredentialSecret != "":
		p.mu.Lock()
		tok := p.token
		p.mu.Unlock()
		if !fresh && tok.value != "" && time.Now().Add(tokenMargin).Before(tok.expires) {
			return "Bearer " + tok.value, nil
		}
		cred, err := p.secretValue(c, s.Credential, s.CredentialSecret, "credential")
		if err != nil || cred == "" {
			return "", errors.Join(err, errNoValue("credential"))
		}
		tok, err = p.fetchToken(ctx, c, cred)
		if err != nil {
			return "", err
		}
		p.mu.Lock()
		p.token = tok
		p.mu.Unlock()
		return "Bearer " + tok.value, nil
	}
	return "", nil
}

func errNoValue(what string) error {
	return fmt.Errorf("the connection's %s is empty", what)
}

// fetchToken exchanges an OAuth2 client's credential for an access token
// (the client credentials grant), at oauth2_server_uri or the catalog's
// own /v1/oauth/tokens. The credential is "client_id:client_secret", or a
// secret alone, which Snowflake's programmatic access tokens are.
func (p *icebergProxy) fetchToken(ctx context.Context, c Connection, credential string) (oauthToken, error) {
	s := c.Iceberg
	uri := s.OAuth2ServerURI
	if uri == "" {
		uri = s.Endpoint + "/v1/oauth/tokens"
	}
	scope := s.Scope
	if scope == "" {
		scope = "catalog"
	}
	form := url.Values{"grant_type": {"client_credentials"}, "scope": {scope}}
	if id, secret, ok := strings.Cut(credential, ":"); ok {
		form.Set("client_id", id)
		form.Set("client_secret", secret)
		tokens.Store(secret, true)
	} else {
		form.Set("client_secret", credential)
	}
	tokens.Store(credential, true)
	ctx, cancel := withTimeout(ctx, c.QueryTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uri, strings.NewReader(form.Encode()))
	if err != nil {
		return oauthToken{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := p.httpClient().Do(req)
	if err != nil {
		return oauthToken{}, fmt.Errorf("get a token from %s: %w", uri, unwrapURL(err))
	}
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return oauthToken{}, fmt.Errorf("get a token from %s: %w", uri, err)
	}
	var out struct {
		AccessToken string          `json:"access_token"`
		ExpiresIn   json.Number     `json:"expires_in"`
		Error       json.RawMessage `json:"error"`
		Description string          `json:"error_description"`
	}
	_ = json.Unmarshal(body, &out)
	if resp.StatusCode != http.StatusOK || out.AccessToken == "" {
		// OAuth2's error and error_description, or an Iceberg REST error.
		var code string
		msg := ""
		if json.Unmarshal(out.Error, &code) == nil {
			msg = code
			if out.Description != "" {
				msg += ": " + out.Description
			}
		} else {
			msg = catalogMessage(body)
		}
		if msg == "" {
			msg = catalogMessage(body)
		}
		return oauthToken{}, fmt.Errorf("the token endpoint %s refused the credential (%s): %s", uri, resp.Status, Scrub(msg))
	}
	life := time.Hour
	if n, err := out.ExpiresIn.Int64(); err == nil && n > 0 {
		life = time.Duration(n) * time.Second
	}
	return oauthToken{value: out.AccessToken, expires: time.Now().Add(life)}, nil
}

func (p *icebergProxy) httpClient() *http.Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.client
}

func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, d)
}

// unwrapURL drops the request description from an HTTP client's error.
func unwrapURL(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		if ue.Timeout() {
			return errors.New("the catalog did not answer in time")
		}
		return ue.Err
	}
	return err
}

// --- requests to the catalog ---------------------------------------------

// catalogResponse is an answer of the remote catalog.
type catalogResponse struct {
	status int
	body   []byte
}

// get sends a GET to the catalog at /v1/ + rel, with the connection's
// credentials, and returns the answer. delegate asks for storage
// credentials.
func (p *icebergProxy) get(ctx context.Context, c Connection, rel string, query url.Values, delegate bool) (catalogResponse, error) {
	res, err := p.getOnce(ctx, c, rel, query, delegate, false)
	oauth := c.Iceberg.SigningName == "" && (c.Iceberg.Credential != "" || c.Iceberg.CredentialSecret != "")
	if err == nil && res.status == http.StatusUnauthorized && oauth {
		// The token may have been revoked or expired early.
		res, err = p.getOnce(ctx, c, rel, query, delegate, true)
	}
	switch {
	case err != nil:
		p.record(err)
	case res.status >= 500 || res.status == http.StatusUnauthorized || res.status == http.StatusForbidden:
		p.record(fmt.Errorf("the catalog answered %d: %s", res.status, catalogMessage(res.body)))
	default:
		p.record(nil)
	}
	return res, err
}

func (p *icebergProxy) getOnce(ctx context.Context, c Connection, rel string, query url.Values, delegate, fresh bool) (catalogResponse, error) {
	auth := ""
	if c.Iceberg.SigningName == "" {
		var err error
		if auth, err = p.authorization(ctx, c, fresh); err != nil {
			return catalogResponse{}, err
		}
	}
	u := c.Iceberg.Endpoint + "/v1/" + rel
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	ctx, cancel := withTimeout(ctx, c.QueryTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return catalogResponse{}, err
	}
	req.Header.Set("Accept", "application/json")
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	if delegate && !c.Iceberg.staticStorage() {
		req.Header.Set("X-Iceberg-Access-Delegation", "vended-credentials")
	}
	if c.Iceberg.SigningName != "" {
		if err := p.sign(ctx, c, req); err != nil {
			return catalogResponse{}, err
		}
	}
	resp, err := p.httpClient().Do(req)
	if err != nil {
		return catalogResponse{}, unwrapURL(err)
	}
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxCatalogResponse+1))
	if err != nil {
		return catalogResponse{}, unwrapURL(err)
	}
	if len(body) > maxCatalogResponse {
		return catalogResponse{}, fmt.Errorf("the catalog's answer is larger than %d MB", maxCatalogResponse>>20)
	}
	return catalogResponse{status: resp.StatusCode, body: body}, nil
}

// catalogMessage is the message of an Iceberg REST error answer, scrubbed
// and short, or the start of the body.
func catalogMessage(body []byte) string {
	var e struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
		} `json:"error"`
	}
	msg := ""
	if json.Unmarshal(body, &e) == nil && e.Error.Message != "" {
		msg = e.Error.Message
	} else {
		msg = strings.TrimSpace(string(body))
	}
	if len(msg) > 300 {
		msg = msg[:300] + "..."
	}
	return Scrub(strings.Map(func(r rune) rune {
		if r < ' ' {
			return ' '
		}
		return r
	}, msg))
}

// rel is the catalog path below /v1/ of a warehouse resource, with the
// catalog's prefix.
func (p *icebergProxy) rel(parts ...string) string {
	p.mu.Lock()
	prefix := p.prefix
	p.mu.Unlock()
	if prefix == "" {
		return strings.Join(parts, "/")
	}
	return prefix + "/" + strings.Join(parts, "/")
}

func namespacePath(ns []string) string { return url.PathEscape(strings.Join(ns, "\x1f")) }

func tableKey(ns []string, name string) string { return strings.Join(ns, "\x1f") + "\x00" + name }

// --- startup ---------------------------------------------------------------

// check checks that the catalog answers with c's settings: its
// configuration and its namespaces. It is the check CREATE and ALTER
// CONNECTION make before storing settings.
func (p *icebergProxy) check(ctx context.Context) error {
	c := p.settings()
	if err := p.loadConfig(ctx, c); err != nil {
		return err
	}
	_, err := p.listNamespaces(ctx, c)
	return err
}

// loadConfig reads the catalog's configuration for the warehouse: the
// prefix of its paths.
func (p *icebergProxy) loadConfig(ctx context.Context, c Connection) error {
	q := url.Values{}
	if c.Iceberg.Warehouse != "" {
		q.Set("warehouse", c.Iceberg.Warehouse)
	}
	res, err := p.get(ctx, c, "config", q, false)
	if err != nil {
		return fmt.Errorf("read the catalog's configuration: %w", err)
	}
	if res.status != http.StatusOK {
		return fmt.Errorf("read the catalog's configuration: the catalog answered %d: %s", res.status, catalogMessage(res.body))
	}
	var cfg struct {
		Defaults  map[string]string `json:"defaults"`
		Overrides map[string]string `json:"overrides"`
	}
	if err := json.Unmarshal(res.body, &cfg); err != nil {
		return fmt.Errorf("read the catalog's configuration: %w", err)
	}
	prefix := cfg.Defaults["prefix"]
	if o, ok := cfg.Overrides["prefix"]; ok {
		prefix = o
	}
	prefix = strings.Trim(prefix, "/")
	if strings.ContainsAny(prefix, "?#\\") || slices.Contains(strings.Split(prefix, "/"), "..") {
		return fmt.Errorf("the catalog's prefix %q is not a path", prefix)
	}
	p.mu.Lock()
	p.prefix = prefix
	p.mu.Unlock()
	return nil
}

// pages fetches every page of a list.
func (p *icebergProxy) pages(ctx context.Context, c Connection, rel string, each func(body []byte) (string, error)) error {
	token := ""
	for range 1000 {
		q := url.Values{}
		if token != "" {
			q.Set("pageToken", token)
		}
		res, err := p.get(ctx, c, rel, q, false)
		if err != nil {
			return err
		}
		if res.status != http.StatusOK {
			return fmt.Errorf("the catalog answered %d: %s", res.status, catalogMessage(res.body))
		}
		if token, err = each(res.body); err != nil || token == "" {
			return err
		}
	}
	return errors.New("the catalog's list does not end")
}

// listNamespaces lists the top-level namespaces, those schema names if set.
func (p *icebergProxy) listNamespaces(ctx context.Context, c Connection) ([][]string, error) {
	var out [][]string
	err := p.pages(ctx, c, p.rel("namespaces"), func(body []byte) (string, error) {
		var page struct {
			Namespaces [][]string `json:"namespaces"`
			Next       *string    `json:"next-page-token"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return "", err
		}
		for _, ns := range page.Namespaces {
			if len(ns) != 1 || ns[0] == "" {
				// Nested namespaces are not attached.
				continue
			}
			if len(c.Schemas) > 0 && !slices.ContainsFunc(c.Schemas, func(s string) bool { return strings.EqualFold(s, ns[0]) }) {
				continue
			}
			out = append(out, ns)
		}
		if page.Next == nil {
			return "", nil
		}
		return *page.Next, nil
	})
	if err != nil {
		return nil, fmt.Errorf("list the catalog's namespaces: %w", err)
	}
	return out, nil
}

// discover finds the catalog's tables and where their files are, before
// DuckDB attaches it: the tables it attaches, and the locations the
// sandbox opens to. It gives up when parent is done, or after
// discoveryTimeout.
func (p *icebergProxy) discover(parent context.Context) (err error) {
	ctx, cancel := context.WithTimeout(parent, discoveryTimeout)
	defer cancel()
	defer func() {
		if err != nil && parent.Err() != nil {
			// Whichever request it cut short, the reason is startup's
			// deadline.
			err = errors.New("finding the catalog's tables did not finish in the time Igloo gives connections at startup")
		}
	}()
	c := p.settings()
	if err := p.loadConfig(ctx, c); err != nil {
		return err
	}
	namespaces, err := p.listNamespaces(ctx, c)
	if err != nil {
		return err
	}
	var found []*remoteTable
	for _, ns := range namespaces {
		err := p.pages(ctx, c, p.rel("namespaces", namespacePath(ns), "tables"), func(body []byte) (string, error) {
			var page struct {
				Identifiers []struct {
					Namespace []string `json:"namespace"`
					Name      string   `json:"name"`
				} `json:"identifiers"`
				Next *string `json:"next-page-token"`
			}
			if err := json.Unmarshal(body, &page); err != nil {
				return "", err
			}
			for _, id := range page.Identifiers {
				if id.Name != "" {
					found = append(found, &remoteTable{namespace: ns, name: id.Name})
				}
			}
			if len(found) > maxTables {
				return "", fmt.Errorf("the catalog has more than %d tables; name the namespaces to attach with schema", maxTables)
			}
			if page.Next == nil {
				return "", nil
			}
			return *page.Next, nil
		})
		if err != nil {
			return fmt.Errorf("list the tables of namespace %s: %w", strings.Join(ns, "."), err)
		}
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(discoveryParallel)
	why := make([]string, len(found))
	for i, t := range found {
		g.Go(func() error {
			res, err := p.get(gctx, c, p.rel("namespaces", namespacePath(t.namespace), "tables", url.PathEscape(t.name)), nil, true)
			switch {
			case err != nil:
				why[i] = err.Error()
			case res.status != http.StatusOK:
				why[i] = fmt.Sprintf("the catalog answered %d: %s", res.status, catalogMessage(res.body))
			default:
				body, roots, err := p.inspect(c, res.body, nil)
				if err != nil {
					why[i] = err.Error()
				} else {
					t.roots, t.warm, t.warmUntil = roots, body, time.Now().Add(warmFor)
				}
			}
			return nil
		})
	}
	_ = g.Wait()
	if ctx.Err() != nil {
		return fmt.Errorf("finding the catalog's tables took longer than %s; name the namespaces to attach with schema", discoveryTimeout)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.namespaces, p.tables, p.skipped = namespaces, map[string]*remoteTable{}, nil
	for i, t := range found {
		if why[i] != "" {
			p.skipped = append(p.skipped, strings.Join(t.namespace, ".")+"."+t.name+": "+Scrub(why[i]))
			continue
		}
		p.tables[tableKey(t.namespace, t.name)] = t
	}
	slices.Sort(p.skipped)
	return nil
}

// roots returns every location the attached tables' files may be in.
func (p *icebergProxy) roots() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for _, t := range p.tables {
		out = append(out, t.roots...)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// summary says what the replica attached of the catalog, for SHOW
// CONNECTIONS.
func (p *icebergProxy) summary() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := fmt.Sprintf("%d tables attached", len(p.tables))
	if len(p.tables) == 1 {
		s = "1 table attached"
	}
	if len(p.skipped) > 0 {
		list := p.skipped
		more := ""
		if len(list) > 5 {
			list, more = list[:5], fmt.Sprintf("; and %d more", len(p.skipped)-5)
		}
		s += fmt.Sprintf("; %d left out: %s%s", len(p.skipped), strings.Join(list, "; "), more)
	}
	return s
}

// --- what the catalog says about a table ------------------------------

// vendedKeys are the storage settings of a catalog's answer DuckDB is
// handed: credentials, their expiry and the region. The rest, the storage
// endpoint and path-style access among them, are dropped.
var vendedKeys = map[string]bool{
	"s3.access-key-id": true, "s3.secret-access-key": true, "s3.session-token": true,
	"s3.session-token-expires-at-ms": true, "s3.region": true, "region": true, "client.region": true,
	"gcs.oauth2.token": true, "gcs.oauth2.token-expires-at": true, "expires-at-ms": true,
}

// storageConfig keeps what DuckDB needs of storage settings a catalog
// hands out. The endpoint and the URL style are the connection's own
// (storage_endpoint): a catalog could otherwise send DuckDB's reads, signed
// with the credentials, to a server of its choosing, or over plain HTTP
// (DuckDB turns TLS off for path-style access).
func storageConfig(c Connection, in map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range in {
		lk := strings.ToLower(k)
		switch {
		case strings.ContainsFunc(v, unicode.IsControl):
			// It would go into a request's headers.
		case lk == "s3.region" || lk == "region" || lk == "client.region":
			// DuckDB makes the region part of the host it connects to.
			if validRegion(v) {
				out[k] = v
			}
		case vendedKeys[lk]:
			out[k] = v
		case strings.HasPrefix(lk, "adls.sas-token."):
			// DuckDB reaches the host the key names.
			if host := lk[len("adls.sas-token."):]; strings.HasSuffix(host, ".dfs.core.windows.net") || strings.HasSuffix(host, ".blob.core.windows.net") {
				out[k] = v
			}
		}
	}
	s := c.Iceberg
	if s.StorageEndpoint != "" {
		hostport, useSSL, _ := parseStorageEndpoint(s.StorageEndpoint)
		scheme := "https://"
		if !useSSL {
			scheme = "http://"
		}
		out["s3.endpoint"] = scheme + hostport
		out["s3.path-style-access"] = strconv.FormatBool(s.StorageURLStyle != "vhost")
	}
	if s.Region != "" && out["s3.region"] == "" && out["region"] == "" && out["client.region"] == "" {
		out["client.region"] = s.Region
	}
	return out
}

// validRegion reports whether s can be a storage region: letters, digits
// and dashes, as AWS's, Google's and Cloudflare's are.
func validRegion(s string) bool {
	if s == "" || len(s) > 40 {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
			return false
		}
	}
	return true
}

// inspect checks the catalog's answer about a table (a LoadTableResult) and
// returns it as DuckDB gets it, and the locations its files may be in. known
// is the table as found at startup, whose locations it must keep; nil while
// finding it.
func (p *icebergProxy) inspect(c Connection, body []byte, known *remoteTable) ([]byte, []string, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		return nil, nil, fmt.Errorf("the catalog's answer about the table is not JSON: %w", err)
	}
	var meta struct {
		Location   string            `json:"location"`
		Properties map[string]string `json:"properties"`
	}
	if err := json.Unmarshal(top["metadata"], &meta); err != nil || meta.Location == "" {
		return nil, nil, errors.New("the catalog's answer has no table location")
	}
	roots, err := p.locs.tableRoots(meta.Location, meta.Properties)
	if err != nil {
		return nil, nil, err
	}
	if known != nil {
		for _, r := range roots {
			if !slices.Contains(known.roots, r) {
				return nil, nil, fmt.Errorf("its files are now in %s, not where they were when this replica started; restart it to read the table", r)
			}
		}
	}
	if raw, ok := top["metadata-location"]; ok && string(raw) != "null" {
		var loc string
		if err := json.Unmarshal(raw, &loc); err != nil {
			return nil, nil, errors.New("the catalog's answer has a bad metadata-location")
		}
		if loc != "" && !under(loc, roots) {
			return nil, nil, fmt.Errorf("its metadata file %s is outside its location %s", loc, roots[0])
		}
	}

	// Storage credentials: kept only when the connection has none of its
	// own, and then only what DuckDB needs.
	static := c.Iceberg.staticStorage()
	cfg := map[string]string{}
	if raw, ok := top["config"]; ok && !static {
		var in map[string]string
		if err := json.Unmarshal(raw, &in); err == nil {
			cfg = storageConfig(c, in)
		}
	}
	type credential struct {
		Prefix string            `json:"prefix"`
		Config map[string]string `json:"config"`
	}
	creds := []credential{}
	if raw, ok := top["storage-credentials"]; ok && !static {
		var in []credential
		if err := json.Unmarshal(raw, &in); err == nil {
			for _, cr := range in {
				// DuckDB scopes these credentials to the prefix, so it must
				// not cover the warehouse or the allowed locations.
				if err := p.locs.checkPrefix(cr.Prefix); err != nil {
					return nil, nil, fmt.Errorf("the catalog hands out credentials for %s: %w", cr.Prefix, err)
				}
				creds = append(creds, credential{Prefix: cr.Prefix, Config: storageConfig(c, cr.Config)})
			}
		}
	}
	top["config"], _ = json.Marshal(cfg)
	top["storage-credentials"], _ = json.Marshal(creds)
	out, err := json.Marshal(top)
	return out, roots, err
}

// under reports whether path lies under one of roots.
func under(path string, roots []string) bool {
	return slices.ContainsFunc(roots, func(r string) bool { return strings.HasPrefix(path, r) })
}

// --- DuckDB's requests -------------------------------------------------------

// readEndpoints are the catalog API DuckDB is told of: reads only, of
// namespaces and tables. Without the views' endpoints listed, DuckDB lists
// no views.
var readEndpoints = []string{
	"GET /v1/{prefix}/namespaces",
	"GET /v1/{prefix}/namespaces/{namespace}",
	"HEAD /v1/{prefix}/namespaces/{namespace}",
	"GET /v1/{prefix}/namespaces/{namespace}/tables",
	"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
	"HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
}

// ServeHTTP answers DuckDB. Lists come from what the replica found at
// startup; loading a table goes to the catalog; nothing else does.
func (p *icebergProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rest, ok := strings.CutPrefix(r.URL.EscapedPath(), p.path+"/v1/")
	if !ok {
		http.NotFound(w, r)
		return
	}
	var parts []string
	for _, s := range strings.Split(rest, "/") {
		u, err := url.PathUnescape(s)
		if err != nil {
			catalogError(w, http.StatusBadRequest, "BadRequestException", "bad path")
			return
		}
		parts = append(parts, u)
	}
	read := r.Method == http.MethodGet || r.Method == http.MethodHead
	switch {
	case len(parts) == 1 && parts[0] == "config" && r.Method == http.MethodGet:
		// No prefix: the proxy adds the catalog's. No other settings: DuckDB
		// must not be told to reach anything but the proxy.
		writeJSON(w, http.StatusOK, map[string]any{"defaults": map[string]string{}, "overrides": map[string]string{}, "endpoints": readEndpoints})
	case len(parts) == 1 && parts[0] == "namespaces" && r.Method == http.MethodGet:
		p.mu.Lock()
		list := [][]string{}
		if r.URL.Query().Get("parent") == "" {
			list = append(list, p.namespaces...)
		}
		p.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{"namespaces": list})
	case len(parts) == 2 && parts[0] == "namespaces" && read:
		ns := strings.Split(parts[1], "\x1f")
		if !p.hasNamespace(ns) {
			catalogError(w, http.StatusNotFound, "NoSuchNamespaceException", "namespace "+strings.Join(ns, ".")+" is not attached")
			return
		}
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"namespace": ns, "properties": map[string]string{}})
	case len(parts) == 3 && parts[0] == "namespaces" && parts[2] == "tables" && r.Method == http.MethodGet:
		ns := strings.Split(parts[1], "\x1f")
		ids := []map[string]any{}
		p.mu.Lock()
		for _, t := range p.tables {
			if slices.Equal(t.namespace, ns) {
				ids = append(ids, map[string]any{"namespace": t.namespace, "name": t.name})
			}
		}
		p.mu.Unlock()
		slices.SortFunc(ids, func(a, b map[string]any) int { return strings.Compare(a["name"].(string), b["name"].(string)) })
		writeJSON(w, http.StatusOK, map[string]any{"identifiers": ids})
	case len(parts) == 3 && parts[0] == "namespaces" && parts[2] == "views" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"identifiers": []any{}})
	case len(parts) == 4 && parts[0] == "namespaces" && parts[2] == "tables" && read:
		p.loadTable(w, r, strings.Split(parts[1], "\x1f"), parts[3])
	case len(parts) == 5 && parts[0] == "namespaces" && parts[2] == "tables" && parts[4] == "metrics" && r.Method == http.MethodPost:
		// Scan reports would tell the catalog what queries filter on.
		w.WriteHeader(http.StatusNoContent)
	case read:
		catalogError(w, http.StatusNotFound, "NotFoundException", "not supported by Igloo's connection")
	default:
		catalogError(w, http.StatusForbidden, "ForbiddenException", "connection "+p.name+" is read-only")
	}
}

func (p *icebergProxy) hasNamespace(ns []string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.ContainsFunc(p.namespaces, func(n []string) bool { return slices.Equal(n, ns) })
}

// loadTable answers DuckDB's load of a table: from the catalog, checked.
func (p *icebergProxy) loadTable(w http.ResponseWriter, r *http.Request, ns []string, name string) {
	p.mu.Lock()
	t := p.tables[tableKey(ns, name)]
	var warm []byte
	if t != nil && t.warm != nil && time.Now().Before(t.warmUntil) {
		warm = t.warm
	}
	if t != nil {
		t.warm = nil
	}
	p.mu.Unlock()
	if t == nil {
		catalogError(w, http.StatusNotFound, "NoSuchTableException", p.missing(ns, name))
		return
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if warm != nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(warm)
		return
	}
	c := p.settings()
	q := url.Values{}
	if s := r.URL.Query().Get("snapshots"); s == "all" || s == "refs" {
		q.Set("snapshots", s)
	}
	res, err := p.get(r.Context(), c, p.rel("namespaces", namespacePath(ns), "tables", url.PathEscape(name)), q, true)
	if err != nil {
		catalogError(w, http.StatusServiceUnavailable, "ServiceUnavailableException",
			fmt.Sprintf("connection %s: the catalog is not reachable: %s", p.name, Scrub(err.Error())))
		return
	}
	if res.status != http.StatusOK {
		catalogError(w, res.status, "RESTException", fmt.Sprintf("connection %s: the catalog answered %d: %s", p.name, res.status, catalogMessage(res.body)))
		return
	}
	body, _, err := p.inspect(c, res.body, t)
	if err != nil {
		p.log.Warn("refused a table the catalog described", "table", strings.Join(ns, ".")+"."+name, "err", Scrub(err.Error()))
		catalogError(w, http.StatusForbidden, "ForbiddenException", fmt.Sprintf("connection %s refuses table %s.%s: %s", p.name, strings.Join(ns, "."), name, Scrub(err.Error())))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

// missing explains why a table is not attached.
func (p *icebergProxy) missing(ns []string, name string) string {
	key := strings.Join(ns, ".") + "." + name
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, s := range p.skipped {
		if strings.HasPrefix(s, key+": ") {
			return fmt.Sprintf("connection %s left out table %s", p.name, s)
		}
	}
	return fmt.Sprintf("table %s is not attached: connection %s attaches the tables it found when this replica started", key, p.name)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		catalogError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

// catalogError answers with an Iceberg REST error.
func catalogError(w http.ResponseWriter, status int, typ, msg string) {
	var b bytes.Buffer
	_ = json.NewEncoder(&b).Encode(map[string]any{"error": map[string]any{"message": msg, "type": typ, "code": status}})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b.Bytes())
}
