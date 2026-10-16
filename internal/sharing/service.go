package sharing

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	iceio "github.com/apache/iceberg-go/io"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/catalog"
)

const (
	// activationLifetime is how long an activation link can be used.
	activationLifetime = 7 * 24 * time.Hour
	// DefaultTokenLifetime is how long a recipient's token is valid after
	// activation, unless CREATE RECIPIENT says otherwise.
	DefaultTokenLifetime = 365 * 24 * time.Hour
)

// Options configure Delta Sharing.
type Options struct {
	Store *Store
	// Key is a secret every replica shares, at least 32 random bytes.
	Key []byte
	// Catalog reads the tables Igloo shares, and Files their data files.
	Catalog *catalog.Client
	Files   iceio.IO
	// Warehouse is the warehouse root with a trailing slash.
	Warehouse string
	// CatalogName is the catalog's name in SQL.
	CatalogName string
	// URL is the sharing endpoint's public URL, including Prefix, for
	// activation links and profile files. Empty uses the URL a recipient
	// reaches the endpoint at, and LocalURL in activation links.
	URL string
	// LocalURL is the endpoint's URL on this machine, such as
	// http://localhost:8444/delta-sharing.
	LocalURL string
	// Engine and Proxy let SQL read external tables.
	Engine Engine
	Proxy  *Proxy
	Audit  *audit.Log
	Log    *slog.Logger
	// Metrics may be nil.
	Metrics *Metrics
}

// Service carries out Delta Sharing statements and serves the protocol.
type Service struct {
	store    *Store
	source   source
	server   *Server
	consumer *Consumer
	linkBase string
	log      *slog.Logger
}

// New sets up Delta Sharing: the sharing endpoint's handler, and external
// tables, whose views it loads.
func New(ctx context.Context, opts Options) (*Service, error) {
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	if opts.Store == nil || opts.Catalog == nil || opts.Engine == nil || opts.Proxy == nil {
		return nil, ErrUnavailable
	}
	tables := IcebergTables(opts.Catalog, opts.Warehouse, opts.Files)
	server, err := NewServer(ServerOptions{
		Store: opts.Store, Key: opts.Key, Files: opts.Files, Warehouse: opts.Warehouse, URL: opts.URL,
		Catalog: opts.CatalogName, Audit: opts.Audit, Log: opts.Log, Metrics: opts.Metrics, Tables: tables,
	})
	if err != nil {
		return nil, err
	}
	tokens, err := newSealer(opts.Key, "provider token v1")
	if err != nil {
		return nil, err
	}
	consumer, err := newConsumer(ctx, opts.Store, tokens, opts.Proxy, opts.Engine, opts.Log)
	if err != nil {
		return nil, err
	}
	if _, err := consumer.Sync(ctx); err != nil {
		opts.Log.Warn("could not load external tables", "err", err)
	}
	base := opts.URL
	if base == "" {
		base = opts.LocalURL
	}
	return &Service{
		store: opts.Store, source: tables.source, server: server, consumer: consumer,
		linkBase: strings.TrimSuffix(base, "/"), log: opts.Log.With("component", "sharing"),
	}, nil
}

// Handler serves the Delta Sharing protocol.
func (s *Service) Handler() *Server { return s.server }

// Consumer keeps external tables.
func (s *Service) Consumer() *Consumer { return s.consumer }

// SetProtection tells the service which tables have masking or row access
// policies, which it then neither shares nor serves (see protect.go). Until
// it is set, the endpoint serves no table and no table can be added to a
// share.
func (s *Service) SetProtection(p Protection) { s.server.SetProtection(p) }

// ProxyPrefix is where DuckDB reads external tables' files: Igloo's own
// proxy, on the loopback interface under a random path. Users other than
// admins must not read it directly while tables have policies, as they may
// not read the warehouse's files.
func (s *Service) ProxyPrefix() string { return s.consumer.proxy.Prefix() }

// Run keeps external tables current, and deletes copies of data files that
// shared tables no longer need, until ctx is done.
func (s *Service) Run(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.collectCopies(ctx)
	}()
	s.consumer.Run(ctx)
	<-done
}

// collectInterval is how often copies of data files are collected; the
// first collection waits collectDelay after a start.
const (
	collectInterval = 6 * time.Hour
	collectDelay    = 10 * time.Minute
)

func (s *Service) collectCopies(ctx context.Context) {
	src, ok := s.source.(*icebergSource)
	if !ok {
		return
	}
	delay := collectDelay
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		delay = collectInterval
		tables, err := s.allSharedTables(ctx)
		if err == nil {
			var n int
			if n, err = src.collect(ctx, tables, s.protectedTable); n > 0 {
				s.log.Info("deleted copies of shared data files that are no longer needed", "files", n)
			}
		}
		if err != nil && ctx.Err() == nil {
			s.log.Warn("could not collect copies of shared data files", "err", err)
		}
	}
}

// protectedTable reports whether a shared table now has policies, so its
// copies are deleted at once rather than kept for earlier URLs, which are
// refused anyway. While policies cannot be checked, nothing counts as
// protected and copies are kept.
func (s *Service) protectedTable(ctx context.Context, t SharedTable) bool {
	return IsProtected(s.server.protection.check(ctx, t.SourceSchema, t.SourceTable))
}

func (s *Service) allSharedTables(ctx context.Context) ([]SharedTable, error) {
	shares, err := s.store.Shares(ctx)
	if err != nil {
		return nil, err
	}
	var out []SharedTable
	for _, sh := range shares {
		tables, err := s.store.ShareTables(ctx, sh.Name)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		out = append(out, tables...)
	}
	return out, nil
}

// Close releases the service's resources. Run must have returned.
func (s *Service) Close() error {
	err := s.consumer.close()
	s.store.Close()
	return err
}

// --- shares -----------------------------------------------------------------

// CreateShare creates an empty share.
func (s *Service) CreateShare(ctx context.Context, name, comment, by string) error {
	return s.store.CreateShare(ctx, Share{Name: name, Comment: comment, CreatedBy: by})
}

// DropShare drops a share; its recipients lose access at once.
func (s *Service) DropShare(ctx context.Context, name string) error {
	defer s.server.Forget()
	return s.store.DropShare(ctx, name)
}

// AddTable adds a table to a share, after checking that it can be shared.
func (s *Service) AddTable(ctx context.Context, t SharedTable) error {
	if err := ValidSharedName(t.Schema); err != nil {
		return err
	}
	if err := ValidSharedName(t.Name); err != nil {
		return err
	}
	if err := s.server.protection.check(ctx, t.SourceSchema, t.SourceTable); err != nil {
		return err
	}
	if _, err := s.source.snapshot(ctx, t.SourceSchema, t.SourceTable, version{}, false); err != nil {
		return err
	}
	return s.store.AddTable(ctx, t)
}

// RemoveTable removes the table recipients know as schema.name from share.
func (s *Service) RemoveTable(ctx context.Context, share, schema, name string) error {
	defer s.server.Forget()
	return s.store.RemoveTable(ctx, share, schema, name)
}

// Shares lists every share.
func (s *Service) Shares(ctx context.Context) ([]Share, error) { return s.store.Shares(ctx) }

// ShareTables lists a share's tables.
func (s *Service) ShareTables(ctx context.Context, share string) ([]SharedTable, error) {
	return s.store.ShareTables(ctx, share)
}

// ShareGrants lists the recipients of share, or of every share.
func (s *Service) ShareGrants(ctx context.Context, share string) ([]ShareGrant, error) {
	return s.store.ShareGrants(ctx, share)
}

// GrantShare lets a recipient read a share.
func (s *Service) GrantShare(ctx context.Context, share, recipient, by string) error {
	defer s.server.Forget()
	return s.store.GrantShare(ctx, share, recipient, by)
}

// RevokeShare stops a recipient reading a share, at once.
func (s *Service) RevokeShare(ctx context.Context, share, recipient string) error {
	defer s.server.Forget()
	return s.store.RevokeShare(ctx, share, recipient)
}

// --- recipients -------------------------------------------------------------

// Activation is a link a recipient uses once to download its profile file.
type Activation struct {
	Link    string
	Expires time.Time
}

func (s *Service) activation() (secretToken, Activation, error) {
	code, err := newToken(activationPrefix)
	if err != nil {
		return code, Activation{}, err
	}
	return code, Activation{Link: s.linkBase + "/activate/" + code.Text, Expires: time.Now().Add(activationLifetime)}, nil
}

// CreateRecipient creates a recipient and returns its activation link.
// lifetime is how long its token will be valid; zero means
// DefaultTokenLifetime.
func (s *Service) CreateRecipient(ctx context.Context, name, comment string, lifetime time.Duration, by string) (Activation, error) {
	if lifetime == 0 {
		lifetime = DefaultTokenLifetime
	}
	code, a, err := s.activation()
	if err != nil {
		return a, err
	}
	err = s.store.CreateRecipient(ctx, Recipient{Name: name, Comment: comment, TokenLifetime: lifetime, CreatedBy: by}, code, a.Expires)
	return a, err
}

// RotateToken gives a recipient a new activation link. Its current tokens
// stop working after keep, at once when keep is zero.
func (s *Service) RotateToken(ctx context.Context, name string, keep time.Duration) (Activation, error) {
	code, a, err := s.activation()
	if err != nil {
		return a, err
	}
	defer s.server.Forget()
	return a, s.store.RotateToken(ctx, name, keep, code, a.Expires)
}

// DropRecipient drops a recipient; its token stops working at once.
func (s *Service) DropRecipient(ctx context.Context, name string) error {
	defer s.server.Forget()
	return s.store.DropRecipient(ctx, name)
}

// Recipients lists every recipient.
func (s *Service) Recipients(ctx context.Context) ([]Recipient, error) {
	return s.store.Recipients(ctx)
}

// Describe explains err, from any of the service's methods, for a SQL
// client. what names the object the statement was about, as in share "x".
func Describe(err error, what string) string {
	var ae *apiError
	switch {
	case err == ErrNotFound:
		return what + " does not exist"
	case errors.Is(err, ErrExists):
		return what + " already exists"
	case errors.Is(err, ErrInUse):
		return what + " is used by external tables; drop them first"
	case errors.As(err, &ae):
		return ae.msg
	}
	return err.Error()
}

// IsNotShareable reports whether err says a table cannot be shared as it is.
func IsNotShareable(err error) bool { return isNotShareable(err) }

// IsProviderError reports whether err came from a provider's answer.
func IsProviderError(err error) bool {
	var pe *providerError
	return errors.As(err, &pe)
}
