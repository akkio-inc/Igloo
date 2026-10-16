package federation

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

// syncInterval is how often a replica picks up connections that other
// replicas created, changed or dropped.
const syncInterval = 15 * time.Second

// Options configures a Manager.
type Options struct {
	// SecretsDir holds the files password_secret names; empty means
	// connections cannot use password_secret.
	SecretsDir string
	// Connect opens a DuckDB connection, for detaching and refreshing
	// connections once the engine is running. Set it with SetConnect.
	Connect func(context.Context) (*duckdb.Conn, error)
	// Protected are host:port addresses no connection may reach: the
	// metadata database's server, whose users, grants and catalog no grant
	// should expose. They are compared however the names are written, and
	// by the addresses they resolve to when a connection is created or
	// changed (see reachesProtected). It guards against mistakes, not a
	// determined admin, who can reach the server another way.
	Protected []string
}

// Manager attaches connections to DuckDB and keeps them in step with the
// store.
type Manager struct {
	store     Store
	secrets   string
	protected []server
	log       *slog.Logger
	token     string

	connect atomic.Pointer[func(context.Context) (*duckdb.Conn, error)]
	// gen counts changes to what is attached; see Generation.
	gen atomic.Uint64

	// mu guards the fields below.
	mu sync.Mutex
	// attached are the connections DuckDB has, by name.
	attached map[string]*attachment
	// failed are connections that existed at startup but could not be
	// attached, with why.
	failed map[string]string
	// started is set once Attach ran: connections created after that wait
	// for a restart.
	started bool
}

// attachment is a connection DuckDB attached.
type attachment struct {
	conn  Connection
	proxy *proxy
}

// NewManager returns a manager for the connections in store.
func NewManager(store Store, opts Options, log *slog.Logger) *Manager {
	m := &Manager{
		store: store, secrets: opts.SecretsDir, protected: parseServers(opts.Protected), log: log, token: newToken(),
		attached: map[string]*attachment{}, failed: map[string]string{},
	}
	if opts.Connect != nil {
		m.SetConnect(opts.Connect)
	}
	return m
}

// SetConnect gives the manager a way to open DuckDB connections once the
// engine runs.
func (m *Manager) SetConnect(f func(context.Context) (*duckdb.Conn, error)) { m.connect.Store(&f) }

// Generation changes whenever a connection is attached, detached or
// changed, which can change what a statement reads.
func (m *Manager) Generation() uint64 { return m.gen.Load() }

// Catalogs returns the attached connections' names, lower-cased, and types.
func (m *Manager) Catalogs() map[string]Type {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]Type, len(m.attached))
	for name, a := range m.attached {
		out[strings.ToLower(name)] = a.conn.Type
	}
	return out
}

// Exists reports whether a connection called name is registered, attached
// here or not.
func (m *Manager) Exists(ctx context.Context, name string) (bool, error) {
	conns, err := m.store.List(ctx)
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(conns, func(c Connection) bool { return strings.EqualFold(c.Name, name) }), nil
}

// extension is the DuckDB extension that attaches a type.
func (t Type) extension() string {
	if t == MySQL {
		return "mysql"
	}
	return "postgres"
}

// Attach attaches every registered connection on conn, the engine's setup
// connection, before the sandbox is locked (see engine.BeforeLockdown) and
// so before any session starts; nothing is attached after that. Each
// attachment changes Generation, which package pgwire's checks follow, so
// they would hold if connections were attached later too. A
// connection whose database cannot be reached is left out, with the
// reason, so one database being down never stops Igloo from starting; the
// remote databases are tried all at once, so startup waits at most about
// the longest connect_timeout.
func (m *Manager) Attach(ctx context.Context, conn *duckdb.Conn) error {
	conns, err := m.store.List(ctx)
	if err != nil {
		return fmt.Errorf("load connections: %w", err)
	}
	m.mu.Lock()
	m.started = true
	m.mu.Unlock()
	if len(conns) == 0 {
		return nil
	}

	loaded := map[Type]error{}
	for _, c := range conns {
		if _, ok := loaded[c.Type]; !ok {
			loaded[c.Type] = exec(ctx, conn, "LOAD "+c.Type.extension())
		}
	}
	if loaded[Postgres] == nil {
		// By default the postgres extension asks the remote database
		// whether its tables changed every time it lists them, so listing
		// any catalog's tables would fail while one database is down. The
		// lists are read once instead, and again on ALTER CONNECTION ...
		// REFRESH; a table created since is still found when named.
		if err := exec(ctx, conn, "SET GLOBAL pg_staleness_query_enabled = false"); err != nil {
			loaded[Postgres] = err
		}
	}
	proxies := make([]*proxy, len(conns))
	errs := make([]error, len(conns))
	var wg sync.WaitGroup
	for i, c := range conns {
		if err := loaded[c.Type]; err != nil {
			errs[i] = fmt.Errorf("DuckDB's %s extension: %w", c.Type.extension(), err)
			continue
		}
		if proxies[i], errs[i] = newProxy(c, m.token, m.secrets, m.log); errs[i] != nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = proxies[i].probe()
		}()
	}
	wg.Wait()

	for i, c := range conns {
		err := errs[i]
		if err == nil {
			err = m.attach(ctx, conn, c, proxies[i])
		} else if proxies[i] != nil {
			proxies[i].close()
		}
		if err != nil {
			m.log.Warn("connection not attached; it is retried when Igloo restarts", "connection", c.Name, "err", err)
			m.mu.Lock()
			m.failed[c.Name] = Scrub(err.Error())
			m.mu.Unlock()
			continue
		}
		m.log.Info("attached connection", "connection", c.Name, "type", c.Type, "host", c.Host, "database", c.Database)
	}
	return nil
}

// attach attaches one connection through its proxy and loads its list of
// tables, so that listing tables (information_schema, psql's \dt) never
// waits on that database again, even while it is down.
func (m *Manager) attach(ctx context.Context, conn *duckdb.Conn, c Connection, p *proxy) error {
	for _, stmt := range attachStatements(c, m.token, p.port()) {
		if err := exec(ctx, conn, stmt); err != nil {
			p.close()
			_ = exec(ctx, conn, "DETACH DATABASE IF EXISTS "+ident(c.Name))
			return errors.New(Scrub(err.Error()))
		}
	}
	m.mu.Lock()
	m.attached[c.Name] = &attachment{conn: c, proxy: p}
	m.mu.Unlock()
	m.gen.Add(1)
	return nil
}

// attachStatements attach c through the proxy listening on port. DuckDB
// knows only the proxy's address and token, kept in a secret so that
// duckdb_databases() does not show it.
func attachStatements(c Connection, token string, port int) []string {
	secret := ident("igloo_connection_" + c.Name)
	var path string
	switch c.Type {
	case MySQL:
		// The MySQL extension's default schema is the database.
		path = fmt.Sprintf("host=127.0.0.1 port=%d user=igloo database=%s ssl_mode=disabled", port, mysqlDSNValue(c.Database))
	default:
		path = fmt.Sprintf("host=127.0.0.1 port=%d user=igloo dbname=%s sslmode=disable", port, c.Name)
	}
	opts := "TYPE " + c.Type.extension() + ", SECRET " + secret + ", READ_ONLY"
	if len(c.Schemas) > 0 {
		quoted := make([]string, len(c.Schemas))
		for i, s := range c.Schemas {
			quoted[i] = literal(s)
		}
		opts += ", SCHEMA [" + strings.Join(quoted, ", ") + "]"
	}
	return []string{
		fmt.Sprintf("CREATE OR REPLACE SECRET %s (TYPE %s, PASSWORD %s)", secret, c.Type.extension(), literal(token)),
		fmt.Sprintf("ATTACH %s AS %s (%s)", literal(path), ident(c.Name), opts),
		"SELECT count(*) FROM duckdb_tables() WHERE database_name = " + literal(c.Name),
	}
}

// mysqlDSNValue quotes a value for the MySQL extension's key=value
// connection string, which escapes \ and " with a backslash.
func mysqlDSNValue(s string) string {
	return `"` + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `"`, `\"`) + `"`
}

// signIn checks that Igloo can sign in to c's database, without DuckDB.
func (m *Manager) signIn(c Connection) error {
	p := &proxy{name: c.Name, secrets: m.secrets, log: m.log, attached: c, current: c}
	return p.probe()
}

// Run keeps this replica in step with the store until ctx is done.
func (m *Manager) Run(ctx context.Context) {
	t := time.NewTicker(syncInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			m.closeAll()
			return
		case <-t.C:
			if err := m.Sync(ctx); err != nil {
				m.log.Warn("could not sync connections", "err", err)
			}
		}
	}
}

// Sync applies changes made to the store since the last sync: new settings
// reach the proxies, and dropped or recreated connections are detached.
func (m *Manager) Sync(ctx context.Context) error {
	conns, err := m.store.List(ctx)
	if err != nil {
		return err
	}
	byName := map[string]Connection{}
	for _, c := range conns {
		byName[c.Name] = c
	}
	m.mu.Lock()
	var gone []string
	for name, a := range m.attached {
		c, ok := byName[name]
		switch {
		case !ok || c.ID != a.conn.ID:
			gone = append(gone, name)
		case !c.UpdatedAt.Equal(a.conn.UpdatedAt):
			a.conn = c
			a.proxy.update(c)
			m.gen.Add(1)
		}
	}
	for name := range m.failed {
		if _, ok := byName[name]; !ok {
			delete(m.failed, name)
		}
	}
	m.mu.Unlock()
	for _, name := range gone {
		if err := m.detach(ctx, name); err != nil {
			m.log.Warn("could not detach a dropped connection", "connection", name, "err", err)
		}
	}
	return nil
}

// detach removes an attached connection from DuckDB and stops its proxy.
func (m *Manager) detach(ctx context.Context, name string) error {
	m.mu.Lock()
	a := m.attached[name]
	delete(m.attached, name)
	m.mu.Unlock()
	if a == nil {
		return nil
	}
	m.gen.Add(1)
	a.proxy.close()
	return m.withConn(ctx, func(conn *duckdb.Conn) error {
		err := exec(ctx, conn, "DETACH DATABASE IF EXISTS "+ident(name))
		if err == nil {
			err = exec(ctx, conn, "DROP SECRET IF EXISTS "+ident("igloo_connection_"+name))
		}
		return err
	})
}

func (m *Manager) withConn(ctx context.Context, f func(*duckdb.Conn) error) error {
	connect := m.connect.Load()
	if connect == nil {
		return errors.New("the engine is not running")
	}
	conn, err := (*connect)(ctx)
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck
	return f(conn)
}

func (m *Manager) closeAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, a := range m.attached {
		a.proxy.close()
	}
}

// --- administration ---------------------------------------------------------

// Create registers c after checking that Igloo can sign in to its
// database. taken are the names of the databases DuckDB has. Replicas
// attach it when they next start.
func (m *Manager) Create(ctx context.Context, c Connection, taken []string) error {
	if err := m.validate(ctx, c, taken); err != nil {
		return err
	}
	if err := m.signIn(c); err != nil {
		return &UnreachableError{c.Name, err}
	}
	return m.store.Create(ctx, c)
}

// Alter changes a connection's settings with set, which applies options to
// it, after checking that Igloo can sign in with the new ones. Host,
// credentials, TLS and timeouts apply on every replica within
// syncInterval, at once on this one; the database and schemas when
// replicas restart.
func (m *Manager) Alter(ctx context.Context, name string, set func(*Connection) error) (Connection, error) {
	c, err := m.get(ctx, name)
	if err != nil {
		return c, err
	}
	if err := set(&c); err != nil {
		return c, &InvalidError{err}
	}
	if err := m.validate(ctx, c, nil); err != nil {
		return c, err
	}
	c.Unusable = ""
	if err := m.signIn(c); err != nil {
		return c, &UnreachableError{c.Name, err}
	}
	if err := m.store.Update(ctx, c); err != nil {
		return c, err
	}
	return c, m.Sync(ctx)
}

// validate checks c's settings, and that it does not reach a protected
// server.
func (m *Manager) validate(ctx context.Context, c Connection, taken []string) error {
	if err := c.Validate(taken); err != nil {
		return &InvalidError{err}
	}
	if p, ok := reachesProtected(ctx, m.protected, c.Host, c.Port); ok {
		return &InvalidError{fmt.Errorf("%s is the server of Igloo's metadata database (%s), which a connection must not reach",
			c.Address(), net.JoinHostPort(p.host, strconv.Itoa(p.port)))}
	}
	return nil
}

// Drop removes a connection and its grants, and detaches it here at once
// and on other replicas within syncInterval.
func (m *Manager) Drop(ctx context.Context, name string) error {
	if err := m.store.Delete(ctx, name); err != nil {
		return err
	}
	return m.Sync(ctx)
}

// Refresh makes DuckDB read again which tables the connection's database
// has. DuckDB's extensions keep one list for all databases of a type, so
// the lists of every attached connection of the same type are read again;
// to keep one that is down from losing its list, Refresh first checks that
// all of them can be reached.
func (m *Manager) Refresh(ctx context.Context, name string) error {
	m.mu.Lock()
	a := m.attached[name]
	var same []Connection
	for _, other := range m.attached {
		if a != nil && other.conn.Type == a.conn.Type {
			same = append(same, other.proxy.settings())
		}
	}
	m.mu.Unlock()
	if a == nil {
		if _, err := m.get(ctx, name); err != nil {
			return err
		}
		return fmt.Errorf("connection %s is not attached on this replica; it is attached when Igloo restarts", name)
	}
	for _, c := range same {
		if err := m.signIn(c); err != nil {
			return &UnreachableError{c.Name, fmt.Errorf("refreshing would drop the list of its tables while it is down: %w", err)}
		}
	}
	clearCache := "pg_clear_cache"
	if a.conn.Type == MySQL {
		clearCache = "mysql_clear_cache"
	}
	m.gen.Add(1)
	return m.withConn(ctx, func(conn *duckdb.Conn) error {
		if err := exec(ctx, conn, "SELECT * FROM "+clearCache+"()"); err != nil {
			return err
		}
		for _, c := range same {
			if err := exec(ctx, conn, "SELECT count(*) FROM duckdb_tables() WHERE database_name = "+literal(c.Name)); err != nil {
				return errors.New(Scrub(err.Error()))
			}
		}
		return nil
	})
}

func (m *Manager) get(ctx context.Context, name string) (Connection, error) {
	conns, err := m.store.List(ctx)
	if err != nil {
		return Connection{}, err
	}
	for _, c := range conns {
		if c.Name == name {
			return c, nil
		}
	}
	return Connection{}, ErrNotFound
}

// Status is a connection with what this replica made of it.
type Status struct {
	Connection
	// State is attached, not attached or failed.
	State string
	// Detail explains the state: why it failed, what waits for a restart,
	// or the last error signing in to the database.
	Detail string
}

// List returns every connection and its state on this replica.
func (m *Manager) List(ctx context.Context) ([]Status, error) {
	conns, err := m.store.List(ctx)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Status, len(conns))
	for i, c := range conns {
		s := Status{Connection: c}
		a := m.attached[c.Name]
		switch {
		case a != nil && a.proxy.attached.attachedFields() != c.attachedFields():
			s.State, s.Detail = "attached", "restart to apply the new database or schema"
		case a != nil:
			s.State = "attached"
			if err := a.proxy.lastError(); err != "" {
				s.Detail = "the last sign-in to the database failed: " + err
			}
		case m.failed[c.Name] != "":
			s.State, s.Detail = "failed", m.failed[c.Name]+"; restart to retry"
		default:
			s.State, s.Detail = "not attached", "created after this replica started; restart to attach"
		}
		if c.Unusable != "" {
			s.Detail = c.Unusable
		}
		out[i] = s
	}
	return out, nil
}

// InvalidError is a connection setting that is not valid.
type InvalidError struct{ Err error }

func (e *InvalidError) Error() string { return e.Err.Error() }
func (e *InvalidError) Unwrap() error { return e.Err }

// UnreachableError means Igloo could not sign in to a connection's
// database.
type UnreachableError struct {
	Name string
	Err  error
}

func (e *UnreachableError) Error() string {
	return fmt.Sprintf("cannot sign in to connection %s's database: %s", e.Name, Scrub(e.Err.Error()))
}
func (e *UnreachableError) Unwrap() error { return e.Err }

// --- DuckDB -----------------------------------------------------------------

func exec(ctx context.Context, conn *duckdb.Conn, stmt string) error {
	rows, err := conn.QueryContext(ctx, stmt, nil)
	if err != nil {
		return err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, len(rows.Columns()))
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func literal(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func ident(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
