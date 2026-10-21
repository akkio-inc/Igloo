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
	"syscall"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

// syncInterval is how often a replica picks up connections that other
// replicas created, changed or dropped.
const syncInterval = 15 * time.Second

// attachMargin is how much of Igloo's startup time Attach leaves for the
// steps after it: a database that has not answered by then is left out.
const attachMargin = 30 * time.Second

// attachRetry is how long Attach keeps trying a database it cannot reach
// yet, as when the machine has just started and its network or DNS is not
// up: a refused sign-in or a wrong setting is not tried again, and a
// mistyped host delays startup by no more than this. A variable for tests.
var attachRetry = 30 * time.Second

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
	// Reserved are the locations Igloo reads with its own credentials,
	// the warehouse and the allowed locations, which no table of an
	// Iceberg REST catalog may be in (see locationRules).
	Reserved []string
	// LocalLocations lets Iceberg REST catalogs' tables be in directories
	// of the server, for tests.
	LocalLocations bool
}

// Manager attaches connections to DuckDB and keeps them in step with the
// store.
type Manager struct {
	store     Store
	secrets   string
	protected []server
	locs      locationRules
	log       *slog.Logger
	token     string
	// vended tracks the storage credentials DuckDB keeps for connections
	// of TYPE iceberg; see sweep.
	vended map[string]time.Time

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
	conn Connection
	ep   endpoint
}

// endpoint is what DuckDB attaches a connection at: a proxy to a
// PostgreSQL or MySQL database, the gateway to Snowflake, or a proxy to an
// Iceberg REST catalog (*icebergProxy).
type endpoint interface {
	// port is the loopback port DuckDB connects to.
	port() int
	// probe signs in to the remote database: a check that it can be
	// reached, which also teaches the endpoint what it needs to serve
	// DuckDB.
	probe() error
	// update applies new settings.
	update(Connection)
	close()
	// lastError says why the last sign-in failed, or "".
	lastError() string
	// settings are the connection's current settings, with the database
	// and schemas it was attached with.
	settings() Connection
	// attachedConn is the connection as DuckDB attached it.
	attachedConn() Connection
}

// newEndpoint listens for DuckDB's connections for c.
func newEndpoint(c Connection, token, secrets string, log *slog.Logger) (endpoint, error) {
	if c.Type == Snowflake {
		return newSnowflakeGateway(c, token, secrets, log)
	}
	return newProxy(c, token, secrets, log)
}

// NewManager returns a manager for the connections in store.
func NewManager(store Store, opts Options, log *slog.Logger) *Manager {
	m := &Manager{
		store: store, secrets: opts.SecretsDir, protected: parseServers(opts.Protected), log: log, token: newToken(),
		locs:     newLocationRules(opts.Reserved, opts.LocalLocations),
		attached: map[string]*attachment{}, failed: map[string]string{}, vended: map[string]time.Time{},
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

// extension is the DuckDB extension that attaches a type. Snowflake
// connections are attached as PostgreSQL databases, at the gateway.
func (t Type) extension() string {
	switch t {
	case MySQL:
		return "mysql"
	case Iceberg:
		return "iceberg"
	}
	return "postgres"
}

// SandboxDirectories returns the locations the attached Iceberg REST
// catalogs' tables keep their files in, each ending in a slash, for DuckDB's
// sandbox to allow (see engine.SandboxDirectories). It is read once, when
// the sandbox is locked, after Attach.
func (m *Manager) SandboxDirectories() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, a := range m.attached {
		if ip, ok := a.ep.(*icebergProxy); ok {
			out = append(out, ip.roots()...)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// Attach attaches every registered connection on conn, the engine's setup
// connection, before the sandbox is locked (see engine.BeforeLockdown) and
// so before any session starts; nothing is attached after that. Each
// attachment changes Generation, which package pgwire's checks follow, so
// they would hold if connections were attached later too. A
// connection whose database cannot be reached is left out, with the
// reason, so one database being down never stops Igloo from starting; the
// remote databases are tried all at once, those that cannot be reached yet
// again for up to attachRetry (see retryUnreachable), and those that have
// not answered attachMargin before ctx's deadline are left out too.
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

	loaded := map[string]error{}
	for _, c := range conns {
		if _, ok := loaded[c.Type.extension()]; !ok {
			loaded[c.Type.extension()] = exec(ctx, conn, "LOAD "+c.Type.extension())
			if c.Type == Iceberg && loaded["iceberg"] == nil {
				// DuckDB reaches the catalog and the files over HTTP.
				loaded["iceberg"] = exec(ctx, conn, "LOAD httpfs")
			}
		}
	}
	if err, ok := loaded["postgres"]; ok && err == nil {
		// By default the postgres extension asks the remote database
		// whether its tables changed every time it lists them, so listing
		// any catalog's tables would fail while one database is down. The
		// lists are read once instead, and again on ALTER CONNECTION ...
		// REFRESH; a table created since is still found when named.
		if err := exec(ctx, conn, "SET GLOBAL pg_staleness_query_enabled = false"); err != nil {
			loaded["postgres"] = err
		}
	}
	wait := ctx
	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		wait, cancel = context.WithDeadline(ctx, deadline.Add(-attachMargin))
		defer cancel()
	}
	proxies := make([]endpoint, len(conns))
	errs := make([]error, len(conns))
	probes := make([]chan error, len(conns))
	for i, c := range conns {
		if err := loaded[c.Type.extension()]; err != nil {
			errs[i] = fmt.Errorf("DuckDB's %s extension: %w", c.Type.extension(), err)
			continue
		}
		if c.Type == Iceberg {
			// Finding the catalog's tables is its probe: the sandbox
			// opens to their locations before it is locked.
			ip, err := newIcebergProxy(c, m.secrets, m.locs, m.log)
			if err == nil {
				err = ip.listen()
			}
			if err != nil {
				errs[i] = err
				continue
			}
			proxies[i] = ip
			probes[i] = make(chan error, 1)
			go func() { probes[i] <- m.retryUnreachable(wait, c.Name, func() error { return ip.discover(wait) }) }()
			continue
		}
		if proxies[i], errs[i] = newEndpoint(c, m.token, m.secrets, m.log); errs[i] != nil {
			continue
		}
		probes[i] = make(chan error, 1)
		go func() { probes[i] <- m.retryUnreachable(wait, c.Name, proxies[i].probe) }()
	}
	for i, probe := range probes {
		if probe != nil {
			errs[i] = awaitProbe(wait, probe)
		}
	}

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
		database := c.Database
		if c.Type == Iceberg {
			database = c.Iceberg.Warehouse
		}
		m.log.Info("attached connection", "connection", c.Name, "type", c.Type, "host", c.ServerHost(), "database", database)
	}
	return nil
}

// retryUnreachable runs probe, and again while it fails because the
// database cannot be reached yet (see unreachable), for up to attachRetry
// or until ctx is done, backing off from 1 s to 10 s between tries.
func (m *Manager) retryUnreachable(ctx context.Context, name string, probe func() error) error {
	until := time.Now().Add(attachRetry)
	wait := time.Second
	for {
		err := probe()
		if err == nil || !unreachable(err) || ctx.Err() != nil || time.Now().Add(wait).After(until) {
			return err
		}
		m.log.Info("connection's database not reachable yet; trying again", "connection", name, "in", wait, "err", Scrub(err.Error()))
		select {
		case <-ctx.Done():
			return err
		case <-time.After(wait):
		}
		wait = min(2*wait, 10*time.Second)
	}
}

// unreachable reports whether err says the database could not be reached
// (no route, refused, reset, timed out, name not resolved), which waiting
// may cure, rather than that it refused the sign-in or a setting is wrong.
func unreachable(err error) bool {
	var dns *net.DNSError
	var op *net.OpError
	if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &dns) || errors.As(err, &op) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH) {
		return true
	}
	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		return true
	}
	// Drivers do not all keep the network error; their text does.
	msg := strings.ToLower(err.Error())
	for _, s := range []string{"connection refused", "no such host", "i/o timeout", "network is unreachable",
		"no route to host", "tls handshake timeout", "context deadline exceeded", "connection reset"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// awaitProbe returns the result of a probe, or an error once ctx is done.
// The probe goes on until its own timeout; closing its endpoint stops a
// Snowflake gateway's at once.
func awaitProbe(ctx context.Context, probe <-chan error) error {
	select {
	case err := <-probe:
		return err
	case <-ctx.Done():
	}
	select {
	case err := <-probe:
		return err
	default:
		return errors.New("no answer in the time Igloo gives connections at startup")
	}
}

// attach attaches one connection through its proxy and loads its list of
// tables, so that listing tables (information_schema, psql's \dt) never
// waits on that database again, even while it is down.
func (m *Manager) attach(ctx context.Context, conn *duckdb.Conn, c Connection, rp endpoint) error {
	var stmts []string
	if p, ok := rp.(*icebergProxy); ok {
		var err error
		if stmts, err = icebergAttachStatements(c, p); err != nil {
			rp.close()
			return errors.New(Scrub(err.Error()))
		}
	} else {
		stmts = attachStatements(c, m.token, rp.port())
	}
	for _, stmt := range stmts {
		if err := exec(ctx, conn, stmt); err != nil {
			rp.close()
			_ = exec(ctx, conn, "DETACH DATABASE IF EXISTS "+ident(c.Name))
			_ = exec(ctx, conn, "DROP SECRET IF EXISTS "+ident("igloo_connection_"+c.Name))
			return errors.New(Scrub(err.Error()))
		}
	}
	if ip, ok := rp.(*icebergProxy); ok {
		// Load the tables' columns, which DuckDB otherwise lists as a
		// placeholder until a table is first read, so that tools browsing
		// the catalog see them. The answers from finding the tables serve.
		names, err := queryRows(ctx, conn, `SELECT DISTINCT schema_name, table_name FROM duckdb_columns()
			WHERE database_name = `+literal(c.Name)+` AND column_name = '__'`)
		if err == nil {
			for _, n := range names {
				schema, _ := n[0].(string)
				table, _ := n[1].(string)
				if err := exec(ctx, conn, "DESCRIBE "+ident(c.Name)+"."+ident(schema)+"."+ident(table)); err != nil {
					m.log.Warn("could not load the columns of a table", "connection", c.Name, "table", schema+"."+table, "err", Scrub(err.Error()))
				}
			}
		}
		m.log.Info("found the tables of a remote Iceberg catalog", "connection", c.Name, "detail", ip.summary())
	}
	m.mu.Lock()
	m.attached[c.Name] = &attachment{conn: c, ep: rp}
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
	// A Snowflake connection's gateway lists only its schemas itself, as
	// Snowflake names them.
	if len(c.Schemas) > 0 && c.Type != Snowflake {
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

// icebergAttachStatements attach c, an Iceberg REST catalog, through its
// proxy, read-only. DuckDB attaches it with no credentials (the proxy adds
// them) and, unless the connection has a storage credential of its own,
// asks for the storage credentials the catalog hands out with each table.
// A storage credential of the connection's own goes into a secret scoped
// to the locations of the tables found, which the sandbox allows anyway.
func icebergAttachStatements(c Connection, p *icebergProxy) ([]string, error) {
	var stmts []string
	delegation := "vended_credentials"
	if c.Iceberg.staticStorage() {
		delegation = "none"
		cred, err := p.secretValue(c, c.Iceberg.StorageCredential, c.Iceberg.StorageCredentialSecret, "storage_credential")
		if err != nil {
			return nil, err
		}
		id, key, err := splitStorageCredential(cred)
		if err != nil {
			return nil, err
		}
		tokens.Store(key, true)
		if roots := p.roots(); len(roots) > 0 {
			opts := []string{"TYPE s3", "KEY_ID " + literal(id), "SECRET " + literal(key)}
			region := c.Iceberg.Region
			if region == "" {
				region = "us-east-1"
			}
			opts = append(opts, "REGION "+literal(region))
			if c.Iceberg.StorageEndpoint != "" {
				hostport, useSSL, _ := parseStorageEndpoint(c.Iceberg.StorageEndpoint)
				style := "path"
				if c.Iceberg.StorageURLStyle == "vhost" {
					style = "vhost"
				}
				opts = append(opts, "ENDPOINT "+literal(hostport), fmt.Sprintf("USE_SSL %t", useSSL), "URL_STYLE "+literal(style))
			}
			quoted := make([]string, len(roots))
			for i, r := range roots {
				quoted[i] = literal(r)
			}
			opts = append(opts, "SCOPE ["+strings.Join(quoted, ", ")+"]")
			stmts = append(stmts, "CREATE OR REPLACE SECRET "+ident("igloo_connection_"+c.Name)+" ("+strings.Join(opts, ", ")+")")
		}
	}
	warehouse := c.Iceberg.Warehouse
	if warehouse == "" {
		warehouse = c.Name
	}
	stmts = append(stmts,
		fmt.Sprintf("ATTACH %s AS %s (TYPE iceberg, ENDPOINT %s, AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE %s, READ_ONLY, "+
			"SUPPORT_NESTED_NAMESPACES false, PURGE_REQUESTED false, REMOVE_FILES_ON_DELETE false)",
			literal(warehouse), ident(c.Name), literal(p.url()), literal(delegation)),
		"SELECT count(*) FROM duckdb_tables() WHERE database_name = "+literal(c.Name),
	)
	return stmts, nil
}

// mysqlDSNValue quotes a value for the MySQL extension's key=value
// connection string, which escapes \ and " with a backslash.
func mysqlDSNValue(s string) string {
	return `"` + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `"`, `\"`) + `"`
}

// signIn checks that Igloo can sign in to c's database, without DuckDB.
// For an Iceberg REST catalog it reads its configuration and namespaces.
func (m *Manager) signIn(ctx context.Context, c Connection) error {
	switch c.Type {
	case Snowflake:
		cl := openSnowflake(c, m.secrets)
		defer cl.close()
		return cl.check(ctx)
	case Iceberg:
		ip, err := newIcebergProxy(c, m.secrets, m.locs, m.log)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(ctx, discoveryTimeout)
		defer cancel()
		return ip.check(ctx)
	}
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
			if err := m.sweep(ctx, time.Now()); err != nil {
				m.log.Warn("could not drop expired storage credentials", "err", Scrub(err.Error()))
			}
		}
	}
}

// vendedLifetime is how long DuckDB keeps storage credentials an Iceberg
// REST catalog handed out. Catalogs hand them out for an hour, and DuckDB
// renews a set that expires during a read under the same name.
const vendedLifetime = 2 * time.Hour

// sweep drops storage credentials catalogs handed out that are older than
// vendedLifetime. DuckDB's iceberg extension (1.5.6) keeps every set it is
// handed, one or more for each read of a table, as a temporary secret and
// never drops it, so without this they pile up, and every read of object
// storage, the warehouse's too, looks through them all for its own. A
// statement still reading a table after vendedLifetime can fail.
func (m *Manager) sweep(ctx context.Context, now time.Time) error {
	m.mu.Lock()
	iceberg := false
	for _, a := range m.attached {
		iceberg = iceberg || a.conn.Type == Iceberg
	}
	m.mu.Unlock()
	if !iceberg && len(m.vended) == 0 {
		return nil
	}
	return m.withConn(ctx, func(conn *duckdb.Conn) error {
		rows, err := queryRows(ctx, conn, `SELECT name FROM duckdb_secrets() WHERE NOT persistent AND starts_with(name, '__internal_ic_')`)
		if err != nil {
			return err
		}
		seen := map[string]bool{}
		var errs []error
		for _, r := range rows {
			name, _ := r[0].(string)
			seen[name] = true
			first, ok := m.vended[name]
			switch {
			case !ok:
				m.vended[name] = now
			case now.Sub(first) >= vendedLifetime:
				if err := exec(ctx, conn, "DROP TEMPORARY SECRET IF EXISTS "+ident(name)); err != nil {
					errs = append(errs, err)
					continue
				}
				delete(m.vended, name)
			}
		}
		for name := range m.vended {
			if !seen[name] {
				delete(m.vended, name)
			}
		}
		return errors.Join(errs...)
	})
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
			a.ep.update(c)
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
	a.ep.close()
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
		a.ep.close()
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
	if err := m.signIn(ctx, c); err != nil {
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
	if err := m.signIn(ctx, c); err != nil {
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
	if p, ok := reachesProtected(ctx, m.protected, c.ServerHost(), c.Port); ok {
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
// the lists of every attached connection of the same extension are read
// again; to keep one that is down from losing its list, Refresh first
// checks that all of them can be reached. Snowflake connections, attached
// with the postgres extension, are listed by their gateways from what they
// read of Snowflake: a refreshed one reads Snowflake again first (keeping
// its list if it cannot), the others list what they have.
func (m *Manager) Refresh(ctx context.Context, name string) error {
	m.mu.Lock()
	a := m.attached[name]
	var same []Connection
	for _, other := range m.attached {
		if a != nil && other.conn.Type.extension() == a.conn.Type.extension() {
			same = append(same, other.ep.settings())
		}
	}
	m.mu.Unlock()
	if a == nil {
		if _, err := m.get(ctx, name); err != nil {
			return err
		}
		return fmt.Errorf("connection %s is not attached on this replica; it is attached when Igloo restarts", name)
	}
	if a.conn.Type == Iceberg {
		// Its tables' locations went into DuckDB's sandbox at startup.
		return &InvalidError{fmt.Errorf("connection %s attaches the tables its catalog had when this replica started, "+
			"whose locations DuckDB's sandbox was then opened to; restart the replica to attach tables created since", name)}
	}
	for _, c := range same {
		if c.Type == Snowflake {
			continue
		}
		if err := m.signIn(ctx, c); err != nil {
			return &UnreachableError{c.Name, fmt.Errorf("refreshing would drop the list of its tables while it is down: %w", err)}
		}
	}
	if g, ok := a.ep.(*sfGateway); ok {
		if err := g.refresh(ctx); err != nil {
			return &UnreachableError{name, err}
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
		case a != nil && a.ep.attachedConn().attachedFields() != c.attachedFields():
			s.State, s.Detail = "attached", "restart to apply the new database or schema"
			if c.Type == Iceberg {
				s.Detail = "restart to apply the new endpoint, warehouse, schema or storage settings"
			}
		case a != nil:
			s.State = "attached"
			if ip, ok := a.ep.(*icebergProxy); ok {
				s.Detail = ip.summary()
				if err := a.ep.lastError(); err != "" {
					s.Detail += "; the last request to the catalog failed: " + err
				}
			} else if err := a.ep.lastError(); err != "" {
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

// queryRows runs a query and returns its rows.
func queryRows(ctx context.Context, conn *duckdb.Conn, query string) ([][]driver.Value, error) {
	rows, err := conn.QueryContext(ctx, query, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var out [][]driver.Value
	for {
		dest := make([]driver.Value, len(rows.Columns()))
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}
		out = append(out, dest)
	}
}

func literal(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func ident(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
