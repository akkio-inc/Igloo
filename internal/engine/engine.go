// Package engine runs SQL on an embedded DuckDB that is attached to the
// Iceberg catalog. Each client session gets its own DuckDB connection; the
// catalog attachment and storage credentials are shared by all of them.
package engine

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/policy"
)

// attachRetryInterval is how long Open waits between attempts to reach the
// catalog. In a fresh deployment the catalog often starts after Igloo.
const attachRetryInterval = 2 * time.Second

// Extensions are the DuckDB extensions Igloo uses: object storage (httpfs,
// aws, azure), Iceberg (which reads manifests with avro), TPC-H data for
// demos and benchmarks, the read cache (cache_httpfs, see cache.go), and
// geospatial functions and sketches (see media.go). The image installs them
// at build time.
var Extensions = append([]string{"httpfs", "aws", "azure", "iceberg", "avro", "tpch", "cache_httpfs"}, mediaExtensions...)

// communityExtensions come from DuckDB's community repository rather than
// the core one. DuckDB checks their signatures on load just the same.
var communityExtensions = map[string]bool{"cache_httpfs": true, "h3": true, "datasketches": true}

// Engine owns the embedded DuckDB database.
type Engine struct {
	connector *duckdb.Connector
	parser    *parser
	// catalog is the attached Iceberg catalog's name in SQL, if any.
	catalog string
	// local is DuckDB's own database, where Igloo keeps its copies of the
	// catalog's views: DuckDB cannot keep views in an Iceberg catalog.
	local string
	// connectStmts run on every new connection so unqualified names
	// resolve to the catalog's default schema, and to the views kept for
	// it in the local database.
	connectStmts []string
	// comments supplies igloo_comments(); see SetComments.
	comments atomic.Pointer[func() []catalog.Comment]
	// cache is the on-disk read cache, or nil when it is off.
	cache *readCache
	// history supplies the history functions; see SetHistory.
	history atomic.Pointer[historyConfig]
	// jobHistory supplies the job history functions; see SetJobHistory.
	jobHistory atomic.Pointer[jobHistoryConfig]
	// locations are where COPY INTO may load from; see Locations.
	locations []string
	// policies supplies the policy reference functions; see SetPolicies.
	policies atomic.Pointer[func() *policy.Snapshot]
}

// Open starts DuckDB and, when a catalog is configured, attaches it. It keeps
// retrying the attach until ctx is done. Once set up, DuckDB is locked down
// so SQL cannot reach files or URLs outside the warehouse.
func Open(ctx context.Context, cfg config.Config, log *slog.Logger) (*Engine, error) {
	connector, err := duckdb.NewConnector(cfg.DuckDB.Path, nil)
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	p, err := newParser()
	if err != nil {
		return nil, errors.Join(err, connector.Close())
	}

	e := &Engine{connector: connector, parser: p}
	if err := e.setup(ctx, cfg, log); err != nil {
		p.close()
		return nil, errors.Join(err, connector.Close())
	}
	if e.cache != nil {
		e.startCache(log)
	}

	return e, nil
}

// CountStatements returns how many statements query holds, without running
// any of them.
func (e *Engine) CountStatements(query string) (int, error) {
	return e.parser.count(query)
}

// Connect opens a connection for one client session.
func (e *Engine) Connect(ctx context.Context) (*duckdb.Conn, error) {
	c, err := e.connector.Connect(ctx)
	if err != nil {
		return nil, err
	}

	conn := c.(*duckdb.Conn)
	for _, stmt := range e.connectStmts {
		if _, err := conn.ExecContext(ctx, stmt, nil); err != nil {
			return nil, errors.Join(fmt.Errorf("select default schema: %w", err), conn.Close())
		}
	}

	return conn, nil
}

// Catalog is the Iceberg catalog's name in SQL, or "" without one.
func (e *Engine) Catalog() string { return e.catalog }

// LocalDatabase is the name of DuckDB's own database.
func (e *Engine) LocalDatabase() string { return e.local }

// LoadSchemas makes the catalog views (information_schema, pg_catalog,
// duckdb_columns) show the real columns of every Iceberg table. DuckDB loads
// an Iceberg table's schema the first time the table is used and until then
// lists a placeholder column named "__"; tools that browse schemas would
// see only that. Loaded schemas are shared by all sessions, so this costs
// one catalog request per table not yet used.
func (e *Engine) LoadSchemas(ctx context.Context, conn *duckdb.Conn) error {
	if e.catalog == "" {
		return nil
	}
	rows, err := conn.QueryContext(ctx, `SELECT DISTINCT schema_name, table_name FROM duckdb_columns()
		WHERE database_name = $1 AND column_name = '__' AND data_type = 'UNKNOWN'`,
		[]driver.NamedValue{{Ordinal: 1, Value: e.catalog}})
	if err != nil {
		return err
	}
	var tables [][2]string
	dest := make([]driver.Value, 2)
	for {
		if err := rows.Next(dest); err != nil {
			if !errors.Is(err, io.EOF) {
				rows.Close() //nolint:errcheck
				return err
			}
			break
		}
		tables = append(tables, [2]string{dest[0].(string), dest[1].(string)})
	}
	if err := rows.Close(); err != nil {
		return err
	}

	var errs []error
	for _, t := range tables {
		stmt := "DESCRIBE " + ident(e.catalog) + "." + ident(t[0]) + "." + ident(t[1])
		if _, err := conn.ExecContext(ctx, stmt, nil); err != nil {
			errs = append(errs, fmt.Errorf("load schema of %s.%s: %w", t[0], t[1], err))
		}
	}
	return errors.Join(errs...)
}

// Ping checks that DuckDB can run a query.
func (e *Engine) Ping(ctx context.Context) error {
	c, err := e.connector.Connect(ctx)
	if err != nil {
		return err
	}
	defer c.Close() //nolint:errcheck
	_, err = c.(*duckdb.Conn).ExecContext(ctx, "SELECT 1", nil)
	return err
}

// Close shuts DuckDB down. Sessions must be closed first.
func (e *Engine) Close() error {
	e.stopCache()
	e.parser.close()
	return e.connector.Close()
}

func (e *Engine) setup(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	c, err := e.connector.Connect(ctx)
	if err != nil {
		return fmt.Errorf("connect to duckdb: %w", err)
	}
	defer c.Close() //nolint:errcheck

	conn := c.(*duckdb.Conn)
	exec := func(stmt string) error {
		_, err := conn.ExecContext(ctx, stmt, nil)
		return err
	}

	for _, stmt := range settingStatements(cfg.DuckDB) {
		if err := exec(stmt); err != nil {
			return fmt.Errorf("configure duckdb (%s): %w", stmt, err)
		}
	}
	if e.local, err = queryString(ctx, conn, "SELECT current_database()"); err != nil {
		return fmt.Errorf("read database name: %w", err)
	}
	if err := e.registerComments(ctx); err != nil {
		return fmt.Errorf("register igloo_comments: %w", err)
	}
	if err := e.registerHistory(ctx); err != nil {
		return fmt.Errorf("register query history: %w", err)
	}
	if err := e.registerJobHistory(ctx); err != nil {
		return fmt.Errorf("register task and load history: %w", err)
	}
	e.locations = loadLocations(cfg)
	if err := e.registerPolicies(ctx); err != nil {
		return fmt.Errorf("register policy references: %w", err)
	}

	if cfg.Catalog.URI == "" {
		log.Warn("IGLOO_CATALOG_URI is not set; tables will not persist")
		// Load the image's extensions while files can still be read.
		if cfg.DuckDB.ExtensionDir != "" {
			for _, stmt := range extensionStatements() {
				if err := exec(stmt); err != nil {
					return fmt.Errorf("load extensions: %w", err)
				}
			}
		}
		return e.lockDown(exec, cfg)
	}

	for _, stmt := range storageStatements(cfg.Storage) {
		if err := exec(stmt); err != nil {
			// Never echo the statement: it can contain a secret key.
			return fmt.Errorf("configure object storage: %w", err)
		}
	}
	if err := e.setupCache(exec, cfg, log); err != nil {
		return err
	}

	attach := attachStatement(cfg.Catalog)
	for {
		err := exec(attach)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return fmt.Errorf("attach catalog %s: %w", cfg.Catalog.URI, err)
		}
		log.Warn("catalog not reachable yet, retrying", "uri", cfg.Catalog.URI, "err", err)
		select {
		case <-ctx.Done():
		case <-time.After(attachRetryInterval):
		}
	}

	// Replicas starting together race to create the schema; the catalog
	// rejects all but one, and a second attempt then finds it exists.
	schema := ident(cfg.Catalog.Name) + "." + ident(cfg.Catalog.Schema)
	for attempt := 1; ; attempt++ {
		err := exec("CREATE SCHEMA IF NOT EXISTS " + schema)
		if err == nil {
			break
		}
		if attempt == 3 || ctx.Err() != nil {
			return fmt.Errorf("create default schema %s: %w", schema, err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(attempt) * time.Second):
		}
	}
	views := ident(e.local) + "." + ident(cfg.Catalog.Schema)
	if err := exec("CREATE SCHEMA IF NOT EXISTS " + views); err != nil {
		return fmt.Errorf("create schema %s for views: %w", views, err)
	}
	e.connectStmts = []string{
		"USE " + schema,
		"SET SESSION search_path = " + literal(schema+","+views),
	}
	e.catalog = cfg.Catalog.Name

	if err := e.lockDown(exec, cfg); err != nil {
		return err
	}

	log.Info("attached catalog", "uri", cfg.Catalog.URI, "warehouse", cfg.Catalog.Warehouse,
		"location", cfg.Storage.Location, "as", cfg.Catalog.Name)
	return nil
}

// queryString runs a query that returns one string.
func queryString(ctx context.Context, conn *duckdb.Conn, query string) (string, error) {
	rows, err := conn.QueryContext(ctx, query, nil)
	if err != nil {
		return "", err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		return "", err
	}
	v, _ := dest[0].(string)
	return v, nil
}

func (e *Engine) lockDown(exec func(string) error, cfg config.Config) error {
	// DuckDB sets up its secret manager on first use and cannot once
	// external access is off, so make sure it is set up now.
	if err := exec("SELECT count(*) FROM duckdb_secrets()"); err != nil {
		return fmt.Errorf("initialize secrets: %w", err)
	}
	cacheDir := ""
	if e.cache != nil {
		cacheDir = e.cache.dir
	}
	for _, stmt := range lockdownStatements(cfg, cacheDir) {
		if err := exec(stmt); err != nil {
			return fmt.Errorf("lock down duckdb (%s): %w", stmt, err)
		}
	}
	return nil
}

func settingStatements(d config.DuckDB) []string {
	var stmts []string
	if d.ExtensionDir != "" {
		// Use only the extensions baked into the image; never fetch code at
		// runtime.
		stmts = append(stmts,
			"SET extension_directory = "+literal(d.ExtensionDir),
			"SET autoinstall_known_extensions = false",
		)
	}
	if d.TempDir != "" {
		stmts = append(stmts, "SET temp_directory = "+literal(d.TempDir))
	}
	if d.MemoryLimit != "" {
		stmts = append(stmts, "SET memory_limit = "+literal(d.MemoryLimit))
	}
	if d.MaxTempSize != "" {
		stmts = append(stmts, "SET max_temp_directory_size = "+literal(d.MaxTempSize))
	}
	if d.Threads > 0 {
		stmts = append(stmts, fmt.Sprintf("SET threads = %d", d.Threads))
	}
	return stmts
}

// storageStatements loads every extension Igloo needs and registers the
// credentials DuckDB uses to read and write table files. Extensions must be
// loaded here: after lockdown DuckDB cannot read extension files.
func storageStatements(s config.Storage) []string {
	stmts := extensionStatements()

	switch s.Type {
	case config.StorageGCS:
		stmts = append(stmts, secret(
			"TYPE gcs",
			"KEY_ID "+literal(s.GCS.HMACKeyID),
			"SECRET "+literal(s.GCS.HMACSecret),
		))

	case config.StorageAzure:
		stmts = append(stmts, "LOAD azure")
		if s.Azure.AccountKey != "" {
			conn := fmt.Sprintf("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=%s",
				s.Azure.Account, s.Azure.AccountKey, strings.TrimPrefix(s.Azure.Host, s.Azure.Account+".dfs."))
			stmts = append(stmts, secret("TYPE azure", "CONNECTION_STRING "+literal(conn)))
		} else {
			stmts = append(stmts, secret(
				"TYPE azure",
				"PROVIDER credential_chain",
				"CHAIN 'workload_identity;managed_identity;env'",
				"ACCOUNT_NAME "+literal(s.Azure.Account),
			))
		}

	default:
		opts := []string{
			"TYPE s3",
			"REGION " + literal(s.S3.Region),
			"URL_STYLE " + literal(s.S3.URLStyle),
			fmt.Sprintf("USE_SSL %t", s.S3.UseSSL),
		}
		if s.S3.Endpoint != "" {
			opts = append(opts, "ENDPOINT "+literal(s.S3.Endpoint))
		}
		if s.S3.AccessKeyID != "" {
			opts = append(opts, "KEY_ID "+literal(s.S3.AccessKeyID), "SECRET "+literal(s.S3.SecretAccessKey))
		} else {
			// IAM roles for service accounts, instance profiles, env vars.
			// Those credentials expire (after an hour for IRSA), so DuckDB
			// must fetch new ones as they do.
			stmts = append(stmts, "LOAD aws")
			opts = append(opts, "PROVIDER credential_chain", "REFRESH auto")
		}
		stmts = append(stmts, secret(opts...))
	}

	return stmts
}

// extensionStatements load the extensions every deployment uses.
func extensionStatements() []string {
	return append([]string{"LOAD httpfs", "LOAD iceberg", "LOAD avro", "LOAD tpch"}, mediaStatements()...)
}

func secret(opts ...string) string {
	return "CREATE OR REPLACE SECRET igloo_storage (" + strings.Join(opts, ", ") + ")"
}

// attachStatement attaches the Iceberg REST catalog. Igloo uses its own
// storage credentials rather than ones vended by the catalog, so it works
// with object stores that have no STS endpoint.
func attachStatement(c config.Catalog) string {
	return fmt.Sprintf(
		"ATTACH %s AS %s (TYPE iceberg, ENDPOINT %s, AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')",
		literal(c.Warehouse), ident(c.Name), literal(c.URI),
	)
}

// lockdownStatements stop SQL from reading or writing anything but the
// warehouse, the allowed locations, the spill directory and the read cache
// (cacheDir, if any): no local files, no other buckets, no arbitrary URLs.
// DuckDB refuses to loosen these settings once external access is off, so
// they hold for every session. They apply without a catalog too, so a
// development server is no more open than a production one.
//
// The cache extension checks the same list before it touches its files, so
// the cache works only if its directory is on it. That gives SQL no more
// than it has: the cache holds copies of warehouse files, which SQL can
// read anyway, and a writer able to plant a block there (with COPY TO) can
// already overwrite the warehouse file itself.
func lockdownStatements(cfg config.Config, cacheDir string) []string {
	var dirs []string
	if cfg.Storage.Location != "" {
		dirs = append(dirs, cfg.Storage.Root())
	}
	dirs = append(dirs, cfg.Storage.AllowedLocations...)
	for _, dir := range []string{cfg.DuckDB.TempDir, cacheDir} {
		if dir != "" {
			dirs = append(dirs, strings.TrimSuffix(dir, "/")+"/")
		}
	}

	return []string{
		"SET allowed_directories = " + list(dirs),
		// CREATE TABLE AS on an Iceberg catalog makes sure a relative
		// directory named "data" exists before writing to the table's real
		// location in object storage. Allowing that exact path keeps CTAS
		// working; the image ships it as an empty directory the server
		// cannot write to.
		"SET allowed_paths = ['data']",
		"SET enable_external_access = false",
	}
}

// InstallExtensions downloads Extensions for this DuckDB version and platform
// into dir. Container builds run it so the image works offline.
func InstallExtensions(ctx context.Context, dir string) error {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return err
	}
	defer connector.Close() //nolint:errcheck

	c, err := connector.Connect(ctx)
	if err != nil {
		return err
	}
	defer c.Close() //nolint:errcheck
	conn := c.(*duckdb.Conn)

	stmts := []string{"SET extension_directory = " + literal(dir)}
	for _, ext := range Extensions {
		stmt := "INSTALL " + ext
		if communityExtensions[ext] {
			stmt += " FROM community"
		}
		stmts = append(stmts, stmt)
	}
	for _, stmt := range stmts {
		if _, err := conn.ExecContext(ctx, stmt, nil); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return nil
}

func list(items []string) string {
	quoted := make([]string, len(items))
	for i, item := range items {
		quoted[i] = literal(item)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

func literal(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func ident(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
