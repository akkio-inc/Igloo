// Package config loads Igloo's runtime settings from IGLOO_* environment
// variables. docs/configuration.md documents every variable.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Config is everything the igloo server needs to start.
type Config struct {
	// ListenAddr is where the Postgres-protocol endpoint listens.
	ListenAddr string
	// HTTPAddr serves health checks and metrics. Empty disables it.
	HTTPAddr string
	// ConsoleAddr serves the web console. Empty disables it.
	ConsoleAddr string
	// FlightAddr serves Arrow Flight SQL. Empty disables it.
	FlightAddr string
	// IcebergAddr serves the Iceberg REST catalog to other engines. Empty
	// disables it.
	IcebergAddr string
	// TLSCertFile and TLSKeyFile enable TLS for SQL clients.
	TLSCertFile string
	TLSKeyFile  string
	// TLSRequired refuses unencrypted sessions when TLS is configured.
	TLSRequired bool

	LogFormat string
	LogLevel  slog.Level

	// AdminUser and AdminPassword define the first administrator.
	AdminUser     string
	AdminPassword string
	// MetadataDatabaseURL is the Postgres database that stores users. Empty
	// means the administrator is the only account.
	MetadataDatabaseURL string

	// StatementTimeout cancels statements that run longer, unless a session
	// sets its own statement_timeout. Zero means no limit.
	StatementTimeout time.Duration
	// MaxConnections caps concurrent client sessions; each holds DuckDB
	// resources.
	MaxConnections int
	// ShutdownDelay is how long Igloo keeps accepting connections, while
	// reporting not ready, after it is asked to stop. It gives load
	// balancers time to stop sending new connections.
	ShutdownDelay time.Duration
	// Pool names the compute pool this replica belongs to: replicas that
	// share a catalog and metadata database but have their own machines,
	// so a client or team gets its own compute and bill.
	Pool string
	// PricePerHour is what this replica's machine costs an hour, for the
	// cost estimates of igloo.usage. Zero means unknown.
	PricePerHour float64

	DuckDB      DuckDB
	Cache       Cache
	Catalog     Catalog
	Storage     Storage
	Maintenance Maintenance
	Audit       Audit
	// OIDC configures single sign-on to the web console (see sso.go).
	OIDC OIDC
	// TokenMaxLifetime caps how long an access token may last.
	TokenMaxLifetime time.Duration
}

// Audit controls the query and login history.
type Audit struct {
	Enabled bool
	// Retention is how long history is kept.
	Retention time.Duration
}

// Cache keeps local copies of the table files Igloo reads from object
// storage, so queries after a restart read them from disk instead of
// fetching them again.
type Cache struct {
	// Dir holds the cached files. Empty turns the cache off.
	Dir string
	// MaxSize caps the cache in bytes. Zero sizes it from the free space on
	// Dir's disk.
	MaxSize int64
}

// Maintenance controls automatic table maintenance: compaction, snapshot
// expiry and orphan file cleanup.
type Maintenance struct {
	Enabled bool
	// Interval is the time between passes over every table.
	Interval time.Duration
	// Retention is how long snapshots, and so time travel, are kept for
	// tables that do not set history.expire.max-snapshot-age-ms.
	Retention time.Duration
}

// DuckDB tunes the embedded query engine.
type DuckDB struct {
	// Path is the DuckDB database file. Empty means in-memory; tables live in
	// the Iceberg catalog, so local state is only temp tables and settings.
	Path string
	// TempDir is where DuckDB spills when a query exceeds MemoryLimit.
	TempDir string
	// MemoryLimit is a DuckDB size string such as "8GB". Empty keeps DuckDB's
	// default of 80% of system memory.
	MemoryLimit string
	// MaxTempSize caps spill files, e.g. "45GiB". Empty keeps DuckDB's
	// default, 90% of the free space on the spill disk.
	MaxTempSize string
	// Threads caps worker threads. Zero keeps DuckDB's default of one per core.
	Threads int
	// ExtensionDir is where pre-installed DuckDB extensions live.
	ExtensionDir string
}

// Catalog points at the Iceberg REST catalog. When URI is empty Igloo runs
// without one and only in-memory tables are available.
type Catalog struct {
	URI string
	// Warehouse is the warehouse name registered in the catalog.
	Warehouse string
	// Name is what the catalog is called in SQL, e.g. lake.public.orders.
	Name string
	// Schema is the default schema (Iceberg namespace) for unqualified names.
	// It defaults to "public", which Postgres tools expect; DuckDB cannot
	// select an Iceberg namespace named "main" as the default.
	Schema string
	// Setup bootstraps the catalog and creates the warehouse on start.
	Setup bool
}

// StorageType is the object store holding table data.
type StorageType string

const (
	StorageS3    StorageType = "s3"
	StorageGCS   StorageType = "gcs"
	StorageAzure StorageType = "azure"
)

// Storage says where table data lives and how to reach it.
type Storage struct {
	// Location is the warehouse root, e.g. s3://bucket/warehouse.
	Location string
	Type     StorageType
	// Bucket is the S3 or GCS bucket, or the ADLS filesystem.
	Bucket string
	// Prefix is the path inside Bucket, without leading or trailing slashes.
	Prefix string
	// AllowedLocations are extra URL prefixes users may read and write.
	AllowedLocations []string

	S3    S3
	GCS   GCS
	Azure Azure
}

// S3 configures Amazon S3 and S3-compatible stores. Leaving AccessKeyID empty
// uses the AWS credential chain (IAM roles for service accounts, instance
// profiles, environment variables).
type S3 struct {
	Endpoint        string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	// URLStyle is "path" for most self-hosted stores, "vhost" for AWS.
	URLStyle string
	UseSSL   bool
	// STSRoleARN is the IAM role Igloo assumes to hand other engines
	// credentials limited to one table. Required on AWS for that;
	// S3-compatible stores scope Igloo's own keys instead.
	STSRoleARN string
}

// GCS configures Google Cloud Storage. DuckDB reaches GCS through its
// S3-compatible API, which needs an HMAC key.
type GCS struct {
	HMACKeyID  string
	HMACSecret string
}

// Azure configures ADLS Gen2. Leaving AccountKey empty uses the Azure
// credential chain (workload identity, managed identity).
type Azure struct {
	Account    string
	AccountKey string
	// Host is the DFS endpoint, normally <account>.dfs.core.windows.net.
	Host string
}

// Load reads the configuration using lookup, normally os.LookupEnv.
func Load(lookup func(string) (string, bool)) (Config, error) {
	getenv := func(key string) string {
		v, _ := lookup(key)
		return v
	}
	get := func(key, fallback string) string {
		if v := strings.TrimSpace(getenv(key)); v != "" {
			return v
		}
		return fallback
	}
	// HTTP can be switched off by setting the variable to an empty string.
	httpAddr, ok := lookup("IGLOO_HTTP_ADDR")
	if !ok {
		httpAddr = ":8080"
	}
	consoleAddr, ok := lookup("IGLOO_CONSOLE_ADDR")
	if !ok {
		consoleAddr = ":8443"
	}
	flightAddr, ok := lookup("IGLOO_FLIGHT_ADDR")
	if !ok {
		flightAddr = ":31337"
	}
	icebergAddr, ok := lookup("IGLOO_ICEBERG_ADDR")
	if !ok {
		icebergAddr = ":8181"
	}

	cfg := Config{
		ListenAddr:          get("IGLOO_LISTEN_ADDR", ":5432"),
		HTTPAddr:            strings.TrimSpace(httpAddr),
		ConsoleAddr:         strings.TrimSpace(consoleAddr),
		FlightAddr:          strings.TrimSpace(flightAddr),
		IcebergAddr:         strings.TrimSpace(icebergAddr),
		TLSCertFile:         get("IGLOO_TLS_CERT_FILE", ""),
		TLSKeyFile:          get("IGLOO_TLS_KEY_FILE", ""),
		LogFormat:           get("IGLOO_LOG_FORMAT", "text"),
		AdminUser:           get("IGLOO_ADMIN_USER", "igloo"),
		AdminPassword:       getenv("IGLOO_ADMIN_PASSWORD"),
		MetadataDatabaseURL: get("IGLOO_METADATA_DATABASE_URL", ""),
		DuckDB: DuckDB{
			Path:         get("IGLOO_DUCKDB_PATH", ""),
			TempDir:      get("IGLOO_DUCKDB_TEMP_DIR", ""),
			MemoryLimit:  get("IGLOO_DUCKDB_MEMORY_LIMIT", ""),
			MaxTempSize:  get("IGLOO_DUCKDB_MAX_TEMP_SIZE", ""),
			ExtensionDir: get("IGLOO_DUCKDB_EXTENSION_DIR", ""),
		},
		Cache: Cache{
			Dir: get("IGLOO_CACHE_DIR", ""),
		},
		Catalog: Catalog{
			URI:       get("IGLOO_CATALOG_URI", ""),
			Warehouse: get("IGLOO_CATALOG_WAREHOUSE", "igloo"),
			Name:      get("IGLOO_CATALOG_NAME", "lake"),
			Schema:    get("IGLOO_CATALOG_SCHEMA", "public"),
		},
		Storage: Storage{
			Location: get("IGLOO_WAREHOUSE_LOCATION", ""),
			S3: S3{
				Endpoint:        get("IGLOO_S3_ENDPOINT", ""),
				Region:          get("IGLOO_S3_REGION", "us-east-1"),
				AccessKeyID:     get("IGLOO_S3_ACCESS_KEY_ID", ""),
				SecretAccessKey: getenv("IGLOO_S3_SECRET_ACCESS_KEY"),
				URLStyle:        get("IGLOO_S3_URL_STYLE", "vhost"),
				STSRoleARN:      get("IGLOO_S3_STS_ROLE_ARN", ""),
			},
			GCS: GCS{
				HMACKeyID:  get("IGLOO_GCS_HMAC_KEY_ID", ""),
				HMACSecret: getenv("IGLOO_GCS_HMAC_SECRET"),
			},
			Azure: Azure{
				AccountKey: getenv("IGLOO_AZURE_ACCOUNT_KEY"),
			},
		},
	}

	var errs []error
	addErr := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if len(cfg.AdminPassword) < 8 {
		addErr("IGLOO_ADMIN_PASSWORD must be set to at least 8 characters")
	}
	if !ValidName(cfg.AdminUser) {
		addErr("IGLOO_ADMIN_USER %q must be a lowercase name of letters, digits and underscores", cfg.AdminUser)
	}

	if (cfg.TLSCertFile == "") != (cfg.TLSKeyFile == "") {
		addErr("set both IGLOO_TLS_CERT_FILE and IGLOO_TLS_KEY_FILE, or neither")
	}
	_, requiredSet := lookup("IGLOO_TLS_REQUIRED")
	required, err := strconv.ParseBool(get("IGLOO_TLS_REQUIRED", "true"))
	switch {
	case err != nil:
		addErr("IGLOO_TLS_REQUIRED must be true or false: %w", err)
	case required && requiredSet && cfg.TLSCertFile == "":
		addErr("IGLOO_TLS_REQUIRED=true needs IGLOO_TLS_CERT_FILE and IGLOO_TLS_KEY_FILE")
	}
	cfg.TLSRequired = required && cfg.TLSCertFile != ""

	if cfg.LogFormat != "text" && cfg.LogFormat != "json" {
		addErr("IGLOO_LOG_FORMAT must be text or json, got %q", cfg.LogFormat)
	}
	if err := cfg.LogLevel.UnmarshalText([]byte(get("IGLOO_LOG_LEVEL", "info"))); err != nil {
		addErr("IGLOO_LOG_LEVEL: %w", err)
	}

	if v := get("IGLOO_STATEMENT_TIMEOUT", "0"); v != "0" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			addErr("IGLOO_STATEMENT_TIMEOUT must be a duration such as 30m or 2h, got %q", v)
		}
		cfg.StatementTimeout = d
	}

	if v := get("IGLOO_SHUTDOWN_DELAY", "0"); v != "0" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			addErr("IGLOO_SHUTDOWN_DELAY must be a duration such as 5s, got %q", v)
		}
		cfg.ShutdownDelay = d
	}

	cfg.Pool = get("IGLOO_POOL", "default")
	if !validPoolName(cfg.Pool) {
		addErr("IGLOO_POOL %q must be a name of up to 63 lowercase letters, digits, hyphens and underscores", cfg.Pool)
	}
	if v := get("IGLOO_PRICE_PER_HOUR", "0"); v != "0" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f < 0 || math.IsInf(f, 0) || math.IsNaN(f) {
			addErr("IGLOO_PRICE_PER_HOUR must be a price such as 0.58, or 0 for unknown, got %q", v)
		}
		cfg.PricePerHour = f
	}

	cfg.MaxConnections = 100
	if v := get("IGLOO_MAX_CONNECTIONS", ""); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			addErr("IGLOO_MAX_CONNECTIONS must be a positive integer, got %q", v)
		}
		cfg.MaxConnections = n
	}

	if v := get("IGLOO_DUCKDB_THREADS", ""); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			addErr("IGLOO_DUCKDB_THREADS must be a positive integer, got %q", v)
		}
		cfg.DuckDB.Threads = n
	}

	switch v := get("IGLOO_CACHE_SIZE", ""); v {
	case "":
	case "0":
		cfg.Cache.Dir = ""
	default:
		n, err := ParseSize(v)
		if err != nil || n <= 0 {
			addErr("IGLOO_CACHE_SIZE must be a size such as 50GB or 40GiB, or 0 to turn the cache off, got %q", v)
		}
		cfg.Cache.MaxSize = n
	}
	if d := cfg.Cache.Dir; d != "" && !strings.HasPrefix(d, "/") {
		addErr("IGLOO_CACHE_DIR must be an absolute path, got %q", d)
	}

	enabled, err := strconv.ParseBool(get("IGLOO_MAINTENANCE", "true"))
	if err != nil {
		addErr("IGLOO_MAINTENANCE must be true or false: %w", err)
	}
	cfg.Maintenance.Enabled = enabled
	if d, err := parseDays(get("IGLOO_MAINTENANCE_INTERVAL", "1h")); err != nil || d <= 0 {
		addErr("IGLOO_MAINTENANCE_INTERVAL must be a positive duration such as 1h, got %q", get("IGLOO_MAINTENANCE_INTERVAL", ""))
	} else {
		cfg.Maintenance.Interval = d
	}
	if d, err := parseDays(get("IGLOO_SNAPSHOT_RETENTION", "7d")); err != nil || d < 0 {
		addErr("IGLOO_SNAPSHOT_RETENTION must be a duration such as 7d or 36h, got %q", get("IGLOO_SNAPSHOT_RETENTION", ""))
	} else {
		cfg.Maintenance.Retention = d
	}

	audit, err := strconv.ParseBool(get("IGLOO_AUDIT", "true"))
	if err != nil {
		addErr("IGLOO_AUDIT must be true or false: %w", err)
	}
	cfg.Audit.Enabled = audit
	if d, err := parseDays(get("IGLOO_AUDIT_RETENTION", "90d")); err != nil || d <= 0 {
		addErr("IGLOO_AUDIT_RETENTION must be a positive duration such as 90d or 720h, got %q", get("IGLOO_AUDIT_RETENTION", ""))
	} else {
		cfg.Audit.Retention = d
	}

	setup, err := strconv.ParseBool(get("IGLOO_CATALOG_SETUP", "true"))
	if err != nil {
		addErr("IGLOO_CATALOG_SETUP must be true or false: %w", err)
	}
	cfg.Catalog.Setup = setup

	useSSL, err := strconv.ParseBool(get("IGLOO_S3_USE_SSL", "true"))
	if err != nil {
		addErr("IGLOO_S3_USE_SSL must be true or false: %w", err)
	}
	cfg.Storage.S3.UseSSL = useSSL

	if s := cfg.Storage.S3.URLStyle; s != "path" && s != "vhost" {
		addErr("IGLOO_S3_URL_STYLE must be path or vhost, got %q", s)
	}
	if cfg.Storage.S3.AccessKeyID != "" && cfg.Storage.S3.SecretAccessKey == "" {
		addErr("IGLOO_S3_SECRET_ACCESS_KEY must be set when IGLOO_S3_ACCESS_KEY_ID is")
	}
	if arn := cfg.Storage.S3.STSRoleARN; arn != "" && !roleARN.MatchString(arn) {
		addErr("IGLOO_S3_STS_ROLE_ARN must be an IAM role ARN such as arn:aws:iam::123456789012:role/igloo-engines, got %q", arn)
	}

	for _, loc := range strings.Split(get("IGLOO_ALLOWED_LOCATIONS", ""), ",") {
		if loc = strings.TrimSpace(loc); loc == "" {
			continue
		}
		if _, err := url.Parse(loc); err != nil || !strings.Contains(loc, "://") {
			addErr("IGLOO_ALLOWED_LOCATIONS entry %q must be a URL such as s3://bucket/path/", loc)
			continue
		}
		cfg.Storage.AllowedLocations = append(cfg.Storage.AllowedLocations, loc)
	}

	if cfg.Catalog.URI != "" {
		if _, err := url.ParseRequestURI(cfg.Catalog.URI); err != nil {
			addErr("IGLOO_CATALOG_URI: %w", err)
		}
		if cfg.Storage.Location == "" {
			addErr("IGLOO_WAREHOUSE_LOCATION must be set when IGLOO_CATALOG_URI is")
		} else if err := cfg.Storage.parseLocation(); err != nil {
			addErr("IGLOO_WAREHOUSE_LOCATION: %w", err)
		}
	}

	loadOIDC(&cfg, lookup, addErr)

	return cfg, errors.Join(errs...)
}

// roleARN matches an IAM role ARN in any AWS partition.
var roleARN = regexp.MustCompile(`^arn:aws[a-z-]*:iam::[0-9]{12}:role/[A-Za-z0-9+=,.@_/-]+$`)

// parseDays parses a Go duration, also accepting a whole number of days such
// as "7d".
func parseDays(s string) (time.Duration, error) {
	if days, ok := strings.CutSuffix(s, "d"); ok {
		n, err := strconv.Atoi(days)
		if err != nil {
			return 0, err
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}

// ParseSize parses a number of bytes such as 50GB, 40 GiB or 1073741824.
// As in DuckDB, KB, MB, GB and TB are powers of 1000, and KiB, MiB, GiB and
// TiB powers of 1024.
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	i := strings.IndexFunc(s, func(r rune) bool { return (r < '0' || r > '9') && r != '.' })
	if i < 0 {
		i = len(s)
	}
	n, err := strconv.ParseFloat(s[:i], 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	units := map[string]float64{
		"": 1, "b": 1, "bytes": 1,
		"kb": 1e3, "mb": 1e6, "gb": 1e9, "tb": 1e12,
		"kib": 1 << 10, "mib": 1 << 20, "gib": 1 << 30, "tib": 1 << 40,
	}
	unit, ok := units[strings.ToLower(strings.TrimSpace(s[i:]))]
	if !ok || n*unit > float64(1<<62) {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return int64(n * unit), nil
}

// parseLocation fills Type, Bucket, Prefix and the Azure account from
// Location.
func (s *Storage) parseLocation() error {
	u, err := url.Parse(s.Location)
	if err != nil {
		return err
	}
	if u.Host == "" {
		return fmt.Errorf("%q has no bucket", s.Location)
	}
	s.Prefix = strings.Trim(u.Path, "/")

	switch u.Scheme {
	case "s3":
		s.Type, s.Bucket = StorageS3, u.Host
	case "gs":
		s.Type, s.Bucket = StorageGCS, u.Host
		if s.GCS.HMACKeyID == "" || s.GCS.HMACSecret == "" {
			return errors.New("gs:// locations need IGLOO_GCS_HMAC_KEY_ID and IGLOO_GCS_HMAC_SECRET")
		}
	case "abfss":
		// abfss://<filesystem>@<account>.dfs.core.windows.net/<prefix>
		account, _, ok := strings.Cut(u.Hostname(), ".")
		if u.User == nil || u.User.Username() == "" || !ok {
			return fmt.Errorf("%q must look like abfss://filesystem@account.dfs.core.windows.net/path", s.Location)
		}
		s.Type, s.Bucket = StorageAzure, u.User.Username()
		s.Azure.Account, s.Azure.Host = account, u.Hostname()
	default:
		return fmt.Errorf("unsupported scheme %q; use s3://, gs:// or abfss://", u.Scheme)
	}
	return nil
}

// Root is the warehouse location with a trailing slash, for prefix checks.
func (s Storage) Root() string {
	return strings.TrimSuffix(s.Location, "/") + "/"
}

// validPoolName reports whether name can name a compute pool: also a
// Kubernetes name, so it may contain hyphens but not start with one.
func validPoolName(name string) bool {
	if name == "" || len(name) > 63 || name[0] == '-' {
		return false
	}
	for _, r := range name {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

// ValidName reports whether name is a valid user name: a safe, unquoted,
// lowercase SQL identifier.
func ValidName(name string) bool {
	if name == "" || len(name) > 63 {
		return false
	}
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r == '_':
		case r >= '0' && r <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}
