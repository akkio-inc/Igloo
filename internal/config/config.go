// Package config loads Igloo's runtime settings from IGLOO_* environment
// variables. docs/configuration.md documents every variable.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/url"
	"os"
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
	// SharingAddr serves the Delta Sharing protocol to recipients of
	// shares. Empty disables it.
	SharingAddr string
	// SharingURL is the sharing endpoint's public URL, ending in
	// /delta-sharing, for activation links and profile files. Empty uses
	// the address a recipient reaches it at.
	SharingURL string
	// TLSCertFile and TLSKeyFile enable TLS for SQL clients.
	TLSCertFile string
	TLSKeyFile  string
	// TLSRequired refuses unencrypted sessions when TLS is configured.
	TLSRequired bool

	LogFormat string
	LogLevel  slog.Level

	// AdminUser and AdminPassword define the first administrator. The
	// password is used only to create the account, and must then pass
	// CheckAdminPassword.
	AdminUser     string
	AdminPassword string
	// MetadataDatabaseURL is the Postgres database that stores users. Empty
	// means the administrator is the only account.
	MetadataDatabaseURL string
	// IDDomains turns on ID domains' work (IGLOO_ID_DOMAINS, off by
	// default): columns joining domains, filling the columns that hold
	// their ids, and statements counting through them.
	IDDomains bool
	// IDSyncMinRows is how many rows a write to a member table must write
	// for Igloo to give its new values ids before it returns
	// (IGLOO_ID_SYNC_MIN_ROWS, 0 by default: every write); smaller writes
	// leave them to a mapping pass shortly after.
	IDSyncMinRows int64
	// BitmapMinRows is how many rows a statement that counts ID domains'
	// IDs must read for Igloo to count them in roaring bitmaps
	// (IGLOO_BITMAP_MIN_ROWS, 10 million by default; "off", -1, never).
	BitmapMinRows int64
	// BitmapIndexes lets tables have bitmap indexes of member columns
	// (IGLOO_BITMAP_INDEXES, off by default; it needs IGLOO_ID_DOMAINS).
	BitmapIndexes bool
	// ReportRewrite has Igloo recognize the counting CTEs of audience
	// reports and answer them through the report API (IGLOO_REPORT_REWRITE,
	// off by default).
	ReportRewrite bool
	// MaxConcurrentStatements is how many queries and data changes may run
	// at once on a replica, whoever runs them; further ones wait in line
	// (IGLOO_MAX_CONCURRENT_STATEMENTS: a number, auto for as many as the
	// cores, -1 here, or off, 0, for no limit, the default).
	MaxConcurrentStatements int
	// ResultCacheSize is the memory the results Igloo keeps of queries may
	// take, to send them again to the same query while no table changes
	// (IGLOO_RESULT_CACHE_SIZE, 256 MiB by default; 0 or off keeps none).
	ResultCacheSize int64
	// ConnectionSecretsDir holds files with the passwords of connections to
	// other databases, which a connection names with password_secret.
	// Empty means connections store their passwords.
	ConnectionSecretsDir string
	// ErasureKey seals open erasure requests' identifiers and salts their
	// hashes; it is kept out of the metadata database on purpose. It is
	// IGLOO_ERASURE_KEY, or the contents of the file IGLOO_ERASURE_KEY_FILE
	// names, as a Kubernetes secret mounts one, without the spaces and line
	// breaks around it.
	ErasureKey string

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

	DuckDB DuckDB
	Cache  Cache
	// Prefetch reads ahead, into the read cache, what scans are about to
	// read (see package prefetch).
	Prefetch    Prefetch
	Catalog     Catalog
	Storage     Storage
	Maintenance Maintenance
	Audit       Audit
	OpenLineage OpenLineage
	// OIDC configures single sign-on to the web console (see sso.go).
	OIDC OIDC
	// TokenMaxLifetime caps how long an access token may last.
	TokenMaxLifetime time.Duration
	// Notifications configures alerts' destinations (see notify.go).
	Notifications Notifications
	// Workers configures distributed execution (see workers.go).
	Workers Workers
}

// Audit controls the query and login history.
type Audit struct {
	Enabled bool
	// Retention is how long history is kept.
	Retention time.Duration
}

// OpenLineage configures sending lineage events (package openlineage).
type OpenLineage struct {
	// URL is the endpoint; empty sends nothing.
	URL    string
	APIKey string
	// Namespace names Igloo's jobs and tables in events.
	Namespace string
	// AllowPrivate allows an endpoint at a private or loopback address.
	AllowPrivate bool
	// SendSQL includes statements' text in events.
	SendSQL bool
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

// Prefetch configures reading ahead into the read cache.
type Prefetch struct {
	// Enabled is IGLOO_PREFETCH, off by default.
	Enabled bool
	// Concurrency is IGLOO_PREFETCH_CONCURRENCY: requests in flight for the
	// whole process; zero means the prefetcher's default.
	Concurrency int
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
	// Memory is how many bytes compaction may hold. Zero sizes it from the
	// memory Go has (see package memlimit).
	Memory int64
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
	// SortSpillBudget is how many bytes a write of rows in order may spill
	// to sort them at once (IGLOO_SORT_SPILL_BUDGET); a larger one sorts
	// a range of the order at a time. Zero means half the spill disk's
	// free space when the write starts; negative, every write sorts at once.
	SortSpillBudget int64
	// Threads caps DuckDB's threads. Zero means several per core when the
	// warehouse is in object storage, as the memory limit allows (see
	// engine.threadsFor), and DuckDB's default of one per core otherwise.
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
	sharingAddr, ok := lookup("IGLOO_SHARING_ADDR")
	if !ok {
		sharingAddr = ":8444"
	}

	cfg := Config{
		ListenAddr:          get("IGLOO_LISTEN_ADDR", ":5432"),
		HTTPAddr:            strings.TrimSpace(httpAddr),
		ConsoleAddr:         strings.TrimSpace(consoleAddr),
		FlightAddr:          strings.TrimSpace(flightAddr),
		IcebergAddr:         strings.TrimSpace(icebergAddr),
		SharingAddr:         strings.TrimSpace(sharingAddr),
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

	cfg.ConnectionSecretsDir = get("IGLOO_CONNECTION_SECRETS_DIR", "")
	cfg.ErasureKey = strings.TrimSpace(getenv("IGLOO_ERASURE_KEY"))

	var errs []error
	addErr := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if file := get("IGLOO_ERASURE_KEY_FILE", ""); file != "" {
		switch b, err := os.ReadFile(file); {
		case cfg.ErasureKey != "":
			addErr("set IGLOO_ERASURE_KEY or IGLOO_ERASURE_KEY_FILE, not both")
		case err != nil:
			addErr("IGLOO_ERASURE_KEY_FILE: %v", err)
		default:
			cfg.ErasureKey = strings.TrimSpace(string(b))
		}
	}

	// How strong it must be is checked only when Igloo creates the account
	// (CheckAdminPassword): an account that exists does not use it.
	if cfg.AdminPassword == "" {
		addErr("IGLOO_ADMIN_PASSWORD must be set")
	}
	if !ValidName(cfg.AdminUser) {
		addErr("IGLOO_ADMIN_USER %q must be a lowercase name of letters, digits and underscores", cfg.AdminUser)
	}
	if cfg.ConnectionSecretsDir != "" && !strings.HasPrefix(cfg.ConnectionSecretsDir, "/") {
		addErr("IGLOO_CONNECTION_SECRETS_DIR %q must be an absolute path", cfg.ConnectionSecretsDir)
	}
	if cfg.ErasureKey != "" && len(cfg.ErasureKey) < 32 {
		addErr("the erasure key, IGLOO_ERASURE_KEY or IGLOO_ERASURE_KEY_FILE's, must be at least 32 characters")
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

	switch v := get("IGLOO_SORT_SPILL_BUDGET", ""); v {
	case "":
	case "off":
		cfg.DuckDB.SortSpillBudget = -1
	default:
		n, err := ParseSize(v)
		if err != nil || n <= 0 {
			addErr("IGLOO_SORT_SPILL_BUDGET must be a size such as 50GB or 40GiB, or off, got %q", v)
		}
		cfg.DuckDB.SortSpillBudget = n
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
	if v := get("IGLOO_MAINTENANCE_MEMORY", ""); v != "" {
		if n, err := ParseSize(v); err != nil || n <= 0 {
			addErr("IGLOO_MAINTENANCE_MEMORY must be a size such as 2GB or 512MiB, got %q", v)
		} else {
			cfg.Maintenance.Memory = n
		}
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

	cfg.OpenLineage.URL = get("IGLOO_OPENLINEAGE_URL", "")
	cfg.OpenLineage.APIKey = get("IGLOO_OPENLINEAGE_API_KEY", "")
	cfg.OpenLineage.Namespace = get("IGLOO_OPENLINEAGE_NAMESPACE", "igloo")
	for name, dst := range map[string]*bool{
		"IGLOO_OPENLINEAGE_ALLOW_PRIVATE": &cfg.OpenLineage.AllowPrivate,
		"IGLOO_OPENLINEAGE_SEND_SQL":      &cfg.OpenLineage.SendSQL,
	} {
		v, err := strconv.ParseBool(get(name, "false"))
		if err != nil {
			addErr("%s must be true or false: %w", name, err)
		}
		*dst = v
	}

	prefetch, err := strconv.ParseBool(get("IGLOO_PREFETCH", "false"))
	if err != nil {
		addErr("IGLOO_PREFETCH must be true or false: %w", err)
	}
	cfg.Prefetch.Enabled = prefetch
	if v := get("IGLOO_PREFETCH_CONCURRENCY", ""); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 4096 {
			addErr("IGLOO_PREFETCH_CONCURRENCY must be a number from 1 to 4096, got %q", v)
		}
		cfg.Prefetch.Concurrency = n
	}

	idDomains, err := strconv.ParseBool(get("IGLOO_ID_DOMAINS", "false"))
	if err != nil {
		addErr("IGLOO_ID_DOMAINS must be true or false: %w", err)
	}
	cfg.IDDomains = idDomains
	if n, err := strconv.ParseInt(get("IGLOO_ID_SYNC_MIN_ROWS", "0"), 10, 64); err != nil || n < 0 {
		addErr("IGLOO_ID_SYNC_MIN_ROWS must be a count of rows, not %q", get("IGLOO_ID_SYNC_MIN_ROWS", "0"))
	} else {
		cfg.IDSyncMinRows = n
	}

	bitmapIndexes, err := strconv.ParseBool(get("IGLOO_BITMAP_INDEXES", "false"))
	if err != nil {
		addErr("IGLOO_BITMAP_INDEXES must be true or false: %w", err)
	}
	if bitmapIndexes && !idDomains {
		addErr("IGLOO_BITMAP_INDEXES needs IGLOO_ID_DOMAINS=true: bitmap indexes hold ID domains' ids")
	}
	cfg.BitmapIndexes = bitmapIndexes
	if cfg.ReportRewrite, err = strconv.ParseBool(get("IGLOO_REPORT_REWRITE", "false")); err != nil {
		addErr("IGLOO_REPORT_REWRITE must be true or false: %w", err)
	}

	switch v := strings.ToLower(get("IGLOO_MAX_CONCURRENT_STATEMENTS", "off")); v {
	case "off", "0":
	case "auto":
		cfg.MaxConcurrentStatements = -1
	default:
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 10_000 {
			addErr("IGLOO_MAX_CONCURRENT_STATEMENTS must be a whole number from 1 to 10000, auto or off, got %q", v)
		}
		cfg.MaxConcurrentStatements = n
	}

	switch v := get("IGLOO_RESULT_CACHE_SIZE", "256MiB"); {
	case v == "0" || strings.EqualFold(v, "off"):
	default:
		n, err := ParseSize(v)
		if err != nil || n <= 0 {
			addErr("IGLOO_RESULT_CACHE_SIZE must be a size such as 256MiB or 1GB, or off, got %q", v)
		}
		cfg.ResultCacheSize = n
	}

	if v := get("IGLOO_BITMAP_MIN_ROWS", "10000000"); strings.EqualFold(v, "off") {
		cfg.BitmapMinRows = -1
	} else if n, err := strconv.ParseInt(v, 10, 64); err != nil || n < 0 {
		addErr("IGLOO_BITMAP_MIN_ROWS must be a count of rows or off, not %q", v)
	} else {
		cfg.BitmapMinRows = n
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
		if err := safeLocation(loc); err != nil {
			addErr("IGLOO_ALLOWED_LOCATIONS entry %q: %w", loc, err)
			continue
		}
		// A trailing slash makes the entry a prefix, so s3://b/land does not
		// match s3://b/landing-private, and agrees with the warehouse root
		// and DuckDB's allowed_directories (see internal/engine).
		cfg.Storage.AllowedLocations = append(cfg.Storage.AllowedLocations, strings.TrimSuffix(loc, "/")+"/")
	}

	if v := get("IGLOO_SHARING_URL", ""); v != "" {
		u, err := url.Parse(strings.TrimSuffix(v, "/"))
		switch {
		case err != nil || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil:
			addErr("IGLOO_SHARING_URL must be a URL such as https://data.example.com:8444, got %q", v)
		case u.Scheme != "https" && !(u.Scheme == "http" && isLoopbackHost(u.Hostname())):
			addErr("IGLOO_SHARING_URL must use https://, which recipients' tokens need, got %q", v)
		default:
			cfg.SharingURL = u.String() + "/delta-sharing"
		}
	}

	cfg.Workers = loadWorkers(lookup, &errs)
	if len(cfg.Workers.Addresses) > 0 && cfg.DuckDB.TempDir == "" {
		addErr("IGLOO_WORKERS needs IGLOO_DUCKDB_TEMP_DIR, where partial results are kept")
	}

	if cfg.Catalog.URI != "" {
		if _, err := url.ParseRequestURI(cfg.Catalog.URI); err != nil {
			addErr("IGLOO_CATALOG_URI: %w", err)
		}
		if cfg.Storage.Location == "" {
			addErr("IGLOO_WAREHOUSE_LOCATION must be set when IGLOO_CATALOG_URI is")
		} else if err := cfg.Storage.parseLocation(); err != nil {
			addErr("IGLOO_WAREHOUSE_LOCATION: %w", err)
		} else if err := safeLocation(cfg.Storage.Location); err != nil {
			addErr("IGLOO_WAREHOUSE_LOCATION %q: %w", cfg.Storage.Location, err)
		}
	}

	loadOIDC(&cfg, lookup, addErr)
	loadNotifications(&cfg, lookup, addErr)

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

// isLoopbackHost reports whether host names this machine.
func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// safeLocation refuses a warehouse or allowed location that could let a
// path climb out of its prefix once it is used for prefix matching: a dot
// segment, percent-encoding, a backslash, or an interior empty segment.
// These are the same shapes checkPath refuses in the paths users name, so
// the allowed set and the paths checked against it agree.
func safeLocation(loc string) error {
	rest := loc
	if i := strings.Index(rest, "://"); i >= 0 {
		rest = rest[i+3:]
		if j := strings.IndexByte(rest, '/'); j >= 0 {
			rest = rest[j+1:]
		} else {
			rest = ""
		}
	}
	switch {
	case strings.Contains(loc, "\\"):
		return errors.New("must not contain a backslash")
	case strings.ContainsAny(loc, "?#"):
		return errors.New("must not contain '?' or '#'")
	case strings.Contains(loc, "%"):
		return errors.New("must not be percent-encoded")
	}
	segs := strings.Split(rest, "/")
	for i, seg := range segs {
		if seg == "." || seg == ".." || (seg == "" && i != 0 && i != len(segs)-1) {
			return errors.New("must not contain '.', '..' or empty path segments")
		}
	}
	return nil
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
