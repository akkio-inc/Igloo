package config

import (
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}))
	if err != nil {
		t.Fatal(err)
	}

	if cfg.ListenAddr != ":5432" || cfg.HTTPAddr != ":8080" || cfg.AdminUser != "igloo" {
		t.Errorf("unexpected server defaults: %+v", cfg)
	}
	if cfg.LogFormat != "text" || cfg.LogLevel != slog.LevelInfo {
		t.Errorf("unexpected log defaults: %s %s", cfg.LogFormat, cfg.LogLevel)
	}
	if cfg.Catalog.URI != "" || cfg.Catalog.Name != "lake" || cfg.Catalog.Schema != "public" || !cfg.Catalog.Setup {
		t.Errorf("unexpected catalog defaults: %+v", cfg.Catalog)
	}
	if !cfg.Storage.S3.UseSSL || cfg.Storage.S3.URLStyle != "vhost" {
		t.Errorf("unexpected S3 defaults: %+v", cfg.Storage.S3)
	}
	if cfg.StatementTimeout != 0 || cfg.MaxConnections != 100 {
		t.Errorf("unexpected limits: timeout %s, max connections %d", cfg.StatementTimeout, cfg.MaxConnections)
	}
	if m := cfg.Maintenance; !m.Enabled || m.Interval != time.Hour || m.Retention != 7*24*time.Hour {
		t.Errorf("unexpected maintenance defaults: %+v", m)
	}
	if a := cfg.Audit; !a.Enabled || a.Retention != 90*24*time.Hour {
		t.Errorf("unexpected audit defaults: %+v", a)
	}
}

func TestAudit(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"IGLOO_ADMIN_PASSWORD":  "password",
		"IGLOO_AUDIT":           "false",
		"IGLOO_AUDIT_RETENTION": "36h",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if a := cfg.Audit; a.Enabled || a.Retention != 36*time.Hour {
		t.Errorf("got %+v", a)
	}
	for _, kv := range [][2]string{
		{"IGLOO_AUDIT", "maybe"},
		{"IGLOO_AUDIT_RETENTION", "0"},
		{"IGLOO_AUDIT_RETENTION", "forever"},
	} {
		if _, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", kv[0]: kv[1]})); err == nil {
			t.Errorf("%s=%s accepted", kv[0], kv[1])
		}
	}
}

func TestSharing(t *testing.T) {
	cfg, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}))
	if err != nil || cfg.SharingAddr != ":8444" || cfg.SharingURL != "" {
		t.Fatalf("defaults: %q %q %v", cfg.SharingAddr, cfg.SharingURL, err)
	}
	cfg, err = Load(env(map[string]string{
		"IGLOO_ADMIN_PASSWORD": "password",
		"IGLOO_SHARING_ADDR":   "",
		"IGLOO_SHARING_URL":    "https://data.example.com:8444/",
	}))
	if err != nil || cfg.SharingAddr != "" || cfg.SharingURL != "https://data.example.com:8444/delta-sharing" {
		t.Errorf("got %q %q %v", cfg.SharingAddr, cfg.SharingURL, err)
	}
	if cfg, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_SHARING_URL": "http://localhost:8444"})); err != nil ||
		cfg.SharingURL != "http://localhost:8444/delta-sharing" {
		t.Errorf("a local URL: %q %v", cfg.SharingURL, err)
	}
	for _, v := range []string{"http://data.example.com", "data.example.com", "https://data.example.com/?x=1", "https://user:pw@data.example.com"} {
		if _, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_SHARING_URL": v})); err == nil {
			t.Errorf("IGLOO_SHARING_URL=%s accepted", v)
		}
	}
}

func TestMaintenance(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"IGLOO_ADMIN_PASSWORD":       "password",
		"IGLOO_MAINTENANCE":          "false",
		"IGLOO_MAINTENANCE_INTERVAL": "10s",
		"IGLOO_SNAPSHOT_RETENTION":   "30d",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if m := cfg.Maintenance; m.Enabled || m.Interval != 10*time.Second || m.Retention != 30*24*time.Hour || m.Memory != 0 {
		t.Errorf("got %+v", m)
	}
	if cfg, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_SNAPSHOT_RETENTION": "0"})); err != nil || cfg.Maintenance.Retention != 0 {
		t.Errorf("retention 0: %v %s", err, cfg.Maintenance.Retention)
	}
	if cfg, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_MAINTENANCE_MEMORY": "2GiB"})); err != nil || cfg.Maintenance.Memory != 2<<30 {
		t.Errorf("memory 2GiB: %v %d", err, cfg.Maintenance.Memory)
	}
	for k, v := range map[string]string{
		"IGLOO_MAINTENANCE":          "sometimes",
		"IGLOO_MAINTENANCE_INTERVAL": "0s",
		"IGLOO_SNAPSHOT_RETENTION":   "a week",
		"IGLOO_MAINTENANCE_MEMORY":   "0",
	} {
		if _, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", k: v})); err == nil {
			t.Errorf("%s=%s accepted", k, v)
		}
	}
}

func TestCache(t *testing.T) {
	load := func(m map[string]string) (Cache, error) {
		m["IGLOO_ADMIN_PASSWORD"] = "password"
		cfg, err := Load(env(m))
		return cfg.Cache, err
	}

	if c, err := load(map[string]string{}); err != nil || c != (Cache{}) {
		t.Errorf("no cache directory: %+v, %v", c, err)
	}
	if c, err := load(map[string]string{"IGLOO_CACHE_DIR": "/var/lib/igloo/cache"}); err != nil || c.Dir != "/var/lib/igloo/cache" || c.MaxSize != 0 {
		t.Errorf("automatic size: %+v, %v", c, err)
	}
	if c, err := load(map[string]string{"IGLOO_CACHE_DIR": "/c", "IGLOO_CACHE_SIZE": "40GiB"}); err != nil || c.MaxSize != 40<<30 {
		t.Errorf("40GiB: %+v, %v", c, err)
	}
	if c, err := load(map[string]string{"IGLOO_CACHE_DIR": "/c", "IGLOO_CACHE_SIZE": "0"}); err != nil || c.Dir != "" {
		t.Errorf("0 should turn the cache off: %+v, %v", c, err)
	}
	for _, bad := range []map[string]string{
		{"IGLOO_CACHE_DIR": "/c", "IGLOO_CACHE_SIZE": "lots"},
		{"IGLOO_CACHE_DIR": "/c", "IGLOO_CACHE_SIZE": "-1GB"},
		{"IGLOO_CACHE_DIR": "/c", "IGLOO_CACHE_SIZE": "0.1"},
		{"IGLOO_CACHE_DIR": "relative/cache"},
	} {
		if _, err := load(bad); err == nil {
			t.Errorf("%v accepted", bad)
		}
	}
}

func TestParseSize(t *testing.T) {
	for s, want := range map[string]int64{
		"1073741824": 1 << 30,
		"512B":       512,
		"50GB":       50e9,
		"50 gb":      50e9,
		"40GiB":      40 << 30,
		"1.5TiB":     3 << 39,
		"100 MiB":    100 << 20,
		"2kb":        2000,
	} {
		if got, err := ParseSize(s); err != nil || got != want {
			t.Errorf("%q: got %d, %v; want %d", s, got, err, want)
		}
	}
	for _, s := range []string{"", "GB", "1PB", "1 G B", "-1", "1e3", "9999999TiB"} {
		if got, err := ParseSize(s); err == nil {
			t.Errorf("%q: accepted as %d", s, got)
		}
	}
}

func TestLimits(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"IGLOO_ADMIN_PASSWORD":    "password",
		"IGLOO_STATEMENT_TIMEOUT": "90m",
		"IGLOO_MAX_CONNECTIONS":   "20",
		"IGLOO_SHUTDOWN_DELAY":    "5s",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.StatementTimeout.Minutes() != 90 || cfg.MaxConnections != 20 || cfg.ShutdownDelay.Seconds() != 5 {
		t.Errorf("got timeout %s, max connections %d", cfg.StatementTimeout, cfg.MaxConnections)
	}
	for k, v := range map[string]string{"IGLOO_STATEMENT_TIMEOUT": "soon", "IGLOO_MAX_CONNECTIONS": "0"} {
		if _, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", k: v})); err == nil {
			t.Errorf("%s=%s accepted", k, v)
		}
	}
}

func TestEmptyHTTPAddrDisablesHTTP(t *testing.T) {
	cfg, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_HTTP_ADDR": ""}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != "" {
		t.Errorf("HTTPAddr = %q, want empty", cfg.HTTPAddr)
	}
}

func TestIceberg(t *testing.T) {
	cfg, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.IcebergAddr != ":8181" || cfg.Storage.S3.STSRoleARN != "" {
		t.Errorf("unexpected Iceberg defaults: %q, %q", cfg.IcebergAddr, cfg.Storage.S3.STSRoleARN)
	}

	cfg, err = Load(env(map[string]string{
		"IGLOO_ADMIN_PASSWORD":  "password",
		"IGLOO_ICEBERG_ADDR":    "",
		"IGLOO_S3_STS_ROLE_ARN": "arn:aws-us-gov:iam::123456789012:role/igloo/engines",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.IcebergAddr != "" || cfg.Storage.S3.STSRoleARN != "arn:aws-us-gov:iam::123456789012:role/igloo/engines" {
		t.Errorf("got %q, %q", cfg.IcebergAddr, cfg.Storage.S3.STSRoleARN)
	}

	for _, arn := range []string{"igloo-engines", "arn:aws:iam::123456789012:user/igloo", "arn:aws:iam::123:role/x"} {
		_, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_S3_STS_ROLE_ARN": arn}))
		if err == nil || !strings.Contains(err.Error(), "IGLOO_S3_STS_ROLE_ARN") {
			t.Errorf("%q: got %v", arn, err)
		}
	}
}

func TestLoadRequiresAdminPassword(t *testing.T) {
	_, err := Load(env(nil))
	if err == nil || !strings.Contains(err.Error(), "IGLOO_ADMIN_PASSWORD") {
		t.Fatalf("expected password error, got %v", err)
	}
}

func TestLoadReportsEveryProblem(t *testing.T) {
	_, err := Load(env(map[string]string{
		"IGLOO_ADMIN_PASSWORD":    "password",
		"IGLOO_ADMIN_USER":        "Bad-Name",
		"IGLOO_DUCKDB_THREADS":    "zero",
		"IGLOO_S3_USE_SSL":        "maybe",
		"IGLOO_S3_URL_STYLE":      "virtual",
		"IGLOO_S3_ACCESS_KEY_ID":  "key",
		"IGLOO_LOG_FORMAT":        "xml",
		"IGLOO_LOG_LEVEL":         "loud",
		"IGLOO_TLS_CERT_FILE":     "/cert.pem",
		"IGLOO_CATALOG_URI":       "http://catalog:8181/catalog",
		"IGLOO_ALLOWED_LOCATIONS": "not-a-url",
	}))
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, want := range []string{
		"ADMIN_USER", "THREADS", "USE_SSL", "URL_STYLE", "SECRET_ACCESS_KEY", "LOG_FORMAT",
		"LOG_LEVEL", "TLS_KEY_FILE", "WAREHOUSE_LOCATION", "ALLOWED_LOCATIONS",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s:\n%s", want, err)
		}
	}
}

func TestWarehouseLocations(t *testing.T) {
	cases := []struct {
		location string
		extra    map[string]string
		want     Storage
	}{
		{
			location: "s3://bucket/warehouse/",
			want:     Storage{Type: StorageS3, Bucket: "bucket", Prefix: "warehouse"},
		},
		{
			location: "s3://bucket",
			want:     Storage{Type: StorageS3, Bucket: "bucket"},
		},
		{
			location: "gs://bucket/a/b",
			extra:    map[string]string{"IGLOO_GCS_HMAC_KEY_ID": "id", "IGLOO_GCS_HMAC_SECRET": "s"},
			want:     Storage{Type: StorageGCS, Bucket: "bucket", Prefix: "a/b"},
		},
		{
			location: "abfss://lake@acct.dfs.core.windows.net/warehouse",
			want: Storage{Type: StorageAzure, Bucket: "lake", Prefix: "warehouse",
				Azure: Azure{Account: "acct", Host: "acct.dfs.core.windows.net"}},
		},
	}
	for _, c := range cases {
		vars := map[string]string{
			"IGLOO_ADMIN_PASSWORD":     "password",
			"IGLOO_CATALOG_URI":        "http://catalog:8181/catalog",
			"IGLOO_WAREHOUSE_LOCATION": c.location,
		}
		for k, v := range c.extra {
			vars[k] = v
		}
		cfg, err := Load(env(vars))
		if err != nil {
			t.Errorf("%s: %v", c.location, err)
			continue
		}
		got := cfg.Storage
		if got.Type != c.want.Type || got.Bucket != c.want.Bucket || got.Prefix != c.want.Prefix ||
			got.Azure.Account != c.want.Azure.Account || got.Azure.Host != c.want.Azure.Host {
			t.Errorf("%s: got %+v", c.location, got)
		}
	}
}

func TestBadWarehouseLocations(t *testing.T) {
	for _, location := range []string{
		"file:///data",
		"s3://",
		"gs://bucket/x", // no HMAC key
		"abfss://acct.dfs.core.windows.net/x",
	} {
		_, err := Load(env(map[string]string{
			"IGLOO_ADMIN_PASSWORD":     "password",
			"IGLOO_CATALOG_URI":        "http://catalog:8181/catalog",
			"IGLOO_WAREHOUSE_LOCATION": location,
		}))
		if err == nil {
			t.Errorf("%s: expected an error", location)
		}
	}
}

func TestAllowedLocations(t *testing.T) {
	// Good entries are kept and each is given a trailing slash, so a prefix
	// check cannot match a sibling (land/ must not match landing-private/).
	cfg, err := Load(env(map[string]string{
		"IGLOO_ADMIN_PASSWORD":    "password",
		"IGLOO_ALLOWED_LOCATIONS": "s3://bucket/land , gs://drops/exports/",
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"s3://bucket/land/", "gs://drops/exports/"}
	if !slices.Equal(cfg.Storage.AllowedLocations, want) {
		t.Errorf("allowed locations: %q, want %q", cfg.Storage.AllowedLocations, want)
	}

	// Entries that could let a path climb out of a prefix are refused.
	for _, loc := range []string{
		"s3://bucket/../other/",
		"s3://bucket/a/../b/",
		"s3://bucket/%2e%2e/",
		"s3://bucket/a//b/",
		`s3://bucket/a\b/`,
		"s3://bucket/land?x=1",
	} {
		_, err := Load(env(map[string]string{
			"IGLOO_ADMIN_PASSWORD":    "password",
			"IGLOO_ALLOWED_LOCATIONS": loc,
		}))
		if err == nil {
			t.Errorf("%s: expected an error", loc)
		}
	}
}

func TestValidName(t *testing.T) {
	for _, name := range []string{"igloo", "a", "data_team_1"} {
		if !ValidName(name) {
			t.Errorf("%q should be valid", name)
		}
	}
	for _, name := range []string{"", "Alice", "1abc", "a-b", "a b", strings.Repeat("x", 64)} {
		if ValidName(name) {
			t.Errorf("%q should be invalid", name)
		}
	}
}

func TestTLSRequired(t *testing.T) {
	base := map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}
	with := func(extra map[string]string) map[string]string {
		m := map[string]string{}
		for k, v := range base {
			m[k] = v
		}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}
	tls := map[string]string{"IGLOO_TLS_CERT_FILE": "/c.pem", "IGLOO_TLS_KEY_FILE": "/k.pem"}

	if cfg, _ := Load(env(base)); cfg.TLSRequired {
		t.Error("TLS cannot be required without a certificate")
	}
	if cfg, _ := Load(env(with(tls))); !cfg.TLSRequired {
		t.Error("TLS should be required once a certificate is configured")
	}
	optOut := with(tls)
	optOut["IGLOO_TLS_REQUIRED"] = "false"
	if cfg, _ := Load(env(optOut)); cfg.TLSRequired {
		t.Error("IGLOO_TLS_REQUIRED=false not honoured")
	}
	if _, err := Load(env(with(map[string]string{"IGLOO_TLS_REQUIRED": "true"}))); err == nil {
		t.Error("requiring TLS without a certificate should be an error")
	}
}

func TestPoolAndPrice(t *testing.T) {
	cfg, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}))
	if err != nil || cfg.Pool != "default" || cfg.PricePerHour != 0 {
		t.Fatalf("defaults: %q %v %v", cfg.Pool, cfg.PricePerHour, err)
	}
	cfg, err = Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_POOL": "acme-reporting", "IGLOO_PRICE_PER_HOUR": "0.58"}))
	if err != nil || cfg.Pool != "acme-reporting" || cfg.PricePerHour != 0.58 {
		t.Fatalf("got %q %v %v", cfg.Pool, cfg.PricePerHour, err)
	}
	for _, kv := range [][2]string{
		{"IGLOO_POOL", "Acme"}, {"IGLOO_POOL", "-acme"}, {"IGLOO_POOL", strings.Repeat("a", 64)}, {"IGLOO_POOL", "a b"},
		{"IGLOO_PRICE_PER_HOUR", "-1"}, {"IGLOO_PRICE_PER_HOUR", "cheap"}, {"IGLOO_PRICE_PER_HOUR", "NaN"},
	} {
		if _, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", kv[0]: kv[1]})); err == nil {
			t.Errorf("%s=%s should be refused", kv[0], kv[1])
		}
	}
}

// The erasure key comes from IGLOO_ERASURE_KEY or a file, as a Kubernetes
// secret mounts one, without the spaces and line break around it: a key
// that differs between replicas by a newline would keep their requests from
// running.
func TestErasureKey(t *testing.T) {
	key := "an erasure key of thirty-two bytes"
	file := filepath.Join(t.TempDir(), "erasure-key")
	if err := os.WriteFile(file, []byte(key+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, vars := range []map[string]string{
		{"IGLOO_ERASURE_KEY": key},
		{"IGLOO_ERASURE_KEY": " " + key + "\n"},
		{"IGLOO_ERASURE_KEY_FILE": file},
	} {
		vars["IGLOO_ADMIN_PASSWORD"] = "password"
		cfg, err := Load(env(vars))
		if err != nil || cfg.ErasureKey != key {
			t.Errorf("%v: key %q, %v", vars, cfg.ErasureKey, err)
		}
	}
	for _, vars := range []map[string]string{
		{"IGLOO_ERASURE_KEY": key, "IGLOO_ERASURE_KEY_FILE": file},
		{"IGLOO_ERASURE_KEY_FILE": file + ".missing"},
		{"IGLOO_ERASURE_KEY": "short"},
	} {
		vars["IGLOO_ADMIN_PASSWORD"] = "password"
		if _, err := Load(env(vars)); err == nil {
			t.Errorf("%v: accepted", vars)
		}
	}
}

// IGLOO_EXCHANGES says where workers exchange rows: object storage unless
// set to local or auto.
func TestExchanges(t *testing.T) {
	cfg, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}))
	if err != nil || cfg.Workers.Exchanges != "object" {
		t.Fatalf("default: %q %v", cfg.Workers.Exchanges, err)
	}
	for _, v := range []string{"local", "auto"} {
		cfg, err = Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_EXCHANGES": v}))
		if err != nil || cfg.Workers.Exchanges != v {
			t.Fatalf("%s: %q %v", v, cfg.Workers.Exchanges, err)
		}
	}
	for _, v := range []string{"nvme", "Local", "s3"} {
		if _, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_EXCHANGES": v})); err == nil {
			t.Errorf("IGLOO_EXCHANGES=%s should be refused", v)
		}
	}
	cfg, err = Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_EXCHANGE_MAX_SIZE": "30GiB"}))
	if err != nil || cfg.Workers.ExchangeMax != 30<<30 {
		t.Errorf("IGLOO_EXCHANGE_MAX_SIZE=30GiB: %d %v", cfg.Workers.ExchangeMax, err)
	}
	if _, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_EXCHANGE_MAX_SIZE": "lots"})); err == nil {
		t.Error("IGLOO_EXCHANGE_MAX_SIZE=lots should be refused")
	}
}

func TestSortSpillBudget(t *testing.T) {
	for v, want := range map[string]int64{"": 0, "off": -1, "50GB": 50e9, "2GiB": 2 << 30} {
		m := map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}
		if v != "" {
			m["IGLOO_SORT_SPILL_BUDGET"] = v
		}
		cfg, err := Load(env(m))
		if err != nil || cfg.DuckDB.SortSpillBudget != want {
			t.Errorf("IGLOO_SORT_SPILL_BUDGET=%q: %d %v, want %d", v, cfg.DuckDB.SortSpillBudget, err, want)
		}
	}
	for _, v := range []string{"0", "-5GB", "lots"} {
		if _, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_SORT_SPILL_BUDGET": v})); err == nil {
			t.Errorf("IGLOO_SORT_SPILL_BUDGET=%s should be refused", v)
		}
	}
}

func TestMaxConcurrentStatements(t *testing.T) {
	for v, want := range map[string]int{"": 0, "off": 0, "0": 0, "auto": -1, "AUTO": -1, "8": 8} {
		m := map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}
		if v != "" {
			m["IGLOO_MAX_CONCURRENT_STATEMENTS"] = v
		}
		cfg, err := Load(env(m))
		if err != nil || cfg.MaxConcurrentStatements != want {
			t.Errorf("IGLOO_MAX_CONCURRENT_STATEMENTS=%q: %d %v, want %d", v, cfg.MaxConcurrentStatements, err, want)
		}
	}
	for _, v := range []string{"-2", "lots", "1.5"} {
		if _, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_MAX_CONCURRENT_STATEMENTS": v})); err == nil {
			t.Errorf("IGLOO_MAX_CONCURRENT_STATEMENTS=%s should be refused", v)
		}
	}
}

func TestStatementReservations(t *testing.T) {
	for v, want := range map[string]float64{"": DefaultStatementReservations, "auto": DefaultStatementReservations,
		"off": 0, "0": 0, "0.25": 0.25, "0.5": 0.5, "2": 2} {
		m := map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}
		if v != "" {
			m["IGLOO_STATEMENT_RESERVATIONS"] = v
		}
		cfg, err := Load(env(m))
		if err != nil || cfg.StatementReservations != want {
			t.Errorf("IGLOO_STATEMENT_RESERVATIONS=%q: %v %v, want %v", v, cfg.StatementReservations, err, want)
		}
	}
	for _, v := range []string{"-1", "5", "lots"} {
		if _, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_STATEMENT_RESERVATIONS": v})); err == nil {
			t.Errorf("IGLOO_STATEMENT_RESERVATIONS=%s should be refused", v)
		}
	}
}

func TestWorkerSpotNotice(t *testing.T) {
	for v, want := range map[string]bool{"": true, "on": true, "off": false, "false": false} {
		m := map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}
		if v != "" {
			m["IGLOO_WORKER_SPOT_NOTICE"] = v
		}
		cfg, err := Load(env(m))
		if err != nil || cfg.Workers.SpotNotice != want {
			t.Errorf("IGLOO_WORKER_SPOT_NOTICE=%q: %v %v, want %v", v, cfg.Workers.SpotNotice, err, want)
		}
	}
	if _, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_WORKER_SPOT_NOTICE": "sometimes"})); err == nil {
		t.Error("IGLOO_WORKER_SPOT_NOTICE=sometimes should be refused")
	}
}

func TestWorkerIdleAfter(t *testing.T) {
	for v, want := range map[string]struct {
		after time.Duration
		auto  bool
	}{"": {10 * time.Minute, true}, "auto": {10 * time.Minute, true}, "5m": {5 * time.Minute, false}} {
		m := map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}
		if v != "" {
			m["IGLOO_WORKER_IDLE_AFTER"] = v
		}
		cfg, err := Load(env(m))
		if err != nil || cfg.Workers.IdleAfter != want.after || cfg.Workers.IdleAuto != want.auto {
			t.Errorf("IGLOO_WORKER_IDLE_AFTER=%q: %v %v %v, want %v", v, cfg.Workers.IdleAfter, cfg.Workers.IdleAuto, err, want)
		}
	}
}

func TestWorkerIsolatedPurgeAfter(t *testing.T) {
	for v, want := range map[string]time.Duration{"": time.Minute, "30s": 30 * time.Second, "5s": 0, "nope": 0} {
		m := map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}
		if v != "" {
			m["IGLOO_WORKER_ISOLATED_PURGE_AFTER"] = v
		}
		cfg, err := Load(env(m))
		if want == 0 && err == nil || want != 0 && (err != nil || cfg.Workers.IsolatedPurgeAfter != want) {
			t.Errorf("IGLOO_WORKER_ISOLATED_PURGE_AFTER=%q: %v %v, want %v", v, cfg.Workers.IsolatedPurgeAfter, err, want)
		}
	}
}

func TestResultCacheSize(t *testing.T) {
	for v, want := range map[string]int64{"": 256 << 20, "0": 0, "off": 0, "1GB": 1e9, "64MiB": 64 << 20} {
		m := map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}
		if v != "" {
			m["IGLOO_RESULT_CACHE_SIZE"] = v
		}
		cfg, err := Load(env(m))
		if err != nil || cfg.ResultCacheSize != want {
			t.Errorf("IGLOO_RESULT_CACHE_SIZE=%q: %d %v, want %d", v, cfg.ResultCacheSize, err, want)
		}
	}
	for _, v := range []string{"-5GB", "lots"} {
		if _, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_RESULT_CACHE_SIZE": v})); err == nil {
			t.Errorf("IGLOO_RESULT_CACHE_SIZE=%s should be refused", v)
		}
	}
}

func TestIDSyncMinRows(t *testing.T) {
	for v, want := range map[string]int64{"": 0, "0": 0, "100000": 100_000} {
		m := map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}
		if v != "" {
			m["IGLOO_ID_SYNC_MIN_ROWS"] = v
		}
		cfg, err := Load(env(m))
		if err != nil || cfg.IDSyncMinRows != want {
			t.Errorf("IGLOO_ID_SYNC_MIN_ROWS=%q: %d %v, want %d", v, cfg.IDSyncMinRows, err, want)
		}
	}
	for _, v := range []string{"-1", "some"} {
		if _, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_ID_SYNC_MIN_ROWS": v})); err == nil {
			t.Errorf("IGLOO_ID_SYNC_MIN_ROWS=%s should be refused", v)
		}
	}
}

func TestReportRewrite(t *testing.T) {
	for v, want := range map[string]bool{"": false, "false": false, "true": true} {
		m := map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}
		if v != "" {
			m["IGLOO_REPORT_REWRITE"] = v
		}
		cfg, err := Load(env(m))
		if err != nil || cfg.ReportRewrite != want {
			t.Errorf("IGLOO_REPORT_REWRITE=%q: %v %v, want %v", v, cfg.ReportRewrite, err, want)
		}
	}
	if _, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_REPORT_REWRITE": "sometimes"})); err == nil {
		t.Error("IGLOO_REPORT_REWRITE=sometimes should be refused")
	}
}

func TestBitmapMinRows(t *testing.T) {
	for v, want := range map[string]int64{"": 10_000_000, "0": 0, "250000000": 250_000_000, "off": -1, "OFF": -1} {
		m := map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}
		if v != "" {
			m["IGLOO_BITMAP_MIN_ROWS"] = v
		}
		cfg, err := Load(env(m))
		if err != nil || cfg.BitmapMinRows != want {
			t.Errorf("IGLOO_BITMAP_MIN_ROWS=%q: %d %v, want %d", v, cfg.BitmapMinRows, err, want)
		}
	}
	for _, v := range []string{"-5", "many", "1e7"} {
		if _, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_BITMAP_MIN_ROWS": v})); err == nil {
			t.Errorf("IGLOO_BITMAP_MIN_ROWS=%s should be refused", v)
		}
	}
}
