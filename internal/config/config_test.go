package config

import (
	"log/slog"
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
	if m := cfg.Maintenance; m.Enabled || m.Interval != 10*time.Second || m.Retention != 30*24*time.Hour {
		t.Errorf("got %+v", m)
	}
	if cfg, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_SNAPSHOT_RETENTION": "0"})); err != nil || cfg.Maintenance.Retention != 0 {
		t.Errorf("retention 0: %v %s", err, cfg.Maintenance.Retention)
	}
	for k, v := range map[string]string{
		"IGLOO_MAINTENANCE":          "sometimes",
		"IGLOO_MAINTENANCE_INTERVAL": "0s",
		"IGLOO_SNAPSHOT_RETENTION":   "a week",
	} {
		if _, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", k: v})); err == nil {
			t.Errorf("%s=%s accepted", k, v)
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
