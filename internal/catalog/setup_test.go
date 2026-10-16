package catalog

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/config"
)

// fakeLakekeeper mimics the parts of Lakekeeper's management API that Setup
// uses.
type fakeLakekeeper struct {
	mu           sync.Mutex
	bootstrapped bool
	warehouses   map[string]map[string]any
	unavailable  int // answer this many requests with 503 first
	conflict     bool
}

func (f *fakeLakekeeper) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.unavailable > 0 {
		f.unavailable--
		http.Error(w, "starting", http.StatusServiceUnavailable)
		return
	}

	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/management/v1/bootstrap":
		if f.bootstrapped {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":{"code":400,"message":"Catalog already bootstrapped","type":"CatalogAlreadyBootstrapped"}}`)
			return
		}
		f.bootstrapped = true
		w.WriteHeader(http.StatusNoContent)

	case r.Method == http.MethodGet && r.URL.Path == "/management/v1/warehouse":
		var list []map[string]any
		for name, req := range f.warehouses {
			list = append(list, map[string]any{"name": name, "storage-profile": req["storage-profile"]})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"warehouses": list})

	case r.Method == http.MethodPost && r.URL.Path == "/management/v1/warehouse":
		if f.conflict {
			http.Error(w, `{"error":{"type":"WarehouseNameAlreadyExists"}}`, http.StatusConflict)
			return
		}
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		f.warehouses[req["warehouse-name"].(string)] = req
		w.WriteHeader(http.StatusCreated)

	default:
		http.NotFound(w, r)
	}
}

func init() { retryInterval = 10 * time.Millisecond }

func setupAgainst(t *testing.T, fake *fakeLakekeeper, storage config.Storage) error {
	t.Helper()
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	cfg := config.Config{
		Catalog: config.Catalog{URI: srv.URL + "/catalog", Warehouse: "igloo"},
		Storage: storage,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return Setup(ctx, cfg, srv.Client(), slog.New(slog.NewTextHandler(io.Discard, nil)))
}

var s3Storage = config.Storage{
	Type: config.StorageS3, Bucket: "igloo", Prefix: "warehouse", Location: "s3://igloo/warehouse",
	S3: config.S3{Endpoint: "storage:9000", Region: "us-east-1", URLStyle: "path", AccessKeyID: "k", SecretAccessKey: "s"},
}

func TestSetupCreatesWarehouseOnce(t *testing.T) {
	fake := &fakeLakekeeper{warehouses: map[string]map[string]any{}}
	for i := 0; i < 2; i++ {
		if err := setupAgainst(t, fake, s3Storage); err != nil {
			t.Fatalf("run %d: %v", i+1, err)
		}
	}
	if !fake.bootstrapped || len(fake.warehouses) != 1 {
		t.Fatalf("bootstrapped=%v warehouses=%v", fake.bootstrapped, fake.warehouses)
	}

	req := fake.warehouses["igloo"]
	profile := req["storage-profile"].(map[string]any)
	if profile["endpoint"] != "http://storage:9000" || profile["flavor"] != "s3-compat" ||
		profile["path-style-access"] != true || profile["key-prefix"] != "warehouse" {
		t.Errorf("unexpected profile: %v", profile)
	}
	cred := req["storage-credential"].(map[string]any)
	if cred["credential-type"] != "access-key" || cred["access-key-id"] != "k" {
		t.Errorf("unexpected credential: %v", cred)
	}
}

func TestSetupWaitsForCatalog(t *testing.T) {
	fake := &fakeLakekeeper{warehouses: map[string]map[string]any{}, unavailable: 3}
	if err := setupAgainst(t, fake, s3Storage); err != nil {
		t.Fatal(err)
	}
	if len(fake.warehouses) != 1 {
		t.Fatal("warehouse not created after the catalog came up")
	}
}

func TestSetupToleratesConcurrentCreate(t *testing.T) {
	fake := &fakeLakekeeper{warehouses: map[string]map[string]any{}, conflict: true}
	if err := setupAgainst(t, fake, s3Storage); err != nil {
		t.Fatalf("a 409 from a racing replica should count as success: %v", err)
	}
}

func TestWarehouseRequests(t *testing.T) {
	cases := []struct {
		name    string
		storage config.Storage
		profile map[string]any
		cred    map[string]any
	}{
		{
			name:    "aws with IAM",
			storage: config.Storage{Type: config.StorageS3, Bucket: "b", S3: config.S3{Region: "eu-west-1", URLStyle: "vhost", UseSSL: true}},
			profile: map[string]any{"type": "s3", "bucket": "b", "region": "eu-west-1", "flavor": "aws", "path-style-access": false},
			cred:    map[string]any{"type": "s3", "credential-type": "aws-system-identity"},
		},
		{
			name:    "gcs",
			storage: config.Storage{Type: config.StorageGCS, Bucket: "b", Prefix: "w"},
			profile: map[string]any{"type": "gcs", "bucket": "b", "key-prefix": "w"},
			cred:    map[string]any{"type": "gcs", "credential-type": "gcp-system-identity"},
		},
		{
			name: "azure with workload identity",
			storage: config.Storage{Type: config.StorageAzure, Bucket: "fs",
				Azure: config.Azure{Account: "acct", Host: "acct.dfs.core.windows.net"}},
			profile: map[string]any{"type": "adls", "account-name": "acct", "filesystem": "fs"},
			cred:    map[string]any{"type": "az", "credential-type": "azure-system-identity"},
		},
		{
			name: "azure with account key",
			storage: config.Storage{Type: config.StorageAzure, Bucket: "fs",
				Azure: config.Azure{Account: "acct", Host: "acct.dfs.core.windows.net", AccountKey: "key"}},
			profile: map[string]any{"type": "adls"},
			cred:    map[string]any{"type": "az", "credential-type": "shared-access-key", "key": "key"},
		},
	}

	for _, c := range cases {
		req, err := warehouseRequest(config.Config{Catalog: config.Catalog{Warehouse: "w"}, Storage: c.storage})
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		profile := req["storage-profile"].(map[string]any)
		cred := req["storage-credential"].(map[string]any)
		for k, v := range c.profile {
			if profile[k] != v {
				t.Errorf("%s: profile[%s] = %v, want %v", c.name, k, profile[k], v)
			}
		}
		for k, v := range c.cred {
			if cred[k] != v {
				t.Errorf("%s: credential[%s] = %v, want %v", c.name, k, cred[k], v)
			}
		}
		if _, ok := profile["host"]; ok && c.storage.Type == config.StorageAzure {
			t.Errorf("%s: default Azure host should not be sent", c.name)
		}
	}
}

func TestManagementURL(t *testing.T) {
	got, err := managementURL("http://lakekeeper:8181/catalog")
	if err != nil || got != "http://lakekeeper:8181/management/v1" {
		t.Fatalf("got %q, %v", got, err)
	}
	if !strings.HasPrefix(got, "http://") {
		t.Fatal("scheme lost")
	}
}

func TestSetupRejectsMovedWarehouse(t *testing.T) {
	fake := &fakeLakekeeper{warehouses: map[string]map[string]any{}}
	if err := setupAgainst(t, fake, s3Storage); err != nil {
		t.Fatal(err)
	}
	moved := s3Storage
	moved.Prefix, moved.Location = "elsewhere", "s3://igloo/elsewhere"
	err := setupAgainst(t, fake, moved)
	if err == nil || !strings.Contains(err.Error(), "IGLOO_WAREHOUSE_LOCATION") {
		t.Fatalf("expected a location mismatch error, got %v", err)
	}
}
