package catalog

import (
	"context"
	"encoding/json"
	"fmt"
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
	// legacy is a Lakekeeper older than 0.13, without format-version
	// policies; refusePolicy refuses to change one.
	legacy, refusePolicy bool
	// policies counts the format-version policies set.
	policies int
}

// allowed are the format versions warehouse name allows, as Lakekeeper
// reports them: all of them unless a policy says otherwise.
func (f *fakeLakekeeper) allowed(name string) []any {
	if a, ok := f.warehouses[name]["allowed-format-versions"].([]any); ok {
		return a
	}
	return []any{1, 2, 3}
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
			w := map[string]any{"name": name, "storage-profile": req["storage-profile"], "warehouse-id": "id-" + name}
			if !f.legacy {
				w["allowed-format-versions"] = f.allowed(name)
			}
			list = append(list, w)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"warehouses": list})

	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/format-version-policy") && !f.legacy:
		name := strings.TrimPrefix(strings.TrimSuffix(r.URL.Path, "/format-version-policy"), "/management/v1/warehouse/id-")
		if f.warehouses[name] == nil || f.refusePolicy {
			http.Error(w, `{"error":{"code":403,"type":"Forbidden"}}`, http.StatusForbidden)
			return
		}
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		var allowed []any
		for _, v := range req["allowed-format-versions"].([]any) {
			allowed = append(allowed, int(v.(float64)))
		}
		f.warehouses[name]["allowed-format-versions"] = allowed
		f.warehouses[name]["default-format-version"] = req["default-format-version"]
		f.policies++
		_ = json.NewEncoder(w).Encode(map[string]any{"name": name})

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

// The warehouse allows tables of format version 1 and 2 only, which
// erasure requests can rewrite, whether Setup created it or found it, by
// Lakekeeper's format-version policy; a Lakekeeper without one, or that
// refuses to change it, does not stop Igloo.
func TestSetupCapsFormatVersions(t *testing.T) {
	fake := &fakeLakekeeper{warehouses: map[string]map[string]any{}}
	for i := 0; i < 2; i++ {
		if err := setupAgainst(t, fake, s3Storage); err != nil {
			t.Fatalf("run %d: %v", i+1, err)
		}
	}
	w := fake.warehouses["igloo"]
	if got := fmt.Sprint(w["allowed-format-versions"]); got != "[1 2]" || w["default-format-version"] != float64(2) || fake.policies != 1 {
		t.Errorf("allowed %s, default %v, policies set %d", got, w["default-format-version"], fake.policies)
	}
	// A warehouse made before allows version 3 until Setup runs.
	fake.warehouses["igloo"]["allowed-format-versions"] = []any{1, 2, 3}
	if err := setupAgainst(t, fake, s3Storage); err != nil || fmt.Sprint(fake.allowed("igloo")) != "[1 2]" || fake.policies != 2 {
		t.Errorf("an existing warehouse: %v, allowed %v", err, fake.allowed("igloo"))
	}
	for _, f := range []*fakeLakekeeper{
		{warehouses: map[string]map[string]any{}, legacy: true},
		{warehouses: map[string]map[string]any{}, refusePolicy: true},
	} {
		if err := setupAgainst(t, f, s3Storage); err != nil || len(f.warehouses) != 1 || f.policies != 0 {
			t.Errorf("legacy %v, refusing %v: %v, %d policies", f.legacy, f.refusePolicy, err, f.policies)
		}
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
