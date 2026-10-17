// Package catalog talks to the Lakekeeper Iceberg catalog directly. Setup
// prepares it for Igloo: bootstrapping a new catalog and registering the
// warehouse Igloo stores its tables in, which every deployment (Docker
// Compose, Helm, the cloud modules) relies on instead of its own setup
// scripts. Client does what DuckDB cannot do through SQL: table maintenance,
// comments and views.
package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/akkio-inc/igloo/internal/config"
)

// retryInterval is how long Setup waits before retrying while the catalog
// is still starting. Tests shorten it.
var retryInterval = 2 * time.Second

// Setup bootstraps the catalog if needed and creates the warehouse if it does
// not exist. It is safe to run from several replicas at once, and retries
// until ctx is done while the catalog is unreachable.
func Setup(ctx context.Context, cfg config.Config, client *http.Client, log *slog.Logger) error {
	api, err := managementURL(cfg.Catalog.URI)
	if err != nil {
		return err
	}
	c := &lakekeeper{api: api, http: client}

	for {
		err := c.setup(ctx, cfg, log)
		if err == nil || !retryable(err) {
			return err
		}
		if ctx.Err() != nil {
			return fmt.Errorf("set up catalog: %w", err)
		}
		log.Warn("catalog not ready yet, retrying", "err", err)
		select {
		case <-ctx.Done():
		case <-time.After(retryInterval):
		}
	}
}

// managementURL derives Lakekeeper's management API from its catalog URL:
// http://host:8181/catalog -> http://host:8181/management/v1.
func managementURL(catalogURI string) (string, error) {
	u, err := url.Parse(catalogURI)
	if err != nil {
		return "", fmt.Errorf("catalog URI: %w", err)
	}
	return u.Scheme + "://" + u.Host + "/management/v1", nil
}

type lakekeeper struct {
	api  string
	http *http.Client
}

// apiError is a non-2xx response from the catalog.
type apiError struct {
	status int
	body   string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("catalog returned HTTP %d: %s", e.status, strings.TrimSpace(e.body))
}

// retryable reports whether err looks like a catalog that is still starting
// rather than a configuration mistake.
func retryable(err error) bool {
	var cfgErr *configError
	if errors.As(err, &cfgErr) {
		return false
	}
	var apiErr *apiError
	if errors.As(err, &apiErr) {
		return apiErr.status >= 500
	}
	return true // network errors
}

func (c *lakekeeper) setup(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	// Bootstrapping is a one-time step; later calls are rejected with an
	// "already bootstrapped" error, which is the expected steady state.
	err := c.call(ctx, http.MethodPost, "/bootstrap", map[string]any{"accept-terms-of-use": true}, nil)
	var apiErr *apiError
	switch {
	case err == nil:
		log.Info("bootstrapped catalog")
	case errors.As(err, &apiErr) && apiErr.status < 500 && strings.Contains(strings.ToLower(apiErr.body), "bootstrap"):
	default:
		return fmt.Errorf("bootstrap catalog: %w", err)
	}

	w, err := c.warehouse(ctx, cfg.Catalog.Warehouse)
	if err != nil {
		return err
	}
	if w != nil {
		if err := checkLocation(w.Profile, cfg.Storage); err != nil {
			return err
		}
		return c.capFormatVersions(ctx, w, log)
	}

	req, err := warehouseRequest(cfg)
	if err != nil {
		return err
	}
	err = c.call(ctx, http.MethodPost, "/warehouse", req, nil)
	switch {
	case errors.As(err, &apiErr) && apiErr.status == http.StatusConflict:
		// Another replica created it first.
	case err != nil:
		return fmt.Errorf("create warehouse %q: %w", cfg.Catalog.Warehouse, err)
	default:
		log.Info("created warehouse", "name", cfg.Catalog.Warehouse, "location", cfg.Storage.Location)
	}
	if w, err = c.warehouse(ctx, cfg.Catalog.Warehouse); err != nil || w == nil {
		return err
	}
	return c.capFormatVersions(ctx, w, log)
}

// warehouseInfo is what Setup reads of a warehouse.
type warehouseInfo struct {
	// WarehouseID is its ID; older Lakekeepers call it id only.
	WarehouseID string         `json:"warehouse-id"`
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Profile     map[string]any `json:"storage-profile"`
	// Allowed are the Iceberg format versions tables may be created in or
	// upgraded to, which Lakekeeper reports from 0.13; nil from older ones.
	Allowed []int `json:"allowed-format-versions"`
}

// warehouse returns the warehouse named name, or nil if there is none.
func (c *lakekeeper) warehouse(ctx context.Context, name string) (*warehouseInfo, error) {
	var list struct {
		Warehouses []warehouseInfo `json:"warehouses"`
	}
	if err := c.call(ctx, http.MethodGet, "/warehouse", nil, &list); err != nil {
		return nil, fmt.Errorf("list warehouses: %w", err)
	}
	for i := range list.Warehouses {
		if list.Warehouses[i].Name == name {
			return &list.Warehouses[i], nil
		}
	}
	return nil, nil
}

// formatVersions are the Iceberg format versions Igloo's warehouse allows
// tables to be created in or upgraded to: erasure requests cannot rewrite
// tables of version 3 yet (see package erasure), and Igloo's own checks do
// not reach another engine that uses the catalog directly.
var formatVersions = []int{1, 2}

// capFormatVersions has the warehouse allow formatVersions only, by
// Lakekeeper's per-warehouse format-version policy, with version 2 the
// default. Lakekeeper checks the policy when a table is created, upgraded
// or registered, so tables of version 3 made before stay writable. A
// Lakekeeper older than 0.13, which has no such policy, or one that
// refuses the change, as for a warehouse an operator manages, is logged:
// Igloo's own checks still refuse version 3 for tables with tags and
// tables erasure requests erase from.
func (c *lakekeeper) capFormatVersions(ctx context.Context, w *warehouseInfo, log *slog.Logger) error {
	if w.Allowed == nil {
		log.Warn("the catalog cannot limit the warehouse's Iceberg format versions (Lakekeeper 0.13 or later can); "+
			"Igloo refuses version 3 itself for tables with tags and tables erasure requests erase from", "warehouse", w.Name)
		return nil
	}
	if !slices.ContainsFunc(w.Allowed, func(v int) bool { return v >= 3 }) {
		return nil
	}
	id := w.WarehouseID
	if id == "" {
		id = w.ID
	}
	err := c.call(ctx, http.MethodPost, "/warehouse/"+url.PathEscape(id)+"/format-version-policy",
		map[string]any{"allowed-format-versions": formatVersions, "default-format-version": 2}, nil)
	var apiErr *apiError
	switch {
	case errors.As(err, &apiErr) && apiErr.status < 500:
		log.Error("the catalog refused to limit the warehouse's Iceberg format versions to 1 and 2; "+
			"Igloo refuses version 3 itself for tables with tags and tables erasure requests erase from, but other engines can make them",
			"warehouse", w.Name, "err", err)
		return nil
	case err != nil:
		return fmt.Errorf("limit the format versions of warehouse %q: %w", w.Name, err)
	}
	log.Info("limited the warehouse's Iceberg format versions to 1 and 2: erasure requests cannot rewrite tables of version 3 yet",
		"warehouse", w.Name)
	return nil
}

// checkLocation fails when an existing warehouse stores data somewhere other
// than IGLOO_WAREHOUSE_LOCATION. Igloo's sandbox only allows the configured
// location, so a mismatch would make every table unreadable.
func checkLocation(profile map[string]any, s config.Storage) error {
	str := func(key string) string { v, _ := profile[key].(string); return v }
	bucket := str("bucket")
	if str("type") == "adls" {
		bucket = str("filesystem")
	}
	if bucket == s.Bucket && strings.Trim(str("key-prefix"), "/") == s.Prefix {
		return nil
	}
	return &configError{fmt.Sprintf(
		"warehouse already exists in bucket %q with prefix %q, but IGLOO_WAREHOUSE_LOCATION is %s; "+
			"point IGLOO_WAREHOUSE_LOCATION at the existing data or use another IGLOO_CATALOG_WAREHOUSE",
		bucket, str("key-prefix"), s.Location)}
}

// configError is a setup problem that retrying will not fix.
type configError struct{ msg string }

func (e *configError) Error() string { return e.msg }

func (c *lakekeeper) call(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.api+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck

	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return &apiError{status: resp.StatusCode, body: string(data)}
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

// warehouseRequest builds the Lakekeeper create-warehouse request for the
// configured storage. When Igloo has no static keys, the catalog uses its own
// cloud identity ("system identity").
func warehouseRequest(cfg config.Config) (map[string]any, error) {
	s := cfg.Storage
	var profile, credential map[string]any

	switch s.Type {
	case config.StorageS3:
		profile = map[string]any{
			"type":              "s3",
			"bucket":            s.Bucket,
			"region":            s.S3.Region,
			"path-style-access": s.S3.URLStyle == "path",
			"sts-enabled":       false,
			"flavor":            "aws",
		}
		if s.S3.Endpoint != "" {
			scheme := "http"
			if s.S3.UseSSL {
				scheme = "https"
			}
			profile["endpoint"] = scheme + "://" + s.S3.Endpoint
			profile["flavor"] = "s3-compat"
		}
		credential = map[string]any{"type": "s3", "credential-type": "aws-system-identity"}
		if s.S3.AccessKeyID != "" {
			credential = map[string]any{
				"type":              "s3",
				"credential-type":   "access-key",
				"access-key-id":     s.S3.AccessKeyID,
				"secret-access-key": s.S3.SecretAccessKey,
			}
		}

	case config.StorageGCS:
		profile = map[string]any{"type": "gcs", "bucket": s.Bucket, "sts-enabled": false}
		credential = map[string]any{"type": "gcs", "credential-type": "gcp-system-identity"}

	case config.StorageAzure:
		profile = map[string]any{
			"type":         "adls",
			"account-name": s.Azure.Account,
			"filesystem":   s.Bucket,
			"sas-enabled":  false,
		}
		if s.Azure.Host != s.Azure.Account+".dfs.core.windows.net" {
			profile["host"] = strings.TrimPrefix(s.Azure.Host, s.Azure.Account+".")
		}
		credential = map[string]any{"type": "az", "credential-type": "azure-system-identity"}
		if s.Azure.AccountKey != "" {
			credential = map[string]any{"type": "az", "credential-type": "shared-access-key", "key": s.Azure.AccountKey}
		}

	default:
		return nil, fmt.Errorf("unsupported storage type %q", s.Type)
	}

	if s.Prefix != "" {
		profile["key-prefix"] = s.Prefix
	}

	return map[string]any{
		"warehouse-name":     cfg.Catalog.Warehouse,
		"storage-profile":    profile,
		"storage-credential": credential,
	}, nil
}
