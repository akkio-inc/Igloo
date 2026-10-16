package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	icecat "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

// Register adds the table whose metadata file is at metadataLoc to the
// catalog as ident, as the Iceberg REST register endpoint does. With
// replace, a table already called ident is replaced in the same step; the
// catalog drops it without deleting its files. iceberg-go cannot ask for
// the replacement, so Igloo makes this call itself.
func (c *Client) Register(ctx context.Context, ident table.Identifier, metadataLoc string, replace bool) (*table.Table, error) {
	prefix, err := c.prefix(ctx)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(map[string]any{
		"name":              icecat.TableNameFromIdent(ident),
		"metadata-location": metadataLoc,
		"overwrite":         replace,
	})
	if err != nil {
		return nil, err
	}
	ns := strings.Join(icecat.NamespaceFromIdent(ident), "\x1f")
	endpoint := strings.TrimSuffix(c.uri, "/") + "/v1/" + url.PathEscape(prefix) + "/namespaces/" + url.PathEscape(ns) + "/register"
	status, data, err := c.call(ctx, http.MethodPost, endpoint, body)
	if err != nil {
		return nil, err
	}
	switch {
	case status == http.StatusConflict:
		return nil, fmt.Errorf("%w: %s", icecat.ErrTableAlreadyExists, strings.Join(ident, "."))
	case status == http.StatusNotFound:
		return nil, fmt.Errorf("%w: %s", icecat.ErrNoSuchNamespace, ns)
	case status < 200 || status > 299:
		return nil, &apiError{status: status, body: string(data)}
	}
	return c.LoadTable(ctx, ident)
}

// NamespaceLocation is where the catalog puts the tables of a namespace
// (schema): Lakekeeper reports it as the namespace's location property.
func (c *Client) NamespaceLocation(ctx context.Context, ns string) (string, error) {
	props, err := c.LoadNamespaceProperties(ctx, table.Identifier{ns})
	if err != nil {
		return "", err
	}
	loc := strings.TrimSuffix(props["location"], "/")
	if loc == "" {
		return "", fmt.Errorf("the catalog does not say where schema %s keeps its tables", ns)
	}
	return loc, nil
}

// prefix is the catalog's path prefix for Igloo's warehouse, from its
// configuration endpoint.
func (c *Client) prefix(ctx context.Context) (string, error) {
	endpoint := strings.TrimSuffix(c.uri, "/") + "/v1/config?warehouse=" + url.QueryEscape(c.warehouse)
	status, data, err := c.call(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		return "", &apiError{status: status, body: string(data)}
	}
	var cfg struct {
		Defaults  map[string]string `json:"defaults"`
		Overrides map[string]string `json:"overrides"`
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return "", fmt.Errorf("read catalog configuration: %w", err)
	}
	if p := cfg.Overrides["prefix"]; p != "" {
		return p, nil
	}
	if p := cfg.Defaults["prefix"]; p != "" {
		return p, nil
	}
	return "", errors.New("the catalog's configuration names no prefix for the warehouse")
}

func (c *Client) call(ctx context.Context, method, endpoint string, body []byte) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := (&http.Client{Transport: ownCredentials{}}).Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close() //nolint:errcheck
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	return resp.StatusCode, data, err
}
