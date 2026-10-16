package catalog

import (
	"context"
	"fmt"
	"iter"

	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/config"
)

// Client is a direct connection to the Iceberg REST catalog. It reads and
// writes table files with Igloo's own storage credentials, as DuckDB does.
type Client struct {
	*rest.Catalog
}

// Open connects to the catalog Igloo serves.
func Open(ctx context.Context, cfg config.Config) (*Client, error) {
	cat, err := rest.NewCatalog(ctx, "igloo", cfg.Catalog.URI,
		rest.WithWarehouseLocation(cfg.Catalog.Warehouse),
		rest.WithAdditionalProps(ioProperties(cfg.Storage)),
		rest.WithCustomTransport(ownCredentials{}),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to catalog %s: %w", cfg.Catalog.URI, err)
	}
	return &Client{Catalog: cat}, nil
}

// Namespaces returns every namespace, including nested ones.
func (c *Client) Namespaces(ctx context.Context) ([]table.Identifier, error) {
	var all []table.Identifier
	var walk func(parent table.Identifier) error
	walk = func(parent table.Identifier) error {
		namespaces, err := c.ListNamespaces(ctx, parent)
		if err != nil {
			return err
		}
		for _, ns := range namespaces {
			all = append(all, ns)
			if err := walk(ns); err != nil {
				return err
			}
		}
		return nil
	}
	return all, walk(nil)
}

// Tables returns every table in every namespace.
func (c *Client) Tables(ctx context.Context) ([]table.Identifier, error) {
	return c.collect(ctx, c.ListTables)
}

// Views returns every view in every namespace.
func (c *Client) Views(ctx context.Context) ([]table.Identifier, error) {
	return c.collect(ctx, c.ListViews)
}

func (c *Client) collect(ctx context.Context, list func(context.Context, table.Identifier) iter.Seq2[table.Identifier, error]) ([]table.Identifier, error) {
	namespaces, err := c.Namespaces(ctx)
	if err != nil {
		return nil, err
	}
	var out []table.Identifier
	for _, ns := range namespaces {
		for ident, err := range list(ctx, ns) {
			if err != nil {
				return nil, err
			}
			out = append(out, ident)
		}
	}
	return out, nil
}

// Ident builds a table or view identifier from a schema (namespace) and name.
func Ident(schema, name string) table.Identifier {
	return table.Identifier{schema, name}
}
