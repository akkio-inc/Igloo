package engine

import (
	"context"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/catalogproxy"
)

// Option configures Open.
type Option func(*options)

type options struct {
	beforeLockdown []func(context.Context, *duckdb.Conn) error
	catalogProxy   []func(*catalogproxy.Options)
}

// BeforeLockdown runs f on the setup connection once storage and the
// catalog are set up and before the sandbox is locked: the last moment at
// which DuckDB loads extensions and attaches other databases (see
// package federation). An error from f fails Open.
func BeforeLockdown(f func(context.Context, *duckdb.Conn) error) Option {
	return func(o *options) { o.beforeLockdown = append(o.beforeLockdown, f) }
}

// CatalogProxy adjusts the options of the proxy DuckDB reaches the catalog
// through. Tests shorten its timings.
func CatalogProxy(f func(*catalogproxy.Options)) Option {
	return func(o *options) { o.catalogProxy = append(o.catalogProxy, f) }
}

func beforeLockdown(ctx context.Context, conn *duckdb.Conn, o options) error {
	for _, f := range o.beforeLockdown {
		if err := f(ctx, conn); err != nil {
			return err
		}
	}
	return nil
}
