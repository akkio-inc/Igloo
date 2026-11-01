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
	sandbox        []func() []string
	// background opens the second DuckDB of a process (see background.go).
	background bool
}

// background opens the second DuckDB of a process, for heavy statements.
func background() Option {
	return func(o *options) { o.background = true }
}

// SandboxDirectories adds the locations f returns, when the sandbox is
// locked, to those SQL may read: the locations of the tables of the Iceberg
// REST catalogs attached as connections (package federation), which
// BeforeLockdown found. Each must end in a slash.
func SandboxDirectories(f func() []string) Option {
	return func(o *options) { o.sandbox = append(o.sandbox, f) }
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

// sandboxDirs returns the locations SandboxDirectories added.
func (o options) sandboxDirs() []string {
	var out []string
	for _, f := range o.sandbox {
		out = append(out, f()...)
	}
	return out
}
