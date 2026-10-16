package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/config"
)

// OpenWorker starts DuckDB for a distributed-execution worker (igloo
// worker): object storage credentials, the read cache and the sandbox are
// set up as for the server, and no catalog is attached, since a worker
// reads only the data files its tasks name.
func OpenWorker(ctx context.Context, cfg config.Config, log *slog.Logger) (*Engine, error) {
	connector, err := duckdb.NewConnector(cfg.DuckDB.Path, nil)
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	p, err := newParser()
	if err != nil {
		return nil, errors.Join(err, connector.Close())
	}
	e := &Engine{connector: connector, parser: p}
	if err := e.setupWorker(ctx, cfg, log); err != nil {
		p.close()
		return nil, errors.Join(err, connector.Close())
	}
	if e.cache != nil {
		e.startCache(log)
	}
	return e, nil
}

func (e *Engine) setupWorker(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	c, err := e.connector.Connect(ctx)
	if err != nil {
		return fmt.Errorf("connect to duckdb: %w", err)
	}
	defer c.Close() //nolint:errcheck
	conn := c.(*duckdb.Conn)
	exec := func(stmt string) error {
		_, err := conn.ExecContext(ctx, stmt, nil)
		return err
	}
	for _, stmt := range settingStatements(cfg.DuckDB) {
		if err := exec(stmt); err != nil {
			return fmt.Errorf("configure duckdb (%s): %w", stmt, err)
		}
	}
	if e.local, err = queryString(ctx, conn, "SELECT current_database()"); err != nil {
		return fmt.Errorf("read database name: %w", err)
	}
	for _, stmt := range storageStatements(cfg.Storage) {
		if err := exec(stmt); err != nil {
			// Never echo the statement: it can contain a secret key.
			return fmt.Errorf("configure object storage: %w", err)
		}
	}
	if err := e.setupCache(exec, cfg, log); err != nil {
		return err
	}
	return e.lockDown(exec, cfg)
}

// Threads returns the number of threads DuckDB runs queries on.
func (e *Engine) Threads(ctx context.Context) (int, error) {
	v, err := e.setting(ctx, "threads")
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(v)
}

func (e *Engine) setting(ctx context.Context, name string) (string, error) {
	c, err := e.connector.Connect(ctx)
	if err != nil {
		return "", err
	}
	defer c.Close() //nolint:errcheck
	return queryString(ctx, c.(*duckdb.Conn), "SELECT current_setting("+literal(name)+")::VARCHAR")
}
