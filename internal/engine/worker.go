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
// worker): the spill directory, object storage credentials, the read cache
// and the sandbox are set up as for the server, and no catalog is attached,
// since a worker reads only the data files its tasks name.
func OpenWorker(ctx context.Context, cfg config.Config, log *slog.Logger) (*Engine, error) {
	release := claimTempDir(cfg.DuckDB.TempDir, log)
	connector, err := duckdb.NewConnector(unsignedDSN(cfg.DuckDB.Path, ownToLoad(cfg)), nil)
	if err != nil {
		release()
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	p, err := newParser()
	if err != nil {
		err = errors.Join(err, connector.Close())
		release()
		return nil, err
	}
	e := &Engine{connector: connector, parser: p, releaseTempDir: release, log: log}
	if err := e.setupWorker(ctx, cfg, log); err != nil {
		p.close()
		err = errors.Join(err, connector.Close())
		release()
		return nil, err
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
	query := func(q string) (string, error) { return queryString(ctx, conn, q) }
	if err := configure(cfg, workerThreadsPerCore, exec, query); err != nil {
		return err
	}
	own, err := loadOwn(cfg, log, exec, query)
	if err != nil {
		return err
	}
	e.bitmaps, e.icebergOwn = own["rb"], own["iceberg"]
	if e.local, err = queryString(ctx, conn, "SELECT current_database()"); err != nil {
		return fmt.Errorf("read database name: %w", err)
	}
	// Workers score models in the statements they run, reading blobs from
	// the warehouse with the same credentials as its tables' files.
	if err := e.setupModels(ctx, cfg, false); err != nil {
		return fmt.Errorf("register model functions: %w", err)
	}
	// And forecast with the model pack's models, as the server does.
	if err := e.setupForecasting(ctx, cfg); err != nil {
		return fmt.Errorf("register forecasting functions: %w", err)
	}
	useAzureIdentity(cfg.Storage, log)
	for _, stmt := range storageStatements(cfg.Storage) {
		if err := exec(stmt); err != nil {
			// Never echo the statement: it can contain a secret key.
			return fmt.Errorf("configure object storage: %w", err)
		}
	}
	if err := e.setupCache(exec, cfg, log); err != nil {
		return err
	}
	return e.lockDown(exec, cfg, nil)
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
