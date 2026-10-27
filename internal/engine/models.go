package engine

import (
	"context"

	iceio "github.com/apache/iceberg-go/io"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/models"
)

// Models is the engine's runtime for models (package models): the blobs
// it reads them from, and the functions that score them and serve their
// methods' results.
func (e *Engine) Models() *models.Runtime { return e.models }

// setupModels gives the engine a model runtime whose blobs are in the
// warehouse, or in memory without one, and defines its functions: the
// server's all of them, a worker's the scoring function.
func (e *Engine) setupModels(ctx context.Context, cfg config.Config, server bool) error {
	e.models = models.NewRuntime()
	if cfg.Storage.Location != "" {
		e.models.SetBlobs(models.NewWarehouse(cfg.Storage.Root(), func(ctx context.Context) (iceio.IO, error) {
			return catalog.FS(ctx, cfg.Storage, cfg.Storage.Location)
		}))
	} else {
		e.models.SetBlobs(models.NewMemory())
	}
	conn, release, err := e.sqlConn(ctx)
	if err != nil {
		return err
	}
	defer release()
	if server {
		return e.models.RegisterServer(conn)
	}
	return e.models.RegisterScoring(conn)
}
