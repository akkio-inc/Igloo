package engine

import (
	"context"

	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/modelpack"
	"github.com/akkio-inc/igloo/internal/tsforecast"
)

// Forecasting is the engine's forecasting with foundation models (package
// tsforecast), from the model pack.
func (e *Engine) Forecasting() *tsforecast.Service { return e.forecasting }

// setupForecasting reads the model pack (IGLOO_MODEL_DIR) and defines the
// forecasting functions, on the server and on workers alike.
func (e *Engine) setupForecasting(ctx context.Context, cfg config.Config) error {
	e.forecasting = tsforecast.New(modelpack.Open(cfg.Models.Dir))
	conn, release, err := e.sqlConn(ctx)
	if err != nil {
		return err
	}
	defer release()
	return tsforecast.Register(conn, e.forecasting)
}
