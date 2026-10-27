package engine

import (
	"context"

	"github.com/akkio-inc/igloo/internal/ai"
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
	if err := tsforecast.Register(conn, e.forecasting); err != nil {
		return err
	}
	// Hosted models, through AI integrations: on the server, which holds
	// their keys (see SetAI).
	return tsforecast.RegisterHosted(conn, func() ai.Hook {
		if b := e.aiHook.Load(); b != nil {
			return b.hook
		}
		return nil
	})
}
