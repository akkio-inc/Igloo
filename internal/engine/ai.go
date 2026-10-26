package engine

import (
	"context"

	"github.com/akkio-inc/igloo/internal/ai"
)

// aiHookBox holds the hook the AI functions find their statements with.
type aiHookBox struct{ hook ai.Hook }

// SetAI connects the AI functions (package ai) to the sessions whose
// statements call them. Until it is set, as on workers, which hold no
// providers' keys, every call fails.
func (e *Engine) SetAI(hook ai.Hook) {
	e.aiHook.Store(&aiHookBox{hook: hook})
}

// registerAI defines the AI functions on the database.
func (e *Engine) registerAI(ctx context.Context) error {
	conn, release, err := e.sqlConn(ctx)
	if err != nil {
		return err
	}
	defer release()
	return ai.Register(conn, func() ai.Hook {
		if b := e.aiHook.Load(); b != nil {
			return b.hook
		}
		return nil
	})
}
