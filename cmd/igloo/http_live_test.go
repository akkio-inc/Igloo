package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/duckdb/duckdb-go/v2"
)

// The liveness check fails only when DuckDB has been invalidated: every
// other failure, a slow ping included, is readiness's, so a busy server is
// never restarted for being busy.
func TestLiveFailsOnlyOnAnInvalidatedDatabase(t *testing.T) {
	invalidated := &duckdb.Error{Type: duckdb.ErrorTypeFatal,
		Msg: "FATAL Error: Failed: database has been invalidated because of a previous fatal error."}
	cases := []struct {
		name string
		ping func(context.Context) error
		dead bool
	}{
		{"healthy", func(context.Context) error { return nil }, false},
		{"invalidated", func(context.Context) error { return invalidated }, true},
		{"invalidated, wrapped", func(context.Context) error { return fmt.Errorf("connect: %w", invalidated) }, true},
		{"another DuckDB error", func(context.Context) error {
			return &duckdb.Error{Type: duckdb.ErrorTypeIO, Msg: "IO Error: disk full"}
		}, false},
		{"not DuckDB's", func(context.Context) error { return errors.New("connection refused") }, false},
		{"slower than its bound", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }, false},
	}
	for _, c := range cases {
		err := live(context.Background(), c.ping)
		if (err != nil) != c.dead {
			t.Errorf("%s: live = %v, want dead %v", c.name, err, c.dead)
		}
	}
}
