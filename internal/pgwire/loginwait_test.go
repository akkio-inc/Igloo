package pgwire

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestLoginWaitsForTheCatalogLockOnlySoLong holds the catalog lock
// exclusively, as a listing or DDL that never finishes would, and checks
// that a login fails once the server's login wait is up, saying why, rather
// than waiting with it, and that logins work again once the lock is free.
func TestLoginWaitsForTheCatalogLockOnlySoLong(t *testing.T) {
	env := newCatalogEnv(t)
	env.srv.loginWait = 500 * time.Millisecond
	unlock := env.srv.engine.CatalogLock()
	locked := true
	defer func() {
		if locked {
			unlock()
		}
	}()
	done := make(chan error, 1)
	go func() {
		conn, err := tryConnect(env.url("igloo", adminPassword), pgx.QueryExecModeSimpleProtocol)
		if err == nil {
			_ = conn.Close(context.Background())
		}
		done <- err
	}()
	var err error
	select {
	case err = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("a login waited for the catalog lock past the login wait")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "57P03" || !strings.Contains(pgErr.Message, "has held for") {
		t.Fatalf("logging in while the catalog lock was held: %v", err)
	}
	unlock()
	locked = false
	if n := count(t, env.admin(t, pgx.QueryExecModeSimpleProtocol), "SELECT 42"); n != 42 {
		t.Errorf("got %d", n)
	}
}
