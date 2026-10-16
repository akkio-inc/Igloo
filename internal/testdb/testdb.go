// Package testdb gives each test binary a PostgreSQL database of its own,
// made in the server IGLOO_TEST_DATABASE_URL names. go test runs packages'
// tests in parallel, and tests that truncate or redact shared tables would
// otherwise see, and change, each other's rows.
//
// The databases are left behind: scripts/test.sh and CI throw the server
// away afterwards.
package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"sync"

	"github.com/jackc/pgx/v5"
)

const envVar = "IGLOO_TEST_DATABASE_URL"

var (
	once  sync.Once
	dbURL string
	dbErr error
)

// URL returns the URL of this test binary's own database, made the first
// time it is asked for, or "" when IGLOO_TEST_DATABASE_URL is not set. It
// panics if the database cannot be made, which fails the test binary with
// the reason.
func URL() string {
	base := os.Getenv(envVar)
	if base == "" {
		return ""
	}
	once.Do(func() { dbURL, dbErr = create(base) })
	if dbErr != nil {
		panic(fmt.Sprintf("testdb: %s: %v", envVar, dbErr))
	}
	return dbURL
}

func create(base string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	name := "igloo_test_" + hex.EncodeToString(b)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, base)
	if err != nil {
		return "", err
	}
	defer conn.Close(ctx) //nolint:errcheck
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		return "", err
	}
	u.Path = "/" + name
	return u.String(), nil
}
