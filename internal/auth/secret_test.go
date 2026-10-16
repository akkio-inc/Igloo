package auth

import (
	"bytes"
	"context"
	"testing"

	"github.com/akkio-inc/igloo/internal/testdb"
)

// TestSecret runs against a real database when IGLOO_TEST_DATABASE_URL is
// set (make test starts one).
func TestSecret(t *testing.T) {
	url := testdb.URL()
	if url == "" {
		t.Skip("IGLOO_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	a, err := OpenPostgresStore(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := OpenPostgresStore(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	// Two replicas get the same secret, and different names different ones.
	first, err := a.Secret(ctx, "test-secret")
	if err != nil {
		t.Fatal(err)
	}
	second, err := b.Secret(ctx, "test-secret")
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 32 || !bytes.Equal(first, second) {
		t.Errorf("replicas got %x and %x", first, second)
	}
	other, err := a.Secret(ctx, "test-secret-2")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first, other) {
		t.Error("two names share a secret")
	}
	mock, err := a.MockSecret(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first, mock) {
		t.Error("a named secret equals the mock-auth secret")
	}
}
