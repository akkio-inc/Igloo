package ai

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/testdb"
)

// testStores runs fn against a MemoryStore, and a PostgresStore when
// IGLOO_TEST_DATABASE_URL is set (make test starts one).
func testStores(t *testing.T, fn func(t *testing.T, s Store)) {
	t.Run("memory", func(t *testing.T) { fn(t, NewMemoryStore()) })
	url := testdb.URL()
	if url == "" {
		return
	}
	t.Run("postgres", func(t *testing.T) {
		s, err := OpenPostgres(ctx, url, bytes.Repeat([]byte{9}, 32))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		for _, q := range []string{"DELETE FROM igloo_ai.usage", "DELETE FROM igloo_ai.integrations", "DELETE FROM igloo_ai.cache"} {
			if _, err := s.pool.Exec(ctx, q); err != nil {
				t.Fatal(err)
			}
		}
		fn(t, s)
	})
}

func TestStore(t *testing.T) {
	testStores(t, func(t *testing.T, s Store) {
		in := Integration{Name: "main", Provider: ProviderBedrock, Region: "us-east-1", AWSAccessKeyID: "AKIDEXAMPLE",
			AWSSecretAccessKey: "secret-example", Headers: map[string]string{"x-gateway": "g"}, Models: map[string]string{"m": "provider-m"},
			DefaultModel: "m", DecisionModel: "", Prices: map[string]Price{"provider-m": {Input: 1, Output: 5}}, MaxConcurrency: 4,
			Enabled: true, Comment: "the main one", CreatedBy: "igloo"}
		if err := s.CreateIntegration(ctx, in, false); err != nil {
			t.Fatal(err)
		}
		if err := s.CreateIntegration(ctx, in, false); !errors.Is(err, ErrExists) {
			t.Errorf("again: %v", err)
		}
		got, err := s.Integration(ctx, "main")
		if err != nil || got.AWSSecretAccessKey != "secret-example" || got.Headers["x-gateway"] != "g" || got.Models["m"] != "provider-m" ||
			got.Prices["provider-m"].Output != 5 || got.MaxConcurrency != 4 || got.CreatedBy != "igloo" || got.Region != "us-east-1" {
			t.Fatalf("read back %+v, %v", got, err)
		}
		_, v1, _ := s.State(ctx)
		got.Comment, got.Enabled = "changed", false
		if err := s.UpdateIntegration(ctx, got); err != nil {
			t.Fatal(err)
		}
		if again, _ := s.Integration(ctx, "main"); again.Comment != "changed" || again.Enabled {
			t.Errorf("updated %+v", again)
		}
		if err := s.GrantUsage(ctx, Grant{Integration: "main", Grantee: "analysts", GranteeType: "role"}); err != nil {
			t.Fatal(err)
		}
		if err := s.GrantUsage(ctx, Grant{Integration: "nope", Grantee: "x", GranteeType: "user"}); !errors.Is(err, ErrNotFound) {
			t.Errorf("grant on a missing integration: %v", err)
		}
		if gs, _ := s.Grants(ctx); len(gs) != 1 || gs[0].Grantee != "analysts" {
			t.Errorf("grants %+v", gs)
		}
		if _, v2, _ := s.State(ctx); v2 == v1 {
			t.Errorf("the version stayed %d across changes", v1)
		}
		if err := s.ForgetGrantee(ctx, "role", "analysts"); err != nil {
			t.Fatal(err)
		}
		if gs, _ := s.Grants(ctx); len(gs) != 0 {
			t.Errorf("grants after forgetting %+v", gs)
		}
		if err := s.DropIntegration(ctx, "main"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Integration(ctx, "main"); !errors.Is(err, ErrNotFound) {
			t.Errorf("after drop: %v", err)
		}

		// The cache.
		epoch, _, _ := s.State(ctx)
		now := time.Now()
		e := CacheEntry{Key: "k1", Function: "ai_complete", Model: "main/m", Value: []byte(`{"text":"a"}`), Tables: []string{"demo.t"},
			Epoch: epoch, Created: now, Expires: now.Add(time.Hour)}
		old := e
		old.Key, old.Expires = "k2", now.Add(-time.Minute)
		if ok, err := s.Put(ctx, epoch, []CacheEntry{e, old}); err != nil || !ok {
			t.Fatalf("put: %v %v", ok, err)
		}
		hits, err := s.Lookup(ctx, []string{"k1", "k2", "k3"}, now)
		if err != nil || len(hits) != 1 || string(hits["k1"].Value) != `{"text":"a"}` {
			t.Errorf("lookup %v %v", hits, err)
		}
		// Put again from another statement's tables: the tables of both.
		e2 := e
		e2.Tables = []string{"demo.u"}
		if _, err := s.Put(ctx, epoch, []CacheEntry{e2}); err != nil {
			t.Fatal(err)
		}
		if hits, _ := s.Lookup(ctx, []string{"k1"}, now); len(hits["k1"].Tables) != 2 {
			t.Errorf("tables %v", hits["k1"].Tables)
		}
		if n, err := s.Expire(ctx, now); err != nil || n != 1 {
			t.Errorf("expired %d %v", n, err)
		}
		if n, err := s.Purge(ctx, []string{"demo.u"}); err != nil || n != 1 {
			t.Errorf("purged %d %v", n, err)
		}
		if ok, err := s.Put(ctx, epoch, []CacheEntry{e}); err != nil || ok {
			t.Errorf("a put from before the purge was kept: %v %v", ok, err)
		}
		if n, _ := s.Remaining(ctx, []string{"demo.t"}); n != 0 {
			t.Errorf("%d remaining", n)
		}
	})
}
